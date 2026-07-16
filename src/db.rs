//! Database entry point.
//!
//! The `DB` struct is the main entry point for the storage engine.
//! It owns all components and provides the public API.

mod options;

pub use options::Options;

use crate::allocator::PageAllocator;
use crate::blob::BlobManager;
use crate::btree::{BTree, LookupResult, PAGE_SIZE};
use crate::buffer::BufferManager;
use crate::concurrency::TransactionManager;
use crate::error::{Error, Result};
use crate::mvcc::PMT;
use crate::recovery::{ParseStatus, RecordType, SyncPolicy, WalManager, WalRecord};
use crate::space::{Device, DeviceOptions};
use crate::storage::StorageEngine;
use crate::storage::format::{
    CommitId, CommitRecord, DatabaseId, FORMAT_VERSION, GenerationId, HistoryId, Manifest,
    ManifestStore, PmtCheckpointId,
};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(any(test, feature = "fault-injection"))]
use std::cell::Cell;

#[cfg(any(test, feature = "fault-injection"))]
thread_local! {
    static FAIL_NEXT_ATOMIC_RENAME: Cell<bool> = const { Cell::new(false) };
}

/// File names for the database.
const DATA_FILE: &str = "seerdb.data";
const BLOB_FILE: &str = "seerdb.blob";
const WAL_FILE: &str = "seerdb.wal";
const META_FILE: &str = "seerdb.meta";
const MANIFEST_FILE: &str = "MANIFEST";

/// Blob GC statistics.
pub struct BlobStats {
    /// Number of files needing garbage collection.
    pub files_needing_gc: usize,
    /// Total valid entries across all files.
    pub total_valid: usize,
    /// Total deleted entries across all files.
    pub total_deleted: usize,
}

/// A seerdb database instance.
///
/// Provides key-value storage with:
/// - Out-of-place B-tree (pages never updated in place)
/// - KV separation (large values in blob files)
/// - WAL for crash recovery
/// - Buffer pool for caching
pub struct DB {
    /// Database directory path.
    path: PathBuf,
    /// Configuration options.
    #[expect(dead_code)]
    options: Options,
    /// Storage engine (coordinates B-tree, buffer, PMT, device).
    engine: StorageEngine,
    /// WAL manager.
    wal: WalManager,
    /// Blob manager.
    blobs: BlobManager,
    /// Transaction manager for MVCC.
    txn_manager: TransactionManager,
    /// Authoritative root-generation publication store.
    manifest: ManifestStore,
    /// Stable database identity.
    database_id: DatabaseId,
    /// Stable logical history identity.
    history_id: HistoryId,
    /// Latest published generation.
    generation_id: GenerationId,
    /// Latest published commit.
    commit_id: CommitId,
    /// Number of mutation records since the last published generation.
    pending_mutations: u64,
    /// Digest over pending mutation records.
    pending_digest: u32,
    /// Whether the database is open.
    is_open: bool,
    /// Whether a failed publication fenced this writer until reopen.
    write_fenced: bool,
}

impl DB {
    /// Open or create a database at the given path.
    pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Create directory if it doesn't exist.
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }

        let data_path = path.join(DATA_FILE);
        let wal_path = path.join(WAL_FILE);
        let meta_path = path.join(META_FILE);
        let manifest_path = path.join(MANIFEST_FILE);

        let mut manifest = ManifestStore::open(&manifest_path)?;
        let current_manifest = manifest.load_latest()?;
        let (database_id, history_id, generation_id, commit_id) =
            if let Some(current) = current_manifest {
                if current.page_size as usize != PAGE_SIZE {
                    return Err(Error::Corruption(format!(
                        "manifest page size {} does not match build page size {PAGE_SIZE}",
                        current.page_size
                    )));
                }
                (
                    current.database_id,
                    current.history_id,
                    current.generation_id,
                    current.commit_id,
                )
            } else {
                (
                    Self::new_database_id(&path),
                    HistoryId::new(1),
                    GenerationId::new(0),
                    CommitId::new(0),
                )
            };

        // Open the data file.
        let device_opts = DeviceOptions {
            use_odirect: options.use_odirect,
            sync_writes: options.sync_writes,
            create: true,
        };
        let device = Device::open(&data_path, &device_opts)?;

        // Create buffer manager.
        let buffer = BufferManager::new(options.buffer_pool_size);

        // Create WAL manager.
        let sync_policy = if options.sync_writes {
            SyncPolicy::FDataSync
        } else {
            SyncPolicy::None
        };
        let wal = WalManager::new(sync_policy);

        // Create blob manager.
        let blob_path = path.join(BLOB_FILE);
        let mut blobs = if blob_path.exists() {
            // Load blob files from disk.
            let blob_data = fs::read(&blob_path)?;
            BlobManager::from_bytes(&blob_data)
                .unwrap_or_else(|| BlobManager::with_threshold(options.blob_threshold))
        } else {
            BlobManager::with_threshold(options.blob_threshold)
        };

        // A published manifest selects an immutable PMT checkpoint. Never
        // pair an older manifest with a newer mutable metadata file.
        let (pmt, allocator) = if let Some(current) = current_manifest {
            if current.pmt_checkpoint_id.get() == 0 {
                (PMT::new(), PageAllocator::new())
            } else {
                let checkpoint_path =
                    path.join(format!("seerdb.meta.{}", current.pmt_checkpoint_id.get()));
                Self::load_meta(&checkpoint_path)?
            }
        } else if meta_path.exists() {
            Self::load_meta(&meta_path)?
        } else {
            (PMT::new(), PageAllocator::new())
        };

        // Create storage engine.
        let mut engine = StorageEngine::new(BTree::new(), buffer, pmt, allocator, device);

        // A published manifest selects the PMT locations for the latest
        // generation. Without one, retain the legacy scan as a migration path.
        if let Some(current) = current_manifest {
            engine.load_from_manifest(current.root_page_id)?;
        } else if !wal_path.exists() {
            engine.load_from_disk()?;
        }

        let recovery = if wal_path.exists() {
            Some(Self::recover_from_wal(
                &wal_path,
                engine.btree_mut(),
                &mut blobs,
            )?)
        } else {
            None
        };

        let mut db = Self {
            path,
            options,
            engine,
            wal,
            blobs,
            txn_manager: TransactionManager::new(),
            manifest,
            database_id,
            history_id,
            generation_id,
            commit_id,
            pending_mutations: 0,
            pending_digest: 0,
            is_open: true,
            write_fenced: false,
        };

        if current_manifest.is_none() && !wal_path.exists() && !meta_path.exists() {
            db.manifest.publish(Manifest {
                database_id: db.database_id,
                history_id: db.history_id,
                generation_id: GenerationId::new(0),
                commit_id: CommitId::new(0),
                page_size: PAGE_SIZE as u32,
                root_page_id: db.engine.btree().root_id() as u64,
                pmt_checkpoint_id: PmtCheckpointId::new(0),
                wal_segment: 0,
                wal_offset: 0,
                mutation_count: 0,
                digest: 0,
                format_version: FORMAT_VERSION,
            })?;
        }

        if let Some(recovery) = recovery {
            if let Some(commit) = recovery.last_commit {
                db.publish_recovered(commit, recovery.last_commit_offset)?;
            } else {
                // Complete mutations without a commit envelope are not
                // visible in the durable protocol and may be discarded.
                fs::remove_file(&wal_path)?;
            }
        }

        Ok(db)
    }

    /// Insert a key-value pair.
    ///
    /// The mutation is applied in memory, journaled durably, and included in
    /// the next published root generation.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_writable()?;

        // Mutate memory first, then make the successful mutation durable in
        // the WAL. No page is written before the WAL reaches disk, and an
        // operation that fails never enters a committed WAL batch.
        if self.blobs.should_separate(value.len()) {
            let ptr = self.blobs.append(key, value.to_vec());
            self.engine.btree_mut().insert_blob(key, ptr)?;
        } else {
            self.engine.btree_mut().upsert(key, value)?;
        }

        self.journal_mutation(WalRecord::put(key, value))?;
        let _page_id = self.engine.allocator_mut().alloc();

        Ok(())
    }

    /// Get a value by key.
    ///
    /// Read path:
    /// 1. Lookup key in B-tree
    /// 2. If value is inline, return it
    /// 3. If value is blob pointer, read from blob file
    /// 4. If deleted (tombstone), return None
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_open()?;

        match self.engine.btree().lookup(key) {
            LookupResult::Found(value) => Ok(Some(value.to_vec())),
            LookupResult::Blob(ptr) => {
                // Read from blob file.
                match self.blobs.read(&ptr) {
                    Some(data) => Ok(Some(data.to_vec())),
                    None => Err(Error::Corruption("blob pointer invalid".into())),
                }
            }
            LookupResult::Deleted => Ok(None),
            LookupResult::NotFound => Ok(None),
        }
    }

    /// Delete a key.
    ///
    /// The tombstone is applied in memory, journaled durably, and included in
    /// the next published root generation.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.check_writable()?;

        if let LookupResult::Blob(ptr) = self.engine.btree().lookup(key) {
            self.blobs.mark_deleted(&ptr);
        }

        let found = self.engine.btree_mut().delete(key)?;
        self.journal_mutation(WalRecord::delete(key))?;
        Ok(found)
    }

    /// Range scan over [start, end).
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.engine.btree().range_scan(start, end).collect()
    }

    /// Write buffered WAL records to disk and sync the mutation prefix.
    fn write_wal_to_disk(&mut self) -> Result<()> {
        let mut wal_buf = Vec::new();
        self.wal.flush(&mut wal_buf)?;
        if !wal_buf.is_empty() {
            let wal_path = self.path.join(WAL_FILE);
            // Append to WAL file (not overwrite).
            use std::io::Write;
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)?;
            file.write_all(&wal_buf)?;
            // Sync to ensure WAL is persisted before modifying data.
            file.sync_data()?;
        }
        Ok(())
    }

    /// Journal a mutation after it has successfully changed memory state.
    fn journal_mutation(&mut self, record: WalRecord) -> Result<()> {
        self.wal.append(&record);
        if let Err(error) = self.write_wal_to_disk() {
            self.write_fenced = true;
            return Err(error);
        }
        self.pending_mutations = self
            .pending_mutations
            .checked_add(1)
            .ok_or_else(|| Error::Wal("mutation count overflow".into()))?;
        self.pending_digest = extend_digest(self.pending_digest, &record);
        Ok(())
    }

    /// Publish a generation after its pages and checkpoints are durable.
    fn publish_generation(
        &mut self,
        commit: CommitRecord,
        append_commit: bool,
        recovered_wal_offset: u64,
    ) -> Result<()> {
        self.engine.flush()?;

        let checkpoint_path = self
            .path
            .join(format!("seerdb.meta.{}", commit.generation_id.get()));
        Self::save_meta(&checkpoint_path, self.engine.pmt(), self.engine.allocator())?;
        // Keep the legacy filename as a compatibility/debug snapshot. It is
        // never authoritative once a manifest selects a checkpoint.
        let meta_path = self.path.join(META_FILE);
        Self::save_meta(&meta_path, self.engine.pmt(), self.engine.allocator())?;

        let blob_path = self.path.join(BLOB_FILE);
        atomic_write(&blob_path, &self.blobs.to_bytes())?;

        let wal_path = self.path.join(WAL_FILE);
        let wal_offset = if append_commit {
            let offset = fs::metadata(&wal_path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            self.wal.append(&WalRecord::commit(commit));
            self.write_wal_to_disk()?;
            offset
        } else {
            recovered_wal_offset
        };

        let manifest = Manifest {
            database_id: self.database_id,
            history_id: self.history_id,
            generation_id: commit.generation_id,
            commit_id: commit.commit_id,
            page_size: PAGE_SIZE as u32,
            root_page_id: commit.root_page_id,
            pmt_checkpoint_id: PmtCheckpointId::new(commit.generation_id.get()),
            wal_segment: 0,
            wal_offset,
            mutation_count: commit.mutation_count,
            digest: commit.digest,
            format_version: FORMAT_VERSION,
        };
        self.manifest.publish(manifest)?;

        if wal_path.exists() {
            fs::remove_file(&wal_path)?;
            sync_directory(&self.path)?;
        }

        self.generation_id = commit.generation_id;
        self.commit_id = commit.commit_id;
        self.pending_mutations = 0;
        self.pending_digest = 0;
        Ok(())
    }

    /// Checkpoint a committed WAL prefix discovered during reopen.
    fn publish_recovered(&mut self, commit: CommitRecord, wal_offset: u64) -> Result<()> {
        if let Err(error) = self.publish_generation(commit, false, wal_offset) {
            self.write_fenced = true;
            return Err(error);
        }
        Ok(())
    }

    /// Flush all pending writes as one durable root generation.
    pub fn flush(&mut self) -> Result<()> {
        self.check_writable()?;
        if self.pending_mutations == 0 {
            return Ok(());
        }

        let commit = CommitRecord {
            commit_id: CommitId::new(
                self.commit_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("commit ID overflow".into()))?,
            ),
            generation_id: GenerationId::new(
                self.generation_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
            ),
            root_page_id: self.engine.btree().root_id() as u64,
            mutation_count: self.pending_mutations,
            digest: self.pending_digest,
        };

        if let Err(error) = self.publish_generation(commit, true, 0) {
            self.write_fenced = true;
            return Err(error);
        }
        Ok(())
    }

    /// Close the database (flush and sync).
    pub fn close(&mut self) -> Result<()> {
        if self.is_open {
            self.flush()?;
            self.is_open = false;
        }
        Ok(())
    }

    /// Run garbage collection on blob files.
    ///
    /// Returns the number of entries reclaimed.
    pub fn gc(&mut self) -> Result<usize> {
        self.check_open()?;
        let reclaimed = self.blobs.gc();
        if reclaimed > 0 {
            // Flush after GC to persist changes.
            self.flush()?;
        }
        Ok(reclaimed)
    }

    /// Get blob GC statistics.
    pub fn blob_stats(&self) -> BlobStats {
        BlobStats {
            files_needing_gc: self.blobs.files_needing_gc().len(),
            total_valid: self.blobs.total_valid_entries(),
            total_deleted: self.blobs.total_deleted_entries(),
        }
    }

    /// Begin a new transaction.
    ///
    /// Returns a transaction handle that can be used to commit or abort.
    pub fn begin_transaction(&self) -> crate::concurrency::Transaction {
        self.txn_manager.begin()
    }

    /// Commit a transaction.
    pub fn commit_transaction(&self, txn: &mut crate::concurrency::Transaction) {
        self.txn_manager.commit(txn);
    }

    /// Abort a transaction.
    pub fn abort_transaction(&self, txn: &mut crate::concurrency::Transaction) {
        self.txn_manager.abort(txn);
    }

    /// Get the latest committed transaction ID.
    pub fn latest_committed_txn(&self) -> u64 {
        self.txn_manager.latest_committed()
    }

    /// Check if the database is open.
    fn check_open(&self) -> Result<()> {
        if !self.is_open {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        Ok(())
    }

    /// Reject writes after a failed publication until the database is reopened.
    fn check_writable(&self) -> Result<()> {
        self.check_open()?;
        if self.write_fenced {
            return Err(Error::NeedsRecovery(
                "writer fenced after a failed durable publication; reopen required".into(),
            ));
        }
        Ok(())
    }

    /// Generate a stable-enough identity for a newly created database.
    fn new_database_id(path: &Path) -> DatabaseId {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path_digest = crc32c::crc32c(path.to_string_lossy().as_bytes());
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&(now as u64).to_le_bytes());
        bytes[8..12].copy_from_slice(&path_digest.to_le_bytes());
        bytes[12..16].copy_from_slice(&std::process::id().to_le_bytes());
        DatabaseId::new(bytes)
    }

    /// Load PMT and allocator from meta file.
    fn load_meta(path: &Path) -> Result<(PMT, PageAllocator)> {
        let data = fs::read(path)?;

        if data.len() < 4 {
            return Err(Error::Corruption("meta file too small".into()));
        }

        let pmt_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

        if data.len() < 4 + pmt_len + 4 {
            return Err(Error::Corruption("meta file truncated".into()));
        }

        let pmt = PMT::from_bytes(&data[4..4 + pmt_len])
            .ok_or_else(|| Error::Corruption("invalid PMT data".into()))?;

        let alloc_offset = 4 + pmt_len;
        let alloc_len = u32::from_le_bytes([
            data[alloc_offset],
            data[alloc_offset + 1],
            data[alloc_offset + 2],
            data[alloc_offset + 3],
        ]) as usize;

        let alloc_data = &data[alloc_offset + 4..alloc_offset + 4 + alloc_len];
        let allocator = PageAllocator::from_bytes(alloc_data)
            .ok_or_else(|| Error::Corruption("invalid allocator data".into()))?;

        Ok((pmt, allocator))
    }

    /// Save PMT and allocator to meta file.
    fn save_meta(path: &Path, pmt: &PMT, allocator: &PageAllocator) -> Result<()> {
        let pmt_bytes = pmt.to_bytes();
        let alloc_bytes = allocator.to_bytes();

        let mut buf = Vec::with_capacity(4 + pmt_bytes.len() + 4 + alloc_bytes.len());
        buf.extend_from_slice(&(pmt_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&pmt_bytes);
        buf.extend_from_slice(&(alloc_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&alloc_bytes);

        atomic_write(path, &buf)
    }

    /// Recover a committed WAL prefix and reject corrupt complete records.
    fn recover_from_wal(
        wal_path: &Path,
        btree: &mut BTree,
        blobs: &mut BlobManager,
    ) -> Result<RecoverySummary> {
        let wal_data = fs::read(wal_path)?;
        let (records, status) = WalManager::parse_records_with_status(&wal_data);
        if status == ParseStatus::Corrupt {
            return Err(Error::Corruption("invalid complete WAL record".into()));
        }

        let mut pending = Vec::new();
        let mut last_commit = None;
        let mut last_commit_offset = 0;
        let mut offset = 0u64;
        for record in &records {
            let record_len = record.to_bytes().len() as u64;
            match record.record_type {
                RecordType::Put | RecordType::Delete => pending.push(record),
                RecordType::Commit => {
                    let commit = record
                        .commit_record()
                        .ok_or_else(|| Error::Corruption("invalid WAL commit envelope".into()))?;
                    if commit.mutation_count != pending.len() as u64
                        || commit.digest != digest_records(&pending)
                    {
                        return Err(Error::Corruption(
                            "WAL commit does not match its mutation prefix".into(),
                        ));
                    }
                    for mutation in pending.drain(..) {
                        apply_mutation(mutation, btree, blobs)?;
                    }
                    last_commit = Some(commit);
                    last_commit_offset = offset;
                }
                _ => {}
            }
            offset += record_len;
        }

        Ok(RecoverySummary {
            last_commit,
            last_commit_offset,
        })
    }
}

/// Recovery result for the committed WAL prefix.
#[derive(Debug, Clone, Copy)]
struct RecoverySummary {
    last_commit: Option<CommitRecord>,
    last_commit_offset: u64,
}

fn extend_digest(current: u32, record: &WalRecord) -> u32 {
    let bytes = record.to_bytes();
    let mut input = Vec::with_capacity(4 + bytes.len());
    input.extend_from_slice(&current.to_le_bytes());
    input.extend_from_slice(&bytes);
    crc32c::crc32c(&input)
}

fn digest_records(records: &[&WalRecord]) -> u32 {
    records
        .iter()
        .fold(0, |digest, record| extend_digest(digest, record))
}

fn apply_mutation(record: &WalRecord, btree: &mut BTree, blobs: &mut BlobManager) -> Result<()> {
    match record.record_type {
        RecordType::Put => {
            if record.payload.len() < 4 {
                return Err(Error::Corruption("WAL put record too small".into()));
            }
            let key_len = u16::from_le_bytes([record.payload[0], record.payload[1]]) as usize;
            let value_len_offset = 2usize
                .checked_add(key_len)
                .ok_or_else(|| Error::Corruption("WAL key length overflow".into()))?;
            if record.payload.len() < value_len_offset + 2 {
                return Err(Error::Corruption("WAL put key is truncated".into()));
            }
            let value_len = u16::from_le_bytes([
                record.payload[value_len_offset],
                record.payload[value_len_offset + 1],
            ]) as usize;
            let value_offset = value_len_offset + 2;
            if record.payload.len() != value_offset + value_len {
                return Err(Error::Corruption("WAL put value is truncated".into()));
            }
            let key = &record.payload[2..value_len_offset];
            let value = &record.payload[value_offset..];
            if blobs.should_separate(value.len()) {
                let pointer = blobs.append(key, value.to_vec());
                btree.insert_blob(key, pointer)?;
            } else {
                btree.upsert(key, value)?;
            }
        }
        RecordType::Delete => {
            if record.payload.len() < 2 {
                return Err(Error::Corruption("WAL delete record too small".into()));
            }
            let key_len = u16::from_le_bytes([record.payload[0], record.payload[1]]) as usize;
            if record.payload.len() != 2 + key_len {
                return Err(Error::Corruption("WAL delete key is truncated".into()));
            }
            let key = &record.payload[2..];
            if let LookupResult::Blob(pointer) = btree.lookup(key) {
                blobs.mark_deleted(&pointer);
            }
            btree.delete(key)?;
        }
        _ => {
            return Err(Error::Corruption(
                "non-mutation passed to WAL applier".into(),
            ));
        }
    }
    Ok(())
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(data)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);

    #[cfg(any(test, feature = "fault-injection"))]
    let injected = FAIL_NEXT_ATOMIC_RENAME.with(|failure| failure.replace(false));
    #[cfg(any(test, feature = "fault-injection"))]
    if injected {
        return Err(std::io::Error::other("injected atomic rename failure").into());
    }

    fs::rename(&temporary, path)?;
    sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
}

#[cfg(any(test, feature = "fault-injection"))]
#[expect(dead_code)]
fn inject_atomic_rename_failure() {
    FAIL_NEXT_ATOMIC_RENAME.with(|failure| failure.set(true));
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    Ok(())
}

impl Drop for DB {
    fn drop(&mut self) {
        // Don't call close() — let the WAL persist for crash recovery.
        // The user should explicitly call close() or flush() to ensure
        // data is persisted and WAL is cleaned up.
        // If the process crashes, the WAL file will be preserved for recovery.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempdir;

    use crate::storage::format::MANIFEST_SLOT_SIZE;

    #[test]
    fn test_db_open() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path().join("test.db"), Options::default());
        assert!(db.is_ok());
    }

    #[test]
    fn test_db_put_get() {
        let dir = tempdir().unwrap();
        let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();

        assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(db.get(b"key3").unwrap(), None);
    }

    #[test]
    fn test_db_delete() {
        let dir = tempdir().unwrap();
        let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

        db.put(b"key", b"value").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));

        db.delete(b"key").unwrap();
        assert_eq!(db.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_db_range() {
        let dir = tempdir().unwrap();
        let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
        db.put(b"d", b"4").unwrap();

        let results = db.range(b"b", b"d");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"b");
        assert_eq!(results[1].0, b"c");
    }

    #[test]
    fn test_db_close() {
        let dir = tempdir().unwrap();
        let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

        db.put(b"key", b"value").unwrap();
        db.close().unwrap();

        // Operations after close should fail.
        assert!(db.put(b"key2", b"value2").is_err());
    }

    #[test]
    fn test_db_meta_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create and populate.
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();
        }

        // Meta file should exist.
        assert!(path.join(META_FILE).exists());
    }

    #[test]
    fn test_db_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Write data and close.
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key1", b"value1").unwrap();
            db.flush().unwrap();
            db.put(b"key2", b"value2").unwrap();
            db.put(b"key3", b"value3").unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        // Reopen and verify data persisted.
        {
            let db = DB::open(&path, Options::default()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
            assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
            assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        }
    }

    #[test]
    fn test_db_rejects_corrupt_page_checksum() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();
        }

        let data_path = path.join(DATA_FILE);
        let mut data = fs::read(&data_path).unwrap();
        assert!(data.len() >= crate::btree::PAGE_SIZE);
        data[crate::btree::PAGE_SIZE - 1] ^= 0x01;
        fs::write(&data_path, data).unwrap();

        let result = DB::open(&path, Options::default());
        assert!(matches!(
            result,
            Err(Error::Corruption(message)) if message.contains("checksum mismatch")
        ));
    }

    #[test]
    fn test_db_discards_uncommitted_wal_suffix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Write data (WAL is written to disk on each put).
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key1", b"value1").unwrap();
            db.put(b"key2", b"value2").unwrap();
            db.put(b"key3", b"value3").unwrap();
            // Don't flush — simulate crash.
            // WAL should be on disk.
        }

        // Verify WAL exists.
        assert!(path.join(WAL_FILE).exists(), "WAL should exist after put");

        // Reopen and verify uncommitted mutations are not visible.
        {
            let db = DB::open(&path, Options::default()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), None);
            assert_eq!(db.get(b"key2").unwrap(), None);
            assert_eq!(db.get(b"key3").unwrap(), None);
        }

        // The uncommitted WAL suffix can be discarded after reopen.
        assert!(
            !path.join(WAL_FILE).exists(),
            "WAL should be deleted after recovery"
        );
    }

    #[test]
    fn test_db_recovers_committed_wal_prefix_with_torn_suffix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let records = vec![
            WalRecord::put(b"key1", b"value1"),
            WalRecord::put(b"key2", b"value2"),
            WalRecord::put(b"key3", b"value3"),
        ];
        let references: Vec<_> = records.iter().collect();
        let commit = CommitRecord {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            root_page_id: 0,
            mutation_count: records.len() as u64,
            digest: digest_records(&references),
        };
        let mut wal_bytes = Vec::new();
        for record in &records {
            wal_bytes.extend_from_slice(&record.to_bytes());
        }
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        wal_bytes.extend_from_slice(&[0xA5, 0x5A, 0x01]);
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let db = DB::open(&path, Options::default()).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert!(!path.join(WAL_FILE).exists());
        assert!(path.join(MANIFEST_FILE).exists());
    }

    #[test]
    fn test_db_reopen_accepts_every_wal_truncation_prefix() {
        let records = vec![
            WalRecord::put(b"key1", b"value1"),
            WalRecord::put(b"key2", b"value2"),
            WalRecord::put(b"key3", b"value3"),
        ];
        let references: Vec<_> = records.iter().collect();
        let commit = CommitRecord {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            root_page_id: 0,
            mutation_count: records.len() as u64,
            digest: digest_records(&references),
        };
        let mut committed_wal = Vec::new();
        for record in &records {
            committed_wal.extend_from_slice(&record.to_bytes());
        }
        committed_wal.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        let committed_len = committed_wal.len();
        committed_wal.extend_from_slice(&[0xA5, 0x5A, 0x01]);

        for cut in 0..=committed_wal.len() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("test.db");
            fs::create_dir_all(&path).unwrap();
            fs::write(path.join(WAL_FILE), &committed_wal[..cut]).unwrap();

            let db = DB::open(&path, Options::default()).unwrap_or_else(|error| {
                panic!("WAL prefix at byte {cut} failed to reopen: {error:?}")
            });
            let committed = cut >= committed_len;
            assert_eq!(
                db.get(b"key1").unwrap(),
                committed.then(|| b"value1".to_vec()),
                "cut={cut}"
            );
            assert_eq!(
                db.get(b"key2").unwrap(),
                committed.then(|| b"value2".to_vec()),
                "cut={cut}"
            );
            assert_eq!(
                db.get(b"key3").unwrap(),
                committed.then(|| b"value3".to_vec()),
                "cut={cut}"
            );
        }
    }

    #[test]
    fn test_db_rejects_wal_commit_digest_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let record = WalRecord::put(b"key", b"value");
        let references = vec![&record];
        let commit = CommitRecord {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            root_page_id: 0,
            mutation_count: 1,
            digest: digest_records(&references) ^ 1,
        };
        let mut wal_bytes = record.to_bytes();
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let result = DB::open(&path, Options::default());
        assert!(matches!(
            result,
            Err(Error::Corruption(message)) if message.contains("WAL commit")
        ));
    }

    #[test]
    fn test_db_rejects_when_both_manifest_slots_are_corrupt() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();
        }

        let manifest_path = path.join(MANIFEST_FILE);
        let mut file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
        for slot in 0..2 {
            file.seek(SeekFrom::Start((slot * MANIFEST_SLOT_SIZE) as u64))
                .unwrap();
            file.write_all(&[0xA5; MANIFEST_SLOT_SIZE]).unwrap();
        }
        file.sync_all().unwrap();

        let result = DB::open(&path, Options::default());
        assert!(matches!(result, Err(Error::Corruption(_))));
    }

    #[test]
    fn test_db_fences_writer_after_sync_failure() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.engine.inject_sync_failure();

        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert!(matches!(
            db.put(b"another", b"value"),
            Err(Error::NeedsRecovery(_))
        ));
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_db_fences_writer_after_page_write_failure() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.engine.inject_write_failure();

        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert!(matches!(
            db.put(b"another", b"value"),
            Err(Error::NeedsRecovery(_))
        ));
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_db_discards_wal_after_atomic_rename_failure() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        inject_atomic_rename_failure();

        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert!(matches!(
            db.put(b"another", b"value"),
            Err(Error::NeedsRecovery(_))
        ));
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
        assert!(!path.join(WAL_FILE).exists());
    }

    #[test]
    fn test_db_blob_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create a large value (>1KB threshold).
        let large_value = vec![0xAB; 2000];

        // Write large value and close.
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key1", &large_value).unwrap();
            db.put(b"key2", b"small").unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        // Verify blob file exists.
        assert!(path.join(BLOB_FILE).exists(), "blob file should exist");

        // Reopen and verify blob data persisted.
        {
            let db = DB::open(&path, Options::default()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), Some(large_value.clone()));
            assert_eq!(db.get(b"key2").unwrap(), Some(b"small".to_vec()));
        }
    }

    #[test]
    fn test_db_transaction() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = DB::open(&path, Options::default()).unwrap();

        // Begin a transaction.
        let mut txn = db.begin_transaction();
        assert!(txn.is_active());
        assert_eq!(txn.id(), 1);

        // Commit the transaction.
        db.commit_transaction(&mut txn);
        assert!(!txn.is_active());
        assert_eq!(db.latest_committed_txn(), 1);

        // Begin another transaction.
        let mut txn2 = db.begin_transaction();
        assert_eq!(txn2.id(), 2);
        assert_eq!(txn2.snapshot_id(), 1); // Can see txn 1

        // Abort the transaction.
        db.abort_transaction(&mut txn2);
        assert!(!txn2.is_active());
        assert_eq!(db.latest_committed_txn(), 1); // Still 1
    }

    #[test]
    fn test_db_concurrent_transactions() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = DB::open(&path, Options::default()).unwrap();
        let db = std::sync::Arc::new(db);
        let mut handles = vec![];

        // Spawn multiple threads that create transactions.
        for _ in 0..10 {
            let db = std::sync::Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                let mut txn = db.begin_transaction();
                // Simulate some work.
                std::thread::yield_now();
                db.commit_transaction(&mut txn);
                txn.id()
            }));
        }

        // Wait for all threads to complete.
        let mut ids = vec![];
        for handle in handles {
            ids.push(handle.join().unwrap());
        }

        // All transactions should have unique IDs.
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 10);

        // Latest committed should be the max ID.
        assert_eq!(db.latest_committed_txn(), 10);
    }

    #[test]
    fn test_db_gc() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut db = DB::open(&path, Options::default()).unwrap();

        // Write some large values (>1KB threshold).
        let large_value = vec![0xAB; 2000];
        db.put(b"key1", &large_value).unwrap();
        db.put(b"key2", &large_value).unwrap();
        db.put(b"key3", &large_value).unwrap();
        db.flush().unwrap();

        // Check initial stats.
        let stats = db.blob_stats();
        assert_eq!(stats.total_valid, 3);
        assert_eq!(stats.total_deleted, 0);
        assert_eq!(stats.files_needing_gc, 0);

        // Delete some entries.
        db.delete(b"key1").unwrap();
        db.delete(b"key2").unwrap();
        db.flush().unwrap();

        // Check stats after delete.
        let stats = db.blob_stats();
        assert_eq!(stats.total_valid, 1);
        assert_eq!(stats.total_deleted, 2);

        // Run GC.
        let reclaimed = db.gc().unwrap();
        assert!(reclaimed > 0);

        // Check stats after GC.
        let stats = db.blob_stats();
        assert_eq!(stats.files_needing_gc, 0);
    }
}
