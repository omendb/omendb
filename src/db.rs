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
use crate::recovery::{RecordType, SyncPolicy, WalManager, WalRecord};
use crate::space::{Device, DeviceOptions};
use crate::storage::StorageEngine;
use std::fs;
use std::path::{Path, PathBuf};

/// File names for the database.
const DATA_FILE: &str = "seerdb.data";
#[expect(dead_code)]
const BLOB_FILE: &str = "seerdb.blob";
const WAL_FILE: &str = "seerdb.wal";
const META_FILE: &str = "seerdb.meta";

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
    /// Whether the database is open.
    is_open: bool,
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
        let blobs = if blob_path.exists() {
            // Load blob files from disk.
            let blob_data = fs::read(&blob_path)?;
            BlobManager::from_bytes(&blob_data).unwrap_or_else(|| {
                BlobManager::with_threshold(options.blob_threshold)
            })
        } else {
            BlobManager::with_threshold(options.blob_threshold)
        };

        // Try to load existing state or create new.
        let (mut pmt, mut allocator) = if meta_path.exists() {
            Self::load_meta(&meta_path)?
        } else {
            (PMT::new(), PageAllocator::new())
        };

        // Check if WAL exists (crash recovery needed).
        let mut btree = BTree::new();
        let recovered_from_wal = if wal_path.exists() {
            // Replay WAL to recover from crash.
            Self::recover_from_wal(&wal_path, &mut btree, &mut pmt, &mut allocator)?;
            // Delete WAL after successful recovery.
            fs::remove_file(&wal_path)?;
            true
        } else {
            false
        };

        // Create storage engine.
        let mut engine = StorageEngine::new(btree, buffer, pmt, allocator, device);

        // Load existing data from disk (only if not recovered from WAL).
        if !recovered_from_wal {
            if let Err(e) = engine.load_from_disk() {
                // Log error but continue — we can still operate with empty tree.
                eprintln!("warning: failed to load data from disk: {e}");
            }
        }

        Ok(Self {
            path,
            options,
            engine,
            wal,
            blobs,
            txn_manager: TransactionManager::new(),
            is_open: true,
        })
    }

    /// Insert a key-value pair.
    ///
    /// Write path:
    /// 1. Log to WAL (for crash recovery)
    /// 2. Write WAL to disk (ensure crash recovery)
    /// 3. If value is large, store in blob file
    /// 4. Insert into B-tree
    /// 5. Track for flush
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_open()?;

        // 1. Log to WAL with key-value data for crash recovery.
        self.wal.append(&WalRecord::put(key, value));

        // 2. Write WAL to disk before modifying B-tree.
        // This ensures crash recovery can replay the WAL.
        self.write_wal_to_disk()?;

        // 3. Check if value should be stored in blob.
        if self.blobs.should_separate(value.len()) {
            // Store in blob file.
            let ptr = self.blobs.append(key, value.to_vec());
            // Insert blob pointer into B-tree.
            self.engine.btree_mut().insert_blob(key, ptr)?;
        } else {
            // Store inline in B-tree.
            self.engine.btree_mut().upsert(key, value)?;
        }

        // 4. Allocate a page for this entry.
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
    /// Write path:
    /// 1. Log to WAL (for crash recovery)
    /// 2. Write WAL to disk (ensure crash recovery)
    /// 3. Insert tombstone in B-tree
    /// 4. If was blob, mark blob for GC
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.check_open()?;

        // 1. Log to WAL with key for crash recovery.
        self.wal.append(&WalRecord::delete(key));

        // 2. Write WAL to disk before modifying B-tree.
        self.write_wal_to_disk()?;

        // 3. Check if existing value is a blob.
        if let LookupResult::Blob(ptr) = self.engine.btree().lookup(key) {
            // Mark blob for GC.
            self.blobs.mark_deleted(&ptr);
        }

        // 3. Insert tombstone in B-tree.
        let found = self.engine.btree_mut().delete(key)?;
        Ok(found)
    }

    /// Range scan over [start, end).
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.engine.btree().range_scan(start, end).collect()
    }

    /// Write WAL records to disk and sync.
    ///
    /// This must be called BEFORE modifying the B-tree to ensure
    /// crash recovery can replay the WAL.
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

    /// Flush all pending writes to disk.
    ///
    /// Order is critical for crash recovery:
    /// 1. Write and sync WAL (so we can replay on crash)
    /// 2. Flush storage engine (write data pages)
    /// 3. Save meta (PMT + allocator state)
    /// 4. Save blob files
    pub fn flush(&mut self) -> Result<()> {
        self.check_open()?;

        // 1. Write and sync WAL first (critical for crash recovery).
        self.write_wal_to_disk()?;

        // 2. Flush storage engine (write data pages).
        self.engine.flush()?;

        // 3. Save meta (PMT + allocator state).
        let meta_path = self.path.join(META_FILE);
        Self::save_meta(&meta_path, self.engine.pmt(), self.engine.allocator())?;

        // 4. Save blob files.
        let blob_path = self.path.join(BLOB_FILE);
        let blob_data = self.blobs.to_bytes();
        fs::write(&blob_path, &blob_data)?;

        // 5. Delete WAL after successful flush (recovery complete).
        let wal_path = self.path.join(WAL_FILE);
        if wal_path.exists() {
            fs::remove_file(&wal_path)?;
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

    /// Recover database state from WAL.
    ///
    /// Replays all WAL records to rebuild B-tree, PMT, and allocator state.
    fn recover_from_wal(
        wal_path: &Path,
        btree: &mut BTree,
        pmt: &mut PMT,
        allocator: &mut PageAllocator,
    ) -> Result<()> {
        let wal_data = fs::read(wal_path)?;
        let records = WalManager::parse_records(&wal_data);

        for record in &records {
            match record.record_type {
                RecordType::PmtUpdate if record.payload.len() >= 20 => {
                    // PMT update: page_id(8) + file_id(4) + offset(8)
                    let page_id = u64::from_le_bytes([
                        record.payload[0], record.payload[1], record.payload[2], record.payload[3],
                        record.payload[4], record.payload[5], record.payload[6], record.payload[7],
                    ]);
                    let file_id = u32::from_le_bytes([
                        record.payload[8], record.payload[9], record.payload[10], record.payload[11],
                    ]);
                    let offset = u64::from_le_bytes([
                        record.payload[12], record.payload[13], record.payload[14], record.payload[15],
                        record.payload[16], record.payload[17], record.payload[18], record.payload[19],
                    ]);
                    pmt.insert(page_id, file_id, offset);
                }
                RecordType::PageAlloc if record.payload.len() >= 12 => {
                    // Page allocation: page_id(8) + file_id(4)
                    let _page_id = u64::from_le_bytes([
                        record.payload[0], record.payload[1], record.payload[2], record.payload[3],
                        record.payload[4], record.payload[5], record.payload[6], record.payload[7],
                    ]);
                    let _page_id = allocator.alloc();
                }
                RecordType::PageDealloc if record.payload.len() >= 8 => {
                    // Page deallocation: page_id(8)
                    let page_id = u64::from_le_bytes([
                        record.payload[0], record.payload[1], record.payload[2], record.payload[3],
                        record.payload[4], record.payload[5], record.payload[6], record.payload[7],
                    ]);
                    allocator.free(page_id);
                }
                RecordType::Put if record.payload.len() >= 4 => {
                    // Put: key_len(u16) + key + value_len(u16) + value
                    let key_len = u16::from_le_bytes([record.payload[0], record.payload[1]]) as usize;
                    if record.payload.len() < 2 + key_len + 2 {
                        continue; // invalid record
                    }
                    let key = &record.payload[2..2 + key_len];
                    let val_len_offset = 2 + key_len;
                    let val_len = u16::from_le_bytes([
                        record.payload[val_len_offset],
                        record.payload[val_len_offset + 1],
                    ]) as usize;
                    if record.payload.len() < val_len_offset + 2 + val_len {
                        continue; // invalid record
                    }
                    let value = &record.payload[val_len_offset + 2..val_len_offset + 2 + val_len];
                    // Replay the put into the B-tree.
                    let _ = btree.upsert(key, value);
                }
                RecordType::Delete if record.payload.len() >= 2 => {
                    // Delete: key_len(u16) + key
                    let key_len = u16::from_le_bytes([record.payload[0], record.payload[1]]) as usize;
                    if record.payload.len() < 2 + key_len {
                        continue; // invalid record
                    }
                    let key = &record.payload[2..2 + key_len];
                    // Replay the delete into the B-tree.
                    let _ = btree.delete(key);
                }
                _ => {} // Other record types not relevant for recovery
            }
        }

        Ok(())
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

        fs::write(path, &buf)?;
        Ok(())
    }
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
    use tempfile::tempdir;

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
    fn test_db_crash_recovery() {
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

        // Reopen and verify data recovered from WAL.
        {
            let db = DB::open(&path, Options::default()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
            assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
            assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        }

        // WAL should be deleted after recovery.
        assert!(!path.join(WAL_FILE).exists(), "WAL should be deleted after recovery");
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
}
