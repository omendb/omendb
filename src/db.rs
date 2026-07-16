//! Database entry point.
//!
//! The `DB` struct is the main entry point for the storage engine.
//! It owns all components and provides the public API.

mod options;

pub use options::Options;

use crate::allocator::PageAllocator;
use crate::blob::BlobManager;
use crate::btree::{BlobPointer, BTree, LookupResult, PAGE_SIZE};
use crate::buffer::{BufferManager, BufferStats};
use crate::concurrency::TransactionManager;
use crate::error::{CheckFailureKind, Error, Result};
use crate::mvcc::PMT;
use crate::recovery::{ParseStatus, RecordType, SyncPolicy, WalManager, WalRecord};
use crate::space::{preallocate_file, reserve_file, Device, DeviceOptions};
use crate::storage::{StorageEngine, StorageMetrics};
use crate::storage::format::{
    CommitId, CommitRecord, DatabaseId, FORMAT_VERSION, GenerationId, HistoryId, Manifest,
    ManifestStore, PmtCheckpointId,
};
use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(any(test, feature = "fault-injection"))]
use std::cell::Cell;

#[cfg(any(test, feature = "fault-injection"))]
thread_local! {
    static FAIL_NEXT_ATOMIC_RENAME: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_WAL_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_WAL_AFTER_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_WAL_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_WAL_AFTER_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_AFTER_MANIFEST: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_WAL_TRUNCATE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_ATOMIC_SHORT_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_ATOMIC_TORN_WRITE: Cell<bool> = const { Cell::new(false) };
}

/// File names for the database.
const DATA_FILE: &str = "seerdb.data";
const BLOB_FILE: &str = "seerdb.blob";
const BLOB_RESERVATION_FILE: &str = "seerdb.blob.reserve";
const WAL_FILE: &str = "seerdb.wal";
const WAL_RESERVATION_FILE: &str = "seerdb.wal.reserve";
const META_FILE: &str = "seerdb.meta";
const MANIFEST_FILE: &str = "MANIFEST";
const LOCK_FILE: &str = "seerdb.lock";
const ARCHIVE_MARKER_FILE: &str = "seerdb.archive";
const META_MAGIC: [u8; 8] = *b"SEERMET1";
const WAL_RESERVATION_SEGMENT_BYTES: u64 = 1024 * 1024;
const WAL_COMMIT_RECORD_BYTES: u64 =
    (4 + 1 + CommitRecord::SERIALIZED_SIZE + 4) as u64;
static NEXT_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenMode {
    Normal,
    Check,
}

#[derive(Debug)]
struct VerificationFailure {
    kind: CheckFailureKind,
    message: String,
}

impl VerificationFailure {
    fn from_error(kind: CheckFailureKind, error: Error) -> Self {
        Self {
            kind,
            message: error_message(error),
        }
    }

    fn into_error(self) -> Error {
        Error::Check {
            kind: self.kind,
            message: self.message,
        }
    }
}

/// Blob GC statistics.
pub struct BlobStats {
    /// Number of files needing garbage collection.
    pub files_needing_gc: usize,
    /// Total valid entries across all files.
    pub total_valid: usize,
    /// Total deleted entries across all files.
    pub total_deleted: usize,
}

/// Durable identity and publication state exposed for recovery diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurabilityStatus {
    /// Stable database identity.
    pub database_id: DatabaseId,
    /// Stable logical history identity.
    pub history_id: HistoryId,
    /// Latest manifest generation known to this handle.
    pub generation_id: GenerationId,
    /// Latest durable commit known to this handle.
    pub commit_id: CommitId,
    /// Mutations currently journaled but not yet published in a generation.
    pub pending_mutations: u64,
    /// Whether this writer must be reopened before accepting more writes.
    pub write_fenced: bool,
}

/// Results from a read-only integrity verification pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerificationReport {
    /// Durable identity and publication state that was verified.
    pub durability: DurabilityStatus,
    /// Number of active PMT pages with valid checksums.
    pub verified_pages: u64,
    /// Current data-file size in bytes.
    pub data_bytes: u64,
    /// Current serialized blob-file size in bytes.
    pub blob_bytes: u64,
    /// Current WAL size in bytes, if a pending batch exists.
    pub wal_bytes: u64,
    /// Physical page slots currently safe for reuse.
    pub reclaimable_pages: u64,
}

/// WAL state observed by a non-mutating offline check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalCheckStatus {
    /// No WAL records are present.
    Clean,
    /// Complete mutation records are present without a commit envelope.
    Pending,
    /// A commit envelope is present and recovery may need to advance the
    /// authoritative manifest.
    NeedsRecovery,
    /// The final WAL record is torn or the reserved suffix is incomplete.
    Incomplete,
}

/// Results from a non-mutating offline integrity check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckReport {
    /// Active-generation verification results.
    pub verification: VerificationReport,
    /// WAL state that the writable open path would reconcile.
    pub wal_status: WalCheckStatus,
}

/// Results from creating and independently verifying a snapshot directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotReport {
    /// Source durability state at the snapshot boundary.
    pub source: DurabilityStatus,
    /// Destination durability state after reopen and verification.
    pub destination: DurabilityStatus,
    /// Number of durable artifacts copied into the snapshot.
    pub copied_files: u32,
    /// Number of destination pages verified after reopen.
    pub verified_pages: u64,
}

/// Results from restoring an immutable archive into a new writable history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestoreReport {
    /// Archive durability state used as the restore source.
    pub source: DurabilityStatus,
    /// New writable history state after restore.
    pub destination: DurabilityStatus,
    /// Number of durable artifacts copied into the new history.
    pub copied_files: u32,
    /// Number of destination pages verified after history fork.
    pub verified_pages: u64,
}

/// Durable action performed while rebuilding a checked database copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairAction {
    /// The source had no WAL requiring reconciliation.
    NoRepair,
    /// A complete mutation-only WAL was discarded as uncommitted.
    DiscardedUncommittedWal,
    /// A complete committed WAL image was reconciled in the destination.
    ReconciledCommittedWal,
    /// A torn WAL suffix was reconciled in the rebuilt copy.
    ReconciledIncompleteWal,
}

/// Results from rebuilding a checked database into a new writable history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairReport {
    /// Source durable identity before rebuild.
    pub source: DurabilityStatus,
    /// WAL state found by the non-mutating source check.
    pub source_wal_status: WalCheckStatus,
    /// New writable identity after recovery and history fork.
    pub destination: DurabilityStatus,
    /// Number of durable artifacts copied into the rebuild workspace.
    pub copied_files: u32,
    /// Number of destination pages verified after rebuild.
    pub verified_pages: u64,
    /// Recovery/rebuild action performed in the destination.
    pub action: RepairAction,
}

/// An owned, read-only snapshot view backed by an independently verified
/// directory copy.
///
/// The copy-backed implementation is intentionally conservative: source page
/// reclamation cannot invalidate the snapshot, and dropping or releasing the
/// handle removes its temporary directory. A future shared-page snapshot can
/// preserve this API while replacing the copy mechanism.
pub struct Snapshot {
    db: Option<DB>,
    path: PathBuf,
    released: bool,
}

impl Snapshot {
    /// Return the snapshot directory path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get a value from the retained snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .get(key)
    }

    /// Scan a range in the retained snapshot.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.db
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .range(start, end)
    }

    /// Verify the retained snapshot independently.
    pub fn verify(&mut self) -> Result<VerificationReport> {
        self.db
            .as_mut()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .verify()
    }

    /// Return the durable identity captured by this snapshot.
    pub fn durability_status(&self) -> Result<DurabilityStatus> {
        Ok(self
            .db
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .durability_status())
    }

    /// Release the snapshot directory immediately.
    pub fn release(mut self) -> Result<()> {
        self.db.take();
        fs::remove_dir_all(&self.path)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.db.take();
        if !self.released {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Results from trimming reclaimable trailing data pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionReport {
    /// Durable identity after compaction.
    pub durability: DurabilityStatus,
    /// Data-file size before truncation.
    pub data_bytes_before: u64,
    /// Data-file size after truncation.
    pub data_bytes_after: u64,
    /// Number of physical page slots removed from the tail.
    pub reclaimed_pages: u64,
    /// Whether the active manifest was mirrored before truncation.
    pub manifest_replicated: bool,
}

/// Cumulative storage work and current artifact sizes for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DBMetrics {
    /// Physical page and publication counters for this open handle.
    pub storage: StorageMetrics,
    /// Number of mutations rejected before application by the WAL budget.
    pub wal_admission_failures: u64,
    /// Buffer-pool occupancy and cache counters for this open handle.
    pub buffer: BufferStats,
    /// Current data-file size in bytes.
    pub data_bytes: u64,
    /// Current blob-file size in bytes.
    pub blob_bytes: u64,
    /// Current logical WAL size in bytes.
    pub wal_bytes: u64,
    /// Fixed WAL capacity extent reserved for future pending mutations.
    pub wal_reserved_bytes: u64,
    /// Physical pages currently safe for reuse.
    pub reclaimable_pages: u64,
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
    /// Logical WAL bytes admitted for the pending generation.
    pending_wal_bytes: u64,
    /// Digest over pending mutation records.
    pending_digest: u32,
    /// Whether the database is open.
    is_open: bool,
    /// Whether a failed publication fenced this writer until reopen.
    write_fenced: bool,
    /// Whether this handle opened an immutable archive/snapshot.
    read_only: bool,
    /// Whether this handle was opened only for non-mutating checks.
    check_only: bool,
    /// Advisory writer ownership for the database directory.
    lock_file: Option<File>,
    /// Number of retryable WAL admission rejections for this handle.
    wal_admission_failures: u64,
}

impl DB {
    /// Open or create a database at the given path.
    pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        Self::open_with_mode(path, options, OpenMode::Normal)
    }

    /// Check an existing database without taking writer ownership or
    /// replaying, truncating, or publishing its WAL.
    pub fn check<P: AsRef<Path>>(path: P, options: Options) -> Result<CheckReport> {
        let mut db = Self::open_with_mode(path, options, OpenMode::Check)
            .map_err(Self::map_check_open_error)?;
        let verification = db
            .verify_inner()
            .map_err(VerificationFailure::into_error)?;
        let wal_status = db
            .wal_check_status()
            .map_err(|error| Self::map_check_error(CheckFailureKind::Wal, error))?;
        Ok(CheckReport {
            verification,
            wal_status,
        })
    }

    fn map_check_open_error(error: Error) -> Error {
        Self::map_check_error(CheckFailureKind::Format, error)
    }

    fn map_check_error(default_kind: CheckFailureKind, error: Error) -> Error {
        match error {
            Error::Check { .. } => error,
            Error::InvalidArgument(message) => Error::Check {
                kind: CheckFailureKind::Target,
                message,
            },
            Error::Io(error) => Error::Check {
                kind: CheckFailureKind::Io,
                message: error.to_string(),
            },
            Error::NeedsRecovery(message) => Error::Check {
                kind: CheckFailureKind::Wal,
                message,
            },
            Error::Corruption(message) => Error::Check {
                kind: default_kind,
                message,
            },
            other => other,
        }
    }

    fn map_checkpoint_check_error(error: Error) -> Error {
        match error {
            Error::Corruption(message) if message.contains("unsupported meta format version") => {
                Error::Check {
                    kind: CheckFailureKind::Format,
                    message,
                }
            }
            other => Self::map_check_error(CheckFailureKind::Checkpoint, other),
        }
    }

    fn open_with_mode<P: AsRef<Path>>(
        path: P,
        options: Options,
        mode: OpenMode,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let check_only = mode == OpenMode::Check;

        // Create directory if it doesn't exist.
        if !path.exists() {
            if check_only {
                return Err(Error::InvalidArgument(format!(
                    "check path does not exist: {}",
                    path.display()
                )));
            }
            fs::create_dir_all(&path)?;
        }

        let data_path = path.join(DATA_FILE);
        let wal_path = path.join(WAL_FILE);
        let meta_path = path.join(META_FILE);
        let manifest_path = path.join(MANIFEST_FILE);
        let archive = path.join(ARCHIVE_MARKER_FILE).is_file();
        let read_only = check_only || archive;
        if check_only && (!manifest_path.is_file() || !data_path.is_file()) {
            return Err(Error::Check {
                kind: CheckFailureKind::Target,
                message: "check target is missing required manifest or data artifacts".into(),
            });
        }
        if archive && !check_only {
            if !manifest_path.is_file() || !data_path.is_file() {
                return Err(Error::Corruption(
                    "read-only archive is missing required artifacts".into(),
                ));
            }
            if wal_path.exists() {
                return Err(Error::NeedsRecovery(
                    "read-only archive contains a pending WAL".into(),
                ));
            }
        }
        let lock_file = if read_only {
            None
        } else {
            Some(Self::acquire_writer_lock(&path.join(LOCK_FILE))?)
        };
        if !read_only {
            clear_blob_reservation(&path)?;
        }

        let mut manifest = if check_only {
            ManifestStore::open_read_only(&manifest_path).map_err(|error| {
                Self::map_check_error(CheckFailureKind::Manifest, error)
            })?
        } else {
            ManifestStore::open(&manifest_path)?
        };
        let current_manifest = manifest.load_latest().map_err(|error| {
            if check_only {
                Self::map_check_error(CheckFailureKind::Manifest, error)
            } else {
                error
            }
        })?;
        if check_only && current_manifest.is_none() {
            return Err(Error::Check {
                kind: CheckFailureKind::Manifest,
                message: "check target has no valid manifest generation".into(),
            });
        }
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
            create: !read_only,
        };
        let device = if check_only {
            Device::open_read_only(&data_path, &device_opts)?
        } else {
            Device::open(&data_path, &device_opts)?
        };

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
            match BlobManager::from_bytes(&blob_data) {
                Some(blobs) => blobs,
                None if check_only => {
                    return Err(Error::Check {
                        kind: CheckFailureKind::Blob,
                        message: "blob file is truncated or has an invalid checksum".into(),
                    });
                }
                None => {
                    return Err(Error::Corruption(
                        "blob file is truncated or has an invalid checksum".into(),
                    ));
                }
            }
        } else {
            BlobManager::with_threshold(options.blob_threshold)
        };
        if current_manifest.is_some_and(|current| {
            blobs.generation_id() != current.generation_id.get()
        }) {
            // A blob image is written before its manifest. If publication
            // stopped between those boundaries, deletion marks from the
            // newer image must not make pages referenced by the older
            // manifest reclaimable.
            blobs.clear_deletion_metadata();
        }

        // A published manifest selects an immutable PMT checkpoint. Never
        // pair an older manifest with a newer mutable metadata file.
        let (pmt, allocator) = if let Some(current) = current_manifest {
            if current.pmt_checkpoint_id.get() == 0 {
                (PMT::new(), PageAllocator::new())
            } else {
                let checkpoint_path =
                    path.join(format!("seerdb.meta.{}", current.pmt_checkpoint_id.get()));
                Self::load_meta(&checkpoint_path).map_err(|error| {
                    if check_only {
                        Self::map_checkpoint_check_error(error)
                    } else {
                        error
                    }
                })?
            }
        } else if meta_path.exists() {
            Self::load_meta(&meta_path).map_err(|error| {
                if check_only {
                    Self::map_checkpoint_check_error(error)
                } else {
                    error
                }
            })?
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

        // WAL replay mutates the logical tree, so materialize a lazily opened
        // generation before applying a committed recovery prefix. A clean
        // reopen remains lazy and serves reads directly through the PMT.
        if wal_path.exists() && !check_only {
            engine.ensure_materialized()?;
        }

        let recovery = if wal_path.exists() && !check_only {
            Some(Self::recover_from_wal(
                &wal_path,
                current_manifest,
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
            pending_wal_bytes: 0,
            pending_digest: 0,
            is_open: true,
            write_fenced: false,
            read_only,
            check_only,
            lock_file,
            wal_admission_failures: 0,
        };

        if !check_only && current_manifest.is_none() && !wal_path.exists() && !meta_path.exists() {
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
        validate_wal_put_lengths(key, value)?;
        let record = WalRecord::put(key, value);
        self.admit_wal_record(&record)?;
        self.engine.prepare_mutation(key)?;

        // Mutate memory first, then make the successful mutation durable in
        // the WAL. No page is written before the WAL reaches disk, and an
        // operation that fails never enters a committed WAL batch.
        let previous_blob = match self.engine.lookup(key)? {
            LookupResult::Blob(pointer) => Some(pointer),
            _ => None,
        };
        let appended_value_len = self
            .blobs
            .should_separate(value.len())
            .then_some(value.len());
        if previous_blob.is_some() || appended_value_len.is_some() {
            self.admit_blob_image(previous_blob.as_ref(), appended_value_len)?;
        }
        if appended_value_len.is_some() {
            let pointer = self.blobs.append(key, value.to_vec());
            if let Err(error) = self.engine.btree_mut().upsert_blob(key, pointer) {
                let _ = self.blobs.rollback_append(&pointer);
                return Err(error.into());
            }
        } else {
            self.engine.btree_mut().upsert(key, value)?;
        }
        if let Some(pointer) = previous_blob {
            self.blobs.mark_deleted(&pointer);
        }

        self.journal_mutation(record)?;

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
        self.check_readable()?;

        match self.engine.lookup(key)? {
            LookupResult::Found(value) => Ok(Some(value)),
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
        validate_wal_key_length(key)?;
        let record = WalRecord::delete(key);
        self.admit_wal_record(&record)?;
        self.engine.prepare_mutation(key)?;

        let previous_blob = match self.engine.lookup(key)? {
            LookupResult::Blob(pointer) => Some(pointer),
            _ => None,
        };
        if previous_blob.is_some() {
            self.admit_blob_image(previous_blob.as_ref(), None)?;
        }
        let found = self.engine.btree_mut().delete(key)?;
        if found && let Some(pointer) = previous_blob {
            self.blobs.mark_deleted(&pointer);
        }
        self.journal_mutation(record)?;
        Ok(found)
    }

    /// Range scan over [start, end).
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_readable()?;
        self.engine
            .range(start, end)?
            .into_iter()
            .filter_map(|(key, value)| match value {
                crate::btree::LookupResult::Found(value) => Some(Ok((key, value))),
                crate::btree::LookupResult::Blob(pointer) => Some(
                    self.blobs
                        .read(&pointer)
                        .map(|value| (key, value.to_vec()))
                        .ok_or_else(|| Error::Corruption("blob pointer invalid".into())),
                ),
                crate::btree::LookupResult::Deleted | crate::btree::LookupResult::NotFound => None,
            })
            .collect()
    }

    /// Write buffered WAL records to disk and optionally force the prefix.
    fn write_wal_to_disk(&mut self, force_sync: bool) -> Result<()> {
        let mut wal_buf = Vec::new();
        self.wal.flush(&mut wal_buf)?;
        let should_sync = force_sync || self.wal.sync_policy() != SyncPolicy::None;
        if !wal_buf.is_empty() || should_sync {
            let wal_path = self.path.join(WAL_FILE);
            let mut file = OpenOptions::new()
                .create(true)
                .append(!wal_buf.is_empty())
                .read(should_sync)
                .write(!wal_buf.is_empty() || should_sync)
                .open(&wal_path)?;
            if !wal_buf.is_empty() {
                // Append to WAL file (not overwrite).
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_WRITE.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected WAL append failure").into());
                }
                use std::io::Write;
                file.write_all(&wal_buf)?;
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_AFTER_WRITE.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected post-append WAL failure").into());
                }
            }
            if should_sync {
                // The commit boundary and any configured per-mutation policy
                // force the WAL before dependent page publication.
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_SYNC.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected WAL sync failure").into());
                }
                match self.wal.sync_policy() {
                    SyncPolicy::SyncAll => file.sync_all()?,
                    SyncPolicy::FDataSync | SyncPolicy::None => file.sync_data()?,
                }
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_AFTER_SYNC.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected post-WAL-sync failure").into());
                }
            }
        }
        Ok(())
    }

    /// Journal a mutation after it has successfully changed memory state.
    fn journal_mutation(&mut self, record: WalRecord) -> Result<()> {
        self.wal.append(&record);
        let sync_mutation = self.wal.sync_policy() != SyncPolicy::None;
        if let Err(error) = self.write_wal_to_disk(sync_mutation) {
            self.write_fenced = true;
            return Err(error);
        }
        self.pending_mutations = self
            .pending_mutations
            .checked_add(1)
            .ok_or_else(|| Error::Wal("mutation count overflow".into()))?;
        self.pending_wal_bytes = self
            .pending_wal_bytes
            .checked_add(record.to_bytes().len() as u64)
            .ok_or_else(|| Error::Wal("WAL byte count overflow".into()))?;
        self.pending_digest = extend_digest(self.pending_digest, &record);
        Ok(())
    }

    /// Reserve enough logical WAL budget for one mutation and the commit that
    /// closes its pending generation. This runs before any tree or blob state
    /// changes, so retryable backpressure cannot leave a partial mutation.
    fn admit_wal_record(&mut self, record: &WalRecord) -> Result<()> {
        let used = self.pending_wal_bytes;
        let required = (record.to_bytes().len() as u64)
            .checked_add(WAL_COMMIT_RECORD_BYTES)
            .ok_or(Error::DiskFull)?;
        let available = self.options.max_wal_bytes.saturating_sub(used);
        if required > available {
            self.wal_admission_failures = self.wal_admission_failures.saturating_add(1);
            return Err(Error::Backpressure {
                required,
                available,
            });
        }

        self.ensure_wal_reservation()?;
        Ok(())
    }

    /// Reserve the next blob image before a mutation changes memory state.
    ///
    /// Linux and macOS retain the physical reservation in a sidecar that is
    /// consumed by the next atomic blob publication. Other platforms use a
    /// best-effort filesystem-space check and keep the final write fallback.
    fn admit_blob_image(
        &self,
        retired: Option<&BlobPointer>,
        appended_value_len: Option<usize>,
    ) -> Result<()> {
        let required = self
            .blobs
            .projected_serialized_size(retired, appended_value_len)
            .ok_or_else(|| Error::InvalidArgument("blob image size overflows".into()))?;
        self.engine.check_artifact_capacity(required)?;

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let reservation_path = self.path.join(BLOB_RESERVATION_FILE);
            let file = match OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&reservation_path)
            {
                Ok(file) => file,
                Err(error) => {
                    let _ = fs::remove_file(&reservation_path);
                    return Err(error.into());
                }
            };
            if let Err(error) = reserve_file(&file, required) {
                drop(file);
                let _ = fs::remove_file(&reservation_path);
                return Err(error.into());
            }
            file.set_len(required)?;
            file.sync_data()?;
            sync_directory(&self.path)?;
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            if required > fs2::available_space(&self.path)? {
                return Err(Error::DiskFull);
            }
        }

        Ok(())
    }

    /// Ensure the database owns a fixed-size WAL reservation extent before a
    /// mutation changes tree or blob state. The extent is rounded to fixed
    /// segments so future WAL growth has a bounded, stable admission domain;
    /// the logical WAL remains separately length-delimited and checksummed.
    fn ensure_wal_reservation(&self) -> Result<u64> {
        if self.options.max_wal_bytes == 0 {
            return Ok(0);
        }

        let remainder = self.options.max_wal_bytes % WAL_RESERVATION_SEGMENT_BYTES;
        let target = self
            .options
            .max_wal_bytes
            .checked_add((WAL_RESERVATION_SEGMENT_BYTES - remainder) % WAL_RESERVATION_SEGMENT_BYTES)
            .ok_or(Error::DiskFull)?;
        let path = self.path.join(WAL_RESERVATION_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let current = file.metadata()?.len();
        if current < target {
            preallocate_file(&file, target)?;
            file.sync_data()?;
            sync_directory(&self.path)?;
        }
        Ok(current.max(target))
    }

    fn write_blob_image(&self, path: &Path, data: &[u8]) -> Result<()> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let reservation = self.path.join(BLOB_RESERVATION_FILE);
            if reservation.is_file() {
                return atomic_write_reserved(path, &reservation, data);
            }
        }

        atomic_write(path, data)
    }

    /// Publish a generation after its pages and checkpoints are durable.
    fn publish_generation(
        &mut self,
        commit: CommitRecord,
        append_commit: bool,
        recovered_wal_offset: u64,
    ) -> Result<()> {
        // Retire the older manifest slot before page writes can reuse any
        // physical versions it names. If the new publication fails, both
        // slots still identify the same current generation and its pages.
        self.mirror_current_manifest()?;
        // A non-syncing mutation path is still ordered before page writes and
        // the commit envelope is always forced at the publication boundary.
        self.write_wal_to_disk(true)?;
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
        self.blobs.set_generation(commit.generation_id.get());
        self.write_blob_image(&blob_path, &self.blobs.to_bytes())?;

        let wal_path = self.path.join(WAL_FILE);
        let wal_offset = if append_commit {
            let offset = fs::metadata(&wal_path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            self.wal.append(&WalRecord::commit(commit));
            self.write_wal_to_disk(true)?;
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

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_AFTER_MANIFEST.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected post-manifest failure").into());
        }

        self.engine.complete_generation();

        if wal_path.exists() {
            fs::remove_file(&wal_path)?;

            #[cfg(any(test, feature = "fault-injection"))]
            if FAIL_NEXT_WAL_TRUNCATE.with(|failure| failure.replace(false)) {
                return Err(std::io::Error::other("injected WAL truncate failure").into());
            }

            sync_directory(&self.path)?;
        }

        self.generation_id = commit.generation_id;
        self.commit_id = commit.commit_id;
        self.pending_mutations = 0;
        self.pending_wal_bytes = 0;
        self.pending_digest = 0;
        Ok(())
    }

    /// Make both manifest slots name the latest durable generation before a
    /// new generation may reuse pages from older slots.
    fn mirror_current_manifest(&mut self) -> Result<()> {
        if let Some(current) = self.manifest.load_latest()? {
            self.manifest.publish(current)?;
        }
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
            if !self.read_only {
                self.flush()?;
            }
            self.is_open = false;
            if let Some(lock_file) = self.lock_file.take() {
                let _ = lock_file.unlock();
            }
        }
        Ok(())
    }

    /// Run garbage collection on blob files.
    ///
    /// Pending mutations are published before reclaiming blobs so an older
    /// durable generation never loses a pointer. Only fully dead blob files
    /// are currently reclaimable without pointer rewriting.
    ///
    /// Returns the number of entries reclaimed.
    pub fn gc(&mut self) -> Result<usize> {
        self.check_writable()?;
        self.flush()?;
        if self.blobs.has_reclaimable_files() {
            // Admission must precede removal from the in-memory catalog. The
            // current image is an upper bound for the compacted image, so a
            // successful reservation covers the subsequent atomic publish.
            self.admit_blob_image(None, None)?;
        }
        let reclaimed = self.blobs.gc();
        if reclaimed > 0 {
            let blob_path = self.path.join(BLOB_FILE);
            if let Err(error) = self.write_blob_image(&blob_path, &self.blobs.to_bytes()) {
                self.write_fenced = true;
                return Err(error);
            }
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

    /// Return durable identity and publication state for diagnostics/recovery.
    pub fn durability_status(&self) -> DurabilityStatus {
        DurabilityStatus {
            database_id: self.database_id,
            history_id: self.history_id,
            generation_id: self.generation_id,
            commit_id: self.commit_id,
            pending_mutations: self.pending_mutations,
            write_fenced: self.write_fenced,
        }
    }

    /// Return storage counters and current artifact sizes for observability.
    pub fn metrics(&self) -> Result<DBMetrics> {
        self.check_open()?;
        let artifact_size = |name: &str| -> Result<u64> {
            let path = self.path.join(name);
            match fs::metadata(path) {
                Ok(metadata) => Ok(metadata.len()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
                Err(error) => Err(error.into()),
            }
        };

        Ok(DBMetrics {
            storage: self.engine.metrics(),
            wal_admission_failures: self.wal_admission_failures,
            buffer: self.engine.buffer_stats(),
            data_bytes: artifact_size(DATA_FILE)?,
            blob_bytes: artifact_size(BLOB_FILE)?,
            wal_bytes: artifact_size(WAL_FILE)?,
            wal_reserved_bytes: artifact_size(WAL_RESERVATION_FILE)?,
            reclaimable_pages: self.engine.reclaimable_page_count() as u64,
        })
    }

    /// Verify the active manifest, checkpoint, pages, blob file, and WAL.
    ///
    /// This pass does not mutate logical state and is intended for DBNext
    /// check/repair tooling and pre-snapshot validation.
    pub fn verify(&mut self) -> Result<VerificationReport> {
        self.check_readable()?;
        self.verify_inner()
            .map_err(|failure| Error::Corruption(failure.message))
    }

    fn verify_inner(&mut self) -> std::result::Result<VerificationReport, VerificationFailure> {
        let manifest = self
            .manifest
            .load_latest()
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Manifest, error))?
            .ok_or_else(|| VerificationFailure {
                kind: CheckFailureKind::Manifest,
                message: "database has no valid manifest".into(),
            })?;
        if manifest.database_id != self.database_id
            || manifest.history_id != self.history_id
            || manifest.generation_id != self.generation_id
            || manifest.commit_id != self.commit_id
        {
            return Err(VerificationFailure {
                kind: CheckFailureKind::Manifest,
                message: "manifest identity does not match the open database".into(),
            });
        }

        let (verified_pages, data_bytes) = self
            .engine
            .verify_pages(manifest.root_page_id)
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::DataPage, error))?;
        let blob_pointers = self
            .engine
            .verify_tree(manifest.root_page_id)
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Structure, error))?;
        for pointer in blob_pointers {
            if self.blobs.read(&pointer).is_none() {
                return Err(VerificationFailure {
                    kind: CheckFailureKind::Blob,
                    message: format!(
                        "blob pointer target is missing: file {}, offset {}, length {}",
                        pointer.file_id, pointer.offset, pointer.length
                    ),
                });
            }
        }

        if manifest.pmt_checkpoint_id.get() != 0 {
            let checkpoint_path = self
                .path
                .join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
            let (checkpoint_pmt, checkpoint_allocator) = Self::load_meta(&checkpoint_path)
                .map_err(|error| {
                    VerificationFailure::from_error(CheckFailureKind::Checkpoint, error)
                })?;
            if checkpoint_pmt.to_bytes() != self.engine.pmt().to_bytes()
                || checkpoint_allocator.to_bytes() != self.engine.allocator().to_bytes()
            {
                return Err(VerificationFailure {
                    kind: CheckFailureKind::Checkpoint,
                    message: "manifest checkpoint does not match active PMT or allocator".into(),
                });
            }
        }

        let blob_path = self.path.join(BLOB_FILE);
        let blob_bytes = if blob_path.exists() {
            let bytes = fs::read(&blob_path).map_err(|error| {
                VerificationFailure::from_error(CheckFailureKind::Blob, error.into())
            })?;
            BlobManager::from_bytes(&bytes).ok_or_else(|| {
                VerificationFailure {
                    kind: CheckFailureKind::Blob,
                    message: "blob file failed integrity verification".into(),
                }
            })?;
            bytes.len() as u64
        } else {
            0
        };

        let wal_path = self.path.join(WAL_FILE);
        let wal_bytes = if wal_path.exists() {
            let bytes = fs::read(&wal_path).map_err(|error| {
                VerificationFailure::from_error(CheckFailureKind::Wal, error.into())
            })?;
            let (_, status) = WalManager::parse_records_with_status(&bytes);
            if status == ParseStatus::Corrupt
                || (!self.check_only && status != ParseStatus::Complete)
            {
                return Err(VerificationFailure {
                    kind: CheckFailureKind::Wal,
                    message: format!("WAL integrity status is {status:?}"),
                });
            }
            bytes.len() as u64
        } else {
            0
        };

        Ok(VerificationReport {
            durability: self.durability_status(),
            verified_pages,
            data_bytes,
            blob_bytes,
            wal_bytes,
            reclaimable_pages: self.engine.reclaimable_page_count() as u64,
        })
    }

    fn wal_check_status(&mut self) -> Result<WalCheckStatus> {
        let wal_path = self.path.join(WAL_FILE);
        if !wal_path.exists() {
            return Ok(WalCheckStatus::Clean);
        }

        let bytes = fs::read(wal_path)?;
        if bytes.is_empty() {
            return Ok(WalCheckStatus::Clean);
        }

        let (records, status) = WalManager::parse_records_with_status(&bytes);
        if status == ParseStatus::Corrupt {
            return Err(Error::Corruption(
                "offline check found a corrupt WAL record".into(),
            ));
        }

        let current_manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        let mut pending = Vec::new();
        let mut saw_commit = false;
        for record in &records {
            match record.record_type {
                RecordType::Put => {
                    decode_put_payload(false, &record.payload)?;
                    pending.push(record);
                }
                RecordType::PutV2 => {
                    decode_put_payload(true, &record.payload)?;
                    pending.push(record);
                }
                RecordType::Delete => {
                    decode_delete_payload(false, &record.payload)?;
                    pending.push(record);
                }
                RecordType::DeleteV2 => {
                    decode_delete_payload(true, &record.payload)?;
                    pending.push(record);
                }
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

                    match commit
                        .generation_id
                        .get()
                        .cmp(&current_manifest.generation_id.get())
                    {
                        std::cmp::Ordering::Less
                            if commit.commit_id > current_manifest.commit_id =>
                        {
                            return Err(Error::Corruption(
                                "WAL commit frontier is inconsistent with manifest".into(),
                            ));
                        }
                        std::cmp::Ordering::Equal => {
                            if commit.commit_id != current_manifest.commit_id
                                || commit.root_page_id != current_manifest.root_page_id
                                || commit.mutation_count != current_manifest.mutation_count
                                || commit.digest != current_manifest.digest
                            {
                                return Err(Error::Corruption(
                                    "WAL commit disagrees with authoritative manifest".into(),
                                ));
                            }
                        }
                        std::cmp::Ordering::Greater
                            if commit.commit_id <= current_manifest.commit_id =>
                        {
                            return Err(Error::Corruption(
                                "WAL commit frontier is inconsistent with manifest".into(),
                            ));
                        }
                        _ => {}
                    }
                    saw_commit = true;
                    pending.clear();
                }
                _ => {}
            }
        }

        match status {
            ParseStatus::Incomplete => Ok(WalCheckStatus::Incomplete),
            ParseStatus::Complete if records.is_empty() => Ok(WalCheckStatus::Clean),
            ParseStatus::Complete if saw_commit => Ok(WalCheckStatus::NeedsRecovery),
            ParseStatus::Complete => Ok(WalCheckStatus::Pending),
            ParseStatus::Corrupt => unreachable!("corrupt WAL status returned above"),
        }
    }

    /// Flush and create an atomically published, independently verified
    /// snapshot in a new directory without mutating the source directory.
    pub fn snapshot<P: AsRef<Path>>(&mut self, destination: P) -> Result<SnapshotReport> {
        self.check_writable()?;
        self.flush()?;
        let source_report = self.verify()?;
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "snapshot destination already exists: {}",
                destination.display()
            )));
        }

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = destination.with_extension("seerdb.snapshot.tmp");
        if temporary.exists() {
            return Err(Error::InvalidArgument(format!(
                "snapshot temporary path already exists: {}",
                temporary.display()
            )));
        }

        let result = (|| {
            fs::create_dir_all(&temporary)?;
            let copied_files = copy_snapshot_artifacts(&self.path, &temporary)?;
            let marker_path = temporary.join(ARCHIVE_MARKER_FILE);
            fs::write(&marker_path, b"SEERDB-ARCHIVE-V1\n")?;
            File::open(&marker_path)?.sync_all()?;
            sync_directory(&temporary)?;

            let mut restored = DB::open(&temporary, self.options.clone())?;
            let restored_report = restored.verify()?;
            if restored_report.durability != source_report.durability
                || restored_report.verified_pages != source_report.verified_pages
            {
                return Err(Error::Corruption(
                    "snapshot verification does not match source durability state".into(),
                ));
            }
            let destination_status = restored_report.durability;
            drop(restored);

            fs::rename(&temporary, &destination)?;
            if let Some(parent) = destination.parent() {
                sync_directory(parent)?;
            }

            Ok(SnapshotReport {
                source: source_report.durability,
                destination: destination_status,
                copied_files,
                verified_pages: restored_report.verified_pages,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
        }
        result
    }

    /// Restore an immutable archive into a new writable history.
    ///
    /// The archive is verified before copying. The destination receives a new
    /// `HistoryId` while preserving the source's database identity and
    /// durable root, so it can advance independently without sharing future
    /// history IDs with the archive.
    pub fn restore<P: AsRef<Path>, Q: AsRef<Path>>(
        archive: P,
        destination: Q,
        options: Options,
    ) -> Result<RestoreReport> {
        let archive = archive.as_ref().to_path_buf();
        if !archive.join(ARCHIVE_MARKER_FILE).is_file() {
            return Err(Error::InvalidArgument(
                "restore source is not an immutable SeerDB archive".into(),
            ));
        }
        let mut source = DB::open(&archive, options.clone())?;
        let source_report = source.verify()?;
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "restore destination already exists: {}",
                destination.display()
            )));
        }

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = Self::next_derived_path(&destination, "restore")?;
        let result = (|| {
            fs::create_dir_all(&temporary)?;
            let copied_files = copy_snapshot_artifacts(&archive, &temporary)?;
            sync_directory(&temporary)?;

            let mut restored = DB::open(&temporary, options.clone())?;
            restored.fork_history()?;
            let restored_report = restored.verify()?;
            if restored_report.durability.database_id != source_report.durability.database_id
                || restored_report.durability.generation_id
                    != source_report.durability.generation_id
                || restored_report.durability.commit_id != source_report.durability.commit_id
                || restored_report.verified_pages != source_report.verified_pages
            {
                return Err(Error::Corruption(
                    "restored history does not match archive root".into(),
                ));
            }
            let destination_status = restored_report.durability;
            drop(restored);

            fs::rename(&temporary, &destination)?;
            if let Some(parent) = destination.parent() {
                sync_directory(parent)?;
            }

            Ok(RestoreReport {
                source: source_report.durability,
                destination: destination_status,
                copied_files,
                verified_pages: restored_report.verified_pages,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
        }
        result
    }

    /// Rebuild a checked database into a new writable history without
    /// mutating the source directory.
    ///
    /// Unlike [`DB::restore`], this operation copies the source WAL as well as
    /// the durable generation. The destination opens normally, so committed
    /// WAL prefixes are reconciled (and replayed when they advance the
    /// manifest), while uncommitted or torn suffixes are reconciled there. The
    /// source is held under a shared advisory lock when
    /// its writer lock exists; an active writer therefore receives
    /// [`Error::DatabaseBusy`] instead of being copied concurrently.
    pub fn repair<P: AsRef<Path>, Q: AsRef<Path>>(
        source: P,
        destination: Q,
        options: Options,
    ) -> Result<RepairReport> {
        let source = source.as_ref().to_path_buf();
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "repair destination already exists: {}",
                destination.display()
            )));
        }

        let _source_lock = Self::acquire_source_shared_lock(&source)?;
        let source_check = DB::check(&source, options.clone())?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = Self::next_derived_path(&destination, "repair")?;
        let action = match source_check.wal_status {
            WalCheckStatus::Clean => RepairAction::NoRepair,
            WalCheckStatus::Pending => RepairAction::DiscardedUncommittedWal,
            WalCheckStatus::NeedsRecovery => RepairAction::ReconciledCommittedWal,
            WalCheckStatus::Incomplete => RepairAction::ReconciledIncompleteWal,
        };

        let result = (|| {
            fs::create_dir_all(&temporary)?;
            let copied_files = copy_repair_artifacts(&source, &temporary)?;
            sync_directory(&temporary)?;

            let mut repaired = DB::open(&temporary, options.clone())?;
            repaired.fork_history()?;
            let repaired_report = repaired.verify()?;
            if repaired_report.durability.database_id
                != source_check.verification.durability.database_id
            {
                return Err(Error::Corruption(
                    "repaired history changed the database identity".into(),
                ));
            }
            let destination_status = repaired_report.durability;
            drop(repaired);

            fs::rename(&temporary, &destination)?;
            if let Some(parent) = destination.parent() {
                sync_directory(parent)?;
            }

            Ok(RepairReport {
                source: source_check.verification.durability,
                source_wal_status: source_check.wal_status,
                destination: destination_status,
                copied_files,
                verified_pages: repaired_report.verified_pages,
                action,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
        }
        result
    }

    /// Create an owned read-only snapshot handle.
    ///
    /// The handle owns a verified temporary directory and removes it on
    /// `release()` or `Drop`. Use [`DB::snapshot`] when the archive should
    /// survive independently of this process.
    pub fn begin_snapshot(&mut self) -> Result<Snapshot> {
        self.check_writable()?;
        let destination = self.next_snapshot_path()?;
        self.snapshot(&destination)?;
        let db = match DB::open(&destination, self.options.clone()) {
            Ok(db) => db,
            Err(error) => {
                let _ = fs::remove_dir_all(&destination);
                return Err(error);
            }
        };
        Ok(Snapshot {
            db: Some(db),
            path: destination,
            released: false,
        })
    }

    fn next_snapshot_path(&self) -> Result<PathBuf> {
        Self::next_derived_path(&self.path, "snapshot")
    }

    fn next_derived_path(path: &Path, kind: &str) -> Result<PathBuf> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("seerdb");
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let id = NEXT_SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
        let destination = parent.join(format!(
            ".{name}.{kind}-{}-{timestamp}-{id}",
            std::process::id()
        ));
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "snapshot destination already exists: {}",
                destination.display()
            )));
        }
        Ok(destination)
    }

    fn fork_history(&mut self) -> Result<()> {
        self.check_writable()?;
        let manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
        let history_id = HistoryId::new(
            manifest
                .history_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("history ID overflow".into()))?,
        );
        let forked = Manifest {
            history_id,
            ..manifest
        };
        self.manifest.publish_replicated(forked)?;
        self.history_id = history_id;
        Ok(())
    }

    /// Reclaim trailing data pages that are no longer referenced by either
    /// manifest slot.
    ///
    /// This is intentionally a bounded first compaction operation. It does
    /// not move interior free pages and does not claim retained in-process
    /// snapshot support. A pending generation is flushed first; then the
    /// active manifest is mirrored into the other slot before truncation so a
    /// torn maintenance operation cannot fall back to a root that needs the
    /// removed tail pages.
    pub fn compact(&mut self) -> Result<CompactionReport> {
        self.check_writable()?;
        let result = self.compact_inner();
        if result.is_err() {
            // A maintenance failure can occur after the manifest barrier or
            // after the file length changed. Reopen is the only universally
            // safe way to reconstruct the active generation and allocator.
            self.write_fenced = true;
        }
        result
    }

    fn compact_inner(&mut self) -> Result<CompactionReport> {
        self.flush()?;

        let (before, after) = self.engine.reclaimable_tail_range()?;
        let mut manifest_replicated = false;
        if after < before {
            let manifest = self
                .manifest
                .load_latest()?
                .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
            self.manifest.publish(manifest)?;
            manifest_replicated = true;
        }

        let (actual_before, actual_after) = self.engine.truncate_reclaimable_tail()?;
        if actual_before != before || actual_after != after {
            return Err(Error::NeedsRecovery(
                "data file changed during compaction planning".into(),
            ));
        }

        Ok(CompactionReport {
            durability: self.durability_status(),
            data_bytes_before: actual_before,
            data_bytes_after: actual_after,
            reclaimed_pages: (actual_before - actual_after) / PAGE_SIZE as u64,
            manifest_replicated,
        })
    }

    /// Inject one device sync failure for the feature-gated fault harness.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_sync_failure(&self) {
        self.engine.inject_sync_failure();
    }

    /// Inject one device page-write failure for the feature-gated fault harness.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_write_failure(&self) {
        self.engine.inject_write_failure();
    }

    /// Inject one disk-full result for the feature-gated fault harness.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_disk_full(&self) {
        self.engine.inject_disk_full();
    }

    /// Set a persistent device capacity limit for the feature-gated fault harness.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_capacity_limit(&self, capacity: u64) {
        self.engine.inject_capacity_limit(capacity);
    }

    /// Inject one atomic artifact rename failure for the feature-gated fault
    /// harness. The next atomic publication on this thread fails before the
    /// rename, leaving the previous artifact available for recovery.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_atomic_rename_failure(&self) {
        inject_atomic_rename_failure();
    }

    /// Inject one failure before the next WAL append.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_wal_write_failure(&self) {
        FAIL_NEXT_WAL_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next WAL append but before its sync.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_wal_after_write_failure(&self) {
        FAIL_NEXT_WAL_AFTER_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure at the next WAL sync boundary.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_wal_sync_failure(&self) {
        FAIL_NEXT_WAL_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next WAL sync boundary.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_wal_after_sync_failure(&self) {
        FAIL_NEXT_WAL_AFTER_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure at the next manifest sync boundary.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_manifest_sync_failure(&self) {
        self.manifest.inject_sync_failure();
    }

    /// Inject one failure after the next manifest becomes authoritative.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_after_manifest_failure(&self) {
        FAIL_NEXT_AFTER_MANIFEST.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next WAL file is removed.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_wal_truncate_failure(&self) {
        FAIL_NEXT_WAL_TRUNCATE.with(|failure| failure.set(true));
    }

    /// Inject one truncated atomic checkpoint image before manifest publish.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_atomic_short_write_failure(&self) {
        FAIL_NEXT_ATOMIC_SHORT_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one checksum-corrupted atomic checkpoint image before manifest
    /// publish.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_atomic_torn_write_failure(&self) {
        FAIL_NEXT_ATOMIC_TORN_WRITE.with(|failure| failure.set(true));
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

    /// Reject ordinary reads after a failed publication until reopen restores
    /// the last authoritative root. The in-memory mutation overlay may be
    /// newer than the manifest after an ambiguous write, so exposing it would
    /// make one handle disagree with the state a crash recovery would choose.
    fn check_readable(&self) -> Result<()> {
        self.check_open()?;
        if self.write_fenced {
            return Err(Error::NeedsRecovery(
                "reads fenced after a failed durable publication; reopen required".into(),
            ));
        }
        Ok(())
    }

    /// Reject writes after a failed publication until the database is reopened.
    fn check_writable(&self) -> Result<()> {
        self.check_open()?;
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        if self.write_fenced {
            return Err(Error::NeedsRecovery(
                "writer fenced after a failed durable publication; reopen required".into(),
            ));
        }
        Ok(())
    }

    fn acquire_writer_lock(path: &Path) -> Result<File> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(file),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Err(Error::DatabaseBusy)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn acquire_source_shared_lock(path: &Path) -> Result<Option<File>> {
        let lock_path = path.join(LOCK_FILE);
        if !lock_path.is_file() {
            return Ok(None);
        }

        let file = OpenOptions::new().read(true).open(lock_path)?;
        match fs2::FileExt::try_lock_shared(&file) {
            Ok(()) => Ok(Some(file)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Err(Error::DatabaseBusy)
            }
            Err(error) => Err(error.into()),
        }
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
        if data.len() >= META_MAGIC.len() && data[..META_MAGIC.len()] == META_MAGIC {
            return Self::load_versioned_meta(&data);
        }
        Self::load_legacy_meta(&data)
    }

    fn load_versioned_meta(data: &[u8]) -> Result<(PMT, PageAllocator)> {
        const HEADER_SIZE: usize = META_MAGIC.len() + 4;
        const CHECKSUM_SIZE: usize = 4;
        if data.len() < HEADER_SIZE + CHECKSUM_SIZE {
            return Err(Error::Corruption("meta file is truncated".into()));
        }

        let version = u32::from_le_bytes(data[META_MAGIC.len()..HEADER_SIZE].try_into().map_err(
            |_| Error::Corruption("meta version is truncated".into()),
        )?);
        if version != FORMAT_VERSION {
            return Err(Error::Corruption(format!(
                "unsupported meta format version {version}"
            )));
        }

        let checksum_offset = data.len() - CHECKSUM_SIZE;
        let expected = u32::from_le_bytes(
            data[checksum_offset..]
                .try_into()
                .map_err(|_| Error::Corruption("meta checksum is truncated".into()))?,
        );
        let actual = crc32c::crc32c(&data[..checksum_offset]);
        if expected != actual {
            return Err(Error::Corruption("meta checksum mismatch".into()));
        }

        Self::load_legacy_meta(&data[HEADER_SIZE..checksum_offset])
    }

    fn load_legacy_meta(data: &[u8]) -> Result<(PMT, PageAllocator)> {

        if data.len() < 4 {
            return Err(Error::Corruption("meta file too small".into()));
        }

        let pmt_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

        let pmt_end = 4usize
            .checked_add(pmt_len)
            .ok_or_else(|| Error::Corruption("meta PMT length overflows".into()))?;
        let alloc_len_start = pmt_end;
        let alloc_len_end = alloc_len_start
            .checked_add(4)
            .ok_or_else(|| Error::Corruption("meta allocator length overflows".into()))?;
        if data.len() < alloc_len_end {
            return Err(Error::Corruption("meta file truncated".into()));
        }

        let pmt = PMT::from_bytes(&data[4..pmt_end])
            .ok_or_else(|| Error::Corruption("invalid PMT data".into()))?;

        let alloc_offset = alloc_len_start;
        let alloc_len = u32::from_le_bytes([
            data[alloc_offset],
            data[alloc_offset + 1],
            data[alloc_offset + 2],
            data[alloc_offset + 3],
        ]) as usize;

        let alloc_end = alloc_len_end
            .checked_add(alloc_len)
            .ok_or_else(|| Error::Corruption("meta allocator length overflows".into()))?;
        if data.len() != alloc_end {
            return Err(Error::Corruption(
                if data.len() < alloc_end {
                    "meta allocator data is truncated"
                } else {
                    "meta file has trailing bytes"
                }
                .into(),
            ));
        }

        let alloc_data = &data[alloc_len_end..alloc_end];
        let allocator = PageAllocator::from_bytes(alloc_data)
            .ok_or_else(|| Error::Corruption("invalid allocator data".into()))?;

        Ok((pmt, allocator))
    }

    /// Save PMT and allocator to meta file.
    fn save_meta(path: &Path, pmt: &PMT, allocator: &PageAllocator) -> Result<()> {
        let pmt_bytes = pmt.to_bytes();
        let alloc_bytes = allocator.to_bytes();

        let pmt_len = u32::try_from(pmt_bytes.len())
            .map_err(|_| Error::InvalidArgument("PMT checkpoint is too large".into()))?;
        let alloc_len = u32::try_from(alloc_bytes.len())
            .map_err(|_| Error::InvalidArgument("allocator checkpoint is too large".into()))?;

        let mut buf = Vec::with_capacity(
            META_MAGIC.len() + 4 + 4 + pmt_bytes.len() + 4 + alloc_bytes.len() + 4,
        );
        buf.extend_from_slice(&META_MAGIC);
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&pmt_len.to_le_bytes());
        buf.extend_from_slice(&pmt_bytes);
        buf.extend_from_slice(&alloc_len.to_le_bytes());
        buf.extend_from_slice(&alloc_bytes);
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        atomic_write(path, &buf)
    }

    /// Recover a committed WAL prefix and reject corrupt complete records.
    fn recover_from_wal(
        wal_path: &Path,
        current_manifest: Option<Manifest>,
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
                RecordType::Put | RecordType::Delete | RecordType::PutV2 | RecordType::DeleteV2 => {
                    pending.push(record)
                }
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

                    if let Some(current) = current_manifest {
                        match commit.generation_id.get().cmp(&current.generation_id.get()) {
                            std::cmp::Ordering::Less => {
                                if commit.commit_id.get() > current.commit_id.get() {
                                    return Err(Error::Corruption(
                                        "WAL commit frontier is inconsistent with manifest".into(),
                                    ));
                                }
                                pending.clear();
                                offset += record_len;
                                continue;
                            }
                            std::cmp::Ordering::Equal => {
                                if commit.commit_id != current.commit_id {
                                    return Err(Error::Corruption(
                                        "WAL commit frontier is inconsistent with manifest".into(),
                                    ));
                                }
                                if commit.root_page_id != current.root_page_id
                                    || commit.mutation_count != current.mutation_count
                                    || commit.digest != current.digest
                                {
                                    return Err(Error::Corruption(
                                        "WAL commit disagrees with authoritative manifest".into(),
                                    ));
                                }
                                pending.clear();
                                offset += record_len;
                                continue;
                            }
                            std::cmp::Ordering::Greater => {
                                if commit.commit_id <= current.commit_id {
                                    return Err(Error::Corruption(
                                        "WAL commit frontier is inconsistent with manifest".into(),
                                    ));
                                }
                            }
                        }
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

fn error_message(error: Error) -> String {
    match error {
        Error::Corruption(message) => message,
        Error::Check { message, .. } => message,
        other => other.to_string(),
    }
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

fn apply_put_mutation(
    key: &[u8],
    value: &[u8],
    btree: &mut BTree,
    blobs: &mut BlobManager,
) -> Result<()> {
    let previous_blob = match btree.lookup(key)? {
        LookupResult::Blob(pointer) => Some(pointer),
        _ => None,
    };

    if blobs.should_separate(value.len()) {
        let pointer = blobs.append(key, value.to_vec());
        if let Err(error) = btree.upsert_blob(key, pointer) {
            let _ = blobs.rollback_append(&pointer);
            return Err(error.into());
        }
    } else {
        btree.upsert(key, value)?;
    }

    if let Some(pointer) = previous_blob {
        blobs.mark_deleted(&pointer);
    }
    Ok(())
}

fn apply_mutation(record: &WalRecord, btree: &mut BTree, blobs: &mut BlobManager) -> Result<()> {
    match record.record_type {
        RecordType::Put => {
            let (key, value) = decode_put_payload(false, &record.payload)?;
            apply_put_mutation(key, value, btree, blobs)?;
        }
        RecordType::PutV2 => {
            let (key, value) = decode_put_payload(true, &record.payload)?;
            apply_put_mutation(key, value, btree, blobs)?;
        }
        RecordType::Delete | RecordType::DeleteV2 => {
            let key = decode_delete_payload(
                record.record_type == RecordType::DeleteV2,
                &record.payload,
            )?;
            let previous_blob = match btree.lookup(key)? {
                LookupResult::Blob(pointer) => Some(pointer),
                _ => None,
            };
            let found = btree.delete(key)?;
            if found && let Some(pointer) = previous_blob {
                blobs.mark_deleted(&pointer);
            }
        }
        _ => {
            return Err(Error::Corruption(
                "non-mutation passed to WAL applier".into(),
            ));
        }
    }
    Ok(())
}

fn decode_put_payload(v2: bool, payload: &[u8]) -> Result<(&[u8], &[u8])> {
    if v2 {
        if payload.len() < 4 + 4 {
            return Err(Error::Corruption("WAL v2 put record too small".into()));
        }
        let key_len = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        let value_len_offset = 4usize
            .checked_add(key_len)
            .ok_or_else(|| Error::Corruption("WAL key length overflow".into()))?;
        if payload.len() < value_len_offset + 4 {
            return Err(Error::Corruption("WAL v2 put key is truncated".into()));
        }
        let value_len = u32::from_le_bytes([
            payload[value_len_offset],
            payload[value_len_offset + 1],
            payload[value_len_offset + 2],
            payload[value_len_offset + 3],
        ]) as usize;
        let value_offset = value_len_offset + 4;
        if payload.len() != value_offset + value_len {
            return Err(Error::Corruption("WAL v2 put value is truncated".into()));
        }
        return Ok((&payload[4..value_len_offset], &payload[value_offset..]));
    }

    // Read the pre-v2 u16 layout so an upgrade can recover an older WAL.
    if payload.len() < 4 {
        return Err(Error::Corruption("WAL put record too small".into()));
    }
    let key_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let value_len_offset = 2usize
        .checked_add(key_len)
        .ok_or_else(|| Error::Corruption("WAL key length overflow".into()))?;
    if payload.len() < value_len_offset + 2 {
        return Err(Error::Corruption("WAL put key is truncated".into()));
    }
    let value_len = u16::from_le_bytes([
        payload[value_len_offset],
        payload[value_len_offset + 1],
    ]) as usize;
    let value_offset = value_len_offset + 2;
    if payload.len() != value_offset + value_len {
        return Err(Error::Corruption("WAL put value is truncated".into()));
    }
    Ok((&payload[2..value_len_offset], &payload[value_offset..]))
}

fn validate_wal_key_length(key: &[u8]) -> Result<()> {
    if u32::try_from(key.len()).is_err() {
        return Err(Error::InvalidArgument(
            "key exceeds the durable WAL length limit".into(),
        ));
    }
    Ok(())
}

fn validate_wal_put_lengths(key: &[u8], value: &[u8]) -> Result<()> {
    validate_wal_key_length(key)?;
    if u32::try_from(value.len()).is_err() {
        return Err(Error::InvalidArgument(
            "value exceeds the durable WAL length limit".into(),
        ));
    }
    Ok(())
}

fn decode_delete_payload(v2: bool, payload: &[u8]) -> Result<&[u8]> {
    if v2 {
        if payload.len() < 4 {
            return Err(Error::Corruption("WAL v2 delete record too small".into()));
        }
        let key_len = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        if payload.len() != 4 + key_len {
            return Err(Error::Corruption("WAL v2 delete key is truncated".into()));
        }
        return Ok(&payload[4..]);
    }

    // Read the pre-v2 u16 layout so an upgrade can recover an older WAL.
    if payload.len() < 2 {
        return Err(Error::Corruption("WAL delete record too small".into()));
    }
    let key_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    if payload.len() != 2 + key_len {
        return Err(Error::Corruption("WAL delete key is truncated".into()));
    }
    Ok(&payload[2..])
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    if let Err(error) = preallocate_file(&file, data.len() as u64) {
        drop(file);
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    #[cfg(any(test, feature = "fault-injection"))]
    let short_write = FAIL_NEXT_ATOMIC_SHORT_WRITE.with(|failure| failure.replace(false));
    #[cfg(not(any(test, feature = "fault-injection")))]
    let short_write = false;
    #[cfg(any(test, feature = "fault-injection"))]
    let torn_write = FAIL_NEXT_ATOMIC_TORN_WRITE.with(|failure| failure.replace(false));
    #[cfg(not(any(test, feature = "fault-injection")))]
    let torn_write = false;

    if short_write {
        let prefix_len = data.len() / 2;
        file.set_len(prefix_len as u64)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&data[..prefix_len])?;
    } else {
        file.write_all(data)?;
        if torn_write && !data.is_empty() {
            let offset = data.len() / 2;
            file.seek(SeekFrom::Start(offset as u64))?;
            file.write_all(&[data[offset] ^ 0xA5])?;
        }
    }
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

    #[cfg(any(test, feature = "fault-injection"))]
    if short_write || torn_write {
        return Err(std::io::Error::other("injected atomic artifact write failure").into());
    }

    sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn atomic_write_reserved(path: &Path, reservation: &Path, data: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(reservation)?;
    if !reserve_file(&file, data.len() as u64)? {
        return Err(Error::DiskFull);
    }
    file.set_len(data.len() as u64)?;
    file.seek(SeekFrom::Start(0))?;
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

    fs::rename(reservation, path)?;
    sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
}

#[cfg(any(test, feature = "fault-injection"))]
#[allow(dead_code)]
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

fn clear_blob_reservation(path: &Path) -> Result<()> {
    let reservation = path.join(BLOB_RESERVATION_FILE);
    if reservation.exists() {
        fs::remove_file(reservation)?;
        sync_directory(path)?;
    }
    Ok(())
}

fn copy_snapshot_artifacts(source: &Path, destination: &Path) -> Result<u32> {
    copy_artifacts(source, destination, false)
}

fn copy_repair_artifacts(source: &Path, destination: &Path) -> Result<u32> {
    copy_artifacts(source, destination, true)
}

fn copy_artifacts(source: &Path, destination: &Path, include_wal: bool) -> Result<u32> {
    let mut copied_files = 0u32;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".tmp")
            || name == LOCK_FILE
            || name == ARCHIVE_MARKER_FILE
            || !(name == MANIFEST_FILE
                || name == DATA_FILE
                || name == BLOB_FILE
                || name == META_FILE
                || name.starts_with("seerdb.meta.")
                || (include_wal && (name == WAL_FILE || name == WAL_RESERVATION_FILE)))
        {
            continue;
        }

        let destination_file = destination.join(name.as_ref());
        fs::copy(entry.path(), &destination_file)?;
        OpenOptions::new()
            .read(true)
            .open(destination_file)?
            .sync_all()?;
        copied_files = copied_files
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("too many snapshot artifacts".into()))?;
    }

    Ok(copied_files)
}

impl Drop for DB {
    fn drop(&mut self) {
        // Don't call close() — let the WAL persist for crash recovery.
        // The user should explicitly call close() or flush() to ensure
        // data is persisted and WAL is cleaned up.
        // If the process crashes, the WAL file will be preserved for recovery.
        if let Some(lock_file) = self.lock_file.take() {
            let _ = lock_file.unlock();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::{Seek, SeekFrom, Write};
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    use std::os::unix::fs::MetadataExt;
    use std::process::Command;
    use tempfile::tempdir;

    use crate::storage::format::MANIFEST_SLOT_SIZE;

    #[test]
    fn test_db_open() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path().join("test.db"), Options::default());
        assert!(db.is_ok());
    }

    #[test]
    fn test_db_allows_only_one_writer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("locked.db");
        let db = DB::open(&path, Options::default()).unwrap();
        assert!(matches!(
            DB::open(&path, Options::default()),
            Err(Error::DatabaseBusy)
        ));
        drop(db);
        assert!(DB::open(&path, Options::default()).is_ok());
    }

    #[test]
    fn test_db_check_is_non_mutating_and_does_not_take_writer_lock() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("check.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"pending", b"value").unwrap();

        let pending = DB::check(&path, Options::default()).unwrap();
        assert_eq!(pending.wal_status, WalCheckStatus::Pending);
        assert_eq!(
            pending.verification.wal_bytes,
            fs::metadata(path.join(WAL_FILE)).unwrap().len()
        );
        assert_eq!(db.get(b"pending").unwrap(), Some(b"value".to_vec()));

        db.flush().unwrap();
        let clean = DB::check(&path, Options::default()).unwrap();
        assert_eq!(clean.wal_status, WalCheckStatus::Clean);
        assert_eq!(clean.verification.wal_bytes, 0);
        db.close().unwrap();
    }

    #[test]
    fn test_db_check_does_not_create_missing_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing.db");

        assert!(matches!(
            DB::check(&path, Options::default()),
            Err(Error::Check { kind: CheckFailureKind::Target, message })
                if message.contains("does not exist")
        ));
        assert!(!path.exists());
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

        let results = db.range(b"b", b"d").unwrap();
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
    fn test_db_metrics_attribute_page_work_and_lazy_reads() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metrics.db");

        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();

            let metrics = db.metrics().unwrap();
            assert_eq!(metrics.storage.physical_page_writes, 1);
            assert_eq!(metrics.storage.page_bytes_written, PAGE_SIZE as u64);
            assert_eq!(metrics.storage.generation_flushes, 1);
            assert_eq!(metrics.storage.syncs, 1);
            assert_eq!(metrics.data_bytes, PAGE_SIZE as u64);
            assert_eq!(metrics.wal_bytes, 0);
        }

        let reopened = DB::open(&path, Options::default()).unwrap();
        let before = reopened.metrics().unwrap();
        assert_eq!(before.storage.logical_page_reads, 0);
        assert_eq!(before.storage.physical_page_reads, 0);
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
        let after = reopened.metrics().unwrap();
        assert_eq!(after.storage.logical_page_reads, 2);
        assert_eq!(after.storage.physical_page_reads, 1);
        assert_eq!(after.storage.page_bytes_read, PAGE_SIZE as u64);
        assert_eq!(after.buffer.reads, 1);
    }

    #[test]
    fn test_db_wal_admission_rejects_before_blob_or_tree_mutation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal-admission.db");
        let value = vec![0xA5; 2_000];
        let record_bytes = WalRecord::put(b"key", &value).to_bytes().len() as u64;
        let exact_budget = record_bytes + WAL_COMMIT_RECORD_BYTES;
        let mut options = Options::for_test();
        options.max_wal_bytes = exact_budget - 1;

        let mut db = DB::open(&path, options).unwrap();
        let error = db.put(b"key", &value).unwrap_err();
        assert!(matches!(
            error,
            Error::Backpressure { required, available }
                if required == exact_budget && available == exact_budget - 1
        ));
        assert_eq!(db.get(b"key").unwrap(), None);
        assert_eq!(db.blob_stats().total_valid, 0);
        assert!(!path.join(WAL_FILE).exists());
        assert!(!db.durability_status().write_fenced);
        assert_eq!(db.metrics().unwrap().wal_admission_failures, 1);

        db.options.max_wal_bytes = exact_budget;
        db.put(b"key", &value).unwrap();
        assert_eq!(
            fs::metadata(path.join(WAL_RESERVATION_FILE))
                .unwrap()
                .len(),
            WAL_RESERVATION_SEGMENT_BYTES
        );
        assert!(path.join(BLOB_RESERVATION_FILE).is_file());
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(
            fs::metadata(path.join(BLOB_RESERVATION_FILE))
                .unwrap()
                .blocks()
                > 0,
            "blob reservation should own physical blocks on this platform"
        );
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(
            fs::metadata(path.join(WAL_RESERVATION_FILE))
                .unwrap()
                .blocks()
                > 0,
            "WAL reservation should own physical blocks on this platform"
        );
        assert_eq!(
            db.metrics().unwrap().wal_reserved_bytes,
            WAL_RESERVATION_SEGMENT_BYTES
        );
        db.flush().unwrap();
        assert!(!path.join(BLOB_RESERVATION_FILE).exists());
        drop(db);

        let reopened = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(value));
    }

    #[test]
    fn test_db_blob_admission_rejects_before_blob_or_tree_mutation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blob-admission.db");
        let value = vec![0x5A; 2_000];
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.inject_capacity_limit(0);

        assert!(matches!(db.put(b"key", &value), Err(Error::DiskFull)));
        assert_eq!(db.get(b"key").unwrap(), None);
        assert_eq!(db.blob_stats().total_valid, 0);
        assert_eq!(db.durability_status().pending_mutations, 0);
        assert!(!db.durability_status().write_fenced);

        drop(db);
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_db_reopen_reads_pmt_pages_on_demand() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        {
            let mut db = DB::open(&path, Options::for_test()).unwrap();
            for index in 0..500 {
                let key = format!("key-{index:06}");
                db.put(key.as_bytes(), b"value").unwrap();
            }
            db.flush().unwrap();
        }

        let mut db = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(db.engine.btree().node_count(), 0);
        assert_eq!(db.engine.buffer_stats().reads, 0);

        assert_eq!(db.get(b"key-000250").unwrap(), Some(b"value".to_vec()));
        assert!(db.engine.buffer_stats().reads > 0);
        assert_eq!(db.engine.btree().node_count(), 0);

        let range = db.range(b"key-000050", b"key-000450").unwrap();
        assert_eq!(range.len(), 400);
        assert_eq!(range.first().unwrap().0, b"key-000050");
        assert_eq!(range.last().unwrap().0, b"key-000449");
        assert_eq!(db.engine.btree().node_count(), 0);

        db.put(b"key-000250", b"updated").unwrap();
        assert!(db.engine.btree().node_count() > 0);
        db.flush().unwrap();
        assert_eq!(db.get(b"key-000250").unwrap(), Some(b"updated".to_vec()));
    }

    #[test]
    fn test_db_sparse_mutation_overlay_preserves_unloaded_ranges() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sparse-mutation.db");

        {
            let mut db = DB::open(&path, Options::for_test()).unwrap();
            for index in 0..500 {
                let key = format!("key-{index:06}");
                let value = format!("value-{index:06}");
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }

        let mut db = DB::open(&path, Options::for_test()).unwrap();
        let durable_page_count = db.engine.pmt().iter().count();
        db.put(b"key-000250", b"updated-250").unwrap();
        db.put(b"key-000450", b"updated-450").unwrap();
        assert!(db.engine.btree().node_count() < durable_page_count);
        assert_eq!(
            db.get(b"key-000250").unwrap(),
            Some(b"updated-250".to_vec())
        );
        assert_eq!(
            db.get(b"key-000450").unwrap(),
            Some(b"updated-450".to_vec())
        );

        let before_delete = db.range(b"key-000240", b"key-000460").unwrap();
        assert_eq!(before_delete.len(), 220);
        assert_eq!(
            before_delete
                .iter()
                .find(|(key, _)| key == b"key-000250")
                .map(|(_, value)| value.as_slice()),
            Some(b"updated-250".as_slice())
        );

        assert!(db.delete(b"key-000300").unwrap());
        let after_delete = db.range(b"key-000240", b"key-000460").unwrap();
        assert_eq!(after_delete.len(), 219);
        assert!(!after_delete.iter().any(|(key, _)| key == b"key-000300"));
        db.flush().unwrap();
        drop(db);

        let reopened = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(
            reopened.get(b"key-000250").unwrap(),
            Some(b"updated-250".to_vec())
        );
        assert_eq!(reopened.get(b"key-000300").unwrap(), None);
        assert_eq!(reopened.get(b"key-000450").unwrap(), Some(b"updated-450".to_vec()));
    }

    #[test]
    fn test_db_sparse_mutation_overlay_split_reopens() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sparse-split.db");

        {
            let mut db = DB::open(&path, Options::for_test()).unwrap();
            for index in 0..500 {
                let key = format!("key-{index:06}");
                db.put(key.as_bytes(), b"value").unwrap();
            }
            db.flush().unwrap();
        }

        let mut db = DB::open(&path, Options::for_test()).unwrap();
        for index in 0..120 {
            let key = format!("key-000250-new-{index:03}");
            db.put(key.as_bytes(), b"new-value").unwrap();
        }
        assert!(db.engine.btree().dirty_page_ids().len() > 1);
        assert_eq!(
            db.range(b"key-000250-new-000", b"key-000250-new-120")
                .unwrap()
                .len(),
            120
        );
        db.flush().unwrap();
        drop(db);

        let reopened = DB::open(&path, Options::for_test()).unwrap();
        let values = reopened
            .range(b"key-000250-new-000", b"key-000250-new-120")
            .unwrap();
        assert_eq!(values.len(), 120);
        assert_eq!(values[0], (b"key-000250-new-000".to_vec(), b"new-value".to_vec()));
        assert_eq!(values[119].0, b"key-000250-new-119");
    }

    #[test]
    fn test_db_rejects_malformed_meta_container() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();
        }

        let checkpoint = path.join("seerdb.meta.1");
        let mut meta = fs::read(&checkpoint).unwrap();
        meta.push(0xA5);
        fs::write(&checkpoint, meta).unwrap();

        assert!(matches!(
            DB::open(&path, Options::default()),
            Err(Error::Corruption(message)) if message.contains("checksum")
        ));
    }

    #[test]
    fn test_db_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Write data and close.
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            let initial = db.durability_status();
            assert_eq!(initial.generation_id.get(), 0);
            assert_eq!(initial.commit_id.get(), 0);
            db.put(b"key1", b"value1").unwrap();
            assert_eq!(db.durability_status().pending_mutations, 1);
            db.flush().unwrap();
            let published = db.durability_status();
            assert_eq!(published.generation_id.get(), 1);
            assert_eq!(published.commit_id.get(), 1);
            assert_eq!(published.pending_mutations, 0);
            db.put(b"key2", b"value2").unwrap();
            db.put(b"key3", b"value3").unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        // Reopen and verify data persisted.
        {
            let db = DB::open(&path, Options::default()).unwrap();
            let status = db.durability_status();
            assert_eq!(status.generation_id.get(), 2);
            assert_eq!(status.commit_id.get(), 2);
            assert_eq!(status.pending_mutations, 0);
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

        let db = DB::open(&path, Options::default()).unwrap();
        let result = db.get(b"key");
        assert!(matches!(
            result,
            Err(Error::Corruption(message)) if message.contains("checksum mismatch")
        ));
        assert!(matches!(
            DB::check(&path, Options::default()),
            Err(Error::Check { kind: CheckFailureKind::DataPage, .. })
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
    fn test_db_process_crash_recovery() {
        if let Some(path) = std::env::var_os("SEERDB_CRASH_CHILD_PATH") {
            let path = PathBuf::from(path);
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"published", b"value-before-crash").unwrap();
            db.flush().unwrap();
            db.put(b"unpublished", b"value-after-wal-only").unwrap();

            // Exit without running Rust destructors. This leaves the WAL
            // mutation on disk while the manifest still names the prior
            // published generation, matching an abrupt process termination.
            std::process::exit(137);
        }

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("db::tests::test_db_process_crash_recovery")
            .arg("--nocapture")
            .env("SEERDB_CRASH_CHILD_PATH", &path)
            .status()
            .unwrap();
        assert!(!status.success(), "crash child unexpectedly exited cleanly");

        let db = DB::open(&path, Options::default()).unwrap();
        assert_eq!(
            db.get(b"published").unwrap(),
            Some(b"value-before-crash".to_vec())
        );
        assert_eq!(db.get(b"unpublished").unwrap(), None);
        assert!(!path.join(WAL_FILE).exists());
    }

    #[test]
    fn test_db_randomized_publication_fault_matrix() {
        fn next(seed: &mut u64) -> u64 {
            *seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            *seed
        }

        fn assert_model(db: &DB, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
            for key_id in 0..16 {
                let key = format!("key-{key_id:02}");
                assert_eq!(
                    db.get(key.as_bytes()).unwrap(),
                    model.get(key.as_bytes()).cloned()
                );
            }
        }

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        let mut committed = BTreeMap::new();
        let mut seed = 0x5EED_CAFE_u64;

        for round in 0..32 {
            let mut candidate = committed.clone();
            let operation_count = (next(&mut seed) % 4 + 1) as usize;
            for operation in 0..operation_count {
                let key_id = next(&mut seed) % 16;
                let key = format!("key-{key_id:02}");
                let value = format!("value-{round:02}-{operation:02}-{key_id:02}");
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                candidate.insert(key.into_bytes(), value.into_bytes());
            }

            let fault = next(&mut seed) % 4;
            match fault {
                1 => db.engine.inject_sync_failure(),
                2 => db.engine.inject_write_failure(),
                3 => inject_atomic_rename_failure(),
                _ => {}
            }

            let result = db.flush();
            if fault == 0 {
                result.unwrap();
                committed = candidate;
                assert_model(&db, &committed);
            } else {
                assert!(result.is_err(), "fault {fault} did not fail publication");
                drop(db);
                db = DB::open(&path, Options::default()).unwrap();
                assert_model(&db, &committed);
            }
        }

        db.close().unwrap();
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_model(&reopened, &committed);
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
        assert!(matches!(
            db.get(b"key"),
            Err(Error::NeedsRecovery(message)) if message.contains("reads fenced")
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
    fn test_db_fences_writer_after_disk_full() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.engine.inject_disk_full();

        assert!(matches!(db.flush(), Err(Error::DiskFull)));
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
    fn test_db_retains_manifest_fallback_before_reusing_pages() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("manifest-retention.db");
        let mut db = DB::open(&path, Options::default()).unwrap();

        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();

        // The next generation can reuse a page from before the current
        // generation, but only after both manifest slots have been fenced to
        // the current root. Fail before the new manifest is published.
        db.put(b"key", b"value-3").unwrap();
        inject_atomic_rename_failure();
        assert!(matches!(db.flush(), Err(Error::Io(_))));
        drop(db);

        // Simulate loss of the newest manifest slot. The mirrored fallback
        // must still name value-2 even though the failed generation reused an
        // older physical page.
        let manifest_path = path.join(MANIFEST_FILE);
        let mut manifest_file = OpenOptions::new()
            .write(true)
            .open(&manifest_path)
            .unwrap();
        manifest_file
            .seek(SeekFrom::Start(MANIFEST_SLOT_SIZE as u64))
            .unwrap();
        manifest_file.write_all(&[0xA5; MANIFEST_SLOT_SIZE]).unwrap();
        manifest_file.sync_all().unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
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
            let replacement = vec![0xCD; 3_000];
            db.put(b"key1", &replacement).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), Some(replacement.clone()));
            db.flush().unwrap();
            assert!(db.blob_stats().total_deleted > 0);
            assert_eq!(db.get(b"key1").unwrap(), Some(replacement));
            assert_eq!(
                db.range(b"key1", b"key3").unwrap(),
                vec![
                    (b"key1".to_vec(), vec![0xCD; 3_000]),
                    (b"key2".to_vec(), b"small".to_vec()),
                ]
            );
            db.close().unwrap();
        }

        // Verify blob file exists.
        assert!(path.join(BLOB_FILE).exists(), "blob file should exist");

        // Reopen and verify blob data persisted.
        {
            let db = DB::open(&path, Options::default()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), Some(vec![0xCD; 3_000]));
            assert_eq!(db.get(b"key2").unwrap(), Some(b"small".to_vec()));
            assert_eq!(
                db.range(b"key1", b"key3").unwrap(),
                vec![
                    (b"key1".to_vec(), vec![0xCD; 3_000]),
                    (b"key2".to_vec(), b"small".to_vec()),
                ]
            );
        }
    }

    #[test]
    fn test_db_recovers_committed_blob_upsert() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blob-recovery.db");
        let initial = vec![0x11; 2_000];
        let replacement = vec![0x22; 3_000];

        let (commit_id, generation_id, root_page_id) = {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", &initial).unwrap();
            db.flush().unwrap();

            (
                db.commit_id.get(),
                db.generation_id.get(),
                db.engine.btree().root_id() as u64,
            )
        };

        let record = WalRecord::put(b"key", &replacement);
        let commit = CommitRecord {
            commit_id: CommitId::new(commit_id + 1),
            generation_id: GenerationId::new(generation_id + 1),
            root_page_id,
            mutation_count: 1,
            digest: digest_records(&[&record]),
        };
        let mut wal_bytes = record.to_bytes();
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(replacement));
    }

    #[test]
    fn test_db_discards_wal_commit_already_published_by_manifest() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("stale-authoritative-wal.db");

        let (commit_id, generation_id, root_page_id) = {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();

            (
                db.commit_id,
                db.generation_id,
                db.engine.btree().root_id() as u64,
            )
        };

        // Model the crash window where manifest publication succeeded but WAL
        // cleanup did not. Replaying this commit would publish the same
        // logical state under a new generation.
        let record = WalRecord::put(b"key", b"value");
        let commit = CommitRecord {
            commit_id,
            generation_id,
            root_page_id,
            mutation_count: 1,
            digest: digest_records(&[&record]),
        };
        let mut wal_bytes = record.to_bytes();
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.commit_id, commit_id);
        assert_eq!(reopened.generation_id, generation_id);
        assert_eq!(reopened.metrics().unwrap().storage.generation_flushes, 0);
        assert!(!path.join(WAL_FILE).exists());
    }

    #[test]
    fn test_db_recovers_committed_large_blob_value() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large-blob-recovery.db");
        let value = vec![0x7B; 70_000];
        let record = WalRecord::put(b"large-key", &value);
        let commit = CommitRecord {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            root_page_id: 0,
            mutation_count: 1,
            digest: digest_records(&[&record]),
        };
        let mut wal_bytes = record.to_bytes();
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"large-key").unwrap(), Some(value));
    }

    #[test]
    fn test_db_replays_legacy_wal_put_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy-wal.db");
        let mut payload = Vec::new();
        payload.extend_from_slice(&(3u16).to_le_bytes());
        payload.extend_from_slice(b"key");
        payload.extend_from_slice(&(5u16).to_le_bytes());
        payload.extend_from_slice(b"value");
        let record = WalRecord::new(RecordType::Put, payload);
        let commit = CommitRecord {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            root_page_id: 0,
            mutation_count: 1,
            digest: digest_records(&[&record]),
        };
        let mut wal_bytes = record.to_bytes();
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
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
        assert_eq!(reclaimed, 0);
        assert_eq!(db.get(b"key3").unwrap(), Some(large_value));

        // Check stats after GC.
        let stats = db.blob_stats();
        assert_eq!(stats.files_needing_gc, 1);

        db.delete(b"key3").unwrap();
        db.flush().unwrap();
        assert_eq!(db.gc().unwrap(), 3);
        assert_eq!(db.blob_stats().files_needing_gc, 0);

        drop(db);
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key1").unwrap(), None);
        assert_eq!(reopened.get(b"key2").unwrap(), None);
        assert_eq!(reopened.get(b"key3").unwrap(), None);
    }

    #[test]
    fn test_db_gc_admission_rejects_before_catalog_reclamation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("gc-admission.db");
        let value = vec![0xAB; 2_000];
        let mut db = DB::open(&path, Options::default()).unwrap();

        db.put(b"key", &value).unwrap();
        db.flush().unwrap();
        db.delete(b"key").unwrap();
        db.flush().unwrap();

        let before_bytes = fs::metadata(path.join(BLOB_FILE)).unwrap().len();
        let before_stats = db.blob_stats();
        assert_eq!(before_stats.total_valid, 0);
        assert_eq!(before_stats.total_deleted, 1);
        assert_eq!(before_stats.files_needing_gc, 1);

        db.inject_capacity_limit(0);
        assert!(matches!(db.gc(), Err(Error::DiskFull)));
        assert_eq!(
            fs::metadata(path.join(BLOB_FILE)).unwrap().len(),
            before_bytes
        );
        let after_failed_stats = db.blob_stats();
        assert_eq!(after_failed_stats.total_valid, before_stats.total_valid);
        assert_eq!(after_failed_stats.total_deleted, before_stats.total_deleted);
        assert_eq!(
            after_failed_stats.files_needing_gc,
            before_stats.files_needing_gc
        );
        assert!(!db.durability_status().write_fenced);

        db.inject_capacity_limit(u64::MAX);
        assert_eq!(db.gc().unwrap(), 1);
        assert_eq!(db.blob_stats().files_needing_gc, 0);
    }
}
