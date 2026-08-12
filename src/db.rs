//! Database entry point.
//!
//! The `DB` struct is the main entry point for the storage engine.
//! It owns all components and provides the public API.

#[path = "db/archive.rs"]
mod archive;
#[path = "db/artifact_io.rs"]
mod artifact_io;
#[path = "db/blob_layout.rs"]
mod blob_layout;
#[path = "db/blob_publication.rs"]
mod blob_publication;
#[path = "db/blob_read_view.rs"]
mod blob_read_view;
#[path = "db/compaction.rs"]
mod compaction;
#[path = "db/diagnostics.rs"]
mod diagnostics;
#[path = "db/durability.rs"]
mod durability;
#[path = "db/metadata.rs"]
mod metadata;
mod mutation;
#[path = "db/open.rs"]
mod open;
mod options;
#[path = "db/publication.rs"]
mod publication;
#[path = "db/read_view.rs"]
mod read_view;
#[path = "db/retention.rs"]
mod retention;
#[path = "db/retention_state.rs"]
mod retention_state;
#[path = "db/snapshot.rs"]
mod snapshot;
#[path = "db/transaction.rs"]
mod transaction;
#[path = "db/vacuum.rs"]
mod vacuum;
#[path = "db/wal_recovery.rs"]
mod wal_recovery;

#[cfg(test)]
use metadata::{MAX_META_DELTA_CHAIN, META_DELTA_MAGIC, META_MAGIC};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use artifact_io::atomic_write_reserved;
#[cfg(any(test, feature = "fault-injection"))]
use artifact_io::inject_atomic_rename_failure;
use artifact_io::{
    atomic_write, atomic_write_without_directory_sync, atomic_write_without_fault_injection,
    cleanup_orphaned_temporary_artifacts, clear_blob_reservation, clear_wal_reservation,
    sync_directory, sync_directory_chain, sync_history_prune_directory, sync_publication_directory,
};
#[cfg(test)]
use blob_layout::MAX_SEGMENTED_CATALOG_DELETED_ENTRIES;
use blob_layout::{
    BLOB_DELTA_FILE, BLOB_FILE, BLOB_RESERVATION_FILE, BLOB_REWRITE_BACKUP_FILE,
    BLOB_SEGMENT_PREFIX, blob_segment_path, blob_storage_size, parse_blob_catalog,
    retained_blob_path, segmented_catalog_needs_consolidation,
};
use blob_read_view::BlobReadView;
use mutation::{Mutation, apply as apply_mutation, require_blob_deletion};
use wal_recovery::{
    decode_delete_payload, decode_put_payload, digest_records, extend_digest,
    validate_wal_key_length, validate_wal_put_lengths,
};

pub use options::{BlobStorageMode, Options};
pub use read_view::ReadView;
pub use snapshot::{RetainedSnapshot, Snapshot};
pub use transaction::{BatchMutation, BatchTransaction, BatchTransactionState};

use crate::allocator::PageAllocator;
use crate::blob::BlobManager;
use crate::btree::{BTree, BlobPointer, LookupResult, MAX_KEY_SIZE, PAGE_SIZE, RangeCursor};
use crate::buffer::{BufferManager, BufferStats};
use crate::concurrency::TransactionManager;
use crate::error::{CheckFailureKind, Error, Result};
use crate::mvcc::PMT;
use crate::recovery::{ParseStatus, RecordType, SyncPolicy, WalManager, WalRecord};
use crate::space::{Device, DeviceOptions};
use crate::storage::format::{
    CommitId, CommitRecord, DatabaseId, FORMAT_VERSION, GenerationId, HistoryId,
    MANIFEST_SLOT_SIZE, Manifest, ManifestHistory, ManifestStore, PmtCheckpointId, ReuseAttempt,
    ReuseLedger, SnapshotId,
};
use crate::storage::{StorageEngine, StorageMetrics};
use fs2::FileExt;
use retention_state::{RetentionLease, RetentionState};
use std::collections::{BTreeSet, HashSet};
use std::fs::{self, File, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::FileExt as PositionalFileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt as PositionalFileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use vacuum::VacuumState;

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
    static FAIL_NEXT_AFTER_BLOB_REWRITE_IMAGE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_AFTER_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_CATALOG_RENAME: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_CATALOG_SHORT_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_CATALOG_TORN_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_HISTORY_PRUNE_DIRECTORY_SYNC: Cell<bool> = const { Cell::new(false) };
}

/// File names for the database.
const DATA_FILE: &str = "seerdb.data";
const WAL_FILE: &str = "seerdb.wal";
const WAL_RESERVATION_FILE: &str = "seerdb.wal.reserve";
const META_FILE: &str = "seerdb.meta";
const MANIFEST_FILE: &str = "MANIFEST";
const MANIFEST_HISTORY_FILE: &str = "seerdb.manifest-history";
const REUSE_LEDGER_FILE: &str = "seerdb.reuse-ledger";
const RETENTION_FILE: &str = "seerdb.retained";
const LOCK_FILE: &str = "seerdb.lock";
const ARCHIVE_MARKER_FILE: &str = "seerdb.archive";
const WAL_RESERVATION_SEGMENT_BYTES: u64 = 1024 * 1024;
const WAL_COMMIT_RECORD_BYTES: u64 = (4 + 1 + CommitRecord::SERIALIZED_SIZE + 4) as u64;
const PUBLICATION_CAPACITY_SAFETY_BYTES: u64 = 8 * PAGE_SIZE as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenMode {
    Normal,
    Create,
    Check,
}

/// Blob GC statistics.
pub struct BlobStats {
    /// Number of files needing garbage collection.
    pub files_needing_gc: usize,
    /// Total valid entries across all files.
    pub total_valid: usize,
    /// Total deleted entries across all files.
    pub total_deleted: usize,
    /// Whether segmented catalog deletion metadata has crossed its maintenance
    /// bound and explicit `DB::gc()` should consolidate it.
    pub catalog_needs_consolidation: bool,
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

fn read_exact_at(file: &File, offset: u64, buffer: &mut [u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut filled = 0;
        while filled < buffer.len() {
            let count = file.read_at(&mut buffer[filled..], offset + filled as u64)?;
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "file read reached end of file",
                ));
            }
            filled += count;
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        let mut filled = 0;
        while filled < buffer.len() {
            let count = file.seek_read(&mut buffer[filled..], offset + filled as u64)?;
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "file read reached end of file",
                ));
            }
            filled += count;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let mut cloned = file.try_clone()?;
        cloned.seek(SeekFrom::Start(offset))?;
        cloned.read_exact(buffer)
    }
}

fn decode_u32(bytes: &[u8]) -> Result<u32> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| Error::Corruption("truncated blob integer".into()))?;
    Ok(u32::from_le_bytes(bytes))
}

fn decode_u64(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::Corruption("truncated blob integer".into()))?;
    Ok(u64::from_le_bytes(bytes))
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
    /// Number of active page versions moved out of interior holes.
    pub relocated_pages: u64,
    /// Whether the active manifest was mirrored before truncation.
    pub manifest_replicated: bool,
}

/// Results from a logical mark-and-rebuild vacuum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VacuumReport {
    /// Durable identity after the rebuild generation.
    pub durability: DurabilityStatus,
    /// Number of live key-value entries copied into the new tree.
    pub live_entries: u64,
    /// Active logical page count before rebuilding.
    pub logical_pages_before: u64,
    /// Active logical page count after rebuilding.
    pub logical_pages_after: u64,
}

/// Progress from one bounded logical vacuum step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VacuumProgress {
    /// Durable identity. It changes only when the final step publishes.
    pub durability: DurabilityStatus,
    /// Number of logical entries consumed by the source cursor.
    pub scanned_entries: u64,
    /// Number of live entries copied into the candidate tree.
    pub live_entries: u64,
    /// Active logical page count before rebuilding.
    pub logical_pages_before: u64,
    /// Candidate page count after publication, or `None` while incomplete.
    pub logical_pages_after: Option<u64>,
    /// Whether this step published the rebuilt generation.
    pub complete: bool,
}

/// Results from pruning superseded manifests and checkpoint files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryPruneReport {
    /// Number of generations retained for the current root and snapshots.
    pub retained_generations: u64,
    /// Number of manifest descriptors removed from the history sidecar.
    pub removed_manifests: u64,
    /// Number of superseded checkpoint files removed.
    pub removed_checkpoints: u64,
    /// Bytes reclaimed from removed checkpoint files.
    pub reclaimed_checkpoint_bytes: u64,
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
    /// Cumulative bytes written to publication artifacts by this handle.
    pub publication: PublicationMetrics,
    /// Cumulative wall-clock time spent in publication phases by this handle.
    pub publication_timing: PublicationTimingMetrics,
}

/// Cumulative bytes written to durable publication artifacts.
///
/// These counters measure successful bytes written by the current publication
/// protocol. Metadata may be a full checkpoint or a bounded PMT delta;
/// blob/image and manifest-history counters still describe their current
/// whole-image/append-only artifacts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublicationMetrics {
    /// Bytes appended to the durable WAL by this open handle.
    pub wal_bytes_written: u64,
    /// Bytes written to PMT/allocator checkpoint images.
    pub metadata_bytes_written: u64,
    /// Bytes written to blob images.
    pub blob_bytes_written: u64,
    /// Bytes appended or rewritten in manifest history.
    pub history_bytes_written: u64,
    /// Bytes written to alternating manifest slots.
    pub manifest_bytes_written: u64,
}

/// Cumulative wall-clock time spent in durable publication phases.
///
/// These timings are diagnostic attribution for the current publication
/// protocol. They are not latency guarantees and may include filesystem cache
/// and scheduler effects from the host running the database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublicationTimingMetrics {
    /// Candidate validation, cloning, and in-memory mutation preparation.
    pub candidate_prepare_ns: u64,
    /// WAL append and durability barriers.
    pub wal_write_ns: u64,
    /// Publication capacity checks and reuse-ledger admission.
    pub admission_ns: u64,
    /// Physical page writes and the data-device sync.
    pub data_flush_ns: u64,
    /// PMT/allocator checkpoint writes and syncs.
    pub metadata_write_ns: u64,
    /// Blob image or segmented catalog/append writes and syncs.
    pub blob_write_ns: u64,
    /// Manifest-history append/checkpoint writes and syncs.
    pub history_write_ns: u64,
    /// Final publication-directory durability barrier.
    pub directory_sync_ns: u64,
    /// Authoritative manifest slot write and sync.
    pub manifest_write_ns: u64,
    /// Pre-reuse manifest mirror write and sync.
    pub manifest_mirror_ns: u64,
    /// Post-manifest cleanup and reclamation bookkeeping.
    pub cleanup_ns: u64,
}

fn elapsed_nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
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
    /// In-memory candidate for a resumable logical vacuum.
    vacuum: Option<VacuumState>,
    /// Durable retained-root registry shared with retained snapshot handles.
    retention: Arc<Mutex<RetentionState>>,
    /// Transaction manager for MVCC.
    txn_manager: TransactionManager,
    /// Authoritative root-generation publication store.
    manifest: ManifestStore,
    /// Durable descriptors for historical roots that can be retained later.
    manifest_history: ManifestHistory,
    /// Reuse attempts whose publication outcome may be indeterminate.
    reuse_ledger: ReuseLedger,
    /// Stable database identity.
    database_id: DatabaseId,
    /// Stable logical history identity.
    history_id: HistoryId,
    /// Latest published generation.
    generation_id: GenerationId,
    /// Latest published commit.
    commit_id: CommitId,
    /// Next commit identity reserved for a new logical publication.
    ///
    /// This may be ahead of `commit_id` when a prior publication could have
    /// reached durable WAL or page media but did not become authoritative.
    /// Such an identity is never reused after reopen.
    next_commit_id: CommitId,
    /// Next physical generation identity reserved for a new publication.
    next_generation_id: GenerationId,
    /// Number of mutation records since the last published generation.
    pending_mutations: u64,
    /// Logical WAL bytes admitted for the pending generation.
    pending_wal_bytes: u64,
    /// Physical WAL reservation extent already established for this handle.
    ///
    /// Keep-size reservation is a handle-level high-water mark. Reissuing a
    /// full APFS `F_PREALLOCATE` request on every mutation can eventually
    /// report `ENOSPC` even though the original extent is already reserved.
    wal_reserved_extent: u64,
    /// Digest over pending mutation records.
    pending_digest: u32,
    /// Whether the pending generation changes the durable blob image/catalog.
    pending_blob_changes: bool,
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
    /// Cumulative publication-artifact write counters.
    publication: PublicationMetrics,
    /// Cumulative publication phase timings for diagnostics.
    publication_timing: PublicationTimingMetrics,
}

impl DB {
    /// Open or create a database at the given path.
    pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        Self::open_with_mode(path, options, OpenMode::Normal)
    }

    /// Create a new database and refuse an existing store.
    ///
    /// The final directory is created with an atomic no-replace operation so
    /// callers cannot accidentally attach a logical catalog to an existing
    /// path. Use [`DB::open`] when reopening is intended.
    pub fn create<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        Self::open_with_mode(path, options, OpenMode::Create)
    }

    /// Insert a key-value pair.
    ///
    /// The mutation is applied in memory, journaled durably, and included in
    /// the next published root generation.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
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
        let had_previous_blob = previous_blob.is_some();
        if had_previous_blob || appended_value_len.is_some() {
            self.admit_blob_image(previous_blob.as_ref(), appended_value_len)?;
        }
        let outcome = apply_mutation(
            Mutation::Put { key, value },
            self.engine.btree_mut(),
            &mut self.blobs,
        )?;
        require_blob_deletion(outcome, "put")?;

        self.journal_mutation(record)?;
        self.pending_blob_changes |= outcome.blob_changed;

        Ok(())
    }

    /// Commit multiple byte-key mutations atomically as one durable batch.
    ///
    /// The complete candidate B-tree/blob state is prepared off to the side
    /// before the batch WAL is appended. A validation, capacity, or B-tree
    /// error therefore leaves the current state untouched. Once the WAL batch
    /// is durable, [`DB::flush`] publishes all mutations under one commit
    /// envelope; a failure at that boundary fences the writer for recovery in
    /// the same way as a single mutation.
    pub fn commit_batch(&mut self, mutations: &[BatchMutation]) -> Result<DurabilityStatus> {
        let expected_commit = self.commit_id;
        self.commit_batch_at(expected_commit, mutations)
    }

    /// Commit multiple byte-key mutations only if the published commit still
    /// matches the caller's expected base.
    ///
    /// This is the storage boundary for optimistic transaction adapters. The
    /// expected-base check happens before validation, WAL admission, or any
    /// candidate tree/blob work, so a stale caller has no side effects. An
    /// empty batch is a validated no-op and returns the current durability
    /// status when the expected base matches.
    pub fn commit_batch_at(
        &mut self,
        expected_commit: CommitId,
        mutations: &[BatchMutation],
    ) -> Result<DurabilityStatus> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        if self.commit_id != expected_commit {
            return Err(Error::SerializationConflict {
                expected: expected_commit,
                current: self.commit_id,
            });
        }
        if mutations.is_empty() {
            return Ok(self.durability_status());
        }
        let candidate_started = Instant::now();
        if self.pending_mutations != 0 {
            return Err(Error::InvalidArgument(
                "commit_batch requires a clean pending generation; flush or discard pending mutations first".into(),
            ));
        }

        let mut records = Vec::with_capacity(mutations.len());
        let mut mutation_bytes = 0u64;
        for mutation in mutations {
            let record = match mutation {
                BatchMutation::Put { key, value } => {
                    validate_wal_put_lengths(key, value)?;
                    WalRecord::put(key, value)
                }
                BatchMutation::Delete { key } => {
                    validate_wal_key_length(key)?;
                    WalRecord::delete(key)
                }
            };
            mutation_bytes = mutation_bytes
                .checked_add(record.to_bytes().len() as u64)
                .ok_or(Error::DiskFull)?;
            records.push(record);
        }

        let required_wal = mutation_bytes
            .checked_add(WAL_COMMIT_RECORD_BYTES)
            .ok_or(Error::DiskFull)?;
        let available_wal = self
            .options
            .max_wal_bytes
            .saturating_sub(self.pending_wal_bytes);
        if required_wal > available_wal {
            self.wal_admission_failures = self.wal_admission_failures.saturating_add(1);
            return Err(Error::Backpressure {
                required: required_wal,
                available: available_wal,
            });
        }

        for mutation in mutations {
            let key = match mutation {
                BatchMutation::Put { key, .. } | BatchMutation::Delete { key } => key,
            };
            self.engine.prepare_mutation(key)?;
        }

        let mut candidate_tree = self.engine.btree().clone();
        let mut candidate_blobs = self.blobs.clone();
        let mut blob_changed = false;
        for mutation in mutations {
            let outcome = match mutation {
                BatchMutation::Put { key, value } => apply_mutation(
                    Mutation::Put { key, value },
                    &mut candidate_tree,
                    &mut candidate_blobs,
                )?,
                BatchMutation::Delete { key } => apply_mutation(
                    Mutation::Delete { key },
                    &mut candidate_tree,
                    &mut candidate_blobs,
                )?,
            };
            require_blob_deletion(outcome, "batch mutation")?;
            blob_changed |= outcome.blob_changed;
        }

        if blob_changed {
            let projected = Self::blob_publication_size(&candidate_blobs)?;
            self.engine.check_artifact_capacity(projected)?;
        }
        self.publication_timing.candidate_prepare_ns = self
            .publication_timing
            .candidate_prepare_ns
            .saturating_add(elapsed_nanos(candidate_started));

        self.ensure_wal_reservation()?;
        if blob_changed && !candidate_blobs.is_segmented() {
            let projected = Self::blob_publication_size(&candidate_blobs)?;
            self.reserve_blob_image(projected)?;
        }

        let next_pending_mutations = u64::try_from(mutations.len())
            .ok()
            .and_then(|count| self.pending_mutations.checked_add(count))
            .ok_or(Error::Wal("mutation count overflow".into()))?;
        let next_pending_bytes = self
            .pending_wal_bytes
            .checked_add(mutation_bytes)
            .ok_or(Error::Wal("WAL byte count overflow".into()))?;
        let next_digest = records.iter().fold(self.pending_digest, |digest, record| {
            extend_digest(digest, record)
        });

        for record in &records {
            self.wal.append(record);
        }
        if let Err(error) = self.write_wal_to_disk(self.wal.sync_policy() != SyncPolicy::None) {
            self.write_fenced = true;
            return Err(error);
        }

        *self.engine.btree_mut() = candidate_tree;
        self.blobs = candidate_blobs;
        self.pending_blob_changes = blob_changed;
        self.pending_mutations = next_pending_mutations;
        self.pending_wal_bytes = next_pending_bytes;
        self.pending_digest = next_digest;
        self.flush()?;
        Ok(self.durability_status())
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

    /// Get a value from a retained historical root.
    ///
    /// IDs returned by [`DB::retain_commit`] are durable across reopen. IDs
    /// owned by [`DB::begin_batch_transaction`] are process-local and expire
    /// when the transaction or process ends.
    ///
    /// The page lookup uses the retained PMT over this handle's device and
    /// buffer pool. Blob values resolve through the immutable blob image
    /// captured with the same retention lease.
    pub fn get_at(&self, snapshot_id: SnapshotId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_readable()?;
        let manifest = self.retained_manifest(snapshot_id)?;
        let pmt = self.retained_pmt(manifest)?;
        let result = self.engine.lookup_at(manifest.root_page_id, &pmt, key)?;
        self.lookup_result_value(result, snapshot_id)
    }

    /// Delete a key.
    ///
    /// The tombstone is applied in memory, journaled durably, and included in
    /// the next published root generation.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
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
        let outcome = apply_mutation(
            Mutation::Delete { key },
            self.engine.btree_mut(),
            &mut self.blobs,
        )?;
        require_blob_deletion(outcome, "delete")?;
        self.journal_mutation(record)?;
        self.pending_blob_changes |= outcome.blob_changed;
        Ok(outcome.changed)
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

    /// Scan a range from a retained historical root.
    ///
    /// Named retention IDs are durable across reopen; transaction-owned IDs
    /// are process-local and are not persisted in the named registry.
    pub fn range_at(
        &self,
        snapshot_id: SnapshotId,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_readable()?;
        let manifest = self.retained_manifest(snapshot_id)?;
        let pmt = self.retained_pmt(manifest)?;
        let blob_path = retained_blob_path(&self.path, snapshot_id);
        let blob_bytes = fs::read(&blob_path)?;
        let blobs = BlobManager::from_bytes(&blob_bytes).ok_or_else(|| {
            Error::Corruption(format!(
                "retained snapshot {} has an invalid blob image",
                snapshot_id.get()
            ))
        })?;
        self.engine
            .range_at(manifest.root_page_id, &pmt, start, end)?
            .into_iter()
            .filter_map(|(key, value)| match value {
                LookupResult::Found(value) => Some(Ok((key, value))),
                LookupResult::Blob(pointer) => Some(
                    blobs
                        .read(&pointer)
                        .map(|value| (key, value.to_vec()))
                        .ok_or_else(|| {
                            Error::Corruption(format!(
                                "retained snapshot {} has an invalid blob pointer",
                                snapshot_id.get()
                            ))
                        }),
                ),
                LookupResult::Deleted | LookupResult::NotFound => None,
            })
            .collect()
    }

    fn retained_manifest(&self, snapshot_id: SnapshotId) -> Result<Manifest> {
        let state = self
            .retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
        state
            .all_roots()
            .find(|root| root.snapshot_id == snapshot_id)
            .map(|root| root.manifest)
            .ok_or_else(|| {
                Error::SnapshotUnavailable(format!(
                    "retained root {} is not active",
                    snapshot_id.get()
                ))
            })
    }

    fn retained_pmt(&self, manifest: Manifest) -> Result<PMT> {
        if manifest.pmt_checkpoint_id.get() == 0 {
            return Ok(PMT::new());
        }
        let checkpoint = self
            .path
            .join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
        Self::load_meta(&checkpoint).map(|(pmt, _)| pmt)
    }

    fn lookup_result_value(
        &self,
        result: LookupResult,
        snapshot_id: SnapshotId,
    ) -> Result<Option<Vec<u8>>> {
        match result {
            LookupResult::Found(value) => Ok(Some(value)),
            LookupResult::Blob(pointer) => {
                let blob_path = retained_blob_path(&self.path, snapshot_id);
                let blob_bytes = fs::read(&blob_path)?;
                let blobs = BlobManager::from_bytes(&blob_bytes).ok_or_else(|| {
                    Error::Corruption(format!(
                        "retained snapshot {} has an invalid blob image",
                        snapshot_id.get()
                    ))
                })?;
                blobs
                    .read(&pointer)
                    .map(|value| Some(value.to_vec()))
                    .ok_or_else(|| {
                        Error::Corruption(format!(
                            "retained snapshot {} has an invalid blob pointer",
                            snapshot_id.get()
                        ))
                    })
            }
            LookupResult::Deleted | LookupResult::NotFound => Ok(None),
        }
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
        self.check_maintenance_idle()?;
        if self.pending_mutations == 0 {
            return Ok(());
        }

        let commit = CommitRecord {
            commit_id: self.next_commit_id,
            generation_id: self.next_generation_id,
            root_page_id: self.engine.btree().root_id() as u64,
            mutation_count: self.pending_mutations,
            digest: self.pending_digest,
        };

        if let Err(error) = self.publish_generation(commit, true, 0) {
            // StorageEngine performs capacity admission before any page
            // write. A refusal at that boundary leaves only an uncommitted
            // WAL mutation prefix, so callers can restore capacity and retry
            // without reopening. Any other publication error may have
            // reached durable media and must fence this handle.
            if !matches!(&error, Error::CapacityPreflight) {
                self.write_fenced = true;
            }
            return Err(error);
        }
        Ok(())
    }

    /// Establish and verify a named durable checkpoint barrier.
    ///
    /// A pending WAL generation is published first. When the database is
    /// already clean, this is idempotent: it verifies the existing manifest
    /// and does not invent an extra commit or generation.
    pub fn checkpoint(&mut self) -> Result<VerificationReport> {
        self.check_writable()?;
        self.flush()?;
        self.verify()
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
    /// durable generation never loses a pointer. Fully dead files are removed
    /// directly; mixed files are compacted by rewriting active B-tree pointers
    /// into a new blob file and sweeping the old file after publication.
    ///
    /// Returns the number of entries reclaimed.
    pub fn gc(&mut self) -> Result<usize> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        self.flush()?;
        if !self
            .retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?
            .is_empty()
        {
            // The current retained read view is copy-backed, but retaining the
            // root still establishes the conservative physical contract. Do
            // not delete blob history until the lease is released.
            return Ok(0);
        }
        if self.blobs.has_reclaimable_files() {
            // Fully-dead file removal changes the active blob image without
            // publishing a new root. Fence the inactive manifest slot first;
            // otherwise it could still name records removed below and become
            // an invalid fallback after a torn newest-slot read.
            self.mirror_current_manifest()?;
            // Admission must precede removal from the in-memory catalog. The
            // current image is an upper bound for the compacted image, so a
            // successful reservation covers the subsequent atomic publish.
            self.admit_blob_image(None, None)?;
        }
        let mut reclaimed = self.blobs.gc();
        if reclaimed > 0 {
            let write_result = if self.blobs.is_segmented() {
                self.publish_blob_rewrite_generation().map(|()| 0)
            } else {
                let blob_path = self.path.join(BLOB_FILE);
                let blob_image = self.blobs.to_bytes();
                self.write_blob_image(&blob_path, &blob_image)
            };
            match write_result {
                Ok(bytes) => {
                    self.publication.blob_bytes_written =
                        self.publication.blob_bytes_written.saturating_add(bytes);
                }
                Err(error) => {
                    self.write_fenced = true;
                    return Err(error);
                }
            }
        }
        if !self.blobs.files_needing_gc().is_empty()
            || segmented_catalog_needs_consolidation(&self.blobs)
        {
            let rewritten = match self.rewrite_mixed_blob_files() {
                Ok(reclaimed) => reclaimed,
                Err(error) if matches!(&error, Error::CapacityPreflight) => return Err(error),
                Err(error) => {
                    self.write_fenced = true;
                    return Err(error);
                }
            };
            reclaimed = reclaimed.saturating_add(rewritten);
        }
        Ok(reclaimed)
    }

    /// Rewrite live blob values into a fresh file and publish their new
    /// pointers as one physical maintenance generation.
    ///
    /// Existing records remain in the candidate blob image but carry deletion
    /// metadata until the new manifest is durable. The prior blob image is
    /// kept under a recovery name across that boundary, so an interrupted
    /// rewrite restores the exact old root image. Once the new root is
    /// authoritative, a second sweep removes the fully dead old files without
    /// changing the logical tree again.
    fn rewrite_mixed_blob_files(&mut self) -> Result<usize> {
        self.engine.ensure_materialized()?;
        let end = vec![u8::MAX; MAX_KEY_SIZE + 1];
        let scan = self
            .engine
            .btree()
            .range_scan(&[], &end)
            .map_err(Error::from)?;

        let mut candidate_blobs = self.blobs.clone();
        candidate_blobs
            .begin_compaction_file()
            .ok_or(Error::DiskFull)?;
        candidate_blobs.mark_all_deleted();
        let mut candidate_tree = self.engine.btree().clone();
        let mut rewritten = 0usize;
        for entry in scan {
            let (key, result) = entry.map_err(Error::from)?;
            let LookupResult::Blob(pointer) = result else {
                continue;
            };
            let value = self.blobs.read(&pointer).ok_or_else(|| {
                Error::Corruption(format!(
                    "active B-tree blob pointer {}:{}:{} is unavailable",
                    pointer.file_id, pointer.offset, pointer.length
                ))
            })?;
            let replacement = candidate_blobs.append(&key, value.to_vec());
            candidate_tree
                .upsert_blob(&key, replacement)
                .map_err(Error::from)?;
            rewritten = rewritten.saturating_add(1);
        }
        if rewritten == 0 {
            return Ok(0);
        }

        let blob_bytes = Self::blob_publication_size(&candidate_blobs)?;
        let candidate_page_count = candidate_tree
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| candidate_tree.node(*page_id).is_some())
            .count();
        let candidate_data_bytes = u64::try_from(candidate_page_count)
            .map_err(|_| Error::DiskFull)?
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(Error::DiskFull)?;
        let metadata_bytes = Self::max_metadata_delta_bytes(
            candidate_tree.node_count(),
            candidate_tree.page_allocator(),
        )?;
        self.preflight_maintenance_capacity(candidate_data_bytes, metadata_bytes, blob_bytes)?;
        self.engine.check_artifact_capacity(blob_bytes)?;
        self.engine.preflight_rebuild_capacity(&candidate_tree)?;
        if !candidate_blobs.is_segmented() {
            self.reserve_blob_image(blob_bytes)?;
        }
        *self.engine.btree_mut() = candidate_tree;
        self.blobs = candidate_blobs;

        self.mirror_current_manifest()?;
        self.engine.flush()?;
        self.publish_blob_rewrite_generation()?;

        let reclaimed = if self.blobs.has_reclaimable_files() {
            self.admit_blob_image(None, None)?;
            let reclaimed = self.blobs.gc();
            if reclaimed > 0 {
                let blob_bytes = if self.blobs.is_segmented() {
                    self.publish_blob_rewrite_generation()?;
                    0
                } else {
                    let blob_path = self.path.join(BLOB_FILE);
                    let blob_image = self.blobs.to_bytes();
                    self.write_blob_image(&blob_path, &blob_image)?
                };
                self.publication.blob_bytes_written = self
                    .publication
                    .blob_bytes_written
                    .saturating_add(blob_bytes);
            }
            reclaimed
        } else {
            0
        };
        Ok(reclaimed)
    }

    /// Get blob GC statistics.
    pub fn blob_stats(&self) -> BlobStats {
        BlobStats {
            files_needing_gc: self.blobs.files_needing_gc().len(),
            total_valid: self.blobs.total_valid_entries(),
            total_deleted: self.blobs.total_deleted_entries(),
            catalog_needs_consolidation: segmented_catalog_needs_consolidation(&self.blobs),
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
            blob_bytes: blob_storage_size(&self.path)?,
            wal_bytes: artifact_size(WAL_FILE)?,
            wal_reserved_bytes: self.wal_reserved_extent,
            reclaimable_pages: self.engine.reclaimable_page_count() as u64,
            publication: self.publication,
            publication_timing: self.publication_timing,
        })
    }

    /// Remove historical manifests and PMT checkpoints that are not needed
    /// by the current root or a durable retained snapshot.
    ///
    /// The history sidecar is atomically replaced before any checkpoint is
    /// deleted. A crash during cleanup therefore leaves harmless extra files,
    /// never a history entry that names a missing checkpoint.
    pub fn prune_history(&mut self) -> Result<HistoryPruneReport> {
        self.check_writable()?;
        self.flush()?;
        let recovery_manifests = self.manifest.load_valid_manifests()?;
        let current = recovery_manifests
            .iter()
            .copied()
            .max_by_key(|manifest| (manifest.generation_id, manifest.commit_id))
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;

        // Both valid slots are recovery roots until a later publication has
        // mirrored the current manifest. Pruning only from `current` can
        // delete the checkpoint needed by the inactive fallback slot, turning
        // a later torn newest-slot recovery into a missing-artifact failure.
        let mut retained = recovery_manifests
            .iter()
            .map(|manifest| manifest.generation_id)
            .collect::<BTreeSet<_>>();
        retained.insert(current.generation_id);
        let state = self
            .retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
        let retained_roots = state
            .all_roots()
            .map(|root| root.manifest)
            .collect::<Vec<_>>();
        retained.extend(retained_roots.iter().map(|manifest| manifest.generation_id));
        drop(state);

        let mut history = self.manifest_history.clone();
        history
            .reconcile_current(current)
            .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        let mut retained_checkpoints = BTreeSet::new();
        let mut protected_manifests = recovery_manifests;
        protected_manifests.extend(retained_roots);
        protected_manifests.extend(
            history
                .manifests()
                .iter()
                .copied()
                .filter(|manifest| retained.contains(&manifest.generation_id)),
        );
        for manifest in protected_manifests {
            if manifest.pmt_checkpoint_id.get() != 0 {
                retained_checkpoints.extend(Self::load_meta_ancestors(
                    &self.path,
                    manifest.pmt_checkpoint_id.get(),
                )?);
            }
        }
        let removed_manifests = history.prune_to_generations(&retained) as u64;
        if history != self.manifest_history {
            self.persist_manifest_history(&history)?;
            self.manifest_history = history;
        }

        let mut removed_checkpoints = 0u64;
        let mut reclaimed_checkpoint_bytes = 0u64;
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(generation) = name
                .to_str()
                .and_then(|name| name.strip_prefix("seerdb.meta."))
                .and_then(|suffix| suffix.parse::<u64>().ok())
            else {
                continue;
            };
            if retained_checkpoints.contains(&generation) {
                continue;
            }
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                continue;
            }
            fs::remove_file(entry.path())?;
            removed_checkpoints = removed_checkpoints.saturating_add(1);
            reclaimed_checkpoint_bytes = reclaimed_checkpoint_bytes.saturating_add(metadata.len());
        }
        if removed_checkpoints > 0 {
            sync_history_prune_directory(&self.path)?;
        }

        Ok(HistoryPruneReport {
            retained_generations: retained.len() as u64,
            removed_manifests,
            removed_checkpoints,
            reclaimed_checkpoint_bytes,
        })
    }

    /// Publish a blob-pointer rewrite without inventing a logical user
    /// commit. The data pages, PMT, and blob image are all selected by the new
    /// physical generation before its manifest becomes authoritative.
    fn publish_blob_rewrite_generation(&mut self) -> Result<()> {
        let current = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
        let generation_id = self.next_generation_id;
        let checkpoint_path = self
            .path
            .join(format!("seerdb.meta.{}", generation_id.get()));
        let checkpoint_bytes = self.save_generation_meta(&checkpoint_path, current)?;
        let meta_path = self.path.join(META_FILE);
        let legacy_meta_bytes = if meta_path.is_file() {
            0
        } else {
            Self::save_meta(&meta_path, self.engine.pmt(), self.engine.allocator())?
        };
        self.publication.metadata_bytes_written = self
            .publication
            .metadata_bytes_written
            .saturating_add(checkpoint_bytes)
            .saturating_add(legacy_meta_bytes);

        self.blobs.set_generation(generation_id.get());
        let blob_path = self.path.join(BLOB_FILE);
        let backup_path = self.path.join(BLOB_REWRITE_BACKUP_FILE);
        let had_blob_image = blob_path.is_file();
        if backup_path.exists() {
            fs::remove_file(&backup_path)?;
        }
        if had_blob_image {
            fs::rename(&blob_path, &backup_path)?;
            sync_directory(&self.path)?;
        }
        let blob_bytes = if self.blobs.is_segmented() {
            self.write_blob_segments()?
        } else {
            let blob_image = self.blobs.to_bytes();
            self.write_blob_image(&blob_path, &blob_image)?
        };
        self.publication.blob_bytes_written = self
            .publication
            .blob_bytes_written
            .saturating_add(blob_bytes);

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_AFTER_BLOB_REWRITE_IMAGE.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected failure after blob rewrite image").into());
        }

        let manifest = Manifest {
            generation_id,
            pmt_checkpoint_id: PmtCheckpointId::new(generation_id.get()),
            root_page_id: self.engine.btree().root_id() as u64,
            ..current
        };
        let mut manifest_history = self.manifest_history.clone();
        manifest_history
            .push(manifest)
            .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        let history_bytes = if self.path.join(MANIFEST_HISTORY_FILE).is_file() {
            self.append_manifest_history(manifest)?
        } else {
            let bytes = manifest_history
                .to_bytes()
                .ok_or_else(|| Error::Wal("manifest history is too large".into()))?;
            self.persist_manifest_history(&manifest_history)?;
            bytes.len() as u64
        };
        self.publication.history_bytes_written = self
            .publication
            .history_bytes_written
            .saturating_add(history_bytes);
        self.manifest_history = manifest_history;
        self.manifest.publish(manifest)?;
        self.publication.manifest_bytes_written = self
            .publication
            .manifest_bytes_written
            .saturating_add(MANIFEST_SLOT_SIZE as u64);
        if self.blobs.is_segmented() {
            self.prune_unreferenced_blob_segments()?;
            self.finish_segment_catalog_backup()?;
        }
        self.engine.complete_generation();
        self.generation_id = generation_id;
        self.next_generation_id = GenerationId::new(
            generation_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        if !self.blobs.is_segmented() && had_blob_image {
            fs::remove_file(&backup_path)?;
            sync_directory(&self.path)?;
        }
        Ok(())
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

    /// Inject one failure after a complete page write and before publication.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_after_write_failure(&self) {
        self.engine.inject_after_write_failure();
    }

    /// Inject one failure after the complete page generation is written but
    /// before its device durability sync.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_page_range_sync_failure(&self) {
        self.engine.inject_page_range_sync_failure();
    }

    /// Inject one final-write ENOSPC after a page write may have completed.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_final_write_disk_full(&self) {
        self.engine.inject_final_write_disk_full();
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

    /// Inject one failure at the safety mirror sync boundary before page reuse.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_manifest_mirror_sync_failure(&self) {
        self.manifest.inject_mirror_sync_failure();
    }

    /// Inject one failure at the coalesced artifact-directory barrier before
    /// the next user manifest publication.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_publication_directory_sync_failure(&self) {
        FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one final directory-sync failure after history pruning removes
    /// obsolete checkpoint files. The active manifest remains authoritative;
    /// reopen should accept the pruned or unpruned directory state.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_history_prune_directory_sync_failure(&self) {
        FAIL_NEXT_HISTORY_PRUNE_DIRECTORY_SYNC.with(|failure| failure.set(true));
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

    /// Inject one failure after a mixed-blob rewrite image is durable but
    /// before its maintenance manifest is published.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_after_blob_rewrite_image_failure(&self) {
        FAIL_NEXT_AFTER_BLOB_REWRITE_IMAGE.with(|failure| failure.set(true));
    }

    /// Inject one failure after a segmented blob suffix is durable but before
    /// its catalog is published.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_after_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_AFTER_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure while syncing a segmented blob suffix.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_sync_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure while syncing a segmented blob catalog temp file.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_catalog_sync_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure after a segmented blob catalog temp file is synced
    /// but before it replaces the previous catalog.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_catalog_rename_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_RENAME.with(|failure| failure.set(true));
    }

    /// Inject one truncated segmented blob catalog image before manifest
    /// publication.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_catalog_short_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_SHORT_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one checksum-corrupted segmented blob catalog image before
    /// manifest publication.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_catalog_torn_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_TORN_WRITE.with(|failure| failure.set(true));
    }

    /// Begin a root-bound byte transaction.
    ///
    /// The transaction captures the current published commit and retains its
    /// physical root in a process-local lease before returning. It can
    /// therefore read a stable version while the database advances and can
    /// commit only against that expected base. The short-lived transaction pin
    /// is intentionally not persisted as a named historical snapshot.
    pub fn begin_batch_transaction(&mut self) -> Result<BatchTransaction> {
        self.check_writable()?;
        self.flush()?;
        let manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        let snapshot_id = self.register_ephemeral_manifest(manifest)?;
        Ok(BatchTransaction {
            base_commit: self.commit_id,
            snapshot_id,
            lease: Some(RetentionLease {
                state: Arc::clone(&self.retention),
                snapshot_id,
                reclamation_dirty: self.engine.reclamation_dirty_handle(),
                released: false,
            }),
            mutations: Vec::new(),
            state: BatchTransactionState::Active,
        })
    }

    /// Begin the legacy transaction-ID bookkeeping primitive.
    ///
    /// This does not bind reads or writes to a durable SeerDB root. Use
    /// [`DB::begin_batch_transaction`] for the data-bearing transaction API.
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

    fn check_maintenance_idle(&self) -> Result<()> {
        if self.vacuum.is_some() {
            return Err(Error::MaintenanceInProgress("logical vacuum"));
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

    fn bootstrap_manifest(&self) -> Manifest {
        Manifest {
            database_id: self.database_id,
            history_id: self.history_id,
            generation_id: GenerationId::new(0),
            commit_id: CommitId::new(0),
            page_size: PAGE_SIZE as u32,
            root_page_id: self.engine.btree().root_id() as u64,
            pmt_checkpoint_id: PmtCheckpointId::new(0),
            wal_segment: 0,
            wal_offset: 0,
            mutation_count: 0,
            digest: 0,
            format_version: FORMAT_VERSION,
        }
    }
}

/// Recovery result for the committed WAL prefix.
#[derive(Debug, Clone, Copy)]
struct RecoverySummary {
    last_commit: Option<CommitRecord>,
    last_commit_offset: u64,
    blob_changed: bool,
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
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::io::{Seek, SeekFrom, Write};
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    use std::os::unix::fs::MetadataExt;
    use std::process::Command;
    use tempfile::tempdir;

    use crate::storage::format::MANIFEST_SLOT_SIZE;

    const TEST_SEGMENT_CATALOG_DELTA_LIMIT: u32 = 64;

    #[test]
    fn test_db_open() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path().join("test.db"), Options::default());
        assert!(db.is_ok());
    }

    #[test]
    fn test_db_open_rejects_existing_directory_without_storage_artifacts() {
        let dir = tempdir().unwrap();
        let empty_path = dir.path().join("empty.db");
        fs::create_dir(&empty_path).unwrap();
        assert!(matches!(
            DB::open(&empty_path, Options::default()),
            Err(Error::Corruption(message))
                if message.contains("no authoritative storage artifacts")
        ));

        let orphan_path = dir.path().join("orphan.db");
        fs::create_dir(&orphan_path).unwrap();
        fs::write(orphan_path.join(BLOB_FILE), b"orphaned blob image").unwrap();
        assert!(matches!(
            DB::open(&orphan_path, Options::default()),
            Err(Error::Corruption(message))
                if message.contains("no authoritative storage artifacts")
        ));
    }

    #[test]
    fn test_db_open_rejects_missing_manifest_artifacts_without_recreating_them() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing-data.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
        drop(db);

        fs::remove_file(path.join(DATA_FILE)).unwrap();
        let check = DB::check(&path, Options::default());
        assert!(
            matches!(
                check,
                Err(Error::Check {
                    kind: CheckFailureKind::Target,
                    ref message
                }) if message.contains("required manifest or data artifacts")
            ),
            "check error: {check:?}"
        );
        assert!(matches!(
            DB::open(&path, Options::default()),
            Err(Error::Corruption(message))
                if message.contains("is missing the data file")
        ));
        assert!(!path.join(DATA_FILE).exists());

        let checkpoint_path = dir.path().join("missing-checkpoint.db");
        let mut db = DB::open(&checkpoint_path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
        drop(db);
        let checkpoint = fs::read_dir(&checkpoint_path)
            .unwrap()
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_prefix("seerdb.meta."))
                    .and_then(|suffix| suffix.parse::<u64>().ok())
                    .is_some()
            })
            .expect("published database has a numbered PMT checkpoint");
        fs::remove_file(&checkpoint).unwrap();

        let check = DB::check(&checkpoint_path, Options::default());
        assert!(matches!(
            check,
            Err(Error::Check {
                kind: CheckFailureKind::Checkpoint,
                ref message
            }) if message.contains("is missing checkpoint")
        ));
        assert!(matches!(
            DB::open(&checkpoint_path, Options::default()),
            Err(Error::Corruption(message))
                if message.contains("is missing checkpoint")
        ));
        assert!(!checkpoint.exists());
    }

    #[test]
    fn test_db_create_refuses_existing_store_without_reinterpreting_it() {
        let dir = tempdir().unwrap();
        let reserved_path = dir.path().join("reserved.db");
        fs::create_dir(&reserved_path).unwrap();
        assert!(matches!(
            DB::create(&reserved_path, Options::default()),
            Err(Error::InvalidArgument(message)) if message.contains("already exists")
        ));

        let path = dir.path().join("nested").join("created.db");
        let mut db = DB::create(&path, Options::default()).unwrap();
        db.put(b"catalog", b"durable").unwrap();
        db.flush().unwrap();
        drop(db);

        assert!(matches!(
            DB::create(&path, Options::default()),
            Err(Error::InvalidArgument(message)) if message.contains("already exists")
        ));
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"catalog").unwrap(), Some(b"durable".to_vec()));
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
    fn test_db_rejects_key_larger_than_page_format_before_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("oversized-key.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        let key = vec![0xA5; MAX_KEY_SIZE + 1];

        assert!(matches!(
            db.put(&key, b"value"),
            Err(Error::InvalidArgument(message))
                if message.contains("maximum B-tree page key size")
        ));
        assert_eq!(db.durability_status().pending_mutations, 0);
        assert!(!path.join(WAL_FILE).exists());
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
            assert!(metrics.publication.wal_bytes_written > 0);
            assert!(metrics.publication.metadata_bytes_written > 0);
            assert_eq!(metrics.publication.blob_bytes_written, 0);
            assert!(metrics.publication.history_bytes_written > 0);
            assert_eq!(
                metrics.publication.manifest_bytes_written,
                MANIFEST_SLOT_SIZE as u64
            );
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
    fn test_db_metadata_delta_reopens_and_preserves_parent_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metadata-delta.db");

        let snapshot_id;
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            for index in 0..200 {
                db.put(
                    format!("key-{index:04}").as_bytes(),
                    format!("value-{index:04}").as_bytes(),
                )
                .unwrap();
            }
            db.flush().unwrap();
            let first = db.durability_status();
            snapshot_id = db.retain_commit(first.commit_id).unwrap();
            db.put(b"key-0000", b"updated-value").unwrap();
            db.flush().unwrap();
            assert_eq!(
                db.get(b"key-0000").unwrap(),
                Some(b"updated-value".to_vec())
            );
            assert_eq!(
                db.get_at(snapshot_id, b"key-0000").unwrap(),
                Some(b"value-0000".to_vec())
            );
        }

        let first_checkpoint = path.join("seerdb.meta.1");
        let second_checkpoint = path.join("seerdb.meta.2");
        let first_bytes = fs::read(&first_checkpoint).unwrap();
        let second_bytes = fs::read(&second_checkpoint).unwrap();
        assert!(first_bytes.starts_with(&META_MAGIC));
        assert!(second_bytes.starts_with(&META_DELTA_MAGIC));
        assert!(second_bytes.len() < first_bytes.len());

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(
            reopened.get(b"key-0000").unwrap(),
            Some(b"updated-value".to_vec())
        );
        assert_eq!(
            reopened.get_at(snapshot_id, b"key-0000").unwrap(),
            Some(b"value-0000".to_vec())
        );
        reopened.release_snapshot(snapshot_id).unwrap();
        reopened.prune_history().unwrap();
        assert!(first_checkpoint.is_file());
        assert!(second_checkpoint.is_file());
    }

    #[test]
    fn test_db_metadata_delta_corruption_fails_closed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt-metadata-delta.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();
        drop(db);

        let checkpoint = path.join("seerdb.meta.2");
        let mut bytes = fs::read(&checkpoint).unwrap();
        assert!(bytes.starts_with(&META_DELTA_MAGIC));
        let valid_delta = bytes.clone();
        bytes.push(0xA5);
        fs::write(&checkpoint, bytes).unwrap();
        assert!(matches!(
            DB::open(&path, Options::default()),
            Err(Error::Corruption(message)) if message.contains("metadata delta")
        ));
        assert!(matches!(
            DB::check(&path, Options::default()),
            Err(Error::Check {
                kind: CheckFailureKind::Checkpoint,
                ..
            })
        ));

        let mut anchorless = valid_delta;
        anchorless[12..20].fill(0);
        let checksum = crc32c::crc32c(&anchorless[..anchorless.len() - 4]);
        let checksum_offset = anchorless.len() - 4;
        anchorless[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
        fs::write(&checkpoint, anchorless).unwrap();
        let error = match DB::open(&path, Options::default()) {
            Ok(_) => panic!("anchorless metadata delta unexpectedly opened"),
            Err(error) => error,
        };
        assert!(
            matches!(error, Error::Corruption(ref message) if message.contains("no full checkpoint parent")),
            "{error:?}"
        );
    }

    #[test]
    fn test_db_metadata_delta_chain_consolidates_at_hard_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metadata-delta-chain.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-0").unwrap();
        db.flush().unwrap();

        for revision in 1..=MAX_META_DELTA_CHAIN + 1 {
            db.put(b"key", format!("value-{revision}").as_bytes())
                .unwrap();
            db.flush().unwrap();
        }
        let consolidation_generation = MAX_META_DELTA_CHAIN as u64 + 2;
        let consolidated = path.join(format!("seerdb.meta.{consolidation_generation}"));
        assert!(fs::read(&consolidated).unwrap().starts_with(&META_MAGIC));
        // The inactive slot still names the immediately previous delta
        // frontier. Publish one more generation so that fallback advances
        // beyond the consolidation before pruning the old chain.
        db.put(b"key", b"value-66").unwrap();
        db.flush().unwrap();
        let report = db.prune_history().unwrap();
        assert_eq!(report.removed_checkpoints, MAX_META_DELTA_CHAIN as u64 + 1);
        assert!(consolidated.is_file());
        assert_eq!(db.get(b"key").unwrap(), Some(b"value-66".to_vec()));
        db.close().unwrap();
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-66".to_vec()));
    }

    #[test]
    fn test_compaction_after_metadata_delta_admits_relocation_sidecar() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metadata-delta-compaction.db");
        let mut db = DB::open(&path, Options::default()).unwrap();

        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();
        assert!(
            fs::read(path.join("seerdb.meta.2"))
                .unwrap()
                .starts_with(&META_DELTA_MAGIC)
        );

        let report = db.compact().unwrap();
        assert_eq!(report.relocated_pages, 1);
        assert!(
            fs::read(path.join("seerdb.meta.3"))
                .unwrap()
                .starts_with(&META_DELTA_MAGIC)
        );
        assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_compaction_final_write_disk_full_reopens_old_root() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("compaction-final-disk-full.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();

        db.inject_final_write_disk_full();
        assert!(matches!(db.compact(), Err(Error::DiskFull)));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        reopened.verify().unwrap();
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
        assert!(path.join(WAL_FILE).is_file());
        assert!(!path.join(WAL_RESERVATION_FILE).exists());
        assert!(fs::metadata(path.join(WAL_FILE)).unwrap().len() < WAL_RESERVATION_SEGMENT_BYTES);
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
            fs::metadata(path.join(WAL_FILE)).unwrap().blocks() > 0,
            "WAL file should own physical blocks on this platform"
        );
        assert_eq!(
            db.metrics().unwrap().wal_reserved_bytes,
            WAL_RESERVATION_SEGMENT_BYTES
        );
        db.flush().unwrap();
        assert!(!path.join(BLOB_RESERVATION_FILE).exists());
        assert!(!path.join(WAL_FILE).exists());
        assert_eq!(db.metrics().unwrap().wal_reserved_bytes, 0);
        drop(db);

        let reopened = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(value));
    }

    #[test]
    fn test_db_removes_legacy_wal_reservation_sidecar_on_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy-wal-reservation.db");
        let db = DB::open(&path, Options::default()).unwrap();
        drop(db);
        fs::write(path.join(WAL_RESERVATION_FILE), [0xA5; 4096]).unwrap();

        let db = DB::open(&path, Options::default()).unwrap();
        assert!(!path.join(WAL_RESERVATION_FILE).exists());
        assert_eq!(db.metrics().unwrap().wal_reserved_bytes, 0);
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
        assert_eq!(
            reopened.get(b"key-000450").unwrap(),
            Some(b"updated-450".to_vec())
        );
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
        assert_eq!(
            values[0],
            (b"key-000250-new-000".to_vec(), b"new-value".to_vec())
        );
        assert_eq!(values[119].0, b"key-000250-new-119");
    }

    #[test]
    fn test_db_sparse_deep_internal_split_does_not_require_unloaded_children() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sparse-deep-split.db");

        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            for index in 0..2_000 {
                let key = format!("key-{index:06}");
                db.put(key.as_bytes(), b"value").unwrap();
            }
            db.flush().unwrap();
        }

        let mut db = DB::open(&path, Options::default()).unwrap();
        for index in 0..600 {
            let key = format!("key-000800-new-{index:04}");
            db.put(key.as_bytes(), b"new-value").unwrap();
        }
        db.flush().unwrap();
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(
            reopened
                .range(b"key-000800-new-0000", b"key-000800-new-0600")
                .unwrap()
                .len(),
            600
        );
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
            Err(Error::Check {
                kind: CheckFailureKind::DataPage,
                ..
            })
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
    fn test_db_capacity_preflight_is_retryable_without_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("retryable-capacity.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();

        let capacity = fs::metadata(path.join(DATA_FILE)).unwrap().len();
        db.inject_capacity_limit(capacity);
        db.put(b"key", b"value-2").unwrap();

        assert!(matches!(db.flush(), Err(Error::CapacityPreflight)));
        assert!(!db.durability_status().write_fenced);
        assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));

        db.inject_capacity_limit(u64::MAX);
        db.flush().unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));

        drop(db);
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
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
        let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
        manifest_file
            .seek(SeekFrom::Start(MANIFEST_SLOT_SIZE as u64))
            .unwrap();
        manifest_file
            .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
            .unwrap();
        manifest_file.sync_all().unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    }

    #[test]
    fn test_db_prune_history_preserves_inactive_manifest_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("prune-fallback.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();

        let first_checkpoint = path.join("seerdb.meta.1");
        assert!(first_checkpoint.is_file());
        db.prune_history().unwrap();
        assert!(first_checkpoint.is_file());
        db.close().unwrap();

        // The newest slot is corrupt, so reopen must use the independently
        // valid older slot whose checkpoint pruning was required to preserve.
        let manifest_path = path.join(MANIFEST_FILE);
        let manifest_file = OpenOptions::new().read(true).open(&manifest_path).unwrap();
        let mut newest = None;
        for slot in 0..2 {
            let mut bytes = [0; MANIFEST_SLOT_SIZE];
            read_exact_at(
                &manifest_file,
                (slot * MANIFEST_SLOT_SIZE) as u64,
                &mut bytes,
            )
            .unwrap();
            if let Some(manifest) = Manifest::from_bytes(&bytes).unwrap()
                && newest.is_none_or(|(_, current)| manifest.is_newer_than(current))
            {
                newest = Some((slot, manifest));
            }
        }
        let newest_slot = newest.expect("published database has a newest manifest").0;
        drop(manifest_file);
        let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
        manifest_file
            .seek(SeekFrom::Start((newest_slot * MANIFEST_SLOT_SIZE) as u64))
            .unwrap();
        manifest_file
            .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
            .unwrap();
        manifest_file.sync_all().unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
    }

    #[test]
    fn test_db_history_prune_directory_failure_reopens_and_retries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("prune-directory-failure.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-0").unwrap();
        db.flush().unwrap();
        for revision in 1..=MAX_META_DELTA_CHAIN + 1 {
            db.put(b"key", format!("value-{revision}").as_bytes())
                .unwrap();
            db.flush().unwrap();
        }
        db.put(b"key", b"value-final").unwrap();
        db.flush().unwrap();
        let obsolete_checkpoint = path.join("seerdb.meta.1");
        assert!(obsolete_checkpoint.is_file());

        db.inject_history_prune_directory_sync_failure();
        assert!(matches!(db.prune_history(), Err(Error::Io(_))));
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-final".to_vec()));
        reopened.verify().unwrap();
        let report = reopened.prune_history().unwrap();
        assert_eq!(report.removed_checkpoints, 0);
        reopened.close().unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-final".to_vec()));
        assert!(!obsolete_checkpoint.is_file());
    }

    #[test]
    fn test_db_gc_mirrors_manifest_before_removing_dead_blob_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("gc-fallback.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"blob", &vec![0xA5; 2_000]).unwrap();
        db.flush().unwrap();
        db.delete(b"blob").unwrap();
        db.flush().unwrap();

        assert_eq!(db.gc().unwrap(), 1);
        db.close().unwrap();

        // GC is a maintenance mutation of the blob artifact without a new
        // logical commit. Losing the newest slot must still reopen the
        // manifest whose blob image is present, rather than a stale root
        // whose record was just removed.
        let manifest_path = path.join(MANIFEST_FILE);
        let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
        manifest_file
            .seek(SeekFrom::Start(MANIFEST_SLOT_SIZE as u64))
            .unwrap();
        manifest_file
            .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
            .unwrap();
        manifest_file.sync_all().unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"blob").unwrap(), None);
    }

    #[test]
    fn test_db_mirror_manifest_sync_failure_precedes_page_reuse() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("manifest-mirror-fault.db");
        let mut db = DB::open(&path, Options::default()).unwrap();

        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();
        let data_bytes_before = fs::metadata(path.join(DATA_FILE)).unwrap().len();

        db.put(b"key", b"value-3").unwrap();
        db.inject_manifest_mirror_sync_failure();
        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert_eq!(
            fs::metadata(path.join(DATA_FILE)).unwrap().len(),
            data_bytes_before
        );
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_append_only_publication_skips_manifest_mirror() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("append-only-no-mirror.db");
        let mut db = DB::open(&path, Options::default()).unwrap();

        db.put(b"key", b"value").unwrap();
        db.inject_manifest_mirror_sync_failure();
        db.flush().unwrap();

        assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
        db.verify().unwrap();
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
    fn test_db_vacuum_step_is_bounded_and_crash_safe() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bounded-vacuum.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        for index in 0..12 {
            let key = format!("key-{index:02}");
            db.put(key.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();
        let before = db.durability_status();

        let progress = db.vacuum_step(3).unwrap();
        assert!(!progress.complete);
        assert_eq!(progress.scanned_entries, 3);
        assert_eq!(progress.live_entries, 3);
        assert_eq!(progress.logical_pages_after, None);
        assert_eq!(db.durability_status(), before);
        assert!(matches!(
            db.put(b"blocked", b"write"),
            Err(Error::MaintenanceInProgress("logical vacuum"))
        ));

        // Dropping an incomplete candidate must not publish or fence the old
        // generation. The drop path also exercises the close-time retry.
        drop(db);
        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key-00").unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.durability_status(), before);

        let mut completed = false;
        while !completed {
            let progress = reopened.vacuum_step(2).unwrap();
            completed = progress.complete;
            if completed {
                assert_eq!(progress.live_entries, 12);
                assert_eq!(progress.logical_pages_after, Some(1));
            } else {
                assert_eq!(progress.logical_pages_after, None);
            }
        }
        assert_eq!(reopened.range(b"key-00", b"key-99").unwrap().len(), 12);
        reopened.close().unwrap();

        let verified = DB::open(&path, Options::default()).unwrap();
        assert_eq!(verified.range(b"key-00", b"key-99").unwrap().len(), 12);
    }

    #[test]
    fn test_db_vacuum_can_be_cancelled_without_publication() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cancel-vacuum.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        for index in 0..4 {
            let key = format!("key-{index}");
            db.put(key.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();
        let before = db.durability_status();

        assert!(!db.vacuum_step(1).unwrap().complete);
        assert!(db.cancel_vacuum().unwrap());
        assert!(!db.cancel_vacuum().unwrap());
        assert_eq!(db.durability_status(), before);
        db.put(b"after-cancel", b"value").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"after-cancel").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_db_vacuum_final_write_disk_full_reopens_old_root() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("vacuum-final-disk-full.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();

        db.inject_final_write_disk_full();
        assert!(matches!(db.vacuum(), Err(Error::DiskFull)));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
        reopened.verify().unwrap();
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
        assert_eq!(reclaimed, 3);
        assert_eq!(db.get(b"key3").unwrap(), Some(large_value));

        // Check stats after GC.
        let stats = db.blob_stats();
        assert_eq!(stats.total_valid, 1);
        assert_eq!(stats.total_deleted, 0);
        assert_eq!(stats.files_needing_gc, 0);

        db.delete(b"key3").unwrap();
        db.flush().unwrap();
        assert_eq!(db.gc().unwrap(), 1);
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

    #[test]
    fn test_db_mixed_gc_capacity_refusal_is_retryable_before_candidate_install() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mixed-gc-admission.db");
        let value = vec![0xCD; 2_000];
        let mut db = DB::open(&path, Options::default()).unwrap();

        for key in [b"live".as_slice(), b"dead-1", b"dead-2"] {
            db.put(key, &value).unwrap();
        }
        db.flush().unwrap();
        db.delete(b"dead-1").unwrap();
        db.delete(b"dead-2").unwrap();
        db.flush().unwrap();
        let before = db.blob_stats();
        assert_eq!(before.total_valid, 1);
        assert_eq!(before.total_deleted, 2);
        assert_eq!(before.files_needing_gc, 1);

        let data_capacity = fs::metadata(path.join(DATA_FILE)).unwrap().len();
        db.inject_capacity_limit(data_capacity);
        assert!(matches!(db.gc(), Err(Error::CapacityPreflight)));
        assert!(!db.durability_status().write_fenced);
        assert_eq!(db.blob_stats().total_valid, before.total_valid);
        assert_eq!(db.blob_stats().total_deleted, before.total_deleted);
        assert_eq!(db.get(b"live").unwrap(), Some(value.clone()));

        db.inject_capacity_limit(u64::MAX);
        assert!(db.gc().unwrap() > 0);
        assert_eq!(db.get(b"live").unwrap(), Some(value));
        db.verify().unwrap();
    }

    #[test]
    fn test_db_mixed_gc_final_write_disk_full_reopens_old_root() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mixed-gc-final-disk-full.db");
        let value = vec![0xEF; 2_000];
        let mut db = DB::open(&path, Options::default()).unwrap();
        for key in [b"live".as_slice(), b"dead-1", b"dead-2"] {
            db.put(key, &value).unwrap();
        }
        db.flush().unwrap();
        db.delete(b"dead-1").unwrap();
        db.delete(b"dead-2").unwrap();
        db.flush().unwrap();

        db.inject_final_write_disk_full();
        assert!(matches!(db.gc(), Err(Error::DiskFull)));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"live").unwrap(), Some(value.clone()));
        assert_eq!(reopened.get(b"dead-1").unwrap(), None);
        assert_eq!(reopened.get(b"dead-2").unwrap(), None);
        assert!(reopened.blob_stats().files_needing_gc > 0);
        assert!(reopened.gc().unwrap() > 0);
        reopened.verify().unwrap();
    }

    #[test]
    fn segmented_catalog_consolidation_bound_is_explicit() {
        let mut blobs = BlobManager::with_threshold_and_mode(1, true);
        let mut pointers = Vec::with_capacity(MAX_SEGMENTED_CATALOG_DELETED_ENTRIES + 1);
        for index in 0..=MAX_SEGMENTED_CATALOG_DELETED_ENTRIES {
            pointers.push(blobs.append(&index.to_le_bytes(), vec![index as u8; 2]));
        }

        for pointer in pointers.iter().take(MAX_SEGMENTED_CATALOG_DELETED_ENTRIES) {
            assert!(blobs.mark_deleted(pointer));
        }
        assert!(!segmented_catalog_needs_consolidation(&blobs));
        assert!(blobs.mark_deleted(&pointers[MAX_SEGMENTED_CATALOG_DELETED_ENTRIES]));
        assert!(segmented_catalog_needs_consolidation(&blobs));
    }

    #[test]
    fn segmented_catalog_consolidation_runs_as_explicit_maintenance() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-catalog-consolidation.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 1,
            ..Options::default()
        };
        let mut db = DB::open(&path, options).unwrap();
        db.blobs.set_segment_target_size_for_test(1_250);

        let groups = (MAX_SEGMENTED_CATALOG_DELETED_ENTRIES / 4) + 1;
        let total = groups * 10;
        let puts = (0..total)
            .map(|index| BatchMutation::Put {
                key: index.to_le_bytes().to_vec(),
                value: vec![index as u8; 100],
            })
            .collect::<Vec<_>>();
        db.commit_batch(&puts).unwrap();

        let deletes = (0..total)
            .filter(|index| index % 10 < 4)
            .map(|index| BatchMutation::Delete {
                key: index.to_le_bytes().to_vec(),
            })
            .collect::<Vec<_>>();
        db.commit_batch(&deletes).unwrap();

        let before = db.blob_stats();
        assert_eq!(before.files_needing_gc, 0);
        assert_eq!(before.total_deleted, deletes.len());
        assert!(before.catalog_needs_consolidation);
        assert!(db.gc().unwrap() > 0);

        let after = db.blob_stats();
        assert_eq!(after.total_deleted, 0);
        assert!(!after.catalog_needs_consolidation);
        assert_eq!(db.get(&0usize.to_le_bytes()).unwrap(), None);
        assert_eq!(db.get(&4usize.to_le_bytes()).unwrap(), Some(vec![4; 100]));
        db.verify().unwrap();
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(&0usize.to_le_bytes()).unwrap(), None);
        assert_eq!(
            reopened.get(&4usize.to_le_bytes()).unwrap(),
            Some(vec![4; 100])
        );
        assert!(!reopened.blob_stats().catalog_needs_consolidation);
        reopened.verify().unwrap();
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 8,
            max_shrink_iters: 1_000,
            .. ProptestConfig::default()
        })]

        #[test]
        fn segmented_rollover_preserves_catalog_and_records(
            target_delta in 0u16..768,
            values in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 1..65),
                1..17
            )
        ) {
            let target = 256 + u64::from(target_delta);
            let dir = tempdir().unwrap();
            let path = dir.path().join("segmented-rollover-property.db");
            let options = Options {
                blob_storage: BlobStorageMode::Segmented,
                blob_threshold: 0,
                ..Options::for_test()
            };
            let mut db = DB::create(&path, options).unwrap();
            db.blobs.set_segment_target_size_for_test(target);

            let mut mutations = Vec::with_capacity(values.len() + 2);
            for (index, value) in values.into_iter().enumerate() {
                mutations.push(BatchMutation::Put {
                    key: format!("rollover-{index:04}").into_bytes(),
                    value,
                });
            }

            // Two records that each fit in one segment but cannot fit
            // together guarantee that the generated run exercises rollover.
            let forced_value = vec![0xD7; target as usize / 2];
            mutations.push(BatchMutation::Put {
                key: b"rollover-forced-a".to_vec(),
                value: forced_value.clone(),
            });
            mutations.push(BatchMutation::Put {
                key: b"rollover-forced-b".to_vec(),
                value: forced_value,
            });

            let expected = mutations
                .iter()
                .filter_map(|mutation| match mutation {
                    BatchMutation::Put { key, value } => Some((key.clone(), value.clone())),
                    BatchMutation::Delete { .. } => None,
                })
                .collect::<BTreeMap<_, _>>();
            db.commit_batch(&mutations).unwrap();
            let segment_ids = db.blobs.segment_file_ids();
            assert!(segment_ids.len() >= 2);
            for file_id in &segment_ids {
                assert!(
                    db.blobs.segment_bytes(*file_id).unwrap().len() <= target as usize,
                    "segment {file_id} exceeded target {target}"
                );
            }

            let deletes = expected
                .keys()
                .enumerate()
                .filter(|(index, _)| index % 4 == 0)
                .map(|(_, key)| BatchMutation::Delete { key: key.clone() })
                .collect::<Vec<_>>();
            db.commit_batch(&deletes).unwrap();
            let mut expected = expected;
            for mutation in &deletes {
                if let BatchMutation::Delete { key } = mutation {
                    expected.remove(key);
                }
            }
            db.verify().unwrap();
            assert_eq!(db.blob_stats().total_deleted, deletes.len());
            drop(db);

            let mut reopened = DB::open(&path, Options::for_test()).unwrap();
            for (key, value) in &expected {
                assert_eq!(reopened.get(key).unwrap(), Some(value.clone()));
            }
            assert_eq!(reopened.blob_stats().total_deleted, deletes.len());
            reopened.verify().unwrap();
        }
    }

    #[test]
    fn test_db_segmented_blob_layout_reopens_and_verifies() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let value = vec![0xA7; 2_000];

        let mut db = DB::create(&path, options.clone()).unwrap();
        db.put(b"key", &value).unwrap();
        db.close().unwrap();
        assert!(path.join(BLOB_FILE).is_file());
        assert!(blob_segment_path(&path, 1).is_file());

        let catalog = fs::read(path.join(BLOB_FILE)).unwrap();
        assert!(BlobManager::is_segment_catalog(&catalog));
        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(value.clone()));
        reopened.verify().unwrap();

        let retained = reopened.retain_current().unwrap();
        assert_eq!(retained.get(b"key").unwrap(), Some(value.clone()));
        let replacement = vec![0xB8; 2_100];
        reopened.put(b"key", &replacement).unwrap();
        reopened.close().unwrap();
        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(replacement));
        reopened.verify().unwrap();

        let archive = dir.path().join("segmented-archive");
        reopened.snapshot(&archive).unwrap();
        reopened.close().unwrap();
        let mut archived = DB::open(&archive, Options::default()).unwrap();
        assert_eq!(archived.get(b"key").unwrap(), Some(vec![0xB8; 2_100]));
        archived.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_blob_rewrite_failure_restores_catalog() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-gc.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let value = vec![0xC1; 2_000];
        let mut db = DB::create(&path, options).unwrap();
        for key in [b"live".as_slice(), b"dead-1", b"dead-2"] {
            db.put(key, &value).unwrap();
        }
        db.flush().unwrap();
        db.delete(b"dead-1").unwrap();
        db.delete(b"dead-2").unwrap();
        db.flush().unwrap();

        db.inject_after_blob_rewrite_image_failure();
        assert!(db.gc().is_err());
        assert!(db.durability_status().write_fenced);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"live").unwrap(), Some(value.clone()));
        assert_eq!(reopened.get(b"dead-1").unwrap(), None);
        assert!(reopened.blob_stats().files_needing_gc > 0);
        assert!(reopened.gc().unwrap() > 0);
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_gc_prunes_unreferenced_segments() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-gc-prune.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let value = vec![0xD2; 2_000];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"live", &value).unwrap();
        db.put(b"dead-1", &value).unwrap();
        db.put(b"dead-2", &value).unwrap();
        db.flush().unwrap();
        let old_segment = blob_segment_path(&path, 1);
        assert!(old_segment.is_file());

        db.delete(b"dead-1").unwrap();
        db.delete(b"dead-2").unwrap();
        db.flush().unwrap();
        assert!(db.gc().unwrap() > 0);
        assert!(blob_segment_path(&path, 2).is_file());
        assert!(!old_segment.exists());
        assert_eq!(db.get(b"live").unwrap(), Some(value.clone()));
        db.verify().unwrap();
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert!(!old_segment.exists());
        assert_eq!(reopened.get(b"live").unwrap(), Some(value));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_append_failure_ignores_orphan_suffix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-append-failure.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xE3; 2_000];
        let pending = vec![0xE4; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();
        let segment = blob_segment_path(&path, 1);
        let catalog_before = fs::read(path.join(BLOB_FILE)).unwrap();
        let segment_len_before = fs::metadata(&segment).unwrap().len();

        db.put(b"pending", &pending).unwrap();
        db.inject_blob_segment_after_write_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
        assert!(fs::metadata(&segment).unwrap().len() > segment_len_before);
        assert_eq!(fs::read(path.join(BLOB_FILE)).unwrap(), catalog_before);
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        assert!(!path.join(BLOB_DELTA_FILE).exists());
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        reopened.verify().unwrap();
        reopened.put(b"pending", &pending).unwrap();
        reopened.flush().unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        assert!(path.join(BLOB_DELTA_FILE).is_file());
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_catalog_delta_chain_reopens_and_preserves_anchor() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-catalog-delta-chain.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xE5; 2_000];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();
        let anchor = fs::read(path.join(BLOB_FILE)).unwrap();

        for index in 0..3 {
            let key = format!("delta-{index}");
            db.put(key.as_bytes(), &vec![0xE6 + index as u8; 2_000])
                .unwrap();
            db.flush().unwrap();
        }
        assert_eq!(fs::read(path.join(BLOB_FILE)).unwrap(), anchor);
        let delta = fs::read(path.join(BLOB_DELTA_FILE)).unwrap();
        assert!(!delta.is_empty());
        assert_eq!(
            BlobManager::segment_catalog_delta_prefix_len(&delta),
            Some(delta.len())
        );
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        for index in 0..3 {
            let key = format!("delta-{index}");
            assert_eq!(
                reopened.get(key.as_bytes()).unwrap(),
                Some(vec![0xE6 + index as u8; 2_000])
            );
        }
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_compaction_advances_catalog_generation() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("segmented-compaction-catalog-generation.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 64,
            ..Options::for_test()
        };
        let blob = vec![0xD5; 2_000];
        let mut db = DB::create(&path, options).unwrap();
        let initial = (0..256)
            .map(|index| BatchMutation::Put {
                key: format!("key-{index:04}").into_bytes(),
                value: vec![index as u8; 128],
            })
            .chain(std::iter::once(BatchMutation::Put {
                key: b"segmented-blob".to_vec(),
                value: blob.clone(),
            }))
            .collect::<Vec<_>>();
        db.commit_batch(&initial).unwrap();

        db.put(b"key-0128", &[0xE6; 128]).unwrap();
        db.flush().unwrap();
        let report = db.compact().unwrap();
        assert!(
            report.relocated_pages > 0,
            "expected an interior relocation"
        );
        assert_eq!(db.get(b"segmented-blob").unwrap(), Some(blob.clone()));
        db.verify().unwrap();
        drop(db);

        let mut reopened = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(reopened.get(b"segmented-blob").unwrap(), Some(blob));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_catalog_delta_sync_failure_discards_future_frame() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-catalog-delta-sync-failure.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xE7; 2_000];
        let pending = vec![0xE8; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();
        db.put(b"pending", &pending).unwrap();
        db.inject_blob_segment_catalog_sync_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        assert!(path.join(BLOB_DELTA_FILE).is_file());
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        reopened.verify().unwrap();
        reopened.put(b"pending", &pending).unwrap();
        reopened.flush().unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_catalog_consolidation_rename_failure_preserves_old_catalog() {
        fn fill_delta_chain(db: &mut DB) {
            for index in 0..TEST_SEGMENT_CATALOG_DELTA_LIMIT {
                let key = format!("delta-{index}");
                db.put(key.as_bytes(), &vec![0xF0; 2_000]).unwrap();
                db.flush().unwrap();
            }
        }

        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-catalog-rename-failure.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xF1; 2_000];
        let pending = vec![0xF2; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();
        let catalog = path.join(BLOB_FILE);
        let catalog_before = fs::read(&catalog).unwrap();
        fill_delta_chain(&mut db);

        db.put(b"pending", &pending).unwrap();
        db.inject_blob_segment_catalog_rename_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
        let backup = path.join(BLOB_REWRITE_BACKUP_FILE);
        assert!(!catalog.exists());
        assert_eq!(fs::read(&backup).unwrap(), catalog_before);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        reopened.verify().unwrap();
        reopened.put(b"pending", &pending).unwrap();
        reopened.inject_after_manifest_failure();
        assert!(reopened.flush().is_err());
        drop(reopened);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        assert!(!backup.exists());
        assert!(!path.join(BLOB_DELTA_FILE).exists());
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_catalog_write_failures_restore_old_catalog() {
        let failures = [
            (
                "short",
                DB::inject_blob_segment_catalog_short_write_failure as fn(&DB),
            ),
            (
                "torn",
                DB::inject_blob_segment_catalog_torn_write_failure as fn(&DB),
            ),
        ];

        for (name, inject_failure) in failures {
            let dir = tempdir().unwrap();
            let path = dir
                .path()
                .join(format!("segmented-catalog-{name}-failure.db"));
            let options = Options {
                blob_storage: BlobStorageMode::Segmented,
                blob_threshold: 4,
                ..Options::default()
            };
            let base = vec![0xF3; 2_000];
            let pending = vec![0xF4; 2_100];
            let mut db = DB::create(&path, options).unwrap();
            db.put(b"base", &base).unwrap();
            db.flush().unwrap();
            for index in 0..TEST_SEGMENT_CATALOG_DELTA_LIMIT {
                let key = format!("delta-{index}");
                db.put(key.as_bytes(), &vec![0xF0; 2_000]).unwrap();
                db.flush().unwrap();
            }

            db.put(b"pending", &pending).unwrap();
            inject_failure(&db);
            assert!(db.flush().is_err(), "{name} catalog write should fail");
            assert!(db.durability_status().write_fenced);
            assert!(path.join(BLOB_REWRITE_BACKUP_FILE).is_file());
            drop(db);

            let mut reopened = DB::open(&path, Options::default()).unwrap();
            assert_eq!(reopened.get(b"base").unwrap(), Some(base));
            assert_eq!(reopened.get(b"pending").unwrap(), None);
            assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
            reopened.verify().unwrap();
            reopened.put(b"pending", &pending).unwrap();
            reopened.flush().unwrap();
            assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
            assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
            assert!(!path.join(BLOB_DELTA_FILE).exists());
            reopened.verify().unwrap();
        }
    }

    #[test]
    fn test_db_segmented_first_catalog_write_failure_restores_empty_catalog() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-first-catalog-failure.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let pending = vec![0xF7; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"pending", &pending).unwrap();
        db.inject_blob_segment_catalog_short_write_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
        assert!(path.join(BLOB_REWRITE_BACKUP_FILE).is_file());
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        reopened.verify().unwrap();
        reopened.put(b"pending", &pending).unwrap();
        reopened.flush().unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_publication_directory_failure_restores_old_catalog() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-directory-failure.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xF5; 2_000];
        let pending = vec![0xF6; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();

        db.put(b"pending", &pending).unwrap();
        db.inject_publication_directory_sync_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        assert!(path.join(BLOB_DELTA_FILE).is_file());
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        reopened.verify().unwrap();
        if reopened.get(b"pending").unwrap().is_none() {
            reopened.put(b"pending", &pending).unwrap();
            reopened.flush().unwrap();
        }
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        assert!(path.join(BLOB_DELTA_FILE).is_file());
        reopened.verify().unwrap();
    }
}
