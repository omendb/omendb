//! Database entry point.
//!
//! The `DB` struct is the main entry point for the storage engine.
//! It owns all components and provides the public API.

#[path = "db/archive.rs"]
mod archive;
#[path = "db/artifact_io.rs"]
mod artifact_io;
#[path = "db/blob_gc.rs"]
mod blob_gc;
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
#[path = "db/query.rs"]
mod query;
#[path = "db/read_view.rs"]
mod read_view;
#[path = "db/reports.rs"]
mod reports;
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
pub(super) use reports::elapsed_nanos;
pub use reports::{
    BlobStats, CheckReport, CompactionReport, DBMetrics, DurabilityStatus, HistoryPruneReport,
    PublicationMetrics, PublicationTimingMetrics, RepairAction, RepairReport, RestoreReport,
    SnapshotReport, VacuumProgress, VacuumReport, VerificationReport, WalCheckStatus,
};
pub use snapshot::{RetainedSnapshot, Snapshot};
pub use transaction::{BatchMutation, BatchTransaction, BatchTransactionState};

use crate::allocator::PageAllocator;
use crate::blob::BlobManager;
use crate::btree::{BTree, BlobPointer, LookupResult, MAX_KEY_SIZE, PAGE_SIZE, RangeCursor};
use crate::buffer::BufferManager;
use crate::concurrency::TransactionManager;
use crate::error::{CheckFailureKind, Error, Result};
use crate::mvcc::PMT;
use crate::recovery::{ParseStatus, RecordType, SyncPolicy, WalManager, WalRecord};
use crate::space::{Device, DeviceOptions};
use crate::storage::StorageEngine;
use crate::storage::format::{
    CommitId, CommitRecord, DatabaseId, FORMAT_VERSION, GenerationId, HistoryId,
    MANIFEST_SLOT_SIZE, Manifest, ManifestHistory, ManifestStore, PmtCheckpointId, ReuseAttempt,
    ReuseLedger, SnapshotId,
};
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
#[path = "db/tests.rs"]
mod tests;
