//! Database entry point.
//!
//! The `DB` struct is the main entry point for the storage engine.
//! It owns all components and provides the public API.

#[path = "db/archive.rs"]
mod archive;
#[path = "db/artifact_io.rs"]
mod artifact_io;
#[path = "db/batch.rs"]
mod batch;
#[path = "db/blob_gc.rs"]
mod blob_gc;
#[path = "db/blob_layout.rs"]
mod blob_layout;
#[path = "db/blob_publication.rs"]
mod blob_publication;
#[path = "db/blob_read_view.rs"]
mod blob_read_view;
#[path = "db/capacity.rs"]
mod capacity;
#[path = "db/commit_catalog.rs"]
mod commit_catalog;
#[path = "db/compaction.rs"]
mod compaction;
#[path = "db/diagnostics.rs"]
mod diagnostics;
#[path = "db/durability.rs"]
mod durability;
#[cfg(any(test, feature = "fault-injection"))]
#[path = "db/faults.rs"]
mod faults;
#[path = "db/history_prune.rs"]
mod history_prune;
#[path = "db/invariants.rs"]
mod invariants;
#[path = "db/io.rs"]
mod io;
#[path = "db/lifecycle.rs"]
mod lifecycle;
#[path = "db/metadata.rs"]
mod metadata;
#[path = "db/metadata_codec.rs"]
mod metadata_codec;
mod mutation;
#[path = "db/open.rs"]
mod open;
#[path = "db/open_catalog.rs"]
mod open_catalog;
#[path = "db/open_components.rs"]
mod open_components;
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
#[path = "db/retention_artifacts.rs"]
mod retention_artifacts;
#[path = "db/retention_state.rs"]
mod retention_state;
#[path = "db/single_write.rs"]
mod single_write;
#[path = "db/snapshot.rs"]
mod snapshot;
#[path = "db/transaction.rs"]
mod transaction;
#[path = "db/vacuum.rs"]
mod vacuum;
#[path = "db/wal_admission.rs"]
mod wal_admission;
#[path = "db/wal_recovery.rs"]
mod wal_recovery;

#[cfg(test)]
use metadata_codec::{MAX_META_DELTA_CHAIN, META_DELTA_MAGIC, META_MAGIC};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use artifact_io::atomic_write_reserved;
use artifact_io::{
    atomic_write, atomic_write_without_directory_sync, atomic_write_without_fault_injection,
    cleanup_orphaned_temporary_artifacts, clear_blob_reservation, clear_wal_reservation,
    sync_directory, sync_directory_chain, sync_publication_directory,
};
use blob_layout::{
    BLOB_DELTA_FILE, BLOB_FILE, BLOB_RESERVATION_FILE, BLOB_REWRITE_BACKUP_FILE,
    BLOB_SEGMENT_PREFIX, blob_segment_path, parse_blob_catalog, retained_blob_path,
};
use blob_read_view::BlobReadView;
#[cfg(test)]
use faults::inject_atomic_rename_failure;
#[cfg(any(test, feature = "fault-injection"))]
use faults::{
    FAIL_NEXT_AFTER_BLOB_REWRITE_IMAGE, FAIL_NEXT_AFTER_MANIFEST, FAIL_NEXT_ATOMIC_RENAME,
    FAIL_NEXT_ATOMIC_SHORT_WRITE, FAIL_NEXT_ATOMIC_TORN_WRITE, FAIL_NEXT_BLOB_SEGMENT_AFTER_WRITE,
    FAIL_NEXT_BLOB_SEGMENT_CATALOG_AFTER_WRITE, FAIL_NEXT_BLOB_SEGMENT_CATALOG_RENAME,
    FAIL_NEXT_BLOB_SEGMENT_CATALOG_SHORT_WRITE, FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC,
    FAIL_NEXT_BLOB_SEGMENT_CATALOG_TORN_WRITE, FAIL_NEXT_BLOB_SEGMENT_SYNC,
    FAIL_NEXT_HISTORY_PRUNE_DIRECTORY_SYNC, FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC,
    FAIL_NEXT_WAL_AFTER_SYNC, FAIL_NEXT_WAL_AFTER_WRITE, FAIL_NEXT_WAL_SYNC,
    FAIL_NEXT_WAL_TRUNCATE, FAIL_NEXT_WAL_WRITE,
};
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
pub(super) use io::{decode_u32, decode_u64, read_exact_at};
use retention_state::RetentionState;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use vacuum::VacuumState;

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

#[cfg(test)]
#[path = "db/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "db/segmented_tests.rs"]
mod segmented_tests;

#[cfg(test)]
#[path = "db/published_commits_tests.rs"]
mod published_commits_tests;
