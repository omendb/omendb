//! Read-only diagnostics, maintenance reports, and publication metrics.
//!
//! These types are projections of DB and storage state. The database remains
//! the authority that creates, updates, and publishes the state they report.

use crate::buffer::BufferStats;
use crate::storage::StorageMetrics;
use crate::storage::format::{CommitId, DatabaseId, GenerationId, HistoryId};
use std::time::Instant;

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

pub(crate) fn elapsed_nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
