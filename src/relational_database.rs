//! Project-facing selection of one typed relational storage backend.
//!
//! The facade keeps application code independent of the physical backend
//! while the adoption work compares the temporary kernel with SeerDB. It is
//! deliberately a closed selection for one database handle: it is not a
//! plugin ABI, does not support per-table engine selection, and does not
//! promise live backend swapping.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::relational::{
    Catalog, ForeignKeyDefinition, IndexDefinition, LogicalVerification, RelationalMutation,
    RelationalSchemaDefinition, RelationalSnapshot, RelationalSnapshotCaptureOptions,
    RelationalStore, Row,
};
use crate::seer_relational::{
    LegacyMigrationOptions, LegacyMigrationReport, SeerRelationalStore, SeerRelationalTransaction,
};
use crate::{
    AttemptRecord, CommitId, DatabaseConfig, DbError, DurabilityStatus, Key, NoFaults, Result,
    RowIdentity, SeerKernel, SeerKernelConfig, SnapshotLease, StorageIdentity, TableId,
    TransactionAttemptId, Value,
};

static NEXT_HANDLE_ID: AtomicU64 = AtomicU64::new(1);

/// Cooperative cancellation shared by a project-facing transaction and its
/// caller.
///
/// Cancellation is observed when the transaction begins, performs a bounded
/// read or stage operation, and immediately before durable publication. It
/// cannot interrupt arbitrary user code or an already-started backend write.
/// Once publication begins, the commit outcome follows the normal durable or
/// recovery-required contract instead of being reported as cancelled.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Create a token in the active state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Request cancellation for all transactions sharing this token.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Return whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Cooperative control for one project-facing operation.
///
/// A deadline bounds admission and every transaction checkpoint. It does not
/// interrupt arbitrary caller code or an already-started backend write.
#[derive(Clone, Debug)]
pub struct OperationControl {
    cancellation: CancellationToken,
    deadline: Option<Instant>,
}

impl OperationControl {
    /// Create active control with no deadline.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            deadline: None,
        }
    }

    /// Create control driven by a caller-owned cancellation token.
    #[must_use]
    pub fn with_cancellation(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            deadline: None,
        }
    }

    /// Add an absolute monotonic deadline to this operation control.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Return a clone of the token that can cancel this operation.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Return the configured deadline, if any.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub(crate) fn check(&self) -> Result<()> {
        ensure_not_cancelled(&self.cancellation)?;
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(DbError::DeadlineExceeded);
        }
        Ok(())
    }
}

impl Default for OperationControl {
    fn default() -> Self {
        Self::new()
    }
}

/// One physical backend selected for a database handle.
///
/// SeerDB is the selected production backend (Stage 2 ADR,
/// `STAGE2_STORAGE_ARCHITECTURE_ADR.md`). `Temporary` is retained solely as
/// the independent implementation behind differential conformance testing;
/// it is not a supported deployment target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalBackendKind {
    /// Conformance-oracle backend used by tests to differentially verify the
    /// selected backend against backend-neutral observable semantics.
    Temporary,
    /// The selected SeerDB-backed production backend.
    Seer,
}

/// Transaction behavior shared by the currently supported relational
/// backends.
///
/// Each transaction reads from one fixed snapshot. Readers may overlap, but
/// writers use one serialized publication lane: a writer whose snapshot is
/// declared for the database handle.
///
/// In [`TransactionProfile::FixedSnapshotSerializedWriter`], any write attempt
/// older than the database head is rejected, including when its writes are
/// disjoint from later writes. Applications should rebuild a rejected writer
/// from a fresh snapshot; OmenDB does not retry it automatically.
///
/// In [`TransactionProfile::SerializableValidatedSnapshot`], concurrent
/// transactions on disjoint keys/ranges can commit without false serialization
/// aborts while true read-write, write-write, and cyclic dependency conflicts
/// are strictly detected and refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionProfile {
    FixedSnapshotSerializedWriter,
    SerializableValidatedSnapshot,
}

/// Stable lifecycle state exposed by the selected relational backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalLifecycleState {
    /// The handle is accepting ordinary work under its declared contract.
    Ready,
    /// The backend may have an uncertain durable outcome and must be reopened.
    RecoveryRequired,
}

/// Read-only operational state for a project-facing database handle.
///
/// `None` means the selected backend does not expose a matching measurement;
/// it is not a zero value and must not be used as one. This report does not
/// perform verification or maintenance and does not change database state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalDatabaseStatus {
    pub backend: RelationalBackendKind,
    pub state: RelationalLifecycleState,
    pub commit: CommitId,
    pub catalog_generation: u64,
    pub generation: Option<u64>,
    pub pending_mutations: Option<u64>,
    pub write_fenced: bool,
    /// Distinct explicitly retained snapshot commits, excluding ordinary
    /// transaction read views that are released with their owning transaction.
    pub retained_snapshots: Option<u64>,
}

/// Severity of one project-facing diagnostic finding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalDiagnosticSeverity {
    /// Informational state that does not require operator action.
    Info,
    /// State that may require follow-up before a configured boundary is met.
    Warning,
    /// State that prevents safe continued use under the current contract.
    Error,
}

/// Component associated with one project-facing diagnostic finding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalDiagnosticComponent {
    /// Handle lifecycle and recovery state.
    Lifecycle,
    /// Durable publication state and write fencing.
    Publication,
    /// Explicit historical retention held by the handle.
    Retention,
}

/// Stable code for one project-facing diagnostic finding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalDiagnosticCode {
    /// The handle is ready under its declared transaction and lifecycle contract.
    Ready,
    /// The handle must be reopened before more writes are accepted.
    RecoveryRequired,
    /// The backend has journaled mutations that are not in a published generation.
    PendingPublication,
    /// The handle is intentionally keeping one or more historical snapshots.
    RetainedSnapshots,
}

impl RelationalDiagnosticCode {
    /// Return a stable, non-sensitive explanation for this finding.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::Ready => "database is ready under its declared lifecycle contract",
            Self::RecoveryRequired => "database requires reopen before more writes are accepted",
            Self::PendingPublication => {
                "mutations are journaled but not yet in a published generation"
            }
            Self::RetainedSnapshots => {
                "the handle is retaining historical snapshots by explicit request"
            }
        }
    }

    /// Return the next operator action associated with this finding.
    #[must_use]
    pub const fn recommended_action(self) -> &'static str {
        match self {
            Self::Ready => "no action",
            Self::RecoveryRequired => "close the handle and reopen the database",
            Self::PendingPublication => {
                "finish or close the handle before relying on a new generation"
            }
            Self::RetainedSnapshots => {
                "release snapshots when historical reads or capture no longer need them"
            }
        }
    }
}

/// One typed, non-sensitive operational finding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalDiagnosticFinding {
    pub severity: RelationalDiagnosticSeverity,
    pub component: RelationalDiagnosticComponent,
    pub code: RelationalDiagnosticCode,
    /// Count associated with the finding, when the backend exposes one.
    pub value: Option<u64>,
}

impl RelationalDiagnosticFinding {
    /// Return the stable, non-sensitive explanation for this finding.
    #[must_use]
    pub const fn message(self) -> &'static str {
        self.code.message()
    }

    /// Return the next operator action associated with this finding.
    #[must_use]
    pub const fn recommended_action(self) -> &'static str {
        self.code.recommended_action()
    }
}

/// A correlated read-only operational snapshot for one selected backend.
///
/// This report does not run integrity verification or repair state. Call
/// [`RelationalDatabase::verify`] separately when an integrity pass is needed;
/// a successful diagnostic report must not be treated as proof of integrity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalDiagnosticReport {
    pub backend: RelationalBackendKind,
    pub identity: StorageIdentity,
    pub status: RelationalDatabaseStatus,
    pub metrics: RelationalMetrics,
    pub findings: Vec<RelationalDiagnosticFinding>,
}

impl RelationalDiagnosticReport {
    /// Return whether any finding requires recovery before safe continuation.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.findings
            .iter()
            .any(|finding| finding.severity == RelationalDiagnosticSeverity::Error)
    }
}

/// A bounded, non-sensitive event emitted by one database handle.
///
/// Events are derived in-memory observability state. They are not durable
/// commit records, are not replayed after reopen, and never contain paths,
/// keys, values, query text, or credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalEvent {
    /// Monotonic sequence within the owning database handle.
    pub sequence: u64,
    pub kind: RelationalEventKind,
    /// Logical commit associated with the event, when one exists.
    pub commit: Option<CommitId>,
}

/// Stable event kinds exposed by the first redacted observability slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalEventKind {
    /// A non-read-only logical transaction became durable and acknowledged.
    CommitAcknowledged,
    /// A previously durable attempt was reconciled without rerunning work.
    AttemptAlreadyCommitted,
    /// A write outcome fenced the handle and requires reopen.
    RecoveryRequired,
    /// Checkpoint work completed successfully.
    CheckpointCompleted,
    /// Compaction work completed successfully.
    CompactionCompleted,
    /// Logical/physical verification completed successfully.
    VerificationCompleted,
    /// A historical snapshot lease was acquired.
    SnapshotRetained,
    /// A historical snapshot lease was released.
    SnapshotReleased,
    /// Caller-selected durable attempt records were forgotten.
    AttemptsForgotten,
}

/// A bounded event-history projection for support and diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalEventHistory {
    /// Events retained in sequence order. The oldest events may be omitted
    /// when `dropped` is non-zero.
    pub events: Vec<RelationalEvent>,
    /// Number of older events evicted from the bounded history.
    pub dropped: u64,
}

impl RelationalEventHistory {
    /// Return whether the history no longer contains its complete prefix.
    #[must_use]
    pub const fn is_truncated(&self) -> bool {
        self.dropped != 0
    }
}

/// The kind of operation observed by a project-facing session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalSessionOperationKind {
    Read,
    Write,
}

/// Stable, redacted event kinds emitted by session admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalSessionEventKind {
    /// An operation acquired and released its session permit.
    OperationCompleted,
    /// An operation could not acquire admission within its configured bound.
    AdmissionRejected,
    /// Cancellation prevented admission.
    CancellationObserved,
    /// A deadline prevented admission.
    DeadlineObserved,
}

/// A bounded, non-sensitive event emitted by one project-facing session.
///
/// The durations describe only session admission and permit ownership. They
/// contain no query, row, key, path, or caller identity information.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalSessionEvent {
    /// Monotonic sequence within the owning session.
    pub sequence: u64,
    pub kind: RelationalSessionEventKind,
    pub operation: RelationalSessionOperationKind,
    pub admission_wait: Duration,
    pub operation_time: Duration,
}

/// A bounded event-history projection for session admission and lifecycle
/// support.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RelationalSessionEventHistory {
    /// Events retained in sequence order. The oldest events may be omitted
    /// when `dropped` is non-zero.
    pub events: Vec<RelationalSessionEvent>,
    /// Number of older events evicted from the bounded history.
    pub dropped: u64,
}

impl RelationalSessionEventHistory {
    /// Return whether the history no longer contains its complete prefix.
    #[must_use]
    pub const fn is_truncated(&self) -> bool {
        self.dropped != 0
    }
}

/// Version of the redacted project-facing support bundle.
pub const RELATIONAL_SUPPORT_BUNDLE_VERSION: u16 = 2;

/// Maximum number of in-memory events retained by one database handle.
pub const RELATIONAL_EVENT_HISTORY_LIMIT: usize = 128;

/// Maximum number of statements in one SQL batch transaction.
pub const RELATIONAL_SQL_BATCH_LIMIT: usize = 1_024;

/// A read-only, redacted support snapshot for one selected backend.
///
/// This bundle does not run integrity verification or repair. The diagnostic
/// report is non-sensitive and non-authoritative for integrity; callers must
/// request [`RelationalDatabase::verify`] separately when that evidence is
/// required. Event history is bounded to the current handle lifetime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalSupportBundle {
    pub version: u16,
    pub diagnostic: RelationalDiagnosticReport,
    pub capabilities: RelationalCapabilityReport,
    pub events: RelationalEventHistory,
    /// Session admission events are empty for a direct database handle.
    pub session_events: RelationalSessionEventHistory,
}

#[derive(Debug, Default)]
struct RelationalEventLog {
    /// Interior mutability so diagnostic recording works under the shared
    /// publication lane without serializing preparation behind `&mut`.
    inner: Mutex<EventLogState>,
}

#[derive(Debug, Default)]
struct EventLogState {
    next_sequence: u64,
    dropped: u64,
    events: VecDeque<RelationalEvent>,
}

impl RelationalEventLog {
    fn record(&self, kind: RelationalEventKind, commit: Option<CommitId>) {
        let mut state = self.inner.lock().expect("event log poisoned");
        state.next_sequence = state.next_sequence.saturating_add(1);
        let sequence = state.next_sequence;
        state.events.push_back(RelationalEvent {
            sequence,
            kind,
            commit,
        });
        if state.events.len() > RELATIONAL_EVENT_HISTORY_LIMIT {
            let _ = state.events.pop_front();
            state.dropped = state.dropped.saturating_add(1);
        }
    }

    fn snapshot(&self) -> RelationalEventHistory {
        let state = self.inner.lock().expect("event log poisoned");
        RelationalEventHistory {
            events: state.events.iter().copied().collect(),
            dropped: state.dropped,
        }
    }
}

/// Backend-neutral logical snapshots captured from one database head.
///
/// This is an in-memory capture primitive, not a portable archive file. The
/// source remains authoritative; callers must serialize and publish a target
/// through a later archive protocol that records lineage and verifies reopen.
/// `source_identity` qualifies the source commit numbers and remains stable
/// across ordinary reopen for both supported backends. `attempts` reports the
/// durable control-plane records observed at the source head so an archive can
/// refuse to discard them before transfer mapping is implemented.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalSnapshotCapture {
    pub source_backend: RelationalBackendKind,
    pub source_identity: StorageIdentity,
    pub source_head: CommitId,
    /// Whether the source supplied an authoritative complete ordered commit
    /// catalog rather than a caller-selected subset of retained snapshots.
    pub complete_history: bool,
    /// Durable transaction-attempt records observed at the source head.
    /// Archives refuse non-empty lists until they can map them safely.
    pub attempts: Vec<AttemptRecord>,
    pub snapshots: Vec<RelationalSnapshot>,
}

/// Report returned after a successful checkpoint publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalCheckpointReport {
    /// Lifecycle status observed immediately before checkpoint work began.
    pub before: RelationalDatabaseStatus,
    /// Lifecycle status from the successful checkpoint result.
    pub after: RelationalDatabaseStatus,
    /// SeerDB pages verified by the checkpoint; unavailable on the temporary backend.
    pub verified_physical_pages: Option<u64>,
    /// SeerDB data-file size observed by checkpoint verification.
    pub data_bytes: Option<u64>,
    /// SeerDB blob-file size observed by checkpoint verification.
    pub blob_bytes: Option<u64>,
    /// SeerDB WAL size observed by checkpoint verification.
    pub wal_bytes: Option<u64>,
    /// SeerDB pages currently safe for reuse.
    pub reclaimable_pages: Option<u64>,
}

/// Report returned after a successful compaction pass.
///
/// Logical and physical work are reported independently. `None` means that
/// the selected backend does not expose that measurement; it is not zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalCompactionReport {
    /// Budget supplied to the compaction pass.
    pub budget: RelationalCompactionBudget,
    /// Backend-qualified work units consumed by the pass. Temporary counts
    /// logical row/index histories; SeerDB counts relocated physical pages.
    /// This is not a time, byte, or latency bound.
    pub work_units_consumed: u64,
    /// Lifecycle status observed immediately before compaction began.
    pub before: RelationalDatabaseStatus,
    /// Lifecycle status from the successful compaction result.
    pub after: RelationalDatabaseStatus,
    /// Temporary-backend row histories considered by this pass.
    pub row_keys_considered: Option<u64>,
    /// Temporary-backend index histories considered by this pass.
    pub index_keys_considered: Option<u64>,
    /// Temporary-backend row history fragments reclaimed by this pass.
    pub row_fragments_reclaimed: Option<u64>,
    /// Temporary-backend index history fragments reclaimed by this pass.
    pub index_fragments_reclaimed: Option<u64>,
    /// SeerDB data-file size before compaction.
    pub data_bytes_before: Option<u64>,
    /// SeerDB data-file size after compaction.
    pub data_bytes_after: Option<u64>,
    /// SeerDB physical pages reclaimed by compaction.
    pub reclaimed_pages: Option<u64>,
    /// SeerDB physical pages relocated by compaction.
    pub relocated_pages: Option<u64>,
}

/// A backend-neutral bound for one project-facing compaction pass.
///
/// The bound applies to the backend's core reclaim iteration. The temporary
/// backend counts logical row/index history keys; SeerDB counts relocated
/// physical pages. Fixed bookkeeping such as flushing or manifest publication
/// may still occur, so this is not a wall-clock, byte, or I/O bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalCompactionBudget {
    /// Maximum backend-qualified work units for the core reclaim pass.
    pub max_work_units: usize,
}

impl RelationalCompactionBudget {
    #[must_use]
    pub const fn new(max_work_units: usize) -> Self {
        Self { max_work_units }
    }

    #[must_use]
    pub const fn unlimited() -> Self {
        Self::new(usize::MAX)
    }
}

/// Configuration for a project-facing relational database.
#[derive(Clone, Debug)]
pub enum RelationalBackendConfig {
    /// Open or create a database using the temporary backend.
    Temporary(DatabaseConfig),
    /// Open or create a database using SeerDB.
    Seer(SeerKernelConfig),
}

/// Backend-neutral operational counters exposed by a selected database.
///
/// A field is `None` when the selected backend does not expose that physical
/// measurement through the current contract. The values are diagnostic
/// counters, not latency or capacity guarantees.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalMetrics {
    pub backend: RelationalBackendKind,
    pub commit: CommitId,
    pub wal_bytes: Option<u64>,
    pub syncs: Option<u64>,
    pub logical_page_reads: Option<u64>,
    pub physical_page_reads: Option<u64>,
    pub physical_page_writes: Option<u64>,
    pub data_bytes: Option<u64>,
    pub blob_bytes: Option<u64>,
    /// Optional attribution for durable publication work exposed by the
    /// selected backend. These are cumulative diagnostics, not guarantees.
    pub publication: Option<RelationalPublicationMetrics>,
}

/// Backend-neutral projection of durable publication work.
///
/// A backend may leave this projection unavailable when it cannot attribute
/// the corresponding physical phases. The fields describe cumulative work
/// performed by the open backend handle; nanosecond values are wall-clock
/// diagnostics and do not define latency SLOs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalPublicationMetrics {
    /// Cumulative encoded WAL bytes written by the backend.
    pub wal_bytes_written: u64,
    /// Cumulative data-page bytes written by the backend.
    pub data_bytes_written: u64,
    /// Cumulative metadata bytes written by the backend.
    pub metadata_bytes_written: u64,
    /// Cumulative blob bytes written by the backend.
    pub blob_bytes_written: u64,
    /// Cumulative history bytes written by the backend.
    pub history_bytes_written: u64,
    /// Cumulative manifest bytes written by the backend.
    pub manifest_bytes_written: u64,
    /// Wall-clock nanoseconds spent preparing a publication candidate.
    pub candidate_prepare_ns: u64,
    /// Wall-clock nanoseconds spent writing the WAL.
    pub wal_write_ns: u64,
    /// Wall-clock nanoseconds spent waiting for publication admission.
    pub admission_ns: u64,
    /// Wall-clock nanoseconds spent flushing data pages.
    pub data_flush_ns: u64,
    /// Wall-clock nanoseconds spent writing metadata.
    pub metadata_write_ns: u64,
    /// Wall-clock nanoseconds spent writing blobs.
    pub blob_write_ns: u64,
    /// Wall-clock nanoseconds spent writing history.
    pub history_write_ns: u64,
    /// Wall-clock nanoseconds spent syncing the database directory.
    pub directory_sync_ns: u64,
    /// Wall-clock nanoseconds spent writing the manifest.
    pub manifest_write_ns: u64,
    /// Wall-clock nanoseconds spent mirroring the manifest.
    pub manifest_mirror_ns: u64,
    /// Wall-clock nanoseconds spent cleaning up publication state.
    pub cleanup_ns: u64,
}

/// A project-facing capability that can be inspected before issuing work.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RelationalCapability {
    /// Typed tables, rows, indexes, and constraints.
    TypedRelational,
    /// Atomic multi-row transactions.
    AtomicTransactions,
    /// Fixed-snapshot transactions with one serialized writer lane.
    FixedSnapshotSerializedWriter,
    /// Secondary-index reads and maintenance.
    SecondaryIndexes,
    /// Immediate foreign-key validation.
    ImmediateForeignKeys,
    /// Deferred (publication-resolved) constraint timing.
    DeferredConstraints,
    /// ON DELETE CASCADE / SET NULL referential actions.
    CascadeReferentialActions,
    /// Explicit retained historical snapshots.
    RetainedSnapshots,
    /// Durable checkpoint publication.
    Checkpoint,
    /// Bounded compaction or reclamation.
    Compaction,
    /// Read-only logical integrity verification.
    IntegrityVerification,
    /// Durable transaction-attempt outcome resolution.
    DurableAttemptReconciliation,
    /// Synchronous bounded session admission with writer preference.
    WaitableSessionAdmission,
    /// Selected-snapshot archive and restore under the bounded archive contract.
    SelectedSnapshotArchiveRestore,
    /// Current-state migration from the temporary backend to SeerDB.
    CurrentStateMigration,
    /// SQL parsing and execution.
    Sql,
    /// PostgreSQL wire-protocol serving.
    Pgwire,
    /// Parallel write transactions.
    ParallelWriters,
    /// Complete-history transfer with preserved lineage.
    FullHistoryTransfer,
    /// Replication or high availability.
    Replication,
}

impl RelationalCapability {
    const ALL: [Self; 18] = [
        Self::TypedRelational,
        Self::AtomicTransactions,
        Self::FixedSnapshotSerializedWriter,
        Self::SecondaryIndexes,
        Self::ImmediateForeignKeys,
        Self::RetainedSnapshots,
        Self::Checkpoint,
        Self::Compaction,
        Self::IntegrityVerification,
        Self::DurableAttemptReconciliation,
        Self::WaitableSessionAdmission,
        Self::SelectedSnapshotArchiveRestore,
        Self::CurrentStateMigration,
        Self::Sql,
        Self::Pgwire,
        Self::ParallelWriters,
        Self::FullHistoryTransfer,
        Self::Replication,
    ];

    /// Return capabilities in stable report order.
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &Self::ALL
    }
}

/// Availability level for one project-facing capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalCapabilityState {
    /// The capability is available under the common facade contract.
    Supported,
    /// The capability is available only within an explicit bounded contract.
    Bounded,
    /// The capability is deliberately refused at this surface.
    Unsupported,
}

impl RelationalCapabilityState {
    /// Return whether callers may use the capability under its stated bounds.
    #[must_use]
    pub const fn is_available(self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

/// One non-sensitive capability/refusal entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalCapabilityInfo {
    pub capability: RelationalCapability,
    pub state: RelationalCapabilityState,
    /// Stable human-readable explanation; this contains no path or payload.
    pub explanation: &'static str,
}

/// Read-only capability and explicit-refusal projection for one backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalCapabilityReport {
    pub backend: RelationalBackendKind,
    pub capabilities: Vec<RelationalCapabilityInfo>,
}

impl RelationalCapabilityReport {
    fn for_backend(backend: RelationalBackendKind) -> Self {
        let capabilities = RelationalCapability::all()
            .iter()
            .copied()
            .map(|capability| capability_info(backend, capability))
            .collect();
        Self {
            backend,
            capabilities,
        }
    }

    /// Return the state for one capability.
    #[must_use]
    pub fn state(&self, capability: RelationalCapability) -> RelationalCapabilityState {
        self.capabilities
            .iter()
            .find(|info| info.capability == capability)
            .map_or(RelationalCapabilityState::Unsupported, |info| info.state)
    }

    /// Return whether one capability is available within its stated bounds.
    #[must_use]
    pub fn supports(&self, capability: RelationalCapability) -> bool {
        self.state(capability).is_available()
    }
}

fn capability_info(
    backend: RelationalBackendKind,
    capability: RelationalCapability,
) -> RelationalCapabilityInfo {
    use RelationalCapability as Capability;
    use RelationalCapabilityState as State;

    let (state, explanation) = match capability {
        Capability::TypedRelational => (
            State::Supported,
            "typed schema, rows, indexes, and constraints are available",
        ),
        Capability::AtomicTransactions => (
            State::Supported,
            "atomic multi-row transactions are available",
        ),
        Capability::FixedSnapshotSerializedWriter => (
            State::Bounded,
            "transactions use fixed snapshots and one serialized writer lane",
        ),
        Capability::SecondaryIndexes => (
            State::Supported,
            "secondary-index reads and transactional maintenance are available",
        ),
        Capability::ImmediateForeignKeys => (
            State::Supported,
            "foreign-key constraints use immediate validation semantics",
        ),
        Capability::DeferredConstraints => (
            State::Bounded,
            "deferred constraint timing resolves at publication validation; \
             the serialized writer makes both timings publication-checked",
        ),
        Capability::CascadeReferentialActions => (
            State::Bounded,
            "ON DELETE CASCADE and SET NULL stage eagerly with a bounded \
             cascade depth; update actions and SET DEFAULT are refused",
        ),
        Capability::RetainedSnapshots => (
            State::Supported,
            "explicit historical snapshot retention is available",
        ),
        Capability::Checkpoint => (
            State::Supported,
            "durable checkpoint publication is available",
        ),
        Capability::Compaction => (
            State::Bounded,
            "reclamation is bounded by the selected maintenance contract",
        ),
        Capability::IntegrityVerification => (
            State::Supported,
            "logical integrity verification is available; physical fields may be unavailable",
        ),
        Capability::DurableAttemptReconciliation => (
            State::Supported,
            "durable transaction attempts can be resolved after reopen",
        ),
        Capability::WaitableSessionAdmission => (
            State::Bounded,
            "synchronous admission waits within a configured bound and prefers writers",
        ),
        Capability::SelectedSnapshotArchiveRestore => (
            State::Bounded,
            "selected current or retained snapshots restore with additive catalog changes",
        ),
        Capability::CurrentStateMigration => match backend {
            RelationalBackendKind::Temporary => (
                State::Bounded,
                "temporary state can migrate to SeerDB with explicit history-loss policy",
            ),
            RelationalBackendKind::Seer => (
                State::Unsupported,
                "current-state migration requires a temporary source handle",
            ),
        },
        Capability::Sql => (
            State::Bounded,
            "bounded embedded SQL translates into the typed catalog and transaction facade",
        ),
        #[cfg(feature = "pgwire")]
        Capability::Pgwire => (
            State::Bounded,
            "the PostgreSQL wire protocol serves the bounded SQL tier with SCRAM-SHA-256 authentication and catalog-backed table grants; trust mode is loopback-only",
        ),
        #[cfg(not(feature = "pgwire"))]
        Capability::Pgwire => (
            State::Unsupported,
            "the PostgreSQL wire protocol is not served",
        ),
        Capability::ParallelWriters => (
            State::Bounded,
            "parallel write transactions are admitted only through the explicit validated parallel-preparation API (serializable certifier); the default transaction path remains one serialized writer that rejects stale snapshots",
        ),
        Capability::FullHistoryTransfer => (
            State::Bounded,
            "authoritative, unpruned relational histories can transfer with ordered mappings",
        ),
        Capability::Replication => (
            State::Unsupported,
            "replication and high availability are not exposed",
        ),
    };
    RelationalCapabilityInfo {
        capability,
        state,
        explanation,
    }
}

/// Result of a read-only integrity check at the project-facing boundary.
///
/// Logical counts are verified for every backend. Physical page and file
/// counts are populated only when the selected backend exposes a matching
/// integrity pass; `None` is intentional and is not a zero claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalVerificationReport {
    pub backend: RelationalBackendKind,
    pub commit: CommitId,
    pub catalog_generation: u64,
    pub verified_tables: u64,
    pub verified_indexes: u64,
    pub verified_rows: u64,
    pub verified_index_entries: u64,
    pub physical_pages: Option<u64>,
    pub data_bytes: Option<u64>,
    pub blob_bytes: Option<u64>,
    pub wal_bytes: Option<u64>,
}

/// Result of an attempt-aware closure transaction.
///
/// A previously published attempt cannot safely recreate the closure's return
/// value without rerunning caller code. The explicit duplicate variant lets a
/// caller reconcile from the durable commit instead.
#[derive(Debug)]
pub enum TransactionAttemptOutcome<T> {
    /// The closure ran and its staged mutations were durably published.
    Applied { value: T, commit: CommitId },
    /// The attempt was already published; the closure was not run.
    AlreadyCommitted { record: AttemptRecord },
}

/// A project-facing typed relational database with one selected backend.
pub struct RelationalDatabase {
    handle_id: u64,
    backend: Backend,
    events: RelationalEventLog,
}

enum Backend {
    Temporary(Box<RelationalStore>),
    Seer(Box<SeerRelationalStore>),
}

#[derive(Clone, Copy)]
struct RelationalCompactionWork {
    row_keys_considered: Option<u64>,
    index_keys_considered: Option<u64>,
    row_fragments_reclaimed: Option<u64>,
    index_fragments_reclaimed: Option<u64>,
    data_bytes_before: Option<u64>,
    data_bytes_after: Option<u64>,
    reclaimed_pages: Option<u64>,
    relocated_pages: Option<u64>,
}

impl RelationalDatabase {
    /// Create a new database. The target directory must not already exist.
    pub fn create(config: RelationalBackendConfig) -> Result<Self> {
        let backend = match config {
            RelationalBackendConfig::Temporary(config) => {
                Backend::Temporary(Box::new(RelationalStore::create(config)?))
            }
            RelationalBackendConfig::Seer(config) => {
                Backend::Seer(Box::new(SeerRelationalStore::<SeerKernel>::create(config)?))
            }
        };
        Ok(Self::with_backend(backend))
    }

    /// Open an existing database without enabling fault injection at the
    /// project-facing boundary.
    pub fn open(config: RelationalBackendConfig) -> Result<Self> {
        let backend = match config {
            RelationalBackendConfig::Temporary(config) => {
                let mut faults = NoFaults;
                Backend::Temporary(Box::new(RelationalStore::open(config, &mut faults)?))
            }
            RelationalBackendConfig::Seer(config) => {
                Backend::Seer(Box::new(SeerRelationalStore::<SeerKernel>::open(config)?))
            }
        };
        Ok(Self::with_backend(backend))
    }

    /// Flush and consume this database handle.
    ///
    /// The selected backend releases its process writer lock as part of close
    /// or drop cleanup. A close error consumes the handle as well; callers
    /// must reopen it to reconcile a recovery-required outcome.
    pub fn close(self) -> Result<()> {
        match self.backend {
            Backend::Temporary(store) => store.close(),
            Backend::Seer(store) => store.close(),
        }
    }

    /// Migrate a temporary database's current state into a new SeerDB
    /// database through the project-facing boundary.
    pub fn migrate_from_temporary(
        source: &Self,
        config: SeerKernelConfig,
        options: LegacyMigrationOptions,
    ) -> Result<(Self, LegacyMigrationReport)> {
        let Backend::Temporary(source) = &source.backend else {
            return Err(DbError::InvalidState(
                "migration source must use the temporary backend".to_owned(),
            ));
        };
        let (migrated, report) =
            SeerRelationalStore::migrate_from_legacy_with_options(source, config, options)?;
        Ok((
            Self::with_backend(Backend::Seer(Box::new(migrated))),
            report,
        ))
    }

    fn with_backend(backend: Backend) -> Self {
        Self {
            handle_id: NEXT_HANDLE_ID.fetch_add(1, Ordering::Relaxed),
            backend,
            events: RelationalEventLog::default(),
        }
    }

    /// Return the backend selected for this database handle.
    #[must_use]
    pub fn backend(&self) -> RelationalBackendKind {
        match self.backend {
            Backend::Temporary(_) => RelationalBackendKind::Temporary,
            Backend::Seer(_) => RelationalBackendKind::Seer,
        }
    }

    /// Return the stable, non-sensitive capability/refusal projection for
    /// this selected backend without performing I/O or changing state.
    #[must_use]
    pub fn capabilities(&self) -> RelationalCapabilityReport {
        RelationalCapabilityReport::for_backend(self.backend())
    }

    /// Return a bounded, read-only, redacted support snapshot.
    pub fn support_bundle(&self) -> Result<RelationalSupportBundle> {
        Ok(RelationalSupportBundle {
            version: RELATIONAL_SUPPORT_BUNDLE_VERSION,
            diagnostic: self.diagnose()?,
            capabilities: self.capabilities(),
            events: self.events.snapshot(),
            session_events: RelationalSessionEventHistory::default(),
        })
    }

    fn record_event(&self, kind: RelationalEventKind, commit: Option<CommitId>) {
        self.events.record(kind, commit);
    }

    /// Record the duplicate-attempt event for a session-level dedup hit.
    pub(crate) fn record_already_committed_event(&self, commit: CommitId) {
        self.record_event(RelationalEventKind::AttemptAlreadyCommitted, Some(commit));
    }

    fn record_commit_result(&self, result: Result<CommitId>) -> Result<CommitId> {
        match &result {
            Ok(commit) => self.record_event(RelationalEventKind::CommitAcknowledged, Some(*commit)),
            Err(DbError::RecoveryRequired) => self.record_event(
                RelationalEventKind::RecoveryRequired,
                Some(self.commit_id()),
            ),
            Err(_) => {}
        }
        result
    }

    /// Return the transaction profile guaranteed by this facade.
    #[must_use]
    pub fn transaction_profile(&self) -> TransactionProfile {
        TransactionProfile::FixedSnapshotSerializedWriter
    }

    /// Publish several prepared transactions; same-snapshot Seer
    /// transactions with disjoint writes share one durable publication.
    ///
    /// Transactions that cannot coalesce (stale snapshot, overlapping
    /// writes, or a non-Seer backend) take the per-transaction commit path
    /// in submission order, so every caller observes the same error its
    /// standalone commit would produce.
    pub fn commit_coalesced(
        &self,
        transactions: Vec<RelationalDatabaseTransaction>,
    ) -> Vec<Result<CommitId>> {
        let mut results: Vec<Option<Result<CommitId>>> =
            (0..transactions.len()).map(|_| None).collect();
        let mut seer: Vec<(usize, SeerRelationalTransaction)> = Vec::new();
        for (index, transaction) in transactions.into_iter().enumerate() {
            if transaction.owner_id != self.handle_id {
                results[index] = Some(Err(invalid_transaction_owner()));
                continue;
            }
            if transaction.ensure_active().is_err() {
                results[index] = Some(Err(DbError::InvalidState(
                    "transaction is no longer active".to_owned(),
                )));
                continue;
            }
            if transaction.is_read_only() {
                let snapshot = transaction.snapshot();
                results[index] = Some(Ok(snapshot));
                continue;
            }
            match transaction.backend {
                TransactionBackend::Seer(inner) => seer.push((index, *inner)),
                TransactionBackend::Temporary(_) => {
                    // The Temporary backend has no durable publication to
                    // overlap; group commit is a Seer-path feature and the
                    // exclusive transaction API serves the in-memory backend.
                    results[index] = Some(Err(DbError::InvalidState(
                        "coalesced publication requires the Seer backend".to_owned(),
                    )));
                }
            }
        }

        if !seer.is_empty() {
            let indices: Vec<usize> = seer.iter().map(|(index, _)| *index).collect();
            let prepared = seer
                .into_iter()
                .map(|(_, transaction)| transaction.into_prepared())
                .collect::<Vec<_>>();
            let outcomes = match &self.backend {
                Backend::Seer(store) => store.commit_transactions_coalesced(prepared),
                _ => unreachable!("only Seer transactions reach the coalesced path"),
            };
            for (index, outcome) in indices.into_iter().zip(outcomes) {
                results[index] = Some(self.record_commit_result(outcome));
            }
        }

        results
            .into_iter()
            .map(|result| result.expect("every result is assigned"))
            .collect()
    }

    /// Return backend-neutral lifecycle state without performing I/O or
    /// changing durable state.
    pub fn status(&self) -> Result<RelationalDatabaseStatus> {
        match &self.backend {
            Backend::Temporary(store) => Ok(temporary_status(store)),
            Backend::Seer(store) => {
                let status = store.durability_status()?;
                Ok(seer_status(store, status))
            }
        }
    }

    /// Return a correlated, read-only operational snapshot without verifying
    /// or changing durable state. Integrity verification remains an explicit
    /// separate operation through [`Self::verify`].
    pub fn diagnose(&self) -> Result<RelationalDiagnosticReport> {
        let status = self.status()?;
        let metrics = self.metrics()?;
        let identity = self.storage_identity()?;
        let mut findings = Vec::with_capacity(3);

        if status.state == RelationalLifecycleState::RecoveryRequired {
            findings.push(RelationalDiagnosticFinding {
                severity: RelationalDiagnosticSeverity::Error,
                component: RelationalDiagnosticComponent::Lifecycle,
                code: RelationalDiagnosticCode::RecoveryRequired,
                value: None,
            });
        } else {
            findings.push(RelationalDiagnosticFinding {
                severity: RelationalDiagnosticSeverity::Info,
                component: RelationalDiagnosticComponent::Lifecycle,
                code: RelationalDiagnosticCode::Ready,
                value: None,
            });
        }

        if let Some(pending) = status.pending_mutations.filter(|pending| *pending != 0) {
            findings.push(RelationalDiagnosticFinding {
                severity: RelationalDiagnosticSeverity::Warning,
                component: RelationalDiagnosticComponent::Publication,
                code: RelationalDiagnosticCode::PendingPublication,
                value: Some(pending),
            });
        }

        if let Some(retained) = status.retained_snapshots.filter(|retained| *retained != 0) {
            findings.push(RelationalDiagnosticFinding {
                severity: RelationalDiagnosticSeverity::Info,
                component: RelationalDiagnosticComponent::Retention,
                code: RelationalDiagnosticCode::RetainedSnapshots,
                value: Some(retained),
            });
        }

        Ok(RelationalDiagnosticReport {
            backend: status.backend,
            identity,
            status,
            metrics,
            findings,
        })
    }

    #[must_use]
    pub fn commit_id(&self) -> CommitId {
        match &self.backend {
            Backend::Temporary(store) => store.commit_id(),
            Backend::Seer(store) => store.commit_id(),
        }
    }

    /// Return the latest published commit ID of this database.
    #[must_use]
    pub fn head(&self) -> CommitId {
        self.commit_id()
    }

    /// Return the stable identity for this database history.
    pub fn storage_identity(&self) -> Result<StorageIdentity> {
        match &self.backend {
            Backend::Temporary(store) => store.storage_identity(),
            Backend::Seer(store) => store.storage_identity(),
        }
    }

    /// Return explicitly retained snapshot commits in ascending order.
    ///
    /// This is an observation of the selected backend handle's retention
    /// leases. It is not a commit-history catalog and does not acquire or
    /// extend a lease; an export or historical read must still retain and
    /// validate each commit before using it.
    #[must_use]
    pub fn retained_snapshot_commits(&self) -> Vec<CommitId> {
        match &self.backend {
            Backend::Temporary(store) => store.retained_snapshot_commits(),
            Backend::Seer(store) => store.retained_snapshot_commits(),
        }
    }

    /// Return the backend's authoritative ordered logical commit catalog.
    ///
    /// This is distinct from handle-local retention leases. A backend may
    /// refuse the request when its format cannot prove that the catalog is
    /// complete; callers must not reconstruct it from row fragments.
    pub fn published_commit_ids(&self) -> Result<Vec<CommitId>> {
        match &self.backend {
            Backend::Temporary(store) => store.published_commits(),
            Backend::Seer(store) => store.published_commits(),
        }
    }

    /// Capture caller-selected current or explicitly retained snapshots under
    /// one exclusive source boundary.
    ///
    /// The selection is normalized into ascending commit order. Each selected
    /// snapshot receives a temporary capture lease, and all capture leases are
    /// released before this method returns. The returned rows and digests are
    /// owned by the caller; this method does not create a target or claim
    /// history-preserving migration.
    pub fn capture_selected_snapshots(
        &mut self,
        snapshots: &[CommitId],
        options: RelationalSnapshotCaptureOptions,
    ) -> Result<RelationalSnapshotCapture> {
        let source_head = self.commit_id();
        let selected = normalize_snapshot_selection(
            snapshots,
            source_head,
            &self.retained_snapshot_commits(),
        )?;
        self.capture_snapshots(selected, options, false)
    }

    /// Capture every logical commit boundary from an authoritative backend
    /// catalog. This is the only capture path that may feed
    /// [`crate::RelationalArchiveMode::FullHistory`].
    pub fn capture_full_history(
        &mut self,
        options: RelationalSnapshotCaptureOptions,
    ) -> Result<RelationalSnapshotCapture> {
        let source_head = self.commit_id();
        let selected = self.published_commit_ids()?;
        if selected.first().copied() != Some(CommitId(0))
            || selected.last().copied() != Some(source_head)
            || selected
                .windows(2)
                .any(|pair| pair[0].0.checked_add(1) != Some(pair[1].0))
        {
            return Err(DbError::InvalidState(
                "backend commit catalog does not cover the complete ordered history".to_owned(),
            ));
        }
        self.capture_snapshots(selected, options, true)
    }

    fn capture_snapshots(
        &mut self,
        selected: Vec<CommitId>,
        options: RelationalSnapshotCaptureOptions,
        complete_history: bool,
    ) -> Result<RelationalSnapshotCapture> {
        if selected.is_empty() {
            return Err(DbError::InvalidState(
                "snapshot capture requires at least one selected commit".to_owned(),
            ));
        }
        if selected.len() > options.max_snapshots {
            return Err(DbError::SnapshotCaptureLimit {
                resource: "snapshots",
                limit: options.max_snapshots,
            });
        }
        let source_identity = self.storage_identity()?;
        let source_head = self.commit_id();
        let attempts = match &self.backend {
            Backend::Temporary(store) => store.attempt_records(options.max_attempts),
            Backend::Seer(store) => store.attempt_records(options.max_attempts),
        }?;
        let mut leases = Vec::with_capacity(selected.len());
        for snapshot in &selected {
            match self.retain(*snapshot) {
                Ok(lease) => leases.push(lease),
                Err(error) => {
                    return match release_capture_leases(self, leases) {
                        Ok(()) => Err(error),
                        Err(cleanup) => Err(DbError::SnapshotCaptureCleanup {
                            capture: Box::new(error),
                            cleanup: Box::new(cleanup),
                        }),
                    };
                }
            }
        }

        let mut rows_captured = 0;
        let capture = match &self.backend {
            Backend::Temporary(store) => selected
                .iter()
                .map(|snapshot| store.capture_snapshot(*snapshot, options, &mut rows_captured))
                .collect(),
            Backend::Seer(store) => selected
                .iter()
                .map(|snapshot| store.capture_snapshot(*snapshot, options, &mut rows_captured))
                .collect(),
        };
        let cleanup = release_capture_leases(self, leases);
        match (capture, cleanup) {
            (Ok(snapshots), Ok(())) => Ok(RelationalSnapshotCapture {
                source_backend: self.backend(),
                source_identity,
                source_head,
                complete_history,
                attempts,
                snapshots,
            }),
            (Err(capture), Err(cleanup)) => Err(DbError::SnapshotCaptureCleanup {
                capture: Box::new(capture),
                cleanup: Box::new(cleanup),
            }),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    pub(crate) fn import_attempt_records(
        &mut self,
        records: &[AttemptRecord],
    ) -> Result<Vec<AttemptRecord>> {
        match &mut self.backend {
            Backend::Temporary(store) => store.import_attempt_records(records),
            Backend::Seer(store) => store.import_attempt_records(records),
        }
    }

    /// Read the currently published schema.
    #[must_use]
    pub fn catalog(&self) -> &Catalog {
        match &self.backend {
            Backend::Temporary(store) => store.catalog(),
            Backend::Seer(store) => store.catalog(),
        }
    }

    /// Read the schema published at one logical snapshot.
    pub fn catalog_at(&self, snapshot: CommitId) -> Result<Catalog> {
        match &self.backend {
            Backend::Temporary(store) => store.catalog_at(snapshot),
            Backend::Seer(store) => store.catalog_at(snapshot),
        }
    }

    /// Execute one statement in the bounded embedded SQL tier.
    ///
    /// SQL is translated into the typed catalog and transaction APIs. This
    /// method accepts one statement at a time and does not imply PostgreSQL
    /// compatibility or a wire protocol.
    pub fn execute_sql(&mut self, sql: &str) -> Result<crate::SqlResult> {
        crate::sql::execute(self, sql)
    }

    /// Execute one statement in the bounded embedded SQL tier with explicit
    /// positional parameters.
    pub fn execute_sql_with_params(
        &mut self,
        sql: &str,
        params: &[crate::Value],
    ) -> Result<crate::SqlResult> {
        crate::sql::execute_with_params(self, sql, params)
    }

    /// Execute several bounded SQL statements in one atomic transaction.
    ///
    /// The batch is the durability and rollback boundary: SeerDB can publish
    /// all staged writes in one physical envelope. Schema statements and
    /// transaction-control SQL remain refused inside the batch; use the
    /// direct schema methods and typed transaction lifecycle for those.
    pub fn execute_sql_batch(&mut self, statements: &[&str]) -> Result<Vec<crate::SqlResult>> {
        if statements.len() > RELATIONAL_SQL_BATCH_LIMIT {
            return Err(DbError::ResourceLimitExceeded(format!(
                "SQL batch has {} statements; limit is {}",
                statements.len(),
                RELATIONAL_SQL_BATCH_LIMIT
            )));
        }
        self.transaction(|database, transaction| {
            statements
                .iter()
                .map(|statement| transaction.execute_sql(database, statement))
                .collect()
        })
        .map(|(results, _)| results)
    }

    /// Execute parameterized SQL statements in one atomic transaction.
    pub fn execute_sql_batch_with_params(
        &mut self,
        statements: &[(&str, &[crate::Value])],
    ) -> Result<Vec<crate::SqlResult>> {
        if statements.len() > RELATIONAL_SQL_BATCH_LIMIT {
            return Err(DbError::ResourceLimitExceeded(format!(
                "SQL batch has {} statements; limit is {}",
                statements.len(),
                RELATIONAL_SQL_BATCH_LIMIT
            )));
        }
        self.transaction(|database, transaction| {
            statements
                .iter()
                .map(|(statement, params)| {
                    transaction.execute_sql_with_params(database, statement, params)
                })
                .collect()
        })
        .map(|(results, _)| results)
    }

    /// Infer each positional parameter's expected column type from the
    /// statement context (single-table scope). Positions context cannot
    /// determine report `None`.
    pub fn sql_parameter_types(&self, sql: &str) -> Result<Vec<Option<crate::ColumnType>>> {
        crate::sql::describe_parameters(self, sql)
    }

    /// Execute an analytical query with chunked morsel scanning, grouping,
    /// and memory budget protection.
    pub fn query_analytical(
        &mut self,
        query: &crate::morsel::AnalyticalQuery,
    ) -> Result<crate::morsel::AnalyticalResult> {
        self.query_analytical_with_control(query, &OperationControl::default())
    }

    /// Execute an analytical query under operation control checkpoints.
    pub fn query_analytical_with_control(
        &mut self,
        query: &crate::morsel::AnalyticalQuery,
        control: &OperationControl,
    ) -> Result<crate::morsel::AnalyticalResult> {
        let mut tx = self.begin_with_control(control)?;
        crate::morsel::AnalyticalExecutor::execute(self, &mut tx, query, control)
    }

    /// Open a replication stream from this primary database starting after `from_commit`.
    #[must_use]
    pub fn open_replication_stream(
        &self,
        from_commit: CommitId,
    ) -> crate::replication::ReplicationStream {
        crate::replication::ReplicationStream::new(from_commit)
    }

    /// Publish a table definition and return its commit.
    pub fn create_table(&mut self, table: crate::TableDefinition) -> Result<CommitId> {
        let result = match &mut self.backend {
            Backend::Temporary(store) => store.create_table(table),
            Backend::Seer(store) => store.create_table(table),
        };
        self.record_commit_result(result)
    }

    /// Publish a new table and its indexes/foreign keys as one schema commit.
    pub fn create_table_with_schema(
        &mut self,
        table: crate::TableDefinition,
        schema: RelationalSchemaDefinition,
    ) -> Result<CommitId> {
        self.create_table_with_schema_and_primary_key(table, None, schema)
    }

    /// Publish a table, its catalog-owned primary-key order, and secondary
    /// schema objects atomically.
    pub fn create_table_with_schema_and_primary_key(
        &mut self,
        table: crate::TableDefinition,
        primary_key: Option<Vec<crate::ColumnId>>,
        schema: RelationalSchemaDefinition,
    ) -> Result<CommitId> {
        let result = match &mut self.backend {
            Backend::Temporary(store) => {
                store.create_table_with_schema_and_primary_key(table, primary_key, schema)
            }
            Backend::Seer(store) => {
                store.create_table_with_schema_and_primary_key(table, primary_key, schema)
            }
        };
        self.record_commit_result(result)
    }

    /// Append one nullable column atomically. Existing physical rows expose a
    /// logical `NULL` for the new field without a table-sized rewrite.
    pub fn add_nullable_column(
        &mut self,
        table: crate::TableId,
        column: crate::ColumnDefinition,
    ) -> Result<CommitId> {
        let result = match &mut self.backend {
            Backend::Temporary(store) => store.add_nullable_column(table, column),
            Backend::Seer(store) => store.add_nullable_column(table, column),
        };
        self.record_commit_result(result)
    }

    /// Publish an index definition and return its commit.
    pub fn create_index(&mut self, index: IndexDefinition) -> Result<CommitId> {
        let result = match &mut self.backend {
            Backend::Temporary(store) => store.create_index(index),
            Backend::Seer(store) => store.create_index(index),
        };
        self.record_commit_result(result)
    }

    /// Publish an index definition and retain its SQL object name in the
    /// selected backend's catalog.
    pub fn create_named_index(&mut self, index: IndexDefinition, name: String) -> Result<CommitId> {
        let result = match &mut self.backend {
            Backend::Temporary(store) => store.create_named_index(index, name),
            Backend::Seer(store) => store.create_named_index(index, name),
        };
        self.record_commit_result(result)
    }

    /// Publish a foreign-key definition and return its commit.
    pub fn create_foreign_key(&mut self, foreign_key: ForeignKeyDefinition) -> Result<CommitId> {
        let result = match &mut self.backend {
            Backend::Temporary(store) => store.create_foreign_key(foreign_key),
            Backend::Seer(store) => store.create_foreign_key(foreign_key),
        };
        self.record_commit_result(result)
    }

    /// Publish a named foreign-key definition and return its commit.
    pub fn create_named_foreign_key(
        &mut self,
        foreign_key: ForeignKeyDefinition,
        name: String,
    ) -> Result<CommitId> {
        let result = match &mut self.backend {
            Backend::Temporary(store) => store.create_named_foreign_key(foreign_key, name),
            Backend::Seer(store) => store.create_named_foreign_key(foreign_key, name),
        };
        self.record_commit_result(result)
    }

    pub fn begin(&self) -> Result<RelationalDatabaseTransaction> {
        self.begin_inner(None)
    }

    /// Begin a transaction controlled by a cooperative cancellation token.
    ///
    /// The token is checked at admission and before each bounded transaction
    /// operation. It does not interrupt arbitrary code run by the caller.
    pub fn begin_with_cancellation(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<RelationalDatabaseTransaction> {
        self.begin_with_control(&OperationControl::with_cancellation(cancellation.clone()))
    }

    /// Begin a transaction controlled by cancellation and an optional
    /// monotonic deadline.
    pub fn begin_with_control(
        &self,
        control: &OperationControl,
    ) -> Result<RelationalDatabaseTransaction> {
        self.begin_inner(Some(control.clone()))
    }

    fn begin_inner(
        &self,
        control: Option<OperationControl>,
    ) -> Result<RelationalDatabaseTransaction> {
        if let Some(control) = &control {
            control.check()?;
        }
        let transaction = match &self.backend {
            Backend::Temporary(store) => TransactionBackend::Temporary(Box::new(store.begin()?)),
            Backend::Seer(store) => TransactionBackend::Seer(Box::new(store.begin()?)),
        };
        Ok(RelationalDatabaseTransaction {
            owner_id: self.handle_id,
            backend: transaction,
            control,
        })
    }

    /// Run one typed transaction against the selected backend.
    pub fn transaction<T, F>(&mut self, operation: F) -> Result<(T, CommitId)>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        let transaction = self.begin()?;
        self.run_transaction(transaction, operation)
    }

    /// Run one transaction with cooperative cancellation before durable
    /// publication.
    pub fn transaction_with_cancellation<T, F>(
        &mut self,
        cancellation: &CancellationToken,
        operation: F,
    ) -> Result<(T, CommitId)>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        let transaction = self.begin_with_cancellation(cancellation)?;
        self.run_transaction(transaction, operation)
    }

    /// Run one transaction with cooperative cancellation and an optional
    /// monotonic deadline.
    pub fn transaction_with_control<T, F>(
        &mut self,
        control: &OperationControl,
        operation: F,
    ) -> Result<(T, CommitId)>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        let transaction = self.begin_with_control(control)?;
        self.run_transaction(transaction, operation)
    }

    /// Run one typed transaction with a durable caller-owned attempt identity.
    ///
    /// If the attempt was already published, the closure is not run again and
    /// the durable commit record is returned explicitly. Callers can use that
    /// commit to read the resulting state after an ambiguous observation.
    pub fn transaction_with_attempt<T, F>(
        &mut self,
        attempt: TransactionAttemptId,
        operation: F,
    ) -> Result<TransactionAttemptOutcome<T>>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        self.transaction_with_attempt_inner(attempt, None, operation)
    }

    /// Run an attempt-aware transaction with cooperative cancellation and an
    /// optional monotonic deadline.
    pub fn transaction_with_attempt_and_control<T, F>(
        &mut self,
        attempt: TransactionAttemptId,
        control: &OperationControl,
        operation: F,
    ) -> Result<TransactionAttemptOutcome<T>>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        self.transaction_with_attempt_inner(attempt, Some(control), operation)
    }

    fn transaction_with_attempt_inner<T, F>(
        &mut self,
        attempt: TransactionAttemptId,
        control: Option<&OperationControl>,
        operation: F,
    ) -> Result<TransactionAttemptOutcome<T>>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        if let Some(control) = control {
            control.check()?;
        }
        if let Some(record) = self.resolve_attempt(attempt)? {
            self.record_event(
                RelationalEventKind::AttemptAlreadyCommitted,
                Some(record.commit),
            );
            return Ok(TransactionAttemptOutcome::AlreadyCommitted { record });
        }
        let transaction = match control {
            Some(control) => self.begin_with_control(control)?,
            None => self.begin()?,
        };
        self.run_transaction_with_attempt(transaction, operation, attempt)
    }

    fn run_transaction<T, F>(
        &mut self,
        mut transaction: RelationalDatabaseTransaction,
        operation: F,
    ) -> Result<(T, CommitId)>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        let value = operation(self, &mut transaction)?;
        let commit = if transaction.is_read_only() {
            transaction.ensure_active()?;
            transaction.snapshot()
        } else {
            transaction.commit(self)?
        };
        Ok((value, commit))
    }

    fn run_transaction_with_attempt<T, F>(
        &mut self,
        mut transaction: RelationalDatabaseTransaction,
        operation: F,
        attempt: TransactionAttemptId,
    ) -> Result<TransactionAttemptOutcome<T>>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        let value = operation(self, &mut transaction)?;
        let commit = if transaction.is_read_only() {
            transaction.ensure_active()?;
            transaction.snapshot()
        } else {
            transaction.commit_with_attempt(self, attempt)?
        };
        Ok(TransactionAttemptOutcome::Applied { value, commit })
    }

    pub fn insert(&mut self, table: TableId, row: Row) -> Result<CommitId> {
        self.commit_batch([RelationalMutation::Insert { table, row }])
    }

    pub fn update(&mut self, table: TableId, row: Row) -> Result<CommitId> {
        self.commit_batch([RelationalMutation::Update { table, row }])
    }

    /// Delete a legacy single-key row. Use [`Self::delete_row`] for composite
    /// primary-key tables.
    pub fn delete(&mut self, table: TableId, primary: Key) -> Result<CommitId> {
        self.commit_batch([RelationalMutation::Delete { table, primary }])
    }

    /// Delete a row by the catalog-owned identity encoded in its values.
    pub fn delete_row(&mut self, table: TableId, row: Row) -> Result<CommitId> {
        self.commit_batch([RelationalMutation::DeleteRow { table, row }])
    }

    pub fn commit_batch(
        &mut self,
        mutations: impl IntoIterator<Item = RelationalMutation>,
    ) -> Result<CommitId> {
        let mut transaction = self.begin()?;
        for mutation in mutations {
            transaction.stage(self, mutation)?;
        }
        transaction.commit(self)
    }

    pub fn commit_batch_with_attempt(
        &mut self,
        mutations: impl IntoIterator<Item = RelationalMutation>,
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        let mut transaction = self.begin()?;
        for mutation in mutations {
            transaction.stage(self, mutation)?;
        }
        transaction.commit_with_attempt(self, attempt)
    }

    pub fn resolve_attempt(
        &self,
        attempt: TransactionAttemptId,
    ) -> Result<Option<crate::AttemptRecord>> {
        match &self.backend {
            Backend::Temporary(store) => store.resolve_attempt(attempt),
            Backend::Seer(store) => store.resolve_attempt(attempt),
        }
    }

    pub fn forget_attempts(&mut self, attempts: &[TransactionAttemptId]) -> Result<usize> {
        let result = match &mut self.backend {
            Backend::Temporary(store) => store.forget_attempts(attempts),
            Backend::Seer(store) => store.forget_attempts(attempts),
        };
        if let Ok(count) = result.as_ref()
            && *count != 0
        {
            self.record_event(
                RelationalEventKind::AttemptsForgotten,
                Some(self.commit_id()),
            );
        }
        result
    }

    /// Publish the current backend state through its checkpoint protocol and
    /// return the before/after lifecycle frontier.
    pub fn checkpoint(&mut self) -> Result<RelationalCheckpointReport> {
        let before = self.status()?;
        let (after, verified_physical_pages, data_bytes, blob_bytes, wal_bytes, reclaimable_pages) =
            match &mut self.backend {
                Backend::Temporary(store) => {
                    store.checkpoint()?;
                    (temporary_status(store), None, None, None, None, None)
                }
                Backend::Seer(store) => {
                    let report = store.checkpoint_with_status()?;
                    (
                        seer_status(store, report.durability),
                        Some(report.physical.verified_pages),
                        Some(report.physical.data_bytes),
                        Some(report.physical.blob_bytes),
                        Some(report.physical.wal_bytes),
                        Some(report.physical.reclaimable_pages),
                    )
                }
            };
        let report = RelationalCheckpointReport {
            before,
            after,
            verified_physical_pages,
            data_bytes,
            blob_bytes,
            wal_bytes,
            reclaimable_pages,
        };
        self.record_event(
            RelationalEventKind::CheckpointCompleted,
            Some(report.after.commit),
        );
        Ok(report)
    }

    /// Run unbounded reclaim and return the work observed by the backend.
    pub fn compact(&mut self) -> Result<RelationalCompactionReport> {
        self.compact_with_budget(RelationalCompactionBudget::unlimited())
    }

    /// Run one backend-neutral bounded reclaim pass.
    pub fn compact_with_budget(
        &mut self,
        budget: RelationalCompactionBudget,
    ) -> Result<RelationalCompactionReport> {
        let before = self.status()?;
        let (work, after) = match &mut self.backend {
            Backend::Temporary(store) => {
                let report = store.compact_with_key_budget(budget.max_work_units)?;
                (
                    RelationalCompactionWork {
                        row_keys_considered: Some(count_as_u64(report.row_keys_considered)?),
                        index_keys_considered: Some(count_as_u64(report.index_keys_considered)?),
                        row_fragments_reclaimed: Some(count_as_u64(
                            report.row_fragments_reclaimed,
                        )?),
                        index_fragments_reclaimed: Some(count_as_u64(
                            report.index_fragments_reclaimed,
                        )?),
                        data_bytes_before: None,
                        data_bytes_after: None,
                        reclaimed_pages: None,
                        relocated_pages: None,
                    },
                    temporary_status(store),
                )
            }
            Backend::Seer(store) => {
                let report = if budget.max_work_units == usize::MAX {
                    store.compact_with_status()?
                } else {
                    store.compact_with_limit_status(budget.max_work_units)?
                };
                (
                    RelationalCompactionWork {
                        row_keys_considered: None,
                        index_keys_considered: None,
                        row_fragments_reclaimed: None,
                        index_fragments_reclaimed: None,
                        data_bytes_before: Some(report.physical.data_bytes_before),
                        data_bytes_after: Some(report.physical.data_bytes_after),
                        reclaimed_pages: Some(report.physical.reclaimed_pages),
                        relocated_pages: Some(report.physical.relocated_pages),
                    },
                    seer_status(store, report.durability),
                )
            }
        };
        let report = RelationalCompactionReport {
            budget,
            work_units_consumed: match &self.backend {
                Backend::Temporary(_) => work
                    .row_keys_considered
                    .unwrap_or_default()
                    .saturating_add(work.index_keys_considered.unwrap_or_default()),
                Backend::Seer(_) => work.relocated_pages.unwrap_or_default(),
            },
            before,
            after,
            row_keys_considered: work.row_keys_considered,
            index_keys_considered: work.index_keys_considered,
            row_fragments_reclaimed: work.row_fragments_reclaimed,
            index_fragments_reclaimed: work.index_fragments_reclaimed,
            data_bytes_before: work.data_bytes_before,
            data_bytes_after: work.data_bytes_after,
            reclaimed_pages: work.reclaimed_pages,
            relocated_pages: work.relocated_pages,
        };
        self.record_event(
            RelationalEventKind::CompactionCompleted,
            Some(report.after.commit),
        );
        Ok(report)
    }

    /// Run a read-only logical integrity check and, for SeerDB, its physical
    /// manifest/page/blob/WAL verification. This call does not publish,
    /// reclaim, or repair state.
    pub fn verify(&mut self) -> Result<RelationalVerificationReport> {
        let result = match &mut self.backend {
            Backend::Temporary(store) => {
                let logical = {
                    store.verify()?;
                    store.verify_logical()?
                };
                Ok(verification_report(
                    RelationalBackendKind::Temporary,
                    store.commit_id(),
                    logical,
                    None,
                    None,
                    None,
                    None,
                ))
            }
            Backend::Seer(store) => {
                let physical = store.verify()?;
                let logical = store.verify_logical()?;
                Ok(verification_report(
                    RelationalBackendKind::Seer,
                    store.commit_id(),
                    logical,
                    Some(physical.verified_pages),
                    Some(physical.data_bytes),
                    Some(physical.blob_bytes),
                    Some(physical.wal_bytes),
                ))
            }
        };
        if let Ok(report) = result.as_ref() {
            self.record_event(
                RelationalEventKind::VerificationCompleted,
                Some(report.commit),
            );
        }
        result
    }

    /// Return common diagnostic counters without exposing a physical engine
    /// type at the project-facing boundary.
    pub fn metrics(&self) -> Result<RelationalMetrics> {
        let metrics = match &self.backend {
            Backend::Temporary(store) => {
                let metrics = store.metrics()?;
                RelationalMetrics {
                    backend: RelationalBackendKind::Temporary,
                    commit: store.commit_id(),
                    wal_bytes: Some(metrics.wal_bytes),
                    syncs: Some(metrics.syncs),
                    logical_page_reads: None,
                    physical_page_reads: None,
                    physical_page_writes: None,
                    data_bytes: None,
                    blob_bytes: None,
                    publication: None,
                }
            }
            Backend::Seer(store) => {
                let metrics = store.metrics()?;
                RelationalMetrics {
                    backend: RelationalBackendKind::Seer,
                    commit: store.commit_id(),
                    wal_bytes: Some(metrics.wal_bytes),
                    syncs: Some(metrics.storage.syncs),
                    logical_page_reads: Some(metrics.storage.logical_page_reads),
                    physical_page_reads: Some(metrics.storage.physical_page_reads),
                    physical_page_writes: Some(metrics.storage.physical_page_writes),
                    data_bytes: Some(metrics.data_bytes),
                    blob_bytes: Some(metrics.blob_bytes),
                    publication: Some(RelationalPublicationMetrics {
                        wal_bytes_written: metrics.publication.wal_bytes_written,
                        data_bytes_written: metrics.storage.page_bytes_written,
                        metadata_bytes_written: metrics.publication.metadata_bytes_written,
                        blob_bytes_written: metrics.publication.blob_bytes_written,
                        history_bytes_written: metrics.publication.history_bytes_written,
                        manifest_bytes_written: metrics.publication.manifest_bytes_written,
                        candidate_prepare_ns: metrics.publication_timing.candidate_prepare_ns,
                        wal_write_ns: metrics.publication_timing.wal_write_ns,
                        admission_ns: metrics.publication_timing.admission_ns,
                        data_flush_ns: metrics.publication_timing.data_flush_ns,
                        metadata_write_ns: metrics.publication_timing.metadata_write_ns,
                        blob_write_ns: metrics.publication_timing.blob_write_ns,
                        history_write_ns: metrics.publication_timing.history_write_ns,
                        directory_sync_ns: metrics.publication_timing.directory_sync_ns,
                        manifest_write_ns: metrics.publication_timing.manifest_write_ns,
                        manifest_mirror_ns: metrics.publication_timing.manifest_mirror_ns,
                        cleanup_ns: metrics.publication_timing.cleanup_ns,
                    }),
                }
            }
        };
        Ok(metrics)
    }

    pub fn get(&self, table: TableId, snapshot: CommitId, primary: Key) -> Result<Option<Row>> {
        match &self.backend {
            Backend::Temporary(store) => store.get(table, snapshot, primary),
            Backend::Seer(store) => store.get(table, snapshot, primary),
        }
    }

    /// Look up a row through the catalog-owned composite primary-key identity.
    pub fn get_by_identity(
        &self,
        table: TableId,
        snapshot: CommitId,
        identity: &RowIdentity,
    ) -> Result<Option<Row>> {
        match &self.backend {
            Backend::Temporary(store) => store.get_by_identity(table, snapshot, identity),
            Backend::Seer(store) => store.get_by_identity(table, snapshot, identity),
        }
    }

    pub fn scan(&self, table: TableId, snapshot: CommitId, limit: usize) -> Result<Vec<Row>> {
        match &self.backend {
            Backend::Temporary(store) => store.scan(table, snapshot, limit),
            Backend::Seer(store) => store.scan(table, snapshot, limit),
        }
    }

    pub fn index_get(
        &self,
        table: TableId,
        snapshot: CommitId,
        index: crate::IndexId,
        values: &[Value],
    ) -> Result<Vec<Row>> {
        match &self.backend {
            Backend::Temporary(store) => store.index_get(table, snapshot, index, values),
            Backend::Seer(store) => store.index_get(table, snapshot, index, values),
        }
    }

    pub fn index_scan(
        &self,
        table: TableId,
        snapshot: CommitId,
        index: crate::IndexId,
        start: Option<&[Value]>,
        end: Option<&[Value]>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        match &self.backend {
            Backend::Temporary(store) => {
                store.index_scan(table, snapshot, index, start, end, limit)
            }
            Backend::Seer(store) => store.index_scan(table, snapshot, index, start, end, limit),
        }
    }

    /// Retain a historical snapshot using the selected backend's native
    /// retention mechanism. Release the returned token when the read is done.
    pub fn retain(&mut self, snapshot: CommitId) -> Result<RelationalSnapshotLease> {
        let lease = match &mut self.backend {
            Backend::Temporary(store) => {
                store.retain(snapshot)?;
                SnapshotLeaseBackend::Temporary(snapshot)
            }
            Backend::Seer(store) => SnapshotLeaseBackend::Seer(store.retain(snapshot)?),
        };
        let lease = RelationalSnapshotLease {
            owner_id: self.handle_id,
            backend: lease,
        };
        self.record_event(RelationalEventKind::SnapshotRetained, Some(snapshot));
        Ok(lease)
    }

    /// Retain the current published root atomically with the selected
    /// backend's current-frontier observation.
    pub fn retain_current(&mut self) -> Result<RelationalSnapshotLease> {
        let backend = match &mut self.backend {
            Backend::Temporary(store) => {
                let snapshot = store.commit_id();
                store.retain(snapshot)?;
                SnapshotLeaseBackend::Temporary(snapshot)
            }
            Backend::Seer(store) => SnapshotLeaseBackend::Seer(store.retain_current()?),
        };
        let snapshot = match &backend {
            SnapshotLeaseBackend::Temporary(snapshot) => *snapshot,
            SnapshotLeaseBackend::Seer(lease) => lease.commit(),
        };
        let lease = RelationalSnapshotLease {
            owner_id: self.handle_id,
            backend,
        };
        self.record_event(RelationalEventKind::SnapshotRetained, Some(snapshot));
        Ok(lease)
    }

    /// Release a lease created by this handle.
    pub fn release(&mut self, lease: RelationalSnapshotLease) -> Result<()> {
        if lease.owner_id != self.handle_id {
            return Err(invalid_transaction_owner());
        }
        let snapshot = match &lease.backend {
            SnapshotLeaseBackend::Temporary(snapshot) => *snapshot,
            SnapshotLeaseBackend::Seer(lease) => lease.commit(),
        };
        let result = match (&mut self.backend, lease.backend) {
            (Backend::Temporary(store), SnapshotLeaseBackend::Temporary(snapshot)) => {
                store.release(snapshot)?;
                Ok(())
            }
            (Backend::Seer(store), SnapshotLeaseBackend::Seer(lease)) => store.release(lease),
            _ => Err(invalid_transaction_owner()),
        };
        if result.is_ok() {
            self.record_event(RelationalEventKind::SnapshotReleased, Some(snapshot));
        }
        result
    }
}

/// A historical retention token returned by [`RelationalDatabase`].
///
/// The token must be released through the originating database handle. The
/// SeerDB token also releases itself when dropped, but explicit release keeps
/// the lifecycle identical for the temporary backend.
#[derive(Debug)]
pub struct RelationalSnapshotLease {
    owner_id: u64,
    backend: SnapshotLeaseBackend,
}

impl RelationalSnapshotLease {
    /// Return the logical commit held by this lease.
    #[must_use]
    pub fn snapshot(&self) -> CommitId {
        match &self.backend {
            SnapshotLeaseBackend::Temporary(snapshot) => *snapshot,
            SnapshotLeaseBackend::Seer(lease) => lease.commit(),
        }
    }
}

#[derive(Debug)]
enum SnapshotLeaseBackend {
    Temporary(CommitId),
    Seer(SnapshotLease),
}

pub struct RelationalDatabaseTransaction {
    owner_id: u64,
    backend: TransactionBackend,
    control: Option<OperationControl>,
}

enum TransactionBackend {
    Temporary(Box<crate::RelationalTransaction>),
    Seer(Box<SeerRelationalTransaction>),
}

impl RelationalDatabaseTransaction {
    /// Attach a caller-selected idempotency identity to this transaction.
    /// Only the Seer backend publishes durable attempt records; the
    /// in-memory backend ignores the identity.
    pub(crate) fn set_attempt(&mut self, attempt: TransactionAttemptId) {
        if let TransactionBackend::Seer(transaction) = &mut self.backend {
            transaction.set_attempt(attempt);
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> CommitId {
        match &self.backend {
            TransactionBackend::Temporary(transaction) => transaction.snapshot(),
            TransactionBackend::Seer(transaction) => transaction.snapshot(),
        }
    }

    pub(crate) fn is_read_only(&self) -> bool {
        match &self.backend {
            TransactionBackend::Temporary(transaction) => transaction.is_read_only(),
            TransactionBackend::Seer(transaction) => transaction.is_read_only(),
        }
    }

    fn ensure_owner(&self, store: &RelationalDatabase) -> Result<()> {
        if self.owner_id == store.handle_id {
            Ok(())
        } else {
            Err(invalid_transaction_owner())
        }
    }

    fn ensure_active(&self) -> Result<()> {
        if let Some(control) = &self.control {
            control.check()?;
        }
        Ok(())
    }

    fn stage(&mut self, store: &RelationalDatabase, mutation: RelationalMutation) -> Result<()> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        match (&mut self.backend, &store.backend) {
            (TransactionBackend::Temporary(transaction), Backend::Temporary(store)) => {
                stage_temporary(transaction, store, mutation)
            }
            (TransactionBackend::Seer(transaction), Backend::Seer(store)) => {
                stage_seer(transaction, store, mutation)
            }
            _ => Err(invalid_transaction_owner()),
        }
    }

    pub fn insert(&mut self, store: &RelationalDatabase, table: TableId, row: Row) -> Result<()> {
        self.stage(store, RelationalMutation::Insert { table, row })
    }

    pub fn update(&mut self, store: &RelationalDatabase, table: TableId, row: Row) -> Result<()> {
        self.stage(store, RelationalMutation::Update { table, row })
    }

    /// Stage a legacy single-key delete. Use [`Self::delete_row`] for
    /// composite primary-key tables.
    pub fn delete(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        primary: Key,
    ) -> Result<()> {
        self.stage(store, RelationalMutation::Delete { table, primary })
    }

    pub fn delete_row(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        row: Row,
    ) -> Result<()> {
        self.stage(store, RelationalMutation::DeleteRow { table, row })
    }

    pub fn get(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        primary: Key,
    ) -> Result<Option<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        match (&mut self.backend, &store.backend) {
            (TransactionBackend::Temporary(transaction), Backend::Temporary(store)) => {
                transaction.get(store, table, primary)
            }
            (TransactionBackend::Seer(transaction), Backend::Seer(store)) => {
                transaction.get(store, table, primary)
            }
            _ => Err(invalid_transaction_owner()),
        }
    }

    /// Look up a row through the catalog-owned composite primary-key identity,
    /// including staged transaction mutations.
    pub fn get_by_identity(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        identity: &RowIdentity,
    ) -> Result<Option<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        match (&mut self.backend, &store.backend) {
            (TransactionBackend::Temporary(transaction), Backend::Temporary(store)) => {
                transaction.get_by_identity(store, table, identity)
            }
            (TransactionBackend::Seer(transaction), Backend::Seer(store)) => {
                transaction.get_by_identity(store, table, identity)
            }
            _ => Err(invalid_transaction_owner()),
        }
    }

    pub fn scan(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        limit: usize,
    ) -> Result<Vec<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        match (&mut self.backend, &store.backend) {
            (TransactionBackend::Temporary(transaction), Backend::Temporary(store)) => {
                transaction.scan(store, table, limit)
            }
            (TransactionBackend::Seer(transaction), Backend::Seer(store)) => {
                transaction.scan(store, table, limit)
            }
            _ => Err(invalid_transaction_owner()),
        }
    }

    /// Execute one bounded embedded SQL query or DML statement inside this
    /// existing typed transaction. Transaction-control SQL is refused; the
    /// caller owns the surrounding typed transaction closure.
    pub fn execute_sql(
        &mut self,
        store: &RelationalDatabase,
        sql: &str,
    ) -> Result<crate::SqlResult> {
        crate::sql::execute_in_transaction(store, self, sql)
    }

    /// Execute one bounded embedded SQL statement with explicit positional
    /// parameters inside this existing typed transaction.
    pub fn execute_sql_with_params(
        &mut self,
        store: &RelationalDatabase,
        sql: &str,
        params: &[crate::Value],
    ) -> Result<crate::SqlResult> {
        crate::sql::execute_in_transaction_with_params(store, self, sql, params)
    }

    /// Execute an analytical query inside this transaction.
    pub fn query_analytical(
        &mut self,
        store: &RelationalDatabase,
        query: &crate::morsel::AnalyticalQuery,
    ) -> Result<crate::morsel::AnalyticalResult> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        let control = self.control.clone().unwrap_or_default();
        crate::morsel::AnalyticalExecutor::execute(store, self, query, &control)
    }

    pub fn index_get(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        index: crate::IndexId,
        values: &[Value],
    ) -> Result<Vec<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        match (&mut self.backend, &store.backend) {
            (TransactionBackend::Temporary(transaction), Backend::Temporary(store)) => {
                transaction.index_get(store, table, index, values)
            }
            (TransactionBackend::Seer(transaction), Backend::Seer(store)) => {
                transaction.index_get(store, table, index, values)
            }
            _ => Err(invalid_transaction_owner()),
        }
    }

    pub fn index_scan(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        index: crate::IndexId,
        start: Option<&[Value]>,
        end: Option<&[Value]>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        match (&mut self.backend, &store.backend) {
            (TransactionBackend::Temporary(transaction), Backend::Temporary(store)) => {
                transaction.index_scan(store, table, index, start, end, limit)
            }
            (TransactionBackend::Seer(transaction), Backend::Seer(store)) => {
                transaction.index_scan(store, table, index, start, end, limit)
            }
            _ => Err(invalid_transaction_owner()),
        }
    }

    pub fn commit(self, store: &mut RelationalDatabase) -> Result<CommitId> {
        self.commit_inner(store, None)
    }

    pub fn commit_with_attempt(
        self,
        store: &mut RelationalDatabase,
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        self.commit_inner(store, Some(attempt))
    }

    pub fn commit_validated(self, store: &mut RelationalDatabase) -> Result<CommitId> {
        self.commit_validated_inner(store, None)
    }

    pub fn commit_validated_with_attempt(
        self,
        store: &mut RelationalDatabase,
        attempt: TransactionAttemptId,
    ) -> Result<CommitId> {
        self.commit_validated_inner(store, Some(attempt))
    }

    fn commit_inner(
        self,
        store: &mut RelationalDatabase,
        attempt: Option<TransactionAttemptId>,
    ) -> Result<CommitId> {
        if self.owner_id != store.handle_id {
            return Err(invalid_transaction_owner());
        }
        self.ensure_active()?;
        if self.is_read_only() {
            return Ok(self.snapshot());
        }
        let result = match (self.backend, &mut store.backend, attempt) {
            (TransactionBackend::Temporary(transaction), Backend::Temporary(store), None) => {
                transaction.commit(store)
            }
            (
                TransactionBackend::Temporary(transaction),
                Backend::Temporary(store),
                Some(attempt),
            ) => transaction.commit_with_attempt(store, attempt),
            (TransactionBackend::Seer(transaction), Backend::Seer(store), None) => {
                transaction.commit(store)
            }
            (TransactionBackend::Seer(transaction), Backend::Seer(store), Some(attempt)) => {
                transaction.commit_with_attempt(store, attempt)
            }
            _ => Err(invalid_transaction_owner()),
        };
        store.record_commit_result(result)
    }

    fn commit_validated_inner(
        self,
        store: &mut RelationalDatabase,
        attempt: Option<TransactionAttemptId>,
    ) -> Result<CommitId> {
        if self.owner_id != store.handle_id {
            return Err(invalid_transaction_owner());
        }
        self.ensure_active()?;
        if self.is_read_only() {
            return Ok(self.snapshot());
        }
        let result = match (self.backend, &mut store.backend, attempt) {
            (TransactionBackend::Temporary(transaction), Backend::Temporary(store), None) => {
                transaction.commit_validated(store)
            }
            (
                TransactionBackend::Temporary(transaction),
                Backend::Temporary(store),
                Some(attempt),
            ) => transaction.commit_validated_with_attempt(store, attempt),
            (TransactionBackend::Seer(transaction), Backend::Seer(store), None) => {
                transaction.commit_validated(store)
            }
            (TransactionBackend::Seer(transaction), Backend::Seer(store), Some(attempt)) => {
                transaction.commit_validated_with_attempt(store, attempt)
            }
            _ => Err(invalid_transaction_owner()),
        };
        store.record_commit_result(result)
    }
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(DbError::Cancelled)
    } else {
        Ok(())
    }
}

fn stage_temporary(
    transaction: &mut crate::RelationalTransaction,
    store: &RelationalStore,
    mutation: RelationalMutation,
) -> Result<()> {
    match mutation {
        RelationalMutation::Insert { table, row } => transaction.insert(store, table, row),
        RelationalMutation::Update { table, row } => transaction.update(store, table, row),
        RelationalMutation::Delete { table, primary } => transaction.delete(store, table, primary),
        RelationalMutation::DeleteRow { table, row } => transaction.delete_row(store, table, row),
    }
}

fn stage_seer(
    transaction: &mut SeerRelationalTransaction,
    store: &SeerRelationalStore,
    mutation: RelationalMutation,
) -> Result<()> {
    match mutation {
        RelationalMutation::Insert { table, row } => transaction.insert(store, table, row),
        RelationalMutation::Update { table, row } => transaction.update(store, table, row),
        RelationalMutation::Delete { table, primary } => transaction.delete(store, table, primary),
        RelationalMutation::DeleteRow { table, row } => transaction.delete_row(store, table, row),
    }
}

fn invalid_transaction_owner() -> DbError {
    DbError::InvalidState("transaction or snapshot lease belongs to another database handle".into())
}

fn normalize_snapshot_selection(
    snapshots: &[CommitId],
    source_head: CommitId,
    retained: &[CommitId],
) -> Result<Vec<CommitId>> {
    if snapshots.is_empty() {
        return Err(DbError::InvalidState(
            "snapshot capture requires at least one selected commit".to_owned(),
        ));
    }
    let mut selected = snapshots.to_vec();
    selected.sort_unstable();
    for pair in selected.windows(2) {
        if pair[0] == pair[1] {
            return Err(DbError::InvalidState(format!(
                "snapshot {} was selected more than once",
                pair[0].0
            )));
        }
    }
    for snapshot in &selected {
        if snapshot.0 > source_head.0
            || (*snapshot != source_head && retained.binary_search(snapshot).is_err())
        {
            return Err(DbError::SnapshotUnavailable(snapshot.0));
        }
    }
    Ok(selected)
}

fn release_capture_leases(
    database: &mut RelationalDatabase,
    leases: Vec<RelationalSnapshotLease>,
) -> Result<()> {
    let mut first_error = None;
    for lease in leases {
        if let Err(error) = database.release(lease)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn lifecycle_state(write_fenced: bool) -> RelationalLifecycleState {
    if write_fenced {
        RelationalLifecycleState::RecoveryRequired
    } else {
        RelationalLifecycleState::Ready
    }
}

fn temporary_status(store: &RelationalStore) -> RelationalDatabaseStatus {
    let write_fenced = store.requires_recovery();
    RelationalDatabaseStatus {
        backend: RelationalBackendKind::Temporary,
        state: lifecycle_state(write_fenced),
        commit: store.commit_id(),
        catalog_generation: store.catalog().generation(),
        generation: Some(store.generation()),
        pending_mutations: None,
        write_fenced,
        retained_snapshots: Some(store.retained_snapshot_count() as u64),
    }
}

fn seer_status(store: &SeerRelationalStore, status: DurabilityStatus) -> RelationalDatabaseStatus {
    RelationalDatabaseStatus {
        backend: RelationalBackendKind::Seer,
        state: lifecycle_state(status.write_fenced),
        commit: status.commit,
        catalog_generation: store.catalog().generation(),
        generation: Some(status.generation),
        pending_mutations: Some(status.pending_mutations),
        write_fenced: status.write_fenced,
        retained_snapshots: Some(store.retained_snapshot_count() as u64),
    }
}

fn count_as_u64(count: usize) -> Result<u64> {
    u64::try_from(count)
        .map_err(|_| DbError::InvalidState("maintenance count exceeds u64".to_owned()))
}

fn verification_report(
    backend: RelationalBackendKind,
    commit: CommitId,
    logical: LogicalVerification,
    physical_pages: Option<u64>,
    data_bytes: Option<u64>,
    blob_bytes: Option<u64>,
    wal_bytes: Option<u64>,
) -> RelationalVerificationReport {
    RelationalVerificationReport {
        backend,
        commit,
        catalog_generation: logical.catalog_generation,
        verified_tables: logical.table_count as u64,
        verified_indexes: logical.index_count as u64,
        verified_rows: logical.row_count as u64,
        verified_index_entries: logical.index_entry_count as u64,
        physical_pages,
        data_bytes,
        blob_bytes,
        wal_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, IndexId, TableDefinition};
    use std::path::Path;
    use tempfile::TempDir;

    fn table() -> TableDefinition {
        TableDefinition {
            id: TableId(7),
            name: "users".to_owned(),
            columns: vec![ColumnDefinition {
                id: ColumnId(1),
                name: "name".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            }],
        }
    }

    fn row(primary: u64, name: &str) -> Row {
        Row {
            primary: Key::new(7, primary),
            values: vec![Value::Text(name.to_owned())],
        }
    }

    fn config(kind: RelationalBackendKind, directory: &Path) -> RelationalBackendConfig {
        match kind {
            RelationalBackendKind::Temporary => {
                RelationalBackendConfig::Temporary(DatabaseConfig {
                    directory: directory.to_owned(),
                })
            }
            RelationalBackendKind::Seer => {
                RelationalBackendConfig::Seer(SeerKernelConfig::new(directory.to_owned()))
            }
        }
    }

    fn exercise(kind: RelationalBackendKind, directory: &Path) {
        let backend_config = config(kind, directory);
        let mut database = RelationalDatabase::create(backend_config.clone()).expect("create");
        assert_eq!(database.backend(), kind);
        let table_commit = database.create_table(table()).expect("table");
        assert_eq!(table_commit, CommitId(1));

        let (visible, commit) = database
            .transaction(|database, transaction| {
                transaction.insert(database, TableId(7), row(1, "alice"))?;
                transaction.get(database, TableId(7), Key::new(7, 1))
            })
            .expect("write transaction");
        assert_eq!(visible, Some(row(1, "alice")));
        assert_eq!(commit, CommitId(2));

        let (count, read_commit) = database
            .transaction(|database, transaction| {
                transaction
                    .scan(database, TableId(7), 10)
                    .map(|rows| rows.len())
            })
            .expect("read transaction");
        assert_eq!(count, 1);
        assert_eq!(read_commit, commit);
        assert_eq!(database.commit_id(), commit);
        let metrics = database.metrics().expect("metrics");
        assert_eq!(metrics.backend, kind);
        assert_eq!(metrics.commit, commit);
        database.checkpoint().expect("checkpoint");
        database.compact().expect("compact");
        assert_eq!(
            database
                .get(TableId(7), commit, Key::new(7, 1))
                .expect("row"),
            Some(row(1, "alice"))
        );

        drop(database);
        let reopened = RelationalDatabase::open(backend_config).expect("reopen");
        assert_eq!(reopened.backend(), kind);
        assert_eq!(reopened.commit_id(), commit);
        assert_eq!(
            reopened
                .scan(TableId(7), commit, 10)
                .expect("reopened rows"),
            vec![row(1, "alice")]
        );
    }

    fn exercise_transaction_profile(kind: RelationalBackendKind, directory: &Path) {
        let mut database = RelationalDatabase::create(config(kind, directory)).expect("create");
        assert_eq!(
            database.transaction_profile(),
            TransactionProfile::FixedSnapshotSerializedWriter
        );
        database.create_table(table()).expect("table");
        database
            .insert(TableId(7), row(1, "alice"))
            .expect("row one");
        database.insert(TableId(7), row(2, "bob")).expect("row two");

        let mut reader = database.begin().expect("reader begin");
        assert_eq!(
            reader
                .get(&database, TableId(7), Key::new(7, 1))
                .expect("initial read"),
            Some(row(1, "alice"))
        );
        database
            .update(TableId(7), row(1, "carol"))
            .expect("later update");
        assert_eq!(
            reader
                .get(&database, TableId(7), Key::new(7, 1))
                .expect("repeatable read"),
            Some(row(1, "alice"))
        );
        assert_eq!(
            reader
                .scan(&database, TableId(7), 10)
                .expect("snapshot scan"),
            vec![row(1, "alice"), row(2, "bob")]
        );
        drop(reader);

        // Both writers observe the same invariant before changing different
        // rows. The first commit succeeds; the second is rejected rather
        // than creating a write-skew history or backend-dependent merge.
        let mut first = database.begin().expect("first begin");
        let mut second = database.begin().expect("second begin");
        assert_eq!(first.snapshot(), second.snapshot());
        assert_eq!(
            first.scan(&database, TableId(7), 10).expect("first read"),
            vec![row(1, "carol"), row(2, "bob")]
        );
        assert_eq!(
            second.scan(&database, TableId(7), 10).expect("second read"),
            vec![row(1, "carol"), row(2, "bob")]
        );
        first
            .update(&database, TableId(7), row(1, "first"))
            .expect("first update");
        second
            .update(&database, TableId(7), row(2, "second"))
            .expect("second update");
        let first_commit = first.commit(&mut database).expect("first commit");
        assert!(matches!(
            second.commit(&mut database),
            Err(DbError::SerializationConflict { snapshot, current })
                if snapshot == first_commit.0 - 1 && current == first_commit.0
        ));
        assert_eq!(
            database
                .scan(TableId(7), first_commit, 10)
                .expect("committed rows"),
            vec![row(1, "first"), row(2, "bob")]
        );
    }

    fn exercise_verification(kind: RelationalBackendKind, directory: &Path) {
        let config = config(kind, directory);
        let mut database = RelationalDatabase::create(config.clone()).expect("create");
        database.create_table(table()).expect("table");
        database
            .create_index(IndexDefinition {
                id: IndexId(11),
                table: TableId(7),
                columns: vec![ColumnId(1)],
                unique: false,
            })
            .expect("index");
        database
            .insert(TableId(7), row(1, "alice"))
            .expect("row one");
        database.insert(TableId(7), row(2, "bob")).expect("row two");
        let commit = database.commit_id();

        let report = database.verify().expect("verify");
        assert_eq!(report.backend, kind);
        assert_eq!(report.commit, commit);
        assert_eq!(report.catalog_generation, 2);
        assert_eq!(report.verified_tables, 1);
        assert_eq!(report.verified_indexes, 1);
        assert_eq!(report.verified_rows, 2);
        assert_eq!(report.verified_index_entries, 2);
        assert_eq!(database.commit_id(), commit);
        match kind {
            RelationalBackendKind::Temporary => assert_eq!(report.physical_pages, None),
            RelationalBackendKind::Seer => assert!(report.physical_pages.is_some()),
        }

        drop(database);
        let mut reopened = RelationalDatabase::open(config).expect("reopen");
        assert_eq!(reopened.verify().expect("reopened verify"), report);
    }

    fn exercise_status(kind: RelationalBackendKind, directory: &Path) {
        let config = config(kind, directory);
        let mut database = RelationalDatabase::create(config).expect("create");
        let initial = database.status().expect("initial status");
        assert_eq!(initial.backend, kind);
        assert_eq!(initial.state, RelationalLifecycleState::Ready);
        assert_eq!(initial.commit, CommitId(0));
        assert_eq!(initial.catalog_generation, 0);
        assert_eq!(initial.generation, Some(0));
        assert!(!initial.write_fenced);
        assert_eq!(initial.retained_snapshots, Some(0));
        match kind {
            RelationalBackendKind::Temporary => assert_eq!(initial.pending_mutations, None),
            RelationalBackendKind::Seer => assert_eq!(initial.pending_mutations, Some(0)),
        }
        let initial_diagnostic = database.diagnose().expect("initial diagnostic");
        assert_eq!(initial_diagnostic.backend, kind);
        assert_eq!(initial_diagnostic.status, initial);
        assert_eq!(initial_diagnostic.metrics.commit, CommitId(0));
        assert!(!initial_diagnostic.has_errors());
        assert!(initial_diagnostic.findings.iter().any(|finding| {
            finding.code == RelationalDiagnosticCode::Ready
                && finding.severity == RelationalDiagnosticSeverity::Info
        }));

        database.create_table(table()).expect("table");
        let commit = database.insert(TableId(7), row(1, "alice")).expect("row");
        let lease = database.retain(commit).expect("retain");
        let second_lease = database.retain(commit).expect("retain again");
        let active = database.status().expect("active status");
        assert_eq!(active.state, RelationalLifecycleState::Ready);
        assert_eq!(active.commit, commit);
        assert_eq!(active.catalog_generation, 1);
        assert_eq!(active.retained_snapshots, Some(1));
        assert!(!active.write_fenced);
        let active_diagnostic = database.diagnose().expect("active diagnostic");
        assert_eq!(active_diagnostic.status, active);
        assert!(active_diagnostic.findings.iter().any(|finding| {
            finding.code == RelationalDiagnosticCode::RetainedSnapshots
                && finding.value == Some(1)
                && finding.component == RelationalDiagnosticComponent::Retention
        }));
        database.release(lease).expect("release");
        assert_eq!(
            database
                .status()
                .expect("partially released status")
                .retained_snapshots,
            Some(1)
        );
        database.release(second_lease).expect("release again");
        assert_eq!(
            database
                .status()
                .expect("released status")
                .retained_snapshots,
            Some(0)
        );
    }

    fn exercise_writer_exclusion(kind: RelationalBackendKind, directory: &Path) {
        let config = config(kind, directory);
        let first = RelationalDatabase::create(config.clone()).expect("first create");
        assert!(matches!(
            RelationalDatabase::open(config.clone()),
            Err(DbError::StorageBusy {
                operation: "open",
                ..
            })
        ));

        drop(first);
        RelationalDatabase::open(config).expect("reopen after drop");
    }

    fn exercise_close(kind: RelationalBackendKind, directory: &Path) {
        let config = config(kind, directory);
        let mut database = RelationalDatabase::create(config.clone()).expect("create");
        database.create_table(table()).expect("table");
        let commit = database.insert(TableId(7), row(1, "alice")).expect("row");

        database.close().expect("close");

        let reopened = RelationalDatabase::open(config).expect("reopen after close");
        assert_eq!(reopened.commit_id(), commit);
        assert_eq!(
            reopened
                .get(TableId(7), commit, Key::new(7, 1))
                .expect("reopened row"),
            Some(row(1, "alice"))
        );
    }

    fn exercise_maintenance_reports(kind: RelationalBackendKind, directory: &Path) {
        let mut database = RelationalDatabase::create(config(kind, directory)).expect("create");
        database.create_table(table()).expect("table");
        database.insert(TableId(7), row(1, "alice")).expect("row");

        let before_checkpoint = database.status().expect("checkpoint status");
        let checkpoint = database.checkpoint().expect("checkpoint");
        assert_eq!(checkpoint.before, before_checkpoint);
        assert_eq!(
            checkpoint.after,
            database.status().expect("after checkpoint status")
        );
        assert_eq!(checkpoint.after.commit, before_checkpoint.commit);
        assert!(
            checkpoint.after.generation.expect("after generation")
                >= before_checkpoint.generation.expect("before generation")
        );
        match kind {
            RelationalBackendKind::Temporary => {
                assert_eq!(checkpoint.verified_physical_pages, None);
                assert_eq!(checkpoint.data_bytes, None);
                assert_eq!(checkpoint.blob_bytes, None);
                assert_eq!(checkpoint.wal_bytes, None);
                assert_eq!(checkpoint.reclaimable_pages, None);
            }
            RelationalBackendKind::Seer => {
                assert!(checkpoint.verified_physical_pages.is_some());
                assert!(checkpoint.data_bytes.is_some());
                assert!(checkpoint.blob_bytes.is_some());
                assert!(checkpoint.wal_bytes.is_some());
                assert!(checkpoint.reclaimable_pages.is_some());
            }
        }

        let repeated = database.checkpoint().expect("repeated checkpoint");
        assert_eq!(repeated.before, checkpoint.after);
        match kind {
            RelationalBackendKind::Temporary => {
                assert!(
                    repeated
                        .after
                        .generation
                        .expect("repeated temporary generation")
                        > checkpoint.after.generation.expect("checkpoint generation")
                );
                assert_eq!(repeated.verified_physical_pages, None);
            }
            RelationalBackendKind::Seer => {
                assert_eq!(repeated.after, checkpoint.after);
                assert_eq!(
                    repeated.verified_physical_pages,
                    checkpoint.verified_physical_pages
                );
                assert_eq!(repeated.data_bytes, checkpoint.data_bytes);
                assert_eq!(repeated.blob_bytes, checkpoint.blob_bytes);
                assert_eq!(repeated.wal_bytes, checkpoint.wal_bytes);
                assert_eq!(repeated.reclaimable_pages, checkpoint.reclaimable_pages);
            }
        }

        let before_compaction = database.status().expect("compaction status");
        let compaction = database.compact().expect("compact");
        assert_eq!(compaction.before, before_compaction);
        assert_eq!(
            compaction.after,
            database.status().expect("after compaction status")
        );
        assert_eq!(compaction.after.commit, before_compaction.commit);
        match kind {
            RelationalBackendKind::Temporary => {
                assert!(compaction.row_keys_considered.is_some());
                assert!(compaction.row_fragments_reclaimed.is_some());
                assert_eq!(compaction.data_bytes_before, None);
                assert_eq!(compaction.reclaimed_pages, None);
            }
            RelationalBackendKind::Seer => {
                assert_eq!(compaction.row_keys_considered, None);
                assert!(compaction.data_bytes_before.is_some());
                assert!(compaction.data_bytes_after.is_some());
                assert!(compaction.reclaimed_pages.is_some());
            }
        }
    }

    #[test]
    fn selected_backend_facade_preserves_typed_workload_shape() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        exercise(
            RelationalBackendKind::Temporary,
            &temporary.path().join("temporary"),
        );

        let seer = tempfile::tempdir().expect("seer directory");
        exercise(RelationalBackendKind::Seer, &seer.path().join("seer"));
    }

    #[test]
    fn selected_backends_share_the_fixed_snapshot_conflict_profile() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        exercise_transaction_profile(
            RelationalBackendKind::Temporary,
            &temporary.path().join("temporary"),
        );

        let seer = tempfile::tempdir().expect("seer directory");
        exercise_transaction_profile(RelationalBackendKind::Seer, &seer.path().join("seer"));
    }

    #[test]
    fn selected_backends_expose_read_only_logical_verification() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        exercise_verification(
            RelationalBackendKind::Temporary,
            &temporary.path().join("temporary"),
        );

        let seer = tempfile::tempdir().expect("seer directory");
        exercise_verification(RelationalBackendKind::Seer, &seer.path().join("seer"));
    }

    #[test]
    fn selected_backends_expose_backend_neutral_lifecycle_status() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        exercise_status(
            RelationalBackendKind::Temporary,
            &temporary.path().join("temporary"),
        );

        let seer = tempfile::tempdir().expect("seer directory");
        exercise_status(RelationalBackendKind::Seer, &seer.path().join("seer"));
    }

    #[test]
    fn selected_backends_reject_duplicate_writable_handles() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        exercise_writer_exclusion(
            RelationalBackendKind::Temporary,
            &temporary.path().join("temporary"),
        );

        let seer = tempfile::tempdir().expect("seer directory");
        exercise_writer_exclusion(RelationalBackendKind::Seer, &seer.path().join("seer"));
    }

    #[test]
    fn selected_backends_flush_and_release_on_explicit_close() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        exercise_close(
            RelationalBackendKind::Temporary,
            &temporary.path().join("temporary"),
        );

        let seer = tempfile::tempdir().expect("seer directory");
        exercise_close(RelationalBackendKind::Seer, &seer.path().join("seer"));
    }

    #[test]
    fn selected_backends_return_backend_neutral_maintenance_reports() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        exercise_maintenance_reports(
            RelationalBackendKind::Temporary,
            &temporary.path().join("temporary"),
        );

        let seer = tempfile::tempdir().expect("seer directory");
        exercise_maintenance_reports(RelationalBackendKind::Seer, &seer.path().join("seer"));
    }

    #[test]
    fn facade_migrates_temporary_state_with_explicit_backend_selection() {
        let parent = tempfile::tempdir().expect("temporary directory");
        let source_config = config(
            RelationalBackendKind::Temporary,
            &parent.path().join("temporary"),
        );
        let target_directory = parent.path().join("seer");
        let mut source = RelationalDatabase::create(source_config).expect("source create");
        source.create_table(table()).expect("source table");
        let source_commit = source
            .insert(TableId(7), row(1, "alice"))
            .expect("source row");

        let (migrated, report) = RelationalDatabase::migrate_from_temporary(
            &source,
            SeerKernelConfig::new(target_directory),
            LegacyMigrationOptions::default(),
        )
        .expect("migrate");
        assert_eq!(migrated.backend(), RelationalBackendKind::Seer);
        assert_eq!(report.source_commit, source_commit);
        assert_eq!(report.retained_snapshot_count, 0);
        assert_eq!(
            migrated
                .scan(TableId(7), report.target_commit, 10)
                .expect("migrated rows"),
            vec![row(1, "alice")]
        );
        assert_eq!(source.commit_id(), source_commit);
        assert_eq!(
            source
                .scan(TableId(7), source_commit, 10)
                .expect("source rows"),
            vec![row(1, "alice")]
        );
    }

    #[test]
    fn transactions_and_leases_cannot_cross_handles() {
        let first = TempDir::new().expect("first directory");
        let second = TempDir::new().expect("second directory");
        let mut first_db =
            RelationalDatabase::create(config(RelationalBackendKind::Temporary, first.path()))
                .expect("first create");
        let mut second_db =
            RelationalDatabase::create(config(RelationalBackendKind::Temporary, second.path()))
                .expect("second create");
        first_db.create_table(table()).expect("first table");
        second_db.create_table(table()).expect("second table");

        let transaction = first_db.begin().expect("begin");
        assert!(matches!(
            transaction.commit(&mut second_db),
            Err(DbError::InvalidState(message))
                if message.contains("another database handle")
        ));

        let lease = first_db.retain(first_db.commit_id()).expect("retain");
        assert!(matches!(
            second_db.release(lease),
            Err(DbError::InvalidState(message))
                if message.contains("another database handle")
        ));
    }

    #[test]
    fn event_history_is_bounded_and_reports_dropped_prefix() {
        let log = RelationalEventLog::default();
        for index in 0..(RELATIONAL_EVENT_HISTORY_LIMIT + 3) {
            log.record(
                RelationalEventKind::CommitAcknowledged,
                Some(CommitId(index as u64)),
            );
        }

        let history = log.snapshot();
        assert_eq!(history.events.len(), RELATIONAL_EVENT_HISTORY_LIMIT);
        assert_eq!(history.dropped, 3);
        assert!(history.is_truncated());
        assert_eq!(history.events[0].sequence, 4);
        assert_eq!(history.events[0].commit, Some(CommitId(3)));
        assert_eq!(
            history.events.last().expect("last event").sequence,
            (RELATIONAL_EVENT_HISTORY_LIMIT + 3) as u64
        );
    }
}
