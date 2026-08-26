//! Synchronous project-facing ownership and admission.

use std::sync::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use crate::relational_database::{
    OperationControl, RELATIONAL_EVENT_HISTORY_LIMIT, RelationalBackendConfig,
    RelationalCapabilityReport, RelationalDatabase, RelationalDatabaseTransaction,
};
use crate::{
    ColumnId, CommitId, DbError, ForeignKeyDefinition, IndexDefinition, IndexId, Key,
    RelationalSchemaDefinition, Result, Row, RowIdentity, TableDefinition, TableId, Value,
};

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

/// A bounded event-history projection for session admission.
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
    #[allow(dead_code)]
    pub const fn is_truncated(&self) -> bool {
        self.dropped != 0
    }
}

/// Bounds for one project-facing database session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalSessionConfig {
    /// Maximum active operations. Immutable reads may overlap; writes and
    /// mixed transactions always require exclusive access.
    pub max_in_flight: usize,
    /// Maximum time a blocked operation waits for session admission before
    /// returning [`DbError::SessionBusy`]. An operation deadline can shorten
    /// this bound, and cancellation removes the waiter cooperatively.
    pub admission_timeout: Duration,
}

/// Common project configuration for creating a selected backend session.
///
/// Backend-specific physical options remain inside [`RelationalBackendConfig`];
/// this wrapper only combines that selection with the common session policy.
#[derive(Clone, Debug)]
pub struct RelationalDatabaseConfig {
    pub backend: RelationalBackendConfig,
    pub session: RelationalSessionConfig,
}

impl RelationalDatabaseConfig {
    /// Build common configuration with the default session policy.
    #[must_use]
    pub fn new(backend: RelationalBackendConfig) -> Self {
        Self {
            backend,
            session: RelationalSessionConfig::default(),
        }
    }

    /// Replace the common session policy without changing backend selection.
    #[must_use]
    pub fn with_session_config(mut self, session: RelationalSessionConfig) -> Self {
        self.session = session;
        self
    }
}

impl Default for RelationalSessionConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 64,
            admission_timeout: Duration::from_secs(30),
        }
    }
}

/// A bounded secondary-index range read request for a session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexScanRequest {
    pub table: TableId,
    pub index: IndexId,
}

/// Admission and lifecycle counters for a project-facing session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalSessionStatus {
    pub active_operations: usize,
    pub waiting_operations: usize,
    pub waiting_writers: usize,
    pub max_in_flight: usize,
    pub closing: bool,
    /// Number of operations that acquired admission and then released it.
    pub completed_operations: u64,
    /// Aggregate time admitted operations spent waiting for a permit.
    pub total_admission_wait: Duration,
    /// Longest admission wait observed for an admitted operation.
    pub max_admission_wait: Duration,
    /// Aggregate time admitted operations held a session permit.
    pub total_operation_time: Duration,
    /// Longest session-permit hold time observed.
    pub max_operation_time: Duration,
    pub rejected_operations: u64,
    pub cancelled_operations: u64,
    pub deadline_expired_operations: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationKind {
    Read,
    Write,
}

#[derive(Debug, Default)]
struct RelationalSessionEventLog {
    next_sequence: u64,
    dropped: u64,
    events: std::collections::VecDeque<RelationalSessionEvent>,
}

impl RelationalSessionEventLog {
    #[allow(dead_code)]
    fn snapshot(&self) -> RelationalSessionEventHistory {
        RelationalSessionEventHistory {
            events: self.events.iter().copied().collect(),
            dropped: self.dropped,
        }
    }

    fn record(
        &mut self,
        kind: RelationalSessionEventKind,
        operation: OperationKind,
        admission_wait: Duration,
        operation_time: Duration,
    ) {
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.events.push_back(RelationalSessionEvent {
            sequence: self.next_sequence,
            kind,
            operation: operation.into(),
            admission_wait,
            operation_time,
        });
        if self.events.len() > RELATIONAL_EVENT_HISTORY_LIMIT {
            let _ = self.events.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
    }
}

#[derive(Debug)]
struct AdmissionState {
    events: RelationalSessionEventLog,
    active_operations: usize,
    active_writers: usize,
    waiting_operations: usize,
    waiting_writers: usize,
    closing: bool,
    completed_operations: u64,
    total_admission_wait: Duration,
    max_admission_wait: Duration,
    total_operation_time: Duration,
    max_operation_time: Duration,
    rejected_operations: u64,
    cancelled_operations: u64,
    deadline_expired_operations: u64,
}

struct OperationAdmission {
    state: Mutex<AdmissionState>,
    changed: Condvar,
    max_in_flight: usize,
    admission_timeout: Duration,
}

const ADMISSION_POLL_INTERVAL: Duration = Duration::from_millis(10);

impl OperationAdmission {
    fn new(config: RelationalSessionConfig) -> Result<Self> {
        if config.max_in_flight == 0 {
            return Err(DbError::InvalidState(
                "session max_in_flight must be positive".to_owned(),
            ));
        }
        if config.admission_timeout.is_zero() {
            return Err(DbError::InvalidState(
                "session admission_timeout must be positive".to_owned(),
            ));
        }
        Ok(Self {
            state: Mutex::new(AdmissionState {
                active_operations: 0,
                active_writers: 0,
                waiting_operations: 0,
                waiting_writers: 0,
                closing: false,
                completed_operations: 0,
                total_admission_wait: Duration::ZERO,
                max_admission_wait: Duration::ZERO,
                total_operation_time: Duration::ZERO,
                max_operation_time: Duration::ZERO,
                rejected_operations: 0,
                cancelled_operations: 0,
                deadline_expired_operations: 0,
                events: RelationalSessionEventLog::default(),
            }),
            changed: Condvar::new(),
            max_in_flight: config.max_in_flight,
            admission_timeout: config.admission_timeout,
        })
    }

    fn acquire(
        &self,
        control: &OperationControl,
        kind: OperationKind,
    ) -> Result<OperationPermit<'_>> {
        let started = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| session_lock_poisoned("admission"))?;
        let mut waiting = false;

        loop {
            if let Err(error) = control.check() {
                self.remove_waiter(&mut state, kind, &mut waiting);
                record_control_rejection(&mut state, kind, &error, started.elapsed());
                return Err(error);
            }
            if state.closing {
                self.remove_waiter(&mut state, kind, &mut waiting);
                state.rejected_operations = state.rejected_operations.saturating_add(1);
                record_admission_rejection(&mut state, kind, started.elapsed());
                return Err(DbError::SessionClosing);
            }
            let blocked = state.active_operations >= self.max_in_flight
                || (kind == OperationKind::Write && state.active_operations != 0)
                || (kind == OperationKind::Read
                    && (state.active_writers != 0 || state.waiting_writers != 0));
            if !blocked {
                self.remove_waiter(&mut state, kind, &mut waiting);
                state.active_operations += 1;
                if kind == OperationKind::Write {
                    state.active_writers += 1;
                }
                return Ok(OperationPermit {
                    admission: self,
                    kind,
                    admission_wait: started.elapsed(),
                    started: Instant::now(),
                });
            }

            if !waiting {
                waiting = true;
                state.waiting_operations = state.waiting_operations.saturating_add(1);
                if kind == OperationKind::Write {
                    state.waiting_writers = state.waiting_writers.saturating_add(1);
                }
            }
            let remaining = self
                .admission_timeout
                .saturating_sub(started.elapsed())
                .min(
                    control
                        .deadline()
                        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                        .unwrap_or(self.admission_timeout),
                );
            if remaining.is_zero() {
                if let Err(error) = control.check() {
                    self.remove_waiter(&mut state, kind, &mut waiting);
                    record_control_rejection(&mut state, kind, &error, started.elapsed());
                    return Err(error);
                }
                self.remove_waiter(&mut state, kind, &mut waiting);
                state.rejected_operations = state.rejected_operations.saturating_add(1);
                record_admission_rejection(&mut state, kind, started.elapsed());
                return Err(DbError::SessionBusy);
            }
            let wait_for = remaining.min(ADMISSION_POLL_INTERVAL);
            state = self
                .changed
                .wait_timeout(state, wait_for)
                .map_err(|_| session_lock_poisoned("admission"))?
                .0;
        }
    }

    #[allow(dead_code)]
    fn promote_to_write(
        &self,
        permit: &mut OperationPermit<'_>,
        control: &OperationControl,
    ) -> Result<()> {
        if permit.kind != OperationKind::Read {
            return Err(DbError::InvalidState(
                "only a read permit can be promoted to a write permit".to_owned(),
            ));
        }
        let started = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| session_lock_poisoned("admission"))?;
        state.active_operations = state
            .active_operations
            .checked_sub(1)
            .expect("operation permit count cannot underflow");
        let mut waiting = true;
        state.waiting_operations = state.waiting_operations.saturating_add(1);
        state.waiting_writers = state.waiting_writers.saturating_add(1);

        loop {
            if let Err(error) = control.check() {
                self.remove_waiter(&mut state, OperationKind::Write, &mut waiting);
                state.active_operations = state.active_operations.saturating_add(1);
                record_control_rejection(
                    &mut state,
                    OperationKind::Read,
                    &error,
                    started.elapsed(),
                );
                self.changed.notify_all();
                return Err(error);
            }
            if state.closing {
                self.remove_waiter(&mut state, OperationKind::Write, &mut waiting);
                state.active_operations = state.active_operations.saturating_add(1);
                state.rejected_operations = state.rejected_operations.saturating_add(1);
                record_admission_rejection(&mut state, OperationKind::Read, started.elapsed());
                self.changed.notify_all();
                return Err(DbError::SessionClosing);
            }
            if state.active_operations == 0 {
                self.remove_waiter(&mut state, OperationKind::Write, &mut waiting);
                state.active_operations = 1;
                state.active_writers = state.active_writers.saturating_add(1);
                permit.kind = OperationKind::Write;
                permit.admission_wait = permit.admission_wait.saturating_add(started.elapsed());
                return Ok(());
            }

            let remaining = self
                .admission_timeout
                .saturating_sub(started.elapsed())
                .min(
                    control
                        .deadline()
                        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                        .unwrap_or(self.admission_timeout),
                );
            if remaining.is_zero() {
                if let Err(error) = control.check() {
                    self.remove_waiter(&mut state, OperationKind::Write, &mut waiting);
                    state.active_operations = state.active_operations.saturating_add(1);
                    record_control_rejection(
                        &mut state,
                        OperationKind::Read,
                        &error,
                        started.elapsed(),
                    );
                    self.changed.notify_all();
                    return Err(error);
                }
                self.remove_waiter(&mut state, OperationKind::Write, &mut waiting);
                state.active_operations = state.active_operations.saturating_add(1);
                state.rejected_operations = state.rejected_operations.saturating_add(1);
                record_admission_rejection(&mut state, OperationKind::Read, started.elapsed());
                self.changed.notify_all();
                return Err(DbError::SessionBusy);
            }
            state = self
                .changed
                .wait_timeout(state, remaining.min(ADMISSION_POLL_INTERVAL))
                .map_err(|_| session_lock_poisoned("admission"))?
                .0;
        }
    }

    fn remove_waiter(&self, state: &mut AdmissionState, kind: OperationKind, waiting: &mut bool) {
        if !*waiting {
            return;
        }
        state.waiting_operations = state
            .waiting_operations
            .checked_sub(1)
            .expect("waiting operation count cannot underflow");
        if kind == OperationKind::Write {
            state.waiting_writers = state
                .waiting_writers
                .checked_sub(1)
                .expect("waiting writer count cannot underflow");
        }
        self.changed.notify_all();
        *waiting = false;
    }

    fn release(&self, kind: OperationKind, admission_wait: Duration, operation_time: Duration) {
        if let Ok(mut state) = self.state.lock() {
            state.active_operations = state
                .active_operations
                .checked_sub(1)
                .expect("operation permit count cannot underflow");
            if kind == OperationKind::Write {
                state.active_writers = state
                    .active_writers
                    .checked_sub(1)
                    .expect("writer permit count cannot underflow");
            }
            state.completed_operations = state.completed_operations.saturating_add(1);
            state.total_admission_wait = state.total_admission_wait.saturating_add(admission_wait);
            state.max_admission_wait = state.max_admission_wait.max(admission_wait);
            state.total_operation_time = state.total_operation_time.saturating_add(operation_time);
            state.max_operation_time = state.max_operation_time.max(operation_time);
            state.events.record(
                RelationalSessionEventKind::OperationCompleted,
                kind,
                admission_wait,
                operation_time,
            );
            self.changed.notify_all();
        }
    }

    fn begin_close(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| session_lock_poisoned("admission"))?;
        if state.active_operations != 0 {
            state.rejected_operations = state.rejected_operations.saturating_add(1);
            return Err(DbError::SessionBusy);
        }
        if state.closing {
            return Err(DbError::SessionClosing);
        }
        state.closing = true;
        self.changed.notify_all();
        Ok(())
    }

    fn status(&self) -> Result<RelationalSessionStatus> {
        let state = self
            .state
            .lock()
            .map_err(|_| session_lock_poisoned("admission"))?;
        Ok(RelationalSessionStatus {
            active_operations: state.active_operations,
            waiting_operations: state.waiting_operations,
            waiting_writers: state.waiting_writers,
            max_in_flight: self.max_in_flight,
            closing: state.closing,
            completed_operations: state.completed_operations,
            total_admission_wait: state.total_admission_wait,
            max_admission_wait: state.max_admission_wait,
            total_operation_time: state.total_operation_time,
            max_operation_time: state.max_operation_time,
            rejected_operations: state.rejected_operations,
            cancelled_operations: state.cancelled_operations,
            deadline_expired_operations: state.deadline_expired_operations,
        })
    }

    #[allow(dead_code)]
    fn event_history(&self) -> Result<RelationalSessionEventHistory> {
        let state = self
            .state
            .lock()
            .map_err(|_| session_lock_poisoned("admission"))?;
        Ok(state.events.snapshot())
    }
}

struct OperationPermit<'a> {
    admission: &'a OperationAdmission,
    kind: OperationKind,
    admission_wait: Duration,
    started: Instant,
}

impl Drop for OperationPermit<'_> {
    fn drop(&mut self) {
        self.admission
            .release(self.kind, self.admission_wait, self.started.elapsed());
    }
}

fn record_control_rejection(
    state: &mut AdmissionState,
    kind: OperationKind,
    error: &DbError,
    admission_wait: Duration,
) {
    let event_kind = match error {
        DbError::Cancelled => RelationalSessionEventKind::CancellationObserved,
        DbError::DeadlineExceeded => RelationalSessionEventKind::DeadlineObserved,
        _ => RelationalSessionEventKind::AdmissionRejected,
    };
    state
        .events
        .record(event_kind, kind, admission_wait, Duration::ZERO);
    match error {
        DbError::Cancelled => {
            state.cancelled_operations = state.cancelled_operations.saturating_add(1);
            state.rejected_operations = state.rejected_operations.saturating_add(1);
        }
        DbError::DeadlineExceeded => {
            state.deadline_expired_operations = state.deadline_expired_operations.saturating_add(1);
            state.rejected_operations = state.rejected_operations.saturating_add(1);
        }
        _ => {}
    }
}

fn record_admission_rejection(
    state: &mut AdmissionState,
    kind: OperationKind,
    admission_wait: Duration,
) {
    state.events.record(
        RelationalSessionEventKind::AdmissionRejected,
        kind,
        admission_wait,
        Duration::ZERO,
    );
}

impl From<OperationKind> for RelationalSessionOperationKind {
    fn from(kind: OperationKind) -> Self {
        match kind {
            OperationKind::Read => Self::Read,
            OperationKind::Write => Self::Write,
        }
    }
}

fn session_lock_poisoned(resource: &'static str) -> DbError {
    DbError::InvalidState(format!("database session {resource} lock is poisoned"))
}

/// A thread-safe owner for project-facing operations on one selected backend.
///
/// Immutable reads may overlap up to the configured bound. Writes, schema
/// changes, maintenance, and mixed transactions use exclusive admission. A
/// blocked operations wait for bounded admission. A waiting writer prevents
/// new readers from entering, which keeps the single serialized write lane
/// from being starved by a steady stream of reads. The session does not spawn
/// workers or interrupt an already-running backend call.
pub struct RelationalDatabaseSession {
    database: RwLock<Option<RelationalDatabase>>,
    admission: OperationAdmission,
}

impl RelationalDatabaseSession {
    /// Create a project-facing session from common and backend-specific
    /// configuration.
    pub fn create(config: RelationalDatabaseConfig) -> Result<Self> {
        RelationalDatabase::create(config.backend)?.into_session(config.session)
    }

    /// Open a project-facing session from common and backend-specific
    /// configuration.
    pub fn open(config: RelationalDatabaseConfig) -> Result<Self> {
        RelationalDatabase::open(config.backend)?.into_session(config.session)
    }

    /// Create a session that owns an already-open project-facing database.
    pub fn new(database: RelationalDatabase, config: RelationalSessionConfig) -> Result<Self> {
        Ok(Self {
            database: RwLock::new(Some(database)),
            admission: OperationAdmission::new(config)?,
        })
    }

    #[must_use]
    /// Return admission/lifecycle state without consuming an operation slot.
    pub fn admission_status(&self) -> Result<RelationalSessionStatus> {
        self.admission.status()
    }

    /// Read the backend-neutral capability/refusal report under admission.
    pub fn capabilities(&self, control: &OperationControl) -> Result<RelationalCapabilityReport> {
        self.read(control, |database| Ok(database.capabilities()))
    }

    /// Return the current commit frontier under session admission.
    pub fn commit_id(&self, control: &OperationControl) -> Result<CommitId> {
        self.read(control, |database| Ok(database.commit_id()))
    }

    /// Run a read-only operation under bounded admission.
    pub fn read<T, F>(&self, control: &OperationControl, operation: F) -> Result<T>
    where
        F: FnOnce(&RelationalDatabase) -> Result<T>,
    {
        let _permit = self.admission.acquire(control, OperationKind::Read)?;
        let database = self.read_database()?;
        let database = database.as_ref().ok_or(DbError::SessionClosed)?;
        let value = operation(database)?;
        control.check()?;
        Ok(value)
    }

    /// Read one legacy single-key row at the current state under bounded
    /// admission.
    pub fn get(
        &self,
        control: &OperationControl,
        table: TableId,
        primary: Key,
    ) -> Result<Option<Row>> {
        self.read(control, |database| database.get(table, primary))
    }

    /// Read a row through the catalog-owned composite primary-key identity.
    pub fn get_by_identity(
        &self,
        control: &OperationControl,
        table: TableId,
        identity: &RowIdentity,
    ) -> Result<Option<Row>> {
        self.read(control, |database| database.get_by_identity(table, identity))
    }

    /// Scan rows at the current state under bounded admission.
    pub fn scan(
        &self,
        control: &OperationControl,
        table: TableId,
        limit: usize,
    ) -> Result<Vec<Row>> {
        self.read(control, |database| database.scan(table, limit))
    }

    /// Read rows matching an exact secondary-index key under bounded admission.
    pub fn index_get(
        &self,
        control: &OperationControl,
        table: TableId,
        index: crate::IndexId,
        values: &[Value],
    ) -> Result<Vec<Row>> {
        self.read(control, |database| database.index_get(table, index, values))
    }

    /// Read all rows of one secondary index in key order under bounded
    /// admission.
    pub fn index_scan(
        &self,
        control: &OperationControl,
        table: TableId,
        index: crate::IndexId,
    ) -> Result<Vec<Row>> {
        self.read(control, |database| database.index_scan(table, index))
    }

    /// Execute one statement in the bounded embedded SQL tier under
    /// exclusive session admission.
    ///
    /// The SQL adapter currently takes a mutable facade handle even for
    /// queries, so SQL statements use the session's serialized write lane.
    /// Use [`Self::transaction`] with the transaction SQL methods when
    /// several statements must share one atomic boundary.
    pub fn execute_sql(&self, control: &OperationControl, sql: &str) -> Result<crate::SqlResult> {
        self.exclusive(control, |database| database.execute_sql(sql))
    }

    /// Execute one parameterized statement in the bounded embedded SQL tier
    /// under exclusive session admission.
    pub fn execute_sql_with_params(
        &self,
        control: &OperationControl,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::SqlResult> {
        self.exclusive(control, |database| {
            database.execute_sql_with_params(sql, params)
        })
    }

    /// Execute several bounded SQL statements in one atomic transaction.
    ///
    /// Use the transaction SQL method when statements need parameters; this
    /// convenience method is for literal statements and one durability
    /// boundary.
    pub fn execute_sql_batch(
        &self,
        control: &OperationControl,
        statements: &[&str],
    ) -> Result<Vec<crate::SqlResult>> {
        if statements.len() > crate::RELATIONAL_SQL_BATCH_LIMIT {
            return Err(DbError::ResourceLimitExceeded(format!(
                "SQL batch has {} statements; limit is {}",
                statements.len(),
                crate::RELATIONAL_SQL_BATCH_LIMIT
            )));
        }
        self.transaction(control, |database, transaction| {
            statements
                .iter()
                .map(|statement| transaction.execute_sql(database, statement))
                .collect()
        })
        .map(|(results, _)| results)
    }

    /// Execute parameterized SQL statements in one atomic transaction.
    pub fn execute_sql_batch_with_params(
        &self,
        control: &OperationControl,
        statements: &[(&str, &[Value])],
    ) -> Result<Vec<crate::SqlResult>> {
        if statements.len() > crate::RELATIONAL_SQL_BATCH_LIMIT {
            return Err(DbError::ResourceLimitExceeded(format!(
                "SQL batch has {} statements; limit is {}",
                statements.len(),
                crate::RELATIONAL_SQL_BATCH_LIMIT
            )));
        }
        self.transaction(control, |database, transaction| {
            statements
                .iter()
                .map(|(statement, params)| {
                    transaction.execute_sql_with_params(database, statement, params)
                })
                .collect()
        })
        .map(|(results, _)| results)
    }

    /// Execute an analytical query with chunked morsel scanning, grouping,
    /// and memory budget protection under a shared read permit.
    pub fn query_analytical(
        &self,
        control: &OperationControl,
        query: &crate::morsel::AnalyticalQuery,
    ) -> Result<crate::morsel::AnalyticalResult> {
        let _permit = self.admission.acquire(control, OperationKind::Read)?;
        let database = self.read_database()?;
        let database = database.as_ref().ok_or(DbError::SessionClosed)?;
        let mut tx = database.begin_with_control(control)?;
        crate::morsel::AnalyticalExecutor::execute(database, &mut tx, query, control)
    }

    /// Publish a table definition under exclusive admission.
    pub fn create_table(
        &self,
        control: &OperationControl,
        table: TableDefinition,
    ) -> Result<CommitId> {
        self.exclusive(control, |database| database.create_table(table))
    }

    /// Publish a table and its secondary schema objects atomically under
    /// exclusive admission.
    pub fn create_table_with_schema(
        &self,
        control: &OperationControl,
        table: TableDefinition,
        schema: RelationalSchemaDefinition,
    ) -> Result<CommitId> {
        self.exclusive(control, |database| {
            database.create_table_with_schema(table, schema)
        })
    }

    /// Publish a table, its catalog-owned primary-key order, and its
    /// secondary schema objects atomically under exclusive admission.
    pub fn create_table_with_schema_and_primary_key(
        &self,
        control: &OperationControl,
        table: TableDefinition,
        primary_key: Option<Vec<ColumnId>>,
        schema: RelationalSchemaDefinition,
    ) -> Result<CommitId> {
        self.exclusive(control, |database| {
            database.create_table_with_schema_and_primary_key(table, primary_key, schema)
        })
    }

    /// Append one nullable column atomically under exclusive admission.
    /// Existing physical rows expose a logical `NULL` for the new field.
    pub fn add_nullable_column(
        &self,
        control: &OperationControl,
        table: crate::TableId,
        column: crate::ColumnDefinition,
    ) -> Result<CommitId> {
        self.exclusive(control, |database| {
            database.add_nullable_column(table, column)
        })
    }

    /// Publish an index definition under exclusive admission.
    pub fn create_index(
        &self,
        control: &OperationControl,
        index: IndexDefinition,
    ) -> Result<CommitId> {
        self.exclusive(control, |database| database.create_index(index))
    }

    /// Publish a named index definition under exclusive admission.
    pub fn create_named_index(
        &self,
        control: &OperationControl,
        index: IndexDefinition,
        name: String,
    ) -> Result<CommitId> {
        self.exclusive(control, |database| database.create_named_index(index, name))
    }

    /// Publish a foreign-key definition under exclusive admission.
    pub fn create_foreign_key(
        &self,
        control: &OperationControl,
        foreign_key: ForeignKeyDefinition,
    ) -> Result<CommitId> {
        self.exclusive(control, |database| database.create_foreign_key(foreign_key))
    }

    /// Publish a named foreign-key definition under exclusive admission.
    pub fn create_named_foreign_key(
        &self,
        control: &OperationControl,
        foreign_key: ForeignKeyDefinition,
        name: String,
    ) -> Result<CommitId> {
        self.exclusive(control, |database| {
            database.create_named_foreign_key(foreign_key, name)
        })
    }

    /// Insert one row in an exclusive single-statement transaction.
    pub fn insert(&self, control: &OperationControl, table: TableId, row: Row) -> Result<CommitId> {
        self.transaction(control, |database, transaction| {
            transaction.insert(database, table, row)
        })
        .map(|(_, commit)| commit)
    }

    /// Update one row in an exclusive single-statement transaction.
    pub fn update(&self, control: &OperationControl, table: TableId, row: Row) -> Result<CommitId> {
        self.transaction(control, |database, transaction| {
            transaction.update(database, table, row)
        })
        .map(|(_, commit)| commit)
    }

    /// Delete one legacy single-key row in an exclusive single-statement
    /// transaction. Use [`Self::delete_row`] for catalog-owned identities.
    pub fn delete(
        &self,
        control: &OperationControl,
        table: TableId,
        primary: Key,
    ) -> Result<CommitId> {
        self.transaction(control, |database, transaction| {
            transaction.delete(database, table, primary)
        })
        .map(|(_, commit)| commit)
    }

    /// Delete one row through its catalog-owned identity in an exclusive
    /// single-statement transaction.
    pub fn delete_row(
        &self,
        control: &OperationControl,
        table: TableId,
        row: Row,
    ) -> Result<CommitId> {
        self.transaction(control, |database, transaction| {
            transaction.delete_row(database, table, row)
        })
        .map(|(_, commit)| commit)
    }

    /// Run a typed transaction under exclusive session admission.
    pub fn transaction<T, F>(
        &self,
        control: &OperationControl,
        operation: F,
    ) -> Result<(T, CommitId)>
    where
        F: FnOnce(&RelationalDatabase, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        let _permit = self.admission.acquire(control, OperationKind::Write)?;
        let mut database = self.write_database()?;
        let database = database.as_mut().ok_or(DbError::SessionClosed)?;
        database.transaction_with_control(control, operation)
    }

    /// Consume the session after all operations have returned and close the
    /// selected backend. A close error consumes the backend as well.
    pub fn close(self) -> Result<()> {
        self.admission.begin_close()?;
        let database = self
            .database
            .into_inner()
            .map_err(|_| session_lock_poisoned("database"))?
            .ok_or(DbError::SessionClosed)?;
        database.close()
    }

    fn exclusive<T, F>(&self, control: &OperationControl, operation: F) -> Result<T>
    where
        F: FnOnce(&mut RelationalDatabase) -> Result<T>,
    {
        let _permit = self.admission.acquire(control, OperationKind::Write)?;
        let mut database = self.write_database()?;
        let database = database.as_mut().ok_or(DbError::SessionClosed)?;
        operation(database)
    }

    fn read_database(&self) -> Result<RwLockReadGuard<'_, Option<RelationalDatabase>>> {
        self.database
            .read()
            .map_err(|_| session_lock_poisoned("database"))
    }

    fn write_database(&self) -> Result<RwLockWriteGuard<'_, Option<RelationalDatabase>>> {
        self.database
            .write()
            .map_err(|_| session_lock_poisoned("database"))
    }
}

impl RelationalDatabase {
    /// Move this database into a bounded, thread-safe synchronous session.
    pub fn into_session(
        self,
        config: RelationalSessionConfig,
    ) -> Result<RelationalDatabaseSession> {
        RelationalDatabaseSession::new(self, config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_event_history_is_bounded() {
        let admission = OperationAdmission::new(RelationalSessionConfig::default())
            .expect("valid session configuration");
        for _ in 0..(RELATIONAL_EVENT_HISTORY_LIMIT + 3) {
            let control = OperationControl::default();
            let permit = admission
                .acquire(&control, OperationKind::Read)
                .expect("read admission");
            drop(permit);
        }

        let history = admission.event_history().expect("event history");
        assert_eq!(history.events.len(), RELATIONAL_EVENT_HISTORY_LIMIT);
        assert_eq!(history.dropped, 3);
        assert!(history.is_truncated());
        assert_eq!(history.events[0].sequence, 4);
        assert_eq!(
            history.events[0].kind,
            RelationalSessionEventKind::OperationCompleted
        );
        assert_eq!(
            history.events.last().expect("last session event").sequence,
            (RELATIONAL_EVENT_HISTORY_LIMIT + 3) as u64
        );
    }
}
