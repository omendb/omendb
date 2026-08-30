//! Project-facing typed relational facade over the direct SeerDB store.
//!
//! One backend, one publication path: every schema change, row write, and
//! SQL statement publishes through [`DirectSeerStore`] onto SeerDB
//! transactions with immediate foreign-key enforcement.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use crate::relational::{
    Catalog, ForeignKeyDefinition, IndexDefinition, RelationalMutation, RelationalSchemaDefinition,
    Row, row_identity_bytes,
};
use crate::row_identity::encode_legacy_key;
use crate::seer_direct::{DirectSeerStore, DirectTransaction};
use crate::{CommitId, DbError, Key, Result, RowIdentity, TableId, Value};

static NEXT_HANDLE_ID: AtomicU64 = AtomicU64::new(1);

/// Maximum statements accepted in one atomic SQL batch.
pub const RELATIONAL_SQL_BATCH_LIMIT: usize = 1_024;
/// Maximum events retained in one session event history ring buffer.
pub const RELATIONAL_EVENT_HISTORY_LIMIT: usize = 128;

/// Cooperative cancellation shared by a project-facing transaction and its
/// caller.
///
/// Cancellation is observed when the transaction begins, during bounded
/// reads and SQL execution loops, while staging, and immediately before
/// durable publication. It cannot interrupt arbitrary user code or an
/// already-started backend write.
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
        if self.cancellation.is_cancelled() {
            return Err(DbError::Cancelled);
        }
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

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(DbError::Cancelled)
    } else {
        Ok(())
    }
}

/// Filesystem location of one direct SeerDB database.
#[derive(Clone, Debug)]
pub struct RelationalBackendConfig {
    /// Directory holding the SeerDB page store, WAL, and direct catalog.
    pub path: PathBuf,
    /// Ack batched commits after one group-synced WAL append, deferring
    /// page materialization and the authority frame to flush/checkpoint/
    /// close. Trades crash-recovery replay work for commit latency.
    pub wal_first_commits: bool,
}

impl RelationalBackendConfig {
    /// Configure a database at `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            wal_first_commits: false,
        }
    }

    /// Enable WAL-first commit acknowledgement.
    #[must_use]
    pub fn with_wal_first_commits(mut self) -> Self {
        self.wal_first_commits = true;
        self
    }
}

/// A capability that can be inspected before issuing work.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RelationalCapability {
    /// Typed tables, rows, indexes, and constraints.
    TypedRelational,
    /// Atomic multi-row transactions over fixed snapshots.
    AtomicTransactions,
    /// Fixed-snapshot transactions with conflict detection.
    FixedSnapshotSerializedWriter,
    /// Secondary-index reads and maintenance.
    SecondaryIndexes,
    /// Immediate foreign-key validation.
    ImmediateForeignKeys,
    /// ON DELETE CASCADE / SET NULL referential actions.
    CascadeReferentialActions,
    /// Synchronous bounded session admission with writer preference.
    WaitableSessionAdmission,
    /// SQL parsing and execution.
    Sql,
    /// PostgreSQL wire-protocol serving.
    Pgwire,
}

impl RelationalCapability {
    /// Every capability the direct backend exposes.
    pub fn all() -> &'static [Self] {
        &[
            Self::TypedRelational,
            Self::AtomicTransactions,
            Self::FixedSnapshotSerializedWriter,
            Self::SecondaryIndexes,
            Self::ImmediateForeignKeys,
            Self::CascadeReferentialActions,
            Self::WaitableSessionAdmission,
            Self::Sql,
            Self::Pgwire,
        ]
    }

    /// Stable machine-readable name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::TypedRelational => "typed_relational",
            Self::AtomicTransactions => "atomic_transactions",
            Self::FixedSnapshotSerializedWriter => "fixed_snapshot_serialized_writer",
            Self::SecondaryIndexes => "secondary_indexes",
            Self::ImmediateForeignKeys => "immediate_foreign_keys",
            Self::CascadeReferentialActions => "cascade_referential_actions",
            Self::WaitableSessionAdmission => "waitable_session_admission",
            Self::Sql => "sql",
            Self::Pgwire => "pgwire",
        }
    }
}

/// State of one inspected capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalCapabilityState {
    /// The backend supports the capability.
    Supported,
    /// The backend does not support the capability.
    Unsupported,
    /// Support is planned but not yet available.
    Planned,
}

/// One reported capability with its state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalCapabilityInfo {
    /// The capability being reported.
    pub capability: RelationalCapability,
    /// Whether the selected backend supports it.
    pub state: RelationalCapabilityState,
}

/// Backend-neutral capability report for the direct SeerDB backend.
#[derive(Clone, Debug)]
pub struct RelationalCapabilityReport {
    pub capabilities: Vec<RelationalCapabilityInfo>,
}

impl RelationalCapabilityReport {
    fn for_direct_backend() -> Self {
        let capabilities = RelationalCapability::all()
            .iter()
            .copied()
            .map(|capability| RelationalCapabilityInfo {
                capability,
                state: RelationalCapabilityState::Supported,
            })
            .collect();
        Self { capabilities }
    }

    /// Return the state for one capability.
    #[must_use]
    pub fn state(&self, capability: RelationalCapability) -> RelationalCapabilityState {
        self.capabilities
            .iter()
            .find(|info| info.capability == capability)
            .map_or(RelationalCapabilityState::Unsupported, |info| info.state)
    }

    /// Return whether the backend supports one capability.
    #[must_use]
    pub fn supports(&self, capability: RelationalCapability) -> bool {
        self.state(capability) == RelationalCapabilityState::Supported
    }
}

/// A project-facing relational database over the direct SeerDB store.
pub struct RelationalDatabase {
    store: DirectSeerStore,
    handle_id: u64,
}

impl RelationalDatabase {
    /// Create an empty database at the configured path.
    pub fn create(config: RelationalBackendConfig) -> Result<Self> {
        let store = DirectSeerStore::create(&config.path, options_from_config(&config))?;
        Ok(Self {
            store,
            handle_id: NEXT_HANDLE_ID.fetch_add(1, Ordering::Relaxed),
        })
    }

    /// Open an existing database at the configured path.
    pub fn open(config: RelationalBackendConfig) -> Result<Self> {
        let store = DirectSeerStore::open(&config.path, options_from_config(&config))?;
        Ok(Self {
            store,
            handle_id: NEXT_HANDLE_ID.fetch_add(1, Ordering::Relaxed),
        })
    }

    /// Flush and close the database.
    pub fn close(self) -> Result<()> {
        self.store.close()
    }

    /// Return the current catalog.
    pub fn catalog(&self) -> &Catalog {
        self.store.catalog()
    }

    /// Return the backend-neutral capability report.
    pub fn capabilities(&self) -> RelationalCapabilityReport {
        RelationalCapabilityReport::for_direct_backend()
    }

    /// Return the latest published logical commit id (the current CSN).
    pub fn commit_id(&self) -> CommitId {
        CommitId(self.store.commit_seq().get())
    }

    /// Alias of [`Self::commit_id`].
    pub fn head(&self) -> CommitId {
        self.commit_id()
    }

    /// Return engine-level storage metrics, including publication-phase
    /// wall-clock timing.
    pub fn metrics(&self) -> crate::DBMetrics {
        self.store.metrics()
    }

    // ---- Schema ------------------------------------------------------------

    /// Publish a new table as one schema commit.
    pub fn create_table(&mut self, table: crate::TableDefinition) -> Result<CommitId> {
        let commit = self.store.create_table(table, None)?;
        Ok(CommitId(commit.get()))
    }

    /// Publish a new table carrying secondary schema objects.
    pub fn create_table_with_schema(
        &mut self,
        table: crate::TableDefinition,
        schema: RelationalSchemaDefinition,
    ) -> Result<CommitId> {
        self.create_table_with_schema_and_primary_key(table, None, schema)
    }

    /// Publish a new table with a composite primary key and secondary schema
    /// objects as one schema commit.
    pub fn create_table_with_schema_and_primary_key(
        &mut self,
        table: crate::TableDefinition,
        primary_key: Option<Vec<crate::ColumnId>>,
        schema: RelationalSchemaDefinition,
    ) -> Result<CommitId> {
        let commit =
            self.store
                .create_table_with_schema_and_primary_key(table, primary_key, schema)?;
        Ok(CommitId(commit.get()))
    }

    /// Publish one nullable column addition.
    pub fn add_nullable_column(
        &mut self,
        table: TableId,
        column: crate::ColumnDefinition,
    ) -> Result<CommitId> {
        let commit = self.store.add_nullable_column(table, column)?;
        Ok(CommitId(commit.get()))
    }

    /// Publish one anonymous secondary index built from existing rows.
    pub fn create_index(&mut self, index: IndexDefinition) -> Result<CommitId> {
        let commit = self.store.create_index(index)?;
        Ok(CommitId(commit.get()))
    }

    /// Publish one named secondary index built from existing rows.
    pub fn create_named_index(&mut self, index: IndexDefinition, name: String) -> Result<CommitId> {
        let commit = self.store.create_named_index(index, name)?;
        Ok(CommitId(commit.get()))
    }

    /// Publish one anonymous foreign-key constraint.
    pub fn create_foreign_key(&mut self, foreign_key: ForeignKeyDefinition) -> Result<CommitId> {
        let commit = self.store.create_foreign_key(foreign_key)?;
        Ok(CommitId(commit.get()))
    }

    /// Publish one named foreign-key constraint after validating existing rows.
    pub fn create_named_foreign_key(
        &mut self,
        foreign_key: ForeignKeyDefinition,
        name: String,
    ) -> Result<CommitId> {
        let commit = self.store.create_named_foreign_key(foreign_key, name)?;
        Ok(CommitId(commit.get()))
    }

    // ---- Rows ---------------------------------------------------------------

    /// Insert one row atomically with its index entries.
    pub fn insert(&self, table: TableId, row: Row) -> Result<CommitId> {
        let commit = self.store.insert(table, row)?;
        Ok(CommitId(commit.get()))
    }

    /// Replace one row identified by the incoming row's primary-key identity.
    pub fn update(&self, table: TableId, row: Row) -> Result<CommitId> {
        let commit = self.store.update(table, row)?;
        Ok(CommitId(commit.get()))
    }

    /// Delete a legacy single-key row. Use [`Self::delete_row`] for composite
    /// primary-key tables.
    pub fn delete(&mut self, table: TableId, primary: Key) -> Result<CommitId> {
        validate_legacy_key(table, primary)?;
        if self.catalog().primary_key(table).is_some() {
            return Err(DbError::InvalidState(
                "legacy key delete requires a legacy primary-key table; use delete_row".to_owned(),
            ));
        }
        let identity = encode_legacy_key(table, primary)?;
        let commit = self.store.delete(table, &identity)?;
        Ok(CommitId(commit.get()))
    }

    /// Delete one row through its full row value.
    pub fn delete_row(&mut self, table: TableId, row: Row) -> Result<CommitId> {
        let definition = self.catalog().table(table)?.clone();
        let identity = row_identity_bytes(self.catalog(), &definition, &row)?;
        let commit = self.store.delete(table, &identity)?;
        Ok(CommitId(commit.get()))
    }

    /// Apply several mutations in one atomic transaction.
    pub fn commit_batch(
        &mut self,
        mutations: impl IntoIterator<Item = RelationalMutation>,
    ) -> Result<CommitId> {
        let mut transaction = self.begin()?;
        for mutation in mutations {
            transaction.stage_mutation(self, mutation)?;
        }
        transaction.commit()
    }

    /// Read one legacy single-key row at the current state.
    pub fn get(&self, table: TableId, primary: Key) -> Result<Option<Row>> {
        validate_legacy_key(table, primary)?;
        if self.catalog().primary_key(table).is_some() {
            return Err(DbError::InvalidState(
                "legacy key lookup requires a legacy primary-key table; use get_by_identity"
                    .to_owned(),
            ));
        }
        let identity = encode_legacy_key(table, primary)?;
        self.store.get(table, &identity)
    }

    /// Look up one row through the catalog-owned composite primary-key identity.
    pub fn get_by_identity(&self, table: TableId, identity: &RowIdentity) -> Result<Option<Row>> {
        let bytes =
            crate::relational::row_identity_bytes_for_lookup(self.catalog(), table, identity)?;
        self.store.get(table, &bytes)
    }

    /// Read up to `limit` rows of one table at the current state.
    pub fn scan(&self, table: TableId, limit: usize) -> Result<Vec<Row>> {
        let mut rows = self.store.scan(table)?;
        rows.truncate(limit);
        Ok(rows)
    }

    /// Exact-value lookup through one secondary index at the current state.
    pub fn index_get(
        &self,
        table: TableId,
        index: crate::IndexId,
        values: &[Value],
    ) -> Result<Vec<Row>> {
        self.store.index_get(table, index, values)
    }

    /// Read all rows of one index in index-key order at the current state.
    pub fn index_scan(&self, table: TableId, index: crate::IndexId) -> Result<Vec<Row>> {
        self.store.index_scan(table, index)
    }

    // ---- Transactions -------------------------------------------------------

    /// Begin one explicit typed transaction at the current snapshot.
    pub fn begin(&self) -> Result<RelationalDatabaseTransaction> {
        RelationalDatabaseTransaction::begin(self, None)
    }

    /// Begin one explicit typed transaction under cooperative control.
    pub fn begin_with_control(
        &self,
        control: &OperationControl,
    ) -> Result<RelationalDatabaseTransaction> {
        control.check()?;
        RelationalDatabaseTransaction::begin(self, Some(control.clone()))
    }

    /// Run one transaction with cooperative cancellation before publication.
    pub fn transaction_with_cancellation<T, F>(
        &mut self,
        cancellation: &CancellationToken,
        operation: F,
    ) -> Result<(T, CommitId)>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        ensure_not_cancelled(cancellation)?;
        let mut transaction =
            self.begin_with_control(&OperationControl::with_cancellation(cancellation.clone()))?;
        self.run_transaction(&mut transaction, operation)
    }

    /// Run one bounded transaction under cooperative control.
    pub fn transaction_with_control<T, F>(
        &mut self,
        control: &OperationControl,
        operation: F,
    ) -> Result<(T, CommitId)>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        let mut transaction = self.begin_with_control(control)?;
        self.run_transaction(&mut transaction, operation)
    }

    /// Run one closure inside a fresh atomic transaction.
    pub fn transaction<T, F>(&mut self, operation: F) -> Result<(T, CommitId)>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        let mut transaction = self.begin()?;
        self.run_transaction(&mut transaction, operation)
    }

    fn run_transaction<T, F>(
        &self,
        transaction: &mut RelationalDatabaseTransaction,
        operation: F,
    ) -> Result<(T, CommitId)>
    where
        F: FnOnce(&Self, &mut RelationalDatabaseTransaction) -> Result<T>,
    {
        let value = operation(self, transaction)?;
        if transaction.is_read_only() {
            transaction.ensure_active()?;
            return Ok((value, transaction.snapshot()));
        }
        let commit =
            std::mem::replace(transaction, RelationalDatabaseTransaction::empty()).commit()?;
        Ok((value, commit))
    }

    // ---- SQL ----------------------------------------------------------------

    /// Execute one bounded SQL statement in its own implicit transaction.
    pub fn execute_sql(&mut self, sql: &str) -> Result<crate::SqlResult> {
        crate::sql::execute_with_params(self, sql, &[])
    }

    /// Execute one bounded SQL statement with positional parameters.
    pub fn execute_sql_with_params(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::SqlResult> {
        crate::sql::execute_with_params(self, sql, params)
    }

    /// Execute several bounded SQL statements in one atomic transaction.
    pub fn execute_sql_batch(&mut self, statements: &[&str]) -> Result<Vec<crate::SqlResult>> {
        if statements.len() > RELATIONAL_SQL_BATCH_LIMIT {
            return Err(DbError::ResourceLimitExceeded(format!(
                "SQL batch has {} statements; limit is {}",
                statements.len(),
                RELATIONAL_SQL_BATCH_LIMIT
            )));
        }
        let (results, _) = self.transaction(|database, transaction| {
            statements
                .iter()
                .map(|statement| transaction.execute_sql(database, statement))
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(results)
    }

    /// Execute several parameterized SQL statements in one atomic transaction.
    pub fn execute_sql_batch_with_params(
        &mut self,
        statements: &[&str],
        params: &[Vec<Value>],
    ) -> Result<Vec<crate::SqlResult>> {
        if statements.len() > RELATIONAL_SQL_BATCH_LIMIT {
            return Err(DbError::ResourceLimitExceeded(format!(
                "SQL batch has {} statements; limit is {}",
                statements.len(),
                RELATIONAL_SQL_BATCH_LIMIT
            )));
        }
        let (results, _) = self.transaction(|database, transaction| {
            statements
                .iter()
                .zip(params)
                .map(|(statement, statement_params)| {
                    transaction.execute_sql_with_params(database, statement, statement_params)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(results)
    }

    /// Return the inferred type of every positional parameter.
    pub fn sql_parameter_types(&self, sql: &str) -> Result<Vec<Option<crate::ColumnType>>> {
        crate::sql::describe_parameters(self, sql)
    }
}

fn options_from_config(config: &RelationalBackendConfig) -> seerdb::db::Options {
    let mut options = seerdb::db::Options::default();
    if config.wal_first_commits {
        options.wal_first_commits = true;
    }
    options
}

fn validate_legacy_key(table: TableId, key: Key) -> Result<()> {
    if key.0[..8] != table.0.to_be_bytes() {
        return Err(DbError::InvalidState(format!(
            "row key does not belong to table {}",
            table.0
        )));
    }
    Ok(())
}

/// An explicit multi-operation transaction owned by one database handle.
pub struct RelationalDatabaseTransaction {
    owner_id: u64,
    wrote: bool,
    control: Option<OperationControl>,
    backend: Option<DirectTransaction>,
}

impl RelationalDatabaseTransaction {
    fn begin(database: &RelationalDatabase, control: Option<OperationControl>) -> Result<Self> {
        let backend = database.store.begin_transaction()?;
        Ok(Self {
            owner_id: database.handle_id,
            wrote: false,
            control,
            backend: Some(backend),
        })
    }

    fn empty() -> Self {
        Self {
            owner_id: 0,
            wrote: false,
            control: None,
            backend: None,
        }
    }

    fn backend(&mut self, store: &RelationalDatabase) -> Result<&mut DirectTransaction> {
        if self.owner_id != store.handle_id {
            return Err(invalid_transaction_owner());
        }
        if let Some(control) = &self.control {
            control.check()?;
        }
        self.backend
            .as_mut()
            .ok_or_else(|| DbError::InvalidState("transaction is no longer active".to_owned()))
    }

    fn ensure_active(&self) -> Result<()> {
        if let Some(control) = &self.control {
            control.check()?;
        }
        if self.backend.is_none() {
            return Err(DbError::InvalidState(
                "transaction is no longer active".to_owned(),
            ));
        }
        Ok(())
    }

    fn ensure_owner(&self, store: &RelationalDatabase) -> Result<()> {
        if self.owner_id == store.handle_id {
            Ok(())
        } else {
            Err(invalid_transaction_owner())
        }
    }

    fn stage_mutation(
        &mut self,
        store: &RelationalDatabase,
        mutation: RelationalMutation,
    ) -> Result<()> {
        match mutation {
            RelationalMutation::Insert { table, row } => self.insert(store, table, row),
            RelationalMutation::Update { table, row } => self.update(store, table, row),
            RelationalMutation::Delete { table, primary } => self.delete(store, table, primary),
            RelationalMutation::DeleteRow { table, row } => self.delete_row(store, table, row),
        }
    }

    /// Return the transaction's fixed read snapshot.
    #[must_use]
    pub fn snapshot(&self) -> CommitId {
        self.backend
            .as_ref()
            .map_or(CommitId::default(), |backend| {
                CommitId(backend.snapshot_csn().get())
            })
    }

    /// Return whether no writes have been staged.
    #[must_use]
    pub(crate) fn is_read_only(&self) -> bool {
        !self.wrote
    }

    /// Stage one row insert plus derived index entries.
    pub fn insert(&mut self, store: &RelationalDatabase, table: TableId, row: Row) -> Result<()> {
        self.backend(store)?.insert(table, row)?;
        self.wrote = true;
        Ok(())
    }

    /// Stage one row replacement plus derived index refreshes.
    pub fn update(&mut self, store: &RelationalDatabase, table: TableId, row: Row) -> Result<()> {
        self.backend(store)?.update(table, row)?;
        self.wrote = true;
        Ok(())
    }

    /// Stage a legacy single-key delete. Use [`Self::delete_row`] for
    /// composite primary-key tables.
    pub fn delete(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        primary: Key,
    ) -> Result<()> {
        let definition = store.catalog().table(table)?.clone();
        validate_legacy_key(table, primary)?;
        if store.catalog().primary_key(table).is_some() {
            return Err(DbError::InvalidState(
                "legacy key delete requires a legacy primary-key table; use delete_row".to_owned(),
            ));
        }
        drop(definition);
        let identity = encode_legacy_key(table, primary)?;
        self.backend(store)?.delete(table, &identity)?;
        self.wrote = true;
        Ok(())
    }

    /// Stage one row delete resolved through the row's own identity.
    pub fn delete_row(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        row: Row,
    ) -> Result<()> {
        let definition = store.catalog().table(table)?.clone();
        let identity = row_identity_bytes(store.catalog(), &definition, &row)?;
        self.backend(store)?.delete(table, &identity)?;
        self.wrote = true;
        Ok(())
    }

    /// Read one legacy single-key row including staged mutations.
    pub fn get(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        primary: Key,
    ) -> Result<Option<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        let definition = store.catalog().table(table)?.clone();
        validate_legacy_key(table, primary)?;
        if store.catalog().primary_key(table).is_some() {
            return Err(DbError::InvalidState(
                "legacy key lookup requires a legacy primary-key table; use get_by_identity"
                    .to_owned(),
            ));
        }
        drop(definition);
        let identity = encode_legacy_key(table, primary)?;
        self.backend
            .as_mut()
            .expect("checked active")
            .get(table, &identity)
    }

    /// Read one row by composite identity including staged mutations.
    pub fn get_by_identity(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        identity: &RowIdentity,
    ) -> Result<Option<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        let bytes =
            crate::relational::row_identity_bytes_for_lookup(store.catalog(), table, identity)?;
        self.backend
            .as_mut()
            .expect("checked active")
            .get(table, &bytes)
    }

    /// Read up to `limit` rows of one table including staged mutations.
    ///
    /// The scan is snapshot-isolated: rows inserted by concurrent commits
    /// after this transaction began are not observed and do not conflict.
    pub fn scan(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        limit: usize,
    ) -> Result<Vec<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        let mut rows = self.backend.as_mut().expect("checked active").scan(table)?;
        rows.truncate(limit);
        Ok(rows)
    }

    /// Read all rows of one table under serializable semantics: the full
    /// table range is registered as a read dependency, so a concurrent
    /// commit inserting into the table after our snapshot fails our commit
    /// with a serialization conflict instead of silently forking history.
    pub fn scan_serializable(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
    ) -> Result<Vec<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        self.backend
            .as_mut()
            .expect("checked active")
            .serializable_scan(table)
    }

    /// Exact-value lookup through one secondary index including staged state.
    pub fn index_get(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        index: crate::IndexId,
        values: &[Value],
    ) -> Result<Vec<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        self.backend
            .as_mut()
            .expect("checked active")
            .index_get(table, index, values)
    }

    /// Read all rows of one index in index-key order including staged state.
    pub fn index_scan(
        &mut self,
        store: &RelationalDatabase,
        table: TableId,
        index: crate::IndexId,
    ) -> Result<Vec<Row>> {
        self.ensure_owner(store)?;
        self.ensure_active()?;
        self.backend
            .as_mut()
            .expect("checked active")
            .index_scan(table, index)
    }

    #[cfg(feature = "pgwire")]
    pub(crate) fn set_operation_control(&mut self, control: &OperationControl) {
        self.control = Some(control.clone());
    }

    pub(crate) fn check_operation_control(&self) -> Result<()> {
        self.control
            .as_ref()
            .map_or(Ok(()), OperationControl::check)
    }

    /// Execute one bounded embedded SQL statement inside this transaction.
    pub fn execute_sql(
        &mut self,
        store: &RelationalDatabase,
        sql: &str,
    ) -> Result<crate::SqlResult> {
        crate::sql::execute_in_transaction(store, self, sql)
    }

    /// Execute one bounded embedded SQL statement with positional parameters
    /// inside this transaction.
    pub fn execute_sql_with_params(
        &mut self,
        store: &RelationalDatabase,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::SqlResult> {
        crate::sql::execute_in_transaction_with_params(store, self, sql, params)
    }

    /// Validate referential integrity and publish every staged change.
    pub fn commit(mut self) -> Result<CommitId> {
        self.ensure_active()?;
        let backend = self.backend.take().expect("checked active");
        let commit = backend.commit()?;
        Ok(CommitId(commit.get()))
    }
}

fn invalid_transaction_owner() -> DbError {
    DbError::InvalidState("transaction or snapshot lease belongs to another database handle".into())
}