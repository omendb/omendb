//! First vertical slice of OmenDB's append-first buffered MVCC range store.
//!
//! This crate exposes a typed relational API and a deliberately bounded
//! embedded SQL tier. It proves atomic logical WAL, Commit-ID-tagged
//! fragments, retained snapshot reads, packed checkpoint recovery, and
//! deterministic corruption behavior without claiming PostgreSQL protocol or
//! SQL compatibility.

mod fault;
mod model;
mod morsel;
mod packed;
mod relational;
mod relational_database;
mod row_identity;
mod runtime;
// This direct path is a qualification surface until historical retention and
// durability-position results are ready to replace the transitional facade.
#[allow(dead_code, clippy::type_complexity)]
mod seer_direct;
mod serializable;
mod session;
mod sql;

pub use fault::{FailOnce, FaultInjector, FaultPoint, NoFaults};
pub use model::{CommitId, IndexId, Key, Mutation, StorageIdentity};
pub use morsel::{
    AggregateAccumulator, AggregateKind, AggregateSpec, AnalyticalExecutor, AnalyticalQuery,
    AnalyticalResult, DEFAULT_MORSEL_SIZE, MorselBatch, MorselScanner,
};
pub use packed::{PACKED_PAGE_BYTES, PackBudget, PackReport, PackedPage, PackedRange, pack_sorted};
pub use relational::{
    Catalog, ColumnDefinition, ColumnId, ColumnType, ConstraintId, ConstraintTiming,
    ForeignKeyDefinition, IndexDefinition, NamedForeignKeyDefinition, NamedIndexDefinition,
    ReferentialAction, RelationalMutation, RelationalSchemaDefinition, Row, TableDefinition,
    TableId, Value, decode_row, encode_row,
};
pub use relational_database::{
    CancellationToken, OperationControl, RELATIONAL_EVENT_HISTORY_LIMIT,
    RELATIONAL_SQL_BATCH_LIMIT, RelationalBackendConfig, RelationalCapability,
    RelationalCapabilityInfo, RelationalCapabilityReport, RelationalCapabilityState,
    RelationalDatabase, RelationalDatabaseTransaction,
};
pub use row_identity::RowIdentity;
pub use runtime::{
    Dispatch, GovernorConfig, GovernorError, GovernorStats, OverloadPolicy, Reactor, ReactorConfig,
    ReactorError, ResourceGovernor, WorkClass, WorkId, WorkItem, WorkerId,
};
pub use seerdb::DBMetrics;
pub use seerdb::PublicationTimingMetrics;
pub use serializable::{
    CertificationConflict, CertifierAlgorithm, CertifierMetrics, SerializableCertifier,
    TransactionDependencySpec,
};
pub use session::{
    IndexScanRequest, RelationalDatabaseConfig, RelationalDatabaseSession, RelationalSessionConfig,
    RelationalSessionStatus,
};
pub use sql::{SqlColumn, SqlResult};
#[cfg(feature = "pgwire")]
pub mod pgwire_server;
pub type Result<T> = std::result::Result<T, DbError>;

/// Stable application-facing classification for a failed transaction
/// attempt. The classification does not retry or resolve the error; it tells
/// the caller which recovery action is valid for the current contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionErrorClass {
    /// The caller cancelled before durable publication; the transaction was aborted.
    Cancelled,
    /// The operation deadline elapsed before its next bounded checkpoint.
    DeadlineExceeded,
    /// The transaction may be rebuilt from a fresh snapshot and retried.
    SerializationRetry,
    /// The caller should reduce resource use or wait for capacity to return.
    Capacity,
    /// Another writable handle owns the database directory.
    Busy,
    /// The handle must be reopened before the outcome can be reconciled.
    ReopenRequired,
    /// The request violates a declared row, index, or foreign-key constraint.
    ConstraintViolation,
    /// The requested snapshot cannot be used by this history.
    SnapshotUnavailable,
    /// The request or transaction state is invalid and should be corrected.
    InvalidRequest,
    /// Durable state failed integrity checks and must not be used.
    Corruption,
    /// The operating system or storage device returned an I/O failure.
    Io,
    /// The error is a storage/backend failure without a more specific action.
    Storage,
}

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("transaction cancelled before durable publication")]
    Cancelled,
    #[error("operation deadline expired before durable publication")]
    DeadlineExceeded,
    #[error("I/O during {operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("corrupt {artifact}: {reason}")]
    Corruption {
        artifact: &'static str,
        reason: String,
    },
    #[error("injected failure at {0:?}")]
    InjectedFailure(FaultPoint),
    #[error("commit IDs must be monotonic: current {current}, proposed {proposed}")]
    NonMonotonicCommit { current: u64, proposed: u64 },
    #[error("snapshot {0} is not retained or current")]
    SnapshotUnavailable(u64),
    #[error(
        "Serializable conflict: transaction snapshot {snapshot} is older than current {current}"
    )]
    SerializationConflict { snapshot: u64, current: u64 },
    #[error("write conflict on SeerDB tree {tree}, key {key:?}")]
    SeerWriteConflict { tree: u64, key: Vec<u8> },
    #[error("tree lifecycle conflict on SeerDB tree {tree}")]
    SeerTreeConflict { tree: u64 },
    #[error(
        "coalesced publication conflict: transaction {writer} already claimed a row identity in table {table}"
    )]
    WriteWriteConflict { table: u64, writer: usize },
    #[error("resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),
    #[error("invalid database state: {0}")]
    InvalidState(String),
    #[error("SQL parse error: {0}")]
    SqlParse(String),
    #[error("invalid SQL parameters: {0}")]
    SqlParameter(String),
    #[error("table {name} does not exist")]
    SqlUndefinedTable { name: String },
    #[error("column {name} does not exist")]
    SqlUndefinedColumn { name: String },
    #[error("division by zero")]
    SqlDivisionByZero,
    #[error("numeric value out of range: {0}")]
    SqlNumericValueOutOfRange(String),
    #[error("unsupported SQL {statement}: {reason}")]
    SqlUnsupported {
        statement: &'static str,
        reason: String,
    },
    #[error("database requires reopen after an ambiguous durable write")]
    RecoveryRequired,
    #[error("transaction commit failed ({commit}) and lease cleanup failed ({cleanup})")]
    TransactionCleanup {
        commit: Box<DbError>,
        cleanup: Box<DbError>,
    },
    #[error("snapshot capture failed ({capture}) and lease cleanup failed ({cleanup})")]
    SnapshotCaptureCleanup {
        capture: Box<DbError>,
        cleanup: Box<DbError>,
    },
    #[error("value is too large: {0} bytes")]
    ValueTooLarge(usize),
    #[error("unique secondary index {index} rejects duplicate key {key:?}")]
    UniqueViolation { index: u64, key: Vec<u8> },
    #[error(
        "foreign key constraint {constraint} on table {table} references missing row in table {referenced_table}"
    )]
    ForeignKeyViolation {
        constraint: u64,
        table: u64,
        referenced_table: u64,
    },
    #[error(
        "cascade from foreign key constraint {constraint} on table {table} exceeded the configured depth bound"
    )]
    CascadeDepthExceeded { constraint: u64, table: u64 },
    #[error("fragment history requires more than the configured bound of {limit} versions")]
    FragmentDebtExceeded { limit: usize },
    #[error("snapshot capture {resource} exceeds the configured limit of {limit}")]
    SnapshotCaptureLimit {
        resource: &'static str,
        limit: usize,
    },
    #[error("storage capacity exhausted: requested {requested} bytes, available {available} bytes")]
    StorageCapacity { requested: u64, available: u64 },
    #[error("database is busy during {operation}: {reason}")]
    StorageBusy {
        operation: &'static str,
        reason: String,
    },
    #[error("storage snapshot {snapshot} is unavailable: {reason}")]
    StorageSnapshotUnavailable { snapshot: u64, reason: String },
    #[error("storage requires reopen after an ambiguous publication: {reason}")]
    StorageRecoveryRequired { reason: String },
    #[error("storage corruption: {reason}")]
    StorageCorruption { reason: String },
    #[error("storage I/O during {operation}: {reason}")]
    StorageIo {
        operation: &'static str,
        reason: String,
    },
    #[error("storage error during {operation}: {reason}")]
    Storage {
        operation: &'static str,
        reason: String,
    },
    #[error("database session is closing")]
    SessionClosing,
    #[error("database session is busy with another operation")]
    SessionBusy,
    #[error("database session has been closed")]
    SessionClosed,
    #[error("migration published at {destination}, but reopening failed: {reason}")]
    MigrationPublished { destination: String, reason: String },
}

impl DbError {
    /// Classify an error without hiding its detailed variant or changing its
    /// retry/reconciliation behavior.
    #[must_use]
    pub fn transaction_class(&self) -> TransactionErrorClass {
        match self {
            Self::Cancelled => TransactionErrorClass::Cancelled,
            Self::DeadlineExceeded => TransactionErrorClass::DeadlineExceeded,
            Self::SerializationConflict { .. }
            | Self::SeerWriteConflict { .. }
            | Self::SeerTreeConflict { .. }
            | Self::WriteWriteConflict { .. } => TransactionErrorClass::SerializationRetry,
            Self::StorageCapacity { .. }
            | Self::SnapshotCaptureLimit { .. }
            | Self::ResourceLimitExceeded(_) => TransactionErrorClass::Capacity,
            Self::StorageBusy { .. } => TransactionErrorClass::Busy,
            Self::RecoveryRequired
            | Self::StorageRecoveryRequired { .. }
            | Self::TransactionCleanup { .. }
            | Self::SnapshotCaptureCleanup { .. }
            | Self::MigrationPublished { .. } => TransactionErrorClass::ReopenRequired,
            Self::UniqueViolation { .. }
            | Self::ForeignKeyViolation { .. }
            | Self::CascadeDepthExceeded { .. } => TransactionErrorClass::ConstraintViolation,
            Self::SnapshotUnavailable(_) | Self::StorageSnapshotUnavailable { .. } => {
                TransactionErrorClass::SnapshotUnavailable
            }
            Self::InvalidState(_)
            | Self::NonMonotonicCommit { .. }
            | Self::ValueTooLarge(_)
            | Self::FragmentDebtExceeded { .. }
            | Self::SqlParse(_)
            | Self::SqlParameter(_)
            | Self::SqlUndefinedTable { .. }
            | Self::SqlUndefinedColumn { .. }
            | Self::SqlDivisionByZero
            | Self::SqlNumericValueOutOfRange(_)
            | Self::SqlUnsupported { .. }
            | Self::SessionClosing
            | Self::SessionClosed => TransactionErrorClass::InvalidRequest,
            Self::SessionBusy => TransactionErrorClass::Busy,
            Self::Corruption { .. } | Self::StorageCorruption { .. } => {
                TransactionErrorClass::Corruption
            }
            Self::Io { .. } | Self::StorageIo { .. } => TransactionErrorClass::Io,
            Self::InjectedFailure(_) | Self::Storage { .. } => TransactionErrorClass::Storage,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DbError, FaultPoint, TransactionErrorClass};

    #[test]
    fn transaction_error_classification_preserves_recovery_actions() {
        assert_eq!(
            DbError::SerializationConflict {
                snapshot: 3,
                current: 4,
            }
            .transaction_class(),
            TransactionErrorClass::SerializationRetry
        );
        assert_eq!(
            DbError::StorageCapacity {
                requested: 10,
                available: 2,
            }
            .transaction_class(),
            TransactionErrorClass::Capacity
        );
        assert_eq!(
            DbError::StorageBusy {
                operation: "open",
                reason: "already owned".to_owned(),
            }
            .transaction_class(),
            TransactionErrorClass::Busy
        );
        assert_eq!(
            DbError::RecoveryRequired.transaction_class(),
            TransactionErrorClass::ReopenRequired
        );
        assert_eq!(
            DbError::UniqueViolation {
                index: 1,
                key: vec![1],
            }
            .transaction_class(),
            TransactionErrorClass::ConstraintViolation
        );
        assert_eq!(
            DbError::StorageCorruption {
                reason: "bad checksum".to_owned(),
            }
            .transaction_class(),
            TransactionErrorClass::Corruption
        );
        assert_eq!(
            DbError::InjectedFailure(FaultPoint::BeforeWalAppend).transaction_class(),
            TransactionErrorClass::Storage
        );
    }
}
