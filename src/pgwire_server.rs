//! PostgreSQL wire-protocol serving for the bounded embedded SQL tier.
//!
//! V1 scope: loopback trust or catalog-backed SCRAM authentication, simple and
//! extended query protocols with parameterized execution, cooperative query
//! cancellation, and wire transaction blocks (`BEGIN`/`COMMIT`/`ROLLBACK`)
//! mapped to the typed transaction API with PostgreSQL aborted-state semantics.
//! Contract details and non-goals live in the repository's server
//! documentation.
//!
//! The implementation is split by lifecycle concern: `auth`
//! (trust/SCRAM/grants/startup), `lifecycle` (configuration, admission,
//! worker accounting, shutdown), `cancellation` (cancel-request routing),
//! `encoding` (typed wire formats), and `handler` (statement execution and
//! protocol dispatch). Shared shapes live in `shared`.

mod auth;
mod cancellation;
mod encoding;
mod handler;
mod lifecycle;
mod shared;

use std::sync::{Arc, RwLock};

use crate::{CancellationToken, OperationControl, RelationalDatabase, Value};

pub use auth::{provision_wire_grant, provision_wire_user};
pub use lifecycle::{
    RunningServer, ServerConfig, ServerError, ServerShutdownHandle, ServerStatus, serve, spawn,
};

/// A shared database behind the wire server.
///
/// The kernel serves concurrent snapshot reads and buffers in-block writes
/// inside the transaction until publication, so statements hold the read
/// lock while executing; only publication paths (autocommit writes, DDL,
/// COMMIT) take the write lock. This is the serialized-writer +
/// concurrent-reader shape from `design/OLTP_COMPETITIVE_GAPS.md`.
pub type SharedDatabase = Arc<RwLock<RelationalDatabase>>;

use pgwire::error::{ErrorInfo, PgWireError};

pub(super) fn pg_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

pub(super) fn map_db_error(error: crate::DbError) -> PgWireError {
    let (code, message) = match &error {
        crate::DbError::Cancelled => (
            "57014",
            "canceling statement due to user request".to_owned(),
        ),
        crate::DbError::UniqueViolation { .. } => ("23505", error.to_string()),
        crate::DbError::ForeignKeyViolation { .. }
        | crate::DbError::CascadeDepthExceeded { .. } => ("23503", error.to_string()),
        crate::DbError::SerializationConflict { .. }
        | crate::DbError::SeerWriteConflict { .. }
        | crate::DbError::SeerTreeConflict { .. }
        | crate::DbError::WriteWriteConflict { .. } => ("40001", error.to_string()),
        crate::DbError::DeadlineExceeded => (
            "57014",
            "canceling statement due to statement timeout".to_owned(),
        ),
        crate::DbError::SqlParse(_) => ("42601", error.to_string()),
        crate::DbError::SqlParameter(_) => ("08P01", error.to_string()),
        crate::DbError::SqlUndefinedTable { .. } => ("42P01", error.to_string()),
        crate::DbError::SqlUndefinedColumn { .. } => ("42703", error.to_string()),
        crate::DbError::SqlDatatypeMismatch { .. } => ("42804", error.to_string()),
        crate::DbError::SqlDivisionByZero => ("22012", error.to_string()),
        crate::DbError::SqlNumericValueOutOfRange(_) => ("22003", error.to_string()),
        crate::DbError::SqlNotNullViolation { .. } => ("23502", error.to_string()),
        crate::DbError::ResourceLimitExceeded(_) => ("54000", error.to_string()),
        crate::DbError::SqlUnsupported { .. } => {
            ("0A000", format!("feature not supported: {error}"))
        }
        _ => ("XX000", error.to_string()),
    };
    pg_error(code, message)
}

#[cfg(test)]
mod tests {
    use super::auth::auth_failure_delay_ms;
    use super::cancellation::CancellationRegistry;
    use super::lifecycle::QueryWorkers;
    use super::shared::ParsedStatement;
    use super::*;
    use crate::DbError;
    use pgwire::api::ClientInfo;
    use pgwire::api::DefaultClient;
    use pgwire::messages::startup::SecretKey;
    use std::time::Duration;

    #[test]
    fn auth_failure_delay_is_bounded_exponential() {
        assert_eq!(auth_failure_delay_ms(0), 0);
        assert_eq!(
            [1, 2, 3, 4, 5, 6, 7, 8].map(auth_failure_delay_ms),
            [100, 200, 400, 800, 1_600, 3_200, 5_000, 5_000]
        );
    }

    #[test]
    fn cancellation_registry_routes_and_releases_query_tokens() {
        let address = "127.0.0.1:6543".parse().expect("test address");
        let mut client = DefaultClient::<ParsedStatement>::new(address, false);
        client.set_pid_and_secret_key(17, SecretKey::I32(23));
        let registry = Arc::new(CancellationRegistry::new());
        registry.register(&client);

        let (token, lease) = registry.begin(&client).expect("registered client");
        assert!(!token.is_cancelled());
        assert!(registry.cancel(17, &SecretKey::I32(23)));
        assert!(token.is_cancelled());
        drop(lease);
        assert!(!registry.cancel(17, &SecretKey::I32(23)));

        registry.cleanup_connection(address);
        assert!(registry.begin(&client).is_none());
    }

    #[test]
    fn sql_parameter_errors_use_protocol_violation_state() {
        let error = map_db_error(DbError::SqlParameter(
            "statement references parameters but none were supplied".to_owned(),
        ));
        match error {
            PgWireError::UserError(info) => assert_eq!(info.code, "08P01"),
            other => panic!("expected user error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn query_worker_tracker_records_terminal_operation_outcomes() {
        let workers = Arc::new(QueryWorkers::new());
        let _ = workers
            .spawn_operation(CancellationToken::new(), || Ok::<_, PgWireError>(()))
            .await
            .expect("successful operation");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        workers
            .spawn_operation(cancellation, || {
                Err::<(), _>(pg_error("XX000", "synthetic failure".to_owned()))
            })
            .await
            .expect("worker completed")
            .expect_err("failed operation");
        assert_eq!(workers.status(), (0, 2, 1, 1));
    }

    #[tokio::test]
    async fn query_worker_tracker_drains_dropped_join_handles() {
        let workers = Arc::new(QueryWorkers::new());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        drop(workers.spawn_operation(CancellationToken::new(), move || {
            started_tx.send(()).expect("worker start receiver");
            release_rx.recv().expect("worker release");
            Ok::<_, PgWireError>(())
        }));
        started_rx.await.expect("worker started");

        let waiter = tokio::spawn({
            let workers = Arc::clone(&workers);
            async move { workers.wait_for_idle().await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());
        release_tx.send(()).expect("release worker");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("worker tracker drained")
            .expect("waiter task");
    }
}
