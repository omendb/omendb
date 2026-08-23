use std::path::Path;
use std::sync::Arc;
use std::thread;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, Key, OperationControl,
    RelationalBackendConfig, RelationalBackendKind, RelationalDatabaseConfig,
    RelationalDatabaseSession, RelationalSessionConfig, Row, TableDefinition, TableId, Value,
};
use tempfile::tempdir;

const USERS_TABLE: TableId = TableId(601);

fn backend_config(kind: RelationalBackendKind, directory: &Path) -> RelationalBackendConfig {
    match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(omendb::SeerKernelConfig::new(directory.to_owned()))
        }
    }
}

fn users_schema() -> TableDefinition {
    TableDefinition {
        id: USERS_TABLE,
        name: "users".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "balance".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "name".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn user_row(id: u64, balance: u64, name: &str) -> Row {
    Row {
        primary: Key::new(USERS_TABLE.0, id),
        values: vec![
            Value::U64(id),
            Value::U64(balance),
            Value::Text(name.to_owned()),
        ],
    }
}

/// Group commit is a Seer-path feature: the in-memory Temporary backend has
/// no durable publication to overlap, so coalesced publication rejects it
/// explicitly and callers use the exclusive transaction API instead.
#[test]
fn group_commit_rejects_temporary_backend_explicitly() {
    let dir = tempdir().expect("tempdir");
    let db_cfg = RelationalDatabaseConfig {
        backend: backend_config(RelationalBackendKind::Temporary, &dir.path().join("db")),
        session: RelationalSessionConfig {
            max_in_flight: 32,
            admission_timeout: std::time::Duration::from_secs(10),
        },
    };
    let session = Arc::new(RelationalDatabaseSession::create(db_cfg).expect("create session"));
    let control = OperationControl::new();
    session
        .create_table(&control, users_schema())
        .expect("create table");

    let error = session
        .transaction_with_group_commit(&control, |db, tx| {
            tx.insert(db, USERS_TABLE, user_row(1, 100, "user_1"))
        })
        .expect_err("temporary backend must reject coalesced publication");
    assert!(
        error.to_string().contains("Seer backend"),
        "unexpected error: {error}"
    );
}

#[test]
fn group_commit_scales_concurrent_writers_on_seer() {
    {
        let dir = tempdir().expect("tempdir");
        let db_dir = dir.path().join("db");
        let db_cfg = RelationalDatabaseConfig {
            backend: backend_config(RelationalBackendKind::Seer, &db_dir),
            session: RelationalSessionConfig {
                max_in_flight: 32,
                admission_timeout: std::time::Duration::from_secs(10),
            },
        };

        let session = Arc::new(RelationalDatabaseSession::create(db_cfg).expect("create session"));
        let control = OperationControl::new();

        // Create table
        session
            .create_table(&control, users_schema())
            .expect("create table");

        // Spawn 8 concurrent worker threads
        let num_workers = 8;
        let ops_per_worker = 25;
        let mut handles = Vec::new();

        for worker_id in 0..num_workers {
            let session_clone = Arc::clone(&session);
            let h = thread::spawn(move || {
                let ctrl = OperationControl::new();
                for i in 0..ops_per_worker {
                    let user_id = worker_id * 1000 + i;
                    let row = user_row(user_id, 100 + user_id, &format!("user_{user_id}"));
                    let (_, commit) = session_clone
                        .transaction_with_group_commit(&ctrl, |db, tx| {
                            tx.insert(db, USERS_TABLE, row.clone())
                        })
                        .expect("group commit transaction");
                    assert!(commit.0 > 0);
                }
            });
            handles.push(h);
        }

        for h in handles {
            h.join().expect("thread join");
        }

        let metrics = session.group_commit_metrics();
        assert_eq!(
            metrics.total_transactions_committed,
            (num_workers * ops_per_worker)
        );
        assert!(metrics.total_publications > 0);

        // Verify logical database integrity
        let report = session.verify(&control).expect("verify session");
        assert_eq!(report.verified_tables, 1);
        assert_eq!(report.verified_rows, num_workers * ops_per_worker);

        // Run checkpoint
        session.checkpoint(&control).expect("checkpoint");
    }
}

/// Attempt-aware group commit: the first call applies and publishes a
/// durable idempotency record inside the coalesced envelope; a resubmission
/// of the same attempt returns the durable record without rerunning the
/// closure.
#[test]
fn group_commit_attempt_dedups_resubmission_on_seer() {
    let dir = tempdir().expect("tempdir");
    let db_cfg = RelationalDatabaseConfig {
        backend: backend_config(RelationalBackendKind::Seer, &dir.path().join("db")),
        session: RelationalSessionConfig {
            max_in_flight: 32,
            admission_timeout: std::time::Duration::from_secs(10),
        },
    };
    let session = RelationalDatabaseSession::create(db_cfg).expect("create session");
    let control = OperationControl::new();
    session
        .create_table(&control, users_schema())
        .expect("create table");

    let attempt = omendb::TransactionAttemptId::new([7u8; 16]);
    let mut runs = 0usize;
    let outcome = session
        .transaction_with_group_commit_attempt(&control, attempt, |db, tx| {
            runs += 1;
            tx.insert(db, USERS_TABLE, user_row(1, 100, "user_1"))
        })
        .expect("first submission applies");
    let commit = match outcome {
        omendb::TransactionAttemptOutcome::Applied { commit, .. } => commit,
        omendb::TransactionAttemptOutcome::AlreadyCommitted { .. } => {
            panic!("first submission must apply")
        }
    };
    assert_eq!(runs, 1);

    // The durable record published with the envelope.
    let record = session
        .resolve_attempt(&control, attempt)
        .expect("resolve")
        .expect("attempt record is durable");
    assert_eq!(record.commit, commit);

    // Resubmission dedups without rerunning the closure.
    let outcome = session
        .transaction_with_group_commit_attempt(&control, attempt, |db, tx| {
            runs += 1;
            tx.insert(db, USERS_TABLE, user_row(1, 999, "duplicate"))
        })
        .expect("resubmission resolves");
    match outcome {
        omendb::TransactionAttemptOutcome::AlreadyCommitted { record } => {
            assert_eq!(record.commit, commit);
        }
        omendb::TransactionAttemptOutcome::Applied { .. } => {
            panic!("resubmission must not reapply")
        }
    }
    assert_eq!(runs, 1);

    // The original row stands; no duplicate publication ran.
    let row = session
        .get(&control, USERS_TABLE, session.commit_id(&control).expect("commit id"), Key::new(USERS_TABLE.0, 1))
        .expect("get")
        .expect("row exists");
    assert_eq!(row.values[1], Value::U64(100));
}

/// A read-only attempt-aware transaction applies without publishing a
/// durable record, matching the exclusive-path semantics.
#[test]
fn group_commit_attempt_read_only_applies_without_record() {
    let dir = tempdir().expect("tempdir");
    let db_cfg = RelationalDatabaseConfig {
        backend: backend_config(RelationalBackendKind::Seer, &dir.path().join("db")),
        session: RelationalSessionConfig {
            max_in_flight: 32,
            admission_timeout: std::time::Duration::from_secs(10),
        },
    };
    let session = RelationalDatabaseSession::create(db_cfg).expect("create session");
    let control = OperationControl::new();
    session
        .create_table(&control, users_schema())
        .expect("create table");

    let attempt = omendb::TransactionAttemptId::new([9u8; 16]);
    let outcome = session
        .transaction_with_group_commit_attempt(&control, attempt, |_db, _tx| Ok(42u64))
        .expect("read-only applies");
    let snapshot = match outcome {
        omendb::TransactionAttemptOutcome::Applied { value, commit } => {
            assert_eq!(value, 42u64);
            commit
        }
        omendb::TransactionAttemptOutcome::AlreadyCommitted { .. } => {
            panic!("read-only must apply")
        }
    };
    assert!(
        session
            .resolve_attempt(&control, attempt)
            .expect("resolve")
            .is_none(),
        "read-only attempts publish no durable record"
    );
    let _ = snapshot;
}
