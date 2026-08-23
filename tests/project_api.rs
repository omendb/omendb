use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, ConstraintId, DatabaseConfig, ForeignKeyDefinition,
    IndexDefinition, IndexId, Key, RelationalBackendConfig, RelationalBackendKind,
    RelationalDatabase, RelationalMutation, RelationalSnapshotCaptureOptions, Row,
    SeerKernelConfig, TableDefinition, TableId, TransactionAttemptId, TransactionAttemptOutcome,
    Value,
};
use tempfile::tempdir;

const USERS: TableId = TableId(1);
const PROJECTS: TableId = TableId(2);
const USER_ID_INDEX: IndexId = IndexId(1);
const USER_EMAIL_INDEX: IndexId = IndexId(2);

fn config(kind: RelationalBackendKind, directory: &Path) -> RelationalBackendConfig {
    match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(SeerKernelConfig::new(directory.to_owned()))
        }
    }
}

fn users_table() -> TableDefinition {
    TableDefinition {
        id: USERS,
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
                name: "email".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn projects_table() -> TableDefinition {
    TableDefinition {
        id: PROJECTS,
        name: "projects".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "owner_id".to_owned(),
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

fn user(id: u64, email: &str) -> Row {
    Row {
        primary: Key::new(USERS.0, id),
        values: vec![Value::U64(id), Value::Text(email.to_owned())],
    }
}

fn project(id: u64, owner_id: u64, name: &str) -> Row {
    Row {
        primary: Key::new(PROJECTS.0, id),
        values: vec![
            Value::U64(id),
            Value::U64(owner_id),
            Value::Text(name.to_owned()),
        ],
    }
}

fn exercise_project_api(kind: RelationalBackendKind, directory: &Path) -> [u8; 32] {
    let database_config = config(kind, directory);
    let mut database = RelationalDatabase::create(database_config.clone()).expect("create");
    assert_eq!(database.backend(), kind);
    let source_identity = database.storage_identity().expect("identity");

    database.create_table(users_table()).expect("users table");
    database
        .create_table(projects_table())
        .expect("projects table");
    database
        .create_index(IndexDefinition {
            id: USER_ID_INDEX,
            table: USERS,
            columns: vec![ColumnId(1)],
            unique: true,
        })
        .expect("user ID index");
    database
        .create_index(IndexDefinition {
            id: USER_EMAIL_INDEX,
            table: USERS,
            columns: vec![ColumnId(2)],
            unique: true,
        })
        .expect("user email index");
    database
        .create_foreign_key(ForeignKeyDefinition {
            id: ConstraintId(1),
            table: PROJECTS,
            columns: vec![ColumnId(2)],
            referenced_table: USERS,
            referenced_columns: vec![ColumnId(1)],
            on_delete: omendb::ReferentialAction::default(),
            timing: omendb::ConstraintTiming::default(),
        })
        .expect("project owner foreign key");

    let (indexed_users, seed_commit) = database
        .transaction(|database, transaction| {
            transaction.insert(database, USERS, user(1, "alice@example.test"))?;
            transaction.insert(database, PROJECTS, project(7, 1, "alpha"))?;
            transaction.index_get(
                database,
                USERS,
                USER_EMAIL_INDEX,
                &[Value::Text("alice@example.test".to_owned())],
            )
        })
        .expect("seed transaction");
    assert_eq!(indexed_users, vec![user(1, "alice@example.test")]);

    let lease = database.retain(seed_commit).expect("retain seed snapshot");
    let update_commit = database
        .update(PROJECTS, project(7, 1, "renamed"))
        .expect("update project");
    let current_lease = database
        .retain(update_commit)
        .expect("retain current snapshot");
    assert_eq!(
        database.retained_snapshot_commits(),
        vec![seed_commit, update_commit]
    );
    let capture = database
        .capture_selected_snapshots(
            &[update_commit, seed_commit],
            RelationalSnapshotCaptureOptions::new(10),
        )
        .expect("capture retained snapshots");
    assert_eq!(capture.source_backend, kind);
    assert_eq!(capture.source_identity, source_identity);
    assert_eq!(capture.source_head, update_commit);
    assert_eq!(
        capture
            .snapshots
            .iter()
            .map(|snapshot| snapshot.commit)
            .collect::<Vec<_>>(),
        vec![seed_commit, update_commit]
    );
    assert_eq!(capture.snapshots[0].tables.len(), 2);
    assert_eq!(
        capture.snapshots[0].tables[1].rows,
        vec![project(7, 1, "alpha")]
    );
    assert_eq!(
        capture.snapshots[1].tables[1].rows,
        vec![project(7, 1, "renamed")]
    );
    assert_ne!(
        capture.snapshots[0].logical_digest,
        capture.snapshots[1].logical_digest
    );
    let captured_current_digest = capture.snapshots[1].logical_digest;
    assert_eq!(
        database.retained_snapshot_commits(),
        vec![seed_commit, update_commit]
    );
    let limited = database
        .capture_selected_snapshots(&[seed_commit], RelationalSnapshotCaptureOptions::new(1));
    assert!(matches!(
        limited,
        Err(omendb::DbError::SnapshotCaptureLimit {
            resource: "rows",
            limit: 1,
        })
    ));
    assert_eq!(
        database
            .get(PROJECTS, seed_commit, Key::new(PROJECTS.0, 7))
            .expect("historical project"),
        Some(project(7, 1, "alpha"))
    );
    assert_eq!(
        database
            .get(PROJECTS, update_commit, Key::new(PROJECTS.0, 7))
            .expect("current project"),
        Some(project(7, 1, "renamed"))
    );
    database.release(lease).expect("release snapshot");
    assert_eq!(database.retained_snapshot_commits(), vec![update_commit]);
    database
        .release(current_lease)
        .expect("release current snapshot");
    assert!(database.retained_snapshot_commits().is_empty());
    assert!(matches!(
        database.capture_selected_snapshots(
            &[seed_commit],
            RelationalSnapshotCaptureOptions::new(10),
        ),
        Err(omendb::DbError::SnapshotUnavailable(snapshot)) if snapshot == seed_commit.0
    ));

    database.verify().expect("verify");
    database.checkpoint().expect("checkpoint");
    database.compact().expect("compact");
    database.close().expect("close");

    let mut reopened = RelationalDatabase::open(database_config).expect("reopen");
    assert_eq!(
        reopened.storage_identity().expect("reopened identity"),
        source_identity
    );
    assert_eq!(reopened.commit_id(), update_commit);
    assert_eq!(
        reopened
            .index_get(
                USERS,
                update_commit,
                USER_EMAIL_INDEX,
                &[Value::Text("alice@example.test".to_owned())],
            )
            .expect("reopened indexed user"),
        vec![user(1, "alice@example.test")]
    );
    assert_eq!(
        reopened
            .scan(PROJECTS, update_commit, 10)
            .expect("reopened project scan"),
        vec![project(7, 1, "renamed")]
    );
    reopened.verify().expect("reopened verify");
    reopened.close().expect("reopened close");
    captured_current_digest
}

fn exercise_attempt_api(kind: RelationalBackendKind, directory: &Path) {
    let database_config = config(kind, directory);
    let mut database = RelationalDatabase::create(database_config.clone()).expect("create");
    database.create_table(users_table()).expect("users table");

    let attempt = TransactionAttemptId::new([41; 16]);
    let first = database
        .transaction_with_attempt(attempt, |database, transaction| {
            transaction.insert(database, USERS, user(41, "attempt@example.test"))?;
            Ok(())
        })
        .expect("attempt transaction");
    let commit = match first {
        TransactionAttemptOutcome::Applied { value: (), commit } => commit,
        TransactionAttemptOutcome::AlreadyCommitted { .. } => {
            panic!("fresh attempt must not already be committed")
        }
    };

    let duplicate = database
        .transaction_with_attempt::<(), _>(attempt, |_database, _transaction| {
            panic!("committed attempt must not rerun the closure")
        })
        .expect("duplicate attempt resolution");
    match duplicate {
        TransactionAttemptOutcome::AlreadyCommitted { record } => {
            assert_eq!(record.attempt, attempt);
            assert_eq!(record.commit, commit);
        }
        TransactionAttemptOutcome::Applied { .. } => {
            panic!("duplicate attempt must return its durable record")
        }
    }
    let record = database
        .resolve_attempt(attempt)
        .expect("resolve attempt")
        .expect("attempt record");
    assert_eq!(record.attempt, attempt);
    assert_eq!(record.commit, commit);

    let conflict = database.commit_batch_with_attempt(
        [RelationalMutation::Update {
            table: USERS,
            row: user(41, "different@example.test"),
        }],
        attempt,
    );
    assert!(matches!(
        conflict,
        Err(omendb::DbError::IdempotencyConflict { attempt: actual, .. })
            if actual == attempt
    ));
    assert_eq!(
        database
            .get(USERS, commit, Key::new(USERS.0, 41))
            .expect("attempt row"),
        Some(user(41, "attempt@example.test"))
    );
    database.close().expect("close");

    let mut reopened = RelationalDatabase::open(database_config).expect("reopen");
    let record = reopened
        .resolve_attempt(attempt)
        .expect("reopen resolve")
        .expect("durable attempt");
    assert_eq!(record.commit, commit);
    assert_eq!(
        reopened
            .get(USERS, record.commit, Key::new(USERS.0, 41))
            .expect("reopened attempt row"),
        Some(user(41, "attempt@example.test"))
    );
    assert_eq!(
        reopened
            .forget_attempts(&[attempt])
            .expect("forget attempt"),
        1
    );
    assert!(
        reopened
            .resolve_attempt(attempt)
            .expect("forgotten attempt")
            .is_none()
    );
    reopened.close().expect("reopened close");
}

#[test]
fn project_facing_api_supports_an_ordinary_transactional_lifecycle() {
    let temporary = tempdir().expect("temporary directory");
    let temporary_digest = exercise_project_api(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    let seer_digest = exercise_project_api(RelationalBackendKind::Seer, &seer.path().join("seer"));
    assert_eq!(temporary_digest, seer_digest);
}

#[test]
fn project_facing_attempts_reconcile_duplicate_and_conflicting_reuse() {
    let temporary = tempdir().expect("temporary directory");
    exercise_attempt_api(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    exercise_attempt_api(RelationalBackendKind::Seer, &seer.path().join("seer"));
}
