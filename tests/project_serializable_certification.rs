use std::path::Path;
use std::sync::{Arc, Barrier};

use omendb::{
    CertificationConflict, CertifierAlgorithm, ColumnDefinition, ColumnId, ColumnType,
    DatabaseConfig, DbError, IndexDefinition, IndexId, Key, OperationControl,
    RelationalBackendConfig, RelationalBackendKind, RelationalDatabase, RelationalDatabaseConfig,
    RelationalDatabaseSession, RelationalSessionConfig, Row, SerializableCertifier,
    TableDefinition, TableId, TransactionDependencySpec, Value,
};
use tempfile::tempdir;

const TABLE: TableId = TableId(200);
const INDEX_STATUS: IndexId = IndexId(201);

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

fn table_def() -> TableDefinition {
    TableDefinition {
        id: TABLE,
        name: "concurrency_items".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "status".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "balance".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

fn item_row(id: u64, status: &str, balance: u64) -> Row {
    Row {
        primary: Key::new(TABLE.0, id),
        values: vec![
            Value::U64(id),
            Value::Text(status.to_owned()),
            Value::U64(balance),
        ],
    }
}

#[test]
fn validated_parallel_preparation_admits_disjoint_concurrent_writers() {
    let dir = tempdir().expect("tempdir");
    let session = Arc::new(
        RelationalDatabaseSession::create(
            RelationalDatabaseConfig::new(backend_config(
                RelationalBackendKind::Temporary,
                &dir.path().join("db"),
            ))
            .with_session_config(RelationalSessionConfig {
                max_in_flight: 4,
                ..RelationalSessionConfig::default()
            }),
        )
        .expect("create session"),
    );

    let control = OperationControl::default();
    session.create_table(&control, table_def()).expect("table");
    session
        .insert(&control, TABLE, item_row(1, "active", 100))
        .expect("row 1");
    session
        .insert(&control, TABLE, item_row(2, "active", 200))
        .expect("row 2");

    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();

    for id in 1..=2 {
        let session = Arc::clone(&session);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            session.transaction_with_validated_parallel_preparation(
                &OperationControl::default(),
                move |database, transaction| {
                    let mut row = transaction
                        .get(database, TABLE, Key::new(TABLE.0, id))?
                        .expect("existing row");
                    row.values[2] = Value::U64(id * 1000);
                    transaction.update(database, TABLE, row)?;
                    barrier.wait();
                    Ok(())
                },
            )
        }));
    }

    let mut commits = 0;
    let mut conflicts = 0;
    for worker in workers {
        match worker.join().expect("worker join") {
            Ok(((), _)) => commits += 1,
            Err(DbError::SerializationConflict { .. }) => conflicts += 1,
            Err(err) => panic!("unexpected error: {err:?}"),
        }
    }

    // Both disjoint writers commit successfully under validated parallel preparation
    assert_eq!(commits, 2);
    assert_eq!(conflicts, 0);

    // Verify both updates took effect
    let snapshot = session.commit_id(&control).expect("head");
    let row1 = session
        .get(&control, TABLE, snapshot, Key::new(TABLE.0, 1))
        .expect("row 1")
        .expect("present");
    let row2 = session
        .get(&control, TABLE, snapshot, Key::new(TABLE.0, 2))
        .expect("row 2")
        .expect("present");
    assert_eq!(row1.values[2], Value::U64(1000));
    assert_eq!(row2.values[2], Value::U64(2000));

    let session = match Arc::try_unwrap(session) {
        Ok(s) => s,
        Err(_) => panic!("dangling session ref"),
    };
    session.close().expect("close");
}

#[test]
fn validated_parallel_preparation_aborts_write_write_conflicts() {
    let dir = tempdir().expect("tempdir");
    let session = Arc::new(
        RelationalDatabaseSession::create(
            RelationalDatabaseConfig::new(backend_config(
                RelationalBackendKind::Temporary,
                &dir.path().join("db"),
            ))
            .with_session_config(RelationalSessionConfig {
                max_in_flight: 4,
                ..RelationalSessionConfig::default()
            }),
        )
        .expect("create session"),
    );

    let control = OperationControl::default();
    session.create_table(&control, table_def()).expect("table");
    session
        .insert(&control, TABLE, item_row(1, "active", 100))
        .expect("row 1");

    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();

    for delta in [10, 20] {
        let session = Arc::clone(&session);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            session.transaction_with_validated_parallel_preparation(
                &OperationControl::default(),
                move |database, transaction| {
                    let mut row = transaction
                        .get(database, TABLE, Key::new(TABLE.0, 1))?
                        .expect("existing row");
                    row.values[2] = Value::U64(100 + delta);
                    transaction.update(database, TABLE, row)?;
                    barrier.wait();
                    Ok(())
                },
            )
        }));
    }

    let mut commits = 0;
    let mut conflicts = 0;
    for worker in workers {
        match worker.join().expect("worker join") {
            Ok(((), _)) => commits += 1,
            Err(DbError::SerializationConflict { .. }) => conflicts += 1,
            Err(err) => panic!("unexpected error: {err:?}"),
        }
    }

    // Exactly one commits; the conflicting write-write is aborted
    assert_eq!(commits, 1);
    assert_eq!(conflicts, 1);

    let session = match Arc::try_unwrap(session) {
        Ok(s) => s,
        Err(_) => panic!("dangling session ref"),
    };
    session.close().expect("close");
}

#[test]
fn validated_parallel_preparation_detects_range_phantom_conflicts() {
    let dir = tempdir().expect("tempdir");
    let mut database = RelationalDatabase::open(backend_config(
        RelationalBackendKind::Temporary,
        &dir.path().join("db"),
    ))
    .expect("open db");

    database.create_table(table_def()).expect("table");
    database
        .insert(TABLE, item_row(10, "active", 100))
        .expect("row 10");
    database
        .insert(TABLE, item_row(20, "active", 200))
        .expect("row 20");

    let mut tx1 = database.begin().expect("begin tx1");
    let rows = tx1.scan(&database, TABLE, 100).expect("scan rows");
    let total: u64 = rows.iter().map(|r| match r.values[2] { Value::U64(v) => v, _ => 0 }).sum();
    tx1.insert(&database, TABLE, item_row(99, "summary", total)).expect("stage insert");

    // Concurrently, an intervening commit inserts row 15 inside the scanned range
    database.insert(TABLE, item_row(15, "active", 150)).expect("intervening insert");

    // Tx1 attempts to commit with precise validation; it must detect the range phantom conflict
    let res1 = tx1.commit_validated(&mut database);
    assert!(
        matches!(res1, Err(DbError::SerializationConflict { .. })),
        "Tx1 scan must conflict with concurrent range insert, got: {res1:?}"
    );
}

#[test]
fn validated_parallel_preparation_detects_secondary_index_phantom_conflicts() {
    let dir = tempdir().expect("tempdir");
    let mut database = RelationalDatabase::open(backend_config(
        RelationalBackendKind::Temporary,
        &dir.path().join("db"),
    ))
    .expect("open db");

    database.create_table(table_def()).expect("table");
    database
        .create_index(IndexDefinition {
            id: INDEX_STATUS,
            table: TABLE,
            columns: vec![ColumnId(2)],
            unique: false,
        })
        .expect("index");

    database
        .insert(TABLE, item_row(1, "pending", 10))
        .expect("row 1");

    // Tx1 reads secondary index
    let mut tx1 = database.begin().expect("begin tx1");
    let rows = tx1.index_get(&database, TABLE, INDEX_STATUS, &[Value::Text("pending".to_owned())]).expect("index get");
    tx1.insert(&database, TABLE, item_row(99, "summary", rows.len() as u64)).expect("stage insert");

    // Intervening transaction inserts a new row with status="pending"
    database.insert(TABLE, item_row(2, "pending", 20)).expect("intervening insert");

    // Tx1 attempts to commit with precise validation; it must detect the index modification
    let res1 = tx1.commit_validated(&mut database);
    assert!(
        matches!(res1, Err(DbError::SerializationConflict { .. })),
        "Tx1 index read must conflict with concurrent index insert, got: {res1:?}"
    );
}

#[test]
fn serializable_certifier_comprehensive_matrix() {
    let mut certifier = SerializableCertifier::new(CertifierAlgorithm::PreciseValidation);

    // Initial state at snapshot 0
    let tx_base = TransactionDependencySpec::new(0, omendb::CommitId(0));
    let c0 = certifier.commit(tx_base);
    assert_eq!(c0, omendb::CommitId(0));

    // Tx1 commits at snapshot 0, writing key 1
    let mut tx1 = TransactionDependencySpec::new(1, omendb::CommitId(0));
    tx1.write(Key::new(1, 1), b"initial".to_vec());
    let c1 = certifier.commit(tx1);
    assert_eq!(c1, omendb::CommitId(1));

    // Tx2 started at snapshot 0 (before key 1 was written) and checks uniqueness of key 1
    let mut tx2_unique = TransactionDependencySpec::new(2, omendb::CommitId(0));
    tx2_unique.check_unique(Key::new(1, 1));
    let err = certifier.validate(&tx2_unique);
    assert_eq!(
        err,
        Err(CertificationConflict::UniqueConstraintViolation {
            key: Key::new(1, 1)
        })
    );

    // Foreign key constraint violation test:
    // Tx3 started at snapshot 1 (where parent key 1 exists) and references parent key 1
    let mut tx3_fk = TransactionDependencySpec::new(3, c1);
    tx3_fk.check_foreign_key(Key::new(1, 1));
    assert!(certifier.validate(&tx3_fk).is_ok());

    // Intervening Tx4 deletes parent key 1
    let mut tx4_delete_parent = TransactionDependencySpec::new(4, c1);
    tx4_delete_parent.delete(Key::new(1, 1));
    let _c4 = certifier.commit(tx4_delete_parent);

    // Now Tx3 fails validation because parent key 1 was deleted after Tx3's snapshot
    let err_fk = certifier.validate(&tx3_fk);
    assert_eq!(
        err_fk,
        Err(CertificationConflict::ForeignKeyViolation {
            key: Key::new(1, 1)
        })
    );

    assert!(certifier.metrics().constraint_conflicts >= 2);
}
