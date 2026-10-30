use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DbError, IndexDefinition, IndexId, IndexKeyPart, Key,
    RelationalBackendConfig, RelationalDatabase, TableDefinition, TableId, Value,
};
use tempfile::tempdir;

const ACCOUNTS: TableId = TableId(1);

fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

fn accounts_table() -> TableDefinition {
    TableDefinition {
        id: ACCOUNTS,
        name: "accounts".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "balance".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "state".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn account(id: u64, balance: u64, state: &str) -> omendb::Row {
    omendb::Row {
        primary: Key::new(ACCOUNTS.0, id),
        values: vec![Value::U64(balance), Value::Text(state.to_owned())],
    }
}

fn exercise_transaction_contract(directory: &Path) {
    let mut database = RelationalDatabase::create(config(directory)).expect("create");
    database
        .create_table(accounts_table())
        .expect("accounts table");
    let seed = database
        .commit_batch([
            omendb::RelationalMutation::Insert {
                table: ACCOUNTS,
                row: account(1, 100, "open"),
            },
            omendb::RelationalMutation::Insert {
                table: ACCOUNTS,
                row: account(2, 100, "open"),
            },
        ])
        .expect("seed accounts");

    // A read-only transaction keeps one snapshot even when a later commit
    // advances the database head. It returns that snapshot without a no-op
    // publication.
    let mut reader = database.begin().expect("begin reader");
    assert_eq!(
        reader.scan(&database, ACCOUNTS, 10).expect("initial scan"),
        vec![account(1, 100, "open"), account(2, 100, "open")]
    );
    let update = database
        .update(ACCOUNTS, account(1, 90, "open"))
        .expect("advance head");
    assert_eq!(
        reader
            .get(&database, ACCOUNTS, Key::new(ACCOUNTS.0, 1))
            .expect("repeatable point read"),
        Some(account(1, 100, "open"))
    );
    assert_eq!(reader.commit().expect("read-only commit"), seed);
    assert_eq!(database.commit_id(), update);

    // A closure failure drops all staged mutations. No partial row may be
    // visible at the current head.
    let aborted = database.transaction(|database, transaction| {
        transaction.insert(database, ACCOUNTS, account(3, 50, "aborted"))?;
        Err::<(), _>(DbError::InvalidState(
            "intentional transaction abort".to_owned(),
        ))
    });
    assert!(matches!(
        aborted,
        Err(DbError::InvalidState(reason)) if reason == "intentional transaction abort"
    ));
    assert_eq!(
        database
            .get(ACCOUNTS, Key::new(ACCOUNTS.0, 3))
            .expect("aborted row lookup"),
        None
    );

    database.close().expect("close");
}

fn assert_aborted<T>(result: omendb::Result<T>) {
    match result {
        Err(DbError::TransactionAborted) => {}
        Err(error) => panic!("expected aborted transaction, got {error}"),
        Ok(_) => panic!("aborted transaction accepted an operation"),
    }
}

fn indexed_database(directory: &Path) -> RelationalDatabase {
    let mut database = RelationalDatabase::create(config(directory)).unwrap();
    database.create_table(accounts_table()).unwrap();
    for (id, column, unique) in [(2, 1, false), (9, 2, true)] {
        database
            .create_index(IndexDefinition {
                id: IndexId(id),
                table: ACCOUNTS,
                parts: vec![IndexKeyPart::Column(ColumnId(column))],
                predicate: None,
                unique,
            })
            .unwrap();
    }
    database.insert(ACCOUNTS, account(1, 100, "one")).unwrap();
    database.insert(ACCOUNTS, account(2, 100, "two")).unwrap();
    database
}

fn assert_indexed_seed(database: &RelationalDatabase) {
    let rows = vec![account(1, 100, "one"), account(2, 100, "two")];
    assert_eq!(database.scan(ACCOUNTS, 10).unwrap(), rows);
    assert_eq!(
        database
            .index_get(ACCOUNTS, IndexId(2), &[Value::U64(100)])
            .unwrap(),
        rows
    );
    for (id, state) in [(1, "one"), (2, "two")] {
        assert_eq!(
            database
                .index_get(ACCOUNTS, IndexId(9), &[Value::Text(state.to_owned())])
                .unwrap(),
            [account(id, 100, state)]
        );
    }
    assert_eq!(database.index_scan(ACCOUNTS, IndexId(2)).unwrap(), rows);
    assert_eq!(database.index_scan(ACCOUNTS, IndexId(9)).unwrap(), rows);
}

#[test]
fn ignored_typed_unique_errors_cannot_commit_partial_rows_or_indexes() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    let database = indexed_database(&path);
    let seed = database.commit_id();
    for update in [false, true] {
        let mut transaction = database.begin().unwrap();
        transaction
            .insert(&database, ACCOUNTS, account(3, 200, "earlier"))
            .unwrap();
        let result = if update {
            transaction.update(&database, ACCOUNTS, account(2, 300, "one"))
        } else {
            transaction.insert(&database, ACCOUNTS, account(4, 300, "one"))
        };
        assert!(matches!(
            result,
            Err(DbError::UniqueViolation { index: 9, .. })
        ));
        assert_eq!(transaction.snapshot(), seed);
        assert_aborted(transaction.scan(&database, ACCOUNTS, 10));
        assert_aborted(transaction.execute_sql(&database, "SELECT 1"));
        assert_aborted(transaction.insert(&database, ACCOUNTS, account(5, 400, "later")));
        assert_aborted(transaction.commit());
        assert_eq!(database.commit_id(), seed);
        assert_indexed_seed(&database);
    }
    database.close().unwrap();
    let reopened = RelationalDatabase::open(config(&path)).unwrap();
    assert_indexed_seed(&reopened);
    reopened.close().unwrap();
}

#[test]
fn caught_operation_errors_cannot_turn_transaction_closures_into_success() {
    let directory = tempdir().unwrap();
    let mut database = indexed_database(&directory.path().join("db"));
    for write_first in [false, true] {
        let result = database.transaction(|database, transaction| {
            if write_first {
                transaction.insert(database, ACCOUNTS, account(3, 200, "earlier"))?;
            }
            assert!(matches!(
                transaction.insert_heap(database, ACCOUNTS, account(4, 300, "one")),
                Err(DbError::UniqueViolation { .. })
            ));
            Ok(())
        });
        assert_aborted(result);
        assert_indexed_seed(&database);
    }
    database.close().unwrap();
}

#[test]
fn ignored_sql_parse_bind_and_execution_errors_abort_the_native_transaction() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("db");
    let mut database = RelationalDatabase::create(config(&path)).unwrap();
    database
        .execute_sql(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, email TEXT, n BIGINT, UNIQUE (email))",
        )
        .unwrap();
    database
        .execute_sql("INSERT INTO items VALUES (1, 'one', 10), (2, 'two', 20)")
        .unwrap();
    type ErrorCheck = fn(&DbError) -> bool;
    let cases: [(&str, ErrorCheck); 8] = [
        (
            "INSERT INTO items VALUES (4, 'four', 40), (5, 'one', 50)",
            |e| matches!(e, DbError::UniqueViolation { .. }),
        ),
        ("UPDATE items SET email = 'same'", |e| {
            matches!(e, DbError::UniqueViolation { .. })
        }),
        ("UPDATE items SET n = n + 9223372036854775792", |e| {
            matches!(e, DbError::SqlNumericValueOutOfRange(_))
        }),
        ("INSERT nonsense", |e| matches!(e, DbError::SqlParse(_))),
        ("SELECT $1", |e| matches!(e, DbError::SqlParameter(_))),
        ("SELECT * FROM missing", |e| {
            matches!(e, DbError::SqlUndefinedTable { .. })
        }),
        ("INSERT INTO items (missing) VALUES (1)", |e| {
            matches!(e, DbError::SqlUndefinedColumn { .. })
        }),
        ("ROLLBACK TO SAVEPOINT missing", |e| {
            matches!(e, DbError::SqlUnsupported { .. })
        }),
    ];
    for (sql, expected_error) in cases {
        let mut transaction = database.begin().unwrap();
        transaction
            .execute_sql(&database, "INSERT INTO items VALUES (3, 'earlier', 30)")
            .unwrap();
        let error = transaction
            .execute_sql_with_params(&database, sql, &[])
            .expect_err(sql);
        assert!(expected_error(&error), "{sql}: {error}");
        assert_aborted(transaction.execute_sql(&database, "SELECT 1"));
        assert_aborted(transaction.commit());
        assert_eq!(
            database
                .execute_sql("SELECT id FROM items ORDER BY id")
                .unwrap()
                .rows,
            [vec![Value::I64(1)], vec![Value::I64(2)]]
        );
        assert_eq!(
            database
                .execute_sql("SELECT id FROM items WHERE email = 'one'")
                .unwrap()
                .rows,
            [vec![Value::I64(1)]]
        );
        assert_eq!(
            database
                .execute_sql("SELECT id FROM items WHERE email = 'two'")
                .unwrap()
                .rows,
            [vec![Value::I64(2)]]
        );
    }
    database.close().unwrap();
    let mut reopened = RelationalDatabase::open(config(&path)).unwrap();
    assert_eq!(
        reopened
            .execute_sql("SELECT id, email, n FROM items ORDER BY id")
            .unwrap()
            .rows,
        [
            vec![Value::I64(1), Value::Text("one".to_owned()), Value::I64(10)],
            vec![Value::I64(2), Value::Text("two".to_owned()), Value::I64(20)]
        ]
    );
    reopened.close().unwrap();
}

#[test]
fn validation_and_read_errors_also_discard_prior_writes() {
    let directory = tempdir().unwrap();
    let database = indexed_database(&directory.path().join("db"));
    let other = RelationalDatabase::create(config(&directory.path().join("other"))).unwrap();
    for operation in ["get", "delete", "scan", "owner"] {
        let mut transaction = database.begin().unwrap();
        transaction
            .insert(&database, ACCOUNTS, account(3, 200, "earlier"))
            .unwrap();
        let error = match operation {
            "get" => transaction
                .get(&database, ACCOUNTS, Key::new(99, 1))
                .map(|_| ()),
            "delete" => transaction.delete(&database, ACCOUNTS, Key::new(99, 1)),
            "scan" => transaction.scan(&database, TableId(99), 10).map(|_| ()),
            "owner" => transaction.execute_sql(&other, "SELECT 1").map(|_| ()),
            _ => unreachable!(),
        }
        .expect_err(operation);
        assert!(
            matches!(error, DbError::InvalidState(_)),
            "{operation}: {error}"
        );
        assert_aborted(transaction.execute_sql(&database, "SELECT 1"));
        assert_aborted(transaction.commit());
        assert_indexed_seed(&database);
    }
    other.close().unwrap();
    database.close().unwrap();
}

#[test]
fn observed_cancellation_cannot_be_caught_to_reuse_the_transaction() {
    let directory = tempdir().unwrap();
    let database = indexed_database(&directory.path().join("db"));
    let token = omendb::CancellationToken::new();
    let control = omendb::OperationControl::with_cancellation(token.clone());
    let mut transaction = database.begin_with_control(&control).unwrap();
    transaction
        .insert(&database, ACCOUNTS, account(3, 200, "earlier"))
        .unwrap();
    token.cancel();
    assert!(matches!(
        transaction.execute_sql(&database, "SELECT 1"),
        Err(DbError::Cancelled)
    ));
    assert_aborted(transaction.scan(&database, ACCOUNTS, 10));
    assert_aborted(transaction.commit());
    assert_indexed_seed(&database);

    // A scanner has its own per-call control. Replacing that control after
    // observing cancellation must not revive the enclosing native transaction.
    let mut transaction = database.begin().unwrap();
    transaction
        .insert(&database, ACCOUNTS, account(3, 200, "earlier"))
        .unwrap();
    let mut scanner = omendb::MorselScanner::new(ACCOUNTS, 1);
    assert!(matches!(
        scanner.next_morsel(&database, &mut transaction, &control),
        Err(DbError::Cancelled)
    ));
    assert_aborted(scanner.next_morsel(
        &database,
        &mut transaction,
        &omendb::OperationControl::default(),
    ));
    assert_aborted(transaction.commit());
    assert_indexed_seed(&database);
    database.close().unwrap();
}

#[test]
fn project_facing_transactions_match_the_fixed_snapshot_profile() {
    let temporary = tempdir().expect("temporary directory");
    exercise_transaction_contract(&temporary.path().join("temporary"));
}
