use std::path::Path;

use omendb::{
    ConstraintId, DbError, RelationalBackendConfig, RelationalCapability,
    RelationalCapabilityState, RelationalDatabase, Value,
};
use tempfile::tempdir;

fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

fn exercise_sql(directory: &Path) -> Vec<Vec<Value>> {
    let database_config = config(directory);
    let mut database = RelationalDatabase::create(database_config.clone()).expect("create");
    assert_eq!(
        database.capabilities().state(RelationalCapability::Sql),
        RelationalCapabilityState::Supported
    );

    let schema = database
        .execute_sql(
            "CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, state TEXT NOT NULL)",
        )
        .expect("create table");
    assert_eq!(schema.affected_rows, 0);
    assert!(schema.commit.is_some());

    let inserted = database
        .execute_sql(
            "INSERT INTO accounts VALUES (1, 100, 'open'), (2, 40, 'open'), (3, 10, 'closed')",
        )
        .expect("insert rows");
    assert_eq!(inserted.affected_rows, 3);

    let before_batch = database.commit_id();
    assert!(
        database
            .execute_sql_batch(&[
                "UPDATE accounts SET balance = 101 WHERE id = 1",
                "INSERT INTO accounts VALUES (1, 1, 'duplicate')",
            ])
            .is_err()
    );
    assert_eq!(database.commit_id(), before_batch);
    assert_eq!(
        database
            .execute_sql("SELECT balance FROM accounts WHERE id = 1")
            .expect("rolled-back batch read")
            .rows,
        vec![vec![Value::I64(100)]]
    );

    let index = database
        .execute_sql("CREATE INDEX accounts_state_idx ON accounts (state)")
        .expect("create named index");
    assert!(index.commit.is_some());
    let state_index = database
        .catalog()
        .indexes()
        .find(|index| database.catalog().index_name(index.id) == Some("accounts_state_idx"))
        .map(|index| index.id);
    assert!(state_index.is_some());

    let altered = database
        .execute_sql("ALTER TABLE accounts ADD COLUMN metadata TEXT")
        .expect("add nullable column");
    assert!(altered.commit.is_some());
    assert_eq!(
        database
            .execute_sql("SELECT id, metadata FROM accounts WHERE id = 1")
            .expect("materialize nullable column")
            .rows,
        vec![vec![Value::I64(1), Value::Null]]
    );
    assert_eq!(
        database
            .execute_sql("INSERT INTO accounts (id, balance, state) VALUES (4, 90, 'open')")
            .expect("insert with omitted nullable column")
            .affected_rows,
        1
    );
    let before_invalid_schema = database.commit_id();
    assert!(matches!(
        database.execute_sql("ALTER TABLE accounts ADD COLUMN state TEXT"),
        Err(DbError::InvalidState(reason)) if reason.contains("already exists")
    ));
    assert!(matches!(
        database.execute_sql("ALTER TABLE accounts ADD COLUMN required TEXT NOT NULL"),
        Err(DbError::SqlUnsupported {
            statement: "ALTER TABLE",
            ..
        })
    ));
    assert_eq!(database.commit_id(), before_invalid_schema);
    let before_invalid_insert = database.commit_id();
    assert!(matches!(
        database.execute_sql("INSERT INTO accounts (id, balance) VALUES (6, 1)"),
        Err(DbError::InvalidState(reason)) if reason.contains("state")
    ));
    assert_eq!(database.commit_id(), before_invalid_insert);

    let selected = database
        .execute_sql(
            "SELECT id, balance, state FROM accounts WHERE balance >= 40 AND state = 'open' LIMIT 2",
        )
        .expect("select rows");
    assert_eq!(
        selected
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "balance", "state"]
    );
    assert_eq!(
        selected.rows,
        vec![
            vec![
                Value::I64(1),
                Value::I64(100),
                Value::Text("open".to_owned())
            ],
            vec![
                Value::I64(2),
                Value::I64(40),
                Value::Text("open".to_owned())
            ],
        ]
    );
    assert_eq!(
        database
            .execute_sql("SELECT DISTINCT state FROM accounts LIMIT 2")
            .expect("distinct values before limit")
            .rows,
        vec![
            vec![Value::Text("open".to_owned())],
            vec![Value::Text("closed".to_owned())],
        ]
    );
    assert_eq!(
        database
            .execute_sql("SELECT DISTINCT state FROM accounts LIMIT 2 OFFSET 1")
            .expect("distinct values with offset")
            .rows,
        vec![vec![Value::Text("closed".to_owned())]],
    );
    assert_eq!(
        database
            .execute_sql("SELECT id FROM accounts WHERE state IN (NULL, 'open') ORDER BY id")
            .expect("IN preserves SQL NULL semantics")
            .rows,
        vec![
            vec![Value::I64(1)],
            vec![Value::I64(2)],
            vec![Value::I64(4)]
        ],
    );
    assert_eq!(
        database
            .execute_sql("SELECT id FROM accounts WHERE state NOT IN (NULL, 'open')")
            .expect("NOT IN preserves SQL NULL semantics")
            .rows,
        Vec::<Vec<Value>>::new(),
    );
    assert_eq!(
        database
            .execute_sql("SELECT id FROM accounts WHERE state NOT IN ('open')")
            .expect("NOT IN without NULL")
            .rows,
        vec![vec![Value::I64(3)]],
    );
    assert_eq!(
        database
            .execute_sql("SELECT id FROM accounts ORDER BY state ASC LIMIT 4")
            .expect("stable ordering tie-breaker")
            .rows,
        vec![
            vec![Value::I64(3)],
            vec![Value::I64(2)],
            vec![Value::I64(1)],
            vec![Value::I64(4)],
        ]
    );
    let empty = database
        .execute_sql("SELECT id FROM accounts LIMIT 0")
        .expect("zero limit");
    assert_eq!(
        empty
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id"]
    );
    assert!(empty.rows.is_empty());
    assert!(matches!(
        database.execute_sql("SELECT id FROM accounts WHERE missing = 1"),
        Err(DbError::InvalidState(reason)) if reason.contains("unknown SQL column")
    ));

    let updated = database
        .execute_sql("UPDATE accounts SET state = 'closed' WHERE id = 2")
        .expect("update row");
    assert_eq!(updated.affected_rows, 1);
    // Equality on an indexed column resolves through the secondary index;
    // results must stay identical to a full scan.
    assert_eq!(
        database
            .execute_sql("SELECT id FROM accounts WHERE state = 'open'")
            .expect("indexed equality select")
            .rows,
        vec![vec![Value::I64(1)], vec![Value::I64(4)]]
    );
    let deleted = database
        .execute_sql("DELETE FROM accounts WHERE state = 'closed'")
        .expect("delete rows");
    assert_eq!(deleted.affected_rows, 2);
    assert!(
        database
            .execute_sql("SELECT id FROM accounts WHERE state = 'closed'")
            .expect("indexed equality after delete")
            .rows
            .is_empty()
    );

    let (transaction_result, transaction_commit) = database
        .transaction(|database, transaction| {
            let inserted = transaction.execute_sql(
                database,
                "INSERT INTO accounts VALUES (5, 90, 'open', 'transactional')",
            )?;
            let updated = transaction
                .execute_sql(database, "UPDATE accounts SET balance = 95 WHERE id = 5")?;
            Ok::<_, DbError>((inserted.affected_rows, updated.affected_rows))
        })
        .expect("typed transaction with SQL statements");
    assert_eq!(transaction_result, (1, 1));
    assert_eq!(transaction_commit, database.commit_id());

    let all = database
        .execute_sql("SELECT * FROM accounts")
        .expect("select all rows");
    database.close().expect("close");

    let mut reopened = RelationalDatabase::open(database_config).expect("reopen");
    assert_eq!(
        reopened
            .catalog()
            .indexes()
            .find(|index| { reopened.catalog().index_name(index.id) == Some("accounts_state_idx") })
            .map(|index| index.id),
        state_index
    );
    let reopened_rows = reopened
        .execute_sql("SELECT * FROM accounts")
        .expect("select after reopen")
        .rows;
    assert_eq!(reopened_rows, all.rows);
    reopened.close().expect("close reopened");
    reopened_rows
}

#[test]
fn embedded_sql_matches_across_backends_and_reopens() {
    let temporary = tempdir().expect("temporary directory");
    exercise_sql(&temporary.path().join("temporary"));
}

fn exercise_sql_schema_constraints(directory: &Path) {
    let database_config = config(directory);
    let mut database = RelationalDatabase::create(database_config.clone()).expect("create");
    database
        .execute_sql(
            "CREATE TABLE parents (id BIGINT, name TEXT NOT NULL, CONSTRAINT parents_pkey PRIMARY KEY (id))",
        )
        .expect("create parent table");
    assert!(
        database
            .catalog()
            .indexes()
            .any(|index| { database.catalog().index_name(index.id) == Some("parents_pkey") })
    );
    database
        .execute_sql("INSERT INTO parents VALUES (1, 'one')")
        .expect("insert parent");

    let before_invalid = database.commit_id();
    assert!(matches!(
        database.execute_sql(
            "CREATE TABLE invalid_children (id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL, FOREIGN KEY (parent_id) REFERENCES missing (id))"
        ),
        Err(DbError::InvalidState(reason)) if reason.contains("table missing")
    ));
    assert_eq!(database.commit_id(), before_invalid);

    let schema = database
        .execute_sql(
            "CREATE TABLE children (id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL, label TEXT NOT NULL, CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parents (id), CONSTRAINT child_label_unique UNIQUE (label))",
        )
        .expect("create constrained child table");
    assert!(schema.commit.is_some());
    assert_eq!(
        database.catalog().foreign_key_name(ConstraintId(1)),
        Some("child_parent_fk")
    );
    assert!(
        database
            .catalog()
            .indexes()
            .any(|index| { database.catalog().index_name(index.id) == Some("child_label_unique") })
    );
    database
        .execute_sql("INSERT INTO children VALUES (1, 1, 'first')")
        .expect("insert valid child");
    assert!(matches!(
        database.execute_sql("INSERT INTO children VALUES (2, 99, 'orphan')"),
        Err(DbError::ForeignKeyViolation { constraint: 1, .. })
    ));
    assert!(matches!(
        database.execute_sql("INSERT INTO children VALUES (2, 1, 'first')"),
        Err(DbError::UniqueViolation { .. })
    ));
    database.close().expect("close");

    let reopened = RelationalDatabase::open(database_config).expect("reopen");
    assert_eq!(
        reopened.catalog().foreign_key_name(ConstraintId(1)),
        Some("child_parent_fk")
    );
    assert!(
        reopened
            .catalog()
            .indexes()
            .any(|index| { reopened.catalog().index_name(index.id) == Some("child_label_unique") })
    );
    reopened.close().expect("close reopened");
}

#[test]
fn embedded_sql_schema_constraints_are_atomic_and_backend_neutral() {
    let temporary = tempdir().expect("temporary directory");
    exercise_sql_schema_constraints(&temporary.path().join("temporary"));
}

fn exercise_sql_oracle(directory: &Path) {
    let mut database = RelationalDatabase::create(config(directory)).expect("create");

    assert!(matches!(
        database.execute_sql("SELECT FROM"),
        Err(DbError::SqlParse(_))
    ));
    assert!(matches!(
        database.execute_sql("SELECT 1; SELECT 2"),
        Err(DbError::SqlUnsupported {
            statement: "multiple statements",
            ..
        })
    ));
    assert!(matches!(
        database.execute_sql("SELECT 1 ORDER BY 1"),
        Err(DbError::SqlUnsupported {
            statement: "SELECT",
            ..
        })
    ));

    database
        .execute_sql(
            "CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, state TEXT NOT NULL)",
        )
        .expect("create accounts");
    database
        .execute_sql("CREATE TABLE events (id BIGINT PRIMARY KEY, state TEXT)")
        .expect("create events");
    database
        .execute_sql("INSERT INTO accounts VALUES (1, 100, 'open'), (2, 100, 'open')")
        .expect("seed accounts");
    database
        .execute_sql("INSERT INTO events VALUES (1, NULL), (2, 'open'), (3, 'closed')")
        .expect("seed events");

    assert_eq!(
        database
            .execute_sql_with_params("SELECT $1 AS answer", &[Value::I64(7)])
            .expect("parameterized constant")
            .rows,
        vec![vec![Value::I64(7)]]
    );
    database
        .execute_sql_with_params(
            "INSERT INTO events VALUES (?1, ?2)",
            &[Value::I64(4), Value::Text("parameterized".to_owned())],
        )
        .expect("parameterized insert");
    assert_eq!(
        database
            .execute_sql_with_params("SELECT state FROM events WHERE id = $1", &[Value::I64(4)],)
            .expect("parameterized predicate")
            .rows,
        vec![vec![Value::Text("parameterized".to_owned())]]
    );
    let mut parameter_transaction = database.begin().expect("begin parameter transaction");
    parameter_transaction
        .execute_sql_with_params(
            &database,
            "UPDATE events SET state = $1 WHERE id = $2",
            &[Value::Text("updated".to_owned()), Value::I64(4)],
        )
        .expect("parameterized transaction update");
    parameter_transaction
        .commit()
        .expect("commit parameter transaction");
    assert!(matches!(
        database.execute_sql_with_params("SELECT $1", &[]),
        Err(DbError::SqlParameter(reason)) if reason.contains("none were supplied")
    ));
    assert!(matches!(
        database.execute_sql_with_params("SELECT 1", &[Value::I64(1)]),
        Err(DbError::SqlParameter(reason)) if reason.contains("does not reference")
    ));
    assert!(matches!(
        database.execute_sql_with_params("SELECT ?", &[Value::I64(1)]),
        Err(DbError::SqlParameter(reason)) if reason.contains("not positional")
    ));
    let before_invalid_parameter = database.commit_id();
    assert!(matches!(
        database.execute_sql_with_params(
            "INSERT INTO events VALUES ($1, $2)",
            &[Value::I64(5)],
        ),
        Err(DbError::SqlParameter(reason)) if reason.contains("only 1 were supplied")
    ));
    assert!(matches!(
        database.execute_sql_with_params(
            "INSERT INTO events VALUES ($1, $2)",
            &[Value::Text("wrong type".to_owned()), Value::Text("bad".to_owned())],
        ),
        Err(DbError::InvalidState(reason)) if reason.contains("does not satisfy SQL column id")
    ));
    assert_eq!(database.commit_id(), before_invalid_parameter);

    assert_eq!(
        database
            .execute_sql("SELECT id FROM events WHERE state IS NULL")
            .expect("IS NULL")
            .rows,
        vec![vec![Value::I64(1)]]
    );
    assert_eq!(
        database
            .execute_sql("SELECT id FROM events WHERE state IS NOT NULL")
            .expect("IS NOT NULL")
            .rows,
        vec![
            vec![Value::I64(2)],
            vec![Value::I64(3)],
            vec![Value::I64(4)]
        ]
    );
    assert_eq!(
        database
            .execute_sql("SELECT id FROM events WHERE state = NULL")
            .expect("three-valued NULL comparison")
            .rows,
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        database
            .execute_sql(
                "SELECT id, state FROM events ORDER BY state ASC NULLS LAST, id DESC LIMIT 2 OFFSET 1",
            )
            .expect("ordered and paginated rows")
            .rows,
        vec![
            vec![Value::I64(2), Value::Text("open".to_owned())],
            vec![Value::I64(4), Value::Text("updated".to_owned())],
        ]
    );
    assert_eq!(
        database
            .execute_sql("SELECT id FROM events ORDER BY id DESC LIMIT 2 OFFSET 1")
            .expect("descending order and offset")
            .rows,
        vec![vec![Value::I64(3)], vec![Value::I64(2)]]
    );
    assert_eq!(
        database
            .execute_sql("SELECT id FROM events ORDER BY state ASC NULLS FIRST LIMIT 1")
            .expect("explicit null ordering")
            .rows,
        vec![vec![Value::I64(1)]]
    );
    assert_eq!(
        database
            .execute_sql("SELECT id FROM events ORDER BY state DESC NULLS LAST LIMIT 1")
            .expect("descending explicit null ordering")
            .rows,
        vec![vec![Value::I64(4)]]
    );
    assert!(matches!(
        database.execute_sql("SELECT id FROM events ORDER BY id + 1"),
        Err(DbError::SqlUnsupported {
            statement: "ORDER BY",
            ..
        })
    ));
    assert!(matches!(
        database.execute_sql("SELECT id FROM events ORDER BY id LIMIT 1 OFFSET -1"),
        Err(DbError::InvalidState(reason)) if reason.contains("OFFSET")
    ));

    assert!(matches!(
        database.execute_sql("INSERT INTO accounts VALUES (1, 90, 'closed')"),
        Err(DbError::InvalidState(reason)) if reason.contains("already exists")
    ));
    assert!(matches!(
        database.execute_sql("INSERT INTO accounts VALUES (3, NULL, 'open')"),
        Err(DbError::InvalidState(reason)) if reason.contains("does not satisfy SQL column balance")
    ));
    assert_eq!(
        database
            .execute_sql("SELECT id, balance, state FROM accounts")
            .expect("constraint failures leave state unchanged")
            .rows
            .len(),
        2
    );

    assert!(matches!(
        database.execute_sql("BEGIN"),
        Err(DbError::SqlUnsupported {
            statement: "transaction control",
            ..
        })
    ));

    database.close().expect("close");
}

#[test]
fn embedded_sql_result_constraint_and_error_oracle_matches_across_backends() {
    let temporary = tempdir().expect("temporary directory");
    exercise_sql_oracle(&temporary.path().join("temporary"));
}

#[test]
fn embedded_sql_refuses_unsupported_and_atomicity_is_preserved() {
    let directory = tempdir().expect("directory");
    let mut database =
        RelationalDatabase::create(config(&directory.path().join("temporary"))).expect("create");
    database
        .execute_sql("CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)")
        .expect("create table");
    database
        .execute_sql("CREATE TABLE blobs (id BIGINT PRIMARY KEY, payload BLOB)")
        .expect("create bytes table");
    assert_eq!(
        database
            .execute_sql("INSERT INTO blobs VALUES (1, X'00ff')")
            .expect("insert bytes")
            .affected_rows,
        1
    );
    assert_eq!(
        database
            .execute_sql("SELECT payload FROM blobs")
            .expect("select bytes")
            .rows,
        vec![vec![Value::Bytes(vec![0, 255])]]
    );

    assert!(matches!(
        database.execute_sql("BEGIN"),
        Err(DbError::SqlUnsupported {
            statement: "transaction control",
            ..
        })
    ));
    // Table aliases are supported; a still-refused FROM shape is a
    // multi-table FROM clause.
    assert!(matches!(
        database.execute_sql("SELECT * FROM (SELECT 1) AS x"),
        Err(DbError::SqlUnsupported {
            statement: "FROM",
            ..
        })
    ));
    assert!(matches!(
        database.execute_sql("CREATE INDEX accounts_balance ON accounts (balance DESC)"),
        Err(DbError::SqlUnsupported {
            statement: "CREATE INDEX",
            ..
        })
    ));

    let aborted = database.transaction(|database, transaction| {
        transaction.execute_sql(database, "INSERT INTO accounts VALUES (1, 10)")?;
        transaction.execute_sql(database, "INSERT INTO accounts VALUES (2, NULL)")?;
        Ok::<_, DbError>(())
    });
    assert!(matches!(aborted, Err(DbError::InvalidState(_))));
    assert_eq!(
        database
            .execute_sql("SELECT * FROM accounts")
            .expect("empty after aborted transaction")
            .rows,
        Vec::<Vec<Value>>::new()
    );
    database.close().expect("close");
}

fn exercise_sql_composite_primary_key(directory: &Path) -> Vec<Vec<Value>> {
    let config = config(directory);
    let mut database = RelationalDatabase::create(config.clone()).expect("create");
    database
        .execute_sql(
            "CREATE TABLE ledger (tenant_id BIGINT, entry_id BIGINT, state TEXT NOT NULL, PRIMARY KEY (tenant_id, entry_id))",
        )
        .expect("create composite table");
    assert_eq!(
        database.catalog().primary_key(omendb::TableId(1)),
        Some([omendb::ColumnId(1), omendb::ColumnId(2)].as_slice())
    );
    database
        .execute_sql("INSERT INTO ledger VALUES (1, 1, 'open'), (1, 2, 'open'), (2, 1, 'open')")
        .expect("insert composite rows");
    assert!(matches!(
        database.execute_sql("INSERT INTO ledger VALUES (NULL, 4, 'invalid')"),
        Err(DbError::InvalidState(reason))
            if reason == "value does not satisfy SQL column tenant_id"
    ));
    database
        .execute_sql(
            "CREATE TABLE ledger_refs (tenant_id BIGINT, entry_id BIGINT, label TEXT NOT NULL, PRIMARY KEY (tenant_id, entry_id), FOREIGN KEY (tenant_id, entry_id) REFERENCES ledger (tenant_id, entry_id))",
        )
        .expect("create composite foreign key");
    database
        .execute_sql("INSERT INTO ledger_refs VALUES (1, 1, 'first')")
        .expect("insert composite foreign-key row");
    assert!(matches!(
        database.execute_sql("INSERT INTO ledger_refs VALUES (9, 9, 'orphan')"),
        Err(DbError::ForeignKeyViolation { .. })
    ));
    let duplicate = database.execute_sql("INSERT INTO ledger VALUES (1, 1, 'duplicate')");
    assert!(matches!(
        duplicate,
        Err(DbError::InvalidState(reason)) if reason.contains("already exists")
    ));
    let updated = database
        .execute_sql("UPDATE ledger SET state = 'closed' WHERE tenant_id = 1")
        .unwrap_or_else(|error| panic!("update composite rows: {error:?}"));
    assert_eq!(updated.affected_rows, 2);
    database
        .execute_sql("CREATE INDEX ledger_state_idx ON ledger (state)")
        .expect("create composite-table index");
    let state_index = database
        .catalog()
        .indexes()
        .find(|index| database.catalog().index_name(index.id) == Some("ledger_state_idx"))
        .map(|index| index.id)
        .expect("composite-table index id");
    let indexed = database
        .index_get(
            omendb::TableId(1),
            state_index,
            &[Value::Text("closed".to_owned())],
        )
        .expect("direct composite index read");
    assert_eq!(indexed.len(), 2);
    let (transaction_indexed, _) = database
        .transaction(|database, transaction| {
            transaction.index_get(
                database,
                omendb::TableId(1),
                state_index,
                &[Value::Text("closed".to_owned())],
            )
        })
        .expect("transactional composite index read");
    assert_eq!(transaction_indexed.len(), 2);
    assert!(matches!(
        database.execute_sql("UPDATE ledger SET tenant_id = 9 WHERE tenant_id = 1"),
        Err(DbError::InvalidState(reason)) if reason.contains("primary key")
    ));
    let exact = database
        .execute_sql("SELECT tenant_id, state FROM ledger WHERE tenant_id = 1 AND entry_id = 2")
        .unwrap_or_else(|error| panic!("exact composite lookup: {error:?}"))
        .rows;
    assert_eq!(
        exact,
        vec![vec![Value::I64(1), Value::Text("closed".to_owned())]]
    );
    let reversed = database
        .execute_sql("SELECT tenant_id, state FROM ledger WHERE entry_id = 2 AND tenant_id = 1")
        .expect("reversed composite lookup")
        .rows;
    assert_eq!(reversed, exact);
    let missed = database
        .execute_sql("SELECT tenant_id FROM ledger WHERE tenant_id = 9 AND entry_id = 9")
        .expect("composite miss")
        .rows;
    assert!(missed.is_empty());
    let partial_scan = database
        .execute_sql("SELECT entry_id FROM ledger WHERE tenant_id = 1 ORDER BY entry_id")
        .expect("partial composite scan")
        .rows;
    assert_eq!(partial_scan, vec![vec![Value::I64(1)], vec![Value::I64(2)]]);
    let updated_exact = database
        .execute_sql("UPDATE ledger SET state = 'flagged' WHERE tenant_id = 2 AND entry_id = 1")
        .expect("exact composite update")
        .affected_rows;
    assert_eq!(updated_exact, 1);
    let rows = database
        .execute_sql("SELECT tenant_id, entry_id, state FROM ledger")
        .expect("select composite rows")
        .rows;
    assert_eq!(rows.len(), 3);
    database.close().expect("close");
    let mut reopened = RelationalDatabase::open(config).expect("reopen");
    let reopened_rows = reopened
        .execute_sql("SELECT tenant_id, entry_id, state FROM ledger")
        .expect("select after reopen")
        .rows;
    assert_eq!(reopened_rows, rows);
    reopened
        .execute_sql("DELETE FROM ledger WHERE tenant_id = 2 AND entry_id = 1")
        .expect("delete composite row");
    reopened.close().expect("close reopened");
    reopened_rows
}

#[test]
fn embedded_sql_composite_primary_keys_match_across_backends_and_reopen() {
    let temporary = tempdir().expect("temporary directory");
    exercise_sql_composite_primary_key(&temporary.path().join("temporary"));
}
