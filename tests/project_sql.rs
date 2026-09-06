use std::path::Path;

use omendb::{
    ConstraintId, DbError, RelationalBackendConfig, RelationalCapability,
    RelationalCapabilityState, RelationalDatabase, Value,
};
use tempfile::tempdir;

fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

#[test]
fn sql_parameter_types_infer_between_bounds_from_column() {
    let directory = tempdir().expect("temporary directory");
    let database_path = directory.path().join("database");
    let mut database = RelationalDatabase::create(config(&database_path)).expect("create");
    database
        .execute_sql(
            "CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, state TEXT)",
        )
        .expect("create accounts");

    assert_eq!(
        database
            .sql_parameter_types(
                "SELECT id FROM accounts WHERE balance BETWEEN $1 AND $2 ORDER BY id",
            )
            .expect("describe BETWEEN parameters"),
        vec![Some(omendb::ColumnType::I64), Some(omendb::ColumnType::I64),]
    );
    database.close().expect("close");
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
        Err(DbError::SqlUndefinedColumn { name }) if name == "missing"
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
fn update_assignment_arithmetic_matches_read_modify_write() {
    let directory = tempfile::tempdir().expect("temp directory");
    let database_path = directory.path().join("assignment-db");
    let mut database = RelationalDatabase::create(config(&database_path)).expect("create database");
    database
        .execute_sql(
            "CREATE TABLE ledgers (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, price NUMERIC)",
        )
        .expect("create table");
    database
        .execute_sql("INSERT INTO ledgers VALUES (1, 100, '10.50'), (2, 40, NULL)")
        .expect("seed");

    // The OLTP read-modify-write shape: column +/- literal and parameter.
    database
        .execute_sql("UPDATE ledgers SET balance = balance + 15 WHERE id = 1")
        .expect("column plus literal");
    database
        .execute_sql("UPDATE ledgers SET balance = balance - 10 WHERE id = 2")
        .expect("column minus literal");
    let balances = database
        .execute_sql("SELECT balance FROM ledgers ORDER BY id")
        .expect("select balances");
    assert_eq!(balances.rows[0][0], Value::I64(115));
    assert_eq!(balances.rows[1][0], Value::I64(30));

    // Parameterized delta through the typed-parameter path.
    let adjusted = database
        .execute_sql_with_params(
            "UPDATE ledgers SET balance = balance + $1 WHERE id = $2",
            &[Value::I64(5), Value::I64(1)],
        )
        .expect("parameterized delta");
    assert_eq!(adjusted.affected_rows, 1);
    let balances = database
        .execute_sql("SELECT balance FROM ledgers ORDER BY id")
        .expect("select balances");
    assert_eq!(balances.rows[0][0], Value::I64(120));

    // Decimal arithmetic at the operand scales; NULL propagates.
    database
        .execute_sql("UPDATE ledgers SET price = price + 0.25 WHERE id = 1")
        .expect("decimal add");
    let prices = database
        .execute_sql("SELECT price FROM ledgers ORDER BY id")
        .expect("select prices");
    assert_eq!(
        prices.rows[0][0],
        Value::Decimal(omendb::DecimalValue::new(10_75, 2).expect("decimal"))
    );
    assert_eq!(prices.rows[1][0], Value::Null);

    // Overflows refuse instead of wrapping.
    database
        .execute_sql("CREATE TABLE wide (id BIGINT PRIMARY KEY, n BIGINT NOT NULL)")
        .expect("wide table");
    database
        .execute_sql("INSERT INTO wide VALUES (1, 9223372036854775807)")
        .expect("seed max");
    assert!(
        database
            .execute_sql("UPDATE wide SET n = n + 1 WHERE id = 1")
            .is_err()
    );

    // Closing the loop: the value read back is exactly the written value.
    let value = database
        .execute_sql("SELECT balance FROM ledgers WHERE id = 1")
        .expect("final read");
    assert_eq!(value.rows[0][0], Value::I64(120));
}

#[test]
fn window_functions_match_postgresql_semantics() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("window-db");
    let mut database = RelationalDatabase::create(config(&database_path)).expect("create database");

    // Sales rows chosen to exercise ties, partitions, and deterministic
    // evaluation independent of scan order.
    database
        .execute_sql("CREATE TABLE sales (rep TEXT PRIMARY KEY, region TEXT NOT NULL, amount BIGINT NOT NULL)")
        .expect("create sales");
    for (region, rep, amount) in [
        ("east", "ana", 100),
        ("east", "bob", 100),
        ("east", "cid", 300),
        ("west", "dee", 200),
        ("west", "eva", 200),
        ("west", "fin", 400),
    ] {
        database
            .execute_sql(&format!(
                "INSERT INTO sales VALUES ('{rep}', '{region}', {amount})"
            ))
            .expect("seed sales");
    }

    // Ranking with ties: rank leaves gaps after ties, dense_rank does not.
    let result = database
        .execute_sql(
            "SELECT rep, amount, rank() OVER (ORDER BY amount), dense_rank() OVER (ORDER BY amount) FROM sales ORDER BY rep",
        )
        .expect("rank and dense_rank");
    assert_eq!(result.rows.len(), 6);
    let by_rep: std::collections::BTreeMap<String, Vec<Value>> = result
        .rows
        .iter()
        .map(|row| {
            let Value::Text(rep) = &row[0] else {
                panic!("rep must be text");
            };
            (rep.clone(), row.clone())
        })
        .collect();
    let rank_dense = |rep: &str| {
        let row = &by_rep[rep];
        let (Value::I64(rank), Value::I64(dense)) = (&row[2], &row[3]) else {
            panic!("{rep} ranks must be i64");
        };
        (*rank, *dense)
    };
    // Ordered by amount: 100,100,200,200,300,400. ana/bob tie at rank 1;
    // dee/eva tie at rank 3 (gap past the first tie); cid 300 is rank 5;
    // fin 400 is rank 6. Dense ranks: 1,1,2,2,3,4.
    assert_eq!(rank_dense("ana"), (1, 1));
    assert_eq!(rank_dense("bob"), (1, 1));
    assert_eq!(rank_dense("dee"), (3, 2));
    assert_eq!(rank_dense("eva"), (3, 2));
    assert_eq!(rank_dense("cid"), (5, 3));
    assert_eq!(rank_dense("fin"), (6, 4));

    // Partitioned running sum (default frame: running prefix).
    let result = database
        .execute_sql(
            "SELECT rep, sum(amount) OVER (PARTITION BY region ORDER BY amount) FROM sales ORDER BY rep",
        )
        .expect("running sum");
    let cumulative = |rep: &str, expected: i64| {
        let row = result
            .rows
            .iter()
            .find(|row| matches!(&row[0], Value::Text(name) if name == rep))
            .unwrap_or_else(|| panic!("missing {rep}"));
        assert_eq!(row[1], Value::I64(expected), "cumulative sum for {rep}");
    };
    // East by amount: 100, 200 (100+100), 500 (+300). West: 200, 400, 800.
    cumulative("ana", 100);
    cumulative("bob", 200);
    cumulative("cid", 500);
    cumulative("dee", 200);
    cumulative("eva", 400);
    cumulative("fin", 800);

    // lag/lead default offset 1; NULL at partition edges.
    let result = database
        .execute_sql(
            "SELECT rep, lag(amount) OVER (PARTITION BY region ORDER BY amount), lead(amount) OVER (PARTITION BY region ORDER BY amount) FROM sales ORDER BY rep",
        )
        .expect("lag and lead");
    let offset_value = |rep: &str, column: usize| {
        result
            .rows
            .iter()
            .find(|row| matches!(&row[0], Value::Text(name) if name == rep))
            .unwrap_or_else(|| panic!("missing {rep}"))
            .get(column)
            .cloned()
            .expect("offset column")
    };
    assert_eq!(offset_value("ana", 1), Value::Null);
    assert_eq!(offset_value("bob", 1), Value::I64(100));
    assert_eq!(offset_value("cid", 1), Value::I64(100));
    assert_eq!(offset_value("cid", 2), Value::Null);
    assert_eq!(offset_value("dee", 2), Value::I64(200));

    // row_number with a tiebreaker; first_value/last_value per partition.
    let result = database
        .execute_sql(
            "SELECT rep, row_number() OVER (ORDER BY amount, rep), first_value(amount) OVER (PARTITION BY region ORDER BY amount), last_value(amount) OVER (PARTITION BY region ORDER BY amount) FROM sales ORDER BY rep",
        )
        .expect("row_number and value functions");
    let row_for = |rep: &str| {
        result
            .rows
            .iter()
            .find(|row| matches!(&row[0], Value::Text(name) if name == rep))
            .unwrap_or_else(|| panic!("missing {rep}"))
            .clone()
    };
    assert_eq!(row_for("ana")[1], Value::I64(1));
    assert_eq!(row_for("fin")[1], Value::I64(6));
    assert_eq!(row_for("ana")[2], Value::I64(100));
    assert_eq!(row_for("cid")[3], Value::I64(300));
    assert_eq!(row_for("fin")[2], Value::I64(200));
    assert_eq!(row_for("fin")[3], Value::I64(400));

    // count(*) over partitions with no ORDER BY: whole-partition count.
    let result = database
        .execute_sql("SELECT count(*) OVER (PARTITION BY region) FROM sales ORDER BY rep")
        .expect("partition count");
    for row in &result.rows {
        assert_eq!(row[0], Value::U64(3));
    }

    // Unsupported shapes fail honestly and leave the session usable.
    for (sql, message) in [
        (
            "SELECT nth_value(amount, 2) OVER (ORDER BY amount) FROM sales",
            "nth_value",
        ),
        (
            "SELECT sum(amount) OVER w FROM sales WINDOW w AS (PARTITION BY region)",
            "named window",
        ),
        (
            "SELECT sum(amount) OVER (ORDER BY amount ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM sales",
            "window frames",
        ),
    ] {
        let error = database.execute_sql(sql).expect_err("refused");
        assert!(
            error.to_string().contains(message),
            "{sql} should refuse about {message}, got: {error}"
        );
    }
    let result = database
        .execute_sql("SELECT count(*) FROM sales")
        .expect("session still usable");
    assert_eq!(result.rows[0][0], Value::U64(6));
}

#[test]
fn serializable_certification_aborts_the_doctors_write_skew() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("skew-db");
    let mut database = RelationalDatabase::create(config(&database_path)).expect("create database");

    // Doctors on call: invariant is "at least one doctor stays on call".
    // Both doctors read the pair, then each removes themselves — classic
    // write skew, legal under snapshot isolation, forbidden serializably.
    database
        .execute_sql("CREATE TABLE doctors (name TEXT PRIMARY KEY, on_call BOOLEAN NOT NULL)")
        .expect("create doctors");
    database
        .execute_sql("INSERT INTO doctors VALUES ('alice', true), ('bob', true)")
        .expect("seed doctors");

    // Both explicit transactions start on one snapshot before any commit.
    let mut alice = database.begin().expect("alice begin");
    let mut bob = database.begin().expect("bob begin");

    let alice_view = alice
        .execute_sql(&database, "SELECT count(*) FROM doctors WHERE on_call")
        .expect("alice reads on-call count");
    let bob_view = bob
        .execute_sql(&database, "SELECT count(*) FROM doctors WHERE on_call")
        .expect("bob reads on-call count");
    assert_eq!(alice_view.rows[0][0], Value::U64(2));
    assert_eq!(bob_view.rows[0][0], Value::U64(2));

    // Each doctor sees two doctors on call and takes themselves off.
    alice
        .execute_sql(
            &database,
            "UPDATE doctors SET on_call = false WHERE name = 'alice'",
        )
        .expect("alice stages update");
    bob.execute_sql(
        &database,
        "UPDATE doctors SET on_call = false WHERE name = 'bob'",
    )
    .expect("bob stages update");

    alice.commit().expect("alice commits first");
    // Bob's commit read rows (the full-table scan behind the count) that
    // alice's commit changed, and wrote rows disjoint from alice's —
    // the rw-antidependency cycle serializability forbids.
    let outcome = bob.commit();
    match outcome {
        Err(omendb::DbError::SerializationConflict { .. }) => {}
        Err(other) => panic!("expected serialization conflict, got {other:?}"),
        Ok(_) => panic!("write skew committed: both doctors off call"),
    }

    // The survivor is alice's world: one doctor on call.
    let result = database
        .execute_sql("SELECT count(*) FROM doctors WHERE on_call")
        .expect("post-commit count");
    assert_eq!(result.rows[0][0], Value::U64(1));
}

#[test]
fn serializable_certification_rejects_stale_point_read_writes() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("point-db");
    let mut database = RelationalDatabase::create(config(&database_path)).expect("create database");

    database
        .execute_sql("CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)")
        .expect("create accounts");
    database
        .execute_sql("INSERT INTO accounts VALUES (1, 100)")
        .expect("seed account");

    // A point read (primary-key lookup) that observes a value a committed
    // writer has since overwritten must fail its own commit when it also
    // writes — even when the writes touch disjoint keys.
    let mut reader = database.begin().expect("reader begin");
    let mut writer = database.begin().expect("writer begin");

    let read = reader
        .execute_sql(&database, "SELECT balance FROM accounts WHERE id = 1")
        .expect("point read");
    assert_eq!(read.rows[0][0], Value::I64(100));

    writer
        .execute_sql(&database, "UPDATE accounts SET balance = 50 WHERE id = 1")
        .expect("writer overwrites the read row");
    writer.commit().expect("writer commits");

    // Reader still sees its snapshot value...
    let still = reader
        .execute_sql(&database, "SELECT balance FROM accounts WHERE id = 1")
        .expect("snapshot re-read");
    assert_eq!(still.rows[0][0], Value::I64(100));
    // ...but its commit, which writes a disjoint key, must fail: the
    // balance decision was made on overwritten data.
    reader
        .execute_sql(&database, "INSERT INTO accounts VALUES (2, 25)")
        .expect("reader stages disjoint write");
    let outcome = reader.commit();
    match outcome {
        Err(omendb::DbError::SerializationConflict { .. }) => {}
        Err(other) => panic!("expected serialization conflict, got {other:?}"),
        Ok(_) => panic!("stale point read committed"),
    }
}

#[test]
fn serializable_certification_lets_read_only_transactions_commit() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("readonly-db");
    let mut database = RelationalDatabase::create(config(&database_path)).expect("create database");

    database
        .execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("create table");
    database
        .execute_sql("INSERT INTO t VALUES (1, 1)")
        .expect("seed row");

    // A reader that scanned everything, followed by a writer that changes
    // everything under it: the read-only commit still succeeds.
    let mut reader = database.begin().expect("reader begin");
    let scanned = reader
        .execute_sql(&database, "SELECT count(*) FROM t")
        .expect("scan");
    assert_eq!(scanned.rows[0][0], Value::U64(1));

    let mut writer = database.begin().expect("writer begin");
    writer
        .execute_sql(&database, "UPDATE t SET v = 2 WHERE id = 1")
        .expect("writer stages");
    writer.commit().expect("writer commits");

    reader.commit().expect("read-only commit always succeeds");
}

#[test]
fn heap_tables_store_update_delete_and_reopen_without_collisions() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("heap-db");
    let mut database = RelationalDatabase::create(config(&database_path)).expect("create database");

    // A table with no PRIMARY KEY: rows get engine-allocated identities.
    database
        .execute_sql(
            "CREATE TABLE history (tid BIGINT, delta BIGINT, mtime TIMESTAMP DEFAULT NULL)",
        )
        .expect("create heap table");
    // Secondary indexes and UNIQUE constraints work over heap rows.
    database
        .execute_sql("CREATE INDEX history_tid_idx ON history (tid)")
        .expect("index heap table");

    database
        .execute_sql(
            "INSERT INTO history VALUES (1, 10, '2026-09-05 01:00:00'), (1, 20, '2026-09-05 01:01:00'), (2, 30, NULL)",
        )
        .expect("multi-row insert");
    let result = database
        .execute_sql("SELECT tid, delta, mtime FROM history ORDER BY tid, delta")
        .expect("scan heap");
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        result.rows[1][2],
        Value::Timestamp(
            omendb::TimestampValue::from_micros(1_788_570_060_000_000).expect("mtime")
        )
    );

    // UPDATE addresses rows by their stored identity; no column is
    // protected on a heap table.
    database
        .execute_sql("UPDATE history SET delta = delta + 5 WHERE tid = 1")
        .expect("update heap rows");
    let result = database
        .execute_sql("SELECT sum(delta) FROM history")
        .expect("post-update sum");
    assert_eq!(result.rows[0][0], Value::I64(70));

    // DELETE by predicate removes exactly the matching rows.
    database
        .execute_sql("DELETE FROM history WHERE delta > 20")
        .expect("delete heap rows");
    let result = database
        .execute_sql("SELECT count(*) FROM history")
        .expect("post-delete count");
    assert_eq!(result.rows[0][0], Value::U64(1));

    // Reopen: committed heap rows survive, and new identities cannot
    // collide with pre-crash ones (they derive from the durable
    // transaction allocator, which never reuses after reopen).
    database.close().expect("close");
    let mut database = RelationalDatabase::open(config(&database_path)).expect("reopen database");
    let result = database
        .execute_sql("SELECT count(*) FROM history")
        .expect("reopened count");
    assert_eq!(result.rows[0][0], Value::U64(1));
    database
        .execute_sql("INSERT INTO history VALUES (3, 40, NULL), (4, 50, NULL)")
        .expect("insert after reopen");
    let result = database
        .execute_sql("SELECT count(*), sum(delta) FROM history")
        .expect("post-reopen totals");
    assert_eq!(result.rows[0][0], Value::U64(3));
    assert_eq!(result.rows[0][1], Value::I64(105));

    // The index still resolves lookups over both generations of rows.
    let result = database
        .execute_sql("SELECT delta FROM history WHERE tid = 3")
        .expect("index lookup after reopen");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::I64(40));
}

#[test]
fn scalar_function_catalog_matches_postgresql_semantics() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("scalar-db");
    let mut database = RelationalDatabase::create(config(&database_path)).expect("create database");
    database
        .execute_sql(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, price DOUBLE PRECISION, created TIMESTAMP)",
        )
        .expect("create table");
    database
        .execute_sql(
            "INSERT INTO t VALUES (1, '  Alice ', 2.7, '2026-09-05 14:30:45.123'), (2, 'BOB', -2.7, '2026-01-15 08:00:00')",
        )
        .expect("seed");

    // Text functions over rows, with NULL coming through the column.
    let result = database
        .execute_sql(
            "SELECT upper(name), lower(name), length(name), btrim(name) FROM t WHERE id = 1",
        )
        .expect("text functions");
    assert_eq!(
        result.rows[0],
        vec![
            Value::Text("  ALICE ".to_owned()),
            Value::Text("  alice ".to_owned()),
            Value::I64(8),
            Value::Text("Alice".to_owned()),
        ]
    );

    // Numeric functions: abs preserves sign semantics, round/floor/ceil
    // match PostgreSQL's half-away-from-zero round and IEEE floor/ceil.
    let result = database
        .execute_sql(
            "SELECT abs(price), round(price), floor(price), ceil(price) FROM t WHERE id = 2",
        )
        .expect("numeric functions");
    assert_eq!(
        result.rows[0],
        vec![
            Value::Float64(omendb::F64::new(2.7)),
            Value::Float64(omendb::F64::new(-3.0)),
            Value::Float64(omendb::F64::new(-3.0)),
            Value::Float64(omendb::F64::new(-2.0)),
        ]
    );

    // EXTRACT: calendar fields and epoch as scale-6 decimal seconds.
    let result = database
        .execute_sql(
            "SELECT extract(year from created), extract(month from created), extract(day from created), extract(hour from created), extract(epoch from created) FROM t WHERE id = 1",
        )
        .expect("extract");
    assert_eq!(
        result.rows[0],
        vec![
            Value::I64(2026),
            Value::I64(9),
            Value::I64(5),
            Value::I64(14),
            Value::Decimal(omendb::DecimalValue::new(1_788_618_645_123_000, 6).expect("epoch")),
        ]
    );

    // date_trunc boundaries match PostgreSQL's rendering. Days since the
    // epoch are computed inline (fixed civil-calendar math):
    //   2026-01-01 = 20454, 2026-09-01 = 20697, 2026-09-05 = 20701.
    let result = database
        .execute_sql(
            "SELECT date_trunc('year', created), date_trunc('month', created), date_trunc('minute', created) FROM t WHERE id = 1",
        )
        .expect("date_trunc");
    assert_eq!(
        result.rows[0],
        vec![
            Value::Timestamp(
                omendb::TimestampValue::from_micros(20_454 * 86_400_000_000).expect("year start")
            ),
            Value::Timestamp(
                omendb::TimestampValue::from_micros(20_697 * 86_400_000_000).expect("month start")
            ),
            Value::Timestamp(
                omendb::TimestampValue::from_micros(
                    20_701 * 86_400_000_000 + 14 * 3_600_000_000 + 30 * 60_000_000
                )
                .expect("minute start")
            ),
        ]
    );

    // Scalar functions over literals (no FROM).
    let result = database
        .execute_sql("SELECT upper('hi'), length('hello'), abs(-5)")
        .expect("literal scalar functions");
    assert_eq!(
        result.rows[0],
        vec![Value::Text("HI".to_owned()), Value::I64(5), Value::I64(5)]
    );
}

#[test]
fn clock_functions_evaluate_and_round_trip() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("clock-db");
    let mut database = RelationalDatabase::create(config(&database_path)).expect("create database");
    database
        .execute_sql(
            "CREATE TABLE events (id BIGINT PRIMARY KEY, created_at TIMESTAMP, created_on DATE)",
        )
        .expect("create table");

    // CURRENT_TIMESTAMP inserts a real timestamp and reads back.
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros() as i64;
    database
        .execute_sql("INSERT INTO events (id, created_at, created_on) VALUES (1, CURRENT_TIMESTAMP, CURRENT_DATE)")
        .expect("insert with clock functions");
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros() as i64;
    let result = database
        .execute_sql("SELECT created_at, created_on FROM events WHERE id = 1")
        .expect("read back");
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Timestamp(timestamp) => {
            assert!(
                (before..=after).contains(&timestamp.0),
                "CURRENT_TIMESTAMP is the real clock: {} not in [{before},{after}]",
                timestamp.0
            );
        }
        other => panic!("created_at is a timestamp: {other:?}"),
    }
    match &result.rows[0][1] {
        Value::Date(date) => {
            let today = before.div_euclid(86_400_000_000);
            assert!((today - 1..=today + 1).contains(&i64::from(date.0)));
        }
        other => panic!("created_on is a date: {other:?}"),
    }

    // now() is the same clock; SELECT renders both in literal positions.
    let result = database.execute_sql("SELECT now()").expect("select now");
    match &result.rows[0][0] {
        Value::Timestamp(timestamp) => {
            assert!(
                (before..=after + 60_000_000).contains(&timestamp.0),
                "now() is the real clock"
            );
        }
        other => panic!("now() is a timestamp: {other:?}"),
    }
}

#[test]
fn explain_names_the_access_path_execution_takes() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("explain-db");
    let mut database = RelationalDatabase::create(config(&database_path)).expect("create database");
    database
        .execute_sql(
            "CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, state TEXT, nickname TEXT)",
        )
        .expect("create table");
    database
        .execute_sql("CREATE INDEX accounts_state_idx ON accounts (state)")
        .expect("create index");
    database
        .execute_sql(
            "INSERT INTO accounts VALUES (1, 100, 'open', 'one'), (2, 40, 'open', 'two'), (3, 10, 'closed', NULL)",
        )
        .expect("seed");

    // Primary-key equality: an identity lookup, and the row comes back.
    let plan = database
        .execute_sql("EXPLAIN SELECT balance FROM accounts WHERE id = 1")
        .expect("explain pk");
    assert_eq!(
        plan.rows[0][0],
        Value::Text("Primary Key Lookup on accounts".to_owned())
    );

    // Secondary-index equality with every indexed column bound.
    let plan = database
        .execute_sql("EXPLAIN SELECT balance FROM accounts WHERE state = 'open'")
        .expect("explain index");
    assert_eq!(
        plan.rows[0][0],
        Value::Text("Index Scan using accounts_state_idx on accounts (state)".to_owned())
    );

    // A predicate the fast path does not model falls to a full scan.
    let plan = database
        .execute_sql("EXPLAIN SELECT balance FROM accounts WHERE balance > 50")
        .expect("explain scan");
    assert_eq!(
        plan.rows[0][0],
        Value::Text("Seq Scan on accounts".to_owned())
    );

    // NULL equality on a nullable column short-circuits to no rows.
    // (A NULL comparison on the NOT NULL primary key is refused at
    // coercion, identically in EXPLAIN and execution.)
    let plan = database
        .execute_sql("EXPLAIN SELECT balance FROM accounts WHERE nickname = NULL")
        .expect("explain null");
    assert_eq!(
        plan.rows[0][0],
        Value::Text("Result (empty) on accounts (NULL equality)".to_owned())
    );

    // UPDATE and DELETE report the same access path as their SELECT probe.
    let plan = database
        .execute_sql("EXPLAIN UPDATE accounts SET balance = 1 WHERE id = 2")
        .expect("explain update");
    assert_eq!(
        plan.rows[0][0],
        Value::Text("Primary Key Lookup on accounts".to_owned())
    );
    let plan = database
        .execute_sql("EXPLAIN DELETE FROM accounts WHERE id = 2")
        .expect("explain delete");
    assert_eq!(
        plan.rows[0][0],
        Value::Text("Primary Key Lookup on accounts".to_owned())
    );

    // EXPLAIN ANALYZE is refused, matching PostgreSQL's execution vs plan
    // distinction without running the statement.
    assert!(
        database
            .execute_sql("EXPLAIN ANALYZE SELECT balance FROM accounts WHERE id = 1")
            .is_err()
    );
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
        Err(DbError::SqlUndefinedTable { name }) if name == "missing"
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
        Err(DbError::SqlDatatypeMismatch { column }) if column == "id"
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
        Err(DbError::SqlNotNullViolation { column }) if column == "balance"
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
    assert!(matches!(
        aborted,
        Err(DbError::SqlNotNullViolation { column }) if column == "balance"
    ));
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
        Err(DbError::SqlNotNullViolation { column }) if column == "tenant_id"
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
