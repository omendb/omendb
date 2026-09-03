//! SQL-tier schema evolution: ALTER TABLE operations, CREATE/DROP INDEX,
//! and DROP TABLE, with reopen-based durability checks. These tests pin
//! the atomic publication contract: a failed multi-operation ALTER TABLE
//! must leave no partial schema change behind.

// Only `config` is used here; the schema constants are consumed by the
// other suites that share this support module.
#[allow(dead_code)]
mod support;

use omendb::{DbError, F64, RelationalDatabase, Value};
use support::config;
use tempfile::tempdir;

fn setup(directory: &std::path::Path) -> RelationalDatabase {
    // create() requires a path that does not exist yet.
    let path = directory.join("evolution-db");
    let mut database = RelationalDatabase::create(config(&path)).expect("create");
    database
        .execute_sql(
            "CREATE TABLE accounts (
                id BIGINT PRIMARY KEY,
                balance BIGINT,
                state TEXT,
                opened DATE
            )",
        )
        .expect("create table");
    for (id, balance, state) in [(1, 100, "open"), (2, 250, "closed"), (3, 75, "open")] {
        database
            .execute_sql(&format!(
                "INSERT INTO accounts (id, balance, state, opened) VALUES ({id}, {balance}, '{state}', '2026-01-15')"
            ))
            .expect("seed row");
    }
    database
}

#[test]
fn alter_table_renames_columns_and_tables_durably() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    database
        .execute_sql("ALTER TABLE accounts RENAME COLUMN balance TO funds")
        .expect("rename column");
    database
        .execute_sql("ALTER TABLE accounts RENAME TO ledger")
        .expect("rename table");

    let rows = database
        .execute_sql("SELECT id, funds FROM ledger WHERE id = 2")
        .expect("query renamed schema");
    assert_eq!(rows.rows, vec![vec![Value::I64(2), Value::I64(250)]]);

    drop(database);
    let mut reopened =
        RelationalDatabase::open(config(&directory.path().join("evolution-db"))).expect("reopen");
    let rows = reopened
        .execute_sql("SELECT funds FROM ledger WHERE id = 2")
        .expect("query after reopen");
    assert_eq!(rows.rows, vec![vec![Value::I64(250)]]);
    // The old names are gone.
    assert!(reopened.execute_sql("SELECT balance FROM ledger").is_err());
}

#[test]
fn alter_table_adds_columns_and_drops_them_durably() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    database
        .execute_sql("ALTER TABLE accounts ADD COLUMN note TEXT")
        .expect("add column");
    database
        .execute_sql("UPDATE accounts SET note = 'watch' WHERE id = 1")
        .expect("write new column");
    database
        .execute_sql("ALTER TABLE accounts DROP COLUMN note")
        .expect("drop column");
    // The dropped column is gone from the projection.
    assert!(
        database
            .execute_sql("SELECT note FROM accounts WHERE id = 1")
            .is_err()
    );
    // Row content is intact with the remaining columns.
    let rows = database
        .execute_sql("SELECT id, balance FROM accounts WHERE id = 1")
        .expect("row after drop");
    assert_eq!(rows.rows, vec![vec![Value::I64(1), Value::I64(100)]]);

    drop(database);
    let mut reopened =
        RelationalDatabase::open(config(&directory.path().join("evolution-db"))).expect("reopen");
    let rows = reopened
        .execute_sql("SELECT id, balance, state, opened FROM accounts WHERE id = 1")
        .expect("full row after reopen");
    assert_eq!(rows.rows[0].len(), 4);
}

#[test]
fn alter_table_changes_types_with_value_conversion() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    // balance: BIGINT -> DOUBLE PRECISION rewrites every stored value.
    database
        .execute_sql("ALTER TABLE accounts ALTER COLUMN balance SET DATA TYPE DOUBLE PRECISION")
        .expect("alter type");
    let rows = database
        .execute_sql("SELECT balance FROM accounts WHERE id = 2")
        .expect("converted value");
    assert_eq!(rows.rows, vec![vec![Value::Float64(F64::new(250.0))]]);

    drop(database);
    let mut reopened =
        RelationalDatabase::open(config(&directory.path().join("evolution-db"))).expect("reopen");
    let rows = reopened
        .execute_sql("SELECT balance FROM accounts WHERE id = 2")
        .expect("converted value after reopen");
    assert_eq!(rows.rows, vec![vec![Value::Float64(F64::new(250.0))]]);
}

#[test]
fn alter_table_type_conversion_failure_is_atomic() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    // state holds 'open', which is not a valid date: the conversion must
    // fail and leave the original schema fully intact.
    let before = database.commit_id();
    assert!(
        database
            .execute_sql("ALTER TABLE accounts ALTER COLUMN state SET DATA TYPE DATE")
            .is_err()
    );
    assert_eq!(database.commit_id(), before);
    let rows = database
        .execute_sql("SELECT state FROM accounts WHERE id = 1")
        .expect("original column intact");
    assert_eq!(rows.rows, vec![vec![Value::Text("open".to_owned())]]);
}

#[test]
fn multi_operation_alter_table_is_all_or_nothing() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    // ADD COLUMN works; DROP COLUMN on the primary key fails. Both are in
    // ONE statement, so neither may take effect.
    let before = database.commit_id();
    assert!(
        database
            .execute_sql("ALTER TABLE accounts ADD COLUMN memo TEXT, DROP COLUMN id")
            .is_err()
    );
    assert_eq!(database.commit_id(), before);
    // No memo column was created.
    assert!(database.execute_sql("SELECT memo FROM accounts").is_err());
    // The primary key is intact.
    let rows = database
        .execute_sql("SELECT id FROM accounts WHERE id = 1")
        .expect("primary key intact");
    assert_eq!(rows.rows, vec![vec![Value::I64(1)]]);
}

#[test]
fn alter_table_refuses_constrained_columns() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    database
        .execute_sql("CREATE INDEX accounts_state_idx ON accounts (state)")
        .expect("create index");
    // state is now indexed: rename/drop/type-change must all refuse.
    for statement in [
        "ALTER TABLE accounts RENAME COLUMN state TO status",
        "ALTER TABLE accounts DROP COLUMN state",
        "ALTER TABLE accounts ALTER COLUMN state SET DATA TYPE TEXT",
    ] {
        assert!(
            database.execute_sql(statement).is_err(),
            "indexed column change must refuse: {statement}"
        );
    }
    // The primary key is constrained the same way.
    assert!(
        database
            .execute_sql("ALTER TABLE accounts DROP COLUMN id")
            .is_err()
    );
    // Dropping the index frees the column again.
    database
        .execute_sql("DROP INDEX accounts_state_idx")
        .expect("drop index");
    database
        .execute_sql("ALTER TABLE accounts RENAME COLUMN state TO status")
        .expect("rename after index drop");
}

#[test]
fn drop_index_and_drop_table_remove_objects_durably() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    database
        .execute_sql("CREATE INDEX accounts_state_idx ON accounts (state)")
        .expect("create index");
    // DROP INDEX IF EXISTS on a missing name is a no-op.
    database
        .execute_sql("DROP INDEX IF EXISTS missing_idx")
        .expect("no-op drop");
    database
        .execute_sql("DROP INDEX accounts_state_idx")
        .expect("drop index");
    assert!(matches!(
        database.execute_sql("DROP INDEX accounts_state_idx"),
        Err(DbError::InvalidState(_))
    ));

    database
        .execute_sql("DROP TABLE accounts")
        .expect("drop table");
    assert!(matches!(
        database.execute_sql("SELECT id FROM accounts"),
        Err(DbError::SqlUndefinedTable { .. })
    ));
    // The durable catalog no longer knows the table.
    drop(database);
    let mut reopened =
        RelationalDatabase::open(config(&directory.path().join("evolution-db"))).expect("reopen");
    assert!(reopened.execute_sql("SELECT id FROM accounts").is_err());
    // And the name is free again.
    reopened
        .execute_sql("CREATE TABLE accounts (id BIGINT PRIMARY KEY, note TEXT)")
        .expect("recreate after drop");
}

#[test]
fn drop_table_refuses_referenced_foreign_keys() {
    let directory = tempdir().expect("tempdir");
    let mut database =
        RelationalDatabase::create(config(&directory.path().join("evolution-db"))).expect("create");
    database
        .execute_sql(
            "CREATE TABLE groups (
                id BIGINT PRIMARY KEY,
                name TEXT
            )",
        )
        .expect("create groups");
    database
        .execute_sql(
            "CREATE TABLE members (
                id BIGINT PRIMARY KEY,
                group_id BIGINT,
                FOREIGN KEY (group_id) REFERENCES groups (id)
            )",
        )
        .expect("create members");
    // The referenced unique index on groups(id) backs members' FK; the
    // drop must refuse while the reference exists.
    assert!(matches!(
        database.execute_sql("DROP TABLE groups"),
        Err(DbError::InvalidState(reason)) if reason.contains("foreign key")
    ));
    // Dropping the referencing table first frees the referenced one.
    database
        .execute_sql("DROP TABLE members")
        .expect("drop referencing table");
    database
        .execute_sql("DROP TABLE groups")
        .expect("drop after reference removed");
}

#[test]
fn alter_table_drop_not_null_relaxes_the_declaration() {
    let directory = tempdir().expect("tempdir");
    let mut database = setup(directory.path());
    database
        .execute_sql("ALTER TABLE accounts ALTER COLUMN state DROP NOT NULL")
        .expect("drop not null");
    // NULL is now accepted.
    database
        .execute_sql("UPDATE accounts SET state = NULL WHERE id = 1")
        .expect("null state");
    let rows = database
        .execute_sql("SELECT id FROM accounts WHERE state IS NULL")
        .expect("null predicate");
    assert_eq!(rows.rows, vec![vec![Value::I64(1)]]);
}
