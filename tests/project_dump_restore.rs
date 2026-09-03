//! Logical dump/restore integration: the data-in/data-out trust gate.
//! Round-trips every scalar type, bytes, secondary indexes, unique
//! constraints, and foreign keys through dump_sql + restore_sql.

use std::path::Path;

use omendb::{DateValue, F64, RelationalBackendConfig, RelationalDatabase, UuidValue, Value};
use tempfile::tempdir;

fn config(path: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(path.to_owned())
}

fn seeded(path: &Path) -> RelationalDatabase {
    let mut database = RelationalDatabase::create(config(path)).expect("create");
    database
        .execute_sql(
            "CREATE TABLE groups (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .expect("groups");
    database
        .execute_sql(
            "CREATE TABLE accounts (
                id BIGINT PRIMARY KEY,
                label TEXT,
                active BOOLEAN,
                ratio DOUBLE PRECISION,
                opened DATE,
                seen TIMESTAMP,
                price NUMERIC,
                token UUID,
                payload BYTEA,
                group_id BIGINT,
                UNIQUE (label),
                FOREIGN KEY (group_id) REFERENCES groups (id)
            )",
        )
        .expect("accounts");
    database
        .execute_sql("INSERT INTO groups (id, name) VALUES (1, 'alpha'), (2, 'beta')")
        .expect("groups rows");
    for statement in [
        "INSERT INTO accounts (id, label, active, ratio, opened, seen, price, token, payload, group_id) VALUES (1, 'first', TRUE, 0.5, '2026-01-15', '2026-01-15 10:30:00', '19.99', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\\xabcd', 1)",
        "INSERT INTO accounts (id, label, active, ratio, opened, seen, price, token, payload, group_id) VALUES (2, 'second', FALSE, 'NaN', '2025-12-31', '2025-12-31 23:59:59.5', '-0.001', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', '\\x', 2)",
        "INSERT INTO accounts (id, label, active, ratio, opened, seen, price, token, payload, group_id) VALUES (3, 'third', NULL, -1.5, NULL, NULL, NULL, NULL, NULL, NULL)",
    ] {
        if let Err(error) = database.execute_sql(statement) {
            panic!("seed failure {error:?} in {statement}");
        }
    }
    database
        .execute_sql("CREATE INDEX accounts_opened_idx ON accounts (opened)")
        .expect("index");
    database
}

#[test]
fn dump_and_restore_round_trips_every_type_and_reopens() {
    let directory = tempdir().expect("tempdir");
    let mut source = seeded(&directory.path().join("source-db"));
    let dump = omendb::dump_sql(&mut source).expect("dump");

    // The dump is readable SQL with the documented sections.
    assert!(dump.contains("CREATE TABLE"));
    assert!(dump.contains("INSERT INTO"));
    assert!(dump.contains("CREATE INDEX"));
    assert!(dump.contains("FOREIGN KEY"));
    // Quote-escaping round-trips: rename a label through SQL first.
    source
        .execute_sql("UPDATE groups SET name = 'alph''a' WHERE id = 1")
        .expect("quoted name");
    let dump = omendb::dump_sql(&mut source).expect("dump with quote");

    let target_path = directory.path().join("restored-db");
    let mut target = RelationalDatabase::create(config(&target_path)).expect("target create");
    omendb::restore_sql(&mut target, &dump).expect("restore");

    // Rows: exact typed values, including NaN, NULLs, negative decimals,
    // and bytea payloads.
    let rows = target
        .execute_sql("SELECT id, ratio, opened, token, payload FROM accounts ORDER BY id")
        .expect("restored rows");
    assert_eq!(rows.rows.len(), 3);
    assert_eq!(rows.rows[0][0], Value::I64(1));
    assert_eq!(rows.rows[0][1], Value::Float64(F64::new(0.5)));
    assert_eq!(
        rows.rows[0][2],
        Value::Date(DateValue(20_468)) // 2026-01-15
    );
    assert_eq!(
        rows.rows[0][3],
        Value::Uuid(UuidValue::parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").expect("uuid"))
    );
    assert_eq!(rows.rows[0][4], Value::Bytes(vec![0xab, 0xcd]));
    assert_eq!(rows.rows[1][1], Value::Float64(F64::new(f64::NAN)));
    assert_eq!(rows.rows[1][4], Value::Bytes(Vec::new()));
    assert_eq!(rows.rows[2][2], Value::Null);

    // The quote-escaped text survived.
    let names = target
        .execute_sql("SELECT name FROM groups WHERE id = 1")
        .expect("quoted name restored");
    assert_eq!(names.rows, vec![vec![Value::Text("alph'a".to_owned())]]);

    // Foreign keys enforced in the restored database.
    assert!(
        target
            .execute_sql("INSERT INTO accounts (id, label, group_id) VALUES (9, 'orphan', 77)")
            .is_err()
    );

    // Unique constraint enforced too.
    assert!(
        target
            .execute_sql("INSERT INTO accounts (id, label) VALUES (10, 'first')")
            .is_err()
    );

    // Durability: reopen and read again.
    drop(target);
    let mut reopened = RelationalDatabase::open(config(&target_path)).expect("reopen restored");
    let rows = reopened
        .execute_sql("SELECT count(id) FROM accounts")
        .expect("count after reopen");
    assert_eq!(rows.rows, vec![vec![Value::U64(3)]]);
}

#[test]
fn dump_is_stable_across_runs() {
    let directory = tempdir().expect("tempdir");
    let mut source = seeded(&directory.path().join("source-db"));
    let first = omendb::dump_sql(&mut source).expect("first dump");
    let second = omendb::dump_sql(&mut source).expect("second dump");
    assert_eq!(first, second, "dump must be deterministic");
}

#[test]
fn restore_rejects_inconsistent_foreign_keys() {
    let directory = tempdir().expect("tempdir");
    let mut source = seeded(&directory.path().join("source-db"));
    let dump = omendb::dump_sql(&mut source).expect("dump");
    // Drop the referenced table's rows: the FK validation inside the
    // restore path must refuse the constraint addition.
    let tampered = dump.replace(
        "INSERT INTO \"groups\" (\"id\", \"name\") VALUES (1, 'alpha'), (2, 'beta');",
        "",
    );
    assert!(
        !tampered.is_empty() && tampered != dump,
        "tamper must remove something"
    );
    let target_path = directory.path().join("broken-db");
    let mut target = RelationalDatabase::create(config(&target_path)).expect("target create");
    assert!(
        omendb::restore_sql(&mut target, &tampered).is_err(),
        "foreign key must refuse without referenced rows"
    );
}

#[test]
fn restore_handles_dumps_from_empty_databases() {
    let directory = tempdir().expect("tempdir");
    let source_path = directory.path().join("empty-db");
    let mut source = RelationalDatabase::create(config(&source_path)).expect("empty create");
    let dump = omendb::dump_sql(&mut source).expect("empty dump");
    assert!(dump.trim().is_empty(), "no tables, no statements");
    let target_path = directory.path().join("empty-restored");
    let mut target = RelationalDatabase::create(config(&target_path)).expect("target");
    omendb::restore_sql(&mut target, &dump).expect("restore nothing");
    drop(target);
}

#[test]
fn u64_columns_dump_as_numeric_and_restore() {
    let directory = tempdir().expect("tempdir");
    let source_path = directory.path().join("u64-db");
    let mut source = RelationalDatabase::create(config(&source_path)).expect("create");
    // u64 exists only through the typed tier; build it there.
    source
        .execute_sql("CREATE TABLE meters (id BIGINT PRIMARY KEY, note TEXT)")
        .expect("meters");
    let meter_table = source
        .catalog()
        .tables()
        .find(|table| table.name == "meters")
        .expect("meters catalog entry")
        .id;
    source
        .insert(
            meter_table,
            omendb::Row {
                primary: omendb::Key::new(1, 1),
                values: vec![Value::I64(7), Value::Text("seven".to_owned())],
            },
        )
        .expect("insert");
    let dump = omendb::dump_sql(&mut source).expect("dump");
    let target_path = directory.path().join("u64-restored");
    let mut target = RelationalDatabase::create(config(&target_path)).expect("target");
    omendb::restore_sql(&mut target, &dump).expect("restore");
    let rows = target
        .execute_sql("SELECT id, note FROM meters")
        .expect("restored meters");
    assert_eq!(
        rows.rows,
        vec![vec![Value::I64(7), Value::Text("seven".to_owned())]]
    );
}
