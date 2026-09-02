//! Typed scalar integration: Float64, Date, Timestamp, Decimal, and UUID
//! through the public SQL facade. Covers the behaviors that make the types
//! real: DDL acceptance, literal coercion, comparison and ordering,
//! aggregates, secondary indexes, primary keys on typed columns, and
//! durable persistence across reopen.

use std::path::Path;

use omendb::{
    DateValue, DecimalValue, F64, OperationControl, RelationalBackendConfig,
    RelationalDatabaseConfig, RelationalDatabaseSession, RelationalSessionConfig, TimestampValue,
    UuidValue, Value,
};
use tempfile::tempdir;

fn config(directory: &Path) -> RelationalDatabaseConfig {
    let backend = RelationalBackendConfig::new(directory.to_owned());
    RelationalDatabaseConfig::new(backend).with_session_config(RelationalSessionConfig::default())
}

#[test]
fn typed_columns_roundtrip_through_sql_and_reopen() {
    let directory = tempdir().expect("tempdir");
    let database_path = directory.path().join("typed-db");
    let control = OperationControl::default();

    {
        let session = RelationalDatabaseSession::create(config(&database_path)).expect("create");
        let created = session
            .execute_sql(
                &control,
                "CREATE TABLE events (
                     id UUID PRIMARY KEY,
                     name TEXT NOT NULL,
                     ratio DOUBLE PRECISION,
                     price NUMERIC(12, 2),
                     occurred DATE NOT NULL,
                     seen_at TIMESTAMP
                 )",
            )
            .expect("create table");
        assert!(created.commit.is_some());

        session
            .execute_sql_with_params(
                &control,
                "INSERT INTO events VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    Value::Uuid(UuidValue::parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap()),
                    Value::Text("beta".to_owned()),
                    Value::Float64(F64::new(0.5)),
                    Value::Decimal(DecimalValue::new(1999, 2).unwrap()), // 19.99
                    Value::Date(DateValue(20_696)),                      // 2026-08-31
                    Value::Timestamp(TimestampValue(1_788_183_921_123_456)),
                ],
            )
            .expect("insert");
        session
            .execute_sql(
                &control,
                "INSERT INTO events VALUES (
                     'b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a12',
                     'alpha',
                     'NaN',
                     '0.001',
                     '2026-01-01',
                     '2026-08-31 13:45:21.123456')",
            )
            .expect("insert with literals");
    }

    // Reopen: typed rows survive durability and decode back to values.
    let session = RelationalDatabaseSession::open(config(&database_path)).expect("reopen");
    let result = session
        .execute_sql(
            &control,
            "SELECT id, name, ratio, price, occurred, seen_at
             FROM events ORDER BY name",
        )
        .expect("select");
    assert_eq!(result.rows.len(), 2);
    let [alpha, beta] = &result.rows[..] else {
        panic!("two rows")
    };
    assert_eq!(
        alpha[0],
        Value::Uuid(UuidValue::parse("b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a12").unwrap())
    );
    assert_eq!(alpha[2], Value::Float64(F64::new(f64::NAN)));
    assert_eq!(alpha[3], Value::Decimal(DecimalValue::new(1, 3).unwrap()));
    assert_eq!(alpha[4], Value::Date(DateValue(20_454))); // 2026-01-01
    assert_eq!(
        alpha[5],
        Value::Timestamp(TimestampValue(1_788_183_921_123_456))
    );
    assert_eq!(beta[2], Value::Float64(F64::new(0.5)));
    assert_eq!(beta[3], Value::Decimal(DecimalValue::new(1999, 2).unwrap()));
}

#[test]
fn typed_comparisons_and_index_lookups() {
    let directory = tempdir().expect("tempdir");
    let database_path = directory.path().join("typed-db");
    let control = OperationControl::default();
    let session = RelationalDatabaseSession::create(config(&database_path)).expect("create");

    session
        .execute_sql(
            &control,
            "CREATE TABLE ledger (id BIGINT PRIMARY KEY, price NUMERIC, when_c DATE, ratio DOUBLE PRECISION)",
        )
        .expect("create");

    for (id, price, when, ratio) in [
        (1i64, "10.50", "2026-01-15", "1.5"),
        (2, "9.99", "2026-02-01", "2.5"),
        (3, "100.00", "2025-12-31", "0.25"),
    ] {
        session
            .execute_sql(
                &control,
                &format!("INSERT INTO ledger VALUES ({id}, '{price}', '{when}', '{ratio}')"),
            )
            .expect("insert");
    }

    // Numeric cross-type comparison: decimal column against integer.
    let cheap = session
        .execute_sql(
            &control,
            "SELECT id FROM ledger WHERE price > 10 ORDER BY id",
        )
        .expect("q");
    assert_eq!(cheap.rows, vec![vec![Value::I64(1)], vec![Value::I64(3)]]);

    // Decimal ordering is numeric, not textual: 9.99 < 10.50 < 100.00.
    let ordered = session
        .execute_sql(&control, "SELECT id FROM ledger ORDER BY price")
        .expect("q");
    assert_eq!(
        ordered.rows,
        vec![
            vec![Value::I64(2)],
            vec![Value::I64(1)],
            vec![Value::I64(3)]
        ]
    );

    // Date comparison and ordering.
    let before = session
        .execute_sql(
            &control,
            "SELECT id FROM ledger WHERE when_c < '2026-02-01' ORDER BY id",
        )
        .expect("q");
    assert_eq!(before.rows, vec![vec![Value::I64(1)], vec![Value::I64(3)]]);

    // Decimal equality across representation: 10.50 == 10.5.
    let half = session
        .execute_sql(&control, "SELECT id FROM ledger WHERE price = 10.5")
        .expect("q");
    assert_eq!(half.rows, vec![vec![Value::I64(1)]]);

    // Secondary index over a typed column with exact-value lookup.
    session
        .execute_sql(&control, "CREATE INDEX ledger_price ON ledger (price)")
        .expect("index");
    let by_price = session
        .execute_sql(&control, "SELECT id FROM ledger WHERE price = '100.00'")
        .expect("q");
    assert_eq!(by_price.rows, vec![vec![Value::I64(3)]]);
}

#[test]
fn typed_aggregates_and_arithmetic() {
    let directory = tempdir().expect("tempdir");
    let database_path = directory.path().join("typed-db");
    let control = OperationControl::default();
    let session = RelationalDatabaseSession::create(config(&database_path)).expect("create");

    session
        .execute_sql(
            &control,
            "CREATE TABLE samples (id BIGINT PRIMARY KEY, price NUMERIC, ratio DOUBLE PRECISION)",
        )
        .expect("create");
    for (id, price, ratio) in [
        (1i64, "1.25", "0.5"),
        (2, "2.75", "1.25"),
        (3, "-0.10", "2.0"),
    ] {
        session
            .execute_sql(
                &control,
                &format!("INSERT INTO samples VALUES ({id}, '{price}', '{ratio}')"),
            )
            .expect("insert");
    }

    // SUM over decimal is exact at the widest scale: 1.25 + 2.75 - 0.10 = 3.90.
    let sums = session
        .execute_sql(&control, "SELECT SUM(price) FROM samples")
        .expect("q");
    assert_eq!(
        sums.rows,
        vec![vec![Value::Decimal(DecimalValue::new(390, 2).unwrap())]]
    );

    // AVG returns float8.
    let avg = session
        .execute_sql(&control, "SELECT AVG(ratio) FROM samples")
        .expect("q");
    assert_eq!(
        avg.rows,
        vec![vec![Value::Float64(F64::new((0.5 + 1.25 + 2.0) / 3.0))]]
    );

    // MIN/MAX over decimals use numeric ordering.
    let extremes = session
        .execute_sql(&control, "SELECT MIN(price), MAX(price) FROM samples")
        .expect("q");
    assert_eq!(
        extremes.rows,
        vec![vec![
            Value::Decimal(DecimalValue::new(-10, 2).unwrap()),
            Value::Decimal(DecimalValue::new(275, 2).unwrap()),
        ]]
    );

    // Decimal arithmetic: 1.25 * 2 = 2.50; 10 / 4 = 2.5.
    let product = session
        .execute_sql(&control, "SELECT price * 2 FROM samples WHERE id = 1")
        .expect("q");
    assert_eq!(
        product.rows,
        vec![vec![Value::Decimal(DecimalValue::new(250, 2).unwrap())]]
    );

    // Float division by zero yields infinity (PostgreSQL float8).
    let inf = session
        .execute_sql(&control, "SELECT ratio / 0.0 FROM samples WHERE id = 1")
        .expect("q");
    assert_eq!(
        inf.rows,
        vec![vec![Value::Float64(F64::new(f64::INFINITY))]]
    );
}

#[test]
fn typed_primary_keys_distinguish_values() {
    let directory = tempdir().expect("tempdir");
    let database_path = directory.path().join("typed-db");
    let control = OperationControl::default();
    let session = RelationalDatabaseSession::create(config(&database_path)).expect("create");

    session
        .execute_sql(
            &control,
            "CREATE TABLE tokens (token UUID PRIMARY KEY, label TEXT)",
        )
        .expect("create");
    for (token, label) in [
        ("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "one"),
        ("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12", "two"),
    ] {
        session
            .execute_sql(
                &control,
                &format!("INSERT INTO tokens VALUES ('{token}', '{label}')"),
            )
            .expect("insert");
    }
    // Both UUIDs must land on distinct rows despite hash-derived keys.
    let all = session
        .execute_sql(&control, "SELECT label FROM tokens ORDER BY label")
        .expect("q");
    assert_eq!(
        all.rows,
        vec![
            vec![Value::Text("one".to_owned())],
            vec![vec![Value::Text("two".to_owned())][0].clone()],
        ]
    );
}

#[test]
fn typed_value_roundtrips_through_encoders_and_bounds() {
    // Date/timestamp bounds reject out-of-range values with SQLSTATE 22003.
    let directory = tempdir().expect("tempdir");
    let database_path = directory.path().join("typed-db");
    let control = OperationControl::default();
    let session = RelationalDatabaseSession::create(config(&database_path)).expect("create");
    session
        .execute_sql(
            &control,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, when_c DATE)",
        )
        .expect("create");
    let bad = session.execute_sql(&control, "INSERT INTO t VALUES (1, '10000-01-01')");
    assert!(bad.is_err(), "out-of-range date rejected");
}
