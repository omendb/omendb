//! Arithmetic must preserve the bound domain, exact values and statement atomicity.
#[allow(dead_code)]
mod support;

use omendb::{ColumnType, DbError, DecimalValue, RelationalDatabase, Value};
use support::config;
use tempfile::tempdir;

fn decimal(mantissa: i128, scale: u32) -> Value {
    Value::Decimal(DecimalValue::new(mantissa, scale).unwrap())
}

fn database(path: &std::path::Path) -> RelationalDatabase {
    let mut db = RelationalDatabase::create(config(path)).unwrap();
    db.execute_sql(
        "CREATE TABLE numbers (id BIGINT PRIMARY KEY, n BIGINT, d NUMERIC, f DOUBLE PRECISION)",
    )
    .unwrap();
    db
}

#[test]
fn signed_integer_operations_keep_their_bound_domain_and_sql_sign_rules() {
    let dir = tempdir().unwrap();
    let mut db = database(&dir.path().join("db"));
    db.execute_sql("INSERT INTO numbers VALUES (1, -7, NULL, NULL)")
        .unwrap();
    for (expression, expected) in [
        ("n + 2", -5),
        ("n - 2", -9),
        ("n * 2", -14),
        ("n / 3", -2),
        ("n % 3", -1),
        ("n / -3", 2),
        ("n % -3", -1),
        ("7 / -3", -2),
        ("7 % -3", 1),
    ] {
        let result = db
            .execute_sql(&format!("SELECT {expression} AS result FROM numbers"))
            .unwrap();
        assert_eq!(result.columns[0].column_type, Some(ColumnType::I64));
        assert_eq!(
            result.rows,
            vec![vec![Value::I64(expected)]],
            "{expression}"
        );
    }
    assert_eq!(
        db.execute_sql("SELECT id FROM numbers WHERE n + 2 < 0")
            .unwrap()
            .rows,
        vec![vec![Value::I64(1)]]
    );
    db.close().unwrap();
}

#[test]
fn subtracting_minimum_integer_from_itself_does_not_negate_it_first() {
    let dir = tempdir().unwrap();
    let mut db = database(&dir.path().join("db"));
    db.execute_sql_with_params(
        "INSERT INTO numbers VALUES (1, $1, NULL, NULL)",
        &[Value::I64(i64::MIN)],
    )
    .unwrap();
    db.execute_sql("UPDATE numbers SET n = n - n").unwrap();
    assert_eq!(
        db.execute_sql("SELECT n FROM numbers").unwrap().rows,
        vec![vec![Value::I64(0)]]
    );
    db.close().unwrap();
}

#[test]
fn arithmetic_nulls_propagate_in_assignments_and_projections() {
    let dir = tempdir().unwrap();
    let mut db = database(&dir.path().join("db"));
    db.execute_sql("INSERT INTO numbers VALUES (1, NULL, NULL, NULL)")
        .unwrap();
    db.execute_sql("UPDATE numbers SET n = n + 1, d = d - 0.25, f = f + 1.0")
        .unwrap();
    assert_eq!(
        db.execute_sql("SELECT n, d, f, n + 1, d - 1, f * 2 FROM numbers")
            .unwrap()
            .rows,
        vec![vec![Value::Null; 6]]
    );
    db.close().unwrap();
}

#[test]
fn mixed_integer_decimal_updates_remain_exact_and_indexed_after_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = database(&path);
    db.execute_sql("CREATE UNIQUE INDEX numbers_decimal ON numbers (d)")
        .unwrap();
    db.execute_sql("INSERT INTO numbers VALUES (1, 9007199254740993, 1.25, NULL)")
        .unwrap();
    assert_eq!(
        db.execute_sql("SELECT d + n FROM numbers").unwrap().rows,
        vec![vec![decimal(900719925474099425, 2)]]
    );
    db.execute_sql("UPDATE numbers SET d = d + n").unwrap();
    let query = "SELECT id, d FROM numbers WHERE d = 9007199254740994.25";
    let expected = vec![vec![Value::I64(1), decimal(900719925474099425, 2)]];
    assert_eq!(db.execute_sql(query).unwrap().rows, expected);
    db.close().unwrap();
    let mut reopened = RelationalDatabase::open(config(&path)).unwrap();
    assert_eq!(reopened.execute_sql(query).unwrap().rows, expected);
    reopened.close().unwrap();
}

#[test]
fn integer_overflow_aborts_preceding_staged_writes() {
    for sql in [
        "SELECT n + 1 FROM numbers WHERE id = 1",
        "SELECT n * 2 FROM numbers WHERE id = 1",
        "UPDATE numbers SET n = n + 1 WHERE id = 1",
        "SELECT id FROM numbers WHERE n + 1 > 0",
        "INSERT INTO saved SELECT id FROM numbers WHERE n + 1 > 0",
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let mut db = database(&path);
        db.execute_sql("CREATE TABLE saved (id BIGINT)").unwrap();
        db.execute_sql_with_params(
            "INSERT INTO numbers VALUES (1, $1, NULL, NULL)",
            &[Value::I64(i64::MAX)],
        )
        .unwrap();
        let head = db.commit_id();
        let mut tx = db.begin().unwrap();
        tx.execute_sql(&db, "INSERT INTO numbers VALUES (2, 7, NULL, NULL)")
            .unwrap();
        assert!(
            matches!(
                tx.execute_sql(&db, sql),
                Err(DbError::SqlNumericValueOutOfRange(_))
            ),
            "{sql}"
        );
        assert!(tx.commit().is_err());
        assert_eq!(db.commit_id(), head);
        db.close().unwrap();
        let mut reopened = RelationalDatabase::open(config(&path)).unwrap();
        assert_eq!(
            reopened
                .execute_sql("SELECT id FROM numbers ORDER BY id")
                .unwrap()
                .rows,
            vec![vec![Value::I64(1)]]
        );
        assert!(
            reopened
                .execute_sql("SELECT id FROM saved")
                .unwrap()
                .rows
                .is_empty()
        );
        reopened.close().unwrap();
    }
}

#[test]
fn unsigned_arithmetic_preserves_its_domain_without_float_promotion() {
    use omendb::{ColumnDefinition, ColumnId, TableDefinition, TableId};
    let dir = tempdir().unwrap();
    let mut db = RelationalDatabase::create(config(&dir.path().join("db"))).unwrap();
    db.create_table(TableDefinition {
        id: TableId(1),
        name: "unsigned_numbers".into(),
        columns: [("id", ColumnId(1)), ("u", ColumnId(2)), ("v", ColumnId(3))]
            .into_iter()
            .map(|(name, id)| ColumnDefinition {
                id,
                name: name.into(),
                data_type: ColumnType::U64,
                nullable: false,
            })
            .collect(),
    })
    .unwrap();
    db.execute_sql("INSERT INTO unsigned_numbers VALUES (1, 3, 2)")
        .unwrap();
    db.execute_sql("UPDATE unsigned_numbers SET u = u + v, v = u - v")
        .unwrap();
    let result = db
        .execute_sql("SELECT u, v, u + v, u * v, u / v, u % v, u + 1 FROM unsigned_numbers")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            Value::U64(5),
            Value::U64(1),
            Value::U64(6),
            Value::U64(5),
            Value::U64(5),
            Value::U64(0),
            Value::I64(6)
        ]]
    );
    assert_eq!(
        result
            .columns
            .iter()
            .map(|c| c.column_type)
            .collect::<Vec<_>>(),
        vec![
            Some(ColumnType::U64),
            Some(ColumnType::U64),
            Some(ColumnType::U64),
            Some(ColumnType::U64),
            Some(ColumnType::U64),
            Some(ColumnType::U64),
            Some(ColumnType::I64)
        ]
    );
    db.execute_sql_with_params(
        "UPDATE unsigned_numbers SET u = $1, v = $2",
        &[Value::U64(u64::MAX), Value::U64(1)],
    )
    .unwrap();
    for sql in [
        "SELECT u + v FROM unsigned_numbers",
        "SELECT v - u FROM unsigned_numbers",
        "SELECT u * u FROM unsigned_numbers",
        "SELECT u + 0 FROM unsigned_numbers",
        "UPDATE unsigned_numbers SET u = u + v",
        "UPDATE unsigned_numbers SET v = v - 2",
    ] {
        assert!(
            matches!(
                db.execute_sql(sql),
                Err(DbError::SqlNumericValueOutOfRange(_))
            ),
            "{sql}"
        );
    }
    assert_eq!(
        db.execute_sql("SELECT u FROM unsigned_numbers")
            .unwrap()
            .rows,
        vec![vec![Value::U64(u64::MAX)]]
    );
    db.close().unwrap();
}

#[test]
fn decimal_multiplication_does_not_rescale_operands_as_if_adding() {
    let dir = tempdir().unwrap();
    let mut db = database(&dir.path().join("db"));
    db.execute_sql("INSERT INTO numbers VALUES (1, 1, 1.25, NULL)")
        .unwrap();
    assert_eq!(
        db.execute_sql("SELECT d * 100000000000000000000000000000000 FROM numbers")
            .unwrap()
            .rows,
        vec![vec![decimal(125 * 10_i128.pow(32), 2)]]
    );
    db.close().unwrap();
}

#[test]
fn decimal_division_preserves_exact_results_and_rounds_with_the_quotient_sign() {
    let dir = tempdir().unwrap();
    let mut db = database(&dir.path().join("db"));
    db.execute_sql("INSERT INTO numbers VALUES (1, 1, 1, NULL)")
        .unwrap();
    for (expression, expected) in [
        ("d / 1", decimal(1, 0)),
        ("d / -3", decimal(-33333, 5)),
        ("d / -6", decimal(-16667, 5)),
    ] {
        assert_eq!(
            db.execute_sql(&format!("SELECT {expression} FROM numbers"))
                .unwrap()
                .rows,
            vec![vec![expected]],
            "{expression}"
        );
    }
    db.close().unwrap();
}

#[test]
fn float_division_by_zero_is_a_statement_error_not_a_committed_infinity() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = database(&path);
    db.execute_sql("INSERT INTO numbers VALUES (1, 1, NULL, 1.0)")
        .unwrap();
    let head = db.commit_id();
    let mut tx = db.begin().unwrap();
    tx.execute_sql(&db, "INSERT INTO numbers VALUES (2, 2, NULL, 2.0)")
        .unwrap();
    assert!(matches!(
        tx.execute_sql(&db, "SELECT f / 0.0 FROM numbers"),
        Err(DbError::SqlDivisionByZero)
    ));
    assert!(tx.commit().is_err());
    assert_eq!(db.commit_id(), head);
    db.close().unwrap();
}
