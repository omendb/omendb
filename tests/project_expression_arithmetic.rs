//! Expression keys must use the same exact arithmetic as scalar SQL.
#[allow(dead_code)]
mod support;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DbError, DecimalValue, F64, IndexId,
    RelationalDatabase, TableDefinition, TableId, Value,
};
use support::config;
use tempfile::tempdir;

fn index(db: &RelationalDatabase, name: &str) -> (TableId, IndexId) {
    let definition = db
        .catalog()
        .indexes()
        .find(|index| db.catalog().index_name(index.id) == Some(name))
        .unwrap();
    (definition.table, definition.id)
}

fn lookup(db: &mut RelationalDatabase, name: &str, key: Value) -> Vec<Value> {
    let (table, index) = index(db, name);
    let mut ids = db
        .index_get(table, index, &[key])
        .unwrap()
        .into_iter()
        .map(|row| row.values[0].clone())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

#[test]
fn mixed_float_keys_preserve_operand_order_through_build_update_and_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = RelationalDatabase::create(config(&path)).unwrap();
    db.execute_sql(
        "CREATE TABLE numbers (id BIGINT PRIMARY KEY, n BIGINT, f DOUBLE PRECISION, v BIGINT)",
    )
    .unwrap();
    db.execute_sql("INSERT INTO numbers VALUES (1, 10, 2, 2), (2, 20, 4, 2)")
        .unwrap();
    for (name, expression, key, ids) in [
        ("quotient", "n / f", 5.0, vec![1, 2]),
        ("difference", "n - f", 8.0, vec![1]),
        ("reverse_quotient", "f / n", 0.2, vec![1, 2]),
        ("reverse_difference", "f - n", -8.0, vec![1]),
        ("nested_quotient", "(n - f) / v", 4.0, vec![1]),
    ] {
        db.execute_sql(&format!("CREATE INDEX {name} ON numbers ({expression})"))
            .unwrap();
        let expected: Vec<Value> = ids.into_iter().map(Value::I64).collect();
        assert_eq!(
            lookup(&mut db, name, Value::Float64(F64(key))),
            expected,
            "{expression}"
        );
    }
    db.execute_sql("UPDATE numbers SET n = n + 2 WHERE id = 1")
        .unwrap();
    assert_eq!(
        lookup(&mut db, "quotient", Value::Float64(F64(5.0))),
        vec![Value::I64(2)]
    );
    assert_eq!(
        lookup(&mut db, "quotient", Value::Float64(F64(6.0))),
        vec![Value::I64(1)]
    );
    let dump = omendb::dump_sql(&mut db).unwrap();
    let restored_path = dir.path().join("restored");
    let mut restored = RelationalDatabase::create(config(&restored_path)).unwrap();
    omendb::restore_sql(&mut restored, &dump).unwrap();
    assert_eq!(
        lookup(&mut restored, "quotient", Value::Float64(F64(6.0))),
        vec![Value::I64(1)]
    );
    assert_eq!(
        lookup(&mut restored, "nested_quotient", Value::Float64(F64(5.0))),
        vec![Value::I64(1)]
    );
    restored.close().unwrap();
    let mut restored = RelationalDatabase::open(config(&restored_path)).unwrap();
    assert_eq!(
        lookup(&mut restored, "nested_quotient", Value::Float64(F64(5.0))),
        vec![Value::I64(1)]
    );
    restored.close().unwrap();
    db.close().unwrap();
    let mut db = RelationalDatabase::open(config(&path)).unwrap();
    assert_eq!(
        lookup(&mut db, "difference", Value::Float64(F64(10.0))),
        vec![Value::I64(1)]
    );
    assert_eq!(
        lookup(&mut db, "nested_quotient", Value::Float64(F64(5.0))),
        vec![Value::I64(1)]
    );
    db.execute_sql("DELETE FROM numbers WHERE id = 1").unwrap();
    assert!(lookup(&mut db, "quotient", Value::Float64(F64(6.0))).is_empty());
    db.close().unwrap();
}

#[test]
fn mixed_signed_keys_do_not_merge_distinct_large_integers() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = RelationalDatabase::create(config(&path)).unwrap();
    db.create_table(TableDefinition {
        id: TableId(1),
        name: "numbers".into(),
        columns: [
            ("id", ColumnType::I64),
            ("n", ColumnType::I64),
            ("u", ColumnType::U64),
        ]
        .into_iter()
        .enumerate()
        .map(|(position, (name, data_type))| ColumnDefinition {
            id: ColumnId(position as u16 + 1),
            name: name.into(),
            data_type,
            nullable: false,
        })
        .collect(),
    })
    .unwrap();
    db.execute_sql("CREATE UNIQUE INDEX exact_sum ON numbers (n + u)")
        .unwrap();
    db.execute_sql("INSERT INTO numbers VALUES (1, 9007199254740992, 0), (2, 9007199254740993, 0)")
        .unwrap();
    assert_eq!(
        lookup(&mut db, "exact_sum", Value::I64(9007199254740993)),
        vec![Value::I64(2)]
    );
    db.execute_sql("CREATE INDEX unsigned_left ON numbers (u - n)")
        .unwrap();
    assert_eq!(
        lookup(&mut db, "unsigned_left", Value::I64(-9007199254740993)),
        vec![Value::I64(2)]
    );
    db.close().unwrap();
    let mut db = RelationalDatabase::open(config(&path)).unwrap();
    assert_eq!(
        lookup(&mut db, "exact_sum", Value::I64(9007199254740992)),
        vec![Value::I64(1)]
    );
    db.close().unwrap();
}

#[test]
fn decimal_and_null_expression_keys_match_scalar_values() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = RelationalDatabase::create(config(&path)).unwrap();
    db.execute_sql("CREATE TABLE numbers (id BIGINT PRIMARY KEY, d NUMERIC, n BIGINT)")
        .unwrap();
    db.execute_sql("INSERT INTO numbers VALUES (1, 1.25, 3), (2, NULL, 0)")
        .unwrap();
    db.execute_sql("CREATE UNIQUE INDEX decimal_product ON numbers (d * n)")
        .unwrap();
    let value = Value::Decimal(DecimalValue::new(375, 2).unwrap());
    assert_eq!(
        lookup(&mut db, "decimal_product", value.clone()),
        vec![Value::I64(1)]
    );
    assert_eq!(
        db.execute_sql("SELECT d * n FROM numbers ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![value], vec![Value::Null]]
    );
    db.execute_sql("UPDATE numbers SET d = d + 1 WHERE id = 1")
        .unwrap();
    db.close().unwrap();
    let mut db = RelationalDatabase::open(config(&path)).unwrap();
    assert_eq!(
        lookup(
            &mut db,
            "decimal_product",
            Value::Decimal(DecimalValue::new(675, 2).unwrap())
        ),
        vec![Value::I64(1)]
    );
    db.close().unwrap();
}

#[test]
fn null_left_operand_does_not_hide_nested_arithmetic_errors() {
    use omendb::{ArithmeticOperator as Op, IndexDefinition, IndexExpression as Expr};
    let dir = tempdir().unwrap();
    let mut db = RelationalDatabase::create(config(&dir.path().join("db"))).unwrap();
    db.execute_sql("CREATE TABLE numbers (a BIGINT, b BIGINT, c BIGINT)")
        .unwrap();
    db.execute_sql("INSERT INTO numbers VALUES (NULL, 7, 0)")
        .unwrap();
    assert!(matches!(
        db.execute_sql("SELECT a + b / c FROM numbers"),
        Err(DbError::SqlDivisionByZero)
    ));
    let table = db.catalog().tables().next().unwrap();
    let column = |position: usize| Box::new(Expr::Column(table.columns[position].id));
    let expression = Expr::Arithmetic {
        left: column(0),
        op: Op::Add,
        right: Box::new(Expr::Arithmetic {
            left: column(1),
            op: Op::Divide,
            right: column(2),
        }),
    };
    let definition = IndexDefinition {
        id: IndexId(1),
        table: table.id,
        parts: vec![omendb::IndexKeyPart::Expression(expression)],
        unique: false,
        predicate: None,
    };
    assert!(matches!(
        db.create_index(definition),
        Err(DbError::SqlDivisionByZero)
    ));
    assert!(db.catalog().indexes().all(|index| index.id != IndexId(1)));
    db.close().unwrap();
}

#[test]
fn expression_errors_abort_staged_rows_and_keys() {
    for (n, divisor, overflow) in [(i64::MAX, 1, true), (7, 0, false)] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let mut db = RelationalDatabase::create(config(&path)).unwrap();
        db.execute_sql("CREATE TABLE numbers (id BIGINT PRIMARY KEY, n BIGINT, v BIGINT)")
            .unwrap();
        let expression = if overflow { "n + v" } else { "n / v" };
        db.execute_sql(&format!(
            "CREATE INDEX arithmetic ON numbers ({expression})"
        ))
        .unwrap();
        let head = db.commit_id();
        let mut tx = db.begin().unwrap();
        tx.execute_sql(&db, "INSERT INTO numbers VALUES (1, 8, 2)")
            .unwrap();
        let error = tx
            .execute_sql_with_params(
                &db,
                "INSERT INTO numbers VALUES (2, $1, $2)",
                &[Value::I64(n), Value::I64(divisor)],
            )
            .unwrap_err();
        assert!(
            if overflow {
                matches!(error, DbError::SqlNumericValueOutOfRange(_))
            } else {
                matches!(error, DbError::SqlDivisionByZero)
            },
            "{error}"
        );
        assert!(tx.commit().is_err());
        assert_eq!(db.commit_id(), head);
        assert!(
            lookup(
                &mut db,
                "arithmetic",
                Value::I64(if overflow { 10 } else { 4 })
            )
            .is_empty()
        );
        db.close().unwrap();
        let mut db = RelationalDatabase::open(config(&path)).unwrap();
        assert!(
            db.execute_sql("SELECT id FROM numbers")
                .unwrap()
                .rows
                .is_empty()
        );
        db.close().unwrap();
    }
}
