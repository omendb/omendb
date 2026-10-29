use omendb::{ColumnType, DbError, RelationalBackendConfig, RelationalDatabase, Value};

#[test]
fn empty_relations_do_not_hide_binding_errors_or_preserve_staged_writes() {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    database
        .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT)")
        .unwrap();
    database
        .execute_sql("CREATE TABLE staging (id BIGINT PRIMARY KEY)")
        .unwrap();
    for source in [
        "SELECT id FROM items WHERE absent = 1",
        "SELECT id FROM items WHERE absent = 1 LIMIT 0",
        "UPDATE items SET id = absent",
        "DELETE FROM items WHERE absent = 1",
        "SELECT SUM(label) FROM items",
        "SELECT COALESCE(id, upper(id)) FROM items",
    ] {
        assert!(
            database.execute_sql(source).is_err(),
            "bind all branches before scanning: {source}"
        );
    }
    let mut transaction = database.begin().unwrap();
    transaction
        .execute_sql(&database, "INSERT INTO staging VALUES (1)")
        .unwrap();
    assert!(matches!(
        transaction.execute_sql(&database, "SELECT id FROM items WHERE absent = 1"),
        Err(DbError::SqlUndefinedColumn { .. })
    ));
    assert!(matches!(
        transaction.commit(),
        Err(DbError::TransactionAborted)
    ));
    assert!(
        database
            .execute_sql("SELECT * FROM staging")
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn numeric_domains_are_coerced_before_operators_and_set_equality() {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    database
        .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, amount DOUBLE PRECISION)")
        .unwrap();
    database
        .execute_sql("INSERT INTO items VALUES (1, 1.0)")
        .unwrap();
    let null_numeric = database.execute_sql("SELECT abs(NULL)").unwrap();
    assert_eq!(null_numeric.columns[0].column_type, Some(ColumnType::I64));
    assert_eq!(null_numeric.rows, vec![vec![Value::Null]]);
    let truncated = database
        .execute_sql("SELECT date_trunc('month', CAST('2026-09-05' AS DATE))")
        .unwrap();
    assert_eq!(
        truncated.columns[0].column_type,
        Some(ColumnType::Timestamp)
    );
    assert_eq!(
        truncated.rows,
        vec![vec![Value::Timestamp(
            omendb::TimestampValue::from_micros(20_697 * 86_400_000_000).unwrap()
        )]]
    );
    for (source, params) in [
        (
            "SELECT (CASE WHEN id = 1 THEN 1 ELSE amount END) / 2 AS quotient FROM items",
            vec![],
        ),
        (
            "SELECT COALESCE($1, 1) / 2 AS quotient FROM items WHERE amount = $1 OR $1 IS NULL",
            vec![Value::Null],
        ),
    ] {
        let result = database.execute_sql_with_params(source, &params).unwrap();
        assert_eq!(result.columns[0].column_type, Some(ColumnType::Float64));
        let Value::Float64(value) = result.rows[0][0] else {
            panic!("float domain: {source}")
        };
        assert_eq!(value.0, 0.5);
    }
    let result = database
        .execute_sql("SELECT id AS value FROM items UNION SELECT amount AS value FROM items")
        .unwrap();
    assert_eq!(result.columns[0].column_type, Some(ColumnType::Float64));
    assert_eq!(
        result.rows.len(),
        1,
        "equal numeric values must deduplicate after coercion"
    );
}

#[test]
fn scalar_subquery_domains_survive_set_equivalence() {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    database
        .execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, f DOUBLE PRECISION)")
        .unwrap();
    database
        .execute_sql(
            "INSERT INTO t VALUES (1, NULL), (9007199254740992, NULL), (9007199254740993, NULL)",
        )
        .unwrap();
    let result = database.execute_sql(
        "SELECT (SELECT f FROM t WHERE id = 1) AS x FROM t WHERE id = 1 UNION SELECT id FROM t WHERE id > 1",
    ).unwrap();
    assert_eq!(result.columns[0].column_type, Some(ColumnType::Float64));
    assert_eq!(result.rows.len(), 2);
    assert!(result.rows.contains(&vec![Value::Null]));
    assert!(result.rows.contains(&vec![Value::Float64(omendb::F64::new(
        9_007_199_254_740_992.0
    ))]));
}

#[test]
fn qualified_join_bindings_and_grouped_projection_keep_their_positions() {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    for source in [
        "CREATE TABLE a (id BIGINT PRIMARY KEY)",
        "CREATE TABLE b (id BIGINT PRIMARY KEY, a_id BIGINT)",
        "CREATE TABLE c (id BIGINT PRIMARY KEY, b_id BIGINT)",
        "INSERT INTO a VALUES (1)",
        "INSERT INTO b VALUES (11, 1)",
        "INSERT INTO c VALUES (111, 11)",
    ] {
        database.execute_sql(source).unwrap();
    }
    let joined = "FROM a x JOIN b y ON x.id = y.a_id JOIN c z ON y.id = z.b_id";
    let result = database
        .execute_sql(&format!(
            "SELECT x.id, y.id, z.id {joined} WHERE y.id = 11 AND x.id = 1"
        ))
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![Value::I64(1), Value::I64(11), Value::I64(111)]]
    );
    let result = database
        .execute_sql(&format!(
            "SELECT COUNT(*) AS n, y.id AS bid, x.id AS aid {joined} GROUP BY x.id, y.id"
        ))
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![Value::U64(1), Value::I64(11), Value::I64(1)]]
    );
    let result = database.execute_sql(
        "SELECT id, x.id AS left_id, y.id AS right_id FROM a x FULL JOIN b y USING(id) ORDER BY id",
    ).unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![Value::I64(1), Value::I64(1), Value::Null],
            vec![Value::I64(11), Value::Null, Value::I64(11)],
        ]
    );
    let result = database
        .execute_sql("SELECT * FROM a x FULL JOIN b y USING(id) ORDER BY id")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![Value::I64(1), Value::Null],
            vec![Value::I64(11), Value::I64(1)],
        ]
    );
    let result = database
        .execute_sql(
            "SELECT COUNT(*) AS n FROM a x JOIN b y ON x.id = y.a_id GROUP BY y.a_id ORDER BY a_id",
        )
        .unwrap();
    assert_eq!(
        result.columns.len(),
        1,
        "unprojected order keys stay internal"
    );
    assert_eq!(result.rows, vec![vec![Value::U64(1)]]);
    let result = database.execute_sql(
        "SELECT COUNT(*) AS n FROM a x JOIN b y ON x.id = y.a_id GROUP BY y.a_id HAVING a_id > 0",
    ).unwrap();
    assert_eq!(result.rows, vec![vec![Value::U64(1)]]);
    let result = database.execute_sql(
        "SELECT y.a_id AS key, COUNT(*) AS n FROM a x JOIN b y ON x.id = y.a_id GROUP BY y.a_id HAVING a_id > 0",
    ).unwrap();
    assert_eq!(result.rows, vec![vec![Value::I64(1), Value::U64(1)]]);
}
