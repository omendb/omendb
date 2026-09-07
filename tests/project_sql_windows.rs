use omendb::{RelationalBackendConfig, RelationalDatabase, Value};

fn database() -> (tempfile::TempDir, RelationalDatabase) {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    database
        .execute_sql("CREATE TABLE sales (id BIGINT PRIMARY KEY, amount BIGINT)")
        .unwrap();
    database
        .execute_sql("INSERT INTO sales VALUES (1, 10), (2, 10), (3, 30), (4, NULL)")
        .unwrap();
    (directory, database)
}

#[test]
fn window_partition_is_evaluated_before_limit_and_offset() {
    let (_directory, mut database) = database();
    let result = database
        .execute_sql("SELECT count(*) OVER (), sum(amount) OVER () FROM sales LIMIT 1 OFFSET 1")
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::U64(4), Value::I64(50)]]);
    let result = database
        .execute_sql("SELECT lead(amount) OVER (ORDER BY id) FROM sales LIMIT 1")
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::I64(10)]]);
}

#[test]
fn default_window_frame_includes_all_ordering_peers() {
    let (_directory, mut database) = database();
    let result = database.execute_sql("SELECT sum(amount) OVER (ORDER BY amount), count(*) OVER (ORDER BY amount), last_value(amount) OVER (ORDER BY amount) FROM sales ORDER BY id").unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![Value::I64(20), Value::U64(2), Value::I64(10)],
            vec![Value::I64(20), Value::U64(2), Value::I64(10)],
            vec![Value::I64(50), Value::U64(3), Value::I64(30)],
            vec![Value::I64(50), Value::U64(4), Value::Null],
        ]
    );
}
