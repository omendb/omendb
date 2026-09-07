use omendb::{DbError, RelationalBackendConfig, RelationalDatabase};

#[test]
fn parameter_description_rejects_positions_outside_protocol_bound() {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .unwrap();
    database
        .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .unwrap();
    for position in ["0", "65536", "999999999999999999999999999999999999999"] {
        let result =
            database.sql_parameter_types(&format!("SELECT id FROM items WHERE id = ${position}"));
        assert!(
            matches!(result, Err(DbError::SqlParameter(_))),
            "position {position} must be rejected before allocating a parameter vector"
        );
    }
    assert_eq!(
        database
            .sql_parameter_types("SELECT id FROM items WHERE id = $65535")
            .unwrap()
            .len(),
        65535
    );
}
