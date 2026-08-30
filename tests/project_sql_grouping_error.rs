use omendb::{DbError, RelationalBackendConfig, RelationalDatabase};
use tempfile::tempdir;

#[test]
fn single_table_grouping_error_is_typed() {
    let directory = tempdir().expect("tempdir");
    let mut database = RelationalDatabase::create(RelationalBackendConfig::new(
        directory.path().to_owned(),
    ))
    .expect("create database");

    database
        .execute_sql(
            "CREATE TABLE accounts (id BIGINT PRIMARY KEY, state TEXT NOT NULL)",
        )
        .expect("create table");

    let error = database
        .execute_sql("SELECT state, COUNT(*) FROM accounts")
        .expect_err("plain projected column must require GROUP BY");
    match error {
        DbError::SqlGroupingError { column } => assert_eq!(column, "state"),
        other => panic!("expected grouping error, got {other:?}"),
    }

    database.close().expect("close database");
}
