use std::fs;

use omendb::{DatabaseConfig, DbError, RelationalBackendConfig, RelationalDatabase};
use tempfile::tempdir;

#[test]
fn temporary_verify_replays_durable_artifacts_while_handle_is_open() {
    let root = tempdir().expect("temporary directory");
    let path = root.path().join("temporary");
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: path.clone(),
        }))
        .expect("create database");
    database
        .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
        .expect("create table");
    database
        .execute_sql("INSERT INTO items VALUES (1, 'value')")
        .expect("insert row");
    database.checkpoint().expect("checkpoint");

    let page_path = fs::read_dir(&path)
        .expect("read database directory")
        .map(|entry| entry.expect("directory entry").path())
        .find(|candidate| {
            candidate
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("omendb.manifest.data-"))
        })
        .expect("checkpoint page artifact");
    let mut page = fs::read(&page_path).expect("read checkpoint page");
    page[24] ^= 0x01;
    fs::write(&page_path, page).expect("corrupt checkpoint page");

    assert!(matches!(database.verify(), Err(DbError::Corruption { .. })));
    database.close().expect("close database");
}
