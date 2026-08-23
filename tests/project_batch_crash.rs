//! Process-termination durability for the bounded SQL batch boundary.
//!
//! The batch is one transaction: after `execute_sql_batch` returns, every
//! staged statement must survive process death together, and a failed batch
//! must leave no partial state behind. This complements the SeerDB fault
//! matrix by covering the SQL adapter's coalesced publication shape through
//! real process exit rather than injected faults.

use std::process::Command;

use omendb::{DatabaseConfig, RelationalBackendConfig, RelationalDatabase};
use tempfile::tempdir;

const CRASH_ENV: &str = "OMENDB_BATCH_CRASH_PATH";
const STATEMENT_COUNT: usize = 64;

fn config(directory: &std::path::Path) -> RelationalBackendConfig {
    RelationalBackendConfig::Temporary(DatabaseConfig {
        directory: directory.to_owned(),
    })
}

#[test]
fn sql_batch_survives_process_termination_atomically() {
    if let Some(path) = std::env::var_os(CRASH_ENV) {
        let mut database = RelationalDatabase::create(config(path.as_ref()))
            .expect("create batch crash child database");
        database
            .execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT NOT NULL)")
            .expect("create batch crash table");
        let statements: Vec<String> = (0..STATEMENT_COUNT)
            .map(|index| format!("INSERT INTO items VALUES ({}, 'batch')", index + 1))
            .collect();
        let references: Vec<&str> = statements.iter().map(String::as_str).collect();
        database
            .execute_sql_batch(&references)
            .expect("commit batch crash child");
        // Terminate hard right after the acknowledged batch: recovery must
        // show the complete batch, never a prefix.
        std::process::exit(137);
    }

    let root = tempdir().expect("temporary directory");
    let path = root.path().join("batch-crash.db");
    let status = Command::new(std::env::current_exe().expect("test executable"))
        .arg("--exact")
        .arg("sql_batch_survives_process_termination_atomically")
        .arg("--nocapture")
        .env(CRASH_ENV, &path)
        .status()
        .expect("run batch crash child");
    assert!(!status.success(), "batch crash child exited cleanly");

    let mut recovered =
        RelationalDatabase::open(config(&path)).expect("reopen after batch process termination");
    let rows = recovered
        .execute_sql("SELECT id FROM items ORDER BY id")
        .expect("read recovered batch rows")
        .rows;
    assert_eq!(
        rows.len(),
        STATEMENT_COUNT,
        "recovery exposed a partial batch"
    );
    for (position, row) in rows.iter().enumerate() {
        assert_eq!(row[0], omendb::Value::I64(position as i64 + 1));
    }
    let report = recovered.verify().expect("recovered database verifies");
    assert_eq!(report.verified_rows, STATEMENT_COUNT as u64);
    recovered.close().expect("close recovered database");
}
