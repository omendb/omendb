// Minimal reproduction test to isolate DB::open() hang

use seerdb::{DBOptions, DB};
use tempfile::TempDir;

#[test]
fn test_minimal_db_open() {
    eprintln!("TEST START");

    let temp_dir = TempDir::new().unwrap();
    eprintln!("TempDir created");

    eprintln!("Calling DB::open...");
    let _db = DBOptions::default()
        .background_flush(false)
        .background_compaction(false)
        .open(temp_dir.path())
        .unwrap();
    eprintln!("DB opened!");

    eprintln!("TEST COMPLETE");
}
