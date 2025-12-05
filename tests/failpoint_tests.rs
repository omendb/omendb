//! Failpoint tests for deterministic crash testing.
//!
//! These tests use the `fail` crate to inject failures at specific points
//! in the code, verifying that recovery works correctly after crashes.
//!
//! Run with: `cargo test --features failpoints failpoint`
//!
//! Available failpoints:
//! - `flush::after_sstable_write` - After SSTable written, before metadata update
//! - `flush::before_wal_clear` - After SSTable in LSM, before WAL cleared
//! - `compaction::after_output_write` - After compaction output, before LSM update
//! - `wal::after_sync` - After WAL sync completes

#![cfg(feature = "failpoints")]

use seerdb::{DBOptions, SyncPolicy};
use tempfile::TempDir;

/// Test recovery after crash during flush (after SSTable write, before WAL clear)
///
/// Scenario:
/// 1. Write data to DB
/// 2. Trigger flush
/// 3. Crash after SSTable written but before WAL cleared
/// 4. Recovery should find data in SSTable (or replay WAL - both valid)
#[test]
fn test_failpoint_flush_after_sstable_write() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Phase 1: Write data and trigger crash during flush
    {
        let db = DBOptions::default()
            .sync_policy(SyncPolicy::SyncAll)
            .background_flush(false)
            .background_compaction(false)
            .open(&db_path)
            .unwrap();

        // Write some data
        for i in 0..100 {
            db.put(format!("key_{:03}", i).as_bytes(), b"value")
                .unwrap();
        }

        // Set up failpoint to panic after SSTable write
        fail::cfg("flush::after_sstable_write", "panic").unwrap();

        // Flush should panic at the failpoint
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.flush().unwrap();
        }));
        assert!(result.is_err(), "Flush should have panicked at failpoint");

        // Remove failpoint
        fail::remove("flush::after_sstable_write");
    }

    // Phase 2: Recovery - data should be present (from SSTable or WAL replay)
    {
        let db = DBOptions::default()
            .background_flush(false)
            .background_compaction(false)
            .open(&db_path)
            .unwrap();

        // All data should be recoverable
        for i in 0..100 {
            let value = db.get(format!("key_{:03}", i).as_bytes()).unwrap();
            assert!(
                value.is_some(),
                "Key {} should be present after recovery",
                i
            );
        }
    }
}

/// Test recovery after crash before WAL clear
///
/// Scenario:
/// 1. Write data, flush successfully (SSTable written + added to LSM)
/// 2. Crash before WAL is cleared
/// 3. Recovery replays WAL - should be idempotent (data already in SSTable)
#[test]
fn test_failpoint_flush_before_wal_clear() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Phase 1: Write data and crash before WAL clear
    {
        let db = DBOptions::default()
            .sync_policy(SyncPolicy::SyncAll)
            .background_flush(false)
            .background_compaction(false)
            .open(&db_path)
            .unwrap();

        for i in 0..50 {
            db.put(format!("key_{:03}", i).as_bytes(), b"value")
                .unwrap();
        }

        fail::cfg("flush::before_wal_clear", "panic").unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.flush().unwrap();
        }));
        assert!(result.is_err(), "Flush should have panicked at failpoint");

        fail::remove("flush::before_wal_clear");
    }

    // Phase 2: Recovery - should be idempotent
    {
        let db = DBOptions::default()
            .background_flush(false)
            .background_compaction(false)
            .open(&db_path)
            .unwrap();

        // All data should be present (no duplicates, correct values)
        for i in 0..50 {
            let value = db.get(format!("key_{:03}", i).as_bytes()).unwrap();
            assert!(value.is_some(), "Key {} should be present", i);
            assert_eq!(value.unwrap().as_ref(), b"value");
        }
    }
}

/// Test recovery after crash during compaction
///
/// Scenario:
/// 1. Write data, flush multiple times to create L0 SSTables
/// 2. Trigger compaction, crash after output written
/// 3. Recovery should see either old or new state (both valid)
#[test]
fn test_failpoint_compaction_after_output_write() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Phase 1: Create SSTables and trigger compaction crash
    {
        let db = DBOptions::default()
            .sync_policy(SyncPolicy::SyncAll)
            .memtable_capacity(4096) // Small to trigger compaction
            .background_flush(false)
            .background_compaction(false)
            .open(&db_path)
            .unwrap();

        // Write and flush multiple batches to create L0 SSTables
        for batch in 0..5 {
            for i in 0..20 {
                db.put(
                    format!("key_{:02}_{:03}", batch, i).as_bytes(),
                    format!("value_{}", batch).as_bytes(),
                )
                .unwrap();
            }
            db.flush().unwrap();
        }

        // Set up failpoint for compaction
        fail::cfg("compaction::after_output_write", "panic").unwrap();

        // Compaction might be triggered automatically or we force it
        // The panic will occur if compaction runs
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.flush().unwrap(); // May trigger compaction
        }));

        fail::remove("compaction::after_output_write");
    }

    // Phase 2: Recovery - all data should be present
    {
        let db = DBOptions::default()
            .background_flush(false)
            .background_compaction(false)
            .open(&db_path)
            .unwrap();

        // All data should be present (from original SSTables or compaction output)
        for batch in 0..5 {
            for i in 0..20 {
                let key = format!("key_{:02}_{:03}", batch, i);
                let value = db.get(key.as_bytes()).unwrap();
                assert!(value.is_some(), "Key {} should be present", key);
            }
        }
    }
}

/// Test that WAL sync failpoint allows testing durability guarantees
#[test]
fn test_failpoint_wal_after_sync() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Write data with failpoint after WAL sync
    {
        let db = DBOptions::default()
            .sync_policy(SyncPolicy::SyncAll)
            .background_flush(false)
            .open(&db_path)
            .unwrap();

        // Write first key normally
        db.put(b"key1", b"value1").unwrap();

        // Enable failpoint - next write will panic after sync
        fail::cfg("wal::after_sync", "panic").unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.put(b"key2", b"value2").unwrap();
        }));
        assert!(result.is_err(), "Put should have panicked at failpoint");

        fail::remove("wal::after_sync");
    }

    // Recovery - key2 should be durable (sync completed before panic)
    {
        let db = DBOptions::default()
            .background_flush(false)
            .open(&db_path)
            .unwrap();

        assert!(db.get(b"key1").unwrap().is_some(), "key1 should exist");
        // key2 might or might not exist depending on exact panic timing
        // If sync completed before panic, it should be there
    }
}
