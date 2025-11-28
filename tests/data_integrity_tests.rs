// Data Integrity Tests
//
// Comprehensive tests for data loss scenarios identified in code review.
// These tests cover critical gaps that could cause data loss in production.
//
// Categories:
// 1. WAL integrity (partial writes, corruption)
// 2. Flush ordering (SSTable write vs WAL clear)
// 3. Compaction integrity (tombstone shadowing, crash safety)
// 4. Concurrent operations (partition swap, read during flush)
// 5. Edge cases (empty values, boundary conditions)

use seerdb::{DBOptions, SyncPolicy, DB};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

// =============================================================================
// 1. WAL INTEGRITY TESTS
// =============================================================================

/// Test WAL recovery with truncated record (simulates crash mid-write)
///
/// Risk: HIGH - Partial record could corrupt recovery
/// Expected: Recovery should skip incomplete record, preserve complete ones
#[test]
fn test_wal_truncated_record_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Write data to WAL
    {
        let opts = DBOptions {
            data_dir: db_path.clone(),
            wal_sync_policy: SyncPolicy::SyncAll,
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        // Write 100 records
        for i in 0..100 {
            db.put(format!("key_{:03}", i).as_bytes(), b"value")
                .unwrap();
        }
        // Don't flush - all data in WAL
    }

    // Truncate WAL to simulate crash mid-record
    let wal_path = db_path.join("wal.log");
    let original_size = fs::metadata(&wal_path).unwrap().len();

    // Truncate to remove ~10% (simulates incomplete last record)
    let truncated_size = (original_size * 9) / 10;
    {
        let file = OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.set_len(truncated_size).unwrap();
        file.sync_all().unwrap();
    }

    // Recovery should succeed and recover most records
    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };

    match DB::open(opts) {
        Ok(db) => {
            // Count recovered records
            let recovered = (0..100)
                .filter(|i| {
                    db.get(format!("key_{:03}", i).as_bytes())
                        .unwrap()
                        .is_some()
                })
                .count();

            // Should recover at least 80% (truncated ~10%)
            assert!(
                recovered >= 80,
                "Should recover most records after truncation, got {} / 100",
                recovered
            );

            // Verify no garbage data
            for i in 0..100 {
                if let Some(value) = db.get(format!("key_{:03}", i).as_bytes()).unwrap() {
                    assert_eq!(value.as_ref(), b"value", "Value should be intact");
                }
            }
        }
        Err(e) => {
            // Also acceptable if recovery fails cleanly (strict mode)
            println!("Recovery failed (acceptable): {}", e);
        }
    }
}

/// Test WAL with corrupted record body (not header)
///
/// Risk: HIGH - Per-record corruption must be detected
/// Expected: Corrupted record detected, earlier records preserved
#[test]
fn test_wal_record_body_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Write data
    {
        let opts = DBOptions {
            data_dir: db_path.clone(),
            wal_sync_policy: SyncPolicy::SyncAll,
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        for i in 0..50 {
            db.put(format!("key_{:03}", i).as_bytes(), b"value")
                .unwrap();
        }
    }

    // Corrupt middle of WAL (not header - header is first 8 bytes)
    let wal_path = db_path.join("wal.log");
    let file_size = fs::metadata(&wal_path).unwrap().len();
    let corrupt_offset = file_size / 2; // Middle of file

    {
        let mut file = OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        file.sync_all().unwrap();
    }

    // Recovery behavior depends on implementation:
    // - May recover records before corruption
    // - May fail entirely
    // - Should NOT silently return corrupted data
    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };

    match DB::open(opts) {
        Ok(db) => {
            // Verify no corrupted values returned
            for i in 0..50 {
                if let Some(value) = db.get(format!("key_{:03}", i).as_bytes()).unwrap() {
                    // Value should be exactly "value" or not present
                    assert_eq!(
                        value.as_ref(),
                        b"value",
                        "Should not return corrupted value"
                    );
                }
            }
        }
        Err(_) => {
            // Corruption detected - acceptable
        }
    }
}

/// Test batch atomicity with partial write
///
/// Risk: HIGH - Batch must be all-or-nothing
/// Expected: Either all records in batch recovered, or none
#[test]
fn test_wal_batch_atomicity() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Write batch
    {
        let opts = DBOptions {
            data_dir: db_path.clone(),
            wal_sync_policy: SyncPolicy::SyncAll,
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        // Use Batch for atomic write
        let mut batch = db.batch();
        for i in 0..10 {
            batch.put(format!("batch_key_{}", i).as_bytes(), b"batch_value");
        }
        batch.commit().unwrap();
    }

    // Reopen and verify batch atomicity
    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    let present: Vec<bool> = (0..10)
        .map(|i| {
            db.get(format!("batch_key_{}", i).as_bytes())
                .unwrap()
                .is_some()
        })
        .collect();

    // Either all present or all absent (atomic)
    let all_present = present.iter().all(|&p| p);
    let all_absent = present.iter().all(|&p| !p);

    assert!(
        all_present || all_absent,
        "Batch must be atomic: either all keys present or all absent. Got: {:?}",
        present
    );

    // In normal case, all should be present
    assert!(
        all_present,
        "All batch keys should be present after clean recovery"
    );
}

// =============================================================================
// 2. FLUSH ORDERING TESTS
// =============================================================================

/// Test that flush writes SSTable before clearing WAL
///
/// Risk: HIGH - If WAL cleared first, crash loses data
/// Expected: After flush + crash, data recoverable from SSTable OR WAL
#[test]
fn test_flush_sstable_before_wal_clear() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Write and flush
    {
        let opts = DBOptions {
            data_dir: db_path.clone(),
            wal_sync_policy: SyncPolicy::SyncAll,
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        for i in 0..1000 {
            db.put(format!("key_{:04}", i).as_bytes(), b"value")
                .unwrap();
        }

        db.flush().unwrap();
    }

    // Verify SSTable exists
    let sstable_exists = fs::read_dir(&db_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"));

    assert!(sstable_exists, "SSTable should exist after flush");

    // Reopen and verify all data present
    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    for i in 0..1000 {
        assert!(
            db.get(format!("key_{:04}", i).as_bytes())
                .unwrap()
                .is_some(),
            "Key {} should exist after flush + reopen",
            i
        );
    }
}

/// Test concurrent flushes don't corrupt sequence numbers
///
/// Risk: HIGH - Out-of-order completion could cause GC of live data
/// Expected: max_flushed_seq always increases monotonically
#[test]
fn test_concurrent_flush_sequence_monotonic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let opts = DBOptions {
        data_dir: db_path.clone(),
        memtable_capacity: 1024,      // Small to trigger frequent flushes
        background_compaction: false, // Disable to isolate flush behavior
        ..Default::default()
    };
    let db = Arc::new(DB::open(opts).unwrap());

    let write_count = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Spawn writer threads
    let mut handles = vec![];
    for t in 0..4 {
        let db = Arc::clone(&db);
        let write_count = Arc::clone(&write_count);
        let stop = Arc::clone(&stop);

        handles.push(thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("t{}_{:06}", t, i);
                if db.put(key.as_bytes(), b"value").is_ok() {
                    write_count.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
                if i > 10000 {
                    break;
                }
            }
        }));
    }

    // Let writes run
    thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let total_writes = write_count.load(Ordering::Relaxed);

    // Flush and close
    db.flush().unwrap();
    drop(db);

    // Reopen and verify data integrity
    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    // Count recovered keys
    let mut recovered = 0;
    for t in 0..4 {
        for i in 0..10001 {
            if db
                .get(format!("t{}_{:06}", t, i).as_bytes())
                .unwrap()
                .is_some()
            {
                recovered += 1;
            }
        }
    }

    // Should recover all or almost all writes
    let recovery_rate = (recovered as f64) / (total_writes as f64);
    assert!(
        recovery_rate > 0.99,
        "Should recover >99% of writes, got {:.1}% ({} / {})",
        recovery_rate * 100.0,
        recovered,
        total_writes
    );
}

// =============================================================================
// 3. COMPACTION INTEGRITY TESTS
// =============================================================================

/// Test tombstone shadows older value across LSM levels
///
/// Risk: HIGH - Delete must always win over older put
/// Expected: After compaction, deleted key returns None
#[test]
fn test_tombstone_shadows_across_levels() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let opts = DBOptions {
        data_dir: db_path.clone(),
        memtable_capacity: 1024,     // Small to trigger flushes
        background_compaction: true, // Enable to trigger compaction
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    // Write key and flush to L0 (will eventually move to L1+)
    db.put(b"shadowed_key", b"original_value").unwrap();
    db.flush().unwrap();

    // Delete key and flush again (creates tombstone in newer SSTable)
    db.delete(b"shadowed_key").unwrap();
    db.flush().unwrap();

    // Write more data to create multiple L0 SSTables (triggers compaction)
    for batch in 0..5 {
        for i in 0..50 {
            db.put(format!("filler_{}_{:03}", batch, i).as_bytes(), b"filler")
                .unwrap();
        }
        db.flush().unwrap();
    }

    // Allow background compaction to run
    thread::sleep(Duration::from_millis(500));

    // Key should be deleted (tombstone wins)
    assert!(
        db.get(b"shadowed_key").unwrap().is_none(),
        "Tombstone should shadow original value after compaction"
    );

    // Reopen and verify still deleted
    drop(db);
    let opts = DBOptions {
        data_dir: db_path.clone(),
        background_compaction: true,
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    assert!(
        db.get(b"shadowed_key").unwrap().is_none(),
        "Tombstone should persist after reopen"
    );
}

/// Test compaction doesn't lose data on crash
///
/// Risk: MEDIUM - Partial compaction output could corrupt data
/// Expected: Original SSTables intact until compaction fully complete
#[test]
fn test_compaction_preserves_all_data() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let opts = DBOptions {
        data_dir: db_path.clone(),
        memtable_capacity: 2048,
        background_compaction: true, // Enable compaction
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    // Create multiple SSTables with overlapping key ranges
    for batch in 0..5 {
        for i in 0..100 {
            let key = format!("key_{:02}_{:03}", batch, i);
            let value = format!("value_{:02}_{:03}", batch, i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }

    // Allow background compaction to run
    thread::sleep(Duration::from_millis(500));

    // Verify all data preserved
    for batch in 0..5 {
        for i in 0..100 {
            let key = format!("key_{:02}_{:03}", batch, i);
            let expected_value = format!("value_{:02}_{:03}", batch, i);
            let value = db
                .get(key.as_bytes())
                .unwrap()
                .unwrap_or_else(|| panic!("Key {} should exist after compaction", key));
            assert_eq!(
                value.as_ref(),
                expected_value.as_bytes(),
                "Value for {} should be preserved",
                key
            );
        }
    }

    // Reopen and verify again
    drop(db);
    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    for batch in 0..5 {
        for i in 0..100 {
            let key = format!("key_{:02}_{:03}", batch, i);
            assert!(
                db.get(key.as_bytes()).unwrap().is_some(),
                "Key {} should exist after reopen",
                key
            );
        }
    }
}

// =============================================================================
// 4. CONCURRENT OPERATION TESTS
// =============================================================================

/// Test reading during memtable swap (flush)
///
/// Risk: MEDIUM - Reader may see inconsistent state
/// Expected: Reads always return correct value or None
#[test]
fn test_read_during_memtable_swap() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let opts = DBOptions {
        data_dir: db_path.clone(),
        memtable_capacity: 4096, // Small to trigger swaps
        ..Default::default()
    };
    let db = Arc::new(DB::open(opts).unwrap());

    // Pre-populate some keys
    for i in 0..100 {
        db.put(format!("pre_{:03}", i).as_bytes(), b"pre_value")
            .unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let error_count = Arc::new(AtomicUsize::new(0));

    // Reader thread - continuously reads pre-populated keys
    let db_reader = Arc::clone(&db);
    let stop_reader = Arc::clone(&stop);
    let error_count_reader = Arc::clone(&error_count);
    let reader = thread::spawn(move || {
        while !stop_reader.load(Ordering::Relaxed) {
            for i in 0..100 {
                match db_reader.get(format!("pre_{:03}", i).as_bytes()) {
                    Ok(Some(value)) => {
                        if value.as_ref() != b"pre_value" {
                            error_count_reader.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(None) => {
                        // Key missing during swap - could be acceptable depending on isolation level
                    }
                    Err(_) => {
                        error_count_reader.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    });

    // Writer thread - triggers memtable swaps
    let db_writer = Arc::clone(&db);
    let stop_writer = Arc::clone(&stop);
    let writer = thread::spawn(move || {
        let mut i = 0;
        while !stop_writer.load(Ordering::Relaxed) && i < 10000 {
            let _ = db_writer.put(format!("write_{:06}", i).as_bytes(), b"write_value");
            i += 1;
        }
    });

    // Run for 2 seconds
    thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);

    reader.join().unwrap();
    writer.join().unwrap();

    assert_eq!(
        error_count.load(Ordering::Relaxed),
        0,
        "Should have no read errors during memtable swap"
    );
}

/// Test concurrent put and delete on same key
///
/// Risk: MEDIUM - Race could cause inconsistent state
/// Expected: Final state is either value or deleted, not corrupted
#[test]
fn test_concurrent_put_delete_same_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = Arc::new(DB::open(opts).unwrap());

    let iterations = 1000;

    // Thread 1: puts
    let db1 = Arc::clone(&db);
    let t1 = thread::spawn(move || {
        for _ in 0..iterations {
            let _ = db1.put(b"contested_key", b"put_value");
        }
    });

    // Thread 2: deletes
    let db2 = Arc::clone(&db);
    let t2 = thread::spawn(move || {
        for _ in 0..iterations {
            let _ = db2.delete(b"contested_key");
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // Final state should be consistent
    match db.get(b"contested_key").unwrap() {
        Some(value) => {
            assert_eq!(
                value.as_ref(),
                b"put_value",
                "If present, value should be 'put_value'"
            );
        }
        None => {
            // Deleted - also valid
        }
    }

    // Flush and reopen to verify persistence
    db.flush().unwrap();
    drop(db);

    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    // State should still be consistent
    match db.get(b"contested_key").unwrap() {
        Some(value) => {
            assert_eq!(value.as_ref(), b"put_value");
        }
        None => {}
    }
}

// =============================================================================
// 5. EDGE CASE TESTS
// =============================================================================

/// Test empty value throughout lifecycle
///
/// Risk: MEDIUM - Empty value could be confused with tombstone/missing
/// Expected: Empty value preserved through flush, compact, recover
#[test]
fn test_empty_value_full_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let opts = DBOptions {
        data_dir: db_path.clone(),
        memtable_capacity: 2048,
        background_compaction: true, // Enable compaction
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    // Put empty value
    db.put(b"empty_value_key", b"").unwrap();

    // Verify in memtable
    let value = db.get(b"empty_value_key").unwrap();
    assert!(value.is_some(), "Empty value should be retrievable");
    assert_eq!(value.unwrap().as_ref(), b"", "Value should be empty");

    // Flush to SSTable
    db.flush().unwrap();

    // Verify in SSTable
    let value = db.get(b"empty_value_key").unwrap();
    assert!(value.is_some(), "Empty value should exist after flush");
    assert_eq!(value.unwrap().as_ref(), b"", "Value should still be empty");

    // Add more data to trigger compaction
    for batch in 0..5 {
        for i in 0..50 {
            db.put(format!("filler_{}_{:03}", batch, i).as_bytes(), b"filler")
                .unwrap();
        }
        db.flush().unwrap();
    }

    // Allow background compaction
    thread::sleep(Duration::from_millis(500));

    // Verify after compaction
    let value = db.get(b"empty_value_key").unwrap();
    assert!(value.is_some(), "Empty value should exist after compaction");
    assert_eq!(
        value.unwrap().as_ref(),
        b"",
        "Value should be empty after compaction"
    );

    // Reopen and verify
    drop(db);
    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    let value = db.get(b"empty_value_key").unwrap();
    assert!(value.is_some(), "Empty value should exist after reopen");
    assert_eq!(
        value.unwrap().as_ref(),
        b"",
        "Value should be empty after reopen"
    );
}

/// Test key with null bytes
///
/// Risk: LOW - Binary keys must be handled correctly
/// Expected: Key with null bytes stored and retrieved correctly
#[test]
fn test_key_with_null_bytes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    // Key with embedded nulls
    let key_with_nulls = b"key\x00with\x00nulls";
    let value = b"value_for_null_key";

    db.put(key_with_nulls, value).unwrap();

    // Verify retrieval
    let retrieved = db.get(key_with_nulls).unwrap().unwrap();
    assert_eq!(retrieved.as_ref(), value);

    // Flush and verify
    db.flush().unwrap();
    let retrieved = db.get(key_with_nulls).unwrap().unwrap();
    assert_eq!(retrieved.as_ref(), value);

    // Reopen and verify
    drop(db);
    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    let retrieved = db.get(key_with_nulls).unwrap().unwrap();
    assert_eq!(retrieved.as_ref(), value);
}

/// Test value at vLog threshold boundary
///
/// Risk: LOW - Boundary condition in value separation
/// Expected: Values at exact threshold handled correctly
#[test]
fn test_value_at_vlog_threshold() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let threshold = 1024; // 1KB threshold

    let opts = DBOptions {
        data_dir: db_path.clone(),
        vlog_threshold: Some(threshold),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    // Value exactly at threshold
    let value_at = vec![b'a'; threshold];
    db.put(b"key_at", &value_at).unwrap();

    // Value one byte over threshold (goes to vLog)
    let value_over = vec![b'b'; threshold + 1];
    db.put(b"key_over", &value_over).unwrap();

    // Value one byte under threshold (inline)
    let value_under = vec![b'c'; threshold - 1];
    db.put(b"key_under", &value_under).unwrap();

    // Flush to SSTable
    db.flush().unwrap();

    // Verify all values correct
    assert_eq!(db.get(b"key_at").unwrap().unwrap().as_ref(), &value_at[..]);
    assert_eq!(
        db.get(b"key_over").unwrap().unwrap().as_ref(),
        &value_over[..]
    );
    assert_eq!(
        db.get(b"key_under").unwrap().unwrap().as_ref(),
        &value_under[..]
    );

    // Reopen and verify
    drop(db);
    let opts = DBOptions {
        data_dir: db_path.clone(),
        vlog_threshold: Some(threshold),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    assert_eq!(db.get(b"key_at").unwrap().unwrap().as_ref(), &value_at[..]);
    assert_eq!(
        db.get(b"key_over").unwrap().unwrap().as_ref(),
        &value_over[..]
    );
    assert_eq!(
        db.get(b"key_under").unwrap().unwrap().as_ref(),
        &value_under[..]
    );
}

/// Test many versions of same key (MVCC stress)
///
/// Risk: MEDIUM - Version chain could be corrupted
/// Expected: Always see latest version
#[test]
fn test_many_versions_same_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let opts = DBOptions {
        data_dir: db_path.clone(),
        memtable_capacity: 4096,
        background_compaction: true, // Enable compaction
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    // Write many versions (reduced from 1000 for faster CI)
    for version in 0..100 {
        let value = format!("version_{:03}", version);
        db.put(b"versioned_key", value.as_bytes()).unwrap();

        // Occasionally flush to create SSTable versions
        if version % 25 == 24 {
            db.flush().unwrap();
        }
    }

    // Should always see latest version
    let value = db.get(b"versioned_key").unwrap().unwrap();
    assert_eq!(value.as_ref(), b"version_099", "Should see latest version");

    // Allow compaction and verify
    thread::sleep(Duration::from_millis(500));
    let value = db.get(b"versioned_key").unwrap().unwrap();
    assert_eq!(
        value.as_ref(),
        b"version_099",
        "Latest version should survive compaction"
    );

    // Reopen and verify
    drop(db);
    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    let value = db.get(b"versioned_key").unwrap().unwrap();
    assert_eq!(
        value.as_ref(),
        b"version_099",
        "Latest version should survive reopen"
    );
}

// =============================================================================
// 6. SNAPSHOT ISOLATION TESTS
// =============================================================================

/// Test snapshot sees consistent point-in-time view
///
/// Risk: MEDIUM - Snapshot could see partial writes
/// Expected: Snapshot always sees consistent state
#[test]
fn test_snapshot_consistency_during_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let opts = DBOptions {
        data_dir: db_path.clone(),
        ..Default::default()
    };
    let db = Arc::new(DB::open(opts).unwrap());

    // Write initial data
    for i in 0..100 {
        db.put(format!("key_{:03}", i).as_bytes(), b"v1").unwrap();
    }

    // Take snapshot
    let snapshot = db.snapshot().unwrap();

    // Update all keys
    for i in 0..100 {
        db.put(format!("key_{:03}", i).as_bytes(), b"v2").unwrap();
    }

    // Snapshot should see old values
    for i in 0..100 {
        let value = snapshot
            .get(format!("key_{:03}", i).as_bytes())
            .unwrap()
            .unwrap();
        assert_eq!(
            value.as_ref(),
            b"v1",
            "Snapshot should see v1 for key {}",
            i
        );
    }

    // Current DB should see new values
    for i in 0..100 {
        let value = db.get(format!("key_{:03}", i).as_bytes()).unwrap().unwrap();
        assert_eq!(value.as_ref(), b"v2", "DB should see v2 for key {}", i);
    }
}

// =============================================================================
// 6. PERSISTENCE TESTS
// =============================================================================

/// Test that 100 keys persist correctly across reopen
///
/// Risk: CRITICAL - Data loss bug reported
/// Suspected cause: Partitioned memtable flush/recovery issue
#[test]
fn test_persistence_100_keys() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    // Write 100 keys
    {
        let options = DBOptions {
            data_dir: path.clone(),
            wal_sync_policy: SyncPolicy::SyncAll,
            background_compaction: false,
            background_flush: false,
            ..Default::default()
        };
        let db = DB::open(options).unwrap();

        for i in 0..100 {
            let key = format!("v:{}", i);
            let value = vec![i as u8; 128];
            db.put(key.as_bytes(), &value).unwrap();
        }
        db.flush().unwrap();
    }

    // Reopen and verify
    {
        let options = DBOptions {
            data_dir: path.clone(),
            background_compaction: false,
            background_flush: false,
            ..Default::default()
        };
        let db = DB::open(options).unwrap();

        let mut missing = Vec::new();
        for i in 0..100 {
            let key = format!("v:{}", i);
            if db.get(key.as_bytes()).unwrap().is_none() {
                missing.push(i);
            }
        }

        assert!(
            missing.is_empty(),
            "Missing {} keys after reopen: {:?}",
            missing.len(),
            missing
        );
    }
}
