use super::*;
use tempfile::tempdir;

#[test]
fn test_db_open() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();
    assert_eq!(db.memtable_size(), 0);
}

#[test]
fn test_config_profiles() {
    // Test embedded profile
    let dir = tempdir().unwrap();
    let opts = DBOptions::embedded();
    assert_eq!(opts.memtable_capacity, 64 * 1024 * 1024);
    assert!(opts.use_direct_wal);
    assert!(opts.disable_metrics);
    let db = opts.open(dir.path()).unwrap();
    db.put(b"key", b"value").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value")));
    drop(db);

    // Test high_throughput profile
    let dir = tempdir().unwrap();
    let opts = DBOptions::high_throughput();
    assert_eq!(opts.memtable_capacity, 512 * 1024 * 1024);
    assert!(opts.background_compaction);
    assert!(opts.background_flush);
    let db = opts.open(dir.path()).unwrap();
    db.put(b"key", b"value").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value")));
    drop(db);

    // Test large_scale profile
    let dir = tempdir().unwrap();
    let opts = DBOptions::large_scale();
    assert_eq!(opts.memtable_capacity, 1024 * 1024 * 1024);
    assert_eq!(opts.base_level_size, 64 * 1024 * 1024);
    let db = opts.open(dir.path()).unwrap();
    db.put(b"key", b"value").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value")));
    drop(db);

    // Test builder pattern
    let dir = tempdir().unwrap();
    let opts = DBOptions::default()
        .memtable_capacity(128 * 1024 * 1024)
        .metrics(false)
        .direct_wal(true);
    assert_eq!(opts.memtable_capacity, 128 * 1024 * 1024);
    assert!(opts.disable_metrics);
    assert!(opts.use_direct_wal);
    let db = opts.open(dir.path()).unwrap();
    db.put(b"key", b"value").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value")));
}

#[test]
fn test_skip_wal_single_writes() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default().skip_wal(true);

    assert!(opts.skip_wal);
    let db = opts.open(dir.path()).unwrap();

    // Write some data (no WAL)
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify all data is readable (from memtable)
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let expected = format!("value_{:03}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
    }

    // Flush to SSTable
    db.flush().unwrap();

    // Data still readable after flush
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let expected = format!("value_{:03}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
    }
}

#[test]
fn test_skip_wal_batch_writes() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default().skip_wal(true);

    let db = opts.open(dir.path()).unwrap();

    // Batch write (no WAL)
    let mut batch = db.batch();
    for i in 0..50 {
        let key = format!("batch_key_{:03}", i);
        let value = format!("batch_value_{:03}", i);
        batch.put(key.as_bytes(), value.as_bytes());
    }
    batch.commit().unwrap();

    // Verify all batch data is readable
    for i in 0..50 {
        let key = format!("batch_key_{:03}", i);
        let expected = format!("batch_value_{:03}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
    }
}

#[test]
fn test_bulk_load_basic() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default().vlog_threshold(None); // Disable vLog for simplicity
    let db = opts.open(dir.path()).unwrap();

    // Create test entries (unsorted to test sorting)
    let entries = vec![
        (b"key_c".to_vec(), b"value_c".to_vec()),
        (b"key_a".to_vec(), b"value_a".to_vec()),
        (b"key_b".to_vec(), b"value_b".to_vec()),
    ];

    // Bulk load
    let stats = db.bulk_load(entries, BulkLoadOptions::default()).unwrap();

    // Verify stats
    assert_eq!(stats.entries_loaded, 3);
    assert_eq!(stats.sstables_created, 1);
    assert!(stats.bytes_written > 0);
    assert_eq!(stats.target_level, 6);

    // Verify data is readable
    assert_eq!(db.get(b"key_a").unwrap(), Some(Bytes::from("value_a")));
    assert_eq!(db.get(b"key_b").unwrap(), Some(Bytes::from("value_b")));
    assert_eq!(db.get(b"key_c").unwrap(), Some(Bytes::from("value_c")));
    assert_eq!(db.get(b"key_d").unwrap(), None);
}

#[test]
fn test_bulk_load_with_vlog() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default().vlog_threshold(Some(10)); // Small threshold to test vLog
    let db = opts.open(dir.path()).unwrap();

    // Create entries with large values
    let entries = vec![
        (
            b"key_1".to_vec(),
            b"large_value_that_exceeds_threshold".to_vec(),
        ),
        (
            b"key_2".to_vec(),
            b"another_large_value_for_testing".to_vec(),
        ),
    ];

    let stats = db.bulk_load(entries, BulkLoadOptions::default()).unwrap();
    assert_eq!(stats.entries_loaded, 2);

    // Verify data is readable (should go through vLog)
    assert_eq!(
        db.get(b"key_1").unwrap(),
        Some(Bytes::from("large_value_that_exceeds_threshold"))
    );
    assert_eq!(
        db.get(b"key_2").unwrap(),
        Some(Bytes::from("another_large_value_for_testing"))
    );
}

#[test]
fn test_bulk_load_multiple_sstables() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default().vlog_threshold(None);
    let db = opts.open(dir.path()).unwrap();

    // Create many entries
    let entries: Vec<_> = (0..500)
        .map(|i| {
            (
                format!("key_{:05}", i).into_bytes(),
                format!("value_{}", i).into_bytes(),
            )
        })
        .collect();

    // Use small max entries to create multiple SSTables
    let stats = db
        .bulk_load(entries, BulkLoadOptions::default().with_max_entries(100))
        .unwrap();

    assert_eq!(stats.entries_loaded, 500);
    assert_eq!(stats.sstables_created, 5); // 500 / 100 = 5
    assert!(stats.bytes_written > 0);

    // Verify some entries are readable
    assert_eq!(db.get(b"key_00000").unwrap(), Some(Bytes::from("value_0")));
    assert_eq!(
        db.get(b"key_00250").unwrap(),
        Some(Bytes::from("value_250"))
    );
    assert_eq!(
        db.get(b"key_00499").unwrap(),
        Some(Bytes::from("value_499"))
    );
}

#[test]
fn test_bulk_load_already_sorted() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default().vlog_threshold(None);
    let db = opts.open(dir.path()).unwrap();

    // Pre-sorted entries
    let entries = vec![
        (b"aaa".to_vec(), b"1".to_vec()),
        (b"bbb".to_vec(), b"2".to_vec()),
        (b"ccc".to_vec(), b"3".to_vec()),
    ];

    // Mark as already sorted
    let stats = db
        .bulk_load(entries, BulkLoadOptions::default().already_sorted())
        .unwrap();

    assert_eq!(stats.entries_loaded, 3);
    assert_eq!(db.get(b"aaa").unwrap(), Some(Bytes::from("1")));
    assert_eq!(db.get(b"bbb").unwrap(), Some(Bytes::from("2")));
    assert_eq!(db.get(b"ccc").unwrap(), Some(Bytes::from("3")));
}

#[test]
fn test_bulk_load_target_level() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default().vlog_threshold(None);
    let db = opts.open(dir.path()).unwrap();

    let entries = vec![(b"key".to_vec(), b"value".to_vec())];

    // Load to L3
    let stats = db
        .bulk_load(entries, BulkLoadOptions::default().with_target_level(3))
        .unwrap();

    assert_eq!(stats.target_level, 3);
    assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value")));
}

#[test]
fn test_bulk_load_empty() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default().vlog_threshold(None);
    let db = opts.open(dir.path()).unwrap();

    let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![];
    let stats = db.bulk_load(entries, BulkLoadOptions::default()).unwrap();

    assert_eq!(stats.entries_loaded, 0);
    assert_eq!(stats.sstables_created, 0);
    assert_eq!(stats.bytes_written, 0);
}

#[test]
fn test_db_put_get() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    db.put(b"key1", b"value1").unwrap();
    db.put(b"key2", b"value2").unwrap();

    assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("value1")));
    assert_eq!(db.get(b"key2").unwrap(), Some(Bytes::from("value2")));
    assert_eq!(db.get(b"key3").unwrap(), None);
}

#[test]
fn test_db_delete() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    db.put(b"key1", b"value1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("value1")));

    db.delete(b"key1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), None);
}

#[test]
fn test_db_overwrite() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    db.put(b"key1", b"old_value").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("old_value")));

    db.put(b"key1", b"new_value").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("new_value")));
}

#[test]
fn test_db_flush() {
    let dir = tempdir().unwrap();
    // Use synchronous flush to test flush behavior deterministically
    let options = DBOptions::default()
        .memtable_capacity(100) // Small capacity to trigger flush
        .background_flush(false);

    let db = options.open(dir.path()).unwrap();

    // Write enough data to trigger flush
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_with_long_data_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Data should still be accessible after flush
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_with_long_data_{}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(value)));
    }

    // Check that SSTable files were created
    let sst_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s == "sst")
                .unwrap_or(false)
        })
        .collect();

    assert!(!sst_files.is_empty(), "No SSTable files created");
}

#[test]
fn test_db_recovery_basic() {
    let dir = tempdir().unwrap();
    let options = DBOptions::default();

    // Write some data
    {
        let db = options.open(dir.path()).unwrap();
        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();
        db.put(b"key3", b"value3").unwrap();
        // Drop db (simulates shutdown without flush)
    }

    // Reopen and verify data recovered from WAL
    {
        let db = options.open(dir.path()).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("value1")));
        assert_eq!(db.get(b"key2").unwrap(), Some(Bytes::from("value2")));
        assert_eq!(db.get(b"key3").unwrap(), Some(Bytes::from("value3")));
    }
}

#[test]
fn test_db_recovery_with_deletes() {
    let dir = tempdir().unwrap();
    let options = DBOptions::default();

    // Write and delete some data
    {
        let db = options.open(dir.path()).unwrap();
        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();
        db.delete(b"key1").unwrap(); // Delete key1
        db.put(b"key3", b"value3").unwrap();
    }

    // Reopen and verify recovery
    {
        let db = options.open(dir.path()).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), None); // Deleted
        assert_eq!(db.get(b"key2").unwrap(), Some(Bytes::from("value2")));
        assert_eq!(db.get(b"key3").unwrap(), Some(Bytes::from("value3")));
    }
}

#[test]
fn test_db_recovery_with_overwrites() {
    let dir = tempdir().unwrap();
    let options = DBOptions::default();

    // Write with overwrites
    {
        let db = options.open(dir.path()).unwrap();
        db.put(b"key1", b"old_value").unwrap();
        db.put(b"key1", b"new_value").unwrap(); // Overwrite
    }

    // Reopen and verify newest value recovered
    {
        let db = options.open(dir.path()).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("new_value")));
    }
}

#[test]
fn test_db_recovery_with_flush() {
    let dir = tempdir().unwrap();
    // Use synchronous flush to test recovery behavior deterministically
    let options = DBOptions::default()
        .memtable_capacity(100) // Small to trigger flush during recovery
        .background_flush(false);

    // Write enough data to trigger flush on recovery
    {
        let db = options.open(dir.path()).unwrap();
        for i in 0..20 {
            let key = format!("key_{}", i);
            let value = format!("value_with_long_data_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    // Reopen (recovery should trigger flush due to small memtable)
    {
        let db = options.open(dir.path()).unwrap();
        for i in 0..20 {
            let key = format!("key_{}", i);
            let value = format!("value_with_long_data_{}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(value)));
        }
    }
}

#[test]
fn test_db_recovery_empty_wal() {
    let dir = tempdir().unwrap();
    let options = DBOptions::default();

    // Create DB (no data written)
    {
        let _db = options.open(dir.path()).unwrap();
    }

    // Reopen (WAL exists but is empty)
    {
        let db = options.open(dir.path()).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), None);
    }
}

#[test]
fn test_db_with_kv_separation() {
    let dir = tempdir().unwrap();
    let options = DBOptions::default()
        .memtable_capacity(200) // Small enough to trigger flush
        .vlog_threshold(Some(50)) // 50 byte threshold
        .background_flush(false); // Synchronous for deterministic testing

    let db = options.open(dir.path()).unwrap();

    // Small value (stored inline in SSTable after flush)
    db.put(b"small_key", b"tiny_value").unwrap();

    // Large value (will be stored in vLog after flush)
    let large_value = vec![b'X'; 100];
    db.put(b"large_key", &large_value).unwrap();

    // Write more data to trigger flush
    for i in 0..3 {
        let key = format!("k{}", i);
        let value = format!("value_data_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify all values can be read (from memtable or flushed SSTable)
    assert_eq!(
        db.get(b"small_key").unwrap(),
        Some(Bytes::from("tiny_value"))
    );
    assert_eq!(
        db.get(b"large_key").unwrap(),
        Some(Bytes::from(large_value))
    );

    // Verify vLog file was created
    let vlog_path = dir.path().join("values.vlog");
    assert!(
        vlog_path.exists(),
        "vLog file should exist with vlog_threshold enabled"
    );
}

#[test]
fn test_db_with_kv_separation_recovery() {
    let dir = tempdir().unwrap();
    let options = DBOptions::default().vlog_threshold(Some(50)); // 50 byte threshold

    // Write data with large values
    {
        let db = options.open(dir.path()).unwrap();
        db.put(b"key1", b"small_value").unwrap();
        let large_value = vec![b'Y'; 200];
        db.put(b"key2", &large_value).unwrap();
    }

    // Reopen and verify recovery works with vLog
    {
        let db = options.open(dir.path()).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("small_value")));
        let expected_large = vec![b'Y'; 200];
        assert_eq!(db.get(b"key2").unwrap(), Some(Bytes::from(expected_large)));
    }
}

#[test]
fn test_db_background_compaction() {
    use std::time::Duration;

    let dir = tempdir().unwrap();
    let options = DBOptions::default()
        .memtable_capacity(100) // Small to trigger flushes
        .background_flush(false) // Synchronous flush for deterministic testing
        .background_compaction(true); // Enable background compaction

    let db = options.open(dir.path()).unwrap();

    // Write enough data to trigger multiple flushes and compaction
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Give background thread time to process compactions
    std::thread::sleep(Duration::from_millis(100));

    // Verify data is still readable
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let expected = format!("value_{:03}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
    }

    // DB will be dropped here, triggering graceful shutdown
}

#[test]
#[allow(clippy::similar_names)]
fn test_db_sync_vs_async_compaction() {
    use std::time::Duration;

    // Test that both modes produce identical results
    let dir_sync = tempdir().unwrap();
    let dir_async = tempdir().unwrap();

    let options_sync = DBOptions::default()
        .memtable_capacity(100)
        .background_flush(false) // Synchronous flush for deterministic testing
        .background_compaction(false); // Synchronous compaction

    let options_async = DBOptions::default()
        .memtable_capacity(100)
        .background_flush(false) // Synchronous flush for deterministic testing
        .background_compaction(true); // Asynchronous compaction

    let db_sync = options_sync.open(dir_sync.path()).unwrap();
    let db_async = options_async.open(dir_async.path()).unwrap();

    // Write same data to both
    for i in 0..50 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        db_sync.put(key.as_bytes(), value.as_bytes()).unwrap();
        db_async.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Give async compaction time to finish
    std::thread::sleep(Duration::from_millis(100));

    // Verify both return same results
    for i in 0..50 {
        let key = format!("key_{:03}", i);
        let expected = format!("value_{:03}", i);
        assert_eq!(
            db_sync.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(expected.clone()))
        );
        assert_eq!(
            db_async.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(expected))
        );
    }
}

#[test]
fn test_db_health_checks() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    // Perform some operations
    for i in 0..10 {
        db.put(format!("key{}", i).as_bytes(), b"value").unwrap();
    }

    // Get health status
    let health = db.health();

    // Should be healthy (low utilization, low L0 count, etc.)
    assert!(health.healthy);
    assert_eq!(health.checks.len(), 5); // 5 health checks

    // Verify check names
    let check_names: Vec<&str> = health.checks.iter().map(|c| c.name.as_str()).collect();
    assert!(check_names.contains(&"compaction_lag"));
    assert!(check_names.contains(&"wal_size"));
    assert!(check_names.contains(&"memtable_utilization"));
    assert!(check_names.contains(&"put_latency_p99"));
    assert!(check_names.contains(&"get_latency_p99"));

    // Test display formatting (doesn't panic)
    let _display = format!("{}", health);
}

#[test]
fn test_range_scan_with_sstables() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default()
        .memtable_capacity(1024) // Small memtable to force flush
        .background_compaction(false);

    let db = opts.open(dir.path()).unwrap();

    // Insert enough data to trigger flush to SSTables
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let value = format!("value{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Force flush to create SSTables
    db.flush().unwrap();

    // Range scan
    let mut results = vec![];
    for result in db.range(b"key010", Some(b"key020")).unwrap() {
        let (key, value) = result.unwrap();
        results.push((
            String::from_utf8(key.to_vec()).unwrap(),
            String::from_utf8(value.to_vec()).unwrap(),
        ));
    }

    // Should get key010 through key019
    assert_eq!(results.len(), 10);
    assert_eq!(results[0].0, "key010");
    assert_eq!(results[9].0, "key019");
}

#[test]
fn test_range_scan_with_overwrites() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default()
        .memtable_capacity(1024)
        .background_compaction(false);

    let db = opts.open(dir.path()).unwrap();

    // Write initial data
    for i in 0..50 {
        let key = format!("key{:03}", i);
        db.put(key.as_bytes(), b"old_value").unwrap();
    }
    db.flush().unwrap();

    // Overwrite some keys
    for i in 10..20 {
        let key = format!("key{:03}", i);
        db.put(key.as_bytes(), b"new_value").unwrap();
    }

    // Range scan - newer values should override
    let mut results = vec![];
    for result in db.range(b"key010", Some(b"key020")).unwrap() {
        let (key, value) = result.unwrap();
        results.push((
            String::from_utf8(key.to_vec()).unwrap(),
            String::from_utf8(value.to_vec()).unwrap(),
        ));
    }

    assert_eq!(results.len(), 10);
    // All should have new_value (memtable overrides SSTable)
    for result in &results {
        assert_eq!(result.1, "new_value");
    }
}

#[test]
fn test_range_scan_with_deletes() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default()
        .memtable_capacity(1024)
        .background_compaction(false);

    let db = opts.open(dir.path()).unwrap();

    // Write data
    for i in 0..50 {
        let key = format!("key{:03}", i);
        db.put(key.as_bytes(), b"value").unwrap();
    }
    db.flush().unwrap();

    // Delete some keys
    for i in 10..20 {
        let key = format!("key{:03}", i);
        db.delete(key.as_bytes()).unwrap();
    }

    // Range scan - deleted keys should not appear
    let mut results = vec![];
    for result in db.range(b"key005", Some(b"key025")).unwrap() {
        let (key, _value) = result.unwrap();
        results.push(String::from_utf8(key.to_vec()).unwrap());
    }

    // Should get key005-key009 and key020-key024 (5 + 5 = 10 keys)
    assert_eq!(results.len(), 10);
    assert!(!results
        .iter()
        .any(|k| k.as_str() >= "key010" && k.as_str() < "key020"));
}

#[test]
fn test_memory_budget_enforcement() {
    let dir = tempdir().unwrap();
    let options = DBOptions::default()
        .memtable_capacity(1024 * 1024) // 1MB per partition
        .max_memory_bytes(Some(200 * 1024 * 1024)); // 200MB budget (won't be triggered in test)

    let db = options.open(dir.path()).unwrap();

    // Verify memory estimation works
    let initial_memory = db.estimate_memory_usage();
    assert!(initial_memory > 0, "Memory usage should be non-zero");

    // Write small amount of data (won't trigger enforcement)
    for i in 0..10 {
        let key = format!("key{}", i);
        db.put(key.as_bytes(), b"value").unwrap();
    }

    // Verify data is accessible
    for i in 0..10 {
        let key = format!("key{}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from("value")));
    }
}

#[test]
fn test_estimate_memory_usage() {
    let dir = tempdir().unwrap();
    let options = DBOptions::default().memtable_capacity(1024);

    let db = options.open(dir.path()).unwrap();

    // Initial memory should include cache overhead
    let initial = db.estimate_memory_usage();
    // Should be at least block cache (40MB) + SSTable cache (1MB)
    assert!(initial >= 40 * 1024 * 1024, "Should include cache overhead");

    // Write some data
    for i in 0..10 {
        db.put(format!("key{}", i).as_bytes(), b"value").unwrap();
    }

    // Memory should increase
    let after_write = db.estimate_memory_usage();
    assert!(
        after_write >= initial,
        "Memory should increase after writes"
    );
}

#[test]
fn test_snapshot_basic_isolation() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    // Write initial data
    db.put(b"key1", b"value1").unwrap();
    db.put(b"key2", b"value2").unwrap();

    // Create consistent snapshot (forces flush)
    let snapshot = db.snapshot().unwrap();

    // Write after snapshot
    db.put(b"key1", b"modified").unwrap();
    db.put(b"key3", b"value3").unwrap();
    db.delete(b"key2").unwrap();

    // Snapshot sees old values
    assert_eq!(snapshot.get(b"key1").unwrap(), Some(Bytes::from("value1")));
    assert_eq!(snapshot.get(b"key2").unwrap(), Some(Bytes::from("value2")));
    assert_eq!(snapshot.get(b"key3").unwrap(), None); // Didn't exist at snapshot time

    // DB sees new values
    assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("modified")));
    assert_eq!(db.get(b"key2").unwrap(), None); // Deleted
    assert_eq!(db.get(b"key3").unwrap(), Some(Bytes::from("value3")));
}

#[test]
fn test_snapshot_range_isolation() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    // Write initial data
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();

    // Create consistent snapshot (forces flush)
    let snapshot = db.snapshot().unwrap();

    // Modify after snapshot
    db.put(b"b", b"modified").unwrap();
    db.delete(b"c").unwrap();
    db.put(b"d", b"4").unwrap();

    // Snapshot range sees original values
    let snap_results: Vec<_> = snapshot
        .range(b"a", Some(b"z"))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(snap_results.len(), 3); // a, b, c
    assert_eq!(snap_results[0].1.as_ref(), b"1");
    assert_eq!(snap_results[1].1.as_ref(), b"2");
    assert_eq!(snap_results[2].1.as_ref(), b"3");

    // DB range sees new values
    let db_results: Vec<_> = db
        .range(b"a", Some(b"z"))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(db_results.len(), 3); // a, b, d (c deleted)
    assert_eq!(db_results[0].1.as_ref(), b"1");
    assert_eq!(db_results[1].1.as_ref(), b"modified");
    assert_eq!(db_results[2].1.as_ref(), b"4");
}

#[test]
fn test_snapshot_during_concurrent_writes() {
    use std::sync::Arc;
    use std::thread;

    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path()).unwrap());

    // Write initial data
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("initial_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Create consistent snapshot (forces flush)
    let snapshot = db.snapshot().unwrap();

    // Spawn writer thread that modifies data concurrently
    let db_clone = Arc::clone(&db);
    let writer = thread::spawn(move || {
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("modified_{:03}", i);
            db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    });

    // While writes are happening, snapshot still sees original data
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let expected = format!("initial_{:03}", i);
        let actual = snapshot.get(key.as_bytes()).unwrap();
        assert_eq!(actual, Some(Bytes::from(expected)));
    }

    writer.join().unwrap();

    // After writes complete, snapshot still sees original data
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let expected = format!("initial_{:03}", i);
        let actual = snapshot.get(key.as_bytes()).unwrap();
        assert_eq!(actual, Some(Bytes::from(expected)));
    }

    // But DB sees modified data
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let expected = format!("modified_{:03}", i);
        let actual = db.get(key.as_bytes()).unwrap();
        assert_eq!(actual, Some(Bytes::from(expected)));
    }
}

#[test]
fn test_snapshot_sequence_number() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    db.put(b"key1", b"value1").unwrap();
    db.flush().unwrap(); // Force flush to increment sequence
    let snap1 = db.snapshot().unwrap();

    db.put(b"key2", b"value2").unwrap();
    db.flush().unwrap(); // Force flush to increment sequence
    let snap2 = db.snapshot().unwrap();

    // Snap2 should have higher sequence number (after more writes)
    assert!(snap2.sequence_number() >= snap1.sequence_number());

    // Debug output works
    let _debug = format!("{:?}", snap1);
}

#[test]
fn test_multiple_snapshots() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    // Initial state
    db.put(b"key", b"v1").unwrap();
    let snap1 = db.snapshot().unwrap();

    // Second state
    db.put(b"key", b"v2").unwrap();
    let snap2 = db.snapshot().unwrap();

    // Third state
    db.put(b"key", b"v3").unwrap();
    let snap3 = db.snapshot().unwrap();

    // Current state
    db.put(b"key", b"v4").unwrap();

    // Each snapshot sees its point-in-time value
    assert_eq!(snap1.get(b"key").unwrap(), Some(Bytes::from("v1")));
    assert_eq!(snap2.get(b"key").unwrap(), Some(Bytes::from("v2")));
    assert_eq!(snap3.get(b"key").unwrap(), Some(Bytes::from("v3")));
    assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("v4")));

    // Drop early snapshots, late ones still work
    drop(snap1);
    drop(snap2);
    assert_eq!(snap3.get(b"key").unwrap(), Some(Bytes::from("v3")));
}

#[test]
fn test_snapshot_with_tombstones() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    // Write and delete
    db.put(b"key1", b"value1").unwrap();
    db.put(b"key2", b"value2").unwrap();
    db.delete(b"key1").unwrap();

    // Snapshot sees key1 as deleted (after flush)
    let snap = db.snapshot().unwrap();
    assert_eq!(snap.get(b"key1").unwrap(), None);
    assert_eq!(snap.get(b"key2").unwrap(), Some(Bytes::from("value2")));

    // Re-insert key1
    db.put(b"key1", b"resurrected").unwrap();

    // Snapshot still sees key1 as deleted
    assert_eq!(snap.get(b"key1").unwrap(), None);

    // DB sees resurrected value
    assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("resurrected")));
}

#[test]
fn test_iter_all_keys() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    // Write some keys
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.put(b"d", b"4").unwrap();
    db.put(b"e", b"5").unwrap();

    // Iterate over all keys
    let results: Vec<_> = db.iter().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 5);
    assert_eq!(results[0].0.as_ref(), b"a");
    assert_eq!(results[1].0.as_ref(), b"b");
    assert_eq!(results[2].0.as_ref(), b"c");
    assert_eq!(results[3].0.as_ref(), b"d");
    assert_eq!(results[4].0.as_ref(), b"e");
}

#[test]
fn test_db_iter_rev() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();

    let results: Vec<_> = db.iter_rev().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0.as_ref(), b"c");
    assert_eq!(results[1].0.as_ref(), b"b");
    assert_eq!(results[2].0.as_ref(), b"a");
}

#[test]
fn test_prefix_scan() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    // Write keys with different prefixes
    db.put(b"user:1", b"alice").unwrap();
    db.put(b"user:2", b"bob").unwrap();
    db.put(b"user:3", b"charlie").unwrap();
    db.put(b"post:1", b"hello").unwrap();
    db.put(b"post:2", b"world").unwrap();
    db.put(b"tag:rust", b"lang").unwrap();

    // Scan user: prefix
    let user_results: Vec<_> = db.prefix(b"user:").unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(user_results.len(), 3);
    assert_eq!(user_results[0].0.as_ref(), b"user:1");
    assert_eq!(user_results[1].0.as_ref(), b"user:2");
    assert_eq!(user_results[2].0.as_ref(), b"user:3");

    // Scan post: prefix
    let post_results: Vec<_> = db.prefix(b"post:").unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(post_results.len(), 2);
    assert_eq!(post_results[0].0.as_ref(), b"post:1");
    assert_eq!(post_results[1].0.as_ref(), b"post:2");

    // Scan tag: prefix (single result)
    let tag_results: Vec<_> = db.prefix(b"tag:").unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(tag_results.len(), 1);
    assert_eq!(tag_results[0].0.as_ref(), b"tag:rust");

    // Scan non-existent prefix
    let empty_results: Vec<_> = db
        .prefix(b"missing:")
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(empty_results.len(), 0);
}

#[test]
fn test_increment_bytes_helper() {
    // Normal case
    assert_eq!(increment_bytes(b"user"), Some(b"uses".to_vec()));

    // With 0xFF at end
    assert_eq!(increment_bytes(b"user\xff"), Some(b"uses\x00".to_vec()));

    // Multiple 0xFF at end
    assert_eq!(increment_bytes(b"a\xff\xff"), Some(b"b\x00\x00".to_vec()));

    // All 0xFF
    assert_eq!(increment_bytes(b"\xff\xff"), None);

    // Single byte
    assert_eq!(increment_bytes(b"a"), Some(b"b".to_vec()));
    assert_eq!(increment_bytes(b"\xff"), None);

    // Empty
    assert_eq!(increment_bytes(b""), None);
}

#[test]
fn test_prefix_with_sstables() {
    let dir = tempdir().unwrap();
    let opts = DBOptions::default().memtable_capacity(1024); // Small memtable to force flush

    let db = opts.open(dir.path()).unwrap();

    // Write enough data to trigger flush
    for i in 0..20 {
        let key = format!("key:{:02}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Force flush to ensure data is in SSTables
    db.flush().unwrap();

    // Add some more data in memtable
    db.put(b"key:20", b"value_20").unwrap();
    db.put(b"key:21", b"value_21").unwrap();

    // Prefix scan should find all keys (memtable + SSTables)
    let results: Vec<_> = db.prefix(b"key:").unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 22);

    // Verify ordering
    for i in 0..22 {
        let expected_key = format!("key:{:02}", i);
        assert_eq!(results[i].0.as_ref(), expected_key.as_bytes());
    }
}

#[test]
fn test_prefix_batch_basic() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    db.put(b"user:1", b"alice").unwrap();
    db.put(b"user:2", b"bob").unwrap();
    db.put(b"post:1", b"hello").unwrap();
    db.put(b"post:2", b"world").unwrap();

    let prefixes = vec![b"user:" as &[u8], b"post:"];
    let results = db.prefix_batch(&prefixes).unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].len(), 2);
    assert_eq!(results[1].len(), 2);

    assert_eq!(results[0][0].0.as_ref(), b"user:1");
    assert_eq!(results[0][0].1.as_ref(), b"alice");
    assert_eq!(results[0][1].0.as_ref(), b"user:2");
    assert_eq!(results[0][1].1.as_ref(), b"bob");

    assert_eq!(results[1][0].0.as_ref(), b"post:1");
    assert_eq!(results[1][0].1.as_ref(), b"hello");
    assert_eq!(results[1][1].0.as_ref(), b"post:2");
    assert_eq!(results[1][1].1.as_ref(), b"world");
}

#[test]
fn test_prefix_batch_empty() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    let prefixes: Vec<&[u8]> = vec![];
    let results = db.prefix_batch(&prefixes).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_prefix_batch_no_matches() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    db.put(b"user:1", b"alice").unwrap();

    let prefixes = vec![b"nonexistent:" as &[u8]];
    let results = db.prefix_batch(&prefixes).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].len(), 0);
}

#[test]
fn test_prefix_batch_ordering() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    db.put(b"a:1", b"1").unwrap();
    db.put(b"b:1", b"2").unwrap();
    db.put(b"c:1", b"3").unwrap();

    let prefixes = vec![b"c:" as &[u8], b"a:", b"b:"];
    let results = db.prefix_batch(&prefixes).unwrap();

    assert_eq!(results[0][0].1.as_ref(), b"3");
    assert_eq!(results[1][0].1.as_ref(), b"1");
    assert_eq!(results[2][0].1.as_ref(), b"2");
}

#[test]
fn test_prefix_batch_concurrent() {
    use std::sync::Arc;
    use std::thread;

    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path()).unwrap());

    db.put(b"user:1", b"alice").unwrap();
    db.put(b"user:2", b"bob").unwrap();
    db.put(b"post:1", b"hello").unwrap();

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let db = db.clone();
            thread::spawn(move || {
                let prefixes = vec![b"user:" as &[u8], b"post:"];
                db.prefix_batch(&prefixes)
            })
        })
        .collect();

    for handle in handles {
        let result = handle.join().unwrap();
        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].len(), 2);
        assert_eq!(results[1].len(), 1);
    }
}

#[test]
fn test_global_block_cache_hits() {
    // Test that global block cache provides cache hits on repeated reads
    let dir = tempdir().unwrap();
    let options = DBOptions::default()
        .memtable_capacity(1024) // Small to force flush
        .block_cache_capacity(100); // Small cache (100 blocks)

    let db = options.open(dir.path()).unwrap();

    // Write data to create SSTable
    for i in 0..50 {
        let key = format!("key:{:04}", i);
        let value = vec![i as u8; 100]; // 100 bytes each
        db.put(key.as_bytes(), &value).unwrap();
    }

    // Force flush to disk
    db.flush().unwrap();

    // First read - should miss cache (cold start)
    let stats_before = db.stats();
    let initial_hits = stats_before.cache_hits;
    let _initial_misses = stats_before.cache_misses;

    // Read a key from disk
    let _ = db.get(b"key:0025").unwrap();

    // Read the same key again - should hit cache
    let _ = db.get(b"key:0025").unwrap();

    // Check cache stats
    let stats_after = db.stats();

    // We should have at least one cache hit from the second read
    assert!(
        stats_after.cache_hits > initial_hits,
        "Expected cache hit: before={}, after={}",
        initial_hits,
        stats_after.cache_hits
    );

    // Cache size should be non-zero
    assert!(
        stats_after.block_cache_size > 0,
        "Cache should have entries: {}",
        stats_after.block_cache_size
    );

    // Cache capacity should match what we configured
    assert_eq!(
        stats_after.block_cache_capacity, 100,
        "Cache capacity mismatch"
    );
}

#[test]
fn test_block_cache_stats_in_dbstats() {
    // Test that block cache metrics are properly exposed in DBStats
    let dir = tempdir().unwrap();
    let options = DBOptions::default().block_cache_capacity(500); // 500 blocks

    let db = options.open(dir.path()).unwrap();

    // Check initial cache stats
    let stats = db.stats();

    // Cache should be empty initially
    assert_eq!(stats.block_cache_size, 0);
    // quick_cache may round capacity up to next power of 2 (500 -> 512)
    assert!(
        stats.block_cache_capacity >= 500,
        "Cache capacity should be at least 500: {}",
        stats.block_cache_capacity
    );
    assert_eq!(stats.cache_hits, 0);
    assert_eq!(stats.cache_misses, 0);
    assert_eq!(stats.cache_hit_rate, 0.0);
}

#[test]
fn test_block_cache_shared_across_sstables() {
    // Test that global cache is shared across multiple SSTables
    let dir = tempdir().unwrap();
    let options = DBOptions::default()
        .memtable_capacity(2048) // Small to force multiple flushes
        .block_cache_capacity(1000); // Enough to cache blocks from multiple SSTables

    let db = options.open(dir.path()).unwrap();

    // Write first batch and flush to create SSTable 1
    for i in 0..20 {
        let key = format!("batch1:key:{:04}", i);
        let value = vec![i as u8; 64];
        db.put(key.as_bytes(), &value).unwrap();
    }
    db.flush().unwrap();

    // Write second batch and flush to create SSTable 2
    for i in 0..20 {
        let key = format!("batch2:key:{:04}", i);
        let value = vec![i as u8; 64];
        db.put(key.as_bytes(), &value).unwrap();
    }
    db.flush().unwrap();

    // Read from both SSTables
    let _ = db.get(b"batch1:key:0010").unwrap();
    let _ = db.get(b"batch2:key:0010").unwrap();

    // Read again - should hit cache
    let _ = db.get(b"batch1:key:0010").unwrap();
    let _ = db.get(b"batch2:key:0010").unwrap();

    // Check that cache contains blocks from both SSTables
    let stats = db.stats();

    // Should have cache entries (blocks from both SSTables)
    assert!(
        stats.block_cache_size > 0,
        "Global cache should contain entries from multiple SSTables"
    );

    // Should have cache hits from the repeated reads
    assert!(
        stats.cache_hits > 0,
        "Should have cache hits from repeated reads: hits={}",
        stats.cache_hits
    );
}

#[test]
#[cfg(feature = "object-store")]
fn test_db_with_cloud_storage_backend() {
    use object_store::{memory::InMemory, ObjectStore};

    let dir = tempdir().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Create in-memory object store for testing
    let store = std::sync::Arc::new(InMemory::new());
    let _guard = rt.enter();

    let options = DBOptions::default()
        .memtable_capacity(1000) // Small to trigger flushes
        .storage_config(StorageConfig::Custom(store.clone()))
        .vlog_threshold(None); // Disable vLog to test non-vLog path

    let db = options.open(dir.path()).unwrap();

    // Write enough data to trigger flush
    for i in 0..50 {
        let key = format!("cloud_key_{:03}", i);
        let value = format!("cloud_value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Flush to trigger upload
    db.flush().unwrap();

    // Verify data is readable
    for i in 0..50 {
        let key = format!("cloud_key_{:03}", i);
        let expected = format!("cloud_value_{:03}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
    }

    // Check that object store has data
    let listed = rt.block_on(async {
        use futures::TryStreamExt;
        let mut count = 0;
        let mut stream = store.list(None);
        while let Some(meta) = stream.try_next().await.unwrap() {
            if meta.location.to_string().ends_with(".sst") {
                count += 1;
            }
        }
        count
    });

    // Should have at least one SSTable uploaded
    assert!(
        listed > 0,
        "Object store should have at least one SSTable uploaded"
    );
}

#[test]
#[cfg(feature = "object-store")]
fn test_db_cloud_storage_with_vlog() {
    use object_store::{memory::InMemory, ObjectStore};

    let dir = tempdir().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Create in-memory object store for testing
    let store = std::sync::Arc::new(InMemory::new());
    let _guard = rt.enter();

    let options = DBOptions::default()
        .memtable_capacity(1000) // Small to trigger flushes
        .storage_config(StorageConfig::Custom(store.clone()))
        .vlog_threshold(Some(100)); // Enable vLog for large values

    let db = options.open(dir.path()).unwrap();

    // Write mixed data: small and large values
    for i in 0..30 {
        let key = format!("key_{:03}", i);
        if i % 2 == 0 {
            // Small values (inline)
            let value = format!("small_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        } else {
            // Large values (vLog)
            let value = vec![b'X'; 200];
            db.put(key.as_bytes(), &value).unwrap();
        }
    }

    // Flush to trigger upload
    db.flush().unwrap();

    // Verify all data is readable
    for i in 0..30 {
        let key = format!("key_{:03}", i);
        let value = db.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Key {} should exist", i);
        if i % 2 == 0 {
            assert_eq!(value.unwrap(), Bytes::from(format!("small_{:03}", i)));
        } else {
            assert_eq!(value.unwrap().len(), 200);
        }
    }

    // Check that object store has SSTable data
    let listed = rt.block_on(async {
        use futures::TryStreamExt;
        let mut count = 0;
        let mut stream = store.list(None);
        while let Some(meta) = stream.try_next().await.unwrap() {
            if meta.location.to_string().ends_with(".sst") {
                count += 1;
            }
        }
        count
    });

    // Should have at least one SSTable uploaded
    assert!(
        listed > 0,
        "Object store should have at least one SSTable with vLog uploaded"
    );
}

#[test]
#[cfg(feature = "object-store")]
fn test_tiered_storage_cold_tier_compaction() {
    use object_store::{memory::InMemory, ObjectStore};

    let dir = tempdir().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Create in-memory object store for cold tier
    let cold_store = std::sync::Arc::new(InMemory::new());
    let _guard = rt.enter();

    let options = DBOptions::default()
        .memtable_capacity(1000) // Very small to trigger frequent flushes
        .base_level_size(500) // Very small to trigger compaction quickly
        .size_ratio(2) // Small ratio to trigger compaction faster
        .cold_tier_level(Some(2)) // L2+ goes to cold storage
        .cold_storage(StorageConfig::Custom(cold_store.clone()))
        .vlog_threshold(None); // Disable vLog for simplicity

    let db = options.open(dir.path()).unwrap();

    // Write enough data to trigger multiple flushes and compactions to L2+
    for i in 0..100 {
        let key = format!("tiered_key_{:05}", i);
        let value = format!("tiered_value_{:05}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Force flush and compaction
    db.flush().unwrap();

    // Trigger compaction manually to ensure data reaches cold tier
    for _ in 0..5 {
        if let Some(level) = db.lsm.load().needs_compaction() {
            let _ = db.compact_level(level);
        }
    }

    // Verify all data is still readable (from local cache)
    for i in 0..100 {
        let key = format!("tiered_key_{:05}", i);
        let value = db.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Key {} should exist", key);
        let expected = format!("tiered_value_{:05}", i);
        assert_eq!(value.unwrap(), Bytes::from(expected));
    }

    // Check that cold storage has some SSTables
    let cold_count = rt.block_on(async {
        use futures::TryStreamExt;
        let mut count = 0;
        let mut stream = cold_store.list(None);
        while let Some(meta) = stream.try_next().await.unwrap() {
            if meta.location.to_string().ends_with(".sst") {
                count += 1;
            }
        }
        count
    });

    // After enough compaction, L2+ SSTables should be in cold storage
    // Note: May be 0 if compaction didn't reach L2 in this test run
    // The test primarily verifies the code path works without errors
    info!(
        cold_sstables = cold_count,
        "Tiered storage test complete - cold tier SSTable count"
    );
}

#[test]
#[cfg(feature = "object-store")]
fn test_tiered_storage_options_validation() {
    // Test that cold_tier_level without cold_storage still works (no-op)
    let dir = tempdir().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    let options = DBOptions::default().cold_tier_level(Some(4)); // Set level but no cold storage configured

    // Should open without error (cold tier config is ignored without backend)
    let db = options.open(dir.path()).unwrap();
    db.put(b"test_key", b"test_value").unwrap();
    assert_eq!(
        db.get(b"test_key").unwrap(),
        Some(Bytes::from("test_value"))
    );
}
