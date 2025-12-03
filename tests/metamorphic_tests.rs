//! Metamorphic testing: Test invariants that should always hold
//!
//! These tests verify properties that must be true regardless of input:
//! - Insert then delete all = empty database
//! - Different insertion orders = same final state
//! - Compaction doesn't change query results
//! - Flush doesn't change query results
//!
//! Run with: `cargo test metamorphic`

use rand::seq::SliceRandom;
use rand::SeedableRng;
use seerdb::{DBOptions, DB};
use std::collections::BTreeMap;
use tempfile::TempDir;

/// Helper to collect all key-value pairs from database
fn collect_all(db: &DB) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut result = BTreeMap::new();
    if let Ok(iter) = db.iter() {
        for item in iter {
            if let Ok((k, v)) = item {
                result.insert(k.to_vec(), v.to_vec());
            }
        }
    }
    result
}

/// Helper to create a test database
fn create_db(temp_dir: &TempDir) -> DB {
    let opts = DBOptions {
        data_dir: temp_dir.path().to_path_buf(),
        background_compaction: false,
        background_flush: false,
        memtable_capacity: 4096, // Small to trigger flushes
        ..Default::default()
    };
    DB::open(opts).expect("Failed to open database")
}

// =============================================================================
// METAMORPHIC PROPERTY 1: Insert then delete all = empty
// =============================================================================

/// Insert N keys, then delete all N keys. Database should be empty.
#[test]
fn test_metamorphic_insert_delete_all_is_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db = create_db(&temp_dir);

    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("key_{:04}", i).into_bytes())
        .collect();

    // Insert all keys
    for key in &keys {
        db.put(key, b"value").unwrap();
    }

    // Verify they exist
    for key in &keys {
        assert!(db.get(key).unwrap().is_some(), "Key should exist after put");
    }

    // Delete all keys
    for key in &keys {
        db.delete(key).unwrap();
    }

    // Verify database is empty
    for key in &keys {
        assert!(
            db.get(key).unwrap().is_none(),
            "Key should not exist after delete"
        );
    }

    // Verify iteration returns nothing
    let all = collect_all(&db);
    assert!(all.is_empty(), "Database should be empty after delete all");
}

/// Same test but with flush between insert and delete
#[test]
fn test_metamorphic_insert_flush_delete_all_is_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db = create_db(&temp_dir);

    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("key_{:04}", i).into_bytes())
        .collect();

    // Insert all keys
    for key in &keys {
        db.put(key, b"value").unwrap();
    }

    // Flush to SSTable
    db.flush().unwrap();

    // Delete all keys
    for key in &keys {
        db.delete(key).unwrap();
    }

    // Flush tombstones
    db.flush().unwrap();

    // Verify database is empty
    for key in &keys {
        assert!(
            db.get(key).unwrap().is_none(),
            "Key {} should not exist after delete",
            String::from_utf8_lossy(key)
        );
    }
}

/// Insert, flush, delete, flush, reopen - should still be empty
#[test]
fn test_metamorphic_insert_delete_reopen_is_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let keys: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("key_{:04}", i).into_bytes())
        .collect();

    // Phase 1: Insert and delete
    {
        let db = create_db(&temp_dir);
        for key in &keys {
            db.put(key, b"value").unwrap();
        }
        db.flush().unwrap();
        for key in &keys {
            db.delete(key).unwrap();
        }
        db.flush().unwrap();
    }

    // Phase 2: Reopen and verify empty
    {
        let opts = DBOptions {
            data_dir: db_path,
            background_compaction: false,
            background_flush: false,
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        for key in &keys {
            assert!(
                db.get(key).unwrap().is_none(),
                "Key should not exist after reopen"
            );
        }
    }
}

// =============================================================================
// METAMORPHIC PROPERTY 2: Different insertion orders = same final state
// =============================================================================

/// Insert keys in different orders, final state should be identical
#[test]
fn test_metamorphic_insertion_order_independent() {
    let keys: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
        .map(|i| {
            (
                format!("key_{:04}", i).into_bytes(),
                format!("value_{:04}", i).into_bytes(),
            )
        })
        .collect();

    // Order 1: Sequential
    let temp_dir1 = TempDir::new().unwrap();
    let db1 = create_db(&temp_dir1);
    for (k, v) in &keys {
        db1.put(k, v).unwrap();
    }
    db1.flush().unwrap();

    // Order 2: Reversed
    let temp_dir2 = TempDir::new().unwrap();
    let db2 = create_db(&temp_dir2);
    for (k, v) in keys.iter().rev() {
        db2.put(k, v).unwrap();
    }
    db2.flush().unwrap();

    // Order 3: Shuffled
    let temp_dir3 = TempDir::new().unwrap();
    let db3 = create_db(&temp_dir3);
    let mut shuffled = keys.clone();
    let mut rng = rand::rngs::StdRng::seed_from_u64(12345);
    shuffled.shuffle(&mut rng);
    for (k, v) in &shuffled {
        db3.put(k, v).unwrap();
    }
    db3.flush().unwrap();

    // Collect final states
    let state1 = collect_all(&db1);
    let state2 = collect_all(&db2);
    let state3 = collect_all(&db3);

    // All should be identical
    assert_eq!(state1, state2, "Sequential vs Reversed differ");
    assert_eq!(state1, state3, "Sequential vs Shuffled differ");
}

/// Interleaved puts to same keys - last write wins
#[test]
fn test_metamorphic_last_write_wins() {
    let temp_dir = TempDir::new().unwrap();
    let db = create_db(&temp_dir);

    // Write multiple values to same key
    db.put(b"key", b"value1").unwrap();
    db.put(b"key", b"value2").unwrap();
    db.put(b"key", b"value3").unwrap();

    // Last write should win
    let result = db.get(b"key").unwrap().unwrap();
    assert_eq!(result.as_ref(), b"value3");

    // Even after flush
    db.flush().unwrap();
    let result = db.get(b"key").unwrap().unwrap();
    assert_eq!(result.as_ref(), b"value3");
}

// =============================================================================
// METAMORPHIC PROPERTY 3: Flush doesn't change query results
// =============================================================================

/// Query results should be identical before and after flush
#[test]
fn test_metamorphic_flush_preserves_results() {
    let temp_dir = TempDir::new().unwrap();
    let db = create_db(&temp_dir);

    // Insert data
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Collect state before flush
    let before_flush = collect_all(&db);

    // Flush
    db.flush().unwrap();

    // Collect state after flush
    let after_flush = collect_all(&db);

    assert_eq!(before_flush, after_flush, "Flush changed query results");
}

/// Multiple flushes should not change results
#[test]
fn test_metamorphic_multiple_flushes_idempotent() {
    let temp_dir = TempDir::new().unwrap();
    let db = create_db(&temp_dir);

    // Insert data
    for i in 0..50 {
        db.put(
            format!("key_{:04}", i).as_bytes(),
            format!("value_{:04}", i).as_bytes(),
        )
        .unwrap();
    }

    db.flush().unwrap();
    let after_first = collect_all(&db);

    db.flush().unwrap();
    let after_second = collect_all(&db);

    db.flush().unwrap();
    let after_third = collect_all(&db);

    assert_eq!(after_first, after_second);
    assert_eq!(after_second, after_third);
}

// =============================================================================
// METAMORPHIC PROPERTY 4: Compaction doesn't change query results
// =============================================================================

/// Trigger compaction, verify results unchanged
#[test]
fn test_metamorphic_compaction_preserves_results() {
    let temp_dir = TempDir::new().unwrap();
    let opts = DBOptions {
        data_dir: temp_dir.path().to_path_buf(),
        background_compaction: false,
        background_flush: false,
        memtable_capacity: 1024, // Very small to trigger many flushes
        ..Default::default()
    };
    let db = DB::open(opts).unwrap();

    // Create multiple SSTables by flushing repeatedly
    for batch in 0..5 {
        for i in 0..20 {
            let key = format!("key_{:02}_{:04}", batch, i);
            let value = format!("value_{:02}_{:04}", batch, i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }

    // Collect state before potential compaction
    let _before_compaction = collect_all(&db);

    // Force more flushes to potentially trigger compaction
    for batch in 5..10 {
        for i in 0..20 {
            let key = format!("key_{:02}_{:04}", batch, i);
            let value = format!("value_{:02}_{:04}", batch, i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }

    // Verify original keys still correct
    for batch in 0..5 {
        for i in 0..20 {
            let key = format!("key_{:02}_{:04}", batch, i);
            let expected = format!("value_{:02}_{:04}", batch, i);
            let result = db.get(key.as_bytes()).unwrap();
            assert_eq!(
                result.as_ref().map(|b| b.as_ref()),
                Some(expected.as_bytes()),
                "Key {} has wrong value after compaction",
                key
            );
        }
    }
}

// =============================================================================
// METAMORPHIC PROPERTY 5: Reopen doesn't change query results
// =============================================================================

/// Close and reopen database, verify results unchanged
#[test]
fn test_metamorphic_reopen_preserves_results() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let expected: BTreeMap<Vec<u8>, Vec<u8>>;

    // Phase 1: Write data
    {
        let db = create_db(&temp_dir);
        for i in 0..100 {
            db.put(
                format!("key_{:04}", i).as_bytes(),
                format!("value_{:04}", i).as_bytes(),
            )
            .unwrap();
        }
        db.flush().unwrap();
        expected = collect_all(&db);
    }

    // Phase 2: Reopen and compare
    {
        let opts = DBOptions {
            data_dir: db_path,
            background_compaction: false,
            background_flush: false,
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();
        let actual = collect_all(&db);
        assert_eq!(expected, actual, "Data changed after reopen");
    }
}

// =============================================================================
// METAMORPHIC PROPERTY 6: Get non-existent key always returns None
// =============================================================================

#[test]
fn test_metamorphic_get_nonexistent_always_none() {
    let temp_dir = TempDir::new().unwrap();
    let db = create_db(&temp_dir);

    // Empty database
    assert!(db.get(b"nonexistent").unwrap().is_none());

    // After some puts
    db.put(b"key1", b"value1").unwrap();
    db.put(b"key2", b"value2").unwrap();
    assert!(db.get(b"nonexistent").unwrap().is_none());

    // After flush
    db.flush().unwrap();
    assert!(db.get(b"nonexistent").unwrap().is_none());

    // After delete
    db.delete(b"key1").unwrap();
    assert!(db.get(b"nonexistent").unwrap().is_none());
    assert!(db.get(b"key1").unwrap().is_none()); // Deleted key also None
}

// =============================================================================
// METAMORPHIC PROPERTY 7: Empty value is distinguishable from None
// =============================================================================

#[test]
fn test_metamorphic_empty_value_vs_none() {
    let temp_dir = TempDir::new().unwrap();
    let db = create_db(&temp_dir);

    // Put empty value
    db.put(b"empty_key", b"").unwrap();

    // Should return Some(empty), not None
    let result = db.get(b"empty_key").unwrap();
    assert!(result.is_some(), "Empty value should be Some, not None");
    assert_eq!(result.unwrap().as_ref(), b"");

    // Non-existent should be None
    assert!(db.get(b"nonexistent").unwrap().is_none());

    // After flush
    db.flush().unwrap();
    let result = db.get(b"empty_key").unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_ref(), b"");
}
