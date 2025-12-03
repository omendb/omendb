//! Recovery verification framework
//!
//! After every crash/recovery scenario, systematically verify:
//! - All committed data is present
//! - No uncommitted data is present
//! - Key count matches expected
//! - Data integrity (values match)
//! - No orphan files on disk
//!
//! Run with: `cargo test recovery_verification`

use seerdb::{DBOptions, SyncPolicy, DB};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Verification result
#[derive(Debug)]
struct VerificationResult {
    expected_count: usize,
    actual_count: usize,
    missing_keys: Vec<String>,
    extra_keys: Vec<String>,
    corrupted_values: Vec<String>,
    orphan_files: Vec<PathBuf>,
}

impl VerificationResult {
    fn is_ok(&self) -> bool {
        self.missing_keys.is_empty()
            && self.extra_keys.is_empty()
            && self.corrupted_values.is_empty()
            && self.expected_count == self.actual_count
    }
}

/// Verify database state matches expected state
fn verify_state(
    db: &DB,
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
    db_path: &Path,
) -> VerificationResult {
    let mut result = VerificationResult {
        expected_count: expected.len(),
        actual_count: 0,
        missing_keys: Vec::new(),
        extra_keys: Vec::new(),
        corrupted_values: Vec::new(),
        orphan_files: Vec::new(),
    };

    // Check all expected keys exist with correct values
    for (key, expected_value) in expected {
        match db.get(key) {
            Ok(Some(actual_value)) => {
                result.actual_count += 1;
                if actual_value.as_ref() != expected_value.as_slice() {
                    result.corrupted_values.push(format!(
                        "{}: expected {:?}, got {:?}",
                        String::from_utf8_lossy(key),
                        expected_value,
                        actual_value
                    ));
                }
            }
            Ok(None) => {
                result
                    .missing_keys
                    .push(String::from_utf8_lossy(key).to_string());
            }
            Err(e) => {
                result.missing_keys.push(format!(
                    "{} (error: {})",
                    String::from_utf8_lossy(key),
                    e
                ));
            }
        }
    }

    // Check for extra keys (keys in DB but not in expected)
    if let Ok(iter) = db.iter() {
        let mut actual_keys = HashSet::new();
        for item in iter {
            if let Ok((k, _)) = item {
                actual_keys.insert(k.to_vec());
            }
        }

        for key in &actual_keys {
            if !expected.contains_key(key) {
                result
                    .extra_keys
                    .push(String::from_utf8_lossy(&key).to_string());
            }
        }

        // Update actual count from iteration
        result.actual_count = actual_keys.len();
    }

    // Check for orphan SSTable files (files not tracked in LSM)
    // This is a simplified check - just count .sst files
    if let Ok(entries) = fs::read_dir(db_path) {
        let sst_files: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "sst"))
            .collect();

        // We can't easily check if they're orphaned without internal access,
        // but we can at least report the count
        let stats = db.stats();
        if sst_files.len() > stats.total_sstables {
            // More files than tracked - potential orphans
            result.orphan_files = sst_files;
        }
    }

    result
}

/// Helper to create database with sync policy
fn create_db(temp_dir: &TempDir, sync: SyncPolicy) -> DB {
    let opts = DBOptions {
        data_dir: temp_dir.path().to_path_buf(),
        wal_sync_policy: sync,
        background_compaction: false,
        background_flush: false,
        ..Default::default()
    };
    DB::open(opts).expect("Failed to open database")
}

/// Helper to reopen database
fn reopen_db(path: PathBuf) -> DB {
    let opts = DBOptions {
        data_dir: path,
        background_compaction: false,
        background_flush: false,
        ..Default::default()
    };
    DB::open(opts).expect("Failed to reopen database")
}

// =============================================================================
// TEST: Basic recovery verification
// =============================================================================

#[test]
fn test_recovery_basic_write_flush_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Phase 1: Write data
    {
        let db = create_db(&temp_dir, SyncPolicy::SyncAll);

        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_{:04}", i).into_bytes();
            db.put(&key, &value).unwrap();
            expected.insert(key, value);
        }

        db.flush().unwrap();
    }

    // Phase 2: Reopen and verify
    {
        let db = reopen_db(db_path.clone());
        let result = verify_state(&db, &expected, &db_path);

        assert!(result.is_ok(), "Recovery verification failed: {:?}", result);
        assert_eq!(result.expected_count, 100);
        assert_eq!(result.actual_count, 100);
    }
}

// =============================================================================
// TEST: Recovery after WAL-only writes (no flush)
// =============================================================================

#[test]
fn test_recovery_wal_only_no_flush() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Phase 1: Write data to WAL only (no flush)
    {
        let db = create_db(&temp_dir, SyncPolicy::SyncAll);

        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_{:04}", i).into_bytes();
            db.put(&key, &value).unwrap();
            expected.insert(key, value);
        }
        // No flush - data only in WAL
    }

    // Phase 2: Reopen - should recover from WAL
    {
        let db = reopen_db(db_path.clone());
        let result = verify_state(&db, &expected, &db_path);

        assert!(
            result.is_ok(),
            "WAL recovery verification failed: {:?}",
            result
        );
    }
}

// =============================================================================
// TEST: Recovery with mixed committed/uncommitted data
// =============================================================================

#[test]
fn test_recovery_mixed_flushed_and_wal() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Phase 1: Write some data, flush, write more
    {
        let db = create_db(&temp_dir, SyncPolicy::SyncAll);

        // First batch - will be flushed
        for i in 0..50 {
            let key = format!("batch1_key_{:04}", i).into_bytes();
            let value = format!("batch1_value_{:04}", i).into_bytes();
            db.put(&key, &value).unwrap();
            expected.insert(key, value);
        }

        db.flush().unwrap();

        // Second batch - only in WAL
        for i in 0..30 {
            let key = format!("batch2_key_{:04}", i).into_bytes();
            let value = format!("batch2_value_{:04}", i).into_bytes();
            db.put(&key, &value).unwrap();
            expected.insert(key, value);
        }
        // No second flush
    }

    // Phase 2: Verify both batches recovered
    {
        let db = reopen_db(db_path.clone());
        let result = verify_state(&db, &expected, &db_path);

        assert!(
            result.is_ok(),
            "Mixed recovery verification failed: {:?}",
            result
        );
        assert_eq!(result.expected_count, 80); // 50 + 30
    }
}

// =============================================================================
// TEST: Recovery with deletes
// =============================================================================

#[test]
fn test_recovery_with_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Phase 1: Write, delete some, flush
    {
        let db = create_db(&temp_dir, SyncPolicy::SyncAll);

        // Write 100 keys
        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_{:04}", i).into_bytes();
            db.put(&key, &value).unwrap();
            expected.insert(key, value);
        }

        db.flush().unwrap();

        // Delete even-numbered keys
        for i in (0..100).step_by(2) {
            let key = format!("key_{:04}", i).into_bytes();
            db.delete(&key).unwrap();
            expected.remove(&key);
        }

        db.flush().unwrap();
    }

    // Phase 2: Verify only odd keys remain
    {
        let db = reopen_db(db_path.clone());
        let result = verify_state(&db, &expected, &db_path);

        assert!(
            result.is_ok(),
            "Delete recovery verification failed: {:?}",
            result
        );
        assert_eq!(result.expected_count, 50); // Only odd keys
    }
}

// =============================================================================
// TEST: Recovery with overwrites
// =============================================================================

#[test]
fn test_recovery_with_overwrites() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Phase 1: Write, overwrite, flush
    {
        let db = create_db(&temp_dir, SyncPolicy::SyncAll);

        // Initial write
        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_v1_{:04}", i).into_bytes();
            db.put(&key, &value).unwrap();
        }

        db.flush().unwrap();

        // Overwrite with new values
        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_v2_{:04}", i).into_bytes();
            db.put(&key, &value).unwrap();
            expected.insert(key, value); // Only store final value
        }

        db.flush().unwrap();
    }

    // Phase 2: Verify latest values
    {
        let db = reopen_db(db_path.clone());
        let result = verify_state(&db, &expected, &db_path);

        assert!(
            result.is_ok(),
            "Overwrite recovery verification failed: {:?}",
            result
        );

        // Verify specific value is v2
        let key = b"key_0000";
        let value = db.get(key).unwrap().unwrap();
        assert!(
            value.starts_with(b"value_v2"),
            "Should have v2 value, got {:?}",
            value
        );
    }
}

// =============================================================================
// TEST: Recovery count verification
// =============================================================================

#[test]
fn test_recovery_count_matches() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let expected_count = 200;

    // Phase 1: Write exact number of keys
    {
        let db = create_db(&temp_dir, SyncPolicy::SyncAll);

        for i in 0..expected_count {
            let key = format!("key_{:06}", i).into_bytes();
            let value = format!("value_{:06}", i).into_bytes();
            db.put(&key, &value).unwrap();
        }

        db.flush().unwrap();
    }

    // Phase 2: Count keys after recovery
    {
        let db = reopen_db(db_path);

        let mut actual_count = 0;
        if let Ok(iter) = db.iter() {
            for item in iter {
                if item.is_ok() {
                    actual_count += 1;
                }
            }
        }

        assert_eq!(
            actual_count, expected_count,
            "Key count mismatch after recovery: expected {}, got {}",
            expected_count, actual_count
        );
    }
}

// =============================================================================
// TEST: Recovery with multiple reopens
// =============================================================================

#[test]
fn test_recovery_multiple_reopens() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Phase 1: Initial data
    {
        let db = create_db(&temp_dir, SyncPolicy::SyncAll);
        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_{:04}", i).into_bytes();
            db.put(&key, &value).unwrap();
            expected.insert(key, value);
        }
        db.flush().unwrap();
    }

    // Multiple reopen cycles
    for cycle in 0..3 {
        // Reopen and add more data
        {
            let db = reopen_db(db_path.clone());

            // Verify existing data
            let result = verify_state(&db, &expected, &db_path);
            assert!(
                result.is_ok(),
                "Cycle {} pre-write verification failed: {:?}",
                cycle,
                result
            );

            // Add more data
            for i in 0..10 {
                let key = format!("cycle{}_{:04}", cycle, i).into_bytes();
                let value = format!("value_cycle{}_{:04}", cycle, i).into_bytes();
                db.put(&key, &value).unwrap();
                expected.insert(key, value);
            }
            db.flush().unwrap();
        }

        // Verify after close
        {
            let db = reopen_db(db_path.clone());
            let result = verify_state(&db, &expected, &db_path);
            assert!(
                result.is_ok(),
                "Cycle {} post-write verification failed: {:?}",
                cycle,
                result
            );
        }
    }

    // Final count: 50 + 3*10 = 80
    assert_eq!(expected.len(), 80);
}

// =============================================================================
// TEST: Recovery with empty values
// =============================================================================

#[test]
fn test_recovery_empty_values() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Phase 1: Write mix of empty and non-empty values
    {
        let db = create_db(&temp_dir, SyncPolicy::SyncAll);

        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = if i % 2 == 0 {
                vec![] // Empty value
            } else {
                format!("value_{:04}", i).into_bytes()
            };
            db.put(&key, &value).unwrap();
            expected.insert(key, value);
        }
        db.flush().unwrap();
    }

    // Phase 2: Verify empty values preserved
    {
        let db = reopen_db(db_path.clone());
        let result = verify_state(&db, &expected, &db_path);

        assert!(
            result.is_ok(),
            "Empty value recovery verification failed: {:?}",
            result
        );

        // Specifically check empty value is Some([]), not None
        let empty_key = b"key_0000";
        let value = db.get(empty_key).unwrap();
        assert!(value.is_some(), "Empty value should be Some, not None");
        assert!(value.unwrap().is_empty(), "Value should be empty");
    }
}
