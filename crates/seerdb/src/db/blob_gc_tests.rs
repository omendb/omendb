//! General blob garbage-collection, capacity, and recovery tests.

use super::*;
use tempfile::tempdir;

#[test]
fn test_db_gc() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let mut db = DB::open(&path, Options::default()).unwrap();

    // Write some large values (>1KB threshold).
    let large_value = vec![0xAB; 2000];
    db.put(b"key1", &large_value).unwrap();
    db.put(b"key2", &large_value).unwrap();
    db.put(b"key3", &large_value).unwrap();
    db.flush().unwrap();

    // Check initial stats.
    let stats = db.blob_stats();
    assert_eq!(stats.total_valid, 3);
    assert_eq!(stats.total_deleted, 0);
    assert_eq!(stats.files_needing_gc, 0);

    // Delete some entries.
    db.delete(b"key1").unwrap();
    db.delete(b"key2").unwrap();
    db.flush().unwrap();

    // Check stats after delete.
    let stats = db.blob_stats();
    assert_eq!(stats.total_valid, 1);
    assert_eq!(stats.total_deleted, 2);

    // Run GC.
    let reclaimed = db.gc().unwrap();
    assert_eq!(reclaimed, 3);
    assert_eq!(db.get(b"key3").unwrap(), Some(large_value));

    // Check stats after GC.
    let stats = db.blob_stats();
    assert_eq!(stats.total_valid, 1);
    assert_eq!(stats.total_deleted, 0);
    assert_eq!(stats.files_needing_gc, 0);

    db.delete(b"key3").unwrap();
    db.flush().unwrap();
    assert_eq!(db.gc().unwrap(), 1);
    assert_eq!(db.blob_stats().files_needing_gc, 0);

    drop(db);
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key1").unwrap(), None);
    assert_eq!(reopened.get(b"key2").unwrap(), None);
    assert_eq!(reopened.get(b"key3").unwrap(), None);
}

#[test]
fn test_db_gc_admission_rejects_before_catalog_reclamation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("gc-admission.db");
    let value = vec![0xAB; 2_000];
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", &value).unwrap();
    db.flush().unwrap();
    db.delete(b"key").unwrap();
    db.flush().unwrap();

    let before_bytes = fs::metadata(path.join(BLOB_FILE)).unwrap().len();
    let before_stats = db.blob_stats();
    assert_eq!(before_stats.total_valid, 0);
    assert_eq!(before_stats.total_deleted, 1);
    assert_eq!(before_stats.files_needing_gc, 1);

    db.inject_capacity_limit(0);
    assert!(matches!(db.gc(), Err(Error::DiskFull)));
    assert_eq!(
        fs::metadata(path.join(BLOB_FILE)).unwrap().len(),
        before_bytes
    );
    let after_failed_stats = db.blob_stats();
    assert_eq!(after_failed_stats.total_valid, before_stats.total_valid);
    assert_eq!(after_failed_stats.total_deleted, before_stats.total_deleted);
    assert_eq!(
        after_failed_stats.files_needing_gc,
        before_stats.files_needing_gc
    );
    assert!(!db.durability_status().write_fenced);

    db.inject_capacity_limit(u64::MAX);
    assert_eq!(db.gc().unwrap(), 1);
    assert_eq!(db.blob_stats().files_needing_gc, 0);
}

#[test]
fn test_db_mixed_gc_capacity_refusal_is_retryable_before_candidate_install() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mixed-gc-admission.db");
    let value = vec![0xCD; 2_000];
    let mut db = DB::open(&path, Options::default()).unwrap();

    for key in [b"live".as_slice(), b"dead-1", b"dead-2"] {
        db.put(key, &value).unwrap();
    }
    db.flush().unwrap();
    db.delete(b"dead-1").unwrap();
    db.delete(b"dead-2").unwrap();
    db.flush().unwrap();
    let before = db.blob_stats();
    assert_eq!(before.total_valid, 1);
    assert_eq!(before.total_deleted, 2);
    assert_eq!(before.files_needing_gc, 1);

    let data_capacity = fs::metadata(path.join(DATA_FILE)).unwrap().len();
    db.inject_capacity_limit(data_capacity);
    assert!(matches!(db.gc(), Err(Error::CapacityPreflight)));
    assert!(!db.durability_status().write_fenced);
    assert_eq!(db.blob_stats().total_valid, before.total_valid);
    assert_eq!(db.blob_stats().total_deleted, before.total_deleted);
    assert_eq!(db.get(b"live").unwrap(), Some(value.clone()));

    db.inject_capacity_limit(u64::MAX);
    assert!(db.gc().unwrap() > 0);
    assert_eq!(db.get(b"live").unwrap(), Some(value));
    db.verify().unwrap();
}

#[test]
fn test_db_mixed_gc_final_write_disk_full_reopens_old_root() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mixed-gc-final-disk-full.db");
    let value = vec![0xEF; 2_000];
    let mut db = DB::open(&path, Options::default()).unwrap();
    for key in [b"live".as_slice(), b"dead-1", b"dead-2"] {
        db.put(key, &value).unwrap();
    }
    db.flush().unwrap();
    db.delete(b"dead-1").unwrap();
    db.delete(b"dead-2").unwrap();
    db.flush().unwrap();

    db.inject_final_write_disk_full();
    assert!(matches!(db.gc(), Err(Error::DiskFull)));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"live").unwrap(), Some(value.clone()));
    assert_eq!(reopened.get(b"dead-1").unwrap(), None);
    assert_eq!(reopened.get(b"dead-2").unwrap(), None);
    assert!(reopened.blob_stats().files_needing_gc > 0);
    assert!(reopened.gc().unwrap() > 0);
    reopened.verify().unwrap();
}
