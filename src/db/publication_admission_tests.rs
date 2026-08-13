//! Publication-admission, metadata-delta, and checkpoint recovery tests.

use super::*;
use std::fs;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::MetadataExt;
use tempfile::tempdir;

use crate::storage::format::MANIFEST_SLOT_SIZE;

#[test]
fn test_db_meta_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create and populate.
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    // Meta file should exist.
    assert!(path.join(META_FILE).exists());
}

#[test]
fn test_db_metrics_attribute_page_work_and_lazy_reads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metrics.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();

        let metrics = db.metrics().unwrap();
        assert_eq!(metrics.storage.physical_page_writes, 1);
        assert_eq!(metrics.storage.page_bytes_written, PAGE_SIZE as u64);
        assert_eq!(metrics.storage.generation_flushes, 1);
        assert_eq!(metrics.storage.syncs, 1);
        assert_eq!(metrics.data_bytes, PAGE_SIZE as u64);
        assert_eq!(metrics.wal_bytes, 0);
        assert!(metrics.publication.wal_bytes_written > 0);
        assert!(metrics.publication.metadata_bytes_written > 0);
        assert_eq!(metrics.publication.blob_bytes_written, 0);
        assert!(metrics.publication.history_bytes_written > 0);
        assert_eq!(
            metrics.publication.manifest_bytes_written,
            MANIFEST_SLOT_SIZE as u64
        );
    }

    let reopened = DB::open(&path, Options::default()).unwrap();
    let before = reopened.metrics().unwrap();
    assert_eq!(before.storage.logical_page_reads, 0);
    assert_eq!(before.storage.physical_page_reads, 0);
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    let after = reopened.metrics().unwrap();
    assert_eq!(after.storage.logical_page_reads, 2);
    assert_eq!(after.storage.physical_page_reads, 1);
    assert_eq!(after.storage.page_bytes_read, PAGE_SIZE as u64);
    assert_eq!(after.buffer.reads, 1);
}

#[test]
fn test_db_metadata_delta_reopens_and_preserves_parent_checkpoint() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata-delta.db");

    let snapshot_id;
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        for index in 0..200 {
            db.put(
                format!("key-{index:04}").as_bytes(),
                format!("value-{index:04}").as_bytes(),
            )
            .unwrap();
        }
        db.flush().unwrap();
        let first = db.durability_status();
        snapshot_id = db.retain_commit(first.commit_id).unwrap();
        db.put(b"key-0000", b"updated-value").unwrap();
        db.flush().unwrap();
        assert_eq!(
            db.get(b"key-0000").unwrap(),
            Some(b"updated-value".to_vec())
        );
        assert_eq!(
            db.get_at(snapshot_id, b"key-0000").unwrap(),
            Some(b"value-0000".to_vec())
        );
    }

    let first_checkpoint = path.join("seerdb.meta.1");
    let second_checkpoint = path.join("seerdb.meta.2");
    let first_bytes = fs::read(&first_checkpoint).unwrap();
    let second_bytes = fs::read(&second_checkpoint).unwrap();
    assert!(first_bytes.starts_with(&META_MAGIC));
    assert!(second_bytes.starts_with(&META_DELTA_MAGIC));
    assert!(second_bytes.len() < first_bytes.len());

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get(b"key-0000").unwrap(),
        Some(b"updated-value".to_vec())
    );
    assert_eq!(
        reopened.get_at(snapshot_id, b"key-0000").unwrap(),
        Some(b"value-0000".to_vec())
    );
    reopened.release_snapshot(snapshot_id).unwrap();
    reopened.prune_history().unwrap();
    assert!(first_checkpoint.is_file());
    assert!(second_checkpoint.is_file());
}

#[test]
fn test_db_metadata_delta_corruption_fails_closed() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("corrupt-metadata-delta.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();
    drop(db);

    let checkpoint = path.join("seerdb.meta.2");
    let mut bytes = fs::read(&checkpoint).unwrap();
    assert!(bytes.starts_with(&META_DELTA_MAGIC));
    let valid_delta = bytes.clone();
    bytes.push(0xA5);
    fs::write(&checkpoint, bytes).unwrap();
    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("metadata delta")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Checkpoint,
            ..
        })
    ));

    let mut anchorless = valid_delta;
    anchorless[12..20].fill(0);
    let checksum = crc32c::crc32c(&anchorless[..anchorless.len() - 4]);
    let checksum_offset = anchorless.len() - 4;
    anchorless[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
    fs::write(&checkpoint, anchorless).unwrap();
    let error = match DB::open(&path, Options::default()) {
        Ok(_) => panic!("anchorless metadata delta unexpectedly opened"),
        Err(error) => error,
    };
    assert!(
        matches!(error, Error::Corruption(ref message) if message.contains("no full checkpoint parent")),
        "{error:?}"
    );
}

#[test]
fn test_db_metadata_delta_chain_consolidates_at_hard_limit() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata-delta-chain.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-0").unwrap();
    db.flush().unwrap();

    for revision in 1..=MAX_META_DELTA_CHAIN + 1 {
        db.put(b"key", format!("value-{revision}").as_bytes())
            .unwrap();
        db.flush().unwrap();
    }
    let consolidation_generation = MAX_META_DELTA_CHAIN as u64 + 2;
    let consolidated = path.join(format!("seerdb.meta.{consolidation_generation}"));
    assert!(fs::read(&consolidated).unwrap().starts_with(&META_MAGIC));
    // The inactive slot still names the immediately previous delta
    // frontier. Publish one more generation so that fallback advances
    // beyond the consolidation before pruning the old chain.
    db.put(b"key", b"value-66").unwrap();
    db.flush().unwrap();
    let report = db.prune_history().unwrap();
    assert_eq!(report.removed_checkpoints, MAX_META_DELTA_CHAIN as u64 + 1);
    assert!(consolidated.is_file());
    assert_eq!(db.get(b"key").unwrap(), Some(b"value-66".to_vec()));
    db.close().unwrap();
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-66".to_vec()));
}

#[test]
fn test_compaction_after_metadata_delta_admits_relocation_sidecar() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata-delta-compaction.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();
    assert!(
        fs::read(path.join("seerdb.meta.2"))
            .unwrap()
            .starts_with(&META_DELTA_MAGIC)
    );

    let report = db.compact().unwrap();
    assert_eq!(report.relocated_pages, 1);
    assert!(
        fs::read(path.join("seerdb.meta.3"))
            .unwrap()
            .starts_with(&META_DELTA_MAGIC)
    );
    assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    reopened.verify().unwrap();
}

#[test]
fn test_compaction_final_write_disk_full_reopens_old_root() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("compaction-final-disk-full.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();

    db.inject_final_write_disk_full();
    assert!(matches!(db.compact(), Err(Error::DiskFull)));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    reopened.verify().unwrap();
}

#[test]
fn test_db_wal_admission_rejects_before_blob_or_tree_mutation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-admission.db");
    let value = vec![0xA5; 2_000];
    let record_bytes = WalRecord::put(b"key", &value).to_bytes().len() as u64;
    let exact_budget = record_bytes + WAL_COMMIT_RECORD_BYTES;
    let mut options = Options::for_test();
    options.max_wal_bytes = exact_budget - 1;

    let mut db = DB::open(&path, options).unwrap();
    let error = db.put(b"key", &value).unwrap_err();
    assert!(matches!(
        error,
        Error::Backpressure { required, available }
            if required == exact_budget && available == exact_budget - 1
    ));
    assert_eq!(db.get(b"key").unwrap(), None);
    assert_eq!(db.blob_stats().total_valid, 0);
    assert!(!path.join(WAL_FILE).exists());
    assert!(!db.durability_status().write_fenced);
    assert_eq!(db.metrics().unwrap().wal_admission_failures, 1);

    db.options.max_wal_bytes = exact_budget;
    db.put(b"key", &value).unwrap();
    assert!(path.join(WAL_FILE).is_file());
    assert!(!path.join(WAL_RESERVATION_FILE).exists());
    assert!(fs::metadata(path.join(WAL_FILE)).unwrap().len() < WAL_RESERVATION_SEGMENT_BYTES);
    assert!(path.join(BLOB_RESERVATION_FILE).is_file());
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert!(
        fs::metadata(path.join(BLOB_RESERVATION_FILE))
            .unwrap()
            .blocks()
            > 0,
        "blob reservation should own physical blocks on this platform"
    );
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert!(
        fs::metadata(path.join(WAL_FILE)).unwrap().blocks() > 0,
        "WAL file should own physical blocks on this platform"
    );
    assert_eq!(
        db.metrics().unwrap().wal_reserved_bytes,
        WAL_RESERVATION_SEGMENT_BYTES
    );
    db.flush().unwrap();
    assert!(!path.join(BLOB_RESERVATION_FILE).exists());
    assert!(!path.join(WAL_FILE).exists());
    assert_eq!(db.metrics().unwrap().wal_reserved_bytes, 0);
    drop(db);

    let reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(value));
}

#[test]
fn test_db_removes_legacy_wal_reservation_sidecar_on_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy-wal-reservation.db");
    let db = DB::open(&path, Options::default()).unwrap();
    drop(db);
    fs::write(path.join(WAL_RESERVATION_FILE), [0xA5; 4096]).unwrap();

    let db = DB::open(&path, Options::default()).unwrap();
    assert!(!path.join(WAL_RESERVATION_FILE).exists());
    assert_eq!(db.metrics().unwrap().wal_reserved_bytes, 0);
}

#[test]
fn test_db_blob_admission_rejects_before_blob_or_tree_mutation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("blob-admission.db");
    let value = vec![0x5A; 2_000];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.inject_capacity_limit(0);

    assert!(matches!(db.put(b"key", &value), Err(Error::DiskFull)));
    assert_eq!(db.get(b"key").unwrap(), None);
    assert_eq!(db.blob_stats().total_valid, 0);
    assert_eq!(db.durability_status().pending_mutations, 0);
    assert!(!db.durability_status().write_fenced);

    drop(db);
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
}
