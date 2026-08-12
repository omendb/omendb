//! DB coordinator behavior and recovery tests.

use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::io::{Seek, SeekFrom, Write};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::MetadataExt;
use std::process::Command;
use tempfile::tempdir;

use crate::storage::format::MANIFEST_SLOT_SIZE;

const TEST_SEGMENT_CATALOG_DELTA_LIMIT: u32 = 64;

#[test]
fn test_db_open() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path().join("test.db"), Options::default());
    assert!(db.is_ok());
}

#[test]
fn test_db_open_creates_nested_path_and_reopens() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nested").join("database.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    reopened.close().unwrap();
}

#[test]
fn test_db_rejects_zero_frame_buffer_pool_before_creating_path() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("invalid-options.db");
    let options = Options {
        buffer_pool_size: PAGE_SIZE - 1,
        ..Options::default()
    };

    assert!(matches!(
        DB::open(&path, options),
        Err(Error::InvalidArgument(message)) if message.contains("at least one page")
    ));
    assert!(!path.exists());
}

#[test]
fn test_db_open_rejects_existing_directory_without_storage_artifacts() {
    let dir = tempdir().unwrap();
    let empty_path = dir.path().join("empty.db");
    fs::create_dir(&empty_path).unwrap();
    assert!(matches!(
        DB::open(&empty_path, Options::default()),
        Err(Error::Corruption(message))
            if message.contains("no authoritative storage artifacts")
    ));

    let orphan_path = dir.path().join("orphan.db");
    fs::create_dir(&orphan_path).unwrap();
    fs::write(orphan_path.join(BLOB_FILE), b"orphaned blob image").unwrap();
    assert!(matches!(
        DB::open(&orphan_path, Options::default()),
        Err(Error::Corruption(message))
            if message.contains("no authoritative storage artifacts")
    ));
}

#[test]
fn test_db_open_rejects_missing_manifest_artifacts_without_recreating_them() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("missing-data.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    drop(db);

    fs::remove_file(path.join(DATA_FILE)).unwrap();
    let check = DB::check(&path, Options::default());
    assert!(
        matches!(
            check,
            Err(Error::Check {
                kind: CheckFailureKind::Target,
                ref message
            }) if message.contains("required manifest or data artifacts")
        ),
        "check error: {check:?}"
    );
    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message))
            if message.contains("is missing the data file")
    ));
    assert!(!path.join(DATA_FILE).exists());

    let checkpoint_path = dir.path().join("missing-checkpoint.db");
    let mut db = DB::open(&checkpoint_path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    drop(db);
    let checkpoint = fs::read_dir(&checkpoint_path)
        .unwrap()
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_prefix("seerdb.meta."))
                .and_then(|suffix| suffix.parse::<u64>().ok())
                .is_some()
        })
        .expect("published database has a numbered PMT checkpoint");
    fs::remove_file(&checkpoint).unwrap();

    let check = DB::check(&checkpoint_path, Options::default());
    assert!(matches!(
        check,
        Err(Error::Check {
            kind: CheckFailureKind::Checkpoint,
            ref message
        }) if message.contains("is missing checkpoint")
    ));
    assert!(matches!(
        DB::open(&checkpoint_path, Options::default()),
        Err(Error::Corruption(message))
            if message.contains("is missing checkpoint")
    ));
    assert!(!checkpoint.exists());
}

#[test]
fn test_db_create_refuses_existing_store_without_reinterpreting_it() {
    let dir = tempdir().unwrap();
    let reserved_path = dir.path().join("reserved.db");
    fs::create_dir(&reserved_path).unwrap();
    assert!(matches!(
        DB::create(&reserved_path, Options::default()),
        Err(Error::InvalidArgument(message)) if message.contains("already exists")
    ));

    let path = dir.path().join("nested").join("created.db");
    let mut db = DB::create(&path, Options::default()).unwrap();
    db.put(b"catalog", b"durable").unwrap();
    db.flush().unwrap();
    drop(db);

    assert!(matches!(
        DB::create(&path, Options::default()),
        Err(Error::InvalidArgument(message)) if message.contains("already exists")
    ));
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"catalog").unwrap(), Some(b"durable".to_vec()));
}

#[test]
fn test_db_allows_only_one_writer() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("locked.db");
    let db = DB::open(&path, Options::default()).unwrap();
    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::DatabaseBusy)
    ));
    drop(db);
    assert!(DB::open(&path, Options::default()).is_ok());
}

#[test]
fn test_db_check_is_non_mutating_and_does_not_take_writer_lock() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("check.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"pending", b"value").unwrap();

    let pending = DB::check(&path, Options::default()).unwrap();
    assert_eq!(pending.wal_status, WalCheckStatus::Pending);
    assert_eq!(
        pending.verification.wal_bytes,
        fs::metadata(path.join(WAL_FILE)).unwrap().len()
    );
    assert_eq!(db.get(b"pending").unwrap(), Some(b"value".to_vec()));

    db.flush().unwrap();
    let clean = DB::check(&path, Options::default()).unwrap();
    assert_eq!(clean.wal_status, WalCheckStatus::Clean);
    assert_eq!(clean.verification.wal_bytes, 0);
    db.close().unwrap();
}

#[test]
fn test_db_check_does_not_create_missing_path() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("missing.db");

    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check { kind: CheckFailureKind::Target, message })
            if message.contains("does not exist")
    ));
    assert!(!path.exists());
}

#[test]
fn test_db_put_get() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

    db.put(b"key1", b"value1").unwrap();
    db.put(b"key2", b"value2").unwrap();

    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    assert_eq!(db.get(b"key3").unwrap(), None);
}

#[test]
fn test_db_rejects_key_larger_than_page_format_before_wal() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oversized-key.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    let key = vec![0xA5; MAX_KEY_SIZE + 1];

    assert!(matches!(
        db.put(&key, b"value"),
        Err(Error::InvalidArgument(message))
            if message.contains("maximum B-tree page key size")
    ));
    assert_eq!(db.durability_status().pending_mutations, 0);
    assert!(!path.join(WAL_FILE).exists());
}

#[test]
fn test_db_delete() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

    db.put(b"key", b"value").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));

    db.delete(b"key").unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);
}

#[test]
fn test_db_range() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.put(b"d", b"4").unwrap();

    let results = db.range(b"b", b"d").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b"b");
    assert_eq!(results[1].0, b"c");
}

#[test]
fn test_db_close() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

    db.put(b"key", b"value").unwrap();
    db.close().unwrap();

    // Operations after close should fail.
    assert!(db.put(b"key2", b"value2").is_err());
}

#[test]
fn test_db_close_publishes_pending_wal_and_releases_writer() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("graceful-close.db");

    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    assert!(path.join(WAL_FILE).is_file());

    db.inject_capacity_limit(0);
    assert!(matches!(db.close(), Err(Error::CapacityPreflight)));
    assert_eq!(db.durability_status().pending_mutations, 1);
    assert!(!db.durability_status().write_fenced);
    assert!(path.join(WAL_FILE).is_file());

    db.inject_capacity_limit(u64::MAX);
    db.close().unwrap();

    assert!(!path.join(WAL_FILE).exists());
    assert_eq!(db.durability_status().pending_mutations, 0);
    assert!(matches!(db.get(b"key"), Err(Error::InvalidArgument(_))));

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    reopened.put(b"next", b"value").unwrap();
    reopened.close().unwrap();
}

#[test]
fn test_db_runtime_invariants_cover_pending_generation_lifecycle() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("invariants.db"), Options::default()).unwrap();

    assert!(db.validate_runtime_state().is_ok());
    db.put(b"key", b"value").unwrap();
    assert!(db.validate_runtime_state().is_ok());
    db.flush().unwrap();
    assert!(db.validate_runtime_state().is_ok());

    db.pending_wal_bytes = 1;
    let error = db.get(b"key").unwrap_err();
    assert!(matches!(error, Error::Corruption(message) if message.contains("pending WAL bytes")));
}

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

#[test]
fn test_db_reopen_reads_pmt_pages_on_demand() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    {
        let mut db = DB::open(&path, Options::for_test()).unwrap();
        for index in 0..500 {
            let key = format!("key-{index:06}");
            db.put(key.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();
    }

    let mut db = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(db.engine.btree().node_count(), 0);
    assert_eq!(db.engine.buffer_stats().reads, 0);

    assert_eq!(db.get(b"key-000250").unwrap(), Some(b"value".to_vec()));
    assert!(db.engine.buffer_stats().reads > 0);
    assert_eq!(db.engine.btree().node_count(), 0);

    let range = db.range(b"key-000050", b"key-000450").unwrap();
    assert_eq!(range.len(), 400);
    assert_eq!(range.first().unwrap().0, b"key-000050");
    assert_eq!(range.last().unwrap().0, b"key-000449");
    assert_eq!(db.engine.btree().node_count(), 0);

    db.put(b"key-000250", b"updated").unwrap();
    assert!(db.engine.btree().node_count() > 0);
    db.flush().unwrap();
    assert_eq!(db.get(b"key-000250").unwrap(), Some(b"updated".to_vec()));
}

#[test]
fn test_db_sparse_mutation_overlay_preserves_unloaded_ranges() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sparse-mutation.db");

    {
        let mut db = DB::open(&path, Options::for_test()).unwrap();
        for index in 0..500 {
            let key = format!("key-{index:06}");
            let value = format!("value-{index:06}");
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }

    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let durable_page_count = db.engine.pmt().iter().count();
    db.put(b"key-000250", b"updated-250").unwrap();
    db.put(b"key-000450", b"updated-450").unwrap();
    assert!(db.engine.btree().node_count() < durable_page_count);
    assert_eq!(
        db.get(b"key-000250").unwrap(),
        Some(b"updated-250".to_vec())
    );
    assert_eq!(
        db.get(b"key-000450").unwrap(),
        Some(b"updated-450".to_vec())
    );

    let before_delete = db.range(b"key-000240", b"key-000460").unwrap();
    assert_eq!(before_delete.len(), 220);
    assert_eq!(
        before_delete
            .iter()
            .find(|(key, _)| key == b"key-000250")
            .map(|(_, value)| value.as_slice()),
        Some(b"updated-250".as_slice())
    );

    assert!(db.delete(b"key-000300").unwrap());
    let after_delete = db.range(b"key-000240", b"key-000460").unwrap();
    assert_eq!(after_delete.len(), 219);
    assert!(!after_delete.iter().any(|(key, _)| key == b"key-000300"));
    db.flush().unwrap();
    drop(db);

    let reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(
        reopened.get(b"key-000250").unwrap(),
        Some(b"updated-250".to_vec())
    );
    assert_eq!(reopened.get(b"key-000300").unwrap(), None);
    assert_eq!(
        reopened.get(b"key-000450").unwrap(),
        Some(b"updated-450".to_vec())
    );
}

#[test]
fn test_db_sparse_mutation_overlay_split_reopens() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sparse-split.db");

    {
        let mut db = DB::open(&path, Options::for_test()).unwrap();
        for index in 0..500 {
            let key = format!("key-{index:06}");
            db.put(key.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();
    }

    let mut db = DB::open(&path, Options::for_test()).unwrap();
    for index in 0..120 {
        let key = format!("key-000250-new-{index:03}");
        db.put(key.as_bytes(), b"new-value").unwrap();
    }
    assert!(db.engine.btree().dirty_page_ids().len() > 1);
    assert_eq!(
        db.range(b"key-000250-new-000", b"key-000250-new-120")
            .unwrap()
            .len(),
        120
    );
    db.flush().unwrap();
    drop(db);

    let reopened = DB::open(&path, Options::for_test()).unwrap();
    let values = reopened
        .range(b"key-000250-new-000", b"key-000250-new-120")
        .unwrap();
    assert_eq!(values.len(), 120);
    assert_eq!(
        values[0],
        (b"key-000250-new-000".to_vec(), b"new-value".to_vec())
    );
    assert_eq!(values[119].0, b"key-000250-new-119");
}

#[test]
fn test_db_sparse_deep_internal_split_does_not_require_unloaded_children() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sparse-deep-split.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        for index in 0..2_000 {
            let key = format!("key-{index:06}");
            db.put(key.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();
    }

    let mut db = DB::open(&path, Options::default()).unwrap();
    for index in 0..600 {
        let key = format!("key-000800-new-{index:04}");
        db.put(key.as_bytes(), b"new-value").unwrap();
    }
    db.flush().unwrap();
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened
            .range(b"key-000800-new-0000", b"key-000800-new-0600")
            .unwrap()
            .len(),
        600
    );
}

#[test]
fn test_db_rejects_malformed_meta_container() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let checkpoint = path.join("seerdb.meta.1");
    let mut meta = fs::read(&checkpoint).unwrap();
    meta.push(0xA5);
    fs::write(&checkpoint, meta).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("checksum")
    ));
}

#[test]
fn test_db_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Write data and close.
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        let initial = db.durability_status();
        assert_eq!(initial.generation_id.get(), 0);
        assert_eq!(initial.commit_id.get(), 0);
        db.put(b"key1", b"value1").unwrap();
        assert_eq!(db.durability_status().pending_mutations, 1);
        db.flush().unwrap();
        let published = db.durability_status();
        assert_eq!(published.generation_id.get(), 1);
        assert_eq!(published.commit_id.get(), 1);
        assert_eq!(published.pending_mutations, 0);
        db.put(b"key2", b"value2").unwrap();
        db.put(b"key3", b"value3").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Reopen and verify data persisted.
    {
        let db = DB::open(&path, Options::default()).unwrap();
        let status = db.durability_status();
        assert_eq!(status.generation_id.get(), 2);
        assert_eq!(status.commit_id.get(), 2);
        assert_eq!(status.pending_mutations, 0);
        assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));
    }
}

#[test]
fn test_db_rejects_corrupt_page_checksum() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let data_path = path.join(DATA_FILE);
    let mut data = fs::read(&data_path).unwrap();
    assert!(data.len() >= crate::btree::PAGE_SIZE);
    data[crate::btree::PAGE_SIZE - 1] ^= 0x01;
    fs::write(&data_path, data).unwrap();

    let db = DB::open(&path, Options::default()).unwrap();
    let result = db.get(b"key");
    assert!(matches!(
        result,
        Err(Error::Corruption(message)) if message.contains("checksum mismatch")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::DataPage,
            ..
        })
    ));
}

#[test]
fn test_db_discards_uncommitted_wal_suffix() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Write data (WAL is written to disk on each put).
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();
        db.put(b"key3", b"value3").unwrap();
        // Don't flush — simulate crash.
        // WAL should be on disk.
    }

    // Verify WAL exists.
    assert!(path.join(WAL_FILE).exists(), "WAL should exist after put");

    // Reopen and verify uncommitted mutations are not visible.
    {
        let db = DB::open(&path, Options::default()).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), None);
        assert_eq!(db.get(b"key2").unwrap(), None);
        assert_eq!(db.get(b"key3").unwrap(), None);
    }

    // The uncommitted WAL suffix can be discarded after reopen.
    assert!(
        !path.join(WAL_FILE).exists(),
        "WAL should be deleted after recovery"
    );
}

#[test]
fn test_db_process_crash_recovery() {
    if let Some(path) = std::env::var_os("SEERDB_CRASH_CHILD_PATH") {
        let path = PathBuf::from(path);
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"published", b"value-before-crash").unwrap();
        db.flush().unwrap();
        db.put(b"unpublished", b"value-after-wal-only").unwrap();

        // Exit without running Rust destructors. This leaves the WAL
        // mutation on disk while the manifest still names the prior
        // published generation, matching an abrupt process termination.
        std::process::exit(137);
    }

    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("db::tests::test_db_process_crash_recovery")
        .arg("--nocapture")
        .env("SEERDB_CRASH_CHILD_PATH", &path)
        .status()
        .unwrap();
    assert!(!status.success(), "crash child unexpectedly exited cleanly");

    let db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        db.get(b"published").unwrap(),
        Some(b"value-before-crash".to_vec())
    );
    assert_eq!(db.get(b"unpublished").unwrap(), None);
    assert!(!path.join(WAL_FILE).exists());
}

#[test]
fn test_db_randomized_publication_fault_matrix() {
    fn next(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *seed
    }

    fn assert_model(db: &DB, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
        for key_id in 0..16 {
            let key = format!("key-{key_id:02}");
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                model.get(key.as_bytes()).cloned()
            );
        }
    }

    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    let mut committed = BTreeMap::new();
    let mut seed = 0x5EED_CAFE_u64;

    for round in 0..32 {
        let mut candidate = committed.clone();
        let operation_count = (next(&mut seed) % 4 + 1) as usize;
        for operation in 0..operation_count {
            let key_id = next(&mut seed) % 16;
            let key = format!("key-{key_id:02}");
            let value = format!("value-{round:02}-{operation:02}-{key_id:02}");
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            candidate.insert(key.into_bytes(), value.into_bytes());
        }

        let fault = next(&mut seed) % 4;
        match fault {
            1 => db.engine.inject_sync_failure(),
            2 => db.engine.inject_write_failure(),
            3 => inject_atomic_rename_failure(),
            _ => {}
        }

        let result = db.flush();
        if fault == 0 {
            result.unwrap();
            committed = candidate;
            assert_model(&db, &committed);
        } else {
            assert!(result.is_err(), "fault {fault} did not fail publication");
            drop(db);
            db = DB::open(&path, Options::default()).unwrap();
            assert_model(&db, &committed);
        }
    }

    db.close().unwrap();
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_model(&reopened, &committed);
}

#[test]
fn test_db_recovers_committed_wal_prefix_with_torn_suffix() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let records = vec![
        WalRecord::put(b"key1", b"value1"),
        WalRecord::put(b"key2", b"value2"),
        WalRecord::put(b"key3", b"value3"),
    ];
    let references: Vec<_> = records.iter().collect();
    let commit = CommitRecord {
        commit_id: CommitId::new(1),
        generation_id: GenerationId::new(1),
        root_page_id: 0,
        mutation_count: records.len() as u64,
        digest: digest_records(&references),
    };
    let mut wal_bytes = Vec::new();
    for record in &records {
        wal_bytes.extend_from_slice(&record.to_bytes());
    }
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    wal_bytes.extend_from_slice(&[0xA5, 0x5A, 0x01]);
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

    let db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));
    assert!(!path.join(WAL_FILE).exists());
    assert!(path.join(MANIFEST_FILE).exists());
}

#[test]
fn test_db_reopen_accepts_every_wal_truncation_prefix() {
    let records = vec![
        WalRecord::put(b"key1", b"value1"),
        WalRecord::put(b"key2", b"value2"),
        WalRecord::put(b"key3", b"value3"),
    ];
    let references: Vec<_> = records.iter().collect();
    let commit = CommitRecord {
        commit_id: CommitId::new(1),
        generation_id: GenerationId::new(1),
        root_page_id: 0,
        mutation_count: records.len() as u64,
        digest: digest_records(&references),
    };
    let mut committed_wal = Vec::new();
    for record in &records {
        committed_wal.extend_from_slice(&record.to_bytes());
    }
    committed_wal.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    let committed_len = committed_wal.len();
    committed_wal.extend_from_slice(&[0xA5, 0x5A, 0x01]);

    for cut in 0..=committed_wal.len() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(WAL_FILE), &committed_wal[..cut]).unwrap();

        let db = DB::open(&path, Options::default())
            .unwrap_or_else(|error| panic!("WAL prefix at byte {cut} failed to reopen: {error:?}"));
        let committed = cut >= committed_len;
        assert_eq!(
            db.get(b"key1").unwrap(),
            committed.then(|| b"value1".to_vec()),
            "cut={cut}"
        );
        assert_eq!(
            db.get(b"key2").unwrap(),
            committed.then(|| b"value2".to_vec()),
            "cut={cut}"
        );
        assert_eq!(
            db.get(b"key3").unwrap(),
            committed.then(|| b"value3".to_vec()),
            "cut={cut}"
        );
    }
}

#[test]
fn test_db_rejects_wal_commit_digest_mismatch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let record = WalRecord::put(b"key", b"value");
    let references = vec![&record];
    let commit = CommitRecord {
        commit_id: CommitId::new(1),
        generation_id: GenerationId::new(1),
        root_page_id: 0,
        mutation_count: 1,
        digest: digest_records(&references) ^ 1,
    };
    let mut wal_bytes = record.to_bytes();
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

    let result = DB::open(&path, Options::default());
    assert!(matches!(
        result,
        Err(Error::Corruption(message)) if message.contains("WAL commit")
    ));
}

#[test]
fn test_db_rejects_when_both_manifest_slots_are_corrupt() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let manifest_path = path.join(MANIFEST_FILE);
    let mut file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
    for slot in 0..2 {
        file.seek(SeekFrom::Start((slot * MANIFEST_SLOT_SIZE) as u64))
            .unwrap();
        file.write_all(&[0xA5; MANIFEST_SLOT_SIZE]).unwrap();
    }
    file.sync_all().unwrap();

    let result = DB::open(&path, Options::default());
    assert!(matches!(result, Err(Error::Corruption(_))));
}

#[test]
fn test_db_fences_writer_after_sync_failure() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.engine.inject_sync_failure();

    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert!(matches!(
        db.put(b"another", b"value"),
        Err(Error::NeedsRecovery(_))
    ));
    assert!(matches!(
        db.get(b"key"),
        Err(Error::NeedsRecovery(message)) if message.contains("reads fenced")
    ));
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
}

#[test]
fn test_db_fences_writer_after_page_write_failure() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.engine.inject_write_failure();

    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert!(matches!(
        db.put(b"another", b"value"),
        Err(Error::NeedsRecovery(_))
    ));
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
}

#[test]
fn test_db_fences_writer_after_disk_full() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.engine.inject_disk_full();

    assert!(matches!(db.flush(), Err(Error::DiskFull)));
    assert!(matches!(
        db.put(b"another", b"value"),
        Err(Error::NeedsRecovery(_))
    ));
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
}

#[test]
fn test_db_capacity_preflight_is_retryable_without_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("retryable-capacity.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();

    let capacity = fs::metadata(path.join(DATA_FILE)).unwrap().len();
    db.inject_capacity_limit(capacity);
    db.put(b"key", b"value-2").unwrap();

    assert!(matches!(db.flush(), Err(Error::CapacityPreflight)));
    assert!(!db.durability_status().write_fenced);
    assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));

    db.inject_capacity_limit(u64::MAX);
    db.flush().unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));

    drop(db);
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
}

#[test]
fn test_db_discards_wal_after_atomic_rename_failure() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    inject_atomic_rename_failure();

    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert!(matches!(
        db.put(b"another", b"value"),
        Err(Error::NeedsRecovery(_))
    ));
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
    assert!(!path.join(WAL_FILE).exists());
}

#[test]
fn test_db_retains_manifest_fallback_before_reusing_pages() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("manifest-retention.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();

    // The next generation can reuse a page from before the current
    // generation, but only after both manifest slots have been fenced to
    // the current root. Fail before the new manifest is published.
    db.put(b"key", b"value-3").unwrap();
    inject_atomic_rename_failure();
    assert!(matches!(db.flush(), Err(Error::Io(_))));
    drop(db);

    // Simulate loss of the newest manifest slot. The mirrored fallback
    // must still name value-2 even though the failed generation reused an
    // older physical page.
    let manifest_path = path.join(MANIFEST_FILE);
    let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
    manifest_file
        .seek(SeekFrom::Start(MANIFEST_SLOT_SIZE as u64))
        .unwrap();
    manifest_file
        .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
        .unwrap();
    manifest_file.sync_all().unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
}

#[test]
fn test_db_prune_history_preserves_inactive_manifest_checkpoint() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("prune-fallback.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();

    let first_checkpoint = path.join("seerdb.meta.1");
    assert!(first_checkpoint.is_file());
    db.prune_history().unwrap();
    assert!(first_checkpoint.is_file());
    db.close().unwrap();

    // The newest slot is corrupt, so reopen must use the independently
    // valid older slot whose checkpoint pruning was required to preserve.
    let manifest_path = path.join(MANIFEST_FILE);
    let manifest_file = OpenOptions::new().read(true).open(&manifest_path).unwrap();
    let mut newest = None;
    for slot in 0..2 {
        let mut bytes = [0; MANIFEST_SLOT_SIZE];
        read_exact_at(
            &manifest_file,
            (slot * MANIFEST_SLOT_SIZE) as u64,
            &mut bytes,
        )
        .unwrap();
        if let Some(manifest) = Manifest::from_bytes(&bytes).unwrap()
            && newest.is_none_or(|(_, current)| manifest.is_newer_than(current))
        {
            newest = Some((slot, manifest));
        }
    }
    let newest_slot = newest.expect("published database has a newest manifest").0;
    drop(manifest_file);
    let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
    manifest_file
        .seek(SeekFrom::Start((newest_slot * MANIFEST_SLOT_SIZE) as u64))
        .unwrap();
    manifest_file
        .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
        .unwrap();
    manifest_file.sync_all().unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
}

#[test]
fn test_db_history_prune_directory_failure_reopens_and_retries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("prune-directory-failure.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-0").unwrap();
    db.flush().unwrap();
    for revision in 1..=MAX_META_DELTA_CHAIN + 1 {
        db.put(b"key", format!("value-{revision}").as_bytes())
            .unwrap();
        db.flush().unwrap();
    }
    db.put(b"key", b"value-final").unwrap();
    db.flush().unwrap();
    let obsolete_checkpoint = path.join("seerdb.meta.1");
    assert!(obsolete_checkpoint.is_file());

    db.inject_history_prune_directory_sync_failure();
    assert!(matches!(db.prune_history(), Err(Error::Io(_))));
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-final".to_vec()));
    reopened.verify().unwrap();
    let report = reopened.prune_history().unwrap();
    assert_eq!(report.removed_checkpoints, 0);
    reopened.close().unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-final".to_vec()));
    assert!(!obsolete_checkpoint.is_file());
}

#[test]
fn test_db_gc_mirrors_manifest_before_removing_dead_blob_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("gc-fallback.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"blob", &vec![0xA5; 2_000]).unwrap();
    db.flush().unwrap();
    db.delete(b"blob").unwrap();
    db.flush().unwrap();

    assert_eq!(db.gc().unwrap(), 1);
    db.close().unwrap();

    // GC is a maintenance mutation of the blob artifact without a new
    // logical commit. Losing the newest slot must still reopen the
    // manifest whose blob image is present, rather than a stale root
    // whose record was just removed.
    let manifest_path = path.join(MANIFEST_FILE);
    let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
    manifest_file
        .seek(SeekFrom::Start(MANIFEST_SLOT_SIZE as u64))
        .unwrap();
    manifest_file
        .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
        .unwrap();
    manifest_file.sync_all().unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"blob").unwrap(), None);
}

#[test]
fn test_db_mirror_manifest_sync_failure_precedes_page_reuse() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("manifest-mirror-fault.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();
    let data_bytes_before = fs::metadata(path.join(DATA_FILE)).unwrap().len();

    db.put(b"key", b"value-3").unwrap();
    db.inject_manifest_mirror_sync_failure();
    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert_eq!(
        fs::metadata(path.join(DATA_FILE)).unwrap().len(),
        data_bytes_before
    );
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    reopened.verify().unwrap();
}

#[test]
fn test_db_append_only_publication_skips_manifest_mirror() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("append-only-no-mirror.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value").unwrap();
    db.inject_manifest_mirror_sync_failure();
    db.flush().unwrap();

    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    db.verify().unwrap();
}

#[test]
fn test_db_blob_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create a large value (>1KB threshold).
    let large_value = vec![0xAB; 2000];

    // Write large value and close.
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key1", &large_value).unwrap();
        db.put(b"key2", b"small").unwrap();
        db.flush().unwrap();
        let replacement = vec![0xCD; 3_000];
        db.put(b"key1", &replacement).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(replacement.clone()));
        db.flush().unwrap();
        assert!(db.blob_stats().total_deleted > 0);
        assert_eq!(db.get(b"key1").unwrap(), Some(replacement));
        assert_eq!(
            db.range(b"key1", b"key3").unwrap(),
            vec![
                (b"key1".to_vec(), vec![0xCD; 3_000]),
                (b"key2".to_vec(), b"small".to_vec()),
            ]
        );
        db.close().unwrap();
    }

    // Verify blob file exists.
    assert!(path.join(BLOB_FILE).exists(), "blob file should exist");

    // Reopen and verify blob data persisted.
    {
        let db = DB::open(&path, Options::default()).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(vec![0xCD; 3_000]));
        assert_eq!(db.get(b"key2").unwrap(), Some(b"small".to_vec()));
        assert_eq!(
            db.range(b"key1", b"key3").unwrap(),
            vec![
                (b"key1".to_vec(), vec![0xCD; 3_000]),
                (b"key2".to_vec(), b"small".to_vec()),
            ]
        );
    }
}

#[test]
fn test_db_recovers_committed_blob_upsert() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("blob-recovery.db");
    let initial = vec![0x11; 2_000];
    let replacement = vec![0x22; 3_000];

    let (commit_id, generation_id, root_page_id) = {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", &initial).unwrap();
        db.flush().unwrap();

        (
            db.commit_id.get(),
            db.generation_id.get(),
            db.engine.btree().root_id() as u64,
        )
    };

    let record = WalRecord::put(b"key", &replacement);
    let commit = CommitRecord {
        commit_id: CommitId::new(commit_id + 1),
        generation_id: GenerationId::new(generation_id + 1),
        root_page_id,
        mutation_count: 1,
        digest: digest_records(&[&record]),
    };
    let mut wal_bytes = record.to_bytes();
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(replacement));
}

#[test]
fn test_db_discards_wal_commit_already_published_by_manifest() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("stale-authoritative-wal.db");

    let (commit_id, generation_id, root_page_id) = {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();

        (
            db.commit_id,
            db.generation_id,
            db.engine.btree().root_id() as u64,
        )
    };

    // Model the crash window where manifest publication succeeded but WAL
    // cleanup did not. Replaying this commit would publish the same
    // logical state under a new generation.
    let record = WalRecord::put(b"key", b"value");
    let commit = CommitRecord {
        commit_id,
        generation_id,
        root_page_id,
        mutation_count: 1,
        digest: digest_records(&[&record]),
    };
    let mut wal_bytes = record.to_bytes();
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    assert_eq!(reopened.commit_id, commit_id);
    assert_eq!(reopened.generation_id, generation_id);
    assert_eq!(reopened.metrics().unwrap().storage.generation_flushes, 0);
    assert!(!path.join(WAL_FILE).exists());
}

#[test]
fn test_db_recovers_committed_large_blob_value() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("large-blob-recovery.db");
    let value = vec![0x7B; 70_000];
    let record = WalRecord::put(b"large-key", &value);
    let commit = CommitRecord {
        commit_id: CommitId::new(1),
        generation_id: GenerationId::new(1),
        root_page_id: 0,
        mutation_count: 1,
        digest: digest_records(&[&record]),
    };
    let mut wal_bytes = record.to_bytes();
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"large-key").unwrap(), Some(value));
}

#[test]
fn test_db_replays_legacy_wal_put_record() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy-wal.db");
    let mut payload = Vec::new();
    payload.extend_from_slice(&(3u16).to_le_bytes());
    payload.extend_from_slice(b"key");
    payload.extend_from_slice(&(5u16).to_le_bytes());
    payload.extend_from_slice(b"value");
    let record = WalRecord::new(RecordType::Put, payload);
    let commit = CommitRecord {
        commit_id: CommitId::new(1),
        generation_id: GenerationId::new(1),
        root_page_id: 0,
        mutation_count: 1,
        digest: digest_records(&[&record]),
    };
    let mut wal_bytes = record.to_bytes();
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn test_db_transaction() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let db = DB::open(&path, Options::default()).unwrap();

    // Begin a transaction.
    let mut txn = db.begin_transaction();
    assert!(txn.is_active());
    assert_eq!(txn.id(), 1);

    // Commit the transaction.
    db.commit_transaction(&mut txn);
    assert!(!txn.is_active());
    assert_eq!(db.latest_committed_txn(), 1);

    // Begin another transaction.
    let mut txn2 = db.begin_transaction();
    assert_eq!(txn2.id(), 2);
    assert_eq!(txn2.snapshot_id(), 1); // Can see txn 1

    // Abort the transaction.
    db.abort_transaction(&mut txn2);
    assert!(!txn2.is_active());
    assert_eq!(db.latest_committed_txn(), 1); // Still 1
}

#[test]
fn test_db_vacuum_step_is_bounded_and_crash_safe() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bounded-vacuum.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    for index in 0..12 {
        let key = format!("key-{index:02}");
        db.put(key.as_bytes(), b"value").unwrap();
    }
    db.flush().unwrap();
    let before = db.durability_status();

    let progress = db.vacuum_step(3).unwrap();
    assert!(!progress.complete);
    assert_eq!(progress.scanned_entries, 3);
    assert_eq!(progress.live_entries, 3);
    assert_eq!(progress.logical_pages_after, None);
    assert_eq!(db.durability_status(), before);
    assert!(matches!(
        db.put(b"blocked", b"write"),
        Err(Error::MaintenanceInProgress("logical vacuum"))
    ));

    // Dropping an incomplete candidate must not publish or fence the old
    // generation. The drop path also exercises the close-time retry.
    drop(db);
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key-00").unwrap(), Some(b"value".to_vec()));
    assert_eq!(reopened.durability_status(), before);

    let mut completed = false;
    while !completed {
        let progress = reopened.vacuum_step(2).unwrap();
        completed = progress.complete;
        if completed {
            assert_eq!(progress.live_entries, 12);
            assert_eq!(progress.logical_pages_after, Some(1));
        } else {
            assert_eq!(progress.logical_pages_after, None);
        }
    }
    assert_eq!(reopened.range(b"key-00", b"key-99").unwrap().len(), 12);
    reopened.close().unwrap();

    let verified = DB::open(&path, Options::default()).unwrap();
    assert_eq!(verified.range(b"key-00", b"key-99").unwrap().len(), 12);
}

#[test]
fn test_db_vacuum_can_be_cancelled_without_publication() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cancel-vacuum.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    for index in 0..4 {
        let key = format!("key-{index}");
        db.put(key.as_bytes(), b"value").unwrap();
    }
    db.flush().unwrap();
    let before = db.durability_status();

    assert!(!db.vacuum_step(1).unwrap().complete);
    assert!(db.cancel_vacuum().unwrap());
    assert!(!db.cancel_vacuum().unwrap());
    assert_eq!(db.durability_status(), before);
    db.put(b"after-cancel", b"value").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"after-cancel").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn test_db_vacuum_final_write_disk_full_reopens_old_root() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("vacuum-final-disk-full.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();

    db.inject_final_write_disk_full();
    assert!(matches!(db.vacuum(), Err(Error::DiskFull)));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    reopened.verify().unwrap();
}

#[test]
fn test_db_concurrent_transactions() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let db = DB::open(&path, Options::default()).unwrap();
    let db = std::sync::Arc::new(db);
    let mut handles = vec![];

    // Spawn multiple threads that create transactions.
    for _ in 0..10 {
        let db = std::sync::Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let mut txn = db.begin_transaction();
            // Simulate some work.
            std::thread::yield_now();
            db.commit_transaction(&mut txn);
            txn.id()
        }));
    }

    // Wait for all threads to complete.
    let mut ids = vec![];
    for handle in handles {
        ids.push(handle.join().unwrap());
    }

    // All transactions should have unique IDs.
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 10);

    // Latest committed should be the max ID.
    assert_eq!(db.latest_committed_txn(), 10);
}

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

#[test]
fn segmented_catalog_consolidation_bound_is_explicit() {
    let mut blobs = BlobManager::with_threshold_and_mode(1, true);
    let mut pointers = Vec::with_capacity(MAX_SEGMENTED_CATALOG_DELETED_ENTRIES + 1);
    for index in 0..=MAX_SEGMENTED_CATALOG_DELETED_ENTRIES {
        pointers.push(
            blobs
                .append(&index.to_le_bytes(), vec![index as u8; 2])
                .unwrap(),
        );
    }

    for pointer in pointers.iter().take(MAX_SEGMENTED_CATALOG_DELETED_ENTRIES) {
        assert!(blobs.mark_deleted(pointer));
    }
    assert!(!segmented_catalog_needs_consolidation(&blobs));
    assert!(blobs.mark_deleted(&pointers[MAX_SEGMENTED_CATALOG_DELETED_ENTRIES]));
    assert!(segmented_catalog_needs_consolidation(&blobs));
}

#[test]
fn segmented_catalog_consolidation_runs_as_explicit_maintenance() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("segmented-catalog-consolidation.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 1,
        ..Options::default()
    };
    let mut db = DB::open(&path, options).unwrap();
    db.blobs.set_segment_target_size_for_test(1_250);

    let groups = (MAX_SEGMENTED_CATALOG_DELETED_ENTRIES / 4) + 1;
    let total = groups * 10;
    let puts = (0..total)
        .map(|index| BatchMutation::Put {
            key: index.to_le_bytes().to_vec(),
            value: vec![index as u8; 100],
        })
        .collect::<Vec<_>>();
    db.commit_batch(&puts).unwrap();

    let deletes = (0..total)
        .filter(|index| index % 10 < 4)
        .map(|index| BatchMutation::Delete {
            key: index.to_le_bytes().to_vec(),
        })
        .collect::<Vec<_>>();
    db.commit_batch(&deletes).unwrap();

    let before = db.blob_stats();
    assert_eq!(before.files_needing_gc, 0);
    assert_eq!(before.total_deleted, deletes.len());
    assert!(before.catalog_needs_consolidation);
    assert!(db.gc().unwrap() > 0);

    let after = db.blob_stats();
    assert_eq!(after.total_deleted, 0);
    assert!(!after.catalog_needs_consolidation);
    assert_eq!(db.get(&0usize.to_le_bytes()).unwrap(), None);
    assert_eq!(db.get(&4usize.to_le_bytes()).unwrap(), Some(vec![4; 100]));
    db.verify().unwrap();
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(&0usize.to_le_bytes()).unwrap(), None);
    assert_eq!(
        reopened.get(&4usize.to_le_bytes()).unwrap(),
        Some(vec![4; 100])
    );
    assert!(!reopened.blob_stats().catalog_needs_consolidation);
    reopened.verify().unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_shrink_iters: 1_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn segmented_rollover_preserves_catalog_and_records(
        target_delta in 0u16..768,
        values in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 1..65),
            1..17
        )
    ) {
        let target = 256 + u64::from(target_delta);
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-rollover-property.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 0,
            ..Options::for_test()
        };
        let mut db = DB::create(&path, options).unwrap();
        db.blobs.set_segment_target_size_for_test(target);

        let mut mutations = Vec::with_capacity(values.len() + 2);
        for (index, value) in values.into_iter().enumerate() {
            mutations.push(BatchMutation::Put {
                key: format!("rollover-{index:04}").into_bytes(),
                value,
            });
        }

        // Two records that each fit in one segment but cannot fit
        // together guarantee that the generated run exercises rollover.
        let forced_value = vec![0xD7; target as usize / 2];
        mutations.push(BatchMutation::Put {
            key: b"rollover-forced-a".to_vec(),
            value: forced_value.clone(),
        });
        mutations.push(BatchMutation::Put {
            key: b"rollover-forced-b".to_vec(),
            value: forced_value,
        });

        let expected = mutations
            .iter()
            .filter_map(|mutation| match mutation {
                BatchMutation::Put { key, value } => Some((key.clone(), value.clone())),
                BatchMutation::Delete { .. } => None,
            })
            .collect::<BTreeMap<_, _>>();
        db.commit_batch(&mutations).unwrap();
        let segment_ids = db.blobs.segment_file_ids();
        assert!(segment_ids.len() >= 2);
        for file_id in &segment_ids {
            assert!(
                db.blobs.segment_bytes(*file_id).unwrap().len() <= target as usize,
                "segment {file_id} exceeded target {target}"
            );
        }

        let deletes = expected
            .keys()
            .enumerate()
            .filter(|(index, _)| index % 4 == 0)
            .map(|(_, key)| BatchMutation::Delete { key: key.clone() })
            .collect::<Vec<_>>();
        db.commit_batch(&deletes).unwrap();
        let mut expected = expected;
        for mutation in &deletes {
            if let BatchMutation::Delete { key } = mutation {
                expected.remove(key);
            }
        }
        db.verify().unwrap();
        assert_eq!(db.blob_stats().total_deleted, deletes.len());
        drop(db);

        let mut reopened = DB::open(&path, Options::for_test()).unwrap();
        for (key, value) in &expected {
            assert_eq!(reopened.get(key).unwrap(), Some(value.clone()));
        }
        assert_eq!(reopened.blob_stats().total_deleted, deletes.len());
        reopened.verify().unwrap();
    }
}

#[test]
fn test_db_segmented_blob_layout_reopens_and_verifies() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("segmented.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 4,
        ..Options::default()
    };
    let value = vec![0xA7; 2_000];

    let mut db = DB::create(&path, options.clone()).unwrap();
    db.put(b"key", &value).unwrap();
    db.close().unwrap();
    assert!(path.join(BLOB_FILE).is_file());
    assert!(blob_segment_path(&path, 1).is_file());

    let catalog = fs::read(path.join(BLOB_FILE)).unwrap();
    assert!(BlobManager::is_segment_catalog(&catalog));
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(value.clone()));
    reopened.verify().unwrap();

    let retained = reopened.retain_current().unwrap();
    assert_eq!(retained.get(b"key").unwrap(), Some(value.clone()));
    let replacement = vec![0xB8; 2_100];
    reopened.put(b"key", &replacement).unwrap();
    reopened.close().unwrap();
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(replacement));
    reopened.verify().unwrap();

    let archive = dir.path().join("segmented-archive");
    reopened.snapshot(&archive).unwrap();
    reopened.close().unwrap();
    let mut archived = DB::open(&archive, Options::default()).unwrap();
    assert_eq!(archived.get(b"key").unwrap(), Some(vec![0xB8; 2_100]));
    archived.verify().unwrap();
}

#[test]
fn test_db_segmented_blob_rewrite_failure_restores_catalog() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("segmented-gc.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 4,
        ..Options::default()
    };
    let value = vec![0xC1; 2_000];
    let mut db = DB::create(&path, options).unwrap();
    for key in [b"live".as_slice(), b"dead-1", b"dead-2"] {
        db.put(key, &value).unwrap();
    }
    db.flush().unwrap();
    db.delete(b"dead-1").unwrap();
    db.delete(b"dead-2").unwrap();
    db.flush().unwrap();

    db.inject_after_blob_rewrite_image_failure();
    assert!(db.gc().is_err());
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"live").unwrap(), Some(value.clone()));
    assert_eq!(reopened.get(b"dead-1").unwrap(), None);
    assert!(reopened.blob_stats().files_needing_gc > 0);
    assert!(reopened.gc().unwrap() > 0);
    reopened.verify().unwrap();
}

#[test]
fn test_db_segmented_gc_prunes_unreferenced_segments() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("segmented-gc-prune.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 4,
        ..Options::default()
    };
    let value = vec![0xD2; 2_000];
    let mut db = DB::create(&path, options).unwrap();
    db.put(b"live", &value).unwrap();
    db.put(b"dead-1", &value).unwrap();
    db.put(b"dead-2", &value).unwrap();
    db.flush().unwrap();
    let old_segment = blob_segment_path(&path, 1);
    assert!(old_segment.is_file());

    db.delete(b"dead-1").unwrap();
    db.delete(b"dead-2").unwrap();
    db.flush().unwrap();
    assert!(db.gc().unwrap() > 0);
    assert!(blob_segment_path(&path, 2).is_file());
    assert!(!old_segment.exists());
    assert_eq!(db.get(b"live").unwrap(), Some(value.clone()));
    db.verify().unwrap();
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert!(!old_segment.exists());
    assert_eq!(reopened.get(b"live").unwrap(), Some(value));
    reopened.verify().unwrap();
}

#[test]
fn test_db_segmented_append_failure_ignores_orphan_suffix() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("segmented-append-failure.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 4,
        ..Options::default()
    };
    let base = vec![0xE3; 2_000];
    let pending = vec![0xE4; 2_100];
    let mut db = DB::create(&path, options).unwrap();
    db.put(b"base", &base).unwrap();
    db.flush().unwrap();
    let segment = blob_segment_path(&path, 1);
    let catalog_before = fs::read(path.join(BLOB_FILE)).unwrap();
    let segment_len_before = fs::metadata(&segment).unwrap().len();

    db.put(b"pending", &pending).unwrap();
    db.inject_blob_segment_after_write_failure();
    assert!(db.flush().is_err());
    assert!(db.durability_status().write_fenced);
    assert!(fs::metadata(&segment).unwrap().len() > segment_len_before);
    assert_eq!(fs::read(path.join(BLOB_FILE)).unwrap(), catalog_before);
    assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
    assert!(!path.join(BLOB_DELTA_FILE).exists());
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"base").unwrap(), Some(base));
    assert_eq!(reopened.get(b"pending").unwrap(), None);
    reopened.verify().unwrap();
    reopened.put(b"pending", &pending).unwrap();
    reopened.flush().unwrap();
    assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
    assert!(path.join(BLOB_DELTA_FILE).is_file());
    reopened.verify().unwrap();
}

#[test]
fn test_db_segmented_catalog_delta_chain_reopens_and_preserves_anchor() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("segmented-catalog-delta-chain.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 4,
        ..Options::default()
    };
    let base = vec![0xE5; 2_000];
    let mut db = DB::create(&path, options).unwrap();
    db.put(b"base", &base).unwrap();
    db.flush().unwrap();
    let anchor = fs::read(path.join(BLOB_FILE)).unwrap();

    for index in 0..3 {
        let key = format!("delta-{index}");
        db.put(key.as_bytes(), &vec![0xE6 + index as u8; 2_000])
            .unwrap();
        db.flush().unwrap();
    }
    assert_eq!(fs::read(path.join(BLOB_FILE)).unwrap(), anchor);
    let delta = fs::read(path.join(BLOB_DELTA_FILE)).unwrap();
    assert!(!delta.is_empty());
    assert_eq!(
        BlobManager::segment_catalog_delta_prefix_len(&delta),
        Some(delta.len())
    );
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"base").unwrap(), Some(base));
    for index in 0..3 {
        let key = format!("delta-{index}");
        assert_eq!(
            reopened.get(key.as_bytes()).unwrap(),
            Some(vec![0xE6 + index as u8; 2_000])
        );
    }
    reopened.verify().unwrap();
}

#[test]
fn test_db_segmented_compaction_advances_catalog_generation() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("segmented-compaction-catalog-generation.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 64,
        ..Options::for_test()
    };
    let blob = vec![0xD5; 2_000];
    let mut db = DB::create(&path, options).unwrap();
    let initial = (0..256)
        .map(|index| BatchMutation::Put {
            key: format!("key-{index:04}").into_bytes(),
            value: vec![index as u8; 128],
        })
        .chain(std::iter::once(BatchMutation::Put {
            key: b"segmented-blob".to_vec(),
            value: blob.clone(),
        }))
        .collect::<Vec<_>>();
    db.commit_batch(&initial).unwrap();

    db.put(b"key-0128", &[0xE6; 128]).unwrap();
    db.flush().unwrap();
    let report = db.compact().unwrap();
    assert!(
        report.relocated_pages > 0,
        "expected an interior relocation"
    );
    assert_eq!(db.get(b"segmented-blob").unwrap(), Some(blob.clone()));
    db.verify().unwrap();
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"segmented-blob").unwrap(), Some(blob));
    reopened.verify().unwrap();
}

#[test]
fn test_db_segmented_catalog_delta_sync_failure_discards_future_frame() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("segmented-catalog-delta-sync-failure.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 4,
        ..Options::default()
    };
    let base = vec![0xE7; 2_000];
    let pending = vec![0xE8; 2_100];
    let mut db = DB::create(&path, options).unwrap();
    db.put(b"base", &base).unwrap();
    db.flush().unwrap();
    db.put(b"pending", &pending).unwrap();
    db.inject_blob_segment_catalog_sync_failure();
    assert!(db.flush().is_err());
    assert!(db.durability_status().write_fenced);
    assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
    assert!(path.join(BLOB_DELTA_FILE).is_file());
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"base").unwrap(), Some(base));
    assert_eq!(reopened.get(b"pending").unwrap(), None);
    reopened.verify().unwrap();
    reopened.put(b"pending", &pending).unwrap();
    reopened.flush().unwrap();
    assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
    reopened.verify().unwrap();
}

#[test]
fn test_db_segmented_catalog_consolidation_rename_failure_preserves_old_catalog() {
    fn fill_delta_chain(db: &mut DB) {
        for index in 0..TEST_SEGMENT_CATALOG_DELTA_LIMIT {
            let key = format!("delta-{index}");
            db.put(key.as_bytes(), &vec![0xF0; 2_000]).unwrap();
            db.flush().unwrap();
        }
    }

    let dir = tempdir().unwrap();
    let path = dir.path().join("segmented-catalog-rename-failure.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 4,
        ..Options::default()
    };
    let base = vec![0xF1; 2_000];
    let pending = vec![0xF2; 2_100];
    let mut db = DB::create(&path, options).unwrap();
    db.put(b"base", &base).unwrap();
    db.flush().unwrap();
    let catalog = path.join(BLOB_FILE);
    let catalog_before = fs::read(&catalog).unwrap();
    fill_delta_chain(&mut db);

    db.put(b"pending", &pending).unwrap();
    db.inject_blob_segment_catalog_rename_failure();
    assert!(db.flush().is_err());
    assert!(db.durability_status().write_fenced);
    let backup = path.join(BLOB_REWRITE_BACKUP_FILE);
    assert!(!catalog.exists());
    assert_eq!(fs::read(&backup).unwrap(), catalog_before);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"base").unwrap(), Some(base));
    assert_eq!(reopened.get(b"pending").unwrap(), None);
    reopened.verify().unwrap();
    reopened.put(b"pending", &pending).unwrap();
    reopened.inject_after_manifest_failure();
    assert!(reopened.flush().is_err());
    drop(reopened);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
    assert!(!backup.exists());
    assert!(!path.join(BLOB_DELTA_FILE).exists());
    reopened.verify().unwrap();
}

#[test]
fn test_db_segmented_catalog_write_failures_restore_old_catalog() {
    let failures = [
        (
            "short",
            DB::inject_blob_segment_catalog_short_write_failure as fn(&DB),
        ),
        (
            "torn",
            DB::inject_blob_segment_catalog_torn_write_failure as fn(&DB),
        ),
    ];

    for (name, inject_failure) in failures {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join(format!("segmented-catalog-{name}-failure.db"));
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xF3; 2_000];
        let pending = vec![0xF4; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();
        for index in 0..TEST_SEGMENT_CATALOG_DELTA_LIMIT {
            let key = format!("delta-{index}");
            db.put(key.as_bytes(), &vec![0xF0; 2_000]).unwrap();
            db.flush().unwrap();
        }

        db.put(b"pending", &pending).unwrap();
        inject_failure(&db);
        assert!(db.flush().is_err(), "{name} catalog write should fail");
        assert!(db.durability_status().write_fenced);
        assert!(path.join(BLOB_REWRITE_BACKUP_FILE).is_file());
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        reopened.verify().unwrap();
        reopened.put(b"pending", &pending).unwrap();
        reopened.flush().unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        assert!(!path.join(BLOB_DELTA_FILE).exists());
        reopened.verify().unwrap();
    }
}

#[test]
fn test_db_segmented_first_catalog_write_failure_restores_empty_catalog() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("segmented-first-catalog-failure.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 4,
        ..Options::default()
    };
    let pending = vec![0xF7; 2_100];
    let mut db = DB::create(&path, options).unwrap();
    db.put(b"pending", &pending).unwrap();
    db.inject_blob_segment_catalog_short_write_failure();
    assert!(db.flush().is_err());
    assert!(db.durability_status().write_fenced);
    assert!(path.join(BLOB_REWRITE_BACKUP_FILE).is_file());
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"pending").unwrap(), None);
    assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
    reopened.verify().unwrap();
    reopened.put(b"pending", &pending).unwrap();
    reopened.flush().unwrap();
    assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
    assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
    reopened.verify().unwrap();
}

#[test]
fn test_db_segmented_publication_directory_failure_restores_old_catalog() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("segmented-directory-failure.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 4,
        ..Options::default()
    };
    let base = vec![0xF5; 2_000];
    let pending = vec![0xF6; 2_100];
    let mut db = DB::create(&path, options).unwrap();
    db.put(b"base", &base).unwrap();
    db.flush().unwrap();

    db.put(b"pending", &pending).unwrap();
    db.inject_publication_directory_sync_failure();
    assert!(db.flush().is_err());
    assert!(db.durability_status().write_fenced);
    assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
    assert!(path.join(BLOB_DELTA_FILE).is_file());
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"base").unwrap(), Some(base));
    assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
    reopened.verify().unwrap();
    if reopened.get(b"pending").unwrap().is_none() {
        reopened.put(b"pending", &pending).unwrap();
        reopened.flush().unwrap();
    }
    assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
    assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
    assert!(path.join(BLOB_DELTA_FILE).is_file());
    reopened.verify().unwrap();
}
