//! DB coordinator behavior and recovery tests.

use super::metadata_codec::{META_LOG_FRAME_HEADER_SIZE, META_LOG_HEADER_SIZE};
use super::*;
use std::io::Write;
use tempfile::tempdir;

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
    let checkpoint = DB::metadata_log_path(&checkpoint_path);
    assert!(
        checkpoint.is_file(),
        "published database has a metadata log"
    );
    fs::remove_file(&checkpoint).unwrap();

    let check = DB::check(&checkpoint_path, Options::default());
    assert!(matches!(
        check,
        Err(Error::Check {
            kind: CheckFailureKind::Target,
            ref message
        }) if message.contains("missing required manifest or data artifacts")
    ));
    assert!(matches!(
        DB::open(&checkpoint_path, Options::default()),
        Err(Error::Corruption(message))
            if message.contains("data file has no authoritative metadata log")
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
fn test_db_runtime_invariants_reject_published_frontier_identity_drift() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("frontier-identity.db"), Options::default()).unwrap();
    let current = db.manifest_history.latest().unwrap();
    db.manifest_history.reset(Manifest {
        commit_id: CommitId::new(current.commit_id.get().saturating_add(1)),
        ..current
    });

    let error = db.get(b"key").unwrap_err();

    assert!(matches!(
        error,
        Error::Corruption(message) if message.contains("published manifest commit")
    ));
}

#[test]
fn test_db_runtime_invariants_reject_clean_root_drift() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("frontier-root.db"), Options::default()).unwrap();
    let current = db.manifest_history.latest().unwrap();
    db.manifest_history.reset(Manifest {
        root_page_id: current.root_page_id.saturating_add(1),
        ..current
    });

    let error = db.get(b"key").unwrap_err();

    assert!(matches!(
        error,
        Error::Corruption(message) if message.contains("clean B-tree root")
    ));
}

#[test]
fn test_db_runtime_invariants_preserve_recovery_fence_outcome() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("frontier-fenced.db"), Options::default()).unwrap();
    let current = db.manifest_history.latest().unwrap();
    db.manifest_history.reset(Manifest {
        commit_id: CommitId::new(current.commit_id.get().saturating_add(1)),
        ..current
    });
    db.write_fenced = true;

    let error = db.get(b"key").unwrap_err();

    assert!(matches!(error, Error::NeedsRecovery(_)));
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

    let metadata_log = DB::metadata_log_path(&path);
    let mut meta = fs::read(&metadata_log).unwrap();
    // Corrupt a byte inside the first frame's payload; the frame checksum
    // refusal must fail the open closed.
    let first_frame_payload = META_LOG_HEADER_SIZE + META_LOG_FRAME_HEADER_SIZE;
    meta[first_frame_payload] ^= 0xA5;
    fs::write(&metadata_log, &meta).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("no valid publication frames")
    ));
}

#[test]
fn test_db_reopen_after_wal_recreation_advances_lsn_segment() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-recreation.db");
    let first_position = {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"first", b"value").unwrap();
        db.flush().unwrap();
        let position = db.durability_status().commit_position;
        db.close().unwrap();
        position
    };

    let second_position = {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"uncommitted", b"discard").unwrap();
        drop(db);

        let db = DB::open(&path, Options::default()).unwrap();
        assert_eq!(db.get(b"uncommitted").unwrap(), None);
        drop(db);

        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"second", b"value").unwrap();
        db.flush().unwrap();
        let position = db.durability_status().commit_position;
        assert!(position.lsn > first_position.lsn);
        db.close().unwrap();
        position
    };

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.durability_status().commit_position,
        second_position
    );
    assert_eq!(reopened.get(b"first").unwrap(), Some(b"value".to_vec()));
    assert_eq!(reopened.get(b"second").unwrap(), Some(b"value".to_vec()));
    reopened.close().unwrap();
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

    // The uncommitted suffix is inert after recovery; the retained file
    // is reclaimed on clean close.
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.close().unwrap();
    }
    assert!(
        !path.join(WAL_FILE).exists(),
        "clean close should reclaim the retained WAL"
    );
}

#[test]
fn test_db_reclaims_retained_wal_mid_session_at_threshold() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-reclaim-mid-session.db");
    let payload = vec![0x5Au8; 16384];
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        // Each admitted record is ~16.4 KiB of WAL, so this session pushes
        // the retained log well past the reclaim threshold in one generation.
        for index in 0..600 {
            let key = format!("key{index}");
            db.put(key.as_bytes(), &payload).unwrap();
        }
        db.flush().unwrap();

        assert!(
            !path.join(WAL_FILE).exists(),
            "retained WAL must be reclaimed once it passes the threshold"
        );
        db.put(b"after", b"value").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"after").unwrap(), Some(b"value".to_vec()));
        db.close().unwrap();
    }

    let mut db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"key0").unwrap(), Some(payload.clone()));
    let last = format!("key{}", 599);
    assert_eq!(db.get(last.as_bytes()).unwrap(), Some(payload.clone()));
    assert_eq!(db.get(b"after").unwrap(), Some(b"value".to_vec()));
    db.verify().unwrap();
    db.close().unwrap();
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
    // logical commit. A torn append tail behind the authority frame must
    // still reopen the current root, rather than a stale root whose record
    // was just removed.
    let metadata_log = DB::metadata_log_path(&path);
    let mut log = OpenOptions::new().append(true).open(&metadata_log).unwrap();
    log.write_all(&[0xA5; 64]).unwrap();
    log.sync_all().unwrap();

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
    let mut wal_bytes = fs::read(path.join(WAL_FILE)).unwrap();
    let wal_end = wal_bytes
        .len()
        .saturating_add(record.to_bytes().len())
        .saturating_add(4 + 1 + CommitRecord::SERIALIZED_SIZE + 4) as u64;
    let commit = CommitRecord {
        commit_id: CommitId::new(commit_id + 1),
        commit_seq: CommitSeq::new(commit_id + 1),
        lsn: Lsn::from_wal_position(0, wal_end).unwrap(),
        generation_id: GenerationId::new(generation_id + 1),
        root_page_id,
        mutation_count: 1,
        digest: digest_records(&[&record]),
    };
    wal_bytes.extend_from_slice(&record.to_bytes());
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(replacement));
}

#[test]
fn test_db_discards_wal_commit_already_published_by_manifest() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("stale-authoritative-wal.db");

    let (commit_id, generation_id) = {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();

        (db.commit_id, db.generation_id)
    };

    // Model the crash window where manifest publication succeeded but WAL
    // cleanup did not. The retained WAL from the successful publication is
    // already the stale authoritative prefix; replaying it must not publish
    // the same logical state under a new generation.
    assert!(path.join(WAL_FILE).is_file());

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    assert_eq!(reopened.commit_id, commit_id);
    assert_eq!(reopened.generation_id, generation_id);
    assert_eq!(reopened.metrics().unwrap().storage.generation_flushes, 0);
    // Retained WAL: published records stay until clean close.
    assert!(path.join(WAL_FILE).exists());
}

#[test]
fn test_db_recovers_committed_large_blob_value() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("large-blob-recovery.db");
    let value = vec![0x7B; 70_000];
    let record = WalRecord::put(b"large-key", &value);
    let wal_end = record
        .to_bytes()
        .len()
        .saturating_add(4 + 1 + CommitRecord::SERIALIZED_SIZE + 4);
    let commit = CommitRecord {
        commit_id: CommitId::new(1),
        commit_seq: CommitSeq::new(1),
        lsn: Lsn::from_wal_position(0, wal_end as u64).unwrap(),
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
    let wal_end = record
        .to_bytes()
        .len()
        .saturating_add(4 + 1 + CommitRecord::SERIALIZED_SIZE + 4);
    let commit = CommitRecord {
        commit_id: CommitId::new(1),
        commit_seq: CommitSeq::new(1),
        lsn: Lsn::from_wal_position(0, wal_end as u64).unwrap(),
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
