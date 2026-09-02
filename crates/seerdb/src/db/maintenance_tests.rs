//! Maintenance-batch publication tests: durable arbitrary rewrites that
//! preserve logical commit identity.

use super::*;
use std::collections::BTreeMap;
use tempfile::tempdir;

fn status_of(db: &DB) -> (u64, u64, u64) {
    let status = db.durability_status();
    (
        status.commit_id.get(),
        status.commit_position.csn.get(),
        status.commit_position.lsn.get(),
    )
}

#[test]
fn maintenance_batch_preserves_commit_identity_and_advances_generation() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("db"), Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    let before = status_of(&db);
    let generation_before = db.durability_status().generation_id.get();
    assert!(before.1 >= 1);

    db.commit_maintenance_batch(&[BatchMutation::Put {
        key: b"maintenance".to_vec(),
        value: b"rewrite".to_vec(),
    }])
    .unwrap();

    let after = status_of(&db);
    assert_eq!(
        after, before,
        "maintenance must not advance commit identity, CSN, or LSN"
    );
    assert!(
        db.durability_status().generation_id.get() > generation_before,
        "maintenance must publish a new physical generation"
    );
    assert_eq!(db.get(b"maintenance").unwrap(), Some(b"rewrite".to_vec()));
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    db.close().unwrap();
}

#[test]
fn maintenance_batch_is_durable_across_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
        db.commit_maintenance_batch(&[
            BatchMutation::Put {
                key: b"rewritten".to_vec(),
                value: b"new-value".to_vec(),
            },
            BatchMutation::Delete {
                key: b"key".to_vec(),
            },
        ])
        .unwrap();
        db.close().unwrap();
    }

    let mut db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"rewritten").unwrap(), Some(b"new-value".to_vec()));
    assert_eq!(db.get(b"key").unwrap(), None);
    db.close().unwrap();
}

#[test]
fn maintenance_batch_settles_wal_first_head_before_cloning_identity() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let options = Options {
        wal_first_commits: true,
        ..Options::default()
    };
    {
        let mut db = DB::open(&path, options.clone()).unwrap();
        db.put(b"wal-first", b"acked").unwrap();
        // The commit is acknowledged but unmaterialized: the maintenance
        // batch must settle it before cloning the identity.
        db.commit_maintenance_batch(&[BatchMutation::Put {
            key: b"maintenance".to_vec(),
            value: b"durable".to_vec(),
        }])
        .unwrap();
        assert_eq!(db.get(b"wal-first").unwrap(), Some(b"acked".to_vec()));
        assert_eq!(db.get(b"maintenance").unwrap(), Some(b"durable".to_vec()));
        db.close().unwrap();
    }

    let mut db = DB::open(&path, options).unwrap();
    assert_eq!(db.get(b"wal-first").unwrap(), Some(b"acked".to_vec()));
    assert_eq!(db.get(b"maintenance").unwrap(), Some(b"durable".to_vec()));
    db.close().unwrap();
}

#[test]
fn logical_commits_continue_after_maintenance() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"first", b"1").unwrap();
    db.flush().unwrap();
    let head = db.durability_status().commit_position.csn.get();

    db.commit_maintenance_batch(&[BatchMutation::Put {
        key: b"maintenance".to_vec(),
        value: b"m".to_vec(),
    }])
    .unwrap();

    // The next logical commit takes exactly head+1: no maintenance CSN was
    // consumed.
    db.put(b"second", b"2").unwrap();
    db.flush().unwrap();
    let status = db.durability_status();
    assert_eq!(status.commit_position.csn.get(), head + 1);
    assert_eq!(db.get(b"second").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"maintenance").unwrap(), Some(b"m".to_vec()));
    db.close().unwrap();
}

#[test]
fn maintenance_batch_rejects_empty_and_requires_clean_pending() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("db"), Options::default()).unwrap();
    db.commit_maintenance_batch(&[]).unwrap();
    db.put(b"pending", b"value").unwrap();
    // An empty batch with a pending generation settles it and succeeds.
    db.commit_maintenance_batch(&[]).unwrap();
    assert_eq!(db.get(b"pending").unwrap(), Some(b"value".to_vec()));
    db.close().unwrap();
}

#[test]
fn maintenance_batch_rejects_invalid_lengths_without_state_change() {
    let dir = tempdir().unwrap();
    let mut db = DB::open(dir.path().join("db"), Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    let before = status_of(&db);

    // A key beyond the page-entry limit fails candidate validation before
    // any publication work; the writer stays usable.
    let oversized_key = vec![0u8; MAX_KEY_SIZE + 1];
    let result = db.commit_maintenance_batch(&[BatchMutation::Put {
        key: oversized_key,
        value: b"v".to_vec(),
    }]);
    assert!(
        matches!(result, Err(Error::InvalidArgument(_))),
        "expected a bounded validation refusal, got {result:?}"
    );
    assert_eq!(status_of(&db), before);
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    db.put(b"after-refusal", b"ok").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"after-refusal").unwrap(), Some(b"ok".to_vec()));
    db.close().unwrap();
}

/// The crash-child seam: publish a logical commit, then run one maintenance
/// batch, then die abruptly. Reopen must see the maintenance rewrite (the
/// authority frame was synced) with the original logical identity intact.
#[test]
fn test_db_process_crash_after_maintenance_batch() {
    if let Some(path) = std::env::var_os("SEERDB_MAINTENANCE_CRASH_CHILD_PATH") {
        let path = PathBuf::from(path);
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"published", b"value-before-maintenance").unwrap();
        db.flush().unwrap();
        let head = db.durability_status().commit_position.csn;
        db.commit_maintenance_batch(&[BatchMutation::Put {
            key: b"maintenance-rewrite".to_vec(),
            value: b"durable".to_vec(),
        }])
        .unwrap();
        assert_eq!(db.durability_status().commit_position.csn, head);
        std::process::exit(137);
    }

    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("db::maintenance_tests::test_db_process_crash_after_maintenance_batch")
        .arg("--nocapture")
        .env("SEERDB_MAINTENANCE_CRASH_CHILD_PATH", &path)
        .status()
        .unwrap();
    assert!(!status.success(), "crash child unexpectedly exited cleanly");

    let mut db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        db.get(b"published").unwrap(),
        Some(b"value-before-maintenance".to_vec())
    );
    assert_eq!(
        db.get(b"maintenance-rewrite").unwrap(),
        Some(b"durable".to_vec())
    );
    // The logical head is unchanged by the maintenance rewrite.
    assert_eq!(db.durability_status().commit_position.csn.get(), 1);
    db.close().unwrap();
}

/// Fault-seam coverage: a failure at the data-sync seam (pages written,
/// durability sync refused) fences the writer; reopen must select the OLD
/// generation — the maintenance frame never became authoritative — and the
/// logical identity is intact.
#[test]
fn maintenance_data_sync_failure_fences_and_reopen_keeps_old_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"published", b"value").unwrap();
        db.flush().unwrap();
        db.inject_page_range_sync_failure();
        let result = db.commit_maintenance_batch(&[BatchMutation::Put {
            key: b"maintenance".to_vec(),
            value: b"m".to_vec(),
        }]);
        assert!(result.is_err(), "injected data-sync failure must surface");
        assert!(matches!(
            db.put(b"more", b"x"),
            Err(Error::NeedsRecovery(_))
        ));
    }

    let mut db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"published").unwrap(), Some(b"value".to_vec()));
    // The authority frame never became durable, so reopen selects the old
    // generation: the rewrite is absent.
    assert_eq!(db.get(b"maintenance").unwrap(), None);
    assert_eq!(db.durability_status().commit_position.csn.get(), 1);
    db.close().unwrap();
}

/// Fault-seam coverage: a failure after the authority frame fences the
/// writer; reopen must atomically select the maintenance generation (it was
/// fully synced) and preserve logical identity.
#[test]
fn maintenance_post_manifest_failure_fences_and_reopen_selects_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"published", b"value").unwrap();
        db.flush().unwrap();
        super::faults::FAIL_NEXT_AFTER_MANIFEST.with(|failure| failure.replace(true));
        let result = db.commit_maintenance_batch(&[BatchMutation::Put {
            key: b"maintenance".to_vec(),
            value: b"m".to_vec(),
        }]);
        assert!(result.is_err(), "injected failure must surface");
        // The writer is fenced; reopen is required.
        assert!(matches!(
            db.put(b"more", b"x"),
            Err(Error::NeedsRecovery(_))
        ));
    }

    let mut db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"published").unwrap(), Some(b"value".to_vec()));
    // The manifest frame was synced before the injected failure, so reopen
    // selects the maintenance generation.
    assert_eq!(db.get(b"maintenance").unwrap(), Some(b"m".to_vec()));
    assert_eq!(db.durability_status().commit_position.csn.get(), 1);
    db.close().unwrap();
}

/// Segmented blob storage: a maintenance batch that changes blob-backed
/// values must publish the segment catalog against the new generation and
/// stay consistent across reopen.
#[test]
fn maintenance_batch_publishes_segmented_blob_catalog() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        blob_threshold: 1,
        ..Options::default()
    };
    {
        let mut db = DB::open(&path, options.clone()).unwrap();
        db.put(b"small", b"v").unwrap();
        db.put(b"blob", vec![7u8; 64].as_slice()).unwrap();
        db.flush().unwrap();
        let head = db.durability_status().commit_position.csn;

        // Rewrite the blob-backed value and a small value in one batch.
        db.commit_maintenance_batch(&[
            BatchMutation::Put {
                key: b"blob".to_vec(),
                value: vec![9u8; 96],
            },
            BatchMutation::Put {
                key: b"small".to_vec(),
                value: b"rewritten".to_vec(),
            },
        ])
        .unwrap();
        assert_eq!(
            db.durability_status().commit_position.csn,
            head,
            "segmented maintenance must not consume a CSN"
        );
        assert_eq!(db.get(b"blob").unwrap(), Some(vec![9u8; 96]));
        assert_eq!(db.get(b"small").unwrap(), Some(b"rewritten".to_vec()));
        db.close().unwrap();
    }

    let mut db = DB::open(&path, options).unwrap();
    assert_eq!(db.get(b"blob").unwrap(), Some(vec![9u8; 96]));
    assert_eq!(db.get(b"small").unwrap(), Some(b"rewritten".to_vec()));
    db.close().unwrap();
}

/// A model-checked sweep: interleaved logical commits and maintenance
/// batches must leave exactly the model state after reopen, with CSNs
/// advancing only per logical commit.
#[test]
fn maintenance_and_logical_commits_interleave_correctly() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    let mut model = BTreeMap::new();
    let mut expected_csn = 0u64;

    for round in 0..8 {
        let key = format!("logical-{round}").into_bytes();
        db.put(&key, b"logical").unwrap();
        db.flush().unwrap();
        model.insert(key, b"logical".to_vec());
        expected_csn += 1;
        assert_eq!(
            db.durability_status().commit_position.csn.get(),
            expected_csn,
            "logical commit {round} must advance the CSN by exactly one"
        );

        let key = format!("maintenance-{round}").into_bytes();
        db.commit_maintenance_batch(&[BatchMutation::Put {
            key: key.clone(),
            value: b"maintenance".to_vec(),
        }])
        .unwrap();
        model.insert(key, b"maintenance".to_vec());
        assert_eq!(
            db.durability_status().commit_position.csn.get(),
            expected_csn,
            "maintenance {round} must not advance the CSN"
        );
    }

    for (key, value) in &model {
        assert_eq!(db.get(key).unwrap(), Some(value.clone()));
    }
    db.close().unwrap();

    let mut db = DB::open(&path, Options::default()).unwrap();
    for (key, value) in &model {
        assert_eq!(db.get(key).unwrap(), Some(value.clone()));
    }
    assert_eq!(
        db.durability_status().commit_position.csn.get(),
        expected_csn
    );
    db.close().unwrap();
}
