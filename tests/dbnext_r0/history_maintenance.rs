//! DBNext R0 historical-generation, reuse-ledger, vacuum, and prune tests.

use super::*;

#[test]
fn dbnext_r0_retains_arbitrary_historical_commit_across_reopen() {
    let root = tempdir().unwrap();
    let path = root.path().join("historical-commit.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"old".to_vec(),
        }])
        .unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"versioned".to_vec(),
        value: b"new".to_vec(),
    }])
    .unwrap();

    let snapshot_id = db.retain_commit(first.commit_id).unwrap();
    assert_eq!(db.get(b"versioned").unwrap(), Some(b"new".to_vec()));
    assert_eq!(
        db.get_at(snapshot_id, b"versioned").unwrap(),
        Some(b"old".to_vec())
    );
    assert_eq!(
        db.range_at(snapshot_id, b"versioned", b"versioned~")
            .unwrap(),
        vec![(b"versioned".to_vec(), b"old".to_vec())]
    );
    db.close().unwrap();
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(
        reopened.retained_snapshot_id(first.commit_id),
        Some(snapshot_id)
    );
    assert_eq!(
        reopened.get_at(snapshot_id, b"versioned").unwrap(),
        Some(b"old".to_vec())
    );
    reopened.release_snapshot(snapshot_id).unwrap();
    assert!(matches!(
        reopened.get_at(snapshot_id, b"versioned"),
        Err(Error::SnapshotUnavailable(_))
    ));
}

#[test]
fn dbnext_r0_late_retention_rejects_reused_physical_generation() {
    let root = tempdir().unwrap();
    let path = root.path().join("late-retention.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"one".to_vec(),
        }])
        .unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"versioned".to_vec(),
        value: b"two".to_vec(),
    }])
    .unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"versioned".to_vec(),
        value: b"three".to_vec(),
    }])
    .unwrap();

    assert!(matches!(
        db.retain_commit(first.commit_id),
        Err(Error::SnapshotUnavailable(message))
            if message.contains("physical pages reused")
    ));
    assert_eq!(db.get(b"versioned").unwrap(), Some(b"three".to_vec()));
}

#[test]
fn dbnext_r0_late_retention_rejects_reuse_after_failed_publication() {
    let root = tempdir().unwrap();
    let path = root.path().join("late-retention-failed-publication.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"one".to_vec(),
        }])
        .unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"versioned".to_vec(),
        value: b"two".to_vec(),
    }])
    .unwrap();

    db.put(b"versioned", b"three").unwrap();
    db.inject_page_range_sync_failure();
    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"versioned").unwrap(), Some(b"two".to_vec()));
    assert_eq!(reopened.durability_status().commit_id.get(), 2);
    assert!(matches!(
        reopened.retain_commit(first.commit_id),
        Err(Error::SnapshotUnavailable(message))
            if message.contains("physical pages reused")
    ));
    reopened.put(b"versioned", b"four").unwrap();
    reopened.flush().unwrap();
    assert_eq!(reopened.durability_status().commit_id.get(), 4);
    assert_eq!(reopened.get(b"versioned").unwrap(), Some(b"four".to_vec()));
    // The abandoned c3 reservation remains until history pruning proves that
    // no retained root can refer to its possibly overwritten slots.
    assert!(path.join("seerdb.reuse-ledger").exists());
}

#[test]
fn dbnext_r0_ambiguous_new_page_reserves_commit_id() {
    let root = tempdir().unwrap();
    let path = root.path().join("ambiguous-new-page.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();

    // The first data page grows the file; no retired slot is available yet.
    db.put(b"versioned", b"one").unwrap();
    db.inject_page_range_sync_failure();
    assert!(matches!(db.flush(), Err(Error::Io(_))));
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.durability_status().commit_id.get(), 0);
    reopened.put(b"versioned", b"two").unwrap();
    reopened.flush().unwrap();
    assert_eq!(reopened.durability_status().commit_id.get(), 2);
    assert_eq!(reopened.get(b"versioned").unwrap(), Some(b"two".to_vec()));
    assert!(!path.join("seerdb.reuse-ledger").exists());
}

#[test]
fn dbnext_r0_defers_successful_reuse_ledger_cleanup_until_reopen() {
    let root = tempdir().unwrap();
    let path = root.path().join("deferred-reuse-ledger-cleanup.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();

    db.put(b"versioned", b"one").unwrap();
    db.flush().unwrap();
    db.put(b"versioned", b"two").unwrap();
    db.flush().unwrap();
    db.put(b"versioned", b"three").unwrap();
    db.flush().unwrap();

    // The third generation reused the first generation's retired root page.
    // The ledger remains as a conservative on-disk recovery hint, but the
    // in-memory ledger is already reconciled after manifest publication.
    assert!(path.join("seerdb.reuse-ledger").is_file());
    assert_eq!(db.get(b"versioned").unwrap(), Some(b"three".to_vec()));

    drop(db);
    let reopened = DB::open(&path, Options::for_test()).unwrap();
    assert!(!path.join("seerdb.reuse-ledger").exists());
    assert_eq!(reopened.get(b"versioned").unwrap(), Some(b"three".to_vec()));
}

#[test]
fn dbnext_r0_vacuum_rebuilds_live_tree_and_preserves_retained_root() {
    let root = tempdir().unwrap();
    let path = root.path().join("vacuum.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();

    for id in 0..200 {
        let key = format!("key-{id:04}");
        let value = format!("value-before-{id:04}");
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    let first = db.durability_status();

    for id in 0..150 {
        let key = format!("key-{id:04}");
        assert!(db.delete(key.as_bytes()).unwrap());
    }
    db.flush().unwrap();
    let snapshot_id = db.retain_commit(first.commit_id).unwrap();

    let report = db.vacuum().unwrap();
    assert_eq!(report.live_entries, 50);
    assert!(report.logical_pages_before >= report.logical_pages_after);
    assert_eq!(db.get(b"key-0000").unwrap(), None);
    assert_eq!(
        db.get(b"key-0199").unwrap(),
        Some(b"value-before-0199".to_vec())
    );
    assert_eq!(
        db.get_at(snapshot_id, b"key-0000").unwrap(),
        Some(b"value-before-0000".to_vec())
    );
    assert!(db.verify().is_ok());

    db.compact().unwrap();
    assert_eq!(
        db.get_at(snapshot_id, b"key-0149").unwrap(),
        Some(b"value-before-0149".to_vec())
    );
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"key-0000").unwrap(), None);
    assert_eq!(
        reopened.get(b"key-0199").unwrap(),
        Some(b"value-before-0199".to_vec())
    );
    assert_eq!(
        reopened.get_at(snapshot_id, b"key-0000").unwrap(),
        Some(b"value-before-0000".to_vec())
    );
    reopened.release_snapshot(snapshot_id).unwrap();
}

#[test]
fn dbnext_r0_vacuum_write_failure_preserves_prior_generation() {
    let root = tempdir().unwrap();
    let path = root.path().join("vacuum-failure.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    db.put(b"keep", b"old").unwrap();
    db.put(b"remove", b"tombstoned").unwrap();
    db.flush().unwrap();
    db.delete(b"remove").unwrap();
    db.flush().unwrap();

    db.inject_write_failure();
    assert!(db.vacuum().is_err());
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"keep").unwrap(), Some(b"old".to_vec()));
    assert_eq!(reopened.get(b"remove").unwrap(), None);
    assert!(reopened.verify().is_ok());
}

#[test]
fn dbnext_r0_vacuum_capacity_refusal_is_retryable_before_rebuild() {
    let root = tempdir().unwrap();
    let path = root.path().join("vacuum-capacity.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    db.put(b"keep", b"old").unwrap();
    db.put(b"remove", b"tombstoned").unwrap();
    db.flush().unwrap();
    db.delete(b"remove").unwrap();
    db.flush().unwrap();

    let before = db.durability_status();
    db.inject_capacity_limit(0);
    assert!(matches!(db.vacuum(), Err(Error::DiskFull)));
    assert_eq!(db.durability_status(), before);
    assert!(!db.durability_status().write_fenced);
    assert_eq!(db.get(b"keep").unwrap(), Some(b"old".to_vec()));
    assert_eq!(db.get(b"remove").unwrap(), None);

    db.inject_capacity_limit(u64::MAX);
    let report = db.vacuum().unwrap();
    assert_eq!(report.live_entries, 1);
    assert_eq!(db.get(b"keep").unwrap(), Some(b"old".to_vec()));
    assert_eq!(db.get(b"remove").unwrap(), None);
    assert!(db.verify().is_ok());
}

#[test]
fn dbnext_r0_prunes_unretained_history_after_atomic_sidecar_publish() {
    let root = tempdir().unwrap();
    let path = root.path().join("history-prune.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"one".to_vec(),
        }])
        .unwrap();
    let snapshot_id = db.retain_commit(first.commit_id).unwrap();
    let second = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"two".to_vec(),
        }])
        .unwrap();
    let third = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"three".to_vec(),
        }])
        .unwrap();

    let second_checkpoint = path.join(format!("seerdb.meta.{}", second.generation_id.get()));
    let first_checkpoint = path.join(format!("seerdb.meta.{}", first.generation_id.get()));
    let third_checkpoint = path.join(format!("seerdb.meta.{}", third.generation_id.get()));
    assert!(first_checkpoint.is_file());
    assert!(second_checkpoint.is_file());
    assert!(third_checkpoint.is_file());

    let report = db.prune_history().unwrap();
    assert_eq!(report.retained_generations, 3);
    // Both manifest slots and the retained snapshot remain recovery roots.
    // The current delta checkpoint also depends on its full and delta
    // ancestors even though the middle logical manifest is unretained.
    assert_eq!(report.removed_checkpoints, 0);
    assert!(second_checkpoint.exists());
    assert!(first_checkpoint.is_file());
    assert!(third_checkpoint.is_file());
    assert_eq!(
        db.get_at(snapshot_id, b"versioned").unwrap(),
        Some(b"one".to_vec())
    );
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(
        reopened.get_at(snapshot_id, b"versioned").unwrap(),
        Some(b"one".to_vec())
    );
    reopened.release_snapshot(snapshot_id).unwrap();
    let report = reopened.prune_history().unwrap();
    assert_eq!(report.retained_generations, 2);
    assert_eq!(report.removed_checkpoints, 0);
    assert!(first_checkpoint.exists());
    assert!(third_checkpoint.is_file());
    assert!(matches!(
        reopened.get_at(snapshot_id, b"versioned"),
        Err(Error::SnapshotUnavailable(_))
    ));
}

#[test]
fn dbnext_r0_history_prune_rename_failure_preserves_old_sidecar() {
    let root = tempdir().unwrap();
    let path = root.path().join("history-prune-failure.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    db.put(b"versioned", b"one").unwrap();
    db.flush().unwrap();
    let first_checkpoint = path.join("seerdb.meta.1");
    db.put(b"versioned", b"two").unwrap();
    db.flush().unwrap();
    assert!(first_checkpoint.is_file());

    db.inject_atomic_rename_failure();
    assert!(db.prune_history().is_err());
    assert!(first_checkpoint.is_file());
    assert_eq!(db.get(b"versioned").unwrap(), Some(b"two".to_vec()));

    let report = db.prune_history().unwrap();
    assert_eq!(report.removed_checkpoints, 0);
    assert!(first_checkpoint.exists());
}

#[test]
fn dbnext_r0_historical_retention_preserves_replaced_blob_values() {
    let root = tempdir().unwrap();
    let path = root.path().join("historical-blob.db");
    let options = Options {
        blob_threshold: 4,
        ..Options::for_test()
    };
    let mut db = DB::open(&path, options.clone()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"blob-key".to_vec(),
            value: b"old-large-value".to_vec(),
        }])
        .unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"blob-key".to_vec(),
        value: b"new-large-value".to_vec(),
    }])
    .unwrap();

    let snapshot_id = db.retain_commit(first.commit_id).unwrap();
    assert_eq!(
        db.get_at(snapshot_id, b"blob-key").unwrap(),
        Some(b"old-large-value".to_vec())
    );
    db.close().unwrap();
    drop(db);

    let reopened = DB::open(&path, options).unwrap();
    assert_eq!(
        reopened.get_at(snapshot_id, b"blob-key").unwrap(),
        Some(b"old-large-value".to_vec())
    );
}
