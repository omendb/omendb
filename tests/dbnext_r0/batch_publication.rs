//! DBNext R0 atomic-batch publication and admission tests.

use super::*;

#[test]
fn dbnext_r0_atomic_batch_commit_reopens_inline_and_blob_values() {
    let root = tempdir().unwrap();
    let path = root.path().join("atomic-batch.db");
    let large = vec![0x4Bu8; 2_048];
    let mutations = vec![
        BatchMutation::Put {
            key: b"tenant/1/inline".to_vec(),
            value: b"alpha".to_vec(),
        },
        BatchMutation::Put {
            key: b"tenant/1/blob".to_vec(),
            value: large.clone(),
        },
        BatchMutation::Put {
            key: b"tenant/2/inline".to_vec(),
            value: b"beta".to_vec(),
        },
    ];

    let mut db = DB::open(&path, Options::default()).unwrap();
    let status = db.commit_batch(&mutations).unwrap();
    assert_eq!(status.commit_id.get(), 1);
    assert_eq!(status.pending_mutations, 0);
    assert!(!status.write_fenced);
    assert_eq!(db.get(b"tenant/1/inline").unwrap(), Some(b"alpha".to_vec()));
    assert_eq!(db.get(b"tenant/1/blob").unwrap(), Some(large.clone()));
    assert_eq!(db.get(b"tenant/2/inline").unwrap(), Some(b"beta".to_vec()));
    assert_eq!(db.blob_stats().total_valid, 1);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.durability_status().commit_id.get(), 1);
    assert_eq!(
        reopened.get(b"tenant/1/inline").unwrap(),
        Some(b"alpha".to_vec())
    );
    assert_eq!(reopened.get(b"tenant/1/blob").unwrap(), Some(large));
    assert_eq!(
        reopened.get(b"tenant/2/inline").unwrap(),
        Some(b"beta".to_vec())
    );
    assert_eq!(reopened.verify().unwrap().wal_bytes, 0);
}

#[test]
fn dbnext_r0_stale_expected_base_is_rejected_without_side_effects() {
    let root = tempdir().unwrap();
    let path = root.path().join("stale-base.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"first".to_vec(),
            value: b"value-1".to_vec(),
        }])
        .unwrap();

    let error = db
        .commit_batch_at(
            CommitId::new(0),
            &[BatchMutation::Put {
                key: b"stale".to_vec(),
                value: b"must-not-appear".to_vec(),
            }],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        Error::SerializationConflict { expected, current }
            if expected == CommitId::new(0) && current == first.commit_id
    ));
    assert_eq!(db.get(b"stale").unwrap(), None);
    assert_eq!(db.durability_status().commit_id, first.commit_id);

    db.commit_batch_at(
        first.commit_id,
        &[BatchMutation::Put {
            key: b"next".to_vec(),
            value: b"value-2".to_vec(),
        }],
    )
    .unwrap();
    assert_eq!(db.get(b"next").unwrap(), Some(b"value-2".to_vec()));
}

#[test]
fn dbnext_r0_atomic_batch_wal_failure_drops_the_whole_candidate() {
    fn run_case<F>(root: &Path, name: &str, options: Options, inject: F)
    where
        F: FnOnce(&DB),
    {
        let path = root.join(name);
        let mutations = [
            BatchMutation::Put {
                key: b"batch/a".to_vec(),
                value: b"a".to_vec(),
            },
            BatchMutation::Put {
                key: b"batch/b".to_vec(),
                value: b"b".to_vec(),
            },
        ];
        let mut db = DB::open(&path, options).unwrap();
        inject(&db);
        assert!(matches!(db.commit_batch(&mutations), Err(Error::Io(_))));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"batch/a").unwrap(), None);
        assert_eq!(reopened.get(b"batch/b").unwrap(), None);
        assert_eq!(reopened.durability_status().commit_id.get(), 0);
        assert!(!reopened.durability_status().write_fenced);
        assert!(!path.join("seerdb.wal").exists());
    }

    let root = tempdir().unwrap();
    run_case(
        root.path(),
        "atomic-batch-wal-before-append.db",
        Options::default(),
        DB::inject_wal_write_failure,
    );
    run_case(
        root.path(),
        "atomic-batch-wal-after-append.db",
        Options::default(),
        DB::inject_wal_after_write_failure,
    );
    run_case(
        root.path(),
        "atomic-batch-wal-sync.db",
        Options {
            sync_writes: true,
            ..Options::default()
        },
        DB::inject_wal_sync_failure,
    );
    run_case(
        root.path(),
        "atomic-batch-wal-after-sync.db",
        Options {
            sync_writes: true,
            ..Options::default()
        },
        DB::inject_wal_after_sync_failure,
    );
}

#[test]
fn dbnext_r0_atomic_batch_post_manifest_failure_recovers_whole_batch() {
    let root = tempdir().unwrap();
    let path = root.path().join("atomic-batch-post-manifest.db");
    let mutations = [
        BatchMutation::Put {
            key: b"batch/a".to_vec(),
            value: b"a".to_vec(),
        },
        BatchMutation::Put {
            key: b"batch/b".to_vec(),
            value: b"b".to_vec(),
        },
    ];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.inject_after_manifest_failure();
    assert!(matches!(db.commit_batch(&mutations), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"batch/a").unwrap(), Some(b"a".to_vec()));
    assert_eq!(reopened.get(b"batch/b").unwrap(), Some(b"b".to_vec()));
    assert_eq!(reopened.durability_status().commit_id.get(), 1);
    assert!(!reopened.durability_status().write_fenced);
    assert!(!path.join("seerdb.wal").exists());
}

#[test]
fn dbnext_r0_atomic_batch_backpressure_is_pre_mutation() {
    let root = tempdir().unwrap();
    let path = root.path().join("atomic-batch-backpressure.db");
    let mut db = DB::open(
        &path,
        Options {
            max_wal_bytes: 1,
            ..Options::default()
        },
    )
    .unwrap();
    let mutations = [BatchMutation::Put {
        key: b"batch/key".to_vec(),
        value: b"value".to_vec(),
    }];

    assert!(matches!(
        db.commit_batch(&mutations),
        Err(Error::Backpressure { .. })
    ));
    assert_eq!(db.durability_status().pending_mutations, 0);
    assert!(!db.durability_status().write_fenced);
    assert_eq!(db.get(b"batch/key").unwrap(), None);
}

#[test]
fn dbnext_r0_atomic_batch_rejects_pending_generation_without_publishing() {
    let root = tempdir().unwrap();
    let path = root.path().join("atomic-batch-pending.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"pending", b"value").unwrap();
    let before = db.durability_status();

    let mutations = [BatchMutation::Put {
        key: b"batch/key".to_vec(),
        value: b"batch-value".to_vec(),
    }];
    assert!(matches!(
        db.commit_batch(&mutations),
        Err(Error::InvalidArgument(message)) if message.contains("clean pending generation")
    ));
    assert_eq!(db.durability_status(), before);
    assert_eq!(db.get(b"pending").unwrap(), Some(b"value".to_vec()));
    assert_eq!(db.get(b"batch/key").unwrap(), None);
}
