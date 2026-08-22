//! DBNext R0 root-bound byte-transaction and grouped-transaction tests.

use super::*;

#[test]
fn dbnext_r0_batch_transaction_binds_root_and_detects_stale_commit() {
    let root = tempdir().unwrap();
    let path = root.path().join("batch-transaction.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    db.put(b"key", b"before").unwrap();
    db.flush().unwrap();

    let mut transaction = db.begin_batch_transaction().unwrap();
    assert_eq!(transaction.snapshot().get(), 1);
    assert_eq!(
        transaction.get(&db, b"key").unwrap(),
        Some(b"before".to_vec())
    );
    transaction.put(b"key", b"staged").unwrap();
    assert_eq!(
        transaction.get(&db, b"key").unwrap(),
        Some(b"staged".to_vec())
    );

    db.commit_batch(&[BatchMutation::Put {
        key: b"key".to_vec(),
        value: b"outside".to_vec(),
    }])
    .unwrap();
    assert_eq!(
        transaction.get(&db, b"key").unwrap(),
        Some(b"staged".to_vec())
    );
    assert!(matches!(
        transaction.commit(&mut db),
        Err(Error::SerializationConflict { expected, current })
            if expected.get() == 1 && current.get() == 2
    ));
    assert!(transaction.is_active());
    transaction.abort().unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"outside".to_vec()));

    let mut committed = db.begin_batch_transaction().unwrap();
    committed.put(b"key", b"committed").unwrap();
    let status = committed.commit(&mut db).unwrap();
    assert_eq!(status.commit_id.get(), 3);
    assert_eq!(db.get(b"key").unwrap(), Some(b"committed".to_vec()));
}

#[test]
fn dbnext_r0_batch_transaction_range_overlay_is_root_bound() {
    let root = tempdir().unwrap();
    let path = root.path().join("batch-transaction-range.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    for index in 0..8 {
        db.put(
            format!("key-{index:02}").as_bytes(),
            format!("before-{index}").as_bytes(),
        )
        .unwrap();
    }
    db.flush().unwrap();

    let mut transaction = db.begin_batch_transaction().unwrap();
    assert_eq!(
        transaction.range(&db, b"key-00", b"key-08").unwrap(),
        (0..8)
            .map(|index| {
                (
                    format!("key-{index:02}").into_bytes(),
                    format!("before-{index}").into_bytes(),
                )
            })
            .collect::<Vec<_>>()
    );

    transaction.put(b"key-02", b"staged-replacement").unwrap();
    transaction.delete(b"key-04").unwrap();
    transaction.put(b"key-07", b"staged-new-value").unwrap();
    assert_eq!(
        transaction.range(&db, b"key-00", b"key-08").unwrap(),
        vec![
            (b"key-00".to_vec(), b"before-0".to_vec()),
            (b"key-01".to_vec(), b"before-1".to_vec()),
            (b"key-02".to_vec(), b"staged-replacement".to_vec()),
            (b"key-03".to_vec(), b"before-3".to_vec()),
            (b"key-05".to_vec(), b"before-5".to_vec()),
            (b"key-06".to_vec(), b"before-6".to_vec()),
            (b"key-07".to_vec(), b"staged-new-value".to_vec()),
        ]
    );

    db.commit_batch(&[BatchMutation::Put {
        key: b"key-02".to_vec(),
        value: b"outside-replacement".to_vec(),
    }])
    .unwrap();
    assert_eq!(
        transaction.range(&db, b"key-00", b"key-08").unwrap(),
        vec![
            (b"key-00".to_vec(), b"before-0".to_vec()),
            (b"key-01".to_vec(), b"before-1".to_vec()),
            (b"key-02".to_vec(), b"staged-replacement".to_vec()),
            (b"key-03".to_vec(), b"before-3".to_vec()),
            (b"key-05".to_vec(), b"before-5".to_vec()),
            (b"key-06".to_vec(), b"before-6".to_vec()),
            (b"key-07".to_vec(), b"staged-new-value".to_vec()),
        ]
    );

    assert!(matches!(
        transaction.commit(&mut db),
        Err(Error::SerializationConflict { expected, current })
            if expected.get() == 1 && current.get() == 2
    ));
    transaction.abort().unwrap();
    assert_eq!(
        db.get(b"key-02").unwrap(),
        Some(b"outside-replacement".to_vec())
    );
    assert_eq!(db.get(b"key-04").unwrap(), Some(b"before-4".to_vec()));
}

#[test]
fn dbnext_r0_batch_transaction_faults_require_explicit_recovery() {
    fn run_case(
        root: &Path,
        name: &str,
        inject: fn(&DB),
        expected_after_reopen: &'static [u8],
        allow_complete_new: bool,
    ) {
        let path = root.join(name);
        let mut db = DB::open(&path, Options::for_test()).unwrap();
        db.put(b"key", b"before").unwrap();
        db.flush().unwrap();
        let mirror_case = name.contains("manifest-mirror");
        if mirror_case {
            db.put(b"seed", b"seed-value").unwrap();
            db.flush().unwrap();
        }

        let mut transaction = db.begin_batch_transaction().unwrap();
        transaction.put(b"key", b"after").unwrap();
        inject(&db);
        let expected_commit = if mirror_case { 3 } else { 2 };

        assert!(matches!(
            transaction.commit(&mut db),
            Err(Error::NeedsRecovery(_))
        ));
        assert!(matches!(
            transaction.state(),
            seerdb::BatchTransactionState::RecoveryRequired { commit }
                if commit.get() == expected_commit
        ));
        assert_eq!(
            transaction.recovery_commit().unwrap().get(),
            expected_commit
        );
        assert!(db.durability_status().write_fenced);

        // A fenced publication may already be durable. The transaction is
        // therefore not semantically abortable; only its process-local root
        // pin can be released before reopening to resolve the outcome.
        assert!(matches!(transaction.abort(), Err(Error::NeedsRecovery(_))));
        transaction.release().unwrap();
        assert!(matches!(
            transaction.put(b"after-recovery", b"nope"),
            Err(Error::NeedsRecovery(_))
        ));
        drop(db);

        let mut reopened = DB::open(&path, Options::for_test()).unwrap();
        let recovered = reopened.get(b"key").unwrap();
        if allow_complete_new {
            assert!(
                recovered == Some(b"before".to_vec()) || recovered == Some(b"after".to_vec()),
                "ambiguous manifest publication exposed an invalid state: {recovered:?}"
            );
        } else {
            assert_eq!(recovered, Some(expected_after_reopen.to_vec()));
        }
        assert!(!reopened.durability_status().write_fenced);
        reopened.verify().unwrap();
    }

    let root = tempdir().unwrap();
    run_case(
        root.path(),
        "transaction-before-wal.db",
        DB::inject_wal_write_failure,
        b"before",
        false,
    );
    run_case(
        root.path(),
        "transaction-page-sync.db",
        DB::inject_page_range_sync_failure,
        b"before",
        false,
    );
    run_case(
        root.path(),
        "transaction-manifest-sync.db",
        DB::inject_manifest_sync_failure,
        b"before",
        true,
    );
    run_case(
        root.path(),
        "transaction-manifest-mirror-sync.db",
        DB::inject_manifest_mirror_sync_failure,
        b"before",
        false,
    );
    run_case(
        root.path(),
        "transaction-after-manifest.db",
        DB::inject_after_manifest_failure,
        b"after",
        false,
    );
    // wal-truncate case retired with WAL retention.
}

#[test]
fn dbnext_r0_grouped_batch_transaction_faults_are_atomic() {
    #[derive(Clone, Copy)]
    enum RecoveryOutcome {
        Old,
        New,
        OldOrNew,
    }

    fn run_case(root: &Path, name: &str, inject: fn(&DB), outcome: RecoveryOutcome) {
        let path = root.join(name);
        let old_blob = vec![0x11; 4096];
        let new_blob = vec![0x22; 4096];
        let mut db = DB::open(&path, Options::for_test()).unwrap();
        db.commit_batch(&[
            BatchMutation::Put {
                key: b"key-a".to_vec(),
                value: b"before-a".to_vec(),
            },
            BatchMutation::Put {
                key: b"blob-key".to_vec(),
                value: old_blob.clone(),
            },
            BatchMutation::Put {
                key: b"remove-key".to_vec(),
                value: b"before-remove".to_vec(),
            },
        ])
        .unwrap();
        let mirror_case = name.contains("manifest-mirror");
        if mirror_case {
            db.put(b"seed", b"seed-value").unwrap();
            db.flush().unwrap();
        }

        let mut transaction = db.begin_batch_transaction().unwrap();
        transaction.put(b"key-a", b"after-a").unwrap();
        transaction.put(b"blob-key", &new_blob).unwrap();
        transaction.delete(b"remove-key").unwrap();
        transaction.put(b"new-key", b"after-new").unwrap();
        inject(&db);
        let expected_commit = if mirror_case { 3 } else { 2 };

        assert!(matches!(
            transaction.commit(&mut db),
            Err(Error::NeedsRecovery(_))
        ));
        assert!(matches!(
            transaction.state(),
            seerdb::BatchTransactionState::RecoveryRequired { commit }
                if commit.get() == expected_commit
        ));
        transaction.release().unwrap();
        drop(transaction);
        drop(db);

        let mut reopened = DB::open(&path, Options::for_test()).unwrap();
        let new_state = reopened.get(b"key-a").unwrap() == Some(b"after-a".to_vec());
        let old_state = reopened.get(b"key-a").unwrap() == Some(b"before-a".to_vec());
        assert!(
            old_state || new_state,
            "grouped publication exposed a partial key-a state"
        );
        match outcome {
            RecoveryOutcome::Old => assert!(old_state),
            RecoveryOutcome::New => assert!(new_state),
            RecoveryOutcome::OldOrNew => {}
        }

        let expected_new = new_state;
        assert_eq!(
            reopened.get(b"blob-key").unwrap(),
            Some(if expected_new { new_blob } else { old_blob })
        );
        assert_eq!(
            reopened.get(b"remove-key").unwrap(),
            if expected_new {
                None
            } else {
                Some(b"before-remove".to_vec())
            }
        );
        assert_eq!(
            reopened.get(b"new-key").unwrap(),
            if expected_new {
                Some(b"after-new".to_vec())
            } else {
                None
            }
        );
        assert!(!reopened.durability_status().write_fenced);
        reopened.verify().unwrap();
    }

    let root = tempdir().unwrap();
    run_case(
        root.path(),
        "grouped-before-wal.db",
        DB::inject_wal_write_failure,
        RecoveryOutcome::Old,
    );
    run_case(
        root.path(),
        "grouped-page-sync.db",
        DB::inject_page_range_sync_failure,
        RecoveryOutcome::Old,
    );
    run_case(
        root.path(),
        "grouped-manifest-sync.db",
        DB::inject_manifest_sync_failure,
        RecoveryOutcome::OldOrNew,
    );
    run_case(
        root.path(),
        "grouped-manifest-mirror-sync.db",
        DB::inject_manifest_mirror_sync_failure,
        RecoveryOutcome::Old,
    );
    run_case(
        root.path(),
        "grouped-publication-directory-sync.db",
        DB::inject_publication_directory_sync_failure,
        RecoveryOutcome::OldOrNew,
    );
    run_case(
        root.path(),
        "grouped-after-manifest.db",
        DB::inject_after_manifest_failure,
        RecoveryOutcome::New,
    );
    // wal-truncate case retired with WAL retention.
}

#[test]
fn dbnext_r0_batch_transaction_pin_is_not_durable_snapshot_state() {
    let root = tempdir().unwrap();
    let path = root.path().join("ephemeral-transaction.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();

    let mut transaction = db.begin_batch_transaction().unwrap();
    let retained_blob = fs::read_dir(&path)
        .unwrap()
        .find_map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name();
            name.to_str()
                .filter(|name| name.starts_with("seerdb.blob.retained."))
                .map(|_| entry.path())
        })
        .unwrap();
    assert!(!path.join("seerdb.retained").exists());

    let orphan = path.join("seerdb.blob.retained.18446744073709551614");
    fs::copy(&retained_blob, &orphan).unwrap();
    transaction.abort().unwrap();
    assert!(!retained_blob.exists());

    db.close().unwrap();
    let reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    assert!(!orphan.exists());
    assert!(!path.join("seerdb.retained").exists());
}
