//! DBNext R0/R1 snapshot, retained-root, and retention-registry tests.

use super::*;

#[test]
fn dbnext_r0_snapshot_is_verified_and_source_is_unchanged() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("snapshot-source.db");
    let snapshot_path = root.path().join("snapshot-copy.db");
    let large_value = vec![0x5Au8; 2_048];
    let mut source = DB::open(&source_path, Options::default()).unwrap();
    source.put(b"inline", b"value").unwrap();
    source.put(b"large", &large_value).unwrap();
    source.flush().unwrap();

    let source_report = source.verify().unwrap();
    assert!(source_report.verified_pages > 0);
    // Retained WAL: verify reports the retained log instead of zero.
    assert!(source_report.wal_bytes > 0);

    let snapshot = source.snapshot(&snapshot_path).unwrap();
    assert_eq!(snapshot.source, source_report.durability);
    assert_eq!(snapshot.destination, source_report.durability);
    assert!(snapshot.copied_files >= 3);
    assert_eq!(snapshot.verified_pages, source_report.verified_pages);
    assert_eq!(source.get(b"inline").unwrap(), Some(b"value".to_vec()));
    assert_eq!(source.get(b"large").unwrap(), Some(large_value.clone()));

    let mut restored = DB::open(&snapshot_path, Options::default()).unwrap();
    let restored_report = restored.verify().unwrap();
    assert_eq!(restored_report.durability, source_report.durability);
    assert!(matches!(
        restored.put(b"must-not-write", b"value"),
        Err(Error::ReadOnly)
    ));

    source.put(b"inline", b"source-updated").unwrap();
    source.delete(b"large").unwrap();
    source.flush().unwrap();
    source.compact().unwrap();
    assert_eq!(
        source.get(b"inline").unwrap(),
        Some(b"source-updated".to_vec())
    );
    assert_eq!(source.get(b"large").unwrap(), None);

    // The verified snapshot is an independent retained root, so source
    // mutation and compaction cannot alter its historical state.
    assert_eq!(restored.get(b"inline").unwrap(), Some(b"value".to_vec()));
    assert_eq!(restored.get(b"large").unwrap(), Some(large_value));
}

#[test]
fn dbnext_r0_owned_snapshot_releases_retained_copy() {
    let root = tempdir().unwrap();
    let path = root.path().join("owned-snapshot-source.db");
    let large_value = vec![0x3Cu8; 2_048];
    let mut source = DB::open(&path, Options::default()).unwrap();
    source.put(b"inline", b"before").unwrap();
    source.put(b"large", &large_value).unwrap();
    source.flush().unwrap();

    let mut snapshot = source.begin_snapshot().unwrap();
    let snapshot_path = snapshot.path().to_path_buf();
    assert_eq!(snapshot.verify().unwrap().wal_bytes, 0);

    source.put(b"inline", b"after").unwrap();
    source.delete(b"large").unwrap();
    source.flush().unwrap();
    source.compact().unwrap();

    assert_eq!(snapshot.get(b"inline").unwrap(), Some(b"before".to_vec()));
    assert_eq!(snapshot.get(b"large").unwrap(), Some(large_value));

    snapshot.release().unwrap();
    assert!(!snapshot_path.exists());
}

#[test]
fn dbnext_r0_owned_retention_handles_keep_independent_leases() {
    let root = tempdir().unwrap();
    let path = root.path().join("owned-retention-leases.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"before").unwrap();
    db.flush().unwrap();

    let first = db.retain_current().unwrap();
    let first_id = first.snapshot_id();
    let second = db.retain_current().unwrap();
    let second_id = second.snapshot_id();
    assert_ne!(first_id, second_id);
    assert_eq!(first.get(b"key").unwrap(), Some(b"before".to_vec()));
    assert_eq!(second.get(b"key").unwrap(), Some(b"before".to_vec()));

    db.put(b"key", b"after").unwrap();
    db.flush().unwrap();
    first.release().unwrap();

    assert!(matches!(
        db.get_at(first_id, b"key"),
        Err(Error::SnapshotUnavailable(_))
    ));
    assert_eq!(
        db.get_at(second_id, b"key").unwrap(),
        Some(b"before".to_vec())
    );
    second.release().unwrap();
    assert!(!path.join("seerdb.retained").exists());
}

#[test]
fn dbnext_r0_retained_root_pins_page_reuse_until_release() {
    let root = tempdir().unwrap();
    let path = root.path().join("retained-root.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"before").unwrap();
    db.flush().unwrap();

    let retained = db.retain_current().unwrap();
    let snapshot_id = retained.snapshot_id();
    assert!(path.join("seerdb.retained").is_file());
    assert_eq!(retained.get(b"key").unwrap(), Some(b"before".to_vec()));

    db.put(b"key", b"after-one").unwrap();
    db.flush().unwrap();
    let first_growth = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert_eq!(db.metrics().unwrap().reclaimable_pages, 0);
    assert_eq!(db.gc().unwrap(), 0);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"after-one".to_vec()));
    assert_eq!(retained.get(b"key").unwrap(), Some(b"before".to_vec()));
    reopened.put(b"key", b"after-two").unwrap();
    reopened.flush().unwrap();
    let second_growth = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert!(second_growth > first_growth);
    // The retained first generation keeps offset zero live; the intermediate
    // generation can already be reused after the second publication.
    assert_eq!(reopened.metrics().unwrap().reclaimable_pages, 1);

    retained.release().unwrap();
    drop(reopened);

    let mut released = DB::open(&path, Options::default()).unwrap();
    released.put(b"key", b"after-release").unwrap();
    released.flush().unwrap();
    let after_release = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert_eq!(after_release, second_growth);
    assert!(!path.join("seerdb.retained").exists());
    assert_ne!(snapshot_id.get(), 0);
}

#[test]
fn dbnext_r1_retained_root_reads_inline_and_blob_values() {
    let root = tempdir().unwrap();
    let path = root.path().join("retained-reads.db");
    let large_before = vec![0x41u8; 2_048];
    let large_after = vec![0x42u8; 2_048];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"inline", b"before").unwrap();
    db.put(b"large", &large_before).unwrap();
    db.flush().unwrap();

    let retained = db.retain_current().unwrap();
    let snapshot_id = retained.snapshot_id();
    db.put(b"inline", b"after").unwrap();
    db.put(b"large", &large_after).unwrap();
    db.flush().unwrap();

    assert_eq!(
        db.get_at(snapshot_id, b"inline").unwrap(),
        Some(b"before".to_vec())
    );
    assert_eq!(
        db.get_at(snapshot_id, b"large").unwrap(),
        Some(large_before.clone())
    );
    assert_eq!(
        db.range_at(snapshot_id, b"inline", b"large\0").unwrap(),
        vec![
            (b"inline".to_vec(), b"before".to_vec()),
            (b"large".to_vec(), large_before.clone()),
        ]
    );

    drop(db);
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get_at(snapshot_id, b"large").unwrap(),
        Some(large_before)
    );
    assert_eq!(reopened.get(b"large").unwrap(), Some(large_after));
    drop(reopened);
    retained.release().unwrap();
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert!(matches!(
        reopened.get_at(snapshot_id, b"inline"),
        Err(Error::SnapshotUnavailable(_))
    ));
}

#[test]
fn dbnext_r1_grouped_transaction_preserves_retained_root_through_maintenance() {
    let root = tempdir().unwrap();
    let path = root.path().join("grouped-retained-maintenance.db");
    let old_blob = vec![0x51u8; 2_048];
    let new_blob = vec![0x52u8; 2_048];
    let mut db = DB::open(&path, Options::default()).unwrap();

    let initial = (0..32)
        .map(|index| BatchMutation::Put {
            key: format!("group-retained-{index:03}").into_bytes(),
            value: format!("before-{index:03}").into_bytes(),
        })
        .chain(std::iter::once(BatchMutation::Put {
            key: b"group-retained-blob".to_vec(),
            value: old_blob.clone(),
        }))
        .collect::<Vec<_>>();
    db.commit_batch(&initial).unwrap();

    let retained = db.retain_current().unwrap();
    let snapshot_id = retained.snapshot_id();
    let mut transaction = db.begin_batch_transaction().unwrap();
    for index in 0..32 {
        let key = format!("group-retained-{index:03}");
        let value = format!("after-{index:03}");
        transaction.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    transaction.put(b"group-retained-blob", &new_blob).unwrap();
    transaction
        .put(b"group-retained-new", b"after-new")
        .unwrap();
    transaction.delete(b"group-retained-000").unwrap();
    transaction.commit(&mut db).unwrap();

    assert_eq!(
        db.get(b"group-retained-001").unwrap(),
        Some(b"after-001".to_vec())
    );
    assert_eq!(
        db.get(b"group-retained-new").unwrap(),
        Some(b"after-new".to_vec())
    );
    assert_eq!(db.get(b"group-retained-000").unwrap(), None);
    assert_eq!(
        retained.get(b"group-retained-001").unwrap(),
        Some(b"before-001".to_vec())
    );
    assert_eq!(
        retained.get(b"group-retained-blob").unwrap(),
        Some(old_blob.clone())
    );

    drop(db);
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    for _ in 0..8 {
        reopened.compact_with_limit(2).unwrap();
    }
    reopened.vacuum().unwrap();
    reopened.verify().unwrap();
    assert_eq!(
        reopened.get(b"group-retained-001").unwrap(),
        Some(b"after-001".to_vec())
    );
    assert_eq!(
        reopened.get(b"group-retained-new").unwrap(),
        Some(b"after-new".to_vec())
    );
    assert_eq!(reopened.get(b"group-retained-000").unwrap(), None);
    assert_eq!(
        reopened.get(b"group-retained-blob").unwrap(),
        Some(new_blob)
    );
    assert_eq!(
        reopened.get_at(snapshot_id, b"group-retained-001").unwrap(),
        Some(b"before-001".to_vec())
    );
    assert_eq!(
        retained.get(b"group-retained-blob").unwrap(),
        Some(old_blob)
    );
    drop(reopened);

    let mut after_reopen = DB::open(&path, Options::default()).unwrap();
    after_reopen.verify().unwrap();
    assert_eq!(
        after_reopen
            .get_at(snapshot_id, b"group-retained-001")
            .unwrap(),
        Some(b"before-001".to_vec())
    );
    retained.release().unwrap();
    drop(after_reopen);

    let final_db = DB::open(&path, Options::default()).unwrap();
    assert!(matches!(
        final_db.get_at(snapshot_id, b"group-retained-001"),
        Err(Error::SnapshotUnavailable(_))
    ));
    assert!(!path.join("seerdb.retained").exists());
}

#[test]
fn dbnext_r1_release_refreshes_reuse_without_reopen() {
    let root = tempdir().unwrap();
    let path = root.path().join("retained-release-refresh.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"before").unwrap();
    db.flush().unwrap();
    let retained = db.retain_current().unwrap();

    db.put(b"key", b"after-retained").unwrap();
    db.flush().unwrap();
    let retained_size = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert_eq!(db.metrics().unwrap().reclaimable_pages, 0);

    retained.release().unwrap();
    db.put(b"key", b"after-release").unwrap();
    db.flush().unwrap();
    assert_eq!(
        fs::metadata(path.join("seerdb.data")).unwrap().len(),
        retained_size
    );
    assert_eq!(db.get(b"key").unwrap(), Some(b"after-release".to_vec()));
}

#[test]
fn dbnext_r0_corrupt_retention_registry_refuses_open() {
    let root = tempdir().unwrap();
    let path = root.path().join("retained-corrupt.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    let retained = db.retain_current().unwrap();
    drop(db);

    let retention_path = path.join("seerdb.retained");
    let mut bytes = fs::read(&retention_path).unwrap();
    bytes[0] ^= 0xFF;
    fs::write(&retention_path, bytes).unwrap();
    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("retention registry")
    ));
    drop(retained);
}

#[test]
fn dbnext_r1_missing_retained_blob_image_refuses_open() {
    let root = tempdir().unwrap();
    let path = root.path().join("retained-blob-missing.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    let retained = db.retain_current().unwrap();
    let snapshot_id = retained.snapshot_id();
    drop(db);

    fs::remove_file(path.join(format!("seerdb.blob.retained.{}", snapshot_id.get()))).unwrap();
    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("blob image")
    ));
    drop(retained);
}

#[test]
fn dbnext_r1_multiple_retained_roots_read_distinct_generations() {
    let root = tempdir().unwrap();
    let path = root.path().join("retained-multiple.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    let mut retained = Vec::new();
    let mut expected = Vec::new();
    for generation in 0..4 {
        let value = format!("value-{generation}");
        db.put(b"key", value.as_bytes()).unwrap();
        db.flush().unwrap();
        let snapshot = db.retain_current().unwrap();
        expected.push((snapshot.snapshot_id(), value.into_bytes()));
        retained.push(snapshot);
    }

    db.put(b"key", b"current").unwrap();
    db.flush().unwrap();
    for (snapshot_id, value) in &expected {
        assert_eq!(
            db.get_at(*snapshot_id, b"key").unwrap(),
            Some(value.clone())
        );
    }

    for snapshot in retained {
        snapshot.release().unwrap();
    }
    assert!(!path.join("seerdb.retained").exists());
}
