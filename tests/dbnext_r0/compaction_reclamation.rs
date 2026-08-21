//! DBNext R0/R1 compaction, reclamation, and maintenance-fault tests.

use super::*;

#[test]
fn dbnext_r0_compact_truncates_reclaimable_tail() {
    let root = tempdir().unwrap();
    let path = root.path().join("compact.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    let first_bytes = fs::metadata(path.join("seerdb.data")).unwrap().len();

    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();
    let second_bytes = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert!(second_bytes > first_bytes);

    db.put(b"key", b"value-3").unwrap();
    db.flush().unwrap();
    let before = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert_eq!(before, second_bytes);

    let report = db.compact().unwrap();
    assert!(report.manifest_replicated);
    assert!(report.reclaimed_pages > 0);
    assert_eq!(report.data_bytes_before, before);
    assert!(report.data_bytes_after < report.data_bytes_before);
    assert_eq!(db.get(b"key").unwrap(), Some(b"value-3".to_vec()));
    assert_eq!(db.verify().unwrap().data_bytes, report.data_bytes_after);

    drop(db);
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-3".to_vec()));
    assert_eq!(
        reopened.verify().unwrap().data_bytes,
        report.data_bytes_after
    );
}

#[test]
fn dbnext_r0_compact_relocates_interior_pages_before_truncation() {
    let root = tempdir().unwrap();
    let path = root.path().join("compact-interior.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    let value = vec![0x2Au8; 128];

    for key_id in 0..256 {
        let key = format!("key-{key_id:04}");
        db.put(key.as_bytes(), &value).unwrap();
    }
    db.flush().unwrap();

    let updated = vec![0x3Bu8; 128];
    db.put(b"key-0128", &updated).unwrap();
    db.flush().unwrap();
    let before = fs::metadata(path.join("seerdb.data")).unwrap().len();

    let report = db.compact().unwrap();
    assert!(
        report.relocated_pages > 0,
        "expected an interior relocation"
    );
    assert!(report.data_bytes_after < before);
    assert_eq!(db.get(b"key-0128").unwrap(), Some(updated.clone()));
    assert!(db.verify().is_ok());
    let retained = db.retain_commit(db.durability_status().commit_id).unwrap();
    assert_eq!(
        db.get_at(retained, b"key-0128").unwrap(),
        Some(updated.clone())
    );
    db.release_snapshot(retained).unwrap();

    drop(db);
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key-0128").unwrap(), Some(updated));
    assert!(reopened.verify().is_ok());
}

#[test]
fn dbnext_r0_bounded_compaction_reopens_between_maintenance_steps() {
    let root = tempdir().unwrap();
    let path = root.path().join("compact-bounded.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    let value = vec![0x52u8; 128];

    for key_id in 0..256 {
        let key = format!("key-{key_id:04}");
        db.put(key.as_bytes(), &value).unwrap();
    }
    db.flush().unwrap();

    let updated = vec![0x63u8; 128];
    db.put(b"key-0128", &updated).unwrap();
    db.flush().unwrap();

    let mut reports = Vec::new();
    for _ in 0..8 {
        let report = db.compact_with_limit(1).unwrap();
        assert!(report.relocated_pages <= 1);
        assert_eq!(db.get(b"key-0128").unwrap(), Some(updated.clone()));
        assert!(db.verify().is_ok());
        let finished =
            report.relocated_pages == 0 && report.data_bytes_after == report.data_bytes_before;
        reports.push(report);

        drop(db);
        db = DB::open(&path, Options::default()).unwrap();
        assert_eq!(db.get(b"key-0128").unwrap(), Some(updated.clone()));
        assert!(db.verify().is_ok());
        if finished {
            break;
        }
    }

    assert!(reports.iter().any(|report| report.relocated_pages == 1));
    drop(db);
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key-0128").unwrap(), Some(updated));
    assert!(reopened.verify().is_ok());
}

#[test]
fn dbnext_r0_sustained_reclamation_preserves_retained_root_and_recovers_space() {
    let root = tempdir().unwrap();
    let path = root.path().join("sustained-reclamation.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    let value = vec![0x31u8; 192];
    let key_count = 64usize;
    let mut model = BTreeMap::new();

    for key_id in 0..key_count {
        let key = format!("key-{key_id:04}");
        db.put(key.as_bytes(), &value).unwrap();
        model.insert(key.into_bytes(), value.clone());
    }
    db.flush().unwrap();
    let retained_commit = db.durability_status().commit_id;
    let retained = db.retain_commit(retained_commit).unwrap();
    let retained_value = db.get_at(retained, b"key-0000").unwrap();
    let initial_bytes = fs::metadata(path.join("seerdb.data")).unwrap().len();
    let mut peak_bytes = initial_bytes;

    for round in 0..24usize {
        for offset in 0..8usize {
            let key_id = (round * 8 + offset) % key_count;
            let key = format!("key-{key_id:04}");
            if (round + offset).is_multiple_of(7) {
                db.delete(key.as_bytes()).unwrap();
                model.remove(key.as_bytes());
            } else {
                let next_value = vec![(0x40 + (round % 32)) as u8; 192 + (offset % 3) * 16];
                db.put(key.as_bytes(), &next_value).unwrap();
                model.insert(key.into_bytes(), next_value);
            }
        }
        db.flush().unwrap();

        for _ in 0..8 {
            let report = db.compact_with_limit(2).unwrap();
            assert!(report.relocated_pages <= 2);
            assert!(db.verify().is_ok());
            if report.relocated_pages == 0 && report.data_bytes_before == report.data_bytes_after {
                break;
            }
        }

        let current_bytes = fs::metadata(path.join("seerdb.data")).unwrap().len();
        peak_bytes = peak_bytes.max(current_bytes);
        for key_id in 0..key_count {
            let key = format!("key-{key_id:04}");
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                model.get(key.as_bytes()).cloned()
            );
        }
        assert_eq!(db.get_at(retained, b"key-0000").unwrap(), retained_value);

        if round % 6 == 5 {
            drop(db);
            db = DB::open(&path, Options::default()).unwrap();
            assert!(db.verify().is_ok());
            assert_eq!(db.get_at(retained, b"key-0000").unwrap(), retained_value);
        }
    }

    let protected_bytes = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert!(
        peak_bytes > initial_bytes,
        "retained history did not exercise physical growth"
    );
    db.release_snapshot(retained).unwrap();
    for _ in 0..32 {
        let report = db.compact_with_limit(4).unwrap();
        if report.relocated_pages == 0 && report.data_bytes_before == report.data_bytes_after {
            break;
        }
    }
    let report = db.vacuum().unwrap();
    assert_eq!(report.live_entries, model.len() as u64);
    db.compact().unwrap();
    let final_bytes = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert!(
        final_bytes < peak_bytes,
        "maintenance did not recover space"
    );
    assert!(
        final_bytes < protected_bytes,
        "releasing the retained root did not make its pages reclaimable"
    );
    for (key, value) in &model {
        assert_eq!(db.get(key).unwrap(), Some(value.clone()));
    }
    assert!(db.verify().is_ok());

    drop(db);
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    for (key, value) in &model {
        assert_eq!(reopened.get(key).unwrap(), Some(value.clone()));
    }
    assert!(reopened.verify().is_ok());
}

#[test]
fn dbnext_r0_sustained_segmented_blob_reclamation_preserves_retained_root() {
    let root = tempdir().unwrap();
    let path = root.path().join("sustained-segmented-reclamation.db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        ..Options::default()
    };
    let mut db = DB::create(&path, options).unwrap();
    let key_count = 24usize;
    let rounds = 12usize;
    let blob_bytes = || {
        fs::read_dir(&path)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("seerdb.blob.segment.")
            })
            .map(|entry| entry.metadata().unwrap().len())
            .sum::<u64>()
    };
    let value = |round: usize, key_id: usize| {
        vec![(0x40 + ((round + key_id) % 32)) as u8; 2_048 + ((round + key_id) % 4) * 256]
    };
    let mut model = BTreeMap::new();

    for key_id in 0..key_count {
        let key = format!("key-{key_id:02}").into_bytes();
        let value = value(0, key_id);
        db.put(&key, &value).unwrap();
        model.insert(key, value);
    }
    db.flush().unwrap();
    let retained_commit = db.durability_status().commit_id;
    let retained = db.retain_commit(retained_commit).unwrap();
    let retained_value = db.get_at(retained, b"key-00").unwrap().unwrap();
    let initial_segment_bytes = blob_bytes();
    let mut peak_segment_bytes = initial_segment_bytes;

    for round in 1..=rounds {
        for key_id in 0..key_count {
            let key = format!("key-{key_id:02}").into_bytes();
            if (round + key_id).is_multiple_of(11) {
                db.delete(&key).unwrap();
                model.remove(&key);
            } else {
                let next_value = value(round, key_id);
                db.put(&key, &next_value).unwrap();
                model.insert(key, next_value);
            }
        }
        db.flush().unwrap();

        for _ in 0..8 {
            let report = db.compact_with_limit(2).unwrap();
            assert!(report.relocated_pages <= 2);
            db.verify().unwrap();
            if report.relocated_pages == 0 && report.data_bytes_before == report.data_bytes_after {
                break;
            }
        }

        peak_segment_bytes = peak_segment_bytes.max(blob_bytes());
        assert_model(&db, &model);
        assert_eq!(
            db.get_at(retained, b"key-00").unwrap(),
            Some(retained_value.clone())
        );
        if round.is_multiple_of(3) {
            db.close().unwrap();
            db = DB::open(&path, Options::default()).unwrap();
            db.verify().unwrap();
            assert_model(&db, &model);
            assert_eq!(
                db.get_at(retained, b"key-00").unwrap(),
                Some(retained_value.clone())
            );
        }
    }

    assert!(peak_segment_bytes > initial_segment_bytes);
    assert_eq!(
        db.gc().unwrap(),
        0,
        "retained root must block blob reclamation"
    );
    db.release_snapshot(retained).unwrap();
    assert!(db.gc().unwrap() > 0);
    assert_eq!(db.blob_stats().files_needing_gc, 0);
    assert_eq!(db.blob_stats().total_deleted, 0);
    db.vacuum().unwrap();
    db.compact().unwrap();
    assert!(blob_bytes() < peak_segment_bytes);
    assert_model(&db, &model);
    db.verify().unwrap();

    db.close().unwrap();
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    reopened.verify().unwrap();
    assert_model(&reopened, &model);
    assert_eq!(reopened.blob_stats().files_needing_gc, 0);
    assert_eq!(reopened.blob_stats().total_deleted, 0);
}

#[test]
fn dbnext_r0_interior_compaction_sync_failure_recovers_old_generation() {
    let root = tempdir().unwrap();
    let path = root.path().join("compact-interior-fault.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    let value = vec![0x4Cu8; 128];
    for key_id in 0..256 {
        let key = format!("key-{key_id:04}");
        db.put(key.as_bytes(), &value).unwrap();
    }
    db.flush().unwrap();
    let updated = vec![0x5Du8; 128];
    db.put(b"key-0128", &updated).unwrap();
    db.flush().unwrap();

    db.inject_sync_failure();
    assert!(matches!(db.compact(), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);

    drop(db);
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key-0128").unwrap(), Some(updated));
    assert!(reopened.verify().is_ok());
}

#[test]
fn dbnext_r0_compact_failure_fences_writer_until_reopen() {
    let root = tempdir().unwrap();
    let path = root.path().join("compact-fault.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-3").unwrap();
    db.flush().unwrap();

    db.inject_sync_failure();
    assert!(matches!(db.compact(), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    assert!(matches!(
        db.put(b"after-fault", b"value"),
        Err(Error::NeedsRecovery(_))
    ));

    drop(db);
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-3".to_vec()));
    assert!(!reopened.durability_status().write_fenced);
}

#[test]
fn dbnext_r0_manifest_sync_fault_fences_compaction_and_recovers() {
    let root = tempdir().unwrap();
    let path = root.path().join("manifest-sync-fault.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    let value = vec![0x4Cu8; 128];
    for key_id in 0..256 {
        let key = format!("key-{key_id:04}");
        db.put(key.as_bytes(), &value).unwrap();
    }
    db.flush().unwrap();
    let updated = vec![0x5Du8; 128];
    db.put(b"key-0128", &updated).unwrap();
    db.flush().unwrap();

    db.inject_manifest_sync_failure();
    assert!(matches!(db.compact(), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    assert!(matches!(
        db.put(b"after-fault", b"value"),
        Err(Error::NeedsRecovery(_))
    ));

    drop(db);
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key-0128").unwrap(), Some(updated));
    assert!(!reopened.durability_status().write_fenced);
}

#[test]
fn dbnext_r0_post_manifest_and_wal_truncate_faults_recover() {
    fn run_case<F>(root: &Path, name: &str, inject: F, expect_wal_before_reopen: bool)
    where
        F: FnOnce(&DB),
    {
        let path = root.join(name);
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();

        inject(&db);
        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert!(db.durability_status().write_fenced);
        assert_eq!(path.join("seerdb.wal").exists(), expect_wal_before_reopen);

        drop(db);
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        assert!(!reopened.durability_status().write_fenced);
        assert!(!path.join("seerdb.wal").exists());
    }

    let root = tempdir().unwrap();
    run_case(
        root.path(),
        "post-manifest.db",
        DB::inject_after_manifest_failure,
        true,
    );
    run_case(
        root.path(),
        "wal-truncate.db",
        DB::inject_wal_truncate_failure,
        false,
    );
}

#[test]
fn dbnext_r0_short_and_torn_checkpoint_frames_preserve_prior_generation() {
    fn run_case<F>(root: &Path, name: &str, inject: F)
    where
        F: FnOnce(&DB),
    {
        let path = root.join(name);
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();

        inject(&db);
        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
        assert!(!reopened.durability_status().write_fenced);
        // The failed generation was never named by a manifest, so whether its
        // frame reached the file (sync seam) or not (write seam), the old
        // root stays authoritative and a retry reuses the generation ID.
        reopened.verify().unwrap();
    }

    let root = tempdir().unwrap();
    run_case(
        root.path(),
        "short-checkpoint.db",
        DB::inject_meta_log_write_failure,
    );
    run_case(
        root.path(),
        "torn-checkpoint.db",
        DB::inject_meta_log_sync_failure,
    );
}

#[test]
fn dbnext_r0_post_page_write_failure_preserves_prior_manifest_generation() {
    let root = tempdir().unwrap();
    let path = root.path().join("post-page-write.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.inject_after_write_failure();

    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
    assert!(!reopened.durability_status().write_fenced);
}

#[test]
fn dbnext_r0_final_write_disk_full_fences_and_recovers_prior_generation() {
    let root = tempdir().unwrap();
    let path = root.path().join("final-write-disk-full.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.inject_final_write_disk_full();

    assert!(matches!(db.flush(), Err(Error::DiskFull)));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
    assert_eq!(reopened.durability_status().commit_id.get(), 1);
    assert!(!reopened.durability_status().write_fenced);
    assert_eq!(reopened.verify().unwrap().wal_bytes, 0);
}
