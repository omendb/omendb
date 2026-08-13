//! Segmented blob catalog qualification and publication tests.

use super::blob_layout::{
    BLOB_DELTA_FILE, BLOB_FILE, BLOB_REWRITE_BACKUP_FILE, MAX_SEGMENTED_CATALOG_DELETED_ENTRIES,
    blob_segment_path, segmented_catalog_needs_consolidation,
};
use super::{BatchMutation, BlobManager, BlobStorageMode, DB, Options};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::fs;
use tempfile::tempdir;

const TEST_SEGMENT_CATALOG_DELTA_LIMIT: u32 = 64;

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
fn test_db_segmented_partial_append_failures_ignore_suffix() {
    let failures = [
        (
            "short",
            DB::inject_blob_segment_short_write_failure as fn(&DB),
        ),
        (
            "torn",
            DB::inject_blob_segment_torn_write_failure as fn(&DB),
        ),
    ];

    for (name, inject_failure) in failures {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join(format!("segmented-{name}-append-failure.db"));
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xE9; 2_000];
        let pending = vec![0xEA; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();

        let segment = blob_segment_path(&path, 1);
        let catalog_before = fs::read(path.join(BLOB_FILE)).unwrap();
        let segment_len_before = fs::metadata(&segment).unwrap().len();

        db.put(b"pending", &pending).unwrap();
        inject_failure(&db);
        assert!(db.flush().is_err(), "{name} suffix write should fail");
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
        reopened.verify().unwrap();
    }
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
fn test_db_segmented_catalog_delta_write_failures_discard_future_frame() {
    let failures = [
        (
            "after-write",
            DB::inject_blob_segment_catalog_after_write_failure as fn(&DB),
        ),
        (
            "sync",
            DB::inject_blob_segment_catalog_sync_failure as fn(&DB),
        ),
    ];

    for (name, inject_failure) in failures {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join(format!("segmented-catalog-delta-{name}-failure.db"));
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
        inject_failure(&db);
        assert!(
            db.flush().is_err(),
            "{name} catalog delta write should fail"
        );
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
