//! Property checks for the public durable byte-KV contract.

#![allow(clippy::disallowed_methods)]

use proptest::prelude::*;
#[cfg(feature = "fault-injection")]
use seerdb::Error;
use seerdb::{BatchMutation, BatchTransactionState, BlobStorageMode, DB, Options};
use std::collections::BTreeMap;
#[cfg(feature = "fault-injection")]
use std::fs;
use tempfile::tempdir;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn key(key_id: u8) -> Vec<u8> {
    format!("property-key-{key_id:03}").into_bytes()
}

#[cfg(feature = "fault-injection")]
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_shrink_iters: 1_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn capacity_refusal_preserves_retryable_blob_layouts(
        key_id in 1u8..32,
        value in prop::collection::vec(any::<u8>(), 0..512)
    ) {
        for segmented in [false, true] {
            let directory = tempdir().unwrap();
            let path = directory.path().join("capacity-property.db");
            let options = Options {
                blob_storage: if segmented {
                    BlobStorageMode::Segmented
                } else {
                    BlobStorageMode::WholeImage
                },
                ..Options::for_test()
            };
            let mut db = DB::create(&path, options).unwrap();
            let blob_key = key(0);
            let blob_value = vec![0xC7; 2_048];
            db.commit_batch(&[BatchMutation::Put {
                key: blob_key.clone(),
                value: blob_value.clone(),
            }])
            .unwrap();
            db.verify().unwrap();

            let pending_key = key(key_id);
            db.put(&pending_key, &value).unwrap();
            let data_capacity = fs::metadata(path.join("seerdb.data")).unwrap().len();
            let before = db.metrics().unwrap().storage.physical_page_writes;
            db.inject_capacity_limit(data_capacity);
            assert!(matches!(db.flush(), Err(Error::CapacityPreflight)));
            let after = db.metrics().unwrap().storage.physical_page_writes;
            assert_eq!(after, before, "capacity refusal must not write pages");
            assert!(!db.durability_status().write_fenced);
            assert_eq!(db.get(&pending_key).unwrap(), Some(value.clone()));

            db.inject_capacity_limit(u64::MAX);
            db.flush().unwrap();
            assert_eq!(db.get(&pending_key).unwrap(), Some(value.clone()));
            db.verify().unwrap();
            drop(db);

            let mut reopened = DB::open(&path, Options::default()).unwrap();
            assert_eq!(reopened.get(&blob_key).unwrap(), Some(blob_value));
            assert_eq!(reopened.get(&pending_key).unwrap(), Some(value.clone()));
            reopened.verify().unwrap();
        }
    }
}

#[cfg(feature = "fault-injection")]
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 4,
        max_shrink_iters: 1_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn mixed_blob_gc_faults_preserve_retryable_catalog(
        live_value in prop::collection::vec(any::<u8>(), 1_025..2_049),
        dead_value in prop::collection::vec(any::<u8>(), 1_025..2_049)
    ) {
        for segmented in [false, true] {
            for fault in 0u8..10 {
                if !segmented && fault >= 3 {
                    continue;
                }

                let directory = tempdir().unwrap();
                let path = directory.path().join("mixed-gc-fault-property.db");
                let options = Options {
                    blob_storage: if segmented {
                        BlobStorageMode::Segmented
                    } else {
                        BlobStorageMode::WholeImage
                    },
                    ..Options::for_test()
                };
                let mut db = DB::create(&path, options).unwrap();
                db.commit_batch(&[
                    BatchMutation::Put {
                        key: b"gc-live".to_vec(),
                        value: live_value.clone(),
                    },
                    BatchMutation::Put {
                        key: b"gc-dead-1".to_vec(),
                        value: dead_value.clone(),
                    },
                    BatchMutation::Put {
                        key: b"gc-dead-2".to_vec(),
                        value: dead_value.clone(),
                    },
                ])
                .unwrap();
                db.commit_batch(&[
                    BatchMutation::Delete {
                        key: b"gc-dead-1".to_vec(),
                    },
                    BatchMutation::Delete {
                        key: b"gc-dead-2".to_vec(),
                    },
                ])
                .unwrap();

                match fault {
                    0 => db.inject_after_blob_rewrite_image_failure(),
                    1 => db.inject_final_write_disk_full(),
                    2 => db.inject_atomic_rename_failure(),
                    3 => db.inject_blob_segment_after_write_failure(),
                    4 => db.inject_blob_segment_catalog_rename_failure(),
                    5 => db.inject_blob_segment_catalog_short_write_failure(),
                    6 => db.inject_blob_segment_catalog_torn_write_failure(),
                    7 => db.inject_blob_segment_sync_failure(),
                    8 => db.inject_blob_segment_catalog_sync_failure(),
                    9 => db.inject_blob_segment_catalog_after_write_failure(),
                    _ => unreachable!(),
                }
                assert!(db.gc().is_err());
                assert!(db.durability_status().write_fenced);
                drop(db);

                let mut reopened = DB::open(&path, Options::default()).unwrap();
                assert!(
                    fs::read_dir(&path)
                        .unwrap()
                        .filter_map(Result::ok)
                        .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp")),
                    "reopen must clean non-authoritative publication temporary files"
                );
                assert_eq!(
                    reopened.get(b"gc-live").unwrap(),
                    Some(live_value.clone())
                );
                assert_eq!(reopened.get(b"gc-dead-1").unwrap(), None);
                assert_eq!(reopened.get(b"gc-dead-2").unwrap(), None);
                assert!(reopened.blob_stats().files_needing_gc > 0);
                reopened.verify().unwrap();

                assert!(reopened.gc().unwrap() > 0);
                assert_eq!(
                    reopened.get(b"gc-live").unwrap(),
                    Some(live_value.clone())
                );
                assert_eq!(reopened.get(b"gc-dead-1").unwrap(), None);
                assert_eq!(reopened.get(b"gc-dead-2").unwrap(), None);
                reopened.verify().unwrap();
                drop(reopened);

                let mut reopened_again = DB::open(&path, Options::default()).unwrap();
                assert_eq!(
                    reopened_again.get(b"gc-live").unwrap(),
                    Some(live_value.clone())
                );
                assert_eq!(reopened_again.get(b"gc-dead-1").unwrap(), None);
                assert_eq!(reopened_again.get(b"gc-dead-2").unwrap(), None);
                reopened_again.verify().unwrap();
            }
        }
    }
}

#[cfg(feature = "fault-injection")]
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_shrink_iters: 1_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn mixed_blob_gc_capacity_refusal_preserves_catalog(
        live_value in prop::collection::vec(any::<u8>(), 1_025..2_049),
        dead_value in prop::collection::vec(any::<u8>(), 1_025..2_049)
    ) {
        for segmented in [false, true] {
            let directory = tempdir().unwrap();
            let path = directory.path().join("mixed-gc-capacity-property.db");
            let options = Options {
                blob_storage: if segmented {
                    BlobStorageMode::Segmented
                } else {
                    BlobStorageMode::WholeImage
                },
                ..Options::for_test()
            };
            let mut db = DB::create(&path, options).unwrap();
            db.commit_batch(&[
                BatchMutation::Put {
                    key: b"gc-live".to_vec(),
                    value: live_value.clone(),
                },
                BatchMutation::Put {
                    key: b"gc-dead-1".to_vec(),
                    value: dead_value.clone(),
                },
                BatchMutation::Put {
                    key: b"gc-dead-2".to_vec(),
                    value: dead_value.clone(),
                },
            ])
            .unwrap();
            db.commit_batch(&[
                BatchMutation::Delete {
                    key: b"gc-dead-1".to_vec(),
                },
                BatchMutation::Delete {
                    key: b"gc-dead-2".to_vec(),
                },
            ])
            .unwrap();
            let before_stats = db.blob_stats();
            let before_page_writes = db.metrics().unwrap().storage.physical_page_writes;
            let data_capacity = fs::metadata(path.join("seerdb.data")).unwrap().len();

            db.inject_capacity_limit(data_capacity);
            let refusal = db.gc();
            assert!(
                matches!(refusal, Err(Error::DiskFull | Error::CapacityPreflight)),
                "unexpected mixed-GC result: segmented={segmented}, data_capacity={data_capacity}, valid={}, deleted={}, files={}, result={refusal:?}",
                before_stats.total_valid,
                before_stats.total_deleted,
                before_stats.files_needing_gc
            );
            let after_stats = db.blob_stats();
            assert_eq!(after_stats.total_valid, before_stats.total_valid);
            assert_eq!(after_stats.total_deleted, before_stats.total_deleted);
            assert_eq!(
                after_stats.files_needing_gc,
                before_stats.files_needing_gc
            );
            assert_eq!(
                db.metrics().unwrap().storage.physical_page_writes,
                before_page_writes
            );
            assert!(!db.durability_status().write_fenced);
            assert_eq!(db.get(b"gc-live").unwrap(), Some(live_value.clone()));

            db.inject_capacity_limit(u64::MAX);
            assert!(db.gc().unwrap() > 0);
            assert_eq!(db.get(b"gc-live").unwrap(), Some(live_value.clone()));
            assert_eq!(db.get(b"gc-dead-1").unwrap(), None);
            assert_eq!(db.get(b"gc-dead-2").unwrap(), None);
            db.verify().unwrap();
            drop(db);

            let mut reopened = DB::open(&path, Options::default()).unwrap();
            assert_eq!(
                reopened.get(b"gc-live").unwrap(),
                Some(live_value.clone())
            );
            assert_eq!(reopened.get(b"gc-dead-1").unwrap(), None);
            assert_eq!(reopened.get(b"gc-dead-2").unwrap(), None);
            reopened.verify().unwrap();
        }
    }
}

#[cfg(feature = "fault-injection")]
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_shrink_iters: 1_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn vacuum_capacity_refusal_preserves_retryable_candidate(
        live_value in prop::collection::vec(any::<u8>(), 1_025..2_049),
        dead_value in prop::collection::vec(any::<u8>(), 1_025..2_049)
    ) {
        for segmented in [false, true] {
            let directory = tempdir().unwrap();
            let path = directory.path().join("vacuum-capacity-property.db");
            let options = Options {
                blob_storage: if segmented {
                    BlobStorageMode::Segmented
                } else {
                    BlobStorageMode::WholeImage
                },
                ..Options::for_test()
            };
            let mut db = DB::create(&path, options).unwrap();
            db.commit_batch(&[
                BatchMutation::Put {
                    key: b"vacuum-live".to_vec(),
                    value: live_value.clone(),
                },
                BatchMutation::Put {
                    key: b"vacuum-dead".to_vec(),
                    value: dead_value.clone(),
                },
            ])
            .unwrap();
            db.commit_batch(&[BatchMutation::Delete {
                key: b"vacuum-dead".to_vec(),
            }])
            .unwrap();
            let before_commit = db.durability_status().commit_id;
            let before_generation = db.durability_status().generation_id;
            let before_page_writes = db.metrics().unwrap().storage.physical_page_writes;

            db.inject_capacity_limit(0);
            let refusal = db.vacuum();
            assert!(matches!(
                refusal,
                Err(Error::DiskFull | Error::CapacityPreflight)
            ));
            let after_page_writes = db.metrics().unwrap().storage.physical_page_writes;
            assert_eq!(after_page_writes, before_page_writes);
            assert_eq!(db.durability_status().commit_id, before_commit);
            assert_eq!(db.durability_status().generation_id, before_generation);
            assert!(!db.durability_status().write_fenced);
            assert_eq!(db.get(b"vacuum-live").unwrap(), Some(live_value.clone()));
            assert_eq!(db.get(b"vacuum-dead").unwrap(), None);

            db.inject_capacity_limit(u64::MAX);
            let report = db.vacuum().unwrap();
            assert_eq!(report.live_entries, 1);
            assert_eq!(db.get(b"vacuum-live").unwrap(), Some(live_value.clone()));
            assert_eq!(db.get(b"vacuum-dead").unwrap(), None);
            db.verify().unwrap();
            drop(db);

            let mut reopened = DB::open(&path, Options::default()).unwrap();
            assert_eq!(
                reopened.get(b"vacuum-live").unwrap(),
                Some(live_value.clone())
            );
            assert_eq!(reopened.get(b"vacuum-dead").unwrap(), None);
            reopened.verify().unwrap();
        }
    }
}

#[cfg(feature = "fault-injection")]
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_shrink_iters: 1_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn publication_faults_reopen_old_or_complete_new(
        new_value in prop::collection::vec(any::<u8>(), 1_025..2_049)
    ) {
        for segmented in [false, true] {
            for fault in 0u8..14 {
                let directory = tempdir().unwrap();
                let path = directory.path().join("publication-property.db");
                let options = Options {
                    blob_storage: if segmented {
                        BlobStorageMode::Segmented
                    } else {
                        BlobStorageMode::WholeImage
                    },
                    ..Options::for_test()
                };
                let old_value = vec![0xD8; 2_048];
                let retained;
                {
                    let mut db = DB::create(&path, options).unwrap();
                    db.commit_batch(&[BatchMutation::Put {
                        key: b"fault-key".to_vec(),
                        value: old_value.clone(),
                    }])
                    .unwrap();
                    let old_commit = db.durability_status().commit_id;
                    retained = db.retain_commit(old_commit).unwrap();
                    if fault == 8 {
                        // The retained root protects generation 1's page. Two
                        // later publications are therefore needed before the
                        // next generation can reuse an unprotected slot and
                        // reach the manifest-mirror boundary.
                        db.put(b"mirror-seed", b"seed-value").unwrap();
                        db.flush().unwrap();
                        db.put(b"mirror-seed", b"seed-value-2").unwrap();
                        db.flush().unwrap();
                    }
                    db.put(b"fault-key", &new_value).unwrap();
                    match fault {
                        0 => db.inject_sync_failure(),
                        1 => db.inject_write_failure(),
                        2 => db.inject_atomic_rename_failure(),
                        3 => db.inject_after_manifest_failure(),
                        4 => db.inject_page_range_sync_failure(),
                        5 => db.inject_after_write_failure(),
                        6 => db.inject_manifest_sync_failure(),
                        7 => db.inject_publication_directory_sync_failure(),
                        8 => db.inject_manifest_mirror_sync_failure(),
                        9 => db.inject_wal_write_failure(),
                        10 => db.inject_wal_after_write_failure(),
                        11 => db.inject_wal_sync_failure(),
                        12 => db.inject_wal_after_sync_failure(),
                        13 => db.inject_wal_truncate_failure(),
                        _ => unreachable!(),
                    }
                    assert!(db.flush().is_err());
                }

                let mut reopened = DB::open(&path, Options::default()).unwrap();
                let recovered = reopened.get(b"fault-key").unwrap().unwrap();
                assert!(
                    recovered.as_slice() == old_value.as_slice()
                        || recovered.as_slice() == new_value.as_slice(),
                    "recovery exposed a partial value for fault {fault}"
                );
                if matches!(fault, 3 | 13) {
                    assert_eq!(recovered.as_slice(), new_value.as_slice());
                }
                assert_eq!(
                    reopened.get_at(retained, b"fault-key").unwrap(),
                    Some(old_value.clone())
                );
                reopened.verify().unwrap();
                reopened.release_snapshot(retained).unwrap();
                reopened.close().unwrap();

                let mut reopened_again = DB::open(&path, Options::default()).unwrap();
                assert_eq!(
                    reopened_again.get(b"fault-key").unwrap(),
                    Some(recovered)
                );
                reopened_again.verify().unwrap();
            }
        }
    }
}

fn model_range(model: &Model, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    model
        .range(start.to_vec()..end.to_vec())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn assert_matches_model(db: &DB, model: &Model) {
    let start = b"property-key-000";
    let end = b"property-key-999";
    assert_eq!(
        db.range(start, end).unwrap(),
        model_range(model, start, end)
    );
    for (key, value) in model {
        assert_eq!(db.get(key).unwrap(), Some(value.clone()));
    }
}

fn assert_snapshot_matches_model(db: &DB, snapshot_id: seerdb::SnapshotId, model: &Model) {
    let start = b"property-key-000";
    let end = b"property-key-999";
    assert_eq!(
        db.range_at(snapshot_id, start, end).unwrap(),
        model_range(model, start, end)
    );
    for (key, value) in model {
        assert_eq!(db.get_at(snapshot_id, key).unwrap(), Some(value.clone()));
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_shrink_iters: 1_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn batch_transactions_preserve_overlay_and_commit_model(
        transactions in prop::collection::vec(
            (
                prop::collection::vec(
                    (1u8..32, prop::collection::vec(any::<u8>(), 0..256), any::<bool>()),
                    0..7
                ),
                any::<bool>()
            ),
            1..16
        )
    ) {
        for segmented in [false, true] {
            let directory = tempdir().unwrap();
            let path = directory.path().join("transaction-property.db");
            let options = Options {
                blob_storage: if segmented {
                    BlobStorageMode::Segmented
                } else {
                    BlobStorageMode::WholeImage
                },
                ..Options::default()
            };
            let mut db = DB::create(&path, options.clone()).unwrap();
            let blob_key = key(0);
            let blob_value = vec![0xB6; 2_048];
            db.commit_batch(&[BatchMutation::Put {
                key: blob_key.clone(),
                value: blob_value.clone(),
            }])
            .unwrap();
            let mut model = Model::from([(blob_key, blob_value)]);

            for (index, (staged, should_commit)) in transactions.iter().enumerate() {
                let mut transaction = db.begin_batch_transaction().unwrap();
                let mut overlay = model.clone();

                for (key_id, value, is_put) in staged {
                    let mutation_key = key(*key_id);
                    if *is_put {
                        transaction.put(&mutation_key, value).unwrap();
                        overlay.insert(mutation_key.clone(), value.clone());
                    } else {
                        transaction.delete(&mutation_key).unwrap();
                        overlay.remove(&mutation_key);
                    }
                    assert_eq!(
                        transaction.get(&db, &mutation_key).unwrap(),
                        overlay.get(&mutation_key).cloned()
                    );
                    assert_eq!(
                        transaction
                            .range(&db, b"property-key-000", b"property-key-999")
                            .unwrap(),
                        model_range(&overlay, b"property-key-000", b"property-key-999")
                    );
                }

                if *should_commit {
                    transaction.commit(&mut db).unwrap();
                    assert_eq!(transaction.state(), BatchTransactionState::Committed);
                    model = overlay;
                } else {
                    transaction.abort().unwrap();
                    assert_eq!(transaction.state(), BatchTransactionState::Aborted);
                }

                assert_matches_model(&db, &model);
                if index % 4 == 3 {
                    db.verify().unwrap();
                    db.close().unwrap();
                    db = DB::open(&path, options.clone()).unwrap();
                    db.verify().unwrap();
                    assert_matches_model(&db, &model);
                }
            }

            db.close().unwrap();
            let mut reopened = DB::open(&path, options).unwrap();
            assert_matches_model(&reopened, &model);
            reopened.verify().unwrap();
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 12,
        max_shrink_iters: 1_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn durable_operations_match_model_across_reopen(
        operations in prop::collection::vec(
            (0u8..32, prop::collection::vec(any::<u8>(), 0..256), any::<bool>()),
            1..48
        )
    ) {
        for segmented in [false, true] {
            let directory = tempdir().unwrap();
            let path = directory.path().join("property.db");
            let options = Options {
                blob_storage: if segmented {
                    BlobStorageMode::Segmented
                } else {
                    BlobStorageMode::WholeImage
                },
                ..Options::default()
            };
            let mut db = DB::create(&path, options).unwrap();
            let mut model = Model::new();
            let snapshot_boundary = operations.len().div_ceil(2);
            let blob_key = key(0);
            let blob_value = vec![0xA5; 2_048];
            db.commit_batch(&[BatchMutation::Put {
                key: blob_key.clone(),
                value: blob_value.clone(),
            }])
            .unwrap();
            model.insert(blob_key, blob_value);
            assert_matches_model(&db, &model);

            let initial_snapshot = db
                .retain_commit(db.durability_status().commit_id)
                .unwrap();
            let mut retained = vec![(initial_snapshot, model.clone())];

            for (index, (key_id, value, is_put)) in operations.iter().cloned().enumerate() {
                let key = key(key_id);
                if is_put {
                    db.commit_batch(&[BatchMutation::Put {
                        key: key.clone(),
                        value: value.clone(),
                    }])
                    .unwrap();
                    model.insert(key.clone(), value);
                } else {
                    db.commit_batch(&[BatchMutation::Delete { key: key.clone() }])
                        .unwrap();
                    model.remove(&key);
                }

                assert_eq!(db.get(&key).unwrap(), model.get(&key).cloned());
                assert_matches_model(&db, &model);

                if index + 1 == snapshot_boundary {
                    let snapshot_id = db
                        .retain_commit(db.durability_status().commit_id)
                        .unwrap();
                    retained.push((snapshot_id, model.clone()));
                }

                for (snapshot_id, snapshot_model) in &retained {
                    assert_snapshot_matches_model(&db, *snapshot_id, snapshot_model);
                }

                if (index + 1) % 8 == 0 {
                    db.compact_with_limit(2).unwrap();
                    db.verify().unwrap();
                    db.close().unwrap();
                    db = DB::open(&path, Options::default()).unwrap();
                    db.verify().unwrap();
                    assert_matches_model(&db, &model);
                    for (snapshot_id, snapshot_model) in &retained {
                        assert_snapshot_matches_model(&db, *snapshot_id, snapshot_model);
                    }
                }
            }

            for (snapshot_id, _) in retained {
                db.release_snapshot(snapshot_id).unwrap();
            }
            db.compact_with_limit(2).unwrap();
            db.vacuum().unwrap();
            db.prune_history().unwrap();
            db.verify().unwrap();
            db.close().unwrap();
            let mut reopened = DB::open(&path, Options::default()).unwrap();
            reopened.verify().unwrap();
            assert_matches_model(&reopened, &model);
        }
    }
}
