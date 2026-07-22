//! Property checks for the public durable byte-KV contract.

#![allow(clippy::disallowed_methods)]

use proptest::prelude::*;
#[cfg(feature = "fault-injection")]
use seerdb::Error;
use seerdb::{BatchMutation, BlobStorageMode, DB, Options};
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
