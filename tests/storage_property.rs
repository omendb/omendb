//! Property checks for the public durable byte-KV contract.

#![allow(clippy::disallowed_methods)]

use proptest::prelude::*;
use seerdb::{BatchMutation, DB, Options};
use std::collections::BTreeMap;
use tempfile::tempdir;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn key(key_id: u8) -> Vec<u8> {
    format!("property-key-{key_id:03}").into_bytes()
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
        let directory = tempdir().unwrap();
        let path = directory.path().join("property.db");
        let mut db = DB::create(&path, Options::default()).unwrap();
        let mut model = Model::new();
        let snapshot_boundary = operations.len().div_ceil(2);
        let mut retained = None;

        for (index, (key_id, value, is_put)) in operations.into_iter().enumerate() {
            let key = key(key_id);
            if is_put {
                db.commit_batch(&[BatchMutation::Put {
                    key: key.clone(),
                    value: value.clone(),
                }]).unwrap();
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
                retained = Some((snapshot_id, model.clone()));
            }

            if let Some((snapshot_id, snapshot_model)) = &retained {
                assert_snapshot_matches_model(&db, *snapshot_id, snapshot_model);
            }

            if (index + 1) % 8 == 0 {
                db.compact_with_limit(2).unwrap();
                db.verify().unwrap();
                db.close().unwrap();
                db = DB::open(&path, Options::default()).unwrap();
                db.verify().unwrap();
                assert_matches_model(&db, &model);
                if let Some((snapshot_id, snapshot_model)) = &retained {
                    assert_snapshot_matches_model(&db, *snapshot_id, snapshot_model);
                }
            }
        }

        let (snapshot_id, _) = retained.take().unwrap();
        db.release_snapshot(snapshot_id).unwrap();
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
