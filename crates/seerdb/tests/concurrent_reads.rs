#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, DB, Options};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

fn key(index: usize) -> Vec<u8> {
    format!("concurrent-key-{index:04}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("concurrent-value-{index:04}").into_bytes()
}

#[test]
fn shared_handle_supports_concurrent_point_and_range_reads() {
    let root = tempdir().unwrap();
    let mut db = DB::create(root.path().join("db"), Options::for_test()).unwrap();
    let mutations: Vec<_> = (0..256)
        .map(|index| BatchMutation::Put {
            key: key(index),
            value: value(index),
        })
        .collect();
    db.commit_batch(&mutations).unwrap();
    db.flush().unwrap();

    let db = Arc::new(db);
    let mut handles = Vec::new();
    for worker in 0..8 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for round in 0..128 {
                let index = (worker * 37 + round * 13) % 256;
                assert_eq!(db.get(&key(index)).unwrap(), Some(value(index)));

                if round % 16 == 0 {
                    let start = key(index.min(224));
                    let end = key((index.min(224)) + 32);
                    let rows = db.range(&start, &end).unwrap();
                    assert_eq!(rows.len(), 32);
                    assert_eq!(rows.first().unwrap().0, start);
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
