#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, BlobStorageMode, DB, Options};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

fn key(index: usize) -> Vec<u8> {
    format!("read-view-key-{index:04}").into_bytes()
}

#[test]
fn view_is_cheap_and_stable_while_writer_advances() {
    let root = tempdir().unwrap();
    let path = root.path().join("db");
    let mut db = DB::create(&path, Options::for_test()).unwrap();
    let old_blob = vec![b'x'; 4096];
    db.commit_batch(&[
        BatchMutation::Put {
            key: key(0),
            value: old_blob.clone(),
        },
        BatchMutation::Put {
            key: key(1),
            value: b"old-inline".to_vec(),
        },
    ])
    .unwrap();
    db.flush().unwrap();

    let before = db.metrics().unwrap();
    let view = Arc::new(db.begin_read_view().unwrap());
    let after = db.metrics().unwrap();
    assert_eq!(after.publication, before.publication);
    assert!(
        !path
            .join("seerdb.blob.retained.18446744073709551615")
            .exists()
    );
    assert_eq!(view.get(&key(0)).unwrap(), Some(old_blob.clone()));

    let mut readers = Vec::new();
    for _ in 0..8 {
        let view = Arc::clone(&view);
        let expected_blob = old_blob.clone();
        readers.push(thread::spawn(move || {
            for _ in 0..64 {
                assert_eq!(view.get(&key(0)).unwrap(), Some(expected_blob.clone()));
                assert_eq!(view.get(&key(1)).unwrap(), Some(b"old-inline".to_vec()));
            }
        }));
    }

    for index in 2..18 {
        db.commit_batch(&[BatchMutation::Put {
            key: key(index),
            value: format!("new-{index}").into_bytes(),
        }])
        .unwrap();
        db.flush().unwrap();
        assert_eq!(view.get(&key(0)).unwrap(), Some(old_blob.clone()));
        assert_eq!(
            db.get(&key(index)).unwrap(),
            Some(format!("new-{index}").into_bytes())
        );
    }

    for reader in readers {
        reader.join().unwrap();
    }
    assert!(db.get(&key(17)).unwrap().is_some());
}

#[test]
fn view_reads_segmented_blob_records_without_a_sidecar() {
    let root = tempdir().unwrap();
    let path = root.path().join("db");
    let options = Options {
        blob_storage: BlobStorageMode::Segmented,
        ..Options::for_test()
    };
    let mut db = DB::create(&path, options.clone()).unwrap();
    let value = vec![b's'; 4096];
    db.commit_batch(&[BatchMutation::Put {
        key: b"segmented-key".to_vec(),
        value: value.clone(),
    }])
    .unwrap();
    db.flush().unwrap();

    let view = db.begin_read_view().unwrap();
    assert_eq!(view.get(b"segmented-key").unwrap(), Some(value));
    drop(view);

    db.commit_batch(&[BatchMutation::Put {
        key: b"next-key".to_vec(),
        value: b"next".to_vec(),
    }])
    .unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"next-key").unwrap(), Some(b"next".to_vec()));
}

#[test]
fn view_protects_old_pages_during_reclamation() {
    let root = tempdir().unwrap();
    let path = root.path().join("db");
    let mut db = DB::create(&path, Options::for_test()).unwrap();
    let old_value = vec![b'o'; 4096];
    db.commit_batch(&[BatchMutation::Put {
        key: b"stable-key".to_vec(),
        value: old_value.clone(),
    }])
    .unwrap();
    db.flush().unwrap();

    let view = db.begin_read_view().unwrap();
    for revision in 0..4 {
        db.commit_batch(&[BatchMutation::Put {
            key: b"stable-key".to_vec(),
            value: format!("new-{revision}").into_bytes(),
        }])
        .unwrap();
        db.flush().unwrap();
    }

    db.compact_with_limit(usize::MAX).unwrap();
    assert_eq!(view.get(b"stable-key").unwrap(), Some(old_value));
    assert_eq!(db.get(b"stable-key").unwrap(), Some(b"new-3".to_vec()));

    drop(view);
    db.compact_with_limit(usize::MAX).unwrap();
    db.verify().unwrap();
}

#[test]
fn view_begin_does_not_flush_unpublished_mutations() {
    let root = tempdir().unwrap();
    let path = root.path().join("db");
    let mut db = DB::create(&path, Options::for_test()).unwrap();
    db.put(b"pending-key", b"pending-value").unwrap();
    let before = db.metrics().unwrap();

    let view = db.begin_read_view().unwrap();
    let after = db.metrics().unwrap();
    assert_eq!(after.publication, before.publication);
    assert_eq!(view.commit_id().get(), 0);
    assert_eq!(view.get(b"pending-key").unwrap(), None);
    drop(view);

    db.flush().unwrap();
    assert_eq!(
        db.get(b"pending-key").unwrap(),
        Some(b"pending-value".to_vec())
    );
}
