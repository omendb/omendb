#![cfg(feature = "fault-injection")]
#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, DB, Options};
use tempfile::tempdir;

#[test]
fn pre_publication_recovery_preserves_old_state_and_accepts_next_commit() {
    let root = tempdir().unwrap();
    let path = root.path().join("pre-publication.db");

    {
        let mut db = DB::open(&path, Options::for_test()).unwrap();
        db.commit_batch(&[BatchMutation::Put {
            key: b"key".to_vec(),
            value: b"value-1".to_vec(),
        }])
        .unwrap();

        db.put(b"key", b"value-2").unwrap();
        db.inject_page_range_sync_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
    }

    let recovered_commit = {
        let mut db = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value-1".to_vec()));
        assert_eq!(db.durability_status().pending_mutations, 0);
        assert!(!db.durability_status().write_fenced);
        db.verify().unwrap();

        let recovered_commit = db.durability_status().commit_id;
        let next = db
            .commit_batch(&[BatchMutation::Put {
                key: b"post-recovery".to_vec(),
                value: b"committed-after-recovery".to_vec(),
            }])
            .unwrap();
        assert!(next.commit_id > recovered_commit);
        assert_eq!(
            db.get(b"post-recovery").unwrap(),
            Some(b"committed-after-recovery".to_vec())
        );
        next.commit_id
    };

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
    assert_eq!(
        reopened.get(b"post-recovery").unwrap(),
        Some(b"committed-after-recovery".to_vec())
    );
    assert_eq!(reopened.durability_status().commit_id, recovered_commit);
    reopened.verify().unwrap();
}

#[test]
fn post_authority_recovery_preserves_new_state_and_accepts_next_commit() {
    let root = tempdir().unwrap();
    let path = root.path().join("post-authority.db");

    {
        let mut db = DB::open(&path, Options::for_test()).unwrap();
        db.commit_batch(&[BatchMutation::Put {
            key: b"key".to_vec(),
            value: b"value-1".to_vec(),
        }])
        .unwrap();

        db.put(b"key", b"value-2").unwrap();
        db.inject_after_manifest_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
    }

    let recovered_commit = {
        let mut db = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        assert_eq!(db.durability_status().pending_mutations, 0);
        assert!(!db.durability_status().write_fenced);
        db.verify().unwrap();

        let recovered_commit = db.durability_status().commit_id;
        let next = db
            .commit_batch(&[BatchMutation::Put {
                key: b"post-recovery".to_vec(),
                value: b"committed-after-recovery".to_vec(),
            }])
            .unwrap();
        assert!(next.commit_id > recovered_commit);
        assert_eq!(
            db.get(b"post-recovery").unwrap(),
            Some(b"committed-after-recovery".to_vec())
        );
        next.commit_id
    };

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    assert_eq!(
        reopened.get(b"post-recovery").unwrap(),
        Some(b"committed-after-recovery".to_vec())
    );
    assert_eq!(reopened.durability_status().commit_id, recovered_commit);
    reopened.verify().unwrap();
}
