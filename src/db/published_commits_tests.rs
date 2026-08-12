//! Direct contract tests for the published logical commit catalog.

use super::*;
use tempfile::tempdir;

#[test]
fn published_commits_deduplicates_maintenance_generations_and_reopens() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("published-commits.db");
    let expected_initial = vec![CommitId::new(0)];
    let expected_commits = vec![CommitId::new(0), CommitId::new(1)];

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        assert_eq!(db.published_commits().unwrap(), expected_initial);

        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
        let committed = db.durability_status();
        assert_eq!(db.published_commits().unwrap(), expected_commits);

        let maintenance = db.vacuum().unwrap();
        assert_eq!(maintenance.durability.commit_id, committed.commit_id);
        assert!(maintenance.durability.generation_id.get() > committed.generation_id.get());
        assert_eq!(db.published_commits().unwrap(), expected_commits);
    }

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.published_commits().unwrap(), expected_commits);
}

#[test]
fn published_commits_refuses_after_unprotected_history_prune() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("published-commits-pruned.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    for value in [b"one".as_slice(), b"two", b"three"] {
        db.put(b"key", value).unwrap();
        db.flush().unwrap();
    }
    assert_eq!(
        db.published_commits().unwrap(),
        vec![
            CommitId::new(0),
            CommitId::new(1),
            CommitId::new(2),
            CommitId::new(3)
        ]
    );

    db.prune_history().unwrap();
    assert!(matches!(
        db.published_commits(),
        Err(Error::SnapshotUnavailable(message))
            if message == "complete commit history is no longer retained"
    ));
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert!(matches!(
        reopened.published_commits(),
        Err(Error::SnapshotUnavailable(message))
            if message == "complete commit history is no longer retained"
    ));
}
