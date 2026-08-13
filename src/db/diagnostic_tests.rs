//! DB::check, verification classification, and offline-diagnostic tests.

use super::*;
use tempfile::tempdir;

#[test]
fn test_db_check_is_non_mutating_and_does_not_take_writer_lock() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("check.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"pending", b"value").unwrap();

    let pending = DB::check(&path, Options::default()).unwrap();
    assert_eq!(pending.wal_status, WalCheckStatus::Pending);
    assert_eq!(
        pending.verification.wal_bytes,
        fs::metadata(path.join(WAL_FILE)).unwrap().len()
    );
    assert_eq!(db.get(b"pending").unwrap(), Some(b"value".to_vec()));

    db.flush().unwrap();
    let clean = DB::check(&path, Options::default()).unwrap();
    assert_eq!(clean.wal_status, WalCheckStatus::Clean);
    assert_eq!(clean.verification.wal_bytes, 0);
    db.close().unwrap();
}

#[test]
fn test_db_check_does_not_create_missing_path() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("missing.db");

    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check { kind: CheckFailureKind::Target, message })
            if message.contains("does not exist")
    ));
    assert!(!path.exists());
}

#[test]
fn test_check_classifies_runtime_storage_state_failures() {
    let error = DB::map_check_error(
        CheckFailureKind::Runtime,
        Error::Corruption("buffer ownership invariant failed".into()),
    );
    assert!(matches!(
        error,
        Error::Check {
            kind: CheckFailureKind::Runtime,
            message
        } if message.contains("buffer ownership invariant")
    ));
}

#[test]
fn test_check_classifies_nested_owner_failures() {
    let cases = [
        (
            Error::Wal("invalid recovery frontier".into()),
            CheckFailureKind::Wal,
        ),
        (
            Error::Buffer("pinned frame".into()),
            CheckFailureKind::Runtime,
        ),
        (
            Error::BTree("malformed routing".into()),
            CheckFailureKind::Structure,
        ),
        (
            Error::SnapshotUnavailable("retained checkpoint is unavailable".into()),
            CheckFailureKind::Checkpoint,
        ),
    ];

    for (error, expected_kind) in cases {
        assert!(matches!(
            DB::map_check_error(CheckFailureKind::Format, error),
            Error::Check { kind, .. } if kind == expected_kind
        ));
    }
}

#[test]
fn test_check_classifies_unavailable_retained_checkpoint() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("unavailable-retained-checkpoint.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"retained", b"value").unwrap();
    db.flush().unwrap();
    db.retain_commit(db.durability_status().commit_id).unwrap();
    db.close().unwrap();

    fs::OpenOptions::new()
        .write(true)
        .open(path.join(DATA_FILE))
        .unwrap()
        .set_len(0)
        .unwrap();

    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Checkpoint,
            message
        }) if message.contains("beyond the data file")
    ));
}
