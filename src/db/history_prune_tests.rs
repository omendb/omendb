//! Manifest fallback, history-prune, and page-reuse safety tests.

use super::*;
use std::io::{Seek, SeekFrom, Write};
use tempfile::tempdir;

use crate::storage::format::MANIFEST_SLOT_SIZE;

#[test]
fn test_db_retains_manifest_fallback_before_reusing_pages() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("manifest-retention.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();

    // The next generation can reuse a page from before the current
    // generation, but only after both manifest slots have been fenced to
    // the current root. Fail before the new manifest is published.
    db.put(b"key", b"value-3").unwrap();
    inject_atomic_rename_failure();
    assert!(matches!(db.flush(), Err(Error::Io(_))));
    drop(db);

    // Simulate loss of the newest manifest slot. The mirrored fallback
    // must still name value-2 even though the failed generation reused an
    // older physical page.
    let manifest_path = path.join(MANIFEST_FILE);
    let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
    manifest_file
        .seek(SeekFrom::Start(MANIFEST_SLOT_SIZE as u64))
        .unwrap();
    manifest_file
        .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
        .unwrap();
    manifest_file.sync_all().unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
}

#[test]
fn test_db_prune_history_preserves_inactive_manifest_checkpoint() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("prune-fallback.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();

    let first_checkpoint = path.join("seerdb.meta.1");
    assert!(first_checkpoint.is_file());
    db.prune_history().unwrap();
    assert!(first_checkpoint.is_file());
    db.close().unwrap();

    // The newest slot is corrupt, so reopen must use the independently
    // valid older slot whose checkpoint pruning was required to preserve.
    let manifest_path = path.join(MANIFEST_FILE);
    let manifest_file = OpenOptions::new().read(true).open(&manifest_path).unwrap();
    let mut newest = None;
    for slot in 0..2 {
        let mut bytes = [0; MANIFEST_SLOT_SIZE];
        read_exact_at(
            &manifest_file,
            (slot * MANIFEST_SLOT_SIZE) as u64,
            &mut bytes,
        )
        .unwrap();
        if let Some(manifest) = Manifest::from_bytes(&bytes).unwrap()
            && newest.is_none_or(|(_, current)| manifest.is_newer_than(current))
        {
            newest = Some((slot, manifest));
        }
    }
    let newest_slot = newest.expect("published database has a newest manifest").0;
    drop(manifest_file);
    let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
    manifest_file
        .seek(SeekFrom::Start((newest_slot * MANIFEST_SLOT_SIZE) as u64))
        .unwrap();
    manifest_file
        .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
        .unwrap();
    manifest_file.sync_all().unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
}

#[test]
fn test_db_history_prune_directory_failure_reopens_and_retries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("prune-directory-failure.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-0").unwrap();
    db.flush().unwrap();
    for revision in 1..=MAX_META_DELTA_CHAIN + 1 {
        db.put(b"key", format!("value-{revision}").as_bytes())
            .unwrap();
        db.flush().unwrap();
    }
    db.put(b"key", b"value-final").unwrap();
    db.flush().unwrap();
    let obsolete_checkpoint = path.join("seerdb.meta.1");
    assert!(obsolete_checkpoint.is_file());

    db.inject_history_prune_directory_sync_failure();
    assert!(matches!(db.prune_history(), Err(Error::Io(_))));
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-final".to_vec()));
    reopened.verify().unwrap();
    let report = reopened.prune_history().unwrap();
    assert_eq!(report.removed_checkpoints, 0);
    reopened.close().unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-final".to_vec()));
    assert!(!obsolete_checkpoint.is_file());
}
