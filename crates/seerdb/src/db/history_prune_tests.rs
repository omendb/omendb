//! Manifest fallback, history-prune, and page-reuse safety tests.

use super::*;
use std::io::Write;
use tempfile::tempdir;

use crate::db::metadata_codec::META_LOG_HEADER_SIZE;

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
    // generation. Fail at the reuse-fencing boundary, before any publication
    // artifact is written, so generation 3 never becomes durable.
    db.put(b"key", b"value-3").unwrap();
    db.inject_manifest_mirror_sync_failure();
    assert!(matches!(db.flush(), Err(Error::Io(_))));
    drop(db);

    // Simulate loss of the newest authority frame: the failed generation
    // never appended a frame, so the log ends at value-2's generation and a
    // torn tail must not change that.
    let metadata_log = DB::metadata_log_path(&path);
    let mut log = OpenOptions::new().append(true).open(&metadata_log).unwrap();
    log.write_all(&[0xA5; 64]).unwrap();
    log.sync_all().unwrap();

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

    let metadata_log = DB::metadata_log_path(&path);
    assert!(metadata_log.is_file());
    db.prune_history().unwrap();
    let parsed = DB::read_meta_log(&path).unwrap().unwrap();
    assert!(parsed.frames.iter().any(|frame| frame.checkpoint_id == 1));
    db.close().unwrap();

    // Truncate the log to just before its newest frame, simulating loss of
    // the newest authority. Reopen must fall back to the previous frame,
    // whose checkpoint chain pruning was required to preserve.
    let parsed = DB::read_meta_log(&path).unwrap().unwrap();
    let keep_len = META_LOG_HEADER_SIZE
        + parsed.frames[..parsed.frames.len() - 1]
            .iter()
            .map(|frame| frame.raw.len())
            .sum::<usize>();
    let file = OpenOptions::new().write(true).open(&metadata_log).unwrap();
    file.set_len(keep_len as u64).unwrap();
    file.sync_all().unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
}

#[test]
fn test_db_history_prune_compaction_failure_reopens_and_retries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("prune-compaction-failure.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-0").unwrap();
    db.flush().unwrap();
    for revision in 1..=MAX_META_DELTA_CHAIN as i32 + 2 {
        db.put(b"key", format!("value-{revision}").as_bytes())
            .unwrap();
        db.flush().unwrap();
    }
    db.put(b"key", b"value-final").unwrap();
    db.flush().unwrap();
    let metadata_log = DB::metadata_log_path(&path);
    let log_len_before = fs::metadata(&metadata_log).unwrap().len();
    assert!(log_len_before > 0);

    db.inject_atomic_rename_failure();
    assert!(matches!(db.prune_history(), Err(Error::Io(_))));
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-final".to_vec()));
    reopened.verify().unwrap();
    let report = reopened.prune_history().unwrap();
    assert!(report.removed_checkpoints > 0);
    reopened.close().unwrap();

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-final".to_vec()));
    let log_len_after = fs::metadata(&metadata_log).unwrap().len();
    assert!(log_len_after < log_len_before);
    reopened.verify().unwrap();
    reopened.close().unwrap();
}
