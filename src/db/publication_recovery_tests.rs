//! Publication, WAL-prefix, and recovery-fence tests.

use super::*;
use std::collections::BTreeMap;
use std::io::{Seek, SeekFrom, Write};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_db_process_crash_recovery() {
    if let Some(path) = std::env::var_os("SEERDB_CRASH_CHILD_PATH") {
        let path = PathBuf::from(path);
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"published", b"value-before-crash").unwrap();
        db.flush().unwrap();
        db.put(b"unpublished", b"value-after-wal-only").unwrap();

        // Exit without running Rust destructors. This leaves the WAL
        // mutation on disk while the manifest still names the prior
        // published generation, matching an abrupt process termination.
        std::process::exit(137);
    }

    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("db::publication_recovery_tests::test_db_process_crash_recovery")
        .arg("--nocapture")
        .env("SEERDB_CRASH_CHILD_PATH", &path)
        .status()
        .unwrap();
    assert!(!status.success(), "crash child unexpectedly exited cleanly");

    let db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        db.get(b"published").unwrap(),
        Some(b"value-before-crash".to_vec())
    );
    assert_eq!(db.get(b"unpublished").unwrap(), None);
    // Retained WAL: published records stay until clean close.
    assert!(path.join(WAL_FILE).exists());
}

#[test]
fn test_db_randomized_publication_fault_matrix() {
    fn next(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *seed
    }

    fn assert_model(db: &DB, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
        for key_id in 0..16 {
            let key = format!("key-{key_id:02}");
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                model.get(key.as_bytes()).cloned()
            );
        }
    }

    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    let mut committed = BTreeMap::new();
    let mut seed = 0x5EED_CAFE_u64;

    for round in 0..32 {
        let mut candidate = committed.clone();
        let operation_count = (next(&mut seed) % 4 + 1) as usize;
        for operation in 0..operation_count {
            let key_id = next(&mut seed) % 16;
            let key = format!("key-{key_id:02}");
            let value = format!("value-{round:02}-{operation:02}-{key_id:02}");
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            candidate.insert(key.into_bytes(), value.into_bytes());
        }

        let fault = next(&mut seed) % 4;
        match fault {
            1 => db.engine.inject_sync_failure(),
            2 => db.engine.inject_write_failure(),
            3 => inject_atomic_rename_failure(),
            _ => {}
        }

        let result = db.flush();
        if fault == 0 {
            result.unwrap();
            committed = candidate;
            assert_model(&db, &committed);
        } else {
            assert!(result.is_err(), "fault {fault} did not fail publication");
            drop(db);
            db = DB::open(&path, Options::default()).unwrap();
            assert_model(&db, &committed);
        }
    }

    db.close().unwrap();
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_model(&reopened, &committed);
}

#[test]
fn test_db_recovers_committed_wal_prefix_with_torn_suffix() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let records = vec![
        WalRecord::put(b"key1", b"value1"),
        WalRecord::put(b"key2", b"value2"),
        WalRecord::put(b"key3", b"value3"),
    ];
    let references: Vec<_> = records.iter().collect();
    let commit = CommitRecord {
        commit_id: CommitId::new(1),
        generation_id: GenerationId::new(1),
        root_page_id: 0,
        mutation_count: records.len() as u64,
        digest: digest_records(&references),
    };
    let mut wal_bytes = Vec::new();
    for record in &records {
        wal_bytes.extend_from_slice(&record.to_bytes());
    }
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    wal_bytes.extend_from_slice(&[0xA5, 0x5A, 0x01]);
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

    let db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));
    // Retained WAL: published records stay until clean close.
    assert!(path.join(WAL_FILE).exists());
    assert!(DB::metadata_log_path(&path).is_file());
}

#[test]
fn test_db_reopen_accepts_every_wal_truncation_prefix() {
    let records = vec![
        WalRecord::put(b"key1", b"value1"),
        WalRecord::put(b"key2", b"value2"),
        WalRecord::put(b"key3", b"value3"),
    ];
    let references: Vec<_> = records.iter().collect();
    let commit = CommitRecord {
        commit_id: CommitId::new(1),
        generation_id: GenerationId::new(1),
        root_page_id: 0,
        mutation_count: records.len() as u64,
        digest: digest_records(&references),
    };
    let mut committed_wal = Vec::new();
    for record in &records {
        committed_wal.extend_from_slice(&record.to_bytes());
    }
    committed_wal.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    let committed_len = committed_wal.len();
    committed_wal.extend_from_slice(&[0xA5, 0x5A, 0x01]);

    for cut in 0..=committed_wal.len() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(WAL_FILE), &committed_wal[..cut]).unwrap();

        let db = DB::open(&path, Options::default())
            .unwrap_or_else(|error| panic!("WAL prefix at byte {cut} failed to reopen: {error:?}"));
        let committed = cut >= committed_len;
        assert_eq!(
            db.get(b"key1").unwrap(),
            committed.then(|| b"value1".to_vec()),
            "cut={cut}"
        );
        assert_eq!(
            db.get(b"key2").unwrap(),
            committed.then(|| b"value2".to_vec()),
            "cut={cut}"
        );
        assert_eq!(
            db.get(b"key3").unwrap(),
            committed.then(|| b"value3".to_vec()),
            "cut={cut}"
        );
    }
}

#[test]
fn test_db_rejects_wal_commit_digest_mismatch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let record = WalRecord::put(b"key", b"value");
    let references = vec![&record];
    let commit = CommitRecord {
        commit_id: CommitId::new(1),
        generation_id: GenerationId::new(1),
        root_page_id: 0,
        mutation_count: 1,
        digest: digest_records(&references) ^ 1,
    };
    let mut wal_bytes = record.to_bytes();
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

    let result = DB::open(&path, Options::default());
    assert!(matches!(
        result,
        Err(Error::Corruption(message)) if message.contains("WAL commit")
    ));
}

#[test]
fn test_db_rejects_when_both_manifest_slots_are_corrupt() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let metadata_log = DB::metadata_log_path(&path);
    let mut file = OpenOptions::new().write(true).open(&metadata_log).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[0xA5; 16]).unwrap();
    file.sync_all().unwrap();

    let result = DB::open(&path, Options::default());
    assert!(matches!(result, Err(Error::Corruption(_))));
}

#[test]
fn test_db_fences_writer_after_sync_failure() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.engine.inject_sync_failure();

    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert!(matches!(
        db.put(b"another", b"value"),
        Err(Error::NeedsRecovery(_))
    ));
    assert!(matches!(
        db.get(b"key"),
        Err(Error::NeedsRecovery(message)) if message.contains("reads fenced")
    ));
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
}

#[test]
fn test_db_fences_writer_after_page_write_failure() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.engine.inject_write_failure();

    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert!(matches!(
        db.put(b"another", b"value"),
        Err(Error::NeedsRecovery(_))
    ));
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
}

#[test]
fn test_db_fences_writer_after_disk_full() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.engine.inject_disk_full();

    assert!(matches!(db.flush(), Err(Error::DiskFull)));
    assert!(matches!(
        db.put(b"another", b"value"),
        Err(Error::NeedsRecovery(_))
    ));
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
}

#[test]
fn test_db_capacity_preflight_is_retryable_without_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("retryable-capacity.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();

    let capacity = fs::metadata(path.join(DATA_FILE)).unwrap().len();
    db.inject_capacity_limit(capacity);
    db.put(b"key", b"value-2").unwrap();

    assert!(matches!(db.flush(), Err(Error::CapacityPreflight)));
    assert!(!db.durability_status().write_fenced);
    assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));

    db.inject_capacity_limit(u64::MAX);
    db.flush().unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));

    drop(db);
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
}

#[test]
fn test_db_discards_wal_after_publication_sync_failure() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    // Data-phase sync failure: no commit envelope reaches the WAL, so
    // recovery must discard the whole uncommitted suffix.
    db.engine.inject_sync_failure();

    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert!(matches!(
        db.put(b"another", b"value"),
        Err(Error::NeedsRecovery(_))
    ));
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
    // Retained WAL: published records stay until clean close.
    assert!(path.join(WAL_FILE).exists());
}

#[test]
fn test_db_default_publication_does_not_force_wal_commit_sync() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("no-wal-commit-barrier.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"one").unwrap();
    db.flush().unwrap();

    // The authority frame is the default CoW acknowledgement point. A fault
    // at the retired standalone WAL-commit barrier must not reject a valid
    // publication or fence the writer.
    db.put(b"key", b"two").unwrap();
    db.inject_wal_sync_failure();
    db.flush().unwrap();
    faults::FAIL_NEXT_WAL_SYNC.with(|failure| failure.set(false));
    assert_eq!(db.get(b"key").unwrap(), Some(b"two".to_vec()));
    assert!(!db.durability_status().write_fenced);
    db.close().unwrap();

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"two".to_vec()));
}
