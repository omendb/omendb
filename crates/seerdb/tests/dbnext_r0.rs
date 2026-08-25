#![cfg(feature = "fault-injection")]
#![allow(clippy::disallowed_methods)]

use seerdb::blob::BlobManager;
use seerdb::recovery::WalRecord;
use seerdb::storage::format::{
    CommitId, CommitRecord, CommitSeq, FORMAT_VERSION, GenerationId, Lsn, MANIFEST_SLOT_SIZE,
    Manifest,
};
use seerdb::{
    BatchMutation, BlobStorageMode, CheckFailureKind, DB, Error, Options, RepairAction,
    WalCheckStatus,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

fn next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

fn assert_model(db: &DB, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    for key_id in 0..32 {
        let key = format!("key-{key_id:02}");
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            model.get(key.as_bytes()).cloned(),
            "mismatch for {key}"
        );
    }

    let expected_range: Vec<_> = model
        .iter()
        .filter(|(key, _)| key.as_slice() >= b"key-00" && key.as_slice() < b"key-99")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(db.range(b"key-00", b"key-99").unwrap(), expected_range);
}

/// Newest publication-frame manifest in the database's authority log.
fn active_manifest(path: &Path) -> Manifest {
    const LOG_HEADER_SIZE: usize = 12;
    const FRAME_HEADER_SIZE: usize = 16;
    const PUB_HEADER_SIZE: usize = 16;
    let bytes = fs::read(path.join("seerdb.meta.log")).unwrap();
    let mut newest = None;
    let mut cursor = LOG_HEADER_SIZE;
    while cursor + FRAME_HEADER_SIZE <= bytes.len() {
        let payload_len =
            u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        let payload_end = cursor + FRAME_HEADER_SIZE + payload_len;
        if payload_end > bytes.len() {
            break;
        }
        let payload = &bytes[cursor + FRAME_HEADER_SIZE..payload_end];
        // Publication payload: magic(8) version(4) manifest_len(4) slot(..).
        if payload.len() >= PUB_HEADER_SIZE + MANIFEST_SLOT_SIZE && payload[..8] == *b"SEERMPB1" {
            let manifest_len = u32::from_le_bytes(payload[12..16].try_into().unwrap()) as usize;
            if manifest_len == MANIFEST_SLOT_SIZE {
                let mut slot = [0u8; MANIFEST_SLOT_SIZE];
                slot.copy_from_slice(
                    &payload[PUB_HEADER_SIZE..PUB_HEADER_SIZE + MANIFEST_SLOT_SIZE],
                );
                if let Ok(Some(manifest)) = Manifest::from_bytes(&slot)
                    && newest.is_none_or(|current| manifest.is_newer_than(current))
                {
                    newest = Some(manifest);
                }
            }
        }
        cursor = payload_end;
    }
    newest.expect("database has an active manifest")
}

/// Overwrite the manifest slot image inside every publication frame with a
/// slot whose format version is one past supported, repairing each frame
/// checksum so rejection happens at manifest validation, not at the frame.
fn bump_manifest_format_version_in_every_frame(log: &mut [u8]) {
    const LOG_HEADER_SIZE: usize = 12;
    const FRAME_HEADER_SIZE: usize = 16;
    const VERSION_OFFSET_IN_SLOT: usize = 8;
    let mut cursor = LOG_HEADER_SIZE;
    while cursor + FRAME_HEADER_SIZE <= log.len() {
        let payload_len = u32::from_le_bytes(log[cursor..cursor + 4].try_into().unwrap()) as usize;
        let payload_end = cursor + FRAME_HEADER_SIZE + payload_len;
        if payload_end > log.len() {
            break;
        }
        let payload_start = cursor + FRAME_HEADER_SIZE;
        let payload = &mut log[payload_start..payload_end];
        if payload.len() >= 16 + MANIFEST_SLOT_SIZE && payload[..8] == *b"SEERMPB1" {
            let slot_start = 16 + VERSION_OFFSET_IN_SLOT;
            payload[slot_start..slot_start + 4]
                .copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
            let checksum = crc32c::crc32c(&log[cursor + 8..payload_end]);
            log[cursor + 4..cursor + 8].copy_from_slice(&checksum.to_le_bytes());
        }
        cursor = payload_end;
    }
}

fn wal_digest(record: &WalRecord) -> u32 {
    let bytes = record.to_bytes();
    let mut input = Vec::with_capacity(4 + bytes.len());
    input.extend_from_slice(&0u32.to_le_bytes());
    input.extend_from_slice(&bytes);
    crc32c::crc32c(&input)
}

#[path = "dbnext_r0/batch_publication.rs"]
mod batch_publication;
#[path = "dbnext_r0/blob_reclamation.rs"]
mod blob_reclamation;
#[path = "dbnext_r0/compaction_reclamation.rs"]
mod compaction_reclamation;
#[path = "dbnext_r0/diagnostics_repair.rs"]
mod diagnostics_repair;
#[path = "dbnext_r0/history_maintenance.rs"]
mod history_maintenance;
#[path = "dbnext_r0/snapshot_retention.rs"]
mod snapshot_retention;
#[path = "dbnext_r0/transactions.rs"]
mod transactions;

#[test]
fn dbnext_r0_seeded_mutations_faults_and_restore() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("source.db");
    let mut db = DB::open(&source_path, Options::default()).unwrap();
    let initial_status = db.durability_status();
    let mut committed = BTreeMap::new();
    let mut seed = 0xDB00_0001_u64;

    for round in 0..40 {
        let mut candidate = committed.clone();
        let operation_count = (next(&mut seed) % 4 + 1) as usize;
        for operation in 0..operation_count {
            let key_id = next(&mut seed) % 32;
            let key = format!("key-{key_id:02}");
            if next(&mut seed).is_multiple_of(5) {
                let expected = candidate.remove(key.as_bytes()).is_some();
                assert_eq!(db.delete(key.as_bytes()).unwrap(), expected);
            } else {
                let value = format!("value-{round:02}-{operation:02}-{key_id:02}");
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                candidate.insert(key.into_bytes(), value.into_bytes());
            }
        }

        let fault = next(&mut seed) % 4;
        match fault {
            1 => db.inject_sync_failure(),
            2 => db.inject_write_failure(),
            3 => db.inject_disk_full(),
            _ => {}
        }

        let result = db.flush();
        if fault == 0 {
            result.unwrap();
            committed = candidate;
            assert_model(&db, &committed);
        } else {
            assert!(result.is_err(), "fault {fault} did not fail publication");
            assert!(db.durability_status().write_fenced);
            drop(db);
            db = DB::open(&source_path, Options::default()).unwrap();
            assert_model(&db, &committed);
        }

        if round % 7 == 0 {
            let status = db.durability_status();
            drop(db);
            db = DB::open(&source_path, Options::default()).unwrap();
            assert_eq!(db.durability_status().database_id, status.database_id);
            assert_eq!(db.durability_status().history_id, status.history_id);
            assert_eq!(db.durability_status().commit_id, status.commit_id);
            assert_eq!(db.durability_status().generation_id, status.generation_id);
            assert_model(&db, &committed);
        }
    }

    let final_status = db.durability_status();
    let archive_path = root.path().join("archive.db");
    db.snapshot(&archive_path).unwrap();
    db.close().unwrap();

    let restored_path = root.path().join("restored.db");
    let restore_report = DB::restore(&archive_path, &restored_path, Options::default()).unwrap();
    let mut restored = DB::open(&restored_path, Options::default()).unwrap();
    assert_model(&restored, &committed);
    assert_eq!(
        restored.durability_status().database_id,
        initial_status.database_id
    );
    assert_ne!(
        restored.durability_status().history_id,
        initial_status.history_id
    );
    assert_eq!(
        restore_report.destination.history_id,
        restored.durability_status().history_id
    );
    assert_eq!(
        restored.durability_status().commit_id,
        final_status.commit_id
    );
    // The fork publishes a fresh physical generation past the archive root
    // (reserved IDs can advance it further), so only ordering is guaranteed.
    assert!(
        restored.durability_status().generation_id > final_status.generation_id,
        "restored generation {:?} must advance past the archive root {:?}",
        restored.durability_status().generation_id,
        final_status.generation_id
    );
    assert_eq!(restored.durability_status().pending_mutations, 0);
    assert!(!restored.durability_status().write_fenced);
    restored.put(b"fork-only", b"child-history").unwrap();
    restored.flush().unwrap();
    assert_eq!(
        restored.get(b"fork-only").unwrap(),
        Some(b"child-history".to_vec())
    );
    assert_eq!(
        restored.durability_status().history_id,
        restore_report.destination.history_id
    );
}

#[test]
fn dbnext_r0_checkpoint_is_verified_and_idempotent_when_clean() {
    let root = tempdir().unwrap();
    let path = root.path().join("checkpoint-barrier.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"checkpointed", b"value").unwrap();

    let first = db.checkpoint().unwrap();
    assert_eq!(first.durability.commit_id.get(), 1);
    // Retained WAL: the checkpoint report reflects the retained log.
    assert_eq!(
        first.wal_bytes,
        fs::metadata(path.join("seerdb.wal")).unwrap().len()
    );
    let second = db.checkpoint().unwrap();
    assert_eq!(second, first);
    assert_eq!(db.get(b"checkpointed").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn dbnext_r0_rejects_corrupt_manifest() {
    let root = tempdir().unwrap();
    let path = root.path().join("corrupt.db");
    let repair_path = root.path().join("corrupt-manifest-repair.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let metadata_log = path.join("seerdb.meta.log");
    // Clobber the whole authority log: header and frames are unreadable, so
    // no generation can be selected and the database must fail closed.
    fs::write(&metadata_log, vec![0xA5u8; 256]).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(_))
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Manifest,
            ..
        })
    ));
    assert!(matches!(
        DB::repair(&path, &repair_path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Manifest,
            ..
        })
    ));
    assert!(!repair_path.exists());
}

#[test]
fn dbnext_r0_reconciles_corrupt_wal_suffix_after_authority() {
    let root = tempdir().unwrap();
    let path = root.path().join("corrupt-wal.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"stable", b"value").unwrap();
        db.flush().unwrap();
    }

    let mut wal = WalRecord::put(b"corrupt", b"suffix").to_bytes();
    let last = wal.len() - 1;
    wal[last] ^= 0xFF;
    fs::write(path.join("seerdb.wal"), wal).unwrap();

    let check = DB::check(&path, Options::default()).unwrap();
    assert_eq!(check.wal_status, WalCheckStatus::Incomplete);
    let opened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(opened.get(b"stable").unwrap(), Some(b"value".to_vec()));
    drop(opened);
    let check = DB::check(&path, Options::default()).unwrap();
    assert_eq!(check.wal_status, WalCheckStatus::Clean);
}

#[test]
fn dbnext_r0_capacity_limit_preserves_last_generation() {
    let root = tempdir().unwrap();
    let path = root.path().join("capacity.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"committed", b"value-1").unwrap();
    db.flush().unwrap();
    let committed_status = db.durability_status();
    let capacity = fs::metadata(path.join("seerdb.data")).unwrap().len();

    db.inject_capacity_limit(capacity);
    db.put(b"uncommitted", b"value-2").unwrap();
    assert!(matches!(db.flush(), Err(Error::CapacityPreflight)));
    assert!(!db.durability_status().write_fenced);
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get(b"committed").unwrap(),
        Some(b"value-1".to_vec())
    );
    assert_eq!(reopened.get(b"uncommitted").unwrap(), None);
    assert_eq!(
        reopened.durability_status().database_id,
        committed_status.database_id
    );
    assert_eq!(
        reopened.durability_status().history_id,
        committed_status.history_id
    );
    assert_eq!(
        reopened.durability_status().generation_id,
        committed_status.generation_id
    );
    assert_eq!(
        reopened.durability_status().commit_id,
        committed_status.commit_id
    );
}

#[test]
fn dbnext_r0_wal_mutation_faults_fence_and_recover_prior_state() {
    fn run_case<F>(root: &Path, name: &str, options: Options, inject: F)
    where
        F: FnOnce(&DB),
    {
        let path = root.join(name);
        let mut db = DB::open(&path, options).unwrap();
        db.put(b"stable", b"before-wal-fault").unwrap();
        db.flush().unwrap();

        inject(&db);
        assert!(matches!(
            db.put(b"pending", b"must-not-commit"),
            Err(Error::Io(_))
        ));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(
            reopened.get(b"stable").unwrap(),
            Some(b"before-wal-fault".to_vec())
        );
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        assert!(!reopened.durability_status().write_fenced);
        // Retained WAL: inert records stay until clean close.
        assert!(path.join("seerdb.wal").exists());
    }

    let root = tempdir().unwrap();
    run_case(
        root.path(),
        "wal-before-append.db",
        Options::default(),
        DB::inject_wal_write_failure,
    );
    run_case(
        root.path(),
        "wal-after-append.db",
        Options::default(),
        DB::inject_wal_after_write_failure,
    );
    run_case(
        root.path(),
        "wal-sync.db",
        Options {
            sync_writes: true,
            ..Options::default()
        },
        DB::inject_wal_sync_failure,
    );
    run_case(
        root.path(),
        "wal-after-sync.db",
        Options {
            sync_writes: true,
            ..Options::default()
        },
        DB::inject_wal_after_sync_failure,
    );
}

#[test]
fn dbnext_r0_rejects_future_manifest_version() {
    let root = tempdir().unwrap();
    let path = root.path().join("future-format.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let metadata_log = path.join("seerdb.meta.log");
    let mut log = fs::read(&metadata_log).unwrap();
    bump_manifest_format_version_in_every_frame(&mut log);
    fs::write(&metadata_log, &log).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("manifest")
    ));
}

#[test]
fn dbnext_r0_concurrent_process_crash_recovery() {
    if let Some(path) = std::env::var_os("SEERDB_R0_CONCURRENT_CRASH_PATH") {
        let path = Path::new(&path);
        let marker = PathBuf::from(
            std::env::var_os("SEERDB_R0_CONCURRENT_CRASH_MARKER")
                .expect("concurrent crash child marker path"),
        );
        let mut db = DB::open(path, Options::default()).unwrap();
        db.put(b"published", b"before-concurrent-crash").unwrap();
        db.flush().unwrap();

        let db = Arc::new(Mutex::new(db));
        let started = Arc::new(AtomicBool::new(false));
        for worker in 0..4 {
            let db = Arc::clone(&db);
            let started = Arc::clone(&started);
            thread::spawn(move || {
                for sequence in 0..256 {
                    let key = format!("worker-{worker:02}-{sequence:04}");
                    let mut db = db.lock().unwrap();
                    if db.put(key.as_bytes(), key.as_bytes()).is_err() {
                        return;
                    }
                    if sequence % 32 == 31 && db.flush().is_ok() {
                        started.store(true, Ordering::Release);
                    }
                }
            });
        }

        while !started.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(1));
        }
        fs::write(marker, b"durable concurrent batch ready").unwrap();
        loop {
            thread::sleep(Duration::from_secs(1));
        }
    }

    let root = tempdir().unwrap();
    let path = root.path().join("concurrent-crash.db");
    let marker = root.path().join("concurrent-crash.ready");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("dbnext_r0_concurrent_process_crash_recovery")
        .arg("--nocapture")
        .env("SEERDB_R0_CONCURRENT_CRASH_PATH", &path)
        .env("SEERDB_R0_CONCURRENT_CRASH_MARKER", &marker)
        .spawn()
        .unwrap();
    let mut ready = false;
    // The child competes with the full integration suite for CPU and disk;
    // readiness is still bounded, but five seconds is too sensitive to
    // ordinary test-runner contention.
    for _ in 0..3000 {
        if marker.exists() {
            ready = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        panic!("concurrent crash child did not publish its ready marker");
    }
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success(), "crash child unexpectedly exited cleanly");

    let recovered = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        recovered.get(b"published").unwrap(),
        Some(b"before-concurrent-crash".to_vec())
    );
    let worker_records = recovered.range(b"worker-00", b"worker-~").unwrap();
    assert!(
        !worker_records.is_empty(),
        "no concurrent batch was recovered"
    );
    for (key, value) in worker_records {
        assert_eq!(key, value, "recovered value is not self-identifying");
    }
    assert!(recovered.durability_status().generation_id.get() >= 1);
    assert_eq!(recovered.durability_status().pending_mutations, 0);
    assert!(!recovered.durability_status().write_fenced);
}

#[test]
fn dbnext_r0_process_crash_discards_uncommitted_batch() {
    if let Some(path) = std::env::var_os("SEERDB_R0_UNCOMMITTED_CRASH_PATH") {
        let path = Path::new(&path);
        let marker = PathBuf::from(
            std::env::var_os("SEERDB_R0_UNCOMMITTED_CRASH_MARKER")
                .expect("uncommitted crash child marker path"),
        );
        let mut db = DB::open(path, Options::default()).unwrap();
        for sequence in 0..128 {
            let key = format!("uncommitted-{sequence:03}");
            let value = format!("value-{sequence:03}");
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        fs::write(marker, b"all uncommitted WAL mutations are ready").unwrap();
        loop {
            thread::sleep(Duration::from_secs(1));
        }
    }

    let root = tempdir().unwrap();
    let path = root.path().join("uncommitted-process-crash.db");
    let marker = root.path().join("uncommitted-process-crash.ready");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"published", b"before-crash").unwrap();
        db.flush().unwrap();
    }

    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("dbnext_r0_process_crash_discards_uncommitted_batch")
        .arg("--nocapture")
        .env("SEERDB_R0_UNCOMMITTED_CRASH_PATH", &path)
        .env("SEERDB_R0_UNCOMMITTED_CRASH_MARKER", &marker)
        .spawn()
        .unwrap();
    let mut ready = false;
    for _ in 0..3000 {
        if marker.exists() {
            ready = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        panic!("uncommitted crash child did not publish its ready marker");
    }
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success(), "crash child unexpectedly exited cleanly");

    let mut recovered = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        recovered.get(b"published").unwrap(),
        Some(b"before-crash".to_vec())
    );
    for sequence in 0..128 {
        let key = format!("uncommitted-{sequence:03}");
        assert_eq!(recovered.get(key.as_bytes()).unwrap(), None, "{key}");
    }
    assert_eq!(recovered.durability_status().pending_mutations, 0);
    assert!(!recovered.durability_status().write_fenced);
    recovered.verify().unwrap();
}

#[test]
fn dbnext_r0_process_crash_publication_matrix() {
    if let Some(path) = std::env::var_os("SEERDB_R0_PROCESS_CRASH_PATH") {
        let path = Path::new(&path);
        let fault = std::env::var("SEERDB_R0_PROCESS_CRASH_FAULT").unwrap();
        let mut db = DB::open(path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        if fault == "manifest-mirror-sync" {
            db.put(b"seed", b"seed-value").unwrap();
            db.flush().unwrap();
        }
        db.put(b"key", b"value-2").unwrap();

        match fault.as_str() {
            "wal-sync" => db.inject_wal_sync_failure(),
            "page-write" => db.inject_write_failure(),
            "page-sync" => db.inject_page_range_sync_failure(),
            "manifest-mirror-sync" => db.inject_manifest_mirror_sync_failure(),
            "manifest-sync" => db.inject_manifest_sync_failure(),
            "after-manifest" => db.inject_after_manifest_failure(),
            "final-disk-full" => db.inject_final_write_disk_full(),
            _ => panic!("unknown process crash fault {fault}"),
        }
        let _ = db.flush();

        // Do not run destructors: the parent must recover the on-disk state
        // from the exact process-termination boundary above.
        std::process::exit(137);
    }

    #[derive(Clone, Copy)]
    enum Expected {
        Old,
        New,
        Either,
    }
    let cases = [
        ("wal-sync", Expected::Either),
        ("page-write", Expected::Old),
        ("page-sync", Expected::Old),
        ("manifest-mirror-sync", Expected::Old),
        ("manifest-sync", Expected::Either),
        ("after-manifest", Expected::New),
        ("final-disk-full", Expected::Old),
    ];
    let root = tempdir().unwrap();
    for (fault, expected) in cases {
        let path = root.path().join(format!("process-crash-{fault}.db"));
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("dbnext_r0_process_crash_publication_matrix")
            .arg("--nocapture")
            .env("SEERDB_R0_PROCESS_CRASH_PATH", &path)
            .env("SEERDB_R0_PROCESS_CRASH_FAULT", fault)
            .status()
            .unwrap();
        assert!(!status.success(), "crash child exited cleanly for {fault}");

        let mut recovered = DB::open(&path, Options::default()).unwrap();
        let recovered_value = recovered.get(b"key").unwrap();
        match expected {
            Expected::Old => assert_eq!(recovered_value, Some(b"value-1".to_vec())),
            Expected::New => assert_eq!(recovered_value, Some(b"value-2".to_vec())),
            Expected::Either => assert!(
                recovered_value == Some(b"value-1".to_vec())
                    || recovered_value == Some(b"value-2".to_vec()),
                "ambiguous fault {fault} exposed an invalid state: {recovered_value:?}"
            ),
        }
        assert_eq!(recovered.durability_status().pending_mutations, 0);
        assert!(!recovered.durability_status().write_fenced);
        assert!(recovered.verify().is_ok());
    }
}
#[test]
fn dbnext_r0_rejects_corrupt_blob_artifact() {
    let root = tempdir().unwrap();
    let path = root.path().join("corrupt-blob.db");
    let repair_path = root.path().join("corrupt-blob-repair.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"large", &vec![0xA5; 2_048]).unwrap();
    db.flush().unwrap();

    let blob_path = path.join("seerdb.blob");
    let mut blob = fs::read(&blob_path).unwrap();
    let corrupt_at = blob.len() - 1;
    blob[corrupt_at] ^= 0xFF;
    fs::write(&blob_path, blob).unwrap();

    assert!(matches!(
        db.verify(),
        Err(Error::Corruption(message)) if message.contains("blob")
    ));
    drop(db);
    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("blob")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Blob,
            ..
        })
    ));
    assert!(matches!(
        DB::repair(&path, &repair_path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Blob,
            ..
        })
    ));
    assert!(!repair_path.exists());
}

#[test]
fn dbnext_r0_rejects_malformed_checkpoint_payload() {
    let root = tempdir().unwrap();
    let path = root.path().join("corrupt-checkpoint.db");
    let repair_path = root.path().join("corrupt-checkpoint-repair.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    // Damage the inner checkpoint image of the newest publication frame and
    // repair the frame checksum, so the container looks intact but the
    // bounded payload decode fails unconditionally instead of falling back.
    let metadata_log = path.join("seerdb.meta.log");
    let mut log = fs::read(&metadata_log).unwrap();
    let mut cursor = 12usize;
    let mut newest_frame = None;
    while cursor + 16 <= log.len() {
        let payload_len = u32::from_le_bytes(log[cursor..cursor + 4].try_into().unwrap()) as usize;
        let payload_end = cursor + 16 + payload_len;
        if payload_end > log.len() {
            break;
        }
        newest_frame = Some((cursor, payload_end));
        cursor = payload_end;
    }
    let (frame_start, frame_end) = newest_frame.expect("published database has frames");
    let last = log[frame_end - 1];
    log[frame_end - 1] = last.wrapping_add(1);
    let checksum = crc32c::crc32c(&log[frame_start + 8..frame_end]);
    log[frame_start + 4..frame_start + 8].copy_from_slice(&checksum.to_le_bytes());
    fs::write(&metadata_log, &log).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("checksum mismatch")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check { .. })
    ));
    assert!(matches!(
        DB::repair(&path, &repair_path, Options::default()),
        Err(Error::Check { .. })
    ));
    assert!(!repair_path.exists());
}

#[test]
fn dbnext_r0_rejects_future_meta_version() {
    let root = tempdir().unwrap();
    let path = root.path().join("future-meta.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let metadata_log = path.join("seerdb.meta.log");
    let mut log = fs::read(&metadata_log).unwrap();
    log[8..12].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
    fs::write(&metadata_log, &log).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("unsupported metadata log format version")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Manifest,
            message,
        }) if message.contains("unsupported metadata log format version")
    ));
}

#[test]
fn dbnext_r0_accepts_legacy_meta_checkpoint() {
    let root = tempdir().unwrap();
    let path = root.path().join("legacy-meta.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    // The legacy whole-image seerdb.meta fallback remains openable for
    // databases that predate the manifest-selected metadata log. The inner
    // checkpoint image of the first publication frame is exactly that file:
    // publication payload magic(8) version(4) manifest_len(4) slot(256) then
    // the SEERMET1 checkpoint image including its own trailing checksum.
    let legacy_dir = root.path().join("legacy-meta-fallback.db");
    fs::create_dir(&legacy_dir).unwrap();
    let log = fs::read(path.join("seerdb.meta.log")).unwrap();
    let payload_len = u32::from_le_bytes(log[12..16].try_into().unwrap()) as usize;
    let payload = &log[28..28 + payload_len];
    let manifest_len = u32::from_le_bytes(payload[12..16].try_into().unwrap()) as usize;
    let legacy_meta = payload[16 + manifest_len..].to_vec();
    fs::write(legacy_dir.join("seerdb.meta"), &legacy_meta).unwrap();

    let mut reopened = DB::open(&legacy_dir, Options::default()).unwrap();
    assert!(!reopened.durability_status().write_fenced);
    reopened.close().unwrap();
}

#[test]
fn dbnext_r0_reopens_deep_tree_with_internal_routing() {
    let root = tempdir().unwrap();
    let path = root.path().join("deep-tree.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    for key_id in 0..600 {
        let key = format!("key-{key_id:04}");
        let value = format!("value-{key_id:04}");
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    for key_id in 0..600 {
        let key = format!("key-{key_id:04}");
        let value = format!("value-{key_id:04}");
        assert_eq!(
            reopened.get(key.as_bytes()).unwrap(),
            Some(value.into_bytes()),
            "reopened lookup failed for {key}"
        );
    }
}

#[test]
fn dbnext_r0_blob_upserts_split_leaves_and_reopen() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-split.db");
    let value = vec![0xB7u8; 2_048];
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        for index in 0..192 {
            let key = format!("blob-key-{index:04}");
            db.put(key.as_bytes(), &value).unwrap();
        }
        db.flush().unwrap();
        assert_eq!(db.get(b"blob-key-0000").unwrap(), Some(value.clone()));
        assert_eq!(db.get(b"blob-key-0096").unwrap(), Some(value.clone()));
        assert_eq!(db.get(b"blob-key-0191").unwrap(), Some(value.clone()));
    }

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"blob-key-0000").unwrap(), Some(value.clone()));
    assert_eq!(reopened.get(b"blob-key-0096").unwrap(), Some(value.clone()));
    assert_eq!(reopened.get(b"blob-key-0191").unwrap(), Some(value));
}
