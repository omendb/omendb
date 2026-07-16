#![cfg(feature = "fault-injection")]
#![allow(clippy::disallowed_methods)]

use seerdb::{DB, Error, Options};
use std::collections::BTreeMap;
use std::fs;
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

fn copy_database(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        if !entry.file_type().unwrap().is_file() {
            continue;
        }
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "tmp")
        {
            continue;
        }
        fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
    }
}

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
    db.close().unwrap();

    let restored_path = root.path().join("restored.db");
    copy_database(&source_path, &restored_path);
    let restored = DB::open(&restored_path, Options::default()).unwrap();
    assert_model(&restored, &committed);
    assert_eq!(
        restored.durability_status().database_id,
        initial_status.database_id
    );
    assert_eq!(
        restored.durability_status().history_id,
        initial_status.history_id
    );
    assert_eq!(
        restored.durability_status().commit_id,
        final_status.commit_id
    );
    assert_eq!(
        restored.durability_status().generation_id,
        final_status.generation_id
    );
    assert_eq!(restored.durability_status().pending_mutations, 0);
    assert!(!restored.durability_status().write_fenced);
}

#[test]
fn dbnext_r0_rejects_corrupt_manifest() {
    let root = tempdir().unwrap();
    let path = root.path().join("corrupt.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let manifest_path = path.join("MANIFEST");
    let slot_size = seerdb::storage::format::MANIFEST_SLOT_SIZE;
    let mut manifest = fs::read(&manifest_path).unwrap();
    manifest[..slot_size].fill(0xA5);
    manifest[slot_size..slot_size * 2].fill(0x5A);
    fs::write(manifest_path, manifest).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(_))
    ));
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
    assert!(matches!(db.flush(), Err(Error::DiskFull)));
    assert!(db.durability_status().write_fenced);
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
fn dbnext_r0_rejects_future_manifest_version() {
    let root = tempdir().unwrap();
    let path = root.path().join("future-format.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let manifest_path = path.join("MANIFEST");
    let slot_size = seerdb::storage::format::MANIFEST_SLOT_SIZE;
    let mut manifest = fs::read(&manifest_path).unwrap();
    for slot in manifest.chunks_exact_mut(slot_size) {
        slot[8..12].copy_from_slice(&(seerdb::storage::format::FORMAT_VERSION + 1).to_le_bytes());
        let checksum = crc32c::crc32c(&slot[..252]);
        slot[252..].copy_from_slice(&checksum.to_le_bytes());
    }
    fs::write(manifest_path, manifest).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(_))
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
                    if sequence % 32 == 31 {
                        if db.flush().is_ok() {
                            started.store(true, Ordering::Release);
                        }
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
    for _ in 0..500 {
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
fn dbnext_r0_snapshot_is_verified_and_source_is_unchanged() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("snapshot-source.db");
    let snapshot_path = root.path().join("snapshot-copy.db");
    let large_value = vec![0x5Au8; 2_048];
    let mut source = DB::open(&source_path, Options::default()).unwrap();
    source.put(b"inline", b"value").unwrap();
    source.put(b"large", &large_value).unwrap();
    source.flush().unwrap();

    let source_report = source.verify().unwrap();
    assert!(source_report.verified_pages > 0);
    assert_eq!(source_report.wal_bytes, 0);

    let snapshot = source.snapshot(&snapshot_path).unwrap();
    assert_eq!(snapshot.source, source_report.durability);
    assert_eq!(snapshot.destination, source_report.durability);
    assert!(snapshot.copied_files >= 3);
    assert_eq!(snapshot.verified_pages, source_report.verified_pages);
    assert_eq!(source.get(b"inline").unwrap(), Some(b"value".to_vec()));
    assert_eq!(source.get(b"large").unwrap(), Some(large_value.clone()));

    let mut restored = DB::open(&snapshot_path, Options::default()).unwrap();
    let restored_report = restored.verify().unwrap();
    assert_eq!(restored_report.durability, source_report.durability);
    assert_eq!(restored.get(b"inline").unwrap(), Some(b"value".to_vec()));
    assert_eq!(restored.get(b"large").unwrap(), Some(large_value));
}

#[test]
fn dbnext_r0_rejects_corrupt_blob_artifact() {
    let root = tempdir().unwrap();
    let path = root.path().join("corrupt-blob.db");
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
}

#[test]
fn dbnext_r0_rejects_malformed_checkpoint_container() {
    let root = tempdir().unwrap();
    let path = root.path().join("corrupt-checkpoint.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let checkpoint_path = path.join("seerdb.meta.1");
    let mut checkpoint = fs::read(&checkpoint_path).unwrap();
    checkpoint.push(0xA5);
    fs::write(checkpoint_path, checkpoint).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("trailing")
    ));
}

#[test]
fn dbnext_r0_compact_truncates_reclaimable_tail() {
    let root = tempdir().unwrap();
    let path = root.path().join("compact.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    let first_bytes = fs::metadata(path.join("seerdb.data")).unwrap().len();

    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();
    let second_bytes = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert!(second_bytes > first_bytes);

    db.put(b"key", b"value-3").unwrap();
    db.flush().unwrap();
    let before = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert_eq!(before, second_bytes);

    let report = db.compact().unwrap();
    assert!(report.manifest_replicated);
    assert!(report.reclaimed_pages > 0);
    assert_eq!(report.data_bytes_before, before);
    assert!(report.data_bytes_after < report.data_bytes_before);
    assert_eq!(db.get(b"key").unwrap(), Some(b"value-3".to_vec()));
    assert_eq!(db.verify().unwrap().data_bytes, report.data_bytes_after);

    drop(db);
    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-3".to_vec()));
    assert_eq!(
        reopened.verify().unwrap().data_bytes,
        report.data_bytes_after
    );
}

#[test]
fn dbnext_r0_compact_failure_fences_writer_until_reopen() {
    let root = tempdir().unwrap();
    let path = root.path().join("compact-fault.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-3").unwrap();
    db.flush().unwrap();

    db.inject_sync_failure();
    assert!(matches!(db.compact(), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    assert!(matches!(
        db.put(b"after-fault", b"value"),
        Err(Error::NeedsRecovery(_))
    ));

    drop(db);
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-3".to_vec()));
    assert!(!reopened.durability_status().write_fenced);
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
