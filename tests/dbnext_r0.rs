#![cfg(feature = "fault-injection")]
#![allow(clippy::disallowed_methods)]

use seerdb::blob::BlobManager;
use seerdb::recovery::WalRecord;
use seerdb::storage::format::{CommitId, CommitRecord, FORMAT_VERSION, GenerationId, Manifest};
use seerdb::{BatchMutation, CheckFailureKind, DB, Error, Options, RepairAction, WalCheckStatus};
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

fn active_manifest(path: &Path) -> Manifest {
    let bytes = fs::read(path.join("MANIFEST")).unwrap();
    bytes
        .chunks_exact(seerdb::storage::format::MANIFEST_SLOT_SIZE)
        .filter_map(|slot| {
            let slot: &[u8; seerdb::storage::format::MANIFEST_SLOT_SIZE] = slot.try_into().unwrap();
            Manifest::from_bytes(slot).unwrap()
        })
        .reduce(|current, candidate| {
            if candidate.is_newer_than(current) {
                candidate
            } else {
                current
            }
        })
        .expect("database has an active manifest")
}

fn wal_digest(record: &WalRecord) -> u32 {
    let bytes = record.to_bytes();
    let mut input = Vec::with_capacity(4 + bytes.len());
    input.extend_from_slice(&0u32.to_le_bytes());
    input.extend_from_slice(&bytes);
    crc32c::crc32c(&input)
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
    assert_eq!(
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
fn dbnext_r0_atomic_batch_commit_reopens_inline_and_blob_values() {
    let root = tempdir().unwrap();
    let path = root.path().join("atomic-batch.db");
    let large = vec![0x4Bu8; 2_048];
    let mutations = vec![
        BatchMutation::Put {
            key: b"tenant/1/inline".to_vec(),
            value: b"alpha".to_vec(),
        },
        BatchMutation::Put {
            key: b"tenant/1/blob".to_vec(),
            value: large.clone(),
        },
        BatchMutation::Put {
            key: b"tenant/2/inline".to_vec(),
            value: b"beta".to_vec(),
        },
    ];

    let mut db = DB::open(&path, Options::default()).unwrap();
    let status = db.commit_batch(&mutations).unwrap();
    assert_eq!(status.commit_id.get(), 1);
    assert_eq!(status.pending_mutations, 0);
    assert!(!status.write_fenced);
    assert_eq!(db.get(b"tenant/1/inline").unwrap(), Some(b"alpha".to_vec()));
    assert_eq!(db.get(b"tenant/1/blob").unwrap(), Some(large.clone()));
    assert_eq!(db.get(b"tenant/2/inline").unwrap(), Some(b"beta".to_vec()));
    assert_eq!(db.blob_stats().total_valid, 1);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.durability_status().commit_id.get(), 1);
    assert_eq!(
        reopened.get(b"tenant/1/inline").unwrap(),
        Some(b"alpha".to_vec())
    );
    assert_eq!(reopened.get(b"tenant/1/blob").unwrap(), Some(large));
    assert_eq!(
        reopened.get(b"tenant/2/inline").unwrap(),
        Some(b"beta".to_vec())
    );
    assert_eq!(reopened.verify().unwrap().wal_bytes, 0);
}

#[test]
fn dbnext_r0_atomic_batch_wal_failure_drops_the_whole_candidate() {
    fn run_case<F>(root: &Path, name: &str, options: Options, inject: F)
    where
        F: FnOnce(&DB),
    {
        let path = root.join(name);
        let mutations = [
            BatchMutation::Put {
                key: b"batch/a".to_vec(),
                value: b"a".to_vec(),
            },
            BatchMutation::Put {
                key: b"batch/b".to_vec(),
                value: b"b".to_vec(),
            },
        ];
        let mut db = DB::open(&path, options).unwrap();
        inject(&db);
        assert!(matches!(db.commit_batch(&mutations), Err(Error::Io(_))));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"batch/a").unwrap(), None);
        assert_eq!(reopened.get(b"batch/b").unwrap(), None);
        assert_eq!(reopened.durability_status().commit_id.get(), 0);
        assert!(!reopened.durability_status().write_fenced);
        assert!(!path.join("seerdb.wal").exists());
    }

    let root = tempdir().unwrap();
    run_case(
        root.path(),
        "atomic-batch-wal-before-append.db",
        Options::default(),
        DB::inject_wal_write_failure,
    );
    run_case(
        root.path(),
        "atomic-batch-wal-after-append.db",
        Options::default(),
        DB::inject_wal_after_write_failure,
    );
    run_case(
        root.path(),
        "atomic-batch-wal-sync.db",
        Options {
            sync_writes: true,
            ..Options::default()
        },
        DB::inject_wal_sync_failure,
    );
    run_case(
        root.path(),
        "atomic-batch-wal-after-sync.db",
        Options {
            sync_writes: true,
            ..Options::default()
        },
        DB::inject_wal_after_sync_failure,
    );
}

#[test]
fn dbnext_r0_atomic_batch_post_manifest_failure_recovers_whole_batch() {
    let root = tempdir().unwrap();
    let path = root.path().join("atomic-batch-post-manifest.db");
    let mutations = [
        BatchMutation::Put {
            key: b"batch/a".to_vec(),
            value: b"a".to_vec(),
        },
        BatchMutation::Put {
            key: b"batch/b".to_vec(),
            value: b"b".to_vec(),
        },
    ];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.inject_after_manifest_failure();
    assert!(matches!(db.commit_batch(&mutations), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"batch/a").unwrap(), Some(b"a".to_vec()));
    assert_eq!(reopened.get(b"batch/b").unwrap(), Some(b"b".to_vec()));
    assert_eq!(reopened.durability_status().commit_id.get(), 1);
    assert!(!reopened.durability_status().write_fenced);
    assert!(!path.join("seerdb.wal").exists());
}

#[test]
fn dbnext_r0_atomic_batch_backpressure_is_pre_mutation() {
    let root = tempdir().unwrap();
    let path = root.path().join("atomic-batch-backpressure.db");
    let mut db = DB::open(
        &path,
        Options {
            max_wal_bytes: 1,
            ..Options::default()
        },
    )
    .unwrap();
    let mutations = [BatchMutation::Put {
        key: b"batch/key".to_vec(),
        value: b"value".to_vec(),
    }];

    assert!(matches!(
        db.commit_batch(&mutations),
        Err(Error::Backpressure { .. })
    ));
    assert_eq!(db.durability_status().pending_mutations, 0);
    assert!(!db.durability_status().write_fenced);
    assert_eq!(db.get(b"batch/key").unwrap(), None);
}

#[test]
fn dbnext_r0_atomic_batch_rejects_pending_generation_without_publishing() {
    let root = tempdir().unwrap();
    let path = root.path().join("atomic-batch-pending.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"pending", b"value").unwrap();
    let before = db.durability_status();

    let mutations = [BatchMutation::Put {
        key: b"batch/key".to_vec(),
        value: b"batch-value".to_vec(),
    }];
    assert!(matches!(
        db.commit_batch(&mutations),
        Err(Error::InvalidArgument(message)) if message.contains("clean pending generation")
    ));
    assert_eq!(db.durability_status(), before);
    assert_eq!(db.get(b"pending").unwrap(), Some(b"value".to_vec()));
    assert_eq!(db.get(b"batch/key").unwrap(), None);
}

#[test]
fn dbnext_r0_checkpoint_is_verified_and_idempotent_when_clean() {
    let root = tempdir().unwrap();
    let path = root.path().join("checkpoint-barrier.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"checkpointed", b"value").unwrap();

    let first = db.checkpoint().unwrap();
    assert_eq!(first.durability.commit_id.get(), 1);
    assert_eq!(first.wal_bytes, 0);
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
fn dbnext_r0_rejects_corrupt_wal_with_typed_diagnosis() {
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

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.to_ascii_lowercase().contains("wal")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Wal,
            ..
        })
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
        assert!(!path.join("seerdb.wal").exists());
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
    assert!(matches!(
        restored.put(b"must-not-write", b"value"),
        Err(Error::ReadOnly)
    ));

    source.put(b"inline", b"source-updated").unwrap();
    source.delete(b"large").unwrap();
    source.flush().unwrap();
    source.compact().unwrap();
    assert_eq!(
        source.get(b"inline").unwrap(),
        Some(b"source-updated".to_vec())
    );
    assert_eq!(source.get(b"large").unwrap(), None);

    // The verified snapshot is an independent retained root, so source
    // mutation and compaction cannot alter its historical state.
    assert_eq!(restored.get(b"inline").unwrap(), Some(b"value".to_vec()));
    assert_eq!(restored.get(b"large").unwrap(), Some(large_value));
}

#[test]
fn dbnext_r0_owned_snapshot_releases_retained_copy() {
    let root = tempdir().unwrap();
    let path = root.path().join("owned-snapshot-source.db");
    let large_value = vec![0x3Cu8; 2_048];
    let mut source = DB::open(&path, Options::default()).unwrap();
    source.put(b"inline", b"before").unwrap();
    source.put(b"large", &large_value).unwrap();
    source.flush().unwrap();

    let mut snapshot = source.begin_snapshot().unwrap();
    let snapshot_path = snapshot.path().to_path_buf();
    assert_eq!(snapshot.verify().unwrap().wal_bytes, 0);

    source.put(b"inline", b"after").unwrap();
    source.delete(b"large").unwrap();
    source.flush().unwrap();
    source.compact().unwrap();

    assert_eq!(snapshot.get(b"inline").unwrap(), Some(b"before".to_vec()));
    assert_eq!(snapshot.get(b"large").unwrap(), Some(large_value));

    snapshot.release().unwrap();
    assert!(!snapshot_path.exists());
}

#[test]
fn dbnext_r0_retained_root_pins_page_reuse_until_release() {
    let root = tempdir().unwrap();
    let path = root.path().join("retained-root.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"before").unwrap();
    db.flush().unwrap();

    let retained = db.retain_current().unwrap();
    let snapshot_id = retained.snapshot_id();
    assert!(path.join("seerdb.retained").is_file());
    assert_eq!(retained.get(b"key").unwrap(), Some(b"before".to_vec()));

    db.put(b"key", b"after-one").unwrap();
    db.flush().unwrap();
    let first_growth = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert_eq!(db.metrics().unwrap().reclaimable_pages, 0);
    assert_eq!(db.gc().unwrap(), 0);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"after-one".to_vec()));
    assert_eq!(retained.get(b"key").unwrap(), Some(b"before".to_vec()));
    reopened.put(b"key", b"after-two").unwrap();
    reopened.flush().unwrap();
    let second_growth = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert!(second_growth > first_growth);
    // The retained first generation keeps offset zero live; the intermediate
    // generation can already be reused after the second publication.
    assert_eq!(reopened.metrics().unwrap().reclaimable_pages, 1);

    retained.release().unwrap();
    drop(reopened);

    let mut released = DB::open(&path, Options::default()).unwrap();
    released.put(b"key", b"after-release").unwrap();
    released.flush().unwrap();
    let after_release = fs::metadata(path.join("seerdb.data")).unwrap().len();
    assert_eq!(after_release, second_growth);
    assert!(!path.join("seerdb.retained").exists());
    assert_ne!(snapshot_id.get(), 0);
}

#[test]
fn dbnext_r0_corrupt_retention_registry_refuses_open() {
    let root = tempdir().unwrap();
    let path = root.path().join("retained-corrupt.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    let retained = db.retain_current().unwrap();
    drop(db);

    let retention_path = path.join("seerdb.retained");
    let mut bytes = fs::read(&retention_path).unwrap();
    bytes[0] ^= 0xFF;
    fs::write(&retention_path, bytes).unwrap();
    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("retention registry")
    ));
    drop(retained);
}

#[test]
fn dbnext_r0_blob_reclamation_survives_reopen() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-reclamation.db");
    let value = vec![0x6Du8; 2_048];
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"large-a", &value).unwrap();
        db.put(b"large-b", &value).unwrap();
        db.flush().unwrap();
        db.delete(b"large-a").unwrap();
        db.delete(b"large-b").unwrap();
        db.flush().unwrap();
    }

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.blob_stats().total_valid, 0);
    assert_eq!(reopened.blob_stats().total_deleted, 2);
    assert_eq!(reopened.gc().unwrap(), 2);
    assert_eq!(reopened.blob_stats().files_needing_gc, 0);
    assert_eq!(reopened.verify().unwrap().blob_bytes, 44);
}

#[test]
fn dbnext_r0_gc_capacity_refusal_preserves_blob_catalog() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-gc-capacity.db");
    let value = vec![0x6Du8; 2_048];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"large", &value).unwrap();
    db.flush().unwrap();
    db.delete(b"large").unwrap();
    db.flush().unwrap();

    let before_bytes = fs::metadata(path.join("seerdb.blob")).unwrap().len();
    let before = db.blob_stats();
    assert_eq!(before.total_valid, 0);
    assert_eq!(before.total_deleted, 1);
    assert_eq!(before.files_needing_gc, 1);

    db.inject_capacity_limit(0);
    assert!(matches!(db.gc(), Err(Error::DiskFull)));
    assert_eq!(
        fs::metadata(path.join("seerdb.blob")).unwrap().len(),
        before_bytes
    );
    let after_failure = db.blob_stats();
    assert_eq!(after_failure.total_valid, before.total_valid);
    assert_eq!(after_failure.total_deleted, before.total_deleted);
    assert_eq!(after_failure.files_needing_gc, before.files_needing_gc);
    assert!(!db.durability_status().write_fenced);

    db.inject_capacity_limit(u64::MAX);
    assert_eq!(db.gc().unwrap(), 1);
    assert_eq!(db.blob_stats().files_needing_gc, 0);
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"large").unwrap(), None);
    assert_eq!(reopened.blob_stats().files_needing_gc, 0);
}

#[test]
fn dbnext_r0_gc_publication_failure_preserves_previous_blob_image() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-gc-fault.db");
    let value = vec![0x6Du8; 2_048];
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"large", &value).unwrap();
    db.flush().unwrap();
    db.delete(b"large").unwrap();
    db.flush().unwrap();

    db.inject_atomic_rename_failure();
    assert!(matches!(db.gc(), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    assert!(path.join("seerdb.blob.reserve").is_file());
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert!(!path.join("seerdb.blob.reserve").exists());
    assert_eq!(reopened.get(b"large").unwrap(), None);
    assert_eq!(reopened.blob_stats().total_deleted, 1);
    assert_eq!(reopened.blob_stats().files_needing_gc, 1);
    assert_eq!(reopened.gc().unwrap(), 1);
    assert_eq!(reopened.blob_stats().files_needing_gc, 0);
}

#[test]
fn dbnext_r0_blob_to_inline_retires_old_value() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-inline-replacement.db");
    let large = vec![0xC3; 2_048];

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", &large).unwrap();
        db.flush().unwrap();
        db.put(b"key", b"inline-value").unwrap();
        db.flush().unwrap();

        let stats = db.blob_stats();
        assert_eq!(stats.total_valid, 0);
        assert_eq!(stats.total_deleted, 1);
        assert_eq!(db.get(b"key").unwrap(), Some(b"inline-value".to_vec()));
        assert_eq!(db.gc().unwrap(), 1);
        let stats = db.blob_stats();
        assert_eq!(stats.files_needing_gc, 0);
        assert_eq!(stats.total_valid, 0);
        assert_eq!(stats.total_deleted, 0);
    }

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap(),
        Some(b"inline-value".to_vec())
    );
    let stats = reopened.blob_stats();
    assert_eq!(stats.files_needing_gc, 0);
    assert_eq!(stats.total_valid, 0);
    assert_eq!(stats.total_deleted, 0);
}

#[test]
fn dbnext_r0_recovery_retires_blob_on_inline_wal_replacement() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-inline-recovery.db");
    let large = vec![0xC3; 2_048];
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", &large).unwrap();
        db.flush().unwrap();
    }

    let current = active_manifest(&path);
    let record = WalRecord::put(b"key", b"inline-after-recovery");
    let commit = CommitRecord {
        commit_id: CommitId::new(current.commit_id.get() + 1),
        generation_id: GenerationId::new(current.generation_id.get() + 1),
        root_page_id: current.root_page_id,
        mutation_count: 1,
        digest: wal_digest(&record),
    };
    let mut wal_bytes = record.to_bytes();
    wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::write(path.join("seerdb.wal"), wal_bytes).unwrap();

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap(),
        Some(b"inline-after-recovery".to_vec())
    );
    assert_eq!(reopened.blob_stats().total_valid, 0);
    assert_eq!(reopened.blob_stats().total_deleted, 1);
    assert_eq!(reopened.gc().unwrap(), 1);
    assert_eq!(reopened.blob_stats().files_needing_gc, 0);
}

#[test]
fn dbnext_r0_newer_blob_image_cannot_reclaim_manifest_value() {
    let root = tempdir().unwrap();
    let path = root.path().join("blob-publication-fence.db");
    let old_value = vec![0x11u8; 2_048];
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"large", &old_value).unwrap();
        db.flush().unwrap();
    }

    // Simulate a blob image written for a newer generation whose manifest
    // publication did not complete. The old manifest still owns offset 0.
    let mut newer = BlobManager::new();
    let old_pointer = newer.append(b"large", old_value.clone());
    newer.append(b"large", vec![0x22; 2_048]);
    assert!(newer.mark_deleted(&old_pointer));
    let mut bytes = newer.to_bytes();
    bytes[20..28].copy_from_slice(&2u64.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..bytes.len() - 4]);
    let checksum_offset = bytes.len() - 4;
    bytes[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
    fs::write(path.join("seerdb.blob"), bytes).unwrap();

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"large").unwrap(), Some(old_value));
    assert_eq!(reopened.blob_stats().total_deleted, 0);
    assert_eq!(reopened.gc().unwrap(), 0);
    assert_eq!(reopened.get(b"large").unwrap(), Some(vec![0x11; 2_048]));
}

#[test]
fn dbnext_r0_verify_rejects_dangling_blob_pointer() {
    let root = tempdir().unwrap();
    let path = root.path().join("dangling-blob.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"large", &vec![0x7Fu8; 2_048]).unwrap();
        db.flush().unwrap();
    }
    fs::remove_file(path.join("seerdb.blob")).unwrap();

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert!(matches!(
        reopened.verify(),
        Err(Error::Corruption(message)) if message.contains("blob pointer target")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check { kind: CheckFailureKind::Blob, message })
            if message.contains("blob pointer target")
    ));
}

#[test]
fn dbnext_r0_offline_check_is_read_only_and_reports_wal_state() {
    let root = tempdir().unwrap();
    let path = root.path().join("offline-check.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"pending", b"value").unwrap();

    // The writer remains open. A real offline check must not contend on the
    // writer lock or reconcile the WAL in the source directory.
    let pending = DB::check(&path, Options::default()).unwrap();
    assert_eq!(pending.wal_status, WalCheckStatus::Pending);
    assert!(path.join("seerdb.wal").is_file());
    assert_eq!(db.get(b"pending").unwrap(), Some(b"value".to_vec()));

    db.flush().unwrap();
    let clean = DB::check(&path, Options::default()).unwrap();
    assert_eq!(clean.wal_status, WalCheckStatus::Clean);
    assert_eq!(clean.verification.wal_bytes, 0);
}

#[test]
fn dbnext_r0_repair_discards_uncommitted_wal_without_mutating_source() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("repair-pending-source.db");
    let destination_path = root.path().join("repair-pending-destination.db");
    let mut db = DB::open(&source_path, Options::default()).unwrap();
    db.put(b"stable", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"pending", b"uncommitted").unwrap();
    assert!(matches!(
        DB::repair(&source_path, &destination_path, Options::default()),
        Err(Error::DatabaseBusy)
    ));
    drop(db);

    let source_check = DB::check(&source_path, Options::default()).unwrap();
    assert_eq!(source_check.wal_status, WalCheckStatus::Pending);
    let report = DB::repair(&source_path, &destination_path, Options::default()).unwrap();
    assert_eq!(report.action, RepairAction::DiscardedUncommittedWal);
    assert_eq!(report.source_wal_status, WalCheckStatus::Pending);
    assert!(source_path.join("seerdb.wal").is_file());
    assert_eq!(
        DB::check(&source_path, Options::default())
            .unwrap()
            .wal_status,
        WalCheckStatus::Pending
    );

    let repaired = DB::open(&destination_path, Options::default()).unwrap();
    assert_eq!(repaired.get(b"stable").unwrap(), Some(b"value-1".to_vec()));
    assert_eq!(repaired.get(b"pending").unwrap(), None);
    assert_ne!(
        repaired.durability_status().history_id,
        report.source.history_id
    );
}

#[test]
fn dbnext_r0_repair_replays_committed_wal_into_new_location() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("repair-committed-source.db");
    let destination_path = root.path().join("repair-committed-destination.db");
    {
        let mut db = DB::open(&source_path, Options::default()).unwrap();
        db.put(b"stable", b"value-1").unwrap();
        db.flush().unwrap();
    }

    let current = active_manifest(&source_path);
    let mutation = WalRecord::put(b"replayed", b"value-2");
    let commit = CommitRecord {
        commit_id: CommitId::new(current.commit_id.get() + 1),
        generation_id: GenerationId::new(current.generation_id.get() + 1),
        root_page_id: current.root_page_id,
        mutation_count: 1,
        digest: wal_digest(&mutation),
    };
    let mut wal = mutation.to_bytes();
    wal.extend_from_slice(&WalRecord::commit(commit).to_bytes());
    fs::write(source_path.join("seerdb.wal"), wal).unwrap();

    let source_check = DB::check(&source_path, Options::default()).unwrap();
    assert_eq!(source_check.wal_status, WalCheckStatus::NeedsRecovery);
    let report = DB::repair(&source_path, &destination_path, Options::default()).unwrap();
    assert_eq!(report.action, RepairAction::ReconciledCommittedWal);
    assert_eq!(report.source_wal_status, WalCheckStatus::NeedsRecovery);
    assert!(source_path.join("seerdb.wal").is_file());

    let repaired = DB::open(&destination_path, Options::default()).unwrap();
    assert_eq!(repaired.get(b"stable").unwrap(), Some(b"value-1".to_vec()));
    assert_eq!(
        repaired.get(b"replayed").unwrap(),
        Some(b"value-2".to_vec())
    );
    assert_eq!(
        repaired.durability_status().generation_id.get(),
        current.generation_id.get() + 1
    );
    assert_ne!(
        repaired.durability_status().history_id,
        report.source.history_id
    );
}

#[test]
fn dbnext_r0_repair_reconciles_torn_wal_in_destination_only() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("repair-torn-source.db");
    let destination_path = root.path().join("repair-torn-destination.db");
    {
        let mut db = DB::open(&source_path, Options::default()).unwrap();
        db.put(b"stable", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"pending", b"uncommitted").unwrap();
    }
    let wal_path = source_path.join("seerdb.wal");
    let before = fs::read(&wal_path).unwrap();
    fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(&[0xA5, 0x5A])
        .unwrap();

    let source_check = DB::check(&source_path, Options::default()).unwrap();
    assert_eq!(source_check.wal_status, WalCheckStatus::Incomplete);
    let report = DB::repair(&source_path, &destination_path, Options::default()).unwrap();
    assert_eq!(report.action, RepairAction::ReconciledIncompleteWal);
    assert_eq!(report.source_wal_status, WalCheckStatus::Incomplete);
    assert_ne!(before, fs::read(&wal_path).unwrap());

    let repaired = DB::open(&destination_path, Options::default()).unwrap();
    assert_eq!(repaired.get(b"stable").unwrap(), Some(b"value-1".to_vec()));
    assert_eq!(repaired.get(b"pending").unwrap(), None);
}

#[test]
fn dbnext_r0_unrecoverable_repair_refuses_truncated_data() {
    let root = tempdir().unwrap();
    let source_path = root.path().join("repair-truncated-source.db");
    let destination_path = root.path().join("repair-truncated-destination.db");
    {
        let mut db = DB::open(&source_path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    let data_path = source_path.join("seerdb.data");
    let original_length = fs::metadata(&data_path).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&data_path)
        .unwrap()
        .set_len(original_length - 1)
        .unwrap();

    assert!(matches!(
        DB::check(&source_path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::DataPage,
            ..
        })
    ));
    assert!(matches!(
        DB::repair(&source_path, &destination_path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::DataPage,
            ..
        })
    ));
    assert!(!destination_path.exists());
    assert_eq!(fs::metadata(&data_path).unwrap().len(), original_length - 1);
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
fn dbnext_r0_rejects_malformed_checkpoint_container() {
    let root = tempdir().unwrap();
    let path = root.path().join("corrupt-checkpoint.db");
    let repair_path = root.path().join("corrupt-checkpoint-repair.db");
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
        Err(Error::Corruption(message)) if message.contains("checksum")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Checkpoint,
            ..
        })
    ));
    assert!(matches!(
        DB::repair(&path, &repair_path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Checkpoint,
            ..
        })
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

    let checkpoint_path = path.join("seerdb.meta.1");
    let mut checkpoint = fs::read(&checkpoint_path).unwrap();
    checkpoint[8..12].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
    let checksum_offset = checkpoint.len() - 4;
    let checksum = crc32c::crc32c(&checkpoint[..checksum_offset]);
    checkpoint[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
    fs::write(&checkpoint_path, checkpoint).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message)) if message.contains("unsupported meta format version")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Format,
            ..
        })
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

    let checkpoint_path = path.join("seerdb.meta.1");
    let checkpoint = fs::read(&checkpoint_path).unwrap();
    let legacy = checkpoint[12..checkpoint.len() - 4].to_vec();
    fs::write(&checkpoint_path, legacy).unwrap();

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    assert!(reopened.verify().is_ok());
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
fn dbnext_r0_manifest_sync_fault_fences_compaction_and_recovers() {
    let root = tempdir().unwrap();
    let path = root.path().join("manifest-sync-fault.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-3").unwrap();
    db.flush().unwrap();

    db.inject_manifest_sync_failure();
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
fn dbnext_r0_post_manifest_and_wal_truncate_faults_recover() {
    fn run_case<F>(root: &Path, name: &str, inject: F, expect_wal_before_reopen: bool)
    where
        F: FnOnce(&DB),
    {
        let path = root.join(name);
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();

        inject(&db);
        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert!(db.durability_status().write_fenced);
        assert_eq!(path.join("seerdb.wal").exists(), expect_wal_before_reopen);

        drop(db);
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        assert!(!reopened.durability_status().write_fenced);
        assert!(!path.join("seerdb.wal").exists());
    }

    let root = tempdir().unwrap();
    run_case(
        root.path(),
        "post-manifest.db",
        DB::inject_after_manifest_failure,
        true,
    );
    run_case(
        root.path(),
        "wal-truncate.db",
        DB::inject_wal_truncate_failure,
        false,
    );
}

#[test]
fn dbnext_r0_short_and_torn_checkpoint_images_preserve_prior_generation() {
    fn run_case<F>(root: &Path, name: &str, inject: F)
    where
        F: FnOnce(&DB),
    {
        let path = root.join(name);
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();

        inject(&db);
        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
        assert!(!reopened.durability_status().write_fenced);
        assert!(path.join("seerdb.meta.2").is_file());
    }

    let root = tempdir().unwrap();
    run_case(
        root.path(),
        "short-checkpoint.db",
        DB::inject_atomic_short_write_failure,
    );
    run_case(
        root.path(),
        "torn-checkpoint.db",
        DB::inject_atomic_torn_write_failure,
    );
}

#[test]
fn dbnext_r0_post_page_write_failure_preserves_prior_manifest_generation() {
    let root = tempdir().unwrap();
    let path = root.path().join("post-page-write.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.inject_after_write_failure();

    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
    assert!(!reopened.durability_status().write_fenced);
}

#[test]
fn dbnext_r0_final_write_disk_full_fences_and_recovers_prior_generation() {
    let root = tempdir().unwrap();
    let path = root.path().join("final-write-disk-full.db");
    let mut db = DB::open(&path, Options::default()).unwrap();

    db.put(b"key", b"value-1").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"value-2").unwrap();
    db.inject_final_write_disk_full();

    assert!(matches!(db.flush(), Err(Error::DiskFull)));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
    assert_eq!(reopened.durability_status().commit_id.get(), 1);
    assert!(!reopened.durability_status().write_fenced);
    assert_eq!(reopened.verify().unwrap().wal_bytes, 0);
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
