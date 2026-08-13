#![cfg(feature = "fault-injection")]
#![allow(clippy::disallowed_methods)]

use seerdb::blob::BlobManager;
use seerdb::recovery::WalRecord;
use seerdb::storage::format::{CommitId, CommitRecord, FORMAT_VERSION, GenerationId, Manifest};
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

#[path = "dbnext_r0/blob_reclamation.rs"]
mod blob_reclamation;
#[path = "dbnext_r0/compaction_reclamation.rs"]
mod compaction_reclamation;
#[path = "dbnext_r0/diagnostics_repair.rs"]
mod diagnostics_repair;
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
fn dbnext_r0_stale_expected_base_is_rejected_without_side_effects() {
    let root = tempdir().unwrap();
    let path = root.path().join("stale-base.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"first".to_vec(),
            value: b"value-1".to_vec(),
        }])
        .unwrap();

    let error = db
        .commit_batch_at(
            CommitId::new(0),
            &[BatchMutation::Put {
                key: b"stale".to_vec(),
                value: b"must-not-appear".to_vec(),
            }],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        Error::SerializationConflict { expected, current }
            if expected == CommitId::new(0) && current == first.commit_id
    ));
    assert_eq!(db.get(b"stale").unwrap(), None);
    assert_eq!(db.durability_status().commit_id, first.commit_id);

    db.commit_batch_at(
        first.commit_id,
        &[BatchMutation::Put {
            key: b"next".to_vec(),
            value: b"value-2".to_vec(),
        }],
    )
    .unwrap();
    assert_eq!(db.get(b"next").unwrap(), Some(b"value-2".to_vec()));
}

#[test]
fn dbnext_r0_retains_arbitrary_historical_commit_across_reopen() {
    let root = tempdir().unwrap();
    let path = root.path().join("historical-commit.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"old".to_vec(),
        }])
        .unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"versioned".to_vec(),
        value: b"new".to_vec(),
    }])
    .unwrap();

    let snapshot_id = db.retain_commit(first.commit_id).unwrap();
    assert_eq!(db.get(b"versioned").unwrap(), Some(b"new".to_vec()));
    assert_eq!(
        db.get_at(snapshot_id, b"versioned").unwrap(),
        Some(b"old".to_vec())
    );
    assert_eq!(
        db.range_at(snapshot_id, b"versioned", b"versioned~")
            .unwrap(),
        vec![(b"versioned".to_vec(), b"old".to_vec())]
    );
    db.close().unwrap();
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(
        reopened.retained_snapshot_id(first.commit_id),
        Some(snapshot_id)
    );
    assert_eq!(
        reopened.get_at(snapshot_id, b"versioned").unwrap(),
        Some(b"old".to_vec())
    );
    reopened.release_snapshot(snapshot_id).unwrap();
    assert!(matches!(
        reopened.get_at(snapshot_id, b"versioned"),
        Err(Error::SnapshotUnavailable(_))
    ));
}

#[test]
fn dbnext_r0_late_retention_rejects_reused_physical_generation() {
    let root = tempdir().unwrap();
    let path = root.path().join("late-retention.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"one".to_vec(),
        }])
        .unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"versioned".to_vec(),
        value: b"two".to_vec(),
    }])
    .unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"versioned".to_vec(),
        value: b"three".to_vec(),
    }])
    .unwrap();

    assert!(matches!(
        db.retain_commit(first.commit_id),
        Err(Error::SnapshotUnavailable(message))
            if message.contains("physical pages reused")
    ));
    assert_eq!(db.get(b"versioned").unwrap(), Some(b"three".to_vec()));
}

#[test]
fn dbnext_r0_late_retention_rejects_reuse_after_failed_publication() {
    let root = tempdir().unwrap();
    let path = root.path().join("late-retention-failed-publication.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"one".to_vec(),
        }])
        .unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"versioned".to_vec(),
        value: b"two".to_vec(),
    }])
    .unwrap();

    db.put(b"versioned", b"three").unwrap();
    db.inject_page_range_sync_failure();
    assert!(matches!(db.flush(), Err(Error::Io(_))));
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"versioned").unwrap(), Some(b"two".to_vec()));
    assert_eq!(reopened.durability_status().commit_id.get(), 2);
    assert!(matches!(
        reopened.retain_commit(first.commit_id),
        Err(Error::SnapshotUnavailable(message))
            if message.contains("physical pages reused")
    ));
    reopened.put(b"versioned", b"four").unwrap();
    reopened.flush().unwrap();
    assert_eq!(reopened.durability_status().commit_id.get(), 4);
    assert_eq!(reopened.get(b"versioned").unwrap(), Some(b"four".to_vec()));
    // The abandoned c3 reservation remains until history pruning proves that
    // no retained root can refer to its possibly overwritten slots.
    assert!(path.join("seerdb.reuse-ledger").exists());
}

#[test]
fn dbnext_r0_ambiguous_new_page_reserves_commit_id() {
    let root = tempdir().unwrap();
    let path = root.path().join("ambiguous-new-page.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();

    // The first data page grows the file; no retired slot is available yet.
    db.put(b"versioned", b"one").unwrap();
    db.inject_page_range_sync_failure();
    assert!(matches!(db.flush(), Err(Error::Io(_))));
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.durability_status().commit_id.get(), 0);
    reopened.put(b"versioned", b"two").unwrap();
    reopened.flush().unwrap();
    assert_eq!(reopened.durability_status().commit_id.get(), 2);
    assert_eq!(reopened.get(b"versioned").unwrap(), Some(b"two".to_vec()));
    assert!(!path.join("seerdb.reuse-ledger").exists());
}

#[test]
fn dbnext_r0_defers_successful_reuse_ledger_cleanup_until_reopen() {
    let root = tempdir().unwrap();
    let path = root.path().join("deferred-reuse-ledger-cleanup.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();

    db.put(b"versioned", b"one").unwrap();
    db.flush().unwrap();
    db.put(b"versioned", b"two").unwrap();
    db.flush().unwrap();
    db.put(b"versioned", b"three").unwrap();
    db.flush().unwrap();

    // The third generation reused the first generation's retired root page.
    // The ledger remains as a conservative on-disk recovery hint, but the
    // in-memory ledger is already reconciled after manifest publication.
    assert!(path.join("seerdb.reuse-ledger").is_file());
    assert_eq!(db.get(b"versioned").unwrap(), Some(b"three".to_vec()));

    drop(db);
    let reopened = DB::open(&path, Options::for_test()).unwrap();
    assert!(!path.join("seerdb.reuse-ledger").exists());
    assert_eq!(reopened.get(b"versioned").unwrap(), Some(b"three".to_vec()));
}

#[test]
fn dbnext_r0_vacuum_rebuilds_live_tree_and_preserves_retained_root() {
    let root = tempdir().unwrap();
    let path = root.path().join("vacuum.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();

    for id in 0..200 {
        let key = format!("key-{id:04}");
        let value = format!("value-before-{id:04}");
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    let first = db.durability_status();

    for id in 0..150 {
        let key = format!("key-{id:04}");
        assert!(db.delete(key.as_bytes()).unwrap());
    }
    db.flush().unwrap();
    let snapshot_id = db.retain_commit(first.commit_id).unwrap();

    let report = db.vacuum().unwrap();
    assert_eq!(report.live_entries, 50);
    assert!(report.logical_pages_before >= report.logical_pages_after);
    assert_eq!(db.get(b"key-0000").unwrap(), None);
    assert_eq!(
        db.get(b"key-0199").unwrap(),
        Some(b"value-before-0199".to_vec())
    );
    assert_eq!(
        db.get_at(snapshot_id, b"key-0000").unwrap(),
        Some(b"value-before-0000".to_vec())
    );
    assert!(db.verify().is_ok());

    db.compact().unwrap();
    assert_eq!(
        db.get_at(snapshot_id, b"key-0149").unwrap(),
        Some(b"value-before-0149".to_vec())
    );
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"key-0000").unwrap(), None);
    assert_eq!(
        reopened.get(b"key-0199").unwrap(),
        Some(b"value-before-0199".to_vec())
    );
    assert_eq!(
        reopened.get_at(snapshot_id, b"key-0000").unwrap(),
        Some(b"value-before-0000".to_vec())
    );
    reopened.release_snapshot(snapshot_id).unwrap();
}

#[test]
fn dbnext_r0_vacuum_write_failure_preserves_prior_generation() {
    let root = tempdir().unwrap();
    let path = root.path().join("vacuum-failure.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    db.put(b"keep", b"old").unwrap();
    db.put(b"remove", b"tombstoned").unwrap();
    db.flush().unwrap();
    db.delete(b"remove").unwrap();
    db.flush().unwrap();

    db.inject_write_failure();
    assert!(db.vacuum().is_err());
    assert!(db.durability_status().write_fenced);
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(reopened.get(b"keep").unwrap(), Some(b"old".to_vec()));
    assert_eq!(reopened.get(b"remove").unwrap(), None);
    assert!(reopened.verify().is_ok());
}

#[test]
fn dbnext_r0_vacuum_capacity_refusal_is_retryable_before_rebuild() {
    let root = tempdir().unwrap();
    let path = root.path().join("vacuum-capacity.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    db.put(b"keep", b"old").unwrap();
    db.put(b"remove", b"tombstoned").unwrap();
    db.flush().unwrap();
    db.delete(b"remove").unwrap();
    db.flush().unwrap();

    let before = db.durability_status();
    db.inject_capacity_limit(0);
    assert!(matches!(db.vacuum(), Err(Error::DiskFull)));
    assert_eq!(db.durability_status(), before);
    assert!(!db.durability_status().write_fenced);
    assert_eq!(db.get(b"keep").unwrap(), Some(b"old".to_vec()));
    assert_eq!(db.get(b"remove").unwrap(), None);

    db.inject_capacity_limit(u64::MAX);
    let report = db.vacuum().unwrap();
    assert_eq!(report.live_entries, 1);
    assert_eq!(db.get(b"keep").unwrap(), Some(b"old".to_vec()));
    assert_eq!(db.get(b"remove").unwrap(), None);
    assert!(db.verify().is_ok());
}

#[test]
fn dbnext_r0_prunes_unretained_history_after_atomic_sidecar_publish() {
    let root = tempdir().unwrap();
    let path = root.path().join("history-prune.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"one".to_vec(),
        }])
        .unwrap();
    let snapshot_id = db.retain_commit(first.commit_id).unwrap();
    let second = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"two".to_vec(),
        }])
        .unwrap();
    let third = db
        .commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"three".to_vec(),
        }])
        .unwrap();

    let second_checkpoint = path.join(format!("seerdb.meta.{}", second.generation_id.get()));
    let first_checkpoint = path.join(format!("seerdb.meta.{}", first.generation_id.get()));
    let third_checkpoint = path.join(format!("seerdb.meta.{}", third.generation_id.get()));
    assert!(first_checkpoint.is_file());
    assert!(second_checkpoint.is_file());
    assert!(third_checkpoint.is_file());

    let report = db.prune_history().unwrap();
    assert_eq!(report.retained_generations, 3);
    // Both manifest slots and the retained snapshot remain recovery roots.
    // The current delta checkpoint also depends on its full and delta
    // ancestors even though the middle logical manifest is unretained.
    assert_eq!(report.removed_checkpoints, 0);
    assert!(second_checkpoint.exists());
    assert!(first_checkpoint.is_file());
    assert!(third_checkpoint.is_file());
    assert_eq!(
        db.get_at(snapshot_id, b"versioned").unwrap(),
        Some(b"one".to_vec())
    );
    drop(db);

    let mut reopened = DB::open(&path, Options::for_test()).unwrap();
    assert_eq!(
        reopened.get_at(snapshot_id, b"versioned").unwrap(),
        Some(b"one".to_vec())
    );
    reopened.release_snapshot(snapshot_id).unwrap();
    let report = reopened.prune_history().unwrap();
    assert_eq!(report.retained_generations, 2);
    assert_eq!(report.removed_checkpoints, 0);
    assert!(first_checkpoint.exists());
    assert!(third_checkpoint.is_file());
    assert!(matches!(
        reopened.get_at(snapshot_id, b"versioned"),
        Err(Error::SnapshotUnavailable(_))
    ));
}

#[test]
fn dbnext_r0_history_prune_rename_failure_preserves_old_sidecar() {
    let root = tempdir().unwrap();
    let path = root.path().join("history-prune-failure.db");
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    db.put(b"versioned", b"one").unwrap();
    db.flush().unwrap();
    let first_checkpoint = path.join("seerdb.meta.1");
    db.put(b"versioned", b"two").unwrap();
    db.flush().unwrap();
    assert!(first_checkpoint.is_file());

    db.inject_atomic_rename_failure();
    assert!(db.prune_history().is_err());
    assert!(first_checkpoint.is_file());
    assert_eq!(db.get(b"versioned").unwrap(), Some(b"two".to_vec()));

    let report = db.prune_history().unwrap();
    assert_eq!(report.removed_checkpoints, 0);
    assert!(first_checkpoint.exists());
}

#[test]
fn dbnext_r0_historical_retention_preserves_replaced_blob_values() {
    let root = tempdir().unwrap();
    let path = root.path().join("historical-blob.db");
    let options = Options {
        blob_threshold: 4,
        ..Options::for_test()
    };
    let mut db = DB::open(&path, options.clone()).unwrap();
    let first = db
        .commit_batch(&[BatchMutation::Put {
            key: b"blob-key".to_vec(),
            value: b"old-large-value".to_vec(),
        }])
        .unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"blob-key".to_vec(),
        value: b"new-large-value".to_vec(),
    }])
    .unwrap();

    let snapshot_id = db.retain_commit(first.commit_id).unwrap();
    assert_eq!(
        db.get_at(snapshot_id, b"blob-key").unwrap(),
        Some(b"old-large-value".to_vec())
    );
    db.close().unwrap();
    drop(db);

    let reopened = DB::open(&path, options).unwrap();
    assert_eq!(
        reopened.get_at(snapshot_id, b"blob-key").unwrap(),
        Some(b"old-large-value".to_vec())
    );
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
fn dbnext_r0_rejects_corrupt_reuse_ledger_with_typed_diagnosis() {
    let root = tempdir().unwrap();
    let path = root.path().join("corrupt-reuse-ledger.db");
    let repair_path = root.path().join("corrupt-reuse-ledger-repair.db");
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"stable", b"one").unwrap();
        db.flush().unwrap();
        db.put(b"stable", b"two").unwrap();
        db.flush().unwrap();
        db.put(b"stable", b"three").unwrap();
        db.inject_page_range_sync_failure();
        assert!(matches!(db.flush(), Err(Error::Io(_))));
    }

    let ledger_path = path.join("seerdb.reuse-ledger");
    let mut ledger = fs::read(&ledger_path).unwrap();
    let last = ledger.len() - 1;
    ledger[last] ^= 0xFF;
    fs::write(&ledger_path, ledger).unwrap();

    assert!(matches!(
        DB::open(&path, Options::default()),
        Err(Error::Corruption(message))
            if message.to_ascii_lowercase().contains("reuse ledger")
    ));
    assert!(matches!(
        DB::check(&path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Format,
            ..
        })
    ));
    assert!(matches!(
        DB::repair(&path, &repair_path, Options::default()),
        Err(Error::Check {
            kind: CheckFailureKind::Format,
            ..
        })
    ));
    assert!(!repair_path.exists());
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
            "wal-truncate" => db.inject_wal_truncate_failure(),
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
        ("wal-truncate", Expected::New),
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
