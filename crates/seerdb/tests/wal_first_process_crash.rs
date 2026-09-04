//! Process-level WAL-first crash qualification at the real default
//! materialization bound.
//!
//! The envelope tests cover WAL-first acks with in-process simulated
//! crashes and tiny synthetic bounds (4 KiB). This suite runs the real
//! default `wal_materialize_bytes` (2 MiB) in child processes that die
//! mid-stream, so recovery must handle the production-shaped cases:
//!
//! 1. many acked-but-unmaterialized commits at crash time (pure replay),
//! 2. the ack stream crossing the auto-materialization bound repeatedly
//!    (crash right after the last trigger batch),
//! 3. the crash landing mid-materialization after acked commits (a torn
//!    flush must not lose or un-ack the synced WAL prefix).
//!
//! The parent drives every case through this same test executable: the
//! child exits 137 at the designated point, the parent reopens with
//! default options (recovery must not depend on the writer's ack
//! policy) and asserts every acked commit survives and the store
//! verifies.

#![cfg(feature = "fault-injection")]
#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, Options};
use std::path::PathBuf;
use std::process::Command;
use tempfile::tempdir;

/// Acked-commit stream sized to cross the real 2 MiB bound several
/// times: each batch is ~64 KiB of WAL, so ~32 batches per window.
const BATCHES: usize = 160;
const OPS_PER_BATCH: usize = 64;
const VALUE_BYTES: usize = 1024;

fn key(batch: usize, op: usize) -> Vec<u8> {
    format!("wf-{batch:04}-{op:04}").into_bytes()
}

fn value(batch: usize, op: usize) -> Vec<u8> {
    let mut value = vec![b'v'; VALUE_BYTES];
    // Encode the batch/op index at the head so a recovery mismatch points
    // at the failing batch.
    value[..8].copy_from_slice(&(batch as u64).to_be_bytes());
    value[8..16].copy_from_slice(&(op as u64).to_be_bytes());
    value
}

fn assert_acked_stream(db: &seerdb::DB, through_batch: usize) {
    for batch in 0..through_batch {
        for op in 0..OPS_PER_BATCH {
            let expected = value(batch, op);
            assert_eq!(
                db.get(&key(batch, op)).unwrap().as_deref(),
                Some(expected.as_slice()),
                "acked commit lost after crash: batch {batch} op {op}"
            );
        }
    }
}

fn wal_first_default_options() -> Options {
    Options {
        wal_first_commits: true,
        ..Options::default()
    }
}

fn run_batch(db: &mut seerdb::DB, batch: usize) {
    let mutations: Vec<BatchMutation> = (0..OPS_PER_BATCH)
        .map(|op| BatchMutation::Put {
            key: key(batch, op),
            value: value(batch, op),
        })
        .collect();
    // The ack IS the durability claim under WAL-first: every batch that
    // returns is expected after any later crash.
    db.commit_batch(&mutations).unwrap();
}

#[test]
fn wal_first_process_crash_matrix_at_default_bound() {
    const CHILD_PATH_ENV: &str = "SEERDB_WAL_FIRST_CRASH_PATH";
    const MODE_ENV: &str = "SEERDB_WAL_FIRST_CRASH_MODE";

    if let (Some(path), Some(mode)) = (
        std::env::var_os(CHILD_PATH_ENV),
        std::env::var(MODE_ENV).ok(),
    ) {
        let path = PathBuf::from(path);
        let mut db = seerdb::DB::open(&path, wal_first_default_options()).unwrap();
        match mode.as_str() {
            // 1. Crash mid-stream with the tail unmaterialized: pure WAL
            //    replay must cover every acked batch.
            "unmaterialized-tail" => {
                for batch in 0..BATCHES {
                    run_batch(&mut db, batch);
                }
            }
            // 2. The ack stream crossed the auto-materialization bound
            //    repeatedly (BATCHES x 64 KiB >> 2 MiB): each trigger
            //    batch acked only after its materialization published.
            //    Crash right after the last one.
            "at-materialization" => {
                for batch in 0..BATCHES {
                    run_batch(&mut db, batch);
                    let status = db.durability_status();
                    assert!(
                        !status.write_fenced,
                        "auto-materialization fenced the writer at batch {batch}"
                    );
                }
            }
            // 3. Crash mid-materialization: inject a page-sync failure
            //    and tear the next flush. Acked commits before the torn
            //    flush are synced in the WAL; recovery must keep them.
            "mid-materialization" => {
                for batch in 0..BATCHES {
                    run_batch(&mut db, batch);
                    if batch == BATCHES / 2 {
                        db.inject_page_range_sync_failure();
                        // The fault tears this materialization; the
                        // result is ignored so the exit boundary below
                        // is the process crash.
                        let _ = db.flush();
                        std::process::exit(137);
                    }
                }
                unreachable!("mid-materialization child exits at BATCHES/2");
            }
            _ => panic!("unknown WAL-first crash mode {mode}"),
        }
        std::process::exit(137);
    }

    let dir = tempdir().unwrap();
    for mode in [
        "unmaterialized-tail",
        "at-materialization",
        "mid-materialization",
    ] {
        let path = dir.path().join(format!("wal-first-{mode}.db"));
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("wal_first_process_crash_matrix_at_default_bound")
            .arg("--nocapture")
            .env(CHILD_PATH_ENV, &path)
            .env(MODE_ENV, mode)
            .status()
            .unwrap();
        assert_eq!(
            status.code(),
            Some(137),
            "crash child exited cleanly for mode {mode}"
        );

        // Reopen with DEFAULT options: recovery must never depend on
        // the writer's ack policy, and the reopening handle is a fresh
        // writer whose own publications use the default (full) path.
        let mut db = seerdb::DB::open(&path, Options::default()).unwrap();
        let status = db.durability_status();
        assert_eq!(
            status.pending_mutations, 0,
            "{mode}: reopened with pending mutations"
        );
        assert!(!status.write_fenced, "{mode}: reopened fenced");
        // Mode 3's child dies at BATCHES/2, so only the batches below
        // it acked; modes 1-2 acked the full stream.
        let acked_batches = if mode == "mid-materialization" {
            BATCHES / 2
        } else {
            BATCHES
        };
        assert_acked_stream(&db, acked_batches);
        db.verify().unwrap();
        db.close().unwrap();
        drop(db);
        // A second reopen/close cycle proves the recovery-point
        // materialization itself persisted cleanly.
        let mut db = seerdb::DB::open(&path, Options::default()).unwrap();
        assert_acked_stream(&db, acked_batches);
        db.close().unwrap();
    }
}
