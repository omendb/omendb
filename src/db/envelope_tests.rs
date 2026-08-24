//! Pipelined publication tests: admit/barrier envelopes, coalesced
//! one-generation-per-group barriers, failure restoration, and reopen.

use super::metadata_codec::MetaLogEntry;
use super::*;
use std::process::Command;
use tempfile::tempdir;

fn mutations(pairs: &[(&[u8], &[u8])]) -> Vec<BatchMutation> {
    pairs
        .iter()
        .map(|(key, value)| BatchMutation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })
        .collect()
}

#[test]
fn test_admit_batch_then_barrier_publishes_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("envelopes.db");

    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"base", b"0").unwrap();

    let expected = db.commit_id;
    let envelope_a = db
        .admit_batch(expected, &mutations(&[(b"a", b"1")]))
        .unwrap();
    let envelope_b = db
        .admit_batch(expected, &mutations(&[(b"b", b"2")]))
        .unwrap();
    assert_ne!(envelope_a.envelope_id, envelope_b.envelope_id);

    // Installed-but-unbarriered state is visible on this handle by contract.
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));

    let results = db.publication_barrier().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, envelope_a.envelope_id);
    assert_eq!(results[1].0, envelope_b.envelope_id);

    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
    // One group publishes ONE generation: both envelopes ack with the same
    // post-publication status and a single commit advances the frontier.
    let status = results[0].1;
    assert_eq!(results[1].1.commit_id, status.commit_id);
    assert_eq!(results[1].1.generation_id, status.generation_id);
    assert!(!status.write_fenced);
    assert_eq!(status.pending_mutations, 0);
    assert_eq!(status.commit_id.get(), 1);
}

#[test]
fn test_empty_barrier_is_noop() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    assert!(db.publication_barrier().unwrap().is_empty());
}

#[test]
fn test_admit_batch_expected_commit_conflict() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("conflict.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"k", b"v").unwrap();
    let stale = CommitId::new(db.commit_id.get() + 99);
    let error = db
        .admit_batch(stale, &mutations(&[(b"x", b"y")]))
        .unwrap_err();
    assert!(matches!(error, Error::SerializationConflict { .. }));
    // No side effects from the rejected admission.
    assert!(db.publication_barrier().unwrap().is_empty());
}

#[test]
fn test_reopen_selects_last_envelope_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("reopen.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        let expected = db.commit_id;
        db.admit_batch(expected, &mutations(&[(b"a", b"1")]))
            .unwrap();
        db.admit_batch(expected, &mutations(&[(b"a", b"2"), (b"b", b"3")]))
            .unwrap();
        db.publication_barrier().unwrap();
    }

    let db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"3"[..]));
}

#[test]
fn test_crash_before_barrier_recovers_previous_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("crash.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"durable", b"yes").unwrap();
        // An explicit flush is what makes the base generation durable; a bare
        // put stays in the pending prefix and is legitimately lost.
        db.flush().unwrap();
        let expected = db.commit_id;
        db.admit_batch(expected, &mutations(&[(b"lost", b"value")]))
            .unwrap();
        // Simulate a crash before the barrier by dropping without publishing.
    }

    let db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"durable").unwrap().as_deref(), Some(&b"yes"[..]));
    assert_eq!(db.get(b"lost").unwrap(), None);
}

#[test]
fn test_barrier_publishes_one_generation_for_the_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("single-generation.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"base", b"before").unwrap();
        db.flush().unwrap();
        let generation_before = db.durability_status().generation_id.get();
        let expected = db.commit_id;
        db.admit_batch(expected, &mutations(&[(b"only-a", b"1")]))
            .unwrap();
        // Envelope B rewrites the SAME key; the group must still publish as
        // ONE generation, so exactly one frame appears past the base.
        db.admit_batch(
            expected,
            &mutations(&[(b"shared", b"after"), (b"only-b", b"2")]),
        )
        .unwrap();
        db.publication_barrier().unwrap();

        let parsed = DB::read_meta_log(&path).unwrap().expect("meta log");
        let new_frames = parsed
            .frames
            .iter()
            .filter(|frame| match &frame.entry {
                MetaLogEntry::Publication { manifest, .. } => {
                    manifest.generation_id.get() > generation_before
                }
                _ => false,
            })
            .count();
        assert_eq!(new_frames, 1, "one group publishes exactly one frame");
    }

    // The single group checkpoint resolves to the final mappings and the
    // reopened database serves every admitted mutation.
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get(b"base").unwrap().as_deref(),
        Some(&b"before"[..])
    );
    assert_eq!(
        reopened.get(b"shared").unwrap().as_deref(),
        Some(&b"after"[..])
    );
    assert_eq!(reopened.get(b"only-a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(reopened.get(b"only-b").unwrap().as_deref(), Some(&b"2"[..]));
}

#[test]
fn test_failed_barrier_restores_envelopes_in_order() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("restore.db");

    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"base", b"0").unwrap();
    db.flush().unwrap();

    let expected = db.commit_id;
    let envelope_a = db
        .admit_batch(expected, &mutations(&[(b"a", b"1")]))
        .unwrap();
    let envelope_b = db
        .admit_batch(expected, &mutations(&[(b"b", b"2")]))
        .unwrap();

    // A real capacity refusal (ENOSPC mid-publication) is hard to trigger
    // deterministically; FAIL_NEXT_GROUP_SYNC fails the barrier at the same
    // stage-1 boundary. The injected IO error fences like any non-preflight
    // failure, while a genuine Error::CapacityPreflight leaves the writer
    // unfenced and retryable - clear the fence here to model that case.
    super::faults::FAIL_NEXT_GROUP_SYNC.with(|failure| failure.set(true));
    assert!(db.publication_barrier().is_err());
    assert!(db.durability_status().write_fenced);
    // Every staged envelope re-enters the pending list in admission order,
    // so a later barrier republishes the whole group.
    let restored: Vec<u64> = db
        .pending_envelopes
        .iter()
        .map(|envelope| envelope.envelope_id)
        .collect();
    assert_eq!(
        restored,
        vec![envelope_a.envelope_id, envelope_b.envelope_id]
    );

    db.write_fenced = false;
    let results = db.publication_barrier().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, envelope_a.envelope_id);
    assert_eq!(results[1].0, envelope_b.envelope_id);
    assert!(!db.durability_status().write_fenced);
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));

    drop(db);
    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(reopened.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(reopened.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
}

#[test]
fn test_frame_append_failure_fences_and_recovery_republishes_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("frame-failure.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"durable", b"yes").unwrap();
        db.flush().unwrap();
        let expected = db.commit_id;
        db.admit_batch(expected, &mutations(&[(b"lost-a", b"1")]))
            .unwrap();
        db.admit_batch(expected, &mutations(&[(b"lost-b", b"2")]))
            .unwrap();
        // Fail the single group frame append. Stage ordering puts the WAL
        // commit sync BEFORE the frame, so the group is fully durable and
        // reopen must recover it by replaying the committed prefix.
        super::faults::FAIL_NEXT_FRAME_APPEND_N.with(|count| count.set(1));
        assert!(db.publication_barrier().is_err());
        assert!(db.durability_status().write_fenced);
    }

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get(b"durable").unwrap().as_deref(),
        Some(&b"yes"[..])
    );
    assert_eq!(reopened.get(b"lost-a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(reopened.get(b"lost-b").unwrap().as_deref(), Some(&b"2"[..]));
    assert!(!reopened.durability_status().write_fenced);
}

#[test]
fn test_pre_commit_failure_keeps_previous_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("pre-commit-failure.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"durable", b"yes").unwrap();
        db.flush().unwrap();
        let expected = db.commit_id;
        db.admit_batch(expected, &mutations(&[(b"lost-a", b"1")]))
            .unwrap();
        db.admit_batch(expected, &mutations(&[(b"lost-b", b"2")]))
            .unwrap();
        // Fail the group's WAL write before any group bytes reach the file
        // (stage 1's unsynced prefix write consumes nothing else - the
        // default SyncPolicy::None skips its sync). No commit record ever
        // becomes durable, so reopen must select the PREVIOUS generation and
        // discard the unpublished mutation prefix.
        super::faults::FAIL_NEXT_WAL_WRITE.with(|failure| failure.set(true));
        assert!(db.publication_barrier().is_err());
        assert!(db.durability_status().write_fenced);
    }

    let reopened = DB::open(&path, Options::default()).unwrap();
    assert_eq!(
        reopened.get(b"durable").unwrap().as_deref(),
        Some(&b"yes"[..])
    );
    assert_eq!(reopened.get(b"lost-a").unwrap(), None);
    assert_eq!(reopened.get(b"lost-b").unwrap(), None);
    assert!(!reopened.durability_status().write_fenced);
}

#[test]
fn test_group_barrier_has_fixed_sync_budget() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sync-budget.db");

    let mut db = DB::open(&path, Options::default()).unwrap();
    db.put(b"base", b"0").unwrap();
    db.flush().unwrap();

    let expected = db.commit_id;
    db.admit_batch(expected, &mutations(&[(b"a", b"1")]))
        .unwrap();
    db.admit_batch(expected, &mutations(&[(b"b", b"2")]))
        .unwrap();

    let syncs_before = db.metrics().unwrap().storage.syncs;
    db.publication_barrier().unwrap();
    let sync_delta = db.metrics().unwrap().storage.syncs - syncs_before;
    // The data device is synced ONCE for the whole two-envelope group; more
    // envelopes never increase this number. Default CoW publication has one
    // data sync plus one metadata-log fsync; an explicit sync_writes policy
    // may add WAL syncs. WAL/meta barriers are tracked only by the process-
    // wide durability_sync_count(), whose global counter cannot be
    // delta-asserted under parallel tests.
    assert_eq!(sync_delta, 1);

    // A second, single-envelope group costs the same one data sync.
    let expected = db.commit_id;
    db.admit_batch(expected, &mutations(&[(b"c", b"3")]))
        .unwrap();
    let syncs_before = db.metrics().unwrap().storage.syncs;
    db.publication_barrier().unwrap();
    assert_eq!(
        db.metrics().unwrap().storage.syncs - syncs_before,
        1,
        "barrier cost is independent of envelope count"
    );
}

#[test]
fn test_fenced_barrier_rejects_retry() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("fenced.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    let expected = db.commit_id;
    db.admit_batch(expected, &mutations(&[(b"a", b"1")]))
        .unwrap();

    // A post-commit failure fences the writer; the barrier must then refuse
    // retries instead of appending a second commit for the same prefix.
    faults::FAIL_NEXT_FRAME_APPEND_N.with(|count| count.set(1));
    assert!(db.publication_barrier().is_err());
    let error = db
        .publication_barrier()
        .expect_err("fenced writer must not retry");
    assert!(!matches!(error, Error::CapacityPreflight));

    // Reopen clears the fence; recovery republishes the durable group.
    drop(db);
    let db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
}

#[test]
fn test_flush_publishes_staged_envelope_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("flush_group.db");

    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        let expected = db.commit_id;
        db.admit_batch(expected, &mutations(&[(b"a", b"1")]))
            .unwrap();
        db.admit_batch(expected, &mutations(&[(b"b", b"2")]))
            .unwrap();
        // Generic flush must route the staged group through its barrier
        // instead of publishing the prefix on a parallel legacy path.
        db.flush().unwrap();
        assert!(db.pending_envelopes.is_empty());
    }

    let db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
}

fn wal_first_options() -> Options {
    Options {
        wal_first_commits: true,
        ..Options::default()
    }
}

#[test]
fn test_wal_first_commits_replay_after_simulated_crash() {
    const CHILD_ENV: &str = "SEERDB_WAL_FIRST_CRASH_CHILD_PATH";
    if let Some(path) = std::env::var_os(CHILD_ENV) {
        let path = PathBuf::from(path);
        let mut db = DB::open(&path, wal_first_options()).unwrap();
        for batch in 0..5 {
            let mutations: Vec<BatchMutation> = (0..4)
                .map(|op| BatchMutation::Put {
                    key: format!("batch{batch}-op{op}").into_bytes(),
                    value: format!("value-{batch}-{op}").into_bytes(),
                })
                .collect();
            // Each batch acks after one group WAL sync; no close runs, so
            // the synced WAL prefix is the only durable evidence.
            db.commit_batch(&mutations).unwrap();
        }
        std::process::exit(137);
    }

    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-first-crash.db");
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("db::envelope_tests::test_wal_first_commits_replay_after_simulated_crash")
        .arg("--nocapture")
        .env(CHILD_ENV, &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(137));

    let mut db = DB::open(&path, Options::default()).unwrap();
    for batch in 0..5 {
        for op in 0..4 {
            let key = format!("batch{batch}-op{op}");
            assert_eq!(
                db.get(key.as_bytes()).unwrap().as_deref(),
                Some(format!("value-{batch}-{op}").as_bytes()),
                "acked wal-first commit must replay after crash"
            );
        }
    }
    db.verify().unwrap();
    db.close().unwrap();
}

#[test]
fn test_wal_first_materializes_on_clean_close() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-first-close.db");
    {
        let mut db = DB::open(&path, wal_first_options()).unwrap();
        db.commit_batch(&[BatchMutation::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }])
        .unwrap();
        db.close().unwrap();
    }
    let mut db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    db.verify().unwrap();
    db.close().unwrap();
}

#[test]
fn test_wal_first_flush_publishes_authority_frame() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-first-flush.db");
    let mut db = DB::open(&path, wal_first_options()).unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"key".to_vec(),
        value: b"v1".to_vec(),
    }])
    .unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"v1".to_vec()));
    db.close().unwrap();

    // After materialization a crash loses nothing: the frame owns the state.
    let mut db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"v1".to_vec()));
    db.verify().unwrap();

    // The engine stays writable after materialization.
    db.commit_batch(&[BatchMutation::Put {
        key: b"key2".to_vec(),
        value: b"v2".to_vec(),
    }])
    .unwrap();
    assert_eq!(db.get(b"key2").unwrap(), Some(b"v2".to_vec()));
    db.close().unwrap();
}

#[test]
fn test_wal_first_blob_batch_survives_crash_before_materialization() {
    const CHILD_ENV: &str = "SEERDB_WAL_FIRST_BLOB_CRASH_CHILD_PATH";
    if let Some(path) = std::env::var_os(CHILD_ENV) {
        let path = PathBuf::from(path);
        let mut db = DB::open(&path, wal_first_options()).unwrap();
        let large = vec![0xABu8; 2048];
        db.commit_batch(&[
            BatchMutation::Put {
                key: b"inline".to_vec(),
                value: b"small".to_vec(),
            },
            BatchMutation::Put {
                key: b"blob".to_vec(),
                value: large,
            },
        ])
        .unwrap();
        std::process::exit(137);
    }

    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-first-blob.db");
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("db::envelope_tests::test_wal_first_blob_batch_survives_crash_before_materialization")
        .arg("--nocapture")
        .env(CHILD_ENV, &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(137));

    let mut db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"inline").unwrap(), Some(b"small".to_vec()));
    assert_eq!(db.get(b"blob").unwrap(), Some(vec![0xABu8; 2048]));
    db.verify().unwrap();
    db.close().unwrap();
}

#[test]
fn test_wal_first_disabled_matches_default_behavior() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-first-off.db");
    let mut db = DB::open(&path, Options::default()).unwrap();
    db.commit_batch(&[BatchMutation::Put {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
    }])
    .unwrap();
    // Default mode still publishes through the full pipeline on flush.
    db.flush().unwrap();
    db.close().unwrap();
    let mut db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    db.verify().unwrap();
    db.close().unwrap();
}

#[test]
fn test_wal_first_auto_materializes_at_bound() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-first-bound.db");
    let options = Options {
        wal_first_commits: true,
        // Each batch is ~2.7 KB of WAL; the bound crosses after two batches.
        wal_materialize_bytes: 4 * 1024,
        ..wal_first_options()
    };
    let mut db = DB::open(&path, options).unwrap();
    for batch in 0..6 {
        let mutations: Vec<BatchMutation> = (0..4)
            .map(|op| BatchMutation::Put {
                key: format!("bound{batch}-op{op}").into_bytes(),
                value: format!("value-{batch}-{op}").into_bytes(),
            })
            .collect();
        db.commit_batch(&mutations).unwrap();
        // The bound forces periodic materialization: the published manifest
        // frontier must advance without any explicit flush call.
        let status = db.durability_status();
        assert_eq!(status.pending_mutations, 0);
        assert!(!status.write_fenced);
    }
    db.verify().unwrap();

    // Every acked batch survives a crash-style exit: materialization kept
    // the unframed window bounded, and recovery covers the remainder.
    drop(db);
    let mut db = DB::open(&path, Options::default()).unwrap();
    for batch in 0..6 {
        for op in 0..4 {
            let key = format!("bound{batch}-op{op}");
            assert_eq!(
                db.get(key.as_bytes()).unwrap().as_deref(),
                Some(format!("value-{batch}-{op}").as_bytes())
            );
        }
    }
    db.close().unwrap();
}

#[test]
fn test_wal_first_zero_bound_disables_auto_materialization() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-first-nobound.db");
    let options = Options {
        wal_first_commits: true,
        wal_materialize_bytes: 0,
        ..wal_first_options()
    };
    let mut db = DB::open(&path, options).unwrap();
    for batch in 0..10 {
        let mutations: Vec<BatchMutation> = (0..4)
            .map(|op| BatchMutation::Put {
                key: format!("nob{batch}-op{op}").into_bytes(),
                value: format!("value-{batch}-{op}").into_bytes(),
            })
            .collect();
        db.commit_batch(&mutations).unwrap();
    }
    // With the bound disabled no publication frame may appear behind the
    // caller's back: only the bootstrap generation-0 frame exists.
    let parsed = DB::read_meta_log(&path).unwrap().unwrap();
    assert!(parsed.frames.iter().all(|frame| match &frame.entry {
        MetaLogEntry::Publication { manifest, .. } => {
            manifest.generation_id == GenerationId::new(0)
        }
        _ => true,
    }));
    db.flush().unwrap();
    let parsed = DB::read_meta_log(&path).unwrap().unwrap();
    assert!(parsed.frames.iter().any(|frame| match &frame.entry {
        MetaLogEntry::Publication { manifest, .. } => {
            manifest.generation_id > GenerationId::new(0)
        }
        _ => false,
    }));
    db.verify().unwrap();
    db.close().unwrap();

    let mut db = DB::open(&path, Options::default()).unwrap();
    for batch in 0..10 {
        for op in 0..4 {
            let key = format!("nob{batch}-op{op}");
            assert_eq!(
                db.get(key.as_bytes()).unwrap().as_deref(),
                Some(format!("value-{batch}-{op}").as_bytes())
            );
        }
    }
    db.close().unwrap();
}

#[test]
fn test_wal_first_sync_failure_fences_and_recovery_resolves_outcome() {
    const CHILD_ENV: &str = "SEERDB_WAL_FIRST_SYNC_FAIL_CHILD_PATH";
    if let Some(path) = std::env::var_os(CHILD_ENV) {
        let path = PathBuf::from(path);
        let mut db = DB::open(&path, wal_first_options()).unwrap();
        let batch1: Vec<BatchMutation> = (0..4)
            .map(|op| BatchMutation::Put {
                key: format!("ack{op}").into_bytes(),
                value: format!("value-{op}").into_bytes(),
            })
            .collect();
        db.commit_batch(&batch1).unwrap();

        // The next group's WAL sync fails after its bytes reached the file:
        // the caller sees Err (outcome unknown), the writer fences.
        db.inject_wal_sync_failure();
        let result = db.commit_batch(&[BatchMutation::Put {
            key: b"ambiguous".to_vec(),
            value: b"value".to_vec(),
        }]);
        assert!(result.is_err());
        assert!(db.durability_status().write_fenced);
        std::process::exit(137);
    }

    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-first-syncfail.db");
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("db::envelope_tests::test_wal_first_sync_failure_fences_and_recovery_resolves_outcome")
        .arg("--nocapture")
        .env(CHILD_ENV, &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(137));

    // Recovery must accept both outcomes of the ambiguous batch: its commit
    // envelope was complete on disk when the process died, so the committed
    // prefix replays (old-or-complete-new accepts unacknowledged complete
    // generations). The acked batch is present either way.
    let mut db = DB::open(&path, Options::default()).unwrap();
    for op in 0..4 {
        let key = format!("ack{op}");
        assert_eq!(
            db.get(key.as_bytes()).unwrap().as_deref(),
            Some(format!("value-{op}").as_bytes()),
            "acked batch must survive"
        );
    }
    assert_eq!(
        db.get(b"ambiguous").unwrap(),
        Some(b"value".to_vec()),
        "complete-but-unacked envelope replays per old-or-complete-new"
    );
    db.verify().unwrap();
    db.close().unwrap();
}

#[test]
fn test_wal_first_materialization_failure_preserves_acked_commits() {
    const CHILD_ENV: &str = "SEERDB_WAL_FIRST_MAT_FAIL_CHILD_PATH";
    if let Some(path) = std::env::var_os(CHILD_ENV) {
        let path = PathBuf::from(path);
        let mut db = DB::open(
            &path,
            Options {
                wal_first_commits: true,
                // A 1-byte bound makes every ack cross it, so the second
                // batch's auto-materialization hits the injected fault.
                wal_materialize_bytes: 1,
                ..wal_first_options()
            },
        )
        .unwrap();
        db.commit_batch(&[BatchMutation::Put {
            key: b"acked".to_vec(),
            value: b"value".to_vec(),
        }])
        .unwrap();

        // Auto-materialization crosses the bound on the next batch; its
        // authority-frame sync fails after pages were written.
        db.inject_manifest_sync_failure();
        let result = db.commit_batch(&[BatchMutation::Put {
            key: b"second".to_vec(),
            value: b"value".to_vec(),
        }]);
        assert!(result.is_err());
        std::process::exit(137);
    }

    let dir = tempdir().unwrap();
    let path = dir.path().join("wal-first-matfail.db");
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("db::envelope_tests::test_wal_first_materialization_failure_preserves_acked_commits")
        .arg("--nocapture")
        .env(CHILD_ENV, &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(137));

    let mut db = DB::open(&path, Options::default()).unwrap();
    assert_eq!(db.get(b"acked").unwrap(), Some(b"value".to_vec()));
    // The second batch may have materialized (frame landed despite the
    // injected sync error being reported) or replay from its WAL prefix;
    // both outcomes are acceptable old-or-complete-new results.
    if let Some(value) = db.get(b"second").unwrap() {
        assert_eq!(value, b"value".to_vec());
    }
    db.verify().unwrap();
    db.close().unwrap();
}

#[test]
#[ignore]
fn probe_wal_first_blob_image_freshness() {
    // Mirrors the failing property-test sequence: initial blob put, then
    // transaction commit/abort cycles with periodic close/reopen, checking
    // the generation recorded inside the on-disk blob image after each step.
    fn disk_blob_generation(path: &std::path::Path) -> Option<u64> {
        let bytes = std::fs::read(path.join(BLOB_FILE)).ok()?;
        if bytes.len() < 28 {
            return Some(0);
        }
        let raw = u64::from_le_bytes(bytes[20..28].try_into().ok()?);
        Some(raw)
    }

    for segmented in [false, true] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blob-freshness.db");
        let options = Options {
            wal_first_commits: true,
            blob_storage: if segmented {
                BlobStorageMode::Segmented
            } else {
                BlobStorageMode::WholeImage
            },
            ..Options::default()
        };
        let mut db = DB::open(&path, options.clone()).unwrap();
        println!("=== segmented={segmented} ===");
        db.commit_batch(&[BatchMutation::Put {
            key: b"blob0".to_vec(),
            value: vec![0xB6; 2048],
        }])
        .unwrap();
        println!(
            "after initial blob put: disk_gen={:?} db_gen={}",
            disk_blob_generation(&path),
            db.durability_status().generation_id.get()
        );

        for index in 0..6usize {
            let mut tx = db.begin_batch_transaction().unwrap();
            tx.put(&format!("tx{index}").into_bytes(), b"v").unwrap();
            if index % 2 == 0 {
                tx.commit(&mut db).unwrap();
            } else {
                tx.abort().unwrap();
            }
            println!(
                "after tx{index} ({}): disk_gen={:?} db_gen={}",
                if index % 2 == 0 { "commit" } else { "abort" },
                disk_blob_generation(&path),
                db.durability_status().generation_id.get()
            );
            if index % 4 == 3 {
                if let Err(error) = db.verify() {
                    println!("verify FAILED at index {index}: {error}");
                    break;
                }
                db.close().unwrap();
                db = DB::open(&path, options.clone()).unwrap();
                if let Err(error) = db.verify() {
                    println!("reopen verify FAILED at index {index}: {error}");
                    break;
                }
            }
        }
        db.close().unwrap();
    }
}
