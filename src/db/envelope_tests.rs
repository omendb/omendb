//! Pipelined publication tests: admit/barrier envelopes, coalesced
//! one-generation-per-group barriers, failure restoration, and reopen.

use super::metadata_codec::MetaLogEntry;
use super::*;
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
        let generation_before = db.durability_status().generation_id;
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
                    manifest.generation_id.get() > generation_before.get()
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
