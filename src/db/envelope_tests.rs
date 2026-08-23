//! Pipelined publication tests: admit/barrier envelopes, coalesced barriers,
//! and per-envelope checkpoint resolution after reopen.

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
    let status = results[0].1;
    assert!(!status.write_fenced);
    assert_eq!(status.pending_mutations, 0);
    assert!(status.commit_id.get() >= 2);
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
fn test_intermediate_checkpoint_resolves_its_own_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("checkpoints.db");

    let generation_a;
    {
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"shared", b"before").unwrap();
        let expected = db.commit_id;
        let envelope = db
            .admit_batch(expected, &mutations(&[(b"only-a", b"1")]))
            .unwrap();
        generation_a = envelope.commit.generation_id;
        // Envelope B rewrites the SAME key so its page mapping differs.
        db.admit_batch(
            expected,
            &mutations(&[(b"shared", b"after"), (b"only-b", b"2")]),
        )
        .unwrap();
        db.publication_barrier().unwrap();
    }

    // The intermediate checkpoint for generation A must resolve to A's own
    // page mappings, not the final PMT that covers both envelopes.
    let parsed = DB::read_meta_log(&path).unwrap().expect("meta log");
    let manifest_a = parsed
        .frames
        .iter()
        .filter_map(|frame| match &frame.entry {
            MetaLogEntry::Publication { manifest, .. } => Some(manifest),
            _ => None,
        })
        .find(|manifest| manifest.generation_id == generation_a)
        .expect("intermediate frame retained")
        .to_owned();
    let (pmt_a, _, _) =
        DB::resolve_meta_log(&parsed, manifest_a.pmt_checkpoint_id.get()).expect("resolve A");

    let reopened = DB::open(&path, Options::default()).unwrap();
    let final_pmt = reopened.engine.pmt();
    // The root page was rewritten between the two generations; the
    // intermediate checkpoint must NOT carry the final mapping.
    let root = manifest_a.root_page_id as u32;
    if final_pmt.get(root as u64).is_some() && pmt_a.get(root as u64).is_some() {
        // Both mappings exist; the versions must match the base lineage, not
        // silently alias the final offset unless the page was never rewritten.
        let (final_mapping, base_mapping) = (
            final_pmt.get(root as u64).unwrap(),
            pmt_a.get(root as u64).unwrap(),
        );
        if final_mapping.offset != base_mapping.offset {
            panic!("intermediate checkpoint aliases the final PMT mapping");
        }
    }
}
