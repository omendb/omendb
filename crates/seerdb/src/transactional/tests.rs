use super::*;
use tempfile::tempdir;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum BeginPause {
    CheckedOpen,
    SampledHead,
}

type BeginHook = (BeginPause, Box<dyn FnOnce()>);
thread_local! {
    static BEGIN_HOOK: std::cell::RefCell<Option<BeginHook>> = const { std::cell::RefCell::new(None) };
}

pub(super) fn pause_begin(point: BeginPause) {
    BEGIN_HOOK.with(|hook| {
        let mut hook = hook.borrow_mut();
        if hook.as_ref().is_some_and(|(at, _)| *at == point) {
            let (_, pause) = hook.take().expect("matching hook");
            pause();
        }
    });
}

#[test]
fn beginning_snapshot_remains_readable_when_gc_runs() {
    let (_directory, database) = database();
    let owned = tree(&database);
    commit_key(&database, owned, b"old");
    let mut writer = database.begin().expect("begin writer");
    writer.put(owned, b"old", b"new").expect("stage update");
    let committed = stage_commit(&mut writer).expect("enqueue update");
    let mut writer = Some(writer);
    let (sampled_tx, sampled_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            BEGIN_HOOK.with(|hook| {
                *hook.borrow_mut() = Some((
                    BeginPause::SampledHead,
                    Box::new(move || {
                        sampled_tx.send(()).expect("signal sampled head");
                        resume_rx.recv().expect("resume begin");
                    }),
                ));
            });
            database.begin().expect("begin reader")
        });
        sampled_rx.recv().expect("wait for sampled head");
        publish_with_lane(&database.runtime, lock_publish(&database.runtime))
            .expect("publish update");
        committed
            .recv()
            .expect("update outcome")
            .expect("update commits");
        // Force GC into the sampling/registration gap if that gap is
        // unprotected. With admission locked, begin must register first.
        let admission_locked = match database.runtime.active_snapshots.try_lock() {
            Ok(guard) => {
                drop(guard);
                false
            }
            Err(std::sync::TryLockError::WouldBlock) => true,
            Err(error) => panic!("snapshot registry: {error}"),
        };
        if !admission_locked {
            drop(writer.take());
            database.gc_versions().expect("GC before registration");
        }
        resume_tx.send(()).expect("resume reader");
        let mut reader = reader.join().expect("reader thread");
        if admission_locked {
            drop(writer.take());
            database.gc_versions().expect("GC after registration");
        }
        assert_eq!(
            reader.get(owned, b"old").expect("snapshot value"),
            Some(b"old".to_vec())
        );
    });
    database.close().expect("close");
}

#[test]
fn beginning_transaction_rechecks_close_before_registration() {
    let (_directory, database) = database();
    let (checked_tx, checked_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let beginning = scope.spawn(|| {
            BEGIN_HOOK.with(|hook| {
                *hook.borrow_mut() = Some((
                    BeginPause::CheckedOpen,
                    Box::new(move || {
                        checked_tx.send(()).expect("signal open check");
                        resume_rx.recv().expect("resume begin");
                    }),
                ));
            });
            database.begin()
        });
        checked_rx.recv().expect("wait for open check");
        database.close().expect("close before registration");
        resume_tx.send(()).expect("resume begin");
        assert!(matches!(
            beginning.join().expect("begin thread"),
            Err(Error::InvalidArgument(_))
        ));
    });
    assert_eq!(
        database
            .runtime
            .oldest_active_snapshot()
            .expect("snapshots"),
        None
    );
    assert_eq!(
        database
            .runtime
            .pending_transactions
            .load(Ordering::Acquire),
        0
    );
}

fn database() -> (tempfile::TempDir, TransactionDatabase) {
    let directory = tempdir().expect("temporary directory");
    let database = TransactionDatabase::create(directory.path().join("db"), Options::for_test())
        .expect("create database");
    (directory, database)
}

fn tree(database: &TransactionDatabase) -> TreeId {
    let mut transaction = database.begin().expect("begin tree transaction");
    let tree = transaction.create_tree().expect("create tree");
    transaction.commit().expect("commit tree");
    tree
}

fn commit_key(database: &TransactionDatabase, tree: TreeId, key: &[u8]) {
    let mut transaction = database.begin().expect("begin");
    transaction.put(tree, key, key).expect("put");
    transaction.commit().expect("commit");
}

#[test]
fn rejected_wave_member_does_not_consume_commit_sequence() {
    let (directory, database) = database();
    let owned = tree(&database);
    let mut stale = database.begin().expect("begin stale writer");
    stale
        .put(owned, b"conflict", b"stale")
        .expect("stage stale write");
    commit_key(&database, owned, b"conflict");
    let head = database.commit_sequence().expect("head");
    let mut survivor = database.begin().expect("begin survivor");
    survivor
        .put(owned, b"survivor", b"value")
        .expect("stage survivor");

    // Enqueue both before publication so the published-state conflict
    // rejects the first member of the same wave as the valid write.
    let rejected = stage_commit(&mut stale).expect("enqueue stale writer");
    let committed = stage_commit(&mut survivor).expect("enqueue survivor");
    publish_with_lane(&database.runtime, lock_publish(&database.runtime)).expect("publish wave");
    assert!(matches!(
        &*rejected
            .recv()
            .expect("rejected outcome")
            .expect_err("conflict"),
        Error::WriteConflict { .. }
    ));
    let position = committed
        .recv()
        .expect("survivor outcome")
        .expect("survivor commits");
    let expected = CommitSeq::new(head.get() + 1);
    assert_eq!(position.csn, expected);
    assert_eq!(
        database.commit_sequence().expect("published head"),
        expected
    );
    let changes = database.read_changes(expected, 2).expect("change stream");
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].transaction, survivor.id());
    drop(stale);
    drop(survivor);
    database.close().expect("close");
    drop(database);

    let reopened = TransactionDatabase::open(directory.path().join("db"), Options::for_test())
        .expect("reopen");
    let mut read = reopened.begin().expect("begin read");
    assert_eq!(
        read.get(owned, b"survivor").expect("survivor visible"),
        Some(b"value".to_vec())
    );
    assert_eq!(
        read.get(owned, b"conflict").expect("winner visible"),
        Some(b"conflict".to_vec())
    );
    drop(read);
    commit_key(&reopened, owned, b"next");
    assert_eq!(
        reopened.commit_sequence().expect("next head"),
        CommitSeq::new(expected.get() + 1)
    );
    reopened.close().expect("close reopened");
}

#[test]
fn dropped_active_transaction_releases_pending_committer_slot() {
    let (directory, database) = database();
    let owned = tree(&database);

    // Begin a transaction and drop it without commit or abort: the
    // shape every wire-server probe (describe, grants, autocommit reads)
    // uses. Drop must release the pending-committer slot, or every
    // later commit leader sees a phantom pending committer and sleeps
    // out the full coalescing window.
    {
        let transaction = database.begin().expect("begin");
        let _ = transaction;
    }
    assert_eq!(
        database
            .runtime
            .pending_transactions
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "dropped active transaction leaked a pending-committer slot"
    );

    // Read-only commits never stage, but they hold a slot between begin
    // and commit; commit must release it too.
    {
        let mut transaction = database.begin().expect("begin read-only");
        let _ = transaction.get(owned, b"missing").expect("point read");
        transaction.commit().expect("commit read-only");
    }
    assert_eq!(
        database
            .runtime
            .pending_transactions
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "read-only commit leaked a pending-committer slot"
    );

    // And a writing commit releases its slot at stage time.
    {
        let mut transaction = database.begin().expect("begin writer");
        transaction.put(owned, b"k", b"v").expect("put");
        transaction.commit().expect("commit writer");
    }
    assert_eq!(
        database
            .runtime
            .pending_transactions
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "writing commit leaked a pending-committer slot"
    );

    drop(database);
    drop(directory);
}

#[test]
fn read_changes_reports_short_tail_below_head_as_corruption() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("db");
    let database = TransactionDatabase::create(&path, Options::for_test()).expect("create");
    let owned = tree(&database);
    commit_key(&database, owned, b"a");
    let head = database.snapshot_export().expect("export").csn;
    assert_eq!(head.get(), 3);

    // Simulate a publisher that advanced the head without its change
    // record becoming durable: delete the head's record through the
    // maintenance lane (no CSN, no map update) and reopen.
    {
        let mut db = database.runtime.db.lock().expect("database mutex");
        db.commit_maintenance_batch(&[BatchMutation::Delete {
            key: change_record_key(head),
        }])
        .expect("delete head record");
    }
    drop(database);

    let reopened = TransactionDatabase::open(&path, Options::for_test()).expect("reopen");
    assert_eq!(reopened.snapshot_export().expect("export").csn, head);

    // A full read ends below the head: the missing tail is corruption,
    // not a silent short read a consumer could checkpoint on.
    let error = reopened
        .read_changes(CommitSeq::new(1), usize::MAX)
        .expect_err("short tail below head");
    assert!(
        matches!(&error, Error::Corruption(message) if message.contains("below the head")),
        "unexpected error: {error:?}"
    );

    // Bounded reads that legitimately stop at the limit still succeed.
    let bounded = reopened
        .read_changes(CommitSeq::new(1), 1)
        .expect("bounded read stops at limit");
    assert_eq!(bounded.len(), 1);
    assert_eq!(bounded[0].commit, CommitSeq::new(1));

    // Reading from the missing position itself is also corruption: the
    // head published a record that does not exist.
    assert!(matches!(
        reopened.read_changes(head, usize::MAX),
        Err(Error::Corruption(_))
    ));
    reopened.close().expect("close");
}

#[test]
fn change_stream_reads_contiguous_history() {
    let (_directory, database) = database();
    let first = tree(&database);
    let second = tree(&database);
    commit_key(&database, first, b"a");
    let mut multi = database.begin().expect("begin");
    multi.put(first, b"b", b"b").expect("put");
    multi.put(second, b"c", b"c").expect("put");
    multi.commit().expect("commit");

    let head = database.snapshot_export().expect("export").csn;
    let changes = database
        .read_changes(CommitSeq::new(1), usize::MAX)
        .expect("read all");
    assert_eq!(changes.len(), head.get() as usize);
    for (position, change) in changes.iter().enumerate() {
        assert_eq!(change.commit.get(), (position + 1) as u64);
    }
    let last = changes.last().expect("non-empty").clone();
    assert_eq!(
        last.writes,
        BTreeSet::from([(first, b"b".to_vec()), (second, b"c".to_vec())])
    );

    // Bounded reads resume exactly where they stopped.
    let tail = database
        .read_changes(CommitSeq::new(head.get() - 1), 1)
        .expect("bounded read");
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].commit.get(), head.get() - 1);

    // Reads above the head are empty.
    assert!(
        database
            .read_changes(CommitSeq::new(head.get() + 1), 8)
            .expect("past head")
            .is_empty()
    );
}

#[test]
fn retention_lease_survives_reopen_and_pins_history() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("db");
    let database = TransactionDatabase::create(&path, Options::for_test()).expect("create");
    let owned = tree(&database);
    commit_key(&database, owned, b"a");
    commit_key(&database, owned, b"b");

    let lease = database
        .acquire_change_lease(b"cdc", CommitSeq::new(2))
        .expect("lease");
    assert_eq!(lease.floor().expect("floor"), CommitSeq::new(2));
    drop(lease);
    drop(database);

    // The lease is durable consumer state: it survives reopen even with
    // no live handles.
    let reopened = TransactionDatabase::open(&path, Options::for_test()).expect("reopen");
    assert_eq!(
        reopened.oldest_retained_change().expect("oldest"),
        Some(CommitSeq::new(1))
    );
    let reattached = reopened
        .acquire_change_lease(b"cdc", CommitSeq::new(1))
        .expect("reattach");
    assert_eq!(reattached.floor().expect("floor"), CommitSeq::new(2));
    assert_eq!(reattached.name(), b"cdc");

    // Advancing releases older records from the stream. The advance and
    // the prune are maintenance: neither adds a change record or consumes
    // a CSN, so the head stays at the last real commit.
    let head = reopened.snapshot_export().expect("export").csn;
    reattached.advance(head).expect("advance");
    let report = reopened.gc_changes().expect("gc");
    assert_eq!(report.changes_after, 1);
    assert_eq!(reopened.snapshot_export().expect("export").csn, head);
    assert_eq!(
        reopened.oldest_retained_change().expect("oldest"),
        Some(head)
    );
    assert!(matches!(
        reopened.read_changes(CommitSeq::new(1), 4),
        Err(Error::ChangesPruned { requested, oldest })
            if requested == CommitSeq::new(1) && oldest == head
    ));
    assert_eq!(reopened.read_changes(head, 4).expect("from floor").len(), 1);

    // Backwards advance is a no-op; release unpins everything.
    reattached.advance(CommitSeq::new(1)).expect("backwards");
    reattached.release().expect("release");
    // A released name can be re-acquired fresh, but it no longer
    // inherits the old floor: reads reaching pruned history report the gap.
    let fresh = reopened
        .acquire_change_lease(b"cdc", CommitSeq::new(1))
        .expect("re-acquire");
    assert_eq!(fresh.floor().expect("floor"), CommitSeq::new(1));
    assert!(matches!(
        reopened.read_changes(CommitSeq::new(1), 4),
        Err(Error::ChangesPruned { .. })
    ));
}

#[test]
fn maintenance_consumes_no_logical_changes() {
    let (_directory, database) = database();
    let owned = tree(&database);
    commit_key(&database, owned, b"a");
    let mut overwriter = database.begin().expect("begin");
    overwriter.put(owned, b"a", b"a2").expect("put");
    overwriter.commit().expect("commit");
    // Tree reservation consumed CSN 1; each logical commit adds one.
    let head = database.snapshot_export().expect("export").csn;
    assert_eq!(head.get(), 4);

    // Every maintenance operation leaves the logical stream untouched:
    // same head, same records, no fabricated system entries. The lease is
    // taken at CSN 1 so gc_changes prunes nothing and the full stream
    // stays readable.
    database.gc_versions().expect("gc versions");
    let lease = database
        .acquire_change_lease(b"cdc", CommitSeq::new(1))
        .expect("lease");
    database.gc_changes().expect("gc changes");
    database.gc_versions().expect("gc versions again");
    assert_eq!(database.snapshot_export().expect("export").csn, head);

    // The stream still reads contiguously from CSN 1 through the head.
    let changes = database
        .read_changes(CommitSeq::new(1), usize::MAX)
        .expect("stream stays contiguous across maintenance");
    assert_eq!(changes.len() as u64, head.get());
    for (position, change) in changes.iter().enumerate() {
        assert_eq!(change.commit.get(), (position + 1) as u64);
        assert_ne!(
            change.transaction.get(),
            0,
            "no fabricated system change records remain"
        );
    }

    // A logical commit after maintenance continues the stream exactly.
    commit_key(&database, owned, b"b");
    let next_head = database.snapshot_export().expect("export").csn;
    assert_eq!(next_head.get(), head.get() + 1);
    let tail = database
        .read_changes(next_head, 1)
        .expect("tail reads without gaps");
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].commit, next_head);

    lease.release().expect("release");
}

#[test]
fn gc_and_maintenance_survive_reopen_with_intact_stream() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("db");
    let database = TransactionDatabase::create(&path, Options::for_test()).expect("create");
    let owned = tree(&database);
    commit_key(&database, owned, b"a");
    let mut overwriter = database.begin().expect("begin");
    overwriter.put(owned, b"a", b"a2").expect("put");
    overwriter.commit().expect("commit");
    let head = database.snapshot_export().expect("export").csn;

    // The original corruption scenario: maintenance prunes history below
    // the lease floor, reopens, then one more commit. The surviving
    // stream must stay contiguous and readable from the floor.
    database.gc_versions().expect("gc versions");
    let _lease = database.acquire_change_lease(b"cdc", head).expect("lease");
    database.gc_changes().expect("gc changes");
    database.close().expect("close");

    let reopened = TransactionDatabase::open(&path, Options::for_test()).expect("reopen");
    assert_eq!(reopened.snapshot_export().expect("export").csn, head);
    let mut transaction = reopened.begin().expect("begin");
    transaction.put(owned, b"c", b"c").expect("put");
    let position = transaction.commit().expect("commit");
    assert_eq!(position.csn.get(), head.get() + 1);

    let changes = reopened
        .read_changes(head, usize::MAX)
        .expect("contiguous stream after reopen and maintenance");
    assert_eq!(changes.len() as u64, position.csn.get() - head.get() + 1);
    for (offset, change) in changes.iter().enumerate() {
        assert_eq!(change.commit.get(), head.get() + offset as u64);
    }
    assert!(matches!(
        reopened.read_changes(CommitSeq::new(1), usize::MAX),
        Err(Error::ChangesPruned { .. })
    ));
    // Old values stay readable: the maintenance rewrites preserved MVCC
    // history for the surviving watermark.
    let mut reader = reopened.begin().expect("begin reader");
    assert_eq!(
        reader.get(owned, b"a").expect("read after reopen"),
        Some(b"a2".to_vec())
    );
    reader.abort().expect("abort");
    reopened.close().expect("close");
}

#[test]
fn allocator_high_water_prevents_txn_id_reuse_after_pruning() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("db");
    let database = TransactionDatabase::create(&path, Options::for_test()).expect("create");
    let owned = tree(&database);
    // Several committed transactions create prunable status records.
    for key in [b"a", b"b", b"c"] {
        commit_key(&database, owned, key);
    }
    let head = database.snapshot_export().expect("export").csn;
    let last_txn = database
        .read_changes(head, 1)
        .expect("last change")
        .pop()
        .expect("non-empty")
        .transaction;

    // GC prunes every status record (no active snapshots pin them) and
    // persists the allocator high-water in the same maintenance batch.
    let report = database.gc_versions().expect("gc");
    assert!(report.statuses_pruned >= 3);
    database.close().expect("close");

    let reopened = TransactionDatabase::open(&path, Options::for_test()).expect("reopen");
    // The reopened allocator must start beyond every pruned identity:
    // a new transaction never reuses an issued TxnId.
    let mut transaction = reopened.begin().expect("begin");
    assert!(transaction.id().get() > last_txn.get());
    transaction.put(owned, b"d", b"d").expect("put");
    transaction.commit().expect("commit");
    reopened.close().expect("close");
}

#[test]
fn gc_changes_without_leases_retains_everything() {
    let (_directory, database) = database();
    let owned = tree(&database);
    commit_key(&database, owned, b"a");
    let report = database.gc_changes().expect("gc without leases");
    assert_eq!(report.floor, None);
    assert_eq!(report.changes_before, report.changes_after);
    assert!(database.oldest_retained_change().expect("oldest").is_some());
}

#[test]
fn multiple_leases_pin_to_the_minimum_floor() {
    let (_directory, database) = database();
    let owned = tree(&database);
    for key in [b"a", b"b", b"c"] {
        commit_key(&database, owned, key);
    }
    let slow = database
        .acquire_change_lease(b"slow", CommitSeq::new(1))
        .expect("slow");
    let fast = database
        .acquire_change_lease(b"fast", CommitSeq::new(1))
        .expect("fast");
    let head = database.snapshot_export().expect("export").csn;
    fast.advance(head).expect("fast advance");

    let report = database.gc_changes().expect("gc");
    assert_eq!(report.changes_after, report.changes_before);

    slow.advance(head).expect("slow advance");
    let pruned = database.gc_changes().expect("gc after both advance");
    assert!(pruned.changes_after < pruned.changes_before);
}

/// Key length for oversized/boundary change-record staging. Chosen so
/// one write costs `12 + key` change-record bytes while both leaf and
/// internal pages still split reliably at this separator size; larger
/// keys approach the internal-page entry budget and fail to split.
const OVERSIZED_KEY_LENGTH: usize = 900;

fn oversized_staging_shape() -> (usize, usize) {
    let per_write = 12 + OVERSIZED_KEY_LENGTH;
    let writes_for_limit = (MAX_CHANGE_RECORD_BYTES - 28) / per_write;
    (OVERSIZED_KEY_LENGTH, writes_for_limit)
}

#[test]
fn oversized_change_record_rejects_staging_without_state_mutation() {
    let (_directory, database) = database();
    let owned = tree(&database);
    commit_key(&database, owned, b"seed");
    let head_before = database.snapshot_export().expect("export").csn;

    // A transaction whose change record exceeds the record bound but
    // whose WAL footprint stays inside the admission budget used to
    // reach the serialized publisher and panic. Staging must return a
    // normal error instead.
    let (_, writes_for_limit) = oversized_staging_shape();
    let error = {
        let mut oversized = database.begin().expect("begin");
        for position in 0..(writes_for_limit + 1) {
            let mut key = vec![0u8; OVERSIZED_KEY_LENGTH];
            key[..8].copy_from_slice(&position.to_be_bytes());
            oversized.put(owned, &key, b"v").expect("put");
        }
        oversized.commit()
    }
    .expect_err("oversized change record");
    assert!(
        matches!(&error, Error::InvalidArgument(message) if message.contains("size limit")),
        "unexpected error: {error:?}"
    );

    // Rejection mutated nothing: the head is unchanged, committed data
    // is intact, and new transactions still work.
    let head_after = database.snapshot_export().expect("export").csn;
    assert_eq!(head_after, head_before);
    assert_eq!(
        database
            .read_changes(CommitSeq::new(1), usize::MAX)
            .expect("read changes")
            .len() as u64,
        head_before.get()
    );
    commit_key(&database, owned, b"after-reject");
    let head_next = database.snapshot_export().expect("export").csn;
    assert_eq!(head_next.get(), head_before.get() + 1);
    let mut read = database.begin().expect("read");
    assert_eq!(
        read.get(owned, b"after-reject").expect("read"),
        Some(b"after-reject".to_vec())
    );
    read.abort().expect("abort");
}

#[test]
fn boundary_sized_change_record_commits() {
    let (_directory, database) = database();
    let owned = tree(&database);

    // Largest write count whose change record still fits the bound.
    let (_, writes_for_limit) = oversized_staging_shape();
    let mut boundary = database.begin().expect("begin");
    for position in 0..writes_for_limit {
        let mut key = vec![0u8; OVERSIZED_KEY_LENGTH];
        key[..8].copy_from_slice(&position.to_be_bytes());
        boundary.put(owned, &key, b"v").expect("put");
    }
    let position = boundary.commit().expect("boundary commit");
    assert!(position.csn.get() >= 1);

    // The committed change record decodes back with the full write set.
    let changes = database
        .read_changes(CommitSeq::new(position.csn.get()), 1)
        .expect("read boundary change");
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].writes.len(), writes_for_limit);
}

#[test]
fn rejected_oversized_staging_keeps_conflict_indexes_exact() {
    let (_directory, database) = database();
    let owned = tree(&database);

    // Stage an oversized transaction but never let it commit: the error
    // surfaces at staging, so queued conflict indexes must not retain
    // its keys.
    let (key_length, writes_for_limit) = oversized_staging_shape();
    let oversized_result = {
        let mut oversized = database.begin().expect("begin");
        for position in 0..(writes_for_limit + 1) {
            let mut key = vec![0u8; key_length];
            key[..8].copy_from_slice(&position.to_be_bytes());
            oversized.put(owned, &key, b"v").expect("put");
        }
        oversized.commit()
    };
    assert!(oversized_result.is_err());

    // A later writer must win the same keys: the rejected staging left
    // no conflict-index residue.
    let mut probe = vec![0u8; key_length];
    probe[..8].copy_from_slice(&0u64.to_be_bytes());
    let mut winner = database.begin().expect("begin");
    winner.put(owned, &probe, b"w").expect("put");
    winner
        .commit()
        .expect("winner commits against rejected keys");
}

#[test]
fn gc_changes_respects_active_snapshot_watermark() {
    let (_directory, database) = database();
    let owned = tree(&database);
    commit_key(&database, owned, b"k");

    // A writing transaction registers a read range (scan), pinning an
    // old snapshot before a later commit lands inside that range. Its
    // commit-time re-validation walks durable change records in
    // (snapshot, current]: record `a` is inside the range, above the
    // snapshot, and below the lease floor taken afterwards.
    let mut range_reader = database.begin().expect("begin reader");
    range_reader.put(owned, b"z", b"z").expect("write");
    {
        let mut cursor = range_reader
            .cursor(owned, b"a", Some(b"m"))
            .expect("cursor registers range");
        while let Some(entry) = cursor.advance().expect("cursor advance") {
            let _ = entry;
        }
    }
    let snapshot = range_reader.snapshot();
    let mut writer = database.begin().expect("writer");
    writer.put(owned, b"a", b"a").expect("put inside range");
    let writer_commit = writer.commit().expect("commit inside range");
    assert!(writer_commit.csn.get() > snapshot.get());
    // One more commit outside the range so the lease floor taken at the
    // head sits strictly above the writer's record: under the old
    // single-floor pruning the writer's record would be deleted.
    commit_key(&database, owned, b"n");
    let head = database.snapshot_export().expect("export").csn;
    assert_eq!(head.get(), writer_commit.csn.get() + 1);

    // The lease floor alone would prune everything below the head;
    // the reader's older snapshot must hold the writer's record back.
    let lease = database.acquire_change_lease(b"cdc", head).expect("lease");
    let report = database.gc_changes().expect("gc");
    assert_eq!(report.floor, Some(snapshot));
    // Records above the snapshot survive; strictly older ones prune.
    assert_eq!(
        report.changes_after as u64,
        report.changes_before as u64 - snapshot.get() + 1
    );
    assert!(
        database
            .read_changes(writer_commit.csn, 1)
            .expect("record survives")
            .iter()
            .any(|change| change.commit == writer_commit.csn)
    );

    // The phantom is still detected at commit time: the surviving
    // record proves the range saw a concurrent write.
    assert!(matches!(
        range_reader.commit(),
        Err(Error::SerializationConflict { .. })
    ));
    lease.release().expect("release");
}

#[test]
fn gc_versions_respects_lease_watermark() {
    let (_directory, database) = database();
    let owned = tree(&database);
    let mut seed = database.begin().expect("seed");
    seed.put(owned, b"k", b"v1").expect("put v1");
    let seed_commit = seed.commit().expect("seed").csn;
    let mut overwriter = database.begin().expect("overwriter");
    overwriter.put(owned, b"k", b"v2").expect("put v2");
    overwriter.commit().expect("overwrite");

    // No active snapshots: the old watermark (None) cleared v1's undo
    // history. A CDC lease whose floor sits at the seed's commit must
    // retain it — the consumer resolves that record against the row
    // state visible at its floor, which is v1, i.e. the undo version.
    let lease = database
        .acquire_change_lease(b"cdc", seed_commit)
        .expect("lease");
    let report = database.gc_versions().expect("gc");
    assert_eq!(report.watermark, Some(seed_commit));
    assert_eq!(
        report.versions_after, 1,
        "lease floor must pin the seed's undo version"
    );

    // The pin holds across maintenance rounds and a reopen: the
    // surviving version resolves without corruption and a reader at
    // the head still sees the current value.
    let report = database.gc_versions().expect("gc again");
    assert_eq!(report.versions_after, 1);
    let mut verifier = database.begin().expect("verifier");
    assert_eq!(
        verifier.get(owned, b"k").expect("verifier reads head"),
        Some(b"v2".to_vec())
    );
    verifier.abort().expect("abort");
    lease.release().expect("release");

    // With the lease gone and no active snapshots, the next pass
    // reclaims the pinned history.
    let report = database.gc_versions().expect("final gc");
    assert_eq!(report.watermark, None);
    assert_eq!(report.versions_after, 0);
}

#[test]
fn gc_retains_boundary_version_for_exact_watermark_snapshot() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed");
    seed.put(tree, b"k", b"v1").expect("put v1");
    seed.commit().expect("seed");

    let mut old = database.begin().expect("old");
    assert_eq!(
        old.get(tree, b"k").expect("pre-gc read"),
        Some(b"v1".to_vec())
    );

    let mut newer = database.begin().expect("newer");
    newer.put(tree, b"k", b"v2").expect("put v2");
    newer.commit().expect("newer commit");

    database.gc_versions().expect("gc");
    assert_eq!(
        old.get(tree, b"k").expect("pinned read after gc"),
        Some(b"v1".to_vec())
    );
    old.abort().expect("abort");
}

#[test]
fn status_freeze_prunes_unreferenced_statuses_and_survives_reopen() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("db");
    let database = TransactionDatabase::create(&path, Options::for_test()).expect("create");
    let owned = tree(&database);
    for i in 0..3 {
        let mut transaction = database.begin().expect("begin");
        transaction
            .put(owned, format!("k{i}").as_bytes(), b"v")
            .expect("put");
        transaction.commit().expect("commit");
    }

    let report = database.gc_versions().expect("gc");
    assert!(report.statuses_pruned > 0, "statuses should prune");
    assert!(
        report.current_records_rewritten > 0,
        "records should freeze"
    );
    drop(database);

    // Frozen records resolve without their status entries; if freezing
    // missed any reference the reopened handle reports unknown
    // transactions instead of values.
    let reopened = TransactionDatabase::open(&path, Options::for_test()).expect("reopen");
    let mut reader = reopened.begin().expect("reader");
    for i in 0..3 {
        assert_eq!(
            reader.get(owned, format!("k{i}").as_bytes()).expect("read"),
            Some(b"v".to_vec())
        );
    }
    // Writes keep working across frozen history: the next before-image
    // carries the resolved CSN instead of an indirection.
    reader.put(owned, b"k0", b"v2").expect("overwrite");
    reader.commit().expect("commit over frozen history");
    let mut verifier = reopened.begin().expect("verifier");
    assert_eq!(
        verifier.get(owned, b"k0").expect("read v2"),
        Some(b"v2".to_vec())
    );
    verifier.commit().expect("verify commit");
}

#[test]
fn pinned_history_keeps_its_status_entries() {
    let (_directory, database) = database();
    let owned = tree(&database);
    let mut seed = database.begin().expect("seed");
    seed.put(owned, b"k", b"v1").expect("put");
    seed.commit().expect("seed");

    let mut old = database.begin().expect("old snapshot holder");
    let baseline = old.get(owned, b"k").expect("baseline read");
    assert_eq!(baseline, Some(b"v1".to_vec()));

    let mut newer = database.begin().expect("newer");
    newer.put(owned, b"k", b"v2").expect("put v2");
    newer.commit().expect("newer commit");

    let report = database.gc_versions().expect("gc with pinned snapshot");
    // Unpinned creators prune immediately; the invariant that matters is
    // that the pinned snapshot still resolves and its creator's status
    // entry survives until release.
    let _ = report.statuses_pruned;
    assert_eq!(
        old.get(owned, b"k").expect("pinned read after gc"),
        Some(b"v1".to_vec())
    );
    old.abort().expect("abort old");

    let released = database.gc_versions().expect("gc after release");
    let final_prune = database.gc_versions().expect("settle pass");
    assert!(
        report.statuses_pruned + released.statuses_pruned + final_prune.statuses_pruned > 0,
        "statuses prune once unpinned"
    );
}

#[test]
fn disjoint_writers_commit_from_one_snapshot() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut first = database.begin().expect("first begin");
    let mut second = database.begin().expect("second begin");
    first.put(tree, b"a", b"one").expect("first write");
    second.put(tree, b"b", b"two").expect("second write");
    assert_eq!(first.commit().expect("first commit").csn.get(), 3);
    assert_eq!(second.commit().expect("disjoint commit").csn.get(), 4);

    let mut reader = database.begin().expect("reader begin");
    assert_eq!(
        reader.get(tree, b"a").expect("read a"),
        Some(b"one".to_vec())
    );
    assert_eq!(
        reader.get(tree, b"b").expect("read b"),
        Some(b"two".to_vec())
    );
}

#[test]
fn same_key_conflict_is_atomic_and_retryable_as_abort() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut first = database.begin().expect("first begin");
    let mut second = database.begin().expect("second begin");
    first.put(tree, b"key", b"first").expect("first write");
    second.put(tree, b"key", b"second").expect("second write");
    second
        .put(tree, b"unrelated", b"must-not-publish")
        .expect("unrelated write");
    first.commit().expect("first commit");
    let outcome = second.commit();
    assert!(matches!(
        outcome,
        Err(Error::WriteConflict { tree: conflict_tree, ref key })
            if conflict_tree == tree && key == b"key"
    ));
    second.abort().expect("abort loser");

    let mut reader = database.begin().expect("reader begin");
    assert_eq!(
        reader.get(tree, b"key").expect("read winner"),
        Some(b"first".to_vec())
    );
    assert_eq!(
        reader.get(tree, b"unrelated").expect("read unrelated"),
        None
    );
}

#[test]
fn dropping_tree_conflicts_with_concurrent_key_writer() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"key", b"old").expect("seed write");
    seed.commit().expect("seed commit");
    drop(seed);

    let mut dropper = database.begin().expect("dropper begin");
    dropper.drop_tree(tree).expect("stage drop");
    let mut writer = database.begin().expect("writer begin");
    writer.put(tree, b"key", b"new").expect("writer write");
    writer.commit().expect("writer commit");
    assert!(matches!(
        dropper.commit(),
        Err(Error::TreeConflict(conflict_tree)) if conflict_tree == tree
    ));
    dropper.abort().expect("abort dropper");
}

#[test]
fn concurrent_threads_commit_disjoint_writes() {
    let (_directory, database) = database();
    let database = Arc::new(database);
    let tree = tree(&database);
    let first_database = Arc::clone(&database);
    let first = std::thread::spawn(move || {
        let mut transaction = first_database.begin().expect("first begin");
        transaction.put(tree, b"a", b"one").expect("first write");
        transaction.commit().expect("first commit");
    });
    let second_database = Arc::clone(&database);
    let second = std::thread::spawn(move || {
        let mut transaction = second_database.begin().expect("second begin");
        transaction.put(tree, b"b", b"two").expect("second write");
        transaction.commit().expect("second commit");
    });
    first.join().expect("first thread");
    second.join().expect("second thread");
    let mut reader = database.begin().expect("reader");
    assert_eq!(
        reader.get(tree, b"a").expect("read a"),
        Some(b"one".to_vec())
    );
    assert_eq!(
        reader.get(tree, b"b").expect("read b"),
        Some(b"two".to_vec())
    );
}

#[test]
fn group_commit_keeps_stream_contiguous_under_load() {
    const THREADS: usize = 4;
    const WRITES_PER_THREAD: usize = 25;
    let (_directory, database) = database();
    let database = Arc::new(database);
    let shared = tree(&database);
    let mut handles = Vec::new();
    for worker in 0..THREADS {
        let database = Arc::clone(&database);
        handles.push(std::thread::spawn(move || {
            for step in 0..WRITES_PER_THREAD {
                let key = format!("w{worker}-k{step}");
                let mut transaction = database.begin().expect("begin");
                transaction
                    .put(shared, key.as_bytes(), key.as_bytes())
                    .expect("put");
                transaction.commit().expect("commit");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("worker");
    }

    let head = database.snapshot_export().expect("export").csn;
    let changes = database
        .read_changes(CommitSeq::new(1), usize::MAX)
        .expect("stream read");
    assert_eq!(changes.len(), head.get() as usize);
    for (position, change) in changes.iter().enumerate() {
        assert_eq!(change.commit.get(), (position + 1) as u64);
    }
    // Every write is visible exactly once at the final state.
    let mut reader = database.begin().expect("reader");
    for worker in 0..THREADS {
        for step in 0..WRITES_PER_THREAD {
            let key = format!("w{worker}-k{step}");
            assert_eq!(
                reader.get(shared, key.as_bytes()).expect("read"),
                Some(key.clone().into_bytes())
            );
        }
    }
}

#[test]
fn concurrent_conflicting_writers_pick_one_winner() {
    let (_directory, database) = database();
    let database = Arc::new(database);
    let shared = tree(&database);
    let mut seed = database.begin().expect("seed");
    seed.put(shared, b"contested", b"base").expect("seed put");
    seed.commit().expect("seed commit");

    let mut handles = Vec::new();
    // Every transaction must begin before any commits so all four share
    // one snapshot; only then does first-committer-wins allow one winner.
    let barrier = Arc::new(std::sync::Barrier::new(4));
    for worker in 0..4usize {
        let database = Arc::clone(&database);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let mut transaction = database.begin().expect("begin");
            barrier.wait();
            transaction
                .put(shared, b"contested", format!("w{worker}").as_bytes())
                .expect("put");
            transaction.commit()
        }));
    }
    let winners = handles
        .into_iter()
        .filter_map(|handle| handle.join().expect("join").ok())
        .count();
    assert_eq!(winners, 1, "first-committer-wins allows exactly one winner");
    let mut reader = database.begin().expect("reader");
    assert!(reader.get(shared, b"contested").expect("read").is_some());
}

#[test]
fn drained_publication_keeps_conflicts_visible_until_install() {
    let (_directory, database) = database();
    let shared = tree(&database);
    let mut seed = database.begin().expect("seed");
    seed.put(shared, b"contested", b"base").expect("seed put");
    seed.commit().expect("seed commit");

    let mut first = database.begin().expect("first");
    let mut second = database.begin().expect("second");
    first
        .put(shared, b"contested", b"first")
        .expect("first put");
    second
        .put(shared, b"contested", b"second")
        .expect("second put");

    let _first_outcome = stage_commit(&mut first).expect("stage first");
    let _lane = lock_publish(&first.runtime);
    let drained = take_staged(&first.runtime);
    let conflict = match stage_commit(&mut second) {
        Ok(_) => panic!("drained writer disappeared from conflict indexes"),
        Err(error) => error,
    };
    assert!(matches!(
        conflict,
        Error::WriteConflict { tree, ref key }
            if tree == shared && key.as_slice() == b"contested"
    ));
    drop(drained);
}

#[test]
fn multi_tree_commit_and_snapshot_visibility_are_atomic() {
    let (_directory, database) = database();
    let first_tree = tree(&database);
    let mut create = database.begin().expect("create second tree");
    let second_tree = create.create_tree().expect("second tree");
    create.commit().expect("second tree commit");

    let mut old = database.begin().expect("old snapshot");
    let mut writer = database.begin().expect("writer");
    writer.put(first_tree, b"one", b"1").expect("first write");
    writer.put(second_tree, b"two", b"2").expect("second write");
    writer.commit().expect("atomic commit");
    assert_eq!(old.get(first_tree, b"one").expect("old first"), None);
    assert_eq!(old.get(second_tree, b"two").expect("old second"), None);

    let mut current = database.begin().expect("current snapshot");
    assert_eq!(
        current.get(first_tree, b"one").expect("first"),
        Some(b"1".to_vec())
    );
    assert_eq!(
        current.get(second_tree, b"two").expect("second"),
        Some(b"2".to_vec())
    );
}

#[test]
fn read_only_commit_does_not_advance_frontier() {
    let (_directory, database) = database();
    let transaction = database.begin().expect("begin");
    let snapshot = transaction.snapshot();
    let mut transaction = transaction;
    assert!(transaction.is_read_only());
    assert_eq!(transaction.commit().expect("commit").csn, snapshot);
    assert_eq!(database.commit_sequence().expect("head"), snapshot);
}

#[test]
fn commit_position_reports_csn_and_lsn_across_reopen() {
    let (directory, database) = database();
    let initial = database.commit_position().expect("initial position");
    assert_eq!(initial.csn, CommitSeq::new(0));
    assert_eq!(initial.lsn, crate::storage::format::Lsn::new(0));

    let mut transaction = database.begin().expect("begin");
    let tree = transaction.create_tree().expect("create tree");
    transaction.put(tree, b"key", b"value").expect("write");
    let position = transaction.commit().expect("commit position");
    drop(transaction);
    assert!(position.csn > initial.csn);
    assert!(position.lsn > initial.lsn);
    assert_eq!(database.commit_position().expect("head"), position);
    database.close().expect("close");

    let reopened = TransactionDatabase::open(directory.path().join("db"), Options::for_test())
        .expect("reopen");
    assert_eq!(
        reopened.commit_position().expect("reopened position"),
        position
    );
    reopened.close().expect("close reopened");
}

#[test]
fn scan_merges_staged_writes_in_order() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"a", b"old").expect("seed a");
    seed.put(tree, b"c", b"old").expect("seed c");
    seed.commit().expect("seed commit");
    let mut transaction = database.begin().expect("begin");
    transaction.put(tree, b"b", b"new").expect("stage b");
    transaction.delete(tree, b"c").expect("delete c");
    assert_eq!(
        transaction.scan(tree, b"a", None, 10).expect("scan"),
        vec![
            (b"a".to_vec(), b"old".to_vec()),
            (b"b".to_vec(), b"new".to_vec())
        ]
    );
    transaction.abort().expect("abort");
}

#[test]
fn tree_lifecycle_and_ids_survive_reopen() {
    let (directory, database) = database();
    let first = tree(&database);
    let mut drop = database.begin().expect("drop begin");
    drop.drop_tree(first).expect("drop tree");
    drop.commit().expect("drop commit");
    let mut burned = database.begin().expect("burn begin");
    let burned_tree = burned.create_tree().expect("burn tree");
    burned.drop_tree(burned_tree).expect("drop burned tree");
    burned.commit().expect("burn commit");
    let mut aborted = database.begin().expect("abort begin");
    let aborted_tree = aborted.create_tree().expect("aborted tree");
    aborted.abort().expect("abort tree");
    std::mem::drop(drop);
    std::mem::drop(burned);
    std::mem::drop(aborted);
    database.close().expect("close");

    let reopened = TransactionDatabase::open(directory.path().join("db"), Options::for_test())
        .expect("reopen");
    let mut transaction = reopened.begin().expect("reopened begin");
    assert_eq!(
        transaction.list_trees().expect("list trees"),
        Vec::<TreeId>::new()
    );
    assert!(
        matches!(transaction.get(first, b"key"), Err(Error::TreeNotFound(tree)) if tree == first)
    );
    let next = transaction.create_tree().expect("next tree");
    assert!(next > burned_tree);
    assert!(next > aborted_tree);
    transaction.abort().expect("abort");
    std::mem::drop(transaction);
    reopened.close().expect("close reopened");
}

#[test]
fn committed_versions_survive_update_and_reopen() {
    let (directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"key", b"old").expect("seed write");
    seed.commit().expect("seed commit");
    drop(seed);

    let mut old = database.begin().expect("old snapshot");
    let mut writer = database.begin().expect("writer");
    writer.put(tree, b"key", b"new").expect("new write");
    writer.commit().expect("new commit");
    assert_eq!(
        old.get(tree, b"key").expect("old value"),
        Some(b"old".to_vec())
    );
    drop(old);
    drop(writer);
    database.close().expect("close");

    let mut raw = DB::open(directory.path().join("db"), Options::for_test()).expect("raw open");
    let bytes = raw
        .get(&tree_key(tree, b"key"))
        .expect("raw value")
        .expect("record");
    let current = decode_current(Some(&bytes)).expect("decode current");
    assert_eq!(current.commit, CommitSeq::new(0));
    assert_eq!(current.value, Some(b"new".to_vec()));
    let status_bytes = raw
        .get(&status_record_key(current.transaction))
        .expect("status value")
        .expect("status record");
    assert_eq!(
        decode_status(&status_bytes).expect("decode status"),
        CommitSeq::new(4)
    );
    let mut version_store = VersionStore::open(
        directory.path().join("db").join(VERSION_STORE_FILE),
        Default::default(),
    )
    .expect("open version store");
    let previous = version_store
        .get(current.undo_head.expect("undo head"))
        .expect("previous version");
    assert_eq!(previous.value, Some(b"old".to_vec()));
    let version_path = directory.path().join("db").join(VERSION_STORE_FILE);
    assert!(
        version_path.is_file(),
        "version store missing before raw close"
    );
    raw.close().expect("raw close");
    assert!(
        version_path.is_file(),
        "version store missing after raw close"
    );

    let reopened = TransactionDatabase::open(directory.path().join("db"), Options::for_test())
        .expect("reopen");
    let mut current = reopened.begin().expect("current snapshot");
    assert_eq!(
        current.get(tree, b"key").expect("current value"),
        Some(b"new".to_vec())
    );
    current.abort().expect("abort");
    drop(current);
    reopened.close().expect("reopened close");
}

#[test]
fn active_transactions_block_close_until_dropped() {
    let (_directory, database) = database();
    let transaction = database.begin().expect("begin");
    assert!(matches!(
        database.close(),
        Err(Error::InvalidArgument(message)) if message.contains("transactions are active")
    ));
    drop(transaction);
    database.close().expect("close after drop");
}

#[test]
fn version_gc_respects_active_snapshot_then_reclaims_history() {
    let (directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"key", b"old").expect("seed write");
    seed.commit().expect("seed commit");

    let mut old = database.begin().expect("old begin");
    let mut writer = database.begin().expect("writer begin");
    writer.put(tree, b"key", b"new").expect("new write");
    writer.commit().expect("writer commit");
    assert_eq!(
        old.get(tree, b"key").expect("old read"),
        Some(b"old".to_vec())
    );

    let retained = database.gc_versions().expect("retain old history");
    assert_eq!(retained.watermark, Some(old.snapshot()));
    assert!(retained.versions_after > 0);
    assert_eq!(
        old.get(tree, b"key").expect("old read after GC"),
        Some(b"old".to_vec())
    );
    old.abort().expect("release old");
    drop(old);

    let reclaimed = database.gc_versions().expect("reclaim history");
    assert_eq!(reclaimed.watermark, None);
    assert_eq!(reclaimed.versions_after, 0);
    drop(writer);
    database.close().expect("close");

    let reopened = TransactionDatabase::open(directory.path().join("db"), Options::for_test())
        .expect("reopen");
    let mut current = reopened.begin().expect("current begin");
    assert_eq!(
        current.get(tree, b"key").expect("current read"),
        Some(b"new".to_vec())
    );
    current.abort().expect("abort current");
    drop(current);
    reopened.close().expect("close reopened");
}

#[test]
fn cursor_merges_storage_and_staged_writes_in_order() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    for key in [b"b", b"d"] {
        seed.put(tree, key, key).expect("seed write");
    }
    seed.commit().expect("seed commit");

    let mut writer = database.begin().expect("writer begin");
    writer.put(tree, b"a", b"a").expect("stage a");
    writer.put(tree, b"c", b"c").expect("stage c");
    writer.delete(tree, b"d").expect("stage delete d");
    let mut cursor = writer.cursor(tree, b"", None).expect("open cursor");
    let mut collected = Vec::new();
    for entry in &mut cursor {
        collected.push(entry.expect("cursor step"));
    }
    assert_eq!(
        collected,
        vec![
            (b"a".to_vec(), b"a".to_vec()),
            (b"b".to_vec(), b"b".to_vec()),
            (b"c".to_vec(), b"c".to_vec()),
        ]
    );
    // Exhausted cursors stay exhausted.
    assert!(cursor.next().is_none());
}

#[test]
fn cursor_respects_bounds_and_created_trees() {
    let (_directory, database) = database();
    let mut creator = database.begin().expect("creator begin");
    let fresh = creator.create_tree().expect("create tree");
    creator.put(fresh, b"k1", b"v1").expect("write");
    creator.commit().expect("commit");

    let mut reader = database.begin().expect("reader begin");
    let mut bounded = reader
        .cursor(fresh, b"k0", Some(b"k1"))
        .expect("bounded cursor");
    assert!(bounded.next().is_none());
    drop(bounded);

    let mut unbounded = reader.cursor(fresh, b"", None).expect("unbounded cursor");
    assert_eq!(
        unbounded.next().expect("first entry").ok(),
        Some((b"k1".to_vec(), b"v1".to_vec()))
    );
    assert!(unbounded.next().is_none());
}

#[test]
fn cursor_holds_fixed_snapshot_under_concurrent_commit() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"before", b"1").expect("seed write");
    seed.commit().expect("seed commit");

    let mut reader = database.begin().expect("reader begin");
    let mut cursor = reader.cursor(tree, b"", None).expect("open cursor");
    assert_eq!(
        cursor.next().expect("snapshot entry").ok(),
        Some((b"before".to_vec(), b"1".to_vec()))
    );

    let mut concurrent = database.begin().expect("concurrent begin");
    concurrent
        .put(tree, b"after", b"2")
        .expect("concurrent write");
    concurrent.commit().expect("concurrent commit");

    // The fixed snapshot never exposes the later commit.
    assert!(cursor.next().is_none());
}

#[test]
fn cursor_range_dependency_rejects_phantom_insert() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"a", b"1").expect("seed write");
    seed.commit().expect("seed commit");

    let mut scanner = database.begin().expect("scanner begin");
    {
        let mut cursor = scanner
            .cursor(tree, b"a", Some(b"z"))
            .expect("range cursor");
        assert!(cursor.next().expect("scan seeded range").is_ok());
    }

    let mut inserter = database.begin().expect("inserter begin");
    inserter.put(tree, b"m", b"phantom").expect("phantom write");
    inserter.commit().expect("phantom commit");

    // The read range was registered, so the upgrade-to-write commit must
    // detect the phantom even though the transaction wrote a different key.
    scanner.put(tree, b"a", b"updated").expect("scanner write");
    assert!(matches!(
        scanner.commit(),
        Err(Error::SerializationConflict { .. })
    ));
}

#[test]
fn write_outside_cursor_range_commits_cleanly() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"in-range", b"1").expect("seed write");
    seed.commit().expect("seed commit");

    let mut scanner = database.begin().expect("scanner begin");
    {
        let mut cursor = scanner
            .cursor(tree, b"a", Some(b"m"))
            .expect("range cursor");
        while cursor.next().is_some() {}
    }

    let mut inserter = database.begin().expect("inserter begin");
    inserter
        .put(tree, b"z-outside", b"2")
        .expect("outside write");
    inserter.commit().expect("outside commit");

    scanner.put(tree, b"in-range", b"updated").expect("write");
    scanner
        .commit()
        .expect("writes outside the range do not conflict");
}

#[test]
fn version_gc_failure_fences_and_reopens_safely() {
    let (directory, database) = database();
    let tree = tree(&database);
    let mut transaction = database.begin().expect("begin");
    transaction.put(tree, b"key", b"value").expect("write");
    transaction.commit().expect("commit");
    drop(transaction);

    crate::mvcc::fail_next_compaction_rename();
    assert!(database.gc_versions().is_err());
    assert!(matches!(
        database.begin(),
        Err(Error::NeedsRecovery(message)) if message.contains("fenced")
    ));
    drop(database);

    let reopened = TransactionDatabase::open(directory.path().join("db"), Options::for_test())
        .expect("reopen after gc failure");
    let mut current = reopened.begin().expect("begin after reopen");
    assert_eq!(
        current.get(tree, b"key").expect("read after reopen"),
        Some(b"value".to_vec())
    );
    current.abort().expect("abort");
    drop(current);
    reopened.close().expect("close reopened");
}

#[test]
fn point_read_write_skew_is_certified() {
    let (_directory, database) = database();
    let tree = tree(&database);
    // Seed the two rows the classic write-skew pattern reads.
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"a", b"on").expect("seed a");
    seed.put(tree, b"b", b"on").expect("seed b");
    seed.commit().expect("seed commit");

    // Two concurrent transactions on one snapshot: each reads the
    // other's write target, then writes its own read target.
    let mut first = database.begin().expect("first begin");
    let mut second = database.begin().expect("second begin");
    assert_eq!(
        first.get(tree, b"b").expect("first reads b"),
        Some(b"on".to_vec())
    );
    assert_eq!(
        second.get(tree, b"a").expect("second reads a"),
        Some(b"on".to_vec())
    );
    first.put(tree, b"a", b"off").expect("first writes a");
    second.put(tree, b"b", b"off").expect("second writes b");

    first.commit().expect("first commits");
    // Whichever order the commits race in, the second transaction's
    // registered point read on the key the first one overwrote must
    // fail its commit: serializability forbids both surviving.
    let outcome = second.commit();
    assert!(
        matches!(outcome, Err(Error::SerializationConflict { .. })),
        "write skew must be certified, got {outcome:?}"
    );
    second.abort().expect("abort skew loser");
    database.close().expect("close");
}

#[test]
fn point_read_anti_dependency_survives_across_waves() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"k", b"v0").expect("seed");
    seed.commit().expect("seed commit");

    // The reader starts before the writer's commit lands, then the
    // writer publishes; the reader's later write must fail on the
    // stale point read even though no queue overlay is involved.
    let mut reader = database.begin().expect("reader begin");
    let mut writer = database.begin().expect("writer begin");
    writer.put(tree, b"k", b"v1").expect("writer stages");
    writer.commit().expect("writer commits");
    assert_eq!(
        reader.get(tree, b"k").expect("reader reads k"),
        Some(b"v0".to_vec())
    );
    reader
        .put(tree, b"other", b"x")
        .expect("reader stages write");
    let outcome = reader.commit();
    assert!(
        matches!(outcome, Err(Error::SerializationConflict { .. })),
        "stale point read must abort, got {outcome:?}"
    );
    reader.abort().expect("abort stale reader");
    database.close().expect("close");
}

#[test]
fn scan_then_write_conflicts_with_concurrent_insert_in_range() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"a", b"1").expect("seed a");
    seed.commit().expect("seed commit");

    // Scanner reads an unbounded range; a concurrent insert inside it
    // must fail the scanner's commit when the scanner also writes.
    let mut scanner = database.begin().expect("scanner begin");
    let mut inserter = database.begin().expect("inserter begin");
    let scanned = scanner.scan(tree, &[], None, usize::MAX).expect("scan");
    assert_eq!(scanned.len(), 1);
    inserter.put(tree, b"b", b"2").expect("inserter writes");
    inserter.commit().expect("inserter commits");
    scanner.put(tree, b"c", b"3").expect("scanner stages write");
    let outcome = scanner.commit();
    assert!(
        matches!(outcome, Err(Error::SerializationConflict { .. })),
        "phantom insert under a scan must abort, got {outcome:?}"
    );
    scanner.abort().expect("abort scanner");
    database.close().expect("close");
}

#[test]
fn scan_without_write_never_conflicts() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut seed = database.begin().expect("seed begin");
    seed.put(tree, b"a", b"1").expect("seed");
    seed.commit().expect("seed commit");

    // Read-only transactions always commit, even when concurrent
    // writers change every key the reader scanned.
    let mut reader = database.begin().expect("reader begin");
    let scanned = reader.scan(tree, &[], None, usize::MAX).expect("scan");
    assert_eq!(scanned.len(), 1);
    let mut writer = database.begin().expect("writer begin");
    writer.put(tree, b"a", b"2").expect("writer overwrites");
    writer.commit().expect("writer commits");
    reader.commit().expect("read-only commit succeeds");
    database.close().expect("close");
}

#[test]
fn point_read_still_sees_own_staged_write() {
    let (_directory, database) = database();
    let tree = tree(&database);
    let mut txn = database.begin().expect("begin");
    txn.put(tree, b"k", b"staged").expect("stage write");
    // The own-write read must both register (for anti-dependency)
    // and return the staged value without a storage round-trip.
    assert_eq!(
        txn.get(tree, b"k").expect("own staged read"),
        Some(b"staged".to_vec())
    );
    txn.commit().expect("commit after own-write read");
    database.close().expect("close");
}

#[test]
fn snapshot_watermark_releases_when_transaction_finishes() {
    let (_directory, database) = database();
    let mut first = database.begin().expect("first begin");
    let second = database.begin().expect("second begin");
    assert_eq!(
        database.oldest_active_snapshot().expect("oldest snapshot"),
        Some(first.snapshot())
    );
    first.commit().expect("first commit");
    assert_eq!(
        database.oldest_active_snapshot().expect("oldest snapshot"),
        Some(second.snapshot())
    );
    drop(second);
    assert_eq!(
        database.oldest_active_snapshot().expect("oldest snapshot"),
        None
    );
    database.close().expect("close");
}
