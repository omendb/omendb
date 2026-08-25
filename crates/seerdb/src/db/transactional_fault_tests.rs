//! Fault qualification for the transactional MVCC commit decision.
//!
//! These tests drive `TransactionDatabase` commits through injected WAL and
//! MVCC version-store failures and require atomic outcomes: a failed commit
//! publishes nothing or everything, an uncertain publication fences new work
//! until reopen, and orphaned version frames never become visible.

use super::faults::FAIL_NEXT_META_LOG_SYNC;
use super::faults::FAIL_NEXT_WAL_WRITE;
use crate::db::Options;
use crate::error::Error;
use crate::storage::format::TreeId;
use crate::transactional::TransactionDatabase;
use tempfile::tempdir;

fn database(name: &str) -> (tempfile::TempDir, TransactionDatabase) {
    let directory = tempdir().expect("temporary directory");
    let database = TransactionDatabase::create(directory.path().join(name), Options::for_test())
        .expect("create transaction database");
    (directory, database)
}

fn seeded_tree(database: &TransactionDatabase) -> TreeSeed {
    let mut setup = database.begin().expect("setup begin");
    let tree = setup.create_tree().expect("create tree");
    setup.put(tree, b"key", b"old").expect("seed write");
    let position = setup.commit().expect("seed commit");
    TreeSeed {
        tree,
        csn: position.csn.get(),
    }
}

struct TreeSeed {
    tree: TreeId,
    csn: u64,
}

#[test]
fn wal_write_failure_publishes_no_partial_transaction() {
    let (_directory, database) = database("wal-write-failure");
    let seed = seeded_tree(&database);

    FAIL_NEXT_WAL_WRITE.with(|failure| failure.set(true));
    let mut writer = database.begin().expect("writer begin");
    writer.put(seed.tree, b"key", b"new").expect("staged write");
    assert!(matches!(writer.commit(), Err(Error::NeedsRecovery(_))));
    drop(writer);

    // The uncertain outcome fences the handle; no new snapshot may observe a
    // half-published transaction.
    assert!(matches!(
        database.begin(),
        Err(Error::NeedsRecovery(message)) if message.contains("fenced")
    ));
    drop(database);

    let reopened = TransactionDatabase::open(
        _directory.path().join("wal-write-failure"),
        Options::for_test(),
    )
    .expect("reopen after failed publication");
    let mut reader = reopened.begin().expect("reader begin after reopen");
    assert_eq!(reader.list_trees().expect("trees"), vec![seed.tree]);
    assert_eq!(
        reader.get(seed.tree, b"key").expect("read after reopen"),
        Some(b"old".to_vec())
    );
    assert_eq!(
        reopened
            .commit_position()
            .expect("commit position")
            .csn
            .get(),
        seed.csn
    );
    reader.abort().expect("abort reader");
    reopened.close().expect("close reopened");
}

#[test]
fn wal_sync_failure_fences_until_reopen_resolves_outcome() {
    let (_directory, database) = database("wal-sync-failure");
    let seed = seeded_tree(&database);

    FAIL_NEXT_META_LOG_SYNC.with(|failure| failure.set(true));
    let mut writer = database.begin().expect("writer begin");
    writer.put(seed.tree, b"key", b"new").expect("staged write");
    assert!(matches!(writer.commit(), Err(Error::NeedsRecovery(_))));
    drop(writer);
    assert!(database.begin().is_err());
    drop(database);

    let reopened = TransactionDatabase::open(
        _directory.path().join("wal-sync-failure"),
        Options::for_test(),
    )
    .expect("reopen after sync failure");
    let mut reader = reopened.begin().expect("reader begin after reopen");
    let value = reader.get(seed.tree, b"key").expect("read after reopen");
    let committed = reopened
        .commit_position()
        .expect("commit position")
        .csn
        .get();
    reader.abort().expect("abort reader");

    // Recovery must resolve to exactly one outcome, with value and commit
    // position in agreement.
    if value == Some(b"new".to_vec()) {
        assert_eq!(committed, seed.csn + 1, "published write must advance CSN");
    } else {
        assert_eq!(value, Some(b"old".to_vec()));
        assert_eq!(committed, seed.csn, "discarded write must not advance CSN");
    }
    reopened.close().expect("close reopened");
}

#[test]
fn orphaned_versions_from_failed_publication_are_reclaimed_by_gc() {
    let (_directory, database) = database("orphan-versions");
    let seed = seeded_tree(&database);

    // The version store is synced before the commit decision, so the failed
    // publication leaves durable but unreachable before-image frames.
    FAIL_NEXT_WAL_WRITE.with(|failure| failure.set(true));
    let mut writer = database.begin().expect("writer begin");
    writer.put(seed.tree, b"key", b"new").expect("staged write");
    assert!(writer.commit().is_err());
    drop(writer);
    drop(database);

    let reopened = TransactionDatabase::open(
        _directory.path().join("orphan-versions"),
        Options::for_test(),
    )
    .expect("reopen with orphaned versions");
    let report = reopened.gc_versions().expect("gc after reopen");
    assert!(
        report.versions_after < report.versions_before,
        "unreachable versions must be reclaimed: {report:?}"
    );

    let mut reader = reopened.begin().expect("reader begin");
    assert_eq!(
        reader.get(seed.tree, b"key").expect("read after gc"),
        Some(b"old".to_vec())
    );
    reader.abort().expect("abort reader");
    reopened.close().expect("close reopened");
}

#[test]
fn version_sync_failure_aborts_commit_without_fencing() {
    let (_directory, database) = database("version-sync-failure");
    let seed = seeded_tree(&database);

    crate::mvcc::fail_next_version_sync();
    let mut writer = database.begin().expect("writer begin");
    writer.put(seed.tree, b"key", b"new").expect("staged write");
    let error = writer
        .commit()
        .expect_err("failed version sync must abort the commit");
    assert!(!matches!(error, Error::NeedsRecovery(_)));
    writer.abort().expect("abort after pre-publication failure");

    // Nothing was published and the engine never entered its commit decision,
    // so the handle stays usable without a reopen.
    let mut retry = database.begin().expect("begin after failed sync");
    assert_eq!(
        retry.get(seed.tree, b"key").expect("read before retry"),
        Some(b"old".to_vec())
    );
    retry.put(seed.tree, b"key", b"new").expect("retry write");
    let position = retry.commit().expect("retry commit");
    assert_eq!(position.csn.get(), seed.csn + 1);

    let mut reader = database.begin().expect("reader begin");
    assert_eq!(
        reader.get(seed.tree, b"key").expect("read committed retry"),
        Some(b"new".to_vec())
    );
    reader.abort().expect("abort reader");
    database.close().expect("close database");
}

#[test]
fn status_records_replay_visibility_and_gc_after_reopen() {
    let (_directory, database) = database("status-replay");
    let seed = seeded_tree(&database);

    // Drop without close() to model a process crash after durable commits.
    drop(database);

    let reopened =
        TransactionDatabase::open(_directory.path().join("status-replay"), Options::for_test())
            .expect("reopen after crash");
    assert_eq!(
        reopened
            .commit_position()
            .expect("replayed position")
            .csn
            .get(),
        seed.csn
    );

    // Visibility resolves through replayed TxnId -> CSN status records.
    let mut reader = reopened.begin().expect("reader begin");
    assert_eq!(
        reader.get(seed.tree, b"key").expect("read replayed value"),
        Some(b"old".to_vec())
    );
    reader.abort().expect("abort reader");

    // GC also resolves status records; with no active snapshots it reclaims
    // every before-image.
    let report = reopened.gc_versions().expect("gc after replay");
    assert_eq!(report.versions_after, 0);
    reopened.close().expect("close reopened");
}
