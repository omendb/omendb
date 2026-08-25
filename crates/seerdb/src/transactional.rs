//! Capability-rich transactional ordered-KV API.
//!
//! This module is the transactional vertical slice above the durable `DB`
//! storage engine. It provides first-class tree identities, fixed snapshots,
//! atomic multi-tree batches, and snapshot-isolation write-conflict checking.
//! The physical engine remains the single publication authority; this module
//! owns transaction coordination, transaction-status resolution, and
//! append-oriented logical version history. The current slice uses the durable
//! commit sequence for visibility and retains the conflict record for
//! restartable change history.
//!
//! The API deliberately does not expose a backend matrix or a fake plugin
//! trait. OmenDB can call this capability-rich surface directly while the
//! server/session layer is built above it.

use crate::db::{BatchMutation, DB, Options};
use crate::error::{Error, Result};
use crate::mvcc::{
    CurrentRecord, VersionStore, decode_current, encode_current, resolve_commit, visible_current,
};
use crate::storage::format::{CommitId, CommitPosition, CommitSeq, TreeId, TxnId};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const TREE_RECORD_PREFIX: &[u8] = b"\x00seerdb/tree/";
const STATUS_RECORD_PREFIX: &[u8] = b"\x00seerdb/status/";
const CHANGE_RECORD_PREFIX: &[u8] = b"\x00seerdb/change/";
const TREE_DATA_PREFIX: u8 = 0x01;
const TREE_LIVE: &[u8] = b"live";
const TREE_DROPPED: &[u8] = b"dropped";
const TREE_RESERVED: &[u8] = b"reserved";
const CHANGE_MAGIC: &[u8; 4] = b"SCM1";
const STATUS_MAGIC: &[u8; 4] = b"SST1";
const MAX_CHANGE_RECORD_BYTES: usize = 16 * 1024 * 1024;
const VERSION_STORE_FILE: &str = "seerdb.mvcc";

/// Lifecycle state of one transaction.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TransactionState {
    /// The transaction may read, stage, commit, or abort.
    Active,
    /// The transaction committed and returned the visible commit sequence.
    Committed { commit: CommitSeq },
    /// The transaction was explicitly aborted or dropped without publishing.
    Aborted,
    /// Publication may have reached durable media; reopen is required before
    /// deciding whether the logical batch became visible.
    RecoveryRequired { commit: CommitSeq },
}

/// Result of a bounded logical MVCC version-store compaction.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct VersionGcReport {
    /// Oldest active snapshot that constrained retention, if any.
    pub watermark: Option<CommitSeq>,
    /// Number of version records before compaction.
    pub versions_before: usize,
    /// Number of version records retained after compaction.
    pub versions_after: usize,
    /// Number of current records whose undo heads were cleared.
    pub current_records_rewritten: usize,
}

#[derive(Debug, Clone, Default)]
struct CommittedChange {
    transaction: TxnId,
    snapshot: CommitSeq,
    changed_trees: BTreeSet<TreeId>,
    writes: BTreeSet<(TreeId, Vec<u8>)>,
}

struct ActiveSnapshots {
    snapshots: BTreeMap<TxnId, CommitSeq>,
}

impl ActiveSnapshots {
    fn new() -> Self {
        Self {
            snapshots: BTreeMap::new(),
        }
    }

    fn insert(&mut self, transaction: TxnId, snapshot: CommitSeq) {
        self.snapshots.insert(transaction, snapshot);
    }

    fn remove(&mut self, transaction: TxnId) {
        self.snapshots.remove(&transaction);
    }

    fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    fn oldest(&self) -> Option<CommitSeq> {
        self.snapshots.values().copied().min()
    }
}

struct ControlState {
    statuses: BTreeMap<TxnId, CommitSeq>,
    changes: BTreeMap<CommitSeq, CommittedChange>,
    max_transaction: u64,
    max_tree: u64,
}

struct Runtime {
    db: Mutex<DB>,
    versions: Mutex<VersionStore>,
    statuses: Mutex<BTreeMap<TxnId, CommitSeq>>,
    changes: Mutex<BTreeMap<CommitSeq, CommittedChange>>,
    active_snapshots: Mutex<ActiveSnapshots>,
    next_transaction: AtomicU64,
    next_tree: AtomicU64,
    closed: AtomicBool,
}

/// A transactional SeerDB handle.
///
/// The handle may be shared by callers using `Arc`; short database-lock
/// sections coordinate durable publication while transaction reads resolve
/// committed logical versions from the current durable state. One handle owns
/// one durable writer directory, and `close` refuses to run while transactions
/// remain live.
pub struct TransactionDatabase {
    runtime: Arc<Runtime>,
}

/// One fixed-snapshot transaction over SeerDB's ordered byte trees.
///
/// Writes to different keys and trees can commit from one snapshot. A write
/// conflicts when another committed transaction changed the same key or the
/// lifecycle of its tree after this transaction's snapshot. Reads resolve one
/// version per key at the fixed snapshot, and a multi-tree commit is published
/// atomically.
pub struct Transaction {
    runtime: Arc<Runtime>,
    id: TxnId,
    snapshot: CommitSeq,
    snapshot_position: CommitPosition,
    writes: BTreeMap<(TreeId, Vec<u8>), Option<Vec<u8>>>,
    created: BTreeSet<TreeId>,
    dropped: BTreeSet<TreeId>,
    state: TransactionState,
    snapshot_registered: bool,
}

impl TransactionDatabase {
    /// Create a new transactional database at `path`.
    pub fn create<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let db = DB::create(path, options)?;
        let versions = VersionStore::create(db.directory().join(VERSION_STORE_FILE))?;
        db.sync_directory_entry()?;
        Self::from_db(db, versions)
    }

    /// Open an existing transactional database at `path`.
    pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let db = DB::open(path, options)?;
        let versions = VersionStore::open(db.directory().join(VERSION_STORE_FILE))?;
        Self::from_db(db, versions)
    }

    fn from_db(mut db: DB, versions: VersionStore) -> Result<Self> {
        let mut versions = versions;
        let ControlState {
            statuses,
            changes,
            max_transaction,
            max_tree,
        } = load_control_state(&mut db, &mut versions)?;
        let next_transaction = max_transaction
            .checked_add(1)
            .ok_or_else(|| Error::Wal("transaction ID exhausted".into()))?;
        let next_tree = max_tree
            .checked_add(1)
            .ok_or_else(|| Error::Wal("tree ID exhausted".into()))?;
        Ok(Self {
            runtime: Arc::new(Runtime {
                db: Mutex::new(db),
                versions: Mutex::new(versions),
                statuses: Mutex::new(statuses),
                changes: Mutex::new(changes),
                active_snapshots: Mutex::new(ActiveSnapshots::new()),
                next_transaction: AtomicU64::new(next_transaction),
                next_tree: AtomicU64::new(next_tree),
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Begin a fixed-snapshot transaction.
    pub fn begin(&self) -> Result<Transaction> {
        let id = allocate_id(&self.runtime.next_transaction, "transaction ID")?;
        let db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        let durability = db.durability_status();
        if durability.write_fenced {
            return Err(Error::NeedsRecovery(
                "transaction database is fenced; reopen required".into(),
            ));
        }
        let snapshot_position = durability.commit_position;
        let snapshot = snapshot_position.csn;
        self.runtime
            .active_snapshots
            .lock()
            .map_err(|_| Error::Corruption("active snapshot registry mutex is poisoned".into()))?
            .insert(TxnId::new(id), snapshot);
        Ok(Transaction {
            runtime: Arc::clone(&self.runtime),
            id: TxnId::new(id),
            snapshot,
            snapshot_position,
            writes: BTreeMap::new(),
            created: BTreeSet::new(),
            dropped: BTreeSet::new(),
            state: TransactionState::Active,
            snapshot_registered: true,
        })
    }

    /// Return the current logical committed sequence number (CSN).
    pub fn commit_sequence(&self) -> Result<CommitSeq> {
        Ok(self.commit_position()?.csn)
    }

    /// Return the current logical and durable commit position.
    pub fn commit_position(&self) -> Result<CommitPosition> {
        let db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        Ok(db.durability_status().commit_position)
    }

    /// Return the oldest snapshot currently pinning logical MVCC history.
    pub fn oldest_active_snapshot(&self) -> Result<Option<CommitSeq>> {
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        self.runtime.oldest_active_snapshot()
    }

    /// Compact logical MVCC history while preserving every active snapshot.
    pub fn gc_versions(&self) -> Result<VersionGcReport> {
        let mut db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        let mut version_store = self
            .runtime
            .versions
            .lock()
            .map_err(|_| Error::Corruption("MVCC version store mutex is poisoned".into()))?;
        let statuses = self
            .runtime
            .statuses
            .lock()
            .map_err(|_| Error::Corruption("transaction status mutex is poisoned".into()))?;
        let watermark = self
            .runtime
            .active_snapshots
            .lock()
            .map_err(|_| Error::Corruption("active snapshot registry mutex is poisoned".into()))?
            .oldest();
        let mut retained = BTreeSet::new();
        let mut rewrites = Vec::new();
        let data_prefix = vec![TREE_DATA_PREFIX];
        {
            let mut gc = GcContext {
                db: &db,
                version_store: &mut version_store,
                statuses: &statuses,
                watermark,
                retained: &mut retained,
                rewrites: &mut rewrites,
            };
            gc.collect(TREE_RECORD_PREFIX, &prefix_end(TREE_RECORD_PREFIX))?;
            gc.collect(&data_prefix, &prefix_end(&data_prefix))?;
        }

        let versions_before = version_store.len();
        if !rewrites.is_empty() {
            db.commit_batch(&rewrites)?;
        }
        let (_, versions_after) = match version_store.compact(&retained) {
            Ok(counts) => counts,
            Err(error) => {
                db.fence_writes();
                return Err(error);
            }
        };
        Ok(VersionGcReport {
            watermark,
            versions_before,
            versions_after,
            current_records_rewritten: rewrites.len(),
        })
    }

    /// Flush and close the underlying durable database.
    ///
    /// A live transaction keeps the handle's active-transaction guard held,
    /// so closing with live transactions is rejected rather than invalidating
    /// their logical snapshots. The closed state is shared with transactions
    /// that still hold an `Arc` to this handle.
    pub fn close(&self) -> Result<()> {
        let mut db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        if !self
            .runtime
            .active_snapshots
            .lock()
            .map_err(|_| Error::Corruption("active snapshot registry mutex is poisoned".into()))?
            .is_empty()
        {
            return Err(Error::InvalidArgument(
                "cannot close database while transactions are active".into(),
            ));
        }
        if self.runtime.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        self.runtime
            .versions
            .lock()
            .map_err(|_| Error::Corruption("MVCC version store mutex is poisoned".into()))?
            .sync()?;
        db.close()?;
        self.runtime.closed.store(true, Ordering::Release);
        Ok(())
    }
}

impl Runtime {
    fn release_snapshot(&self, transaction: TxnId) -> Result<()> {
        self.active_snapshots
            .lock()
            .map_err(|_| Error::Corruption("active snapshot registry mutex is poisoned".into()))?
            .remove(transaction);
        Ok(())
    }

    fn oldest_active_snapshot(&self) -> Result<Option<CommitSeq>> {
        Ok(self
            .active_snapshots
            .lock()
            .map_err(|_| Error::Corruption("active snapshot registry mutex is poisoned".into()))?
            .oldest())
    }

    fn reserve_tree(&self, owner: TxnId, tree: TreeId) -> Result<CommitSeq> {
        let mut db = self
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        let mut versions = self
            .versions
            .lock()
            .map_err(|_| Error::Corruption("MVCC version store mutex is poisoned".into()))?;
        let mut statuses = self
            .statuses
            .lock()
            .map_err(|_| Error::Corruption("transaction status mutex is poisoned".into()))?;
        let current = db.durability_status().commit_position.csn;
        let next = CommitSeq::new(
            current
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?,
        );
        let change = CommittedChange {
            transaction: owner,
            snapshot: current,
            changed_trees: BTreeSet::from([tree]),
            writes: BTreeSet::new(),
        };
        let current_record = decode_current(db.get(&tree_record_key(tree))?.as_deref())?;
        let undo_head = append_before_image(&mut versions, &current_record)?;
        let reservation = encode_current(&CurrentRecord {
            transaction: owner,
            commit: CommitSeq::new(0),
            undo_head,
            value: Some(TREE_RESERVED.to_vec()),
        })?;
        versions.sync()?;
        let mutations = [
            BatchMutation::Put {
                key: tree_record_key(tree),
                value: reservation,
            },
            BatchMutation::Put {
                key: status_record_key(owner),
                value: encode_status(next),
            },
            BatchMutation::Put {
                key: change_record_key(next),
                value: encode_change(&change)?,
            },
        ];
        let status = db.commit_batch_at(CommitId::new(current.get()), &mutations)?;
        let committed = status.commit_position.csn;
        if committed != next {
            return Err(Error::NeedsRecovery(format!(
                "tree reservation expected {:?}, storage published {:?}",
                next, committed
            )));
        }
        statuses.insert(owner, committed);
        lock_changes(self).insert(committed, change);
        Ok(committed)
    }
}

impl Transaction {
    /// Return the stable transaction identity.
    #[must_use]
    pub fn id(&self) -> TxnId {
        self.id
    }

    /// Return the commit sequence captured at begin.
    #[must_use]
    pub fn snapshot(&self) -> CommitSeq {
        self.snapshot
    }

    /// Return the logical and durable position captured at transaction begin.
    #[must_use]
    pub fn snapshot_position(&self) -> CommitPosition {
        self.snapshot_position
    }

    /// Return the transaction lifecycle state.
    #[must_use]
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Return whether the transaction may still perform work.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Return whether no user or tree mutation has been staged.
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.writes.is_empty() && self.created.is_empty() && self.dropped.is_empty()
    }

    /// Allocate and stage creation of a first-class ordered tree.
    pub fn create_tree(&mut self) -> Result<TreeId> {
        self.check_active()?;
        let tree = TreeId::new(allocate_id(&self.runtime.next_tree, "tree ID")?);
        if let Err(error) = self.runtime.reserve_tree(self.id, tree) {
            if matches!(&error, Error::NeedsRecovery(_)) || is_recovery_error(&self.runtime)? {
                let commit = CommitSeq::new(
                    self.snapshot
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?,
                );
                self.state = TransactionState::RecoveryRequired { commit };
                self.release_snapshot()?;
                return Err(Error::NeedsRecovery(format!(
                    "tree reservation for {:?} may be durable: {error}",
                    tree
                )));
            }
            return Err(error);
        }
        self.created.insert(tree);
        Ok(tree)
    }

    /// Stage a tree drop. Existing values remain physically available to old
    /// snapshots; the tree is hidden from snapshots at and after the commit.
    pub fn drop_tree(&mut self, tree: TreeId) -> Result<()> {
        self.check_active()?;
        if !self.created.contains(&tree) && !self.tree_visible(tree)? {
            return Err(Error::TreeNotFound(tree));
        }
        self.dropped.insert(tree);
        Ok(())
    }

    /// Stage an upsert in one tree.
    pub fn put(&mut self, tree: TreeId, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_active()?;
        self.check_tree_for_write(tree)?;
        self.writes
            .insert((tree, key.to_vec()), Some(value.to_vec()));
        Ok(())
    }

    /// Stage a delete in one tree.
    pub fn delete(&mut self, tree: TreeId, key: &[u8]) -> Result<()> {
        self.check_active()?;
        self.check_tree_for_write(tree)?;
        self.writes.insert((tree, key.to_vec()), None);
        Ok(())
    }

    /// Read one key through this transaction's snapshot and staged writes.
    pub fn get(&self, tree: TreeId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_active()?;
        self.check_tree_visible_for_read(tree)?;
        if let Some(value) = self.writes.get(&(tree, key.to_vec())) {
            return Ok(value.clone());
        }
        let db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        let current = decode_current(db.get(&tree_key(tree, key))?.as_deref())?;
        let mut version_store = self
            .runtime
            .versions
            .lock()
            .map_err(|_| Error::Corruption("MVCC version store mutex is poisoned".into()))?;
        let statuses = self
            .runtime
            .statuses
            .lock()
            .map_err(|_| Error::Corruption("transaction status mutex is poisoned".into()))?;
        Ok(
            visible_current(&mut version_store, &statuses, &current, self.snapshot)?
                .and_then(|version| version.value),
        )
    }

    /// List trees visible to this transaction in stable ID order.
    ///
    /// The result includes trees created by this transaction and excludes
    /// trees dropped by it. Lifecycle metadata is validated before it is
    /// returned, so malformed control records fail closed.
    pub fn list_trees(&self) -> Result<Vec<TreeId>> {
        self.check_active()?;
        let db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        let mut version_store = self
            .runtime
            .versions
            .lock()
            .map_err(|_| Error::Corruption("MVCC version store mutex is poisoned".into()))?;
        let statuses = self
            .runtime
            .statuses
            .lock()
            .map_err(|_| Error::Corruption("transaction status mutex is poisoned".into()))?;
        let end = prefix_end(TREE_RECORD_PREFIX);
        let mut trees = BTreeSet::new();
        for (key, value) in db.range(TREE_RECORD_PREFIX, &end)? {
            if key.len() != TREE_RECORD_PREFIX.len() + 8 {
                return Err(Error::Corruption("malformed tree lifecycle key".into()));
            }
            let tree = TreeId::new(u64::from_be_bytes(
                key[TREE_RECORD_PREFIX.len()..]
                    .try_into()
                    .map_err(|_| Error::Corruption("malformed tree lifecycle ID".into()))?,
            ));
            let current = decode_current(Some(&value))?;
            match visible_current(&mut version_store, &statuses, &current, self.snapshot)?
                .and_then(|version| version.value)
                .as_deref()
            {
                Some(TREE_LIVE) => {
                    trees.insert(tree);
                }
                Some(TREE_DROPPED) | Some(TREE_RESERVED) | None => {
                    trees.remove(&tree);
                }
                Some(_) => {
                    return Err(Error::Corruption("malformed tree lifecycle value".into()));
                }
            }
        }
        trees.extend(self.created.iter().copied());
        for tree in &self.dropped {
            trees.remove(tree);
        }
        Ok(trees.into_iter().collect())
    }

    /// Scan `[start,end)` in key order. `None` for `end` scans through the
    /// end of the tree.
    pub fn scan(
        &self,
        tree: TreeId,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_active()?;
        self.check_tree_visible_for_read(tree)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        if end.is_some_and(|end| start > end) {
            return Err(Error::InvalidArgument(
                "scan end must not precede scan start".into(),
            ));
        }

        let mut values = BTreeMap::new();
        if !self.created.contains(&tree) {
            let prefix = tree_prefix(tree);
            let physical_start = append(&prefix, start);
            let physical_end = end
                .map(|end| append(&prefix, end))
                .unwrap_or_else(|| prefix_end(&prefix));
            if end != Some(start) {
                let db = self.runtime.db.lock().map_err(|_| {
                    Error::Corruption("transaction database mutex is poisoned".into())
                })?;
                let entries = db.range(&physical_start, &physical_end)?;
                let mut version_store = self.runtime.versions.lock().map_err(|_| {
                    Error::Corruption("MVCC version store mutex is poisoned".into())
                })?;
                let statuses = self.runtime.statuses.lock().map_err(|_| {
                    Error::Corruption("transaction status mutex is poisoned".into())
                })?;
                for (key, value) in entries {
                    let Some(user_key) = decode_tree_key(tree, &key) else {
                        continue;
                    };
                    let current = decode_current(Some(&value))?;
                    if let Some(version) =
                        visible_current(&mut version_store, &statuses, &current, self.snapshot)?
                        && let Some(value) = version.value
                    {
                        values.insert(user_key, value);
                    }
                }
            }
        }
        for ((mutation_tree, key), value) in &self.writes {
            if *mutation_tree != tree
                || key.as_slice() < start
                || end.is_some_and(|end| key.as_slice() >= end)
            {
                continue;
            }
            match value {
                Some(value) => {
                    values.insert(key.clone(), value.clone());
                }
                None => {
                    values.remove(key);
                }
            }
        }
        Ok(values.into_iter().take(limit).collect())
    }

    /// Publish all staged tree and key mutations atomically.
    pub fn commit(&mut self) -> Result<CommitPosition> {
        self.check_active()?;
        if self.is_read_only() {
            let position = self.snapshot_position;
            self.state = TransactionState::Committed {
                commit: position.csn,
            };
            self.release_snapshot()?;
            return Ok(position);
        }
        let result = commit_transaction(self);
        match result {
            Ok(position) => {
                self.state = TransactionState::Committed {
                    commit: position.csn,
                };
                self.release_snapshot()?;
                Ok(position)
            }
            Err(error)
                if matches!(&error, Error::NeedsRecovery(_))
                    || is_recovery_error(&self.runtime)? =>
            {
                let commit = CommitSeq::new(
                    self.snapshot
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?,
                );
                self.state = TransactionState::RecoveryRequired { commit };
                self.release_snapshot()?;
                Err(Error::NeedsRecovery(format!(
                    "transaction {:?} may have committed at {:?}: {error}",
                    self.id, commit
                )))
            }
            Err(error) => Err(error),
        }
    }

    /// Abort without publishing staged state.
    pub fn abort(&mut self) -> Result<()> {
        self.check_active()?;
        self.state = TransactionState::Aborted;
        self.release_snapshot()
    }

    fn release_snapshot(&mut self) -> Result<()> {
        if self.snapshot_registered {
            self.runtime.release_snapshot(self.id)?;
            self.snapshot_registered = false;
        }
        Ok(())
    }

    fn check_active(&self) -> Result<()> {
        if self.is_active() {
            Ok(())
        } else {
            Err(Error::TransactionInactive)
        }
    }

    fn tree_visible(&self, tree: TreeId) -> Result<bool> {
        let db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        let mut version_store = self
            .runtime
            .versions
            .lock()
            .map_err(|_| Error::Corruption("MVCC version store mutex is poisoned".into()))?;
        let statuses = self
            .runtime
            .statuses
            .lock()
            .map_err(|_| Error::Corruption("transaction status mutex is poisoned".into()))?;
        tree_visible(&db, &mut version_store, &statuses, tree, self.snapshot)
    }

    fn check_tree_visible_for_read(&self, tree: TreeId) -> Result<()> {
        if self.dropped.contains(&tree) {
            return Err(Error::TreeNotFound(tree));
        }
        if self.created.contains(&tree) || self.tree_visible(tree)? {
            Ok(())
        } else {
            Err(Error::TreeNotFound(tree))
        }
    }

    fn check_tree_for_write(&self, tree: TreeId) -> Result<()> {
        if self.dropped.contains(&tree) {
            return Err(Error::TreeNotFound(tree));
        }
        if self.created.contains(&tree) || self.tree_visible(tree)? {
            Ok(())
        } else {
            Err(Error::TreeNotFound(tree))
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.snapshot_registered {
            let _ = self.runtime.release_snapshot(self.id);
            self.snapshot_registered = false;
        }
    }
}

fn commit_transaction(transaction: &Transaction) -> Result<CommitPosition> {
    let mut db = transaction
        .runtime
        .db
        .lock()
        .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
    if transaction.runtime.closed.load(Ordering::Acquire) {
        return Err(Error::InvalidArgument("database is closed".into()));
    }
    let mut version_store = transaction
        .runtime
        .versions
        .lock()
        .map_err(|_| Error::Corruption("MVCC version store mutex is poisoned".into()))?;
    let mut statuses = transaction
        .runtime
        .statuses
        .lock()
        .map_err(|_| Error::Corruption("transaction status mutex is poisoned".into()))?;
    let current = db.durability_status().commit_position.csn;
    validate_conflicts(transaction, &db, &statuses, current)?;

    let next = CommitSeq::new(
        current
            .get()
            .checked_add(1)
            .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?,
    );
    let mut mutations = Vec::new();
    let mut writes = BTreeSet::new();
    for ((tree, key), value) in &transaction.writes {
        if transaction.dropped.contains(tree) {
            continue;
        }
        let storage_key = tree_key(*tree, key);
        let current_record = decode_current(db.get(&storage_key)?.as_deref())?;
        let undo_head = append_before_image(&mut version_store, &current_record)?;
        mutations.push(BatchMutation::Put {
            key: storage_key,
            value: encode_current(&CurrentRecord {
                transaction: transaction.id,
                commit: CommitSeq::new(0),
                undo_head,
                value: value.clone(),
            })?,
        });
        writes.insert((*tree, key.clone()));
    }

    let mut changed_trees = transaction.created.clone();
    changed_trees.extend(transaction.dropped.iter().copied());
    for tree in &changed_trees {
        let lifecycle = if transaction.dropped.contains(tree) {
            TREE_DROPPED
        } else {
            TREE_LIVE
        };
        let lifecycle_key = tree_record_key(*tree);
        let current_record = decode_current(db.get(&lifecycle_key)?.as_deref())?;
        let undo_head = append_before_image(&mut version_store, &current_record)?;
        mutations.push(BatchMutation::Put {
            key: lifecycle_key,
            value: encode_current(&CurrentRecord {
                transaction: transaction.id,
                commit: CommitSeq::new(0),
                undo_head,
                value: Some(lifecycle.to_vec()),
            })?,
        });
    }

    let change = CommittedChange {
        transaction: transaction.id,
        snapshot: transaction.snapshot,
        changed_trees: changed_trees.clone(),
        writes: writes.clone(),
    };
    mutations.push(BatchMutation::Put {
        key: status_record_key(transaction.id),
        value: encode_status(next),
    });
    mutations.push(BatchMutation::Put {
        key: change_record_key(next),
        value: encode_change(&change)?,
    });
    version_store.sync()?;
    let status = db.commit_batch_at(CommitId::new(current.get()), &mutations)?;
    let committed = status.commit_position.csn;
    if committed != next {
        return Err(Error::Corruption(format!(
            "transaction expected commit {:?}, storage published {:?}",
            next, committed
        )));
    }
    statuses.insert(transaction.id, committed);
    lock_changes(&transaction.runtime).insert(committed, change);
    Ok(status.commit_position)
}

struct GcContext<'a> {
    db: &'a DB,
    version_store: &'a mut VersionStore,
    statuses: &'a BTreeMap<TxnId, CommitSeq>,
    watermark: Option<CommitSeq>,
    retained: &'a mut BTreeSet<crate::storage::format::VersionId>,
    rewrites: &'a mut Vec<BatchMutation>,
}

impl GcContext<'_> {
    fn collect(&mut self, start: &[u8], end: &[u8]) -> Result<()> {
        for (key, value) in self.db.range(start, end)? {
            let current = decode_current(Some(&value))?;
            retain_current_chain(
                self.version_store,
                self.statuses,
                &current,
                self.watermark,
                self.retained,
            )?;
            let clear_history = match self.watermark {
                None => true,
                Some(watermark) => {
                    resolve_commit(self.statuses, current.transaction, current.commit)? <= watermark
                }
            };
            if clear_history && current.undo_head.is_some() {
                let mut rewritten = current;
                rewritten.undo_head = None;
                self.rewrites.push(BatchMutation::Put {
                    key,
                    value: encode_current(&rewritten)?,
                });
            }
        }
        Ok(())
    }
}

fn retain_current_chain(
    version_store: &mut VersionStore,
    statuses: &BTreeMap<TxnId, CommitSeq>,
    current: &CurrentRecord,
    watermark: Option<CommitSeq>,
    retained: &mut BTreeSet<crate::storage::format::VersionId>,
) -> Result<()> {
    let Some(watermark) = watermark else {
        let mut head = current.undo_head;
        while let Some(id) = head {
            head = version_store.get(id)?.previous;
        }
        return Ok(());
    };
    let current_commit = resolve_commit(statuses, current.transaction, current.commit)?;
    if current_commit <= watermark {
        return Ok(());
    }
    let mut head = current.undo_head;
    while let Some(id) = head {
        retained.insert(id);
        let record = version_store.get(id)?;
        let commit = resolve_commit(statuses, record.transaction, record.commit)?;
        if commit <= watermark {
            break;
        }
        head = record.previous;
    }
    Ok(())
}

fn append_before_image(
    version_store: &mut VersionStore,
    current: &CurrentRecord,
) -> Result<Option<crate::storage::format::VersionId>> {
    version_store
        .append(
            current.undo_head,
            current.transaction,
            current.commit,
            current.value.as_deref(),
        )
        .map(Some)
}

fn validate_conflicts(
    transaction: &Transaction,
    db: &DB,
    statuses: &BTreeMap<TxnId, CommitSeq>,
    current: CommitSeq,
) -> Result<()> {
    if transaction.snapshot > current {
        return Err(Error::SerializationConflict {
            expected: CommitId::new(transaction.snapshot.get()),
            current: CommitId::new(current.get()),
        });
    }

    for tree in &transaction.created {
        let current_record = decode_current(db.get(&tree_record_key(*tree))?.as_deref())?;
        if current_record.transaction != transaction.id
            || current_record.value.as_deref() != Some(TREE_RESERVED)
        {
            return Err(Error::TreeConflict(*tree));
        }
    }

    for tree in &transaction.dropped {
        let current_record = decode_current(db.get(&tree_record_key(*tree))?.as_deref())?;
        let current_commit =
            resolve_commit(statuses, current_record.transaction, current_record.commit)?;
        if current_commit > transaction.snapshot && current_record.transaction != transaction.id {
            return Err(Error::TreeConflict(*tree));
        }
        if tree_has_conflicting_write(db, statuses, *tree, transaction.snapshot, transaction.id)? {
            return Err(Error::TreeConflict(*tree));
        }
    }

    for (tree, key) in transaction.writes.keys() {
        if transaction.created.contains(tree) {
            continue;
        }
        let lifecycle = decode_current(db.get(&tree_record_key(*tree))?.as_deref())?;
        let lifecycle_commit = resolve_commit(statuses, lifecycle.transaction, lifecycle.commit)?;
        if lifecycle_commit > transaction.snapshot && lifecycle.transaction != transaction.id {
            return Err(Error::TreeConflict(*tree));
        }
        let current_record = decode_current(db.get(&tree_key(*tree, key))?.as_deref())?;
        let current_commit =
            resolve_commit(statuses, current_record.transaction, current_record.commit)?;
        if current_commit > transaction.snapshot && current_record.transaction != transaction.id {
            return Err(Error::WriteConflict {
                tree: *tree,
                key: key.clone(),
            });
        }
    }
    Ok(())
}

fn tree_has_conflicting_write(
    db: &DB,
    statuses: &BTreeMap<TxnId, CommitSeq>,
    tree: TreeId,
    snapshot: CommitSeq,
    transaction: TxnId,
) -> Result<bool> {
    let prefix = tree_prefix(tree);
    let end = prefix_end(&prefix);
    for (key, value) in db.range(&prefix, &end)? {
        if decode_tree_key(tree, &key).is_none() {
            continue;
        }
        let current = decode_current(Some(&value))?;
        let current_commit = resolve_commit(statuses, current.transaction, current.commit)?;
        if current_commit > snapshot && current.transaction != transaction {
            return Ok(true);
        }
    }
    Ok(false)
}

fn lock_changes(
    runtime: &Runtime,
) -> std::sync::MutexGuard<'_, BTreeMap<CommitSeq, CommittedChange>> {
    match runtime.changes.lock() {
        Ok(changes) => changes,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn is_recovery_error(runtime: &Arc<Runtime>) -> Result<bool> {
    let db = runtime
        .db
        .lock()
        .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
    Ok(db.durability_status().write_fenced)
}

fn tree_visible(
    db: &DB,
    version_store: &mut VersionStore,
    statuses: &BTreeMap<TxnId, CommitSeq>,
    tree: TreeId,
    snapshot: CommitSeq,
) -> Result<bool> {
    let current = decode_current(db.get(&tree_record_key(tree))?.as_deref())?;
    match visible_current(version_store, statuses, &current, snapshot)?
        .and_then(|version| version.value)
    {
        Some(value) if value.as_slice() == TREE_LIVE => Ok(true),
        Some(value) if value.as_slice() == TREE_DROPPED || value.as_slice() == TREE_RESERVED => {
            Ok(false)
        }
        None => Ok(false),
        Some(_) => Err(Error::Corruption(format!(
            "tree {:?} has an invalid lifecycle record",
            tree
        ))),
    }
}

fn validate_version_chain(version_store: &mut VersionStore, current: &CurrentRecord) -> Result<()> {
    let mut head = current.undo_head;
    while let Some(id) = head {
        let record = version_store.get(id)?;
        if !matches!(
            record.value.as_deref(),
            Some(TREE_LIVE) | Some(TREE_DROPPED) | Some(TREE_RESERVED) | None
        ) {
            return Err(Error::Corruption("malformed tree lifecycle value".into()));
        }
        head = record.previous;
    }
    Ok(())
}

fn load_control_state(db: &mut DB, version_store: &mut VersionStore) -> Result<ControlState> {
    let mut statuses = BTreeMap::new();
    let status_end = prefix_end(STATUS_RECORD_PREFIX);
    let mut max_transaction = 0;
    for (key, value) in db.range(STATUS_RECORD_PREFIX, &status_end)? {
        if key.len() != STATUS_RECORD_PREFIX.len() + 8 {
            return Err(Error::Corruption("malformed transaction status key".into()));
        }
        let transaction = TxnId::new(u64::from_be_bytes(
            key[STATUS_RECORD_PREFIX.len()..]
                .try_into()
                .map_err(|_| Error::Corruption("malformed transaction status ID".into()))?,
        ));
        if transaction.get() == 0 {
            return Err(Error::Corruption("transaction status ID is zero".into()));
        }
        let commit = decode_status(&value)?;
        if statuses.insert(transaction, commit).is_some() {
            return Err(Error::Corruption(
                "duplicate transaction status record".into(),
            ));
        }
        max_transaction = max_transaction.max(transaction.get());
    }

    let mut changes = BTreeMap::new();
    let mut max_tree = 0;
    let tree_end = prefix_end(TREE_RECORD_PREFIX);
    for (key, value) in db.range(TREE_RECORD_PREFIX, &tree_end)? {
        if key.len() != TREE_RECORD_PREFIX.len() + 8 {
            return Err(Error::Corruption("malformed tree lifecycle key".into()));
        }
        let tree = u64::from_be_bytes(
            key[TREE_RECORD_PREFIX.len()..]
                .try_into()
                .map_err(|_| Error::Corruption("malformed tree lifecycle ID".into()))?,
        );
        let current = decode_current(Some(&value))?;
        if !matches!(
            current.value.as_deref(),
            Some(TREE_LIVE) | Some(TREE_DROPPED) | Some(TREE_RESERVED)
        ) {
            return Err(Error::Corruption("malformed tree lifecycle value".into()));
        }
        validate_version_chain(version_store, &current)?;
        max_tree = max_tree.max(tree);
    }

    let change_end = prefix_end(CHANGE_RECORD_PREFIX);
    for (key, value) in db.range(CHANGE_RECORD_PREFIX, &change_end)? {
        if key.len() != CHANGE_RECORD_PREFIX.len() + 8 {
            return Err(Error::Corruption("malformed transaction change key".into()));
        }
        let commit = CommitSeq::new(u64::from_be_bytes(
            key[CHANGE_RECORD_PREFIX.len()..]
                .try_into()
                .map_err(|_| Error::Corruption("malformed transaction commit ID".into()))?,
        ));
        let change = decode_change(&value)?;
        if change.snapshot > commit {
            return Err(Error::Corruption(
                "transaction change snapshot is newer than its commit".into(),
            ));
        }
        max_transaction = max_transaction.max(change.transaction.get());
        max_tree = max_tree.max(
            change
                .changed_trees
                .iter()
                .map(|tree| tree.get())
                .max()
                .unwrap_or(0),
        );
        if changes.insert(commit, change).is_some() {
            return Err(Error::Corruption(
                "duplicate transaction change record".into(),
            ));
        }
    }
    Ok(ControlState {
        statuses,
        changes,
        max_transaction,
        max_tree,
    })
}

fn status_record_key(transaction: TxnId) -> Vec<u8> {
    append(STATUS_RECORD_PREFIX, &transaction.get().to_be_bytes())
}

fn encode_status(commit: CommitSeq) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(STATUS_MAGIC.len() + 8);
    bytes.extend_from_slice(STATUS_MAGIC);
    bytes.extend_from_slice(&commit.get().to_be_bytes());
    bytes
}

fn decode_status(bytes: &[u8]) -> Result<CommitSeq> {
    if bytes.len() != STATUS_MAGIC.len() + 8 || &bytes[..STATUS_MAGIC.len()] != STATUS_MAGIC {
        return Err(Error::Corruption(
            "malformed transaction status record".into(),
        ));
    }
    let commit = CommitSeq::new(u64::from_be_bytes(
        bytes[STATUS_MAGIC.len()..]
            .try_into()
            .map_err(|_| Error::Corruption("malformed transaction status commit".into()))?,
    ));
    if commit.get() == 0 {
        return Err(Error::Corruption(
            "transaction status commit is zero".into(),
        ));
    }
    Ok(commit)
}

fn encode_change(change: &CommittedChange) -> Result<Vec<u8>> {
    let tree_count = u32::try_from(change.changed_trees.len())
        .map_err(|_| Error::InvalidArgument("too many changed trees".into()))?;
    let write_count = u32::try_from(change.writes.len())
        .map_err(|_| Error::InvalidArgument("too many transaction writes".into()))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CHANGE_MAGIC);
    bytes.extend_from_slice(&change.transaction.get().to_be_bytes());
    bytes.extend_from_slice(&change.snapshot.get().to_be_bytes());
    bytes.extend_from_slice(&tree_count.to_be_bytes());
    for tree in &change.changed_trees {
        bytes.extend_from_slice(&tree.get().to_be_bytes());
    }
    bytes.extend_from_slice(&write_count.to_be_bytes());
    for (tree, key) in &change.writes {
        let length = u32::try_from(key.len())
            .map_err(|_| Error::InvalidArgument("transaction key is too large".into()))?;
        bytes.extend_from_slice(&tree.get().to_be_bytes());
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(key);
    }
    if bytes.len() > MAX_CHANGE_RECORD_BYTES {
        return Err(Error::InvalidArgument(
            "transaction conflict record is too large".into(),
        ));
    }
    Ok(bytes)
}

fn decode_change(bytes: &[u8]) -> Result<CommittedChange> {
    if bytes.len() > MAX_CHANGE_RECORD_BYTES {
        return Err(Error::Corruption(
            "transaction conflict record exceeds limit".into(),
        ));
    }
    let mut cursor = 0;
    if take(bytes, &mut cursor, 4)? != CHANGE_MAGIC {
        return Err(Error::Corruption(
            "transaction conflict record has invalid magic".into(),
        ));
    }
    let transaction = TxnId::new(read_u64(bytes, &mut cursor)?);
    let snapshot = CommitSeq::new(read_u64(bytes, &mut cursor)?);
    let tree_count = read_u32(bytes, &mut cursor)? as usize;
    let mut changed_trees = BTreeSet::new();
    for _ in 0..tree_count {
        changed_trees.insert(TreeId::new(read_u64(bytes, &mut cursor)?));
    }
    let write_count = read_u32(bytes, &mut cursor)? as usize;
    let mut writes = BTreeSet::new();
    for _ in 0..write_count {
        let tree = TreeId::new(read_u64(bytes, &mut cursor)?);
        let key_length = read_u32(bytes, &mut cursor)? as usize;
        let key = take(bytes, &mut cursor, key_length)?.to_vec();
        writes.insert((tree, key));
    }
    if cursor != bytes.len() {
        return Err(Error::Corruption(
            "transaction conflict record has trailing bytes".into(),
        ));
    }
    Ok(CommittedChange {
        transaction,
        snapshot,
        changed_trees,
        writes,
    })
}

fn tree_prefix(tree: TreeId) -> Vec<u8> {
    let mut prefix = vec![TREE_DATA_PREFIX];
    prefix.extend_from_slice(&tree.get().to_be_bytes());
    prefix
}

fn tree_key(tree: TreeId, key: &[u8]) -> Vec<u8> {
    append(&tree_prefix(tree), key)
}

fn tree_record_key(tree: TreeId) -> Vec<u8> {
    append(TREE_RECORD_PREFIX, &tree.get().to_be_bytes())
}

fn change_record_key(commit: CommitSeq) -> Vec<u8> {
    append(CHANGE_RECORD_PREFIX, &commit.get().to_be_bytes())
}

fn append(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(prefix.len() + suffix.len());
    result.extend_from_slice(prefix);
    result.extend_from_slice(suffix);
    result
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut result = prefix.to_vec();
    for index in (0..result.len()).rev() {
        if result[index] != u8::MAX {
            result[index] += 1;
            result.truncate(index + 1);
            return result;
        }
    }
    result.push(0);
    result
}

fn decode_tree_key(tree: TreeId, key: &[u8]) -> Option<Vec<u8>> {
    let prefix = tree_prefix(tree);
    key.strip_prefix(prefix.as_slice()).map(ToOwned::to_owned)
}

fn allocate_id(counter: &AtomicU64, label: &str) -> Result<u64> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map_err(|_| Error::Wal(format!("{label} exhausted")))
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, length: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| Error::Corruption("transaction record length overflow".into()))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Corruption("truncated transaction conflict record".into()))?;
    *cursor = end;
    Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_be_bytes(
        take(bytes, cursor, 4)?
            .try_into()
            .map_err(|_| Error::Corruption("invalid transaction record integer".into()))?,
    ))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    Ok(u64::from_be_bytes(
        take(bytes, cursor, 8)?
            .try_into()
            .map_err(|_| Error::Corruption("invalid transaction record integer".into()))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn database() -> (tempfile::TempDir, TransactionDatabase) {
        let directory = tempdir().expect("temporary directory");
        let database =
            TransactionDatabase::create(directory.path().join("db"), Options::for_test())
                .expect("create database");
        (directory, database)
    }

    fn tree(database: &TransactionDatabase) -> TreeId {
        let mut transaction = database.begin().expect("begin tree transaction");
        let tree = transaction.create_tree().expect("create tree");
        transaction.commit().expect("commit tree");
        tree
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

        let reader = database.begin().expect("reader begin");
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
        assert!(matches!(
            second.commit(),
            Err(Error::WriteConflict { tree: conflict_tree, ref key })
                if conflict_tree == tree && key == b"key"
        ));
        second.abort().expect("abort loser");

        let reader = database.begin().expect("reader begin");
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
        let reader = database.begin().expect("reader");
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
    fn multi_tree_commit_and_snapshot_visibility_are_atomic() {
        let (_directory, database) = database();
        let first_tree = tree(&database);
        let mut create = database.begin().expect("create second tree");
        let second_tree = create.create_tree().expect("second tree");
        create.commit().expect("second tree commit");

        let old = database.begin().expect("old snapshot");
        let mut writer = database.begin().expect("writer");
        writer.put(first_tree, b"one", b"1").expect("first write");
        writer.put(second_tree, b"two", b"2").expect("second write");
        writer.commit().expect("atomic commit");
        assert_eq!(old.get(first_tree, b"one").expect("old first"), None);
        assert_eq!(old.get(second_tree, b"two").expect("old second"), None);

        let current = database.begin().expect("current snapshot");
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

        let old = database.begin().expect("old snapshot");
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
        let mut version_store =
            VersionStore::open(directory.path().join("db").join(VERSION_STORE_FILE))
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
}
