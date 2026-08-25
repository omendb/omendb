//! Capability-rich transactional ordered-KV API.
//!
//! This module is the transactional vertical slice above the durable `DB`
//! storage engine. It provides first-class tree identities, fixed snapshots,
//! atomic multi-tree batches, and snapshot-isolation write-conflict checking.
//! The physical engine remains the single publication authority; this module
//! owns transaction coordination and the durable conflict record that makes
//! concurrent in-process transactions restartable.
//!
//! The API deliberately does not expose a backend matrix or a fake plugin
//! trait. OmenDB can call this capability-rich surface directly while the
//! server/session layer is built above it.

use crate::db::{BatchMutation, DB, Options, ReadView};
use crate::error::{Error, Result};
use crate::storage::format::{CommitId, CommitSeq, TreeId, TxnId};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const TREE_RECORD_PREFIX: &[u8] = b"\x00seerdb/tree/";
const CHANGE_RECORD_PREFIX: &[u8] = b"\x00seerdb/change/";
const TREE_DATA_PREFIX: u8 = 0x01;
const TREE_LIVE: &[u8] = b"live";
const TREE_DROPPED: &[u8] = b"dropped";
const CHANGE_MAGIC: &[u8; 4] = b"SCM1";
const MAX_CHANGE_RECORD_BYTES: usize = 16 * 1024 * 1024;

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

#[derive(Debug, Clone, Default)]
struct CommittedChange {
    transaction: TxnId,
    snapshot: CommitSeq,
    changed_trees: BTreeSet<TreeId>,
    writes: BTreeSet<(TreeId, Vec<u8>)>,
}

struct Runtime {
    db: Mutex<DB>,
    changes: Mutex<BTreeMap<CommitSeq, CommittedChange>>,
    next_transaction: AtomicU64,
    next_tree: AtomicU64,
    active_transactions: AtomicUsize,
    closed: AtomicBool,
}

/// A transactional SeerDB handle.
///
/// The handle may be shared by callers using `Arc`; short database-lock
/// sections coordinate durable publication while transaction reads use their
/// independent generation-bound [`ReadView`]. One handle owns one durable
/// writer directory, and `close` refuses to run while transactions remain
/// live.
pub struct TransactionDatabase {
    runtime: Arc<Runtime>,
}

/// One fixed-snapshot transaction over SeerDB's ordered byte trees.
///
/// Writes to different keys and trees can commit from one snapshot. A write
/// conflicts when another committed transaction changed the same key or the
/// lifecycle of its tree after this transaction's snapshot. Reads never see
/// a mixture of generations, and a multi-tree commit is published atomically.
pub struct Transaction {
    runtime: Arc<Runtime>,
    id: TxnId,
    snapshot: CommitSeq,
    view: Option<Arc<ReadView>>,
    writes: BTreeMap<(TreeId, Vec<u8>), Option<Vec<u8>>>,
    created: BTreeSet<TreeId>,
    dropped: BTreeSet<TreeId>,
    state: TransactionState,
}

impl TransactionDatabase {
    /// Create a new transactional database at `path`.
    pub fn create<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        Self::from_db(DB::create(path, options)?)
    }

    /// Open an existing transactional database at `path`.
    pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        Self::from_db(DB::open(path, options)?)
    }

    fn from_db(mut db: DB) -> Result<Self> {
        let (changes, max_transaction, max_tree) = load_control_state(&mut db)?;
        let next_transaction = max_transaction
            .checked_add(1)
            .ok_or_else(|| Error::Wal("transaction ID exhausted".into()))?;
        let next_tree = max_tree
            .checked_add(1)
            .ok_or_else(|| Error::Wal("tree ID exhausted".into()))?;
        Ok(Self {
            runtime: Arc::new(Runtime {
                db: Mutex::new(db),
                changes: Mutex::new(changes),
                next_transaction: AtomicU64::new(next_transaction),
                next_tree: AtomicU64::new(next_tree),
                active_transactions: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Begin a fixed-snapshot transaction.
    pub fn begin(&self) -> Result<Transaction> {
        let id = allocate_id(&self.runtime.next_transaction, "transaction ID")?;
        let mut db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        let view = Arc::new(db.begin_read_view()?);
        let snapshot = CommitSeq::new(view.commit_id().get());
        self.runtime
            .active_transactions
            .fetch_add(1, Ordering::AcqRel);
        Ok(Transaction {
            runtime: Arc::clone(&self.runtime),
            id: TxnId::new(id),
            snapshot,
            view: Some(view),
            writes: BTreeMap::new(),
            created: BTreeSet::new(),
            dropped: BTreeSet::new(),
            state: TransactionState::Active,
        })
    }

    /// Return the current committed sequence number.
    pub fn commit_sequence(&self) -> Result<CommitSeq> {
        let db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        Ok(CommitSeq::new(db.durability_status().commit_id.get()))
    }

    /// Flush and close the underlying durable database.
    ///
    /// A live transaction owns a physical read-view lease, so closing with
    /// live transactions is rejected rather than silently invalidating their
    /// snapshots. The closed state is shared with transactions that still
    /// hold an `Arc` to this handle.
    pub fn close(&self) -> Result<()> {
        let mut db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        if self.runtime.active_transactions.load(Ordering::Acquire) != 0 {
            return Err(Error::InvalidArgument(
                "cannot close database while transactions are active".into(),
            ));
        }
        if self.runtime.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        db.close()?;
        self.runtime.closed.store(true, Ordering::Release);
        Ok(())
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
        let view = self.view.as_ref().ok_or(Error::TransactionInactive)?;
        view.get(&tree_key(tree, key))
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
                let view = self.view.as_ref().ok_or(Error::TransactionInactive)?;
                for (key, value) in view.range(&physical_start, &physical_end)? {
                    let Some(user_key) = decode_tree_key(tree, &key) else {
                        continue;
                    };
                    values.insert(user_key, value);
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
    pub fn commit(&mut self) -> Result<CommitSeq> {
        self.check_active()?;
        if self.is_read_only() {
            let commit = self.snapshot;
            self.state = TransactionState::Committed { commit };
            self.view.take();
            return Ok(commit);
        }
        let result = commit_transaction(self);
        match result {
            Ok(commit) => {
                self.state = TransactionState::Committed { commit };
                self.view.take();
                Ok(commit)
            }
            Err(error) if is_recovery_error(&self.runtime)? => {
                let commit = CommitSeq::new(
                    self.snapshot
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?,
                );
                self.state = TransactionState::RecoveryRequired { commit };
                self.view.take();
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
        self.view.take();
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
        let view = self.view.as_ref().ok_or(Error::TransactionInactive)?;
        tree_visible(view, tree)
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
        self.runtime
            .active_transactions
            .fetch_sub(1, Ordering::AcqRel);
    }
}

fn commit_transaction(transaction: &Transaction) -> Result<CommitSeq> {
    let mut db = transaction
        .runtime
        .db
        .lock()
        .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
    if transaction.runtime.closed.load(Ordering::Acquire) {
        return Err(Error::InvalidArgument("database is closed".into()));
    }
    let current = CommitSeq::new(db.durability_status().commit_id.get());
    validate_conflicts(transaction, current)?;

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
        match value {
            Some(value) => mutations.push(BatchMutation::Put {
                key: storage_key,
                value: value.clone(),
            }),
            None => mutations.push(BatchMutation::Delete { key: storage_key }),
        }
        writes.insert((*tree, key.clone()));
    }

    let mut changed_trees = transaction.created.clone();
    changed_trees.extend(transaction.dropped.iter().copied());
    for tree in &changed_trees {
        mutations.push(BatchMutation::Put {
            key: tree_record_key(*tree),
            value: if transaction.dropped.contains(tree) {
                TREE_DROPPED.to_vec()
            } else {
                TREE_LIVE.to_vec()
            },
        });
    }

    let change = CommittedChange {
        transaction: transaction.id,
        snapshot: transaction.snapshot,
        changed_trees: changed_trees.clone(),
        writes: writes.clone(),
    };
    mutations.push(BatchMutation::Put {
        key: change_record_key(next),
        value: encode_change(&change)?,
    });
    let status = db.commit_batch_at(CommitId::new(current.get()), &mutations)?;
    let committed = CommitSeq::new(status.commit_id.get());
    if committed != next {
        return Err(Error::Corruption(format!(
            "transaction expected commit {:?}, storage published {:?}",
            next, committed
        )));
    }
    lock_changes(&transaction.runtime).insert(committed, change);
    Ok(committed)
}

fn validate_conflicts(transaction: &Transaction, current: CommitSeq) -> Result<()> {
    if transaction.snapshot > current {
        return Err(Error::SerializationConflict {
            expected: CommitId::new(transaction.snapshot.get()),
            current: CommitId::new(current.get()),
        });
    }
    if transaction.snapshot == current {
        return Ok(());
    }
    let changes = lock_changes(&transaction.runtime);
    let mut commit = transaction.snapshot.get().saturating_add(1);
    while commit <= current.get() {
        let sequence = CommitSeq::new(commit);
        let Some(change) = changes.get(&sequence) else {
            // A commit not written by this transactional surface cannot be
            // certified against a byte-level snapshot. Fail closed.
            return Err(Error::SerializationConflict {
                expected: CommitId::new(transaction.snapshot.get()),
                current: CommitId::new(current.get()),
            });
        };
        for tree in &transaction.created {
            if change.changed_trees.contains(tree) {
                return Err(Error::TreeConflict(*tree));
            }
        }
        for tree in &transaction.dropped {
            if change.changed_trees.contains(tree)
                || change.writes.iter().any(|(changed, _)| changed == tree)
            {
                return Err(Error::TreeConflict(*tree));
            }
        }
        for (tree, key) in transaction.writes.keys() {
            if change.changed_trees.contains(tree) {
                return Err(Error::TreeConflict(*tree));
            }
            if change.writes.contains(&(*tree, key.clone())) {
                return Err(Error::WriteConflict {
                    tree: *tree,
                    key: key.clone(),
                });
            }
        }
        commit = commit.saturating_add(1);
    }
    Ok(())
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

fn tree_visible(view: &ReadView, tree: TreeId) -> Result<bool> {
    match view.get(&tree_record_key(tree))? {
        Some(value) if value == TREE_LIVE => Ok(true),
        Some(value) if value == TREE_DROPPED => Ok(false),
        Some(_) => Err(Error::Corruption(format!(
            "tree {:?} has an invalid lifecycle record",
            tree
        ))),
        None => Ok(false),
    }
}

fn load_control_state(db: &mut DB) -> Result<(BTreeMap<CommitSeq, CommittedChange>, u64, u64)> {
    let mut changes = BTreeMap::new();
    let mut max_transaction = 0;
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
        if value.as_slice() != TREE_LIVE && value.as_slice() != TREE_DROPPED {
            return Err(Error::Corruption("malformed tree lifecycle value".into()));
        }
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
    Ok((changes, max_transaction, max_tree))
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
        assert_eq!(first.commit().expect("first commit").get(), 2);
        assert_eq!(second.commit().expect("disjoint commit").get(), 3);

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
        assert_eq!(transaction.commit().expect("commit"), snapshot);
        assert_eq!(database.commit_sequence().expect("head"), snapshot);
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
        std::mem::drop(drop);
        std::mem::drop(burned);
        database.close().expect("close");

        let reopened = TransactionDatabase::open(directory.path().join("db"), Options::for_test())
            .expect("reopen");
        let mut transaction = reopened.begin().expect("reopened begin");
        assert!(
            matches!(transaction.get(first, b"key"), Err(Error::TreeNotFound(tree)) if tree == first)
        );
        let next = transaction.create_tree().expect("next tree");
        assert!(next > burned_tree);
        transaction.abort().expect("abort");
        std::mem::drop(transaction);
        reopened.close().expect("close reopened");
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
}
