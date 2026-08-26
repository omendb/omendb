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

use crate::db::{BatchMutation, DB, DBMetrics, Options};
use crate::error::{Error, Result};
use crate::mvcc::{
    CurrentRecord, VersionStore, decode_current, encode_current, resolve_commit, visible_current,
};
use crate::storage::format::{CommitId, CommitPosition, CommitSeq, Lsn, TreeId, TxnId};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const TREE_RECORD_PREFIX: &[u8] = b"\x00seerdb/tree/";
const STATUS_RECORD_PREFIX: &[u8] = b"\x00seerdb/status/";
const CHANGE_RECORD_PREFIX: &[u8] = b"\x00seerdb/change/";
const LEASE_RECORD_PREFIX: &[u8] = b"\x00seerdb/lease/";
/// Longest accepted retention-lease name in bytes.
const MAX_LEASE_NAME_LEN: usize = 255;
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
    /// Number of current records rewritten (history cleared and/or frozen).
    pub current_records_rewritten: usize,
    /// Number of committed status entries pruned after freezing released
    /// their last placeholder reference.
    pub statuses_pruned: usize,
}

/// A commit that became visible in the committed-change stream.
///
/// `commit` is the stream position: every published commit sequence number
/// has exactly one change record, so consecutive records are gap-free while
/// a retention lease pins their range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedChange {
    /// Committed sequence number of the change (its stream position).
    pub commit: CommitSeq,
    /// Transaction that produced the change; zero for system writers.
    pub transaction: TxnId,
    /// Snapshot the transaction read at begin time.
    pub snapshot: CommitSeq,
    /// Trees whose lifecycle or contents changed.
    pub changed_trees: BTreeSet<TreeId>,
    /// Written keys, including tree reservations with no key writes.
    pub writes: BTreeSet<(TreeId, Vec<u8>)>,
}

/// A durable restart point for consumers: resume snapshots at `csn` after
/// replaying the physical log through `restart_lsn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotExport {
    /// Logical committed visibility order at export time.
    pub csn: CommitSeq,
    /// Durable WAL end position covering every commit up to `csn`.
    pub restart_lsn: Lsn,
}

/// Outcome of a committed-change retention pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeGcReport {
    /// Minimum floor across active retention leases, if any.
    pub floor: Option<CommitSeq>,
    /// Change records before the pass.
    pub changes_before: usize,
    /// Change records retained after the pass.
    pub changes_after: usize,
}

/// A durable retention lease pinning committed-change records.
///
/// The lease survives process restarts by design: drop does not release it,
/// because a crashed consumer must still find its history on reopen. Call
/// [`RetentionLease::release`] explicitly when the consumer is done. While
/// any lease is active, maintenance never prunes change records at or above
/// its floor; with no leases, nothing is pruned.
pub struct RetentionLease {
    runtime: Arc<Runtime>,
    name: Vec<u8>,
    released: AtomicBool,
}

impl RetentionLease {
    /// Lease identity used to reattach after restart.
    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Current durable floor pinned by this lease.
    pub fn floor(&self) -> Result<CommitSeq> {
        self.check_active()?;
        let leases = self
            .runtime
            .leases
            .lock()
            .map_err(|_| Error::Corruption("retention lease mutex is poisoned".into()))?;
        leases.get(&self.name).copied().ok_or_else(|| {
            Error::Corruption("retention lease record vanished from runtime state".into())
        })
    }

    fn check_active(&self) -> Result<()> {
        if self.released.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument(
                "retention lease was already released".into(),
            ));
        }
        Ok(())
    }

    /// Advance the floor to `csn`, durably releasing older history. The
    /// floor only moves forward; going backwards is accepted as a no-op and
    /// returns the unchanged floor.
    pub fn advance(&self, csn: CommitSeq) -> Result<CommitSeq> {
        self.check_active()?;
        // Floor moves consume a commit sequence number; join the lane first.
        let _lane = lock_publish(&self.runtime);
        let staged = take_staged(&self.runtime);
        let mut db = lock_db(&self.runtime);
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        publish_drained(&mut db, &self.runtime, staged)?;
        let mut leases = self
            .runtime
            .leases
            .lock()
            .map_err(|_| Error::Corruption("retention lease mutex is poisoned".into()))?;
        let current = leases.get(&self.name).copied().ok_or_else(|| {
            Error::Corruption("retention lease record vanished from runtime state".into())
        })?;
        if csn <= current {
            return Ok(current);
        }
        publish_lease_write(&mut db, &self.runtime, &mut leases, &self.name, Some(csn))?;
        Ok(csn)
    }

    /// Durably remove the lease, unpinning history it held back. The lease
    /// cannot be used afterwards; releasing twice reports an error without
    /// touching storage.
    pub fn release(&self) -> Result<()> {
        self.check_active()?;
        let _lane = lock_publish(&self.runtime);
        let staged = take_staged(&self.runtime);
        let mut db = lock_db(&self.runtime);
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        publish_drained(&mut db, &self.runtime, staged)?;
        let mut leases = self
            .runtime
            .leases
            .lock()
            .map_err(|_| Error::Corruption("retention lease mutex is poisoned".into()))?;
        if leases.get(&self.name).is_none() {
            return Err(Error::Corruption(
                "retention lease record vanished from runtime state".into(),
            ));
        }
        publish_lease_write(&mut db, &self.runtime, &mut leases, &self.name, None)?;
        self.released.store(true, Ordering::Release);
        Ok(())
    }
}

impl Drop for RetentionLease {
    fn drop(&mut self) {
        // Deliberate no-op: leases are durable consumer state and must
        // outlive careless handle drops. Release requires an explicit call.
    }
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
    leases: BTreeMap<Vec<u8>, CommitSeq>,
    max_transaction: u64,
    max_tree: u64,
}

/// A commit staged for publication: validated against published state and
/// queued work, carrying data mutations only. The publish lane assigns the
/// commit sequence number when it installs the wave, so assignment can never
/// collide with an in-flight publication.
struct StagedCommit {
    transaction: TxnId,
    /// Snapshot the transaction began at; recorded in the change record.
    snapshot: CommitSeq,
    /// Data mutations over tree/current records, built at stage time.
    mutations: Vec<BatchMutation>,
    changed_trees: BTreeSet<TreeId>,
    writes: BTreeSet<(TreeId, Vec<u8>)>,
    /// Filled by the publisher immediately before installation.
    assigned: CommitSeq,
    outcome: std::sync::mpsc::Sender<std::result::Result<CommitPosition, Arc<Error>>>,
}

/// Pending unpublished commits plus derived conflict indexes. Guarded by its
/// own mutex so validation runs concurrently with physical publication.
#[derive(Default)]
struct PrepareState {
    queue: VecDeque<StagedCommit>,
    /// Keys written by queued commits, for first-committer-wins against
    /// work that is not yet visible in storage.
    keys: BTreeSet<(TreeId, Vec<u8>)>,
    /// Trees with queued lifecycle or data changes.
    trees: BTreeSet<TreeId>,
}

struct Runtime {
    db: Mutex<DB>,
    versions: Mutex<VersionStore>,
    statuses: Mutex<BTreeMap<TxnId, CommitSeq>>,
    changes: Mutex<BTreeMap<CommitSeq, CommittedChange>>,
    leases: Mutex<BTreeMap<Vec<u8>, CommitSeq>>,
    active_snapshots: Mutex<ActiveSnapshots>,
    prepare: Mutex<PrepareState>,
    /// Publish lane: the holder drains staged commits before its own
    /// physical publication, keeping assign order equal to publish order.
    publish: Mutex<()>,
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
    read_ranges: BTreeSet<(TreeId, Vec<u8>, Option<Vec<u8>>)>,
    state: TransactionState,
    snapshot_registered: bool,
}

/// Ordered forward cursor over one tree at the transaction's fixed snapshot.
///
/// The cursor merges snapshot-visible storage with the transaction's own
/// staged writes, so it reads its own writes. Creating it registers a range
/// dependency: a concurrent commit that writes any key inside the scanned
/// range conflicts with this transaction's later commit, protecting reads
/// from phantoms under snapshot isolation.
pub struct Cursor<'a> {
    transaction: &'a Transaction,
    tree: TreeId,
    end: Option<Vec<u8>>,
    position: Option<Vec<u8>>,
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
            leases,
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
                leases: Mutex::new(leases),
                active_snapshots: Mutex::new(ActiveSnapshots::new()),
                prepare: Mutex::new(PrepareState::default()),
                publish: Mutex::new(()),
                next_transaction: AtomicU64::new(next_transaction),
                next_tree: AtomicU64::new(next_tree),
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Begin a fixed-snapshot transaction.
    pub fn begin(&self) -> Result<Transaction> {
        let id = allocate_id(&self.runtime.next_transaction, "transaction ID")?;
        // The snapshot registry is taken after the database guard's scope so
        // begin never holds the database lock while acquiring it; GC paths
        // lock in the opposite order.
        let (snapshot, snapshot_position) = {
            let db =
                self.runtime.db.lock().map_err(|_| {
                    Error::Corruption("transaction database mutex is poisoned".into())
                })?;
            if self.runtime.closed.load(Ordering::Acquire) {
                return Err(Error::InvalidArgument("database is closed".into()));
            }
            let durability = db.durability_status();
            if durability.write_fenced {
                return Err(Error::NeedsRecovery(
                    "transaction database is fenced; reopen required".into(),
                ));
            }
            let position = durability.commit_position;
            (position.csn, position)
        };
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
            read_ranges: BTreeSet::new(),
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

    /// Return engine-level metrics, including cumulative publication-phase
    /// wall-clock timing.
    pub fn metrics(&self) -> Result<DBMetrics> {
        let db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        db.metrics()
    }

    /// Compact logical MVCC history while preserving every active snapshot.
    pub fn gc_versions(&self) -> Result<VersionGcReport> {
        // GC rewrites current records, consuming a commit sequence number;
        // join the publish lane and settle staged commits first.
        let _lane = lock_publish(&self.runtime);
        let staged = take_staged(&self.runtime);
        let mut db = lock_db(&self.runtime);
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        publish_drained(&mut db, &self.runtime, staged)?;
        let mut version_store = self
            .runtime
            .versions
            .lock()
            .map_err(|_| Error::Corruption("MVCC version store mutex is poisoned".into()))?;
        let mut statuses = self
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
        let mut unfrozen = BTreeSet::new();
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

        // Retained undo versions still pin their creators' status entries.
        for id in &retained {
            let record = version_store.get(*id)?;
            if record.commit.get() == 0 {
                unfrozen.insert(record.transaction);
            }
        }

        // Durable status freezing: a committed status entry whose creator no
        // longer holds any placeholder reference can be pruned from storage
        // and memory. Freezing rewrites landed in the batch above; pruning
        // them in the same locked section keeps one consistent view.
        let prunable: Vec<TxnId> = statuses
            .keys()
            .filter(|transaction| !unfrozen.contains(transaction))
            .copied()
            .collect();
        let statuses_pruned = prunable.len();
        if !prunable.is_empty() {
            let deletions: Vec<BatchMutation> = prunable
                .iter()
                .map(|transaction| BatchMutation::Delete {
                    key: status_record_key(*transaction),
                })
                .collect();
            db.commit_batch(&deletions)?;
            for transaction in &prunable {
                statuses.remove(transaction);
            }
        }

        Ok(VersionGcReport {
            watermark,
            versions_before,
            versions_after,
            current_records_rewritten: rewrites.len(),
            statuses_pruned,
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

    /// Acquire or reattach to a durable retention lease named `name`.
    ///
    /// The lease pins committed-change records from its floor onward so a
    /// consumer can stream them without gaps across restarts. Reattaching to
    /// an existing lease with `start` at or below its floor is free; a higher
    /// `start` advances the floor durably. The floor never moves backwards:
    /// history the caller failed to pin may already be pruned, and reads that
    /// reach below the floor report [`Error::ChangesPruned`].
    pub fn acquire_change_lease(&self, name: &[u8], start: CommitSeq) -> Result<RetentionLease> {
        if name.is_empty() || name.len() > MAX_LEASE_NAME_LEN {
            return Err(Error::InvalidArgument(format!(
                "lease name must be 1..={MAX_LEASE_NAME_LEN} bytes"
            )));
        }
        if start.get() == 0 {
            return Err(Error::InvalidArgument(
                "lease start must be a nonzero commit sequence".into(),
            ));
        }
        let _lane = lock_publish(&self.runtime);
        let staged = take_staged(&self.runtime);
        let mut db = lock_db(&self.runtime);
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        publish_drained(&mut db, &self.runtime, staged)?;
        let mut leases = self
            .runtime
            .leases
            .lock()
            .map_err(|_| Error::Corruption("retention lease mutex is poisoned".into()))?;
        match leases.get(name) {
            Some(&floor) if start <= floor => (),
            _ => publish_lease_write(&mut db, &self.runtime, &mut leases, name, Some(start))?,
        }
        Ok(RetentionLease {
            runtime: Arc::clone(&self.runtime),
            name: name.to_vec(),
            released: AtomicBool::new(false),
        })
    }

    /// Read up to `limit` committed changes starting exactly at `from`.
    ///
    /// Records are returned in commit order with no gaps. While a retention
    /// lease covers the range this cannot fail for retention reasons; reads
    /// reaching below the oldest retained record report
    /// [`Error::ChangesPruned`]. A `from` above the current head returns an
    /// empty result.
    pub fn read_changes(&self, from: CommitSeq, limit: usize) -> Result<Vec<CommittedChange>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        let changes = lock_changes(&self.runtime);
        let head = db.durability_status().commit_position.csn;
        if from > head {
            return Ok(Vec::new());
        }
        let Some(&oldest) = changes.keys().next() else {
            return Err(Error::Corruption(
                "committed-change stream has no records despite published commits".into(),
            ));
        };
        if from < oldest {
            return Err(Error::ChangesPruned {
                requested: from,
                oldest,
            });
        }
        let mut out = Vec::with_capacity(limit.min(64));
        let mut expected = from;
        for (&commit, change) in changes.range(from..) {
            if commit != expected {
                return Err(Error::Corruption(format!(
                    "committed-change stream has a gap before {commit:?}"
                )));
            }
            out.push(change.clone());
            if out.len() == limit {
                break;
            }
            let Some(next) = expected.get().checked_add(1) else {
                break;
            };
            expected = CommitSeq::new(next);
        }
        Ok(out)
    }

    /// Oldest committed-change record still retained, if any.
    pub fn oldest_retained_change(&self) -> Result<Option<CommitSeq>> {
        Ok(lock_changes(&self.runtime).keys().next().copied())
    }

    /// Prune committed-change records strictly below the minimum active
    /// retention-lease floor. With no leases nothing is pruned, keeping the
    /// full history available until a consumer takes responsibility for it.
    pub fn gc_changes(&self) -> Result<ChangeGcReport> {
        // Pruning consumes a commit sequence number for its system record;
        // join the publish lane and settle staged commits first.
        let _lane = lock_publish(&self.runtime);
        let staged = take_staged(&self.runtime);
        let mut db = lock_db(&self.runtime);
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        publish_drained(&mut db, &self.runtime, staged)?;
        let leases = self
            .runtime
            .leases
            .lock()
            .map_err(|_| Error::Corruption("retention lease mutex is poisoned".into()))?;
        let mut changes = lock_changes(&self.runtime);
        let before = changes.len();
        let Some(floor) = leases.values().copied().min() else {
            return Ok(ChangeGcReport {
                floor: None,
                changes_before: before,
                changes_after: before,
            });
        };
        let stale_commits: Vec<CommitSeq> =
            changes.range(..floor).map(|(commit, _)| *commit).collect();
        if stale_commits.is_empty() {
            return Ok(ChangeGcReport {
                floor: Some(floor),
                changes_before: before,
                changes_after: before,
            });
        }
        let current = db.durability_status().commit_position.csn;
        let next = CommitSeq::new(
            current
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?,
        );
        // System change record keeps the CSN stream gap-free.
        let change = CommittedChange {
            commit: next,
            transaction: TxnId::new(0),
            snapshot: current,
            changed_trees: BTreeSet::new(),
            writes: BTreeSet::new(),
        };
        let mut mutations: Vec<BatchMutation> = stale_commits
            .iter()
            .map(|commit| BatchMutation::Delete {
                key: change_record_key(*commit),
            })
            .collect();
        mutations.push(BatchMutation::Put {
            key: change_record_key(next),
            value: encode_change(&change)?,
        });
        let expected = db.durability_status().commit_id;
        let status = db.commit_batch_at(expected, &mutations)?;
        let committed = status.commit_position.csn;
        if committed != next {
            return Err(Error::Corruption(format!(
                "change GC expected commit {:?}, storage published {:?}",
                next, committed
            )));
        }
        for commit in &stale_commits {
            changes.remove(commit);
        }
        changes.insert(committed, change);
        Ok(ChangeGcReport {
            floor: Some(floor),
            changes_before: before,
            changes_after: changes.len(),
        })
    }

    /// Export a durable restart point: resume logical snapshots at `csn`
    /// after replaying the physical log through `restart_lsn`.
    pub fn snapshot_export(&self) -> Result<SnapshotExport> {
        let db = self
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        let position = db.durability_status().commit_position;
        Ok(SnapshotExport {
            csn: position.csn,
            restart_lsn: position.lsn,
        })
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
        // Tree reservations consume a commit sequence number, so they join
        // the publish lane ahead of their inline publication.
        let _lane = lock_publish(self);
        let staged = take_staged(self);
        let mut db = lock_db(self);
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        publish_drained(&mut db, self, staged)?;
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
            commit: next,
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
        let expected = db.durability_status().commit_id;
        let status = db.commit_batch_at(expected, &mutations)?;
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

    /// Open an ordered forward cursor over `[start,end)` in key order.
    ///
    /// The cursor resolves visibility at this transaction's fixed snapshot and
    /// observes the transaction's own staged writes. Creating it registers a
    /// range dependency, so a concurrent commit writing any key inside the
    /// range conflicts with this transaction's commit (phantom protection).
    pub fn cursor(&mut self, tree: TreeId, start: &[u8], end: Option<&[u8]>) -> Result<Cursor<'_>> {
        self.check_active()?;
        self.check_tree_visible_for_read(tree)?;
        if let Some(end) = end
            && start > end
        {
            return Err(Error::InvalidArgument(
                "cursor end must not precede cursor start".into(),
            ));
        }
        self.read_ranges
            .insert((tree, start.to_vec(), end.map(ToOwned::to_owned)));
        Ok(Cursor {
            transaction: self,
            tree,
            end: end.map(ToOwned::to_owned),
            position: Some(start.to_vec()),
        })
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

impl Cursor<'_> {
    /// Advance to the next visible entry in key order.
    fn advance(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            let Some(position) = self.position.clone() else {
                return Ok(None);
            };
            if let Some(end) = &self.end
                && position.as_slice() >= end.as_slice()
            {
                self.position = None;
                return Ok(None);
            }
            let storage = self.storage_entry(&position)?;
            let staged = self.staged_entry(&position);
            let chosen = match (&storage, &staged) {
                (None, None) => {
                    self.position = None;
                    return Ok(None);
                }
                (Some((storage_key, _)), None) => storage_key.clone(),
                (None, Some((staged_key, _))) => staged_key.clone(),
                (Some((storage_key, _)), Some((staged_key, _))) => {
                    storage_key.min(staged_key).clone()
                }
            };
            self.position = Some(successor(&chosen));
            match staged.iter().find(|(key, _)| *key == chosen) {
                Some((_, Some(value))) => return Ok(Some((chosen, value.clone()))),
                // A staged delete shadows the storage entry; keep scanning.
                Some((_, None)) => continue,
                None => {
                    debug_assert_eq!(storage.as_ref().map(|(key, _)| key), Some(&chosen));
                    if let Some((_, value)) = storage {
                        return Ok(Some((chosen, value)));
                    }
                    continue;
                }
            }
        }
    }

    /// First snapshot-visible storage entry at or after `position`.
    fn storage_entry(&self, position: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.transaction.created.contains(&self.tree) {
            return Ok(None);
        }
        let prefix = tree_prefix(self.tree);
        let physical_start = append(&prefix, position);
        let physical_end = self
            .end
            .as_deref()
            .map(|end| append(&prefix, end))
            .unwrap_or_else(|| prefix_end(&prefix));
        let db = self
            .transaction
            .runtime
            .db
            .lock()
            .map_err(|_| Error::Corruption("transaction database mutex is poisoned".into()))?;
        let mut version_store = self
            .transaction
            .runtime
            .versions
            .lock()
            .map_err(|_| Error::Corruption("MVCC version store mutex is poisoned".into()))?;
        let statuses = self
            .transaction
            .runtime
            .statuses
            .lock()
            .map_err(|_| Error::Corruption("transaction status mutex is poisoned".into()))?;
        for (key, value) in db.range(&physical_start, &physical_end)? {
            let Some(user_key) = decode_tree_key(self.tree, &key) else {
                continue;
            };
            let current = decode_current(Some(&value))?;
            if let Some(version) = visible_current(
                &mut version_store,
                &statuses,
                &current,
                self.transaction.snapshot,
            )? && let Some(value) = version.value
            {
                return Ok(Some((user_key, value)));
            }
        }
        Ok(None)
    }

    /// First staged write at or after `position`, as `(key, staged value)`.
    fn staged_entry(&self, position: &[u8]) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        self.transaction
            .writes
            .range((self.tree, position.to_vec())..)
            .take_while(|((tree, key), _)| {
                *tree == self.tree && self.end.as_ref().is_none_or(|end| key.as_slice() < end)
            })
            .next()
            .map(|((_, key), value)| (key.clone(), value.clone()))
    }
}

impl Iterator for Cursor<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.advance().transpose()
    }
}

/// Smallest key strictly greater than `key` across all byte strings.
fn successor(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0);
    next
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
    let receiver = stage_commit(transaction)?;
    // Opportunistically become the group-commit leader; every committer
    // drains the queue, so staged work always reaches the publish lane.
    let _ = publish_staged(&transaction.runtime);
    match receiver.recv() {
        Ok(outcome) => outcome.map_err(materialize_failure),
        Err(_) => Err(Error::Corruption(
            "commit publisher dropped the transaction outcome".into(),
        )),
    }
}

/// Reconstruct an owned error for a waiter from the shared publisher
/// failure. String-carrying variants survive exactly; the rest degrade to a
/// corruption report because publication failures are terminal for the group.
fn materialize_failure(failure: Arc<Error>) -> Error {
    match &*failure {
        Error::NeedsRecovery(message) => Error::NeedsRecovery(message.clone()),
        Error::InvalidArgument(message) => Error::InvalidArgument(message.clone()),
        Error::Wal(message) => Error::Wal(message.clone()),
        Error::Corruption(message) => Error::Corruption(message.clone()),
        Error::Backpressure {
            required,
            available,
        } => Error::Backpressure {
            required: *required,
            available: *available,
        },
        other => Error::Corruption(format!("publication failed: {other:?}")),
    }
}

/// Validate the transaction against published state and queued-but-
/// unpublished work, assign the next commit sequence number, and enqueue the
/// physical batch. Locks are held only for validation and staging, never for
/// WAL or version-store syncs.
fn stage_commit(
    transaction: &Transaction,
) -> Result<std::sync::mpsc::Receiver<std::result::Result<CommitPosition, Arc<Error>>>> {
    let mut prepare = lock_prepare(&transaction.runtime);
    let db = transaction
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
    let statuses = transaction
        .runtime
        .statuses
        .lock()
        .map_err(|_| Error::Corruption("transaction status mutex is poisoned".into()))?;
    let current = db.durability_status().commit_position.csn;
    validate_conflicts(transaction, &db, &statuses, current)?;
    reject_queued_conflicts(transaction, &prepare)?;

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

    // Status and change records are built by the publisher once the wave's
    // sequence numbers are assigned; staged mutations stay data-only.

    let (sender, receiver) = std::sync::mpsc::channel();
    prepare.keys.extend(writes.iter().cloned());
    prepare.trees.extend(changed_trees.iter().copied());
    prepare.trees.extend(writes.iter().map(|(tree, _)| *tree));
    prepare.queue.push_back(StagedCommit {
        transaction: transaction.id,
        snapshot: transaction.snapshot,
        mutations,
        changed_trees,
        writes,
        assigned: CommitSeq::new(0),
        outcome: sender,
    });
    Ok(receiver)
}

/// Reject conflicts against commits that are validated and CSN-assigned but
/// not yet installed. Without this pass two transactions could both win the
/// same key against published state; first-committer-wins is decided in
/// assignment order instead.
fn reject_queued_conflicts(transaction: &Transaction, prepare: &PrepareState) -> Result<()> {
    for (tree, key) in transaction.writes.keys() {
        if prepare.keys.contains(&(*tree, key.clone())) {
            return Err(Error::WriteConflict {
                tree: *tree,
                key: key.clone(),
            });
        }
    }
    for tree in &transaction.dropped {
        if prepare.trees.contains(tree) {
            return Err(Error::TreeConflict(*tree));
        }
    }
    for staged in &prepare.queue {
        for (tree, start, end) in &transaction.read_ranges {
            for (changed_tree, changed_key) in &staged.writes {
                if *changed_tree == *tree
                    && changed_key.as_slice() >= start.as_slice()
                    && end
                        .as_ref()
                        .is_none_or(|end| changed_key.as_slice() < end.as_slice())
                {
                    // The queued writer has no assigned sequence yet; the
                    // transaction's own snapshot names the visibility point
                    // the phantom appeared after.
                    return Err(Error::SerializationConflict {
                        expected: CommitId::new(transaction.snapshot.get()),
                        current: CommitId::new(transaction.snapshot.get()),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Publish everything staged so far as one atomic durable batch. The publish
/// lane serializes install order to match assignment order; control-plane
/// writers call this before their own inline publication.
fn publish_staged(runtime: &Runtime) -> Result<()> {
    let _lane = lock_publish(runtime);
    // Swap the queue before touching the database handle: staging holds the
    // prepare mutex while waiting for the database lock, so taking the
    // database lock first would deadlock against an in-flight staging.
    let staged = take_staged(runtime);
    if staged.queue.is_empty() {
        return Ok(());
    }
    let mut db = lock_db(runtime);
    publish_drained(&mut db, runtime, staged)
}

/// Take the staged-commit queue and its conflict indexes, resetting them for
/// the next assignment wave. Must run before any caller acquires the database
/// lock; staging holds the prepare mutex across its own database-lock wait.
fn take_staged(runtime: &Runtime) -> PrepareState {
    let mut prepare = lock_prepare(runtime);
    std::mem::take(&mut *prepare)
}

/// Publish previously staged commits while both the publish lane and the
/// database handle are held by the caller. Control-plane writers run this
/// before their own inline publication so every consumer of a commit sequence
/// number passes through one ordered lane.
fn publish_drained(db: &mut DB, runtime: &Runtime, mut queue: PrepareState) -> Result<()> {
    if queue.queue.is_empty() {
        return Ok(());
    }

    let head = db.durability_status().commit_position;
    // Assignment happens here, under the lane with the database handle held:
    // wave members take head+1..=head+n in queue order, so published
    // sequence numbers are contiguous and collision-free by construction.
    for (position, staged) in queue.queue.iter_mut().enumerate() {
        let assigned = head
            .csn
            .get()
            .checked_add(position as u64 + 1)
            .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?;
        staged.assigned = CommitSeq::new(assigned);
    }
    let expected = db.durability_status().commit_id;
    let assigned_last = queue.queue.back().expect("non-empty queue").assigned;
    let batches: Vec<Vec<BatchMutation>> = queue
        .queue
        .iter()
        .map(|staged| {
            let mut batch = staged.mutations.clone();
            batch.push(BatchMutation::Put {
                key: status_record_key(staged.transaction),
                value: encode_status(staged.assigned),
            });
            let change = CommittedChange {
                commit: staged.assigned,
                transaction: staged.transaction,
                snapshot: staged.snapshot,
                changed_trees: staged.changed_trees.clone(),
                writes: staged.writes.clone(),
            };
            batch.push(BatchMutation::Put {
                key: change_record_key(staged.assigned),
                value: encode_change(&change).expect("change record fits"),
            });
            batch
        })
        .collect();

    // Before-images must reach disk before any current record names them.
    // A sync failure here precedes all publication, so it is a certain
    // abort: retryable, no fence.
    let sync_outcome = lock_versions(runtime).sync();
    if let Err(error) = sync_outcome {
        resolve_group(queue.queue.make_contiguous(), None, Arc::new(error));
        return Ok(());
    }

    match db.commit_group_at(expected, &batches) {
        Ok(status) => {
            if status.commit_position.csn != assigned_last {
                let error = Arc::new(Error::Corruption(format!(
                    "group publication expected {:?}, storage published {:?}",
                    assigned_last, status.commit_position.csn
                )));
                db.fence_writes();
                resolve_group(queue.queue.make_contiguous(), None, error);
                return Ok(());
            }
            {
                let mut statuses = lock_statuses(runtime);
                let mut changes = lock_changes(runtime);
                for staged in &queue.queue {
                    statuses.insert(staged.transaction, staged.assigned);
                    changes.insert(
                        staged.assigned,
                        CommittedChange {
                            commit: staged.assigned,
                            transaction: staged.transaction,
                            snapshot: staged.snapshot,
                            changed_trees: staged.changed_trees.clone(),
                            writes: staged.writes.clone(),
                        },
                    );
                }
            }
            for staged in &queue.queue {
                let position = CommitPosition::new(staged.assigned, status.commit_position.lsn);
                let _ = staged.outcome.send(Ok(position));
            }
            Ok(())
        }
        Err(error) => {
            // The engine owns fence semantics; forward the shared outcome.
            let uncertain =
                matches!(error, Error::NeedsRecovery(_)) || db.durability_status().write_fenced;
            let failure = Arc::new(error);
            let members = queue.queue.make_contiguous();
            if uncertain {
                resolve_group_uncertain(members, failure);
            } else {
                resolve_group(members, None, failure);
            }
            Ok(())
        }
    }
}

/// Settle every member with `position` on success or the shared failure.
fn resolve_group(queue: &[StagedCommit], position: Option<CommitPosition>, failure: Arc<Error>) {
    for staged in queue {
        let _ = match position {
            Some(position) => staged.outcome.send(Ok(position)),
            None => staged.outcome.send(Err(Arc::clone(&failure))),
        };
    }
}

/// Uncertain publication: each transaction may have committed at its
/// assigned sequence; reopen resolves the single atomic group outcome.
fn resolve_group_uncertain(queue: &[StagedCommit], failure: Arc<Error>) {
    for staged in queue {
        let message = format!(
            "transaction {:?} may have committed at {:?}: {failure}",
            staged.transaction, staged.assigned
        );
        let _ = staged
            .outcome
            .send(Err(Arc::new(Error::NeedsRecovery(message))));
    }
}

fn lock_prepare(runtime: &Runtime) -> std::sync::MutexGuard<'_, PrepareState> {
    match runtime.prepare.lock() {
        Ok(prepare) => prepare,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn lock_publish(runtime: &Runtime) -> std::sync::MutexGuard<'_, ()> {
    match runtime.publish.lock() {
        Ok(lane) => lane,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn lock_statuses(runtime: &Runtime) -> std::sync::MutexGuard<'_, BTreeMap<TxnId, CommitSeq>> {
    match runtime.statuses.lock() {
        Ok(statuses) => statuses,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn lock_db(runtime: &Runtime) -> std::sync::MutexGuard<'_, DB> {
    match runtime.db.lock() {
        Ok(db) => db,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn lock_versions(runtime: &Runtime) -> std::sync::MutexGuard<'_, VersionStore> {
    match runtime.versions.lock() {
        Ok(versions) => versions,
        Err(poisoned) => poisoned.into_inner(),
    }
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
            let mut current = decode_current(Some(&value))?;
            retain_current_chain(
                self.version_store,
                self.statuses,
                &current,
                self.watermark,
                self.retained,
            )?;
            // Status freezing: once a creator's committed CSN is durable it
            // can never change, so writing it into the record makes the
            // record self-describing and releases its status-table entry.
            let resolved = resolve_commit(self.statuses, current.transaction, current.commit)?;
            let clear_history = match self.watermark {
                None => true,
                Some(watermark) => resolved <= watermark,
            };
            let needs_freeze = current.commit.get() != resolved.get();
            let needs_undo_clear = clear_history && current.undo_head.is_some();
            if needs_freeze || needs_undo_clear {
                if needs_freeze {
                    current.commit = resolved;
                }
                if needs_undo_clear {
                    current.undo_head = None;
                }
                self.rewrites.push(BatchMutation::Put {
                    key,
                    value: encode_current(&current)?,
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
    validate_range_dependencies(transaction, db, current)
}

/// Reject commits whose registered read ranges saw a phantom: any concurrent
/// commit after the transaction's snapshot that wrote inside the range.
fn validate_range_dependencies(
    transaction: &Transaction,
    db: &DB,
    current: CommitSeq,
) -> Result<()> {
    for (tree, start, end) in &transaction.read_ranges {
        let prefix = CHANGE_RECORD_PREFIX;
        for (key, value) in db.range(prefix, &prefix_end(prefix))? {
            let Some(commit) = decode_change_commit(&key) else {
                continue;
            };
            if commit <= transaction.snapshot || commit > current {
                continue;
            }
            let change = decode_change(&key, &value)?;
            for (changed_tree, changed_key) in &change.writes {
                if *changed_tree != *tree
                    || changed_key.as_slice() < start.as_slice()
                    || end
                        .as_ref()
                        .is_some_and(|end| changed_key.as_slice() >= end.as_slice())
                {
                    continue;
                }
                return Err(Error::SerializationConflict {
                    expected: CommitId::new(transaction.snapshot.get()),
                    current: CommitId::new(current.get()),
                });
            }
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
        let change = decode_change(&key, &value)?;
        if change.snapshot > change.commit {
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
    let mut leases = BTreeMap::new();
    let lease_end = prefix_end(LEASE_RECORD_PREFIX);
    for (key, value) in db.range(LEASE_RECORD_PREFIX, &lease_end)? {
        let name = &key[LEASE_RECORD_PREFIX.len()..];
        if name.is_empty() || name.len() > MAX_LEASE_NAME_LEN {
            return Err(Error::Corruption(
                "retention lease record has malformed name".into(),
            ));
        }
        let floor = decode_lease_floor(&value)?;
        if leases.insert(name.to_vec(), floor).is_some() {
            return Err(Error::Corruption("duplicate retention lease record".into()));
        }
    }
    Ok(ControlState {
        statuses,
        changes,
        leases,
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

fn decode_change(key: &[u8], bytes: &[u8]) -> Result<CommittedChange> {
    let commit = decode_change_commit(key)
        .ok_or_else(|| Error::Corruption("malformed transaction change key".into()))?;
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
        commit,
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

fn lease_record_key(name: &[u8]) -> Vec<u8> {
    append(LEASE_RECORD_PREFIX, name)
}

fn encode_lease_floor(floor: CommitSeq) -> Vec<u8> {
    floor.get().to_be_bytes().to_vec()
}

fn decode_lease_floor(bytes: &[u8]) -> Result<CommitSeq> {
    let raw = <[u8; 8]>::try_from(bytes)
        .map_err(|_| Error::Corruption("malformed retention lease record".into()))?;
    let floor = CommitSeq::new(u64::from_be_bytes(raw));
    if floor.get() == 0 {
        return Err(Error::Corruption("retention lease floor is zero".into()));
    }
    Ok(floor)
}

/// Publish a retention-lease mutation (set or clear one floor) as one atomic
/// durable batch. Every batch consumes exactly one commit sequence number,
/// so an empty system change record is written alongside to keep the
/// committed-change stream gap-free.
fn publish_lease_write(
    db: &mut DB,
    runtime: &Runtime,
    leases: &mut BTreeMap<Vec<u8>, CommitSeq>,
    name: &[u8],
    floor: Option<CommitSeq>,
) -> Result<()> {
    let current = db.durability_status().commit_position.csn;
    let next = CommitSeq::new(
        current
            .get()
            .checked_add(1)
            .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?,
    );
    let change = CommittedChange {
        commit: next,
        transaction: TxnId::new(0),
        snapshot: current,
        changed_trees: BTreeSet::new(),
        writes: BTreeSet::new(),
    };
    let lease_mutation = match floor {
        Some(csn) => BatchMutation::Put {
            key: lease_record_key(name),
            value: encode_lease_floor(csn),
        },
        None => BatchMutation::Delete {
            key: lease_record_key(name),
        },
    };
    let mutations = [
        lease_mutation,
        BatchMutation::Put {
            key: change_record_key(next),
            value: encode_change(&change)?,
        },
    ];
    let status = db.commit_batch_at(db.durability_status().commit_id, &mutations)?;
    let committed = status.commit_position.csn;
    if committed != next {
        return Err(Error::Corruption(format!(
            "retention write expected commit {:?}, storage published {:?}",
            next, committed
        )));
    }
    match floor {
        Some(csn) => {
            leases.insert(name.to_vec(), csn);
        }
        None => {
            leases.remove(name);
        }
    }
    lock_changes(runtime).insert(committed, change);
    Ok(())
}

fn decode_change_commit(key: &[u8]) -> Option<CommitSeq> {
    key.strip_prefix(CHANGE_RECORD_PREFIX)
        .and_then(|rest| <[u8; 8]>::try_from(rest).ok())
        .map(|bytes| CommitSeq::new(u64::from_be_bytes(bytes)))
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

    fn commit_key(database: &TransactionDatabase, tree: TreeId, key: &[u8]) {
        let mut transaction = database.begin().expect("begin");
        transaction.put(tree, key, key).expect("put");
        transaction.commit().expect("commit");
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

        // Advancing releases older records from the stream.
        let head = reopened.snapshot_export().expect("export").csn;
        reattached.advance(head).expect("advance");
        let report = reopened.gc_changes().expect("gc");
        // Records pinned at the floor plus the advance/GC system records.
        assert_eq!(report.changes_after, 3);
        assert_eq!(
            reopened.oldest_retained_change().expect("oldest"),
            Some(head)
        );
        assert!(matches!(
            reopened.read_changes(CommitSeq::new(1), 4),
            Err(Error::ChangesPruned { requested, oldest })
                if requested == CommitSeq::new(1) && oldest == head
        ));
        assert_eq!(reopened.read_changes(head, 4).expect("from floor").len(), 3);

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
        let reader = database.begin().expect("reader");
        assert!(reader.get(shared, b"contested").expect("read").is_some());
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
