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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const TREE_RECORD_PREFIX: &[u8] = b"\x00seerdb/tree/";
const STATUS_RECORD_PREFIX: &[u8] = b"\x00seerdb/status/";
const CHANGE_RECORD_PREFIX: &[u8] = b"\x00seerdb/change/";
const LEASE_RECORD_PREFIX: &[u8] = b"\x00seerdb/lease/";
const ALLOCATOR_RECORD_KEY: &[u8] = b"\x00seerdb/allocator";
const ALLOCATOR_MAGIC: &[u8; 4] = b"SAL1";
/// Coalescing window a group-commit leader waits for concurrent committers
/// to land in the staging queue before the durability barrier starts. Only
/// entered while pending transactions are not yet queued, so a lone
/// committer pays nothing.
const COALESCE_WINDOW: std::time::Duration = std::time::Duration::from_micros(750);

/// Coalescing waits a leader performs before draining. Bounded so a
/// long-running transaction (oversized staging, slow validation) cannot pin
/// the publish lane behind it: after this many windows the leader publishes
/// what has landed and the straggler joins the next wave.
const MAX_COALESCE_WAITS: usize = 4;

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
    /// Effective floor used for pruning, bounded by the minimum active lease
    /// floor and the oldest active transaction snapshot.
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

/// A commit staged for publication: validated against unpublished work
/// only, carrying the raw write set. The publish lane assigns the commit
/// sequence number when it installs the wave, so assignment can never
/// collide with an in-flight publication, and it builds the physical
/// mutations (before-images, current records) under the database lock so
/// staging never waits behind an in-flight wave sync.
struct StagedCommit {
    transaction: TxnId,
    /// Snapshot the transaction began at; recorded in the change record.
    snapshot: CommitSeq,
    /// Raw staged writes in tree/key order; the publisher encodes current
    /// records with before-images at wave time. Deletes are `None`.
    writes: BTreeMap<(TreeId, Vec<u8>), Option<Vec<u8>>>,
    /// Trees this commit creates or drops, with their target lifecycle.
    tree_lifecycles: BTreeMap<TreeId, &'static [u8]>,
    /// Registered read ranges (phantom checks), replayed by the publish
    /// lane against the durable change stream.
    read_ranges: BTreeSet<(TreeId, Vec<u8>, Option<Vec<u8>>)>,
    /// Registered point reads, validated against queued writes at stage
    /// time and against published current records at wave time.
    point_reads: BTreeSet<(TreeId, Vec<u8>)>,
    changed_trees: BTreeSet<TreeId>,
    /// Key set written by this commit, for the queue's overlay indexes.
    write_keys: BTreeSet<(TreeId, Vec<u8>)>,
    /// Encoded change record, preflighted at stage time so the publish
    /// lane only assigns its key. The assigned sequence number lives in
    /// the record key, not the payload, so the encoding cannot depend on
    /// publication order.
    change_record: Vec<u8>,
    /// Physical mutations over tree/current records, built by the publish
    /// lane at wave time from `writes` and `tree_lifecycles`.
    mutations: Vec<BatchMutation>,
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

/// Conflict indexes for a queue that has left `PrepareState` but has
/// not completed physical publication yet. Stagers must consult both
/// queued and publishing indexes so draining cannot create a window
/// where an unpublished write disappears from conflict detection.
#[derive(Default)]
struct PublishingState {
    keys: BTreeSet<(TreeId, Vec<u8>)>,
    trees: BTreeSet<TreeId>,
}

struct PublishingGuard<'a> {
    runtime: &'a Runtime,
}

impl Drop for PublishingGuard<'_> {
    fn drop(&mut self) {
        let mut publishing = lock_publishing(self.runtime);
        publishing.keys.clear();
        publishing.trees.clear();
    }
}

struct DrainedCommits<'a> {
    queue: VecDeque<StagedCommit>,
    _publishing: PublishingGuard<'a>,
}

struct Runtime {
    db: Mutex<DB>,
    versions: Mutex<VersionStore>,
    statuses: Mutex<BTreeMap<TxnId, CommitSeq>>,
    changes: Mutex<BTreeMap<CommitSeq, CommittedChange>>,
    leases: Mutex<BTreeMap<Vec<u8>, CommitSeq>>,
    active_snapshots: Mutex<ActiveSnapshots>,
    prepare: Mutex<PrepareState>,
    publishing: Mutex<PublishingState>,
    /// Publish lane: the holder drains staged commits before its own
    /// physical publication, keeping assign order equal to publish order.
    publish: Mutex<()>,
    /// Mirror of the database's published commit position for `begin`,
    /// guarded by its own tiny mutex so a wave's database-guard hold never
    /// blocks transaction begins. Updated at wave install under the
    /// database lock; always a real published position, never speculative.
    published_position: Mutex<CommitPosition>,
    /// Lock-free mirror of the database's write fence for `begin`.
    write_fenced: AtomicBool,
    /// Transactions begun but not yet committed or aborted. Publish-lane
    /// leaders use it to detect committers that will stage soon and
    /// coalesce them into the wave before the durability barrier starts;
    /// staging no longer needs the database guard, so those committers can
    /// actually arrive while the leader waits.
    pending_transactions: AtomicUsize,
    /// Mirror of each tree's durable lifecycle record at the published
    /// head, so per-operation tree-visibility checks never take the
    /// database guard (a wave holds it through its durability sync).
    /// Updated at wave install and by tree reservations under the publish
    /// lane; readers pair it with the in-memory statuses and version store.
    tree_lifecycle_mirror: Mutex<BTreeMap<TreeId, CurrentRecord>>,
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
    /// Exact keys read through `get`, registered so a concurrent commit
    /// that overwrites or deletes one after this transaction's snapshot
    /// fails this transaction's commit (read-write anti-dependency).
    /// Point reads validate through an O(1) current-record lookup at
    /// publication instead of the range machinery's change-stream scan.
    point_reads: BTreeSet<(TreeId, Vec<u8>)>,
    /// Snapshot-resolved tree visibility, computed once per tree: the fixed
    /// snapshot makes the answer immutable for the transaction's lifetime.
    tree_states: BTreeMap<TreeId, bool>,
    state: TransactionState,
    snapshot_registered: bool,
    /// Whether this transaction still holds its slot in the publish lane's
    /// pending-committers count. The count drops at stage time (the commit
    /// has landed in the queue) so group-commit leaders wait only for
    /// committers that can still arrive, never for threads already staged.
    pending_counted: bool,
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
        let sync_class = options.sync_class;
        let db = DB::create(path, options)?;
        let versions = VersionStore::create(db.directory().join(VERSION_STORE_FILE), sync_class)?;
        db.sync_directory_entry()?;
        Self::from_db(db, versions)
    }

    /// Open an existing transactional database at `path`.
    pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let sync_class = options.sync_class;
        let db = DB::open(path, options)?;
        let versions = VersionStore::open(db.directory().join(VERSION_STORE_FILE), sync_class)?;
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
        let opening_status = db.durability_status();
        let opening_tree_mirror = load_control_state_tree_mirror(&db);
        Ok(Self {
            runtime: Arc::new(Runtime {
                db: Mutex::new(db),
                versions: Mutex::new(versions),
                statuses: Mutex::new(statuses),
                changes: Mutex::new(changes),
                leases: Mutex::new(leases),
                active_snapshots: Mutex::new(ActiveSnapshots::new()),
                prepare: Mutex::new(PrepareState::default()),
                publishing: Mutex::new(PublishingState::default()),
                publish: Mutex::new(()),
                published_position: Mutex::new(opening_status.commit_position),
                write_fenced: AtomicBool::new(opening_status.write_fenced),
                pending_transactions: AtomicUsize::new(0),
                tree_lifecycle_mirror: Mutex::new(opening_tree_mirror),
                next_transaction: AtomicU64::new(next_transaction),
                next_tree: AtomicU64::new(next_tree),
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Begin a fixed-snapshot transaction.
    pub fn begin(&self) -> Result<Transaction> {
        let id = allocate_id(&self.runtime.next_transaction, "transaction ID")?;
        // The head and fence state come from lock-free mirrors, not the
        // database guard: a wave may hold the guard for its whole sync, and
        // transactions must still begin during that window so their commits
        // can coalesce into the next wave. A stale mirror is always a real
        // published position, so a snapshot taken from it is valid; the
        // publish lane re-checks fenced state under the guard.
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        if self.runtime.write_fenced.load(Ordering::Acquire) {
            return Err(Error::NeedsRecovery(
                "transaction database is fenced; reopen required".into(),
            ));
        }
        #[cfg(test)]
        tests::pause_begin(tests::BeginPause::CheckedOpen);
        // Admission holds the registry before sampling the head. GC either
        // observes this snapshot or finishes its watermark read before we
        // sample a head at least as new as the state it is pruning.
        // This path takes only registry -> published_position, never db.
        let mut snapshots =
            self.runtime.active_snapshots.lock().map_err(|_| {
                Error::Corruption("active snapshot registry mutex is poisoned".into())
            })?;
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        let (snapshot, snapshot_position) = {
            let position =
                self.runtime.published_position.lock().map_err(|_| {
                    Error::Corruption("published position mutex is poisoned".into())
                })?;
            (position.csn, *position)
        };
        #[cfg(test)]
        tests::pause_begin(tests::BeginPause::SampledHead);
        snapshots.insert(TxnId::new(id), snapshot);
        self.runtime
            .pending_transactions
            .fetch_add(1, Ordering::AcqRel);
        drop(snapshots);
        Ok(Transaction {
            runtime: Arc::clone(&self.runtime),
            id: TxnId::new(id),
            snapshot,
            snapshot_position,
            writes: BTreeMap::new(),
            created: BTreeSet::new(),
            dropped: BTreeSet::new(),
            read_ranges: BTreeSet::new(),
            point_reads: BTreeSet::new(),
            tree_states: BTreeMap::new(),
            state: TransactionState::Active,
            snapshot_registered: true,
            pending_counted: true,
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

    /// Compact logical MVCC history while preserving every active snapshot
    /// and durable retention-lease floor.
    pub fn gc_versions(&self) -> Result<VersionGcReport> {
        // GC rewrites current records as maintenance: join the publish lane
        // and settle staged commits first so the rewrite set is computed
        // against the settled head, then publish rewrites, status pruning,
        // and the allocator high-water as ONE maintenance generation that
        // consumes no commit sequence number.
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
        // Retention history is pinned by both active snapshots and retention
        // leases: a CDC consumer holding a lease resolves its pinned change
        // records against the row state visible at the lease floor, so undo
        // history at or below the oldest lease floor must survive too.
        // Lock order note: every lease and snapshot lock site takes the
        // active-snapshot registry last, so holding it and then taking the
        // lease registry cannot deadlock.
        let watermark = self.runtime.retention_watermark()?;
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

        // Retained undo versions still pin their creators' status entries.
        for id in &retained {
            let record = version_store.get(*id)?;
            if record.commit.get() == 0 {
                unfrozen.insert(record.transaction);
            }
        }

        // Durable status freezing: a committed status entry whose creator no
        // longer holds any placeholder reference can be pruned from storage
        // and memory. The prune set is computed BEFORE any records vanish so
        // the pruned identities are covered by the high-water.
        let prunable: Vec<TxnId> = statuses
            .keys()
            .filter(|transaction| !unfrozen.contains(transaction))
            .copied()
            .collect();
        let statuses_pruned = prunable.len();

        // One maintenance batch: record rewrites, status deletions, and the
        // allocator high-water covering every identity whose implying records
        // are being removed. The high-water is written BEFORE pruning makes
        // those identities un-reconstructible, so reopen can never reuse an
        // issued transaction or tree ID.
        if !rewrites.is_empty() || !prunable.is_empty() {
            let mut mutations = rewrites.clone();
            for transaction in &prunable {
                mutations.push(BatchMutation::Delete {
                    key: status_record_key(*transaction),
                });
            }
            mutations.push(BatchMutation::Put {
                key: ALLOCATOR_RECORD_KEY.to_vec(),
                value: encode_allocator_high_water(&AllocatorHighWater {
                    next_transaction: self.runtime.next_transaction.load(Ordering::Acquire).max(
                        statuses
                            .keys()
                            .next_back()
                            .map_or(0, |transaction| transaction.get()),
                    ),
                    next_tree: self.runtime.next_tree.load(Ordering::Acquire),
                }),
            });
            commit_maintenance(&mut db, &self.runtime, &mutations)?;
            // The rewrites above may have frozen or cleared lifecycle-record
            // history while the batch pruned the status entries those records
            // pointed at; rebuild the mirror so visibility checks never see
            // a pruned indirection.
            if let Ok(mut mirror) = self.runtime.tree_lifecycle_mirror.lock() {
                *mirror = load_control_state_tree_mirror(&db);
            }
        }

        // Compact the version store only after the rewritten current records
        // are durable: a live current record must never name an undo version
        // that no longer exists in the store.
        let (_, versions_after) = match version_store.compact(&retained) {
            Ok(counts) => counts,
            Err(error) => {
                db.fence_writes();
                self.runtime.write_fenced.store(true, Ordering::Release);
                return Err(error);
            }
        };

        for transaction in &prunable {
            statuses.remove(transaction);
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
        // Match maintenance's db -> versions -> registry order. Keep
        // admission excluded through the closed transition, so a begin that
        // already passed the fast closed check cannot register after close.
        let versions = self
            .runtime
            .versions
            .lock()
            .map_err(|_| Error::Corruption("MVCC version store mutex is poisoned".into()))?;
        let snapshots =
            self.runtime.active_snapshots.lock().map_err(|_| {
                Error::Corruption("active snapshot registry mutex is poisoned".into())
            })?;
        if !snapshots.is_empty() {
            return Err(Error::InvalidArgument(
                "cannot close database while transactions are active".into(),
            ));
        }
        if self.runtime.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        versions.sync()?;
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
    /// empty result. A short read that ends below the head reports
    /// [`Error::Corruption`]: every commit sequence number carries exactly
    /// one change record, so a missing tail record means the durable stream
    /// is inconsistent rather than exhausted.
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
                return Ok(out);
            }
            let Some(next) = expected.get().checked_add(1) else {
                return Ok(out);
            };
            expected = CommitSeq::new(next);
        }
        // The map ended before the limit was reached. Every commit sequence
        // number below the head has exactly one change record — logical
        // commits, tree reservations, everything — so a tail ending below
        // the head means the durable stream is inconsistent: a CSN was
        // published without its record. Report it instead of returning a
        // short read that silently advances a consumer's checkpoint.
        let last = out.last().map(|change| change.commit);
        let incomplete_tail = match last {
            Some(last) => last < head,
            None => from <= head,
        };
        if incomplete_tail {
            return Err(Error::Corruption(format!(
                "committed-change stream ends at {:?} below the head {head:?}",
                last.unwrap_or(from)
            )));
        }
        Ok(out)
    }

    /// Oldest committed-change record still retained, if any.
    pub fn oldest_retained_change(&self) -> Result<Option<CommitSeq>> {
        Ok(lock_changes(&self.runtime).keys().next().copied())
    }

    /// Prune committed-change records below the older of the minimum active
    /// retention-lease floor and the oldest active transaction snapshot: a
    /// lease alone must not unpin records that active transactions still
    /// need for commit-time range re-validation. With no leases nothing is
    /// pruned, keeping the full history available until a consumer takes
    /// responsibility for it.
    pub fn gc_changes(&self) -> Result<ChangeGcReport> {
        // Pruning is maintenance, not a visibility event: it joins the
        // publish lane only to settle staged commits against the same head
        // the change map already reflects, then publishes the deletions
        // without consuming a commit sequence number.
        let _lane = lock_publish(&self.runtime);
        let staged = take_staged(&self.runtime);
        let mut db = lock_db(&self.runtime);
        if self.runtime.closed.load(Ordering::Acquire) {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        publish_drained(&mut db, &self.runtime, staged)?;
        // Read the oldest active snapshot before taking the lease guard so
        // the two registries are never nested in opposite orders elsewhere.
        // Lease mutations all hold the publish lane this function already
        // holds, so the read order does not affect which floors are visible.
        let oldest_active = self.runtime.oldest_active_snapshot()?;
        let leases = self
            .runtime
            .leases
            .lock()
            .map_err(|_| Error::Corruption("retention lease mutex is poisoned".into()))?;
        let mut changes = lock_changes(&self.runtime);
        // Pruning is bounded by both retention floors. Active transactions
        // re-validate read ranges at commit time by walking durable change
        // records in (snapshot, current]; records above the oldest active
        // snapshot are still needed for phantom conflict detection, so a
        // lease floor alone must not unpin them.
        let before = changes.len();
        let Some(floor) = leases
            .values()
            .copied()
            .min()
            .map(|lease| oldest_active.map_or(lease, |snapshot| lease.min(snapshot)))
        else {
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
        let mutations: Vec<BatchMutation> = stale_commits
            .iter()
            .map(|commit| BatchMutation::Delete {
                key: change_record_key(*commit),
            })
            .collect();
        commit_maintenance(&mut db, &self.runtime, &mutations)?;
        for commit in &stale_commits {
            changes.remove(commit);
        }
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

    /// Minimum CSN whose retention history must survive GC: the older of the
    /// oldest active transaction snapshot and the oldest durable lease
    /// floor. Returns `None` when neither pins anything.
    fn retention_watermark(&self) -> Result<Option<CommitSeq>> {
        let snapshot = self.oldest_active_snapshot()?;
        let lease = self
            .leases
            .lock()
            .map_err(|_| Error::Corruption("retention lease mutex is poisoned".into()))?
            .values()
            .copied()
            .min();
        Ok(match (snapshot, lease) {
            (Some(snapshot), Some(lease)) => Some(snapshot.min(lease)),
            (snapshot, lease) => snapshot.or(lease),
        })
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
        if let Ok(mut mirror) = self.tree_lifecycle_mirror.lock() {
            mirror.insert(
                tree,
                CurrentRecord {
                    transaction: owner,
                    commit: CommitSeq::new(0),
                    undo_head,
                    value: Some(TREE_RESERVED.to_vec()),
                },
            );
        }
        if let Ok(mut published) = self.published_position.lock() {
            *published = status.commit_position;
        }
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
    ///
    /// The read registers an anti-dependency: a concurrent commit that
    /// overwrites or deletes this key after the transaction's snapshot
    /// fails this transaction's commit, closing the write-skew hole a
    /// snapshot point read would otherwise leave open.
    pub fn get(&mut self, tree: TreeId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_active()?;
        self.check_tree_visible_for_read(tree)?;
        if !self.writes.contains_key(&(tree, key.to_vec())) {
            // Only snapshot-sourced reads register: a read served from this
            // transaction's own staged write is not a snapshot dependency.
            self.point_reads.insert((tree, key.to_vec()));
        }
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
    ///
    /// The scanned range registers a read dependency exactly as `cursor`
    /// does: a concurrent commit writing inside the range after the
    /// transaction's snapshot fails this transaction's commit (phantom
    /// protection), so a scan-then-write transaction cannot fork history.
    pub fn scan(
        &mut self,
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
        self.read_ranges
            .insert((tree, start.to_vec(), end.map(ToOwned::to_owned)));
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
        if self.pending_counted {
            // Read-only commits and aborts never stage; they release their
            // committer slot here. Writers already released theirs at stage
            // time, when the commit landed in the queue.
            self.runtime
                .pending_transactions
                .fetch_sub(1, Ordering::AcqRel);
            self.pending_counted = false;
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

    /// Resolve tree visibility without the cache, for read paths that take
    /// the database guard for their data reads anyway. Write paths use the
    /// cached [`Self::tree_visible`] instead.
    fn tree_visible_uncached(&self, tree: TreeId) -> Result<bool> {
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

    /// Resolve whether a tree is visible at this transaction's snapshot.
    /// The snapshot fixes the answer for the transaction's lifetime, so it
    /// is resolved once per tree and cached: concurrent transactions keep
    /// staging their commits while an in-flight wave holds the database
    /// guard, instead of blocking every operation behind it.
    fn tree_visible(&mut self, tree: TreeId) -> Result<bool> {
        if let Some(visible) = self.tree_states.get(&tree) {
            return Ok(*visible);
        }
        let visible = {
            let lifecycle = self
                .runtime
                .tree_lifecycle_mirror
                .lock()
                .map_err(|_| Error::Corruption("tree lifecycle mirror mutex is poisoned".into()))?
                .get(&tree)
                .cloned()
                .ok_or(Error::TreeNotFound(tree))?;
            let mut version_store =
                self.runtime.versions.lock().map_err(|_| {
                    Error::Corruption("MVCC version store mutex is poisoned".into())
                })?;
            let statuses =
                self.runtime.statuses.lock().map_err(|_| {
                    Error::Corruption("transaction status mutex is poisoned".into())
                })?;
            visible_current(&mut version_store, &statuses, &lifecycle, self.snapshot)?
                .and_then(|version| version.value)
                .is_some_and(|value| value.as_slice() == TREE_LIVE)
        };
        self.tree_states.insert(tree, visible);
        Ok(visible)
    }

    fn check_tree_visible_for_read(&self, tree: TreeId) -> Result<()> {
        if self.dropped.contains(&tree) {
            return Err(Error::TreeNotFound(tree));
        }
        if self.created.contains(&tree) || self.tree_visible_uncached(tree)? {
            Ok(())
        } else {
            Err(Error::TreeNotFound(tree))
        }
    }

    fn check_tree_for_write(&mut self, tree: TreeId) -> Result<()> {
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
        if self.pending_counted {
            // A transaction dropped without commit or abort still held a
            // pending-committer slot (wire-server probes and any caller that
            // discards a snapshot early). Group-commit leaders wait only for
            // committers that can still arrive, so a dropped slot that never
            // releases makes every later commit leader sleep out the full
            // coalescing window. Release it here, mirroring the read-only
            // commit and abort paths.
            self.runtime
                .pending_transactions
                .fetch_sub(1, Ordering::AcqRel);
            self.pending_counted = false;
        }
    }
}

fn commit_transaction(transaction: &mut Transaction) -> Result<CommitPosition> {
    let receiver = stage_commit(transaction)?;
    // Leader/follower group commit. A committer whose outcome is already
    // settled returns immediately, never touching the publish lane: the
    // lane is held through a whole wave, so queueing on it would serialize
    // every follower's return one by one and collapse waves to singletons.
    // Instead, an unsettled committer briefly polls its outcome (the
    // in-flight leader is publishing the queue it staged into) and only
    // leads its own wave when the lane is free. Progress is guaranteed:
    // every loop either returns a settled outcome or leads a wave that
    // drains the queue the committer's own staged work sits in, and a
    // leading wave always settles the leader's commit (it drained the
    // queue after staging into it).
    let settled = loop {
        if let Ok(result) = receiver.try_recv() {
            break Some(result);
        }
        match transaction.runtime.publish.try_lock() {
            Ok(lane) => {
                let _ = publish_with_lane(&transaction.runtime, lane);
            }
            Err(_) => {
                // A leader holds the lane through its wave. Wait for the
                // outcome without queueing behind it — and keep the message
                // when it arrives: recv_timeout consumes from the channel,
                // so discarding its result would drop the only outcome this
                // committer will ever receive and leave it spinning as an
                // empty-queue leader forever.
                match receiver.recv_timeout(COALESCE_WINDOW) {
                    Ok(result) => break Some(result),
                    Err(_) => continue,
                }
            }
        }
    };
    match settled {
        Some(result) => result.map_err(materialize_failure),
        None => Err(Error::Corruption(
            "commit publisher dropped the transaction outcome".into(),
        )),
    }
}

/// Reconstruct an owned error for a waiter from the shared publisher
/// failure. String-carrying variants and member-local conflict outcomes
/// survive exactly; the rest degrade to a corruption report because
/// publication failures are terminal for the group.
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
        // Wave-time validation failures are member-local certain no-ops:
        // the member published nothing, so its exact conflict surfaces to
        // the committer unchanged.
        Error::WriteConflict { tree, key } => Error::WriteConflict {
            tree: *tree,
            key: key.clone(),
        },
        Error::TreeConflict(tree) => Error::TreeConflict(*tree),
        Error::SerializationConflict { expected, current } => Error::SerializationConflict {
            expected: *expected,
            current: *current,
        },
        Error::TreeNotFound(tree) => Error::TreeNotFound(*tree),
        other => Error::Corruption(format!("publication failed: {other:?}")),
    }
}

/// Validate the transaction against published state and queued-but-
/// unpublished work, assign the next commit sequence number, and enqueue the
/// physical batch. Locks are held only for validation and staging, never for
/// WAL or version-store syncs.
/// unpublished work and enqueue the raw write set. Staging never touches
/// the database handle: it must not wait behind an in-flight wave sync, or
/// concurrent commits can never accumulate into a wave. Published-state
/// validation and mutation building happen in the publish lane at wave
/// time, under the database lock the wave already holds.
fn stage_commit(
    transaction: &mut Transaction,
) -> Result<std::sync::mpsc::Receiver<std::result::Result<CommitPosition, Arc<Error>>>> {
    let mut prepare = lock_prepare(&transaction.runtime);
    if transaction.runtime.closed.load(Ordering::Acquire) {
        return Err(Error::InvalidArgument("database is closed".into()));
    }
    // A point read of a key this commit also writes is subsumed by the
    // write-write check on that key: any concurrent writer to the key
    // already aborts this commit, so validating the read again is pure
    // publish-lane cost. Prune those reads; the write-skew shape the
    // certifier must catch is read K / write J != K, which stays.
    transaction
        .point_reads
        .retain(|(tree, key)| !transaction.writes.contains_key(&(*tree, key.clone())));
    reject_unpublished_conflicts(transaction, &prepare.keys, &prepare.trees)?;
    {
        let publishing = lock_publishing(&transaction.runtime);
        reject_unpublished_conflicts(transaction, &publishing.keys, &publishing.trees)?;
    }

    let mut changed_trees = transaction.created.clone();
    changed_trees.extend(transaction.dropped.iter().copied());
    let mut write_keys = BTreeSet::new();
    for (tree, key) in transaction.writes.keys() {
        if transaction.dropped.contains(tree) {
            continue;
        }
        write_keys.insert((*tree, key.clone()));
    }
    let mut writes = BTreeMap::new();
    for ((tree, key), value) in &transaction.writes {
        if transaction.dropped.contains(tree) {
            continue;
        }
        writes.insert((*tree, key.clone()), value.clone());
    }
    let mut tree_lifecycles = BTreeMap::new();
    for tree in &changed_trees {
        let lifecycle = if transaction.dropped.contains(tree) {
            TREE_DROPPED
        } else {
            TREE_LIVE
        };
        tree_lifecycles.insert(*tree, lifecycle);
    }

    // The change record is encoded and bounds-checked here, before the
    // commit joins the queue, so the serialized publish lane never panics
    // on an oversized transaction and rejected staging leaves no partial
    // state behind.
    let change_record = encode_change_body(
        transaction.id,
        transaction.snapshot,
        &changed_trees,
        &write_keys,
    )?;

    let (sender, receiver) = std::sync::mpsc::channel();
    prepare.keys.extend(write_keys.iter().cloned());
    prepare.trees.extend(changed_trees.iter().copied());
    prepare
        .trees
        .extend(write_keys.iter().map(|(tree, _)| *tree));
    prepare.queue.push_back(StagedCommit {
        transaction: transaction.id,
        snapshot: transaction.snapshot,
        writes,
        tree_lifecycles,
        read_ranges: transaction.read_ranges.clone(),
        point_reads: transaction.point_reads.clone(),
        changed_trees,
        write_keys,
        change_record,
        mutations: Vec::new(),
        assigned: CommitSeq::new(0),
        outcome: sender,
    });
    // The commit has landed in the queue: the transaction is no longer a
    // will-stage-soon candidate, so leaders stop waiting on it. This also
    // lets the caller pass through publish_staged without polluting the
    // signal for the next wave's leader.
    if transaction.pending_counted {
        transaction
            .runtime
            .pending_transactions
            .fetch_sub(1, Ordering::AcqRel);
        transaction.pending_counted = false;
    }
    Ok(receiver)
}

/// Reject conflicts against validated work that is not yet visible in the
/// published database image. The indexes cover both the staging queue and a
/// queue already drained into the physical publication lane.
fn reject_unpublished_conflicts(
    transaction: &Transaction,
    keys: &BTreeSet<(TreeId, Vec<u8>)>,
    trees: &BTreeSet<TreeId>,
) -> Result<()> {
    for (tree, key) in transaction.writes.keys() {
        if keys.contains(&(*tree, key.clone())) {
            return Err(Error::WriteConflict {
                tree: *tree,
                key: key.clone(),
            });
        }
    }
    for tree in &transaction.dropped {
        if trees.contains(tree) {
            return Err(Error::TreeConflict(*tree));
        }
    }
    for (tree, start, end) in &transaction.read_ranges {
        for (changed_tree, changed_key) in keys {
            if *changed_tree == *tree
                && changed_key.as_slice() >= start.as_slice()
                && end
                    .as_ref()
                    .is_none_or(|end| changed_key.as_slice() < end.as_slice())
            {
                // The unpublished writer has no visible sequence yet; the
                // transaction's own snapshot names the visibility point the
                // phantom appeared after.
                return Err(Error::SerializationConflict {
                    expected: CommitId::new(transaction.snapshot.get()),
                    current: CommitId::new(transaction.snapshot.get()),
                });
            }
        }
    }
    for (tree, key) in &transaction.point_reads {
        if keys.contains(&(*tree, key.clone())) {
            return Err(Error::SerializationConflict {
                expected: CommitId::new(transaction.snapshot.get()),
                current: CommitId::new(transaction.snapshot.get()),
            });
        }
    }
    Ok(())
}

/// Publish everything staged so far as one atomic durable batch. The publish
/// lane serializes install order to match assignment order; control-plane
/// writers call this before their own inline publication.
/// Lead a wave while already holding the publish lane. Control-plane writers
/// and committers that won a lane race share this body: coalesce briefly,
/// drain everything staged, publish one wave.
fn publish_with_lane(runtime: &Runtime, _lane: std::sync::MutexGuard<'_, ()>) -> Result<()> {
    // Coalescing window: publication is serialized behind one durability
    // barrier per wave, so a wave that drains only its taker collapses the
    // group and every commit pays the full barrier. Staging no longer needs
    // the database guard, so committers between `begin` and the queue can
    // actually land while the leader waits: give them a bounded window, the
    // same tradeoff PostgreSQL makes with `commit_delay`. The wait continues
    // only while transactions not yet in the queue are running; the
    // single-client path sees none and never waits.
    let mut waits = 0;
    // Baseline is the queue depth at entry, not zero: a leader that already
    // drained work into the queue must not wait just because the queue is
    // non-empty. It waits only for commits that land AFTER it took the lane
    // (a growing queue), which is the coalescing this window exists for.
    let mut last_queued = staged_queue_len(runtime);
    while waits < MAX_COALESCE_WAITS {
        let queued = staged_queue_len(runtime);
        let pending = runtime.pending_transactions.load(Ordering::Acquire);
        // Wait while transactions that have not staged yet are running, or
        // while fresh commits keep landing in the queue (a busy client pack
        // re-loops in microseconds, so a growing queue means more will
        // arrive). Stop as soon as neither holds.
        if pending <= queued && queued <= last_queued {
            break;
        }
        std::thread::sleep(COALESCE_WINDOW);
        waits += 1;
        last_queued = queued;
    }
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

/// Current staging-queue depth, without draining it.
fn staged_queue_len(runtime: &Runtime) -> usize {
    lock_prepare(runtime).queue.len()
}

/// Take the staged-commit queue and its conflict indexes, resetting them for
/// the next assignment wave. Must run before any caller acquires the database
/// lock; staging holds the prepare mutex across its own database-lock wait.
fn take_staged(runtime: &Runtime) -> DrainedCommits<'_> {
    let mut prepare = lock_prepare(runtime);
    let queue = std::mem::take(&mut prepare.queue);
    let keys = std::mem::take(&mut prepare.keys);
    let trees = std::mem::take(&mut prepare.trees);
    let mut publishing = lock_publishing(runtime);
    debug_assert!(publishing.keys.is_empty());
    debug_assert!(publishing.trees.is_empty());
    publishing.keys = keys;
    publishing.trees = trees;
    drop(publishing);
    DrainedCommits {
        queue,
        _publishing: PublishingGuard { runtime },
    }
}

/// Publish previously staged commits while both the publish lane and the
/// database handle are held by the caller. Control-plane writers run this
/// before their own inline publication so every consumer of a commit sequence
/// number passes through one ordered lane.
fn publish_drained(db: &mut DB, runtime: &Runtime, mut queue: DrainedCommits<'_>) -> Result<()> {
    if queue.queue.is_empty() {
        return Ok(());
    }

    let head = db.durability_status().commit_position;
    // Build phase: validate each member against the settled published state
    // and build its physical mutations (before-images, current records),
    // member by member in queue order. Staging enqueued only overlay-
    // validated raw writes, so this is the first point where the member's
    // conflicts with published state are decided — the same checks that
    // used to run at stage time, now against the pre-wave head under the
    // database lock the wave already holds. A failed member resolves with
    // its conflict error and is skipped; a later member validating against
    // state that includes only the survivors is exactly as strict as the
    // pre-pipeline stage-time validation.
    {
        let mut version_store = lock_versions(runtime);
        let statuses = lock_statuses(runtime);
        let mut members: VecDeque<StagedCommit> = VecDeque::new();
        for mut staged in queue.queue.drain(..) {
            let outcome = validate_against_published(&staged, db, &statuses).and_then(|()| {
                // Only surviving members consume CSNs: physical publication
                // advances by the number of batches, not the original queue
                // length. Assign before encoding, but reuse this position if
                // mutation construction rejects the member.
                let assigned = head
                    .csn
                    .get()
                    .checked_add(members.len() as u64 + 1)
                    .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?;
                staged.assigned = CommitSeq::new(assigned);
                build_mutations(&mut staged, db, &mut version_store)
            });
            match outcome {
                Ok(()) => members.push_back(staged),
                Err(error) => {
                    let _ = staged.outcome.send(Err(Arc::new(error)));
                }
            }
        }
        queue.queue = members;
    }
    if queue.queue.is_empty() {
        return Ok(());
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
            // The payload was encoded and bounds-checked at stage time; the
            // publisher only assigns its key, which carries the sequence
            // number.
            batch.push(BatchMutation::Put {
                key: change_record_key(staged.assigned),
                value: staged.change_record.clone(),
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
                runtime.write_fenced.store(true, Ordering::Release);
                resolve_group(queue.queue.make_contiguous(), None, error);
                return Ok(());
            }
            if let Ok(mut published) = runtime.published_position.lock() {
                *published = status.commit_position;
            }
            if let Ok(mut mirror) = runtime.tree_lifecycle_mirror.lock() {
                for staged in &queue.queue {
                    for tree in staged.tree_lifecycles.keys() {
                        if let Some(BatchMutation::Put { value, .. }) = staged
                            .mutations
                            .iter()
                            .find(|mutation| {
                                matches!(mutation, BatchMutation::Put { key, .. } if *key == tree_record_key(*tree))
                            })
                            && let Ok(record) = decode_current(Some(value))
                        {
                            mirror.insert(*tree, record);
                        }
                    }
                }
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
                            writes: staged.write_keys.clone(),
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
            let fenced = db.durability_status().write_fenced;
            if fenced {
                runtime.write_fenced.store(true, Ordering::Release);
            }
            let uncertain = matches!(error, Error::NeedsRecovery(_)) || fenced;
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

fn lock_publishing(runtime: &Runtime) -> std::sync::MutexGuard<'_, PublishingState> {
    match runtime.publishing.lock() {
        Ok(publishing) => publishing,
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

/// Validate a staged commit against the settled published state: the same
/// checks that used to run at stage time, moved into the publish lane so
/// staging never waits behind a wave sync. Runs under the database lock
/// against the pre-wave head, which is at least as strict as stage-time
/// validation: any write that landed between stage and now is caught here.
fn validate_against_published(
    staged: &StagedCommit,
    db: &DB,
    statuses: &BTreeMap<TxnId, CommitSeq>,
) -> Result<()> {
    let current = db.durability_status().commit_position.csn;
    if staged.snapshot > current {
        return Err(Error::SerializationConflict {
            expected: CommitId::new(staged.snapshot.get()),
            current: CommitId::new(current.get()),
        });
    }

    for (tree, lifecycle) in &staged.tree_lifecycles {
        let current_record = decode_current(db.get(&tree_record_key(*tree))?.as_deref())?;
        if *lifecycle == TREE_LIVE
            && current_record.transaction != staged.transaction
            && current_record.value.as_deref() != Some(TREE_RESERVED)
        {
            // Creating a tree whose reservation is not ours.
            return Err(Error::TreeConflict(*tree));
        }
        let current_commit =
            resolve_commit(statuses, current_record.transaction, current_record.commit)?;
        if current_commit > staged.snapshot && current_record.transaction != staged.transaction {
            return Err(Error::TreeConflict(*tree));
        }
        if *lifecycle == TREE_DROPPED
            && tree_has_conflicting_write(db, statuses, *tree, staged.snapshot, staged.transaction)?
        {
            return Err(Error::TreeConflict(*tree));
        }
    }

    for (tree, key) in staged.writes.keys() {
        if staged.tree_lifecycles.contains_key(tree) {
            // Writes into a tree this commit creates or drops take their
            // conflict from the lifecycle record.
            continue;
        }
        let lifecycle = decode_current(db.get(&tree_record_key(*tree))?.as_deref())?;
        let lifecycle_commit = resolve_commit(statuses, lifecycle.transaction, lifecycle.commit)?;
        if lifecycle_commit > staged.snapshot && lifecycle.transaction != staged.transaction {
            return Err(Error::TreeConflict(*tree));
        }
        let current_record = decode_current(db.get(&tree_key(*tree, key))?.as_deref())?;
        let current_commit =
            resolve_commit(statuses, current_record.transaction, current_record.commit)?;
        if current_commit > staged.snapshot && current_record.transaction != staged.transaction {
            return Err(Error::WriteConflict {
                tree: *tree,
                key: key.clone(),
            });
        }
    }
    // Read-write anti-dependency: every registered point read must still
    // resolve to the version this transaction's snapshot saw. The current
    // record carries the latest committer, so one lookup per read decides
    // whether a concurrent transaction overwrote or deleted the key after
    // our snapshot — the write-skew certification point.
    for (tree, key) in &staged.point_reads {
        let current_record = decode_current(db.get(&tree_key(*tree, key))?.as_deref())?;
        let current_commit =
            resolve_commit(statuses, current_record.transaction, current_record.commit)?;
        if current_commit > staged.snapshot && current_record.transaction != staged.transaction {
            return Err(Error::SerializationConflict {
                expected: CommitId::new(staged.snapshot.get()),
                current: CommitId::new(current.get()),
            });
        }
    }
    validate_staged_range_dependencies(staged, db, current)
}

/// Reject commits whose registered read ranges saw a phantom: any concurrent
/// commit after the transaction's snapshot that wrote inside the range.
///
/// Change records are keyed `prefix + commit.to_be_bytes()`, so they sort
/// in commit order: the scan starts at `snapshot + 1`, and the B-tree seek
/// skips every record at or below the snapshot instead of decoding it.
/// Work is proportional to commits since the snapshot, not to history.
fn validate_staged_range_dependencies(
    staged: &StagedCommit,
    db: &DB,
    current: CommitSeq,
) -> Result<()> {
    for (tree, start, end) in &staged.read_ranges {
        let first = staged
            .snapshot
            .get()
            .checked_add(1)
            .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?;
        let scan_start = change_record_key(CommitSeq::new(first));
        // Inclusive of `current`: a commit at the head itself is a
        // registered-range phantom candidate. The pre-wave head read by
        // validate_against_published is `current`, and records above it
        // are not yet visible, so they are excluded by the end bound
        // rather than decoded and filtered.
        let last = current
            .get()
            .checked_add(1)
            .ok_or_else(|| Error::Wal("commit sequence exhausted".into()))?;
        let scan_end = change_record_key(CommitSeq::new(last));
        for (key, value) in db.range(&scan_start, &scan_end)? {
            let Some(commit) = decode_change_commit(&key) else {
                continue;
            };
            if commit > current {
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
                    expected: CommitId::new(staged.snapshot.get()),
                    current: CommitId::new(current.get()),
                });
            }
        }
    }
    Ok(())
}

/// Build the physical mutations for a staged commit: before-images for the
/// overwritten current records, then the new current records themselves.
/// Runs in the publish lane under the database lock; the version store is
/// held by the caller.
fn build_mutations(
    staged: &mut StagedCommit,
    db: &DB,
    version_store: &mut VersionStore,
) -> Result<()> {
    let mut mutations = Vec::new();
    for ((tree, key), value) in &staged.writes {
        let storage_key = tree_key(*tree, key);
        let current_record = decode_current(db.get(&storage_key)?.as_deref())?;
        let undo_head = append_before_image(version_store, &current_record)?;
        mutations.push(BatchMutation::Put {
            key: storage_key,
            value: encode_current(&CurrentRecord {
                transaction: staged.transaction,
                commit: CommitSeq::new(0),
                undo_head,
                value: value.clone(),
            })?,
        });
    }
    for (tree, lifecycle) in &staged.tree_lifecycles {
        let lifecycle_key = tree_record_key(*tree);
        let current_record = decode_current(db.get(&lifecycle_key)?.as_deref())?;
        let undo_head = append_before_image(version_store, &current_record)?;
        mutations.push(BatchMutation::Put {
            key: lifecycle_key,
            value: encode_current(&CurrentRecord {
                transaction: staged.transaction,
                commit: CommitSeq::new(0),
                undo_head,
                value: Some(lifecycle.to_vec()),
            })?,
        });
    }
    staged.mutations = mutations;
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

/// Load the durable tree lifecycle mirror for `open`. Reads the same
/// lifecycle records `load_control_state` validates; kept in a small map so
/// runtime visibility checks skip the database guard.
fn load_control_state_tree_mirror(db: &DB) -> BTreeMap<TreeId, CurrentRecord> {
    let mut mirror = BTreeMap::new();
    let tree_end = prefix_end(TREE_RECORD_PREFIX);
    let Ok(entries) = db.range(TREE_RECORD_PREFIX, &tree_end) else {
        return mirror;
    };
    for (key, value) in entries {
        let Ok(tree) = key[TREE_RECORD_PREFIX.len()..].try_into() else {
            continue;
        };
        let tree = u64::from_be_bytes(tree);
        if let Ok(current) = decode_current(Some(&value)) {
            mirror.insert(TreeId::new(tree), current);
        }
    }
    mirror
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
    // The durable allocator high-water covers identities whose implying
    // records (status, change, lifecycle) maintenance already pruned: it is
    // a floor, never an upper bound, and merges with whatever records still
    // survive.
    if let Some(value) = db.get(ALLOCATOR_RECORD_KEY)? {
        let water = decode_allocator_high_water(&value)?;
        max_transaction = max_transaction.max(water.next_transaction.saturating_sub(1));
        max_tree = max_tree.max(water.next_tree.saturating_sub(1));
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

/// Encode the change-record payload. The stream position lives in the
/// record key, not the body, so the body can be built and bounds-checked
/// before the publish lane assigns the sequence number.
fn encode_change_body(
    transaction: TxnId,
    snapshot: CommitSeq,
    changed_trees: &BTreeSet<TreeId>,
    writes: &BTreeSet<(TreeId, Vec<u8>)>,
) -> Result<Vec<u8>> {
    let tree_count = u32::try_from(changed_trees.len())
        .map_err(|_| Error::InvalidArgument("too many changed trees".into()))?;
    let write_count = u32::try_from(writes.len())
        .map_err(|_| Error::InvalidArgument("too many transaction writes".into()))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CHANGE_MAGIC);
    bytes.extend_from_slice(&transaction.get().to_be_bytes());
    bytes.extend_from_slice(&snapshot.get().to_be_bytes());
    bytes.extend_from_slice(&tree_count.to_be_bytes());
    for tree in changed_trees {
        bytes.extend_from_slice(&tree.get().to_be_bytes());
    }
    bytes.extend_from_slice(&write_count.to_be_bytes());
    for (tree, key) in writes {
        let length = u32::try_from(key.len())
            .map_err(|_| Error::InvalidArgument("transaction key is too large".into()))?;
        bytes.extend_from_slice(&tree.get().to_be_bytes());
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(key);
    }
    if bytes.len() > MAX_CHANGE_RECORD_BYTES {
        return Err(Error::InvalidArgument(
            "transaction change record exceeds the size limit".into(),
        ));
    }
    Ok(bytes)
}

fn encode_change(change: &CommittedChange) -> Result<Vec<u8>> {
    encode_change_body(
        change.transaction,
        change.snapshot,
        &change.changed_trees,
        &change.writes,
    )
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

/// Durable allocator high-water: the transaction and tree identities whose
/// records (status, change, or lifecycle) may already have been pruned by
/// maintenance. Persisted before the records that currently imply them are
/// removed, so reopen never reuses a previously issued identity.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct AllocatorHighWater {
    next_transaction: u64,
    next_tree: u64,
}

fn encode_allocator_high_water(water: &AllocatorHighWater) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20);
    bytes.extend_from_slice(ALLOCATOR_MAGIC);
    bytes.extend_from_slice(&water.next_transaction.to_be_bytes());
    bytes.extend_from_slice(&water.next_tree.to_be_bytes());
    bytes
}

fn decode_allocator_high_water(bytes: &[u8]) -> Result<AllocatorHighWater> {
    if bytes.len() != 20 || &bytes[..4] != ALLOCATOR_MAGIC {
        return Err(Error::Corruption(
            "malformed allocator high-water record".into(),
        ));
    }
    Ok(AllocatorHighWater {
        next_transaction: u64::from_be_bytes(bytes[4..12].try_into().expect("checked length")),
        next_tree: u64::from_be_bytes(bytes[12..20].try_into().expect("checked length")),
    })
}

/// Publish a retention-lease mutation (set or clear one floor) as one atomic
/// maintenance batch. Lease state is consumer bookkeeping, not a logical
/// visibility event: no commit sequence number is consumed and no
/// committed-change record is written.
/// Publish a maintenance batch, mirroring a database write fence into the
/// lock-free flag so `begin` rejects new transactions without taking the
/// database guard.
fn commit_maintenance(db: &mut DB, runtime: &Runtime, mutations: &[BatchMutation]) -> Result<()> {
    if let Err(error) = db.commit_maintenance_batch(mutations) {
        if db.durability_status().write_fenced {
            runtime.write_fenced.store(true, Ordering::Release);
        }
        return Err(error);
    }
    Ok(())
}

fn publish_lease_write(
    db: &mut DB,
    runtime: &Runtime,
    leases: &mut BTreeMap<Vec<u8>, CommitSeq>,
    name: &[u8],
    floor: Option<CommitSeq>,
) -> Result<()> {
    let lease_mutation = match floor {
        Some(csn) => BatchMutation::Put {
            key: lease_record_key(name),
            value: encode_lease_floor(csn),
        },
        None => BatchMutation::Delete {
            key: lease_record_key(name),
        },
    };
    commit_maintenance(db, runtime, &[lease_mutation])?;
    match floor {
        Some(csn) => {
            leases.insert(name.to_vec(), csn);
        }
        None => {
            leases.remove(name);
        }
    }
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
mod tests;
