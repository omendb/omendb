//! Database entry point.
//!
//! The `DB` struct is the main entry point for the storage engine.
//! It owns all components and provides the public API.

mod options;

pub use options::{BlobStorageMode, Options};

use crate::allocator::PageAllocator;
use crate::blob::BlobManager;
use crate::btree::{BTree, BlobPointer, LookupResult, MAX_KEY_SIZE, PAGE_SIZE, RangeCursor};
use crate::buffer::{BufferManager, BufferStats};
use crate::concurrency::TransactionManager;
use crate::error::{CheckFailureKind, Error, Result};
use crate::mvcc::{PMT, PageMapping};
use crate::recovery::{ParseStatus, RecordType, SyncPolicy, WalManager, WalRecord};
use crate::space::{Device, DeviceOptions, preallocate_file, reserve_file};
use crate::storage::format::{
    CommitId, CommitRecord, DatabaseId, FORMAT_VERSION, GenerationId, HistoryId,
    MANIFEST_SLOT_SIZE, Manifest, ManifestHistory, ManifestStore, PmtCheckpointId, RetainedRoot,
    RetentionRegistry, ReuseAttempt, ReuseLedger, SnapshotId,
};
use crate::storage::{StorageEngine, StorageMetrics, StorageReadView};
use fs2::FileExt;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt as PositionalFileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt as PositionalFileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[cfg(any(test, feature = "fault-injection"))]
use std::cell::Cell;

#[cfg(any(test, feature = "fault-injection"))]
thread_local! {
    static FAIL_NEXT_ATOMIC_RENAME: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_WAL_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_WAL_AFTER_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_WAL_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_WAL_AFTER_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_AFTER_MANIFEST: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_WAL_TRUNCATE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_ATOMIC_SHORT_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_ATOMIC_TORN_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_AFTER_BLOB_REWRITE_IMAGE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_AFTER_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_CATALOG_RENAME: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_CATALOG_SHORT_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_BLOB_SEGMENT_CATALOG_TORN_WRITE: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_HISTORY_PRUNE_DIRECTORY_SYNC: Cell<bool> = const { Cell::new(false) };
}

/// File names for the database.
const DATA_FILE: &str = "seerdb.data";
const BLOB_FILE: &str = "seerdb.blob";
const BLOB_DELTA_FILE: &str = "seerdb.blob.delta";
const BLOB_SEGMENT_PREFIX: &str = "seerdb.blob.segment.";
const BLOB_RESERVATION_FILE: &str = "seerdb.blob.reserve";
const BLOB_REWRITE_BACKUP_FILE: &str = "seerdb.blob.rewrite-old";
const WAL_FILE: &str = "seerdb.wal";
const WAL_RESERVATION_FILE: &str = "seerdb.wal.reserve";
const META_FILE: &str = "seerdb.meta";
const MANIFEST_FILE: &str = "MANIFEST";
const MANIFEST_HISTORY_FILE: &str = "seerdb.manifest-history";
const REUSE_LEDGER_FILE: &str = "seerdb.reuse-ledger";
const RETENTION_FILE: &str = "seerdb.retained";
const LOCK_FILE: &str = "seerdb.lock";
const ARCHIVE_MARKER_FILE: &str = "seerdb.archive";
const META_MAGIC: [u8; 8] = *b"SEERMET1";
const META_DELTA_MAGIC: [u8; 8] = *b"SEERMDL1";
const META_DELTA_VERSION: u32 = 1;
const META_DELTA_HEADER_SIZE: usize = 8 + 4 + 8 + 4 + 4 + 4;
const META_DELTA_CHECKSUM_SIZE: usize = 4;
const MAX_META_DELTA_CHAIN: usize = 64;
/// Maximum accumulated deletion offsets before explicit segmented catalog
/// consolidation is requested by `DB::gc()`.
const MAX_SEGMENTED_CATALOG_DELETED_ENTRIES: usize = 4096;
const WAL_RESERVATION_SEGMENT_BYTES: u64 = 1024 * 1024;
const WAL_COMMIT_RECORD_BYTES: u64 = (4 + 1 + CommitRecord::SERIALIZED_SIZE + 4) as u64;
const PUBLICATION_CAPACITY_SAFETY_BYTES: u64 = 8 * PAGE_SIZE as u64;
static NEXT_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

fn retained_blob_path(path: &Path, snapshot_id: SnapshotId) -> PathBuf {
    path.join(format!("{BLOB_FILE}.retained.{}", snapshot_id.get()))
}

fn blob_segment_path(path: &Path, file_id: u32) -> PathBuf {
    path.join(format!("{BLOB_SEGMENT_PREFIX}{file_id:010}"))
}

fn read_blob_segments(path: &Path) -> Result<HashMap<u32, Vec<u8>>> {
    let mut segments = HashMap::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(suffix) = name
            .to_str()
            .and_then(|name| name.strip_prefix(BLOB_SEGMENT_PREFIX))
        else {
            continue;
        };
        let file_id = suffix
            .parse::<u32>()
            .map_err(|_| Error::Corruption("blob segment has an invalid file ID".into()))?;
        if file_id == 0
            || file_id == u32::MAX
            || segments.insert(file_id, fs::read(entry.path())?).is_some()
        {
            return Err(Error::Corruption(
                "blob segment IDs are invalid or duplicated".into(),
            ));
        }
    }
    Ok(segments)
}

fn parse_blob_catalog(
    path: &Path,
    bytes: &[u8],
    target_generation: Option<u64>,
) -> Result<Option<BlobManager>> {
    if BlobManager::is_segment_catalog(bytes) {
        let segments = read_blob_segments(path)?;
        let delta_log = match fs::read(path.join(BLOB_DELTA_FILE)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let parsed = BlobManager::from_segment_catalog_with_delta_log(
            bytes,
            &segments,
            &delta_log,
            target_generation,
        );
        Ok(parsed)
    } else {
        Ok(BlobManager::from_bytes(bytes))
    }
}

fn blob_storage_size(path: &Path) -> Result<u64> {
    let mut total = match fs::metadata(path.join(BLOB_FILE)) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry
                .file_name()
                .to_string_lossy()
                .starts_with(BLOB_SEGMENT_PREFIX)
        {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    if let Ok(metadata) = fs::metadata(path.join(BLOB_DELTA_FILE)) {
        total = total.saturating_add(metadata.len());
    }
    Ok(total)
}

fn segmented_catalog_needs_consolidation(blobs: &BlobManager) -> bool {
    blobs.is_segmented()
        && (blobs.total_deleted_entries() > MAX_SEGMENTED_CATALOG_DELETED_ENTRIES
            || blobs.catalog_needs_consolidation())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenMode {
    Normal,
    Create,
    Check,
}

#[derive(Debug)]
struct VerificationFailure {
    kind: CheckFailureKind,
    message: String,
}

impl VerificationFailure {
    fn from_error(kind: CheckFailureKind, error: Error) -> Self {
        Self {
            kind,
            message: error_message(error),
        }
    }

    fn into_error(self) -> Error {
        Error::Check {
            kind: self.kind,
            message: self.message,
        }
    }
}

/// Blob GC statistics.
pub struct BlobStats {
    /// Number of files needing garbage collection.
    pub files_needing_gc: usize,
    /// Total valid entries across all files.
    pub total_valid: usize,
    /// Total deleted entries across all files.
    pub total_deleted: usize,
    /// Whether segmented catalog deletion metadata has crossed its maintenance
    /// bound and explicit `DB::gc()` should consolidate it.
    pub catalog_needs_consolidation: bool,
}

/// One mutation in an atomic multi-record commit.
///
/// The batch API is intentionally byte-oriented so general Rust consumers can
/// define their own typed/indexed adapter above SeerDB. All mutations are
/// validated against one candidate state before any WAL bytes or in-memory
/// tree/blob state are changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchMutation {
    /// Insert or replace an inline/blob-separated value.
    Put {
        /// User key.
        key: Vec<u8>,
        /// User value.
        value: Vec<u8>,
    },
    /// Delete a key; deleting an absent key is a durable no-op, matching
    /// [`DB::delete`] semantics.
    Delete {
        /// User key.
        key: Vec<u8>,
    },
}

/// Lifecycle state of a root-bound byte transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchTransactionState {
    /// The transaction can stage mutations and be committed or aborted.
    Active,
    /// The transaction published its expected-base batch successfully.
    Committed,
    /// The transaction was explicitly aborted.
    Aborted,
    /// Publication failed after the storage engine fenced the writer. The
    /// commit may already be durable; reopen is required before deciding the
    /// outcome, and only lease cleanup is permitted on this handle.
    RecoveryRequired { commit: CommitId },
}

/// A root-bound byte transaction over SeerDB.
///
/// The transaction captures one durable commit root and keeps its physical
/// pages/blob image protected while it is active. Reads use that root and
/// overlay staged mutations, while commit uses expected-base validation so a
/// stale writer cannot publish against a newer root. The existing
/// `concurrency::Transaction` type remains a low-level ID/read-set primitive;
/// this type is the data-bearing transaction boundary.
pub struct BatchTransaction {
    base_commit: CommitId,
    snapshot_id: SnapshotId,
    lease: Option<RetentionLease>,
    mutations: Vec<BatchMutation>,
    state: BatchTransactionState,
}

impl BatchTransaction {
    /// Return the immutable commit root captured at transaction start.
    #[must_use]
    pub fn snapshot(&self) -> CommitId {
        self.base_commit
    }

    /// Return the transaction lifecycle state.
    #[must_use]
    pub fn state(&self) -> BatchTransactionState {
        self.state
    }

    /// Whether the transaction can still stage or publish work.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state == BatchTransactionState::Active
    }

    /// Return the attempted commit that requires reopen reconciliation.
    #[must_use]
    pub fn recovery_commit(&self) -> Option<CommitId> {
        match self.state {
            BatchTransactionState::RecoveryRequired { commit } => Some(commit),
            _ => None,
        }
    }

    /// Return the staged byte mutations in commit order.
    #[must_use]
    pub fn mutations(&self) -> &[BatchMutation] {
        &self.mutations
    }

    /// Stage an insert/upsert in this transaction.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_active()?;
        validate_wal_put_lengths(key, value)?;
        self.mutations.push(BatchMutation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    /// Stage a delete in this transaction.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.check_active()?;
        validate_wal_key_length(key)?;
        self.mutations
            .push(BatchMutation::Delete { key: key.to_vec() });
        Ok(())
    }

    /// Read through the captured root and staged mutations.
    pub fn get(&self, db: &DB, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_active()?;
        for mutation in self.mutations.iter().rev() {
            match mutation {
                BatchMutation::Put {
                    key: mutation_key,
                    value,
                } if mutation_key.as_slice() == key => return Ok(Some(value.clone())),
                BatchMutation::Delete { key: mutation_key } if mutation_key.as_slice() == key => {
                    return Ok(None);
                }
                _ => {}
            }
        }
        db.get_at(self.snapshot_id, key)
    }

    /// Scan through the captured root and staged mutations over `[start,end)`.
    pub fn range(&self, db: &DB, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_active()?;
        let mut values = db
            .range_at(self.snapshot_id, start, end)?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for mutation in &self.mutations {
            match mutation {
                BatchMutation::Put { key, value }
                    if key.as_slice() >= start && key.as_slice() < end =>
                {
                    values.insert(key.clone(), value.clone());
                }
                BatchMutation::Delete { key }
                    if key.as_slice() >= start && key.as_slice() < end =>
                {
                    values.remove(key);
                }
                _ => {}
            }
        }
        Ok(values.into_iter().collect())
    }

    /// Publish the staged mutations against the captured commit root.
    ///
    /// A validation or serialization failure leaves this transaction active so
    /// the caller can inspect the error and explicitly abort it. A fenced
    /// publication failure transitions the transaction to
    /// [`BatchTransactionState::RecoveryRequired`]: the commit may already be
    /// durable, so callers must release the lease and reopen before deciding
    /// the outcome. Once publication succeeds, the transaction is committed
    /// even if releasing its temporary root lease fails; that cleanup failure
    /// is returned explicitly.
    pub fn commit(&mut self, db: &mut DB) -> Result<DurabilityStatus> {
        self.check_active()?;
        let attempted_commit = if self.mutations.is_empty() {
            self.base_commit
        } else {
            CommitId::new(
                self.base_commit
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("transaction commit ID overflow".into()))?,
            )
        };
        let status = match db.commit_batch_at(self.base_commit, &self.mutations) {
            Ok(status) => status,
            Err(error) if db.durability_status().write_fenced => {
                self.state = BatchTransactionState::RecoveryRequired {
                    commit: attempted_commit,
                };
                return Err(Error::NeedsRecovery(format!(
                    "transaction commit {:?} may be durable after publication failure: {error}",
                    attempted_commit
                )));
            }
            Err(error) => return Err(error),
        };
        self.state = BatchTransactionState::Committed;
        if let Some(lease) = self.lease.as_mut()
            && let Err(cleanup) = lease.release()
        {
            return Err(Error::CommitCleanup {
                commit: status.commit_id,
                cleanup: Box::new(cleanup),
            });
        }
        self.lease.take();
        Ok(status)
    }

    /// Abort the transaction and release its retained root.
    pub fn abort(&mut self) -> Result<()> {
        self.check_active()?;
        if let Some(lease) = self.lease.as_mut() {
            lease.release()?;
            self.lease.take();
        }
        self.state = BatchTransactionState::Aborted;
        Ok(())
    }

    /// Release the root lease after a committed cleanup failure or an
    /// indeterminate publication outcome. Releasing does not resolve an
    /// indeterminate commit; reopen is still required.
    pub fn release(&mut self) -> Result<()> {
        if let Some(lease) = self.lease.as_mut() {
            lease.release()?;
            self.lease.take();
        }
        Ok(())
    }

    fn check_active(&self) -> Result<()> {
        match self.state {
            BatchTransactionState::Active => Ok(()),
            BatchTransactionState::RecoveryRequired { commit } => Err(Error::NeedsRecovery(
                format!("transaction commit {commit:?} requires database reopen"),
            )),
            BatchTransactionState::Committed | BatchTransactionState::Aborted => Err(
                Error::InvalidArgument("transaction is no longer active".into()),
            ),
        }
    }
}

impl Drop for BatchTransaction {
    fn drop(&mut self) {
        let _ = self.lease.take();
    }
}

/// Durable identity and publication state exposed for recovery diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurabilityStatus {
    /// Stable database identity.
    pub database_id: DatabaseId,
    /// Stable logical history identity.
    pub history_id: HistoryId,
    /// Latest manifest generation known to this handle.
    pub generation_id: GenerationId,
    /// Latest durable commit known to this handle.
    pub commit_id: CommitId,
    /// Mutations currently journaled but not yet published in a generation.
    pub pending_mutations: u64,
    /// Whether this writer must be reopened before accepting more writes.
    pub write_fenced: bool,
}

/// Results from a read-only integrity verification pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerificationReport {
    /// Durable identity and publication state that was verified.
    pub durability: DurabilityStatus,
    /// Number of active PMT pages with valid checksums.
    pub verified_pages: u64,
    /// Current data-file size in bytes.
    pub data_bytes: u64,
    /// Current serialized blob-file size in bytes.
    pub blob_bytes: u64,
    /// Current WAL size in bytes, if a pending batch exists.
    pub wal_bytes: u64,
    /// Physical page slots currently safe for reuse.
    pub reclaimable_pages: u64,
}

/// WAL state observed by a non-mutating offline check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalCheckStatus {
    /// No WAL records are present.
    Clean,
    /// Complete mutation records are present without a commit envelope.
    Pending,
    /// A commit envelope is present and recovery may need to advance the
    /// authoritative manifest.
    NeedsRecovery,
    /// The final WAL record is torn or the reserved suffix is incomplete.
    Incomplete,
}

/// Results from a non-mutating offline integrity check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckReport {
    /// Active-generation verification results.
    pub verification: VerificationReport,
    /// WAL state that the writable open path would reconcile.
    pub wal_status: WalCheckStatus,
}

/// Results from creating and independently verifying a snapshot directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotReport {
    /// Source durability state at the snapshot boundary.
    pub source: DurabilityStatus,
    /// Destination durability state after reopen and verification.
    pub destination: DurabilityStatus,
    /// Number of durable artifacts copied into the snapshot.
    pub copied_files: u32,
    /// Number of destination pages verified after reopen.
    pub verified_pages: u64,
}

/// Results from restoring an immutable archive into a new writable history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestoreReport {
    /// Archive durability state used as the restore source.
    pub source: DurabilityStatus,
    /// New writable history state after restore.
    pub destination: DurabilityStatus,
    /// Number of durable artifacts copied into the new history.
    pub copied_files: u32,
    /// Number of destination pages verified after history fork.
    pub verified_pages: u64,
}

/// Durable action performed while rebuilding a checked database copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairAction {
    /// The source had no WAL requiring reconciliation.
    NoRepair,
    /// A complete mutation-only WAL was discarded as uncommitted.
    DiscardedUncommittedWal,
    /// A complete committed WAL image was reconciled in the destination.
    ReconciledCommittedWal,
    /// A torn WAL suffix was reconciled in the rebuilt copy.
    ReconciledIncompleteWal,
}

/// Results from rebuilding a checked database into a new writable history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairReport {
    /// Source durable identity before rebuild.
    pub source: DurabilityStatus,
    /// WAL state found by the non-mutating source check.
    pub source_wal_status: WalCheckStatus,
    /// New writable identity after recovery and history fork.
    pub destination: DurabilityStatus,
    /// Number of durable artifacts copied into the rebuild workspace.
    pub copied_files: u32,
    /// Number of destination pages verified after rebuild.
    pub verified_pages: u64,
    /// Recovery/rebuild action performed in the destination.
    pub action: RepairAction,
}

/// An owned, read-only snapshot view backed by an independently verified
/// directory copy.
///
/// The copy-backed implementation is intentionally conservative: source page
/// reclamation cannot invalidate the snapshot, and dropping or releasing the
/// handle removes its temporary directory. A future shared-page snapshot can
/// preserve this API while replacing the copy mechanism.
pub struct Snapshot {
    db: Option<DB>,
    path: PathBuf,
    released: bool,
}

/// An owned retained snapshot backed by a verified read-only copy and a
/// durable root-generation retention lease.
///
/// The copy is the current read implementation; the lease is the physical
/// safety boundary. Retained page versions are not reused while the lease is
/// live, so the eventual in-history reader can replace the copy without
/// changing the reclamation contract.
pub struct RetainedSnapshot {
    snapshot: Option<Snapshot>,
    lease: Option<RetentionLease>,
}

struct RetentionState {
    path: PathBuf,
    root_path: PathBuf,
    registry: RetentionRegistry,
    /// Process-local transaction roots. These intentionally do not enter the
    /// durable named-snapshot registry: a crashed process must not leave a
    /// short-lived transaction pin blocking reclamation after reopen.
    ephemeral_roots: BTreeMap<SnapshotId, RetainedRoot>,
    next_ephemeral_snapshot_id: SnapshotId,
    protected_offsets: Arc<Mutex<HashSet<u64>>>,
    offsets_by_snapshot: BTreeMap<SnapshotId, HashSet<u64>>,
}

struct RetentionLease {
    state: Arc<Mutex<RetentionState>>,
    snapshot_id: SnapshotId,
    reclamation_dirty: Arc<AtomicBool>,
    released: bool,
}

/// A cheap immutable read handle bound to one published SeerDB generation.
///
/// The handle owns a process-local retention lease and independent page/blob
/// descriptors. It does not copy the PMT, serialize blob bytes, or write a
/// sidecar at creation time. Writers continue to publish newer generations;
/// this view remains on the root and physical files it captured.
pub struct ReadView {
    storage: StorageReadView,
    blobs: BlobReadView,
    lease: Option<RetentionLease>,
    durability: DurabilityStatus,
}

struct BlobReadView {
    files: HashMap<u32, File>,
    bases: HashMap<u32, u64>,
}

impl BlobReadView {
    fn open(path: &Path, blobs: &BlobManager) -> Result<Self> {
        if blobs.is_segmented() {
            let mut files = HashMap::new();
            let mut bases = HashMap::new();
            for file_id in blobs.segment_file_ids() {
                let file = OpenOptions::new()
                    .read(true)
                    .open(blob_segment_path(path, file_id))?;
                files.insert(file_id, file);
                bases.insert(file_id, 0);
            }
            return Ok(Self { files, bases });
        }

        let file_ids = blobs.segment_file_ids();
        if file_ids.is_empty() {
            return Ok(Self {
                files: HashMap::new(),
                bases: HashMap::new(),
            });
        }
        let file = OpenOptions::new().read(true).open(path.join(BLOB_FILE))?;
        let file_len = file.metadata()?.len();
        let mut files = HashMap::new();
        let mut bases = HashMap::new();
        let mut cursor;
        let mut header = [0u8; 32];
        if file_len >= header.len() as u64 {
            read_exact_at(&file, 0, &mut header)?;
        }

        if header[..8] == *b"SEERBLB1" {
            if decode_u32(&header[8..12])? != 1 {
                return Err(Error::Corruption("unsupported blob image version".into()));
            }
            let count = decode_u32(&header[28..32])? as usize;
            cursor = 32;
            for _ in 0..count {
                let mut descriptor = [0u8; 12];
                read_exact_at(&file, cursor, &mut descriptor)?;
                let file_id = decode_u32(&descriptor[..4])?;
                let data_len = decode_u64(&descriptor[4..12])?;
                let base = cursor
                    .checked_add(12)
                    .ok_or_else(|| Error::Corruption("blob image offset overflow".into()))?;
                let data_end = base
                    .checked_add(data_len)
                    .ok_or_else(|| Error::Corruption("blob image length overflow".into()))?;
                if file_id == 0 || data_end > file_len || files.contains_key(&file_id) {
                    return Err(Error::Corruption("invalid blob image descriptor".into()));
                }
                files.insert(file_id, file.try_clone()?);
                bases.insert(file_id, base);
                cursor = data_end;
                let mut deleted_count = [0u8; 4];
                read_exact_at(&file, cursor, &mut deleted_count)?;
                let deleted_bytes = u64::from(decode_u32(&deleted_count)?)
                    .checked_mul(8)
                    .ok_or_else(|| Error::Corruption("blob deletion metadata overflow".into()))?;
                cursor = cursor
                    .checked_add(4)
                    .and_then(|offset| offset.checked_add(deleted_bytes))
                    .ok_or_else(|| Error::Corruption("blob image offset overflow".into()))?;
            }
        } else {
            let mut count_bytes = [0u8; 4];
            read_exact_at(&file, 0, &mut count_bytes)?;
            let count = decode_u32(&count_bytes)? as usize;
            cursor = 4;
            for _ in 0..count {
                let mut descriptor = [0u8; 8];
                read_exact_at(&file, cursor, &mut descriptor)?;
                let file_id = decode_u32(&descriptor[..4])?;
                let data_len = u64::from(decode_u32(&descriptor[4..8])?);
                let base = cursor
                    .checked_add(8)
                    .ok_or_else(|| Error::Corruption("blob image offset overflow".into()))?;
                let data_end = base
                    .checked_add(data_len)
                    .ok_or_else(|| Error::Corruption("blob image length overflow".into()))?;
                if file_id == 0 || data_end > file_len || files.contains_key(&file_id) {
                    return Err(Error::Corruption("invalid legacy blob descriptor".into()));
                }
                files.insert(file_id, file.try_clone()?);
                bases.insert(file_id, base);
                cursor = data_end;
            }
        }

        for file_id in file_ids {
            if !files.contains_key(&file_id) {
                return Err(Error::Corruption(format!(
                    "blob image is missing file {file_id}"
                )));
            }
        }
        Ok(Self { files, bases })
    }

    fn read(&self, pointer: &BlobPointer) -> Result<Vec<u8>> {
        let file = self.files.get(&pointer.file_id).ok_or_else(|| {
            Error::Corruption(format!(
                "blob pointer names missing file {}",
                pointer.file_id
            ))
        })?;
        let base = *self.bases.get(&pointer.file_id).ok_or_else(|| {
            Error::Corruption(format!(
                "blob pointer has no base for file {}",
                pointer.file_id
            ))
        })?;
        let offset = base
            .checked_add(pointer.offset)
            .ok_or_else(|| Error::Corruption("blob pointer offset overflow".into()))?;
        let mut header = [0u8; 12];
        read_exact_at(file, offset, &mut header)?;
        let length = decode_u32(&header[8..12])?;
        if length != pointer.length {
            return Err(Error::Corruption(
                "blob pointer length does not match record".into(),
            ));
        }
        let value_len = usize::try_from(length)
            .map_err(|_| Error::Corruption("blob value length overflows memory".into()))?;
        let record_len = 12usize
            .checked_add(value_len)
            .ok_or_else(|| Error::Corruption("blob record length overflow".into()))?;
        let mut record = vec![0u8; record_len];
        read_exact_at(file, offset, &mut record)?;
        let mut crc_bytes = [0u8; 4];
        read_exact_at(
            file,
            offset
                .checked_add(record_len as u64)
                .ok_or_else(|| Error::Corruption("blob record offset overflow".into()))?,
            &mut crc_bytes,
        )?;
        let stored_crc = u32::from_le_bytes(crc_bytes);
        if stored_crc != crc32c::crc32c(&record) {
            return Err(Error::Corruption("blob record checksum mismatch".into()));
        }
        Ok(record[12..].to_vec())
    }
}

fn read_exact_at(file: &File, offset: u64, buffer: &mut [u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut filled = 0;
        while filled < buffer.len() {
            let count = file.read_at(&mut buffer[filled..], offset + filled as u64)?;
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "file read reached end of file",
                ));
            }
            filled += count;
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        let mut filled = 0;
        while filled < buffer.len() {
            let count = file.seek_read(&mut buffer[filled..], offset + filled as u64)?;
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "file read reached end of file",
                ));
            }
            filled += count;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let mut cloned = file.try_clone()?;
        cloned.seek(SeekFrom::Start(offset))?;
        cloned.read_exact(buffer)
    }
}

fn decode_u32(bytes: &[u8]) -> Result<u32> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| Error::Corruption("truncated blob integer".into()))?;
    Ok(u32::from_le_bytes(bytes))
}

fn decode_u64(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::Corruption("truncated blob integer".into()))?;
    Ok(u64::from_le_bytes(bytes))
}

impl ReadView {
    /// Return the generation captured by this view.
    #[must_use]
    pub fn commit_id(&self) -> CommitId {
        self.durability.commit_id
    }

    /// Return the durability state captured by this view.
    #[must_use]
    pub fn durability_status(&self) -> DurabilityStatus {
        self.durability
    }

    /// Read a key from the captured generation.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.storage.lookup(key)? {
            LookupResult::Found(value) => Ok(Some(value)),
            LookupResult::Blob(pointer) => self.blobs.read(&pointer).map(Some),
            LookupResult::Deleted | LookupResult::NotFound => Ok(None),
        }
    }

    /// Read a range from the captured generation.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.storage
            .range(start, end)?
            .into_iter()
            .filter_map(|(key, result)| match result {
                LookupResult::Found(value) => Some(Ok((key, value))),
                LookupResult::Blob(pointer) => {
                    Some(self.blobs.read(&pointer).map(|value| (key, value)))
                }
                LookupResult::Deleted | LookupResult::NotFound => None,
            })
            .collect()
    }

    /// Release the view's root lease before dropping the handle.
    pub fn release(mut self) -> Result<()> {
        if let Some(mut lease) = self.lease.take() {
            lease.release()?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for ReadView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadView")
            .field("durability", &self.durability)
            .finish_non_exhaustive()
    }
}

impl Snapshot {
    /// Return the snapshot directory path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get a value from the retained snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .get(key)
    }

    /// Scan a range in the retained snapshot.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.db
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .range(start, end)
    }

    /// Verify the retained snapshot independently.
    pub fn verify(&mut self) -> Result<VerificationReport> {
        self.db
            .as_mut()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .verify()
    }

    /// Return the durable identity captured by this snapshot.
    pub fn durability_status(&self) -> Result<DurabilityStatus> {
        Ok(self
            .db
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .durability_status())
    }

    /// Release the snapshot directory immediately.
    pub fn release(mut self) -> Result<()> {
        self.db.take();
        fs::remove_dir_all(&self.path)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.db.take();
        if !self.released {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

impl RetentionState {
    fn load(path: PathBuf, protected_offsets: Arc<Mutex<HashSet<u64>>>) -> Result<Self> {
        let registry = if path.exists() {
            let bytes = fs::read(&path)?;
            RetentionRegistry::from_bytes(&bytes)
                .map_err(|message| Error::Corruption(format!("retention registry {message}")))?
        } else {
            RetentionRegistry::new()
        };
        Ok(Self {
            root_path: path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
            path,
            registry,
            ephemeral_roots: BTreeMap::new(),
            next_ephemeral_snapshot_id: SnapshotId::new(u64::MAX),
            protected_offsets,
            offsets_by_snapshot: BTreeMap::new(),
        })
    }

    fn persist(&self, registry: &RetentionRegistry) -> Result<()> {
        if registry.is_empty() {
            if self.path.exists() {
                fs::remove_file(&self.path)?;
                sync_directory(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
            }
            return Ok(());
        }
        let bytes = registry
            .to_bytes()
            .ok_or_else(|| Error::Wal("retention registry is too large".into()))?;
        atomic_write(&self.path, &bytes)
    }

    fn replace_protected_offsets(&self) -> Result<()> {
        let mut protected = HashSet::new();
        for offsets in self.offsets_by_snapshot.values() {
            protected.extend(offsets);
        }
        *self
            .protected_offsets
            .lock()
            .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))? =
            protected;
        Ok(())
    }

    fn install_offsets(
        &mut self,
        offsets_by_snapshot: BTreeMap<SnapshotId, HashSet<u64>>,
    ) -> Result<()> {
        self.offsets_by_snapshot = offsets_by_snapshot;
        self.replace_protected_offsets()
    }

    fn insert(&mut self, manifest: Manifest, offsets: HashSet<u64>) -> Result<SnapshotId> {
        let mut candidate = self.registry.clone();
        let snapshot_id = candidate
            .insert(manifest)
            .ok_or_else(|| Error::Wal("snapshot ID overflow".into()))?;
        self.persist(&candidate)?;
        self.registry = candidate;
        self.offsets_by_snapshot.insert(snapshot_id, offsets);
        self.replace_protected_offsets()?;
        Ok(snapshot_id)
    }

    fn remove(&mut self, snapshot_id: SnapshotId) -> Result<()> {
        if let Some(root) = self.ephemeral_roots.remove(&snapshot_id) {
            self.offsets_by_snapshot.remove(&snapshot_id);
            self.replace_protected_offsets()?;
            let blob_path = retained_blob_path(&self.root_path, root.snapshot_id);
            if blob_path.exists() {
                fs::remove_file(blob_path)?;
                sync_directory(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
            }
            return Ok(());
        }
        let mut candidate = self.registry.clone();
        if candidate.remove(snapshot_id).is_none() {
            return Err(Error::InvalidArgument(format!(
                "unknown retained snapshot {}",
                snapshot_id.get()
            )));
        }
        self.persist(&candidate)?;
        self.registry = candidate;
        self.offsets_by_snapshot.remove(&snapshot_id);
        self.replace_protected_offsets()?;
        let blob_path = retained_blob_path(&self.root_path, snapshot_id);
        if blob_path.exists() {
            fs::remove_file(blob_path)?;
            sync_directory(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
        }
        Ok(())
    }

    fn roots(&self) -> &[RetainedRoot] {
        self.registry.roots()
    }

    fn all_roots(&self) -> impl Iterator<Item = &RetainedRoot> {
        self.registry
            .roots()
            .iter()
            .chain(self.ephemeral_roots.values())
    }

    fn is_empty(&self) -> bool {
        self.registry.is_empty() && self.ephemeral_roots.is_empty()
    }

    fn next_snapshot_id(&self) -> SnapshotId {
        self.registry.next_snapshot_id()
    }

    fn next_ephemeral_snapshot_id(&self) -> SnapshotId {
        self.next_ephemeral_snapshot_id
    }

    fn insert_ephemeral(
        &mut self,
        manifest: Manifest,
        offsets: HashSet<u64>,
    ) -> Result<SnapshotId> {
        let snapshot_id = self.next_ephemeral_snapshot_id;
        if snapshot_id.get() == 0
            || self
                .registry
                .roots()
                .iter()
                .any(|root| root.snapshot_id == snapshot_id)
        {
            return Err(Error::Wal("ephemeral snapshot ID overflow".into()));
        }
        self.next_ephemeral_snapshot_id = SnapshotId::new(snapshot_id.get() - 1);
        self.ephemeral_roots.insert(
            snapshot_id,
            RetainedRoot {
                snapshot_id,
                manifest,
            },
        );
        self.offsets_by_snapshot.insert(snapshot_id, offsets);
        self.replace_protected_offsets()?;
        Ok(snapshot_id)
    }
}

impl RetentionLease {
    fn release(&mut self) -> Result<()> {
        if !self.released {
            self.state
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?
                .remove(self.snapshot_id)?;
            self.reclamation_dirty.store(true, Ordering::Release);
            self.released = true;
        }
        Ok(())
    }
}

impl Drop for RetentionLease {
    fn drop(&mut self) {
        if !self.released {
            if let Ok(mut state) = self.state.lock() {
                let _ = state.remove(self.snapshot_id);
            }
            self.reclamation_dirty.store(true, Ordering::Release);
            self.released = true;
        }
    }
}

impl RetainedSnapshot {
    /// Return the durable retention identifier.
    pub fn snapshot_id(&self) -> SnapshotId {
        self.lease
            .as_ref()
            .map(|lease| lease.snapshot_id)
            .unwrap_or_default()
    }

    /// Return the path of the conservative read copy while it is live.
    pub fn path(&self) -> Option<&Path> {
        self.snapshot.as_ref().map(Snapshot::path)
    }

    /// Read a value from the retained snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.snapshot
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .get(key)
    }

    /// Scan a range in the retained snapshot.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.snapshot
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .range(start, end)
    }

    /// Verify the retained snapshot independently.
    pub fn verify(&mut self) -> Result<VerificationReport> {
        self.snapshot
            .as_mut()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .verify()
    }

    /// Return the durable identity captured by this retained snapshot.
    pub fn durability_status(&self) -> Result<DurabilityStatus> {
        self.snapshot
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("snapshot has been released".into()))?
            .durability_status()
    }

    /// Release the physical root-retention lease and temporary read copy.
    pub fn release(mut self) -> Result<()> {
        let lease_result = self.lease.as_mut().map_or(Ok(()), RetentionLease::release);
        let snapshot_result = self.snapshot.take().map_or(Ok(()), Snapshot::release);
        if lease_result.is_ok() {
            self.lease.take();
        }
        lease_result.and(snapshot_result)
    }
}

/// Results from trimming reclaimable trailing data pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionReport {
    /// Durable identity after compaction.
    pub durability: DurabilityStatus,
    /// Data-file size before truncation.
    pub data_bytes_before: u64,
    /// Data-file size after truncation.
    pub data_bytes_after: u64,
    /// Number of physical page slots removed from the tail.
    pub reclaimed_pages: u64,
    /// Number of active page versions moved out of interior holes.
    pub relocated_pages: u64,
    /// Whether the active manifest was mirrored before truncation.
    pub manifest_replicated: bool,
}

/// Results from a logical mark-and-rebuild vacuum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VacuumReport {
    /// Durable identity after the rebuild generation.
    pub durability: DurabilityStatus,
    /// Number of live key-value entries copied into the new tree.
    pub live_entries: u64,
    /// Active logical page count before rebuilding.
    pub logical_pages_before: u64,
    /// Active logical page count after rebuilding.
    pub logical_pages_after: u64,
}

/// Progress from one bounded logical vacuum step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VacuumProgress {
    /// Durable identity. It changes only when the final step publishes.
    pub durability: DurabilityStatus,
    /// Number of logical entries consumed by the source cursor.
    pub scanned_entries: u64,
    /// Number of live entries copied into the candidate tree.
    pub live_entries: u64,
    /// Active logical page count before rebuilding.
    pub logical_pages_before: u64,
    /// Candidate page count after publication, or `None` while incomplete.
    pub logical_pages_after: Option<u64>,
    /// Whether this step published the rebuilt generation.
    pub complete: bool,
}

/// Results from pruning superseded manifests and checkpoint files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryPruneReport {
    /// Number of generations retained for the current root and snapshots.
    pub retained_generations: u64,
    /// Number of manifest descriptors removed from the history sidecar.
    pub removed_manifests: u64,
    /// Number of superseded checkpoint files removed.
    pub removed_checkpoints: u64,
    /// Bytes reclaimed from removed checkpoint files.
    pub reclaimed_checkpoint_bytes: u64,
}

/// Cumulative storage work and current artifact sizes for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DBMetrics {
    /// Physical page and publication counters for this open handle.
    pub storage: StorageMetrics,
    /// Number of mutations rejected before application by the WAL budget.
    pub wal_admission_failures: u64,
    /// Buffer-pool occupancy and cache counters for this open handle.
    pub buffer: BufferStats,
    /// Current data-file size in bytes.
    pub data_bytes: u64,
    /// Current blob-file size in bytes.
    pub blob_bytes: u64,
    /// Current logical WAL size in bytes.
    pub wal_bytes: u64,
    /// Fixed WAL capacity extent reserved for future pending mutations.
    pub wal_reserved_bytes: u64,
    /// Physical pages currently safe for reuse.
    pub reclaimable_pages: u64,
    /// Cumulative bytes written to publication artifacts by this handle.
    pub publication: PublicationMetrics,
    /// Cumulative wall-clock time spent in publication phases by this handle.
    pub publication_timing: PublicationTimingMetrics,
}

/// Cumulative bytes written to durable publication artifacts.
///
/// These counters measure successful bytes written by the current publication
/// protocol. Metadata may be a full checkpoint or a bounded PMT delta;
/// blob/image and manifest-history counters still describe their current
/// whole-image/append-only artifacts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublicationMetrics {
    /// Bytes appended to the durable WAL by this open handle.
    pub wal_bytes_written: u64,
    /// Bytes written to PMT/allocator checkpoint images.
    pub metadata_bytes_written: u64,
    /// Bytes written to blob images.
    pub blob_bytes_written: u64,
    /// Bytes appended or rewritten in manifest history.
    pub history_bytes_written: u64,
    /// Bytes written to alternating manifest slots.
    pub manifest_bytes_written: u64,
}

/// Cumulative wall-clock time spent in durable publication phases.
///
/// These timings are diagnostic attribution for the current publication
/// protocol. They are not latency guarantees and may include filesystem cache
/// and scheduler effects from the host running the database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublicationTimingMetrics {
    /// Candidate validation, cloning, and in-memory mutation preparation.
    pub candidate_prepare_ns: u64,
    /// WAL append and durability barriers.
    pub wal_write_ns: u64,
    /// Publication capacity checks and reuse-ledger admission.
    pub admission_ns: u64,
    /// Physical page writes and the data-device sync.
    pub data_flush_ns: u64,
    /// PMT/allocator checkpoint writes and syncs.
    pub metadata_write_ns: u64,
    /// Blob image or segmented catalog/append writes and syncs.
    pub blob_write_ns: u64,
    /// Manifest-history append/checkpoint writes and syncs.
    pub history_write_ns: u64,
    /// Final publication-directory durability barrier.
    pub directory_sync_ns: u64,
    /// Authoritative manifest slot write and sync.
    pub manifest_write_ns: u64,
    /// Pre-reuse manifest mirror write and sync.
    pub manifest_mirror_ns: u64,
    /// Post-manifest cleanup and reclamation bookkeeping.
    pub cleanup_ns: u64,
}

fn elapsed_nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

struct MetaDelta {
    parent_checkpoint_id: u64,
    updates: Vec<(u64, PageMapping)>,
    removals: Vec<u64>,
    allocator: PageAllocator,
}

struct VacuumState {
    source_generation: GenerationId,
    source_commit: CommitId,
    cursor: RangeCursor,
    candidate_tree: BTree,
    candidate_blobs: BlobManager,
    scanned_entries: u64,
    live_entries: u64,
    logical_pages_before: u64,
}

/// A seerdb database instance.
///
/// Provides key-value storage with:
/// - Out-of-place B-tree (pages never updated in place)
/// - KV separation (large values in blob files)
/// - WAL for crash recovery
/// - Buffer pool for caching
pub struct DB {
    /// Database directory path.
    path: PathBuf,
    /// Configuration options.
    options: Options,
    /// Storage engine (coordinates B-tree, buffer, PMT, device).
    engine: StorageEngine,
    /// WAL manager.
    wal: WalManager,
    /// Blob manager.
    blobs: BlobManager,
    /// In-memory candidate for a resumable logical vacuum.
    vacuum: Option<VacuumState>,
    /// Durable retained-root registry shared with retained snapshot handles.
    retention: Arc<Mutex<RetentionState>>,
    /// Transaction manager for MVCC.
    txn_manager: TransactionManager,
    /// Authoritative root-generation publication store.
    manifest: ManifestStore,
    /// Durable descriptors for historical roots that can be retained later.
    manifest_history: ManifestHistory,
    /// Reuse attempts whose publication outcome may be indeterminate.
    reuse_ledger: ReuseLedger,
    /// Stable database identity.
    database_id: DatabaseId,
    /// Stable logical history identity.
    history_id: HistoryId,
    /// Latest published generation.
    generation_id: GenerationId,
    /// Latest published commit.
    commit_id: CommitId,
    /// Next commit identity reserved for a new logical publication.
    ///
    /// This may be ahead of `commit_id` when a prior publication could have
    /// reached durable WAL or page media but did not become authoritative.
    /// Such an identity is never reused after reopen.
    next_commit_id: CommitId,
    /// Next physical generation identity reserved for a new publication.
    next_generation_id: GenerationId,
    /// Number of mutation records since the last published generation.
    pending_mutations: u64,
    /// Logical WAL bytes admitted for the pending generation.
    pending_wal_bytes: u64,
    /// Physical WAL reservation extent already established for this handle.
    ///
    /// Keep-size reservation is a handle-level high-water mark. Reissuing a
    /// full APFS `F_PREALLOCATE` request on every mutation can eventually
    /// report `ENOSPC` even though the original extent is already reserved.
    wal_reserved_extent: u64,
    /// Digest over pending mutation records.
    pending_digest: u32,
    /// Whether the pending generation changes the durable blob image/catalog.
    pending_blob_changes: bool,
    /// Whether the database is open.
    is_open: bool,
    /// Whether a failed publication fenced this writer until reopen.
    write_fenced: bool,
    /// Whether this handle opened an immutable archive/snapshot.
    read_only: bool,
    /// Whether this handle was opened only for non-mutating checks.
    check_only: bool,
    /// Advisory writer ownership for the database directory.
    lock_file: Option<File>,
    /// Number of retryable WAL admission rejections for this handle.
    wal_admission_failures: u64,
    /// Cumulative publication-artifact write counters.
    publication: PublicationMetrics,
    /// Cumulative publication phase timings for diagnostics.
    publication_timing: PublicationTimingMetrics,
}

impl DB {
    /// Open or create a database at the given path.
    pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        Self::open_with_mode(path, options, OpenMode::Normal)
    }

    /// Create a new database and refuse an existing store.
    ///
    /// The final directory is created with an atomic no-replace operation so
    /// callers cannot accidentally attach a logical catalog to an existing
    /// path. Use [`DB::open`] when reopening is intended.
    pub fn create<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        Self::open_with_mode(path, options, OpenMode::Create)
    }

    /// Check an existing database without taking writer ownership or
    /// replaying, truncating, or publishing its WAL.
    pub fn check<P: AsRef<Path>>(path: P, options: Options) -> Result<CheckReport> {
        let mut db = Self::open_with_mode(path, options, OpenMode::Check)
            .map_err(Self::map_check_open_error)?;
        let verification = db.verify_inner().map_err(VerificationFailure::into_error)?;
        let wal_status = db
            .wal_check_status()
            .map_err(|error| Self::map_check_error(CheckFailureKind::Wal, error))?;
        Ok(CheckReport {
            verification,
            wal_status,
        })
    }

    fn map_check_open_error(error: Error) -> Error {
        Self::map_check_error(CheckFailureKind::Format, error)
    }

    fn map_check_error(default_kind: CheckFailureKind, error: Error) -> Error {
        match error {
            Error::Check { .. } => error,
            Error::InvalidArgument(message) => Error::Check {
                kind: CheckFailureKind::Target,
                message,
            },
            Error::Io(error) => Error::Check {
                kind: CheckFailureKind::Io,
                message: error.to_string(),
            },
            Error::NeedsRecovery(message) => Error::Check {
                kind: CheckFailureKind::Wal,
                message,
            },
            Error::Corruption(message) => Error::Check {
                kind: default_kind,
                message,
            },
            other => other,
        }
    }

    fn map_checkpoint_check_error(error: Error) -> Error {
        match error {
            Error::Corruption(message) if message.contains("unsupported meta format version") => {
                Error::Check {
                    kind: CheckFailureKind::Format,
                    message,
                }
            }
            other => Self::map_check_error(CheckFailureKind::Checkpoint, other),
        }
    }

    fn open_with_mode<P: AsRef<Path>>(path: P, options: Options, mode: OpenMode) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let path_preexisted = path.exists();
        let check_only = mode == OpenMode::Check;

        match mode {
            OpenMode::Check if !path.exists() => {
                return Err(Error::InvalidArgument(format!(
                    "check path does not exist: {}",
                    path.display()
                )));
            }
            OpenMode::Create => {
                if path.exists() {
                    return Err(Error::InvalidArgument(format!(
                        "database path already exists: {}",
                        path.display()
                    )));
                }
                if let Some(parent) = path.parent()
                    && !parent.as_os_str().is_empty()
                {
                    fs::create_dir_all(parent)?;
                }
                fs::create_dir(&path)?;
                // The directory entry itself is part of the acknowledged
                // create boundary. Sync the parent chain before publishing
                // any manifest/data artifacts so a power loss cannot lose the
                // newly created database directory while retaining its files.
                sync_directory_chain(path.parent().unwrap_or_else(|| Path::new(".")))?;
            }
            OpenMode::Normal if !path.exists() => fs::create_dir_all(&path)?,
            OpenMode::Check | OpenMode::Normal => {}
        }

        let data_path = path.join(DATA_FILE);
        let wal_path = path.join(WAL_FILE);
        let meta_path = path.join(META_FILE);
        let manifest_path = path.join(MANIFEST_FILE);
        let archive = path.join(ARCHIVE_MARKER_FILE).is_file();
        let read_only = check_only || archive;
        if mode == OpenMode::Normal
            && path_preexisted
            && !manifest_path.is_file()
            && !data_path.is_file()
            && !wal_path.is_file()
            && !meta_path.is_file()
        {
            return Err(Error::Corruption(format!(
                "existing database path has no authoritative storage artifacts: {}",
                path.display()
            )));
        }
        if check_only && (!manifest_path.is_file() || !data_path.is_file()) {
            return Err(Error::Check {
                kind: CheckFailureKind::Target,
                message: "check target is missing required manifest or data artifacts".into(),
            });
        }
        if archive && !check_only {
            if !manifest_path.is_file() || !data_path.is_file() {
                return Err(Error::Corruption(
                    "read-only archive is missing required artifacts".into(),
                ));
            }
            if wal_path.exists() {
                return Err(Error::NeedsRecovery(
                    "read-only archive contains a pending WAL".into(),
                ));
            }
        }
        let lock_file = if read_only {
            None
        } else {
            Some(Self::acquire_writer_lock(&path.join(LOCK_FILE))?)
        };
        if !read_only {
            cleanup_orphaned_temporary_artifacts(&path)?;
            clear_blob_reservation(&path)?;
            clear_wal_reservation(&path)?;
        }

        let mut manifest = if check_only {
            ManifestStore::open_read_only(&manifest_path)
                .map_err(|error| Self::map_check_error(CheckFailureKind::Manifest, error))?
        } else {
            ManifestStore::open(&manifest_path)?
        };
        let current_manifest = manifest.load_latest().map_err(|error| {
            if check_only {
                Self::map_check_error(CheckFailureKind::Manifest, error)
            } else {
                error
            }
        })?;
        if let Some(current) = current_manifest {
            if !data_path.is_file() {
                let error = Error::Corruption(format!(
                    "manifest generation {} is missing the data file",
                    current.generation_id.get()
                ));
                return Err(if check_only {
                    Self::map_check_error(CheckFailureKind::Target, error)
                } else {
                    error
                });
            }
            if current.pmt_checkpoint_id.get() != 0 {
                let checkpoint_path =
                    path.join(format!("seerdb.meta.{}", current.pmt_checkpoint_id.get()));
                if !checkpoint_path.is_file() {
                    let error = Error::Corruption(format!(
                        "manifest generation {} is missing checkpoint {}",
                        current.generation_id.get(),
                        current.pmt_checkpoint_id.get()
                    ));
                    return Err(if check_only {
                        Self::map_check_error(CheckFailureKind::Checkpoint, error)
                    } else {
                        error
                    });
                }
            }
        }
        if check_only && current_manifest.is_none() {
            return Err(Error::Check {
                kind: CheckFailureKind::Manifest,
                message: "check target has no valid manifest generation".into(),
            });
        }
        let (database_id, history_id, generation_id, commit_id) =
            if let Some(current) = current_manifest {
                if current.page_size as usize != PAGE_SIZE {
                    return Err(Error::Corruption(format!(
                        "manifest page size {} does not match build page size {PAGE_SIZE}",
                        current.page_size
                    )));
                }
                (
                    current.database_id,
                    current.history_id,
                    current.generation_id,
                    current.commit_id,
                )
            } else {
                (
                    Self::new_database_id(&path),
                    HistoryId::new(1),
                    GenerationId::new(0),
                    CommitId::new(0),
                )
            };

        let manifest_history_path = path.join(MANIFEST_HISTORY_FILE);
        let mut manifest_history = if manifest_history_path.exists() {
            let bytes = fs::read(&manifest_history_path)?;
            ManifestHistory::from_bytes(&bytes)
                .map_err(|message| Error::Corruption(format!("manifest history {message}")))?
        } else {
            ManifestHistory::new()
        };
        if let Some(current) = current_manifest {
            manifest_history
                .reconcile_current(current)
                .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
            if !read_only {
                let bytes = manifest_history
                    .to_bytes()
                    .ok_or_else(|| Error::Wal("manifest history is too large".into()))?;
                // Rewrite only at open/reconciliation boundaries. Normal
                // commits append one checksummed frame below.
                atomic_write(&manifest_history_path, &bytes)?;
            }
        } else if manifest_history.latest().is_some() {
            return Err(Error::Corruption(
                "manifest history exists without an authoritative manifest".into(),
            ));
        }

        let reuse_ledger_path = path.join(REUSE_LEDGER_FILE);
        let mut reuse_ledger = if reuse_ledger_path.is_file() {
            let bytes = fs::read(&reuse_ledger_path)?;
            ReuseLedger::from_bytes(&bytes).map_err(|message| {
                let error = Error::Corruption(format!("reuse ledger {message}"));
                if check_only {
                    Self::map_check_error(CheckFailureKind::Format, error)
                } else {
                    error
                }
            })?
        } else {
            ReuseLedger::new()
        };
        let pruned_reuse_attempts = reuse_ledger.prune_published(&manifest_history);
        if pruned_reuse_attempts > 0 && !read_only && !check_only {
            Self::persist_reuse_ledger_at(&path, &reuse_ledger)?;
        }
        if current_manifest.is_none() && !reuse_ledger.attempts().is_empty() {
            return Err(Error::Corruption(
                "reuse ledger exists without an authoritative manifest".into(),
            ));
        }

        let mut next_commit_id = CommitId::new(
            commit_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("commit ID overflow".into()))?,
        );
        let mut next_generation_id = GenerationId::new(
            generation_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        for attempt in reuse_ledger.attempts() {
            let reserved_commit = CommitId::new(
                attempt
                    .commit_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("commit ID overflow".into()))?,
            );
            let reserved_generation = GenerationId::new(
                attempt
                    .generation_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
            );
            if reserved_commit > next_commit_id {
                next_commit_id = reserved_commit;
            }
            if reserved_generation > next_generation_id {
                next_generation_id = reserved_generation;
            }
        }

        let protected_offsets = Arc::new(Mutex::new(HashSet::new()));
        let retention_path = path.join(RETENTION_FILE);
        let retention = Arc::new(Mutex::new(
            RetentionState::load(retention_path, Arc::clone(&protected_offsets)).map_err(
                |error| {
                    if check_only {
                        Self::map_check_error(CheckFailureKind::Format, error)
                    } else {
                        error
                    }
                },
            )?,
        ));
        if !read_only && !check_only {
            Self::cleanup_orphaned_retained_blobs(&path, &retention)?;
        }
        {
            let mut state = retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            let offsets = Self::load_retained_offset_map(&path, &state, database_id, history_id)
                .map_err(|error| {
                    if check_only {
                        Self::map_check_error(CheckFailureKind::Checkpoint, error)
                    } else {
                        error
                    }
                })?;
            state.install_offsets(offsets).map_err(|error| {
                if check_only {
                    Self::map_check_error(CheckFailureKind::Checkpoint, error)
                } else {
                    error
                }
            })?;
        }

        // Open the data file.
        let device_opts = DeviceOptions {
            use_odirect: options.use_odirect,
            sync_writes: options.sync_writes,
            create: !read_only,
        };
        let device = if check_only {
            Device::open_read_only(&data_path, &device_opts)?
        } else {
            Device::open(&data_path, &device_opts)?
        };

        // Create buffer manager.
        let buffer = BufferManager::new(options.buffer_pool_size);

        // Create WAL manager.
        let sync_policy = if options.sync_writes {
            SyncPolicy::FDataSync
        } else {
            SyncPolicy::None
        };
        let wal = WalManager::new(sync_policy);

        // Recover an interrupted mixed-blob rewrite before loading the blob
        // catalog. A rewrite keeps the previous image under a side filename
        // until its maintenance manifest is authoritative.
        let recovered_blob_bytes =
            Self::recover_blob_rewrite_backup(&path, current_manifest, read_only)?;

        // Create blob manager.
        let blob_path = path.join(BLOB_FILE);
        let mut blobs = if blob_path.exists() {
            // Load blob files from disk.
            let blob_data = recovered_blob_bytes
                .as_deref()
                .map_or_else(|| fs::read(&blob_path), |bytes| Ok(bytes.to_vec()))?;
            match parse_blob_catalog(
                &path,
                &blob_data,
                current_manifest.map(|manifest| manifest.generation_id.get()),
            )? {
                Some(blobs) => blobs,
                None if check_only => {
                    return Err(Error::Check {
                        kind: CheckFailureKind::Blob,
                        message: "blob catalog or segment is invalid".into(),
                    });
                }
                None => {
                    return Err(Error::Corruption(
                        "blob catalog or segment is invalid".into(),
                    ));
                }
            }
        } else {
            BlobManager::with_threshold_and_mode(
                options.blob_threshold,
                options.blob_storage == BlobStorageMode::Segmented,
            )
        };
        if current_manifest
            .is_some_and(|current| blobs.generation_id() != current.generation_id.get())
        {
            // A blob image is written before its manifest. If publication
            // stopped between those boundaries, deletion marks from the
            // newer image must not make pages referenced by the older
            // manifest reclaimable.
            blobs.clear_deletion_metadata();
        }

        // A published manifest selects an immutable PMT checkpoint. Never
        // pair an older manifest with a newer mutable metadata file.
        let (pmt, allocator) = if let Some(current) = current_manifest {
            if current.pmt_checkpoint_id.get() == 0 {
                (PMT::new(), PageAllocator::new())
            } else {
                let checkpoint_path =
                    path.join(format!("seerdb.meta.{}", current.pmt_checkpoint_id.get()));
                Self::load_meta(&checkpoint_path).map_err(|error| {
                    if check_only {
                        Self::map_checkpoint_check_error(error)
                    } else {
                        error
                    }
                })?
            }
        } else if meta_path.exists() {
            Self::load_meta(&meta_path).map_err(|error| {
                if check_only {
                    Self::map_checkpoint_check_error(error)
                } else {
                    error
                }
            })?
        } else {
            (PMT::new(), PageAllocator::new())
        };

        // Create storage engine.
        let mut engine = StorageEngine::new_with_protected_offsets(
            BTree::new(),
            buffer,
            pmt,
            allocator,
            device,
            Arc::clone(&protected_offsets),
        );
        let retained_offsets = protected_offsets
            .lock()
            .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))?
            .clone();
        engine.set_protected_offsets(retained_offsets)?;

        // A published manifest selects the PMT locations for the latest
        // generation. Without one, retain the legacy scan as a migration path.
        if let Some(current) = current_manifest {
            engine.load_from_manifest(current.root_page_id)?;
        } else if !wal_path.exists() {
            engine.load_from_disk()?;
        }

        // WAL replay mutates the logical tree, so materialize a lazily opened
        // generation before applying a committed recovery prefix. A clean
        // reopen remains lazy and serves reads directly through the PMT.
        if wal_path.exists() && !check_only {
            engine.ensure_materialized()?;
        }

        let recovery = if wal_path.exists() && !check_only {
            Some(Self::recover_from_wal(
                &wal_path,
                current_manifest,
                engine.btree_mut(),
                &mut blobs,
            )?)
        } else {
            None
        };

        let mut db = Self {
            path,
            options,
            engine,
            wal,
            blobs,
            vacuum: None,
            retention,
            txn_manager: TransactionManager::new(),
            manifest,
            manifest_history,
            reuse_ledger,
            database_id,
            history_id,
            generation_id,
            commit_id,
            next_commit_id,
            next_generation_id,
            pending_mutations: 0,
            pending_wal_bytes: 0,
            wal_reserved_extent: 0,
            pending_digest: 0,
            pending_blob_changes: recovery
                .as_ref()
                .is_some_and(|summary| summary.blob_changed),
            is_open: true,
            write_fenced: false,
            read_only,
            check_only,
            lock_file,
            wal_admission_failures: 0,
            publication: PublicationMetrics::default(),
            publication_timing: PublicationTimingMetrics::default(),
        };

        if !check_only && current_manifest.is_none() && !wal_path.exists() && !meta_path.exists() {
            let initial = Manifest {
                database_id: db.database_id,
                history_id: db.history_id,
                generation_id: GenerationId::new(0),
                commit_id: CommitId::new(0),
                page_size: PAGE_SIZE as u32,
                root_page_id: db.engine.btree().root_id() as u64,
                pmt_checkpoint_id: PmtCheckpointId::new(0),
                wal_segment: 0,
                wal_offset: 0,
                mutation_count: 0,
                digest: 0,
                format_version: FORMAT_VERSION,
            };
            db.manifest_history.reset(initial);
            db.persist_manifest_history(&db.manifest_history)?;
            db.manifest.publish(initial)?;
        }

        if let Some(recovery) = recovery {
            if let Some(commit) = recovery.last_commit {
                db.publish_recovered(commit, recovery.last_commit_offset)?;
            } else {
                // Complete mutations without a commit envelope are not
                // visible in the durable protocol and may be discarded.
                fs::remove_file(&wal_path)?;
            }
        }

        Ok(db)
    }

    /// Insert a key-value pair.
    ///
    /// The mutation is applied in memory, journaled durably, and included in
    /// the next published root generation.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        validate_wal_put_lengths(key, value)?;
        let record = WalRecord::put(key, value);
        self.admit_wal_record(&record)?;
        self.engine.prepare_mutation(key)?;

        // Mutate memory first, then make the successful mutation durable in
        // the WAL. No page is written before the WAL reaches disk, and an
        // operation that fails never enters a committed WAL batch.
        let previous_blob = match self.engine.lookup(key)? {
            LookupResult::Blob(pointer) => Some(pointer),
            _ => None,
        };
        let appended_value_len = self
            .blobs
            .should_separate(value.len())
            .then_some(value.len());
        let had_previous_blob = previous_blob.is_some();
        if had_previous_blob || appended_value_len.is_some() {
            self.admit_blob_image(previous_blob.as_ref(), appended_value_len)?;
        }
        if appended_value_len.is_some() {
            let pointer = self.blobs.append(key, value.to_vec());
            if let Err(error) = self.engine.btree_mut().upsert_blob(key, pointer) {
                let _ = self.blobs.rollback_append(&pointer);
                return Err(error.into());
            }
        } else {
            self.engine.btree_mut().upsert(key, value)?;
        }
        if let Some(pointer) = previous_blob {
            self.blobs.mark_deleted(&pointer);
        }

        self.journal_mutation(record)?;
        self.pending_blob_changes |= had_previous_blob || appended_value_len.is_some();

        Ok(())
    }

    /// Commit multiple byte-key mutations atomically as one durable batch.
    ///
    /// The complete candidate B-tree/blob state is prepared off to the side
    /// before the batch WAL is appended. A validation, capacity, or B-tree
    /// error therefore leaves the current state untouched. Once the WAL batch
    /// is durable, [`DB::flush`] publishes all mutations under one commit
    /// envelope; a failure at that boundary fences the writer for recovery in
    /// the same way as a single mutation.
    pub fn commit_batch(&mut self, mutations: &[BatchMutation]) -> Result<DurabilityStatus> {
        let expected_commit = self.commit_id;
        self.commit_batch_at(expected_commit, mutations)
    }

    /// Commit multiple byte-key mutations only if the published commit still
    /// matches the caller's expected base.
    ///
    /// This is the storage boundary for optimistic transaction adapters. The
    /// expected-base check happens before validation, WAL admission, or any
    /// candidate tree/blob work, so a stale caller has no side effects. An
    /// empty batch is a validated no-op and returns the current durability
    /// status when the expected base matches.
    pub fn commit_batch_at(
        &mut self,
        expected_commit: CommitId,
        mutations: &[BatchMutation],
    ) -> Result<DurabilityStatus> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        if self.commit_id != expected_commit {
            return Err(Error::SerializationConflict {
                expected: expected_commit,
                current: self.commit_id,
            });
        }
        if mutations.is_empty() {
            return Ok(self.durability_status());
        }
        let candidate_started = Instant::now();
        if self.pending_mutations != 0 {
            return Err(Error::InvalidArgument(
                "commit_batch requires a clean pending generation; flush or discard pending mutations first".into(),
            ));
        }

        let mut records = Vec::with_capacity(mutations.len());
        let mut mutation_bytes = 0u64;
        for mutation in mutations {
            let record = match mutation {
                BatchMutation::Put { key, value } => {
                    validate_wal_put_lengths(key, value)?;
                    WalRecord::put(key, value)
                }
                BatchMutation::Delete { key } => {
                    validate_wal_key_length(key)?;
                    WalRecord::delete(key)
                }
            };
            mutation_bytes = mutation_bytes
                .checked_add(record.to_bytes().len() as u64)
                .ok_or(Error::DiskFull)?;
            records.push(record);
        }

        let required_wal = mutation_bytes
            .checked_add(WAL_COMMIT_RECORD_BYTES)
            .ok_or(Error::DiskFull)?;
        let available_wal = self
            .options
            .max_wal_bytes
            .saturating_sub(self.pending_wal_bytes);
        if required_wal > available_wal {
            self.wal_admission_failures = self.wal_admission_failures.saturating_add(1);
            return Err(Error::Backpressure {
                required: required_wal,
                available: available_wal,
            });
        }

        for mutation in mutations {
            let key = match mutation {
                BatchMutation::Put { key, .. } | BatchMutation::Delete { key } => key,
            };
            self.engine.prepare_mutation(key)?;
        }

        let mut candidate_tree = self.engine.btree().clone();
        let mut candidate_blobs = self.blobs.clone();
        let mut blob_changed = false;
        for mutation in mutations {
            match mutation {
                BatchMutation::Put { key, value } => {
                    let previous_blob = match candidate_tree.lookup(key).map_err(Error::from)? {
                        LookupResult::Blob(pointer) => Some(pointer),
                        _ => None,
                    };
                    let separates = candidate_blobs.should_separate(value.len());
                    if separates {
                        let pointer = candidate_blobs.append(key, value.clone());
                        if let Err(error) = candidate_tree.upsert_blob(key, pointer) {
                            let _ = candidate_blobs.rollback_append(&pointer);
                            return Err(error.into());
                        }
                    } else {
                        candidate_tree.upsert(key, value).map_err(Error::from)?;
                    }
                    if let Some(pointer) = previous_blob {
                        if !candidate_blobs.mark_deleted(&pointer) {
                            return Err(Error::Corruption(
                                "batch replacement references a missing blob".into(),
                            ));
                        }
                        blob_changed = true;
                    }
                    blob_changed |= separates;
                }
                BatchMutation::Delete { key } => {
                    let previous_blob = match candidate_tree.lookup(key).map_err(Error::from)? {
                        LookupResult::Blob(pointer) => Some(pointer),
                        _ => None,
                    };
                    let _ = candidate_tree.delete(key).map_err(Error::from)?;
                    if let Some(pointer) = previous_blob {
                        if !candidate_blobs.mark_deleted(&pointer) {
                            return Err(Error::Corruption(
                                "batch delete references a missing blob".into(),
                            ));
                        }
                        blob_changed = true;
                    }
                }
            }
        }

        if blob_changed {
            let projected = Self::blob_publication_size(&candidate_blobs)?;
            self.engine.check_artifact_capacity(projected)?;
        }
        self.publication_timing.candidate_prepare_ns = self
            .publication_timing
            .candidate_prepare_ns
            .saturating_add(elapsed_nanos(candidate_started));

        self.ensure_wal_reservation()?;
        if blob_changed && !candidate_blobs.is_segmented() {
            let projected = Self::blob_publication_size(&candidate_blobs)?;
            self.reserve_blob_image(projected)?;
        }

        let next_pending_mutations = u64::try_from(mutations.len())
            .ok()
            .and_then(|count| self.pending_mutations.checked_add(count))
            .ok_or(Error::Wal("mutation count overflow".into()))?;
        let next_pending_bytes = self
            .pending_wal_bytes
            .checked_add(mutation_bytes)
            .ok_or(Error::Wal("WAL byte count overflow".into()))?;
        let next_digest = records.iter().fold(self.pending_digest, |digest, record| {
            extend_digest(digest, record)
        });

        for record in &records {
            self.wal.append(record);
        }
        if let Err(error) = self.write_wal_to_disk(self.wal.sync_policy() != SyncPolicy::None) {
            self.write_fenced = true;
            return Err(error);
        }

        *self.engine.btree_mut() = candidate_tree;
        self.blobs = candidate_blobs;
        self.pending_blob_changes = blob_changed;
        self.pending_mutations = next_pending_mutations;
        self.pending_wal_bytes = next_pending_bytes;
        self.pending_digest = next_digest;
        self.flush()?;
        Ok(self.durability_status())
    }

    /// Get a value by key.
    ///
    /// Read path:
    /// 1. Lookup key in B-tree
    /// 2. If value is inline, return it
    /// 3. If value is blob pointer, read from blob file
    /// 4. If deleted (tombstone), return None
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_readable()?;

        match self.engine.lookup(key)? {
            LookupResult::Found(value) => Ok(Some(value)),
            LookupResult::Blob(ptr) => {
                // Read from blob file.
                match self.blobs.read(&ptr) {
                    Some(data) => Ok(Some(data.to_vec())),
                    None => Err(Error::Corruption("blob pointer invalid".into())),
                }
            }
            LookupResult::Deleted => Ok(None),
            LookupResult::NotFound => Ok(None),
        }
    }

    /// Get a value from a retained historical root.
    ///
    /// IDs returned by [`DB::retain_commit`] are durable across reopen. IDs
    /// owned by [`DB::begin_batch_transaction`] are process-local and expire
    /// when the transaction or process ends.
    ///
    /// The page lookup uses the retained PMT over this handle's device and
    /// buffer pool. Blob values resolve through the immutable blob image
    /// captured with the same retention lease.
    pub fn get_at(&self, snapshot_id: SnapshotId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_readable()?;
        let manifest = self.retained_manifest(snapshot_id)?;
        let pmt = self.retained_pmt(manifest)?;
        let result = self.engine.lookup_at(manifest.root_page_id, &pmt, key)?;
        self.lookup_result_value(result, snapshot_id)
    }

    /// Delete a key.
    ///
    /// The tombstone is applied in memory, journaled durably, and included in
    /// the next published root generation.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        validate_wal_key_length(key)?;
        let record = WalRecord::delete(key);
        self.admit_wal_record(&record)?;
        self.engine.prepare_mutation(key)?;

        let previous_blob = match self.engine.lookup(key)? {
            LookupResult::Blob(pointer) => Some(pointer),
            _ => None,
        };
        if previous_blob.is_some() {
            self.admit_blob_image(previous_blob.as_ref(), None)?;
        }
        let found = self.engine.btree_mut().delete(key)?;
        let blob_changed = found && previous_blob.is_some();
        if blob_changed && let Some(pointer) = previous_blob {
            self.blobs.mark_deleted(&pointer);
        }
        self.journal_mutation(record)?;
        self.pending_blob_changes |= blob_changed;
        Ok(found)
    }

    /// Range scan over [start, end).
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_readable()?;
        self.engine
            .range(start, end)?
            .into_iter()
            .filter_map(|(key, value)| match value {
                crate::btree::LookupResult::Found(value) => Some(Ok((key, value))),
                crate::btree::LookupResult::Blob(pointer) => Some(
                    self.blobs
                        .read(&pointer)
                        .map(|value| (key, value.to_vec()))
                        .ok_or_else(|| Error::Corruption("blob pointer invalid".into())),
                ),
                crate::btree::LookupResult::Deleted | crate::btree::LookupResult::NotFound => None,
            })
            .collect()
    }

    /// Scan a range from a retained historical root.
    ///
    /// Named retention IDs are durable across reopen; transaction-owned IDs
    /// are process-local and are not persisted in the named registry.
    pub fn range_at(
        &self,
        snapshot_id: SnapshotId,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_readable()?;
        let manifest = self.retained_manifest(snapshot_id)?;
        let pmt = self.retained_pmt(manifest)?;
        let blob_path = retained_blob_path(&self.path, snapshot_id);
        let blob_bytes = fs::read(&blob_path)?;
        let blobs = BlobManager::from_bytes(&blob_bytes).ok_or_else(|| {
            Error::Corruption(format!(
                "retained snapshot {} has an invalid blob image",
                snapshot_id.get()
            ))
        })?;
        self.engine
            .range_at(manifest.root_page_id, &pmt, start, end)?
            .into_iter()
            .filter_map(|(key, value)| match value {
                LookupResult::Found(value) => Some(Ok((key, value))),
                LookupResult::Blob(pointer) => Some(
                    blobs
                        .read(&pointer)
                        .map(|value| (key, value.to_vec()))
                        .ok_or_else(|| {
                            Error::Corruption(format!(
                                "retained snapshot {} has an invalid blob pointer",
                                snapshot_id.get()
                            ))
                        }),
                ),
                LookupResult::Deleted | LookupResult::NotFound => None,
            })
            .collect()
    }

    fn retained_manifest(&self, snapshot_id: SnapshotId) -> Result<Manifest> {
        let state = self
            .retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
        state
            .all_roots()
            .find(|root| root.snapshot_id == snapshot_id)
            .map(|root| root.manifest)
            .ok_or_else(|| {
                Error::SnapshotUnavailable(format!(
                    "retained root {} is not active",
                    snapshot_id.get()
                ))
            })
    }

    fn retained_pmt(&self, manifest: Manifest) -> Result<PMT> {
        if manifest.pmt_checkpoint_id.get() == 0 {
            return Ok(PMT::new());
        }
        let checkpoint = self
            .path
            .join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
        Self::load_meta(&checkpoint).map(|(pmt, _)| pmt)
    }

    fn lookup_result_value(
        &self,
        result: LookupResult,
        snapshot_id: SnapshotId,
    ) -> Result<Option<Vec<u8>>> {
        match result {
            LookupResult::Found(value) => Ok(Some(value)),
            LookupResult::Blob(pointer) => {
                let blob_path = retained_blob_path(&self.path, snapshot_id);
                let blob_bytes = fs::read(&blob_path)?;
                let blobs = BlobManager::from_bytes(&blob_bytes).ok_or_else(|| {
                    Error::Corruption(format!(
                        "retained snapshot {} has an invalid blob image",
                        snapshot_id.get()
                    ))
                })?;
                blobs
                    .read(&pointer)
                    .map(|value| Some(value.to_vec()))
                    .ok_or_else(|| {
                        Error::Corruption(format!(
                            "retained snapshot {} has an invalid blob pointer",
                            snapshot_id.get()
                        ))
                    })
            }
            LookupResult::Deleted | LookupResult::NotFound => Ok(None),
        }
    }

    /// Write buffered WAL records to disk and optionally force the prefix.
    fn write_wal_to_disk(&mut self, force_sync: bool) -> Result<()> {
        let started = Instant::now();
        let result = self.write_wal_to_disk_inner(force_sync);
        self.publication_timing.wal_write_ns = self
            .publication_timing
            .wal_write_ns
            .saturating_add(elapsed_nanos(started));
        result
    }

    fn write_wal_to_disk_inner(&mut self, force_sync: bool) -> Result<()> {
        let mut wal_buf = Vec::new();
        self.wal.flush(&mut wal_buf)?;
        let should_sync = force_sync || self.wal.sync_policy() != SyncPolicy::None;
        if !wal_buf.is_empty() || should_sync {
            let wal_path = self.path.join(WAL_FILE);
            let mut file = OpenOptions::new()
                .create(true)
                .append(!wal_buf.is_empty())
                .read(should_sync)
                .write(!wal_buf.is_empty() || should_sync)
                .open(&wal_path)?;
            if !wal_buf.is_empty() {
                // Append to WAL file (not overwrite).
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_WRITE.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected WAL append failure").into());
                }
                use std::io::Write;
                file.write_all(&wal_buf)?;
                self.publication.wal_bytes_written = self
                    .publication
                    .wal_bytes_written
                    .saturating_add(wal_buf.len() as u64);
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_AFTER_WRITE.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected post-append WAL failure").into());
                }
            }
            if should_sync {
                // The commit boundary and any configured per-mutation policy
                // force the WAL before dependent page publication.
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_SYNC.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected WAL sync failure").into());
                }
                match self.wal.sync_policy() {
                    SyncPolicy::SyncAll => file.sync_all()?,
                    SyncPolicy::FDataSync | SyncPolicy::None => file.sync_data()?,
                }
                #[cfg(any(test, feature = "fault-injection"))]
                if FAIL_NEXT_WAL_AFTER_SYNC.with(|failure| failure.replace(false)) {
                    return Err(std::io::Error::other("injected post-WAL-sync failure").into());
                }
            }
        }
        Ok(())
    }

    /// Journal a mutation after it has successfully changed memory state.
    fn journal_mutation(&mut self, record: WalRecord) -> Result<()> {
        self.wal.append(&record);
        let sync_mutation = self.wal.sync_policy() != SyncPolicy::None;
        if let Err(error) = self.write_wal_to_disk(sync_mutation) {
            self.write_fenced = true;
            return Err(error);
        }
        self.pending_mutations = self
            .pending_mutations
            .checked_add(1)
            .ok_or_else(|| Error::Wal("mutation count overflow".into()))?;
        self.pending_wal_bytes = self
            .pending_wal_bytes
            .checked_add(record.to_bytes().len() as u64)
            .ok_or_else(|| Error::Wal("WAL byte count overflow".into()))?;
        self.pending_digest = extend_digest(self.pending_digest, &record);
        Ok(())
    }

    /// Reserve enough logical WAL budget for one mutation and the commit that
    /// closes its pending generation. This runs before any tree or blob state
    /// changes, so retryable backpressure cannot leave a partial mutation.
    fn admit_wal_record(&mut self, record: &WalRecord) -> Result<()> {
        let used = self.pending_wal_bytes;
        let required = (record.to_bytes().len() as u64)
            .checked_add(WAL_COMMIT_RECORD_BYTES)
            .ok_or(Error::DiskFull)?;
        let available = self.options.max_wal_bytes.saturating_sub(used);
        if required > available {
            self.wal_admission_failures = self.wal_admission_failures.saturating_add(1);
            return Err(Error::Backpressure {
                required,
                available,
            });
        }

        self.ensure_wal_reservation()?;
        Ok(())
    }

    /// Reserve the next blob image before a mutation changes memory state.
    ///
    /// Linux and macOS retain the physical reservation in a sidecar that is
    /// consumed by the next atomic blob publication. Other platforms use a
    /// best-effort filesystem-space check and keep the final write fallback.
    fn admit_blob_image(
        &self,
        retired: Option<&BlobPointer>,
        appended_value_len: Option<usize>,
    ) -> Result<()> {
        let required = if self.blobs.is_segmented() {
            self.blobs
                .projected_segment_write_size(retired, appended_value_len)
        } else {
            self.blobs
                .projected_serialized_size(retired, appended_value_len)
        }
        .ok_or_else(|| Error::InvalidArgument("blob image size overflows".into()))?;
        self.engine.check_artifact_capacity(required)?;
        if self.blobs.is_segmented() {
            return Ok(());
        }
        self.reserve_blob_image(required)
    }

    fn blob_publication_size(blobs: &BlobManager) -> Result<u64> {
        if blobs.is_segmented() {
            blobs
                .segment_write_size()
                .ok_or_else(|| Error::InvalidArgument("blob catalog size overflows".into()))
        } else {
            blobs
                .serialized_size()
                .ok_or_else(|| Error::InvalidArgument("blob image size overflows".into()))
        }
    }

    fn reserve_blob_image(&self, required: u64) -> Result<()> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let reservation_path = self.path.join(BLOB_RESERVATION_FILE);
            let file = match OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&reservation_path)
            {
                Ok(file) => file,
                Err(error) => {
                    let _ = fs::remove_file(&reservation_path);
                    return Err(error.into());
                }
            };
            if let Err(error) = reserve_file(&file, required) {
                drop(file);
                let _ = fs::remove_file(&reservation_path);
                return Err(error.into());
            }
            file.set_len(required)?;
            file.sync_data()?;
            sync_directory(&self.path)?;
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            if required > fs2::available_space(&self.path)? {
                return Err(Error::DiskFull);
            }
        }

        Ok(())
    }

    /// Ensure the database owns a fixed-size WAL reservation extent before a
    /// mutation changes tree or blob state. The extent is rounded to fixed
    /// segments so future WAL growth has a bounded, stable admission domain;
    /// the logical WAL remains separately length-delimited and checksummed.
    fn ensure_wal_reservation(&mut self) -> Result<u64> {
        let target = self.wal_reservation_target()?;
        if target == 0 {
            self.wal_reserved_extent = 0;
            return Ok(0);
        }
        if self.wal_reserved_extent >= target {
            return Ok(self.wal_reserved_extent);
        }

        // Reserve the physical extent on the file that will receive WAL
        // bytes. A separate sidecar would consume capacity without protecting
        // the actual append path.
        let path = self.path.join(WAL_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let current = file.metadata()?.len();
        if current < target {
            let physically_reserved = reserve_file(&file, target)?;
            if !physically_reserved
                && fs2::available_space(&self.path)? < target.saturating_sub(current)
            {
                return Err(Error::DiskFull);
            }
            file.sync_data()?;
            sync_directory(&self.path)?;
        }
        self.wal_reserved_extent = target;
        Ok(current.max(target))
    }

    fn wal_reservation_target(&self) -> Result<u64> {
        if self.options.max_wal_bytes == 0 {
            return Ok(0);
        }

        let remainder = self.options.max_wal_bytes % WAL_RESERVATION_SEGMENT_BYTES;
        self.options
            .max_wal_bytes
            .checked_add(
                (WAL_RESERVATION_SEGMENT_BYTES - remainder) % WAL_RESERVATION_SEGMENT_BYTES,
            )
            .ok_or(Error::DiskFull)
    }

    fn write_blob_image(&self, path: &Path, data: &[u8]) -> Result<u64> {
        self.write_blob_image_with_directory_sync(path, data, true)
    }

    /// Write a blob image while deferring the containing-directory sync to the
    /// publication barrier. The caller must sync the directory before making
    /// a new manifest authoritative.
    fn write_blob_image_without_directory_sync(&self, path: &Path, data: &[u8]) -> Result<u64> {
        self.write_blob_image_with_directory_sync(path, data, false)
    }

    /// Append only new record bytes for the segmented blob layout, then
    /// atomically publish its small catalog. A failed append can leave an
    /// ignored suffix; the catalog length is the recovery frontier and the
    /// next publication truncates that suffix before appending.
    fn prepare_segment_catalog_backup(&self, consolidate: bool) -> Result<()> {
        if !consolidate {
            return Ok(());
        }
        let backup_path = self.path.join(BLOB_REWRITE_BACKUP_FILE);
        if backup_path.exists() {
            return Ok(());
        }

        let catalog_path = self.path.join(BLOB_FILE);
        if catalog_path.exists() {
            fs::rename(&catalog_path, &backup_path)?;
            sync_directory(&self.path)?;
            return Ok(());
        }

        // A first segmented publication has no catalog to rename. Keep a
        // valid empty catalog for the generation selected by the current
        // manifest so a failed first catalog publication can recover to the
        // empty database just like later publications recover to their old
        // catalog.
        let generation_id = self
            .manifest_history
            .latest()
            .map_or(0, |manifest| manifest.generation_id.get());
        let mut empty = BlobManager::with_threshold_and_mode(self.blobs.threshold(), true);
        empty.set_generation(generation_id);
        let bytes = empty.to_segment_catalog_bytes();
        atomic_write_without_fault_injection(&backup_path, &bytes)
    }

    fn finish_segment_catalog_backup(&self) -> Result<()> {
        let backup_path = self.path.join(BLOB_REWRITE_BACKUP_FILE);
        let consolidated = backup_path.exists();
        if consolidated {
            fs::remove_file(backup_path)?;
            let delta_path = self.path.join(BLOB_DELTA_FILE);
            if delta_path.exists() {
                fs::remove_file(delta_path)?;
            }
            sync_directory(&self.path)?;
        }
        Ok(())
    }

    fn append_segment_catalog_delta(&self, delta: &[u8]) -> Result<u64> {
        let delta_path = self.path.join(BLOB_DELTA_FILE);
        let existing = match fs::read(&delta_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let prefix = BlobManager::segment_catalog_delta_prefix_len_through_generation(
            &existing,
            self.blobs.persisted_segment_catalog_generation(),
        )
        .ok_or_else(|| Error::Corruption("segmented catalog delta log is invalid".into()))?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&delta_path)?;
        if u64::try_from(prefix).map_err(|_| Error::DiskFull)? != file.metadata()?.len() {
            file.set_len(u64::try_from(prefix).map_err(|_| Error::DiskFull)?)?;
        }
        file.seek(SeekFrom::Start(
            u64::try_from(prefix).map_err(|_| Error::DiskFull)?,
        ))?;
        file.write_all(delta)?;
        file.flush()?;

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected blob catalog sync failure").into());
        }
        file.sync_all()?;
        Ok(delta.len() as u64)
    }

    fn write_blob_segments(&mut self) -> Result<u64> {
        let catalog_path = self.path.join(BLOB_FILE);
        let consolidate = !catalog_path.exists() || self.blobs.catalog_needs_consolidation();
        self.prepare_segment_catalog_backup(consolidate)?;
        let mut bytes_written = 0u64;
        for file_id in self.blobs.segment_file_ids() {
            let data = self
                .blobs
                .segment_bytes(file_id)
                .ok_or_else(|| Error::Corruption("blob segment disappeared from catalog".into()))?;
            let persisted = self.blobs.persisted_segment_length(file_id);
            let persisted_usize = usize::try_from(persisted)
                .map_err(|_| Error::Corruption("blob segment length overflows usize".into()))?;
            if data.len() < persisted_usize {
                return Err(Error::Corruption(
                    "blob segment shrank below its catalog frontier".into(),
                ));
            }
            if data.len() == persisted_usize {
                continue;
            }

            let segment_path = blob_segment_path(&self.path, file_id);
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&segment_path)?;
            let physical_len = file.metadata()?.len();
            if physical_len < persisted {
                return Err(Error::Corruption(
                    "blob segment is shorter than its catalog frontier".into(),
                ));
            }
            if physical_len != persisted {
                file.set_len(persisted)?;
            }
            let new_len = data.len() as u64;
            reserve_file(&file, new_len)?;
            file.set_len(new_len)?;
            file.seek(SeekFrom::Start(persisted))?;
            file.write_all(&data[persisted_usize..])?;
            file.flush()?;

            #[cfg(any(test, feature = "fault-injection"))]
            if FAIL_NEXT_BLOB_SEGMENT_SYNC.with(|failure| failure.replace(false)) {
                return Err(std::io::Error::other("injected blob segment sync failure").into());
            }
            file.sync_all()?;
            bytes_written = bytes_written.saturating_add(new_len - persisted);

            #[cfg(any(test, feature = "fault-injection"))]
            if FAIL_NEXT_BLOB_SEGMENT_AFTER_WRITE.with(|failure| failure.replace(false)) {
                return Err(
                    std::io::Error::other("injected failure after blob segment write").into(),
                );
            }
        }

        if consolidate {
            let catalog = self.blobs.to_segment_catalog_bytes();
            atomic_write_without_directory_sync(&catalog_path, &catalog)?;
            bytes_written = bytes_written.saturating_add(catalog.len() as u64);
            self.blobs.mark_segment_catalog_consolidated();
        } else {
            let delta = self
                .blobs
                .to_segment_catalog_delta_bytes()
                .ok_or_else(|| Error::Corruption("segmented catalog delta overflows".into()))?;
            bytes_written =
                bytes_written.saturating_add(self.append_segment_catalog_delta(&delta)?);
            self.blobs.mark_segment_delta_persisted();
        }
        Ok(bytes_written)
    }

    /// Remove segment files no longer named by the authoritative active
    /// catalog. This runs only after the manifest publication barrier, so an
    /// interrupted rewrite leaves the old segments available for catalog
    /// recovery.
    fn prune_unreferenced_blob_segments(&self) -> Result<()> {
        let live = self
            .blobs
            .segment_file_ids()
            .into_iter()
            .collect::<HashSet<_>>();
        let mut removed = false;
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let Some(file_id) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix(BLOB_SEGMENT_PREFIX))
                .and_then(|suffix| suffix.parse::<u32>().ok())
            else {
                continue;
            };
            if !live.contains(&file_id) {
                fs::remove_file(entry.path())?;
                removed = true;
            }
        }
        if removed {
            sync_directory(&self.path)?;
        }
        Ok(())
    }

    fn write_blob_image_with_directory_sync(
        &self,
        path: &Path,
        data: &[u8],
        sync_parent: bool,
    ) -> Result<u64> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let reservation = self.path.join(BLOB_RESERVATION_FILE);
            if reservation.is_file() {
                atomic_write_reserved(path, &reservation, data, sync_parent)?;
                return Ok(data.len() as u64);
            }
        }

        if sync_parent {
            atomic_write(path, data)?;
        } else {
            atomic_write_without_directory_sync(path, data)?;
        }
        Ok(data.len() as u64)
    }

    /// Reconcile a blob image or segmented catalog kept aside while a blob
    /// publication crosses the blob-image/manifest boundary.
    ///
    /// The backup has the generation selected by the prior manifest. If that
    /// generation is still authoritative, the publication did not complete
    /// and the backup must be restored. If the manifest advanced, publication
    /// completed and the backup is only stale cleanup state.
    fn recover_blob_rewrite_backup(
        path: &Path,
        current_manifest: Option<Manifest>,
        read_only: bool,
    ) -> Result<Option<Vec<u8>>> {
        let backup_path = path.join(BLOB_REWRITE_BACKUP_FILE);
        if !backup_path.is_file() {
            return Ok(None);
        }

        let manifest_generation = current_manifest.map(|manifest| manifest.generation_id.get());
        let blob_path = path.join(BLOB_FILE);
        if let Some(manifest_generation) = manifest_generation
            && blob_path.is_file()
            && let Some(blobs) = fs::read(&blob_path).ok().and_then(|bytes| {
                parse_blob_catalog(path, &bytes, Some(manifest_generation))
                    .ok()
                    .flatten()
            })
            && blobs.generation_id() == manifest_generation
        {
            if !read_only {
                fs::remove_file(&backup_path)?;
                if blobs.is_segmented() {
                    let delta_path = path.join(BLOB_DELTA_FILE);
                    if delta_path.exists() {
                        fs::remove_file(delta_path)?;
                    }
                }
                sync_directory(path)?;
            }
            return Ok(None);
        }

        let backup_bytes = fs::read(&backup_path)?;
        let backup_blobs = parse_blob_catalog(path, &backup_bytes, manifest_generation)?
            .ok_or_else(|| {
                Error::Corruption("interrupted blob rewrite backup is invalid".into())
            })?;
        let Some(manifest_generation) = manifest_generation else {
            if read_only {
                return Ok(Some(backup_bytes));
            }
            if blob_path.exists() {
                fs::remove_file(&blob_path)?;
            }
            fs::rename(&backup_path, &blob_path)?;
            sync_directory(path)?;
            return Ok(None);
        };

        let backup_generation = backup_blobs.generation_id();
        if backup_generation > manifest_generation {
            return Err(Error::Corruption(format!(
                "blob rewrite backup generation {} is newer than manifest {}",
                backup_generation, manifest_generation
            )));
        }
        if backup_generation < manifest_generation {
            if !read_only {
                fs::remove_file(&backup_path)?;
                sync_directory(path)?;
            }
            return Ok(None);
        }

        let current_blobs = if blob_path.is_file() {
            fs::read(&blob_path)
                .ok()
                .and_then(|bytes| parse_blob_catalog(path, &bytes, None).ok().flatten())
        } else {
            None
        };
        let needs_restore = current_blobs
            .as_ref()
            .is_none_or(|blobs| blobs.generation_id() != manifest_generation);
        if !needs_restore {
            if !read_only {
                fs::remove_file(&backup_path)?;
                sync_directory(path)?;
            }
            return Ok(None);
        }
        if read_only {
            return Ok(Some(backup_bytes));
        }

        if blob_path.exists() {
            fs::remove_file(&blob_path)?;
        }
        fs::rename(&backup_path, &blob_path)?;
        sync_directory(path)?;
        Ok(None)
    }

    fn persist_manifest_history(&self, history: &ManifestHistory) -> Result<()> {
        self.persist_manifest_history_with_directory_sync(history, true)
    }

    fn persist_manifest_history_without_directory_sync(
        &self,
        history: &ManifestHistory,
    ) -> Result<()> {
        self.persist_manifest_history_with_directory_sync(history, false)
    }

    fn persist_manifest_history_with_directory_sync(
        &self,
        history: &ManifestHistory,
        sync_parent: bool,
    ) -> Result<()> {
        let bytes = history
            .to_bytes()
            .ok_or_else(|| Error::Wal("manifest history is too large".into()))?;
        if sync_parent {
            atomic_write(&self.path.join(MANIFEST_HISTORY_FILE), &bytes)
        } else {
            atomic_write_without_directory_sync(&self.path.join(MANIFEST_HISTORY_FILE), &bytes)
        }
    }

    fn persist_reuse_ledger(&self) -> Result<()> {
        Self::persist_reuse_ledger_at(&self.path, &self.reuse_ledger)
    }

    fn persist_reuse_ledger_at(path: &Path, ledger: &ReuseLedger) -> Result<()> {
        let ledger_path = path.join(REUSE_LEDGER_FILE);
        if ledger.attempts().is_empty() {
            if ledger_path.exists() {
                fs::remove_file(ledger_path)?;
                sync_directory(path)?;
            }
            return Ok(());
        }
        let bytes = ledger
            .to_bytes()
            .ok_or_else(|| Error::Wal("reuse ledger is too large".into()))?;
        atomic_write_without_fault_injection(&ledger_path, &bytes)
    }

    /// Admit the complete candidate publication before the first page write.
    ///
    /// Individual atomic artifacts reserve their own temporary files later in
    /// publication. A filesystem can therefore report ENOSPC after the data
    /// page generation has already reached the device unless their aggregate
    /// footprint is checked first. This is a conservative same-filesystem
    /// guard; a concurrent external consumer can still force the final
    /// write-time DiskFull/recovery path.
    fn preflight_publication_capacity(&self) -> Result<()> {
        let dirty_page_count = self
            .engine
            .btree()
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| self.engine.btree().node(*page_id).is_some())
            .count() as u64;
        let reused_page_count = self.engine.pending_reuse_offsets().len() as u64;
        let new_page_count = dirty_page_count.saturating_sub(reused_page_count);

        let pmt_bytes = (self.engine.pmt().to_bytes().len() as u64)
            .checked_add(
                dirty_page_count
                    .checked_mul((8 + PageMapping::SERIALIZED_SIZE) as u64)
                    .ok_or(Error::DiskFull)?,
            )
            .ok_or(Error::DiskFull)?;
        let allocator_bytes = (self.engine.allocator().to_bytes().len() as u64)
            .checked_add(dirty_page_count.checked_mul(8).ok_or(Error::DiskFull)?)
            .ok_or(Error::DiskFull)?;
        let full_meta_bytes = (META_MAGIC.len() as u64)
            .checked_add(4 + 4 + 4 + 4)
            .and_then(|size| size.checked_add(pmt_bytes))
            .and_then(|size| size.checked_add(allocator_bytes))
            .ok_or(Error::DiskFull)?;
        let parent = self
            .manifest_history
            .latest()
            .unwrap_or_else(|| self.bootstrap_manifest());
        let (checkpoint_meta_bytes, _) =
            self.generation_meta_bytes(parent, dirty_page_count as usize)?;
        let legacy_meta_bytes = if self.path.join(META_FILE).is_file() {
            0
        } else {
            full_meta_bytes
        };
        let blob_bytes = Self::blob_publication_size(&self.blobs)?;
        let ledger_bytes = self
            .reuse_ledger
            .to_bytes()
            .ok_or_else(|| Error::Wal("reuse ledger is too large".into()))?
            .len() as u64;
        let history_entry_bytes = ManifestHistory::entry_bytes(Manifest {
            database_id: self.database_id,
            history_id: self.history_id,
            generation_id: GenerationId::new(0),
            commit_id: CommitId::new(0),
            page_size: PAGE_SIZE as u32,
            root_page_id: 0,
            pmt_checkpoint_id: PmtCheckpointId::new(0),
            wal_segment: 0,
            wal_offset: 0,
            mutation_count: 0,
            digest: 0,
            format_version: FORMAT_VERSION,
        })
        .len() as u64;
        let history_length = match fs::metadata(self.path.join(MANIFEST_HISTORY_FILE)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => self
                .manifest_history
                .to_bytes()
                .map_or(0, |bytes| bytes.len() as u64),
            Err(error) => return Err(error.into()),
        };
        let history_bytes = history_length
            .checked_add(history_entry_bytes)
            .ok_or(Error::DiskFull)?;
        let required = new_page_count
            .checked_mul(PAGE_SIZE as u64)
            .and_then(|size| size.checked_add(checkpoint_meta_bytes))
            .and_then(|size| size.checked_add(legacy_meta_bytes))
            .and_then(|size| size.checked_add(blob_bytes))
            .and_then(|size| size.checked_add(ledger_bytes))
            .and_then(|size| size.checked_add(history_bytes))
            .and_then(|size| size.checked_add(PUBLICATION_CAPACITY_SAFETY_BYTES))
            .ok_or(Error::DiskFull)?;

        if fs2::available_space(&self.path)? < required {
            return Err(Error::CapacityPreflight);
        }
        Ok(())
    }

    /// Admit a maintenance generation before it writes new data pages.
    ///
    /// Maintenance publications do not carry a WAL reservation, so account
    /// for their new data extent and all atomic sidecar artifacts directly.
    /// This is conservative: the metadata argument may be a full checkpoint
    /// even when the selected generation will use a smaller delta.
    fn preflight_maintenance_capacity(
        &self,
        data_bytes: u64,
        metadata_bytes: u64,
        blob_bytes: u64,
    ) -> Result<()> {
        let ledger_bytes = self
            .reuse_ledger
            .to_bytes()
            .ok_or_else(|| Error::Wal("reuse ledger is too large".into()))?
            .len() as u64;
        let history_entry_bytes =
            ManifestHistory::entry_bytes(self.bootstrap_manifest()).len() as u64;
        let history_length = match fs::metadata(self.path.join(MANIFEST_HISTORY_FILE)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => self
                .manifest_history
                .to_bytes()
                .map_or(0, |bytes| bytes.len() as u64),
            Err(error) => return Err(error.into()),
        };
        let history_bytes = history_length
            .checked_add(history_entry_bytes)
            .ok_or(Error::DiskFull)?;
        let required = data_bytes
            .checked_add(metadata_bytes)
            .and_then(|size| size.checked_add(blob_bytes))
            .and_then(|size| size.checked_add(ledger_bytes))
            .and_then(|size| size.checked_add(history_bytes))
            .and_then(|size| size.checked_add(PUBLICATION_CAPACITY_SAFETY_BYTES))
            .ok_or(Error::DiskFull)?;
        if fs2::available_space(&self.path)? < required {
            return Err(Error::CapacityPreflight);
        }
        Ok(())
    }

    fn full_metadata_bytes_for_candidate(candidate: &BTree) -> Result<u64> {
        let page_count = u64::try_from(candidate.node_count()).map_err(|_| Error::DiskFull)?;
        let pmt_bytes = 4u64
            .checked_add(
                page_count
                    .checked_mul((8 + PageMapping::SERIALIZED_SIZE) as u64)
                    .ok_or(Error::DiskFull)?,
            )
            .ok_or(Error::DiskFull)?;
        let allocator_bytes = candidate.page_allocator().to_bytes().len() as u64;
        (META_MAGIC.len() as u64)
            .checked_add(4 + 4 + 4 + 4)
            .and_then(|size| size.checked_add(pmt_bytes))
            .and_then(|size| size.checked_add(allocator_bytes))
            .ok_or(Error::DiskFull)
    }

    /// Bound a metadata delta that may update every current mapping.
    ///
    /// Interior relocation changes existing PMT mappings after the normal
    /// dirty-page admission point. Passing zero dirty pages to
    /// `generation_meta_bytes` would therefore under-account the sidecar that
    /// is written after the relocation. The relocation path does not change
    /// the logical page set, so the current PMT count bounds both its mapping
    /// updates and any conservative removal allowance.
    fn max_metadata_delta_bytes(page_count: usize, allocator: &PageAllocator) -> Result<u64> {
        let page_count = u64::try_from(page_count).map_err(|_| Error::DiskFull)?;
        let update_bytes = page_count
            .checked_mul((8 + PageMapping::SERIALIZED_SIZE) as u64)
            .ok_or(Error::DiskFull)?;
        let removal_bytes = page_count.checked_mul(8).ok_or(Error::DiskFull)?;
        (META_DELTA_HEADER_SIZE as u64)
            .checked_add(update_bytes)
            .and_then(|size| size.checked_add(removal_bytes))
            .and_then(|size| size.checked_add(allocator.to_bytes().len() as u64))
            .and_then(|size| size.checked_add(META_DELTA_CHECKSUM_SIZE as u64))
            .ok_or(Error::DiskFull)
    }

    fn append_manifest_history(&self, manifest: Manifest) -> Result<u64> {
        self.append_manifest_history_with_directory_sync(manifest, true)
    }

    fn append_manifest_history_without_directory_sync(&self, manifest: Manifest) -> Result<u64> {
        self.append_manifest_history_with_directory_sync(manifest, false)
    }

    fn append_manifest_history_with_directory_sync(
        &self,
        manifest: Manifest,
        sync_parent: bool,
    ) -> Result<u64> {
        let path = self.path.join(MANIFEST_HISTORY_FILE);
        let existing_len = fs::metadata(&path)?.len();
        let header_len = u64::try_from(ManifestHistory::header_bytes().len())
            .map_err(|_| Error::Corruption("manifest history header is too large".into()))?;
        let entry_len = u64::try_from(ManifestHistory::entry_bytes(manifest).len())
            .map_err(|_| Error::Corruption("manifest history entry is too large".into()))?;
        if existing_len < header_len {
            return Err(Error::Corruption("manifest history is truncated".into()));
        }

        // A crash may leave a partial final frame. Remove only that tail before
        // appending so recovery never has to scan through a misaligned frame.
        let complete_len = header_len + (existing_len - header_len) / entry_len * entry_len;
        let mut bytes_written = 0u64;
        if complete_len != existing_len {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(complete_len)?;
            file.sync_all()?;
            if sync_parent {
                sync_directory(&self.path)?;
            }
        }

        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        let entry = ManifestHistory::entry_bytes(manifest);
        file.write_all(&entry)?;
        bytes_written = bytes_written.saturating_add(entry.len() as u64);
        file.flush()?;
        file.sync_all()?;
        if sync_parent {
            sync_directory(&self.path)?;
        } else {
            // The caller owns the final publication-directory barrier.
        }
        Ok(bytes_written)
    }

    /// Publish a generation after its pages and checkpoints are durable.
    fn publish_generation(
        &mut self,
        commit: CommitRecord,
        append_commit: bool,
        recovered_wal_offset: u64,
    ) -> Result<()> {
        let parent_manifest = self
            .manifest_history
            .latest()
            .unwrap_or_else(|| self.bootstrap_manifest());
        if self.engine.reclamation_needs_refresh() {
            self.engine.refresh_reclamation()?;
        }
        let reuse_offsets = self.engine.pending_reuse_offsets();
        let reused_slots = !reuse_offsets.is_empty();
        // Retire the older manifest slot before page writes can reuse any
        // physical versions it names. Append-only generations do not need
        // this extra sync because the older fallback root names untouched
        // pages; reused slots must fence that root before page write-back.
        if reused_slots {
            let started = Instant::now();
            let result = self.mirror_current_manifest();
            self.publication_timing.manifest_mirror_ns = self
                .publication_timing
                .manifest_mirror_ns
                .saturating_add(elapsed_nanos(started));
            result?;
        }
        // Mutation records have already been written to the WAL by the
        // mutation or batch admission path. The commit envelope is appended
        // and forced only after the new out-of-place pages are durable below.
        // The manifest remains the visibility barrier, so forcing the
        // uncommitted mutation prefix before page write-back adds a sync
        // without strengthening recovery: an incomplete publication still
        // reopens the old root, while a durable commit record is enough to
        // replay the complete generation.
        self.write_wal_to_disk(false)?;
        self.reuse_ledger
            .push(ReuseAttempt {
                commit_id: commit.commit_id,
                generation_id: commit.generation_id,
                offsets: reuse_offsets,
            })
            .map_err(|message| Error::Corruption(format!("reuse ledger {message}")))?;
        let admission_started = Instant::now();
        let preflight_result = self.preflight_publication_capacity();
        self.publication_timing.admission_ns = self
            .publication_timing
            .admission_ns
            .saturating_add(elapsed_nanos(admission_started));
        if let Err(error) = preflight_result {
            self.reuse_ledger.remove_generation(commit.generation_id);
            return Err(error);
        }
        self.persist_reuse_ledger()?;
        let flush_started = Instant::now();
        let flush_result = self.engine.flush_after_reclamation_refresh();
        self.publication_timing.data_flush_ns = self
            .publication_timing
            .data_flush_ns
            .saturating_add(elapsed_nanos(flush_started));
        if let Err(error) = flush_result {
            // Capacity preflight is guaranteed to issue no page I/O, so its
            // reservation can be removed and the mutation remains retryable.
            // Every other error leaves the reservation durable until reopen
            // proves whether this generation reached manifest history.
            if matches!(&error, Error::CapacityPreflight)
                && self.reuse_ledger.remove_generation(commit.generation_id)
            {
                self.persist_reuse_ledger()?;
            }
            return Err(error);
        }

        let metadata_started = Instant::now();
        let checkpoint_path = self
            .path
            .join(format!("seerdb.meta.{}", commit.generation_id.get()));
        let checkpoint_bytes = self.save_generation_meta(&checkpoint_path, parent_manifest)?;
        // Keep the legacy filename as a compatibility/debug snapshot. It is
        // never authoritative once a manifest selects a checkpoint. Write it
        // only once so it does not turn every delta publication back into a
        // whole-image metadata write.
        let meta_path = self.path.join(META_FILE);
        let legacy_meta_bytes = if meta_path.is_file() {
            0
        } else {
            Self::save_meta_without_directory_sync(
                &meta_path,
                self.engine.pmt(),
                self.engine.allocator(),
            )?
        };
        self.publication.metadata_bytes_written = self
            .publication
            .metadata_bytes_written
            .saturating_add(checkpoint_bytes)
            .saturating_add(legacy_meta_bytes);
        self.publication_timing.metadata_write_ns = self
            .publication_timing
            .metadata_write_ns
            .saturating_add(elapsed_nanos(metadata_started));

        let blob_started = Instant::now();
        let blob_bytes = if self.blobs.is_segmented() || self.pending_blob_changes {
            self.blobs.set_generation(commit.generation_id.get());
            if self.blobs.is_segmented() {
                self.write_blob_segments()?
            } else {
                let blob_path = self.path.join(BLOB_FILE);
                let blob_image = self.blobs.to_bytes();
                self.write_blob_image_without_directory_sync(&blob_path, &blob_image)?
            }
        } else {
            0
        };
        self.publication.blob_bytes_written = self
            .publication
            .blob_bytes_written
            .saturating_add(blob_bytes);
        self.publication_timing.blob_write_ns = self
            .publication_timing
            .blob_write_ns
            .saturating_add(elapsed_nanos(blob_started));

        let wal_path = self.path.join(WAL_FILE);
        let wal_offset = if append_commit {
            let offset = fs::metadata(&wal_path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            self.wal.append(&WalRecord::commit(commit));
            self.write_wal_to_disk(true)?;
            offset
        } else {
            recovered_wal_offset
        };

        let manifest = Manifest {
            database_id: self.database_id,
            history_id: self.history_id,
            generation_id: commit.generation_id,
            commit_id: commit.commit_id,
            page_size: PAGE_SIZE as u32,
            root_page_id: commit.root_page_id,
            pmt_checkpoint_id: PmtCheckpointId::new(commit.generation_id.get()),
            wal_segment: 0,
            wal_offset,
            mutation_count: commit.mutation_count,
            digest: commit.digest,
            format_version: FORMAT_VERSION,
        };
        let history_started = Instant::now();
        let mut manifest_history = self.manifest_history.clone();
        manifest_history
            .push(manifest)
            .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        let history_bytes = if self.path.join(MANIFEST_HISTORY_FILE).is_file() {
            self.append_manifest_history_without_directory_sync(manifest)?
        } else {
            let bytes = manifest_history
                .to_bytes()
                .ok_or_else(|| Error::Wal("manifest history is too large".into()))?;
            self.persist_manifest_history_without_directory_sync(&manifest_history)?;
            bytes.len() as u64
        };
        self.publication.history_bytes_written = self
            .publication
            .history_bytes_written
            .saturating_add(history_bytes);
        self.publication_timing.history_write_ns = self
            .publication_timing
            .history_write_ns
            .saturating_add(elapsed_nanos(history_started));
        // The candidate checkpoint, blob image, and manifest history have all
        // been file-synced. One final directory barrier makes their renamed or
        // created entries durable before the manifest can select the new
        // generation. The safety mirror and reuse ledger were already synced
        // before page reuse.
        let directory_started = Instant::now();
        let directory_result = sync_publication_directory(&self.path);
        self.publication_timing.directory_sync_ns = self
            .publication_timing
            .directory_sync_ns
            .saturating_add(elapsed_nanos(directory_started));
        directory_result?;
        self.manifest_history = manifest_history;
        let manifest_started = Instant::now();
        let manifest_result = self.manifest.publish(manifest);
        self.publication_timing.manifest_write_ns = self
            .publication_timing
            .manifest_write_ns
            .saturating_add(elapsed_nanos(manifest_started));
        manifest_result?;
        self.publication.manifest_bytes_written = self
            .publication
            .manifest_bytes_written
            .saturating_add(MANIFEST_SLOT_SIZE as u64);

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_AFTER_MANIFEST.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected post-manifest failure").into());
        }

        let cleanup_started = Instant::now();
        if self.blobs.is_segmented() {
            self.prune_unreferenced_blob_segments()?;
            self.finish_segment_catalog_backup()?;
        }

        let removed_reuse_attempt = self.reuse_ledger.remove_generation(commit.generation_id);
        let pruned_reuse_attempts = self.reuse_ledger.prune_published(&self.manifest_history);
        // Once the manifest is durable, a successful reuse attempt is no
        // longer authoritative. Keep its on-disk ledger entry until the next
        // publication or reopen when this generation actually reused slots;
        // both paths reconcile it against manifest history. This avoids one
        // non-authoritative delete plus directory sync per reused generation.
        // Keep eager cleanup for empty reservations so a normal append-only
        // first publication does not leave a misleading ledger artifact.
        if (removed_reuse_attempt || pruned_reuse_attempts > 0) && !reused_slots {
            self.persist_reuse_ledger()?;
        }

        self.engine.complete_generation();

        if wal_path.exists() {
            fs::remove_file(&wal_path)?;

            #[cfg(any(test, feature = "fault-injection"))]
            if FAIL_NEXT_WAL_TRUNCATE.with(|failure| failure.replace(false)) {
                return Err(std::io::Error::other("injected WAL truncate failure").into());
            }
            // WAL removal is cleanup after the manifest has selected the
            // generation. If the directory entry removal is not durable, a
            // reopen sees the already-published commit and discards the stale
            // WAL; forcing that non-authoritative deletion would add one
            // directory sync to every successful publication.
        }
        self.wal_reserved_extent = 0;
        self.publication_timing.cleanup_ns = self
            .publication_timing
            .cleanup_ns
            .saturating_add(elapsed_nanos(cleanup_started));

        self.generation_id = commit.generation_id;
        self.commit_id = commit.commit_id;
        self.next_generation_id = GenerationId::new(
            self.next_generation_id.get().max(
                commit
                    .generation_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
            ),
        );
        self.next_commit_id = CommitId::new(
            self.next_commit_id.get().max(
                commit
                    .commit_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("commit ID overflow".into()))?,
            ),
        );
        self.pending_mutations = 0;
        self.pending_wal_bytes = 0;
        self.pending_digest = 0;
        self.pending_blob_changes = false;
        Ok(())
    }

    /// Make both manifest slots name the latest durable generation before a
    /// new generation may reuse pages from older slots.
    fn mirror_current_manifest(&mut self) -> Result<()> {
        if let Some(current) = self.manifest.load_latest()? {
            self.manifest.publish_mirrored(current)?;
        }
        Ok(())
    }

    /// Checkpoint a committed WAL prefix discovered during reopen.
    fn publish_recovered(&mut self, commit: CommitRecord, wal_offset: u64) -> Result<()> {
        if let Err(error) = self.publish_generation(commit, false, wal_offset) {
            self.write_fenced = true;
            return Err(error);
        }
        Ok(())
    }

    /// Flush all pending writes as one durable root generation.
    pub fn flush(&mut self) -> Result<()> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        if self.pending_mutations == 0 {
            return Ok(());
        }

        let commit = CommitRecord {
            commit_id: self.next_commit_id,
            generation_id: self.next_generation_id,
            root_page_id: self.engine.btree().root_id() as u64,
            mutation_count: self.pending_mutations,
            digest: self.pending_digest,
        };

        if let Err(error) = self.publish_generation(commit, true, 0) {
            // StorageEngine performs capacity admission before any page
            // write. A refusal at that boundary leaves only an uncommitted
            // WAL mutation prefix, so callers can restore capacity and retry
            // without reopening. Any other publication error may have
            // reached durable media and must fence this handle.
            if !matches!(&error, Error::CapacityPreflight) {
                self.write_fenced = true;
            }
            return Err(error);
        }
        Ok(())
    }

    /// Establish and verify a named durable checkpoint barrier.
    ///
    /// A pending WAL generation is published first. When the database is
    /// already clean, this is idempotent: it verifies the existing manifest
    /// and does not invent an extra commit or generation.
    pub fn checkpoint(&mut self) -> Result<VerificationReport> {
        self.check_writable()?;
        self.flush()?;
        self.verify()
    }

    /// Close the database (flush and sync).
    pub fn close(&mut self) -> Result<()> {
        if self.is_open {
            if !self.read_only {
                self.flush()?;
            }
            self.is_open = false;
            if let Some(lock_file) = self.lock_file.take() {
                let _ = lock_file.unlock();
            }
        }
        Ok(())
    }

    /// Run garbage collection on blob files.
    ///
    /// Pending mutations are published before reclaiming blobs so an older
    /// durable generation never loses a pointer. Fully dead files are removed
    /// directly; mixed files are compacted by rewriting active B-tree pointers
    /// into a new blob file and sweeping the old file after publication.
    ///
    /// Returns the number of entries reclaimed.
    pub fn gc(&mut self) -> Result<usize> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        self.flush()?;
        if !self
            .retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?
            .is_empty()
        {
            // The current retained read view is copy-backed, but retaining the
            // root still establishes the conservative physical contract. Do
            // not delete blob history until the lease is released.
            return Ok(0);
        }
        if self.blobs.has_reclaimable_files() {
            // Fully-dead file removal changes the active blob image without
            // publishing a new root. Fence the inactive manifest slot first;
            // otherwise it could still name records removed below and become
            // an invalid fallback after a torn newest-slot read.
            self.mirror_current_manifest()?;
            // Admission must precede removal from the in-memory catalog. The
            // current image is an upper bound for the compacted image, so a
            // successful reservation covers the subsequent atomic publish.
            self.admit_blob_image(None, None)?;
        }
        let mut reclaimed = self.blobs.gc();
        if reclaimed > 0 {
            let write_result = if self.blobs.is_segmented() {
                self.publish_blob_rewrite_generation().map(|()| 0)
            } else {
                let blob_path = self.path.join(BLOB_FILE);
                let blob_image = self.blobs.to_bytes();
                self.write_blob_image(&blob_path, &blob_image)
            };
            match write_result {
                Ok(bytes) => {
                    self.publication.blob_bytes_written =
                        self.publication.blob_bytes_written.saturating_add(bytes);
                }
                Err(error) => {
                    self.write_fenced = true;
                    return Err(error);
                }
            }
        }
        if !self.blobs.files_needing_gc().is_empty()
            || segmented_catalog_needs_consolidation(&self.blobs)
        {
            let rewritten = match self.rewrite_mixed_blob_files() {
                Ok(reclaimed) => reclaimed,
                Err(error) if matches!(&error, Error::CapacityPreflight) => return Err(error),
                Err(error) => {
                    self.write_fenced = true;
                    return Err(error);
                }
            };
            reclaimed = reclaimed.saturating_add(rewritten);
        }
        Ok(reclaimed)
    }

    /// Rewrite live blob values into a fresh file and publish their new
    /// pointers as one physical maintenance generation.
    ///
    /// Existing records remain in the candidate blob image but carry deletion
    /// metadata until the new manifest is durable. The prior blob image is
    /// kept under a recovery name across that boundary, so an interrupted
    /// rewrite restores the exact old root image. Once the new root is
    /// authoritative, a second sweep removes the fully dead old files without
    /// changing the logical tree again.
    fn rewrite_mixed_blob_files(&mut self) -> Result<usize> {
        self.engine.ensure_materialized()?;
        let end = vec![u8::MAX; MAX_KEY_SIZE + 1];
        let scan = self
            .engine
            .btree()
            .range_scan(&[], &end)
            .map_err(Error::from)?;

        let mut candidate_blobs = self.blobs.clone();
        candidate_blobs
            .begin_compaction_file()
            .ok_or(Error::DiskFull)?;
        candidate_blobs.mark_all_deleted();
        let mut candidate_tree = self.engine.btree().clone();
        let mut rewritten = 0usize;
        for entry in scan {
            let (key, result) = entry.map_err(Error::from)?;
            let LookupResult::Blob(pointer) = result else {
                continue;
            };
            let value = self.blobs.read(&pointer).ok_or_else(|| {
                Error::Corruption(format!(
                    "active B-tree blob pointer {}:{}:{} is unavailable",
                    pointer.file_id, pointer.offset, pointer.length
                ))
            })?;
            let replacement = candidate_blobs.append(&key, value.to_vec());
            candidate_tree
                .upsert_blob(&key, replacement)
                .map_err(Error::from)?;
            rewritten = rewritten.saturating_add(1);
        }
        if rewritten == 0 {
            return Ok(0);
        }

        let blob_bytes = Self::blob_publication_size(&candidate_blobs)?;
        let candidate_page_count = candidate_tree
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| candidate_tree.node(*page_id).is_some())
            .count();
        let candidate_data_bytes = u64::try_from(candidate_page_count)
            .map_err(|_| Error::DiskFull)?
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(Error::DiskFull)?;
        let metadata_bytes = Self::max_metadata_delta_bytes(
            candidate_tree.node_count(),
            candidate_tree.page_allocator(),
        )?;
        self.preflight_maintenance_capacity(candidate_data_bytes, metadata_bytes, blob_bytes)?;
        self.engine.check_artifact_capacity(blob_bytes)?;
        self.engine.preflight_rebuild_capacity(&candidate_tree)?;
        if !candidate_blobs.is_segmented() {
            self.reserve_blob_image(blob_bytes)?;
        }
        *self.engine.btree_mut() = candidate_tree;
        self.blobs = candidate_blobs;

        self.mirror_current_manifest()?;
        self.engine.flush()?;
        self.publish_blob_rewrite_generation()?;

        let reclaimed = if self.blobs.has_reclaimable_files() {
            self.admit_blob_image(None, None)?;
            let reclaimed = self.blobs.gc();
            if reclaimed > 0 {
                let blob_bytes = if self.blobs.is_segmented() {
                    self.publish_blob_rewrite_generation()?;
                    0
                } else {
                    let blob_path = self.path.join(BLOB_FILE);
                    let blob_image = self.blobs.to_bytes();
                    self.write_blob_image(&blob_path, &blob_image)?
                };
                self.publication.blob_bytes_written = self
                    .publication
                    .blob_bytes_written
                    .saturating_add(blob_bytes);
            }
            reclaimed
        } else {
            0
        };
        Ok(reclaimed)
    }

    /// Get blob GC statistics.
    pub fn blob_stats(&self) -> BlobStats {
        BlobStats {
            files_needing_gc: self.blobs.files_needing_gc().len(),
            total_valid: self.blobs.total_valid_entries(),
            total_deleted: self.blobs.total_deleted_entries(),
            catalog_needs_consolidation: segmented_catalog_needs_consolidation(&self.blobs),
        }
    }

    /// Return durable identity and publication state for diagnostics/recovery.
    pub fn durability_status(&self) -> DurabilityStatus {
        DurabilityStatus {
            database_id: self.database_id,
            history_id: self.history_id,
            generation_id: self.generation_id,
            commit_id: self.commit_id,
            pending_mutations: self.pending_mutations,
            write_fenced: self.write_fenced,
        }
    }

    /// Return storage counters and current artifact sizes for observability.
    pub fn metrics(&self) -> Result<DBMetrics> {
        self.check_open()?;
        let artifact_size = |name: &str| -> Result<u64> {
            let path = self.path.join(name);
            match fs::metadata(path) {
                Ok(metadata) => Ok(metadata.len()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
                Err(error) => Err(error.into()),
            }
        };

        Ok(DBMetrics {
            storage: self.engine.metrics(),
            wal_admission_failures: self.wal_admission_failures,
            buffer: self.engine.buffer_stats(),
            data_bytes: artifact_size(DATA_FILE)?,
            blob_bytes: blob_storage_size(&self.path)?,
            wal_bytes: artifact_size(WAL_FILE)?,
            wal_reserved_bytes: self.wal_reserved_extent,
            reclaimable_pages: self.engine.reclaimable_page_count() as u64,
            publication: self.publication,
            publication_timing: self.publication_timing,
        })
    }

    /// Verify the active manifest, checkpoint, pages, blob file, and WAL.
    ///
    /// This pass does not mutate logical state and is intended for DBNext
    /// check/repair tooling and pre-snapshot validation.
    pub fn verify(&mut self) -> Result<VerificationReport> {
        self.check_readable()?;
        self.verify_inner()
            .map_err(|failure| Error::Corruption(failure.message))
    }

    fn verify_inner(&mut self) -> std::result::Result<VerificationReport, VerificationFailure> {
        let manifest = self
            .manifest
            .load_latest()
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Manifest, error))?
            .ok_or_else(|| VerificationFailure {
                kind: CheckFailureKind::Manifest,
                message: "database has no valid manifest".into(),
            })?;
        if manifest.database_id != self.database_id
            || manifest.history_id != self.history_id
            || manifest.generation_id != self.generation_id
            || manifest.commit_id != self.commit_id
        {
            return Err(VerificationFailure {
                kind: CheckFailureKind::Manifest,
                message: "manifest identity does not match the open database".into(),
            });
        }

        let (verified_pages, data_bytes) = self
            .engine
            .verify_pages(manifest.root_page_id)
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::DataPage, error))?;
        let blob_pointers = self
            .engine
            .verify_tree(manifest.root_page_id)
            .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Structure, error))?;
        for pointer in blob_pointers {
            if self.blobs.read(&pointer).is_none() {
                return Err(VerificationFailure {
                    kind: CheckFailureKind::Blob,
                    message: format!(
                        "blob pointer target is missing: file {}, offset {}, length {}",
                        pointer.file_id, pointer.offset, pointer.length
                    ),
                });
            }
        }

        if manifest.pmt_checkpoint_id.get() != 0 {
            let checkpoint_path = self
                .path
                .join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
            let (checkpoint_pmt, checkpoint_allocator) = Self::load_meta(&checkpoint_path)
                .map_err(|error| {
                    VerificationFailure::from_error(CheckFailureKind::Checkpoint, error)
                })?;
            if checkpoint_pmt.to_bytes() != self.engine.pmt().to_bytes()
                || checkpoint_allocator.to_bytes() != self.engine.allocator().to_bytes()
            {
                return Err(VerificationFailure {
                    kind: CheckFailureKind::Checkpoint,
                    message: "manifest checkpoint does not match active PMT or allocator".into(),
                });
            }
        }

        let blob_path = self.path.join(BLOB_FILE);
        let blob_bytes = if blob_path.exists() {
            let bytes = fs::read(&blob_path).map_err(|error| {
                VerificationFailure::from_error(CheckFailureKind::Blob, error.into())
            })?;
            parse_blob_catalog(&self.path, &bytes, Some(self.generation_id.get()))
                .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Blob, error))?
                .ok_or_else(|| VerificationFailure {
                    kind: CheckFailureKind::Blob,
                    message: "blob catalog failed integrity verification".into(),
                })?;
            blob_storage_size(&self.path)
                .map_err(|error| VerificationFailure::from_error(CheckFailureKind::Blob, error))?
        } else {
            0
        };

        let wal_path = self.path.join(WAL_FILE);
        let wal_bytes = if wal_path.exists() {
            let bytes = fs::read(&wal_path).map_err(|error| {
                VerificationFailure::from_error(CheckFailureKind::Wal, error.into())
            })?;
            let (_, status) = WalManager::parse_records_with_status(&bytes);
            if status == ParseStatus::Corrupt
                || (!self.check_only && status != ParseStatus::Complete)
            {
                return Err(VerificationFailure {
                    kind: CheckFailureKind::Wal,
                    message: format!("WAL integrity status is {status:?}"),
                });
            }
            bytes.len() as u64
        } else {
            0
        };

        Ok(VerificationReport {
            durability: self.durability_status(),
            verified_pages,
            data_bytes,
            blob_bytes,
            wal_bytes,
            reclaimable_pages: self.engine.reclaimable_page_count() as u64,
        })
    }

    fn wal_check_status(&mut self) -> Result<WalCheckStatus> {
        let wal_path = self.path.join(WAL_FILE);
        if !wal_path.exists() {
            return Ok(WalCheckStatus::Clean);
        }

        let bytes = fs::read(wal_path)?;
        if bytes.is_empty() {
            return Ok(WalCheckStatus::Clean);
        }

        let (records, status) = WalManager::parse_records_with_status(&bytes);
        if status == ParseStatus::Corrupt {
            return Err(Error::Corruption(
                "offline check found a corrupt WAL record".into(),
            ));
        }

        let current_manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        let mut pending = Vec::new();
        let mut saw_commit = false;
        for record in &records {
            match record.record_type {
                RecordType::Put => {
                    decode_put_payload(false, &record.payload)?;
                    pending.push(record);
                }
                RecordType::PutV2 => {
                    decode_put_payload(true, &record.payload)?;
                    pending.push(record);
                }
                RecordType::Delete => {
                    decode_delete_payload(false, &record.payload)?;
                    pending.push(record);
                }
                RecordType::DeleteV2 => {
                    decode_delete_payload(true, &record.payload)?;
                    pending.push(record);
                }
                RecordType::Commit => {
                    let commit = record
                        .commit_record()
                        .ok_or_else(|| Error::Corruption("invalid WAL commit envelope".into()))?;
                    if commit.mutation_count != pending.len() as u64
                        || commit.digest != digest_records(&pending)
                    {
                        return Err(Error::Corruption(
                            "WAL commit does not match its mutation prefix".into(),
                        ));
                    }

                    match commit
                        .generation_id
                        .get()
                        .cmp(&current_manifest.generation_id.get())
                    {
                        std::cmp::Ordering::Less
                            if commit.commit_id > current_manifest.commit_id =>
                        {
                            return Err(Error::Corruption(
                                "WAL commit frontier is inconsistent with manifest".into(),
                            ));
                        }
                        std::cmp::Ordering::Equal => {
                            if commit.commit_id != current_manifest.commit_id
                                || commit.root_page_id != current_manifest.root_page_id
                                || commit.mutation_count != current_manifest.mutation_count
                                || commit.digest != current_manifest.digest
                            {
                                return Err(Error::Corruption(
                                    "WAL commit disagrees with authoritative manifest".into(),
                                ));
                            }
                        }
                        std::cmp::Ordering::Greater
                            if commit.commit_id <= current_manifest.commit_id =>
                        {
                            return Err(Error::Corruption(
                                "WAL commit frontier is inconsistent with manifest".into(),
                            ));
                        }
                        _ => {}
                    }
                    saw_commit = true;
                    pending.clear();
                }
                _ => {}
            }
        }

        match status {
            ParseStatus::Incomplete => Ok(WalCheckStatus::Incomplete),
            ParseStatus::Complete if records.is_empty() => Ok(WalCheckStatus::Clean),
            ParseStatus::Complete if saw_commit => Ok(WalCheckStatus::NeedsRecovery),
            ParseStatus::Complete => Ok(WalCheckStatus::Pending),
            ParseStatus::Corrupt => unreachable!("corrupt WAL status returned above"),
        }
    }

    /// Flush and create an atomically published, independently verified
    /// snapshot in a new directory without mutating the source directory.
    pub fn snapshot<P: AsRef<Path>>(&mut self, destination: P) -> Result<SnapshotReport> {
        self.check_writable()?;
        self.flush()?;
        let source_report = self.verify()?;
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "snapshot destination already exists: {}",
                destination.display()
            )));
        }

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = destination.with_extension("seerdb.snapshot.tmp");
        if temporary.exists() {
            return Err(Error::InvalidArgument(format!(
                "snapshot temporary path already exists: {}",
                temporary.display()
            )));
        }

        let result = (|| {
            fs::create_dir_all(&temporary)?;
            let copied_files = copy_snapshot_artifacts(&self.path, &temporary)?;
            let marker_path = temporary.join(ARCHIVE_MARKER_FILE);
            fs::write(&marker_path, b"SEERDB-ARCHIVE-V1\n")?;
            File::open(&marker_path)?.sync_all()?;
            sync_directory(&temporary)?;

            let mut restored = DB::open(&temporary, self.options.clone())?;
            let restored_report = restored.verify()?;
            if restored_report.durability != source_report.durability
                || restored_report.verified_pages != source_report.verified_pages
            {
                return Err(Error::Corruption(
                    "snapshot verification does not match source durability state".into(),
                ));
            }
            let destination_status = restored_report.durability;
            drop(restored);

            fs::rename(&temporary, &destination)?;
            if let Some(parent) = destination.parent() {
                sync_directory(parent)?;
            }

            Ok(SnapshotReport {
                source: source_report.durability,
                destination: destination_status,
                copied_files,
                verified_pages: restored_report.verified_pages,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
        }
        result
    }

    /// Restore an immutable archive into a new writable history.
    ///
    /// The archive is verified before copying. The destination receives a new
    /// `HistoryId` while preserving the source's database identity and
    /// durable root, so it can advance independently without sharing future
    /// history IDs with the archive.
    pub fn restore<P: AsRef<Path>, Q: AsRef<Path>>(
        archive: P,
        destination: Q,
        options: Options,
    ) -> Result<RestoreReport> {
        let archive = archive.as_ref().to_path_buf();
        if !archive.join(ARCHIVE_MARKER_FILE).is_file() {
            return Err(Error::InvalidArgument(
                "restore source is not an immutable SeerDB archive".into(),
            ));
        }
        let mut source = DB::open(&archive, options.clone())?;
        let source_report = source.verify()?;
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "restore destination already exists: {}",
                destination.display()
            )));
        }

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = Self::next_derived_path(&destination, "restore")?;
        let result = (|| {
            fs::create_dir_all(&temporary)?;
            let copied_files = copy_snapshot_artifacts(&archive, &temporary)?;
            sync_directory(&temporary)?;

            let mut restored = DB::open(&temporary, options.clone())?;
            restored.fork_history()?;
            let restored_report = restored.verify()?;
            if restored_report.durability.database_id != source_report.durability.database_id
                || restored_report.durability.generation_id
                    != source_report.durability.generation_id
                || restored_report.durability.commit_id != source_report.durability.commit_id
                || restored_report.verified_pages != source_report.verified_pages
            {
                return Err(Error::Corruption(
                    "restored history does not match archive root".into(),
                ));
            }
            let destination_status = restored_report.durability;
            drop(restored);

            fs::rename(&temporary, &destination)?;
            if let Some(parent) = destination.parent() {
                sync_directory(parent)?;
            }

            Ok(RestoreReport {
                source: source_report.durability,
                destination: destination_status,
                copied_files,
                verified_pages: restored_report.verified_pages,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
        }
        result
    }

    /// Rebuild a checked database into a new writable history without
    /// mutating the source directory.
    ///
    /// Unlike [`DB::restore`], this operation copies the source WAL as well as
    /// the durable generation. The destination opens normally, so committed
    /// WAL prefixes are reconciled (and replayed when they advance the
    /// manifest), while uncommitted or torn suffixes are reconciled there. The
    /// source is held under a shared advisory lock when
    /// its writer lock exists; an active writer therefore receives
    /// [`Error::DatabaseBusy`] instead of being copied concurrently.
    pub fn repair<P: AsRef<Path>, Q: AsRef<Path>>(
        source: P,
        destination: Q,
        options: Options,
    ) -> Result<RepairReport> {
        let source = source.as_ref().to_path_buf();
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "repair destination already exists: {}",
                destination.display()
            )));
        }

        let _source_lock = Self::acquire_source_shared_lock(&source)?;
        let source_check = DB::check(&source, options.clone())?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = Self::next_derived_path(&destination, "repair")?;
        let action = match source_check.wal_status {
            WalCheckStatus::Clean => RepairAction::NoRepair,
            WalCheckStatus::Pending => RepairAction::DiscardedUncommittedWal,
            WalCheckStatus::NeedsRecovery => RepairAction::ReconciledCommittedWal,
            WalCheckStatus::Incomplete => RepairAction::ReconciledIncompleteWal,
        };

        let result = (|| {
            fs::create_dir_all(&temporary)?;
            let copied_files = copy_repair_artifacts(&source, &temporary)?;
            sync_directory(&temporary)?;

            let mut repaired = DB::open(&temporary, options.clone())?;
            repaired.fork_history()?;
            let repaired_report = repaired.verify()?;
            if repaired_report.durability.database_id
                != source_check.verification.durability.database_id
            {
                return Err(Error::Corruption(
                    "repaired history changed the database identity".into(),
                ));
            }
            let destination_status = repaired_report.durability;
            drop(repaired);

            fs::rename(&temporary, &destination)?;
            if let Some(parent) = destination.parent() {
                sync_directory(parent)?;
            }

            Ok(RepairReport {
                source: source_check.verification.durability,
                source_wal_status: source_check.wal_status,
                destination: destination_status,
                copied_files,
                verified_pages: repaired_report.verified_pages,
                action,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
        }
        result
    }

    /// Create an owned read-only snapshot handle.
    ///
    /// The handle owns a verified temporary directory and removes it on
    /// `release()` or `Drop`. Use [`DB::snapshot`] when the archive should
    /// survive independently of this process.
    pub fn begin_snapshot(&mut self) -> Result<Snapshot> {
        self.check_writable()?;
        let destination = self.next_snapshot_path()?;
        self.snapshot(&destination)?;
        let db = match DB::open(&destination, self.options.clone()) {
            Ok(db) => db,
            Err(error) => {
                let _ = fs::remove_dir_all(&destination);
                return Err(error);
            }
        };
        Ok(Snapshot {
            db: Some(db),
            path: destination,
            released: false,
        })
    }

    /// Retain the current root generation with a durable physical lease.
    ///
    /// The retained root is registered durably before its pages can be
    /// reclaimed. Historical reads use the source device and the retained
    /// PMT; the independently verified copy remains available for callers
    /// that need an isolated archive-style handle.
    pub fn retain_current(&mut self) -> Result<RetainedSnapshot> {
        self.check_writable()?;
        let snapshot = self.begin_snapshot()?;
        let manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        // Each owned handle needs its own durable lease. Reusing a commit's
        // existing root here would let releasing one handle unprotect the
        // pages still needed by another handle.
        let snapshot_id = self.register_retained_manifest(manifest, false)?;

        Ok(RetainedSnapshot {
            snapshot: Some(snapshot),
            lease: Some(RetentionLease {
                state: Arc::clone(&self.retention),
                snapshot_id,
                reclamation_dirty: self.engine.reclamation_dirty_handle(),
                released: false,
            }),
        })
    }

    /// Retain an arbitrary published commit for shared historical reads.
    ///
    /// The returned ID is stable across reopen because the commit-to-root
    /// descriptor is recorded in the durable retention registry. Callers can
    /// use [`DB::get_at`] and [`DB::range_at`] with that ID without copying the
    /// database directory.
    pub fn retain_commit(&mut self, commit_id: CommitId) -> Result<SnapshotId> {
        self.check_writable()?;
        self.flush()?;
        if let Some(snapshot_id) = self.retained_snapshot_id(commit_id) {
            return Ok(snapshot_id);
        }
        let manifest = self
            .manifest_history
            .find_commit(commit_id)
            .ok_or_else(|| {
                Error::SnapshotUnavailable(format!("commit {} is not retained", commit_id.get()))
            })?;
        self.register_retained_manifest(manifest, true)
    }

    /// Pin the active root for a short-lived transaction without rewalking the
    /// entire B-tree on every begin.
    ///
    /// The durable blob image and physical page offsets are still copied and
    /// registered before this returns, so later reclamation cannot overwrite
    /// the pinned root. Full graph and blob-target validation remains the
    /// responsibility of [`DB::retain_commit`], [`DB::check`], and
    /// [`DB::verify`]; reads through this pin validate pages and blob records
    /// at their access boundaries and fail closed on corruption.
    pub fn retain_current_commit(&mut self) -> Result<SnapshotId> {
        self.check_writable()?;
        self.flush()?;
        let manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        if manifest.commit_id != self.commit_id || manifest.generation_id != self.generation_id {
            return Err(Error::Corruption(
                "active manifest does not match the database frontier".into(),
            ));
        }
        self.register_current_transaction_manifest(manifest)
    }

    /// Begin a cheap immutable view over the active published generation.
    ///
    /// Unlike historical retention, this path does not serialize a blob
    /// sidecar or copy the PMT. The view pins the immutable PMT and opens the
    /// current page/blob files before returning; the process-local lease keeps
    /// reclamation from reusing pages or deleting old blob segments while it
    /// is live.
    pub fn begin_read_view(&mut self) -> Result<ReadView> {
        self.check_readable()?;
        self.check_maintenance_idle()?;
        let manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        if manifest.commit_id != self.commit_id || manifest.generation_id != self.generation_id {
            return Err(Error::Corruption(
                "active manifest does not match the database frontier".into(),
            ));
        }

        let snapshot_id = self.register_read_view_manifest(manifest)?;
        let mut lease = RetentionLease {
            state: Arc::clone(&self.retention),
            snapshot_id,
            reclamation_dirty: self.engine.reclamation_dirty_handle(),
            released: false,
        };
        let storage = match self.engine.read_view(manifest.root_page_id) {
            Ok(storage) => storage,
            Err(error) => {
                let _ = lease.release();
                return Err(error);
            }
        };
        let blobs = match BlobReadView::open(&self.path, &self.blobs) {
            Ok(blobs) => blobs,
            Err(error) => {
                let _ = lease.release();
                return Err(error);
            }
        };
        Ok(ReadView {
            storage,
            blobs,
            lease: Some(lease),
            durability: self.durability_status(),
        })
    }

    /// Return the active retention ID for a commit, if one exists.
    pub fn retained_snapshot_id(&self, commit_id: CommitId) -> Option<SnapshotId> {
        self.retention
            .lock()
            .ok()?
            .roots()
            .iter()
            .find_map(|root| (root.manifest.commit_id == commit_id).then_some(root.snapshot_id))
    }

    /// Release a durable historical retention lease by ID.
    pub fn release_snapshot(&mut self, snapshot_id: SnapshotId) -> Result<()> {
        self.check_writable()?;
        self.retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?
            .remove(snapshot_id)?;
        self.engine
            .reclamation_dirty_handle()
            .store(true, Ordering::Release);
        Ok(())
    }

    fn register_retained_manifest(
        &mut self,
        manifest: Manifest,
        deduplicate_commit: bool,
    ) -> Result<SnapshotId> {
        self.register_manifest(manifest, deduplicate_commit, false, true)
    }

    fn register_current_transaction_manifest(&mut self, manifest: Manifest) -> Result<SnapshotId> {
        self.register_manifest(manifest, false, true, false)
    }

    fn register_read_view_manifest(&mut self, manifest: Manifest) -> Result<SnapshotId> {
        let snapshot_id = {
            let state = self
                .retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            state.next_ephemeral_snapshot_id()
        };
        {
            let mut state = self
                .retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            state.insert_ephemeral(manifest, HashSet::new())?;
            let protected = state
                .protected_offsets
                .lock()
                .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))?
                .clone();
            self.engine.set_protected_offsets(protected)?;
        }
        Ok(snapshot_id)
    }

    fn register_ephemeral_manifest(&mut self, manifest: Manifest) -> Result<SnapshotId> {
        self.register_manifest(manifest, false, true, true)
    }

    fn register_manifest(
        &mut self,
        manifest: Manifest,
        deduplicate_commit: bool,
        ephemeral: bool,
        validate_tree: bool,
    ) -> Result<SnapshotId> {
        if deduplicate_commit
            && let Some(snapshot_id) = self.retained_snapshot_id(manifest.commit_id)
        {
            return Ok(snapshot_id);
        }
        let snapshot_id = {
            let state = self
                .retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            if ephemeral {
                state.next_ephemeral_snapshot_id()
            } else {
                state.next_snapshot_id()
            }
        };
        let retained_blob = retained_blob_path(&self.path, snapshot_id);
        // Build the immutable retention sidecar from the verified in-memory
        // manager. Segmented stores use whole-image sidecars for now; this
        // keeps historical reads independent of active segment cleanup while
        // the active publication path avoids rewriting those segments.
        let mut retained_blobs = self.blobs.clone();
        // Deletion markers describe the active root. An older retained root
        // may still legitimately reference a value that a later commit
        // replaced, so its immutable sidecar must preserve the append-only
        // record bytes while omitting active-root deletion metadata.
        retained_blobs.clear_deletion_metadata();
        let blob_bytes = retained_blobs.to_bytes();
        if let Err(error) = atomic_write(&retained_blob, &blob_bytes) {
            let _ = fs::remove_file(&retained_blob);
            return Err(error);
        }
        if manifest.pmt_checkpoint_id.get() != 0 {
            let checkpoint = self
                .path
                .join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
            let (pmt, _) = match Self::load_meta(&checkpoint) {
                Ok(meta) => meta,
                Err(error) => {
                    let _ = fs::remove_file(&retained_blob);
                    return Err(error);
                }
            };
            if let Err(error) = self.validate_historical_page_liveness(manifest, &pmt) {
                let _ = fs::remove_file(&retained_blob);
                return Err(error);
            }
            if validate_tree {
                let pointers = match self.engine.verify_tree_at(manifest.root_page_id, &pmt) {
                    Ok(pointers) => pointers,
                    Err(error) => {
                        let _ = fs::remove_file(&retained_blob);
                        return Err(error);
                    }
                };
                if pointers
                    .iter()
                    .any(|pointer| retained_blobs.read(pointer).is_none())
                {
                    let _ = fs::remove_file(&retained_blob);
                    return Err(Error::SnapshotUnavailable(format!(
                        "commit {} has no complete historical blob image",
                        manifest.commit_id.get()
                    )));
                }
            }
        }
        let offsets = match Self::load_manifest_offsets(&self.path, manifest, snapshot_id) {
            Ok(offsets) => offsets,
            Err(error) => {
                let _ = fs::remove_file(&retained_blob);
                return Err(error);
            }
        };
        let snapshot_id = {
            let mut state = self
                .retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            let result = if ephemeral {
                state.insert_ephemeral(manifest, offsets)
            } else {
                state.insert(manifest, offsets)
            };
            if let Err(error) = result {
                let _ = fs::remove_file(&retained_blob);
                return Err(error);
            }
            snapshot_id
        };
        let protected = {
            let state = self
                .retention
                .lock()
                .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
            state
                .protected_offsets
                .lock()
                .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))?
                .clone()
        };
        if let Err(error) = self.engine.set_protected_offsets(protected) {
            self.write_fenced = true;
            return Err(error);
        }
        Ok(snapshot_id)
    }

    /// Refuse a historical retention request once a later published
    /// generation has reused one of the target root's physical page slots.
    ///
    /// Published manifest history and the durable pre-reuse ledger together
    /// establish whether a later generation may have overwritten the target
    /// bytes. Retention must fail closed rather than treating a different,
    /// structurally valid page as the requested historical value.
    fn validate_historical_page_liveness(&self, target: Manifest, target_pmt: &PMT) -> Result<()> {
        let target_by_offset: BTreeMap<_, _> = target_pmt
            .iter()
            .map(|(_, mapping)| (mapping.offset, *mapping))
            .collect();
        if target_by_offset.is_empty() {
            return Ok(());
        }

        for attempt in self
            .reuse_ledger
            .attempts()
            .iter()
            .filter(|attempt| attempt.generation_id > target.generation_id)
        {
            if attempt
                .offsets
                .iter()
                .any(|offset| target_by_offset.contains_key(offset))
            {
                return Err(Error::SnapshotUnavailable(format!(
                    "commit {} has physical pages reused by an uncertain generation {}",
                    target.commit_id.get(),
                    attempt.generation_id.get()
                )));
            }
        }

        for later in self
            .manifest_history
            .manifests()
            .iter()
            .filter(|manifest| manifest.generation_id > target.generation_id)
        {
            if later.pmt_checkpoint_id.get() == 0 {
                continue;
            }
            let checkpoint = self
                .path
                .join(format!("seerdb.meta.{}", later.pmt_checkpoint_id.get()));
            let (later_pmt, _) = Self::load_meta(&checkpoint).map_err(|error| {
                Error::SnapshotUnavailable(format!(
                    "commit {} cannot establish page liveness through generation {}: {error}",
                    target.commit_id.get(),
                    later.generation_id.get()
                ))
            })?;
            if later_pmt.iter().any(|(_, mapping)| {
                target_by_offset
                    .get(&mapping.offset)
                    .is_some_and(|target_mapping| *target_mapping != *mapping)
            }) {
                return Err(Error::SnapshotUnavailable(format!(
                    "commit {} has physical pages reused by a later generation",
                    target.commit_id.get()
                )));
            }
        }
        Ok(())
    }

    fn next_snapshot_path(&self) -> Result<PathBuf> {
        Self::next_derived_path(&self.path, "snapshot")
    }

    fn next_derived_path(path: &Path, kind: &str) -> Result<PathBuf> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("seerdb");
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let id = NEXT_SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
        let destination = parent.join(format!(
            ".{name}.{kind}-{}-{timestamp}-{id}",
            std::process::id()
        ));
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "snapshot destination already exists: {}",
                destination.display()
            )));
        }
        Ok(destination)
    }

    fn fork_history(&mut self) -> Result<()> {
        self.check_writable()?;
        let manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
        let history_id = HistoryId::new(
            manifest
                .history_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("history ID overflow".into()))?,
        );
        let forked = Manifest {
            history_id,
            ..manifest
        };
        let mut manifest_history = self.manifest_history.clone();
        manifest_history.reset(forked);
        self.persist_manifest_history(&manifest_history)?;
        self.manifest.publish_replicated(forked)?;
        self.manifest_history = manifest_history;
        self.history_id = history_id;
        Ok(())
    }

    /// Reclaim data pages that are no longer referenced by either manifest
    /// slot.
    ///
    /// A pending generation is flushed first. Unprotected active pages are
    /// then copied from high offsets into lower interior holes, published as
    /// a maintenance generation, and finally followed by crash-safe tail
    /// truncation. Retained-root pages are never overwritten or reclaimed.
    pub fn compact(&mut self) -> Result<CompactionReport> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        let result = self.compact_inner(None);
        if result.is_err() && !matches!(&result, Err(Error::CapacityPreflight)) {
            // A maintenance failure can occur after the manifest barrier or
            // after the file length changed. Reopen is the only universally
            // safe way to reconstruct the active generation and allocator.
            self.write_fenced = true;
        }
        result
    }

    /// Reclaim data pages while bounding one maintenance generation.
    ///
    /// At most `max_relocated_pages` active pages are copied into lower
    /// unprotected holes in this call. A zero limit still trims an already
    /// reclaimable tail but performs no interior relocation. Callers can
    /// schedule repeated calls to keep maintenance latency and staging memory
    /// bounded without weakening the manifest publication barrier.
    pub fn compact_with_limit(&mut self, max_relocated_pages: usize) -> Result<CompactionReport> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        let result = self.compact_inner(Some(max_relocated_pages));
        if result.is_err() && !matches!(&result, Err(Error::CapacityPreflight)) {
            self.write_fenced = true;
        }
        result
    }

    /// Rebuild the active tree from live entries and publish it as a new
    /// maintenance generation.
    ///
    /// This is the first complete logical reclamation primitive: tombstones
    /// and obsolete versions are omitted from the rebuilt tree, while the
    /// old PMT and blob image remain protected until the new manifest is
    /// authoritative. The unbounded convenience method drains the same
    /// resumable cursor used by [`DB::vacuum_step`].
    pub fn vacuum(&mut self) -> Result<VacuumReport> {
        self.check_writable()?;
        loop {
            let progress = self.vacuum_step(usize::MAX)?;
            if progress.complete {
                return Ok(VacuumReport {
                    durability: progress.durability,
                    live_entries: progress.live_entries,
                    logical_pages_before: progress.logical_pages_before,
                    logical_pages_after: progress.logical_pages_after.ok_or_else(|| {
                        Error::Corruption("completed vacuum has no page count".into())
                    })?,
                });
            }
        }
    }

    /// Advance logical reclamation in a bounded call.
    ///
    /// The candidate tree remains private to this handle and is published
    /// only on the final step. A crash or explicit cancellation therefore
    /// leaves the previous manifest and blob image authoritative. The writer
    /// lane remains reserved while a step is pending, so mutations and other
    /// maintenance calls are rejected until completion or cancellation.
    pub fn vacuum_step(&mut self, max_entries: usize) -> Result<VacuumProgress> {
        self.check_writable()?;
        if max_entries == 0 {
            return Err(Error::InvalidArgument(
                "vacuum step must process at least one entry".into(),
            ));
        }
        if self.vacuum.is_none() {
            self.start_vacuum()?;
        }

        let mut state = self.vacuum.take().ok_or_else(|| {
            Error::Corruption("vacuum state disappeared after initialization".into())
        })?;
        if state.source_generation != self.generation_id || state.source_commit != self.commit_id {
            return Err(Error::NeedsRecovery(
                "vacuum source generation changed before publication".into(),
            ));
        }

        let mut complete = false;
        for _ in 0..max_entries {
            let next = state.cursor.next(self.engine.btree());
            let Some(entry) = next else {
                complete = true;
                break;
            };
            let (key, result) = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    return Err(error.into());
                }
            };
            let value = match result {
                LookupResult::Found(value) => value,
                LookupResult::Blob(pointer) => self
                    .blobs
                    .read(&pointer)
                    .map(|value| value.to_vec())
                    .ok_or_else(|| {
                        Error::Corruption(format!(
                            "active B-tree blob pointer {}:{}:{} is unavailable",
                            pointer.file_id, pointer.offset, pointer.length
                        ))
                    })?,
                LookupResult::Deleted | LookupResult::NotFound => continue,
            };
            if state.candidate_blobs.should_separate(value.len()) {
                let pointer = state.candidate_blobs.append(&key, value);
                state.candidate_tree.upsert_blob(&key, pointer)?;
            } else {
                state.candidate_tree.upsert(&key, &value)?;
            }
            state.scanned_entries = state.scanned_entries.saturating_add(1);
            state.live_entries = state.live_entries.saturating_add(1);
        }

        if !complete {
            let progress = self.vacuum_progress(&state, false);
            self.vacuum = Some(state);
            return Ok(progress);
        }

        let candidate_blob_bytes = Self::blob_publication_size(&state.candidate_blobs)?;
        let candidate_page_count =
            u64::try_from(state.candidate_tree.node_count()).map_err(|_| Error::DiskFull)?;
        let candidate_data_bytes = candidate_page_count
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(Error::DiskFull)?;
        let candidate_metadata_bytes =
            Self::full_metadata_bytes_for_candidate(&state.candidate_tree)?;
        if let Err(error) = self.preflight_maintenance_capacity(
            candidate_data_bytes,
            candidate_metadata_bytes,
            candidate_blob_bytes,
        ) {
            self.vacuum = Some(state);
            return Err(error);
        }
        if let Err(error) = self.engine.check_artifact_capacity(candidate_blob_bytes) {
            self.vacuum = Some(state);
            return Err(error);
        }
        if let Err(error) = if state.candidate_blobs.is_segmented() {
            Ok(())
        } else {
            self.reserve_blob_image(candidate_blob_bytes)
        } {
            self.vacuum = Some(state);
            return Err(error);
        }
        if let Err(error) = self
            .engine
            .preflight_rebuild_capacity(&state.candidate_tree)
        {
            self.vacuum = Some(state);
            return Err(error);
        }

        let result = self.finish_vacuum(state);
        if result.is_err() {
            self.write_fenced = true;
        }
        let report = result?;
        Ok(VacuumProgress {
            durability: report.durability,
            scanned_entries: report.live_entries,
            live_entries: report.live_entries,
            logical_pages_before: report.logical_pages_before,
            logical_pages_after: Some(report.logical_pages_after),
            complete: true,
        })
    }

    /// Cancel an in-memory vacuum candidate without changing the durable root.
    pub fn cancel_vacuum(&mut self) -> Result<bool> {
        self.check_writable()?;
        Ok(self.vacuum.take().is_some())
    }

    fn start_vacuum(&mut self) -> Result<()> {
        self.flush()?;
        self.mirror_current_manifest()?;
        self.engine.ensure_materialized()?;
        let end = vec![u8::MAX; MAX_KEY_SIZE + 1];
        let cursor = self
            .engine
            .btree()
            .range_cursor(&[], &end)
            .map_err(Error::from)?;
        self.vacuum = Some(VacuumState {
            source_generation: self.generation_id,
            source_commit: self.commit_id,
            cursor,
            candidate_tree: BTree::new(),
            candidate_blobs: BlobManager::with_threshold_and_mode(
                self.blobs.threshold(),
                self.blobs.is_segmented(),
            ),
            scanned_entries: 0,
            live_entries: 0,
            logical_pages_before: self.engine.pmt().len() as u64,
        });
        Ok(())
    }

    fn vacuum_progress(&self, state: &VacuumState, complete: bool) -> VacuumProgress {
        VacuumProgress {
            durability: self.durability_status(),
            scanned_entries: state.scanned_entries,
            live_entries: state.live_entries,
            logical_pages_before: state.logical_pages_before,
            logical_pages_after: None,
            complete,
        }
    }

    fn finish_vacuum(&mut self, state: VacuumState) -> Result<VacuumReport> {
        let VacuumState {
            candidate_tree,
            candidate_blobs,
            live_entries,
            logical_pages_before,
            ..
        } = state;
        self.engine.prepare_logical_rebuild(candidate_tree)?;
        self.blobs = candidate_blobs;
        self.engine.flush()?;
        self.publish_blob_rewrite_generation()?;
        Ok(VacuumReport {
            durability: self.durability_status(),
            live_entries,
            logical_pages_before,
            logical_pages_after: self.engine.pmt().len() as u64,
        })
    }

    /// Remove historical manifests and PMT checkpoints that are not needed
    /// by the current root or a durable retained snapshot.
    ///
    /// The history sidecar is atomically replaced before any checkpoint is
    /// deleted. A crash during cleanup therefore leaves harmless extra files,
    /// never a history entry that names a missing checkpoint.
    pub fn prune_history(&mut self) -> Result<HistoryPruneReport> {
        self.check_writable()?;
        self.flush()?;
        let recovery_manifests = self.manifest.load_valid_manifests()?;
        let current = recovery_manifests
            .iter()
            .copied()
            .max_by_key(|manifest| (manifest.generation_id, manifest.commit_id))
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;

        // Both valid slots are recovery roots until a later publication has
        // mirrored the current manifest. Pruning only from `current` can
        // delete the checkpoint needed by the inactive fallback slot, turning
        // a later torn newest-slot recovery into a missing-artifact failure.
        let mut retained = recovery_manifests
            .iter()
            .map(|manifest| manifest.generation_id)
            .collect::<BTreeSet<_>>();
        retained.insert(current.generation_id);
        let state = self
            .retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
        let retained_roots = state
            .all_roots()
            .map(|root| root.manifest)
            .collect::<Vec<_>>();
        retained.extend(retained_roots.iter().map(|manifest| manifest.generation_id));
        drop(state);

        let mut history = self.manifest_history.clone();
        history
            .reconcile_current(current)
            .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        let mut retained_checkpoints = BTreeSet::new();
        let mut protected_manifests = recovery_manifests;
        protected_manifests.extend(retained_roots);
        protected_manifests.extend(
            history
                .manifests()
                .iter()
                .copied()
                .filter(|manifest| retained.contains(&manifest.generation_id)),
        );
        for manifest in protected_manifests {
            if manifest.pmt_checkpoint_id.get() != 0 {
                retained_checkpoints.extend(Self::load_meta_ancestors(
                    &self.path,
                    manifest.pmt_checkpoint_id.get(),
                )?);
            }
        }
        let removed_manifests = history.prune_to_generations(&retained) as u64;
        if history != self.manifest_history {
            self.persist_manifest_history(&history)?;
            self.manifest_history = history;
        }

        let mut removed_checkpoints = 0u64;
        let mut reclaimed_checkpoint_bytes = 0u64;
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(generation) = name
                .to_str()
                .and_then(|name| name.strip_prefix("seerdb.meta."))
                .and_then(|suffix| suffix.parse::<u64>().ok())
            else {
                continue;
            };
            if retained_checkpoints.contains(&generation) {
                continue;
            }
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                continue;
            }
            fs::remove_file(entry.path())?;
            removed_checkpoints = removed_checkpoints.saturating_add(1);
            reclaimed_checkpoint_bytes = reclaimed_checkpoint_bytes.saturating_add(metadata.len());
        }
        if removed_checkpoints > 0 {
            sync_history_prune_directory(&self.path)?;
        }

        Ok(HistoryPruneReport {
            retained_generations: retained.len() as u64,
            removed_manifests,
            removed_checkpoints,
            reclaimed_checkpoint_bytes,
        })
    }

    /// Publish a PMT relocation without inventing a logical user commit.
    ///
    /// The caller must have mirrored the current manifest before writing the
    /// relocated pages. A new generation ID makes the physical checkpoint
    /// authoritative while preserving commit identity and WAL digest.
    fn publish_compaction_generation(&mut self) -> Result<()> {
        let current = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
        let generation_id = self.next_generation_id;
        let checkpoint_path = self
            .path
            .join(format!("seerdb.meta.{}", generation_id.get()));
        let checkpoint_bytes = self.save_generation_meta(&checkpoint_path, current)?;
        let meta_path = self.path.join(META_FILE);
        let legacy_meta_bytes = if meta_path.is_file() {
            0
        } else {
            Self::save_meta(&meta_path, self.engine.pmt(), self.engine.allocator())?
        };
        self.publication.metadata_bytes_written = self
            .publication
            .metadata_bytes_written
            .saturating_add(checkpoint_bytes)
            .saturating_add(legacy_meta_bytes);

        if self.blobs.is_segmented() {
            // A compaction generation changes the manifest-selected physical
            // root even when the blob pointers do not change. Advance the
            // segmented catalog frontier with an empty delta (or a bounded
            // consolidation) so the catalog can be validated against the
            // same root generation after reopen.
            self.blobs.set_generation(generation_id.get());
            let blob_bytes = self.write_blob_segments()?;
            self.publication.blob_bytes_written = self
                .publication
                .blob_bytes_written
                .saturating_add(blob_bytes);
        }

        let manifest = Manifest {
            generation_id,
            pmt_checkpoint_id: PmtCheckpointId::new(generation_id.get()),
            root_page_id: self.engine.btree().root_id() as u64,
            ..current
        };
        let mut manifest_history = self.manifest_history.clone();
        manifest_history
            .push(manifest)
            .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        let history_bytes = if self.path.join(MANIFEST_HISTORY_FILE).is_file() {
            self.append_manifest_history(manifest)?
        } else {
            let bytes = manifest_history
                .to_bytes()
                .ok_or_else(|| Error::Wal("manifest history is too large".into()))?;
            self.persist_manifest_history(&manifest_history)?;
            bytes.len() as u64
        };
        self.publication.history_bytes_written = self
            .publication
            .history_bytes_written
            .saturating_add(history_bytes);
        self.manifest_history = manifest_history;
        self.manifest.publish(manifest)?;
        self.publication.manifest_bytes_written = self
            .publication
            .manifest_bytes_written
            .saturating_add(MANIFEST_SLOT_SIZE as u64);

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_AFTER_MANIFEST.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected post-manifest failure").into());
        }

        if self.blobs.is_segmented() {
            self.prune_unreferenced_blob_segments()?;
            self.finish_segment_catalog_backup()?;
        }
        self.engine.complete_generation();
        self.generation_id = generation_id;
        self.next_generation_id = GenerationId::new(
            generation_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        Ok(())
    }

    /// Publish a blob-pointer rewrite without inventing a logical user
    /// commit. The data pages, PMT, and blob image are all selected by the new
    /// physical generation before its manifest becomes authoritative.
    fn publish_blob_rewrite_generation(&mut self) -> Result<()> {
        let current = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
        let generation_id = self.next_generation_id;
        let checkpoint_path = self
            .path
            .join(format!("seerdb.meta.{}", generation_id.get()));
        let checkpoint_bytes = self.save_generation_meta(&checkpoint_path, current)?;
        let meta_path = self.path.join(META_FILE);
        let legacy_meta_bytes = if meta_path.is_file() {
            0
        } else {
            Self::save_meta(&meta_path, self.engine.pmt(), self.engine.allocator())?
        };
        self.publication.metadata_bytes_written = self
            .publication
            .metadata_bytes_written
            .saturating_add(checkpoint_bytes)
            .saturating_add(legacy_meta_bytes);

        self.blobs.set_generation(generation_id.get());
        let blob_path = self.path.join(BLOB_FILE);
        let backup_path = self.path.join(BLOB_REWRITE_BACKUP_FILE);
        let had_blob_image = blob_path.is_file();
        if backup_path.exists() {
            fs::remove_file(&backup_path)?;
        }
        if had_blob_image {
            fs::rename(&blob_path, &backup_path)?;
            sync_directory(&self.path)?;
        }
        let blob_bytes = if self.blobs.is_segmented() {
            self.write_blob_segments()?
        } else {
            let blob_image = self.blobs.to_bytes();
            self.write_blob_image(&blob_path, &blob_image)?
        };
        self.publication.blob_bytes_written = self
            .publication
            .blob_bytes_written
            .saturating_add(blob_bytes);

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_AFTER_BLOB_REWRITE_IMAGE.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected failure after blob rewrite image").into());
        }

        let manifest = Manifest {
            generation_id,
            pmt_checkpoint_id: PmtCheckpointId::new(generation_id.get()),
            root_page_id: self.engine.btree().root_id() as u64,
            ..current
        };
        let mut manifest_history = self.manifest_history.clone();
        manifest_history
            .push(manifest)
            .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        let history_bytes = if self.path.join(MANIFEST_HISTORY_FILE).is_file() {
            self.append_manifest_history(manifest)?
        } else {
            let bytes = manifest_history
                .to_bytes()
                .ok_or_else(|| Error::Wal("manifest history is too large".into()))?;
            self.persist_manifest_history(&manifest_history)?;
            bytes.len() as u64
        };
        self.publication.history_bytes_written = self
            .publication
            .history_bytes_written
            .saturating_add(history_bytes);
        self.manifest_history = manifest_history;
        self.manifest.publish(manifest)?;
        self.publication.manifest_bytes_written = self
            .publication
            .manifest_bytes_written
            .saturating_add(MANIFEST_SLOT_SIZE as u64);
        if self.blobs.is_segmented() {
            self.prune_unreferenced_blob_segments()?;
            self.finish_segment_catalog_backup()?;
        }
        self.engine.complete_generation();
        self.generation_id = generation_id;
        self.next_generation_id = GenerationId::new(
            generation_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        if !self.blobs.is_segmented() && had_blob_image {
            fs::remove_file(&backup_path)?;
            sync_directory(&self.path)?;
        }
        Ok(())
    }

    fn compact_inner(&mut self, max_relocated_pages: Option<usize>) -> Result<CompactionReport> {
        self.flush()?;
        self.engine.refresh_reclamation()?;

        let (before, _) = self.engine.reclaimable_tail_range()?;
        let has_reclaimable_pages = self.engine.reclaimable_page_count() > 0;
        let mut manifest_replicated = false;
        let mut relocated_pages = 0;
        if has_reclaimable_pages {
            let will_relocate =
                max_relocated_pages != Some(0) && self.engine.has_relocatable_interior_page()?;
            if will_relocate {
                let metadata_bytes = Self::max_metadata_delta_bytes(
                    self.engine.pmt().len(),
                    self.engine.allocator(),
                )?;
                let blob_bytes = if self.blobs.is_segmented() {
                    Self::blob_publication_size(&self.blobs)?
                } else {
                    0
                };
                self.preflight_maintenance_capacity(0, metadata_bytes, blob_bytes)?;
            }
            // Both slots must continue to name the old PMT until all moved
            // copies are durable. This is the maintenance equivalent of the
            // normal generation reuse barrier.
            self.mirror_current_manifest()?;
            manifest_replicated = true;
            relocated_pages = match max_relocated_pages {
                Some(limit) => self.engine.relocate_interior_pages_with_limit(limit)? as u64,
                None => self.engine.relocate_interior_pages()? as u64,
            };
            if relocated_pages > 0 {
                self.publish_compaction_generation()?;
            }
        }

        let (planned_before, planned_after) = self.engine.reclaimable_tail_range()?;
        let (actual_before, actual_after) = self.engine.truncate_reclaimable_tail()?;
        if actual_before != planned_before || actual_after != planned_after {
            return Err(Error::NeedsRecovery(
                "data file changed during compaction planning".into(),
            ));
        }

        Ok(CompactionReport {
            durability: self.durability_status(),
            data_bytes_before: before,
            data_bytes_after: actual_after,
            reclaimed_pages: (before - actual_after) / PAGE_SIZE as u64,
            relocated_pages,
            manifest_replicated,
        })
    }

    /// Inject one device sync failure for the feature-gated fault harness.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_sync_failure(&self) {
        self.engine.inject_sync_failure();
    }

    /// Inject one device page-write failure for the feature-gated fault harness.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_write_failure(&self) {
        self.engine.inject_write_failure();
    }

    /// Inject one failure after a complete page write and before publication.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_after_write_failure(&self) {
        self.engine.inject_after_write_failure();
    }

    /// Inject one failure after the complete page generation is written but
    /// before its device durability sync.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_page_range_sync_failure(&self) {
        self.engine.inject_page_range_sync_failure();
    }

    /// Inject one final-write ENOSPC after a page write may have completed.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_final_write_disk_full(&self) {
        self.engine.inject_final_write_disk_full();
    }

    /// Inject one disk-full result for the feature-gated fault harness.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_disk_full(&self) {
        self.engine.inject_disk_full();
    }

    /// Set a persistent device capacity limit for the feature-gated fault harness.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_capacity_limit(&self, capacity: u64) {
        self.engine.inject_capacity_limit(capacity);
    }

    /// Inject one atomic artifact rename failure for the feature-gated fault
    /// harness. The next atomic publication on this thread fails before the
    /// rename, leaving the previous artifact available for recovery.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_atomic_rename_failure(&self) {
        inject_atomic_rename_failure();
    }

    /// Inject one failure before the next WAL append.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_wal_write_failure(&self) {
        FAIL_NEXT_WAL_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next WAL append but before its sync.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_wal_after_write_failure(&self) {
        FAIL_NEXT_WAL_AFTER_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure at the next WAL sync boundary.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_wal_sync_failure(&self) {
        FAIL_NEXT_WAL_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next WAL sync boundary.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_wal_after_sync_failure(&self) {
        FAIL_NEXT_WAL_AFTER_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure at the next manifest sync boundary.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_manifest_sync_failure(&self) {
        self.manifest.inject_sync_failure();
    }

    /// Inject one failure at the safety mirror sync boundary before page reuse.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_manifest_mirror_sync_failure(&self) {
        self.manifest.inject_mirror_sync_failure();
    }

    /// Inject one failure at the coalesced artifact-directory barrier before
    /// the next user manifest publication.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_publication_directory_sync_failure(&self) {
        FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one final directory-sync failure after history pruning removes
    /// obsolete checkpoint files. The active manifest remains authoritative;
    /// reopen should accept the pruned or unpruned directory state.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_history_prune_directory_sync_failure(&self) {
        FAIL_NEXT_HISTORY_PRUNE_DIRECTORY_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next manifest becomes authoritative.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_after_manifest_failure(&self) {
        FAIL_NEXT_AFTER_MANIFEST.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next WAL file is removed.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_wal_truncate_failure(&self) {
        FAIL_NEXT_WAL_TRUNCATE.with(|failure| failure.set(true));
    }

    /// Inject one truncated atomic checkpoint image before manifest publish.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_atomic_short_write_failure(&self) {
        FAIL_NEXT_ATOMIC_SHORT_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one checksum-corrupted atomic checkpoint image before manifest
    /// publish.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_atomic_torn_write_failure(&self) {
        FAIL_NEXT_ATOMIC_TORN_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure after a mixed-blob rewrite image is durable but
    /// before its maintenance manifest is published.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_after_blob_rewrite_image_failure(&self) {
        FAIL_NEXT_AFTER_BLOB_REWRITE_IMAGE.with(|failure| failure.set(true));
    }

    /// Inject one failure after a segmented blob suffix is durable but before
    /// its catalog is published.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_after_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_AFTER_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure while syncing a segmented blob suffix.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_sync_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure while syncing a segmented blob catalog temp file.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_catalog_sync_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure after a segmented blob catalog temp file is synced
    /// but before it replaces the previous catalog.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_catalog_rename_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_RENAME.with(|failure| failure.set(true));
    }

    /// Inject one truncated segmented blob catalog image before manifest
    /// publication.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_catalog_short_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_SHORT_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one checksum-corrupted segmented blob catalog image before
    /// manifest publication.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_blob_segment_catalog_torn_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_TORN_WRITE.with(|failure| failure.set(true));
    }

    /// Begin a root-bound byte transaction.
    ///
    /// The transaction captures the current published commit and retains its
    /// physical root in a process-local lease before returning. It can
    /// therefore read a stable version while the database advances and can
    /// commit only against that expected base. The short-lived transaction pin
    /// is intentionally not persisted as a named historical snapshot.
    pub fn begin_batch_transaction(&mut self) -> Result<BatchTransaction> {
        self.check_writable()?;
        self.flush()?;
        let manifest = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest generation".into()))?;
        let snapshot_id = self.register_ephemeral_manifest(manifest)?;
        Ok(BatchTransaction {
            base_commit: self.commit_id,
            snapshot_id,
            lease: Some(RetentionLease {
                state: Arc::clone(&self.retention),
                snapshot_id,
                reclamation_dirty: self.engine.reclamation_dirty_handle(),
                released: false,
            }),
            mutations: Vec::new(),
            state: BatchTransactionState::Active,
        })
    }

    /// Begin the legacy transaction-ID bookkeeping primitive.
    ///
    /// This does not bind reads or writes to a durable SeerDB root. Use
    /// [`DB::begin_batch_transaction`] for the data-bearing transaction API.
    pub fn begin_transaction(&self) -> crate::concurrency::Transaction {
        self.txn_manager.begin()
    }

    /// Commit a transaction.
    pub fn commit_transaction(&self, txn: &mut crate::concurrency::Transaction) {
        self.txn_manager.commit(txn);
    }

    /// Abort a transaction.
    pub fn abort_transaction(&self, txn: &mut crate::concurrency::Transaction) {
        self.txn_manager.abort(txn);
    }

    /// Get the latest committed transaction ID.
    pub fn latest_committed_txn(&self) -> u64 {
        self.txn_manager.latest_committed()
    }

    /// Check if the database is open.
    fn check_open(&self) -> Result<()> {
        if !self.is_open {
            return Err(Error::InvalidArgument("database is closed".into()));
        }
        Ok(())
    }

    /// Reject ordinary reads after a failed publication until reopen restores
    /// the last authoritative root. The in-memory mutation overlay may be
    /// newer than the manifest after an ambiguous write, so exposing it would
    /// make one handle disagree with the state a crash recovery would choose.
    fn check_readable(&self) -> Result<()> {
        self.check_open()?;
        if self.write_fenced {
            return Err(Error::NeedsRecovery(
                "reads fenced after a failed durable publication; reopen required".into(),
            ));
        }
        Ok(())
    }

    /// Reject writes after a failed publication until the database is reopened.
    fn check_writable(&self) -> Result<()> {
        self.check_open()?;
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        if self.write_fenced {
            return Err(Error::NeedsRecovery(
                "writer fenced after a failed durable publication; reopen required".into(),
            ));
        }
        Ok(())
    }

    fn check_maintenance_idle(&self) -> Result<()> {
        if self.vacuum.is_some() {
            return Err(Error::MaintenanceInProgress("logical vacuum"));
        }
        Ok(())
    }

    fn acquire_writer_lock(path: &Path) -> Result<File> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(file),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Err(Error::DatabaseBusy)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn acquire_source_shared_lock(path: &Path) -> Result<Option<File>> {
        let lock_path = path.join(LOCK_FILE);
        if !lock_path.is_file() {
            return Ok(None);
        }

        let file = OpenOptions::new().read(true).open(lock_path)?;
        match fs2::FileExt::try_lock_shared(&file) {
            Ok(()) => Ok(Some(file)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Err(Error::DatabaseBusy)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Generate a stable-enough identity for a newly created database.
    fn new_database_id(path: &Path) -> DatabaseId {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path_digest = crc32c::crc32c(path.to_string_lossy().as_bytes());
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&(now as u64).to_le_bytes());
        bytes[8..12].copy_from_slice(&path_digest.to_le_bytes());
        bytes[12..16].copy_from_slice(&std::process::id().to_le_bytes());
        DatabaseId::new(bytes)
    }

    fn bootstrap_manifest(&self) -> Manifest {
        Manifest {
            database_id: self.database_id,
            history_id: self.history_id,
            generation_id: GenerationId::new(0),
            commit_id: CommitId::new(0),
            page_size: PAGE_SIZE as u32,
            root_page_id: self.engine.btree().root_id() as u64,
            pmt_checkpoint_id: PmtCheckpointId::new(0),
            wal_segment: 0,
            wal_offset: 0,
            mutation_count: 0,
            digest: 0,
            format_version: FORMAT_VERSION,
        }
    }

    /// Load PMT and allocator from meta file.
    fn load_meta(path: &Path) -> Result<(PMT, PageAllocator)> {
        Self::load_meta_with_depth(path).map(|(pmt, allocator, _)| (pmt, allocator))
    }

    /// Load a full checkpoint or a bounded metadata-delta chain.
    fn load_meta_with_depth(path: &Path) -> Result<(PMT, PageAllocator, usize)> {
        let mut current_path = path.to_path_buf();
        let mut deltas = Vec::new();
        let mut visited = HashSet::new();
        let (mut pmt, mut allocator) = loop {
            let data = fs::read(&current_path)?;
            if data.len() >= META_DELTA_MAGIC.len()
                && data[..META_DELTA_MAGIC.len()] == META_DELTA_MAGIC
            {
                let delta = Self::load_meta_delta(&data)?;
                if delta.parent_checkpoint_id != 0 && !visited.insert(delta.parent_checkpoint_id) {
                    return Err(Error::Corruption(
                        "metadata delta chain contains a cycle".into(),
                    ));
                }
                deltas.push(delta);
                if deltas.len() > MAX_META_DELTA_CHAIN {
                    return Err(Error::Corruption(format!(
                        "metadata delta chain exceeds maximum length {MAX_META_DELTA_CHAIN}"
                    )));
                }
                let parent = deltas
                    .last()
                    .map(|delta| delta.parent_checkpoint_id)
                    .ok_or_else(|| Error::Corruption("metadata delta disappeared".into()))?;
                if parent == 0 {
                    return Err(Error::Corruption(
                        "metadata delta has no full checkpoint parent".into(),
                    ));
                }
                current_path = current_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(format!("seerdb.meta.{parent}"));
                continue;
            }

            break if data.len() >= META_MAGIC.len() && data[..META_MAGIC.len()] == META_MAGIC {
                Self::load_versioned_meta(&data)?
            } else {
                Self::load_legacy_meta(&data)?
            };
        };

        for delta in deltas.iter().rev() {
            for page_id in &delta.removals {
                if pmt.remove(*page_id).is_none() {
                    return Err(Error::Corruption(format!(
                        "metadata delta removes unknown page {page_id}"
                    )));
                }
            }
            for (page_id, mapping) in &delta.updates {
                pmt.insert_persisted(*page_id, *mapping);
            }
            allocator = delta.allocator.clone();
        }

        Ok((pmt, allocator, deltas.len()))
    }

    fn load_meta_delta(data: &[u8]) -> Result<MetaDelta> {
        if data.len() < META_DELTA_HEADER_SIZE + META_DELTA_CHECKSUM_SIZE {
            return Err(Error::Corruption("metadata delta is truncated".into()));
        }
        let version = u32::from_le_bytes(
            data[8..12]
                .try_into()
                .map_err(|_| Error::Corruption("metadata delta version is truncated".into()))?,
        );
        if version != META_DELTA_VERSION {
            return Err(Error::Corruption(format!(
                "unsupported metadata delta version {version}"
            )));
        }

        let checksum_offset = data
            .len()
            .checked_sub(META_DELTA_CHECKSUM_SIZE)
            .ok_or_else(|| Error::Corruption("metadata delta checksum is truncated".into()))?;
        let expected = u32::from_le_bytes(
            data[checksum_offset..]
                .try_into()
                .map_err(|_| Error::Corruption("metadata delta checksum is truncated".into()))?,
        );
        if crc32c::crc32c(&data[..checksum_offset]) != expected {
            return Err(Error::Corruption("metadata delta checksum mismatch".into()));
        }

        let parent_checkpoint_id = u64::from_le_bytes(
            data[12..20]
                .try_into()
                .map_err(|_| Error::Corruption("metadata delta parent is truncated".into()))?,
        );
        let update_count =
            u32::from_le_bytes(data[20..24].try_into().map_err(|_| {
                Error::Corruption("metadata delta update count is truncated".into())
            })?) as usize;
        let removal_count =
            u32::from_le_bytes(data[24..28].try_into().map_err(|_| {
                Error::Corruption("metadata delta removal count is truncated".into())
            })?) as usize;
        let allocator_len = u32::from_le_bytes(data[28..32].try_into().map_err(|_| {
            Error::Corruption("metadata delta allocator length is truncated".into())
        })?) as usize;

        let update_bytes = update_count
            .checked_mul(8 + PageMapping::SERIALIZED_SIZE)
            .ok_or_else(|| Error::Corruption("metadata delta updates overflow".into()))?;
        let removal_bytes = removal_count
            .checked_mul(8)
            .ok_or_else(|| Error::Corruption("metadata delta removals overflow".into()))?;
        let allocator_start = META_DELTA_HEADER_SIZE
            .checked_add(update_bytes)
            .and_then(|offset| offset.checked_add(removal_bytes))
            .ok_or_else(|| Error::Corruption("metadata delta layout overflows".into()))?;
        let checksum_expected_end = allocator_start
            .checked_add(allocator_len)
            .and_then(|offset| offset.checked_add(META_DELTA_CHECKSUM_SIZE))
            .ok_or_else(|| Error::Corruption("metadata delta layout overflows".into()))?;
        if checksum_expected_end != data.len() {
            return Err(Error::Corruption(
                "metadata delta has trailing or truncated bytes".into(),
            ));
        }

        let mut updates = Vec::with_capacity(update_count);
        let mut offset = META_DELTA_HEADER_SIZE;
        let mut previous_page = None;
        for _ in 0..update_count {
            let page_id =
                u64::from_le_bytes(data[offset..offset + 8].try_into().map_err(|_| {
                    Error::Corruption("metadata delta page ID is truncated".into())
                })?);
            if previous_page.is_some_and(|previous| page_id <= previous) {
                return Err(Error::Corruption(
                    "metadata delta updates are not strictly sorted".into(),
                ));
            }
            previous_page = Some(page_id);
            offset += 8;
            let mapping_end = offset + PageMapping::SERIALIZED_SIZE;
            let mapping =
                PageMapping::from_bytes(data[offset..mapping_end].try_into().map_err(|_| {
                    Error::Corruption("metadata delta mapping is truncated".into())
                })?);
            if mapping.version == u64::MAX {
                return Err(Error::Corruption(
                    "metadata delta mapping version is exhausted".into(),
                ));
            }
            updates.push((page_id, mapping));
            offset = mapping_end;
        }

        let mut removals = Vec::with_capacity(removal_count);
        let mut previous_page = None;
        for _ in 0..removal_count {
            let page_id =
                u64::from_le_bytes(data[offset..offset + 8].try_into().map_err(|_| {
                    Error::Corruption("metadata delta removal is truncated".into())
                })?);
            if previous_page.is_some_and(|previous| page_id <= previous) {
                return Err(Error::Corruption(
                    "metadata delta removals are not strictly sorted".into(),
                ));
            }
            previous_page = Some(page_id);
            removals.push(page_id);
            offset += 8;
        }

        if updates
            .iter()
            .any(|(page_id, _)| removals.binary_search(page_id).is_ok())
        {
            return Err(Error::Corruption(
                "metadata delta updates and removals overlap".into(),
            ));
        }

        let allocator =
            PageAllocator::from_bytes(&data[allocator_start..allocator_start + allocator_len])
                .ok_or_else(|| Error::Corruption("metadata delta allocator is invalid".into()))?;
        Ok(MetaDelta {
            parent_checkpoint_id,
            updates,
            removals,
            allocator,
        })
    }

    fn cleanup_orphaned_retained_blobs(
        path: &Path,
        retention: &Arc<Mutex<RetentionState>>,
    ) -> Result<()> {
        let state = retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
        let retained_ids = state
            .roots()
            .iter()
            .map(|root| root.snapshot_id)
            .collect::<HashSet<_>>();
        drop(state);

        let prefix = format!("{BLOB_FILE}.retained.");
        let mut removed = false;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(id) = name
                .strip_prefix(&prefix)
                .and_then(|suffix| suffix.parse::<u64>().ok())
            else {
                continue;
            };
            if !retained_ids.contains(&SnapshotId::new(id)) {
                fs::remove_file(entry.path())?;
                removed = true;
            }
        }
        if removed {
            sync_directory(path)?;
        }
        Ok(())
    }

    fn load_retained_offset_map(
        path: &Path,
        state: &RetentionState,
        database_id: DatabaseId,
        history_id: HistoryId,
    ) -> Result<BTreeMap<SnapshotId, HashSet<u64>>> {
        let mut offsets_by_snapshot = BTreeMap::new();
        for root in state.roots() {
            if root.manifest.database_id != database_id {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} belongs to another database",
                    root.snapshot_id.get()
                )));
            }
            if root.manifest.history_id != history_id {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} belongs to another history",
                    root.snapshot_id.get()
                )));
            }
            if root.manifest.page_size as usize != PAGE_SIZE {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} has page size {}",
                    root.snapshot_id.get(),
                    root.manifest.page_size
                )));
            }
            let blob_path = retained_blob_path(path, root.snapshot_id);
            let blob_bytes = fs::read(&blob_path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    Error::Corruption(format!(
                        "retained snapshot {} is missing its blob image",
                        root.snapshot_id.get()
                    ))
                } else {
                    error.into()
                }
            })?;
            if BlobManager::from_bytes(&blob_bytes).is_none() {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} has an invalid blob image",
                    root.snapshot_id.get()
                )));
            }
            let protected = Self::load_manifest_offsets(path, root.manifest, root.snapshot_id)?;
            offsets_by_snapshot.insert(root.snapshot_id, protected);
        }
        Ok(offsets_by_snapshot)
    }

    fn load_manifest_offsets(
        path: &Path,
        manifest: Manifest,
        snapshot_id: SnapshotId,
    ) -> Result<HashSet<u64>> {
        if manifest.pmt_checkpoint_id.get() == 0 {
            if manifest.root_page_id != 0 {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} has a root without a checkpoint",
                    snapshot_id.get()
                )));
            }
            return Ok(HashSet::new());
        }

        let checkpoint = path.join(format!("seerdb.meta.{}", manifest.pmt_checkpoint_id.get()));
        let (pmt, _) = Self::load_meta(&checkpoint)?;
        if !pmt.contains(manifest.root_page_id) {
            return Err(Error::Corruption(format!(
                "retained snapshot {} names a root missing from its checkpoint",
                snapshot_id.get()
            )));
        }
        let mut protected = HashSet::new();
        let data_bytes = fs::metadata(path.join(DATA_FILE))?.len();
        for (_, mapping) in pmt.iter() {
            if mapping.file_id != 0 || !mapping.offset.is_multiple_of(PAGE_SIZE as u64) {
                return Err(Error::Corruption(format!(
                    "retained snapshot {} names an invalid page mapping",
                    snapshot_id.get()
                )));
            }
            let end = mapping
                .offset
                .checked_add(PAGE_SIZE as u64)
                .ok_or_else(|| {
                    Error::Corruption(format!(
                        "retained snapshot {} has an overflowing page mapping",
                        snapshot_id.get()
                    ))
                })?;
            if end > data_bytes {
                return Err(Error::SnapshotUnavailable(format!(
                    "retained snapshot {} names pages beyond the data file",
                    snapshot_id.get()
                )));
            }
            protected.insert(mapping.offset);
        }
        Ok(protected)
    }

    fn load_versioned_meta(data: &[u8]) -> Result<(PMT, PageAllocator)> {
        const HEADER_SIZE: usize = META_MAGIC.len() + 4;
        const CHECKSUM_SIZE: usize = 4;
        if data.len() < HEADER_SIZE + CHECKSUM_SIZE {
            return Err(Error::Corruption("meta file is truncated".into()));
        }

        let version = u32::from_le_bytes(
            data[META_MAGIC.len()..HEADER_SIZE]
                .try_into()
                .map_err(|_| Error::Corruption("meta version is truncated".into()))?,
        );
        if version != FORMAT_VERSION {
            return Err(Error::Corruption(format!(
                "unsupported meta format version {version}"
            )));
        }

        let checksum_offset = data.len() - CHECKSUM_SIZE;
        let expected = u32::from_le_bytes(
            data[checksum_offset..]
                .try_into()
                .map_err(|_| Error::Corruption("meta checksum is truncated".into()))?,
        );
        let actual = crc32c::crc32c(&data[..checksum_offset]);
        if expected != actual {
            return Err(Error::Corruption("meta checksum mismatch".into()));
        }

        Self::load_legacy_meta(&data[HEADER_SIZE..checksum_offset])
    }

    fn load_legacy_meta(data: &[u8]) -> Result<(PMT, PageAllocator)> {
        if data.len() < 4 {
            return Err(Error::Corruption("meta file too small".into()));
        }

        let pmt_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

        let pmt_end = 4usize
            .checked_add(pmt_len)
            .ok_or_else(|| Error::Corruption("meta PMT length overflows".into()))?;
        let alloc_len_start = pmt_end;
        let alloc_len_end = alloc_len_start
            .checked_add(4)
            .ok_or_else(|| Error::Corruption("meta allocator length overflows".into()))?;
        if data.len() < alloc_len_end {
            return Err(Error::Corruption("meta file truncated".into()));
        }

        let pmt = PMT::from_bytes(&data[4..pmt_end])
            .ok_or_else(|| Error::Corruption("invalid PMT data".into()))?;

        let alloc_offset = alloc_len_start;
        let alloc_len = u32::from_le_bytes([
            data[alloc_offset],
            data[alloc_offset + 1],
            data[alloc_offset + 2],
            data[alloc_offset + 3],
        ]) as usize;

        let alloc_end = alloc_len_end
            .checked_add(alloc_len)
            .ok_or_else(|| Error::Corruption("meta allocator length overflows".into()))?;
        if data.len() != alloc_end {
            return Err(Error::Corruption(
                if data.len() < alloc_end {
                    "meta allocator data is truncated"
                } else {
                    "meta file has trailing bytes"
                }
                .into(),
            ));
        }

        let alloc_data = &data[alloc_len_end..alloc_end];
        let allocator = PageAllocator::from_bytes(alloc_data)
            .ok_or_else(|| Error::Corruption("invalid allocator data".into()))?;

        Ok((pmt, allocator))
    }

    /// Save PMT and allocator to meta file.
    fn save_meta(path: &Path, pmt: &PMT, allocator: &PageAllocator) -> Result<u64> {
        Self::save_meta_with_directory_sync(path, pmt, allocator, true)
    }

    fn save_meta_without_directory_sync(
        path: &Path,
        pmt: &PMT,
        allocator: &PageAllocator,
    ) -> Result<u64> {
        Self::save_meta_with_directory_sync(path, pmt, allocator, false)
    }

    fn save_meta_with_directory_sync(
        path: &Path,
        pmt: &PMT,
        allocator: &PageAllocator,
        sync_parent: bool,
    ) -> Result<u64> {
        let pmt_bytes = pmt.to_bytes();
        let alloc_bytes = allocator.to_bytes();

        let pmt_len = u32::try_from(pmt_bytes.len())
            .map_err(|_| Error::InvalidArgument("PMT checkpoint is too large".into()))?;
        let alloc_len = u32::try_from(alloc_bytes.len())
            .map_err(|_| Error::InvalidArgument("allocator checkpoint is too large".into()))?;

        let mut buf = Vec::with_capacity(
            META_MAGIC.len() + 4 + 4 + pmt_bytes.len() + 4 + alloc_bytes.len() + 4,
        );
        buf.extend_from_slice(&META_MAGIC);
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&pmt_len.to_le_bytes());
        buf.extend_from_slice(&pmt_bytes);
        buf.extend_from_slice(&alloc_len.to_le_bytes());
        buf.extend_from_slice(&alloc_bytes);
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        if sync_parent {
            atomic_write(path, &buf)?;
        } else {
            atomic_write_without_directory_sync(path, &buf)?;
        }
        Ok(buf.len() as u64)
    }

    fn save_meta_delta_without_directory_sync(
        path: &Path,
        parent_checkpoint_id: u64,
        parent_pmt: &PMT,
        pmt: &PMT,
        allocator: &PageAllocator,
    ) -> Result<u64> {
        let mut updates = pmt
            .iter()
            .filter_map(|(page_id, mapping)| {
                (parent_pmt.get(page_id) != Some(mapping)).then_some((page_id, *mapping))
            })
            .collect::<Vec<_>>();
        updates.sort_unstable_by_key(|(page_id, _)| *page_id);
        let mut removals = parent_pmt
            .iter()
            .filter_map(|(page_id, _)| (!pmt.contains(page_id)).then_some(page_id))
            .collect::<Vec<_>>();
        removals.sort_unstable();

        let update_count = u32::try_from(updates.len())
            .map_err(|_| Error::InvalidArgument("metadata delta has too many updates".into()))?;
        let removal_count = u32::try_from(removals.len())
            .map_err(|_| Error::InvalidArgument("metadata delta has too many removals".into()))?;
        let allocator_bytes = allocator.to_bytes();
        let allocator_len = u32::try_from(allocator_bytes.len())
            .map_err(|_| Error::InvalidArgument("metadata delta allocator is too large".into()))?;

        let total_len = META_DELTA_HEADER_SIZE
            .checked_add(
                updates
                    .len()
                    .checked_mul(8 + PageMapping::SERIALIZED_SIZE)
                    .ok_or(Error::DiskFull)?,
            )
            .and_then(|size| size.checked_add(removals.len().checked_mul(8)?))
            .and_then(|size| size.checked_add(allocator_bytes.len()))
            .and_then(|size| size.checked_add(META_DELTA_CHECKSUM_SIZE))
            .ok_or(Error::DiskFull)?;
        let mut buf = Vec::with_capacity(total_len);
        buf.extend_from_slice(&META_DELTA_MAGIC);
        buf.extend_from_slice(&META_DELTA_VERSION.to_le_bytes());
        buf.extend_from_slice(&parent_checkpoint_id.to_le_bytes());
        buf.extend_from_slice(&update_count.to_le_bytes());
        buf.extend_from_slice(&removal_count.to_le_bytes());
        buf.extend_from_slice(&allocator_len.to_le_bytes());
        for (page_id, mapping) in updates {
            buf.extend_from_slice(&page_id.to_le_bytes());
            buf.extend_from_slice(&mapping.to_bytes());
        }
        for page_id in removals {
            buf.extend_from_slice(&page_id.to_le_bytes());
        }
        buf.extend_from_slice(&allocator_bytes);
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        atomic_write_without_directory_sync(path, &buf)?;
        Ok(buf.len() as u64)
    }

    fn load_meta_ancestors(path: &Path, checkpoint_id: u64) -> Result<BTreeSet<u64>> {
        let mut ancestors = BTreeSet::new();
        let mut current_id = checkpoint_id;
        for _ in 0..=MAX_META_DELTA_CHAIN {
            if !ancestors.insert(current_id) {
                return Err(Error::Corruption(
                    "metadata delta chain contains a cycle".into(),
                ));
            }
            let current_path = path.join(format!("seerdb.meta.{current_id}"));
            let data = fs::read(&current_path)?;
            if data.len() < META_DELTA_MAGIC.len()
                || data[..META_DELTA_MAGIC.len()] != META_DELTA_MAGIC
            {
                return Ok(ancestors);
            }
            let delta = Self::load_meta_delta(&data)?;
            if delta.parent_checkpoint_id == 0 {
                return Err(Error::Corruption(
                    "metadata delta has no full checkpoint parent".into(),
                ));
            }
            current_id = delta.parent_checkpoint_id;
        }
        Err(Error::Corruption(format!(
            "metadata delta chain exceeds maximum length {MAX_META_DELTA_CHAIN}"
        )))
    }

    fn generation_meta_bytes(
        &self,
        parent: Manifest,
        dirty_page_count: usize,
    ) -> Result<(u64, bool)> {
        let pmt_bytes = (self.engine.pmt().to_bytes().len() as u64)
            .checked_add(
                (dirty_page_count as u64)
                    .checked_mul((8 + PageMapping::SERIALIZED_SIZE) as u64)
                    .ok_or(Error::DiskFull)?,
            )
            .ok_or(Error::DiskFull)?;
        let allocator_bytes = (self.engine.allocator().to_bytes().len() as u64)
            .checked_add(
                (dirty_page_count as u64)
                    .checked_mul(8)
                    .ok_or(Error::DiskFull)?,
            )
            .ok_or(Error::DiskFull)?;
        let full_bytes = (META_MAGIC.len() as u64)
            .checked_add(4 + 4 + 4 + 4)
            .and_then(|size| size.checked_add(pmt_bytes))
            .and_then(|size| size.checked_add(allocator_bytes))
            .ok_or(Error::DiskFull)?;
        if parent.pmt_checkpoint_id.get() == 0 {
            return Ok((full_bytes, true));
        }

        let checkpoint = self
            .path
            .join(format!("seerdb.meta.{}", parent.pmt_checkpoint_id.get()));
        let (_, _, depth) = Self::load_meta_with_depth(&checkpoint)?;
        if depth >= MAX_META_DELTA_CHAIN {
            return Ok((full_bytes, true));
        }
        let delta_bytes = ((META_DELTA_HEADER_SIZE + META_DELTA_CHECKSUM_SIZE) as u64)
            .checked_add(
                (dirty_page_count as u64)
                    .checked_mul((8 + PageMapping::SERIALIZED_SIZE + 8) as u64)
                    .ok_or(Error::DiskFull)?,
            )
            .and_then(|size| size.checked_add(self.engine.allocator().to_bytes().len() as u64))
            .ok_or(Error::DiskFull)?;
        Ok((delta_bytes, false))
    }

    fn save_generation_meta(&self, path: &Path, parent: Manifest) -> Result<u64> {
        if parent.pmt_checkpoint_id.get() == 0 {
            return Self::save_meta_without_directory_sync(
                path,
                self.engine.pmt(),
                self.engine.allocator(),
            );
        }
        let parent_path = self
            .path
            .join(format!("seerdb.meta.{}", parent.pmt_checkpoint_id.get()));
        let (parent_pmt, _, depth) = Self::load_meta_with_depth(&parent_path)?;
        if depth >= MAX_META_DELTA_CHAIN {
            Self::save_meta_without_directory_sync(path, self.engine.pmt(), self.engine.allocator())
        } else {
            Self::save_meta_delta_without_directory_sync(
                path,
                parent.pmt_checkpoint_id.get(),
                &parent_pmt,
                self.engine.pmt(),
                self.engine.allocator(),
            )
        }
    }

    /// Recover a committed WAL prefix and reject corrupt complete records.
    fn recover_from_wal(
        wal_path: &Path,
        current_manifest: Option<Manifest>,
        btree: &mut BTree,
        blobs: &mut BlobManager,
    ) -> Result<RecoverySummary> {
        let wal_data = fs::read(wal_path)?;
        let (records, status) = WalManager::parse_records_with_status(&wal_data);
        if status == ParseStatus::Corrupt {
            return Err(Error::Corruption("invalid complete WAL record".into()));
        }

        let mut pending = Vec::new();
        let mut last_commit = None;
        let mut last_commit_offset = 0;
        let mut blob_changed = false;
        let mut offset = 0u64;
        for record in &records {
            let record_len = record.to_bytes().len() as u64;
            match record.record_type {
                RecordType::Put | RecordType::Delete | RecordType::PutV2 | RecordType::DeleteV2 => {
                    pending.push(record)
                }
                RecordType::Commit => {
                    let commit = record
                        .commit_record()
                        .ok_or_else(|| Error::Corruption("invalid WAL commit envelope".into()))?;
                    if commit.mutation_count != pending.len() as u64
                        || commit.digest != digest_records(&pending)
                    {
                        return Err(Error::Corruption(
                            "WAL commit does not match its mutation prefix".into(),
                        ));
                    }

                    if let Some(current) = current_manifest {
                        match commit.generation_id.get().cmp(&current.generation_id.get()) {
                            std::cmp::Ordering::Less => {
                                if commit.commit_id.get() > current.commit_id.get() {
                                    return Err(Error::Corruption(
                                        "WAL commit frontier is inconsistent with manifest".into(),
                                    ));
                                }
                                pending.clear();
                                offset += record_len;
                                continue;
                            }
                            std::cmp::Ordering::Equal => {
                                if commit.commit_id != current.commit_id {
                                    return Err(Error::Corruption(
                                        "WAL commit frontier is inconsistent with manifest".into(),
                                    ));
                                }
                                if commit.root_page_id != current.root_page_id
                                    || commit.mutation_count != current.mutation_count
                                    || commit.digest != current.digest
                                {
                                    return Err(Error::Corruption(
                                        "WAL commit disagrees with authoritative manifest".into(),
                                    ));
                                }
                                pending.clear();
                                offset += record_len;
                                continue;
                            }
                            std::cmp::Ordering::Greater => {
                                if commit.commit_id <= current.commit_id {
                                    return Err(Error::Corruption(
                                        "WAL commit frontier is inconsistent with manifest".into(),
                                    ));
                                }
                            }
                        }
                    }

                    for mutation in pending.drain(..) {
                        blob_changed |= apply_mutation(mutation, btree, blobs)?;
                    }
                    last_commit = Some(commit);
                    last_commit_offset = offset;
                }
                _ => {}
            }
            offset += record_len;
        }

        Ok(RecoverySummary {
            last_commit,
            last_commit_offset,
            blob_changed,
        })
    }
}

/// Recovery result for the committed WAL prefix.
#[derive(Debug, Clone, Copy)]
struct RecoverySummary {
    last_commit: Option<CommitRecord>,
    last_commit_offset: u64,
    blob_changed: bool,
}

fn error_message(error: Error) -> String {
    match error {
        Error::Corruption(message) => message,
        Error::Check { message, .. } => message,
        other => other.to_string(),
    }
}

fn extend_digest(current: u32, record: &WalRecord) -> u32 {
    let bytes = record.to_bytes();
    let mut input = Vec::with_capacity(4 + bytes.len());
    input.extend_from_slice(&current.to_le_bytes());
    input.extend_from_slice(&bytes);
    crc32c::crc32c(&input)
}

fn digest_records(records: &[&WalRecord]) -> u32 {
    records
        .iter()
        .fold(0, |digest, record| extend_digest(digest, record))
}

fn apply_put_mutation(
    key: &[u8],
    value: &[u8],
    btree: &mut BTree,
    blobs: &mut BlobManager,
) -> Result<bool> {
    let previous_blob = match btree.lookup(key)? {
        LookupResult::Blob(pointer) => Some(pointer),
        _ => None,
    };
    let had_previous_blob = previous_blob.is_some();

    let separates = blobs.should_separate(value.len());
    if separates {
        let pointer = blobs.append(key, value.to_vec());
        if let Err(error) = btree.upsert_blob(key, pointer) {
            let _ = blobs.rollback_append(&pointer);
            return Err(error.into());
        }
    } else {
        btree.upsert(key, value)?;
    }

    if let Some(pointer) = previous_blob {
        blobs.mark_deleted(&pointer);
    }
    Ok(separates || had_previous_blob)
}

fn apply_mutation(record: &WalRecord, btree: &mut BTree, blobs: &mut BlobManager) -> Result<bool> {
    match record.record_type {
        RecordType::Put => {
            let (key, value) = decode_put_payload(false, &record.payload)?;
            apply_put_mutation(key, value, btree, blobs)
        }
        RecordType::PutV2 => {
            let (key, value) = decode_put_payload(true, &record.payload)?;
            apply_put_mutation(key, value, btree, blobs)
        }
        RecordType::Delete | RecordType::DeleteV2 => {
            let key =
                decode_delete_payload(record.record_type == RecordType::DeleteV2, &record.payload)?;
            let previous_blob = match btree.lookup(key)? {
                LookupResult::Blob(pointer) => Some(pointer),
                _ => None,
            };
            let found = btree.delete(key)?;
            let blob_changed = found && previous_blob.is_some();
            if blob_changed && let Some(pointer) = previous_blob {
                blobs.mark_deleted(&pointer);
            }
            Ok(blob_changed)
        }
        _ => Err(Error::Corruption(
            "non-mutation passed to WAL applier".into(),
        )),
    }
}

fn decode_put_payload(v2: bool, payload: &[u8]) -> Result<(&[u8], &[u8])> {
    if v2 {
        if payload.len() < 4 + 4 {
            return Err(Error::Corruption("WAL v2 put record too small".into()));
        }
        let key_len = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        let value_len_offset = 4usize
            .checked_add(key_len)
            .ok_or_else(|| Error::Corruption("WAL key length overflow".into()))?;
        if payload.len() < value_len_offset + 4 {
            return Err(Error::Corruption("WAL v2 put key is truncated".into()));
        }
        let value_len = u32::from_le_bytes([
            payload[value_len_offset],
            payload[value_len_offset + 1],
            payload[value_len_offset + 2],
            payload[value_len_offset + 3],
        ]) as usize;
        let value_offset = value_len_offset + 4;
        if payload.len() != value_offset + value_len {
            return Err(Error::Corruption("WAL v2 put value is truncated".into()));
        }
        return Ok((&payload[4..value_len_offset], &payload[value_offset..]));
    }

    // Read the pre-v2 u16 layout so an upgrade can recover an older WAL.
    if payload.len() < 4 {
        return Err(Error::Corruption("WAL put record too small".into()));
    }
    let key_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let value_len_offset = 2usize
        .checked_add(key_len)
        .ok_or_else(|| Error::Corruption("WAL key length overflow".into()))?;
    if payload.len() < value_len_offset + 2 {
        return Err(Error::Corruption("WAL put key is truncated".into()));
    }
    let value_len =
        u16::from_le_bytes([payload[value_len_offset], payload[value_len_offset + 1]]) as usize;
    let value_offset = value_len_offset + 2;
    if payload.len() != value_offset + value_len {
        return Err(Error::Corruption("WAL put value is truncated".into()));
    }
    Ok((&payload[2..value_len_offset], &payload[value_offset..]))
}

fn validate_wal_key_length(key: &[u8]) -> Result<()> {
    if key.len() > MAX_KEY_SIZE {
        return Err(Error::InvalidArgument(
            "key exceeds the maximum B-tree page key size".into(),
        ));
    }
    if u32::try_from(key.len()).is_err() {
        return Err(Error::InvalidArgument(
            "key exceeds the durable WAL length limit".into(),
        ));
    }
    Ok(())
}

fn validate_wal_put_lengths(key: &[u8], value: &[u8]) -> Result<()> {
    validate_wal_key_length(key)?;
    if u32::try_from(value.len()).is_err() {
        return Err(Error::InvalidArgument(
            "value exceeds the durable WAL length limit".into(),
        ));
    }
    Ok(())
}

fn decode_delete_payload(v2: bool, payload: &[u8]) -> Result<&[u8]> {
    if v2 {
        if payload.len() < 4 {
            return Err(Error::Corruption("WAL v2 delete record too small".into()));
        }
        let key_len = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        if payload.len() != 4 + key_len {
            return Err(Error::Corruption("WAL v2 delete key is truncated".into()));
        }
        return Ok(&payload[4..]);
    }

    // Read the pre-v2 u16 layout so an upgrade can recover an older WAL.
    if payload.len() < 2 {
        return Err(Error::Corruption("WAL delete record too small".into()));
    }
    let key_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    if payload.len() != 2 + key_len {
        return Err(Error::Corruption("WAL delete key is truncated".into()));
    }
    Ok(&payload[2..])
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    atomic_write_with_fault_injection(path, data, true)
}

/// Persist the reuse reservation without consuming a fault intended for a
/// PMT/blob checkpoint. The ledger has its own real I/O error path; the
/// publication-fault harness targets the artifact under test explicitly.
fn atomic_write_without_fault_injection(path: &Path, data: &[u8]) -> Result<()> {
    atomic_write_with_fault_injection(path, data, false)
}

fn atomic_write_with_fault_injection(path: &Path, data: &[u8], inject_faults: bool) -> Result<()> {
    atomic_write_with_options(path, data, inject_faults, true)
}

fn atomic_write_without_directory_sync(path: &Path, data: &[u8]) -> Result<()> {
    atomic_write_with_options(path, data, true, false)
}

fn atomic_write_with_options(
    path: &Path,
    data: &[u8],
    inject_faults: bool,
    sync_parent: bool,
) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    if let Err(error) = preallocate_file(&file, data.len() as u64) {
        drop(file);
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    #[cfg(any(test, feature = "fault-injection"))]
    let short_write = inject_faults
        && ((path.file_name().is_some_and(|name| name == BLOB_FILE)
            && FAIL_NEXT_BLOB_SEGMENT_CATALOG_SHORT_WRITE.with(|failure| failure.replace(false)))
            || FAIL_NEXT_ATOMIC_SHORT_WRITE.with(|failure| failure.replace(false)));
    #[cfg(not(any(test, feature = "fault-injection")))]
    let short_write = false;
    #[cfg(not(any(test, feature = "fault-injection")))]
    let _ = inject_faults;
    #[cfg(any(test, feature = "fault-injection"))]
    let torn_write = inject_faults
        && ((path.file_name().is_some_and(|name| name == BLOB_FILE)
            && FAIL_NEXT_BLOB_SEGMENT_CATALOG_TORN_WRITE.with(|failure| failure.replace(false)))
            || FAIL_NEXT_ATOMIC_TORN_WRITE.with(|failure| failure.replace(false)));
    #[cfg(not(any(test, feature = "fault-injection")))]
    let torn_write = false;

    if short_write {
        let prefix_len = data.len() / 2;
        file.set_len(prefix_len as u64)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&data[..prefix_len])?;
    } else {
        file.write_all(data)?;
        if torn_write && !data.is_empty() {
            let offset = data.len() / 2;
            file.seek(SeekFrom::Start(offset as u64))?;
            file.write_all(&[data[offset] ^ 0xA5])?;
        }
    }
    file.flush()?;

    #[cfg(any(test, feature = "fault-injection"))]
    if inject_faults
        && path.file_name().is_some_and(|name| name == BLOB_FILE)
        && FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC.with(|failure| failure.replace(false))
    {
        return Err(std::io::Error::other("injected blob catalog sync failure").into());
    }
    file.sync_all()?;
    drop(file);

    #[cfg(any(test, feature = "fault-injection"))]
    let blob_catalog_rename_failure = inject_faults
        && path.file_name().is_some_and(|name| name == BLOB_FILE)
        && FAIL_NEXT_BLOB_SEGMENT_CATALOG_RENAME.with(|failure| failure.replace(false));
    #[cfg(any(test, feature = "fault-injection"))]
    if blob_catalog_rename_failure {
        return Err(std::io::Error::other("injected blob catalog rename failure").into());
    }

    #[cfg(any(test, feature = "fault-injection"))]
    let injected = inject_faults && FAIL_NEXT_ATOMIC_RENAME.with(|failure| failure.replace(false));
    #[cfg(any(test, feature = "fault-injection"))]
    if injected {
        return Err(std::io::Error::other("injected atomic rename failure").into());
    }

    fs::rename(&temporary, path)?;

    #[cfg(any(test, feature = "fault-injection"))]
    if short_write || torn_write {
        return Err(std::io::Error::other("injected atomic artifact write failure").into());
    }

    if sync_parent {
        sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
    } else {
        Ok(())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn atomic_write_reserved(
    path: &Path,
    reservation: &Path,
    data: &[u8],
    sync_parent: bool,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(reservation)?;
    if !reserve_file(&file, data.len() as u64)? {
        return Err(Error::DiskFull);
    }
    file.set_len(data.len() as u64)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(data)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);

    #[cfg(any(test, feature = "fault-injection"))]
    let injected = FAIL_NEXT_ATOMIC_RENAME.with(|failure| failure.replace(false));
    #[cfg(any(test, feature = "fault-injection"))]
    if injected {
        return Err(std::io::Error::other("injected atomic rename failure").into());
    }

    fs::rename(reservation, path)?;
    if sync_parent {
        sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
    } else {
        Ok(())
    }
}

#[cfg(any(test, feature = "fault-injection"))]
#[allow(dead_code)]
fn inject_atomic_rename_failure() {
    FAIL_NEXT_ATOMIC_RENAME.with(|failure| failure.set(true));
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    Ok(())
}

fn sync_publication_directory(path: &Path) -> Result<()> {
    #[cfg(any(test, feature = "fault-injection"))]
    if FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC.with(|failure| failure.replace(false)) {
        return Err(std::io::Error::other("injected publication directory sync failure").into());
    }
    sync_directory(path)
}

fn sync_history_prune_directory(path: &Path) -> Result<()> {
    #[cfg(any(test, feature = "fault-injection"))]
    if FAIL_NEXT_HISTORY_PRUNE_DIRECTORY_SYNC.with(|failure| failure.replace(false)) {
        return Err(std::io::Error::other("injected history-prune directory sync failure").into());
    }
    sync_directory(path)
}

/// Sync a newly created directory and each newly reachable parent directory.
///
/// `create_dir_all` can create more than one ancestor. Syncing only the
/// immediate parent would leave an outer directory entry vulnerable to being
/// lost after an acknowledged create on filesystems that honor directory
/// durability separately from file durability.
fn sync_directory_chain(path: &Path) -> Result<()> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    let mut current = path;
    loop {
        sync_directory(current)?;
        let Some(parent) = current.parent() else {
            break;
        };
        if parent == current {
            break;
        }
        if parent.as_os_str().is_empty() {
            sync_directory(Path::new("."))?;
            break;
        }
        current = parent;
    }
    Ok(())
}

/// Remove non-authoritative atomic-publication temporary files left by an
/// interrupted write. They are safe to discard because every authoritative
/// artifact is selected by the manifest or its catalog, never by a `.tmp`
/// name. Read-only/check handles deliberately leave them untouched.
fn cleanup_orphaned_temporary_artifacts(path: &Path) -> Result<()> {
    let mut removed = false;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() && entry.file_name().to_string_lossy().ends_with(".tmp") {
            fs::remove_file(entry.path())?;
            removed = true;
        }
    }
    if removed {
        sync_directory(path)?;
    }
    Ok(())
}

fn clear_blob_reservation(path: &Path) -> Result<()> {
    let reservation = path.join(BLOB_RESERVATION_FILE);
    if reservation.exists() {
        fs::remove_file(reservation)?;
        sync_directory(path)?;
    }
    Ok(())
}

fn clear_wal_reservation(path: &Path) -> Result<()> {
    let reservation = path.join(WAL_RESERVATION_FILE);
    if reservation.exists() {
        fs::remove_file(reservation)?;
        sync_directory(path)?;
    }
    Ok(())
}

fn copy_snapshot_artifacts(source: &Path, destination: &Path) -> Result<u32> {
    copy_artifacts(source, destination, false)
}

fn copy_repair_artifacts(source: &Path, destination: &Path) -> Result<u32> {
    copy_artifacts(source, destination, true)
}

fn copy_artifacts(source: &Path, destination: &Path, include_wal: bool) -> Result<u32> {
    let mut copied_files = 0u32;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".tmp")
            || name == LOCK_FILE
            || name == ARCHIVE_MARKER_FILE
            || !(name == MANIFEST_FILE
                || name == MANIFEST_HISTORY_FILE
                || name == REUSE_LEDGER_FILE
                || name == DATA_FILE
                || name == BLOB_FILE
                || name == BLOB_DELTA_FILE
                || name.starts_with(BLOB_SEGMENT_PREFIX)
                || name == META_FILE
                || name.starts_with("seerdb.meta.")
                || (include_wal && name == WAL_FILE))
        {
            continue;
        }

        let destination_file = destination.join(name.as_ref());
        fs::copy(entry.path(), &destination_file)?;
        OpenOptions::new()
            .read(true)
            .open(destination_file)?
            .sync_all()?;
        copied_files = copied_files
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("too many snapshot artifacts".into()))?;
    }

    Ok(copied_files)
}

impl Drop for DB {
    fn drop(&mut self) {
        // Don't call close() — let the WAL persist for crash recovery.
        // The user should explicitly call close() or flush() to ensure
        // data is persisted and WAL is cleaned up.
        // If the process crashes, the WAL file will be preserved for recovery.
        if let Some(lock_file) = self.lock_file.take() {
            let _ = lock_file.unlock();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::io::{Seek, SeekFrom, Write};
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    use std::os::unix::fs::MetadataExt;
    use std::process::Command;
    use tempfile::tempdir;

    use crate::storage::format::MANIFEST_SLOT_SIZE;

    const TEST_SEGMENT_CATALOG_DELTA_LIMIT: u32 = 64;

    #[test]
    fn test_db_open() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path().join("test.db"), Options::default());
        assert!(db.is_ok());
    }

    #[test]
    fn test_db_open_rejects_existing_directory_without_storage_artifacts() {
        let dir = tempdir().unwrap();
        let empty_path = dir.path().join("empty.db");
        fs::create_dir(&empty_path).unwrap();
        assert!(matches!(
            DB::open(&empty_path, Options::default()),
            Err(Error::Corruption(message))
                if message.contains("no authoritative storage artifacts")
        ));

        let orphan_path = dir.path().join("orphan.db");
        fs::create_dir(&orphan_path).unwrap();
        fs::write(orphan_path.join(BLOB_FILE), b"orphaned blob image").unwrap();
        assert!(matches!(
            DB::open(&orphan_path, Options::default()),
            Err(Error::Corruption(message))
                if message.contains("no authoritative storage artifacts")
        ));
    }

    #[test]
    fn test_db_open_rejects_missing_manifest_artifacts_without_recreating_them() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing-data.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
        drop(db);

        fs::remove_file(path.join(DATA_FILE)).unwrap();
        let check = DB::check(&path, Options::default());
        assert!(
            matches!(
                check,
                Err(Error::Check {
                    kind: CheckFailureKind::Target,
                    ref message
                }) if message.contains("required manifest or data artifacts")
            ),
            "check error: {check:?}"
        );
        assert!(matches!(
            DB::open(&path, Options::default()),
            Err(Error::Corruption(message))
                if message.contains("is missing the data file")
        ));
        assert!(!path.join(DATA_FILE).exists());

        let checkpoint_path = dir.path().join("missing-checkpoint.db");
        let mut db = DB::open(&checkpoint_path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
        drop(db);
        let checkpoint = fs::read_dir(&checkpoint_path)
            .unwrap()
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_prefix("seerdb.meta."))
                    .and_then(|suffix| suffix.parse::<u64>().ok())
                    .is_some()
            })
            .expect("published database has a numbered PMT checkpoint");
        fs::remove_file(&checkpoint).unwrap();

        let check = DB::check(&checkpoint_path, Options::default());
        assert!(matches!(
            check,
            Err(Error::Check {
                kind: CheckFailureKind::Checkpoint,
                ref message
            }) if message.contains("is missing checkpoint")
        ));
        assert!(matches!(
            DB::open(&checkpoint_path, Options::default()),
            Err(Error::Corruption(message))
                if message.contains("is missing checkpoint")
        ));
        assert!(!checkpoint.exists());
    }

    #[test]
    fn test_db_create_refuses_existing_store_without_reinterpreting_it() {
        let dir = tempdir().unwrap();
        let reserved_path = dir.path().join("reserved.db");
        fs::create_dir(&reserved_path).unwrap();
        assert!(matches!(
            DB::create(&reserved_path, Options::default()),
            Err(Error::InvalidArgument(message)) if message.contains("already exists")
        ));

        let path = dir.path().join("nested").join("created.db");
        let mut db = DB::create(&path, Options::default()).unwrap();
        db.put(b"catalog", b"durable").unwrap();
        db.flush().unwrap();
        drop(db);

        assert!(matches!(
            DB::create(&path, Options::default()),
            Err(Error::InvalidArgument(message)) if message.contains("already exists")
        ));
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"catalog").unwrap(), Some(b"durable".to_vec()));
    }

    #[test]
    fn test_db_allows_only_one_writer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("locked.db");
        let db = DB::open(&path, Options::default()).unwrap();
        assert!(matches!(
            DB::open(&path, Options::default()),
            Err(Error::DatabaseBusy)
        ));
        drop(db);
        assert!(DB::open(&path, Options::default()).is_ok());
    }

    #[test]
    fn test_db_check_is_non_mutating_and_does_not_take_writer_lock() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("check.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"pending", b"value").unwrap();

        let pending = DB::check(&path, Options::default()).unwrap();
        assert_eq!(pending.wal_status, WalCheckStatus::Pending);
        assert_eq!(
            pending.verification.wal_bytes,
            fs::metadata(path.join(WAL_FILE)).unwrap().len()
        );
        assert_eq!(db.get(b"pending").unwrap(), Some(b"value".to_vec()));

        db.flush().unwrap();
        let clean = DB::check(&path, Options::default()).unwrap();
        assert_eq!(clean.wal_status, WalCheckStatus::Clean);
        assert_eq!(clean.verification.wal_bytes, 0);
        db.close().unwrap();
    }

    #[test]
    fn test_db_check_does_not_create_missing_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing.db");

        assert!(matches!(
            DB::check(&path, Options::default()),
            Err(Error::Check { kind: CheckFailureKind::Target, message })
                if message.contains("does not exist")
        ));
        assert!(!path.exists());
    }

    #[test]
    fn test_db_put_get() {
        let dir = tempdir().unwrap();
        let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();

        assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(db.get(b"key3").unwrap(), None);
    }

    #[test]
    fn test_db_rejects_key_larger_than_page_format_before_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("oversized-key.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        let key = vec![0xA5; MAX_KEY_SIZE + 1];

        assert!(matches!(
            db.put(&key, b"value"),
            Err(Error::InvalidArgument(message))
                if message.contains("maximum B-tree page key size")
        ));
        assert_eq!(db.durability_status().pending_mutations, 0);
        assert!(!path.join(WAL_FILE).exists());
    }

    #[test]
    fn test_db_delete() {
        let dir = tempdir().unwrap();
        let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

        db.put(b"key", b"value").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));

        db.delete(b"key").unwrap();
        assert_eq!(db.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_db_range() {
        let dir = tempdir().unwrap();
        let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
        db.put(b"d", b"4").unwrap();

        let results = db.range(b"b", b"d").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"b");
        assert_eq!(results[1].0, b"c");
    }

    #[test]
    fn test_db_close() {
        let dir = tempdir().unwrap();
        let mut db = DB::open(dir.path().join("test.db"), Options::default()).unwrap();

        db.put(b"key", b"value").unwrap();
        db.close().unwrap();

        // Operations after close should fail.
        assert!(db.put(b"key2", b"value2").is_err());
    }

    #[test]
    fn test_db_meta_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create and populate.
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();
        }

        // Meta file should exist.
        assert!(path.join(META_FILE).exists());
    }

    #[test]
    fn test_db_metrics_attribute_page_work_and_lazy_reads() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metrics.db");

        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();

            let metrics = db.metrics().unwrap();
            assert_eq!(metrics.storage.physical_page_writes, 1);
            assert_eq!(metrics.storage.page_bytes_written, PAGE_SIZE as u64);
            assert_eq!(metrics.storage.generation_flushes, 1);
            assert_eq!(metrics.storage.syncs, 1);
            assert_eq!(metrics.data_bytes, PAGE_SIZE as u64);
            assert_eq!(metrics.wal_bytes, 0);
            assert!(metrics.publication.wal_bytes_written > 0);
            assert!(metrics.publication.metadata_bytes_written > 0);
            assert_eq!(metrics.publication.blob_bytes_written, 0);
            assert!(metrics.publication.history_bytes_written > 0);
            assert_eq!(
                metrics.publication.manifest_bytes_written,
                MANIFEST_SLOT_SIZE as u64
            );
        }

        let reopened = DB::open(&path, Options::default()).unwrap();
        let before = reopened.metrics().unwrap();
        assert_eq!(before.storage.logical_page_reads, 0);
        assert_eq!(before.storage.physical_page_reads, 0);
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
        let after = reopened.metrics().unwrap();
        assert_eq!(after.storage.logical_page_reads, 2);
        assert_eq!(after.storage.physical_page_reads, 1);
        assert_eq!(after.storage.page_bytes_read, PAGE_SIZE as u64);
        assert_eq!(after.buffer.reads, 1);
    }

    #[test]
    fn test_db_metadata_delta_reopens_and_preserves_parent_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metadata-delta.db");

        let snapshot_id;
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            for index in 0..200 {
                db.put(
                    format!("key-{index:04}").as_bytes(),
                    format!("value-{index:04}").as_bytes(),
                )
                .unwrap();
            }
            db.flush().unwrap();
            let first = db.durability_status();
            snapshot_id = db.retain_commit(first.commit_id).unwrap();
            db.put(b"key-0000", b"updated-value").unwrap();
            db.flush().unwrap();
            assert_eq!(
                db.get(b"key-0000").unwrap(),
                Some(b"updated-value".to_vec())
            );
            assert_eq!(
                db.get_at(snapshot_id, b"key-0000").unwrap(),
                Some(b"value-0000".to_vec())
            );
        }

        let first_checkpoint = path.join("seerdb.meta.1");
        let second_checkpoint = path.join("seerdb.meta.2");
        let first_bytes = fs::read(&first_checkpoint).unwrap();
        let second_bytes = fs::read(&second_checkpoint).unwrap();
        assert!(first_bytes.starts_with(&META_MAGIC));
        assert!(second_bytes.starts_with(&META_DELTA_MAGIC));
        assert!(second_bytes.len() < first_bytes.len());

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(
            reopened.get(b"key-0000").unwrap(),
            Some(b"updated-value".to_vec())
        );
        assert_eq!(
            reopened.get_at(snapshot_id, b"key-0000").unwrap(),
            Some(b"value-0000".to_vec())
        );
        reopened.release_snapshot(snapshot_id).unwrap();
        reopened.prune_history().unwrap();
        assert!(first_checkpoint.is_file());
        assert!(second_checkpoint.is_file());
    }

    #[test]
    fn test_db_metadata_delta_corruption_fails_closed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt-metadata-delta.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();
        drop(db);

        let checkpoint = path.join("seerdb.meta.2");
        let mut bytes = fs::read(&checkpoint).unwrap();
        assert!(bytes.starts_with(&META_DELTA_MAGIC));
        let valid_delta = bytes.clone();
        bytes.push(0xA5);
        fs::write(&checkpoint, bytes).unwrap();
        assert!(matches!(
            DB::open(&path, Options::default()),
            Err(Error::Corruption(message)) if message.contains("metadata delta")
        ));
        assert!(matches!(
            DB::check(&path, Options::default()),
            Err(Error::Check {
                kind: CheckFailureKind::Checkpoint,
                ..
            })
        ));

        let mut anchorless = valid_delta;
        anchorless[12..20].fill(0);
        let checksum = crc32c::crc32c(&anchorless[..anchorless.len() - 4]);
        let checksum_offset = anchorless.len() - 4;
        anchorless[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
        fs::write(&checkpoint, anchorless).unwrap();
        let error = match DB::open(&path, Options::default()) {
            Ok(_) => panic!("anchorless metadata delta unexpectedly opened"),
            Err(error) => error,
        };
        assert!(
            matches!(error, Error::Corruption(ref message) if message.contains("no full checkpoint parent")),
            "{error:?}"
        );
    }

    #[test]
    fn test_db_metadata_delta_chain_consolidates_at_hard_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metadata-delta-chain.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-0").unwrap();
        db.flush().unwrap();

        for revision in 1..=MAX_META_DELTA_CHAIN + 1 {
            db.put(b"key", format!("value-{revision}").as_bytes())
                .unwrap();
            db.flush().unwrap();
        }
        let consolidation_generation = MAX_META_DELTA_CHAIN as u64 + 2;
        let consolidated = path.join(format!("seerdb.meta.{consolidation_generation}"));
        assert!(fs::read(&consolidated).unwrap().starts_with(&META_MAGIC));
        // The inactive slot still names the immediately previous delta
        // frontier. Publish one more generation so that fallback advances
        // beyond the consolidation before pruning the old chain.
        db.put(b"key", b"value-66").unwrap();
        db.flush().unwrap();
        let report = db.prune_history().unwrap();
        assert_eq!(report.removed_checkpoints, MAX_META_DELTA_CHAIN as u64 + 1);
        assert!(consolidated.is_file());
        assert_eq!(db.get(b"key").unwrap(), Some(b"value-66".to_vec()));
        db.close().unwrap();
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-66".to_vec()));
    }

    #[test]
    fn test_compaction_after_metadata_delta_admits_relocation_sidecar() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("metadata-delta-compaction.db");
        let mut db = DB::open(&path, Options::default()).unwrap();

        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();
        assert!(
            fs::read(path.join("seerdb.meta.2"))
                .unwrap()
                .starts_with(&META_DELTA_MAGIC)
        );

        let report = db.compact().unwrap();
        assert_eq!(report.relocated_pages, 1);
        assert!(
            fs::read(path.join("seerdb.meta.3"))
                .unwrap()
                .starts_with(&META_DELTA_MAGIC)
        );
        assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_compaction_final_write_disk_full_reopens_old_root() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("compaction-final-disk-full.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();

        db.inject_final_write_disk_full();
        assert!(matches!(db.compact(), Err(Error::DiskFull)));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_wal_admission_rejects_before_blob_or_tree_mutation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal-admission.db");
        let value = vec![0xA5; 2_000];
        let record_bytes = WalRecord::put(b"key", &value).to_bytes().len() as u64;
        let exact_budget = record_bytes + WAL_COMMIT_RECORD_BYTES;
        let mut options = Options::for_test();
        options.max_wal_bytes = exact_budget - 1;

        let mut db = DB::open(&path, options).unwrap();
        let error = db.put(b"key", &value).unwrap_err();
        assert!(matches!(
            error,
            Error::Backpressure { required, available }
                if required == exact_budget && available == exact_budget - 1
        ));
        assert_eq!(db.get(b"key").unwrap(), None);
        assert_eq!(db.blob_stats().total_valid, 0);
        assert!(!path.join(WAL_FILE).exists());
        assert!(!db.durability_status().write_fenced);
        assert_eq!(db.metrics().unwrap().wal_admission_failures, 1);

        db.options.max_wal_bytes = exact_budget;
        db.put(b"key", &value).unwrap();
        assert!(path.join(WAL_FILE).is_file());
        assert!(!path.join(WAL_RESERVATION_FILE).exists());
        assert!(fs::metadata(path.join(WAL_FILE)).unwrap().len() < WAL_RESERVATION_SEGMENT_BYTES);
        assert!(path.join(BLOB_RESERVATION_FILE).is_file());
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(
            fs::metadata(path.join(BLOB_RESERVATION_FILE))
                .unwrap()
                .blocks()
                > 0,
            "blob reservation should own physical blocks on this platform"
        );
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(
            fs::metadata(path.join(WAL_FILE)).unwrap().blocks() > 0,
            "WAL file should own physical blocks on this platform"
        );
        assert_eq!(
            db.metrics().unwrap().wal_reserved_bytes,
            WAL_RESERVATION_SEGMENT_BYTES
        );
        db.flush().unwrap();
        assert!(!path.join(BLOB_RESERVATION_FILE).exists());
        assert!(!path.join(WAL_FILE).exists());
        assert_eq!(db.metrics().unwrap().wal_reserved_bytes, 0);
        drop(db);

        let reopened = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(value));
    }

    #[test]
    fn test_db_removes_legacy_wal_reservation_sidecar_on_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy-wal-reservation.db");
        let db = DB::open(&path, Options::default()).unwrap();
        drop(db);
        fs::write(path.join(WAL_RESERVATION_FILE), [0xA5; 4096]).unwrap();

        let db = DB::open(&path, Options::default()).unwrap();
        assert!(!path.join(WAL_RESERVATION_FILE).exists());
        assert_eq!(db.metrics().unwrap().wal_reserved_bytes, 0);
    }

    #[test]
    fn test_db_blob_admission_rejects_before_blob_or_tree_mutation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blob-admission.db");
        let value = vec![0x5A; 2_000];
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.inject_capacity_limit(0);

        assert!(matches!(db.put(b"key", &value), Err(Error::DiskFull)));
        assert_eq!(db.get(b"key").unwrap(), None);
        assert_eq!(db.blob_stats().total_valid, 0);
        assert_eq!(db.durability_status().pending_mutations, 0);
        assert!(!db.durability_status().write_fenced);

        drop(db);
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_db_reopen_reads_pmt_pages_on_demand() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        {
            let mut db = DB::open(&path, Options::for_test()).unwrap();
            for index in 0..500 {
                let key = format!("key-{index:06}");
                db.put(key.as_bytes(), b"value").unwrap();
            }
            db.flush().unwrap();
        }

        let mut db = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(db.engine.btree().node_count(), 0);
        assert_eq!(db.engine.buffer_stats().reads, 0);

        assert_eq!(db.get(b"key-000250").unwrap(), Some(b"value".to_vec()));
        assert!(db.engine.buffer_stats().reads > 0);
        assert_eq!(db.engine.btree().node_count(), 0);

        let range = db.range(b"key-000050", b"key-000450").unwrap();
        assert_eq!(range.len(), 400);
        assert_eq!(range.first().unwrap().0, b"key-000050");
        assert_eq!(range.last().unwrap().0, b"key-000449");
        assert_eq!(db.engine.btree().node_count(), 0);

        db.put(b"key-000250", b"updated").unwrap();
        assert!(db.engine.btree().node_count() > 0);
        db.flush().unwrap();
        assert_eq!(db.get(b"key-000250").unwrap(), Some(b"updated".to_vec()));
    }

    #[test]
    fn test_db_sparse_mutation_overlay_preserves_unloaded_ranges() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sparse-mutation.db");

        {
            let mut db = DB::open(&path, Options::for_test()).unwrap();
            for index in 0..500 {
                let key = format!("key-{index:06}");
                let value = format!("value-{index:06}");
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }

        let mut db = DB::open(&path, Options::for_test()).unwrap();
        let durable_page_count = db.engine.pmt().iter().count();
        db.put(b"key-000250", b"updated-250").unwrap();
        db.put(b"key-000450", b"updated-450").unwrap();
        assert!(db.engine.btree().node_count() < durable_page_count);
        assert_eq!(
            db.get(b"key-000250").unwrap(),
            Some(b"updated-250".to_vec())
        );
        assert_eq!(
            db.get(b"key-000450").unwrap(),
            Some(b"updated-450".to_vec())
        );

        let before_delete = db.range(b"key-000240", b"key-000460").unwrap();
        assert_eq!(before_delete.len(), 220);
        assert_eq!(
            before_delete
                .iter()
                .find(|(key, _)| key == b"key-000250")
                .map(|(_, value)| value.as_slice()),
            Some(b"updated-250".as_slice())
        );

        assert!(db.delete(b"key-000300").unwrap());
        let after_delete = db.range(b"key-000240", b"key-000460").unwrap();
        assert_eq!(after_delete.len(), 219);
        assert!(!after_delete.iter().any(|(key, _)| key == b"key-000300"));
        db.flush().unwrap();
        drop(db);

        let reopened = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(
            reopened.get(b"key-000250").unwrap(),
            Some(b"updated-250".to_vec())
        );
        assert_eq!(reopened.get(b"key-000300").unwrap(), None);
        assert_eq!(
            reopened.get(b"key-000450").unwrap(),
            Some(b"updated-450".to_vec())
        );
    }

    #[test]
    fn test_db_sparse_mutation_overlay_split_reopens() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sparse-split.db");

        {
            let mut db = DB::open(&path, Options::for_test()).unwrap();
            for index in 0..500 {
                let key = format!("key-{index:06}");
                db.put(key.as_bytes(), b"value").unwrap();
            }
            db.flush().unwrap();
        }

        let mut db = DB::open(&path, Options::for_test()).unwrap();
        for index in 0..120 {
            let key = format!("key-000250-new-{index:03}");
            db.put(key.as_bytes(), b"new-value").unwrap();
        }
        assert!(db.engine.btree().dirty_page_ids().len() > 1);
        assert_eq!(
            db.range(b"key-000250-new-000", b"key-000250-new-120")
                .unwrap()
                .len(),
            120
        );
        db.flush().unwrap();
        drop(db);

        let reopened = DB::open(&path, Options::for_test()).unwrap();
        let values = reopened
            .range(b"key-000250-new-000", b"key-000250-new-120")
            .unwrap();
        assert_eq!(values.len(), 120);
        assert_eq!(
            values[0],
            (b"key-000250-new-000".to_vec(), b"new-value".to_vec())
        );
        assert_eq!(values[119].0, b"key-000250-new-119");
    }

    #[test]
    fn test_db_sparse_deep_internal_split_does_not_require_unloaded_children() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sparse-deep-split.db");

        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            for index in 0..2_000 {
                let key = format!("key-{index:06}");
                db.put(key.as_bytes(), b"value").unwrap();
            }
            db.flush().unwrap();
        }

        let mut db = DB::open(&path, Options::default()).unwrap();
        for index in 0..600 {
            let key = format!("key-000800-new-{index:04}");
            db.put(key.as_bytes(), b"new-value").unwrap();
        }
        db.flush().unwrap();
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(
            reopened
                .range(b"key-000800-new-0000", b"key-000800-new-0600")
                .unwrap()
                .len(),
            600
        );
    }

    #[test]
    fn test_db_rejects_malformed_meta_container() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();
        }

        let checkpoint = path.join("seerdb.meta.1");
        let mut meta = fs::read(&checkpoint).unwrap();
        meta.push(0xA5);
        fs::write(&checkpoint, meta).unwrap();

        assert!(matches!(
            DB::open(&path, Options::default()),
            Err(Error::Corruption(message)) if message.contains("checksum")
        ));
    }

    #[test]
    fn test_db_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Write data and close.
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            let initial = db.durability_status();
            assert_eq!(initial.generation_id.get(), 0);
            assert_eq!(initial.commit_id.get(), 0);
            db.put(b"key1", b"value1").unwrap();
            assert_eq!(db.durability_status().pending_mutations, 1);
            db.flush().unwrap();
            let published = db.durability_status();
            assert_eq!(published.generation_id.get(), 1);
            assert_eq!(published.commit_id.get(), 1);
            assert_eq!(published.pending_mutations, 0);
            db.put(b"key2", b"value2").unwrap();
            db.put(b"key3", b"value3").unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        // Reopen and verify data persisted.
        {
            let db = DB::open(&path, Options::default()).unwrap();
            let status = db.durability_status();
            assert_eq!(status.generation_id.get(), 2);
            assert_eq!(status.commit_id.get(), 2);
            assert_eq!(status.pending_mutations, 0);
            assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
            assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
            assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        }
    }

    #[test]
    fn test_db_rejects_corrupt_page_checksum() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();
        }

        let data_path = path.join(DATA_FILE);
        let mut data = fs::read(&data_path).unwrap();
        assert!(data.len() >= crate::btree::PAGE_SIZE);
        data[crate::btree::PAGE_SIZE - 1] ^= 0x01;
        fs::write(&data_path, data).unwrap();

        let db = DB::open(&path, Options::default()).unwrap();
        let result = db.get(b"key");
        assert!(matches!(
            result,
            Err(Error::Corruption(message)) if message.contains("checksum mismatch")
        ));
        assert!(matches!(
            DB::check(&path, Options::default()),
            Err(Error::Check {
                kind: CheckFailureKind::DataPage,
                ..
            })
        ));
    }

    #[test]
    fn test_db_discards_uncommitted_wal_suffix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Write data (WAL is written to disk on each put).
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key1", b"value1").unwrap();
            db.put(b"key2", b"value2").unwrap();
            db.put(b"key3", b"value3").unwrap();
            // Don't flush — simulate crash.
            // WAL should be on disk.
        }

        // Verify WAL exists.
        assert!(path.join(WAL_FILE).exists(), "WAL should exist after put");

        // Reopen and verify uncommitted mutations are not visible.
        {
            let db = DB::open(&path, Options::default()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), None);
            assert_eq!(db.get(b"key2").unwrap(), None);
            assert_eq!(db.get(b"key3").unwrap(), None);
        }

        // The uncommitted WAL suffix can be discarded after reopen.
        assert!(
            !path.join(WAL_FILE).exists(),
            "WAL should be deleted after recovery"
        );
    }

    #[test]
    fn test_db_process_crash_recovery() {
        if let Some(path) = std::env::var_os("SEERDB_CRASH_CHILD_PATH") {
            let path = PathBuf::from(path);
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"published", b"value-before-crash").unwrap();
            db.flush().unwrap();
            db.put(b"unpublished", b"value-after-wal-only").unwrap();

            // Exit without running Rust destructors. This leaves the WAL
            // mutation on disk while the manifest still names the prior
            // published generation, matching an abrupt process termination.
            std::process::exit(137);
        }

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("db::tests::test_db_process_crash_recovery")
            .arg("--nocapture")
            .env("SEERDB_CRASH_CHILD_PATH", &path)
            .status()
            .unwrap();
        assert!(!status.success(), "crash child unexpectedly exited cleanly");

        let db = DB::open(&path, Options::default()).unwrap();
        assert_eq!(
            db.get(b"published").unwrap(),
            Some(b"value-before-crash".to_vec())
        );
        assert_eq!(db.get(b"unpublished").unwrap(), None);
        assert!(!path.join(WAL_FILE).exists());
    }

    #[test]
    fn test_db_randomized_publication_fault_matrix() {
        fn next(seed: &mut u64) -> u64 {
            *seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            *seed
        }

        fn assert_model(db: &DB, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
            for key_id in 0..16 {
                let key = format!("key-{key_id:02}");
                assert_eq!(
                    db.get(key.as_bytes()).unwrap(),
                    model.get(key.as_bytes()).cloned()
                );
            }
        }

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        let mut committed = BTreeMap::new();
        let mut seed = 0x5EED_CAFE_u64;

        for round in 0..32 {
            let mut candidate = committed.clone();
            let operation_count = (next(&mut seed) % 4 + 1) as usize;
            for operation in 0..operation_count {
                let key_id = next(&mut seed) % 16;
                let key = format!("key-{key_id:02}");
                let value = format!("value-{round:02}-{operation:02}-{key_id:02}");
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                candidate.insert(key.into_bytes(), value.into_bytes());
            }

            let fault = next(&mut seed) % 4;
            match fault {
                1 => db.engine.inject_sync_failure(),
                2 => db.engine.inject_write_failure(),
                3 => inject_atomic_rename_failure(),
                _ => {}
            }

            let result = db.flush();
            if fault == 0 {
                result.unwrap();
                committed = candidate;
                assert_model(&db, &committed);
            } else {
                assert!(result.is_err(), "fault {fault} did not fail publication");
                drop(db);
                db = DB::open(&path, Options::default()).unwrap();
                assert_model(&db, &committed);
            }
        }

        db.close().unwrap();
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_model(&reopened, &committed);
    }

    #[test]
    fn test_db_recovers_committed_wal_prefix_with_torn_suffix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let records = vec![
            WalRecord::put(b"key1", b"value1"),
            WalRecord::put(b"key2", b"value2"),
            WalRecord::put(b"key3", b"value3"),
        ];
        let references: Vec<_> = records.iter().collect();
        let commit = CommitRecord {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            root_page_id: 0,
            mutation_count: records.len() as u64,
            digest: digest_records(&references),
        };
        let mut wal_bytes = Vec::new();
        for record in &records {
            wal_bytes.extend_from_slice(&record.to_bytes());
        }
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        wal_bytes.extend_from_slice(&[0xA5, 0x5A, 0x01]);
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let db = DB::open(&path, Options::default()).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert!(!path.join(WAL_FILE).exists());
        assert!(path.join(MANIFEST_FILE).exists());
    }

    #[test]
    fn test_db_reopen_accepts_every_wal_truncation_prefix() {
        let records = vec![
            WalRecord::put(b"key1", b"value1"),
            WalRecord::put(b"key2", b"value2"),
            WalRecord::put(b"key3", b"value3"),
        ];
        let references: Vec<_> = records.iter().collect();
        let commit = CommitRecord {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            root_page_id: 0,
            mutation_count: records.len() as u64,
            digest: digest_records(&references),
        };
        let mut committed_wal = Vec::new();
        for record in &records {
            committed_wal.extend_from_slice(&record.to_bytes());
        }
        committed_wal.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        let committed_len = committed_wal.len();
        committed_wal.extend_from_slice(&[0xA5, 0x5A, 0x01]);

        for cut in 0..=committed_wal.len() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("test.db");
            fs::create_dir_all(&path).unwrap();
            fs::write(path.join(WAL_FILE), &committed_wal[..cut]).unwrap();

            let db = DB::open(&path, Options::default()).unwrap_or_else(|error| {
                panic!("WAL prefix at byte {cut} failed to reopen: {error:?}")
            });
            let committed = cut >= committed_len;
            assert_eq!(
                db.get(b"key1").unwrap(),
                committed.then(|| b"value1".to_vec()),
                "cut={cut}"
            );
            assert_eq!(
                db.get(b"key2").unwrap(),
                committed.then(|| b"value2".to_vec()),
                "cut={cut}"
            );
            assert_eq!(
                db.get(b"key3").unwrap(),
                committed.then(|| b"value3".to_vec()),
                "cut={cut}"
            );
        }
    }

    #[test]
    fn test_db_rejects_wal_commit_digest_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let record = WalRecord::put(b"key", b"value");
        let references = vec![&record];
        let commit = CommitRecord {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            root_page_id: 0,
            mutation_count: 1,
            digest: digest_records(&references) ^ 1,
        };
        let mut wal_bytes = record.to_bytes();
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let result = DB::open(&path, Options::default());
        assert!(matches!(
            result,
            Err(Error::Corruption(message)) if message.contains("WAL commit")
        ));
    }

    #[test]
    fn test_db_rejects_when_both_manifest_slots_are_corrupt() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();
        }

        let manifest_path = path.join(MANIFEST_FILE);
        let mut file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
        for slot in 0..2 {
            file.seek(SeekFrom::Start((slot * MANIFEST_SLOT_SIZE) as u64))
                .unwrap();
            file.write_all(&[0xA5; MANIFEST_SLOT_SIZE]).unwrap();
        }
        file.sync_all().unwrap();

        let result = DB::open(&path, Options::default());
        assert!(matches!(result, Err(Error::Corruption(_))));
    }

    #[test]
    fn test_db_fences_writer_after_sync_failure() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.engine.inject_sync_failure();

        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert!(matches!(
            db.put(b"another", b"value"),
            Err(Error::NeedsRecovery(_))
        ));
        assert!(matches!(
            db.get(b"key"),
            Err(Error::NeedsRecovery(message)) if message.contains("reads fenced")
        ));
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_db_fences_writer_after_page_write_failure() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.engine.inject_write_failure();

        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert!(matches!(
            db.put(b"another", b"value"),
            Err(Error::NeedsRecovery(_))
        ));
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_db_fences_writer_after_disk_full() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.engine.inject_disk_full();

        assert!(matches!(db.flush(), Err(Error::DiskFull)));
        assert!(matches!(
            db.put(b"another", b"value"),
            Err(Error::NeedsRecovery(_))
        ));
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_db_capacity_preflight_is_retryable_without_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("retryable-capacity.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();

        let capacity = fs::metadata(path.join(DATA_FILE)).unwrap().len();
        db.inject_capacity_limit(capacity);
        db.put(b"key", b"value-2").unwrap();

        assert!(matches!(db.flush(), Err(Error::CapacityPreflight)));
        assert!(!db.durability_status().write_fenced);
        assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));

        db.inject_capacity_limit(u64::MAX);
        db.flush().unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value-2".to_vec()));

        drop(db);
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    }

    #[test]
    fn test_db_discards_wal_after_atomic_rename_failure() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        inject_atomic_rename_failure();

        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert!(matches!(
            db.put(b"another", b"value"),
            Err(Error::NeedsRecovery(_))
        ));
        drop(db);

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
        assert!(!path.join(WAL_FILE).exists());
    }

    #[test]
    fn test_db_retains_manifest_fallback_before_reusing_pages() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("manifest-retention.db");
        let mut db = DB::open(&path, Options::default()).unwrap();

        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();

        // The next generation can reuse a page from before the current
        // generation, but only after both manifest slots have been fenced to
        // the current root. Fail before the new manifest is published.
        db.put(b"key", b"value-3").unwrap();
        inject_atomic_rename_failure();
        assert!(matches!(db.flush(), Err(Error::Io(_))));
        drop(db);

        // Simulate loss of the newest manifest slot. The mirrored fallback
        // must still name value-2 even though the failed generation reused an
        // older physical page.
        let manifest_path = path.join(MANIFEST_FILE);
        let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
        manifest_file
            .seek(SeekFrom::Start(MANIFEST_SLOT_SIZE as u64))
            .unwrap();
        manifest_file
            .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
            .unwrap();
        manifest_file.sync_all().unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
    }

    #[test]
    fn test_db_prune_history_preserves_inactive_manifest_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("prune-fallback.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();

        let first_checkpoint = path.join("seerdb.meta.1");
        assert!(first_checkpoint.is_file());
        db.prune_history().unwrap();
        assert!(first_checkpoint.is_file());
        db.close().unwrap();

        // The newest slot is corrupt, so reopen must use the independently
        // valid older slot whose checkpoint pruning was required to preserve.
        let manifest_path = path.join(MANIFEST_FILE);
        let manifest_file = OpenOptions::new().read(true).open(&manifest_path).unwrap();
        let mut newest = None;
        for slot in 0..2 {
            let mut bytes = [0; MANIFEST_SLOT_SIZE];
            read_exact_at(
                &manifest_file,
                (slot * MANIFEST_SLOT_SIZE) as u64,
                &mut bytes,
            )
            .unwrap();
            if let Some(manifest) = Manifest::from_bytes(&bytes).unwrap()
                && newest.is_none_or(|(_, current)| manifest.is_newer_than(current))
            {
                newest = Some((slot, manifest));
            }
        }
        let newest_slot = newest.expect("published database has a newest manifest").0;
        drop(manifest_file);
        let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
        manifest_file
            .seek(SeekFrom::Start((newest_slot * MANIFEST_SLOT_SIZE) as u64))
            .unwrap();
        manifest_file
            .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
            .unwrap();
        manifest_file.sync_all().unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-1".to_vec()));
    }

    #[test]
    fn test_db_history_prune_directory_failure_reopens_and_retries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("prune-directory-failure.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value-0").unwrap();
        db.flush().unwrap();
        for revision in 1..=MAX_META_DELTA_CHAIN + 1 {
            db.put(b"key", format!("value-{revision}").as_bytes())
                .unwrap();
            db.flush().unwrap();
        }
        db.put(b"key", b"value-final").unwrap();
        db.flush().unwrap();
        let obsolete_checkpoint = path.join("seerdb.meta.1");
        assert!(obsolete_checkpoint.is_file());

        db.inject_history_prune_directory_sync_failure();
        assert!(matches!(db.prune_history(), Err(Error::Io(_))));
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-final".to_vec()));
        reopened.verify().unwrap();
        let report = reopened.prune_history().unwrap();
        assert_eq!(report.removed_checkpoints, 0);
        reopened.close().unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-final".to_vec()));
        assert!(!obsolete_checkpoint.is_file());
    }

    #[test]
    fn test_db_gc_mirrors_manifest_before_removing_dead_blob_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("gc-fallback.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"blob", &vec![0xA5; 2_000]).unwrap();
        db.flush().unwrap();
        db.delete(b"blob").unwrap();
        db.flush().unwrap();

        assert_eq!(db.gc().unwrap(), 1);
        db.close().unwrap();

        // GC is a maintenance mutation of the blob artifact without a new
        // logical commit. Losing the newest slot must still reopen the
        // manifest whose blob image is present, rather than a stale root
        // whose record was just removed.
        let manifest_path = path.join(MANIFEST_FILE);
        let mut manifest_file = OpenOptions::new().write(true).open(&manifest_path).unwrap();
        manifest_file
            .seek(SeekFrom::Start(MANIFEST_SLOT_SIZE as u64))
            .unwrap();
        manifest_file
            .write_all(&[0xA5; MANIFEST_SLOT_SIZE])
            .unwrap();
        manifest_file.sync_all().unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"blob").unwrap(), None);
    }

    #[test]
    fn test_db_mirror_manifest_sync_failure_precedes_page_reuse() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("manifest-mirror-fault.db");
        let mut db = DB::open(&path, Options::default()).unwrap();

        db.put(b"key", b"value-1").unwrap();
        db.flush().unwrap();
        db.put(b"key", b"value-2").unwrap();
        db.flush().unwrap();
        let data_bytes_before = fs::metadata(path.join(DATA_FILE)).unwrap().len();

        db.put(b"key", b"value-3").unwrap();
        db.inject_manifest_mirror_sync_failure();
        assert!(matches!(db.flush(), Err(Error::Io(_))));
        assert_eq!(
            fs::metadata(path.join(DATA_FILE)).unwrap().len(),
            data_bytes_before
        );
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value-2".to_vec()));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_append_only_publication_skips_manifest_mirror() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("append-only-no-mirror.db");
        let mut db = DB::open(&path, Options::default()).unwrap();

        db.put(b"key", b"value").unwrap();
        db.inject_manifest_mirror_sync_failure();
        db.flush().unwrap();

        assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
        db.verify().unwrap();
    }

    #[test]
    fn test_db_blob_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create a large value (>1KB threshold).
        let large_value = vec![0xAB; 2000];

        // Write large value and close.
        {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key1", &large_value).unwrap();
            db.put(b"key2", b"small").unwrap();
            db.flush().unwrap();
            let replacement = vec![0xCD; 3_000];
            db.put(b"key1", &replacement).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), Some(replacement.clone()));
            db.flush().unwrap();
            assert!(db.blob_stats().total_deleted > 0);
            assert_eq!(db.get(b"key1").unwrap(), Some(replacement));
            assert_eq!(
                db.range(b"key1", b"key3").unwrap(),
                vec![
                    (b"key1".to_vec(), vec![0xCD; 3_000]),
                    (b"key2".to_vec(), b"small".to_vec()),
                ]
            );
            db.close().unwrap();
        }

        // Verify blob file exists.
        assert!(path.join(BLOB_FILE).exists(), "blob file should exist");

        // Reopen and verify blob data persisted.
        {
            let db = DB::open(&path, Options::default()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), Some(vec![0xCD; 3_000]));
            assert_eq!(db.get(b"key2").unwrap(), Some(b"small".to_vec()));
            assert_eq!(
                db.range(b"key1", b"key3").unwrap(),
                vec![
                    (b"key1".to_vec(), vec![0xCD; 3_000]),
                    (b"key2".to_vec(), b"small".to_vec()),
                ]
            );
        }
    }

    #[test]
    fn test_db_recovers_committed_blob_upsert() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blob-recovery.db");
        let initial = vec![0x11; 2_000];
        let replacement = vec![0x22; 3_000];

        let (commit_id, generation_id, root_page_id) = {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", &initial).unwrap();
            db.flush().unwrap();

            (
                db.commit_id.get(),
                db.generation_id.get(),
                db.engine.btree().root_id() as u64,
            )
        };

        let record = WalRecord::put(b"key", &replacement);
        let commit = CommitRecord {
            commit_id: CommitId::new(commit_id + 1),
            generation_id: GenerationId::new(generation_id + 1),
            root_page_id,
            mutation_count: 1,
            digest: digest_records(&[&record]),
        };
        let mut wal_bytes = record.to_bytes();
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(replacement));
    }

    #[test]
    fn test_db_discards_wal_commit_already_published_by_manifest() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("stale-authoritative-wal.db");

        let (commit_id, generation_id, root_page_id) = {
            let mut db = DB::open(&path, Options::default()).unwrap();
            db.put(b"key", b"value").unwrap();
            db.flush().unwrap();

            (
                db.commit_id,
                db.generation_id,
                db.engine.btree().root_id() as u64,
            )
        };

        // Model the crash window where manifest publication succeeded but WAL
        // cleanup did not. Replaying this commit would publish the same
        // logical state under a new generation.
        let record = WalRecord::put(b"key", b"value");
        let commit = CommitRecord {
            commit_id,
            generation_id,
            root_page_id,
            mutation_count: 1,
            digest: digest_records(&[&record]),
        };
        let mut wal_bytes = record.to_bytes();
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.commit_id, commit_id);
        assert_eq!(reopened.generation_id, generation_id);
        assert_eq!(reopened.metrics().unwrap().storage.generation_flushes, 0);
        assert!(!path.join(WAL_FILE).exists());
    }

    #[test]
    fn test_db_recovers_committed_large_blob_value() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large-blob-recovery.db");
        let value = vec![0x7B; 70_000];
        let record = WalRecord::put(b"large-key", &value);
        let commit = CommitRecord {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            root_page_id: 0,
            mutation_count: 1,
            digest: digest_records(&[&record]),
        };
        let mut wal_bytes = record.to_bytes();
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"large-key").unwrap(), Some(value));
    }

    #[test]
    fn test_db_replays_legacy_wal_put_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy-wal.db");
        let mut payload = Vec::new();
        payload.extend_from_slice(&(3u16).to_le_bytes());
        payload.extend_from_slice(b"key");
        payload.extend_from_slice(&(5u16).to_le_bytes());
        payload.extend_from_slice(b"value");
        let record = WalRecord::new(RecordType::Put, payload);
        let commit = CommitRecord {
            commit_id: CommitId::new(1),
            generation_id: GenerationId::new(1),
            root_page_id: 0,
            mutation_count: 1,
            digest: digest_records(&[&record]),
        };
        let mut wal_bytes = record.to_bytes();
        wal_bytes.extend_from_slice(&WalRecord::commit(commit).to_bytes());
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(WAL_FILE), wal_bytes).unwrap();

        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_db_transaction() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = DB::open(&path, Options::default()).unwrap();

        // Begin a transaction.
        let mut txn = db.begin_transaction();
        assert!(txn.is_active());
        assert_eq!(txn.id(), 1);

        // Commit the transaction.
        db.commit_transaction(&mut txn);
        assert!(!txn.is_active());
        assert_eq!(db.latest_committed_txn(), 1);

        // Begin another transaction.
        let mut txn2 = db.begin_transaction();
        assert_eq!(txn2.id(), 2);
        assert_eq!(txn2.snapshot_id(), 1); // Can see txn 1

        // Abort the transaction.
        db.abort_transaction(&mut txn2);
        assert!(!txn2.is_active());
        assert_eq!(db.latest_committed_txn(), 1); // Still 1
    }

    #[test]
    fn test_db_vacuum_step_is_bounded_and_crash_safe() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bounded-vacuum.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        for index in 0..12 {
            let key = format!("key-{index:02}");
            db.put(key.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();
        let before = db.durability_status();

        let progress = db.vacuum_step(3).unwrap();
        assert!(!progress.complete);
        assert_eq!(progress.scanned_entries, 3);
        assert_eq!(progress.live_entries, 3);
        assert_eq!(progress.logical_pages_after, None);
        assert_eq!(db.durability_status(), before);
        assert!(matches!(
            db.put(b"blocked", b"write"),
            Err(Error::MaintenanceInProgress("logical vacuum"))
        ));

        // Dropping an incomplete candidate must not publish or fence the old
        // generation. The drop path also exercises the close-time retry.
        drop(db);
        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key-00").unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.durability_status(), before);

        let mut completed = false;
        while !completed {
            let progress = reopened.vacuum_step(2).unwrap();
            completed = progress.complete;
            if completed {
                assert_eq!(progress.live_entries, 12);
                assert_eq!(progress.logical_pages_after, Some(1));
            } else {
                assert_eq!(progress.logical_pages_after, None);
            }
        }
        assert_eq!(reopened.range(b"key-00", b"key-99").unwrap().len(), 12);
        reopened.close().unwrap();

        let verified = DB::open(&path, Options::default()).unwrap();
        assert_eq!(verified.range(b"key-00", b"key-99").unwrap().len(), 12);
    }

    #[test]
    fn test_db_vacuum_can_be_cancelled_without_publication() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cancel-vacuum.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        for index in 0..4 {
            let key = format!("key-{index}");
            db.put(key.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();
        let before = db.durability_status();

        assert!(!db.vacuum_step(1).unwrap().complete);
        assert!(db.cancel_vacuum().unwrap());
        assert!(!db.cancel_vacuum().unwrap());
        assert_eq!(db.durability_status(), before);
        db.put(b"after-cancel", b"value").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"after-cancel").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_db_vacuum_final_write_disk_full_reopens_old_root() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("vacuum-final-disk-full.db");
        let mut db = DB::open(&path, Options::default()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();

        db.inject_final_write_disk_full();
        assert!(matches!(db.vacuum(), Err(Error::DiskFull)));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_concurrent_transactions() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = DB::open(&path, Options::default()).unwrap();
        let db = std::sync::Arc::new(db);
        let mut handles = vec![];

        // Spawn multiple threads that create transactions.
        for _ in 0..10 {
            let db = std::sync::Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                let mut txn = db.begin_transaction();
                // Simulate some work.
                std::thread::yield_now();
                db.commit_transaction(&mut txn);
                txn.id()
            }));
        }

        // Wait for all threads to complete.
        let mut ids = vec![];
        for handle in handles {
            ids.push(handle.join().unwrap());
        }

        // All transactions should have unique IDs.
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 10);

        // Latest committed should be the max ID.
        assert_eq!(db.latest_committed_txn(), 10);
    }

    #[test]
    fn test_db_gc() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut db = DB::open(&path, Options::default()).unwrap();

        // Write some large values (>1KB threshold).
        let large_value = vec![0xAB; 2000];
        db.put(b"key1", &large_value).unwrap();
        db.put(b"key2", &large_value).unwrap();
        db.put(b"key3", &large_value).unwrap();
        db.flush().unwrap();

        // Check initial stats.
        let stats = db.blob_stats();
        assert_eq!(stats.total_valid, 3);
        assert_eq!(stats.total_deleted, 0);
        assert_eq!(stats.files_needing_gc, 0);

        // Delete some entries.
        db.delete(b"key1").unwrap();
        db.delete(b"key2").unwrap();
        db.flush().unwrap();

        // Check stats after delete.
        let stats = db.blob_stats();
        assert_eq!(stats.total_valid, 1);
        assert_eq!(stats.total_deleted, 2);

        // Run GC.
        let reclaimed = db.gc().unwrap();
        assert_eq!(reclaimed, 3);
        assert_eq!(db.get(b"key3").unwrap(), Some(large_value));

        // Check stats after GC.
        let stats = db.blob_stats();
        assert_eq!(stats.total_valid, 1);
        assert_eq!(stats.total_deleted, 0);
        assert_eq!(stats.files_needing_gc, 0);

        db.delete(b"key3").unwrap();
        db.flush().unwrap();
        assert_eq!(db.gc().unwrap(), 1);
        assert_eq!(db.blob_stats().files_needing_gc, 0);

        drop(db);
        let reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key1").unwrap(), None);
        assert_eq!(reopened.get(b"key2").unwrap(), None);
        assert_eq!(reopened.get(b"key3").unwrap(), None);
    }

    #[test]
    fn test_db_gc_admission_rejects_before_catalog_reclamation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("gc-admission.db");
        let value = vec![0xAB; 2_000];
        let mut db = DB::open(&path, Options::default()).unwrap();

        db.put(b"key", &value).unwrap();
        db.flush().unwrap();
        db.delete(b"key").unwrap();
        db.flush().unwrap();

        let before_bytes = fs::metadata(path.join(BLOB_FILE)).unwrap().len();
        let before_stats = db.blob_stats();
        assert_eq!(before_stats.total_valid, 0);
        assert_eq!(before_stats.total_deleted, 1);
        assert_eq!(before_stats.files_needing_gc, 1);

        db.inject_capacity_limit(0);
        assert!(matches!(db.gc(), Err(Error::DiskFull)));
        assert_eq!(
            fs::metadata(path.join(BLOB_FILE)).unwrap().len(),
            before_bytes
        );
        let after_failed_stats = db.blob_stats();
        assert_eq!(after_failed_stats.total_valid, before_stats.total_valid);
        assert_eq!(after_failed_stats.total_deleted, before_stats.total_deleted);
        assert_eq!(
            after_failed_stats.files_needing_gc,
            before_stats.files_needing_gc
        );
        assert!(!db.durability_status().write_fenced);

        db.inject_capacity_limit(u64::MAX);
        assert_eq!(db.gc().unwrap(), 1);
        assert_eq!(db.blob_stats().files_needing_gc, 0);
    }

    #[test]
    fn test_db_mixed_gc_capacity_refusal_is_retryable_before_candidate_install() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mixed-gc-admission.db");
        let value = vec![0xCD; 2_000];
        let mut db = DB::open(&path, Options::default()).unwrap();

        for key in [b"live".as_slice(), b"dead-1", b"dead-2"] {
            db.put(key, &value).unwrap();
        }
        db.flush().unwrap();
        db.delete(b"dead-1").unwrap();
        db.delete(b"dead-2").unwrap();
        db.flush().unwrap();
        let before = db.blob_stats();
        assert_eq!(before.total_valid, 1);
        assert_eq!(before.total_deleted, 2);
        assert_eq!(before.files_needing_gc, 1);

        let data_capacity = fs::metadata(path.join(DATA_FILE)).unwrap().len();
        db.inject_capacity_limit(data_capacity);
        assert!(matches!(db.gc(), Err(Error::CapacityPreflight)));
        assert!(!db.durability_status().write_fenced);
        assert_eq!(db.blob_stats().total_valid, before.total_valid);
        assert_eq!(db.blob_stats().total_deleted, before.total_deleted);
        assert_eq!(db.get(b"live").unwrap(), Some(value.clone()));

        db.inject_capacity_limit(u64::MAX);
        assert!(db.gc().unwrap() > 0);
        assert_eq!(db.get(b"live").unwrap(), Some(value));
        db.verify().unwrap();
    }

    #[test]
    fn test_db_mixed_gc_final_write_disk_full_reopens_old_root() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mixed-gc-final-disk-full.db");
        let value = vec![0xEF; 2_000];
        let mut db = DB::open(&path, Options::default()).unwrap();
        for key in [b"live".as_slice(), b"dead-1", b"dead-2"] {
            db.put(key, &value).unwrap();
        }
        db.flush().unwrap();
        db.delete(b"dead-1").unwrap();
        db.delete(b"dead-2").unwrap();
        db.flush().unwrap();

        db.inject_final_write_disk_full();
        assert!(matches!(db.gc(), Err(Error::DiskFull)));
        assert!(db.durability_status().write_fenced);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"live").unwrap(), Some(value.clone()));
        assert_eq!(reopened.get(b"dead-1").unwrap(), None);
        assert_eq!(reopened.get(b"dead-2").unwrap(), None);
        assert!(reopened.blob_stats().files_needing_gc > 0);
        assert!(reopened.gc().unwrap() > 0);
        reopened.verify().unwrap();
    }

    #[test]
    fn segmented_catalog_consolidation_bound_is_explicit() {
        let mut blobs = BlobManager::with_threshold_and_mode(1, true);
        let mut pointers = Vec::with_capacity(MAX_SEGMENTED_CATALOG_DELETED_ENTRIES + 1);
        for index in 0..=MAX_SEGMENTED_CATALOG_DELETED_ENTRIES {
            pointers.push(blobs.append(&index.to_le_bytes(), vec![index as u8; 2]));
        }

        for pointer in pointers.iter().take(MAX_SEGMENTED_CATALOG_DELETED_ENTRIES) {
            assert!(blobs.mark_deleted(pointer));
        }
        assert!(!segmented_catalog_needs_consolidation(&blobs));
        assert!(blobs.mark_deleted(&pointers[MAX_SEGMENTED_CATALOG_DELETED_ENTRIES]));
        assert!(segmented_catalog_needs_consolidation(&blobs));
    }

    #[test]
    fn segmented_catalog_consolidation_runs_as_explicit_maintenance() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-catalog-consolidation.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 1,
            ..Options::default()
        };
        let mut db = DB::open(&path, options).unwrap();
        db.blobs.set_segment_target_size_for_test(1_250);

        let groups = (MAX_SEGMENTED_CATALOG_DELETED_ENTRIES / 4) + 1;
        let total = groups * 10;
        let puts = (0..total)
            .map(|index| BatchMutation::Put {
                key: index.to_le_bytes().to_vec(),
                value: vec![index as u8; 100],
            })
            .collect::<Vec<_>>();
        db.commit_batch(&puts).unwrap();

        let deletes = (0..total)
            .filter(|index| index % 10 < 4)
            .map(|index| BatchMutation::Delete {
                key: index.to_le_bytes().to_vec(),
            })
            .collect::<Vec<_>>();
        db.commit_batch(&deletes).unwrap();

        let before = db.blob_stats();
        assert_eq!(before.files_needing_gc, 0);
        assert_eq!(before.total_deleted, deletes.len());
        assert!(before.catalog_needs_consolidation);
        assert!(db.gc().unwrap() > 0);

        let after = db.blob_stats();
        assert_eq!(after.total_deleted, 0);
        assert!(!after.catalog_needs_consolidation);
        assert_eq!(db.get(&0usize.to_le_bytes()).unwrap(), None);
        assert_eq!(db.get(&4usize.to_le_bytes()).unwrap(), Some(vec![4; 100]));
        db.verify().unwrap();
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(&0usize.to_le_bytes()).unwrap(), None);
        assert_eq!(
            reopened.get(&4usize.to_le_bytes()).unwrap(),
            Some(vec![4; 100])
        );
        assert!(!reopened.blob_stats().catalog_needs_consolidation);
        reopened.verify().unwrap();
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 8,
            max_shrink_iters: 1_000,
            .. ProptestConfig::default()
        })]

        #[test]
        fn segmented_rollover_preserves_catalog_and_records(
            target_delta in 0u16..768,
            values in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 1..65),
                1..17
            )
        ) {
            let target = 256 + u64::from(target_delta);
            let dir = tempdir().unwrap();
            let path = dir.path().join("segmented-rollover-property.db");
            let options = Options {
                blob_storage: BlobStorageMode::Segmented,
                blob_threshold: 0,
                ..Options::for_test()
            };
            let mut db = DB::create(&path, options).unwrap();
            db.blobs.set_segment_target_size_for_test(target);

            let mut mutations = Vec::with_capacity(values.len() + 2);
            for (index, value) in values.into_iter().enumerate() {
                mutations.push(BatchMutation::Put {
                    key: format!("rollover-{index:04}").into_bytes(),
                    value,
                });
            }

            // Two records that each fit in one segment but cannot fit
            // together guarantee that the generated run exercises rollover.
            let forced_value = vec![0xD7; target as usize / 2];
            mutations.push(BatchMutation::Put {
                key: b"rollover-forced-a".to_vec(),
                value: forced_value.clone(),
            });
            mutations.push(BatchMutation::Put {
                key: b"rollover-forced-b".to_vec(),
                value: forced_value,
            });

            let expected = mutations
                .iter()
                .filter_map(|mutation| match mutation {
                    BatchMutation::Put { key, value } => Some((key.clone(), value.clone())),
                    BatchMutation::Delete { .. } => None,
                })
                .collect::<BTreeMap<_, _>>();
            db.commit_batch(&mutations).unwrap();
            let segment_ids = db.blobs.segment_file_ids();
            assert!(segment_ids.len() >= 2);
            for file_id in &segment_ids {
                assert!(
                    db.blobs.segment_bytes(*file_id).unwrap().len() <= target as usize,
                    "segment {file_id} exceeded target {target}"
                );
            }

            let deletes = expected
                .keys()
                .enumerate()
                .filter(|(index, _)| index % 4 == 0)
                .map(|(_, key)| BatchMutation::Delete { key: key.clone() })
                .collect::<Vec<_>>();
            db.commit_batch(&deletes).unwrap();
            let mut expected = expected;
            for mutation in &deletes {
                if let BatchMutation::Delete { key } = mutation {
                    expected.remove(key);
                }
            }
            db.verify().unwrap();
            assert_eq!(db.blob_stats().total_deleted, deletes.len());
            drop(db);

            let mut reopened = DB::open(&path, Options::for_test()).unwrap();
            for (key, value) in &expected {
                assert_eq!(reopened.get(key).unwrap(), Some(value.clone()));
            }
            assert_eq!(reopened.blob_stats().total_deleted, deletes.len());
            reopened.verify().unwrap();
        }
    }

    #[test]
    fn test_db_segmented_blob_layout_reopens_and_verifies() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let value = vec![0xA7; 2_000];

        let mut db = DB::create(&path, options.clone()).unwrap();
        db.put(b"key", &value).unwrap();
        db.close().unwrap();
        assert!(path.join(BLOB_FILE).is_file());
        assert!(blob_segment_path(&path, 1).is_file());

        let catalog = fs::read(path.join(BLOB_FILE)).unwrap();
        assert!(BlobManager::is_segment_catalog(&catalog));
        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(value.clone()));
        reopened.verify().unwrap();

        let retained = reopened.retain_current().unwrap();
        assert_eq!(retained.get(b"key").unwrap(), Some(value.clone()));
        let replacement = vec![0xB8; 2_100];
        reopened.put(b"key", &replacement).unwrap();
        reopened.close().unwrap();
        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(replacement));
        reopened.verify().unwrap();

        let archive = dir.path().join("segmented-archive");
        reopened.snapshot(&archive).unwrap();
        reopened.close().unwrap();
        let mut archived = DB::open(&archive, Options::default()).unwrap();
        assert_eq!(archived.get(b"key").unwrap(), Some(vec![0xB8; 2_100]));
        archived.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_blob_rewrite_failure_restores_catalog() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-gc.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let value = vec![0xC1; 2_000];
        let mut db = DB::create(&path, options).unwrap();
        for key in [b"live".as_slice(), b"dead-1", b"dead-2"] {
            db.put(key, &value).unwrap();
        }
        db.flush().unwrap();
        db.delete(b"dead-1").unwrap();
        db.delete(b"dead-2").unwrap();
        db.flush().unwrap();

        db.inject_after_blob_rewrite_image_failure();
        assert!(db.gc().is_err());
        assert!(db.durability_status().write_fenced);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"live").unwrap(), Some(value.clone()));
        assert_eq!(reopened.get(b"dead-1").unwrap(), None);
        assert!(reopened.blob_stats().files_needing_gc > 0);
        assert!(reopened.gc().unwrap() > 0);
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_gc_prunes_unreferenced_segments() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-gc-prune.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let value = vec![0xD2; 2_000];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"live", &value).unwrap();
        db.put(b"dead-1", &value).unwrap();
        db.put(b"dead-2", &value).unwrap();
        db.flush().unwrap();
        let old_segment = blob_segment_path(&path, 1);
        assert!(old_segment.is_file());

        db.delete(b"dead-1").unwrap();
        db.delete(b"dead-2").unwrap();
        db.flush().unwrap();
        assert!(db.gc().unwrap() > 0);
        assert!(blob_segment_path(&path, 2).is_file());
        assert!(!old_segment.exists());
        assert_eq!(db.get(b"live").unwrap(), Some(value.clone()));
        db.verify().unwrap();
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert!(!old_segment.exists());
        assert_eq!(reopened.get(b"live").unwrap(), Some(value));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_append_failure_ignores_orphan_suffix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-append-failure.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xE3; 2_000];
        let pending = vec![0xE4; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();
        let segment = blob_segment_path(&path, 1);
        let catalog_before = fs::read(path.join(BLOB_FILE)).unwrap();
        let segment_len_before = fs::metadata(&segment).unwrap().len();

        db.put(b"pending", &pending).unwrap();
        db.inject_blob_segment_after_write_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
        assert!(fs::metadata(&segment).unwrap().len() > segment_len_before);
        assert_eq!(fs::read(path.join(BLOB_FILE)).unwrap(), catalog_before);
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        assert!(!path.join(BLOB_DELTA_FILE).exists());
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        reopened.verify().unwrap();
        reopened.put(b"pending", &pending).unwrap();
        reopened.flush().unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        assert!(path.join(BLOB_DELTA_FILE).is_file());
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_catalog_delta_chain_reopens_and_preserves_anchor() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-catalog-delta-chain.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xE5; 2_000];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();
        let anchor = fs::read(path.join(BLOB_FILE)).unwrap();

        for index in 0..3 {
            let key = format!("delta-{index}");
            db.put(key.as_bytes(), &vec![0xE6 + index as u8; 2_000])
                .unwrap();
            db.flush().unwrap();
        }
        assert_eq!(fs::read(path.join(BLOB_FILE)).unwrap(), anchor);
        let delta = fs::read(path.join(BLOB_DELTA_FILE)).unwrap();
        assert!(!delta.is_empty());
        assert_eq!(
            BlobManager::segment_catalog_delta_prefix_len(&delta),
            Some(delta.len())
        );
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        for index in 0..3 {
            let key = format!("delta-{index}");
            assert_eq!(
                reopened.get(key.as_bytes()).unwrap(),
                Some(vec![0xE6 + index as u8; 2_000])
            );
        }
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_compaction_advances_catalog_generation() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("segmented-compaction-catalog-generation.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 64,
            ..Options::for_test()
        };
        let blob = vec![0xD5; 2_000];
        let mut db = DB::create(&path, options).unwrap();
        let initial = (0..256)
            .map(|index| BatchMutation::Put {
                key: format!("key-{index:04}").into_bytes(),
                value: vec![index as u8; 128],
            })
            .chain(std::iter::once(BatchMutation::Put {
                key: b"segmented-blob".to_vec(),
                value: blob.clone(),
            }))
            .collect::<Vec<_>>();
        db.commit_batch(&initial).unwrap();

        db.put(b"key-0128", &[0xE6; 128]).unwrap();
        db.flush().unwrap();
        let report = db.compact().unwrap();
        assert!(
            report.relocated_pages > 0,
            "expected an interior relocation"
        );
        assert_eq!(db.get(b"segmented-blob").unwrap(), Some(blob.clone()));
        db.verify().unwrap();
        drop(db);

        let mut reopened = DB::open(&path, Options::for_test()).unwrap();
        assert_eq!(reopened.get(b"segmented-blob").unwrap(), Some(blob));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_catalog_delta_sync_failure_discards_future_frame() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-catalog-delta-sync-failure.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xE7; 2_000];
        let pending = vec![0xE8; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();
        db.put(b"pending", &pending).unwrap();
        db.inject_blob_segment_catalog_sync_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        assert!(path.join(BLOB_DELTA_FILE).is_file());
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        reopened.verify().unwrap();
        reopened.put(b"pending", &pending).unwrap();
        reopened.flush().unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_catalog_consolidation_rename_failure_preserves_old_catalog() {
        fn fill_delta_chain(db: &mut DB) {
            for index in 0..TEST_SEGMENT_CATALOG_DELTA_LIMIT {
                let key = format!("delta-{index}");
                db.put(key.as_bytes(), &vec![0xF0; 2_000]).unwrap();
                db.flush().unwrap();
            }
        }

        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-catalog-rename-failure.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xF1; 2_000];
        let pending = vec![0xF2; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();
        let catalog = path.join(BLOB_FILE);
        let catalog_before = fs::read(&catalog).unwrap();
        fill_delta_chain(&mut db);

        db.put(b"pending", &pending).unwrap();
        db.inject_blob_segment_catalog_rename_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
        let backup = path.join(BLOB_REWRITE_BACKUP_FILE);
        assert!(!catalog.exists());
        assert_eq!(fs::read(&backup).unwrap(), catalog_before);
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        reopened.verify().unwrap();
        reopened.put(b"pending", &pending).unwrap();
        reopened.inject_after_manifest_failure();
        assert!(reopened.flush().is_err());
        drop(reopened);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        assert!(!backup.exists());
        assert!(!path.join(BLOB_DELTA_FILE).exists());
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_catalog_write_failures_restore_old_catalog() {
        let failures = [
            (
                "short",
                DB::inject_blob_segment_catalog_short_write_failure as fn(&DB),
            ),
            (
                "torn",
                DB::inject_blob_segment_catalog_torn_write_failure as fn(&DB),
            ),
        ];

        for (name, inject_failure) in failures {
            let dir = tempdir().unwrap();
            let path = dir
                .path()
                .join(format!("segmented-catalog-{name}-failure.db"));
            let options = Options {
                blob_storage: BlobStorageMode::Segmented,
                blob_threshold: 4,
                ..Options::default()
            };
            let base = vec![0xF3; 2_000];
            let pending = vec![0xF4; 2_100];
            let mut db = DB::create(&path, options).unwrap();
            db.put(b"base", &base).unwrap();
            db.flush().unwrap();
            for index in 0..TEST_SEGMENT_CATALOG_DELTA_LIMIT {
                let key = format!("delta-{index}");
                db.put(key.as_bytes(), &vec![0xF0; 2_000]).unwrap();
                db.flush().unwrap();
            }

            db.put(b"pending", &pending).unwrap();
            inject_failure(&db);
            assert!(db.flush().is_err(), "{name} catalog write should fail");
            assert!(db.durability_status().write_fenced);
            assert!(path.join(BLOB_REWRITE_BACKUP_FILE).is_file());
            drop(db);

            let mut reopened = DB::open(&path, Options::default()).unwrap();
            assert_eq!(reopened.get(b"base").unwrap(), Some(base));
            assert_eq!(reopened.get(b"pending").unwrap(), None);
            assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
            reopened.verify().unwrap();
            reopened.put(b"pending", &pending).unwrap();
            reopened.flush().unwrap();
            assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
            assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
            assert!(!path.join(BLOB_DELTA_FILE).exists());
            reopened.verify().unwrap();
        }
    }

    #[test]
    fn test_db_segmented_first_catalog_write_failure_restores_empty_catalog() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-first-catalog-failure.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let pending = vec![0xF7; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"pending", &pending).unwrap();
        db.inject_blob_segment_catalog_short_write_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
        assert!(path.join(BLOB_REWRITE_BACKUP_FILE).is_file());
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), None);
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        reopened.verify().unwrap();
        reopened.put(b"pending", &pending).unwrap();
        reopened.flush().unwrap();
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        reopened.verify().unwrap();
    }

    #[test]
    fn test_db_segmented_publication_directory_failure_restores_old_catalog() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("segmented-directory-failure.db");
        let options = Options {
            blob_storage: BlobStorageMode::Segmented,
            blob_threshold: 4,
            ..Options::default()
        };
        let base = vec![0xF5; 2_000];
        let pending = vec![0xF6; 2_100];
        let mut db = DB::create(&path, options).unwrap();
        db.put(b"base", &base).unwrap();
        db.flush().unwrap();

        db.put(b"pending", &pending).unwrap();
        db.inject_publication_directory_sync_failure();
        assert!(db.flush().is_err());
        assert!(db.durability_status().write_fenced);
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        assert!(path.join(BLOB_DELTA_FILE).is_file());
        drop(db);

        let mut reopened = DB::open(&path, Options::default()).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(base));
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        reopened.verify().unwrap();
        if reopened.get(b"pending").unwrap().is_none() {
            reopened.put(b"pending", &pending).unwrap();
            reopened.flush().unwrap();
        }
        assert_eq!(reopened.get(b"pending").unwrap(), Some(pending));
        assert!(!path.join(BLOB_REWRITE_BACKUP_FILE).exists());
        assert!(path.join(BLOB_DELTA_FILE).is_file());
        reopened.verify().unwrap();
    }
}
