use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::artifact::{self, Manifest};
use crate::fault::{FaultInjector, FaultPoint, NoFaults};
use crate::model::{CommitId, IndexId, Key, Mutation, StorageIdentity};
use crate::packed::{PackBudget, PackedRange, pack_sorted};
use crate::runtime::{ReactorError, WorkId};
use crate::{AttemptRecord, DbError, Result, TransactionAttemptId, wal};
use fs2::FileExt;

const WAL_NAME: &str = "omendb.wal";
const MANIFEST_NAME: &str = "omendb.manifest";
const LOCK_NAME: &str = "omendb.lock";
const IDENTITY_NAME: &str = "omendb.identity";
const IDENTITY_MAGIC: [u8; 4] = *b"DBID";
const IDENTITY_VERSION: u32 = 1;
const IDENTITY_BYTES: usize = 36;
const MAX_FRAGMENT_CHAIN: usize = 64;
const STATE_MAGIC: [u8; 4] = *b"DBST";
const STATE_VERSION: u16 = 3;
const LEGACY_STATE_VERSION: u16 = 2;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum PhysicalKey {
    Fixed(Key),
    Bytes(Vec<u8>),
}

impl PhysicalKey {
    fn fixed(key: Key) -> Self {
        Self::Fixed(key)
    }

    fn bytes(key: Vec<u8>) -> Result<Self> {
        if key.is_empty() {
            return Err(DbError::InvalidState(
                "variable-width storage key must not be empty".to_owned(),
            ));
        }
        Ok(Self::Bytes(key))
    }
}

#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub directory: PathBuf,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseMetrics {
    pub wal_bytes: u64,
    pub fragment_bytes: u64,
    pub packed_page_bytes: u64,
    pub manifest_bytes: u64,
    pub syncs: u64,
    pub compaction_runs: u64,
    pub fragments_reclaimed: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionBudget {
    pub max_row_keys: usize,
    pub max_index_keys: usize,
}

impl CompactionBudget {
    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            max_row_keys: usize::MAX,
            max_index_keys: usize::MAX,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactionReport {
    pub row_keys_considered: usize,
    pub index_keys_considered: usize,
    pub row_fragments_reclaimed: usize,
    pub index_fragments_reclaimed: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionWork {
    pub work_id: WorkId,
    pub budget: CompactionBudget,
}

#[derive(Debug, thiserror::Error)]
pub enum MaintenanceError {
    #[error("maintenance work {expected:?} does not match dispatched work {actual:?}")]
    WorkMismatch { expected: WorkId, actual: WorkId },
    #[error("maintenance dispatch must be reclaim work")]
    WrongWorkClass,
    #[error("reactor error: {0}")]
    Reactor(#[from] ReactorError),
    #[error("database maintenance error: {0}")]
    Database(#[from] DbError),
}

#[derive(Debug)]
pub struct Transaction {
    snapshot: CommitId,
    active_snapshots: Arc<Mutex<BTreeMap<CommitId, usize>>>,
    point_reads: BTreeSet<PhysicalKey>,
    range_reads: Vec<(PhysicalKey, PhysicalKey)>,
    index_reads: Vec<(IndexId, Vec<u8>)>,
    index_range_reads: Vec<(IndexId, Vec<u8>, Vec<u8>)>,
    mutations: Vec<Mutation>,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let mut active = self
            .active_snapshots
            .lock()
            .expect("active snapshot pins lock poisoned");
        let Some(count) = active.get_mut(&self.snapshot) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            active.remove(&self.snapshot);
        }
    }
}

impl Transaction {
    #[must_use]
    pub fn snapshot(&self) -> CommitId {
        self.snapshot
    }

    pub fn get(&mut self, database: &Database, key: Key) -> Result<Option<Vec<u8>>> {
        self.get_physical(database, PhysicalKey::fixed(key))
    }

    pub fn get_bytes(&mut self, database: &Database, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let physical = PhysicalKey::bytes(key)?;
        self.get_physical(database, physical)
    }

    fn get_physical(&mut self, database: &Database, key: PhysicalKey) -> Result<Option<Vec<u8>>> {
        self.point_reads.insert(key.clone());
        let committed = database.get_physical(self.snapshot, &key)?;
        Ok(self.staged_value(&key).unwrap_or(committed))
    }

    pub fn scan(
        &mut self,
        database: &Database,
        start: Key,
        end: Key,
        limit: usize,
    ) -> Result<Vec<(Key, Vec<u8>)>> {
        let rows = self.scan_physical(
            database,
            PhysicalKey::fixed(start),
            PhysicalKey::fixed(end),
            limit,
        )?;
        rows.into_iter()
            .map(|(key, value)| match key {
                PhysicalKey::Fixed(key) => Ok((key, value)),
                PhysicalKey::Bytes(_) => Err(DbError::Corruption {
                    artifact: "temporary row",
                    reason: "fixed-width scan returned a variable-width key".to_owned(),
                }),
            })
            .collect()
    }

    pub fn scan_bytes(
        &mut self,
        database: &Database,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.scan_physical(
            database,
            PhysicalKey::bytes(start)?,
            PhysicalKey::bytes(end)?,
            limit,
        )?;
        rows.into_iter()
            .map(|(key, value)| match key {
                PhysicalKey::Bytes(key) => Ok((key, value)),
                PhysicalKey::Fixed(_) => Err(DbError::Corruption {
                    artifact: "temporary row",
                    reason: "variable-width scan returned a fixed key".to_owned(),
                }),
            })
            .collect()
    }

    fn scan_physical(
        &mut self,
        database: &Database,
        start: PhysicalKey,
        end: PhysicalKey,
        limit: usize,
    ) -> Result<Vec<(PhysicalKey, Vec<u8>)>> {
        self.range_reads.push((start.clone(), end.clone()));
        let mut rows: BTreeMap<PhysicalKey, Vec<u8>> = database
            .scan_physical(self.snapshot, &start, &end, usize::MAX)?
            .into_iter()
            .collect();
        for mutation in &self.mutations {
            match mutation {
                Mutation::Put { key, value } => {
                    rows.insert(PhysicalKey::fixed(*key), value.clone());
                }
                Mutation::Delete { key } => {
                    rows.remove(&PhysicalKey::fixed(*key));
                }
                Mutation::BytePut { key, value } => {
                    if let Ok(key) = PhysicalKey::bytes(key.clone()) {
                        rows.insert(key, value.clone());
                    }
                }
                Mutation::ByteDelete { key } => {
                    if let Ok(key) = PhysicalKey::bytes(key.clone()) {
                        rows.remove(&key);
                    }
                }
                _ => {}
            }
        }
        Ok(rows
            .range(start..end)
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    pub fn put(&mut self, key: Key, value: Vec<u8>) {
        self.mutations.push(Mutation::Put { key, value });
    }

    pub fn delete(&mut self, key: Key) {
        self.mutations.push(Mutation::Delete { key });
    }

    pub fn put_bytes(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.mutations.push(Mutation::BytePut { key, value });
    }

    pub fn delete_bytes(&mut self, key: Vec<u8>) {
        self.mutations.push(Mutation::ByteDelete { key });
    }

    pub fn index_put(&mut self, index: IndexId, index_key: Vec<u8>, primary: Key) {
        self.mutations.push(Mutation::IndexPut {
            index,
            index_key,
            primary,
        });
    }

    pub fn index_delete(&mut self, index: IndexId, index_key: Vec<u8>, primary: Key) {
        self.mutations.push(Mutation::IndexDelete {
            index,
            index_key,
            primary,
        });
    }

    pub fn index_put_bytes(&mut self, index: IndexId, index_key: Vec<u8>, primary: Vec<u8>) {
        self.mutations.push(Mutation::ByteIndexPut {
            index,
            index_key,
            primary,
        });
    }

    pub fn index_delete_bytes(&mut self, index: IndexId, index_key: Vec<u8>, primary: Vec<u8>) {
        self.mutations.push(Mutation::ByteIndexDelete {
            index,
            index_key,
            primary,
        });
    }

    pub fn index_get(
        &mut self,
        database: &Database,
        index: IndexId,
        index_key: Vec<u8>,
    ) -> Result<Vec<Key>> {
        self.index_get_physical(database, index, index_key)?
            .into_iter()
            .map(|primary| match primary {
                PhysicalKey::Fixed(primary) => Ok(primary),
                PhysicalKey::Bytes(_) => Err(DbError::Corruption {
                    artifact: "temporary secondary index",
                    reason: "fixed index read returned a variable-width primary".to_owned(),
                }),
            })
            .collect()
    }

    pub fn index_get_bytes(
        &mut self,
        database: &Database,
        index: IndexId,
        index_key: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>> {
        self.index_get_physical(database, index, index_key)?
            .into_iter()
            .map(|primary| match primary {
                PhysicalKey::Bytes(primary) => Ok(primary),
                PhysicalKey::Fixed(_) => Err(DbError::Corruption {
                    artifact: "temporary secondary index",
                    reason: "byte index read returned a fixed-width primary".to_owned(),
                }),
            })
            .collect()
    }

    fn index_get_physical(
        &mut self,
        database: &Database,
        index: IndexId,
        index_key: Vec<u8>,
    ) -> Result<Vec<PhysicalKey>> {
        self.index_reads.push((index, index_key.clone()));
        let has_staged_create = self.has_staged_index_create(index);
        let committed = if database.indexes.contains_key(&index) {
            database.index_get_physical(self.snapshot, index, &index_key)?
        } else if has_staged_create {
            database.validate_snapshot(self.snapshot)?;
            Vec::new()
        } else {
            database.index_get_physical(self.snapshot, index, &index_key)?
        };
        Ok(self.staged_index_members(index, &index_key, committed))
    }

    pub fn index_scan(
        &mut self,
        database: &Database,
        index: IndexId,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Key)>> {
        self.index_scan_physical(database, index, start, end, limit)?
            .into_iter()
            .map(|(index_key, primary)| match primary {
                PhysicalKey::Fixed(primary) => Ok((index_key, primary)),
                PhysicalKey::Bytes(_) => Err(DbError::Corruption {
                    artifact: "temporary secondary index",
                    reason: "fixed index scan returned a variable-width primary".to_owned(),
                }),
            })
            .collect()
    }

    pub fn index_scan_bytes(
        &mut self,
        database: &Database,
        index: IndexId,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.index_scan_physical(database, index, start, end, limit)?
            .into_iter()
            .map(|(index_key, primary)| match primary {
                PhysicalKey::Bytes(primary) => Ok((index_key, primary)),
                PhysicalKey::Fixed(_) => Err(DbError::Corruption {
                    artifact: "temporary secondary index",
                    reason: "byte index scan returned a fixed-width primary".to_owned(),
                }),
            })
            .collect()
    }

    fn index_scan_physical(
        &mut self,
        database: &Database,
        index: IndexId,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PhysicalKey)>> {
        self.index_range_reads
            .push((index, start.clone(), end.clone()));
        let has_staged_create = self.has_staged_index_create(index);
        let committed = if database.indexes.contains_key(&index) {
            database.index_scan_physical(self.snapshot, index, &start, &end, usize::MAX)?
        } else if has_staged_create {
            database.validate_snapshot(self.snapshot)?;
            Vec::new()
        } else {
            database.index_scan_physical(self.snapshot, index, &start, &end, usize::MAX)?
        };
        let mut entries: BTreeMap<Vec<u8>, BTreeSet<PhysicalKey>> = BTreeMap::new();
        for (index_key, primary) in committed {
            entries.entry(index_key).or_default().insert(primary);
        }
        for mutation in &self.mutations {
            match mutation {
                Mutation::IndexPut {
                    index: mutation_index,
                    index_key,
                    primary,
                } if *mutation_index == index => {
                    entries
                        .entry(index_key.clone())
                        .or_default()
                        .insert(PhysicalKey::fixed(*primary));
                }
                Mutation::IndexDelete {
                    index: mutation_index,
                    index_key,
                    primary,
                } if *mutation_index == index => {
                    if let Some(members) = entries.get_mut(index_key) {
                        members.remove(&PhysicalKey::fixed(*primary));
                        if members.is_empty() {
                            entries.remove(index_key);
                        }
                    }
                }
                Mutation::ByteIndexPut {
                    index: mutation_index,
                    index_key,
                    primary,
                } if *mutation_index == index => {
                    entries
                        .entry(index_key.clone())
                        .or_default()
                        .insert(PhysicalKey::bytes(primary.clone())?);
                }
                Mutation::ByteIndexDelete {
                    index: mutation_index,
                    index_key,
                    primary,
                } if *mutation_index == index => {
                    let primary = PhysicalKey::bytes(primary.clone())?;
                    if let Some(members) = entries.get_mut(index_key) {
                        members.remove(&primary);
                        if members.is_empty() {
                            entries.remove(index_key);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(entries
            .range(start..end)
            .flat_map(|(index_key, members)| {
                members
                    .iter()
                    .map(|primary| (index_key.clone(), primary.clone()))
            })
            .take(limit)
            .collect())
    }

    fn staged_value(&self, key: &PhysicalKey) -> Option<Option<Vec<u8>>> {
        self.mutations
            .iter()
            .rev()
            .find_map(|mutation| match mutation {
                Mutation::Put {
                    key: mutation_key,
                    value,
                } if PhysicalKey::fixed(*mutation_key) == *key => Some(Some(value.clone())),
                Mutation::Delete { key: mutation_key }
                    if PhysicalKey::fixed(*mutation_key) == *key =>
                {
                    Some(None)
                }
                Mutation::BytePut {
                    key: mutation_key,
                    value,
                } if PhysicalKey::bytes(mutation_key.clone()).ok().as_ref() == Some(key) => {
                    Some(Some(value.clone()))
                }
                Mutation::ByteDelete { key: mutation_key }
                    if PhysicalKey::bytes(mutation_key.clone()).ok().as_ref() == Some(key) =>
                {
                    Some(None)
                }
                _ => None,
            })
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.mutations.is_empty()
    }

    fn has_staged_index_create(&self, index: IndexId) -> bool {
        self.mutations.iter().any(|mutation| {
            matches!(mutation, Mutation::CreateIndex { index: mutation_index, .. } if *mutation_index == index)
        })
    }

    fn staged_index_members(
        &self,
        index: IndexId,
        index_key: &[u8],
        committed: Vec<PhysicalKey>,
    ) -> Vec<PhysicalKey> {
        let mut members: BTreeSet<PhysicalKey> = committed.into_iter().collect();
        for mutation in &self.mutations {
            match mutation {
                Mutation::IndexPut {
                    index: mutation_index,
                    index_key: mutation_key,
                    primary,
                } if *mutation_index == index && mutation_key == index_key => {
                    members.insert(PhysicalKey::fixed(*primary));
                }
                Mutation::IndexDelete {
                    index: mutation_index,
                    index_key: mutation_key,
                    primary,
                } if *mutation_index == index && mutation_key == index_key => {
                    members.remove(&PhysicalKey::fixed(*primary));
                }
                Mutation::ByteIndexPut {
                    index: mutation_index,
                    index_key: mutation_key,
                    primary,
                } if *mutation_index == index && mutation_key == index_key => {
                    if let Ok(primary) = PhysicalKey::bytes(primary.clone()) {
                        members.insert(primary);
                    }
                }
                Mutation::ByteIndexDelete {
                    index: mutation_index,
                    index_key: mutation_key,
                    primary,
                } if *mutation_index == index && mutation_key == index_key => {
                    if let Ok(primary) = PhysicalKey::bytes(primary.clone()) {
                        members.remove(&primary);
                    }
                }
                _ => {}
            }
        }
        members.into_iter().collect()
    }
}

#[derive(Clone, Debug)]
struct Fragment {
    commit: CommitId,
    value: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct IndexFragment {
    commit: CommitId,
    primary: PhysicalKey,
    present: bool,
}

#[derive(Clone, Debug)]
struct IndexState {
    unique: bool,
    current: BTreeMap<Vec<u8>, BTreeSet<PhysicalKey>>,
    fragments: BTreeMap<Vec<u8>, Vec<IndexFragment>>,
}

#[derive(Debug)]
pub struct Database {
    config: DatabaseConfig,
    identity: StorageIdentity,
    _writer_lock: Option<File>,
    current: BTreeMap<PhysicalKey, Vec<u8>>,
    fragments: BTreeMap<PhysicalKey, Vec<Fragment>>,
    indexes: BTreeMap<IndexId, IndexState>,
    retained: BTreeMap<CommitId, usize>,
    active_snapshots: Arc<Mutex<BTreeMap<CommitId, usize>>>,
    commit: CommitId,
    generation: u64,
    attempts: BTreeMap<TransactionAttemptId, AttemptRecord>,
    /// Ordered logical commit boundaries. This is durable metadata rather
    /// than a second row/index history, and it remains intact when fragment
    /// compaction removes old value versions.
    published_commits: BTreeSet<CommitId>,
    /// Legacy v2 checkpoints did not persist the commit catalog. They remain
    /// usable for ordinary reads but cannot claim complete-history transfer.
    history_complete: bool,
    recovery_required: bool,
    metrics: DatabaseMetrics,
}

impl Database {
    #[must_use]
    pub fn config(directory: PathBuf) -> DatabaseConfig {
        DatabaseConfig { directory }
    }

    pub fn create(config: DatabaseConfig) -> Result<Self> {
        Self::initialize(config, true)
    }

    fn initialize(config: DatabaseConfig, acquire_lock: bool) -> Result<Self> {
        fs::create_dir_all(&config.directory)
            .map_err(|source| io_error("create database directory", source))?;
        let writer_lock = acquire_lock
            .then(|| acquire_writer_lock(&config.directory))
            .transpose()?;
        let identity = load_or_create_identity(&config.directory, acquire_lock)?;
        Ok(Self {
            config,
            identity,
            _writer_lock: writer_lock,
            current: BTreeMap::new(),
            fragments: BTreeMap::new(),
            indexes: BTreeMap::new(),
            retained: BTreeMap::new(),
            active_snapshots: Arc::new(Mutex::new(BTreeMap::new())),
            commit: CommitId(0),
            generation: 0,
            attempts: BTreeMap::new(),
            published_commits: BTreeSet::from([CommitId(0)]),
            history_complete: true,
            recovery_required: false,
            metrics: DatabaseMetrics::default(),
        })
    }

    pub fn open(config: DatabaseConfig, faults: &mut dyn FaultInjector) -> Result<Self> {
        faults.check(FaultPoint::DuringRecovery)?;
        let mut database = Self::initialize(config.clone(), true)?;
        database.recover(config)?;
        Ok(database)
    }

    /// Close this writer handle.
    ///
    /// Temporary commits synchronously append and sync their WAL records, so
    /// there is no buffered publication to flush here. A fenced handle still
    /// reports that recovery is required; consuming the handle then releases
    /// its OS lock through normal drop cleanup.
    pub fn close(self) -> Result<()> {
        if self.recovery_required {
            Err(DbError::RecoveryRequired)
        } else {
            Ok(())
        }
    }

    /// Reopen durable artifacts for a read-only integrity pass without
    /// attempting to become a second writer.
    pub(crate) fn open_for_verification(
        config: DatabaseConfig,
        faults: &mut dyn FaultInjector,
    ) -> Result<Self> {
        faults.check(FaultPoint::DuringRecovery)?;
        let mut database = Self::initialize(config.clone(), false)?;
        database.recover(config)?;
        Ok(database)
    }

    pub(crate) fn database_config(&self) -> DatabaseConfig {
        self.config.clone()
    }

    fn recover(&mut self, config: DatabaseConfig) -> Result<()> {
        if let Some(manifest) = artifact::read_manifest(&manifest_path(&config))? {
            let payload = artifact::read_pages(
                &artifact::data_path(&manifest_path(&config), manifest.generation),
                manifest.generation,
                manifest.logical_len,
            )?;
            if crc32c::crc32c(&payload) != manifest.payload_checksum {
                return Err(DbError::Corruption {
                    artifact: "checkpoint payload",
                    reason: "manifest checksum mismatch".to_owned(),
                });
            }
            self.decode_state(&payload)?;
            if self.commit.0 != manifest.commit {
                return Err(DbError::Corruption {
                    artifact: "checkpoint manifest",
                    reason: "state commit differs from manifest commit".to_owned(),
                });
            }
            self.generation = manifest.generation;
            let ranges = range_path(&config, manifest.generation);
            if !ranges.exists() {
                return Err(DbError::Corruption {
                    artifact: "packed range",
                    reason: "manifest range artifact is missing".to_owned(),
                });
            }
            let packed = PackedRange::read(&ranges, manifest.generation)?;
            if packed.checksum() != manifest.range_checksum {
                return Err(DbError::Corruption {
                    artifact: "checkpoint manifest",
                    reason: "manifest checksum disagrees with packed range".to_owned(),
                });
            }
        }
        for (commit, mutations) in wal::replay(&wal_path(&config), self.commit)? {
            if commit.0 != self.commit.0 + 1 {
                return Err(DbError::Corruption {
                    artifact: "WAL",
                    reason: format!("commit gap at {}", commit.0),
                });
            }
            self.apply(commit, &mutations)?;
        }
        Ok(())
    }

    pub fn commit(
        &mut self,
        mutations: Vec<Mutation>,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.commit_batch(self.commit, mutations, faults)
    }

    /// Resolve a durable transaction attempt after reopening this history.
    ///
    /// `Some` means the attempt's logical mutation batch was published. The
    /// caller must not execute the batch again; it can use the returned
    /// commit to read the resulting state. `None` means no durable record was
    /// found and the caller may build a fresh transaction.
    pub fn resolve_attempt(&self, attempt: TransactionAttemptId) -> Option<AttemptRecord> {
        self.attempts.get(&attempt).copied()
    }

    /// Return durable attempt records in deterministic identity order.
    pub fn attempt_records(&self, limit: usize) -> Result<Vec<AttemptRecord>> {
        let mut records = self
            .attempts
            .values()
            .copied()
            .take(limit.saturating_add(1));
        let records = records.by_ref().collect::<Vec<_>>();
        if records.len() > limit {
            return Err(DbError::SnapshotCaptureLimit {
                resource: "transaction attempts",
                limit,
            });
        }
        Ok(records)
    }

    /// Return every durable logical commit boundary, including the initial
    /// empty state. This catalog is distinct from retained snapshot leases;
    /// callers still need to retain each returned commit before reading it.
    pub fn published_commits(&self) -> Result<Vec<CommitId>> {
        if !self.history_complete {
            return Err(DbError::InvalidState(
                "complete commit history is unavailable for this checkpoint format".to_owned(),
            ));
        }
        let commits = self.published_commits.iter().copied().collect::<Vec<_>>();
        if commits.first().copied() != Some(CommitId(0))
            || commits.last().copied() != Some(self.commit)
            || commits
                .windows(2)
                .any(|pair| pair[0].0.checked_add(1) != Some(pair[1].0))
        {
            return Err(DbError::InvalidState(
                "complete commit history is unavailable for this checkpoint format".to_owned(),
            ));
        }
        Ok(commits)
    }

    /// Publish imported transaction-attempt records in one target-history
    /// control-plane commit. The target commit is authoritative; source
    /// commit numbers are never reused.
    pub fn import_attempt_records(
        &mut self,
        records: &[AttemptRecord],
        faults: &mut dyn FaultInjector,
    ) -> Result<Vec<AttemptRecord>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let mut seen = BTreeSet::new();
        for record in records {
            if !seen.insert(record.attempt) {
                return Err(DbError::InvalidState(
                    "duplicate transaction attempt in archive".to_owned(),
                ));
            }
            if let Some(existing) = self.attempts.get(&record.attempt) {
                return Err(DbError::IdempotencyConflict {
                    attempt: record.attempt,
                    existing_digest: existing.digest,
                    requested_digest: record.digest,
                });
            }
        }
        let commit = self.commit(
            records
                .iter()
                .map(|record| Mutation::RecordAttempt {
                    attempt: record.attempt,
                    digest: record.digest,
                })
                .collect(),
            faults,
        )?;
        Ok(records
            .iter()
            .map(|record| AttemptRecord {
                attempt: record.attempt,
                commit,
                digest: record.digest,
            })
            .collect())
    }

    /// Publish a transaction together with a durable idempotency record.
    ///
    /// Reusing the same attempt and identical logical mutations returns the
    /// original commit without publishing again. Reusing it for different
    /// mutations is rejected. An ambiguous error must still be followed by
    /// reopen and [`Self::resolve_attempt`].
    pub fn commit_with_attempt(
        &mut self,
        base_snapshot: CommitId,
        attempt: TransactionAttemptId,
        mutations: Vec<Mutation>,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        if mutations.is_empty() {
            return Err(DbError::InvalidState("empty transaction".to_owned()));
        }
        if mutations.iter().any(|mutation| {
            matches!(
                mutation,
                Mutation::RecordAttempt { .. } | Mutation::ForgetAttempt { .. }
            )
        }) {
            return Err(DbError::InvalidState(
                "transaction attempt metadata is reserved by the storage kernel".to_owned(),
            ));
        }
        let digest = crate::attempt::digest_mutations(&mutations);
        if let Some(record) = self.attempts.get(&attempt).copied() {
            if record.digest == digest {
                return Ok(record.commit);
            }
            return Err(DbError::IdempotencyConflict {
                attempt,
                existing_digest: record.digest,
                requested_digest: digest,
            });
        }
        let mut durable = mutations;
        durable.push(Mutation::RecordAttempt { attempt, digest });
        self.commit_batch(base_snapshot, durable, faults)
    }

    /// Forget durable attempt records after the caller has decided that no
    /// retry may use those identities again.
    ///
    /// The deletion is one durable commit. If it returns an ambiguous error,
    /// reopen and resolve each identity before deciding whether cleanup or
    /// application work remains. Forgotten identities must never be reused.
    pub fn forget_attempts(
        &mut self,
        attempts: &[TransactionAttemptId],
        faults: &mut dyn FaultInjector,
    ) -> Result<usize> {
        let attempts = attempts
            .iter()
            .copied()
            .filter(|attempt| self.attempts.contains_key(attempt))
            .collect::<std::collections::BTreeSet<_>>();
        if attempts.is_empty() {
            return Ok(0);
        }
        let count = attempts.len();
        let mutations = attempts
            .into_iter()
            .map(|attempt| Mutation::ForgetAttempt { attempt })
            .collect();
        self.commit_batch(self.commit, mutations, faults)?;
        Ok(count)
    }

    pub fn begin(&self) -> Transaction {
        let snapshot = self.commit;
        self.active_snapshots
            .lock()
            .expect("active snapshot pins lock poisoned")
            .entry(snapshot)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        Transaction {
            snapshot,
            active_snapshots: Arc::clone(&self.active_snapshots),
            point_reads: BTreeSet::new(),
            range_reads: Vec::new(),
            index_reads: Vec::new(),
            index_range_reads: Vec::new(),
            mutations: Vec::new(),
        }
    }

    pub fn commit_transaction(
        &mut self,
        mut transaction: Transaction,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        if self.has_conflict(&transaction) {
            return Err(DbError::SerializationConflict {
                snapshot: transaction.snapshot.0,
                current: self.commit.0,
            });
        }
        let snapshot = transaction.snapshot;
        let mutations = std::mem::take(&mut transaction.mutations);
        self.commit_batch(snapshot, mutations, faults)
    }

    pub fn commit_transaction_with_attempt(
        &mut self,
        mut transaction: Transaction,
        attempt: TransactionAttemptId,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        if self.has_conflict(&transaction) {
            return Err(DbError::SerializationConflict {
                snapshot: transaction.snapshot.0,
                current: self.commit.0,
            });
        }
        let snapshot = transaction.snapshot;
        let mutations = std::mem::take(&mut transaction.mutations);
        self.commit_with_attempt(snapshot, attempt, mutations, faults)
    }

    pub fn commit_transaction_validated(
        &mut self,
        mut transaction: Transaction,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        if self.has_precise_conflict(&transaction) {
            return Err(DbError::SerializationConflict {
                snapshot: transaction.snapshot.0,
                current: self.commit.0,
            });
        }
        let snapshot = transaction.snapshot;
        let mutations = std::mem::take(&mut transaction.mutations);
        self.commit_batch(snapshot, mutations, faults)
    }

    pub fn commit_transaction_validated_with_attempt(
        &mut self,
        mut transaction: Transaction,
        attempt: TransactionAttemptId,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        if self.has_precise_conflict(&transaction) {
            return Err(DbError::SerializationConflict {
                snapshot: transaction.snapshot.0,
                current: self.commit.0,
            });
        }
        let snapshot = transaction.snapshot;
        let mutations = std::mem::take(&mut transaction.mutations);
        self.commit_with_attempt(snapshot, attempt, mutations, faults)
    }

    fn commit_batch(
        &mut self,
        base_snapshot: CommitId,
        mutations: Vec<Mutation>,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        if self.recovery_required {
            return Err(DbError::RecoveryRequired);
        }
        if mutations.is_empty() {
            return Err(DbError::InvalidState("empty transaction".to_owned()));
        }
        if base_snapshot.0 > self.commit.0 {
            return Err(DbError::SnapshotUnavailable(base_snapshot.0));
        }
        self.validate_mutations(&mutations)?;
        let commit = CommitId(self.commit.0 + 1);
        faults.check(FaultPoint::BeforeWalAppend)?;
        let bytes = match wal::append(&wal_path(&self.config), commit, &mutations, faults) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.recovery_required = true;
                return Err(error);
            }
        };
        self.metrics.wal_bytes = self.metrics.wal_bytes.saturating_add(bytes);
        self.metrics.syncs = self.metrics.syncs.saturating_add(1);
        if let Err(error) = self.apply(commit, &mutations) {
            self.recovery_required = true;
            return Err(error);
        }
        Ok(commit)
    }

    fn has_conflict(&self, transaction: &Transaction) -> bool {
        if transaction.snapshot.0 >= self.commit.0 {
            return false;
        }

        // The relational contract currently exposes a fixed-snapshot,
        // serialized-writer profile. A stale writer must be rebuilt from a
        // fresh snapshot even when its keys are disjoint; otherwise the
        // temporary backend would admit histories that SeerDB rejects and
        // callers would observe backend-dependent isolation.
        if !transaction.is_read_only() {
            return true;
        }

        self.has_precise_conflict(transaction)
    }

    pub fn has_precise_conflict(&self, transaction: &Transaction) -> bool {
        if transaction.snapshot.0 >= self.commit.0 {
            return false;
        }

        transaction
            .point_reads
            .iter()
            .any(|key| self.changed_after(transaction.snapshot, key))
            || transaction
                .range_reads
                .iter()
                .any(|(start, end)| self.range_changed_after(transaction.snapshot, start, end))
            || transaction
                .index_reads
                .iter()
                .any(|(index, key)| self.index_changed_after(transaction.snapshot, *index, key))
            || transaction
                .index_range_reads
                .iter()
                .any(|(index, start, end)| {
                    self.index_range_changed_after(transaction.snapshot, *index, start, end)
                })
            || transaction.mutations.iter().any(|mutation| match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => {
                    self.changed_after(transaction.snapshot, &PhysicalKey::fixed(*key))
                }
                Mutation::BytePut { key, .. } | Mutation::ByteDelete { key } => {
                    PhysicalKey::bytes(key.clone())
                        .is_ok_and(|key| self.changed_after(transaction.snapshot, &key))
                }
                Mutation::IndexPut {
                    index, index_key, ..
                }
                | Mutation::IndexDelete {
                    index, index_key, ..
                } => self.index_changed_after(transaction.snapshot, *index, index_key),
                Mutation::ByteIndexPut {
                    index, index_key, ..
                }
                | Mutation::ByteIndexDelete {
                    index, index_key, ..
                } => self.index_changed_after(transaction.snapshot, *index, index_key),
                Mutation::CreateIndex { index, .. } => self.indexes.contains_key(index),
                Mutation::RecordAttempt { .. } | Mutation::ForgetAttempt { .. } => false,
            })
    }

    fn changed_after(&self, snapshot: CommitId, key: &PhysicalKey) -> bool {
        self.fragments
            .get(key)
            .and_then(|history| history.last())
            .is_some_and(|fragment| fragment.commit > snapshot)
    }

    fn range_changed_after(
        &self,
        snapshot: CommitId,
        start: &PhysicalKey,
        end: &PhysicalKey,
    ) -> bool {
        self.fragments.range(start..end).any(|(_, history)| {
            history
                .last()
                .is_some_and(|fragment| fragment.commit > snapshot)
        })
    }

    fn index_changed_after(&self, snapshot: CommitId, index: IndexId, key: &[u8]) -> bool {
        self.indexes
            .get(&index)
            .and_then(|state| state.fragments.get(key))
            .and_then(|history| history.last())
            .is_some_and(|fragment| fragment.commit > snapshot)
    }

    fn index_range_changed_after(
        &self,
        snapshot: CommitId,
        index: IndexId,
        start: &[u8],
        end: &[u8],
    ) -> bool {
        self.indexes.get(&index).is_some_and(|state| {
            state
                .fragments
                .range(start.to_vec()..end.to_vec())
                .any(|(_, history)| {
                    history
                        .last()
                        .is_some_and(|fragment| fragment.commit > snapshot)
                })
        })
    }

    pub fn get(&self, snapshot: CommitId, key: Key) -> Result<Option<Vec<u8>>> {
        self.get_physical(snapshot, &PhysicalKey::fixed(key))
    }

    pub fn get_bytes(&self, snapshot: CommitId, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let key = PhysicalKey::bytes(key)?;
        self.get_physical(snapshot, &key)
    }

    /// Whether this byte key has a durable version at or before `snapshot`.
    /// The compatibility adapter uses this to distinguish a new byte-key
    /// tombstone from an absent key in a legacy typed-index namespace.
    pub fn has_bytes_history(&self, snapshot: CommitId, key: &[u8]) -> Result<bool> {
        self.validate_snapshot(snapshot)?;
        let key = PhysicalKey::bytes(key.to_vec())?;
        Ok(self
            .fragments
            .get(&key)
            .is_some_and(|history| history.iter().any(|fragment| fragment.commit <= snapshot)))
    }

    fn get_physical(&self, snapshot: CommitId, key: &PhysicalKey) -> Result<Option<Vec<u8>>> {
        self.validate_snapshot(snapshot)?;
        if snapshot == self.commit {
            return Ok(self.current.get(key).cloned());
        }
        let Some(history) = self.fragments.get(key) else {
            return Ok(None);
        };
        Ok(history
            .iter()
            .rev()
            .find(|fragment| fragment.commit <= snapshot)
            .and_then(|fragment| fragment.value.clone()))
    }

    pub fn scan(
        &self,
        snapshot: CommitId,
        start: Key,
        end: Key,
        limit: usize,
    ) -> Result<Vec<(Key, Vec<u8>)>> {
        self.scan_physical(
            snapshot,
            &PhysicalKey::fixed(start),
            &PhysicalKey::fixed(end),
            limit,
        )?
        .into_iter()
        .map(|(key, value)| match key {
            PhysicalKey::Fixed(key) => Ok((key, value)),
            PhysicalKey::Bytes(_) => Err(DbError::Corruption {
                artifact: "temporary row",
                reason: "fixed-width scan returned a variable-width key".to_owned(),
            }),
        })
        .collect()
    }

    pub fn scan_bytes(
        &self,
        snapshot: CommitId,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start = PhysicalKey::bytes(start)?;
        let end = PhysicalKey::bytes(end)?;
        self.scan_physical(snapshot, &start, &end, limit)?
            .into_iter()
            .map(|(key, value)| match key {
                PhysicalKey::Bytes(key) => Ok((key, value)),
                PhysicalKey::Fixed(_) => Err(DbError::Corruption {
                    artifact: "temporary row",
                    reason: "byte scan returned a fixed-width key".to_owned(),
                }),
            })
            .collect()
    }

    fn scan_physical(
        &self,
        snapshot: CommitId,
        start: &PhysicalKey,
        end: &PhysicalKey,
        limit: usize,
    ) -> Result<Vec<(PhysicalKey, Vec<u8>)>> {
        self.validate_snapshot(snapshot)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let keys: BTreeSet<PhysicalKey> = self
            .current
            .keys()
            .cloned()
            .chain(self.fragments.keys().cloned())
            .collect();
        let mut rows = Vec::new();
        for key in keys.range(start.clone()..end.clone()).cloned() {
            if let Some(value) = self.get_physical(snapshot, &key)? {
                rows.push((key, value));
                if rows.len() >= limit {
                    break;
                }
            }
        }
        Ok(rows)
    }

    pub fn index_get(
        &self,
        snapshot: CommitId,
        index: IndexId,
        index_key: &[u8],
    ) -> Result<Vec<Key>> {
        self.index_get_physical(snapshot, index, index_key)?
            .into_iter()
            .map(|primary| match primary {
                PhysicalKey::Fixed(primary) => Ok(primary),
                PhysicalKey::Bytes(_) => Err(DbError::Corruption {
                    artifact: "temporary secondary index",
                    reason: "fixed index read returned a variable-width primary".to_owned(),
                }),
            })
            .collect()
    }

    pub fn index_get_bytes(
        &self,
        snapshot: CommitId,
        index: IndexId,
        index_key: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        self.index_get_physical(snapshot, index, index_key)?
            .into_iter()
            .map(|primary| match primary {
                PhysicalKey::Bytes(primary) => Ok(primary),
                PhysicalKey::Fixed(_) => Err(DbError::Corruption {
                    artifact: "temporary secondary index",
                    reason: "byte index read returned a fixed-width primary".to_owned(),
                }),
            })
            .collect()
    }

    fn index_get_physical(
        &self,
        snapshot: CommitId,
        index: IndexId,
        index_key: &[u8],
    ) -> Result<Vec<PhysicalKey>> {
        self.validate_snapshot(snapshot)?;
        let state = self.indexes.get(&index).ok_or_else(|| {
            DbError::InvalidState(format!("secondary index {} does not exist", index.0))
        })?;
        Ok(index_members_at(state, snapshot, index_key))
    }

    pub fn index_scan(
        &self,
        snapshot: CommitId,
        index: IndexId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Key)>> {
        self.index_scan_physical(snapshot, index, start, end, limit)?
            .into_iter()
            .map(|(index_key, primary)| match primary {
                PhysicalKey::Fixed(primary) => Ok((index_key, primary)),
                PhysicalKey::Bytes(_) => Err(DbError::Corruption {
                    artifact: "temporary secondary index",
                    reason: "fixed index scan returned a variable-width primary".to_owned(),
                }),
            })
            .collect()
    }

    pub fn index_scan_bytes(
        &self,
        snapshot: CommitId,
        index: IndexId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.index_scan_physical(snapshot, index, start, end, limit)?
            .into_iter()
            .map(|(index_key, primary)| match primary {
                PhysicalKey::Bytes(primary) => Ok((index_key, primary)),
                PhysicalKey::Fixed(_) => Err(DbError::Corruption {
                    artifact: "temporary secondary index",
                    reason: "byte index scan returned a fixed-width primary".to_owned(),
                }),
            })
            .collect()
    }

    fn index_scan_physical(
        &self,
        snapshot: CommitId,
        index: IndexId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PhysicalKey)>> {
        self.validate_snapshot(snapshot)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let state = self.indexes.get(&index).ok_or_else(|| {
            DbError::InvalidState(format!("secondary index {} does not exist", index.0))
        })?;
        let mut keys: BTreeSet<Vec<u8>> = state.current.keys().cloned().collect();
        keys.extend(state.fragments.keys().cloned());
        let mut rows = Vec::new();
        for index_key in keys.range(start.to_vec()..end.to_vec()) {
            for primary in index_members_at(state, snapshot, index_key) {
                rows.push((index_key.clone(), primary));
                if rows.len() == limit {
                    return Ok(rows);
                }
            }
        }
        Ok(rows)
    }

    pub fn retain(&mut self, snapshot: CommitId) -> Result<()> {
        if snapshot.0 > self.commit.0 {
            return Err(DbError::SnapshotUnavailable(snapshot.0));
        }
        let count = self.retained.entry(snapshot).or_insert(0);
        *count = count
            .checked_add(1)
            .ok_or_else(|| DbError::InvalidState("retention lease count exhausted".to_owned()))?;
        Ok(())
    }

    pub fn release(&mut self, snapshot: CommitId) {
        let Some(count) = self.retained.get_mut(&snapshot) else {
            return;
        };
        if *count <= 1 {
            self.retained.remove(&snapshot);
        } else {
            *count -= 1;
        }
    }

    #[must_use]
    pub fn retained_snapshot_count(&self) -> usize {
        self.retained.len()
    }

    /// Return explicitly retained snapshot commits in ascending order.
    ///
    /// This is an observation of this store handle's retention leases. It is
    /// not a commit-history catalog and does not acquire or extend a lease.
    #[must_use]
    pub fn retained_snapshot_commits(&self) -> Vec<CommitId> {
        self.retained.keys().copied().collect()
    }

    pub fn checkpoint(&mut self, faults: &mut dyn FaultInjector) -> Result<()> {
        if self.recovery_required {
            return Err(DbError::RecoveryRequired);
        }
        let payload = self.encode_state()?;
        let generation = self.generation + 1;
        let data = artifact::data_path(&manifest_path(&self.config), generation);
        let page_bytes = artifact::write_pages(&data, generation, &payload, faults)?;
        self.metrics.packed_page_bytes = self.metrics.packed_page_bytes.saturating_add(page_bytes);
        self.metrics.syncs = self.metrics.syncs.saturating_add(1);
        // The range file is a derived fixed-width-key accelerator. Variable-
        // width keys remain authoritative in the checksummed state payload
        // until the range codec grows a byte-key representation. Write and
        // sync it before publishing the manifest so its partial coverage is
        // never mistaken for a complete checkpoint.
        let ranges = self.pack_current_ranges(generation, PackBudget::unlimited())?;
        let range_checksum = ranges.checksum();
        let range_file = range_path(&self.config, generation);
        ranges.write(&range_file, faults)?;
        self.metrics.packed_page_bytes = self
            .metrics
            .packed_page_bytes
            .saturating_add(ranges.report().bytes as u64);
        self.metrics.syncs = self.metrics.syncs.saturating_add(1);
        faults.check(FaultPoint::AfterWalSync)?;
        let manifest = Manifest {
            generation,
            commit: self.commit.0,
            logical_len: payload.len() as u64,
            payload_checksum: crc32c::crc32c(&payload),
            range_checksum,
        };
        artifact::publish_manifest(&manifest_path(&self.config), manifest, faults)?;
        self.metrics.manifest_bytes = self.metrics.manifest_bytes.saturating_add(44);
        self.metrics.syncs = self.metrics.syncs.saturating_add(2);
        faults.check(FaultPoint::AfterManifestPublish)?;
        let verified = artifact::read_manifest(&manifest_path(&self.config))?.ok_or_else(|| {
            DbError::Corruption {
                artifact: "manifest",
                reason: "published manifest disappeared".to_owned(),
            }
        })?;
        let verified_payload =
            artifact::read_pages(&data, verified.generation, verified.logical_len)?;
        let verified_ranges = PackedRange::read(&range_file, verified.generation)?;
        if verified.commit != self.commit.0
            || crc32c::crc32c(&verified_payload) != verified.payload_checksum
            || verified_ranges.checksum() != verified.range_checksum
        {
            return Err(DbError::Corruption {
                artifact: "checkpoint",
                reason: "verification failed".to_owned(),
            });
        }
        if generation > 1 {
            let _ = fs::remove_file(range_path(&self.config, generation - 1));
            let _ = fs::remove_file(artifact::data_path(
                &manifest_path(&self.config),
                generation - 1,
            ));
        }
        wal::truncate(&wal_path(&self.config), faults)?;
        artifact::sync_parent(&wal_path(&self.config))?;
        self.generation = generation;
        Ok(())
    }

    #[must_use]
    pub fn commit_id(&self) -> CommitId {
        self.commit
    }

    #[must_use]
    pub fn storage_identity(&self) -> StorageIdentity {
        self.identity
    }

    #[must_use]
    pub(crate) fn requires_recovery(&self) -> bool {
        self.recovery_required
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn metrics(&self) -> &DatabaseMetrics {
        &self.metrics
    }

    pub fn pack_current_ranges(&self, generation: u64, budget: PackBudget) -> Result<PackedRange> {
        // Physical byte keys are intentionally excluded from this legacy
        // fixed-width accelerator. The authoritative checkpoint payload still
        // contains them, and reads use the in-memory/replayed state.
        let entries: Vec<(Key, Vec<u8>)> = self
            .current
            .iter()
            .filter_map(|(key, value)| match key {
                PhysicalKey::Fixed(key) => Some((*key, value.clone())),
                PhysicalKey::Bytes(_) => None,
            })
            .collect();
        pack_sorted(generation, &entries, budget)
    }

    #[must_use]
    pub fn fragment_chain_len(&self, key: Key) -> usize {
        self.fragments
            .get(&PhysicalKey::fixed(key))
            .map_or(0, Vec::len)
    }

    #[must_use]
    pub fn secondary_index_ids(&self) -> Vec<IndexId> {
        self.indexes.keys().copied().collect()
    }

    pub fn secondary_index_unique(&self, index: IndexId) -> Result<bool> {
        self.indexes
            .get(&index)
            .map(|state| state.unique)
            .ok_or_else(|| {
                DbError::InvalidState(format!("secondary index {} does not exist", index.0))
            })
    }

    pub fn create_secondary_index(
        &mut self,
        index: IndexId,
        unique: bool,
        faults: &mut dyn FaultInjector,
    ) -> Result<CommitId> {
        self.commit_batch(
            self.commit,
            vec![Mutation::CreateIndex { index, unique }],
            faults,
        )
    }

    pub fn compact(&mut self) -> Result<CompactionReport> {
        self.compact_with_budget(CompactionBudget::unlimited())
    }

    pub fn compact_with_budget(&mut self, budget: CompactionBudget) -> Result<CompactionReport> {
        let mut faults = NoFaults;
        self.compact_with_budget_and_faults(budget, &mut faults)
    }

    /// Compact at most `max_keys` logical row or index histories in one pass.
    ///
    /// This is the temporary backend's implementation of the
    /// project-facing maintenance work-unit budget. Row and index histories
    /// share the limit, so the returned considered counts never exceed it in
    /// total.
    pub fn compact_with_key_budget(&mut self, max_keys: usize) -> Result<CompactionReport> {
        let mut faults = NoFaults;
        self.compact_with_key_budget_and_faults(max_keys, &mut faults)
    }

    pub fn compact_with_budget_and_faults(
        &mut self,
        budget: CompactionBudget,
        faults: &mut dyn FaultInjector,
    ) -> Result<CompactionReport> {
        let row_keys: BTreeSet<PhysicalKey> = self
            .fragments
            .keys()
            .take(budget.max_row_keys)
            .cloned()
            .collect();
        let index_keys: BTreeSet<(IndexId, Vec<u8>)> = self
            .indexes
            .iter()
            .flat_map(|(index, state)| state.fragments.keys().cloned().map(|key| (*index, key)))
            .take(budget.max_index_keys)
            .collect();
        self.compact_histories_for(&row_keys, &index_keys, faults)
    }

    pub fn compact_with_key_budget_and_faults(
        &mut self,
        max_keys: usize,
        faults: &mut dyn FaultInjector,
    ) -> Result<CompactionReport> {
        let row_keys: BTreeSet<PhysicalKey> =
            self.fragments.keys().take(max_keys).cloned().collect();
        let remaining = max_keys.saturating_sub(row_keys.len());
        let index_keys: BTreeSet<(IndexId, Vec<u8>)> = self
            .indexes
            .iter()
            .flat_map(|(index, state)| state.fragments.keys().cloned().map(|key| (*index, key)))
            .take(remaining)
            .collect();
        self.compact_histories_for(&row_keys, &index_keys, faults)
    }

    fn compact_histories_for(
        &mut self,
        row_keys: &BTreeSet<PhysicalKey>,
        index_keys: &BTreeSet<(IndexId, Vec<u8>)>,
        faults: &mut dyn FaultInjector,
    ) -> Result<CompactionReport> {
        let mut protected: BTreeSet<CommitId> = self.retained.keys().copied().collect();
        protected.extend(
            self.active_snapshots
                .lock()
                .expect("active snapshot pins lock poisoned")
                .keys()
                .copied(),
        );
        let mut report = CompactionReport {
            row_keys_considered: row_keys.len(),
            index_keys_considered: index_keys.len(),
            ..CompactionReport::default()
        };
        for key in row_keys {
            if let Some(history) = self.fragments.get_mut(key) {
                let before = history.len();
                compact_history(history, &protected, self.commit)?;
                report.row_fragments_reclaimed += before.saturating_sub(history.len());
                faults.check(FaultPoint::DuringCompaction)?;
            }
        }
        let mut rebuild = BTreeSet::new();
        for (index, key) in index_keys {
            if let Some(state) = self.indexes.get_mut(index)
                && let Some(history) = state.fragments.get_mut(key)
            {
                let before = history.len();
                compact_index_history(history, &protected, self.commit)?;
                report.index_fragments_reclaimed += before.saturating_sub(history.len());
                rebuild.insert(*index);
                faults.check(FaultPoint::DuringCompaction)?;
            }
        }
        for index in rebuild {
            if let Some(state) = self.indexes.get_mut(&index) {
                rebuild_index_current(state, self.commit);
            }
        }
        if report.row_fragments_reclaimed != 0 || report.index_fragments_reclaimed != 0 {
            // The catalog remains useful for diagnostics, but the physical
            // fragments needed to reconstruct every old state are gone.
            self.history_complete = false;
        }
        if report.row_keys_considered > 0 || report.index_keys_considered > 0 {
            self.metrics.compaction_runs = self.metrics.compaction_runs.saturating_add(1);
            self.metrics.fragments_reclaimed = self.metrics.fragments_reclaimed.saturating_add(
                (report.row_fragments_reclaimed + report.index_fragments_reclaimed) as u64,
            );
        }
        Ok(report)
    }

    fn validate_mutations(&self, mutations: &[Mutation]) -> Result<()> {
        let mut indexes = self.indexes.clone();
        for mutation in mutations {
            match mutation {
                Mutation::Put { .. } | Mutation::Delete { .. } => {}
                Mutation::BytePut { key, .. } | Mutation::ByteDelete { key } => {
                    PhysicalKey::bytes(key.clone())?;
                }
                Mutation::CreateIndex { index, unique } => {
                    if indexes
                        .insert(
                            *index,
                            IndexState {
                                unique: *unique,
                                current: BTreeMap::new(),
                                fragments: BTreeMap::new(),
                            },
                        )
                        .is_some()
                    {
                        return Err(DbError::InvalidState(format!(
                            "secondary index {} already exists",
                            index.0
                        )));
                    }
                }
                Mutation::IndexPut {
                    index,
                    index_key,
                    primary,
                } => {
                    let state = indexes.get_mut(index).ok_or_else(|| {
                        DbError::InvalidState(format!("secondary index {} does not exist", index.0))
                    })?;
                    let entries = state.current.entry(index_key.clone()).or_default();
                    let primary = PhysicalKey::fixed(*primary);
                    if state.unique && entries.iter().any(|candidate| candidate != &primary) {
                        return Err(DbError::UniqueViolation {
                            index: index.0,
                            key: index_key.clone(),
                        });
                    }
                    entries.insert(primary);
                }
                Mutation::IndexDelete {
                    index,
                    index_key,
                    primary,
                } => {
                    let state = indexes.get_mut(index).ok_or_else(|| {
                        DbError::InvalidState(format!("secondary index {} does not exist", index.0))
                    })?;
                    let primary = PhysicalKey::fixed(*primary);
                    if let Some(entries) = state.current.get_mut(index_key) {
                        entries.remove(&primary);
                        if entries.is_empty() {
                            state.current.remove(index_key);
                        }
                    }
                }
                Mutation::ByteIndexPut {
                    index,
                    index_key,
                    primary,
                } => {
                    let state = indexes.get_mut(index).ok_or_else(|| {
                        DbError::InvalidState(format!("secondary index {} does not exist", index.0))
                    })?;
                    let primary = PhysicalKey::bytes(primary.clone())?;
                    let entries = state.current.entry(index_key.clone()).or_default();
                    if state.unique && entries.iter().any(|candidate| candidate != &primary) {
                        return Err(DbError::UniqueViolation {
                            index: index.0,
                            key: index_key.clone(),
                        });
                    }
                    entries.insert(primary);
                }
                Mutation::ByteIndexDelete {
                    index,
                    index_key,
                    primary,
                } => {
                    let state = indexes.get_mut(index).ok_or_else(|| {
                        DbError::InvalidState(format!("secondary index {} does not exist", index.0))
                    })?;
                    let primary = PhysicalKey::bytes(primary.clone())?;
                    if let Some(entries) = state.current.get_mut(index_key) {
                        entries.remove(&primary);
                        if entries.is_empty() {
                            state.current.remove(index_key);
                        }
                    }
                }
                Mutation::RecordAttempt { attempt, digest } => {
                    if let Some(existing) = self.attempts.get(attempt)
                        && existing.digest != *digest
                    {
                        return Err(DbError::IdempotencyConflict {
                            attempt: *attempt,
                            existing_digest: existing.digest,
                            requested_digest: *digest,
                        });
                    }
                }
                Mutation::ForgetAttempt { .. } => {}
            }
        }
        Ok(())
    }

    fn apply(&mut self, commit: CommitId, mutations: &[Mutation]) -> Result<()> {
        self.validate_mutations(mutations)?;
        for mutation in mutations {
            match mutation {
                Mutation::Put { key, value } => {
                    let key = PhysicalKey::fixed(*key);
                    self.current.insert(key.clone(), value.clone());
                    self.metrics.fragment_bytes = self
                        .metrics
                        .fragment_bytes
                        .saturating_add((21 + value.len()) as u64);
                    self.fragments.entry(key).or_default().push(Fragment {
                        commit,
                        value: Some(value.clone()),
                    });
                }
                Mutation::Delete { key } => {
                    let key = PhysicalKey::fixed(*key);
                    self.current.remove(&key);
                    self.metrics.fragment_bytes = self.metrics.fragment_bytes.saturating_add(17);
                    self.fragments.entry(key).or_default().push(Fragment {
                        commit,
                        value: None,
                    });
                }
                Mutation::BytePut { key, value } => {
                    let key = PhysicalKey::bytes(key.clone())?;
                    self.current.insert(key.clone(), value.clone());
                    self.metrics.fragment_bytes = self
                        .metrics
                        .fragment_bytes
                        .saturating_add((21 + key_len(&key) + value.len()) as u64);
                    self.fragments.entry(key).or_default().push(Fragment {
                        commit,
                        value: Some(value.clone()),
                    });
                }
                Mutation::ByteDelete { key } => {
                    let key = PhysicalKey::bytes(key.clone())?;
                    self.current.remove(&key);
                    self.metrics.fragment_bytes = self
                        .metrics
                        .fragment_bytes
                        .saturating_add((17 + key_len(&key)) as u64);
                    self.fragments.entry(key).or_default().push(Fragment {
                        commit,
                        value: None,
                    });
                }
                Mutation::CreateIndex { index, unique } => {
                    self.indexes.insert(
                        *index,
                        IndexState {
                            unique: *unique,
                            current: BTreeMap::new(),
                            fragments: BTreeMap::new(),
                        },
                    );
                }
                Mutation::IndexPut {
                    index,
                    index_key,
                    primary,
                } => {
                    let state = self.indexes.get_mut(index).ok_or_else(|| {
                        DbError::InvalidState(format!("secondary index {} does not exist", index.0))
                    })?;
                    state
                        .current
                        .entry(index_key.clone())
                        .or_default()
                        .insert(PhysicalKey::fixed(*primary));
                    state
                        .fragments
                        .entry(index_key.clone())
                        .or_default()
                        .push(IndexFragment {
                            commit,
                            primary: PhysicalKey::fixed(*primary),
                            present: true,
                        });
                }
                Mutation::IndexDelete {
                    index,
                    index_key,
                    primary,
                } => {
                    let state = self.indexes.get_mut(index).ok_or_else(|| {
                        DbError::InvalidState(format!("secondary index {} does not exist", index.0))
                    })?;
                    let primary = PhysicalKey::fixed(*primary);
                    if let Some(entries) = state.current.get_mut(index_key) {
                        entries.remove(&primary);
                        if entries.is_empty() {
                            state.current.remove(index_key);
                        }
                    }
                    state
                        .fragments
                        .entry(index_key.clone())
                        .or_default()
                        .push(IndexFragment {
                            commit,
                            primary,
                            present: false,
                        });
                }
                Mutation::ByteIndexPut {
                    index,
                    index_key,
                    primary,
                } => {
                    let state = self.indexes.get_mut(index).ok_or_else(|| {
                        DbError::InvalidState(format!("secondary index {} does not exist", index.0))
                    })?;
                    let primary = PhysicalKey::bytes(primary.clone())?;
                    state
                        .current
                        .entry(index_key.clone())
                        .or_default()
                        .insert(primary.clone());
                    state
                        .fragments
                        .entry(index_key.clone())
                        .or_default()
                        .push(IndexFragment {
                            commit,
                            primary,
                            present: true,
                        });
                }
                Mutation::ByteIndexDelete {
                    index,
                    index_key,
                    primary,
                } => {
                    let state = self.indexes.get_mut(index).ok_or_else(|| {
                        DbError::InvalidState(format!("secondary index {} does not exist", index.0))
                    })?;
                    let primary = PhysicalKey::bytes(primary.clone())?;
                    if let Some(entries) = state.current.get_mut(index_key) {
                        entries.remove(&primary);
                        if entries.is_empty() {
                            state.current.remove(index_key);
                        }
                    }
                    state
                        .fragments
                        .entry(index_key.clone())
                        .or_default()
                        .push(IndexFragment {
                            commit,
                            primary,
                            present: false,
                        });
                }
                Mutation::RecordAttempt { attempt, digest } => {
                    self.attempts.insert(
                        *attempt,
                        AttemptRecord {
                            attempt: *attempt,
                            commit,
                            digest: *digest,
                        },
                    );
                }
                Mutation::ForgetAttempt { attempt } => {
                    self.attempts.remove(attempt);
                }
            }
        }
        self.commit = commit;
        self.published_commits.insert(commit);
        Ok(())
    }

    fn validate_snapshot(&self, snapshot: CommitId) -> Result<()> {
        if snapshot == self.commit
            || self.retained.contains_key(&snapshot)
            || self
                .active_snapshots
                .lock()
                .expect("active snapshot pins lock poisoned")
                .contains_key(&snapshot)
        {
            return Ok(());
        }
        Err(DbError::SnapshotUnavailable(snapshot.0))
    }

    fn encode_state(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&STATE_MAGIC);
        bytes.extend_from_slice(&STATE_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        put_u32(
            &mut bytes,
            u32::try_from(self.current.len())
                .map_err(|_| DbError::InvalidState("too many rows".to_owned()))?,
        );
        for (key, value) in &self.current {
            put_physical_key(&mut bytes, key)?;
            put_bytes(&mut bytes, value)?;
        }
        put_u32(
            &mut bytes,
            u32::try_from(self.fragments.len())
                .map_err(|_| DbError::InvalidState("too many fragment keys".to_owned()))?,
        );
        for (key, history) in &self.fragments {
            put_physical_key(&mut bytes, key)?;
            put_u32(
                &mut bytes,
                u32::try_from(history.len())
                    .map_err(|_| DbError::InvalidState("too many fragments".to_owned()))?,
            );
            for fragment in history {
                bytes.extend_from_slice(&fragment.commit.0.to_le_bytes());
                match &fragment.value {
                    Some(value) => {
                        bytes.push(1);
                        put_bytes(&mut bytes, value)?;
                    }
                    None => bytes.push(2),
                }
            }
        }
        put_u32(
            &mut bytes,
            u32::try_from(self.indexes.len())
                .map_err(|_| DbError::InvalidState("too many secondary indexes".to_owned()))?,
        );
        for (index, state) in &self.indexes {
            bytes.extend_from_slice(&index.0.to_le_bytes());
            bytes.push(u8::from(state.unique));
            put_u32(
                &mut bytes,
                u32::try_from(state.fragments.len())
                    .map_err(|_| DbError::InvalidState("too many index keys".to_owned()))?,
            );
            for (index_key, history) in &state.fragments {
                put_bytes(&mut bytes, index_key)?;
                put_u32(
                    &mut bytes,
                    u32::try_from(history.len()).map_err(|_| {
                        DbError::InvalidState("too many index fragments".to_owned())
                    })?,
                );
                for fragment in history {
                    bytes.extend_from_slice(&fragment.commit.0.to_le_bytes());
                    put_physical_key(&mut bytes, &fragment.primary)?;
                    bytes.push(u8::from(fragment.present));
                }
            }
        }
        put_u32(
            &mut bytes,
            u32::try_from(self.retained.len())
                .map_err(|_| DbError::InvalidState("too many retained snapshots".to_owned()))?,
        );
        for snapshot in self.retained.keys() {
            bytes.extend_from_slice(&snapshot.0.to_le_bytes());
        }
        bytes.extend_from_slice(&self.commit.0.to_le_bytes());
        put_u32(
            &mut bytes,
            u32::try_from(self.attempts.len())
                .map_err(|_| DbError::InvalidState("too many transaction attempts".to_owned()))?,
        );
        for record in self.attempts.values() {
            bytes.extend_from_slice(&record.attempt.0);
            bytes.extend_from_slice(&record.commit.0.to_le_bytes());
            bytes.extend_from_slice(&record.digest);
        }
        bytes.push(u8::from(self.history_complete));
        put_u32(
            &mut bytes,
            u32::try_from(self.published_commits.len())
                .map_err(|_| DbError::InvalidState("too many published commits".to_owned()))?,
        );
        for commit in &self.published_commits {
            bytes.extend_from_slice(&commit.0.to_le_bytes());
        }
        Ok(bytes)
    }

    fn decode_state(&mut self, bytes: &[u8]) -> Result<()> {
        let mut cursor = Cursor::new(bytes);
        if cursor.fixed::<4>()? != STATE_MAGIC {
            return Err(cursor.corrupt("unsupported temporary state version"));
        }
        let version = cursor.u16()?;
        if version != STATE_VERSION && version != LEGACY_STATE_VERSION {
            return Err(cursor.corrupt("unsupported temporary state version"));
        }
        let _reserved = cursor.u16()?;
        self.current.clear();
        self.fragments.clear();
        self.retained.clear();
        self.attempts.clear();
        self.published_commits.clear();
        for _ in 0..cursor.u32()? {
            let key = cursor.physical_key()?;
            self.current.insert(key, cursor.bytes()?);
        }
        for _ in 0..cursor.u32()? {
            let key = cursor.physical_key()?;
            let mut history = Vec::new();
            for _ in 0..cursor.u32()? {
                let commit = CommitId(cursor.u64()?);
                let value = match cursor.byte()? {
                    1 => Some(cursor.bytes()?),
                    2 => None,
                    _ => return Err(cursor.corrupt("unknown fragment tag")),
                };
                history.push(Fragment { commit, value });
            }
            self.fragments.insert(key, history);
        }
        self.indexes.clear();
        for _ in 0..cursor.u32()? {
            let index = IndexId(cursor.u64()?);
            let unique = match cursor.byte()? {
                0 => false,
                1 => true,
                _ => return Err(cursor.corrupt("invalid index uniqueness flag")),
            };
            let mut state = IndexState {
                unique,
                current: BTreeMap::new(),
                fragments: BTreeMap::new(),
            };
            for _ in 0..cursor.u32()? {
                let index_key = cursor.bytes()?;
                let mut history = Vec::new();
                for _ in 0..cursor.u32()? {
                    let commit = CommitId(cursor.u64()?);
                    let primary = cursor.physical_key()?;
                    let present = match cursor.byte()? {
                        0 => false,
                        1 => true,
                        _ => return Err(cursor.corrupt("invalid index fragment flag")),
                    };
                    history.push(IndexFragment {
                        commit,
                        primary,
                        present,
                    });
                }
                state.fragments.insert(index_key, history);
            }
            self.indexes.insert(index, state);
        }
        for _ in 0..cursor.u32()? {
            self.retained.insert(CommitId(cursor.u64()?), 1);
        }
        self.commit = CommitId(cursor.u64()?);
        if cursor.remaining() > 0 {
            for _ in 0..cursor.u32()? {
                let mut attempt = [0; 16];
                attempt.copy_from_slice(&cursor.fixed::<16>()?);
                let record = AttemptRecord {
                    attempt: TransactionAttemptId(attempt),
                    commit: CommitId(cursor.u64()?),
                    digest: cursor.fixed::<32>()?,
                };
                self.attempts.insert(record.attempt, record);
            }
        }
        if version == STATE_VERSION {
            self.history_complete = match cursor.byte()? {
                0 => false,
                1 => true,
                _ => return Err(cursor.corrupt("invalid commit-history completeness flag")),
            };
            let count = cursor.u32()?;
            let mut previous: Option<CommitId> = None;
            for _ in 0..count {
                let commit = CommitId(cursor.u64()?);
                if commit.0 > self.commit.0
                    || previous.is_some_and(|previous| previous.0.checked_add(1) != Some(commit.0))
                {
                    return Err(cursor.corrupt("published commit catalog is not ordered"));
                }
                previous = Some(commit);
                self.published_commits.insert(commit);
            }
            if self.history_complete
                && (self.published_commits.first().copied() != Some(CommitId(0))
                    || self.published_commits.last().copied() != Some(self.commit))
            {
                return Err(cursor.corrupt(
                    "complete published commit catalog does not cover the database frontier",
                ));
            }
        } else {
            // The v2 payload has no authoritative commit catalog. Derive a
            // useful observation for ordinary debugging, but keep the
            // history-complete flag false so FullHistory cannot guess.
            self.history_complete = false;
            self.published_commits.insert(CommitId(0));
            self.published_commits.insert(self.commit);
            self.published_commits.extend(
                self.fragments
                    .values()
                    .flat_map(|history| history.iter().map(|fragment| fragment.commit)),
            );
            self.published_commits
                .extend(self.indexes.values().flat_map(|state| {
                    state
                        .fragments
                        .values()
                        .flat_map(|history| history.iter().map(|fragment| fragment.commit))
                }));
        }
        for state in self.indexes.values_mut() {
            rebuild_index_current(state, self.commit);
        }
        cursor.finish()
    }
}

fn index_members_at(state: &IndexState, snapshot: CommitId, index_key: &[u8]) -> Vec<PhysicalKey> {
    let Some(history) = state.fragments.get(index_key) else {
        return Vec::new();
    };
    let mut seen = BTreeSet::new();
    let mut members = BTreeSet::new();
    for fragment in history.iter().rev() {
        if fragment.commit <= snapshot && seen.insert(fragment.primary.clone()) && fragment.present
        {
            members.insert(fragment.primary.clone());
        }
    }
    members.into_iter().collect()
}

fn rebuild_index_current(state: &mut IndexState, commit: CommitId) {
    state.current.clear();
    for index_key in state.fragments.keys() {
        for primary in index_members_at(state, commit, index_key) {
            state
                .current
                .entry(index_key.clone())
                .or_default()
                .insert(primary);
        }
    }
}

fn compact_history(
    history: &mut Vec<Fragment>,
    retained: &BTreeSet<CommitId>,
    current: CommitId,
) -> Result<()> {
    if history.len() <= MAX_FRAGMENT_CHAIN {
        return Ok(());
    }
    let mut keep = BTreeMap::<CommitId, Fragment>::new();
    for snapshot in retained.iter().copied().chain(std::iter::once(current)) {
        if let Some(fragment) = history
            .iter()
            .rev()
            .find(|fragment| fragment.commit <= snapshot)
        {
            keep.insert(fragment.commit, fragment.clone());
        }
    }
    if keep.len() > MAX_FRAGMENT_CHAIN {
        return Err(DbError::FragmentDebtExceeded {
            limit: MAX_FRAGMENT_CHAIN,
        });
    }
    *history = keep.into_values().collect();
    Ok(())
}

fn compact_index_history(
    history: &mut Vec<IndexFragment>,
    retained: &BTreeSet<CommitId>,
    current: CommitId,
) -> Result<()> {
    if history.len() <= MAX_FRAGMENT_CHAIN {
        return Ok(());
    }
    let primaries: BTreeSet<PhysicalKey> = history
        .iter()
        .map(|fragment| fragment.primary.clone())
        .collect();
    let mut keep = BTreeMap::<(CommitId, PhysicalKey), IndexFragment>::new();
    for snapshot in retained.iter().copied().chain(std::iter::once(current)) {
        for primary in &primaries {
            if let Some(fragment) = history
                .iter()
                .rev()
                .find(|fragment| fragment.primary == *primary && fragment.commit <= snapshot)
            {
                keep.insert(
                    (fragment.commit, fragment.primary.clone()),
                    fragment.clone(),
                );
            }
        }
    }
    // An index key can legitimately reference many primary rows. The debt
    // bound applies to each primary's version chain, not to the aggregate
    // membership list for one encoded index value.
    let max_primary_chain = primaries
        .iter()
        .map(|primary| {
            keep.values()
                .filter(|fragment| fragment.primary == *primary)
                .count()
        })
        .max()
        .unwrap_or_default();
    if max_primary_chain > MAX_FRAGMENT_CHAIN {
        return Err(DbError::FragmentDebtExceeded {
            limit: MAX_FRAGMENT_CHAIN,
        });
    }
    *history = keep.into_values().collect();
    Ok(())
}

fn wal_path(config: &DatabaseConfig) -> PathBuf {
    config.directory.join(WAL_NAME)
}

fn identity_path(directory: &Path) -> PathBuf {
    directory.join(IDENTITY_NAME)
}

fn load_or_create_identity(directory: &Path, persist_if_missing: bool) -> Result<StorageIdentity> {
    let path = identity_path(directory);
    match fs::read(&path) {
        Ok(bytes) => decode_identity(&bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let identity = generated_identity(directory);
            if persist_if_missing {
                write_identity(&path, directory, identity)?;
            }
            Ok(identity)
        }
        Err(source) => Err(io_error("read database identity", source)),
    }
}

fn generated_identity(directory: &Path) -> StorageIdentity {
    let mut entropy = Vec::new();
    entropy.extend_from_slice(directory.to_string_lossy().as_bytes());
    entropy.extend_from_slice(
        &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    entropy.extend_from_slice(&std::process::id().to_le_bytes());
    let digest = Sha256::digest(entropy);
    let mut database_id = [0_u8; 16];
    database_id.copy_from_slice(&digest[..16]);
    let mut history_bytes = [0_u8; 8];
    history_bytes.copy_from_slice(&digest[16..24]);
    let history_id = u64::from_le_bytes(history_bytes).max(1);
    StorageIdentity {
        database_id,
        history_id,
    }
}

fn write_identity(path: &Path, directory: &Path, identity: StorageIdentity) -> Result<()> {
    let mut bytes = [0_u8; IDENTITY_BYTES];
    bytes[..4].copy_from_slice(&IDENTITY_MAGIC);
    bytes[4..8].copy_from_slice(&IDENTITY_VERSION.to_le_bytes());
    bytes[8..24].copy_from_slice(&identity.database_id);
    bytes[24..32].copy_from_slice(&identity.history_id.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..32]);
    bytes[32..36].copy_from_slice(&checksum.to_le_bytes());
    let temporary_path = path.with_file_name(format!("{IDENTITY_NAME}.tmp-{}", std::process::id()));
    let mut file = File::create(&temporary_path)
        .map_err(|source| io_error("create database identity", source))?;
    file.write_all(&bytes)
        .map_err(|source| io_error("write database identity", source))?;
    file.sync_all()
        .map_err(|source| io_error("sync database identity", source))?;
    fs::rename(&temporary_path, path)
        .map_err(|source| io_error("publish database identity", source))?;
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error("sync database identity directory", source))
}

fn decode_identity(bytes: &[u8]) -> Result<StorageIdentity> {
    if bytes.len() != IDENTITY_BYTES || bytes[..4] != IDENTITY_MAGIC {
        return Err(identity_corruption("invalid identity header"));
    }
    if u32::from_le_bytes(bytes[4..8].try_into().expect("identity version width"))
        != IDENTITY_VERSION
    {
        return Err(identity_corruption("unsupported identity version"));
    }
    let expected = u32::from_le_bytes(bytes[32..36].try_into().expect("identity checksum width"));
    if crc32c::crc32c(&bytes[..32]) != expected {
        return Err(identity_corruption("identity checksum mismatch"));
    }
    let mut database_id = [0_u8; 16];
    database_id.copy_from_slice(&bytes[8..24]);
    let history_id = u64::from_le_bytes(bytes[24..32].try_into().expect("history ID width"));
    if history_id == 0 {
        return Err(identity_corruption("history ID is zero"));
    }
    Ok(StorageIdentity {
        database_id,
        history_id,
    })
}

fn identity_corruption(reason: &str) -> DbError {
    DbError::Corruption {
        artifact: "database identity",
        reason: reason.to_owned(),
    }
}

fn manifest_path(config: &DatabaseConfig) -> PathBuf {
    config.directory.join(MANIFEST_NAME)
}

fn range_path(config: &DatabaseConfig, generation: u64) -> PathBuf {
    config
        .directory
        .join(format!("omendb.ranges-{generation:016x}.pages"))
}

fn io_error(operation: &'static str, source: std::io::Error) -> DbError {
    DbError::Io { operation, source }
}

fn acquire_writer_lock(directory: &Path) -> Result<File> {
    let path = directory.join(LOCK_NAME);
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| io_error("open database lock", source))?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(file),
        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => {
            Err(DbError::StorageBusy {
                operation: "open",
                reason: format!(
                    "database directory is already owned: {}",
                    directory.display()
                ),
            })
        }
        Err(source) => Err(io_error("lock database", source)),
    }
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_key(bytes: &mut Vec<u8>, key: Key) {
    bytes.extend_from_slice(&key.0);
}

fn put_physical_key(bytes: &mut Vec<u8>, key: &PhysicalKey) -> Result<()> {
    match key {
        PhysicalKey::Fixed(key) => {
            bytes.push(1);
            put_key(bytes, *key);
        }
        PhysicalKey::Bytes(key) => {
            bytes.push(2);
            put_bytes(bytes, key)?;
        }
    }
    Ok(())
}

fn key_len(key: &PhysicalKey) -> usize {
    match key {
        PhysicalKey::Fixed(_) => 16,
        PhysicalKey::Bytes(key) => key.len(),
    }
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| DbError::ValueTooLarge(value.len()))?;
    put_u32(bytes, length);
    bytes.extend_from_slice(value);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn byte(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| self.corrupt("missing byte"))?;
        self.offset += 1;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32> {
        let end = self.offset + 4;
        let value = u32::from_le_bytes(
            self.bytes
                .get(self.offset..end)
                .ok_or_else(|| self.corrupt("missing u32"))?
                .try_into()
                .expect("u32 width"),
        );
        self.offset = end;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16> {
        let end = self.offset + 2;
        let value = u16::from_le_bytes(
            self.bytes
                .get(self.offset..end)
                .ok_or_else(|| self.corrupt("missing u16"))?
                .try_into()
                .expect("u16 width"),
        );
        self.offset = end;
        Ok(value)
    }

    fn u64(&mut self) -> Result<u64> {
        let end = self.offset + 8;
        let value = u64::from_le_bytes(
            self.bytes
                .get(self.offset..end)
                .ok_or_else(|| self.corrupt("missing u64"))?
                .try_into()
                .expect("u64 width"),
        );
        self.offset = end;
        Ok(value)
    }

    fn key(&mut self) -> Result<Key> {
        let end = self.offset + 16;
        let key = Key(self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| self.corrupt("missing key"))?
            .try_into()
            .expect("key width"));
        self.offset = end;
        Ok(key)
    }

    fn physical_key(&mut self) -> Result<PhysicalKey> {
        match self.byte()? {
            1 => Ok(PhysicalKey::Fixed(self.key()?)),
            2 => PhysicalKey::bytes(self.bytes()?),
            _ => Err(self.corrupt("unknown physical-key tag")),
        }
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let length = self.u32()? as usize;
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| self.corrupt("value length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| self.corrupt("truncated value"))?
            .to_vec();
        self.offset = end;
        Ok(value)
    }

    fn finish(self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(self.corrupt("trailing checkpoint bytes"))
        }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        let end = self.offset + N;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| self.corrupt("missing fixed-width field"))?
            .try_into()
            .expect("fixed field width");
        self.offset = end;
        Ok(value)
    }

    fn corrupt(&self, reason: &str) -> DbError {
        DbError::Corruption {
            artifact: "checkpoint state",
            reason: reason.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    use super::{Database, DatabaseConfig};
    use crate::fault::{FailOnce, FaultPoint, NoFaults};
    use crate::model::{CommitId, IndexId, Key, Mutation};
    use crate::packed::{PackBudget, pack_sorted};
    use crate::{DbError, TransactionAttemptId};

    fn database(directory: &tempfile::TempDir) -> Database {
        Database::create(DatabaseConfig {
            directory: directory.path().to_path_buf(),
        })
        .expect("create")
    }

    #[test]
    fn atomic_commit_and_retained_snapshot_reads() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        let key = Key::new(1, 1);
        let first = db
            .commit(
                vec![Mutation::Put {
                    key,
                    value: b"old".to_vec(),
                }],
                &mut NoFaults,
            )
            .expect("commit");
        db.retain(first).expect("retain");
        let second = db
            .commit(
                vec![Mutation::Put {
                    key,
                    value: b"new".to_vec(),
                }],
                &mut NoFaults,
            )
            .expect("commit");
        assert_eq!(db.get(first, key).expect("old"), Some(b"old".to_vec()));
        assert_eq!(db.get(second, key).expect("new"), Some(b"new".to_vec()));
        assert_eq!(
            db.scan(second, Key([0; 16]), Key([0xff; 16]), 10)
                .expect("scan")
                .len(),
            1
        );
        assert_eq!(db.fragment_chain_len(key), 2);
    }

    #[test]
    fn transaction_reads_own_staged_row_writes() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        let first = Key::new(1, 1);
        let second = Key::new(1, 2);
        db.commit(
            vec![Mutation::Put {
                key: first,
                value: b"old".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("seed");

        let mut transaction = db.begin();
        assert_eq!(
            transaction.get(&db, first).expect("committed read"),
            Some(b"old".to_vec())
        );
        transaction.put(first, b"new".to_vec());
        transaction.put(second, b"second".to_vec());
        assert_eq!(
            transaction.get(&db, first).expect("staged update"),
            Some(b"new".to_vec())
        );
        assert_eq!(
            transaction
                .scan(&db, Key([0; 16]), Key([0xff; 16]), 10)
                .expect("staged scan"),
            vec![(first, b"new".to_vec()), (second, b"second".to_vec())]
        );
        transaction.delete(first);
        assert_eq!(transaction.get(&db, first).expect("staged delete"), None);
        assert_eq!(
            transaction
                .scan(&db, Key([0; 16]), Key([0xff; 16]), 10)
                .expect("scan after delete"),
            vec![(second, b"second".to_vec())]
        );
    }

    #[test]
    fn active_transaction_pins_snapshot_during_compaction() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        let key = Key::new(1, 3);
        let first = db
            .commit(
                vec![Mutation::Put {
                    key,
                    value: b"old".to_vec(),
                }],
                &mut NoFaults,
            )
            .expect("seed");
        let mut transaction = db.begin();

        for value in 0..65 {
            db.commit(
                vec![Mutation::Put {
                    key,
                    value: value.to_string().into_bytes(),
                }],
                &mut NoFaults,
            )
            .expect("update");
        }
        db.compact().expect("compact");

        assert_eq!(
            transaction.get(&db, key).expect("pinned historical read"),
            Some(b"old".to_vec())
        );
        drop(transaction);
        assert!(matches!(
            db.get(first, key),
            Err(DbError::SnapshotUnavailable(snapshot)) if snapshot == first.0
        ));
    }

    #[test]
    fn transaction_reads_own_staged_index_membership() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        let index = IndexId(51);
        let first = Key::new(51, 1);
        let second = Key::new(51, 2);
        db.commit(
            vec![Mutation::CreateIndex {
                index,
                unique: false,
            }],
            &mut NoFaults,
        )
        .expect("create index");

        let mut transaction = db.begin();
        transaction.index_put(index, b"same".to_vec(), first);
        transaction.index_put(index, b"same".to_vec(), second);
        assert_eq!(
            transaction
                .index_get(&db, index, b"same".to_vec())
                .expect("staged index lookup"),
            vec![first, second]
        );
        assert_eq!(
            transaction
                .index_scan(&db, index, b"s".to_vec(), b"t".to_vec(), 10)
                .expect("staged index scan"),
            vec![(b"same".to_vec(), first), (b"same".to_vec(), second)]
        );
        transaction.index_delete(index, b"same".to_vec(), first);
        assert_eq!(
            transaction
                .index_get(&db, index, b"same".to_vec())
                .expect("staged index delete"),
            vec![second]
        );
    }

    #[test]
    fn byte_rows_and_indexes_survive_snapshot_checkpoint_and_reopen() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        let index = IndexId(52);
        let first = b"table/tenant-a/row-1".to_vec();
        let second = b"table/tenant-a/row-2".to_vec();
        db.commit(
            vec![
                Mutation::CreateIndex {
                    index,
                    unique: false,
                },
                Mutation::BytePut {
                    key: first.clone(),
                    value: b"old".to_vec(),
                },
                Mutation::BytePut {
                    key: second.clone(),
                    value: b"second".to_vec(),
                },
                Mutation::ByteIndexPut {
                    index,
                    index_key: b"tenant-a".to_vec(),
                    primary: first.clone(),
                },
                Mutation::ByteIndexPut {
                    index,
                    index_key: b"tenant-a".to_vec(),
                    primary: second.clone(),
                },
            ],
            &mut NoFaults,
        )
        .expect("seed byte state");
        let retained = db.commit_id();
        db.retain(retained).expect("retain");

        let current = db
            .commit(
                vec![
                    Mutation::BytePut {
                        key: first.clone(),
                        value: b"new".to_vec(),
                    },
                    Mutation::ByteDelete {
                        key: second.clone(),
                    },
                    Mutation::ByteIndexDelete {
                        index,
                        index_key: b"tenant-a".to_vec(),
                        primary: first.clone(),
                    },
                    Mutation::ByteIndexDelete {
                        index,
                        index_key: b"tenant-a".to_vec(),
                        primary: second.clone(),
                    },
                    Mutation::ByteIndexPut {
                        index,
                        index_key: b"tenant-b".to_vec(),
                        primary: first.clone(),
                    },
                ],
                &mut NoFaults,
            )
            .expect("update byte state");
        assert_eq!(
            db.get_bytes(current, first.clone()).expect("current first"),
            Some(b"new".to_vec())
        );
        assert_eq!(
            db.get_bytes(current, second.clone())
                .expect("current second"),
            None
        );
        assert_eq!(
            db.scan_bytes(retained, b"table/".to_vec(), b"table0".to_vec(), 10,)
                .expect("retained scan"),
            vec![
                (first.clone(), b"old".to_vec()),
                (second.clone(), b"second".to_vec()),
            ]
        );
        assert_eq!(
            db.index_get_bytes(current, index, b"tenant-a")
                .expect("old index"),
            Vec::<Vec<u8>>::new()
        );
        assert_eq!(
            db.index_get_bytes(current, index, b"tenant-b")
                .expect("new index"),
            vec![first.clone()]
        );

        db.checkpoint(&mut NoFaults).expect("checkpoint");
        drop(db);
        let reopened = Database::open(
            DatabaseConfig {
                directory: directory.path().to_path_buf(),
            },
            &mut NoFaults,
        )
        .expect("reopen");
        assert_eq!(reopened.commit_id(), current);
        assert_eq!(
            reopened
                .get_bytes(current, first.clone())
                .expect("reopened first"),
            Some(b"new".to_vec())
        );
        assert_eq!(
            reopened
                .scan_bytes(retained, b"table/".to_vec(), b"table0".to_vec(), 10,)
                .expect("reopened retained scan"),
            vec![(first, b"old".to_vec()), (second, b"second".to_vec()),]
        );
        assert_eq!(
            reopened
                .index_get_bytes(current, index, b"tenant-b")
                .expect("reopened index"),
            vec![b"table/tenant-a/row-1".to_vec()]
        );
    }

    #[test]
    fn byte_mutation_wal_reopen_recovers_after_post_sync_failure() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let key = b"variable-width-row".to_vec();
        let mut db = Database::create(config.clone()).expect("create");
        assert!(
            db.commit(
                vec![
                    Mutation::CreateIndex {
                        index: IndexId(53),
                        unique: false,
                    },
                    Mutation::BytePut {
                        key: key.clone(),
                        value: b"value".to_vec(),
                    },
                    Mutation::ByteIndexPut {
                        index: IndexId(53),
                        index_key: b"lookup".to_vec(),
                        primary: key.clone(),
                    },
                ],
                &mut FailOnce::at([FaultPoint::AfterWalSync]),
            )
            .is_err()
        );
        drop(db);

        let reopened = Database::open(config, &mut NoFaults).expect("reopen");
        assert_eq!(
            reopened
                .get_bytes(CommitId(1), key.clone())
                .expect("reopened value"),
            Some(b"value".to_vec())
        );
        assert_eq!(
            reopened
                .index_get_bytes(CommitId(1), IndexId(53), b"lookup")
                .expect("reopened index"),
            vec![key]
        );
    }

    #[test]
    fn fixed_and_byte_ranges_are_isolated() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        let fixed = Key::new(54, 1);
        let byte = b"range/byte".to_vec();
        let commit = db
            .commit(
                vec![
                    Mutation::Put {
                        key: fixed,
                        value: b"fixed".to_vec(),
                    },
                    Mutation::BytePut {
                        key: byte.clone(),
                        value: b"byte".to_vec(),
                    },
                ],
                &mut NoFaults,
            )
            .expect("commit");
        assert_eq!(
            db.scan(commit, Key([0; 16]), Key([0xff; 16]), 10)
                .expect("fixed scan"),
            vec![(fixed, b"fixed".to_vec())]
        );
        assert_eq!(
            db.scan_bytes(commit, b"range/".to_vec(), b"range0".to_vec(), 10,)
                .expect("byte scan"),
            vec![(byte, b"byte".to_vec())]
        );
    }

    #[test]
    fn wal_reopen_recovers_after_post_sync_failure() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        let key = Key::new(2, 2);
        let result = db.commit(
            vec![Mutation::Put {
                key,
                value: b"value".to_vec(),
            }],
            &mut FailOnce::at([FaultPoint::AfterWalSync]),
        );
        assert!(result.is_err());
        drop(db);
        let reopened = Database::open(
            DatabaseConfig {
                directory: directory.path().to_path_buf(),
            },
            &mut NoFaults,
        )
        .expect("reopen");
        assert_eq!(reopened.commit_id(), CommitId(1));
    }

    #[test]
    fn durable_attempt_resolves_after_ambiguous_wal_failure_and_retries_idempotently() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let key = Key::new(2, 4);
        let attempt = TransactionAttemptId::new([4; 16]);
        let mutations = vec![Mutation::Put {
            key,
            value: b"value".to_vec(),
        }];
        let mut db = Database::create(config.clone()).expect("create");
        assert!(
            db.commit_with_attempt(
                CommitId(0),
                attempt,
                mutations.clone(),
                &mut FailOnce::at([FaultPoint::AfterWalSync]),
            )
            .is_err()
        );
        drop(db);

        let mut reopened = Database::open(config, &mut NoFaults).expect("reopen");
        let record = reopened.resolve_attempt(attempt).expect("resolved attempt");
        assert_eq!(record.commit, CommitId(1));
        assert_eq!(reopened.commit_id(), CommitId(1));
        assert_eq!(
            reopened
                .commit_with_attempt(CommitId(1), attempt, mutations, &mut NoFaults)
                .expect("idempotent retry"),
            CommitId(1)
        );
        assert_eq!(reopened.commit_id(), CommitId(1));
        assert!(matches!(
            reopened.commit_with_attempt(
                CommitId(1),
                attempt,
                vec![Mutation::Put {
                    key,
                    value: b"different".to_vec(),
                }],
                &mut NoFaults,
            ),
            Err(DbError::IdempotencyConflict { .. })
        ));
        assert_eq!(
            reopened
                .forget_attempts(&[attempt, attempt], &mut NoFaults)
                .expect("forget attempt"),
            1
        );
        assert!(reopened.resolve_attempt(attempt).is_none());
        reopened.checkpoint(&mut NoFaults).expect("checkpoint");
        drop(reopened);
        let reopened = Database::open(
            DatabaseConfig {
                directory: directory.path().to_path_buf(),
            },
            &mut NoFaults,
        )
        .expect("reopen checkpoint");
        assert!(reopened.resolve_attempt(attempt).is_none());
    }

    #[test]
    fn ambiguous_wal_failure_requires_reopen_before_retry() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = DatabaseConfig {
            directory: directory.path().to_path_buf(),
        };
        let key = Key::new(2, 3);
        let mut db = Database::create(config.clone()).expect("create");
        assert!(matches!(
            db.commit(
                vec![Mutation::Put {
                    key,
                    value: b"value".to_vec(),
                }],
                &mut FailOnce::at([FaultPoint::AfterWalSync]),
            ),
            Err(DbError::InjectedFailure(FaultPoint::AfterWalSync))
        ));
        assert!(matches!(
            db.commit(
                vec![Mutation::Put {
                    key,
                    value: b"value".to_vec(),
                }],
                &mut NoFaults,
            ),
            Err(DbError::RecoveryRequired)
        ));
        assert!(matches!(
            db.checkpoint(&mut NoFaults),
            Err(DbError::RecoveryRequired)
        ));
        drop(db);
        let reopened = Database::open(config, &mut NoFaults).expect("reopen");
        assert_eq!(reopened.commit_id(), CommitId(1));
        assert_eq!(
            reopened.get(CommitId(1), key).expect("value"),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn checkpoint_reopen_and_wal_truncation() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        let key = Key::new(3, 3);
        db.commit(
            vec![Mutation::Put {
                key,
                value: b"value".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("commit");
        db.checkpoint(&mut NoFaults).expect("checkpoint");
        assert_eq!(
            std::fs::metadata(directory.path().join("omendb.wal"))
                .expect("WAL")
                .len(),
            0
        );
        drop(db);
        let reopened = Database::open(
            DatabaseConfig {
                directory: directory.path().to_path_buf(),
            },
            &mut NoFaults,
        )
        .expect("reopen");
        assert_eq!(reopened.commit_id(), CommitId(1));
    }

    #[test]
    fn checkpoint_publishes_v4_manifest_and_retires_old_artifacts() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        db.commit(
            vec![Mutation::Put {
                key: Key::new(3, 30),
                value: b"first".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("first commit");
        db.checkpoint(&mut NoFaults).expect("first checkpoint");
        assert_eq!(
            std::fs::metadata(directory.path().join("omendb.manifest"))
                .expect("manifest")
                .len(),
            44
        );
        let first_data = directory
            .path()
            .join("omendb.manifest.data-0000000000000001.pages");
        let first_ranges = directory
            .path()
            .join("omendb.ranges-0000000000000001.pages");
        assert!(first_data.exists());
        assert!(first_ranges.exists());

        db.commit(
            vec![Mutation::Put {
                key: Key::new(3, 31),
                value: b"second".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("second commit");
        db.checkpoint(&mut NoFaults).expect("second checkpoint");
        assert!(!first_data.exists());
        assert!(!first_ranges.exists());
        assert!(
            directory
                .path()
                .join("omendb.manifest.data-0000000000000002.pages")
                .exists()
        );
        assert!(
            directory
                .path()
                .join("omendb.ranges-0000000000000002.pages")
                .exists()
        );

        drop(db);
        let reopened = Database::open(
            DatabaseConfig {
                directory: directory.path().to_path_buf(),
            },
            &mut NoFaults,
        )
        .expect("reopen");
        assert_eq!(reopened.commit_id(), CommitId(2));
    }

    #[test]
    fn reopen_rebuilds_unique_index_current() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        let index = IndexId(41);
        let first = Key::new(41, 1);
        db.commit(
            vec![
                Mutation::CreateIndex {
                    index,
                    unique: true,
                },
                Mutation::IndexPut {
                    index,
                    index_key: b"same".to_vec(),
                    primary: first,
                },
            ],
            &mut NoFaults,
        )
        .expect("create unique index");
        db.checkpoint(&mut NoFaults).expect("checkpoint");
        drop(db);

        let mut reopened = Database::open(
            DatabaseConfig {
                directory: directory.path().to_path_buf(),
            },
            &mut NoFaults,
        )
        .expect("reopen");
        assert_eq!(
            reopened
                .index_get(CommitId(1), index, b"same")
                .expect("index lookup"),
            vec![first]
        );
        assert!(matches!(
            reopened.commit(
                vec![Mutation::IndexPut {
                    index,
                    index_key: b"same".to_vec(),
                    primary: Key::new(41, 2),
                }],
                &mut NoFaults,
            ),
            Err(DbError::UniqueViolation { .. })
        ));
    }

    #[test]
    fn old_v3_manifest_refuses_recovery() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        db.commit(
            vec![Mutation::Put {
                key: Key::new(42, 1),
                value: b"value".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("commit");
        db.checkpoint(&mut NoFaults).expect("checkpoint");
        drop(db);

        let manifest = directory.path().join("omendb.manifest");
        let mut bytes = std::fs::read(&manifest).expect("read manifest");
        bytes[4..8].copy_from_slice(&3_u32.to_le_bytes());
        std::fs::write(&manifest, bytes).expect("write old manifest version");
        assert!(matches!(
            Database::open(
                DatabaseConfig {
                    directory: directory.path().to_path_buf()
                },
                &mut NoFaults
            ),
            Err(DbError::Corruption {
                artifact: "checkpoint artifact",
                ..
            })
        ));
    }

    #[test]
    fn packed_range_corruption_refuses_recovery() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        db.commit(
            vec![Mutation::Put {
                key: Key::new(3, 4),
                value: b"value".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("commit");
        db.checkpoint(&mut NoFaults).expect("checkpoint");
        drop(db);
        let path = directory
            .path()
            .join("omendb.ranges-0000000000000001.pages");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("range pages");
        file.seek(SeekFrom::Start(64)).expect("seek");
        file.write_all(&[0xff]).expect("corrupt");
        file.sync_all().expect("sync");
        assert!(matches!(
            Database::open(
                DatabaseConfig {
                    directory: directory.path().to_path_buf()
                },
                &mut NoFaults
            ),
            Err(DbError::Corruption { .. })
        ));
    }

    #[test]
    fn database_identity_corruption_refuses_open() {
        let directory = tempfile::tempdir().expect("tempdir");
        let db = database(&directory);
        let identity = db.storage_identity();
        db.close().expect("close");

        let path = directory.path().join("omendb.identity");
        let mut file = OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open identity");
        file.seek(SeekFrom::Start(0)).expect("seek identity");
        file.write_all(&[0xff]).expect("corrupt identity");
        file.sync_all().expect("sync identity");

        let result = Database::open(
            DatabaseConfig {
                directory: directory.path().to_path_buf(),
            },
            &mut NoFaults,
        );
        assert!(matches!(
            result,
            Err(DbError::Corruption {
                artifact: "database identity",
                ..
            })
        ));
        assert_ne!(identity.database_id, [0; 16]);
    }

    #[test]
    fn valid_but_wrong_packed_range_refuses_recovery() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        db.commit(
            vec![Mutation::Put {
                key: Key::new(3, 4),
                value: b"value".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("commit");
        db.checkpoint(&mut NoFaults).expect("checkpoint");
        drop(db);

        let path = directory
            .path()
            .join("omendb.ranges-0000000000000001.pages");
        let replacement = pack_sorted(
            1,
            &[(Key::new(3, 5), b"different".to_vec())],
            PackBudget::unlimited(),
        )
        .expect("pack replacement");
        replacement
            .write(&path, &mut NoFaults)
            .expect("write replacement");

        assert!(matches!(
            Database::open(
                DatabaseConfig {
                    directory: directory.path().to_path_buf()
                },
                &mut NoFaults
            ),
            Err(DbError::Corruption {
                artifact: "checkpoint manifest",
                ..
            })
        ));
    }

    #[test]
    fn missing_packed_range_refuses_recovery() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        db.commit(
            vec![Mutation::Put {
                key: Key::new(3, 5),
                value: b"value".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("commit");
        db.checkpoint(&mut NoFaults).expect("checkpoint");
        std::fs::remove_file(
            directory
                .path()
                .join("omendb.ranges-0000000000000001.pages"),
        )
        .expect("remove range");
        drop(db);
        assert!(matches!(
            Database::open(
                DatabaseConfig {
                    directory: directory.path().to_path_buf()
                },
                &mut NoFaults
            ),
            Err(DbError::Corruption {
                artifact: "packed range",
                ..
            })
        ));
    }

    #[test]
    fn checkpoint_reopen_preserves_retained_snapshot() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        let key = Key::new(5, 5);
        let first = db
            .commit(
                vec![Mutation::Put {
                    key,
                    value: b"old".to_vec(),
                }],
                &mut NoFaults,
            )
            .expect("commit one");
        db.retain(first).expect("retain");
        db.commit(
            vec![Mutation::Put {
                key,
                value: b"new".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("commit two");
        db.checkpoint(&mut NoFaults).expect("checkpoint");
        drop(db);
        let reopened = Database::open(
            DatabaseConfig {
                directory: directory.path().to_path_buf(),
            },
            &mut NoFaults,
        )
        .expect("reopen");
        assert_eq!(
            reopened.get(first, key).expect("old"),
            Some(b"old".to_vec())
        );
        assert_eq!(
            reopened.get(CommitId(2), key).expect("new"),
            Some(b"new".to_vec())
        );
    }

    #[test]
    fn complete_wal_corruption_refuses_recovery() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        db.commit(
            vec![Mutation::Put {
                key: Key::new(4, 4),
                value: b"value".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("commit");
        drop(db);
        let path = directory.path().join("omendb.wal");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("WAL");
        file.seek(SeekFrom::End(-1)).expect("seek");
        file.write_all(&[0xff]).expect("corrupt");
        file.sync_all().expect("sync");
        assert!(matches!(
            Database::open(
                DatabaseConfig {
                    directory: directory.path().to_path_buf()
                },
                &mut NoFaults
            ),
            Err(DbError::Corruption { .. })
        ));
    }

    #[test]
    fn wal_commit_identity_corruption_refuses_recovery() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        db.commit(
            vec![Mutation::Put {
                key: Key::new(6, 6),
                value: b"value".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("commit");
        drop(db);

        // The mutation payload remains intact, but the commit ID no longer
        // matches the checksum that was forced with the frame.
        let path = directory.path().join("omendb.wal");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("WAL");
        file.seek(SeekFrom::Start(8)).expect("commit header");
        file.write_all(&2_u64.to_le_bytes()).expect("corrupt");
        file.sync_all().expect("sync");
        assert!(matches!(
            Database::open(
                DatabaseConfig {
                    directory: directory.path().to_path_buf()
                },
                &mut NoFaults
            ),
            Err(DbError::Corruption {
                artifact: "WAL",
                ..
            })
        ));
    }

    #[test]
    fn checkpoint_range_failure_does_not_publish_manifest() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut db = database(&directory);
        db.commit(
            vec![Mutation::Put {
                key: Key::new(7, 7),
                value: b"value".to_vec(),
            }],
            &mut NoFaults,
        )
        .expect("commit");
        assert!(
            db.checkpoint(&mut FailOnce::at([FaultPoint::PackedPageSync]))
                .is_err()
        );
        assert!(!directory.path().join("omendb.manifest").exists());
        drop(db);
        let reopened = Database::open(
            DatabaseConfig {
                directory: directory.path().to_path_buf(),
            },
            &mut NoFaults,
        )
        .expect("reopen from WAL");
        assert_eq!(reopened.commit_id(), CommitId(1));
    }
}
