//! Concrete OmenDB ownership seam for the Rust SeerDB byte engine.
//!
//! This is intentionally a kernel boundary, not a second relational model.
//! OmenDB owns row/index/catalog encoding and transaction policy; SeerDB owns
//! ordered arbitrary-byte storage, durable publication, retained roots, and
//! physical reclamation.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

use seerdb::{
    BatchMutation, DB, Error as SeerError, Options, ReadView, SnapshotId as SeerSnapshotId,
};

pub use crate::model::StorageIdentity;
use crate::{CommitId, DbError, Result};

/// Configuration for a SeerDB-backed OmenDB kernel.
#[derive(Clone, Debug)]
pub struct SeerKernelConfig {
    pub directory: PathBuf,
    pub options: Options,
}

impl SeerKernelConfig {
    #[must_use]
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            options: Options::default(),
        }
    }
}

/// A commit qualified by the SeerDB history that published it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SnapshotIdentity {
    pub storage: StorageIdentity,
    pub commit: CommitId,
}

/// Durable publication state projected without exposing SeerDB's physical
/// status type to OmenDB callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SeerDurabilityStatus {
    pub storage: StorageIdentity,
    pub generation: u64,
    pub commit: CommitId,
    pub pending_mutations: u64,
    pub write_fenced: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SeerCheckpointReport {
    pub physical: seerdb::VerificationReport,
    pub durability: SeerDurabilityStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SeerCompactionReport {
    pub physical: seerdb::CompactionReport,
    pub durability: SeerDurabilityStatus,
}

/// One atomic byte-key mutation owned by OmenDB's logical layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvMutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Result of a successful durable publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitOutcome {
    /// Commit visible after this call returns successfully.
    pub commit: CommitId,
    /// Whether SeerDB acknowledged the commit envelope and publication.
    pub acknowledged: bool,
    /// Successful outcomes never require reopen; this field makes the
    /// ambiguous-failure contract explicit at the OmenDB boundary.
    pub requires_reopen: bool,
}

/// An owned logical retention lease.
///
/// Multiple callers may retain the same commit. Each call returns a distinct
/// token, and releasing one token cannot invalidate another caller's view.
/// Dropping the token releases it automatically, including when a transaction
/// is abandoned after staging or validation fails.
#[derive(Debug)]
pub struct SnapshotLease {
    identity: SnapshotIdentity,
    commit: CommitId,
    token: u64,
    registry: Weak<Mutex<LeaseRegistry>>,
    released: AtomicBool,
}

impl PartialEq for SnapshotLease {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity && self.token == other.token
    }
}

impl Eq for SnapshotLease {}

impl SnapshotLease {
    #[must_use]
    pub fn identity(&self) -> SnapshotIdentity {
        self.identity
    }

    #[must_use]
    pub fn commit(&self) -> CommitId {
        self.commit
    }
}

struct LeaseState {
    snapshot_id: SeerSnapshotId,
    tokens: BTreeMap<u64, ()>,
}

struct LeaseRegistry {
    db: Arc<Mutex<DB>>,
    leases: BTreeMap<CommitId, LeaseState>,
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        if let Ok(mut registry) = registry.lock() {
            let _ = release_lease_token(&mut registry, self.commit, self.token);
        }
    }
}

impl Drop for LeaseRegistry {
    fn drop(&mut self) {
        for (_, state) in std::mem::take(&mut self.leases) {
            let result = match self.db.lock() {
                Ok(mut db) => db.release_snapshot(state.snapshot_id),
                Err(poisoned) => poisoned.into_inner().release_snapshot(state.snapshot_id),
            };
            let _ = result;
        }
    }
}

/// OmenDB's concrete arbitrary-byte-key boundary over Rust SeerDB.
pub struct SeerKernel {
    db: Arc<Mutex<DB>>,
    leases: Arc<Mutex<LeaseRegistry>>,
    /// Strong cache for the current generation-bound view. The cache is
    /// invalidated before publication and reclamation so it cannot pin an old
    /// physical root across a state transition.
    current_view: Mutex<Option<Arc<ReadView>>>,
    next_lease_token: u64,
}

impl SeerKernel {
    pub fn create(config: &SeerKernelConfig) -> Result<Self> {
        Self::from_db(
            DB::create(&config.directory, config.options.clone())
                .map_err(|error| map_storage_error("create", error, false))?,
        )
    }

    pub fn open(config: &SeerKernelConfig) -> Result<Self> {
        Self::from_db(
            DB::open(&config.directory, config.options.clone())
                .map_err(|error| map_storage_error("open", error, false))?,
        )
    }

    /// Flush and close this writer handle, releasing SeerDB's process lock.
    ///
    /// The kernel owns the lease registry. Dropping it before closing the
    /// physical handle releases durable snapshot roots while the database is
    /// still available; the consumed kernel cannot be used after this call.
    pub fn close(self) -> Result<()> {
        let SeerKernel {
            db,
            leases,
            current_view,
            ..
        } = self;
        drop(current_view);
        drop(leases);
        let mut db = db.lock().map_err(|_| database_lock_error())?;
        let result = db.close();
        let fenced = db.durability_status().write_fenced;
        result.map_err(|error| map_storage_error("close", error, fenced))
    }

    fn from_db(db: DB) -> Result<Self> {
        let db = Arc::new(Mutex::new(db));
        let leases = Arc::new(Mutex::new(LeaseRegistry {
            db: Arc::clone(&db),
            leases: BTreeMap::new(),
        }));
        Ok(Self {
            db,
            leases,
            current_view: Mutex::new(None),
            next_lease_token: 1,
        })
    }

    /// Commit row, index, and catalog byte mutations as one SeerDB batch.
    ///
    /// Shared access is safe: the kernel's internal `Mutex<DB>` serializes
    /// durable publication, so concurrent callers overlap only their waiting,
    /// never the publication itself.
    pub fn commit(&self, expected: CommitId, mutations: &[KvMutation]) -> Result<CommitOutcome> {
        self.invalidate_current_view()?;
        let mutations = mutations
            .iter()
            .map(|mutation| match mutation {
                KvMutation::Put { key, value } => BatchMutation::Put {
                    key: key.clone(),
                    value: value.clone(),
                },
                KvMutation::Delete { key } => BatchMutation::Delete { key: key.clone() },
            })
            .collect::<Vec<_>>();
        let (status, fenced) = {
            let mut db = self.db.lock().map_err(|_| database_lock_error())?;
            let status = db.commit_batch_at(seer_commit(expected), &mutations);
            let fenced = db.durability_status().write_fenced;
            (status, fenced)
        };
        let status = status.map_err(|error| map_storage_error("commit", error, fenced))?;
        Ok(CommitOutcome {
            commit: CommitId(status.commit_id.get()),
            acknowledged: true,
            requires_reopen: false,
        })
    }

    /// Resolve a durable transaction attempt after reopening this history.
    ///
    /// `Some` means the logical mutation batch was published and must not be
    /// executed again. `None` means no durable outcome record was found.
    pub fn resolve_attempt(
        &self,
        attempt: crate::TransactionAttemptId,
    ) -> Result<Option<crate::AttemptRecord>> {
        let Some(bytes) = self.get(self.commit_id(), &crate::attempt::seer_key(attempt))? else {
            return Ok(None);
        };
        let record = crate::attempt::decode_record(&bytes)?;
        if record.attempt != attempt {
            return Err(DbError::Corruption {
                artifact: "transaction attempt record",
                reason: "record identity does not match its key".to_owned(),
            });
        }
        Ok(Some(record))
    }

    /// Return durable attempt records in deterministic identity order.
    pub fn attempt_records(&self, limit: usize) -> Result<Vec<crate::AttemptRecord>> {
        let (start, end) = crate::attempt::seer_key_range();
        let records = self
            .scan(self.commit_id(), &start, &end, limit.saturating_add(1))?
            .into_iter()
            .map(|(key, value)| {
                let attempt = crate::attempt::decode_seer_key(&key)?;
                let record = crate::attempt::decode_record(&value)?;
                if record.attempt != attempt {
                    return Err(DbError::Corruption {
                        artifact: "transaction attempt record",
                        reason: "attempt key and record identity differ".to_owned(),
                    });
                }
                Ok(record)
            })
            .collect::<Result<Vec<_>>>()?;
        if records.len() > limit {
            return Err(DbError::SnapshotCaptureLimit {
                resource: "transaction attempts",
                limit,
            });
        }
        Ok(records)
    }

    /// Publish imported transaction-attempt records in one target-history
    /// control-plane commit. Source commit numbers are represented only in
    /// the archive mapping; the target record uses the target commit assigned
    /// by SeerDB.
    pub fn import_attempt_records(
        &mut self,
        records: &[crate::AttemptRecord],
    ) -> Result<Vec<crate::AttemptRecord>> {
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
            if let Some(existing) = self.resolve_attempt(record.attempt)? {
                return Err(DbError::IdempotencyConflict {
                    attempt: record.attempt,
                    existing_digest: existing.digest,
                    requested_digest: record.digest,
                });
            }
        }
        let target_commit = CommitId(
            self.commit_id()
                .0
                .checked_add(1)
                .ok_or_else(|| DbError::InvalidState("commit ID exhausted".to_owned()))?,
        );
        let mutations = records
            .iter()
            .map(|record| crate::KvMutation::Put {
                key: crate::attempt::seer_key(record.attempt),
                value: crate::attempt::encode_record(crate::AttemptRecord {
                    attempt: record.attempt,
                    commit: target_commit,
                    digest: record.digest,
                })
                .to_vec(),
            })
            .collect::<Vec<_>>();
        self.commit(self.commit_id(), &mutations)?;
        Ok(records
            .iter()
            .map(|record| crate::AttemptRecord {
                attempt: record.attempt,
                commit: target_commit,
                digest: record.digest,
            })
            .collect())
    }

    /// Publish a batch together with a durable idempotency record.
    ///
    /// Reusing the same attempt and identical byte mutations returns the
    /// original commit without publishing again. Reusing it for different
    /// mutations is rejected. An ambiguous error must still be followed by
    /// reopen and [`Self::resolve_attempt`].
    pub fn commit_with_attempt(
        &self,
        expected: CommitId,
        attempt: crate::TransactionAttemptId,
        mutations: &[KvMutation],
    ) -> Result<CommitOutcome> {
        if mutations.is_empty() {
            return Err(DbError::InvalidState("empty transaction".to_owned()));
        }
        let digest = crate::attempt::digest_kv_mutations(mutations);
        if let Some(record) = self.resolve_attempt(attempt)? {
            if record.digest == digest {
                return Ok(CommitOutcome {
                    commit: record.commit,
                    acknowledged: true,
                    requires_reopen: false,
                });
            }
            return Err(DbError::IdempotencyConflict {
                attempt,
                existing_digest: record.digest,
                requested_digest: digest,
            });
        }
        let commit = CommitId(
            expected
                .0
                .checked_add(1)
                .ok_or_else(|| DbError::InvalidState("commit ID exhausted".to_owned()))?,
        );
        let record = crate::AttemptRecord {
            attempt,
            commit,
            digest,
        };
        let mut durable = mutations.to_vec();
        durable.push(KvMutation::Put {
            key: crate::attempt::seer_key(attempt),
            value: crate::attempt::encode_record(record).to_vec(),
        });
        let outcome = self.commit(expected, &durable)?;
        Ok(outcome)
    }

    /// Forget durable attempt records after the caller has decided that no
    /// retry may use those identities again.
    ///
    /// The deletion is one durable commit. If it returns an ambiguous error,
    /// reopen and resolve each identity before deciding whether cleanup or
    /// application work remains. Forgotten identities must never be reused.
    pub fn forget_attempts(&mut self, attempts: &[crate::TransactionAttemptId]) -> Result<usize> {
        let mut existing = BTreeSet::new();
        for attempt in attempts {
            if self.resolve_attempt(*attempt)?.is_some() {
                existing.insert(*attempt);
            }
        }
        if existing.is_empty() {
            return Ok(0);
        }
        let count = existing.len();
        let mutations = existing
            .into_iter()
            .map(|attempt| KvMutation::Delete {
                key: crate::attempt::seer_key(attempt),
            })
            .collect::<Vec<_>>();
        self.commit(self.commit_id(), &mutations)?;
        Ok(count)
    }

    /// Arm one SeerDB publication fault for the feature-gated conformance
    /// harness. Production builds do not expose this test seam.
    #[cfg(feature = "seerdb-fault-injection")]
    pub fn inject_fault(&self, point: crate::FaultPoint) -> Result<()> {
        let db = self.db.lock().map_err(|_| database_lock_error())?;
        match point {
            crate::FaultPoint::BeforeWalAppend => db.inject_wal_write_failure(),
            crate::FaultPoint::AfterWalAppend => db.inject_wal_after_write_failure(),
            crate::FaultPoint::WalSync => db.inject_wal_sync_failure(),
            crate::FaultPoint::AfterWalSync => db.inject_wal_after_sync_failure(),
            crate::FaultPoint::DataSync => db.inject_sync_failure(),
            crate::FaultPoint::PackedPageSync => db.inject_page_range_sync_failure(),
            crate::FaultPoint::ManifestMirrorSync => db.inject_manifest_mirror_sync_failure(),
            crate::FaultPoint::ManifestSync => db.inject_manifest_sync_failure(),
            crate::FaultPoint::AfterManifestPublish => db.inject_after_manifest_failure(),
            // WalTruncate retired from the SeerDB surface: under WAL
            // retention, log removal is threshold-gated cleanup after
            // manifest selection, so it is not an authoritative publication
            // seam. The Temporary store keeps its own point.
            crate::FaultPoint::ShortWrite => db.inject_meta_log_short_write_failure(),
            crate::FaultPoint::TornWrite => db.inject_meta_log_torn_write_failure(),
            unsupported => {
                return Err(DbError::InvalidState(format!(
                    "fault point {unsupported:?} is not a SeerDB publication seam"
                )));
            }
        }
        Ok(())
    }

    pub fn get(&self, snapshot: CommitId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if snapshot == self.commit_id() {
            let view = self.current_read_view(snapshot)?;
            return self.read_view_get(&view, key);
        }
        let snapshot_id = self.snapshot_id(snapshot)?;
        self.db
            .lock()
            .map_err(|_| database_lock_error())?
            .get_at(snapshot_id, key)
            .map_err(|error| {
                map_storage_error_for_snapshot("get historical", error, false, Some(snapshot.0))
            })
    }

    /// Read through an already captured immutable SeerDB generation.
    ///
    /// The view owns its page/blob handles and reclamation lease, so this
    /// path does not take the serialized writer mutex while doing I/O.
    pub(crate) fn read_view_get(&self, view: &ReadView, key: &[u8]) -> Result<Option<Vec<u8>>> {
        view.get(key)
            .map_err(|error| map_storage_error("read view get", error, false))
    }

    pub(crate) fn read_view_scan(
        &self,
        view: &ReadView,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        view.range(start, end)
            .map(|rows| rows.into_iter().take(limit).collect())
            .map_err(|error| map_storage_error("read view scan", error, false))
    }

    pub(crate) fn begin_current_read_view(&self) -> Result<Arc<ReadView>> {
        let expected = self.commit_id();
        self.current_read_view(expected)
    }

    pub fn scan(
        &self,
        snapshot: CommitId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if snapshot == self.commit_id() {
            let view = self.current_read_view(snapshot)?;
            return self.read_view_scan(&view, start, end, limit);
        }
        let snapshot_id = self.snapshot_id(snapshot)?;
        self.db
            .lock()
            .map_err(|_| database_lock_error())?
            .range_at(snapshot_id, start, end)
            .map(|rows| rows.into_iter().take(limit).collect())
            .map_err(|error| map_storage_error_for_snapshot("scan", error, false, Some(snapshot.0)))
    }

    /// Acquire one caller-owned lease for a historical commit.
    pub fn retain(&mut self, commit: CommitId) -> Result<SnapshotLease> {
        let snapshot = self.snapshot_identity(commit)?;
        self.retain_snapshot(snapshot)
    }

    /// Explicitly create a durable lease for the current root.
    ///
    /// Ordinary typed transactions use an in-process current read view
    /// instead, because they remain inside this process and need only
    /// generation-bound page/blob handles. Keep this method for callers that
    /// explicitly need a durable current-root retention record.
    pub fn retain_current_transaction(&mut self) -> Result<SnapshotLease> {
        let snapshot = self.snapshot_identity(self.commit_id())?;
        self.retain_snapshot_with(snapshot, true)
    }

    /// Retain a commit only when it belongs to this database/history.
    pub fn retain_snapshot(&mut self, snapshot: SnapshotIdentity) -> Result<SnapshotLease> {
        self.retain_snapshot_with(snapshot, false)
    }

    fn retain_snapshot_with(
        &mut self,
        snapshot: SnapshotIdentity,
        current_transaction: bool,
    ) -> Result<SnapshotLease> {
        let current = self.storage_identity()?;
        if snapshot.storage != current {
            return Err(DbError::StorageSnapshotUnavailable {
                snapshot: snapshot.commit.0,
                reason: "snapshot belongs to a different database history".into(),
            });
        }
        let commit = snapshot.commit;
        let registry = Arc::clone(&self.leases);
        let existing_snapshot_id = self
            .leases
            .lock()
            .map_err(|_| lease_registry_lock_error())?
            .leases
            .get(&commit)
            .map(|state| state.snapshot_id);
        let snapshot_id = if let Some(snapshot_id) = existing_snapshot_id {
            snapshot_id
        } else if current_transaction {
            self.db
                .lock()
                .map_err(|_| database_lock_error())?
                .retain_current_commit()
                .map_err(|error| {
                    map_storage_error_for_snapshot(
                        "retain current transaction root",
                        error,
                        false,
                        Some(commit.0),
                    )
                })?
        } else {
            self.db
                .lock()
                .map_err(|_| database_lock_error())?
                .retain_commit(seer_commit(commit))
                .map_err(|error| {
                    map_storage_error_for_snapshot("retain", error, false, Some(commit.0))
                })?
        };
        let token = self.next_lease_token;
        self.next_lease_token = self
            .next_lease_token
            .checked_add(1)
            .ok_or_else(|| DbError::InvalidState("snapshot lease token exhausted".into()))?;
        registry
            .lock()
            .map_err(|_| lease_registry_lock_error())?
            .leases
            .entry(commit)
            .or_insert_with(|| LeaseState {
                snapshot_id,
                tokens: BTreeMap::new(),
            })
            .tokens
            .insert(token, ());
        Ok(SnapshotLease {
            identity: snapshot,
            commit,
            token,
            registry: Arc::downgrade(&registry),
            released: AtomicBool::new(false),
        })
    }

    /// Release one caller-owned lease without invalidating sibling leases.
    /// Dropping the lease is equivalent when explicit release is not needed.
    pub fn release(&mut self, lease: &mut SnapshotLease) -> Result<()> {
        if lease.identity.storage != self.storage_identity()? {
            return Err(DbError::StorageSnapshotUnavailable {
                snapshot: lease.commit.0,
                reason: "snapshot lease belongs to a different database history".into(),
            });
        }
        let result = {
            let mut registry = self
                .leases
                .lock()
                .map_err(|_| lease_registry_lock_error())?;
            release_lease_token(&mut registry, lease.commit, lease.token)
        };
        if result.is_ok() {
            lease.released.store(true, Ordering::Release);
        }
        result
    }

    pub fn checkpoint(&mut self) -> Result<seerdb::VerificationReport> {
        self.checkpoint_with_status().map(|report| report.physical)
    }

    pub(crate) fn checkpoint_with_status(&mut self) -> Result<SeerCheckpointReport> {
        self.invalidate_current_view()?;
        let (result, fenced) = {
            let mut db = self.db.lock().map_err(|_| database_lock_error())?;
            let result = db.checkpoint();
            let fenced = db.durability_status().write_fenced;
            (result, fenced)
        };
        let physical = result.map_err(|error| map_storage_error("checkpoint", error, fenced))?;
        Ok(SeerCheckpointReport {
            durability: project_durability_status(physical.durability),
            physical,
        })
    }

    /// Create an independently verified immutable archive of the current
    /// durable history without changing this kernel's directory.
    pub fn snapshot<P: AsRef<Path>>(&mut self, destination: P) -> Result<seerdb::SnapshotReport> {
        let (report, fenced) = {
            let mut db = self.db.lock().map_err(|_| database_lock_error())?;
            let report = db.snapshot(destination);
            let fenced = db.durability_status().write_fenced;
            (report, fenced)
        };
        report.map_err(|error| map_storage_error("snapshot", error, fenced))
    }

    /// Restore an immutable archive into a new writable history and return a
    /// kernel attached to that history.
    pub fn restore<P: AsRef<Path>>(
        config: &SeerKernelConfig,
        archive: P,
    ) -> Result<(Self, seerdb::RestoreReport)> {
        let report = DB::restore(archive, &config.directory, config.options.clone())
            .map_err(|error| map_storage_error("restore", error, false))?;
        let db = DB::open(&config.directory, config.options.clone())
            .map_err(|error| map_storage_error("open restored history", error, false))?;
        Ok((Self::from_db(db)?, report))
    }

    pub fn verify(&mut self) -> Result<seerdb::VerificationReport> {
        self.db
            .lock()
            .map_err(|_| database_lock_error())?
            .verify()
            .map_err(|error| map_storage_error("verify", error, false))
    }

    pub fn compact(&mut self) -> Result<seerdb::CompactionReport> {
        self.compact_with_status().map(|report| report.physical)
    }

    pub(crate) fn compact_with_status(&mut self) -> Result<SeerCompactionReport> {
        self.invalidate_current_view()?;
        let (result, fenced) = {
            let mut db = self.db.lock().map_err(|_| database_lock_error())?;
            let result = db.compact();
            let fenced = db.durability_status().write_fenced;
            (result, fenced)
        };
        let physical = result.map_err(|error| map_storage_error("compact", error, fenced))?;
        Ok(SeerCompactionReport {
            durability: project_durability_status(physical.durability),
            physical,
        })
    }

    /// Run one bounded physical-reclamation generation.
    pub fn compact_with_limit(
        &mut self,
        max_relocated_pages: usize,
    ) -> Result<seerdb::CompactionReport> {
        self.compact_with_limit_status(max_relocated_pages)
            .map(|report| report.physical)
    }

    pub(crate) fn compact_with_limit_status(
        &mut self,
        max_relocated_pages: usize,
    ) -> Result<SeerCompactionReport> {
        self.invalidate_current_view()?;
        let (result, fenced) = {
            let mut db = self.db.lock().map_err(|_| database_lock_error())?;
            let result = db.compact_with_limit(max_relocated_pages);
            let fenced = db.durability_status().write_fenced;
            (result, fenced)
        };
        let physical =
            result.map_err(|error| map_storage_error("compact with limit", error, fenced))?;
        Ok(SeerCompactionReport {
            durability: project_durability_status(physical.durability),
            physical,
        })
    }

    /// Return the underlying storage counters for attribution and admission
    /// decisions at the OmenDB boundary.
    pub fn metrics(&self) -> Result<seerdb::DBMetrics> {
        self.db
            .lock()
            .map_err(|_| database_lock_error())?
            .metrics()
            .map_err(|error| map_storage_error("metrics", error, false))
    }

    #[must_use]
    pub fn commit_id(&self) -> CommitId {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(poisoned) => poisoned.into_inner(),
        };
        CommitId(db.durability_status().commit_id.get())
    }

    /// Return the complete logical commit catalog exposed by SeerDB's
    /// durable manifest history. Repeated physical generations for one
    /// logical commit are collapsed; callers still acquire retention leases
    /// before reading the returned commits.
    pub fn published_commits(&self) -> Result<Vec<CommitId>> {
        let db = self.db.lock().map_err(|_| database_lock_error())?;
        Ok(db
            .published_commits()
            .map_err(|error| match error {
                SeerError::SnapshotUnavailable(reason) => DbError::InvalidState(reason),
                error => map_storage_error("published commit catalog", error, false),
            })?
            .into_iter()
            .map(|commit| CommitId(commit.get()))
            .collect())
    }

    /// Return the stable database/history identity for this kernel.
    pub fn storage_identity(&self) -> Result<StorageIdentity> {
        let db = self.db.lock().map_err(|_| database_lock_error())?;
        Ok(storage_identity(db.durability_status()))
    }

    /// Return durable publication state for diagnostics and recovery
    /// decisions without exposing SeerDB's physical status type.
    pub fn durability_status(&self) -> Result<SeerDurabilityStatus> {
        let db = self.db.lock().map_err(|_| database_lock_error())?;
        Ok(project_durability_status(db.durability_status()))
    }

    /// Count distinct commits with explicit durable retention leases held by
    /// this kernel. Ordinary process-local transaction read views are not
    /// included.
    #[must_use]
    pub fn retained_snapshot_count(&self) -> usize {
        let registry = match self.leases.lock() {
            Ok(registry) => registry,
            Err(poisoned) => poisoned.into_inner(),
        };
        registry.leases.len()
    }

    /// Return explicitly retained snapshot commits in ascending order.
    ///
    /// This is an observation of this kernel's in-process retention leases. It
    /// is not a commit-history catalog and does not acquire or extend a lease.
    #[must_use]
    pub fn retained_snapshot_commits(&self) -> Vec<CommitId> {
        let registry = match self.leases.lock() {
            Ok(registry) => registry,
            Err(poisoned) => poisoned.into_inner(),
        };
        registry.leases.keys().copied().collect()
    }

    /// Qualify a commit with this kernel's stable database/history identity.
    pub fn snapshot_identity(&self, commit: CommitId) -> Result<SnapshotIdentity> {
        Ok(SnapshotIdentity {
            storage: self.storage_identity()?,
            commit,
        })
    }

    #[cfg(test)]
    pub(crate) fn active_lease_count(&self) -> usize {
        let registry = match self.leases.lock() {
            Ok(registry) => registry,
            Err(poisoned) => poisoned.into_inner(),
        };
        registry
            .leases
            .values()
            .map(|state| state.tokens.len())
            .sum()
    }

    fn snapshot_id(&self, snapshot: CommitId) -> Result<SeerSnapshotId> {
        self.leases
            .lock()
            .map_err(|_| lease_registry_lock_error())?
            .leases
            .get(&snapshot)
            .map(|state| state.snapshot_id)
            .ok_or_else(|| DbError::StorageSnapshotUnavailable {
                snapshot: snapshot.0,
                reason: "historical reads require an owned retention lease".into(),
            })
    }

    fn invalidate_current_view(&self) -> Result<()> {
        self.current_view
            .lock()
            .map_err(|_| DbError::InvalidState("current read-view cache is poisoned".into()))?
            .take();
        Ok(())
    }

    fn current_read_view(&self, expected: CommitId) -> Result<Arc<ReadView>> {
        if let Some(view) = self
            .current_view
            .lock()
            .map_err(|_| DbError::InvalidState("current read-view cache is poisoned".into()))?
            .as_ref()
            .filter(|view| view.commit_id().get() == expected.0)
            .cloned()
        {
            return Ok(view);
        }

        let mut db = self.db.lock().map_err(|_| database_lock_error())?;
        let actual = CommitId(db.durability_status().commit_id.get());
        if actual != expected {
            return Err(DbError::StorageSnapshotUnavailable {
                snapshot: expected.0,
                reason: format!(
                    "current commit advanced to {} before read-view capture",
                    actual.0
                ),
            });
        }
        let view = Arc::new(
            db.begin_read_view()
                .map_err(|error| map_storage_error("begin read view", error, false))?,
        );
        *self
            .current_view
            .lock()
            .map_err(|_| DbError::InvalidState("current read-view cache is poisoned".into()))? =
            Some(Arc::clone(&view));
        Ok(view)
    }
}

fn storage_identity(status: seerdb::DurabilityStatus) -> StorageIdentity {
    StorageIdentity {
        database_id: status.database_id.as_bytes(),
        history_id: status.history_id.get(),
    }
}

fn project_durability_status(status: seerdb::DurabilityStatus) -> SeerDurabilityStatus {
    SeerDurabilityStatus {
        storage: storage_identity(status),
        generation: status.generation_id.get(),
        commit: CommitId(status.commit_id.get()),
        pending_mutations: status.pending_mutations,
        write_fenced: status.write_fenced,
    }
}

fn release_lease_token(registry: &mut LeaseRegistry, commit: CommitId, token: u64) -> Result<()> {
    let state = registry.leases.get(&commit).ok_or_else(|| {
        DbError::InvalidState(format!("unknown snapshot lease for commit {}", commit.0))
    })?;
    if !state.tokens.contains_key(&token) {
        return Err(DbError::InvalidState(
            "snapshot lease was already released".into(),
        ));
    }
    if state.tokens.len() > 1 {
        registry
            .leases
            .get_mut(&commit)
            .expect("lease state exists after lookup")
            .tokens
            .remove(&token);
        return Ok(());
    }
    let state = registry
        .leases
        .remove(&commit)
        .expect("lease state exists after lookup");
    let release = match registry.db.lock() {
        Ok(mut db) => db.release_snapshot(state.snapshot_id).map_err(|error| {
            map_storage_error_for_snapshot("release", error, false, Some(commit.0))
        }),
        Err(_) => Err(database_lock_error()),
    };
    if let Err(error) = release {
        registry.leases.insert(commit, state);
        return Err(error);
    }
    Ok(())
}

fn database_lock_error() -> DbError {
    DbError::InvalidState("SeerKernel database mutex is poisoned".into())
}

fn lease_registry_lock_error() -> DbError {
    DbError::InvalidState("SeerKernel lease registry mutex is poisoned".into())
}

fn seer_commit(commit: CommitId) -> seerdb::CommitId {
    seerdb::CommitId::new(commit.0)
}

fn map_storage_error(operation: &'static str, error: SeerError, fenced: bool) -> DbError {
    map_storage_error_for_snapshot(operation, error, fenced, None)
}

fn map_storage_error_for_snapshot(
    operation: &'static str,
    error: SeerError,
    fenced: bool,
    snapshot: Option<u64>,
) -> DbError {
    if fenced {
        return DbError::StorageRecoveryRequired {
            reason: format!("{operation}: {error}"),
        };
    }
    match error {
        SeerError::DatabaseBusy => DbError::StorageBusy {
            operation,
            reason: "another writable handle owns the database directory".to_owned(),
        },
        SeerError::DiskFull | SeerError::CapacityPreflight => DbError::StorageCapacity {
            requested: 1,
            available: 0,
        },
        SeerError::Backpressure {
            required,
            available,
        } => DbError::StorageCapacity {
            requested: required,
            available,
        },
        SeerError::SerializationConflict { expected, current } => DbError::SerializationConflict {
            snapshot: expected.get(),
            current: current.get(),
        },
        SeerError::SnapshotUnavailable(reason) => DbError::StorageSnapshotUnavailable {
            snapshot: snapshot.unwrap_or(0),
            reason,
        },
        SeerError::NeedsRecovery(reason) => DbError::StorageRecoveryRequired { reason },
        SeerError::Corruption(reason)
        | SeerError::Check {
            message: reason, ..
        } => DbError::StorageCorruption { reason },
        SeerError::Io(source) => DbError::StorageIo {
            operation,
            reason: source.to_string(),
        },
        other => DbError::Storage {
            operation,
            reason: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}

    fn batch(commit: u64, row: &[u8], index: &[u8], catalog: &[u8]) -> Vec<KvMutation> {
        vec![
            KvMutation::Put {
                key: row.to_vec(),
                value: format!("row-{commit}").into_bytes(),
            },
            KvMutation::Put {
                key: index.to_vec(),
                value: row.to_vec(),
            },
            KvMutation::Put {
                key: catalog.to_vec(),
                value: format!("catalog-{commit}").into_bytes(),
            },
        ]
    }

    #[test]
    fn storage_error_mapping_preserves_backend_neutral_actions() {
        let assert_class = |error: SeerError,
                            fenced: bool,
                            snapshot: Option<u64>,
                            expected: crate::TransactionErrorClass| {
            assert_eq!(
                map_storage_error_for_snapshot("test operation", error, fenced, snapshot)
                    .transaction_class(),
                expected
            );
        };

        assert_class(
            SeerError::DatabaseBusy,
            false,
            None,
            crate::TransactionErrorClass::Busy,
        );
        assert_class(
            SeerError::DiskFull,
            false,
            None,
            crate::TransactionErrorClass::Capacity,
        );
        assert_class(
            SeerError::CapacityPreflight,
            false,
            None,
            crate::TransactionErrorClass::Capacity,
        );
        assert_class(
            SeerError::Backpressure {
                required: 10,
                available: 2,
            },
            false,
            None,
            crate::TransactionErrorClass::Capacity,
        );
        assert_class(
            SeerError::SerializationConflict {
                expected: seerdb::CommitId::new(3),
                current: seerdb::CommitId::new(4),
            },
            false,
            None,
            crate::TransactionErrorClass::SerializationRetry,
        );
        assert_class(
            SeerError::SnapshotUnavailable("expired root".to_owned()),
            false,
            Some(7),
            crate::TransactionErrorClass::SnapshotUnavailable,
        );
        assert_class(
            SeerError::NeedsRecovery("manifest publication is fenced".to_owned()),
            false,
            None,
            crate::TransactionErrorClass::ReopenRequired,
        );
        assert_class(
            SeerError::Corruption("bad checksum".to_owned()),
            false,
            None,
            crate::TransactionErrorClass::Corruption,
        );
        assert_class(
            SeerError::Io(std::io::Error::other("device error")),
            false,
            None,
            crate::TransactionErrorClass::Io,
        );
        assert_class(
            SeerError::InvalidArgument("bad request".to_owned()),
            false,
            None,
            crate::TransactionErrorClass::Storage,
        );

        // Once SeerDB fences writes, the underlying error no longer gives the
        // caller a safe retry decision. Reopen/reconcile remains authoritative.
        assert_class(
            SeerError::DiskFull,
            true,
            None,
            crate::TransactionErrorClass::ReopenRequired,
        );

        let snapshot = map_storage_error_for_snapshot(
            "read",
            SeerError::SnapshotUnavailable("expired root".to_owned()),
            false,
            Some(7),
        );
        assert!(matches!(
            snapshot,
            DbError::StorageSnapshotUnavailable { snapshot: 7, reason }
                if reason == "expired root"
        ));
    }

    #[test]
    fn durable_attempt_reopens_and_returns_original_commit_without_republication() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = SeerKernelConfig::new(directory.path().join("db"));
        let attempt = crate::TransactionAttemptId::new([8; 16]);
        let mutations = vec![KvMutation::Put {
            key: b"row".to_vec(),
            value: b"value".to_vec(),
        }];
        let kernel = SeerKernel::create(&config).expect("create");
        assert_eq!(
            kernel
                .commit_with_attempt(CommitId(0), attempt, &mutations)
                .expect("commit")
                .commit,
            CommitId(1)
        );
        drop(kernel);

        let mut reopened = SeerKernel::open(&config).expect("reopen");
        let record = reopened
            .resolve_attempt(attempt)
            .expect("resolve")
            .expect("record");
        assert_eq!(record.commit, CommitId(1));
        assert_eq!(
            reopened
                .commit_with_attempt(CommitId(1), attempt, &mutations)
                .expect("idempotent retry")
                .commit,
            CommitId(1)
        );
        assert_eq!(reopened.commit_id(), CommitId(1));
        assert!(matches!(
            reopened.commit_with_attempt(
                CommitId(1),
                attempt,
                &[KvMutation::Put {
                    key: b"row".to_vec(),
                    value: b"different".to_vec(),
                }],
            ),
            Err(crate::DbError::IdempotencyConflict { .. })
        ));
        assert_eq!(
            reopened
                .forget_attempts(&[attempt, attempt])
                .expect("forget attempt"),
            1
        );
        assert_eq!(reopened.commit_id(), CommitId(2));
        assert!(
            reopened
                .resolve_attempt(attempt)
                .expect("resolve")
                .is_none()
        );
        drop(reopened);
        let reopened = SeerKernel::open(&config).expect("reopen forgotten");
        assert!(
            reopened
                .resolve_attempt(attempt)
                .expect("resolve")
                .is_none()
        );
    }

    #[cfg(feature = "seerdb-fault-injection")]
    #[test]
    fn durable_attempt_resolves_old_or_complete_new_after_ambiguous_publication() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = SeerKernelConfig::new(directory.path().join("db"));
        let attempt = crate::TransactionAttemptId::new([9; 16]);
        let mutations = [KvMutation::Put {
            key: b"ambiguous-row".to_vec(),
            value: b"value".to_vec(),
        }];
        let kernel = SeerKernel::create(&config).expect("create");
        kernel
            .inject_fault(crate::FaultPoint::AfterWalSync)
            .expect("arm fault");
        assert!(
            kernel
                .commit_with_attempt(CommitId(0), attempt, &mutations)
                .is_err()
        );
        drop(kernel);

        let mut reopened = SeerKernel::open(&config).expect("reopen");
        let expected = reopened.commit_id();
        if reopened
            .resolve_attempt(attempt)
            .expect("resolve")
            .is_none()
        {
            reopened
                .commit_with_attempt(expected, attempt, &mutations)
                .expect("retry absent attempt");
        }
        let record = reopened
            .resolve_attempt(attempt)
            .expect("resolve final")
            .expect("durable outcome");
        assert_eq!(record.commit, reopened.commit_id());
        assert_eq!(
            reopened
                .get(record.commit, b"ambiguous-row")
                .expect("read outcome"),
            Some(b"value".to_vec())
        );

        reopened
            .inject_fault(crate::FaultPoint::AfterWalSync)
            .expect("arm cleanup fault");
        assert!(reopened.forget_attempts(&[attempt]).is_err());
        drop(reopened);
        let mut reopened = SeerKernel::open(&config).expect("reopen cleanup");
        if reopened
            .resolve_attempt(attempt)
            .expect("resolve cleanup")
            .is_some()
        {
            reopened.forget_attempts(&[attempt]).expect("retry cleanup");
        }
        assert!(
            reopened
                .resolve_attempt(attempt)
                .expect("resolve final")
                .is_none()
        );
    }

    #[test]
    fn arbitrary_r1_namespace_batch_survives_reopen_and_owned_leases() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let row = b"\x10row/users/0001";
        let old_index = b"\x20index/users/email/alice/0001";
        let new_index = b"\x20index/users/email/bob/0001";
        let catalog = b"\x00catalog/table/users/index/email";
        let mut kernel = SeerKernel::create(&config).expect("create SeerKernel");

        let first = kernel
            .commit(CommitId(0), &batch(1, row, old_index, catalog))
            .expect("first atomic namespace batch");
        assert_eq!(first.commit, CommitId(1));
        let mut lease_one = kernel.retain(CommitId(1)).expect("first lease");
        let mut lease_two = kernel.retain(CommitId(1)).expect("second lease");
        kernel
            .commit(
                CommitId(1),
                &[
                    KvMutation::Put {
                        key: row.to_vec(),
                        value: b"row-2".to_vec(),
                    },
                    KvMutation::Delete {
                        key: old_index.to_vec(),
                    },
                    KvMutation::Put {
                        key: new_index.to_vec(),
                        value: row.to_vec(),
                    },
                    KvMutation::Put {
                        key: catalog.to_vec(),
                        value: b"catalog-2".to_vec(),
                    },
                ],
            )
            .expect("second atomic namespace batch");

        assert_eq!(
            kernel.get(CommitId(2), row).expect("current row"),
            Some(b"row-2".to_vec())
        );
        assert_eq!(
            kernel
                .scan(
                    CommitId(2),
                    b"\x20index/users/email/",
                    b"\x20index/users/email0",
                    10
                )
                .expect("current index scan"),
            vec![(new_index.to_vec(), row.to_vec())]
        );
        assert_eq!(
            kernel.get(CommitId(1), row).expect("retained row"),
            Some(b"row-1".to_vec())
        );
        kernel.release(&mut lease_one).expect("release first lease");
        assert_eq!(
            kernel
                .get(CommitId(1), catalog)
                .expect("sibling retained read"),
            Some(b"catalog-1".to_vec())
        );
        kernel
            .release(&mut lease_two)
            .expect("release second lease");
        assert!(matches!(
            kernel.get(CommitId(1), row),
            Err(DbError::StorageSnapshotUnavailable { snapshot: 1, .. })
        ));
        kernel.checkpoint().expect("idempotent checkpoint");
        kernel.verify().expect("integrity verification");
        drop(kernel);

        let mut reopened = SeerKernel::open(&config).expect("reopen SeerKernel");
        assert_eq!(reopened.commit_id(), CommitId(2));
        let mut retained = reopened.retain(CommitId(1)).expect("reopen retained root");
        assert_eq!(
            reopened
                .get(CommitId(1), old_index)
                .expect("reopened old index"),
            Some(row.to_vec())
        );
        reopened
            .release(&mut retained)
            .expect("release reopened root");
        reopened.compact().expect("bounded compaction");
    }

    #[test]
    fn snapshot_and_restore_create_independent_writable_history() {
        let source_directory = tempfile::tempdir().expect("source directory");
        let archive_parent = tempfile::tempdir().expect("archive parent");
        let restored_parent = tempfile::tempdir().expect("restore parent");
        let archive = archive_parent.path().join("archive");
        let restored_directory = restored_parent.path().join("restored");
        let source_config = SeerKernelConfig::new(source_directory.path().join("seerdb"));
        let restored_config = SeerKernelConfig::new(restored_directory.clone());
        let mut source = SeerKernel::create(&source_config).expect("create source");

        source
            .commit(
                CommitId(0),
                &[KvMutation::Put {
                    key: b"name".to_vec(),
                    value: b"source".to_vec(),
                }],
            )
            .expect("seed source");
        source.checkpoint().expect("checkpoint source");
        let snapshot = source.snapshot(&archive).expect("snapshot source");
        assert_eq!(snapshot.source.commit_id.get(), 1);
        assert_eq!(snapshot.destination.commit_id.get(), 1);
        assert!(snapshot.verified_pages > 0);

        let (mut restored, restore) =
            SeerKernel::restore(&restored_config, &archive).expect("restore archive");
        assert_eq!(restore.source.commit_id.get(), 1);
        assert_eq!(restore.destination.commit_id.get(), 1);
        assert_eq!(restore.verified_pages, snapshot.verified_pages);
        assert_eq!(restored.commit_id(), CommitId(1));
        let source_identity = source.storage_identity().expect("source identity");
        let restored_identity = restored.storage_identity().expect("restored identity");
        assert_ne!(source_identity, restored_identity);
        let source_snapshot = source
            .snapshot_identity(CommitId(1))
            .expect("source snapshot identity");
        assert!(matches!(
            restored.retain_snapshot(source_snapshot),
            Err(DbError::StorageSnapshotUnavailable { snapshot: 1, .. })
        ));
        assert_eq!(
            restored.get(CommitId(1), b"name").expect("restored read"),
            Some(b"source".to_vec())
        );

        restored
            .commit(
                CommitId(1),
                &[KvMutation::Put {
                    key: b"name".to_vec(),
                    value: b"restored".to_vec(),
                }],
            )
            .expect("advance restored history");
        restored.checkpoint().expect("checkpoint restored");
        restored.verify().expect("verify restored");
        drop(restored);
        drop(source);

        let mut source = SeerKernel::open(&source_config).expect("reopen source");
        assert_eq!(source.commit_id(), CommitId(1));
        assert_eq!(
            source.get(CommitId(1), b"name").expect("source read"),
            Some(b"source".to_vec())
        );
        source.verify().expect("verify source");
    }

    #[test]
    fn current_read_view_cache_shares_views_until_invalidation() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let kernel = SeerKernel::create(&config).expect("create SeerKernel");
        kernel
            .commit(
                CommitId(0),
                &[KvMutation::Put {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }],
            )
            .expect("seed commit");

        let first = kernel
            .begin_current_read_view()
            .expect("begin first current view");
        let second = kernel
            .begin_current_read_view()
            .expect("begin second current view");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            kernel
                .get(CommitId(1), b"key")
                .expect("cached current read"),
            Some(b"value".to_vec())
        );

        let cached = kernel
            .current_view
            .lock()
            .expect("current view cache")
            .as_ref()
            .expect("cached current view")
            .clone();
        drop(first);
        drop(second);
        assert!(Arc::strong_count(&cached) >= 2);

        kernel
            .commit(
                CommitId(1),
                &[KvMutation::Put {
                    key: b"key".to_vec(),
                    value: b"next".to_vec(),
                }],
            )
            .expect("advance current generation");
        assert_eq!(
            kernel.get(CommitId(2), b"key").expect("new current read"),
            Some(b"next".to_vec())
        );
        let current = kernel
            .current_view
            .lock()
            .expect("current view cache")
            .as_ref()
            .expect("new cached current view")
            .clone();
        assert!(!Arc::ptr_eq(&cached, &current));
    }

    #[test]
    fn snapshot_lease_cannot_be_released_by_a_different_history() {
        let first_directory = tempfile::tempdir().expect("first directory");
        let second_directory = tempfile::tempdir().expect("second directory");
        let first_config = SeerKernelConfig::new(first_directory.path().join("seerdb"));
        let second_config = SeerKernelConfig::new(second_directory.path().join("seerdb"));
        let mut first = SeerKernel::create(&first_config).expect("create first");
        let mut second = SeerKernel::create(&second_config).expect("create second");
        first
            .commit(
                CommitId(0),
                &[KvMutation::Put {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }],
            )
            .expect("first commit");
        let mut lease = first.retain(CommitId(1)).expect("first lease");
        assert!(matches!(
            second.release(&mut lease),
            Err(DbError::StorageSnapshotUnavailable { snapshot: 1, .. })
        ));
        first.release(&mut lease).expect("release first lease");
    }

    #[test]
    fn dropped_snapshot_lease_releases_durable_root() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut kernel = SeerKernel::create(&config).expect("create SeerKernel");
        kernel
            .commit(CommitId(0), &batch(1, b"row", b"index", b"catalog"))
            .expect("publish seed");

        let lease = kernel.retain(CommitId(1)).expect("retain root");
        assert_eq!(kernel.leases.lock().unwrap().leases.len(), 1);
        drop(lease);
        assert!(kernel.leases.lock().unwrap().leases.is_empty());

        kernel.compact().expect("released root is reclaimable");
    }

    #[cfg(feature = "seerdb-fault-injection")]
    #[test]
    fn fenced_compaction_failure_maps_to_reopen_required() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut kernel = SeerKernel::create(&config).expect("create SeerKernel");
        for commit in 1..=8 {
            kernel
                .commit(
                    CommitId(commit - 1),
                    &[KvMutation::Put {
                        key: b"seed".to_vec(),
                        value: format!("value-{commit}").into_bytes(),
                    }],
                )
                .expect("seed commit");
        }
        kernel
            .inject_fault(crate::FaultPoint::AfterManifestPublish)
            .expect("arm compaction fault");

        let result = kernel.compact();
        assert!(result.is_err(), "unexpected compaction result: {result:?}");
        assert!(matches!(
            result,
            Err(DbError::StorageRecoveryRequired { .. })
        ));
        assert!(kernel.durability_status().expect("status").write_fenced);
    }

    #[cfg(feature = "seerdb-fault-injection")]
    #[test]
    fn failed_last_lease_release_preserves_token_for_recovery() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config = SeerKernelConfig::new(directory.path().join("seerdb"));
        let mut kernel = SeerKernel::create(&config).expect("create SeerKernel");
        kernel
            .commit(
                CommitId(0),
                &[KvMutation::Put {
                    key: b"seed".to_vec(),
                    value: b"before".to_vec(),
                }],
            )
            .expect("seed commit");
        let mut lease = kernel.retain(CommitId(1)).expect("retain seed");

        kernel
            .inject_fault(crate::FaultPoint::AfterManifestPublish)
            .expect("arm publication fault");
        assert!(
            kernel
                .commit(
                    CommitId(1),
                    &[KvMutation::Put {
                        key: b"seed".to_vec(),
                        value: b"after".to_vec(),
                    }],
                )
                .is_err()
        );

        assert!(matches!(
            kernel.release(&mut lease),
            Err(DbError::StorageRecoveryRequired { .. })
        ));
        assert_eq!(kernel.active_lease_count(), 1);
    }

    #[test]
    fn kernel_and_snapshot_leases_remain_send() {
        assert_send::<SeerKernel>();
        assert_send::<SnapshotLease>();
    }
}
