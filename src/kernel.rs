//! The storage-kernel seam: transactional versioned byte-key/value storage.
//!
//! One [`StorageKernel`] implementation backs one relational store. The
//! contract is deliberately narrower than any engine's full surface: atomic
//! CAS batches over [`KvMutation`]s, point/range reads at an explicit
//! snapshot, snapshot-retention leases that pin history against reclamation,
//! durable idempotency (attempt) records, and maintenance entry points.
//!
//! Engine-specific capabilities (fault injection, physical archive/restore,
//! legacy migration) stay as inherent methods on concrete kernels; generic
//! relational code never reaches past this trait.
//!
//! OmenDB ships [`crate::SeerKernel`] as its durable Rust engine,
//! [`crate::TemporaryKernel`] as a compatibility adapter, and
//! [`InMemoryKernel`] for tests and conformance work. New backends implement
//! this trait plus their config/constructor pair.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::seer_kernel::CommitOutcome;
use crate::seer_kernel::SnapshotIdentity;
use crate::{
    AttemptRecord, CommitId, DbError, DurabilityStatus, KvMutation, Result, StorageIdentity,
    TransactionAttemptId,
};

/// A transactional versioned byte-key/value storage engine.
pub trait StorageKernel: Send {
    /// An immutable point-in-time view over committed state. Views must stay
    /// readable after newer commits publish; engines back them with whatever
    /// generation-pinning mechanism they own.
    type ReadView: Send;

    /// Caller-owned proof that one snapshot's history is pinned against
    /// reclamation. Dropping or releasing the lease un-pins it.
    type Lease: Send;

    /// Backend-specific integrity report from checkpoint/verify passes.
    type IntegrityReport: Send;
    /// Backend-specific compaction report.
    type CompactionReport: Send;
    /// Backend-specific operational counters.
    type Metrics: Send;

    /// Flush and close this writer handle, releasing engine locks. The
    /// kernel is unusable afterwards.
    fn close(self) -> Result<()>
    where
        Self: Sized;

    /// Current visible commit frontier.
    fn commit_id(&self) -> CommitId;

    /// Publish mutations atomically iff the frontier still equals
    /// `expected`. Returns the new frontier; a mismatch is a retryable
    /// serialization conflict with no side effects.
    fn commit(&self, expected: CommitId, mutations: &[KvMutation]) -> Result<CommitOutcome>;

    /// Publish with a durable idempotency record. Re-submitting the same
    /// attempt with identical mutations returns the original commit without
    /// republishing; different mutations reject with
    /// [`DbError::IdempotencyConflict`].
    fn commit_with_attempt(
        &self,
        expected: CommitId,
        attempt: TransactionAttemptId,
        mutations: &[KvMutation],
    ) -> Result<CommitOutcome>;

    /// Resolve one attempt against durable state: `Some` means its batch was
    /// published and must not rerun.
    fn resolve_attempt(&self, attempt: TransactionAttemptId) -> Result<Option<AttemptRecord>>;

    /// Durable attempt records in deterministic identity order, up to
    /// `limit`. Exceeding `limit` fails with a resource error rather than
    /// truncating silently.
    fn attempt_records(&self, limit: usize) -> Result<Vec<AttemptRecord>>;

    /// Import externally supplied attempt records in one atomic batch.
    fn import_attempt_records(&mut self, records: &[AttemptRecord]) -> Result<Vec<AttemptRecord>>;

    /// Forget resolved attempt identities; forgotten IDs must never be
    /// reused. Returns how many existed.
    fn forget_attempts(&mut self, attempts: &[TransactionAttemptId]) -> Result<usize>;

    /// Read one key at `snapshot`. Reading the current frontier must not
    /// block behind publication; historical snapshots require a held lease
    /// (engines refuse unpinned historical reads).
    fn get(&self, snapshot: CommitId, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Range scan `[start, end)` at `snapshot`, at most `limit` pairs.
    fn scan(
        &self,
        snapshot: CommitId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Capture an immutable view of the current frontier. The view stays
    /// consistent even as later commits publish.
    fn begin_current_read_view(&self) -> Result<Arc<Self::ReadView>>;

    /// Point read through a previously captured view.
    fn view_get(&self, view: &Self::ReadView, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Range scan `[start, end)` through a previously captured view.
    fn view_scan(
        &self,
        view: &Self::ReadView,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Frontier commit this view was captured at. Views are atomic with
    /// their frontier observation, so relational snapshot identity comes
    /// from the view itself rather than a separate read.
    fn view_commit_id(&self, view: &Self::ReadView) -> CommitId;

    /// Pin `commit`'s history behind a caller-owned lease.
    fn retain(&mut self, commit: CommitId) -> Result<Self::Lease>;

    /// Pin the current frontier atomically with observing it.
    fn retain_current(&mut self) -> Result<Self::Lease>;

    /// Release one lease. Idempotent per token; engines may keep other
    /// leases on the same snapshot pinned.
    fn release_lease(&mut self, lease: &mut Self::Lease) -> Result<()>;

    /// Number of commits currently pinned by leases.
    fn retained_snapshot_count(&self) -> usize;

    /// Commits currently pinned by leases, sorted.
    fn retained_snapshot_commits(&self) -> Vec<CommitId>;

    /// Publish current state through the engine's checkpoint protocol.
    fn checkpoint(&mut self) -> Result<Self::IntegrityReport>;

    /// Run a read-only integrity pass; never repairs state.
    fn verify(&mut self) -> Result<Self::IntegrityReport>;

    /// Reclaim dead versions/pages, respecting every held lease.
    fn compact(&mut self) -> Result<Self::CompactionReport>;

    /// Reclaim at most `max_work_units` units, respecting every held lease.
    /// Engines without bounded maintenance may use the unbounded operation.
    fn compact_with_limit(&mut self, max_work_units: usize) -> Result<Self::CompactionReport> {
        let _ = max_work_units;
        self.compact()
    }

    /// Operational counters snapshot.
    fn metrics(&self) -> Result<Self::Metrics>;

    /// Commits visible at this frontier, sorted (authoritative catalog).
    fn published_commits(&self) -> Result<Vec<CommitId>>;

    /// Qualify a snapshot commit with this kernel's database/history
    /// identity.
    fn snapshot_identity(&self, commit: CommitId) -> Result<SnapshotIdentity>;

    /// Stable engine/storage identity for this database directory.
    fn storage_identity(&self) -> Result<StorageIdentity>;

    /// Lifecycle/publication status projection.
    fn durability_status(&self) -> Result<DurabilityStatus>;
}

/// In-memory [`StorageKernel`] for tests and conformance work: full-copy
/// snapshot views over one `BTreeMap`, lease counting, and attempt records
/// stored in reserved keyspace exactly like production kernels.
/// Snapshot view for [`InMemoryKernel`]: frozen state plus the frontier it
/// was captured at.
pub struct InMemoryView {
    state: BTreeMap<Vec<u8>, Vec<u8>>,
    commit: CommitId,
}

#[derive(Default)]
pub struct InMemoryKernel {
    /// Committed state per commit frontier; index 0 is the empty database.
    /// Mutex mirrors production kernels: publication serializes behind it,
    /// which is what lets the trait take `&self` for commits.
    generations: Mutex<Vec<BTreeMap<Vec<u8>, Vec<u8>>>>,
    retained: Mutex<Vec<CommitId>>,
}

impl InMemoryKernel {
    pub fn new() -> Self {
        Self {
            generations: Mutex::new(vec![BTreeMap::new()]),
            retained: Mutex::new(Vec::new()),
        }
    }

    fn state_at(&self, snapshot: CommitId) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let generations = self
            .generations
            .lock()
            .map_err(|_| kernel_lock_poisoned())?;
        let index = snapshot.0 as usize;
        let state = generations
            .get(index)
            .ok_or_else(|| DbError::StorageSnapshotUnavailable {
                snapshot: snapshot.0,
                reason: "snapshot is not present in this history".into(),
            })?;
        Ok(state
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

fn kernel_lock_poisoned() -> DbError {
    DbError::InvalidState("in-memory kernel lock poisoned".to_owned())
}

impl StorageKernel for InMemoryKernel {
    type ReadView = InMemoryView;
    type Lease = CommitId;
    type IntegrityReport = usize;
    type CompactionReport = usize;
    type Metrics = ();

    fn close(self) -> Result<()> {
        // Dropping releases all state; production engines flush and release
        // OS-level handles here instead.
        Ok(())
    }

    fn commit_id(&self) -> CommitId {
        let generations = self.generations.lock().expect("generations");
        CommitId(generations.len() as u64 - 1)
    }

    fn commit(&self, expected: CommitId, mutations: &[KvMutation]) -> Result<CommitOutcome> {
        let mut generations = self
            .generations
            .lock()
            .map_err(|_| kernel_lock_poisoned())?;
        let current = CommitId(generations.len() as u64 - 1);
        if current != expected {
            return Err(DbError::SerializationConflict {
                snapshot: expected.0,
                current: current.0,
            });
        }
        let mut next = generations[expected.0 as usize].clone();
        for mutation in mutations {
            match mutation {
                KvMutation::Put { key, value } => {
                    next.insert(key.clone(), value.clone());
                }
                KvMutation::Delete { key } => {
                    next.remove(key);
                }
            }
        }
        generations.push(next);
        Ok(CommitOutcome {
            commit: CommitId(expected.0 + 1),
            acknowledged: true,
            requires_reopen: false,
        })
    }

    fn commit_with_attempt(
        &self,
        expected: CommitId,
        attempt: TransactionAttemptId,
        mutations: &[KvMutation],
    ) -> Result<CommitOutcome> {
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
                attempt: record.attempt,
                existing_digest: record.digest,
                requested_digest: digest,
            });
        }
        let record = AttemptRecord {
            attempt,
            commit: CommitId(expected.0 + 1),
            digest,
        };
        let mut durable = mutations.to_vec();
        durable.push(KvMutation::Put {
            key: crate::attempt::seer_key(attempt),
            value: crate::attempt::encode_record(record).to_vec(),
        });
        self.commit(expected, &durable)
    }

    fn resolve_attempt(&self, attempt: TransactionAttemptId) -> Result<Option<AttemptRecord>> {
        let bytes = self.get(self.commit_id(), &crate::attempt::seer_key(attempt))?;
        let Some(bytes) = bytes else {
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

    fn attempt_records(&self, limit: usize) -> Result<Vec<AttemptRecord>> {
        let (start, end) = crate::attempt::seer_key_range();
        let records = self
            .scan(self.commit_id(), &start, &end, limit.saturating_add(1))?
            .into_iter()
            .map(|(_, value)| crate::attempt::decode_record(&value))
            .collect::<Result<Vec<_>>>()?;
        if records.len() > limit {
            return Err(DbError::SnapshotCaptureLimit {
                resource: "transaction attempts",
                limit,
            });
        }
        Ok(records)
    }

    fn import_attempt_records(&mut self, records: &[AttemptRecord]) -> Result<Vec<AttemptRecord>> {
        for record in records {
            if self.resolve_attempt(record.attempt)?.is_some() {
                return Err(DbError::InvalidState(
                    "duplicate transaction attempt import".to_owned(),
                ));
            }
        }
        let target = CommitId(self.commit_id().0 + 1);
        let mutations = records
            .iter()
            .map(|record| KvMutation::Put {
                key: crate::attempt::seer_key(record.attempt),
                value: crate::attempt::encode_record(AttemptRecord {
                    attempt: record.attempt,
                    commit: target,
                    digest: record.digest,
                })
                .to_vec(),
            })
            .collect::<Vec<_>>();
        self.commit(self.commit_id(), &mutations)?;
        Ok(records
            .iter()
            .map(|record| AttemptRecord {
                attempt: record.attempt,
                commit: target,
                digest: record.digest,
            })
            .collect())
    }

    fn forget_attempts(&mut self, attempts: &[TransactionAttemptId]) -> Result<usize> {
        let existing: Vec<TransactionAttemptId> = attempts
            .iter()
            .copied()
            .filter(|attempt| self.resolve_attempt(*attempt).is_ok_and(|r| r.is_some()))
            .collect();
        if existing.is_empty() {
            return Ok(0);
        }
        let mutations = existing
            .iter()
            .map(|attempt| KvMutation::Delete {
                key: crate::attempt::seer_key(*attempt),
            })
            .collect::<Vec<_>>();
        self.commit(self.commit_id(), &mutations)?;
        Ok(existing.len())
    }

    fn get(&self, snapshot: CommitId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .state_at(snapshot)?
            .into_iter()
            .find(|(candidate, _)| candidate == key)
            .map(|(_, value)| value))
    }

    fn scan(
        &self,
        snapshot: CommitId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .state_at(snapshot)?
            .into_iter()
            .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end)
            .take(limit)
            .collect())
    }

    fn begin_current_read_view(&self) -> Result<Arc<Self::ReadView>> {
        let mut generations = self
            .generations
            .lock()
            .map_err(|_| kernel_lock_poisoned())?;
        let commit = CommitId(generations.len() as u64 - 1);
        let state = generations.last_mut().expect("frontier");
        let taken = std::mem::take(state);
        *state = taken.clone();
        Ok(Arc::new(InMemoryView {
            state: taken,
            commit,
        }))
    }

    fn view_get(&self, view: &Self::ReadView, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(view.state.get(key).cloned())
    }

    fn view_commit_id(&self, view: &Self::ReadView) -> CommitId {
        view.commit
    }

    fn view_scan(
        &self,
        view: &Self::ReadView,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(view
            .state
            .range(start.to_vec()..end.to_vec())
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn retain(&mut self, commit: CommitId) -> Result<Self::Lease> {
        self.state_at(commit)?;
        let mut retained = self.retained.lock().map_err(|_| kernel_lock_poisoned())?;
        if !retained.contains(&commit) {
            retained.push(commit);
        }
        Ok(commit)
    }

    fn retain_current(&mut self) -> Result<Self::Lease> {
        let frontier = self.commit_id();
        self.retain(frontier)
    }

    fn release_lease(&mut self, lease: &mut Self::Lease) -> Result<()> {
        let mut retained = self.retained.lock().map_err(|_| kernel_lock_poisoned())?;
        retained.retain(|commit| commit != lease);
        Ok(())
    }

    fn retained_snapshot_count(&self) -> usize {
        self.retained
            .lock()
            .map(|retained| retained.len())
            .unwrap_or(0)
    }

    fn retained_snapshot_commits(&self) -> Vec<CommitId> {
        self.retained
            .lock()
            .map(|retained| retained.clone())
            .unwrap_or_default()
    }

    fn checkpoint(&mut self) -> Result<Self::IntegrityReport> {
        let generations = self
            .generations
            .lock()
            .map_err(|_| kernel_lock_poisoned())?;
        Ok(generations.len())
    }

    fn verify(&mut self) -> Result<Self::IntegrityReport> {
        self.checkpoint()
    }

    fn compact(&mut self) -> Result<Self::CompactionReport> {
        Ok(0)
    }

    fn metrics(&self) -> Result<Self::Metrics> {
        Ok(())
    }

    fn published_commits(&self) -> Result<Vec<CommitId>> {
        let count = {
            let generations = self
                .generations
                .lock()
                .map_err(|_| kernel_lock_poisoned())?;
            generations.len() as u64
        };
        Ok((0..count).map(CommitId).collect())
    }

    fn storage_identity(&self) -> Result<StorageIdentity> {
        Ok(StorageIdentity {
            database_id: [0; 16],
            history_id: 0,
        })
    }

    fn snapshot_identity(&self, commit: CommitId) -> Result<SnapshotIdentity> {
        Ok(SnapshotIdentity {
            storage: self.storage_identity()?,
            commit,
        })
    }

    fn durability_status(&self) -> Result<DurabilityStatus> {
        Ok(DurabilityStatus {
            storage: self.storage_identity()?,
            generation: self.commit_id().0,
            commit: self.commit_id(),
            pending_mutations: 0,
            write_fenced: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(key: &str, value: &str) -> KvMutation {
        KvMutation::Put {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn exercise_generic_kernel<K: StorageKernel>(kernel: K) {
        // CAS commit: stale expected base conflicts without side effects.
        let outcome = kernel.commit(CommitId(0), &[kv("a", "1")]).expect("first");
        assert_eq!(outcome.commit, CommitId(1));
        assert!(matches!(
            kernel.commit(CommitId(0), &[kv("b", "2")]),
            Err(DbError::SerializationConflict { .. })
        ));
        assert_eq!(kernel.get(CommitId(1), b"b").expect("read"), None);
        // Snapshot isolation: an old view stays frozen across publications.
        let view = kernel.begin_current_read_view().expect("view");
        kernel
            .commit(CommitId(1), &[kv("a", "2"), kv("b", "x")])
            .expect("second");
        assert_eq!(
            kernel.view_get(&view, b"a").expect("frozen read"),
            Some(b"1".to_vec())
        );
        assert_eq!(
            kernel
                .view_scan(&view, b"a", b"z", 10)
                .expect("frozen scan")
                .len(),
            1
        );
        assert_eq!(kernel.view_get(&view, b"b").expect("frozen miss"), None);

        // Historical reads require a lease; released history is refused.
        let mut kernel = kernel;
        let mut lease = kernel.retain(CommitId(1)).expect("retain");
        assert_eq!(
            kernel
                .get(CommitId(1), b"a")
                .expect("leased historical read"),
            Some(b"1".to_vec())
        );
        assert_eq!(kernel.retained_snapshot_count(), 1);
        kernel.release_lease(&mut lease).expect("release");
        assert_eq!(kernel.retained_snapshot_count(), 0);

        // Attempt records: dedup returns the original commit; conflicting
        // reuse rejects.
        let attempt = TransactionAttemptId([7; 16]);
        let first = kernel
            .commit_with_attempt(CommitId(2), attempt, &[kv("c", "3")])
            .expect("attempted commit");
        let replay = kernel
            .commit_with_attempt(CommitId(first.commit.0 - 1), attempt, &[kv("c", "3")])
            .expect("attempt replay");
        assert_eq!(replay.commit, first.commit);
        assert!(
            kernel
                .commit_with_attempt(CommitId(replay.commit.0), attempt, &[kv("c", "4")])
                .is_err()
        );
        assert_eq!(
            kernel.resolve_attempt(attempt).expect("resolve"),
            Some(AttemptRecord {
                attempt,
                commit: first.commit,
                digest: crate::attempt::digest_kv_mutations(&[kv("c", "3")]),
            })
        );
        assert_eq!(kernel.forget_attempts(&[attempt]).expect("forget"), 1);
        assert_eq!(
            kernel.resolve_attempt(attempt).expect("resolved gone"),
            None
        );
    }

    #[test]
    fn in_memory_kernel_satisfies_the_storage_contract() {
        exercise_generic_kernel(InMemoryKernel::new());
    }

    #[test]
    fn seer_kernel_satisfies_the_storage_contract_through_the_seam() {
        let directory = tempfile::tempdir().expect("tempdir");
        let kernel = crate::SeerKernel::create(&crate::SeerKernelConfig::new(
            directory.path().join("seerdb"),
        ))
        .expect("create seer kernel");
        exercise_generic_kernel(kernel);
    }

    #[test]
    fn status_projection_and_frontier_catalog_stay_consistent() {
        let mut kernel = InMemoryKernel::new();
        kernel.commit(CommitId(0), &[kv("k", "v")]).expect("commit");

        let status = kernel.durability_status().expect("status");
        assert_eq!(status.commit, CommitId(1));
        assert!(!status.write_fenced);
        assert_eq!(status.pending_mutations, 0);

        let published = kernel.published_commits().expect("published");
        assert_eq!(published, vec![CommitId(0), CommitId(1)]);

        let identity = kernel.storage_identity().expect("identity");
        assert_eq!(identity.database_id, [0; 16]);

        let lease = kernel.retain_current().expect("retain current");
        assert_eq!(lease, CommitId(1));
        kernel.verify().expect("verify");
        kernel.checkpoint().expect("checkpoint");
        kernel.compact().expect("compact");
        kernel.metrics().expect("metrics");
        kernel.close().expect("close");
    }
}

impl StorageKernel for crate::SeerKernel {
    type ReadView = seerdb_shim::ReadView;
    type Lease = crate::SnapshotLease;
    type IntegrityReport = seerdb_shim::VerificationReport;
    type CompactionReport = seerdb_shim::CompactionReport;
    type Metrics = seerdb_shim::DBMetrics;

    fn close(self) -> Result<()> {
        crate::SeerKernel::close(self)
    }

    fn commit_id(&self) -> CommitId {
        crate::SeerKernel::commit_id(self)
    }

    fn commit(&self, expected: CommitId, mutations: &[KvMutation]) -> Result<CommitOutcome> {
        crate::SeerKernel::commit(self, expected, mutations)
    }

    fn commit_with_attempt(
        &self,
        expected: CommitId,
        attempt: TransactionAttemptId,
        mutations: &[KvMutation],
    ) -> Result<CommitOutcome> {
        crate::SeerKernel::commit_with_attempt(self, expected, attempt, mutations)
    }

    fn resolve_attempt(&self, attempt: TransactionAttemptId) -> Result<Option<AttemptRecord>> {
        crate::SeerKernel::resolve_attempt(self, attempt)
    }

    fn attempt_records(&self, limit: usize) -> Result<Vec<AttemptRecord>> {
        crate::SeerKernel::attempt_records(self, limit)
    }

    fn import_attempt_records(&mut self, records: &[AttemptRecord]) -> Result<Vec<AttemptRecord>> {
        crate::SeerKernel::import_attempt_records(self, records)
    }

    fn forget_attempts(&mut self, attempts: &[TransactionAttemptId]) -> Result<usize> {
        crate::SeerKernel::forget_attempts(self, attempts)
    }

    fn get(&self, snapshot: CommitId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        crate::SeerKernel::get(self, snapshot, key)
    }

    fn scan(
        &self,
        snapshot: CommitId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        crate::SeerKernel::scan(self, snapshot, start, end, limit)
    }

    fn begin_current_read_view(&self) -> Result<Arc<Self::ReadView>> {
        crate::SeerKernel::begin_current_read_view(self)
    }

    fn view_get(&self, view: &Self::ReadView, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read_view_get(view, key)
    }

    fn view_scan(
        &self,
        view: &Self::ReadView,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.read_view_scan(view, start, end, limit)
    }

    fn view_commit_id(&self, view: &Self::ReadView) -> CommitId {
        CommitId(view.commit_id().get())
    }

    fn retain(&mut self, commit: CommitId) -> Result<Self::Lease> {
        crate::SeerKernel::retain(self, commit)
    }

    fn retain_current(&mut self) -> Result<Self::Lease> {
        self.retain_current_transaction()
    }

    fn release_lease(&mut self, lease: &mut Self::Lease) -> Result<()> {
        crate::SeerKernel::release(self, lease)
    }

    fn retained_snapshot_count(&self) -> usize {
        crate::SeerKernel::retained_snapshot_count(self)
    }

    fn retained_snapshot_commits(&self) -> Vec<CommitId> {
        crate::SeerKernel::retained_snapshot_commits(self)
    }

    fn checkpoint(&mut self) -> Result<Self::IntegrityReport> {
        crate::SeerKernel::checkpoint(self)
    }

    fn verify(&mut self) -> Result<Self::IntegrityReport> {
        crate::SeerKernel::verify(self)
    }

    fn compact(&mut self) -> Result<Self::CompactionReport> {
        crate::SeerKernel::compact(self)
    }

    fn compact_with_limit(&mut self, max_work_units: usize) -> Result<Self::CompactionReport> {
        crate::SeerKernel::compact_with_limit(self, max_work_units)
    }

    fn metrics(&self) -> Result<Self::Metrics> {
        crate::SeerKernel::metrics(self)
    }

    fn published_commits(&self) -> Result<Vec<CommitId>> {
        crate::SeerKernel::published_commits(self)
    }

    fn storage_identity(&self) -> Result<StorageIdentity> {
        crate::SeerKernel::storage_identity(self)
    }

    fn durability_status(&self) -> Result<DurabilityStatus> {
        crate::SeerKernel::durability_status(self)
    }

    fn snapshot_identity(&self, commit: CommitId) -> Result<SnapshotIdentity> {
        crate::SeerKernel::snapshot_identity(self, commit)
    }
}

/// Type aliases keeping seerdb report names out of this module's signatures.
mod seerdb_shim {
    pub use seerdb::{CompactionReport, DBMetrics, ReadView, VerificationReport};
}
