//! Adapter for the legacy durable byte store used by the temporary backend.
//!
//! The relational layer talks only to [`StorageKernel`]. This adapter keeps
//! the existing temporary backend's on-disk behavior while allowing it to use
//! the same relational implementation as SeerDB.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::fault::NoFaults;
use crate::kernel::StorageKernel;
use crate::model::{IndexId, Key, Mutation};
use crate::store::{Database, DatabaseConfig, DatabaseMetrics};
use crate::{
    AttemptRecord, CommitId, CommitOutcome, DbError, DurabilityStatus, KvMutation, Result,
    SnapshotIdentity, StorageIdentity, TransactionAttemptId,
};

const INDEX_NAMESPACE: u8 = 0x20;
const INDEX_PREFIX_LEN: usize = 1 + 8 + 8;

fn legacy_catalog_key() -> Key {
    Key::new(u64::MAX, 0)
}

pub struct TemporaryReadView {
    transaction: crate::Transaction,
}

pub struct TemporaryKernel {
    database: Mutex<Database>,
}

impl TemporaryKernel {
    pub(crate) fn create(config: DatabaseConfig) -> Result<Self> {
        Ok(Self {
            database: Mutex::new(Database::create(config)?),
        })
    }

    pub(crate) fn open(config: DatabaseConfig) -> Result<Self> {
        let mut faults = NoFaults;
        Ok(Self {
            database: Mutex::new(Database::open(config, &mut faults)?),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Database>> {
        self.database
            .lock()
            .map_err(|_| DbError::InvalidState("temporary kernel lock poisoned".to_owned()))
    }

    fn mutations(mutations: &[KvMutation]) -> Vec<Mutation> {
        mutations
            .iter()
            .map(|mutation| match mutation {
                KvMutation::Put { key, value } if key == b"\x00omendb/catalog/v1" => {
                    Mutation::Put {
                        key: legacy_catalog_key(),
                        value: value.clone(),
                    }
                }
                KvMutation::Put { key, value } => Mutation::BytePut {
                    key: key.clone(),
                    value: value.clone(),
                },
                KvMutation::Delete { key } if key == b"\x00omendb/catalog/v1" => Mutation::Delete {
                    key: legacy_catalog_key(),
                },
                KvMutation::Delete { key } => Mutation::ByteDelete { key: key.clone() },
            })
            .collect()
    }

    fn get_value(database: &Database, snapshot: CommitId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key == b"\x00omendb/catalog/v1" {
            return database.get(snapshot, legacy_catalog_key());
        }
        if let Some(value) = database.get_bytes(snapshot, key.to_vec())? {
            return Ok(Some(value));
        }
        if database.has_bytes_history(snapshot, key)? {
            return Ok(None);
        }
        Self::legacy_index_value(database, snapshot, key)
    }

    /// Older temporary-format databases stored secondary indexes in the
    /// engine's typed index namespace. The shared relational layer now uses
    /// ordinary byte keys, so expose those legacy entries through the adapter
    /// until a database is rewritten by a deliberate format migration.
    fn legacy_index_value(
        database: &Database,
        snapshot: CommitId,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        if key.len() < INDEX_PREFIX_LEN || key[0] != INDEX_NAMESPACE {
            return Ok(None);
        }
        let index = IndexId(u64::from_be_bytes(
            key[9..INDEX_PREFIX_LEN]
                .try_into()
                .expect("index prefix width"),
        ));
        let entries = match database.index_scan_bytes(snapshot, index, &[], &[u8::MAX], usize::MAX)
        {
            Ok(entries) => entries,
            Err(DbError::InvalidState(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let prefix = &key[..INDEX_PREFIX_LEN];
        for (values, identity) in entries {
            let mut candidate = prefix.to_vec();
            candidate.extend_from_slice(&values);
            candidate.extend_from_slice(&identity);
            if candidate == key {
                return if database.has_bytes_history(snapshot, key)? {
                    Ok(None)
                } else {
                    Ok(Some(identity))
                };
            }
        }
        Ok(None)
    }

    fn legacy_index_scan(
        database: &Database,
        snapshot: CommitId,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if start.len() < INDEX_PREFIX_LEN || start[0] != INDEX_NAMESPACE {
            return Ok(Vec::new());
        }
        let index = IndexId(u64::from_be_bytes(
            start[9..INDEX_PREFIX_LEN]
                .try_into()
                .expect("index prefix width"),
        ));
        let entries = match database.index_scan_bytes(snapshot, index, &[], &[u8::MAX], usize::MAX)
        {
            Ok(entries) => entries,
            Err(DbError::InvalidState(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let prefix = &start[..INDEX_PREFIX_LEN];
        let mut rows = Vec::new();
        for (values, identity) in entries {
            let mut key = prefix.to_vec();
            key.extend_from_slice(&values);
            key.extend_from_slice(&identity);
            if key.as_slice() >= start
                && key.as_slice() < end
                && !database.has_bytes_history(snapshot, &key)?
            {
                rows.push((key, identity));
            }
        }
        Ok(rows)
    }

    fn scan_value(
        database: &Database,
        snapshot: CommitId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut rows = BTreeMap::from_iter(database.scan_bytes(
            snapshot,
            start.to_vec(),
            end.to_vec(),
            usize::MAX,
        )?);
        for (key, value) in Self::legacy_index_scan(database, snapshot, start, end)? {
            rows.entry(key).or_insert(value);
        }
        Ok(rows.into_iter().take(limit).collect())
    }

    pub(crate) fn requires_recovery(&self) -> bool {
        self.lock().map(|db| db.requires_recovery()).unwrap_or(true)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.lock().map(|db| db.generation()).unwrap_or_default()
    }

    pub(crate) fn compact_with_limit(
        &mut self,
        max_work_units: usize,
    ) -> Result<crate::store::CompactionReport> {
        self.lock()?.compact_with_key_budget(max_work_units)
    }
}

impl StorageKernel for TemporaryKernel {
    type ReadView = TemporaryReadView;
    type Lease = CommitId;
    type IntegrityReport = ();
    type CompactionReport = crate::store::CompactionReport;
    type Metrics = DatabaseMetrics;

    fn close(self) -> Result<()> {
        self.database
            .into_inner()
            .map_err(|_| DbError::InvalidState("temporary kernel lock poisoned".to_owned()))?
            .close()
    }

    fn commit_id(&self) -> CommitId {
        self.database
            .lock()
            .expect("temporary kernel lock")
            .commit_id()
    }

    fn commit(&self, expected: CommitId, mutations: &[KvMutation]) -> Result<CommitOutcome> {
        let mut database = self.lock()?;
        if database.commit_id() != expected {
            return Err(DbError::SerializationConflict {
                snapshot: expected.0,
                current: database.commit_id().0,
            });
        }
        let mut faults = NoFaults;
        let commit = database.commit(Self::mutations(mutations), &mut faults)?;
        Ok(CommitOutcome {
            commit,
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
        let mut database = self.lock()?;
        if database.resolve_attempt(attempt).is_none() && database.commit_id() != expected {
            return Err(DbError::SerializationConflict {
                snapshot: expected.0,
                current: database.commit_id().0,
            });
        }
        let mut faults = NoFaults;
        let commit = database.commit_with_attempt(
            expected,
            attempt,
            Self::mutations(mutations),
            &mut faults,
        )?;
        Ok(CommitOutcome {
            commit,
            acknowledged: true,
            requires_reopen: false,
        })
    }

    fn resolve_attempt(&self, attempt: TransactionAttemptId) -> Result<Option<AttemptRecord>> {
        Ok(self.lock()?.resolve_attempt(attempt))
    }

    fn attempt_records(&self, limit: usize) -> Result<Vec<AttemptRecord>> {
        self.lock()?.attempt_records(limit)
    }

    fn import_attempt_records(&mut self, records: &[AttemptRecord]) -> Result<Vec<AttemptRecord>> {
        let mut faults = NoFaults;
        self.lock()?.import_attempt_records(records, &mut faults)
    }

    fn forget_attempts(&mut self, attempts: &[TransactionAttemptId]) -> Result<usize> {
        let mut faults = NoFaults;
        self.lock()?.forget_attempts(attempts, &mut faults)
    }

    fn get(&self, snapshot: CommitId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let database = self.lock()?;
        Self::get_value(&database, snapshot, key)
    }

    fn scan(
        &self,
        snapshot: CommitId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let database = self.lock()?;
        Self::scan_value(&database, snapshot, start, end, limit)
    }

    fn begin_current_read_view(&self) -> Result<Arc<Self::ReadView>> {
        let database = self.lock()?;
        Ok(Arc::new(TemporaryReadView {
            transaction: database.begin(),
        }))
    }

    fn view_get(&self, view: &Self::ReadView, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let database = self.lock()?;
        Self::get_value(&database, view.transaction.snapshot(), key)
    }

    fn view_scan(
        &self,
        view: &Self::ReadView,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let database = self.lock()?;
        Self::scan_value(&database, view.transaction.snapshot(), start, end, limit)
    }

    fn view_commit_id(&self, view: &Self::ReadView) -> CommitId {
        view.transaction.snapshot()
    }

    fn retain(&mut self, commit: CommitId) -> Result<Self::Lease> {
        self.lock()?.retain(commit)?;
        Ok(commit)
    }

    fn retain_current(&mut self) -> Result<Self::Lease> {
        let commit = self.commit_id();
        self.retain(commit)
    }

    fn release_lease(&mut self, lease: &mut Self::Lease) -> Result<()> {
        self.lock()?.release(*lease);
        Ok(())
    }

    fn retained_snapshot_count(&self) -> usize {
        self.lock()
            .map(|database| database.retained_snapshot_count())
            .unwrap_or_default()
    }

    fn retained_snapshot_commits(&self) -> Vec<CommitId> {
        self.lock()
            .map(|database| database.retained_snapshot_commits())
            .unwrap_or_default()
    }

    fn checkpoint(&mut self) -> Result<Self::IntegrityReport> {
        let mut faults = NoFaults;
        self.lock()?.checkpoint(&mut faults)
    }

    fn verify(&mut self) -> Result<Self::IntegrityReport> {
        let (config, commit, generation, indexes) = {
            let database = self.lock()?;
            if database.requires_recovery() {
                return Err(DbError::RecoveryRequired);
            }
            (
                database.database_config(),
                database.commit_id(),
                database.generation(),
                database.secondary_index_ids(),
            )
        };
        let mut faults = NoFaults;
        let recovered = Database::open_for_verification(config, &mut faults)?;
        if recovered.commit_id() != commit
            || recovered.generation() != generation
            || recovered.secondary_index_ids() != indexes
        {
            return Err(DbError::Corruption {
                artifact: "temporary database",
                reason: "reopened durable state differs from the active handle".to_owned(),
            });
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<Self::CompactionReport> {
        self.lock()?.compact()
    }

    fn compact_with_limit(&mut self, max_work_units: usize) -> Result<Self::CompactionReport> {
        TemporaryKernel::compact_with_limit(self, max_work_units)
    }

    fn metrics(&self) -> Result<Self::Metrics> {
        Ok(self.lock()?.metrics().clone())
    }

    fn published_commits(&self) -> Result<Vec<CommitId>> {
        self.lock()?.published_commits()
    }

    fn snapshot_identity(&self, commit: CommitId) -> Result<SnapshotIdentity> {
        Ok(SnapshotIdentity {
            storage: self.storage_identity()?,
            commit,
        })
    }

    fn storage_identity(&self) -> Result<StorageIdentity> {
        Ok(self.lock()?.storage_identity())
    }

    fn durability_status(&self) -> Result<DurabilityStatus> {
        let database = self.lock()?;
        Ok(DurabilityStatus {
            storage: database.storage_identity(),
            generation: database.generation(),
            commit: database.commit_id(),
            pending_mutations: 0,
            write_fenced: database.requires_recovery(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attempt_publication_preserves_compare_and_swap_contract() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let kernel = TemporaryKernel::create(DatabaseConfig {
            directory: directory.path().join("db"),
        })
        .expect("create kernel");
        let first = [KvMutation::Put {
            key: b"first".to_vec(),
            value: b"value".to_vec(),
        }];
        let outcome = kernel.commit(CommitId(0), &first).expect("initial commit");
        assert_eq!(outcome.commit, CommitId(1));

        let attempt = TransactionAttemptId::new([7; 16]);
        let error = kernel
            .commit_with_attempt(
                CommitId(0),
                attempt,
                &[KvMutation::Put {
                    key: b"second".to_vec(),
                    value: b"value".to_vec(),
                }],
            )
            .expect_err("stale expected frontier must reject a new attempt");
        assert!(matches!(error, DbError::SerializationConflict { .. }));
    }
}
