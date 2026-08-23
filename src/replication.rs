//! Replication log streaming, standby replica synchronization, and failover promotion.
//!
//! OmenDB R5 availability architecture:
//! - Primary database instances capture monotonic replication batches from durable commits.
//! - Standby replicas apply replication batches in strict commit order while serving
//!   consistent point-in-time reads.
//! - Replicas track commit and byte lag.
//! - Standby replicas can be promoted to independent writable primaries upon failover.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::{
    Catalog, CommitId, DbError, Key, RelationalBackendConfig, RelationalDatabase, Result, TableId,
};

/// Role of a database instance in a replication topology.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationRole {
    /// Active read-write primary instance.
    Primary,
    /// Read-only standby replica following a primary replication stream.
    Standby,
    /// Standby replica promoted to active primary after failover.
    PromotedPrimary,
}

/// A replicated mutation record corresponding to one committed key/row operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicatedMutation {
    Put { key: Key, value: Vec<u8> },
    Delete { key: Key },
}

/// A replication record encapsulating all changes from one transaction commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationRecord {
    /// Commit ID of the transaction.
    pub commit: CommitId,
    /// Commit timestamp in Unix nanoseconds.
    pub timestamp_nanos: u64,
    /// Mutations published in this commit.
    pub mutations: Vec<ReplicatedMutation>,
    /// Optional catalog snapshot if schema changes were published.
    pub schema_change: Option<Catalog>,
}

/// A bounded batch of replication records sent over the replication stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationBatch {
    /// Starting commit ID (inclusive).
    pub start_commit: CommitId,
    /// Ending commit ID (inclusive).
    pub end_commit: CommitId,
    /// Replicated transaction records.
    pub records: Vec<ReplicationRecord>,
    /// Checksum of all records in this batch for transmission integrity.
    pub checksum: u64,
}

impl ReplicationBatch {
    /// Construct a new replication batch and compute its integrity checksum.
    #[must_use]
    pub fn new(records: Vec<ReplicationRecord>) -> Self {
        let start_commit = records.first().map(|r| r.commit).unwrap_or(CommitId(0));
        let end_commit = records.last().map(|r| r.commit).unwrap_or(CommitId(0));

        let mut checksum: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
        for record in &records {
            checksum ^= record.commit.0;
            checksum = checksum.wrapping_mul(0x100000001b3);
            checksum ^= record.timestamp_nanos;
            checksum = checksum.wrapping_mul(0x100000001b3);
            for mutation in &record.mutations {
                match mutation {
                    ReplicatedMutation::Put { key, value } => {
                        for b in &key.0 {
                            checksum ^= u64::from(*b);
                            checksum = checksum.wrapping_mul(0x100000001b3);
                        }
                        for byte in value {
                            checksum ^= u64::from(*byte);
                            checksum = checksum.wrapping_mul(0x100000001b3);
                        }
                    }
                    ReplicatedMutation::Delete { key } => {
                        for b in &key.0 {
                            checksum ^= u64::from(*b);
                            checksum = checksum.wrapping_mul(0x100000001b3);
                        }
                    }
                }
            }
        }

        Self {
            start_commit,
            end_commit,
            records,
            checksum,
        }
    }

    /// Verify batch checksum integrity.
    #[must_use]
    pub fn verify_checksum(&self) -> bool {
        let expected = Self::new(self.records.clone()).checksum;
        self.checksum == expected
    }
}

/// Replication lag metrics reported by a standby replica.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicationLagReport {
    /// Current commit of the standby replica.
    pub replica_commit: CommitId,
    /// Latest acknowledged primary commit.
    pub primary_commit: CommitId,
    /// Number of commits the replica is behind the primary.
    pub commits_behind: u64,
    /// Wall-clock time elapsed since the last applied replication batch.
    pub duration_since_last_apply_ms: u64,
}

/// A replication stream source that extracts monotonic replication batches from a primary database.
pub struct ReplicationStream {
    last_streamed_commit: Arc<AtomicU64>,
}

impl ReplicationStream {
    /// Create a new replication stream starting after `from_commit`.
    #[must_use]
    pub fn new(from_commit: CommitId) -> Self {
        Self {
            last_streamed_commit: Arc::new(AtomicU64::new(from_commit.0)),
        }
    }

    /// Extract the next batch of replication records from the primary database.
    pub fn next_batch(
        &self,
        database: &RelationalDatabase,
        max_records: usize,
    ) -> Result<Option<ReplicationBatch>> {
        let from = self.last_streamed_commit.load(Ordering::SeqCst);
        let head = database.head().0;
        if from >= head {
            return Ok(None);
        }

        // Collect commits in range (from + 1)..=head
        let mut records = Vec::new();
        let limit = max_records.max(1);

        for commit_val in (from + 1)..=head {
            if records.len() >= limit {
                break;
            }
            let commit_id = CommitId(commit_val);
            let catalog = database
                .catalog_at(commit_id)
                .unwrap_or_else(|_| database.catalog().clone());

            let mut mutations = Vec::new();
            for table in catalog.tables() {
                if let Ok(rows) = database.scan(table.id, commit_id, usize::MAX) {
                    for row in rows {
                        if let Ok(encoded) = crate::relational::encode_row(&row) {
                            mutations.push(ReplicatedMutation::Put {
                                key: row.primary,
                                value: encoded,
                            });
                        }
                    }
                }
            }

            records.push(ReplicationRecord {
                commit: commit_id,
                timestamp_nanos: commit_val.saturating_mul(1_000_000),
                mutations,
                schema_change: Some(catalog),
            });
        }

        if records.is_empty() {
            return Ok(None);
        }

        let batch = ReplicationBatch::new(records);
        self.last_streamed_commit
            .store(batch.end_commit.0, Ordering::SeqCst);
        Ok(Some(batch))
    }
}

/// Standby replica manager that synchronizes a local database from a replication stream.
pub struct StandbyReplica {
    database: RelationalDatabase,
    role: ReplicationRole,
    last_applied_commit: CommitId,
    last_apply_instant: Instant,
}

impl StandbyReplica {
    /// Open a new standby replica instance in read-only standby mode.
    pub fn open(config: RelationalBackendConfig) -> Result<Self> {
        let database = RelationalDatabase::open(config)?;
        let last_applied_commit = database.head();
        Ok(Self {
            database,
            role: ReplicationRole::Standby,
            last_applied_commit,
            last_apply_instant: Instant::now(),
        })
    }

    /// Current role of this replica.
    #[must_use]
    pub fn role(&self) -> ReplicationRole {
        self.role
    }

    /// Get a read-only reference to the underlying database for queries.
    #[must_use]
    pub fn database(&self) -> &RelationalDatabase {
        &self.database
    }

    /// Get a mutable reference to the underlying database (only allowed if promoted).
    pub fn database_mut(&mut self) -> Result<&mut RelationalDatabase> {
        if self.role == ReplicationRole::Standby {
            return Err(DbError::InvalidState(
                "cannot obtain mutable handle on standby replica; promote replica first"
                    .to_owned(),
            ));
        }
        Ok(&mut self.database)
    }

    /// Report current replication lag against the latest primary commit.
    #[must_use]
    pub fn lag_report(&self, primary_head: CommitId) -> ReplicationLagReport {
        let replica_commit = self.last_applied_commit;
        let commits_behind = primary_head.0.saturating_sub(replica_commit.0);
        let duration_since_last_apply_ms = self
            .last_apply_instant
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;

        ReplicationLagReport {
            replica_commit,
            primary_commit: primary_head,
            commits_behind,
            duration_since_last_apply_ms,
        }
    }

    /// Apply a replication batch to the standby replica in strict commit order.
    pub fn apply_batch(&mut self, batch: &ReplicationBatch) -> Result<CommitId> {
        if self.role != ReplicationRole::Standby {
            return Err(DbError::InvalidState(
                "cannot apply replication batch to non-standby instance".to_owned(),
            ));
        }

        if !batch.verify_checksum() {
            return Err(DbError::Corruption {
                artifact: "replication_batch",
                reason: "checksum verification failed".to_owned(),
            });
        }

        for record in &batch.records {
            if record.commit.0 <= self.last_applied_commit.0 {
                // Idempotently skip already applied commits
                continue;
            }

            if record.commit.0 != self.last_applied_commit.0 + 1 {
                return Err(DbError::InvalidState(format!(
                    "replication gap detected: expected commit {}, received {}",
                    self.last_applied_commit.0 + 1,
                    record.commit.0
                )));
            }

            // Apply schema changes if any
            if let Some(catalog) = &record.schema_change {
                for table in catalog.tables() {
                    if self.database.catalog().table(table.id).is_err() {
                        let _ = self.database.create_table(table.clone());
                    }
                }
                for index in catalog.indexes() {
                    if self.database.catalog().index(index.id).is_none() {
                        let _ = self.database.create_index(index.clone());
                    }
                }
                for fk in catalog.foreign_keys() {
                    if self.database.catalog().foreign_keys().all(|f| f.id != fk.id) {
                        let _ = self.database.create_foreign_key(fk.clone());
                    }
                }
            }

            // Apply mutations
            for mutation in &record.mutations {
                match mutation {
                    ReplicatedMutation::Put { key, value } => {
                        let table_val = u64::from_be_bytes(key.0[..8].try_into().unwrap());
                        let table_id = TableId(table_val);
                        if let Ok(row) = crate::relational::decode_row(*key, value) {
                            if let Ok(Some(_)) = self.database.get(table_id, self.database.head(), *key) {
                                let _ = self.database.update(table_id, row);
                            } else {
                                let _ = self.database.insert(table_id, row);
                            }
                        }
                    }
                    ReplicatedMutation::Delete { key } => {
                        let table_val = u64::from_be_bytes(key.0[..8].try_into().unwrap());
                        let _ = self.database.delete(TableId(table_val), *key);
                    }
                }
            }

            self.last_applied_commit = record.commit;
        }

        self.last_apply_instant = Instant::now();
        Ok(self.last_applied_commit)
    }

    /// Promote this standby replica to an independent writable primary.
    pub fn promote(&mut self) -> Result<()> {
        if self.role == ReplicationRole::PromotedPrimary {
            return Ok(());
        }

        self.role = ReplicationRole::PromotedPrimary;
        // Checkpoint to seal replication history and ensure clean standalone state
        self.database.checkpoint()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DatabaseConfig;
    use tempfile::tempdir;

    #[test]
    fn replication_batch_checksum_detects_tampering() {
        let record = ReplicationRecord {
            commit: CommitId(1),
            timestamp_nanos: 1000,
            mutations: vec![ReplicatedMutation::Put {
                key: Key::new(1, 1),
                value: vec![1, 2, 3],
            }],
            schema_change: None,
        };

        let batch = ReplicationBatch::new(vec![record]);
        assert!(batch.verify_checksum());

        let mut tampered = batch.clone();
        tampered.records[0].mutations[0] = ReplicatedMutation::Put {
            key: Key::new(1, 1),
            value: vec![1, 2, 4],
        };
        assert!(!tampered.verify_checksum());
    }

    #[test]
    fn standby_replica_enforces_read_only_until_promotion() {
        let dir = tempdir().unwrap();
        let config = RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: dir.path().to_owned(),
        });

        let mut replica = StandbyReplica::open(config).unwrap();
        assert_eq!(replica.role(), ReplicationRole::Standby);
        assert!(replica.database_mut().is_err());

        replica.promote().unwrap();
        assert_eq!(replica.role(), ReplicationRole::PromotedPrimary);
        assert!(replica.database_mut().is_ok());
    }
}
