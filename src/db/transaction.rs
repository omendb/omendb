//! Root-bound byte transaction API and lifecycle ownership.
//!
//! `DB` owns durable publication and the current mutable state. This module
//! owns the staged mutation list, root-bound read overlay, expected-base
//! commit contract, and cleanup state for one data-bearing transaction.

use super::retention_state::RetentionLease;
use super::{
    DB, DurabilityStatus, Error, Result, validate_wal_key_length, validate_wal_put_lengths,
};
use crate::storage::format::{CommitId, SnapshotId};
use std::collections::BTreeMap;

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
pub struct BatchTransaction {
    pub(super) base_commit: CommitId,
    pub(super) snapshot_id: SnapshotId,
    pub(super) lease: Option<RetentionLease>,
    pub(super) mutations: Vec<BatchMutation>,
    pub(super) state: BatchTransactionState,
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
