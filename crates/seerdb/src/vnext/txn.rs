//! Minimal transaction state machine for storage-kernel vNext.
//!
//! This module owns logical transaction phase, snapshot identity, and staged
//! authoritative mutations. Validation/conflict tracking, WAL append/sync, and
//! visibility publication remain separate components so the transaction object
//! does not recreate the old monolithic runtime/publication lock.

use super::{
    CommitDecision, CommitPosition, CommitSeq, LogEncodeError, LoggedMutation, Lsn,
    StorageObjectDescriptor, TxnId, mutation_digest,
};

/// Logical phase of one vNext transaction.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TransactionPhase {
    Active,
    Validating,
    Prepared,
    WalAppended,
    DurableDecision,
    Visible,
    Aborted,
    RecoveryRequired,
    Released,
}

/// Rejected transaction-state operation.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("transaction operation requires phase {expected:?}, found {actual:?}")]
    WrongPhase {
        expected: TransactionPhase,
        actual: TransactionPhase,
    },
    #[error("derived storage object {0:?} cannot define synchronous commit durability")]
    DerivedMutation(super::StorageObjectId),
    #[error("transaction contains more mutations than the vNext log ordinal can represent")]
    TooManyMutations,
    #[error("durable LSN {durable:?} does not match appended decision LSN {appended:?}")]
    WalLsnMismatch { appended: Lsn, durable: Lsn },
    #[error(transparent)]
    LogEncoding(#[from] LogEncodeError),
}

/// One staged transaction before the validation/log/visibility services land.
pub struct Transaction {
    id: TxnId,
    snapshot: CommitSeq,
    phase: TransactionPhase,
    mutations: Vec<LoggedMutation>,
    commit_seq: Option<CommitSeq>,
    decision_lsn: Option<Lsn>,
    position: Option<CommitPosition>,
}

impl Transaction {
    /// Start an active transaction at a fixed logical snapshot frontier.
    #[must_use]
    pub fn new(id: TxnId, snapshot: CommitSeq) -> Self {
        Self {
            id,
            snapshot,
            phase: TransactionPhase::Active,
            mutations: Vec::new(),
            commit_seq: None,
            decision_lsn: None,
            position: None,
        }
    }

    #[must_use]
    pub const fn id(&self) -> TxnId {
        self.id
    }

    #[must_use]
    pub const fn snapshot(&self) -> CommitSeq {
        self.snapshot
    }

    #[must_use]
    pub const fn phase(&self) -> TransactionPhase {
        self.phase
    }

    #[must_use]
    pub fn mutations(&self) -> &[LoggedMutation] {
        &self.mutations
    }

    #[must_use]
    pub const fn decision_lsn(&self) -> Option<Lsn> {
        self.decision_lsn
    }

    #[must_use]
    pub const fn commit_position(&self) -> Option<CommitPosition> {
        self.position
    }

    /// Stage one authoritative ordered put while the transaction is active.
    pub fn stage_ordered_put(
        &mut self,
        object: StorageObjectDescriptor,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TransactionError> {
        self.require_phase(TransactionPhase::Active)?;
        self.require_authoritative(object)?;
        let ordinal = self.next_ordinal()?;
        self.mutations.push(LoggedMutation::ordered_put(
            self.id,
            ordinal,
            object.id(),
            key,
            value,
        ));
        Ok(())
    }

    /// Stage one authoritative ordered delete while the transaction is active.
    pub fn stage_ordered_delete(
        &mut self,
        object: StorageObjectDescriptor,
        key: Vec<u8>,
    ) -> Result<(), TransactionError> {
        self.require_phase(TransactionPhase::Active)?;
        self.require_authoritative(object)?;
        let ordinal = self.next_ordinal()?;
        self.mutations.push(LoggedMutation::ordered_delete(
            self.id,
            ordinal,
            object.id(),
            key,
        ));
        Ok(())
    }

    /// Freeze the write set before conflict validation.
    pub fn begin_validation(&mut self) -> Result<(), TransactionError> {
        self.transition(TransactionPhase::Active, TransactionPhase::Validating)
    }

    /// Record successful validation and assign the transaction's visibility CSN.
    /// No WAL bytes have been written yet, so this state remains safely abortable.
    pub fn mark_prepared(&mut self, csn: CommitSeq) -> Result<(), TransactionError> {
        self.transition(TransactionPhase::Validating, TransactionPhase::Prepared)?;
        self.commit_seq = Some(csn);
        Ok(())
    }

    /// Build the durable commit decision for the frozen authoritative write set.
    pub fn commit_decision(&self) -> Result<CommitDecision, TransactionError> {
        self.require_phase(TransactionPhase::Prepared)?;
        let csn = self.commit_seq.ok_or(TransactionError::WrongPhase {
            expected: TransactionPhase::Prepared,
            actual: self.phase,
        })?;
        let mutation_count =
            u32::try_from(self.mutations.len()).map_err(|_| TransactionError::TooManyMutations)?;
        let digest = mutation_digest(&self.mutations)?;
        Ok(CommitDecision::new(self.id, csn, mutation_count, digest))
    }

    /// Record successful physical append of the commit decision. From this point
    /// an abort is unsafe because the appended decision may survive a crash even
    /// before an explicit durability barrier.
    pub fn mark_wal_appended(&mut self, lsn: Lsn) -> Result<(), TransactionError> {
        self.transition(TransactionPhase::Prepared, TransactionPhase::WalAppended)?;
        self.decision_lsn = Some(lsn);
        Ok(())
    }

    /// Mark the appended commit decision durable at its exact WAL end position.
    pub fn mark_durable(&mut self, lsn: Lsn) -> Result<CommitPosition, TransactionError> {
        self.require_phase(TransactionPhase::WalAppended)?;
        let appended = self.decision_lsn.ok_or(TransactionError::WrongPhase {
            expected: TransactionPhase::WalAppended,
            actual: self.phase,
        })?;
        if lsn != appended {
            return Err(TransactionError::WalLsnMismatch {
                appended,
                durable: lsn,
            });
        }
        let csn = self.commit_seq.ok_or(TransactionError::WrongPhase {
            expected: TransactionPhase::WalAppended,
            actual: self.phase,
        })?;
        let position = CommitPosition::new(csn, lsn);
        self.position = Some(position);
        self.phase = TransactionPhase::DurableDecision;
        Ok(position)
    }

    /// Mark an append/sync/publication outcome ambiguous. This state cannot be
    /// aborted or released in-process; reopen/recovery must resolve it.
    pub fn mark_recovery_required(&mut self) -> Result<(), TransactionError> {
        match self.phase {
            TransactionPhase::Prepared
            | TransactionPhase::WalAppended
            | TransactionPhase::DurableDecision => {
                self.phase = TransactionPhase::RecoveryRequired;
                Ok(())
            }
            actual => Err(TransactionError::WrongPhase {
                expected: TransactionPhase::Prepared,
                actual,
            }),
        }
    }

    /// Publish a durable decision to readers.
    pub fn mark_visible(&mut self) -> Result<(), TransactionError> {
        self.transition(TransactionPhase::DurableDecision, TransactionPhase::Visible)
    }

    /// Abort only while no commit decision may have reached the physical WAL.
    pub fn abort(&mut self) -> Result<(), TransactionError> {
        match self.phase {
            TransactionPhase::Active
            | TransactionPhase::Validating
            | TransactionPhase::Prepared => {
                self.phase = TransactionPhase::Aborted;
                self.commit_seq = None;
                self.decision_lsn = None;
                Ok(())
            }
            actual => Err(TransactionError::WrongPhase {
                expected: TransactionPhase::Active,
                actual,
            }),
        }
    }

    /// Release terminal visibility/abort bookkeeping.
    pub fn release(&mut self) -> Result<(), TransactionError> {
        match self.phase {
            TransactionPhase::Visible | TransactionPhase::Aborted => {
                self.phase = TransactionPhase::Released;
                Ok(())
            }
            actual => Err(TransactionError::WrongPhase {
                expected: TransactionPhase::Visible,
                actual,
            }),
        }
    }

    fn next_ordinal(&self) -> Result<u32, TransactionError> {
        u32::try_from(self.mutations.len()).map_err(|_| TransactionError::TooManyMutations)
    }

    fn require_authoritative(
        &self,
        object: StorageObjectDescriptor,
    ) -> Result<(), TransactionError> {
        if object.authority().requires_commit_recovery() {
            Ok(())
        } else {
            Err(TransactionError::DerivedMutation(object.id()))
        }
    }

    fn require_phase(&self, expected: TransactionPhase) -> Result<(), TransactionError> {
        if self.phase == expected {
            Ok(())
        } else {
            Err(TransactionError::WrongPhase {
                expected,
                actual: self.phase,
            })
        }
    }

    fn transition(
        &mut self,
        expected: TransactionPhase,
        next: TransactionPhase,
    ) -> Result<(), TransactionError> {
        self.require_phase(expected)?;
        self.phase = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{ObjectAuthority, StorageObjectId};

    fn object(id: u64, authority: ObjectAuthority) -> StorageObjectDescriptor {
        StorageObjectDescriptor::new(StorageObjectId::new(id), authority)
    }

    #[test]
    fn staged_mutations_are_ordinal_and_object_scoped() {
        let mut txn = Transaction::new(TxnId::new(7), CommitSeq::new(3));
        txn.stage_ordered_put(
            object(11, ObjectAuthority::Authoritative),
            b"alpha".to_vec(),
            b"one".to_vec(),
        )
        .expect("put stages");
        txn.stage_ordered_delete(object(13, ObjectAuthority::Authoritative), b"beta".to_vec())
            .expect("delete stages");

        assert_eq!(txn.mutations().len(), 2);
        assert_eq!(txn.mutations()[0].ordinal(), 0);
        assert_eq!(txn.mutations()[0].object(), StorageObjectId::new(11));
        assert_eq!(txn.mutations()[1].ordinal(), 1);
        assert_eq!(txn.mutations()[1].object(), StorageObjectId::new(13));
    }

    #[test]
    fn derived_mutation_is_rejected_from_durable_write_set() {
        let mut txn = Transaction::new(TxnId::new(9), CommitSeq::new(4));
        assert!(matches!(
            txn.stage_ordered_put(
                object(17, ObjectAuthority::Derived),
                b"key".to_vec(),
                b"value".to_vec()
            ),
            Err(TransactionError::DerivedMutation(id)) if id == StorageObjectId::new(17)
        ));
        assert!(txn.mutations().is_empty());
    }

    #[test]
    fn commit_state_machine_separates_append_durability_and_visibility() {
        let mut txn = Transaction::new(TxnId::new(19), CommitSeq::new(12));
        txn.stage_ordered_put(
            object(23, ObjectAuthority::Authoritative),
            b"key".to_vec(),
            b"value".to_vec(),
        )
        .expect("mutation stages");
        txn.begin_validation().expect("validation begins");
        txn.mark_prepared(CommitSeq::new(13))
            .expect("prepare succeeds");

        let decision = txn.commit_decision().expect("decision builds");
        assert_eq!(decision.txn_id(), TxnId::new(19));
        assert_eq!(decision.csn(), CommitSeq::new(13));
        assert_eq!(decision.mutation_count(), 1);
        assert_eq!(
            decision.mutation_digest(),
            mutation_digest(txn.mutations()).expect("digest computes")
        );

        let lsn = Lsn::from_wal_position(2, 4096).expect("lsn packs");
        txn.mark_wal_appended(lsn).expect("append records");
        assert_eq!(txn.phase(), TransactionPhase::WalAppended);
        assert_eq!(txn.decision_lsn(), Some(lsn));
        let position = txn.mark_durable(lsn).expect("decision becomes durable");
        assert_eq!(position.csn, CommitSeq::new(13));
        assert_eq!(position.lsn, lsn);
        txn.mark_visible().expect("commit becomes visible");
        txn.release().expect("terminal state releases");
        assert_eq!(txn.phase(), TransactionPhase::Released);
    }

    #[test]
    fn appended_or_uncertain_transaction_cannot_abort() {
        let mut txn = Transaction::new(TxnId::new(37), CommitSeq::new(20));
        txn.begin_validation().expect("validation begins");
        txn.mark_prepared(CommitSeq::new(21)).expect("prepared");
        let lsn = Lsn::from_wal_position(0, 100).expect("lsn");
        txn.mark_wal_appended(lsn).expect("append records");
        assert!(txn.abort().is_err());
        txn.mark_recovery_required()
            .expect("outcome becomes uncertain");
        assert_eq!(txn.phase(), TransactionPhase::RecoveryRequired);
        assert!(txn.abort().is_err());
        assert!(txn.release().is_err());
    }

    #[test]
    fn durable_barrier_must_name_the_exact_appended_decision_lsn() {
        let mut txn = Transaction::new(TxnId::new(41), CommitSeq::new(30));
        txn.begin_validation().expect("validation begins");
        txn.mark_prepared(CommitSeq::new(31)).expect("prepared");
        let appended = Lsn::from_wal_position(1, 128).expect("appended lsn");
        let wrong = Lsn::from_wal_position(1, 129).expect("wrong lsn");
        txn.mark_wal_appended(appended).expect("append records");
        assert!(matches!(
            txn.mark_durable(wrong),
            Err(TransactionError::WalLsnMismatch { .. })
        ));
        assert_eq!(txn.phase(), TransactionPhase::WalAppended);
        txn.mark_durable(appended)
            .expect("correct barrier succeeds");
    }

    #[test]
    fn writes_freeze_when_validation_begins_and_clean_preappend_state_can_abort() {
        let mut txn = Transaction::new(TxnId::new(29), CommitSeq::new(20));
        txn.begin_validation().expect("validation begins");
        assert!(matches!(
            txn.stage_ordered_delete(object(31, ObjectAuthority::Authoritative), b"late".to_vec()),
            Err(TransactionError::WrongPhase {
                expected: TransactionPhase::Active,
                actual: TransactionPhase::Validating,
            })
        ));
        txn.mark_prepared(CommitSeq::new(21)).expect("prepared");
        txn.abort().expect("preappend prepare may abort");
        assert_eq!(txn.phase(), TransactionPhase::Aborted);
        txn.release().expect("abort releases");
        assert_eq!(txn.phase(), TransactionPhase::Released);
    }
}
