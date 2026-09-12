//! Transaction-log recovery validation for storage-kernel vNext.
//!
//! Recovery consumes already-framed records in durable LSN order. Mutations may
//! be interleaved across transactions, but each transaction's mutation ordinals
//! must be contiguous and only a validated commit decision emits replayable
//! state. Aborted or unterminated transactions never become visible.

use super::{CommitPosition, CommitSeq, LogRecord, LoggedMutation, Lsn, TxnId, mutation_digest};
use std::collections::{HashMap, HashSet};

/// One transaction proven committed by a valid durable decision.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecoveredTransaction {
    txn_id: TxnId,
    position: CommitPosition,
    mutations: Vec<LoggedMutation>,
}

impl RecoveredTransaction {
    #[must_use]
    pub const fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    #[must_use]
    pub const fn position(&self) -> CommitPosition {
        self.position
    }

    #[must_use]
    pub fn mutations(&self) -> &[LoggedMutation] {
        &self.mutations
    }

    #[must_use]
    pub fn into_mutations(self) -> Vec<LoggedMutation> {
        self.mutations
    }
}

/// Fail-closed recovery validation error.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum RecoveryError {
    #[error("log record LSN {actual:?} does not advance beyond {previous:?}")]
    NonMonotonicLsn { previous: Lsn, actual: Lsn },
    #[error("transaction {txn:?} received mutation ordinal {actual}, expected {expected}")]
    MutationOrdinal {
        txn: TxnId,
        expected: u32,
        actual: u32,
    },
    #[error("transaction {0:?} received a record after a terminal decision")]
    RecordAfterTerminal(TxnId),
    #[error("transaction {txn:?} commit names {declared} mutations but recovery has {actual}")]
    MutationCount {
        txn: TxnId,
        declared: u32,
        actual: u32,
    },
    #[error("transaction {txn:?} mutation digest does not match its commit decision")]
    MutationDigest { txn: TxnId },
    #[error("commit sequence {actual:?} does not advance beyond {previous:?}")]
    NonMonotonicCommitSeq {
        previous: CommitSeq,
        actual: CommitSeq,
    },
    #[error("recovery could not encode the validated mutation set")]
    MutationEncoding,
}

/// Stateful validator for one sequential vNext log replay.
#[derive(Default)]
pub struct RecoveryAssembler {
    pending: HashMap<TxnId, Vec<LoggedMutation>>,
    terminal: HashSet<TxnId>,
    last_lsn: Option<Lsn>,
    last_csn: Option<CommitSeq>,
}

impl RecoveryAssembler {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Consume one complete record at the durable end-LSN of that record.
    ///
    /// Returns a transaction only when a commit decision validates every staged
    /// mutation. A caller may safely ignore any pending entries left at EOF.
    pub fn push(
        &mut self,
        end_lsn: Lsn,
        record: LogRecord,
    ) -> Result<Option<RecoveredTransaction>, RecoveryError> {
        self.advance_lsn(end_lsn)?;
        match record {
            LogRecord::Mutation(mutation) => {
                let txn = mutation.txn_id();
                if self.terminal.contains(&txn) {
                    return Err(RecoveryError::RecordAfterTerminal(txn));
                }
                let pending = self.pending.entry(txn).or_default();
                let expected = u32::try_from(pending.len()).unwrap_or(u32::MAX);
                if mutation.ordinal() != expected {
                    return Err(RecoveryError::MutationOrdinal {
                        txn,
                        expected,
                        actual: mutation.ordinal(),
                    });
                }
                pending.push(mutation);
                Ok(None)
            }
            LogRecord::Abort(txn) => {
                if !self.terminal.insert(txn) {
                    return Err(RecoveryError::RecordAfterTerminal(txn));
                }
                self.pending.remove(&txn);
                Ok(None)
            }
            LogRecord::Commit(decision) => {
                let txn = decision.txn_id();
                if !self.terminal.insert(txn) {
                    return Err(RecoveryError::RecordAfterTerminal(txn));
                }
                if let Some(previous) = self.last_csn {
                    if decision.csn() <= previous {
                        return Err(RecoveryError::NonMonotonicCommitSeq {
                            previous,
                            actual: decision.csn(),
                        });
                    }
                }

                let mutations = self.pending.remove(&txn).unwrap_or_default();
                let actual = u32::try_from(mutations.len()).unwrap_or(u32::MAX);
                if decision.mutation_count() != actual {
                    return Err(RecoveryError::MutationCount {
                        txn,
                        declared: decision.mutation_count(),
                        actual,
                    });
                }
                let digest =
                    mutation_digest(&mutations).map_err(|_| RecoveryError::MutationEncoding)?;
                if decision.mutation_digest() != digest {
                    return Err(RecoveryError::MutationDigest { txn });
                }

                self.last_csn = Some(decision.csn());
                Ok(Some(RecoveredTransaction {
                    txn_id: txn,
                    position: CommitPosition::new(decision.csn(), end_lsn),
                    mutations,
                }))
            }
        }
    }

    /// Number of transactions with mutations but no terminal decision yet.
    #[must_use]
    pub fn pending_transactions(&self) -> usize {
        self.pending.len()
    }

    fn advance_lsn(&mut self, actual: Lsn) -> Result<(), RecoveryError> {
        if let Some(previous) = self.last_lsn {
            if actual <= previous {
                return Err(RecoveryError::NonMonotonicLsn { previous, actual });
            }
        }
        self.last_lsn = Some(actual);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{CommitDecision, StorageObjectId};

    fn lsn(offset: u64) -> Lsn {
        Lsn::from_wal_position(0, offset).expect("test LSN fits")
    }

    fn put(txn: u64, ordinal: u32, object: u64, key: &[u8]) -> LoggedMutation {
        LoggedMutation::ordered_put(
            TxnId::new(txn),
            ordinal,
            StorageObjectId::new(object),
            key.to_vec(),
            key.to_vec(),
        )
    }

    #[test]
    fn interleaved_transactions_emit_only_after_valid_commit() {
        let first = vec![put(1, 0, 11, b"a"), put(1, 1, 13, b"b")];
        let second = vec![put(2, 0, 17, b"x")];
        let mut recovery = RecoveryAssembler::new();

        assert!(
            recovery
                .push(lsn(10), LogRecord::Mutation(first[0].clone()))
                .expect("first mutation")
                .is_none()
        );
        assert!(
            recovery
                .push(lsn(20), LogRecord::Mutation(second[0].clone()))
                .expect("second txn mutation")
                .is_none()
        );
        assert!(
            recovery
                .push(lsn(30), LogRecord::Mutation(first[1].clone()))
                .expect("second first-txn mutation")
                .is_none()
        );

        let first_commit = CommitDecision::new(
            TxnId::new(1),
            CommitSeq::new(5),
            2,
            mutation_digest(&first).expect("digest"),
        );
        let committed = recovery
            .push(lsn(40), LogRecord::Commit(first_commit))
            .expect("commit validates")
            .expect("transaction emits");
        assert_eq!(committed.txn_id(), TxnId::new(1));
        assert_eq!(
            committed.position(),
            CommitPosition::new(CommitSeq::new(5), lsn(40))
        );
        assert_eq!(committed.mutations(), first.as_slice());
        assert_eq!(recovery.pending_transactions(), 1);
    }

    #[test]
    fn aborted_and_torn_transactions_never_emit() {
        let mut recovery = RecoveryAssembler::new();
        recovery
            .push(lsn(10), LogRecord::Mutation(put(3, 0, 19, b"lost")))
            .expect("mutation stages");
        assert!(
            recovery
                .push(lsn(20), LogRecord::Abort(TxnId::new(3)))
                .expect("abort validates")
                .is_none()
        );
        recovery
            .push(lsn(30), LogRecord::Mutation(put(4, 0, 23, b"torn")))
            .expect("unterminated mutation stages");
        assert_eq!(recovery.pending_transactions(), 1);
    }

    #[test]
    fn ordinal_count_digest_and_terminal_rules_fail_closed() {
        let mut ordinal = RecoveryAssembler::new();
        assert!(matches!(
            ordinal.push(lsn(10), LogRecord::Mutation(put(7, 1, 29, b"gap"))),
            Err(RecoveryError::MutationOrdinal {
                expected: 0,
                actual: 1,
                ..
            })
        ));

        let mutation = put(8, 0, 31, b"count");
        let mut count = RecoveryAssembler::new();
        count
            .push(lsn(10), LogRecord::Mutation(mutation.clone()))
            .expect("mutation stages");
        assert!(matches!(
            count.push(
                lsn(20),
                LogRecord::Commit(CommitDecision::new(
                    TxnId::new(8),
                    CommitSeq::new(1),
                    2,
                    mutation_digest(&[mutation.clone()]).expect("digest")
                ))
            ),
            Err(RecoveryError::MutationCount { .. })
        ));

        let mut digest = RecoveryAssembler::new();
        digest
            .push(lsn(10), LogRecord::Mutation(mutation))
            .expect("mutation stages");
        assert!(matches!(
            digest.push(
                lsn(20),
                LogRecord::Commit(CommitDecision::new(
                    TxnId::new(8),
                    CommitSeq::new(1),
                    1,
                    0xdead_beef
                ))
            ),
            Err(RecoveryError::MutationDigest { .. })
        ));

        let mut terminal = RecoveryAssembler::new();
        terminal
            .push(lsn(10), LogRecord::Abort(TxnId::new(9)))
            .expect("abort terminal");
        assert!(matches!(
            terminal.push(lsn(20), LogRecord::Mutation(put(9, 0, 37, b"late"))),
            Err(RecoveryError::RecordAfterTerminal(txn)) if txn == TxnId::new(9)
        ));
    }

    #[test]
    fn record_lsn_and_commit_sequence_must_advance() {
        let mut lsn_order = RecoveryAssembler::new();
        lsn_order
            .push(lsn(10), LogRecord::Abort(TxnId::new(1)))
            .expect("first record");
        assert!(matches!(
            lsn_order.push(lsn(10), LogRecord::Abort(TxnId::new(2))),
            Err(RecoveryError::NonMonotonicLsn { .. })
        ));

        let mut csn_order = RecoveryAssembler::new();
        let empty_digest = mutation_digest(&[]).expect("empty digest");
        csn_order
            .push(
                lsn(10),
                LogRecord::Commit(CommitDecision::new(
                    TxnId::new(11),
                    CommitSeq::new(7),
                    0,
                    empty_digest,
                )),
            )
            .expect("first commit");
        assert!(matches!(
            csn_order.push(
                lsn(20),
                LogRecord::Commit(CommitDecision::new(
                    TxnId::new(12),
                    CommitSeq::new(7),
                    0,
                    empty_digest,
                ))
            ),
            Err(RecoveryError::NonMonotonicCommitSeq { .. })
        ));
    }
}
