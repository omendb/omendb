//! Minimal ordered append lane for vNext commit decisions.
//!
//! The lane couples commit-sequence assignment to physical WAL append order.
//! It does not perform durability barriers, install MVCC records, publish status,
//! or hold any database-wide execution lock. This is the smallest baseline that
//! makes recovery's strictly increasing CSN contract true under concurrent
//! committers. A future reservation-based WAL may replace the mutex if it can
//! preserve the same order invariant measurably better.

use super::{
    AppendTicket, CommitSeq, DurableLog, DurableLogError, FinalWriteSetError, LogEncodeError,
    PreparedLogBatch, Transaction, TransactionError, TransactionPhase,
};
use std::sync::{Arc, Mutex};

struct AppendLaneState {
    next_csn: Option<u64>,
}

/// Assigns CSNs in the same order transactions enter the physical WAL.
pub struct CommitAppender {
    log: Arc<DurableLog>,
    lane: Mutex<AppendLaneState>,
}

impl CommitAppender {
    /// Resume after the highest committed CSN recovered from the WAL.
    #[must_use]
    pub fn new(log: Arc<DurableLog>, recovered_csn: CommitSeq) -> Self {
        Self {
            log,
            lane: Mutex::new(AppendLaneState {
                next_csn: recovered_csn.get().checked_add(1),
            }),
        }
    }

    /// Assign a CSN and append one validated transaction's complete logical
    /// commit batch. The transaction must already be in `Validating` phase.
    ///
    /// The original mutation stream is validated through the shared canonical
    /// final-effect reducer before entering the append lane. Encoding also
    /// happens before changing transaction phase or touching the WAL. These
    /// failures remain cleanly abortable and consume neither CSN nor WAL bytes.
    /// A clean pre-existing fence aborts this transaction. An I/O error from the
    /// current append makes its outcome uncertain and moves it to
    /// `RecoveryRequired`.
    pub fn append_validated(
        &self,
        transaction: &mut Transaction,
    ) -> Result<AppendTicket, CommitAppendError> {
        if transaction.phase() != TransactionPhase::Validating {
            return Err(CommitAppendError::WrongPhase(transaction.phase()));
        }

        // The integrated coordinator consumes this same normalized view for
        // intents and installation. The append primitive validates it here too
        // so standalone use cannot write a WAL stream recovery must reject.
        let _ = transaction.final_effects()?;

        let mut lane = self
            .lane
            .lock()
            .map_err(|_| CommitAppendError::AppendLanePoisoned)?;
        let raw_csn = lane.next_csn.ok_or(CommitAppendError::CommitSeqExhausted)?;
        let csn = CommitSeq::new(raw_csn);

        // This is deliberately before `mark_prepared`: oversized/encoding
        // failures remain cleanly abortable and consume neither CSN nor WAL.
        let batch = PreparedLogBatch::for_commit(transaction.id(), csn, transaction.mutations())?;
        transaction.mark_prepared(csn)?;

        let ticket = match self.log.append(&batch) {
            Ok(ticket) => ticket,
            Err(error @ DurableLogError::Io { .. }) => {
                let state_error = transaction.mark_recovery_required().err();
                return Err(CommitAppendError::OutcomeUncertain { error, state_error });
            }
            Err(error @ (DurableLogError::Fenced | DurableLogError::Poisoned)) => {
                // `DurableLog` did not enter this transaction's device append.
                // Its Prepared state is still cleanly abortable.
                transaction.abort()?;
                return Err(CommitAppendError::LogUnavailable(error));
            }
        };

        // A successful device append consumed this CSN regardless of any later
        // in-process bookkeeping failure. Advance first so the physical WAL can
        // never receive a duplicate CSN.
        lane.next_csn = raw_csn.checked_add(1);
        if let Err(source) = transaction.mark_wal_appended(ticket.decision_lsn()) {
            let state_error = transaction.mark_recovery_required().err();
            return Err(CommitAppendError::StateAfterAppend {
                source,
                state_error,
            });
        }
        Ok(ticket)
    }

    /// CSN that the next successful append would receive, or `None` when the
    /// commit-sequence domain is exhausted.
    pub fn next_csn(&self) -> Result<Option<CommitSeq>, CommitAppendError> {
        let lane = self
            .lane
            .lock()
            .map_err(|_| CommitAppendError::AppendLanePoisoned)?;
        Ok(lane.next_csn.map(CommitSeq::new))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CommitAppendError {
    #[error("commit append requires a validating transaction, found {0:?}")]
    WrongPhase(TransactionPhase),
    #[error("commit append lane is poisoned")]
    AppendLanePoisoned,
    #[error("commit sequence space is exhausted")]
    CommitSeqExhausted,
    #[error(transparent)]
    WriteSet(#[from] FinalWriteSetError),
    #[error(transparent)]
    Encoding(#[from] LogEncodeError),
    #[error(transparent)]
    Transaction(#[from] TransactionError),
    #[error("transaction log is unavailable before this transaction appended: {0}")]
    LogUnavailable(DurableLogError),
    #[error("transaction append outcome is uncertain: {error}")]
    OutcomeUncertain {
        error: DurableLogError,
        state_error: Option<TransactionError>,
    },
    #[error("WAL append succeeded but transaction bookkeeping failed: {source}")]
    StateAfterAppend {
        source: TransactionError,
        state_error: Option<TransactionError>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{
        LogDevice, LogParseStatus, ObjectAuthority, RecoveryAssembler, StorageObjectDescriptor,
        StorageObjectId, TxnId, parse_log_prefix_frames,
    };
    use std::io;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Default)]
    struct MemoryLogDevice {
        bytes: Mutex<Vec<u8>>,
        fail_append: AtomicBool,
    }

    impl MemoryLogDevice {
        fn bytes(&self) -> Vec<u8> {
            self.bytes.lock().expect("memory log lock").clone()
        }

        fn fail_next_append(&self) {
            self.fail_append.store(true, Ordering::Release);
        }
    }

    impl LogDevice for MemoryLogDevice {
        fn append(&self, bytes: &[u8]) -> io::Result<super::super::Lsn> {
            let mut log = self
                .bytes
                .lock()
                .map_err(|_| io::Error::other("memory log poisoned"))?;
            if self.fail_append.swap(false, Ordering::AcqRel) {
                let half = bytes.len() / 2;
                log.extend_from_slice(&bytes[..half]);
                return Err(io::Error::other("injected partial append"));
            }
            log.extend_from_slice(bytes);
            super::super::Lsn::from_wal_position(0, log.len() as u64)
                .ok_or_else(|| io::Error::other("memory log LSN overflow"))
        }

        fn sync_through(&self, _lsn: super::super::Lsn) -> io::Result<()> {
            Ok(())
        }
    }

    fn object() -> StorageObjectDescriptor {
        StorageObjectDescriptor::new(StorageObjectId::new(1), ObjectAuthority::Authoritative)
    }

    fn validating(txn: u64) -> Transaction {
        let mut transaction = Transaction::new(TxnId::new(txn), CommitSeq::new(0));
        let key = format!("key-{txn:04}").into_bytes();
        transaction
            .stage_ordered_put(object(), key.clone(), key)
            .expect("mutation stages");
        transaction.begin_validation().expect("validation begins");
        transaction
    }

    #[test]
    fn concurrent_committers_get_csn_order_identical_to_wal_order() {
        let device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(device.clone()));
        let appender = Arc::new(CommitAppender::new(log, CommitSeq::new(0)));
        let barrier = Arc::new(Barrier::new(17));
        let mut workers = Vec::new();

        for txn in 1..=16u64 {
            let appender = Arc::clone(&appender);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                let mut transaction = validating(txn);
                barrier.wait();
                let ticket = appender
                    .append_validated(&mut transaction)
                    .expect("append succeeds");
                assert_eq!(transaction.phase(), TransactionPhase::WalAppended);
                ticket
            }));
        }
        barrier.wait();
        let mut tickets: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker completes"))
            .collect();
        tickets.sort_by_key(|ticket| ticket.decision_lsn());
        for (index, ticket) in tickets.iter().enumerate() {
            assert_eq!(ticket.csn(), CommitSeq::new(index as u64 + 1));
        }

        let (frames, status) = parse_log_prefix_frames(&device.bytes());
        assert_eq!(status, LogParseStatus::Complete);
        let mut recovery = RecoveryAssembler::new();
        let mut recovered = Vec::new();
        for frame in frames {
            let lsn = frame.end_lsn(0, 0).expect("LSN resolves");
            if let Some(transaction) = recovery
                .push(lsn, frame.into_record())
                .expect("recovery validates")
            {
                recovered.push(transaction.position().csn);
            }
        }
        assert_eq!(recovered, (1..=16).map(CommitSeq::new).collect::<Vec<_>>());
    }

    #[test]
    fn partial_append_fences_log_and_marks_transaction_recovery_required() {
        let device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(device.clone()));
        let appender = CommitAppender::new(log.clone(), CommitSeq::new(0));
        let mut transaction = validating(1);
        device.fail_next_append();

        assert!(matches!(
            appender.append_validated(&mut transaction),
            Err(CommitAppendError::OutcomeUncertain { .. })
        ));
        assert_eq!(transaction.phase(), TransactionPhase::RecoveryRequired);
        assert!(log.is_fenced());
        assert_eq!(
            appender.next_csn().expect("lane reads"),
            Some(CommitSeq::new(1))
        );
    }

    #[test]
    fn reserved_transaction_is_rejected_before_csn_or_wal() {
        let device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(device.clone()));
        let appender = CommitAppender::new(log, CommitSeq::new(0));
        let mut transaction = validating(0);

        assert!(matches!(
            appender.append_validated(&mut transaction),
            Err(CommitAppendError::WriteSet(
                FinalWriteSetError::ReservedTransaction
            ))
        ));
        assert_eq!(transaction.phase(), TransactionPhase::Validating);
        assert!(device.bytes().is_empty());
        assert_eq!(
            appender.next_csn().expect("lane reads"),
            Some(CommitSeq::new(1))
        );
    }

    #[test]
    fn wrong_phase_and_exhaustion_touch_neither_transaction_nor_wal() {
        let device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(device.clone()));
        let appender = CommitAppender::new(log, CommitSeq::new(u64::MAX));
        let mut transaction = validating(1);
        assert!(matches!(
            appender.append_validated(&mut transaction),
            Err(CommitAppendError::CommitSeqExhausted)
        ));
        assert_eq!(transaction.phase(), TransactionPhase::Validating);
        assert!(device.bytes().is_empty());

        let log = Arc::new(DurableLog::new(Arc::new(MemoryLogDevice::default())));
        let appender = CommitAppender::new(log, CommitSeq::new(0));
        let mut active = Transaction::new(TxnId::new(2), CommitSeq::new(0));
        assert!(matches!(
            appender.append_validated(&mut active),
            Err(CommitAppendError::WrongPhase(TransactionPhase::Active))
        ));
        assert_eq!(active.phase(), TransactionPhase::Active);
    }
}
