//! Scheduler-neutral durability seam for the vNext transaction log.
//!
//! Prepared transactions are encoded into contiguous batches whose final record
//! is their durable commit decision. Appending establishes a physical log order
//! and returns the decision end-LSN; `sync_through` establishes durability. The
//! two operations are deliberately separate so autonomous sync and group commit
//! share one contract.
//!
//! The baseline wrapper serializes physical append/sync operations so a failing
//! operation cannot race a later success across the fence. This is a WAL-device
//! coordination lock only, not a database/transaction lock. A measured future
//! implementation may replace it with explicit in-flight epochs/reservations.
//!
//! Any append or sync I/O failure is outcome-uncertain and fences this handle
//! until reopen. Callers may retry only failures that happen while building a
//! batch before the device is touched.

use super::{
    CommitDecision, CommitSeq, LogEncodeError, LogRecord, LoggedMutation, Lsn, Transaction, TxnId,
    mutation_digest,
};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Physical log operations that may make a commit outcome uncertain.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LogIoOperation {
    Append,
    Sync,
}

/// Device contract below commit scheduling.
///
/// `append` must place the supplied bytes contiguously in one total log order
/// and return the LSN immediately after the final byte. Returning success does
/// not imply durability. The baseline `DurableLog` serializes calls into this
/// device; a future wrapper may expose safe parallel reservations without
/// changing transaction records or recovery.
///
/// If either method returns an I/O error, the caller must assume the requested
/// operation may have partially or fully happened.
pub trait LogDevice: Send + Sync {
    fn append(&self, bytes: &[u8]) -> io::Result<Lsn>;
    fn sync_through(&self, lsn: Lsn) -> io::Result<()>;
}

/// Complete encoded transaction ready for one contiguous physical append.
#[derive(Debug, Clone)]
pub struct PreparedLogBatch {
    txn_id: TxnId,
    csn: CommitSeq,
    bytes: Vec<u8>,
}

impl PreparedLogBatch {
    /// Encode a transaction already in `Prepared` state.
    pub fn from_transaction(transaction: &Transaction) -> Result<Self, PrepareLogBatchError> {
        let decision = transaction.commit_decision()?;
        Ok(Self::from_decision(
            decision,
            transaction.mutations(),
        )?)
    }

    /// Encode a candidate commit before changing transaction phase or touching
    /// the WAL. The append-order sequencer uses this to keep encode failures
    /// cleanly retryable while assigning CSN and WAL order in one lane.
    pub(crate) fn for_commit(
        txn_id: TxnId,
        csn: CommitSeq,
        mutations: &[LoggedMutation],
    ) -> Result<Self, LogEncodeError> {
        let count = u32::try_from(mutations.len()).map_err(|_| LogEncodeError::RecordTooLarge)?;
        let digest = mutation_digest(mutations)?;
        Self::from_decision(
            CommitDecision::new(txn_id, csn, count, digest),
            mutations,
        )
    }

    fn from_decision(
        decision: CommitDecision,
        mutations: &[LoggedMutation],
    ) -> Result<Self, LogEncodeError> {
        let mut encoded = Vec::with_capacity(mutations.len() + 1);
        let mut total = 0usize;

        for mutation in mutations {
            let bytes = LogRecord::Mutation(mutation.clone()).to_bytes()?;
            total = total
                .checked_add(bytes.len())
                .ok_or(LogEncodeError::RecordTooLarge)?;
            encoded.push(bytes);
        }
        let decision_bytes = LogRecord::Commit(decision).to_bytes()?;
        total = total
            .checked_add(decision_bytes.len())
            .ok_or(LogEncodeError::RecordTooLarge)?;
        encoded.push(decision_bytes);

        let mut bytes = Vec::with_capacity(total);
        for record in encoded {
            bytes.extend_from_slice(&record);
        }
        Ok(Self {
            txn_id: decision.txn_id(),
            csn: decision.csn(),
            bytes,
        })
    }

    #[must_use]
    pub const fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    #[must_use]
    pub const fn csn(&self) -> CommitSeq {
        self.csn
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Pre-I/O failure while building a prepared transaction batch.
#[derive(Debug, thiserror::Error)]
pub enum PrepareLogBatchError {
    #[error(transparent)]
    Transaction(#[from] super::TransactionError),
    #[error(transparent)]
    Encoding(#[from] LogEncodeError),
}

/// Identity and physical position of an appended commit decision.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AppendTicket {
    txn_id: TxnId,
    csn: CommitSeq,
    decision_lsn: Lsn,
}

impl AppendTicket {
    #[must_use]
    pub const fn txn_id(self) -> TxnId {
        self.txn_id
    }

    #[must_use]
    pub const fn csn(self) -> CommitSeq {
        self.csn
    }

    #[must_use]
    pub const fn decision_lsn(self) -> Lsn {
        self.decision_lsn
    }
}

/// Failure from the durable-log wrapper.
#[derive(Debug, thiserror::Error)]
pub enum DurableLogError {
    #[error("vNext transaction log is fenced until recovery/reopen")]
    Fenced,
    #[error("vNext transaction log operation lock is poisoned")]
    Poisoned,
    #[error("transaction log {operation:?} failed and fenced the handle: {source}")]
    Io {
        operation: LogIoOperation,
        #[source]
        source: io::Error,
    },
}

/// Fencing wrapper around a scheduler/device-specific append implementation.
pub struct DurableLog {
    device: Arc<dyn LogDevice>,
    operations: Mutex<()>,
    fenced: AtomicBool,
    durable_lsn: AtomicU64,
}

impl DurableLog {
    #[must_use]
    pub fn new(device: Arc<dyn LogDevice>) -> Self {
        Self {
            device,
            operations: Mutex::new(()),
            fenced: AtomicBool::new(false),
            durable_lsn: AtomicU64::new(0),
        }
    }

    /// Append one already-prepared transaction without forcing durability.
    pub fn append(&self, batch: &PreparedLogBatch) -> Result<AppendTicket, DurableLogError> {
        let _operation = self.lock_operation()?;
        self.ensure_open()?;
        let decision_lsn = self.device.append(batch.as_bytes()).map_err(|source| {
            self.fenced.store(true, Ordering::Release);
            DurableLogError::Io {
                operation: LogIoOperation::Append,
                source,
            }
        })?;
        Ok(AppendTicket {
            txn_id: batch.txn_id(),
            csn: batch.csn(),
            decision_lsn,
        })
    }

    /// Make every append through the requested LSN durable.
    pub fn sync_through(&self, lsn: Lsn) -> Result<(), DurableLogError> {
        let _operation = self.lock_operation()?;
        self.ensure_open()?;
        self.device.sync_through(lsn).map_err(|source| {
            self.fenced.store(true, Ordering::Release);
            DurableLogError::Io {
                operation: LogIoOperation::Sync,
                source,
            }
        })?;
        self.durable_lsn.fetch_max(lsn.get(), Ordering::AcqRel);
        Ok(())
    }

    #[must_use]
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }

    /// Highest LSN this handle has successfully synchronized.
    #[must_use]
    pub fn durable_lsn(&self) -> Option<Lsn> {
        let raw = self.durable_lsn.load(Ordering::Acquire);
        (raw != 0).then_some(Lsn::new(raw))
    }

    fn lock_operation(&self) -> Result<std::sync::MutexGuard<'_, ()>, DurableLogError> {
        self.operations.lock().map_err(|_| {
            self.fenced.store(true, Ordering::Release);
            DurableLogError::Poisoned
        })
    }

    fn ensure_open(&self) -> Result<(), DurableLogError> {
        if self.is_fenced() {
            Err(DurableLogError::Fenced)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{
        CommitPosition, ObjectAuthority, RecoveryAssembler, StorageObjectDescriptor,
        StorageObjectId, TransactionPhase, parse_log_prefix_frames,
    };
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct MemoryState {
        bytes: Vec<u8>,
        durable_offset: u64,
    }

    #[derive(Default)]
    struct MemoryLogDevice {
        state: Mutex<MemoryState>,
        fail_append_after: AtomicUsize,
        fail_sync: AtomicBool,
    }

    impl MemoryLogDevice {
        fn bytes(&self) -> Vec<u8> {
            self.state.lock().expect("memory log lock").bytes.clone()
        }

        fn durable_offset(&self) -> u64 {
            self.state.lock().expect("memory log lock").durable_offset
        }

        fn fail_next_append_after(&self, bytes: usize) {
            self.fail_append_after.store(bytes + 1, Ordering::Release);
        }

        fn fail_next_sync(&self) {
            self.fail_sync.store(true, Ordering::Release);
        }
    }

    impl LogDevice for MemoryLogDevice {
        fn append(&self, bytes: &[u8]) -> io::Result<Lsn> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| io::Error::other("memory log poisoned"))?;
            let encoded_failure = self.fail_append_after.swap(0, Ordering::AcqRel);
            if encoded_failure != 0 {
                let count = (encoded_failure - 1).min(bytes.len());
                state.bytes.extend_from_slice(&bytes[..count]);
                return Err(io::Error::other("injected partial append"));
            }
            state.bytes.extend_from_slice(bytes);
            Lsn::from_wal_position(0, state.bytes.len() as u64)
                .ok_or_else(|| io::Error::other("memory log offset overflow"))
        }

        fn sync_through(&self, lsn: Lsn) -> io::Result<()> {
            if self.fail_sync.swap(false, Ordering::AcqRel) {
                return Err(io::Error::other("injected sync failure"));
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| io::Error::other("memory log poisoned"))?;
            if lsn.segment() != 0 || lsn.offset() > state.bytes.len() as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "sync frontier was never appended",
                ));
            }
            state.durable_offset = state.durable_offset.max(lsn.offset());
            Ok(())
        }
    }

    fn object(id: u64) -> StorageObjectDescriptor {
        StorageObjectDescriptor::new(StorageObjectId::new(id), ObjectAuthority::Authoritative)
    }

    fn prepared(txn_id: u64, snapshot: u64, csn: u64, key: &[u8]) -> Transaction {
        let mut txn = Transaction::new(TxnId::new(txn_id), CommitSeq::new(snapshot));
        txn.stage_ordered_put(object(11), key.to_vec(), key.to_vec())
            .expect("mutation stages");
        txn.begin_validation().expect("validation begins");
        txn.mark_prepared(CommitSeq::new(csn))
            .expect("transaction prepares");
        txn
    }

    #[test]
    fn append_and_sync_are_separate_and_support_group_durability() {
        let device = Arc::new(MemoryLogDevice::default());
        let log = DurableLog::new(device.clone());
        let mut first = prepared(1, 0, 1, b"alpha");
        let mut second = prepared(2, 0, 2, b"beta");
        let first_batch = PreparedLogBatch::from_transaction(&first).expect("first encodes");
        let second_batch = PreparedLogBatch::from_transaction(&second).expect("second encodes");

        let first_ticket = log.append(&first_batch).expect("first appends");
        first
            .mark_wal_appended(first_ticket.decision_lsn())
            .expect("first records append");
        let second_ticket = log.append(&second_batch).expect("second appends");
        second
            .mark_wal_appended(second_ticket.decision_lsn())
            .expect("second records append");
        assert!(first_ticket.decision_lsn() < second_ticket.decision_lsn());
        assert_eq!(device.durable_offset(), 0);
        assert_eq!(log.durable_lsn(), None);

        log.sync_through(second_ticket.decision_lsn())
            .expect("group sync succeeds");
        assert_eq!(log.durable_lsn(), Some(second_ticket.decision_lsn()));
        first
            .mark_durable(first_ticket.decision_lsn())
            .expect("first becomes durable");
        second
            .mark_durable(second_ticket.decision_lsn())
            .expect("second becomes durable");
        assert_eq!(first.phase(), TransactionPhase::DurableDecision);
        assert_eq!(second.phase(), TransactionPhase::DurableDecision);

        let bytes = device.bytes();
        let (frames, status) = parse_log_prefix_frames(&bytes);
        assert_eq!(status, super::super::LogParseStatus::Complete);
        let mut recovery = RecoveryAssembler::new();
        let mut commits = Vec::new();
        for frame in frames {
            let end_lsn = frame.end_lsn(0, 0).expect("frame LSN resolves");
            if let Some(committed) = recovery
                .push(end_lsn, frame.into_record())
                .expect("recovery validates")
            {
                commits.push(committed);
            }
        }
        assert_eq!(commits.len(), 2);
        assert_eq!(
            commits[0].position(),
            CommitPosition::new(CommitSeq::new(1), first_ticket.decision_lsn())
        );
        assert_eq!(
            commits[1].position(),
            CommitPosition::new(CommitSeq::new(2), second_ticket.decision_lsn())
        );
    }

    #[test]
    fn partial_append_fences_and_requires_recovery_resolution() {
        let device = Arc::new(MemoryLogDevice::default());
        let log = DurableLog::new(device.clone());
        let mut txn = prepared(3, 0, 3, b"partial");
        let batch = PreparedLogBatch::from_transaction(&txn).expect("batch encodes");
        device.fail_next_append_after(batch.as_bytes().len() / 2);

        assert!(matches!(
            log.append(&batch),
            Err(DurableLogError::Io {
                operation: LogIoOperation::Append,
                ..
            })
        ));
        txn.mark_recovery_required()
            .expect("uncertain append requires recovery");
        assert_eq!(txn.phase(), TransactionPhase::RecoveryRequired);
        assert!(txn.abort().is_err());
        assert!(log.is_fenced());
        assert!(matches!(log.append(&batch), Err(DurableLogError::Fenced)));

        let bytes = device.bytes();
        let (frames, status) = parse_log_prefix_frames(&bytes);
        assert!(matches!(
            status,
            super::super::LogParseStatus::Complete | super::super::LogParseStatus::Incomplete
        ));
        assert!(
            !frames
                .iter()
                .any(|frame| matches!(frame.record(), LogRecord::Commit(_)))
        );
    }

    #[test]
    fn sync_failure_fences_an_uncertain_commit() {
        let device = Arc::new(MemoryLogDevice::default());
        let log = DurableLog::new(device.clone());
        let mut txn = prepared(5, 0, 5, b"sync");
        let batch = PreparedLogBatch::from_transaction(&txn).expect("batch encodes");
        let ticket = log.append(&batch).expect("append succeeds");
        txn.mark_wal_appended(ticket.decision_lsn())
            .expect("append records");
        device.fail_next_sync();

        assert!(matches!(
            log.sync_through(ticket.decision_lsn()),
            Err(DurableLogError::Io {
                operation: LogIoOperation::Sync,
                ..
            })
        ));
        txn.mark_recovery_required()
            .expect("sync uncertainty requires recovery");
        assert_eq!(txn.phase(), TransactionPhase::RecoveryRequired);
        assert!(txn.abort().is_err());
        assert!(log.is_fenced());
        assert_eq!(log.durable_lsn(), None);
        assert!(matches!(
            log.sync_through(ticket.decision_lsn()),
            Err(DurableLogError::Fenced)
        ));
    }
}
