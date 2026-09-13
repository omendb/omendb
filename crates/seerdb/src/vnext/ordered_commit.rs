//! Durable-WAL-first commit orchestration for transient ordered MVCC objects.
//!
//! This is the first executable integration of ADR 0014's transaction order.
//! It deliberately does not make buffered pages persistent recovery authority:
//! callers must still use transient/non-authoritative page I/O until page
//! dependency capture and structurally complete checkpoints land.

use super::{
    BTreeError, BTreeObject, BufferPool, CommitAppendError, CommitAppender, CommitPosition,
    DurableLog, DurableLogError, FinalEffect, FinalWriteSetError, InstallContext,
    InstallEffectResult, MvccCodecError, MvccRecord, MvccValue, OrderedMvccInstallError,
    OrderedMvccInstaller, StatusTableError, StorageObjectId, Transaction, TransactionError,
    TransactionPhase, TransactionStatus, TransactionStatusTable, TxnId, UndoStore, UndoStoreError,
    VersionId, VisibilityError, VisibilityFrontier, WriteIntentError, WriteIntentTable,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

/// Synchronous write-admission and commit coordinator for ordered objects.
///
/// One coordinator is intended to represent one runtime's write-admission
/// fence. The lower WAL/undo components have their own I/O fences; this higher
/// fence prevents a post-decision failure from releasing logical key ownership
/// back to a still-running writer population.
pub struct OrderedCommitCoordinator<'a> {
    appender: &'a CommitAppender,
    log: &'a DurableLog,
    statuses: &'a TransactionStatusTable,
    frontier: &'a VisibilityFrontier,
    intents: &'a WriteIntentTable,
    undo: &'a UndoStore,
    fenced: AtomicBool,
}

impl<'a> OrderedCommitCoordinator<'a> {
    #[must_use]
    pub const fn new(
        appender: &'a CommitAppender,
        log: &'a DurableLog,
        statuses: &'a TransactionStatusTable,
        frontier: &'a VisibilityFrontier,
        intents: &'a WriteIntentTable,
        undo: &'a UndoStore,
    ) -> Self {
        Self {
            appender,
            log,
            statuses,
            frontier,
            intents,
            undo,
            fenced: AtomicBool::new(false),
        }
    }

    /// Begin one write transaction at the current contiguous visibility
    /// frontier and register its transaction owner before any current record can
    /// reference it.
    pub fn begin_write(&self, txn_id: TxnId) -> Result<Transaction, OrderedCommitError> {
        self.ensure_open()?;
        self.statuses.begin(txn_id)?;
        Ok(Transaction::new(txn_id, self.frontier.snapshot()))
    }

    /// Cleanly abort a transaction whose commit decision cannot have reached
    /// the WAL.
    pub fn abort(&self, transaction: &mut Transaction) -> Result<(), OrderedCommitError> {
        match transaction.phase() {
            TransactionPhase::Active
            | TransactionPhase::Validating
            | TransactionPhase::Prepared => {}
            actual => return Err(OrderedCommitError::WrongPhase(actual)),
        }
        self.statuses.abort(transaction.id())?;
        if let Err(error) = transaction.abort() {
            self.fenced.store(true, Ordering::Release);
            return Err(OrderedCommitError::Transaction(error));
        }
        Ok(())
    }

    /// Commit one transaction across one or more authoritative ordered objects.
    ///
    /// Deterministic object/record representability checks happen before the
    /// transaction enters `Validating`, so those failures leave it active and
    /// retryable. Once WAL append may have happened, every uncertain or
    /// post-decision failure fences this coordinator before the intent guard is
    /// dropped and moves the transaction to `RecoveryRequired` where possible.
    pub fn commit(
        &self,
        transaction: &mut Transaction,
        buffer: &BufferPool,
        trees: &[&BTreeObject],
    ) -> Result<CommitPosition, OrderedCommitError> {
        self.ensure_open()?;
        if transaction.phase() != TransactionPhase::Active {
            return Err(OrderedCommitError::WrongPhase(transaction.phase()));
        }

        let effects = transaction.final_effects()?;
        let intent_guard = self.intents.try_acquire(transaction.id(), &effects)?;
        let objects = self.preflight(transaction, buffer, trees, &effects)?;
        self.require_active_status(transaction.id())?;

        transaction.begin_validation()?;
        let ticket = match self.appender.append_validated(transaction) {
            Ok(ticket) => ticket,
            Err(error) => {
                if transaction.phase() == TransactionPhase::RecoveryRequired {
                    self.fenced.store(true, Ordering::Release);
                } else {
                    self.cleanup_clean_append_failure(transaction)?;
                }
                return Err(OrderedCommitError::Append(error));
            }
        };

        if let Err(error) = self.log.sync_through(ticket.decision_lsn()) {
            self.fence_after_wal(transaction);
            return Err(OrderedCommitError::Log(error));
        }
        let position = match transaction.mark_durable(ticket.decision_lsn()) {
            Ok(position) => position,
            Err(error) => {
                self.fence_after_wal(transaction);
                return Err(OrderedCommitError::Transaction(error));
            }
        };

        let installer = OrderedMvccInstaller::new(self.statuses, self.undo);
        let mut max_undo = None;
        for effect in &effects {
            let Some(tree) = objects.get(&effect.object()).copied() else {
                self.fence_after_wal(transaction);
                return Err(OrderedCommitError::MissingObject(effect.object()));
            };
            match installer.install(
                tree,
                buffer,
                &intent_guard,
                effect,
                InstallContext::Live {
                    snapshot: transaction.snapshot(),
                },
            ) {
                Ok(InstallEffectResult::Installed { appended_undo, .. }) => {
                    if let Some(version) = appended_undo {
                        max_undo = Some(max_version(max_undo, version));
                    }
                }
                Ok(InstallEffectResult::AlreadyInstalled { .. }) => {}
                Err(error) => {
                    self.fence_after_wal(transaction);
                    return Err(OrderedCommitError::Install(error));
                }
            }
        }

        if let Some(version) = max_undo {
            if let Err(error) = self.undo.sync_through(version) {
                self.fence_after_wal(transaction);
                return Err(OrderedCommitError::Undo(error));
            }
        }

        if let Err(error) =
            self.frontier
                .publish_commit(self.statuses, transaction.id(), ticket.csn())
        {
            self.fence_after_wal(transaction);
            return Err(OrderedCommitError::Visibility(error));
        }
        if let Err(error) = transaction.mark_visible() {
            self.fence_after_wal(transaction);
            return Err(OrderedCommitError::Transaction(error));
        }
        if let Err(error) = transaction.release() {
            self.fenced.store(true, Ordering::Release);
            return Err(OrderedCommitError::Transaction(error));
        }

        intent_guard.release();
        Ok(position)
    }

    /// Whether a post-WAL ambiguity has stopped further write admission.
    #[must_use]
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }

    fn preflight<'b>(
        &self,
        transaction: &Transaction,
        buffer: &BufferPool,
        trees: &[&'b BTreeObject],
        effects: &[FinalEffect],
    ) -> Result<HashMap<StorageObjectId, &'b BTreeObject>, OrderedCommitError> {
        let mut objects = HashMap::with_capacity(trees.len());
        for tree in trees {
            let descriptor = tree.descriptor();
            if !descriptor.authority().requires_commit_recovery() {
                return Err(OrderedCommitError::DerivedObject(descriptor.id()));
            }
            if objects.insert(descriptor.id(), *tree).is_some() {
                return Err(OrderedCommitError::DuplicateObject(descriptor.id()));
            }
        }

        for effect in effects {
            let tree = objects
                .get(&effect.object())
                .copied()
                .ok_or(OrderedCommitError::MissingObject(effect.object()))?;
            let value = match effect.kind() {
                super::MutationKind::OrderedPut => MvccValue::Inline(effect.value().to_vec()),
                super::MutationKind::OrderedDelete => MvccValue::Tombstone,
            };
            let encoded = MvccRecord::installed(transaction.id(), effect.ordinal(), None, value)
                .to_bytes()?;
            tree.preflight_inline_upsert(buffer, effect.key(), &encoded)?;
        }
        Ok(objects)
    }

    fn require_active_status(&self, txn: TxnId) -> Result<(), OrderedCommitError> {
        let actual = self.statuses.status(txn)?;
        if actual == Some(TransactionStatus::Active) {
            Ok(())
        } else {
            Err(OrderedCommitError::WriterStatus { txn, actual })
        }
    }

    fn cleanup_clean_append_failure(
        &self,
        transaction: &mut Transaction,
    ) -> Result<(), OrderedCommitError> {
        if let Err(error) = self.statuses.abort(transaction.id()) {
            self.fenced.store(true, Ordering::Release);
            return Err(OrderedCommitError::Status(error));
        }
        if transaction.phase() != TransactionPhase::Aborted {
            if let Err(error) = transaction.abort() {
                self.fenced.store(true, Ordering::Release);
                return Err(OrderedCommitError::Transaction(error));
            }
        }
        Ok(())
    }

    fn fence_after_wal(&self, transaction: &mut Transaction) {
        self.fenced.store(true, Ordering::Release);
        if transaction.phase() != TransactionPhase::RecoveryRequired {
            let _ = transaction.mark_recovery_required();
        }
    }

    fn ensure_open(&self) -> Result<(), OrderedCommitError> {
        if self.is_fenced() {
            Err(OrderedCommitError::Fenced)
        } else {
            Ok(())
        }
    }
}

fn max_version(current: Option<VersionId>, candidate: VersionId) -> VersionId {
    match current {
        Some(current) if current >= candidate => current,
        _ => candidate,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OrderedCommitError {
    #[error("ordered commit coordinator is fenced until recovery/reopen")]
    Fenced,
    #[error("ordered commit requires an active transaction, found {0:?}")]
    WrongPhase(TransactionPhase),
    #[error("ordered commit object set contains duplicate object {0:?}")]
    DuplicateObject(StorageObjectId),
    #[error("ordered commit is missing authoritative object {0:?}")]
    MissingObject(StorageObjectId),
    #[error("derived object {0:?} cannot participate in authoritative ordered commit")]
    DerivedObject(StorageObjectId),
    #[error("transaction {txn:?} has invalid commit-coordinator status {actual:?}")]
    WriterStatus {
        txn: TxnId,
        actual: Option<TransactionStatus>,
    },
    #[error(transparent)]
    WriteSet(#[from] FinalWriteSetError),
    #[error(transparent)]
    Intent(#[from] WriteIntentError),
    #[error(transparent)]
    BTree(#[from] BTreeError),
    #[error(transparent)]
    Codec(#[from] MvccCodecError),
    #[error(transparent)]
    Status(#[from] StatusTableError),
    #[error(transparent)]
    Append(#[from] CommitAppendError),
    #[error(transparent)]
    Log(#[from] DurableLogError),
    #[error(transparent)]
    Transaction(#[from] TransactionError),
    #[error(transparent)]
    Install(#[from] OrderedMvccInstallError),
    #[error(transparent)]
    Undo(#[from] UndoStoreError),
    #[error(transparent)]
    Visibility(#[from] VisibilityError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{
        BTreeLookup, LogDevice, LogIoOperation, ObjectAuthority, OrderedMvccReader, PageIo,
        PageKey, StorageObjectDescriptor,
    };
    use durable_fs::SyncClass;
    use std::io;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex, RwLock};

    #[derive(Default)]
    struct MemoryPageIo {
        pages: RwLock<HashMap<PageKey, Vec<u8>>>,
    }

    impl PageIo for MemoryPageIo {
        fn read_page(&self, key: PageKey, destination: &mut [u8]) -> io::Result<()> {
            let pages = self
                .pages
                .read()
                .map_err(|_| io::Error::other("page map poisoned"))?;
            let page = pages
                .get(&key)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing page"))?;
            destination.copy_from_slice(page);
            Ok(())
        }

        fn write_page(&self, key: PageKey, source: &[u8]) -> io::Result<()> {
            self.pages
                .write()
                .map_err(|_| io::Error::other("page map poisoned"))?
                .insert(key, source.to_vec());
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemoryLogDevice {
        bytes: Mutex<Vec<u8>>,
        fail_sync: AtomicBool,
    }

    impl MemoryLogDevice {
        fn bytes(&self) -> Vec<u8> {
            self.bytes.lock().expect("log lock").clone()
        }

        fn fail_next_sync(&self) {
            self.fail_sync.store(true, Ordering::Release);
        }
    }

    impl LogDevice for MemoryLogDevice {
        fn append(&self, bytes: &[u8]) -> io::Result<super::super::Lsn> {
            let mut log = self
                .bytes
                .lock()
                .map_err(|_| io::Error::other("log poisoned"))?;
            log.extend_from_slice(bytes);
            super::super::Lsn::from_wal_position(0, log.len() as u64)
                .ok_or_else(|| io::Error::other("log LSN overflow"))
        }

        fn sync_through(&self, _lsn: super::super::Lsn) -> io::Result<()> {
            if self.fail_sync.swap(false, Ordering::AcqRel) {
                Err(io::Error::other("injected WAL sync failure"))
            } else {
                Ok(())
            }
        }
    }

    fn descriptor(id: u64) -> StorageObjectDescriptor {
        StorageObjectDescriptor::new(StorageObjectId::new(id), ObjectAuthority::Authoritative)
    }

    #[test]
    fn multi_object_commit_publishes_only_after_install_and_preserves_old_snapshot() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(16, 512, page_device).expect("buffer");
        let rows = BTreeObject::create(descriptor(1), &buffer).expect("rows");
        let index = BTreeObject::create(descriptor(2), &buffer).expect("index");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo =
            UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device));
        let appender = CommitAppender::new(log.clone(), super::super::CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator = OrderedCommitCoordinator::new(
            &appender,
            log.as_ref(),
            &statuses,
            &frontier,
            &intents,
            &undo,
        );

        let mut first = coordinator
            .begin_write(TxnId::new(1))
            .expect("first begins");
        first
            .stage_ordered_put(descriptor(1), b"row".to_vec(), b"row-v1".to_vec())
            .expect("row stages");
        first
            .stage_ordered_put(descriptor(2), b"idx".to_vec(), b"idx-v1".to_vec())
            .expect("index stages");
        let first_position = coordinator
            .commit(&mut first, &buffer, &[&rows, &index])
            .expect("first commits");
        assert_eq!(first.phase(), TransactionPhase::Released);
        assert_eq!(frontier.snapshot(), first_position.csn);
        assert_eq!(log.durable_lsn(), Some(first_position.lsn));

        let old_snapshot = frontier.snapshot();
        let mut second = coordinator
            .begin_write(TxnId::new(2))
            .expect("second begins");
        second
            .stage_ordered_put(descriptor(1), b"row".to_vec(), b"row-v2".to_vec())
            .expect("row update stages");
        second
            .stage_ordered_put(descriptor(2), b"idx".to_vec(), b"idx-v2".to_vec())
            .expect("index update stages");
        let second_position = coordinator
            .commit(&mut second, &buffer, &[&rows, &index])
            .expect("second commits");
        assert_eq!(second_position.csn.get(), first_position.csn.get() + 1);
        assert!(
            undo.durable_version()
                .is_some_and(|version| version.get() >= 2)
        );

        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert_eq!(
            reader
                .lookup(&rows, &buffer, b"row", None, old_snapshot)
                .expect("old row"),
            super::super::MvccLookup::Found(b"row-v1".to_vec())
        );
        assert_eq!(
            reader
                .lookup(&index, &buffer, b"idx", None, old_snapshot)
                .expect("old index"),
            super::super::MvccLookup::Found(b"idx-v1".to_vec())
        );
        assert_eq!(
            reader
                .lookup(&rows, &buffer, b"row", None, frontier.snapshot())
                .expect("new row"),
            super::super::MvccLookup::Found(b"row-v2".to_vec())
        );
        assert_eq!(
            reader
                .lookup(&index, &buffer, b"idx", None, frontier.snapshot())
                .expect("new index"),
            super::super::MvccLookup::Found(b"idx-v2".to_vec())
        );
    }

    #[test]
    fn oversized_value_is_refused_before_wal_and_remains_retryable() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(4, 384, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo =
            UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device.clone()));
        let appender = CommitAppender::new(log.clone(), super::super::CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator = OrderedCommitCoordinator::new(
            &appender,
            log.as_ref(),
            &statuses,
            &frontier,
            &intents,
            &undo,
        );
        let mut transaction = coordinator
            .begin_write(TxnId::new(10))
            .expect("transaction begins");
        transaction
            .stage_ordered_put(descriptor(1), b"key".to_vec(), vec![0; 512])
            .expect("stages");

        assert!(matches!(
            coordinator.commit(&mut transaction, &buffer, &[&tree]),
            Err(OrderedCommitError::BTree(BTreeError::EntryTooLarge))
        ));
        assert_eq!(transaction.phase(), TransactionPhase::Active);
        assert!(log_device.bytes().is_empty());
        assert!(!coordinator.is_fenced());
        coordinator.abort(&mut transaction).expect("clean abort");
    }

    #[test]
    fn wal_sync_failure_fences_before_installation() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo =
            UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device.clone()));
        let appender = CommitAppender::new(log.clone(), super::super::CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator = OrderedCommitCoordinator::new(
            &appender,
            log.as_ref(),
            &statuses,
            &frontier,
            &intents,
            &undo,
        );
        let mut transaction = coordinator
            .begin_write(TxnId::new(20))
            .expect("transaction begins");
        transaction
            .stage_ordered_put(descriptor(1), b"key".to_vec(), b"value".to_vec())
            .expect("stages");
        log_device.fail_next_sync();

        assert!(matches!(
            coordinator.commit(&mut transaction, &buffer, &[&tree]),
            Err(OrderedCommitError::Log(DurableLogError::Io {
                operation: LogIoOperation::Sync,
                ..
            }))
        ));
        assert!(coordinator.is_fenced());
        assert_eq!(transaction.phase(), TransactionPhase::RecoveryRequired);
        assert_eq!(frontier.snapshot(), super::super::CommitSeq::new(0));
        assert_eq!(
            tree.lookup(&buffer, b"key").expect("lookup"),
            BTreeLookup::NotFound
        );
    }

    #[test]
    fn post_decision_install_failure_fences_before_intent_release() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        tree.insert(&buffer, b"key", b"not-an-mvcc-record")
            .expect("corrupt logical seed");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo =
            UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device));
        let appender = CommitAppender::new(log.clone(), super::super::CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator = OrderedCommitCoordinator::new(
            &appender,
            log.as_ref(),
            &statuses,
            &frontier,
            &intents,
            &undo,
        );
        let mut transaction = coordinator
            .begin_write(TxnId::new(30))
            .expect("transaction begins");
        transaction
            .stage_ordered_put(descriptor(1), b"key".to_vec(), b"value".to_vec())
            .expect("stages");

        assert!(matches!(
            coordinator.commit(&mut transaction, &buffer, &[&tree]),
            Err(OrderedCommitError::Install(OrderedMvccInstallError::Codec(
                _
            )))
        ));
        assert!(coordinator.is_fenced());
        assert_eq!(transaction.phase(), TransactionPhase::RecoveryRequired);
        assert_eq!(frontier.snapshot(), super::super::CommitSeq::new(0));
        assert_eq!(
            statuses.status(TxnId::new(30)).expect("status"),
            Some(TransactionStatus::Active)
        );
        assert_eq!(
            intents
                .owner(StorageObjectId::new(1), b"key")
                .expect("intent owner"),
            None,
            "intent may release only after the runtime fence is established"
        );
        assert!(matches!(
            coordinator.begin_write(TxnId::new(31)),
            Err(OrderedCommitError::Fenced)
        ));
    }
}
