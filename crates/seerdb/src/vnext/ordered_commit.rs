//! Durable-WAL-first commit orchestration for ordered MVCC objects.
//!
//! The coordinator integrates ADR 0014's transaction order. Write intents are
//! acquired before predecessor validation, so write conflicts and current-record
//! corruption are discovered before commit authority enters the WAL. Required
//! before-images may be appended during this pre-WAL validation phase but remain
//! unreachable garbage on a clean abort. Shared page mutation begins only after
//! the decision WAL and all referenced undo are durable. Page persistence still
//! does not become recovery authority until structurally complete checkpoint
//! publication lands.

use super::{
    BTreeError, BTreeObject, BufferError, BufferPool, CommitAppendError, CommitAppender,
    CommitPosition, DurableLogError, FinalEffect, FinalWriteSetError, InstallContext,
    MvccCodecError, MvccRecord, MvccValue, OrderedMvccInstallError, OrderedMvccInstaller,
    PageDependencyTable, PageMaterialization, PrepareEffectResult, RuntimeAdmission,
    RuntimeAdmissionError, StatusTableError, StorageObjectId, Transaction, TransactionError,
    TransactionPhase, TransactionStatus,
    TransactionStatusTable, TxnId, UndoStore, UndoStoreError, VersionId, VisibilityError,
    VisibilityFrontier, WriteIntentError, WriteIntentTable,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

/// Synchronous write-admission and commit coordinator for ordered objects.
///
/// One coordinator is intended to represent one runtime's write-admission
/// fence. The lower WAL/undo components have their own I/O fences; this higher
/// fence prevents an unresolved failure from returning control to an ordinary
/// writer population when recovery/reopen is required.
pub struct OrderedCommitCoordinator<'a> {
    appender: &'a CommitAppender,
    statuses: &'a TransactionStatusTable,
    frontier: &'a VisibilityFrontier,
    intents: &'a WriteIntentTable,
    undo: &'a UndoStore,
    page_dependencies: Option<&'a PageDependencyTable>,
    runtime_admission: Option<&'a RuntimeAdmission>,
    fenced: AtomicBool,
}

impl<'a> OrderedCommitCoordinator<'a> {
    /// Construct the transient coordinator without page-materialization
    /// dependency attachment.
    #[must_use]
    pub const fn new(
        appender: &'a CommitAppender,
        statuses: &'a TransactionStatusTable,
        frontier: &'a VisibilityFrontier,
        intents: &'a WriteIntentTable,
        undo: &'a UndoStore,
    ) -> Self {
        Self {
            appender,
            statuses,
            frontier,
            intents,
            undo,
            page_dependencies: None,
            runtime_admission: None,
            fenced: AtomicBool::new(false),
        }
    }

    /// Construct a coordinator that attaches exact WAL/undo requirements to
    /// mutated B-tree pages and advances the same table only after successful
    /// durability barriers.
    #[must_use]
    pub fn with_page_dependencies(
        appender: &'a CommitAppender,
        statuses: &'a TransactionStatusTable,
        frontier: &'a VisibilityFrontier,
        intents: &'a WriteIntentTable,
        undo: &'a UndoStore,
        page_dependencies: &'a PageDependencyTable,
    ) -> Self {
        if let Some(lsn) = appender.durable_lsn() {
            page_dependencies.advance_wal(lsn);
        }
        if let Some(version) = undo.durable_version() {
            page_dependencies.advance_undo(version);
        }
        Self {
            appender,
            statuses,
            frontier,
            intents,
            undo,
            page_dependencies: Some(page_dependencies),
            runtime_admission: None,
            fenced: AtomicBool::new(false),
        }
    }

    /// Attach the runtime admission/drain authority used by checkpoint capture.
    ///
    /// Commit admission is acquired at the start of `commit` and held through
    /// page installation and visibility completion. Checkpoint drain therefore
    /// cannot establish its capture cut while an admitted commit is still able
    /// to mutate authoritative state.
    #[must_use]
    pub fn with_runtime_admission(mut self, runtime_admission: &'a RuntimeAdmission) -> Self {
        self.runtime_admission = Some(runtime_admission);
        self
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
    /// the WAL. This remains available after the coordinator is fenced so a
    /// caller can discard an active transaction before reopening the runtime.
    pub fn abort(&self, transaction: &mut Transaction) -> Result<(), OrderedCommitError> {
        match transaction.phase() {
            TransactionPhase::Active
            | TransactionPhase::Validating
            | TransactionPhase::Prepared => {}
            actual => return Err(OrderedCommitError::WrongPhase(actual)),
        }
        self.statuses.abort(transaction.id())?;
        if let Err(error) = transaction.abort() {
            self.fence_admission();
            return Err(OrderedCommitError::Transaction(error));
        }
        Ok(())
    }

    /// Commit one transaction across one or more authoritative ordered objects.
    ///
    /// Intents, deterministic representability checks and predecessor conflict
    /// validation all happen before `begin_validation`/WAL append. Preparing a
    /// predecessor may append an unreachable undo before-image, but no page can
    /// reference it and no commit decision exists yet. Once WAL append may have
    /// happened, every uncertain or post-decision failure fences this
    /// coordinator. After decision durability, one grouped undo barrier covers
    /// all prepared effects before the first shared page mutation.
    pub fn commit(
        &self,
        transaction: &mut Transaction,
        buffer: &BufferPool,
        trees: &[&BTreeObject],
    ) -> Result<CommitPosition, OrderedCommitError> {
        self.ensure_open()?;
        let _runtime_admission = self
            .runtime_admission
            .map(RuntimeAdmission::admit_commit)
            .transpose()?;
        if transaction.phase() != TransactionPhase::Active {
            return Err(OrderedCommitError::WrongPhase(transaction.phase()));
        }

        let effects = transaction.final_effects()?;
        let intent_guard = self.intents.try_acquire(transaction.id(), &effects)?;
        let objects = self.preflight(transaction, buffer, trees, &effects)?;
        self.require_active_status(transaction.id())?;

        let installer = OrderedMvccInstaller::new(self.statuses, self.undo);
        let mut prepared_effects = Vec::with_capacity(effects.len());
        let mut max_required_undo = None;
        for effect in &effects {
            let tree = objects
                .get(&effect.object())
                .copied()
                .ok_or(OrderedCommitError::MissingObject(effect.object()))?;
            let prepared = match installer.prepare(
                tree,
                buffer,
                &intent_guard,
                effect,
                InstallContext::Live {
                    snapshot: transaction.snapshot(),
                },
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    if prepare_failure_requires_fence(&error) {
                        self.fence_admission();
                    }
                    return Err(OrderedCommitError::Install(error));
                }
            };
            if let Some(version) = prepared.required_undo() {
                max_required_undo = Some(max_version(max_required_undo, version));
            }
            if let PrepareEffectResult::Prepared(prepared) = prepared {
                prepared_effects.push((tree, prepared));
            }
        }

        transaction.begin_validation()?;
        let ticket = match self.appender.append_validated(transaction) {
            Ok(ticket) => ticket,
            Err(error) => {
                if transaction.phase() == TransactionPhase::RecoveryRequired {
                    self.fence_after_wal(transaction);
                } else {
                    self.cleanup_clean_append_failure(transaction)?;
                }
                return Err(OrderedCommitError::Append(error));
            }
        };

        if let Err(error) = self.appender.sync_through(ticket.decision_lsn()) {
            self.fence_after_wal(transaction);
            return Err(OrderedCommitError::Log(error));
        }
        if let Some(dependencies) = self.page_dependencies {
            dependencies.advance_wal(ticket.decision_lsn());
        }
        let position = match transaction.mark_durable(ticket.decision_lsn()) {
            Ok(position) => position,
            Err(error) => {
                self.fence_after_wal(transaction);
                return Err(OrderedCommitError::Transaction(error));
            }
        };

        if let Some(version) = max_required_undo {
            let durable = match self.undo.sync_through(version) {
                Ok(durable) => durable,
                Err(error) => {
                    self.fence_after_wal(transaction);
                    return Err(OrderedCommitError::Undo(error));
                }
            };
            if let Some(dependencies) = self.page_dependencies {
                dependencies.advance_undo(durable);
            }
        }

        for (tree, prepared) in &prepared_effects {
            let installed = if let Some(dependencies) = self.page_dependencies {
                installer.apply_prepared_with_dependencies(
                    tree,
                    buffer,
                    &intent_guard,
                    prepared,
                    PageMaterialization::new(dependencies, ticket.decision_lsn()),
                )
            } else {
                installer.apply_prepared(tree, buffer, &intent_guard, prepared)
            };
            if let Err(error) = installed {
                self.fence_after_wal(transaction);
                return Err(OrderedCommitError::Install(error));
            }
        }

        // Readiness publication is not completion: a synchronous commit is only
        // acknowledged once the contiguous frontier covers this CSN, so a
        // snapshot taken after the acknowledgement must observe the effect.
        if let Err(error) =
            self.frontier
                .complete_commit(self.statuses, transaction.id(), ticket.csn())
        {
            self.fence_after_wal(transaction);
            return Err(OrderedCommitError::Visibility(error));
        }
        if let Err(error) = transaction.mark_visible() {
            self.fence_after_wal(transaction);
            return Err(OrderedCommitError::Transaction(error));
        }
        if let Err(error) = transaction.release() {
            self.fence_after_wal(transaction);
            return Err(OrderedCommitError::Transaction(error));
        }

        intent_guard.release();
        Ok(position)
    }

    /// Whether an unresolved runtime failure has stopped further write
    /// admission until recovery/reopen.
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
            self.fence_admission();
            return Err(OrderedCommitError::Status(error));
        }
        if transaction.phase() != TransactionPhase::Aborted
            && let Err(error) = transaction.abort()
        {
            self.fence_admission();
            return Err(OrderedCommitError::Transaction(error));
        }
        Ok(())
    }

    /// Establish the unresolved-failure fence.
    ///
    /// Every post-decision failure path routes through here so completion
    /// waiters are woken with recovery-required semantics instead of blocking on
    /// a frontier that cannot advance until recovery.
    fn fence_after_wal(&self, transaction: &mut Transaction) {
        self.fence_admission();
        if transaction.phase() != TransactionPhase::RecoveryRequired {
            let _ = transaction.mark_recovery_required();
        }
        self.frontier.require_recovery();
    }

    fn fence_admission(&self) {
        self.fenced.store(true, Ordering::Release);
        if let Some(runtime_admission) = self.runtime_admission {
            runtime_admission.require_recovery();
        }
    }

    fn ensure_open(&self) -> Result<(), OrderedCommitError> {
        if self.is_fenced() {
            return Err(OrderedCommitError::Fenced);
        }
        if let Some(runtime_admission) = self.runtime_admission {
            runtime_admission.ensure_read_admission()?;
        }
        Ok(())
    }
}

fn max_version(current: Option<VersionId>, candidate: VersionId) -> VersionId {
    match current {
        Some(current) if current >= candidate => current,
        _ => candidate,
    }
}

fn prepare_failure_requires_fence(error: &OrderedMvccInstallError) -> bool {
    !matches!(
        error,
        OrderedMvccInstallError::SnapshotConflict { .. }
            | OrderedMvccInstallError::ActiveWriterConflict(_)
            | OrderedMvccInstallError::BTree(BTreeError::Buffer(BufferError::NoVictim))
    )
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
    Admission(#[from] RuntimeAdmissionError),
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
        BTreeLookup, CommitSeq, DependencyCheckedPageIo, DurableLog, LogDevice, LogIoOperation,
        ObjectAuthority, OrderedMvccReader, PageId, PageIo, PageKey, RecordOwner,
        StorageObjectDescriptor, StoreDirectory,
    };
    use durable_fs::SyncClass;
    use std::io;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::{Duration, Instant};

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
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device));
        let appender = CommitAppender::new(log, super::super::CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator =
            OrderedCommitCoordinator::new(&appender, &statuses, &frontier, &intents, &undo);

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
        assert_eq!(appender.durable_lsn(), Some(first_position.lsn));

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
    fn dependency_aware_commit_advances_both_materialization_frontiers() {
        let physical = Arc::new(MemoryPageIo::default());
        let page_dependencies = Arc::new(PageDependencyTable::new());
        let checked = Arc::new(DependencyCheckedPageIo::new(
            physical,
            Arc::clone(&page_dependencies),
        ));
        let buffer = BufferPool::new(8, 512, checked).expect("buffer");
        let tree = BTreeObject::create(descriptor(9), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device));
        let appender = CommitAppender::new(log, super::super::CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator = OrderedCommitCoordinator::with_page_dependencies(
            &appender,
            &statuses,
            &frontier,
            &intents,
            &undo,
            page_dependencies.as_ref(),
        );

        let mut first = coordinator
            .begin_write(TxnId::new(90))
            .expect("first begins");
        first
            .stage_ordered_put(descriptor(9), b"key".to_vec(), b"v1".to_vec())
            .expect("first stages");
        coordinator
            .commit(&mut first, &buffer, &[&tree])
            .expect("first commits");

        let mut second = coordinator
            .begin_write(TxnId::new(91))
            .expect("second begins");
        second
            .stage_ordered_put(descriptor(9), b"key".to_vec(), b"v2".to_vec())
            .expect("second stages");
        let position = coordinator
            .commit(&mut second, &buffer, &[&tree])
            .expect("second commits");

        let page = PageKey::new(tree.descriptor().id(), PageId::new(0));
        let required = page_dependencies
            .requirements(page)
            .expect("requirements read");
        assert_eq!(required.required_wal(), position.lsn);
        assert_eq!(required.required_undo(), Some(VersionId::new(1)));
        assert_eq!(page_dependencies.durable_wal(), position.lsn);
        assert_eq!(page_dependencies.durable_undo(), Some(VersionId::new(1)));
        assert!(page_dependencies.is_eligible(page).expect("page eligible"));
        buffer.flush_page(page).expect("eligible page flushes");
    }

    #[test]
    fn checkpoint_drain_refuses_new_commit_before_wal_and_reopens_cleanly() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(14), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device.clone()));
        let appender = CommitAppender::new(log, CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let admission = RuntimeAdmission::new();
        let coordinator =
            OrderedCommitCoordinator::new(&appender, &statuses, &frontier, &intents, &undo)
                .with_runtime_admission(&admission);

        let mut transaction = coordinator
            .begin_write(TxnId::new(140))
            .expect("transaction begins");
        transaction
            .stage_ordered_put(descriptor(14), b"key".to_vec(), b"value".to_vec())
            .expect("stages");

        let checkpoint = admission.begin_checkpoint().expect("checkpoint starts");
        assert!(matches!(
            coordinator.commit(&mut transaction, &buffer, &[&tree]),
            Err(OrderedCommitError::Admission(
                RuntimeAdmissionError::CheckpointInProgress
            ))
        ));
        assert_eq!(transaction.phase(), TransactionPhase::Active);
        assert!(log_device.bytes().is_empty());
        drop(checkpoint);

        coordinator
            .commit(&mut transaction, &buffer, &[&tree])
            .expect("commit proceeds after checkpoint interval");
        assert_eq!(admission.active_commits().expect("count"), 0);
    }

    #[test]
    fn snapshot_conflict_is_rejected_before_wal_decision() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(12), &buffer).expect("tree");
        let newer = MvccRecord::new(
            RecordOwner::Frozen(super::super::CommitSeq::new(2)),
            None,
            MvccValue::Inline(b"newer".to_vec()),
        );
        tree.insert(&buffer, b"key", &newer.to_bytes().expect("encode"))
            .expect("seed newer predecessor");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device.clone()));
        let appender = CommitAppender::new(log, super::super::CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator =
            OrderedCommitCoordinator::new(&appender, &statuses, &frontier, &intents, &undo);
        let mut transaction = coordinator
            .begin_write(TxnId::new(120))
            .expect("transaction begins at snapshot zero");
        transaction
            .stage_ordered_put(descriptor(12), b"key".to_vec(), b"mine".to_vec())
            .expect("stages");

        assert!(matches!(
            coordinator.commit(&mut transaction, &buffer, &[&tree]),
            Err(OrderedCommitError::Install(
                OrderedMvccInstallError::SnapshotConflict { .. }
            ))
        ));
        assert_eq!(transaction.phase(), TransactionPhase::Active);
        assert!(log_device.bytes().is_empty());
        assert!(!coordinator.is_fenced());
        assert_eq!(
            tree.lookup(&buffer, b"key").expect("lookup"),
            BTreeLookup::Found(newer.to_bytes().expect("encode"))
        );
        coordinator
            .abort(&mut transaction)
            .expect("conflict aborts cleanly");
    }

    #[test]
    fn corrupt_current_is_rejected_before_wal_and_fences_runtime() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        tree.insert(&buffer, b"key", b"not-an-mvcc-record")
            .expect("corrupt logical seed");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device.clone()));
        let appender = CommitAppender::new(log, super::super::CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator =
            OrderedCommitCoordinator::new(&appender, &statuses, &frontier, &intents, &undo);
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
        assert_eq!(transaction.phase(), TransactionPhase::Active);
        assert!(log_device.bytes().is_empty());
        assert_eq!(frontier.snapshot(), super::super::CommitSeq::new(0));
        coordinator
            .abort(&mut transaction)
            .expect("active transaction aborts");
        assert!(matches!(
            coordinator.begin_write(TxnId::new(31)),
            Err(OrderedCommitError::Fenced)
        ));
    }

    #[test]
    fn oversized_value_is_refused_before_wal_and_remains_retryable() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(4, 384, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device.clone()));
        let appender = CommitAppender::new(log, super::super::CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator =
            OrderedCommitCoordinator::new(&appender, &statuses, &frontier, &intents, &undo);
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
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device.clone()));
        let appender = CommitAppender::new(log, super::super::CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator =
            OrderedCommitCoordinator::new(&appender, &statuses, &frontier, &intents, &undo);
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

    /// Block until `txn` has published `csn` as committed, or give up.
    ///
    /// Readiness publication happens inside completion, so observing it is how a
    /// test waits for the exact state where a frontier gap can withhold
    /// acknowledgement.
    fn wait_until_committed(statuses: &TransactionStatusTable, txn: TxnId, csn: CommitSeq) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if statuses.status(txn).expect("status") == Some(TransactionStatus::Committed(csn)) {
                return true;
            }
            std::thread::yield_now();
        }
        false
    }

    /// A later commit must not be acknowledged while an earlier admitted CSN is
    /// published ready but not visible; closing the gap completes it.
    #[test]
    fn later_commit_waits_for_the_earlier_csn_visibility_gap_to_close() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device));
        let appender = CommitAppender::new(log, CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator =
            OrderedCommitCoordinator::new(&appender, &statuses, &frontier, &intents, &undo);

        let mut first = coordinator
            .begin_write(TxnId::new(31))
            .expect("first begins");
        first
            .stage_ordered_put(descriptor(1), b"a".to_vec(), b"a-v1".to_vec())
            .expect("stages");
        let first_position = coordinator
            .commit(&mut first, &buffer, &[&tree])
            .expect("first commits");
        assert_eq!(first_position.csn, CommitSeq::new(1));
        assert_eq!(frontier.snapshot(), CommitSeq::new(1));

        // An admitted writer that appended CSN 2 and then stalled before
        // readiness publication. Its intents stay retained, as in the runtime.
        let mut stalled = coordinator
            .begin_write(TxnId::new(32))
            .expect("stalled begins");
        stalled
            .stage_ordered_put(descriptor(1), b"b".to_vec(), b"b-v1".to_vec())
            .expect("stages");
        let stalled_effects = stalled.final_effects().expect("canonical effects");
        let stalled_intents = intents
            .try_acquire(stalled.id(), &stalled_effects)
            .expect("stalled intents");
        stalled.begin_validation().expect("stalled validates");
        assert_eq!(
            appender
                .append_validated(&mut stalled)
                .expect("stalled appends")
                .csn(),
            CommitSeq::new(2)
        );

        let mut later = coordinator
            .begin_write(TxnId::new(33))
            .expect("later begins");
        later
            .stage_ordered_put(descriptor(1), b"c".to_vec(), b"c-v1".to_vec())
            .expect("stages");
        let later_id = later.id();

        let (started_tx, started_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                started_tx.send(()).expect("signal start");
                let result = coordinator.commit(&mut later, &buffer, &[&tree]);
                result_tx.send(result).expect("send result");
            });
            started_rx.recv().expect("commit starts");

            assert!(
                wait_until_committed(&statuses, later_id, CommitSeq::new(3)),
                "later commit never published readiness"
            );
            assert_eq!(
                frontier.snapshot(),
                CommitSeq::new(1),
                "readiness must not advance the visible frontier across the gap"
            );
            assert!(
                matches!(result_rx.try_recv(), Err(TryRecvError::Empty)),
                "later commit was acknowledged before its CSN was visible"
            );

            frontier
                .publish_commit(&statuses, stalled.id(), CommitSeq::new(2))
                .expect("stalled publishes");
            let position = result_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("later commit returns once the gap closes")
                .expect("later commit succeeds");
            assert_eq!(position.csn, CommitSeq::new(3));
            assert_eq!(frontier.snapshot(), CommitSeq::new(3));
        });

        drop(stalled_intents);
        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"c", None, frontier.snapshot())
                .expect("later row"),
            super::super::MvccLookup::Found(b"c-v1".to_vec())
        );
    }

    /// An unresolved earlier durable decision must wake waiting commits with a
    /// recovery-required outcome instead of acknowledging them or blocking
    /// forever.
    #[test]
    fn unresolved_earlier_decision_fails_waiting_commit_with_recovery_required() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let log_device = Arc::new(MemoryLogDevice::default());
        let log = Arc::new(DurableLog::new(log_device));
        let appender = CommitAppender::new(log, CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let coordinator =
            OrderedCommitCoordinator::new(&appender, &statuses, &frontier, &intents, &undo);

        let mut first = coordinator
            .begin_write(TxnId::new(41))
            .expect("first begins");
        first
            .stage_ordered_put(descriptor(1), b"a".to_vec(), b"a-v1".to_vec())
            .expect("stages");
        coordinator
            .commit(&mut first, &buffer, &[&tree])
            .expect("first commits");

        let mut stalled = coordinator
            .begin_write(TxnId::new(42))
            .expect("stalled begins");
        stalled
            .stage_ordered_put(descriptor(1), b"b".to_vec(), b"b-v1".to_vec())
            .expect("stages");
        stalled.begin_validation().expect("stalled validates");
        appender
            .append_validated(&mut stalled)
            .expect("stalled appends");

        let mut later = coordinator
            .begin_write(TxnId::new(43))
            .expect("later begins");
        later
            .stage_ordered_put(descriptor(1), b"c".to_vec(), b"c-v1".to_vec())
            .expect("stages");
        let later_id = later.id();

        let (started_tx, started_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                started_tx.send(()).expect("signal start");
                let result = coordinator.commit(&mut later, &buffer, &[&tree]);
                result_tx.send(result).expect("send result");
            });
            started_rx.recv().expect("commit starts");
            assert!(
                wait_until_committed(&statuses, later_id, CommitSeq::new(3)),
                "later commit never published readiness"
            );
            assert!(
                matches!(
                    result_rx.recv_timeout(Duration::from_millis(500)),
                    Err(RecvTimeoutError::Timeout)
                ),
                "later commit must not be acknowledged while CSN 2 is unpublished"
            );

            // The stalled transaction fails after its durable decision existed.
            coordinator.fence_after_wal(&mut stalled);
            let result = result_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("waiting commit wakes");
            assert!(
                matches!(
                    result,
                    Err(OrderedCommitError::Visibility(
                        VisibilityError::RecoveryRequired { .. }
                    ))
                ),
                "waiting commit must report recovery, not success: {result:?}"
            );
            assert!(coordinator.is_fenced());
        });
    }
}
