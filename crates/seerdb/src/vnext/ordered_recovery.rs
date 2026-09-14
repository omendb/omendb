//! Ordered-access-method application of validated recovered transactions.
//!
//! Recovery uses the same canonical final effects, write intents, prepared MVCC
//! predecessor state, grouped undo barrier, current-record installer, and
//! visibility frontier as live commit. The baseline is intentionally sequential
//! in CSN order. The WAL scanner/caller must establish the retained WAL
//! durability barrier before passing validated transactions here.

use super::{
    BTreeError, BTreeObject, BufferPool, CommitSeq, FinalEffect, FinalWriteSetError,
    InstallContext, Lsn, MvccCodecError, MvccRecord, MvccValue, OrderedMvccInstallError,
    OrderedMvccInstaller, PageDependencyTable, PageMaterialization, PrepareEffectResult,
    RecoveredTransaction, StatusTableError, StorageObjectId, TransactionStatus,
    TransactionStatusTable, TxnId, UndoStore, UndoStoreError, VersionId, VisibilityError,
    VisibilityFrontier, WriteIntentError, WriteIntentTable,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RecoveryApplyResult {
    Applied,
    AlreadyApplied,
}

/// Sequential recovery applicator for authoritative ordered objects.
pub struct OrderedRecoveryApplier<'a> {
    statuses: &'a TransactionStatusTable,
    frontier: &'a VisibilityFrontier,
    intents: &'a WriteIntentTable,
    undo: &'a UndoStore,
    page_dependencies: Option<&'a PageDependencyTable>,
    lane: Mutex<()>,
    fenced: AtomicBool,
}

impl<'a> OrderedRecoveryApplier<'a> {
    #[must_use]
    pub const fn new(
        statuses: &'a TransactionStatusTable,
        frontier: &'a VisibilityFrontier,
        intents: &'a WriteIntentTable,
        undo: &'a UndoStore,
    ) -> Self {
        Self {
            statuses,
            frontier,
            intents,
            undo,
            page_dependencies: None,
            lane: Mutex::new(()),
            fenced: AtomicBool::new(false),
        }
    }

    /// Construct dependency-aware recovery. The caller must advance the table's
    /// WAL frontier only after the retained WAL has been successfully
    /// synchronized through the replay transaction's decision LSN. Existing
    /// durable undo is seeded immediately because `UndoStore` owns that barrier.
    #[must_use]
    pub fn with_page_dependencies(
        statuses: &'a TransactionStatusTable,
        frontier: &'a VisibilityFrontier,
        intents: &'a WriteIntentTable,
        undo: &'a UndoStore,
        page_dependencies: &'a PageDependencyTable,
    ) -> Self {
        if let Some(version) = undo.durable_version() {
            page_dependencies.advance_undo(version);
        }
        Self {
            statuses,
            frontier,
            intents,
            undo,
            page_dependencies: Some(page_dependencies),
            lane: Mutex::new(()),
            fenced: AtomicBool::new(false),
        }
    }

    /// Apply one validated committed transaction after the current recovery
    /// frontier. Repeating the exact transaction already covered by this
    /// process-local frontier is an idempotent no-op; a different/missing owner
    /// cannot claim an already-visible CSN.
    pub fn apply(
        &self,
        recovered: &RecoveredTransaction,
        buffer: &BufferPool,
        trees: &[&BTreeObject],
    ) -> Result<RecoveryApplyResult, OrderedRecoveryError> {
        self.ensure_open()?;
        let _lane = self.lane.lock().map_err(|_| {
            self.fenced.store(true, Ordering::Release);
            OrderedRecoveryError::Poisoned
        })?;
        self.ensure_open()?;

        let txn = recovered.txn_id();
        let position = recovered.position();
        let csn = position.csn;
        let visible = self.frontier.snapshot();
        if csn <= visible {
            let actual = self.statuses.status(txn)?;
            if actual == Some(TransactionStatus::Committed(csn)) {
                return Ok(RecoveryApplyResult::AlreadyApplied);
            }
            return self.fail(OrderedRecoveryError::AlreadyVisibleIdentity {
                visible,
                txn,
                requested: csn,
                actual,
            });
        }
        let expected = visible
            .get()
            .checked_add(1)
            .map(CommitSeq::new)
            .ok_or(OrderedRecoveryError::CommitSeqExhausted)?;
        if csn != expected {
            self.fenced.store(true, Ordering::Release);
            return Err(OrderedRecoveryError::NonContiguousCommit {
                visible,
                expected,
                actual: csn,
            });
        }
        if let Some(dependencies) = self.page_dependencies {
            let durable = dependencies.durable_wal();
            if durable < position.lsn {
                return self.fail(OrderedRecoveryError::WalFrontierBehind {
                    required: position.lsn,
                    durable,
                });
            }
        }

        let effects = match recovered.final_effects() {
            Ok(effects) => effects,
            Err(error) => return self.fail(OrderedRecoveryError::WriteSet(error)),
        };
        let intent_guard = match self.intents.try_acquire(txn, &effects) {
            Ok(guard) => guard,
            Err(error) => return self.fail(OrderedRecoveryError::Intent(error)),
        };
        let objects = match self.preflight(txn, buffer, trees, &effects) {
            Ok(objects) => objects,
            Err(error) => return self.fail(error),
        };
        if let Err(error) = self.statuses.recover_committed(txn, csn) {
            return self.fail(OrderedRecoveryError::Status(error));
        }

        let installer = OrderedMvccInstaller::new(self.statuses, self.undo);
        let mut prepared_effects = Vec::with_capacity(effects.len());
        let mut max_required_undo = None;
        for effect in &effects {
            let Some(tree) = objects.get(&effect.object()).copied() else {
                return self.fail(OrderedRecoveryError::MissingObject(effect.object()));
            };
            let prepared = match installer.prepare(
                tree,
                buffer,
                &intent_guard,
                effect,
                InstallContext::Recovery { commit: csn },
            ) {
                Ok(prepared) => prepared,
                Err(error) => return self.fail(OrderedRecoveryError::Install(error)),
            };
            if let Some(version) = prepared.required_undo() {
                max_required_undo = Some(max_version(max_required_undo, version));
            }
            if let PrepareEffectResult::Prepared(prepared) = prepared {
                prepared_effects.push((tree, prepared));
            }
        }

        if let Some(version) = max_required_undo {
            let durable = match self.undo.sync_through(version) {
                Ok(durable) => durable,
                Err(error) => return self.fail(OrderedRecoveryError::Undo(error)),
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
                    PageMaterialization::new(dependencies, position.lsn),
                )
            } else {
                installer.apply_prepared(tree, buffer, &intent_guard, prepared)
            };
            if let Err(error) = installed {
                return self.fail(OrderedRecoveryError::Install(error));
            }
        }

        if let Err(error) = self.frontier.publish_recovered(self.statuses, txn, csn) {
            return self.fail(OrderedRecoveryError::Visibility(error));
        }
        intent_guard.release();
        Ok(RecoveryApplyResult::Applied)
    }

    #[must_use]
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }

    fn preflight<'b>(
        &self,
        txn: TxnId,
        buffer: &BufferPool,
        trees: &[&'b BTreeObject],
        effects: &[FinalEffect],
    ) -> Result<HashMap<StorageObjectId, &'b BTreeObject>, OrderedRecoveryError> {
        let mut objects = HashMap::with_capacity(trees.len());
        for tree in trees {
            let descriptor = tree.descriptor();
            if !descriptor.authority().requires_commit_recovery() {
                return Err(OrderedRecoveryError::DerivedObject(descriptor.id()));
            }
            if objects.insert(descriptor.id(), *tree).is_some() {
                return Err(OrderedRecoveryError::DuplicateObject(descriptor.id()));
            }
        }
        for effect in effects {
            let tree = objects
                .get(&effect.object())
                .copied()
                .ok_or(OrderedRecoveryError::MissingObject(effect.object()))?;
            let value = match effect.kind() {
                super::MutationKind::OrderedPut => MvccValue::Inline(effect.value().to_vec()),
                super::MutationKind::OrderedDelete => MvccValue::Tombstone,
            };
            let encoded = MvccRecord::installed(txn, effect.ordinal(), None, value).to_bytes()?;
            tree.preflight_inline_upsert(buffer, effect.key(), &encoded)?;
        }
        Ok(objects)
    }

    fn fail<T>(&self, error: OrderedRecoveryError) -> Result<T, OrderedRecoveryError> {
        self.fenced.store(true, Ordering::Release);
        Err(error)
    }

    fn ensure_open(&self) -> Result<(), OrderedRecoveryError> {
        if self.is_fenced() {
            Err(OrderedRecoveryError::Fenced)
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
pub enum OrderedRecoveryError {
    #[error("ordered recovery applicator is fenced")]
    Fenced,
    #[error("ordered recovery lane is poisoned")]
    Poisoned,
    #[error("commit sequence space is exhausted")]
    CommitSeqExhausted,
    #[error(
        "recovered commit is not contiguous after {visible:?}: expected {expected:?}, got {actual:?}"
    )]
    NonContiguousCommit {
        visible: CommitSeq,
        expected: CommitSeq,
        actual: CommitSeq,
    },
    #[error(
        "transaction {txn:?} cannot claim already-visible commit {requested:?} at frontier {visible:?}; status is {actual:?}"
    )]
    AlreadyVisibleIdentity {
        visible: CommitSeq,
        txn: TxnId,
        requested: CommitSeq,
        actual: Option<TransactionStatus>,
    },
    #[error("retained WAL durability {durable:?} is behind recovered decision {required:?}")]
    WalFrontierBehind { required: Lsn, durable: Lsn },
    #[error("ordered recovery object set contains duplicate object {0:?}")]
    DuplicateObject(StorageObjectId),
    #[error("ordered recovery is missing authoritative object {0:?}")]
    MissingObject(StorageObjectId),
    #[error("derived object {0:?} cannot receive authoritative recovery replay")]
    DerivedObject(StorageObjectId),
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
    Install(#[from] OrderedMvccInstallError),
    #[error(transparent)]
    Undo(#[from] UndoStoreError),
    #[error(transparent)]
    Visibility(#[from] VisibilityError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::format::Lsn;
    use crate::vnext::{
        BTreeLookup, CommitDecision, DependencyCheckedPageIo, LogRecord, LoggedMutation,
        ObjectAuthority, OrderedMvccReader, PageId, PageIo, PageKey, RecoveryAssembler,
        StorageObjectDescriptor, StoreDirectory, mutation_digest,
    };
    use durable_fs::SyncClass;
    use std::io;
    use std::sync::{Arc, RwLock};

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

    fn descriptor(id: u64) -> StorageObjectDescriptor {
        StorageObjectDescriptor::new(StorageObjectId::new(id), ObjectAuthority::Authoritative)
    }

    fn recovered(txn: u64, csn: u64, writes: &[(u64, &[u8], &[u8])]) -> RecoveredTransaction {
        let mutations: Vec<_> = writes
            .iter()
            .enumerate()
            .map(|(ordinal, (object, key, value))| {
                LoggedMutation::ordered_put(
                    TxnId::new(txn),
                    ordinal as u32,
                    StorageObjectId::new(*object),
                    key.to_vec(),
                    value.to_vec(),
                )
            })
            .collect();
        let mut assembler = RecoveryAssembler::new();
        for (index, mutation) in mutations.iter().cloned().enumerate() {
            assembler
                .push(
                    Lsn::from_wal_position(0, (index as u64 + 1) * 10).expect("lsn"),
                    LogRecord::Mutation(mutation),
                )
                .expect("mutation validates");
        }
        let decision = CommitDecision::new(
            TxnId::new(txn),
            CommitSeq::new(csn),
            mutations.len() as u32,
            mutation_digest(&mutations).expect("digest"),
        );
        assembler
            .push(
                Lsn::from_wal_position(0, (mutations.len() as u64 + 1) * 10).expect("lsn"),
                LogRecord::Commit(decision),
            )
            .expect("commit validates")
            .expect("committed transaction")
    }

    #[test]
    fn replay_is_ordered_visible_and_completed_retry_is_allocation_free() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let applier = OrderedRecoveryApplier::new(&statuses, &frontier, &intents, &undo);

        let first = recovered(1, 1, &[(1, b"key", b"v1")]);
        let second = recovered(2, 2, &[(1, b"key", b"v2")]);
        assert_eq!(
            applier
                .apply(&first, &buffer, &[&tree])
                .expect("first applies"),
            RecoveryApplyResult::Applied
        );
        assert_eq!(
            applier
                .apply(&second, &buffer, &[&tree])
                .expect("second applies"),
            RecoveryApplyResult::Applied
        );
        assert_eq!(frontier.snapshot(), CommitSeq::new(2));
        assert_eq!(undo.durable_version(), Some(VersionId::new(1)));

        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"key", None, CommitSeq::new(1))
                .expect("old snapshot"),
            super::super::MvccLookup::Found(b"v1".to_vec())
        );
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"key", None, CommitSeq::new(2))
                .expect("new snapshot"),
            super::super::MvccLookup::Found(b"v2".to_vec())
        );

        assert_eq!(
            applier
                .apply(&second, &buffer, &[&tree])
                .expect("completed retry"),
            RecoveryApplyResult::AlreadyApplied
        );
        assert_eq!(undo.durable_version(), Some(VersionId::new(1)));
    }

    #[test]
    fn dependency_aware_recovery_requires_wal_barrier_and_advances_undo() {
        let physical = Arc::new(MemoryPageIo::default());
        let page_dependencies = Arc::new(PageDependencyTable::new());
        let checked = Arc::new(DependencyCheckedPageIo::new(
            physical,
            Arc::clone(&page_dependencies),
        ));
        let buffer = BufferPool::new(8, 512, checked).expect("buffer");
        let tree = BTreeObject::create(descriptor(8), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let first = recovered(80, 1, &[(8, b"key", b"v1")]);

        let blocked = OrderedRecoveryApplier::with_page_dependencies(
            &statuses,
            &frontier,
            &intents,
            &undo,
            page_dependencies.as_ref(),
        );
        assert!(matches!(
            blocked.apply(&first, &buffer, &[&tree]),
            Err(OrderedRecoveryError::WalFrontierBehind { required, .. })
                if required == first.position().lsn
        ));
        assert_eq!(frontier.snapshot(), CommitSeq::new(0));
        assert_eq!(
            tree.lookup(&buffer, b"key").expect("lookup"),
            BTreeLookup::NotFound
        );

        page_dependencies.advance_wal(first.position().lsn);
        let applier = OrderedRecoveryApplier::with_page_dependencies(
            &statuses,
            &frontier,
            &intents,
            &undo,
            page_dependencies.as_ref(),
        );
        applier
            .apply(&first, &buffer, &[&tree])
            .expect("first applies after WAL barrier");

        let second = recovered(81, 2, &[(8, b"key", b"v2")]);
        page_dependencies.advance_wal(second.position().lsn);
        applier
            .apply(&second, &buffer, &[&tree])
            .expect("second applies");
        let page = PageKey::new(tree.descriptor().id(), PageId::new(0));
        let required = page_dependencies
            .requirements(page)
            .expect("requirements read");
        assert_eq!(required.required_wal(), second.position().lsn);
        assert_eq!(required.required_undo(), Some(VersionId::new(1)));
        assert_eq!(page_dependencies.durable_undo(), Some(VersionId::new(1)));
        assert!(page_dependencies.is_eligible(page).expect("page eligible"));
        buffer.flush_page(page).expect("eligible page flushes");
    }

    #[test]
    fn already_visible_csn_cannot_be_claimed_by_a_different_transaction() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let applier = OrderedRecoveryApplier::new(&statuses, &frontier, &intents, &undo);
        let first = recovered(1, 1, &[(1, b"key", b"v1")]);
        applier
            .apply(&first, &buffer, &[&tree])
            .expect("first applies");

        let impostor = recovered(9, 1, &[(1, b"other", b"wrong")]);
        assert!(matches!(
            applier.apply(&impostor, &buffer, &[&tree]),
            Err(OrderedRecoveryError::AlreadyVisibleIdentity {
                txn,
                requested,
                actual: None,
                ..
            }) if txn == TxnId::new(9) && requested == CommitSeq::new(1)
        ));
        assert!(applier.is_fenced());
        assert_eq!(
            tree.lookup(&buffer, b"other").expect("lookup"),
            BTreeLookup::NotFound
        );
    }

    #[test]
    fn out_of_order_recovery_fails_closed_before_install() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, page_device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let applier = OrderedRecoveryApplier::new(&statuses, &frontier, &intents, &undo);
        let second = recovered(2, 2, &[(1, b"key", b"v2")]);

        assert!(matches!(
            applier.apply(&second, &buffer, &[&tree]),
            Err(OrderedRecoveryError::NonContiguousCommit { .. })
        ));
        assert!(applier.is_fenced());
        assert_eq!(frontier.snapshot(), CommitSeq::new(0));
        assert_eq!(
            tree.lookup(&buffer, b"key").expect("lookup"),
            BTreeLookup::NotFound
        );
    }

    #[test]
    fn partial_multi_object_install_is_hidden_and_fences_recovery() {
        let page_device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(12, 512, page_device).expect("buffer");
        let first = BTreeObject::create(descriptor(1), &buffer).expect("first tree");
        let second = BTreeObject::create(descriptor(2), &buffer).expect("second tree");
        second
            .insert(&buffer, b"bad", b"not-an-mvcc-record")
            .expect("corrupt logical seed");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let applier = OrderedRecoveryApplier::new(&statuses, &frontier, &intents, &undo);
        let transaction = recovered(5, 1, &[(1, b"good", b"installed"), (2, b"bad", b"fails")]);

        assert!(matches!(
            applier.apply(&transaction, &buffer, &[&first, &second]),
            Err(OrderedRecoveryError::Install(
                OrderedMvccInstallError::Codec(_)
            ))
        ));
        assert!(applier.is_fenced());
        assert_eq!(frontier.snapshot(), CommitSeq::new(0));
        assert_eq!(
            statuses.status(TxnId::new(5)).expect("status"),
            Some(super::super::TransactionStatus::Committed(CommitSeq::new(
                1
            )))
        );
        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert_eq!(
            reader
                .lookup(&first, &buffer, b"good", None, frontier.snapshot())
                .expect("partial install stays invisible"),
            super::super::MvccLookup::NotFound
        );
        assert!(matches!(
            applier.apply(&transaction, &buffer, &[&first, &second]),
            Err(OrderedRecoveryError::Fenced)
        ));
    }
}
