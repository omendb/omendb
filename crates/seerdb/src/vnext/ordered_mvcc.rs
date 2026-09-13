//! Ordered-access-method MVCC current-record installation for vNext.
//!
//! This is the first B-tree consumer of the shared transaction/status/undo
//! primitives. It deliberately does not own commit scheduling or status
//! publication. Callers must hold the addressed logical write intent. The
//! integrated runtime prepares predecessor/undo state for the whole transaction,
//! synchronizes the required undo frontier once, then applies the prepared page
//! mutations. The plain `install` entry point remains available for transient
//! standalone qualification.

use super::{
    BTreeError, BTreeLookup, BTreeObject, BufferPool, CommitSeq, FinalEffect, MvccCodecError,
    MvccRecord, MvccValue, PageDependencies, PageMaterialization, RecordOwner, StatusTableError,
    StorageObjectId, TransactionStatus, TransactionStatusTable, TxnId, UndoStore, UndoStoreError,
    VersionId, WriteIntentGuard,
};

/// Ordering context used to classify the predecessor occupying a current slot.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InstallContext {
    /// Normal commit installation while the writer remains `Active` in the
    /// process-local status table. A committed predecessor newer than the
    /// writer's snapshot is a write conflict.
    Live { snapshot: CommitSeq },
    /// Recovery replay of a transaction already proven committed at `commit`.
    /// Replay is expected to run in validated CSN order from one checkpoint.
    Recovery { commit: CommitSeq },
}

/// Result of installing one canonical final effect.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InstallEffectResult {
    /// A new current record was installed. `appended_undo` identifies a newly
    /// allocated before-image; `undo_head` is the predecessor linked by the new
    /// current record and may instead be inherited while bypassing an abort.
    Installed {
        undo_head: Option<VersionId>,
        appended_undo: Option<VersionId>,
    },
    /// The exact final effect was already retained in the current slot.
    AlreadyInstalled { undo_head: Option<VersionId> },
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum PreparedPredecessor {
    Absent,
    Present(MvccRecord),
}

/// One effect whose predecessor has been validated and whose required
/// before-image, if any, has already been appended to the undo store.
///
/// Preparation does not mutate B-tree pages. The intent guard must remain held
/// through application so the expected logical predecessor cannot be replaced
/// by another integrated writer between these phases.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PreparedOrderedMvccEffect {
    effect: FinalEffect,
    context: InstallContext,
    predecessor: PreparedPredecessor,
    current: MvccRecord,
    appended_undo: Option<VersionId>,
}

impl PreparedOrderedMvccEffect {
    /// Highest undo version the resulting current record references.
    #[must_use]
    pub const fn required_undo(&self) -> Option<VersionId> {
        self.current.undo_head()
    }

    /// Newly appended before-image allocated while preparing this effect.
    #[must_use]
    pub const fn appended_undo(&self) -> Option<VersionId> {
        self.appended_undo
    }

    #[must_use]
    pub const fn effect(&self) -> &FinalEffect {
        &self.effect
    }
}

/// Outcome of the non-page-mutating preparation phase.
///
/// The prepared plan is boxed so the common `AlreadyInstalled` retry outcome
/// does not carry an entire predecessor/value plan by value.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PrepareEffectResult {
    Prepared(Box<PreparedOrderedMvccEffect>),
    AlreadyInstalled { undo_head: Option<VersionId> },
}

impl PrepareEffectResult {
    /// Highest undo version that must be durable before this effect can be
    /// safely materialized or published as complete.
    #[must_use]
    pub const fn required_undo(&self) -> Option<VersionId> {
        match self {
            Self::Prepared(prepared) => prepared.required_undo(),
            Self::AlreadyInstalled { undo_head } => *undo_head,
        }
    }
}

/// Access-method-specific current-record installer over the shared vNext
/// status and undo services.
pub struct OrderedMvccInstaller<'a> {
    statuses: &'a TransactionStatusTable,
    undo: &'a UndoStore,
}

impl<'a> OrderedMvccInstaller<'a> {
    #[must_use]
    pub const fn new(statuses: &'a TransactionStatusTable, undo: &'a UndoStore) -> Self {
        Self { statuses, undo }
    }

    /// Prepare one canonical final effect without mutating shared B-tree pages.
    ///
    /// This validates the exact current predecessor and appends any required
    /// complete before-image. The caller may prepare every effect, issue one
    /// grouped undo durability barrier, and then apply the resulting plans.
    pub fn prepare(
        &self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        intents: &WriteIntentGuard<'_>,
        effect: &FinalEffect,
        context: InstallContext,
    ) -> Result<PrepareEffectResult, OrderedMvccInstallError> {
        self.validate_call(tree, intents, effect, context)?;
        let intended = intended_value(effect);
        let identity = effect.install_identity();

        let (predecessor, undo_head, appended_undo) = match tree.lookup(buffer, effect.key())? {
            BTreeLookup::NotFound => (PreparedPredecessor::Absent, None, None),
            BTreeLookup::Found(bytes) => {
                let current = MvccRecord::from_bytes(&bytes)?;
                if current.has_install_identity(identity) {
                    if current.value() == &intended {
                        return Ok(PrepareEffectResult::AlreadyInstalled {
                            undo_head: current.undo_head(),
                        });
                    }
                    return Err(OrderedMvccInstallError::IdentityContentMismatch {
                        txn: effect.txn_id(),
                        ordinal: effect.ordinal(),
                    });
                }

                let predecessor = PreparedPredecessor::Present(current.clone());
                let (undo_head, appended_undo) = match current.owner() {
                    RecordOwner::Transaction(owner) if owner == effect.txn_id() => {
                        return Err(OrderedMvccInstallError::DifferentEffectBySameTransaction {
                            txn: owner,
                        });
                    }
                    RecordOwner::Transaction(owner) => {
                        match self
                            .statuses
                            .status(owner)?
                            .ok_or(OrderedMvccInstallError::UnknownCurrentOwner(owner))?
                        {
                            TransactionStatus::Active => {
                                return Err(OrderedMvccInstallError::ActiveWriterConflict(owner));
                            }
                            TransactionStatus::Aborted => (current.undo_head(), None),
                            TransactionStatus::Committed(csn) => {
                                self.require_predecessor_order(csn, context)?;
                                let version = self.undo.append(&current)?;
                                (Some(version), Some(version))
                            }
                        }
                    }
                    RecordOwner::Frozen(csn) => {
                        self.require_predecessor_order(csn, context)?;
                        let version = self.undo.append(&current)?;
                        (Some(version), Some(version))
                    }
                };
                (predecessor, undo_head, appended_undo)
            }
            BTreeLookup::Blob(_) => return Err(OrderedMvccInstallError::UnexpectedBlobCurrent),
            BTreeLookup::Deleted => {
                return Err(OrderedMvccInstallError::UnexpectedRawTombstoneCurrent);
            }
        };

        Ok(PrepareEffectResult::Prepared(Box::new(
            PreparedOrderedMvccEffect {
                effect: effect.clone(),
                context,
                predecessor,
                current: MvccRecord::installed(
                    effect.txn_id(),
                    effect.ordinal(),
                    undo_head,
                    intended,
                ),
                appended_undo,
            },
        )))
    }

    /// Apply one prepared effect without page dependency attachment.
    pub fn apply_prepared(
        &self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        intents: &WriteIntentGuard<'_>,
        prepared: &PreparedOrderedMvccEffect,
    ) -> Result<InstallEffectResult, OrderedMvccInstallError> {
        self.apply_prepared_inner(tree, buffer, intents, prepared, None)
    }

    /// Apply one prepared effect and attach the exact durable WAL/undo
    /// requirements to every B-tree page image changed by the operation.
    pub fn apply_prepared_with_dependencies(
        &self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        intents: &WriteIntentGuard<'_>,
        prepared: &PreparedOrderedMvccEffect,
        materialization: PageMaterialization<'_>,
    ) -> Result<InstallEffectResult, OrderedMvccInstallError> {
        self.apply_prepared_inner(tree, buffer, intents, prepared, Some(materialization))
    }

    /// Convenience transient install. This preserves the original standalone
    /// behavior: preparation and page mutation happen in one call and undo is
    /// not synchronized here.
    pub fn install(
        &self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        intents: &WriteIntentGuard<'_>,
        effect: &FinalEffect,
        context: InstallContext,
    ) -> Result<InstallEffectResult, OrderedMvccInstallError> {
        match self.prepare(tree, buffer, intents, effect, context)? {
            PrepareEffectResult::AlreadyInstalled { undo_head } => {
                Ok(InstallEffectResult::AlreadyInstalled { undo_head })
            }
            PrepareEffectResult::Prepared(prepared) => {
                self.apply_prepared(tree, buffer, intents, &prepared)
            }
        }
    }

    /// Convenience dependency-aware install for isolated callers. Unlike the
    /// transient entry point, this synchronizes the referenced undo head before
    /// mutating the page and advances the supplied dependency frontier. The
    /// integrated transaction/recovery paths use explicit batch preparation to
    /// retain one grouped barrier across all effects.
    pub fn install_with_dependencies(
        &self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        intents: &WriteIntentGuard<'_>,
        effect: &FinalEffect,
        context: InstallContext,
        materialization: PageMaterialization<'_>,
    ) -> Result<InstallEffectResult, OrderedMvccInstallError> {
        match self.prepare(tree, buffer, intents, effect, context)? {
            PrepareEffectResult::AlreadyInstalled { undo_head } => {
                if let Some(version) = undo_head {
                    let durable = self.undo.sync_through(version)?;
                    materialization.dependencies().advance_undo(durable);
                }
                Ok(InstallEffectResult::AlreadyInstalled { undo_head })
            }
            PrepareEffectResult::Prepared(prepared) => {
                if let Some(version) = prepared.required_undo() {
                    let durable = self.undo.sync_through(version)?;
                    materialization.dependencies().advance_undo(durable);
                }
                self.apply_prepared_with_dependencies(
                    tree,
                    buffer,
                    intents,
                    &prepared,
                    materialization,
                )
            }
        }
    }

    fn apply_prepared_inner(
        &self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        intents: &WriteIntentGuard<'_>,
        prepared: &PreparedOrderedMvccEffect,
        dependency: Option<PageMaterialization<'_>>,
    ) -> Result<InstallEffectResult, OrderedMvccInstallError> {
        self.validate_call(tree, intents, &prepared.effect, prepared.context)?;

        if let Some(result) = self.validate_prepared_predecessor(tree, buffer, prepared)? {
            return Ok(result);
        }

        let encoded = prepared.current.to_bytes()?;
        if let Some(materialization) = dependency {
            tree.upsert_with_dependencies(
                buffer,
                prepared.effect.key(),
                &encoded,
                materialization.dependencies(),
                PageDependencies::new(materialization.required_wal(), prepared.current.undo_head()),
            )?;
        } else {
            tree.upsert(buffer, prepared.effect.key(), &encoded)?;
        }
        Ok(InstallEffectResult::Installed {
            undo_head: prepared.current.undo_head(),
            appended_undo: prepared.appended_undo,
        })
    }

    fn validate_prepared_predecessor(
        &self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        prepared: &PreparedOrderedMvccEffect,
    ) -> Result<Option<InstallEffectResult>, OrderedMvccInstallError> {
        match tree.lookup(buffer, prepared.effect.key())? {
            BTreeLookup::NotFound => {
                if prepared.predecessor == PreparedPredecessor::Absent {
                    Ok(None)
                } else {
                    Err(self.predecessor_changed(prepared))
                }
            }
            BTreeLookup::Found(bytes) => {
                let current = MvccRecord::from_bytes(&bytes)?;
                if current.has_install_identity(prepared.effect.install_identity()) {
                    if current.value() == prepared.current.value() {
                        return Ok(Some(InstallEffectResult::AlreadyInstalled {
                            undo_head: current.undo_head(),
                        }));
                    }
                    return Err(OrderedMvccInstallError::IdentityContentMismatch {
                        txn: prepared.effect.txn_id(),
                        ordinal: prepared.effect.ordinal(),
                    });
                }
                match &prepared.predecessor {
                    PreparedPredecessor::Present(expected) if expected == &current => Ok(None),
                    _ => Err(self.predecessor_changed(prepared)),
                }
            }
            BTreeLookup::Blob(_) => Err(OrderedMvccInstallError::UnexpectedBlobCurrent),
            BTreeLookup::Deleted => Err(OrderedMvccInstallError::UnexpectedRawTombstoneCurrent),
        }
    }

    fn predecessor_changed(&self, prepared: &PreparedOrderedMvccEffect) -> OrderedMvccInstallError {
        OrderedMvccInstallError::PreparedPredecessorChanged {
            txn: prepared.effect.txn_id(),
            object: prepared.effect.object(),
        }
    }

    fn validate_call(
        &self,
        tree: &BTreeObject,
        intents: &WriteIntentGuard<'_>,
        effect: &FinalEffect,
        context: InstallContext,
    ) -> Result<(), OrderedMvccInstallError> {
        if tree.descriptor().id() != effect.object() {
            return Err(OrderedMvccInstallError::ObjectMismatch {
                tree: tree.descriptor().id(),
                effect: effect.object(),
            });
        }
        if intents.txn_id() != effect.txn_id() || !intents.owns(effect.object(), effect.key()) {
            return Err(OrderedMvccInstallError::IntentNotHeld {
                txn: effect.txn_id(),
                object: effect.object(),
            });
        }

        let actual = self.statuses.status(effect.txn_id())?;
        match (context, actual) {
            (InstallContext::Live { .. }, Some(TransactionStatus::Active)) => Ok(()),
            (InstallContext::Recovery { commit }, Some(TransactionStatus::Committed(actual)))
                if commit.get() != 0 && actual == commit =>
            {
                Ok(())
            }
            (InstallContext::Recovery { commit }, _) if commit.get() == 0 => {
                Err(OrderedMvccInstallError::ReservedCommitSeq)
            }
            (_, actual) => Err(OrderedMvccInstallError::WriterStatus {
                txn: effect.txn_id(),
                actual,
            }),
        }
    }

    fn require_predecessor_order(
        &self,
        predecessor: CommitSeq,
        context: InstallContext,
    ) -> Result<(), OrderedMvccInstallError> {
        match context {
            InstallContext::Live { snapshot } if predecessor > snapshot => {
                Err(OrderedMvccInstallError::SnapshotConflict {
                    predecessor,
                    snapshot,
                })
            }
            InstallContext::Recovery { commit } if predecessor >= commit => {
                Err(OrderedMvccInstallError::ReplayOrderConflict {
                    predecessor,
                    replay: commit,
                })
            }
            _ => Ok(()),
        }
    }
}

fn intended_value(effect: &FinalEffect) -> MvccValue {
    match effect.kind() {
        super::MutationKind::OrderedPut => MvccValue::Inline(effect.value().to_vec()),
        super::MutationKind::OrderedDelete => MvccValue::Tombstone,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OrderedMvccInstallError {
    #[error(transparent)]
    BTree(#[from] BTreeError),
    #[error(transparent)]
    Codec(#[from] MvccCodecError),
    #[error(transparent)]
    Status(#[from] StatusTableError),
    #[error(transparent)]
    Undo(#[from] UndoStoreError),
    #[error("ordered MVCC tree {tree:?} does not match final effect object {effect:?}")]
    ObjectMismatch {
        tree: StorageObjectId,
        effect: StorageObjectId,
    },
    #[error("transaction {txn:?} does not hold the required write intent for object {object:?}")]
    IntentNotHeld { txn: TxnId, object: StorageObjectId },
    #[error("transaction {txn:?} has invalid installer status {actual:?}")]
    WriterStatus {
        txn: TxnId,
        actual: Option<TransactionStatus>,
    },
    #[error("recovery install uses reserved commit sequence zero")]
    ReservedCommitSeq,
    #[error("current record belongs to active transaction {0:?}")]
    ActiveWriterConflict(TxnId),
    #[error("current record belongs to transaction {0:?} with no recovered status")]
    UnknownCurrentOwner(TxnId),
    #[error("transaction {txn:?} already occupies the current slot with a different final effect")]
    DifferentEffectBySameTransaction { txn: TxnId },
    #[error(
        "install identity ({txn:?}, {ordinal}) matches the current record but its logical value differs"
    )]
    IdentityContentMismatch { txn: TxnId, ordinal: u32 },
    #[error(
        "prepared predecessor changed before transaction {txn:?} could install object {object:?}"
    )]
    PreparedPredecessorChanged { txn: TxnId, object: StorageObjectId },
    #[error("current record commit {predecessor:?} is newer than writer snapshot {snapshot:?}")]
    SnapshotConflict {
        predecessor: CommitSeq,
        snapshot: CommitSeq,
    },
    #[error("recovery replay at {replay:?} encountered non-older current commit {predecessor:?}")]
    ReplayOrderConflict {
        predecessor: CommitSeq,
        replay: CommitSeq,
    },
    #[error("ordered MVCC current slot unexpectedly contains a blob pointer")]
    UnexpectedBlobCurrent,
    #[error("ordered MVCC current slot unexpectedly contains a raw B-tree tombstone")]
    UnexpectedRawTombstoneCurrent,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{
        LoggedMutation, Lsn, ObjectAuthority, PageDependencyTable, PageId, PageIo, PageKey,
        StorageObjectDescriptor, WriteIntentTable, normalize_final_effects,
    };
    use durable_fs::SyncClass;
    use std::collections::HashMap;
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

    fn make_effect(txn: u64, object: u64, key: &[u8], value: Option<&[u8]>) -> FinalEffect {
        let mutation = match value {
            Some(value) => LoggedMutation::ordered_put(
                TxnId::new(txn),
                0,
                StorageObjectId::new(object),
                key.to_vec(),
                value.to_vec(),
            ),
            None => LoggedMutation::ordered_delete(
                TxnId::new(txn),
                0,
                StorageObjectId::new(object),
                key.to_vec(),
            ),
        };
        normalize_final_effects(TxnId::new(txn), &[mutation])
            .expect("normalizes")
            .pop()
            .expect("one effect")
    }

    fn decode_current(tree: &BTreeObject, buffer: &BufferPool, key: &[u8]) -> MvccRecord {
        let BTreeLookup::Found(bytes) = tree.lookup(buffer, key).expect("lookup") else {
            panic!("expected encoded current record");
        };
        MvccRecord::from_bytes(&bytes).expect("current decodes")
    }

    fn setup(
        object: u64,
    ) -> (
        BufferPool,
        BTreeObject,
        tempfile::TempDir,
        UndoStore,
        TransactionStatusTable,
        WriteIntentTable,
    ) {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let tree = BTreeObject::create(descriptor(object), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo =
            UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier).expect("undo");
        (
            buffer,
            tree,
            directory,
            undo,
            TransactionStatusTable::new(),
            WriteIntentTable::new(),
        )
    }

    #[test]
    fn installs_absent_put_and_delete_with_final_effect_identity() {
        let (buffer, tree, _directory, undo, statuses, intents) = setup(1);
        let installer = OrderedMvccInstaller::new(&statuses, &undo);

        for (txn, key, value) in [
            (1, b"put".as_slice(), Some(b"value".as_slice())),
            (2, b"delete".as_slice(), None),
        ] {
            statuses.begin(TxnId::new(txn)).expect("writer begins");
            let effect = make_effect(txn, 1, key, value);
            let effects = [effect.clone()];
            let guard = intents
                .try_acquire(TxnId::new(txn), &effects)
                .expect("intent");
            assert_eq!(
                installer
                    .install(
                        &tree,
                        &buffer,
                        &guard,
                        &effect,
                        InstallContext::Live {
                            snapshot: CommitSeq::new(0),
                        },
                    )
                    .expect("installs"),
                InstallEffectResult::Installed {
                    undo_head: None,
                    appended_undo: None,
                }
            );
            let current = decode_current(&tree, &buffer, key);
            assert_eq!(current.install_identity(), Some(effect.install_identity()));
            let expected = value.map_or(MvccValue::Tombstone, |bytes| {
                MvccValue::Inline(bytes.to_vec())
            });
            assert_eq!(current.value(), &expected);
        }
    }

    #[test]
    fn preparation_allocates_history_without_mutating_shared_page() {
        let (buffer, tree, _directory, undo, statuses, intents) = setup(7);
        let installer = OrderedMvccInstaller::new(&statuses, &undo);
        let prior = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(4)),
            None,
            MvccValue::Inline(b"old".to_vec()),
        );
        tree.insert(&buffer, b"key", &prior.to_bytes().expect("encode"))
            .expect("seed");
        statuses.begin(TxnId::new(70)).expect("writer begins");
        let effect = make_effect(70, 7, b"key", Some(b"new"));
        let effects = [effect.clone()];
        let guard = intents
            .try_acquire(TxnId::new(70), &effects)
            .expect("intent");

        let PrepareEffectResult::Prepared(prepared) = installer
            .prepare(
                &tree,
                &buffer,
                &guard,
                &effect,
                InstallContext::Live {
                    snapshot: CommitSeq::new(4),
                },
            )
            .expect("prepares")
        else {
            panic!("effect should require installation");
        };
        assert_eq!(prepared.required_undo(), Some(VersionId::new(1)));
        assert_eq!(prepared.appended_undo(), Some(VersionId::new(1)));
        assert_eq!(decode_current(&tree, &buffer, b"key"), prior);
        assert_eq!(undo.durable_version(), None);

        undo.sync_through(VersionId::new(1)).expect("undo syncs");
        assert_eq!(
            installer
                .apply_prepared(&tree, &buffer, &guard, &prepared)
                .expect("prepared effect applies"),
            InstallEffectResult::Installed {
                undo_head: Some(VersionId::new(1)),
                appended_undo: Some(VersionId::new(1)),
            }
        );
        assert_eq!(
            decode_current(&tree, &buffer, b"key").value(),
            &MvccValue::Inline(b"new".to_vec())
        );
    }

    #[test]
    fn dependency_aware_install_attaches_decision_and_actual_undo_head() {
        let (buffer, tree, _directory, undo, statuses, intents) = setup(6);
        let installer = OrderedMvccInstaller::new(&statuses, &undo);
        let prior = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(2)),
            None,
            MvccValue::Inline(b"old".to_vec()),
        );
        tree.insert(&buffer, b"key", &prior.to_bytes().expect("encode"))
            .expect("seed");
        statuses.begin(TxnId::new(60)).expect("writer begins");
        let effect = make_effect(60, 6, b"key", Some(b"new"));
        let effects = [effect.clone()];
        let guard = intents
            .try_acquire(TxnId::new(60), &effects)
            .expect("intent");
        let dependencies = PageDependencyTable::new();
        let wal = Lsn::new(500);

        let result = installer
            .install_with_dependencies(
                &tree,
                &buffer,
                &guard,
                &effect,
                InstallContext::Live {
                    snapshot: CommitSeq::new(2),
                },
                PageMaterialization::new(&dependencies, wal),
            )
            .expect("tracked install");
        assert_eq!(
            result,
            InstallEffectResult::Installed {
                undo_head: Some(VersionId::new(1)),
                appended_undo: Some(VersionId::new(1)),
            }
        );
        assert_eq!(dependencies.durable_undo(), Some(VersionId::new(1)));
        assert_eq!(
            dependencies
                .requirements(PageKey::new(tree.descriptor().id(), PageId::new(0)))
                .expect("page requirements"),
            PageDependencies::new(wal, Some(VersionId::new(1)))
        );
    }

    #[test]
    fn replacement_appends_one_before_image_and_repeat_is_allocation_free() {
        let (buffer, tree, _directory, undo, statuses, intents) = setup(2);
        let installer = OrderedMvccInstaller::new(&statuses, &undo);
        let prior = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(3)),
            None,
            MvccValue::Inline(b"old".to_vec()),
        );
        tree.insert(&buffer, b"key", &prior.to_bytes().expect("encode"))
            .expect("seed");
        statuses.begin(TxnId::new(7)).expect("writer begins");
        let effect = make_effect(7, 2, b"key", Some(b"new"));
        let effects = [effect.clone()];
        let guard = intents
            .try_acquire(TxnId::new(7), &effects)
            .expect("intent");

        assert_eq!(
            installer
                .install(
                    &tree,
                    &buffer,
                    &guard,
                    &effect,
                    InstallContext::Live {
                        snapshot: CommitSeq::new(3),
                    },
                )
                .expect("first install"),
            InstallEffectResult::Installed {
                undo_head: Some(VersionId::new(1)),
                appended_undo: Some(VersionId::new(1)),
            }
        );
        assert_eq!(undo.get(VersionId::new(1)).expect("undo reads"), prior);
        assert_eq!(
            installer
                .install(
                    &tree,
                    &buffer,
                    &guard,
                    &effect,
                    InstallContext::Live {
                        snapshot: CommitSeq::new(3),
                    },
                )
                .expect("repeat install"),
            InstallEffectResult::AlreadyInstalled {
                undo_head: Some(VersionId::new(1)),
            }
        );
        assert_eq!(
            undo.sync_through(VersionId::new(1)).expect("sync"),
            VersionId::new(1)
        );
    }

    #[test]
    fn aborted_current_is_bypassed_without_becoming_history() {
        let (buffer, tree, _directory, undo, statuses, intents) = setup(3);
        let installer = OrderedMvccInstaller::new(&statuses, &undo);
        let prior = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(2)),
            None,
            MvccValue::Inline(b"prior".to_vec()),
        );
        let version = undo.append(&prior).expect("prior undo");
        let aborted = MvccRecord::installed(
            TxnId::new(4),
            0,
            Some(version),
            MvccValue::Inline(b"aborted".to_vec()),
        );
        tree.insert(&buffer, b"key", &aborted.to_bytes().expect("encode"))
            .expect("seed");
        statuses.begin(TxnId::new(4)).expect("aborted begins");
        statuses.abort(TxnId::new(4)).expect("aborts");
        statuses.begin(TxnId::new(5)).expect("writer begins");
        let effect = make_effect(5, 3, b"key", Some(b"replacement"));
        let effects = [effect.clone()];
        let guard = intents
            .try_acquire(TxnId::new(5), &effects)
            .expect("intent");

        assert_eq!(
            installer
                .install(
                    &tree,
                    &buffer,
                    &guard,
                    &effect,
                    InstallContext::Live {
                        snapshot: CommitSeq::new(2),
                    },
                )
                .expect("install"),
            InstallEffectResult::Installed {
                undo_head: Some(version),
                appended_undo: None,
            }
        );
        assert_eq!(
            decode_current(&tree, &buffer, b"key").undo_head(),
            Some(version)
        );
        assert_eq!(undo.sync_through(version).expect("sync"), version);
    }

    #[test]
    fn active_and_newer_committed_predecessors_fail_before_undo_allocation() {
        let (buffer, tree, _directory, undo, statuses, intents) = setup(4);
        let installer = OrderedMvccInstaller::new(&statuses, &undo);
        statuses.begin(TxnId::new(10)).expect("owner begins");
        let active = MvccRecord::installed(
            TxnId::new(10),
            0,
            None,
            MvccValue::Inline(b"active".to_vec()),
        );
        tree.insert(&buffer, b"key", &active.to_bytes().expect("encode"))
            .expect("seed");
        statuses.begin(TxnId::new(11)).expect("writer begins");
        let effect = make_effect(11, 4, b"key", Some(b"new"));
        let effects = [effect.clone()];
        let guard = intents
            .try_acquire(TxnId::new(11), &effects)
            .expect("intent");
        assert!(matches!(
            installer.install(
                &tree,
                &buffer,
                &guard,
                &effect,
                InstallContext::Live {
                    snapshot: CommitSeq::new(5),
                },
            ),
            Err(OrderedMvccInstallError::ActiveWriterConflict(txn)) if txn == TxnId::new(10)
        ));

        statuses.abort(TxnId::new(10)).expect("owner aborts");
        let committed = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(9)),
            None,
            MvccValue::Inline(b"newer".to_vec()),
        );
        tree.upsert(&buffer, b"key", &committed.to_bytes().expect("encode"))
            .expect("replace seed");
        assert!(matches!(
            installer.install(
                &tree,
                &buffer,
                &guard,
                &effect,
                InstallContext::Live {
                    snapshot: CommitSeq::new(5),
                },
            ),
            Err(OrderedMvccInstallError::SnapshotConflict {
                predecessor,
                snapshot,
            }) if predecessor == CommitSeq::new(9) && snapshot == CommitSeq::new(5)
        ));
        assert_eq!(undo.durable_version(), None);
    }

    #[test]
    fn recovery_requires_matching_writer_status_and_strict_commit_order() {
        let (buffer, tree, _directory, undo, statuses, intents) = setup(5);
        let installer = OrderedMvccInstaller::new(&statuses, &undo);
        let prior = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(6)),
            None,
            MvccValue::Inline(b"prior".to_vec()),
        );
        tree.insert(&buffer, b"key", &prior.to_bytes().expect("encode"))
            .expect("seed");
        statuses
            .recover_committed(TxnId::new(20), CommitSeq::new(7))
            .expect("writer recovered");
        let first_effect = make_effect(20, 5, b"key", Some(b"replayed"));
        let first_effects = [first_effect.clone()];
        let first_guard = intents
            .try_acquire(TxnId::new(20), &first_effects)
            .expect("intent");
        installer
            .install(
                &tree,
                &buffer,
                &first_guard,
                &first_effect,
                InstallContext::Recovery {
                    commit: CommitSeq::new(7),
                },
            )
            .expect("ordered recovery install");
        drop(first_guard);

        let newer = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(9)),
            None,
            MvccValue::Inline(b"newer".to_vec()),
        );
        tree.upsert(&buffer, b"other", &newer.to_bytes().expect("encode"))
            .expect("seed newer");
        statuses
            .recover_committed(TxnId::new(21), CommitSeq::new(9))
            .expect("second writer recovered");
        let second_effect = make_effect(21, 5, b"other", Some(b"bad-order"));
        let second_effects = [second_effect.clone()];
        let second_guard = intents
            .try_acquire(TxnId::new(21), &second_effects)
            .expect("intent");
        assert!(matches!(
            installer.install(
                &tree,
                &buffer,
                &second_guard,
                &second_effect,
                InstallContext::Recovery {
                    commit: CommitSeq::new(8),
                },
            ),
            Err(OrderedMvccInstallError::WriterStatus { .. })
        ));
        assert!(matches!(
            installer.install(
                &tree,
                &buffer,
                &second_guard,
                &second_effect,
                InstallContext::Recovery {
                    commit: CommitSeq::new(9),
                },
            ),
            Err(OrderedMvccInstallError::ReplayOrderConflict { .. })
        ));
    }
}
