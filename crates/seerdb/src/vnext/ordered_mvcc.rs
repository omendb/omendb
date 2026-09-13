//! Ordered-access-method MVCC current-record installation for vNext.
//!
//! This is the first B-tree consumer of the shared transaction/status/undo
//! primitives. It deliberately does not own commit scheduling, status
//! publication, or page persistence. Callers must hold the addressed logical
//! write intent. Until page dependency capture and checkpoint publication land,
//! use this installer only with non-authoritative/transient page I/O; a dirty
//! page containing an undo reference is not yet safe to persist independently.

use super::{
    BTreeError, BTreeLookup, BTreeObject, BufferPool, CommitSeq, FinalEffect, MvccCodecError,
    MvccRecord, MvccValue, RecordOwner, StatusTableError, StorageObjectId, TransactionStatus,
    TransactionStatusTable, TxnId, UndoStore, UndoStoreError, VersionId, WriteIntentGuard,
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

    /// Install one canonical final effect into an ordered B-tree current slot.
    ///
    /// The caller must retain an intent guard covering this `(object,key)` for
    /// the complete read-before-image-replace sequence. A successful undo append
    /// is intentionally not synchronized here so a transaction can group the
    /// undo barrier across effects. If replacement fails after append, that undo
    /// entry is unreachable allocation/GC work; retry must not invent an abort.
    pub fn install(
        &self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        intents: &WriteIntentGuard<'_>,
        effect: &FinalEffect,
        context: InstallContext,
    ) -> Result<InstallEffectResult, OrderedMvccInstallError> {
        self.validate_call(tree, intents, effect, context)?;
        let intended = intended_value(effect);
        let identity = effect.install_identity();

        let (undo_head, appended_undo) = match tree.lookup(buffer, effect.key())? {
            BTreeLookup::NotFound => (None, None),
            BTreeLookup::Found(bytes) => {
                let current = MvccRecord::from_bytes(&bytes)?;
                if current.has_install_identity(identity) {
                    if current.value() == &intended {
                        return Ok(InstallEffectResult::AlreadyInstalled {
                            undo_head: current.undo_head(),
                        });
                    }
                    return Err(OrderedMvccInstallError::IdentityContentMismatch {
                        txn: effect.txn_id(),
                        ordinal: effect.ordinal(),
                    });
                }

                match current.owner() {
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
                }
            }
            BTreeLookup::Blob(_) => return Err(OrderedMvccInstallError::UnexpectedBlobCurrent),
            BTreeLookup::Deleted => {
                return Err(OrderedMvccInstallError::UnexpectedRawTombstoneCurrent);
            }
        };

        let current = MvccRecord::installed(
            effect.txn_id(),
            effect.ordinal(),
            undo_head,
            intended,
        );
        let encoded = current.to_bytes()?;
        tree.upsert(buffer, effect.key(), &encoded)?;
        Ok(InstallEffectResult::Installed {
            undo_head,
            appended_undo,
        })
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
        if intents.txn_id() != effect.txn_id()
            || !intents.owns(effect.object(), effect.key())
        {
            return Err(OrderedMvccInstallError::IntentNotHeld {
                txn: effect.txn_id(),
                object: effect.object(),
            });
        }

        let actual = self.statuses.status(effect.txn_id())?;
        match (context, actual) {
            (InstallContext::Live { .. }, Some(TransactionStatus::Active)) => Ok(()),
            (
                InstallContext::Recovery { commit },
                Some(TransactionStatus::Committed(actual)),
            ) if commit.get() != 0 && actual == commit => Ok(()),
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
    IntentNotHeld {
        txn: TxnId,
        object: StorageObjectId,
    },
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
    #[error("current record commit {predecessor:?} is newer than writer snapshot {snapshot:?}")]
    SnapshotConflict {
        predecessor: CommitSeq,
        snapshot: CommitSeq,
    },
    #[error(
        "recovery replay at {replay:?} encountered non-older current commit {predecessor:?}"
    )]
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
        LoggedMutation, ObjectAuthority, PageIo, StorageObjectDescriptor, WriteIntentTable,
        normalize_final_effects,
    };
    use durable_fs::SyncClass;
    use std::collections::HashMap;
    use std::io;
    use std::sync::{Arc, RwLock};

    #[derive(Default)]
    struct MemoryPageIo {
        pages: RwLock<HashMap<super::super::PageKey, Vec<u8>>>,
    }

    impl PageIo for MemoryPageIo {
        fn read_page(
            &self,
            key: super::super::PageKey,
            destination: &mut [u8],
        ) -> io::Result<()> {
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

        fn write_page(&self, key: super::super::PageKey, source: &[u8]) -> io::Result<()> {
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

    fn effect(txn: u64, object: u64, key: &[u8], value: Option<&[u8]>) -> FinalEffect {
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

    fn decode_current(
        tree: &BTreeObject,
        buffer: &BufferPool,
        key: &[u8],
    ) -> MvccRecord {
        let BTreeLookup::Found(bytes) = tree.lookup(buffer, key).expect("lookup") else {
            panic!("expected encoded current record");
        };
        MvccRecord::from_bytes(&bytes).expect("current decodes")
    }

    #[test]
    fn installs_absent_put_and_delete_with_final_effect_identity() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo = UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier)
            .expect("undo");
        let statuses = TransactionStatusTable::new();
        let intents = WriteIntentTable::new();
        let installer = OrderedMvccInstaller::new(&statuses, &undo);

        for (txn, key, value) in [(1, b"put".as_slice(), Some(b"value".as_slice())), (2, b"delete".as_slice(), None)] {
            statuses.begin(TxnId::new(txn)).expect("writer begins");
            let effect = effect(txn, 1, key, value);
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
            assert_eq!(
                current.value(),
                &value.map_or(MvccValue::Tombstone, |value| MvccValue::Inline(value.to_vec()))
            );
        }
    }

    #[test]
    fn replacement_appends_one_before_image_and_repeat_is_allocation_free() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let tree = BTreeObject::create(descriptor(2), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo = UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier)
            .expect("undo");
        let statuses = TransactionStatusTable::new();
        let intents = WriteIntentTable::new();
        let installer = OrderedMvccInstaller::new(&statuses, &undo);

        let prior = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(3)),
            None,
            MvccValue::Inline(b"old".to_vec()),
        );
        tree.insert(&buffer, b"key", &prior.to_bytes().expect("encode"))
            .expect("seed");
        statuses.begin(TxnId::new(7)).expect("writer begins");
        let effect = effect(7, 2, b"key", Some(b"new"));
        let effects = [effect.clone()];
        let guard = intents.try_acquire(TxnId::new(7), &effects).expect("intent");

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
            VersionId::new(1),
            "repeat install must not append a second predecessor"
        );
    }

    #[test]
    fn aborted_current_is_bypassed_without_becoming_history() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let tree = BTreeObject::create(descriptor(3), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo = UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier)
            .expect("undo");
        let statuses = TransactionStatusTable::new();
        let intents = WriteIntentTable::new();
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
        let effect = effect(5, 3, b"key", Some(b"replacement"));
        let effects = [effect.clone()];
        let guard = intents.try_acquire(TxnId::new(5), &effects).expect("intent");

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
        assert_eq!(decode_current(&tree, &buffer, b"key").undo_head(), Some(version));
        assert_eq!(
            undo.sync_through(version).expect("sync"),
            version,
            "aborted current must not be appended as history"
        );
    }

    #[test]
    fn active_and_newer_committed_predecessors_fail_before_undo_allocation() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let tree = BTreeObject::create(descriptor(4), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo = UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier)
            .expect("undo");
        let statuses = TransactionStatusTable::new();
        let intents = WriteIntentTable::new();
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
        let effect = effect(11, 4, b"key", Some(b"new"));
        let effects = [effect.clone()];
        let guard = intents.try_acquire(TxnId::new(11), &effects).expect("intent");
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
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let tree = BTreeObject::create(descriptor(5), &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo = UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier)
            .expect("undo");
        let statuses = TransactionStatusTable::new();
        let intents = WriteIntentTable::new();
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
        let effect = effect(20, 5, b"key", Some(b"replayed"));
        let effects = [effect.clone()];
        let guard = intents.try_acquire(TxnId::new(20), &effects).expect("intent");
        installer
            .install(
                &tree,
                &buffer,
                &guard,
                &effect,
                InstallContext::Recovery {
                    commit: CommitSeq::new(7),
                },
            )
            .expect("ordered recovery install");

        let newer = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(8)),
            None,
            MvccValue::Inline(b"newer".to_vec()),
        );
        tree.upsert(&buffer, b"other", &newer.to_bytes().expect("encode"))
            .expect("seed newer");
        statuses
            .recover_committed(TxnId::new(21), CommitSeq::new(7))
            .expect("second writer recovered");
        let effect = effect(21, 5, b"other", Some(b"bad-order"));
        let effects = [effect.clone()];
        let guard = intents.try_acquire(TxnId::new(21), &effects).expect("intent");
        assert!(matches!(
            installer.install(
                &tree,
                &buffer,
                &guard,
                &effect,
                InstallContext::Recovery {
                    commit: CommitSeq::new(7),
                },
            ),
            Err(OrderedMvccInstallError::ReplayOrderConflict { .. })
        ));
    }
}
