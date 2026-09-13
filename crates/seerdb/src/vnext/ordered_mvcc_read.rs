//! Point snapshot resolution for ordered-access-method MVCC records.
//!
//! Visibility is resolved from transaction status and complete undo records,
//! never from physical page/frame versions. Range/cursor reads must reuse this
//! resolver rather than inventing a second visibility policy.

use super::{
    BTreeError, BTreeLookup, BTreeObject, BufferPool, CommitSeq, MvccCodecError, MvccRecord,
    MvccValue, RecordVisibility, StatusTableError, TransactionStatusTable, TxnId, UndoStore,
    UndoStoreError, VersionId,
};

/// Logical result of one MVCC point lookup at a fixed snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MvccLookup {
    Found(Vec<u8>),
    Deleted,
    NotFound,
}

/// Ordered point-reader over the shared status and undo services.
pub struct OrderedMvccReader<'a> {
    statuses: &'a TransactionStatusTable,
    undo: &'a UndoStore,
}

impl<'a> OrderedMvccReader<'a> {
    #[must_use]
    pub const fn new(statuses: &'a TransactionStatusTable, undo: &'a UndoStore) -> Self {
        Self { statuses, undo }
    }

    /// Resolve one logical key for `reader` at `snapshot`.
    ///
    /// The current B-tree slot is decoded as an `MvccRecord`. Invisible active,
    /// aborted, or newer committed owners follow the complete undo chain until
    /// a visible version or logical absence is reached. An active transaction
    /// sees its own installed current record through the shared status resolver.
    pub fn lookup(
        &self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        key: &[u8],
        reader: Option<TxnId>,
        snapshot: CommitSeq,
    ) -> Result<MvccLookup, OrderedMvccReadError> {
        let current = match tree.lookup(buffer, key)? {
            BTreeLookup::NotFound => return Ok(MvccLookup::NotFound),
            BTreeLookup::Found(bytes) => MvccRecord::from_bytes(&bytes)?,
            BTreeLookup::Blob(_) => return Err(OrderedMvccReadError::UnexpectedBlobCurrent),
            BTreeLookup::Deleted => {
                return Err(OrderedMvccReadError::UnexpectedRawTombstoneCurrent);
            }
        };
        self.resolve(current, reader, snapshot)
    }

    fn resolve(
        &self,
        mut record: MvccRecord,
        reader: Option<TxnId>,
        snapshot: CommitSeq,
    ) -> Result<MvccLookup, OrderedMvccReadError> {
        let mut containing_undo: Option<VersionId> = None;
        loop {
            match self.statuses.visibility(record.owner(), reader, snapshot)? {
                RecordVisibility::Visible => {
                    return Ok(match record.value() {
                        MvccValue::Inline(value) => MvccLookup::Found(value.clone()),
                        MvccValue::Tombstone => MvccLookup::Deleted,
                    });
                }
                RecordVisibility::Active
                | RecordVisibility::Aborted
                | RecordVisibility::NewerCommit(_) => {
                    let Some(next) = record.undo_head() else {
                        return Ok(MvccLookup::NotFound);
                    };
                    if let Some(current) = containing_undo {
                        if next.get() >= current.get() {
                            return Err(OrderedMvccReadError::NonDecreasingUndo {
                                current,
                                next,
                            });
                        }
                    }
                    record = self.undo.get(next)?;
                    containing_undo = Some(next);
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OrderedMvccReadError {
    #[error(transparent)]
    BTree(#[from] BTreeError),
    #[error(transparent)]
    Codec(#[from] MvccCodecError),
    #[error(transparent)]
    Status(#[from] StatusTableError),
    #[error(transparent)]
    Undo(#[from] UndoStoreError),
    #[error("MVCC undo chain does not strictly decrease: {current:?} -> {next:?}")]
    NonDecreasingUndo { current: VersionId, next: VersionId },
    #[error("ordered MVCC current slot unexpectedly contains a blob pointer")]
    UnexpectedBlobCurrent,
    #[error("ordered MVCC current slot unexpectedly contains a raw B-tree tombstone")]
    UnexpectedRawTombstoneCurrent,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{
        ObjectAuthority, PageIo, PageKey, RecordOwner, StorageObjectDescriptor, StorageObjectId,
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

    fn setup() -> (
        BufferPool,
        BTreeObject,
        tempfile::TempDir,
        UndoStore,
        TransactionStatusTable,
    ) {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let descriptor = StorageObjectDescriptor::new(
            StorageObjectId::new(1),
            ObjectAuthority::Authoritative,
        );
        let tree = BTreeObject::create(descriptor, &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo =
            UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier).expect("undo");
        (
            buffer,
            tree,
            directory,
            undo,
            TransactionStatusTable::new(),
        )
    }

    fn seed(tree: &BTreeObject, buffer: &BufferPool, key: &[u8], record: &MvccRecord) {
        tree.upsert(buffer, key, &record.to_bytes().expect("encode"))
            .expect("seed current");
    }

    #[test]
    fn visible_frozen_current_returns_value_and_tombstone() {
        let (buffer, tree, _directory, undo, statuses) = setup();
        let reader = OrderedMvccReader::new(&statuses, &undo);
        seed(
            &tree,
            &buffer,
            b"value",
            &MvccRecord::new(
                RecordOwner::Frozen(CommitSeq::new(4)),
                None,
                MvccValue::Inline(b"visible".to_vec()),
            ),
        );
        seed(
            &tree,
            &buffer,
            b"deleted",
            &MvccRecord::new(
                RecordOwner::Frozen(CommitSeq::new(4)),
                None,
                MvccValue::Tombstone,
            ),
        );

        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"value", None, CommitSeq::new(4))
                .expect("lookup"),
            MvccLookup::Found(b"visible".to_vec())
        );
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"deleted", None, CommitSeq::new(4))
                .expect("lookup"),
            MvccLookup::Deleted
        );
    }

    #[test]
    fn newer_committed_current_follows_undo_to_visible_history() {
        let (buffer, tree, _directory, undo, statuses) = setup();
        let older = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(4)),
            None,
            MvccValue::Inline(b"old".to_vec()),
        );
        let older_id = undo.append(&older).expect("undo append");
        statuses
            .recover_committed(TxnId::new(10), CommitSeq::new(8))
            .expect("status");
        seed(
            &tree,
            &buffer,
            b"key",
            &MvccRecord::installed(
                TxnId::new(10),
                0,
                Some(older_id),
                MvccValue::Inline(b"new".to_vec()),
            ),
        );

        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"key", None, CommitSeq::new(5))
                .expect("older snapshot"),
            MvccLookup::Found(b"old".to_vec())
        );
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"key", None, CommitSeq::new(8))
                .expect("new snapshot"),
            MvccLookup::Found(b"new".to_vec())
        );
    }

    #[test]
    fn other_active_reader_follows_undo_but_owner_sees_own_current() {
        let (buffer, tree, _directory, undo, statuses) = setup();
        let older = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(2)),
            None,
            MvccValue::Inline(b"old".to_vec()),
        );
        let older_id = undo.append(&older).expect("undo append");
        let owner = TxnId::new(20);
        statuses.begin(owner).expect("owner active");
        seed(
            &tree,
            &buffer,
            b"key",
            &MvccRecord::installed(
                owner,
                0,
                Some(older_id),
                MvccValue::Inline(b"mine".to_vec()),
            ),
        );

        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"key", None, CommitSeq::new(10))
                .expect("other reader"),
            MvccLookup::Found(b"old".to_vec())
        );
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"key", Some(owner), CommitSeq::new(2))
                .expect("own reader"),
            MvccLookup::Found(b"mine".to_vec())
        );
    }

    #[test]
    fn aborted_current_follows_undo_and_absent_predecessor_is_not_found() {
        let (buffer, tree, _directory, undo, statuses) = setup();
        let older = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(3)),
            None,
            MvccValue::Inline(b"prior".to_vec()),
        );
        let older_id = undo.append(&older).expect("undo append");
        let aborted = TxnId::new(30);
        statuses.begin(aborted).expect("active");
        statuses.abort(aborted).expect("aborted");
        seed(
            &tree,
            &buffer,
            b"with-history",
            &MvccRecord::installed(
                aborted,
                0,
                Some(older_id),
                MvccValue::Inline(b"aborted".to_vec()),
            ),
        );

        let absent = TxnId::new(31);
        statuses.begin(absent).expect("active");
        seed(
            &tree,
            &buffer,
            b"without-history",
            &MvccRecord::installed(
                absent,
                0,
                None,
                MvccValue::Inline(b"uncommitted".to_vec()),
            ),
        );

        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert_eq!(
            reader
                .lookup(
                    &tree,
                    &buffer,
                    b"with-history",
                    None,
                    CommitSeq::new(10),
                )
                .expect("aborted lookup"),
            MvccLookup::Found(b"prior".to_vec())
        );
        assert_eq!(
            reader
                .lookup(
                    &tree,
                    &buffer,
                    b"without-history",
                    None,
                    CommitSeq::new(10),
                )
                .expect("active lookup"),
            MvccLookup::NotFound
        );
    }

    #[test]
    fn multi_hop_chain_skips_newer_versions_until_snapshot_visible() {
        let (buffer, tree, _directory, undo, statuses) = setup();
        let oldest = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(2)),
            None,
            MvccValue::Inline(b"v2".to_vec()),
        );
        let oldest_id = undo.append(&oldest).expect("oldest");
        let middle = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(5)),
            Some(oldest_id),
            MvccValue::Inline(b"v5".to_vec()),
        );
        let middle_id = undo.append(&middle).expect("middle");
        seed(
            &tree,
            &buffer,
            b"key",
            &MvccRecord::new(
                RecordOwner::Frozen(CommitSeq::new(9)),
                Some(middle_id),
                MvccValue::Inline(b"v9".to_vec()),
            ),
        );

        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"key", None, CommitSeq::new(3))
                .expect("lookup"),
            MvccLookup::Found(b"v2".to_vec())
        );
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"key", None, CommitSeq::new(6))
                .expect("lookup"),
            MvccLookup::Found(b"v5".to_vec())
        );
    }

    #[test]
    fn unknown_transaction_owner_fails_closed() {
        let (buffer, tree, _directory, undo, statuses) = setup();
        seed(
            &tree,
            &buffer,
            b"key",
            &MvccRecord::installed(
                TxnId::new(99),
                0,
                None,
                MvccValue::Inline(b"unknown".to_vec()),
            ),
        );
        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert!(matches!(
            reader.lookup(&tree, &buffer, b"key", None, CommitSeq::new(100)),
            Err(OrderedMvccReadError::Status(StatusTableError::UnknownTxn(txn)))
                if txn == TxnId::new(99)
        ));
    }
}
