//! Snapshot resolution for ordered-access-method MVCC records.
//!
//! Visibility is resolved from transaction status and complete undo records,
//! never from physical page/frame versions. Point and range reads share one
//! resolver so cursor filtering cannot diverge from point lookup semantics.

use super::{
    BTreeError, BTreeLookup, BTreeObject, BufferPool, CommitSeq, MvccCodecError, MvccRecord,
    MvccValue, RangeCursor, RecordVisibility, StatusTableError, TransactionStatusTable, TxnId,
    UndoStore, UndoStoreError, VersionId,
};

/// Logical result of one MVCC point lookup at a fixed snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MvccLookup {
    Found(Vec<u8>),
    Deleted,
    NotFound,
}

/// Snapshot-stable logical range cursor over ordered MVCC records.
///
/// The underlying B-tree cursor still restarts by logical key after every raw
/// batch. This wrapper fixes reader/snapshot identity and counts only visible
/// values toward caller limits; tombstones and versions absent at the snapshot
/// do not prematurely shorten a logical batch.
pub struct OrderedMvccRangeCursor {
    raw: RangeCursor,
    reader: Option<TxnId>,
    snapshot: CommitSeq,
}

impl OrderedMvccRangeCursor {
    /// Whether the underlying ordered range is exhausted.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.raw.is_done()
    }

    /// Return up to `limit` visible logical rows.
    ///
    /// Raw batches never exceed the number of still-needed visible rows. Since
    /// one raw slot can produce at most one visible row, the cursor cannot skip
    /// an unreturned visible row when it reaches the requested logical limit.
    pub fn next_batch(
        &mut self,
        resolver: &OrderedMvccReader<'_>,
        tree: &BTreeObject,
        buffer: &BufferPool,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OrderedMvccReadError> {
        if limit == 0 || self.raw.is_done() {
            return Ok(Vec::new());
        }

        let mut visible = Vec::with_capacity(limit);
        while visible.len() < limit && !self.raw.is_done() {
            let remaining = limit - visible.len();
            let rows = self.raw.next_batch(tree, buffer, remaining)?;
            if rows.is_empty() {
                break;
            }
            for (key, lookup) in rows {
                if let MvccLookup::Found(value) =
                    resolver.resolve_lookup(lookup, self.reader, self.snapshot)?
                {
                    visible.push((key, value));
                }
            }
        }
        Ok(visible)
    }
}

/// Ordered reader over the shared status and undo services.
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
        self.resolve_lookup(tree.lookup(buffer, key)?, reader, snapshot)
    }

    /// Create a resumable `[start, end)` cursor at one fixed logical snapshot.
    #[must_use]
    pub fn range_cursor(
        &self,
        tree: &BTreeObject,
        start: &[u8],
        end: &[u8],
        reader: Option<TxnId>,
        snapshot: CommitSeq,
    ) -> OrderedMvccRangeCursor {
        OrderedMvccRangeCursor {
            raw: tree.range_cursor(start, end),
            reader,
            snapshot,
        }
    }

    fn resolve_lookup(
        &self,
        lookup: BTreeLookup,
        reader: Option<TxnId>,
        snapshot: CommitSeq,
    ) -> Result<MvccLookup, OrderedMvccReadError> {
        let current = match lookup {
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
                            return Err(OrderedMvccReadError::NonDecreasingUndo { current, next });
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
        let descriptor =
            StorageObjectDescriptor::new(StorageObjectId::new(1), ObjectAuthority::Authoritative);
        let tree = BTreeObject::create(descriptor, &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let undo =
            UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier).expect("undo");
        (buffer, tree, directory, undo, TransactionStatusTable::new())
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
            &MvccRecord::installed(absent, 0, None, MvccValue::Inline(b"uncommitted".to_vec())),
        );

        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"with-history", None, CommitSeq::new(10))
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
    fn range_cursor_counts_visible_rows_instead_of_physical_slots() {
        let (buffer, tree, _directory, undo, statuses) = setup();
        seed(
            &tree,
            &buffer,
            b"k0",
            &MvccRecord::new(
                RecordOwner::Frozen(CommitSeq::new(2)),
                None,
                MvccValue::Inline(b"v0".to_vec()),
            ),
        );
        seed(
            &tree,
            &buffer,
            b"k1",
            &MvccRecord::new(
                RecordOwner::Frozen(CommitSeq::new(2)),
                None,
                MvccValue::Tombstone,
            ),
        );

        let active = TxnId::new(40);
        statuses.begin(active).expect("active writer");
        seed(
            &tree,
            &buffer,
            b"k2",
            &MvccRecord::installed(active, 0, None, MvccValue::Inline(b"hidden".to_vec())),
        );
        seed(
            &tree,
            &buffer,
            b"k3",
            &MvccRecord::new(
                RecordOwner::Frozen(CommitSeq::new(2)),
                None,
                MvccValue::Inline(b"v3".to_vec()),
            ),
        );

        let older = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(2)),
            None,
            MvccValue::Inline(b"old4".to_vec()),
        );
        let older_id = undo.append(&older).expect("older undo");
        let committed = TxnId::new(41);
        statuses
            .recover_committed(committed, CommitSeq::new(8))
            .expect("committed writer");
        seed(
            &tree,
            &buffer,
            b"k4",
            &MvccRecord::installed(
                committed,
                0,
                Some(older_id),
                MvccValue::Inline(b"new4".to_vec()),
            ),
        );
        seed(
            &tree,
            &buffer,
            b"k5",
            &MvccRecord::new(
                RecordOwner::Frozen(CommitSeq::new(2)),
                None,
                MvccValue::Inline(b"v5".to_vec()),
            ),
        );

        let reader = OrderedMvccReader::new(&statuses, &undo);
        let mut cursor = reader.range_cursor(&tree, b"k0", b"k9", None, CommitSeq::new(3));
        let first = cursor
            .next_batch(&reader, &tree, &buffer, 3)
            .expect("first visible batch");
        assert_eq!(
            first,
            vec![
                (b"k0".to_vec(), b"v0".to_vec()),
                (b"k3".to_vec(), b"v3".to_vec()),
                (b"k4".to_vec(), b"old4".to_vec()),
            ]
        );
        assert!(!cursor.is_done());

        let second = cursor
            .next_batch(&reader, &tree, &buffer, 3)
            .expect("second visible batch");
        assert_eq!(second, vec![(b"k5".to_vec(), b"v5".to_vec())]);
        assert!(cursor.is_done());
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
