//! Ordered MVCC reads with a transaction-private staged overlay.

use super::{
    BTreeObject, BufferPool, MutationKind, MvccLookup, OrderedMvccRangeCursor,
    OrderedMvccReadError, OrderedMvccReader, StagedOrderedLookup, Transaction,
};
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Clone, Eq, PartialEq)]
enum PrivateRangeValue {
    Put(Vec<u8>),
    Delete,
}

/// Resumable logical range cursor merging one transaction's captured private
/// staged view with its fixed shared MVCC snapshot.
pub struct OrderedTransactionRangeCursor {
    base: OrderedMvccRangeCursor,
    staged: Vec<(Vec<u8>, PrivateRangeValue)>,
    staged_index: usize,
    base_pending: VecDeque<(Vec<u8>, Vec<u8>)>,
}

impl OrderedTransactionRangeCursor {
    /// Whether both the shared snapshot and captured private overlay are
    /// exhausted.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.base.is_done()
            && self.base_pending.is_empty()
            && self.staged_index >= self.staged.len()
    }

    /// Return up to `limit` visible rows from the merged transaction view.
    pub fn next_batch(
        &mut self,
        resolver: &OrderedMvccReader<'_>,
        tree: &BTreeObject,
        buffer: &BufferPool,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OrderedMvccReadError> {
        if limit == 0 || self.is_done() {
            return Ok(Vec::new());
        }

        let mut rows = Vec::with_capacity(limit);
        while rows.len() < limit {
            if self.base_pending.is_empty() && !self.base.is_done() {
                let remaining = limit - rows.len();
                let prefetch = remaining.max(16);
                self.base_pending
                    .extend(self.base.next_batch(resolver, tree, buffer, prefetch)?);
            }

            let base_key = self.base_pending.front().map(|(key, _)| key.as_slice());
            let staged = self.staged.get(self.staged_index);
            let staged_key = staged.map(|(key, _)| key.as_slice());

            match (base_key, staged_key) {
                (None, None) => break,
                (Some(_), None) => {
                    if let Some(row) = self.base_pending.pop_front() {
                        rows.push(row);
                    }
                }
                (None, Some(_)) => self.consume_staged(&mut rows),
                (Some(base), Some(private)) if base < private => {
                    if let Some(row) = self.base_pending.pop_front() {
                        rows.push(row);
                    }
                }
                (Some(base), Some(private)) if base == private => {
                    self.base_pending.pop_front();
                    self.consume_staged(&mut rows);
                }
                (Some(_), Some(_)) => self.consume_staged(&mut rows),
            }
        }
        Ok(rows)
    }

    fn consume_staged(&mut self, output: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        let Some((key, value)) = self.staged.get(self.staged_index) else {
            return;
        };
        if let PrivateRangeValue::Put(value) = value {
            output.push((key.clone(), value.clone()));
        }
        self.staged_index += 1;
    }
}

impl OrderedMvccReader<'_> {
    /// Read one ordered key from a transaction's private staged view, falling
    /// back to the transaction's fixed snapshot only when the key is unchanged.
    pub fn lookup_transaction(
        &self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        transaction: &Transaction,
        key: &[u8],
    ) -> Result<MvccLookup, OrderedMvccReadError> {
        match transaction.staged_ordered_lookup(tree.descriptor().id(), key) {
            StagedOrderedLookup::Put(value) => Ok(MvccLookup::Found(value.to_vec())),
            StagedOrderedLookup::Delete => Ok(MvccLookup::Deleted),
            StagedOrderedLookup::Unchanged => self.lookup(
                tree,
                buffer,
                key,
                Some(transaction.id()),
                transaction.snapshot(),
            ),
        }
    }

    /// Create a transaction range cursor. The private staged overlay is captured
    /// at cursor creation, giving one stable statement-like view while the base
    /// side remains fixed at the transaction's original snapshot.
    #[must_use]
    pub fn range_cursor_transaction(
        &self,
        tree: &BTreeObject,
        transaction: &Transaction,
        start: &[u8],
        end: &[u8],
    ) -> OrderedTransactionRangeCursor {
        let object = tree.descriptor().id();
        let mut staged = BTreeMap::new();
        for mutation in transaction.mutations() {
            if mutation.object() != object || mutation.key() < start || mutation.key() >= end {
                continue;
            }
            let value = match mutation.kind() {
                MutationKind::OrderedPut => PrivateRangeValue::Put(mutation.value().to_vec()),
                MutationKind::OrderedDelete => PrivateRangeValue::Delete,
            };
            staged.insert(mutation.key().to_vec(), value);
        }
        OrderedTransactionRangeCursor {
            base: self.range_cursor(
                tree,
                start,
                end,
                Some(transaction.id()),
                transaction.snapshot(),
            ),
            staged: staged.into_iter().collect(),
            staged_index: 0,
            base_pending: VecDeque::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::format::CommitSeq;
    use crate::vnext::{
        MvccRecord, MvccValue, ObjectAuthority, PageIo, PageKey, RecordOwner,
        StorageObjectDescriptor, StorageObjectId, TransactionStatusTable, TxnId, UndoStore,
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

    fn seed(tree: &BTreeObject, buffer: &BufferPool, key: &[u8], value: &[u8], csn: u64) {
        let record = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(csn)),
            None,
            MvccValue::Inline(value.to_vec()),
        );
        tree.insert(buffer, key, &record.to_bytes().expect("encode"))
            .expect("seed");
    }

    #[test]
    fn staged_put_and_delete_override_shared_snapshot_without_installing() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        seed(&tree, &buffer, b"existing", b"base", 1);

        let directory = tempfile::tempdir().expect("tempdir");
        let undo =
            UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier).expect("undo");
        let statuses = TransactionStatusTable::new();
        let reader = OrderedMvccReader::new(&statuses, &undo);
        let mut transaction = Transaction::new(TxnId::new(7), CommitSeq::new(1));
        transaction
            .stage_ordered_put(descriptor(1), b"existing".to_vec(), b"private".to_vec())
            .expect("private update");
        transaction
            .stage_ordered_put(descriptor(1), b"new".to_vec(), b"inserted".to_vec())
            .expect("private insert");
        transaction
            .stage_ordered_delete(descriptor(1), b"gone".to_vec())
            .expect("private delete");

        assert_eq!(
            reader
                .lookup_transaction(&tree, &buffer, &transaction, b"existing")
                .expect("private update reads"),
            MvccLookup::Found(b"private".to_vec())
        );
        assert_eq!(
            reader
                .lookup_transaction(&tree, &buffer, &transaction, b"new")
                .expect("private insert reads"),
            MvccLookup::Found(b"inserted".to_vec())
        );
        assert_eq!(
            reader
                .lookup_transaction(&tree, &buffer, &transaction, b"gone")
                .expect("private delete reads"),
            MvccLookup::Deleted
        );
        assert_eq!(
            tree.lookup(&buffer, b"new").expect("shared tree lookup"),
            super::super::BTreeLookup::NotFound,
            "private staged insert must not be installed into shared pages"
        );
    }

    #[test]
    fn unchanged_key_falls_back_to_fixed_transaction_snapshot() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let older = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(1)),
            None,
            MvccValue::Inline(b"old".to_vec()),
        );
        let directory = tempfile::tempdir().expect("tempdir");
        let undo =
            UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier).expect("undo");
        let older_id = undo.append(&older).expect("older undo");
        let writer = TxnId::new(9);
        let statuses = TransactionStatusTable::new();
        statuses
            .recover_committed(writer, CommitSeq::new(2))
            .expect("newer status");
        let current = MvccRecord::installed(
            writer,
            0,
            Some(older_id),
            MvccValue::Inline(b"new".to_vec()),
        );
        tree.insert(&buffer, b"key", &current.to_bytes().expect("encode"))
            .expect("seed current");

        let transaction = Transaction::new(TxnId::new(10), CommitSeq::new(1));
        let reader = OrderedMvccReader::new(&statuses, &undo);
        assert_eq!(
            reader
                .lookup_transaction(&tree, &buffer, &transaction, b"key")
                .expect("snapshot read"),
            MvccLookup::Found(b"old".to_vec())
        );
    }

    #[test]
    fn transaction_range_merges_private_inserts_updates_and_deletes_in_key_order() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        seed(&tree, &buffer, b"k1", b"v1", 1);
        seed(&tree, &buffer, b"k3", b"v3", 1);
        seed(&tree, &buffer, b"k5", b"v5", 1);

        let directory = tempfile::tempdir().expect("tempdir");
        let undo =
            UndoStore::open(directory.path().join("undo"), SyncClass::KernelBarrier).expect("undo");
        let statuses = TransactionStatusTable::new();
        let reader = OrderedMvccReader::new(&statuses, &undo);
        let mut transaction = Transaction::new(TxnId::new(12), CommitSeq::new(1));
        transaction
            .stage_ordered_put(descriptor(1), b"k0".to_vec(), b"p0".to_vec())
            .expect("insert k0");
        transaction
            .stage_ordered_delete(descriptor(1), b"k1".to_vec())
            .expect("delete k1");
        transaction
            .stage_ordered_put(descriptor(1), b"k3".to_vec(), b"p3".to_vec())
            .expect("replace k3");
        transaction
            .stage_ordered_put(descriptor(1), b"k4".to_vec(), b"first4".to_vec())
            .expect("first k4");
        transaction
            .stage_ordered_put(descriptor(1), b"k4".to_vec(), b"p4".to_vec())
            .expect("final k4");
        transaction
            .stage_ordered_put(descriptor(1), b"k6".to_vec(), b"removed".to_vec())
            .expect("insert k6");
        transaction
            .stage_ordered_delete(descriptor(1), b"k6".to_vec())
            .expect("delete k6");

        let mut cursor = reader.range_cursor_transaction(&tree, &transaction, b"k0", b"k9");
        assert_eq!(
            cursor
                .next_batch(&reader, &tree, &buffer, 2)
                .expect("first batch"),
            vec![
                (b"k0".to_vec(), b"p0".to_vec()),
                (b"k3".to_vec(), b"p3".to_vec()),
            ]
        );
        assert_eq!(
            cursor
                .next_batch(&reader, &tree, &buffer, 2)
                .expect("second batch"),
            vec![
                (b"k4".to_vec(), b"p4".to_vec()),
                (b"k5".to_vec(), b"v5".to_vec()),
            ]
        );
        assert!(
            cursor
                .next_batch(&reader, &tree, &buffer, 2)
                .expect("exhausted batch")
                .is_empty()
        );
        assert!(cursor.is_done());
        assert_eq!(
            tree.lookup(&buffer, b"k0").expect("shared insert check"),
            super::super::BTreeLookup::NotFound
        );
        assert_eq!(
            reader
                .lookup(&tree, &buffer, b"k3", None, CommitSeq::new(1))
                .expect("shared update check"),
            MvccLookup::Found(b"v3".to_vec())
        );
    }
}
