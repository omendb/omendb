//! Ordered MVCC reads with a transaction-private staged overlay.

use super::{
    BTreeObject, BufferPool, MvccLookup, OrderedMvccReadError, OrderedMvccReader,
    StagedOrderedLookup, Transaction,
};

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{
        MvccRecord, MvccValue, ObjectAuthority, PageIo, PageKey, RecordOwner,
        StorageObjectDescriptor, StorageObjectId, TransactionStatusTable, TxnId, UndoStore,
    };
    use crate::storage::format::CommitSeq;
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

    #[test]
    fn staged_put_and_delete_override_shared_snapshot_without_installing() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(8, 512, device).expect("buffer");
        let tree = BTreeObject::create(descriptor(1), &buffer).expect("tree");
        let base = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(1)),
            None,
            MvccValue::Inline(b"base".to_vec()),
        );
        tree.insert(&buffer, b"existing", &base.to_bytes().expect("encode"))
            .expect("seed");

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
}
