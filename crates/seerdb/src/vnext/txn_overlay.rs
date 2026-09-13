//! Private read-your-writes view over one transaction's staged mutations.
//!
//! Shared current records are not modified before the durable decision. Reads
//! therefore consult the latest staged mutation for a logical key before
//! falling back to the transaction's fixed MVCC snapshot.

use super::{MutationKind, StorageObjectId, Transaction};

/// Latest private staged effect for one ordered logical key.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StagedOrderedLookup<'a> {
    Put(&'a [u8]),
    Delete,
    Unchanged,
}

impl Transaction {
    /// Return the latest staged effect for `(object,key)` without changing the
    /// transaction or allocating a normalized write set.
    #[must_use]
    pub fn staged_ordered_lookup(
        &self,
        object: StorageObjectId,
        key: &[u8],
    ) -> StagedOrderedLookup<'_> {
        let Some(mutation) = self
            .mutations()
            .iter()
            .rev()
            .find(|mutation| mutation.object() == object && mutation.key() == key)
        else {
            return StagedOrderedLookup::Unchanged;
        };
        match mutation.kind() {
            MutationKind::OrderedPut => StagedOrderedLookup::Put(mutation.value()),
            MutationKind::OrderedDelete => StagedOrderedLookup::Delete,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::format::CommitSeq;
    use crate::vnext::{ObjectAuthority, StorageObjectDescriptor, TxnId};

    fn object(id: u64) -> StorageObjectDescriptor {
        StorageObjectDescriptor::new(StorageObjectId::new(id), ObjectAuthority::Authoritative)
    }

    #[test]
    fn latest_same_key_staged_effect_wins_without_cross_object_aliasing() {
        let mut transaction = Transaction::new(TxnId::new(1), CommitSeq::new(0));
        transaction
            .stage_ordered_put(object(1), b"key".to_vec(), b"first".to_vec())
            .expect("first put");
        transaction
            .stage_ordered_put(object(2), b"key".to_vec(), b"other-object".to_vec())
            .expect("other object put");
        transaction
            .stage_ordered_delete(object(1), b"key".to_vec())
            .expect("delete");
        transaction
            .stage_ordered_put(object(1), b"key".to_vec(), b"final".to_vec())
            .expect("final put");

        assert_eq!(
            transaction.staged_ordered_lookup(StorageObjectId::new(1), b"key"),
            StagedOrderedLookup::Put(b"final")
        );
        assert_eq!(
            transaction.staged_ordered_lookup(StorageObjectId::new(2), b"key"),
            StagedOrderedLookup::Put(b"other-object")
        );
        assert_eq!(
            transaction.staged_ordered_lookup(StorageObjectId::new(1), b"missing"),
            StagedOrderedLookup::Unchanged
        );
    }

    #[test]
    fn staged_delete_is_visible_to_private_overlay() {
        let mut transaction = Transaction::new(TxnId::new(2), CommitSeq::new(0));
        transaction
            .stage_ordered_put(object(1), b"key".to_vec(), b"value".to_vec())
            .expect("put");
        transaction
            .stage_ordered_delete(object(1), b"key".to_vec())
            .expect("delete");
        assert_eq!(
            transaction.staged_ordered_lookup(StorageObjectId::new(1), b"key"),
            StagedOrderedLookup::Delete
        );
    }
}
