//! Canonical physical write-set normalization for vNext transactions.
//!
//! WAL authentication remains defined over the original ordered mutation
//! stream. Only after that stream is validated do installation, write-intent
//! acquisition, and replay reduce it to one final physical effect per
//! `(StorageObjectId, key)`. Keeping this reducer shared prevents live commit
//! and recovery from inventing different same-key semantics.

use super::{LoggedMutation, MutationKind, StorageObjectId, TxnId};
use std::collections::BTreeMap;

/// One canonical final physical effect retained from an authenticated mutation
/// stream. The original final mutation ordinal is preserved as replay identity.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FinalEffect {
    mutation: LoggedMutation,
}

impl FinalEffect {
    #[must_use]
    pub const fn txn_id(&self) -> TxnId {
        self.mutation.txn_id()
    }

    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.mutation.ordinal()
    }

    #[must_use]
    pub const fn object(&self) -> StorageObjectId {
        self.mutation.object()
    }

    #[must_use]
    pub const fn kind(&self) -> MutationKind {
        self.mutation.kind()
    }

    #[must_use]
    pub fn key(&self) -> &[u8] {
        self.mutation.key()
    }

    #[must_use]
    pub fn value(&self) -> &[u8] {
        self.mutation.value()
    }

    #[must_use]
    pub fn mutation(&self) -> &LoggedMutation {
        &self.mutation
    }
}

/// Rejection while validating and normalizing one transaction's mutation list.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum FinalWriteSetError {
    #[error("mutation ordinal {ordinal} belongs to transaction {actual:?}, expected {expected:?}")]
    WrongTransaction {
        expected: TxnId,
        actual: TxnId,
        ordinal: u32,
    },
    #[error("mutation ordinal {actual} is not contiguous; expected {expected}")]
    NonContiguousOrdinal { expected: u32, actual: u32 },
    #[error("transaction contains more mutations than the vNext ordinal domain can represent")]
    TooManyMutations,
}

/// Validate an original transaction mutation stream and reduce it to one final
/// physical effect per `(object, key)` in canonical object/key order.
///
/// Repeated keys overwrite only the normalized physical view. The original WAL
/// sequence, count, digest, and ordinals remain unchanged and authoritative.
pub fn normalize_final_effects(
    txn_id: TxnId,
    mutations: &[LoggedMutation],
) -> Result<Vec<FinalEffect>, FinalWriteSetError> {
    let mut final_by_key: BTreeMap<(StorageObjectId, Vec<u8>), LoggedMutation> = BTreeMap::new();

    for (index, mutation) in mutations.iter().enumerate() {
        let expected = u32::try_from(index).map_err(|_| FinalWriteSetError::TooManyMutations)?;
        if mutation.ordinal() != expected {
            return Err(FinalWriteSetError::NonContiguousOrdinal {
                expected,
                actual: mutation.ordinal(),
            });
        }
        if mutation.txn_id() != txn_id {
            return Err(FinalWriteSetError::WrongTransaction {
                expected: txn_id,
                actual: mutation.txn_id(),
                ordinal: mutation.ordinal(),
            });
        }
        final_by_key.insert(
            (mutation.object(), mutation.key().to_vec()),
            mutation.clone(),
        );
    }

    Ok(final_by_key
        .into_values()
        .map(|mutation| FinalEffect { mutation })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(txn: u64, ordinal: u32, object: u64, key: &[u8], value: &[u8]) -> LoggedMutation {
        LoggedMutation::ordered_put(
            TxnId::new(txn),
            ordinal,
            StorageObjectId::new(object),
            key.to_vec(),
            value.to_vec(),
        )
    }

    fn delete(txn: u64, ordinal: u32, object: u64, key: &[u8]) -> LoggedMutation {
        LoggedMutation::ordered_delete(
            TxnId::new(txn),
            ordinal,
            StorageObjectId::new(object),
            key.to_vec(),
        )
    }

    #[test]
    fn repeated_writes_reduce_to_the_last_physical_effect() {
        let mutations = vec![
            put(7, 0, 11, b"k", b"a"),
            put(7, 1, 11, b"k", b"b"),
            delete(7, 2, 11, b"k"),
            put(7, 3, 11, b"k", b"c"),
        ];
        let effects = normalize_final_effects(TxnId::new(7), &mutations).expect("normalizes");
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].ordinal(), 3);
        assert_eq!(effects[0].kind(), MutationKind::OrderedPut);
        assert_eq!(effects[0].value(), b"c");
    }

    #[test]
    fn delete_and_put_transitions_keep_only_the_final_kind() {
        let put_delete = [put(5, 0, 2, b"a", b"v"), delete(5, 1, 2, b"a")];
        let effects = normalize_final_effects(TxnId::new(5), &put_delete).expect("normalizes");
        assert_eq!(effects[0].ordinal(), 1);
        assert_eq!(effects[0].kind(), MutationKind::OrderedDelete);
        assert!(effects[0].value().is_empty());

        let delete_put = [delete(6, 0, 2, b"a"), put(6, 1, 2, b"a", b"new")];
        let effects = normalize_final_effects(TxnId::new(6), &delete_put).expect("normalizes");
        assert_eq!(effects[0].ordinal(), 1);
        assert_eq!(effects[0].kind(), MutationKind::OrderedPut);
        assert_eq!(effects[0].value(), b"new");
    }

    #[test]
    fn equal_keys_in_different_objects_remain_distinct_and_canonical() {
        let mutations = vec![
            put(9, 0, 3, b"same", b"third"),
            put(9, 1, 1, b"z", b"last-key"),
            put(9, 2, 1, b"a", b"first-key"),
            put(9, 3, 2, b"same", b"second"),
        ];
        let effects = normalize_final_effects(TxnId::new(9), &mutations).expect("normalizes");
        let order: Vec<_> = effects
            .iter()
            .map(|effect| (effect.object(), effect.key().to_vec(), effect.ordinal()))
            .collect();
        assert_eq!(
            order,
            vec![
                (StorageObjectId::new(1), b"a".to_vec(), 2),
                (StorageObjectId::new(1), b"z".to_vec(), 1),
                (StorageObjectId::new(2), b"same".to_vec(), 3),
                (StorageObjectId::new(3), b"same".to_vec(), 0),
            ]
        );
    }

    #[test]
    fn malformed_original_stream_is_rejected_before_reduction() {
        let gap = [put(1, 0, 1, b"a", b"a"), put(1, 2, 1, b"a", b"b")];
        assert!(matches!(
            normalize_final_effects(TxnId::new(1), &gap),
            Err(FinalWriteSetError::NonContiguousOrdinal {
                expected: 1,
                actual: 2,
            })
        ));

        let wrong_txn = [put(1, 0, 1, b"a", b"a"), put(2, 1, 1, b"b", b"b")];
        assert!(matches!(
            normalize_final_effects(TxnId::new(1), &wrong_txn),
            Err(FinalWriteSetError::WrongTransaction {
                expected,
                actual,
                ordinal: 1,
            }) if expected == TxnId::new(1) && actual == TxnId::new(2)
        ));
    }
}
