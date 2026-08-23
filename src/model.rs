use std::fmt;

/// Stable identity for one writable OmenDB database history.
///
/// Commit numbers are meaningful only within this identity. Backends may use
/// different physical representations, but every project-facing archive or
/// snapshot boundary uses this common qualifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageIdentity {
    pub database_id: [u8; 16],
    pub history_id: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommitId(pub u64);

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct IndexId(pub u64);

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct Key(pub [u8; 16]);

impl Key {
    #[must_use]
    pub fn new(tenant: u64, record: u64) -> Self {
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&tenant.to_be_bytes());
        bytes[8..].copy_from_slice(&record.to_be_bytes());
        Self(bytes)
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("Key").field(&self.0).finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Put {
        key: Key,
        value: Vec<u8>,
    },
    Delete {
        key: Key,
    },
    CreateIndex {
        index: IndexId,
        unique: bool,
    },
    IndexPut {
        index: IndexId,
        index_key: Vec<u8>,
        primary: Key,
    },
    IndexDelete {
        index: IndexId,
        index_key: Vec<u8>,
        primary: Key,
    },
    /// Variable-width row or catalog bytes for relational backends whose
    /// physical identity is not representable by [`Key`].
    #[doc(hidden)]
    BytePut {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Delete one variable-width relational key.
    #[doc(hidden)]
    ByteDelete {
        key: Vec<u8>,
    },
    /// Add one variable-width primary to a secondary-index entry.
    #[doc(hidden)]
    ByteIndexPut {
        index: IndexId,
        index_key: Vec<u8>,
        primary: Vec<u8>,
    },
    /// Remove one variable-width primary from a secondary-index entry.
    #[doc(hidden)]
    ByteIndexDelete {
        index: IndexId,
        index_key: Vec<u8>,
        primary: Vec<u8>,
    },
    /// Durable transaction-attempt metadata. This is consumed by the
    /// storage kernel and is not part of the relational row/index model.
    #[doc(hidden)]
    RecordAttempt {
        attempt: crate::TransactionAttemptId,
        digest: [u8; 32],
    },
    /// Durable deletion of transaction-attempt metadata after the caller has
    /// decided that no retry may use the identity again.
    #[doc(hidden)]
    ForgetAttempt {
        attempt: crate::TransactionAttemptId,
    },
}
