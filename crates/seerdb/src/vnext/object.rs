//! Storage-object authority classification for the vNext kernel.

use super::StorageObjectId;

/// Whether a storage object is part of authoritative committed state or is a
/// rebuildable representation derived from authoritative state.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum ObjectAuthority {
    /// The object participates in the transaction's durable recovery record.
    /// Losing or silently lagging this state could change committed semantics.
    Authoritative,
    /// The object names the logical frontier it covers and may be rebuilt or
    /// caught up from authoritative state plus the committed-change stream.
    Derived,
}

impl ObjectAuthority {
    /// Return whether mutations of this object must be recoverable as part of
    /// the committing transaction before synchronous acknowledgement.
    #[must_use]
    pub const fn requires_commit_recovery(self) -> bool {
        matches!(self, Self::Authoritative)
    }
}

/// Minimal kernel-owned metadata shared by every storage object.
///
/// Access-method-specific metadata deliberately lives outside this descriptor;
/// the storage kernel only needs the object's identity and authority class at
/// this stage of the rewrite.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct StorageObjectDescriptor {
    id: StorageObjectId,
    authority: ObjectAuthority,
}

impl StorageObjectDescriptor {
    /// Construct a storage-object descriptor.
    #[must_use]
    pub const fn new(id: StorageObjectId, authority: ObjectAuthority) -> Self {
        Self { id, authority }
    }

    /// Return the stable storage-object identity.
    #[must_use]
    pub const fn id(self) -> StorageObjectId {
        self.id
    }

    /// Return the object's authority class.
    #[must_use]
    pub const fn authority(self) -> ObjectAuthority {
        self.authority
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authoritative_objects_require_transaction_recovery_coverage() {
        let object = StorageObjectDescriptor::new(
            StorageObjectId::new(7),
            ObjectAuthority::Authoritative,
        );
        assert_eq!(object.id(), StorageObjectId::new(7));
        assert!(object.authority().requires_commit_recovery());
    }

    #[test]
    fn derived_objects_do_not_define_commit_durability() {
        let object = StorageObjectDescriptor::new(StorageObjectId::new(9), ObjectAuthority::Derived);
        assert!(!object.authority().requires_commit_recovery());
    }
}
