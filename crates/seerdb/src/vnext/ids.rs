//! Storage-kernel vNext identities that are independent of any access method.

/// Stable identity of one storage object managed by the vNext kernel.
///
/// A storage object is a physical/logical structure that participates in the
/// shared buffer, transaction, log, recovery, and lifetime machinery. Examples
/// include an ordered index, a canonical row family, or a derived analytical
/// representation. The identifier deliberately carries no SQL or access-method
/// meaning.
#[derive(Debug, Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageObjectId(u64);

impl StorageObjectId {
    /// Construct an object identity from its stable integer representation.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the stable integer representation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Stable logical identity of one page inside the storage kernel.
///
/// `PageId` is intentionally distinct from a physical byte offset, frame
/// number, WAL position, page version, or B-tree-local array index. Physical
/// placement may change whenever a dirty page is materialized out-of-place.
#[derive(Debug, Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageId(u64);

impl PageId {
    /// Construct a logical page identity.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the stable integer representation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Process-local identity of one resident buffer frame.
///
/// Frame identities are never persisted and must never be used as logical page
/// identities or recovery references. `usize` is intentional because a frame
/// is an in-process slot rather than a disk-format field.
#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameId(usize);

impl FrameId {
    /// Construct a frame identity from its process-local slot index.
    #[must_use]
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    /// Return the process-local slot index.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_ids_round_trip_without_cross_domain_conversion() {
        let object = StorageObjectId::new(u64::MAX - 1);
        let page = PageId::new(u64::MAX);
        assert_eq!(object.get(), u64::MAX - 1);
        assert_eq!(page.get(), u64::MAX);
    }

    #[test]
    fn frame_id_is_explicitly_process_local() {
        let frame = FrameId::new(17);
        assert_eq!(frame.index(), 17);
    }
}
