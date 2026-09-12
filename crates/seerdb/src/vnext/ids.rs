//! Storage-kernel vNext identities that are independent of any access method.

use std::num::NonZeroU64;

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

/// Stable logical identity of one page inside a storage object.
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

/// Complete logical page identity used by translation and page I/O.
///
/// Page numbers are scoped to their storage object so access methods can
/// allocate independently without manufacturing globally unique page IDs.
#[derive(Debug, Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageKey {
    object: StorageObjectId,
    page: PageId,
}

impl PageKey {
    /// Construct an object-scoped page key.
    #[must_use]
    pub const fn new(object: StorageObjectId, page: PageId) -> Self {
        Self { object, page }
    }

    /// Return the owning storage object.
    #[must_use]
    pub const fn object(self) -> StorageObjectId {
        self.object
    }

    /// Return the logical page identity within the object.
    #[must_use]
    pub const fn page(self) -> PageId {
        self.page
    }
}

/// Process-local identity of one buffer-frame slot.
///
/// A slot can be reused for many logical pages over the lifetime of a process,
/// so a `FrameId` alone is not a stable reference to resident page contents.
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

/// Nonzero process-local incarnation of a buffer-frame slot.
///
/// Incarnations advance whenever a free slot is reserved for a new page. They
/// are never persisted; their sole purpose is to prevent stale `FrameId`
/// references from becoming valid again after slot reuse (the ABA problem).
#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameIncarnation(NonZeroU64);

impl FrameIncarnation {
    pub(crate) fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    /// Return the process-local incarnation number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Stale-safe process-local reference to one resident frame incarnation.
///
/// Translation tables and long-lived diagnostics must use this pair rather
/// than a bare `FrameId`. The pair is still process-local and must never appear
/// in durable metadata.
#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRef {
    frame: FrameId,
    incarnation: FrameIncarnation,
}

impl FrameRef {
    pub(crate) const fn new(frame: FrameId, incarnation: FrameIncarnation) -> Self {
        Self { frame, incarnation }
    }

    /// Return the underlying frame slot.
    #[must_use]
    pub const fn frame(self) -> FrameId {
        self.frame
    }

    /// Return the slot incarnation represented by this reference.
    #[must_use]
    pub const fn incarnation(self) -> FrameIncarnation {
        self.incarnation
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
    fn page_key_keeps_object_and_page_domains_explicit() {
        let key = PageKey::new(StorageObjectId::new(11), PageId::new(29));
        assert_eq!(key.object(), StorageObjectId::new(11));
        assert_eq!(key.page(), PageId::new(29));
    }

    #[test]
    fn frame_reference_includes_nonzero_incarnation() {
        let frame = FrameId::new(17);
        let incarnation = FrameIncarnation::new(3).expect("nonzero incarnation");
        let reference = FrameRef::new(frame, incarnation);
        assert_eq!(reference.frame(), frame);
        assert_eq!(reference.incarnation().get(), 3);
        assert!(FrameIncarnation::new(0).is_none());
    }
}
