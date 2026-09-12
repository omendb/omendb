//! Buffer-resident ordered B-tree access method for storage-kernel vNext.
//!
//! The object owns only access-method metadata. Page lifetime, caching,
//! translation, I/O, and dirty state belong to `BufferPool`; there is no
//! `Vec<Option<Arc<Node>>>` or generation clone in this path.

mod page_v3;

use super::{BufferError, BufferPool, PageId, PageKey, StorageObjectDescriptor};
use crate::btree::BlobPointer;
use page_v3::{NodePage, PAGE_SIZE, PageValue};

const MAX_ROUTING_DEPTH: usize = 128;

/// Result of a point lookup through the vNext ordered B-tree path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreeLookup {
    /// Key exists with an inline payload.
    Found(Vec<u8>),
    /// Key exists with the current compatibility blob pointer.
    Blob(BlobPointer),
    /// Key is represented by a tombstone.
    Deleted,
    /// Key is absent from the routed leaf.
    NotFound,
}

/// Failure while traversing a vNext B-tree object.
#[derive(Debug, thiserror::Error)]
pub enum BTreeReadError {
    /// Buffer/kernel operation failed.
    #[error(transparent)]
    Buffer(#[from] BufferError),
    /// The current compatibility codec is fixed at 4 KiB.
    #[error("v3 B-tree pages require {expected} bytes, buffer pool uses {actual}")]
    PageSize { expected: usize, actual: usize },
    /// A page failed checksum/layout/routing validation.
    #[error("B-tree page {page:?} is corrupt: {reason}")]
    Corruption {
        page: PageId,
        reason: &'static str,
    },
    /// A routing cycle or implausibly deep tree was encountered.
    #[error("B-tree routing exceeded the maximum supported depth")]
    RoutingDepthExceeded,
}

/// Ordered B-tree access-method metadata backed by shared buffer pages.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BTreeObject {
    descriptor: StorageObjectDescriptor,
    root: PageId,
}

impl BTreeObject {
    /// Bind a storage object to its logical root page.
    #[must_use]
    pub const fn new(descriptor: StorageObjectDescriptor, root: PageId) -> Self {
        Self { descriptor, root }
    }

    /// Return kernel-owned metadata for this access method.
    #[must_use]
    pub const fn descriptor(self) -> StorageObjectDescriptor {
        self.descriptor
    }

    /// Return the current logical root page.
    #[must_use]
    pub const fn root(self) -> PageId {
        self.root
    }

    /// Point lookup through buffer-owned guarded pages.
    pub fn lookup(&self, buffer: &BufferPool, key: &[u8]) -> Result<BTreeLookup, BTreeReadError> {
        if buffer.page_size() != PAGE_SIZE {
            return Err(BTreeReadError::PageSize {
                expected: PAGE_SIZE,
                actual: buffer.page_size(),
            });
        }

        let mut current = self.root;
        for _ in 0..MAX_ROUTING_DEPTH {
            let page_key = PageKey::new(self.descriptor.id(), current);
            let guard = buffer.pin(page_key)?;
            let bytes = guard.read()?;
            let page = NodePage::parse(bytes.as_ref()).map_err(|error| {
                BTreeReadError::Corruption {
                    page: current,
                    reason: error.0,
                }
            })?;

            if page.is_leaf() {
                return match page.search(key) {
                    Some(Ok(index)) => match page.value(index) {
                        Some(PageValue::Inline(value)) => Ok(BTreeLookup::Found(value.to_vec())),
                        Some(PageValue::Blob(pointer)) => Ok(BTreeLookup::Blob(pointer)),
                        Some(PageValue::Tombstone) => Ok(BTreeLookup::Deleted),
                        None => Err(BTreeReadError::Corruption {
                            page: current,
                            reason: "leaf value payload is malformed",
                        }),
                    },
                    Some(Err(_)) => Ok(BTreeLookup::NotFound),
                    None => Err(BTreeReadError::Corruption {
                        page: current,
                        reason: "leaf search encountered malformed key data",
                    }),
                };
            }

            let child = page.child_for_key(key).ok_or(BTreeReadError::Corruption {
                page: current,
                reason: "internal routing payload is malformed",
            })?;
            if child == current.get() {
                return Err(BTreeReadError::Corruption {
                    page: current,
                    reason: "internal page routes to itself",
                });
            }
            current = PageId::new(child);
        }

        Err(BTreeReadError::RoutingDepthExceeded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::{BTree, LookupResult, Node};
    use crate::vnext::{ObjectAuthority, PageIo, StorageObjectId};
    use std::collections::HashMap;
    use std::io;
    use std::sync::{Arc, RwLock};

    #[derive(Default)]
    struct MemoryPageIo {
        pages: RwLock<HashMap<PageKey, Vec<u8>>>,
    }

    impl MemoryPageIo {
        fn insert_node(&self, object: StorageObjectId, page: PageId, mut node: Node) {
            node.update_checksum();
            self.pages
                .write()
                .expect("page map writable")
                .insert(PageKey::new(object, page), node.as_bytes().to_vec());
        }

        fn corrupt_byte(&self, key: PageKey, offset: usize) {
            let mut pages = self.pages.write().expect("page map writable");
            let page = pages.get_mut(&key).expect("page exists");
            page[offset] ^= 0x80;
        }
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
            if page.len() != destination.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "page size mismatch",
                ));
            }
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

    fn object(id: u64) -> StorageObjectDescriptor {
        StorageObjectDescriptor::new(StorageObjectId::new(id), ObjectAuthority::Authoritative)
    }

    fn convert_reference(result: LookupResult) -> BTreeLookup {
        match result {
            LookupResult::Found(value) => BTreeLookup::Found(value),
            LookupResult::Blob(pointer) => BTreeLookup::Blob(pointer),
            LookupResult::Deleted => BTreeLookup::Deleted,
            LookupResult::NotFound => BTreeLookup::NotFound,
        }
    }

    #[test]
    fn guarded_leaf_lookup_matches_reference_tree() {
        let mut reference = BTree::new();
        reference.insert(b"alpha", b"one").expect("insert alpha");
        reference.insert(b"beta", b"two").expect("insert beta");
        reference.insert(b"gamma", b"three").expect("insert gamma");

        let descriptor = object(11);
        let root = PageId::new(reference.root_id() as u64);
        let device = Arc::new(MemoryPageIo::default());
        device.insert_node(
            descriptor.id(),
            root,
            reference
                .node(reference.root_id())
                .expect("root is resident")
                .clone(),
        );
        let buffer = BufferPool::new(2, PAGE_SIZE, device).expect("buffer creates");
        let tree = BTreeObject::new(descriptor, root);

        for key in [b"alpha".as_slice(), b"beta", b"gamma", b"missing"] {
            assert_eq!(
                tree.lookup(&buffer, key).expect("vNext lookup succeeds"),
                convert_reference(reference.lookup(key).expect("reference lookup succeeds"))
            );
        }
        let stats = buffer.stats().expect("buffer stats");
        assert_eq!(stats.loads, 1);
        assert_eq!(stats.translation_entries, 1);
    }

    #[test]
    fn internal_routing_loads_children_through_shared_buffer() {
        let descriptor = object(17);
        let device = Arc::new(MemoryPageIo::default());

        let mut root = Node::new_internal();
        root.set_leftmost_child(1);
        root.insert_child(b"m", 2).expect("separator inserts");
        let mut left = Node::new_leaf();
        left.insert(b"a", b"left").expect("left value inserts");
        let mut right = Node::new_leaf();
        right.insert(b"m", b"middle").expect("middle inserts");
        right.insert(b"z", b"right").expect("right value inserts");

        device.insert_node(descriptor.id(), PageId::new(0), root);
        device.insert_node(descriptor.id(), PageId::new(1), left);
        device.insert_node(descriptor.id(), PageId::new(2), right);

        let buffer = BufferPool::new(3, PAGE_SIZE, device).expect("buffer creates");
        let tree = BTreeObject::new(descriptor, PageId::new(0));
        assert_eq!(
            tree.lookup(&buffer, b"a").expect("left lookup"),
            BTreeLookup::Found(b"left".to_vec())
        );
        assert_eq!(
            tree.lookup(&buffer, b"m").expect("equal separator routes right"),
            BTreeLookup::Found(b"middle".to_vec())
        );
        assert_eq!(
            tree.lookup(&buffer, b"z").expect("right lookup"),
            BTreeLookup::Found(b"right".to_vec())
        );
    }

    #[test]
    fn corrupted_page_fails_closed() {
        let descriptor = object(23);
        let device = Arc::new(MemoryPageIo::default());
        let mut leaf = Node::new_leaf();
        leaf.insert(b"key", b"value").expect("value inserts");
        let page = PageId::new(0);
        let page_key = PageKey::new(descriptor.id(), page);
        device.insert_node(descriptor.id(), page, leaf);
        device.corrupt_byte(page_key, PAGE_SIZE - 1);

        let buffer = BufferPool::new(1, PAGE_SIZE, device).expect("buffer creates");
        let tree = BTreeObject::new(descriptor, page);
        assert!(matches!(
            tree.lookup(&buffer, b"key"),
            Err(BTreeReadError::Corruption { .. })
        ));
    }

    #[test]
    fn page_size_mismatch_is_rejected_before_io() {
        let descriptor = object(29);
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(1, PAGE_SIZE * 2, device).expect("buffer creates");
        let tree = BTreeObject::new(descriptor, PageId::new(0));
        assert!(matches!(
            tree.lookup(&buffer, b"key"),
            Err(BTreeReadError::PageSize { .. })
        ));
    }
}
