//! Concurrent buffered B-link tree for storage-kernel vNext.
//!
//! Ordinary point reads and leaf-local inserts never take an object-wide lock.
//! Structural modification is serialized only while splits propagate. High
//! fences and right links keep the tree searchable while a split is installed,
//! so this coarse structural baseline can later be replaced by page-local SMO
//! coordination without changing the page/search contract.

use super::super::{BufferError, BufferPool, PageId, PageKey, StorageObjectDescriptor};
use super::page_v4::{
    self, InsertResult, InternalEntryOwned, LeafEntryOwned, LeafValueOwned, PageError, PageRef,
    PageValue, RemoveResult,
};
use crate::btree::BlobPointer;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

const MAX_ROUTING_DEPTH: usize = 128;
const RESERVED_PAGE_ID: u64 = u64::MAX;

/// Result of a point lookup through the native vNext B-tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreeLookup {
    Found(Vec<u8>),
    Blob(BlobPointer),
    Deleted,
    NotFound,
}

/// Failure while operating on a native vNext B-tree.
#[derive(Debug, thiserror::Error)]
pub enum BTreeError {
    #[error(transparent)]
    Buffer(#[from] BufferError),
    #[error("B-tree page {page:?} is corrupt: {reason}")]
    Corruption { page: PageId, reason: &'static str },
    #[error("B-tree routing exceeded the maximum supported depth")]
    RoutingDepthExceeded,
    #[error("duplicate key")]
    DuplicateKey,
    #[error("logical page ID space is exhausted")]
    PageIdExhausted,
    #[error("B-tree structural-modification lock is poisoned")]
    StructuralLockPoisoned,
    #[error("B-tree entry does not fit an empty page")]
    EntryTooLarge,
}

/// Ordered access-method metadata over the shared vNext buffer pool.
///
/// `root` is atomically replaceable after root splits. `next_page` is object
/// local and reserves logical page IDs only; physical placement is delegated to
/// the kernel. The structural mutex is deliberately a baseline for rare SMOs,
/// not part of ordinary reads/writes and not a long-term global tree latch.
pub struct BTreeObject {
    descriptor: StorageObjectDescriptor,
    root: AtomicU64,
    next_page: AtomicU64,
    structural: Mutex<()>,
}

impl BTreeObject {
    /// Create a fresh tree and install its empty root as logical page zero.
    pub fn create(
        descriptor: StorageObjectDescriptor,
        buffer: &BufferPool,
    ) -> Result<Self, BTreeError> {
        let root = PageId::new(0);
        let image = page_v4::empty_leaf(buffer.page_size())
            .map_err(|error| Self::page_error(root, error))?;
        let guard = buffer.create_page(PageKey::new(descriptor.id(), root), &image)?;
        drop(guard);
        Ok(Self {
            descriptor,
            root: AtomicU64::new(root.get()),
            next_page: AtomicU64::new(1),
            structural: Mutex::new(()),
        })
    }

    /// Open already-materialized v4 object metadata.
    ///
    /// `next_page` must be greater than every allocated page ID and is expected
    /// to come from checkpoint/object metadata once that layer lands.
    pub fn open(
        descriptor: StorageObjectDescriptor,
        root: PageId,
        next_page: PageId,
    ) -> Result<Self, BTreeError> {
        if root.get() == RESERVED_PAGE_ID || next_page.get() == RESERVED_PAGE_ID {
            return Err(BTreeError::PageIdExhausted);
        }
        Ok(Self {
            descriptor,
            root: AtomicU64::new(root.get()),
            next_page: AtomicU64::new(next_page.get()),
            structural: Mutex::new(()),
        })
    }

    #[must_use]
    pub const fn descriptor(&self) -> StorageObjectDescriptor {
        self.descriptor
    }

    #[must_use]
    pub fn root(&self) -> PageId {
        PageId::new(self.root.load(Ordering::Acquire))
    }

    /// Point lookup with B-link right correction at every level.
    pub fn lookup(&self, buffer: &BufferPool, key: &[u8]) -> Result<BTreeLookup, BTreeError> {
        let mut current = self.root();
        for _ in 0..MAX_ROUTING_DEPTH {
            let guard = buffer.pin(self.page_key(current))?;
            let bytes = guard.read()?;
            let page =
                PageRef::parse(bytes.as_ref()).map_err(|error| Self::page_error(current, error))?;
            if let Some(right) = page
                .follow_right(key)
                .map_err(|error| Self::page_error(current, error))?
            {
                current = right;
                continue;
            }
            if page.is_leaf() {
                return match page
                    .search(key)
                    .map_err(|error| Self::page_error(current, error))?
                {
                    Ok(index) => Self::lookup_value(current, &page, index),
                    Err(_) => Ok(BTreeLookup::NotFound),
                };
            }
            current = page
                .child_for_key(key)
                .map_err(|error| Self::page_error(current, error))?;
        }
        Err(BTreeError::RoutingDepthExceeded)
    }

    /// Insert one inline key/value. Leaf-local inserts use only a frame-local
    /// write guard; the structural mutex is entered only after a full page is
    /// observed and is revalidated under the mutex.
    pub fn insert(&self, buffer: &BufferPool, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        loop {
            let (leaf, _) = self.find_leaf_path(buffer, key)?;
            let guard = buffer.pin(self.page_key(leaf))?;
            let mut bytes = guard.write()?;
            match page_v4::try_insert_inline(&mut bytes, key, value)
                .map_err(|error| Self::page_error(leaf, error))?
            {
                InsertResult::Inserted => return Ok(()),
                InsertResult::Duplicate => return Err(BTreeError::DuplicateKey),
                InsertResult::FollowRight(_) => continue,
                InsertResult::Full => break,
            }
        }
        self.insert_with_split(buffer, key, value)
    }

    /// Remove one key without eager merge/rebalance. Empty/underfull leaves are
    /// legal; background or measured merge policy can be added independently.
    pub fn delete(&self, buffer: &BufferPool, key: &[u8]) -> Result<bool, BTreeError> {
        loop {
            let (leaf, _) = self.find_leaf_path(buffer, key)?;
            let guard = buffer.pin(self.page_key(leaf))?;
            let mut bytes = guard.write()?;
            match page_v4::remove_leaf(&mut bytes, key)
                .map_err(|error| Self::page_error(leaf, error))?
            {
                RemoveResult::Removed => return Ok(true),
                RemoveResult::Missing => return Ok(false),
                RemoveResult::FollowRight(_) => continue,
            }
        }
    }

    /// Bounded materialized range helper over leaf right links.
    ///
    /// The resumable cursor layer re-enters this helper by logical key rather
    /// than retaining page/slot state across calls.
    pub fn range(
        &self,
        buffer: &BufferPool,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, BTreeLookup)>, BTreeError> {
        if start >= end || limit == 0 {
            return Ok(Vec::new());
        }
        let (mut current, _) = self.find_leaf_path(buffer, start)?;
        let mut output = Vec::new();
        for _ in 0..MAX_ROUTING_DEPTH.saturating_mul(1024) {
            let guard = buffer.pin(self.page_key(current))?;
            let bytes = guard.read()?;
            let page =
                PageRef::parse(bytes.as_ref()).map_err(|error| Self::page_error(current, error))?;
            if !page.is_leaf() {
                return Err(Self::page_error(
                    current,
                    PageError("range reached an internal page"),
                ));
            }
            if let Some(right) = page
                .follow_right(start)
                .map_err(|error| Self::page_error(current, error))?
            {
                current = right;
                continue;
            }
            for entry in page
                .leaf_entries_owned()
                .map_err(|error| Self::page_error(current, error))?
            {
                if entry.key.as_slice() < start {
                    continue;
                }
                if entry.key.as_slice() >= end {
                    return Ok(output);
                }
                let value = match entry.value {
                    LeafValueOwned::Inline(value) => BTreeLookup::Found(value),
                    LeafValueOwned::Blob(pointer) => BTreeLookup::Blob(pointer),
                    LeafValueOwned::Tombstone => BTreeLookup::Deleted,
                };
                output.push((entry.key, value));
                if output.len() == limit {
                    return Ok(output);
                }
            }
            let Some(right) = page.right_sibling() else {
                return Ok(output);
            };
            if page
                .high_fence()
                .map_err(|error| Self::page_error(current, error))?
                .is_some_and(|high| high >= end)
            {
                return Ok(output);
            }
            current = right;
        }
        Err(BTreeError::RoutingDepthExceeded)
    }

    fn insert_with_split(
        &self,
        buffer: &BufferPool,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), BTreeError> {
        let _structural = self
            .structural
            .lock()
            .map_err(|_| BTreeError::StructuralLockPoisoned)?;

        loop {
            let (leaf, mut path) = self.find_leaf_path(buffer, key)?;
            let leaf_guard = buffer.pin(self.page_key(leaf))?;
            let mut bytes = leaf_guard.write()?;
            match page_v4::try_insert_inline(&mut bytes, key, value)
                .map_err(|error| Self::page_error(leaf, error))?
            {
                InsertResult::Inserted => return Ok(()),
                InsertResult::Duplicate => return Err(BTreeError::DuplicateKey),
                InsertResult::FollowRight(_) => continue,
                InsertResult::Full => {}
            }

            let page =
                PageRef::parse(bytes.as_ref()).map_err(|error| Self::page_error(leaf, error))?;
            let old_high = page
                .high_fence()
                .map_err(|error| Self::page_error(leaf, error))?
                .map(ToOwned::to_owned);
            let old_right = page.right_sibling();
            let mut entries = page
                .leaf_entries_owned()
                .map_err(|error| Self::page_error(leaf, error))?;
            let insertion = match entries.binary_search_by(|entry| entry.key.as_slice().cmp(key)) {
                Ok(_) => return Err(BTreeError::DuplicateKey),
                Err(index) => index,
            };
            entries.insert(
                insertion,
                LeafEntryOwned {
                    key: key.to_vec(),
                    value: LeafValueOwned::Inline(value.to_vec()),
                },
            );
            let split = page_v4::choose_leaf_split(&entries)
                .map_err(|error| Self::page_error(leaf, error))?;
            let separator = entries[split].key.clone();
            let right_entries = entries.split_off(split);
            let right_id = self.allocate_page()?;
            let left_image = page_v4::build_leaf(
                buffer.page_size(),
                Some(&separator),
                Some(right_id),
                &entries,
            )
            .map_err(|error| Self::map_build_error(leaf, error))?;
            let right_image = page_v4::build_leaf(
                buffer.page_size(),
                old_high.as_deref(),
                old_right,
                &right_entries,
            )
            .map_err(|error| Self::map_build_error(right_id, error))?;

            let right_guard = buffer.create_page(self.page_key(right_id), &right_image)?;
            bytes.copy_from_slice(&left_image);
            drop(bytes);
            drop(right_guard);
            drop(leaf_guard);

            self.propagate_split(buffer, &mut path, separator, right_id)?;
            return Ok(());
        }
    }

    fn propagate_split(
        &self,
        buffer: &BufferPool,
        path: &mut Vec<PageId>,
        mut separator: Vec<u8>,
        mut right_id: PageId,
    ) -> Result<(), BTreeError> {
        while let Some(parent_id) = path.pop() {
            let parent_guard = buffer.pin(self.page_key(parent_id))?;
            let mut bytes = parent_guard.write()?;
            match page_v4::try_insert_internal(&mut bytes, &separator, right_id)
                .map_err(|error| Self::page_error(parent_id, error))?
            {
                InsertResult::Inserted => return Ok(()),
                InsertResult::Duplicate => {
                    return Err(Self::page_error(
                        parent_id,
                        PageError("duplicate separator during split propagation"),
                    ));
                }
                InsertResult::FollowRight(_) => {
                    return Err(Self::page_error(
                        parent_id,
                        PageError("structural path routed outside parent fence"),
                    ));
                }
                InsertResult::Full => {}
            }

            let page = PageRef::parse(bytes.as_ref())
                .map_err(|error| Self::page_error(parent_id, error))?;
            let old_high = page
                .high_fence()
                .map_err(|error| Self::page_error(parent_id, error))?
                .map(ToOwned::to_owned);
            let old_right = page.right_sibling();
            let old_leftmost = page.leftmost_child().ok_or_else(|| {
                Self::page_error(parent_id, PageError("internal page lacks leftmost child"))
            })?;
            let mut entries = page
                .internal_entries_owned()
                .map_err(|error| Self::page_error(parent_id, error))?;
            let insertion = match entries.binary_search_by(|entry| entry.key.cmp(&separator)) {
                Ok(_) => {
                    return Err(Self::page_error(
                        parent_id,
                        PageError("duplicate separator during internal split"),
                    ));
                }
                Err(index) => index,
            };
            entries.insert(
                insertion,
                InternalEntryOwned {
                    key: separator,
                    child: right_id,
                },
            );
            let split = page_v4::choose_internal_split(&entries)
                .map_err(|error| Self::page_error(parent_id, error))?;
            let right_entries = entries.split_off(split + 1);
            let promoted_entry = entries.pop().ok_or_else(|| {
                Self::page_error(parent_id, PageError("internal split lost promoted entry"))
            })?;
            let promoted = promoted_entry.key;
            let right_leftmost = promoted_entry.child;

            let new_right_id = self.allocate_page()?;
            let left_image = page_v4::build_internal(
                buffer.page_size(),
                Some(&promoted),
                Some(new_right_id),
                old_leftmost,
                &entries,
            )
            .map_err(|error| Self::map_build_error(parent_id, error))?;
            let right_image = page_v4::build_internal(
                buffer.page_size(),
                old_high.as_deref(),
                old_right,
                right_leftmost,
                &right_entries,
            )
            .map_err(|error| Self::map_build_error(new_right_id, error))?;

            let new_right_guard = buffer.create_page(self.page_key(new_right_id), &right_image)?;
            bytes.copy_from_slice(&left_image);
            drop(bytes);
            drop(new_right_guard);
            drop(parent_guard);

            separator = promoted;
            right_id = new_right_id;
        }

        // If an earlier root promotion failed after publishing a B-link split,
        // `right_id` may be a sibling reached by following right from the
        // current root rather than the root itself. The eventual promoted root
        // must retain the full older chain on its left, not start at only the
        // most recently split sibling.
        let root_leftmost = self.root();
        let new_root = self.allocate_page()?;
        let entries = [InternalEntryOwned {
            key: separator,
            child: right_id,
        }];
        let root_image =
            page_v4::build_internal(buffer.page_size(), None, None, root_leftmost, &entries)
                .map_err(|error| Self::map_build_error(new_root, error))?;
        let guard = buffer.create_page(self.page_key(new_root), &root_image)?;
        drop(guard);
        self.root.store(new_root.get(), Ordering::Release);
        Ok(())
    }

    fn find_leaf_path(
        &self,
        buffer: &BufferPool,
        key: &[u8],
    ) -> Result<(PageId, Vec<PageId>), BTreeError> {
        let mut current = self.root();
        let mut path = Vec::new();
        for _ in 0..MAX_ROUTING_DEPTH {
            let guard = buffer.pin(self.page_key(current))?;
            let bytes = guard.read()?;
            let page =
                PageRef::parse(bytes.as_ref()).map_err(|error| Self::page_error(current, error))?;
            if let Some(right) = page
                .follow_right(key)
                .map_err(|error| Self::page_error(current, error))?
            {
                current = right;
                continue;
            }
            if page.is_leaf() {
                return Ok((current, path));
            }
            let child = page
                .child_for_key(key)
                .map_err(|error| Self::page_error(current, error))?;
            path.push(current);
            current = child;
        }
        Err(BTreeError::RoutingDepthExceeded)
    }

    fn lookup_value(
        page_id: PageId,
        page: &PageRef<'_>,
        index: usize,
    ) -> Result<BTreeLookup, BTreeError> {
        match page
            .value(index)
            .map_err(|error| Self::page_error(page_id, error))?
        {
            PageValue::Inline(value) => Ok(BTreeLookup::Found(value.to_vec())),
            PageValue::Blob(pointer) => Ok(BTreeLookup::Blob(pointer)),
            PageValue::Tombstone => Ok(BTreeLookup::Deleted),
        }
    }

    fn allocate_page(&self) -> Result<PageId, BTreeError> {
        let mut observed = self.next_page.load(Ordering::Acquire);
        loop {
            if observed == RESERVED_PAGE_ID {
                return Err(BTreeError::PageIdExhausted);
            }
            let next = observed.checked_add(1).ok_or(BTreeError::PageIdExhausted)?;
            match self.next_page.compare_exchange_weak(
                observed,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(PageId::new(observed)),
                Err(actual) => observed = actual,
            }
        }
    }

    fn page_key(&self, page: PageId) -> PageKey {
        PageKey::new(self.descriptor.id(), page)
    }

    fn page_error(page: PageId, error: PageError) -> BTreeError {
        BTreeError::Corruption {
            page,
            reason: error.0,
        }
    }

    fn map_build_error(page: PageId, error: PageError) -> BTreeError {
        if error.0.contains("does not fit") || error.0.contains("slot length") {
            BTreeError::EntryTooLarge
        } else {
            Self::page_error(page, error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::{BTree, LookupResult};
    use crate::vnext::{ObjectAuthority, PageIo, StorageObjectId};
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

    fn descriptor(id: u64) -> StorageObjectDescriptor {
        StorageObjectDescriptor::new(StorageObjectId::new(id), ObjectAuthority::Authoritative)
    }

    #[test]
    fn native_tree_matches_reference_for_split_heavy_inserts() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(128, 1024, device).expect("buffer creates");
        let tree = BTreeObject::create(descriptor(41), &buffer).expect("tree creates");
        let mut reference = BTree::new();

        for number in 0..400u32 {
            let key = format!("key-{number:04}");
            let value = format!("value-{number:04}");
            tree.insert(&buffer, key.as_bytes(), value.as_bytes())
                .expect("vNext insert");
            reference
                .insert(key.as_bytes(), value.as_bytes())
                .expect("reference insert");
        }
        assert_ne!(tree.root(), PageId::new(0), "root should split");

        for number in 0..400u32 {
            let key = format!("key-{number:04}");
            let expected = match reference.lookup(key.as_bytes()).expect("reference lookup") {
                LookupResult::Found(value) => BTreeLookup::Found(value),
                other => panic!("unexpected reference result: {other:?}"),
            };
            assert_eq!(
                tree.lookup(&buffer, key.as_bytes()).expect("vNext lookup"),
                expected
            );
        }
    }

    #[test]
    fn range_follows_leaf_links_after_splits() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(64, 768, device).expect("buffer creates");
        let tree = BTreeObject::create(descriptor(43), &buffer).expect("tree creates");
        for number in 0..200u32 {
            let key = format!("k-{number:04}");
            tree.insert(&buffer, key.as_bytes(), key.as_bytes())
                .expect("insert");
        }
        let rows = tree
            .range(&buffer, b"k-0050", b"k-0075", 100)
            .expect("range succeeds");
        assert_eq!(rows.len(), 25);
        assert_eq!(rows.first().expect("first").0, b"k-0050");
        assert_eq!(rows.last().expect("last").0, b"k-0074");
    }

    #[test]
    fn delete_does_not_require_rebalance() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(16, 1024, device).expect("buffer creates");
        let tree = BTreeObject::create(descriptor(47), &buffer).expect("tree creates");
        tree.insert(&buffer, b"alpha", b"one").expect("insert");
        tree.insert(&buffer, b"beta", b"two").expect("insert");
        assert!(tree.delete(&buffer, b"alpha").expect("delete"));
        assert!(!tree.delete(&buffer, b"alpha").expect("repeat delete"));
        assert_eq!(
            tree.lookup(&buffer, b"alpha").expect("lookup"),
            BTreeLookup::NotFound
        );
        assert_eq!(
            tree.lookup(&buffer, b"beta").expect("lookup"),
            BTreeLookup::Found(b"two".to_vec())
        );
    }

    #[test]
    fn concurrent_unique_inserts_share_one_tree() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = Arc::new(BufferPool::new(256, 1024, device).expect("buffer creates"));
        let tree = Arc::new(BTreeObject::create(descriptor(53), &buffer).expect("tree creates"));
        let mut workers = Vec::new();
        for worker in 0..4u32 {
            let buffer = Arc::clone(&buffer);
            let tree = Arc::clone(&tree);
            workers.push(std::thread::spawn(move || {
                for number in 0..100u32 {
                    let key = format!("w{worker}-{number:04}");
                    tree.insert(&buffer, key.as_bytes(), key.as_bytes())
                        .expect("concurrent insert");
                }
            }));
        }
        for worker in workers {
            worker.join().expect("worker completes");
        }
        for worker in 0..4u32 {
            for number in [0u32, 25, 99] {
                let key = format!("w{worker}-{number:04}");
                assert_eq!(
                    tree.lookup(&buffer, key.as_bytes()).expect("lookup"),
                    BTreeLookup::Found(key.as_bytes().to_vec())
                );
            }
        }
    }
}
