//! B-tree operations: insert, lookup, delete, range scan.
//!
//! This module implements a B-tree over a sparse logical page store. Loaded
//! pages are owned in memory; an outer storage engine may fill missing pages
//! on demand before a mutation.
//!
//! # Design
//!
//! - **Insert**: Find leaf, insert key-value, split if full, propagate splits up
//! - **Lookup**: Traverse from root to leaf, binary search at each level
//! - **Delete**: Find leaf, mark tombstone, merge if underfull
//! - **Range scan**: Cursor-based forward/reverse iteration
//!
//! # Out-of-Place Writes
//!
//! Mutations still modify loaded nodes directly, while StorageEngine owns the
//! page-loading and durable copy-on-write publication boundary. Split
//! propagation and logical-page allocation live in the sibling `split` module;
//! this file retains the public mutation and routing surface.

use crate::allocator::PageAllocator;
use crate::btree::node::{BlobPointer, InsertError, Node, SplitError, ValueRef};
use std::collections::HashSet;
use std::sync::Arc;

#[path = "tree/range.rs"]
mod range;
#[path = "tree/split.rs"]
mod split;

pub(crate) use range::RangeCursor;
pub use range::RangeScan;

/// Page ID type (index into the page store).
pub type PageId = u32;

/// Result of a lookup operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupResult {
    /// Key found with inline value.
    Found(Vec<u8>),
    /// Key found with blob pointer.
    Blob(crate::btree::node::BlobPointer),
    /// Key is deleted (tombstone).
    Deleted,
    /// Key not found.
    NotFound,
}

/// A simple in-memory B-tree.
///
/// This is the core B-tree logic. In the full implementation, this will be
/// backed by the buffer manager and PMT. Missing slots represent pages that
/// have not been loaded into this logical view yet. Cloning a tree shares
/// clean page buffers; the first mutation of a shared page creates a private
/// copy, which gives batch staging atomicity without a full deep clone.
#[derive(Clone)]
pub struct BTree {
    /// Loaded nodes indexed by stable logical page ID. `None` is an unloaded
    /// page in a sparse PMT-backed view.
    nodes: Vec<Option<Arc<Node>>>,
    /// Page ID of the root node.
    root: PageId,
    /// Logical pages that may have changed since the last published generation.
    dirty_pages: HashSet<PageId>,
    /// Single owner of stable logical page IDs used by this tree.
    page_allocator: PageAllocator,
}

impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

impl BTree {
    /// Create a new empty B-tree with a single leaf root.
    pub fn new() -> Self {
        let root = Node::new_leaf();
        Self {
            nodes: vec![Some(Arc::new(root))],
            root: 0,
            dirty_pages: HashSet::from([0]),
            page_allocator: PageAllocator::new(),
        }
    }

    /// Create a B-tree from existing nodes (for loading from disk).
    pub fn from_nodes(nodes: Vec<Node>, root: PageId) -> Self {
        Self::from_nodes_with_allocator(nodes, root, PageAllocator::new())
    }

    /// Create a B-tree from existing nodes and preserve its page allocator.
    pub fn from_nodes_with_allocator(
        nodes: Vec<Node>,
        root: PageId,
        mut page_allocator: PageAllocator,
    ) -> Self {
        page_allocator.advance_next_id(nodes.len() as u64);
        Self {
            nodes: nodes.into_iter().map(|node| Some(Arc::new(node))).collect(),
            root,
            dirty_pages: HashSet::new(),
            page_allocator,
        }
    }

    /// Create a sparse logical view with slots for a PMT page range.
    ///
    /// The slots are intentionally unloaded. StorageEngine fills only the
    /// pages needed by a mutation path and keeps the rest in the PMT-backed
    /// immutable generation.
    pub fn from_sparse_with_allocator(
        page_count: usize,
        root: PageId,
        mut page_allocator: PageAllocator,
    ) -> Self {
        page_allocator.advance_next_id(page_count as u64);
        Self {
            nodes: vec![None; page_count],
            root,
            dirty_pages: HashSet::new(),
            page_allocator,
        }
    }

    /// Replace the tree's logical page allocator during storage bootstrap.
    pub fn with_page_allocator(mut self, page_allocator: PageAllocator) -> Self {
        self.page_allocator = page_allocator;
        self.page_allocator.advance_next_id(self.nodes.len() as u64);
        self
    }

    /// Access the logical page allocator for durable checkpoints.
    pub fn page_allocator(&self) -> &PageAllocator {
        &self.page_allocator
    }

    /// Mutably access the logical page allocator.
    pub fn page_allocator_mut(&mut self) -> &mut PageAllocator {
        &mut self.page_allocator
    }

    /// Move the allocator out while replacing it with a fresh allocator.
    pub fn take_page_allocator(&mut self) -> PageAllocator {
        std::mem::take(&mut self.page_allocator)
    }

    /// Add a node at a specific page ID (for loading from disk).
    pub fn add_node(&mut self, node: Node, page_id: PageId) {
        let idx = page_id as usize;
        if idx >= self.nodes.len() {
            self.nodes.resize_with(idx + 1, || None);
        }
        self.nodes[idx] = Some(Arc::new(node));
        self.dirty_pages.remove(&page_id);
    }

    /// Get the root page ID.
    pub fn root_id(&self) -> PageId {
        self.root
    }

    /// Number of nodes in the tree.
    pub fn node_count(&self) -> usize {
        self.nodes.iter().flatten().count()
    }

    /// Return the logical pages changed since the last published generation.
    pub fn dirty_page_ids(&self) -> Vec<PageId> {
        let mut pages: Vec<_> = self.dirty_pages.iter().copied().collect();
        pages.sort_unstable();
        pages
    }

    /// Mark the current in-memory tree as clean after manifest publication.
    pub fn clear_dirty(&mut self) {
        self.dirty_pages.clear();
    }

    /// Return owned values for every dirty leaf entry in a key range.
    ///
    /// StorageEngine uses this to overlay sparse mutation pages over the
    /// immutable PMT-backed generation without loading unrelated leaves.
    pub fn dirty_leaf_entries(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, LookupResult)>, BTreeError> {
        let mut entries = Vec::new();
        let mut page_ids: Vec<_> = self.dirty_pages.iter().copied().collect();
        page_ids.sort_unstable();
        for page_id in page_ids {
            let Some(node) = self.node(page_id) else {
                return Err(BTreeError::MissingPage(page_id));
            };
            if !node.is_leaf() {
                continue;
            }
            for index in 0..node.count() {
                let key = node
                    .key(index)
                    .ok_or_else(|| BTreeError::Corruption("dirty leaf key is malformed".into()))?;
                if key.as_slice() < start || key.as_slice() >= end {
                    continue;
                }
                let value = match node.value(index) {
                    Some(ValueRef::Inline(value)) => LookupResult::Found(value.to_vec()),
                    Some(ValueRef::Blob(pointer)) => LookupResult::Blob(pointer),
                    Some(ValueRef::Tombstone) => LookupResult::Deleted,
                    None => {
                        return Err(BTreeError::Corruption(
                            "dirty leaf value payload is malformed".into(),
                        ));
                    }
                };
                entries.push((key, value));
            }
        }
        Ok(entries)
    }

    /// Get a reference to a node by page ID.
    pub fn node(&self, id: PageId) -> Option<&Node> {
        self.nodes
            .get(id as usize)
            .and_then(Option::as_ref)
            .map(Arc::as_ref)
    }

    /// Get a mutable reference to a node by page ID.
    fn node_mut(&mut self, id: PageId) -> Option<&mut Node> {
        let node = self
            .nodes
            .get_mut(id as usize)
            .and_then(Option::as_mut)
            .map(Arc::make_mut)?;
        self.dirty_pages.insert(id);
        Some(node)
    }

    /// Insert a key-value pair into the B-tree.
    ///
    /// If the key already exists, returns an error.
    /// May cause node splits if the leaf is full.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        let leaf_id = self.find_leaf(key)?;
        let result = self
            .node_mut(leaf_id)
            .ok_or(BTreeError::MissingPage(leaf_id))?
            .insert(key, value);

        match result {
            Ok(()) => Ok(()),
            Err(InsertError::PageFull) => {
                let snapshot = self.nodes.clone();
                let root = self.root;
                let dirty_pages = self.dirty_pages.clone();
                let page_allocator = self.page_allocator.clone();
                let result = self.split_and_insert_leaf(leaf_id, key, value);
                if result.is_err() {
                    self.nodes = snapshot;
                    self.root = root;
                    self.dirty_pages = dirty_pages;
                    self.page_allocator = page_allocator;
                }
                result
            }
            Err(InsertError::DuplicateKey(_)) => Err(BTreeError::DuplicateKey),
            Err(e) => Err(BTreeError::InsertFailed(e)),
        }
    }

    /// Insert or update a key-value pair (upsert).
    ///
    /// If the key already exists, replaces the value.
    /// May cause node splits if the leaf is full.
    pub fn upsert(&mut self, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        let leaf_id = self.find_leaf(key)?;
        let existing_index = self.node(leaf_id).and_then(|node| node.search(key).ok());

        let Some(index) = existing_index else {
            // A missing key follows the fully split-aware insert path.
            return self.insert(key, value);
        };

        let same_size_inline = matches!(
            self.node(leaf_id).and_then(|node| node.value(index)),
            Some(ValueRef::Inline(old_value)) if old_value.len() == value.len()
        );
        if same_size_inline {
            self.node_mut(leaf_id)
                .ok_or(BTreeError::MissingPage(leaf_id))?
                .replace_value(index, value)
                .map_err(BTreeError::InsertFailed)?;
            return Ok(());
        }

        let snapshot = self.nodes.clone();
        let root = self.root;
        let dirty_pages = self.dirty_pages.clone();
        let page_allocator = self.page_allocator.clone();
        let result = match self
            .node_mut(leaf_id)
            .ok_or(BTreeError::MissingPage(leaf_id))?
            .replace_value_resized(index, value)
        {
            Ok(()) => Ok(()),
            Err(InsertError::PageFull) => {
                self.replace_full_entry_with_split(leaf_id, index, key, value)
            }
            Err(error) => Err(BTreeError::InsertFailed(error)),
        };
        if result.is_err() {
            self.nodes = snapshot;
            self.root = root;
            self.dirty_pages = dirty_pages;
            self.page_allocator = page_allocator;
        }
        result
    }

    /// Lookup a key in the B-tree.
    pub fn lookup(&self, key: &[u8]) -> Result<LookupResult, BTreeError> {
        let leaf_id = self.find_leaf(key)?;
        let node = self
            .node(leaf_id)
            .ok_or_else(|| BTreeError::Corruption("leaf page is missing".into()))?;

        Ok(match node.search(key) {
            Ok(idx) => match node.value(idx) {
                Some(ValueRef::Inline(data)) => LookupResult::Found(data.to_vec()),
                Some(ValueRef::Blob(ptr)) => LookupResult::Blob(ptr),
                Some(ValueRef::Tombstone) => LookupResult::Deleted,
                None => {
                    return Err(BTreeError::Corruption(
                        "leaf value payload is malformed".into(),
                    ));
                }
            },
            Err(_) => LookupResult::NotFound,
        })
    }

    /// Delete a key by inserting a tombstone.
    ///
    /// Returns true if the key was found (even if already deleted).
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, BTreeError> {
        let leaf_id = self.find_leaf(key)?;
        let node = self
            .node_mut(leaf_id)
            .ok_or(BTreeError::MissingPage(leaf_id))?;

        let index = match node.search(key) {
            Ok(index) => index,
            Err(_) => return Ok(false),
        };
        if matches!(node.value(index), Some(ValueRef::Tombstone)) {
            return Ok(false);
        }

        node.insert_tombstone(key)
            .map_err(BTreeError::InsertFailed)?;
        Ok(true)
    }

    /// Insert a blob pointer for a key.
    ///
    /// This stores the blob pointer in the B-tree instead of the actual value.
    pub fn insert_blob(&mut self, key: &[u8], ptr: BlobPointer) -> Result<(), BTreeError> {
        let leaf_id = self.find_leaf(key)?;
        match self
            .node_mut(leaf_id)
            .ok_or(BTreeError::MissingPage(leaf_id))?
            .insert_blob(key, ptr)
        {
            Ok(()) => Ok(()),
            Err(InsertError::PageFull) => {
                let snapshot = self.nodes.clone();
                let root = self.root;
                let dirty_pages = self.dirty_pages.clone();
                let page_allocator = self.page_allocator.clone();
                let result = self.split_and_insert_blob_leaf(leaf_id, key, ptr);
                if result.is_err() {
                    self.nodes = snapshot;
                    self.root = root;
                    self.dirty_pages = dirty_pages;
                    self.page_allocator = page_allocator;
                }
                result
            }
            Err(error) => Err(BTreeError::InsertFailed(error)),
        }
    }

    /// Insert or replace a blob pointer for a key.
    pub fn upsert_blob(&mut self, key: &[u8], ptr: BlobPointer) -> Result<(), BTreeError> {
        let leaf_id = self.find_leaf(key)?;
        let Some(index) = self.node(leaf_id).and_then(|node| node.search(key).ok()) else {
            return match self
                .node_mut(leaf_id)
                .ok_or(BTreeError::MissingPage(leaf_id))?
                .insert_blob(key, ptr)
            {
                Ok(()) => Ok(()),
                Err(InsertError::PageFull) => {
                    let snapshot = self.nodes.clone();
                    let root = self.root;
                    let dirty_pages = self.dirty_pages.clone();
                    let page_allocator = self.page_allocator.clone();
                    let result = self.split_and_insert_blob_leaf(leaf_id, key, ptr);
                    if result.is_err() {
                        self.nodes = snapshot;
                        self.root = root;
                        self.dirty_pages = dirty_pages;
                        self.page_allocator = page_allocator;
                    }
                    result
                }
                Err(error) => Err(BTreeError::InsertFailed(error)),
            };
        };

        let snapshot = self.nodes.clone();
        let root = self.root;
        let dirty_pages = self.dirty_pages.clone();
        let page_allocator = self.page_allocator.clone();
        let result = (|| {
            self.node_mut(leaf_id)
                .ok_or(BTreeError::MissingPage(leaf_id))?
                .remove_entry(index)
                .map_err(BTreeError::InsertFailed)?;
            match self
                .node_mut(leaf_id)
                .ok_or(BTreeError::MissingPage(leaf_id))?
                .insert_blob(key, ptr)
            {
                Ok(()) => Ok(()),
                Err(InsertError::PageFull) => self.split_and_insert_blob_leaf(leaf_id, key, ptr),
                Err(error) => Err(BTreeError::InsertFailed(error)),
            }
        })();
        if result.is_err() {
            self.nodes = snapshot;
            self.root = root;
            self.dirty_pages = dirty_pages;
            self.page_allocator = page_allocator;
        }
        result
    }

    /// Create a forward range scan over [start, end).
    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<RangeScan<'_>, BTreeError> {
        RangeScan::new(self, start.to_vec(), end.to_vec())
    }

    /// Create a resumable forward range cursor for bounded maintenance.
    pub(crate) fn range_cursor(&self, start: &[u8], end: &[u8]) -> Result<RangeCursor, BTreeError> {
        RangeCursor::new(self, start.to_vec(), end.to_vec())
    }

    // -- Internal helpers --

    /// Find the leaf node where `key` should reside.
    fn find_leaf(&self, key: &[u8]) -> Result<PageId, BTreeError> {
        let mut current = self.root;
        let mut visited = HashSet::new();

        loop {
            if !visited.insert(current) {
                return Err(BTreeError::Corruption(
                    "cycle detected during B-tree descent".into(),
                ));
            }
            let node = self.node(current).ok_or(BTreeError::MissingPage(current))?;
            if node.is_leaf() {
                return Ok(current);
            }

            // Internal node: use an allocation-free upper-bound search over
            // its separator array. Equal separators route to the child on
            // their right.
            let child_id = node
                .child_for_key(key)
                .ok_or_else(|| BTreeError::Corruption("internal routing is malformed".into()))?;

            if child_id > u32::MAX as u64 {
                return Err(BTreeError::Corruption(
                    "internal child page ID exceeds the logical ID width".into(),
                ));
            }
            current = child_id as u32;
        }
    }

    /// Find a target leaf while validating every page and routing edge that
    /// the search visits. A cursor must not turn corruption into an ordinary
    /// end-of-stream condition.
    fn find_path_to_leaf_checked(
        &self,
        current: PageId,
        target: PageId,
        path: &mut Vec<(PageId, usize)>,
        active: &mut HashSet<PageId>,
    ) -> Result<bool, BTreeError> {
        if !active.insert(current) {
            return Err(BTreeError::Corruption(
                "cycle detected while locating range leaf".into(),
            ));
        }

        let result = (|| {
            let node = self
                .node(current)
                .ok_or_else(|| BTreeError::Corruption("range page is missing".into()))?;
            if node.is_leaf() {
                return Ok(current == target);
            }

            let leftmost = node.leftmost_child();
            if leftmost > u32::MAX as u64 {
                return Err(BTreeError::Corruption(
                    "internal child page ID exceeds the logical ID width".into(),
                ));
            }
            let mut children = Vec::with_capacity(node.count() + 1);
            children.push(leftmost as PageId);
            for index in 0..node.count() {
                let child = node
                    .child_id(index)
                    .ok_or_else(|| BTreeError::Corruption("internal child is malformed".into()))?;
                if child > u32::MAX as u64 {
                    return Err(BTreeError::Corruption(
                        "internal child page ID exceeds the logical ID width".into(),
                    ));
                }
                children.push(child as PageId);
            }

            for (position, child) in children.into_iter().enumerate() {
                path.push((current, position));
                if self.find_path_to_leaf_checked(child, target, path, active)? {
                    return Ok(true);
                }
                path.pop();
            }
            Ok(false)
        })();

        active.remove(&current);
        result
    }

    /// Descend through leftmost children, reporting malformed routing state.
    fn leftmost_leaf_checked(&self, mut current: PageId) -> Result<Option<PageId>, BTreeError> {
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(current) {
                return Err(BTreeError::Corruption(
                    "cycle detected while locating next range leaf".into(),
                ));
            }
            let node = self
                .node(current)
                .ok_or_else(|| BTreeError::Corruption("range page is missing".into()))?;
            if node.is_leaf() {
                return Ok(Some(current));
            }

            let next = node.leftmost_child();
            if next > u32::MAX as u64 {
                return Err(BTreeError::Corruption(
                    "internal child page ID exceeds the logical ID width".into(),
                ));
            }
            current = next as PageId;
        }
    }

    /// Find the leaf immediately to the right of `target`, validating the
    /// route and returning corruption instead of silently stopping a scan.
    fn next_leaf_from_parent_hint(
        &self,
        target: PageId,
        parent_hint: u32,
    ) -> Option<Option<PageId>> {
        let mut current = target;
        let mut visited = HashSet::new();

        loop {
            if !visited.insert(current) {
                return None;
            }

            let parent_id = if current == target {
                parent_hint
            } else {
                self.node(current)?.parent_id()
            };
            if parent_id == 0 {
                return (current == self.root).then_some(None);
            }

            let parent = self.node(parent_id as PageId)?;
            if !parent.is_internal() {
                return None;
            }
            let child_position = if parent.leftmost_child() == current as u64 {
                0
            } else {
                (0..parent.count())
                    .find(|&index| parent.child_id(index) == Some(current as u64))
                    .map(|index| index + 1)?
            };

            if child_position < parent.count() {
                let next_child = parent.child_id(child_position)?;
                let next_child = u32::try_from(next_child).ok()? as PageId;
                return self.leftmost_leaf_checked(next_child).ok();
            }
            current = parent_id as PageId;
        }
    }

    fn next_leaf_checked(
        &self,
        target: PageId,
        parent_hint: u32,
    ) -> Result<Option<PageId>, BTreeError> {
        if let Some(next_leaf) = self.next_leaf_from_parent_hint(target, parent_hint) {
            return Ok(next_leaf);
        }

        let mut path = Vec::new();
        let mut active = HashSet::new();
        if !self.find_path_to_leaf_checked(self.root, target, &mut path, &mut active)? {
            return Err(BTreeError::Corruption(
                "range leaf is not reachable from the root".into(),
            ));
        }

        for (parent_id, child_position) in path.into_iter().rev() {
            let parent = self
                .node(parent_id)
                .ok_or_else(|| BTreeError::Corruption("range parent page is missing".into()))?;
            if child_position < parent.count() {
                let next_child = parent
                    .child_id(child_position)
                    .ok_or_else(|| BTreeError::Corruption("internal child is malformed".into()))?;
                if next_child > u32::MAX as u64 {
                    return Err(BTreeError::Corruption(
                        "internal child page ID exceeds the logical ID width".into(),
                    ));
                }
                return self.leftmost_leaf_checked(next_child as PageId);
            }
        }
        Ok(None)
    }
}

/// Error from B-tree operations.
#[derive(Debug, thiserror::Error)]
pub enum BTreeError {
    #[error("duplicate key")]
    DuplicateKey,
    #[error("insert failed: {0}")]
    InsertFailed(InsertError),
    #[error("split failed: {0}")]
    SplitFailed(SplitError),
    #[error("logical page ID exhausted")]
    PageIdExhausted,
    #[error("B-tree page {0} is not loaded")]
    MissingPage(PageId),
    #[error("B-tree corruption: {0}")]
    Corruption(String),
}

#[cfg(test)]
#[path = "tree_tests.rs"]
mod tests;
