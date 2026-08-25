//! B-tree routing, lookup, and range-scan surface.
//!
//! This module implements a B-tree over a sparse logical page store. Loaded
//! pages are owned in memory; an outer storage engine may fill missing pages
//! on demand before a mutation.
//!
//! # Design
//!
//! - **Insert**: Logical mutation entrypoints live in the sibling `mutation`
//!   module; full-page propagation lives in `split`
//! - **Lookup**: Route from root to leaf, then binary search at the leaf
//! - **Delete**: The mutation module finds the leaf and marks a tombstone
//! - **Range scan**: Cursor-based forward iteration
//!
//! # Out-of-Place Writes
//!
//! Mutations still modify loaded nodes directly, while StorageEngine owns the
//! page-loading and durable copy-on-write publication boundary. Split
//! propagation and logical-page allocation live in the sibling `split` module;
//! checked descent and leaf navigation live in the sibling `routing` module;
//! this file retains the B-tree state, lookup, and range surface.

use crate::allocator::PageAllocator;
use crate::btree::node::{InsertError, Node, SplitError, ValueRef};
use std::collections::HashSet;
use std::sync::Arc;

#[path = "tree/mutation.rs"]
mod mutation;
#[path = "tree/range.rs"]
mod range;
#[path = "tree/routing.rs"]
mod routing;
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

    /// Create a forward range scan over [start, end).
    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<RangeScan<'_>, BTreeError> {
        RangeScan::new(self, start.to_vec(), end.to_vec())
    }

    /// Create a resumable forward range cursor for bounded maintenance.
    pub(crate) fn range_cursor(&self, start: &[u8], end: &[u8]) -> Result<RangeCursor, BTreeError> {
        RangeCursor::new(self, start.to_vec(), end.to_vec())
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
