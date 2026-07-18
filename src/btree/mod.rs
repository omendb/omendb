//! B-tree data structure with out-of-place writes.
//!
//! Nodes are fixed-size (4KB default) pages containing sorted key-value pairs.
//! The reader accepts legacy prefix-compressed entries, while live mutations
//! use self-contained keys until restart-point compression is proven. Pages
//! are never updated in place — writes create new versions at different
//! locations tracked by the PMT.

pub(crate) mod node;
mod tree;

pub use node::{
    BLOB_POINTER_SIZE, BlobPointer, InsertError, MAX_KEY_SIZE, Node, NodeHeader, PAGE_SIZE,
    PageType, SplitError, Tombstone, ValueRef, ValueType,
};
pub use tree::{BTree, BTreeError, LookupResult, RangeScan};
