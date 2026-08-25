//! B-tree node format and ownership boundaries.
//!
//! A node is a fixed-size 4 KiB slotted page. The implementation is split by
//! responsibility:
//!
//! - [`access`] reads and searches the page without mutating it.
//! - [`mutation`] owns leaf inserts, replacements, deletes, and compaction.
//! - [`internal_mutation`] owns internal separator and child-pointer inserts.
//! - [`split`] owns page split and rebuild policy.
//! - [`page_format`] owns headers, serialization, validation, and checksums.
//!
//! The decoder remains compatible with the older prefix-compressed v2 entry
//! form. New mutations intentionally use self-contained keys until a bounded
//! restart-point compression scheme is implemented and benchmarked.

mod access;
mod internal_mutation;
mod mutation;
#[path = "node/page_format.rs"]
mod page_format;
mod split;

pub use page_format::{BLOB_POINTER_SIZE, BlobPointer, NodeHeader, PageType, Tombstone, ValueType};

/// Fixed physical page size in bytes for the current on-disk format.
pub const PAGE_SIZE: usize = 4096;

/// Maximum key length that fits in both a leaf entry and a promoted internal
/// separator, including one slot and the fixed page header.
pub const MAX_KEY_SIZE: usize = PAGE_SIZE - page_format::HEADER_SIZE - 4 - 4 - 8;

/// A B-tree node backed by a fixed-size page buffer.
///
/// The node manages its own memory layout using a slotted page design:
/// - Slots (fixed 4 bytes each) grow forward from the header
/// - Entries (variable-length) grow backward from the end of the page
/// - Free space sits in the middle
#[derive(Clone)]
pub struct Node {
    /// Raw page buffer (always PAGE_SIZE bytes).
    data: Box<[u8; PAGE_SIZE]>,
}

/// Reference to a value in a leaf node.
#[derive(Debug)]
pub enum ValueRef<'a> {
    /// Value stored inline in the page.
    Inline(&'a [u8]),
    /// Value stored in a blob file.
    Blob(BlobPointer),
    /// Key has been deleted.
    Tombstone,
}

/// Error from inserting into a node.
#[derive(Debug, Clone, thiserror::Error)]
pub enum InsertError {
    #[error("page is full")]
    PageFull,
    #[error("entry is too large for a page")]
    EntryTooLarge,
    #[error("wrong node type for this operation")]
    WrongNodeType,
    #[error("entry index {0} is out of bounds")]
    InvalidIndex(usize),
    #[error("replacement value size mismatch: expected {expected}, got {actual}")]
    ValueSizeMismatch { expected: usize, actual: usize },
    #[error("duplicate key at index {0}")]
    DuplicateKey(usize),
}

/// Error from splitting a node.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SplitError {
    #[error("node has too few keys to split")]
    TooFewKeys,
    #[error("node data corruption")]
    Corruption,
    #[error("failed to insert into new node")]
    InsertFailed,
}

#[cfg(test)]
#[path = "node_tests.rs"]
mod tests;
