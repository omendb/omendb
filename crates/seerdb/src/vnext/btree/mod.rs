//! Native ordered access method for storage-kernel vNext.
//!
//! The legacy v3 node codec remains in the old engine as a semantic oracle. The
//! vNext path uses its own page format and owns only object/root/allocation
//! metadata; page lifetime, translation, dirty state and I/O belong to the
//! shared buffer/storage kernel.

mod page_v4;
mod tree_v4;

pub use tree_v4::{BTreeError, BTreeLookup, BTreeObject};
