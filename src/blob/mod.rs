//! Blob file management for KV separation.
//!
//! Large values (>blob_threshold) are stored in append-only blob files.
//! The B-tree stores blob pointers (file_id, offset, length) instead of
//! the actual values.

mod file;
mod manager;
mod record;

pub use file::BlobFile;
pub use manager::{BlobManager, BlobManagerError, DEFAULT_BLOB_THRESHOLD};
pub(crate) use record::BlobRecord;
