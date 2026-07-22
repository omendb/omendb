//! Database configuration options.

use crate::blob::DEFAULT_BLOB_THRESHOLD;

/// Physical layout used for separated values.
///
/// `WholeImage` is the compatibility default. `Segmented` stores immutable
/// record streams in separate files and publishes only a checksummed catalog
/// on each generation. Existing databases keep the layout selected by their
/// catalog, so changing this option never silently migrates a store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobStorageMode {
    /// Serialize the complete blob catalog and record streams into one image.
    WholeImage,
    /// Append records to immutable segment files and publish a small catalog.
    Segmented,
}

/// Configuration options for a database instance.
#[derive(Debug, Clone)]
pub struct Options {
    /// Buffer pool size in bytes. Default: 128MB.
    pub buffer_pool_size: usize,

    /// Page size in bytes. Default: 4096.
    pub page_size: usize,

    /// Blob separation threshold. Values larger than this stored in blob files. Default: 1024.
    pub blob_threshold: usize,

    /// Physical layout for newly created blob stores. Existing stores use the
    /// layout encoded by their on-disk catalog.
    pub blob_storage: BlobStorageMode,

    /// Use O_DIRECT on Linux (bypass page cache). Default: false for portability.
    pub use_odirect: bool,

    /// Sync writes to disk. Default: false (caller must call flush).
    pub sync_writes: bool,

    /// Maximum logical WAL bytes admitted for one pending generation,
    /// including its commit envelope. A fixed reservation extent is rounded
    /// from this limit in 1 MiB segments. Default: 64 MiB.
    pub max_wal_bytes: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            buffer_pool_size: 128 * 1024 * 1024, // 128MB
            page_size: 4096,
            blob_threshold: DEFAULT_BLOB_THRESHOLD,
            blob_storage: BlobStorageMode::WholeImage,
            use_odirect: false,
            sync_writes: false,
            max_wal_bytes: 64 * 1024 * 1024,
        }
    }
}

impl Options {
    /// Create options for testing (small buffer pool).
    pub fn for_test() -> Self {
        Self {
            buffer_pool_size: 4096 * 10, // 10 pages
            ..Default::default()
        }
    }
}
