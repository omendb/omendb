//! Database configuration options.

use crate::blob::DEFAULT_BLOB_THRESHOLD;
use crate::btree::PAGE_SIZE;
use crate::error::{Error, Result};

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
///
/// The alpha on-disk format uses the fixed [`crate::PAGE_SIZE`] page size.
/// Page sizing is an engine format decision, not a per-database tuning
/// option; changing it requires a separately versioned format and buffer/I/O
/// implementation.
#[derive(Debug, Clone)]
pub struct Options {
    /// Buffer pool size in bytes. Default: 128MB.
    pub buffer_pool_size: usize,

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
            blob_threshold: DEFAULT_BLOB_THRESHOLD,
            blob_storage: BlobStorageMode::WholeImage,
            use_odirect: false,
            sync_writes: false,
            max_wal_bytes: 64 * 1024 * 1024,
        }
    }
}

impl Options {
    /// Validate configuration before opening or creating filesystem state.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.buffer_pool_size < PAGE_SIZE {
            return Err(Error::InvalidArgument(format!(
                "buffer_pool_size must be at least one page ({PAGE_SIZE} bytes)"
            )));
        }
        Ok(())
    }

    /// Create options for testing (small buffer pool).
    pub fn for_test() -> Self {
        Self {
            buffer_pool_size: 4096 * 10, // 10 pages
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PAGE_SIZE;

    #[test]
    fn alpha_page_format_is_fixed_and_buffer_default_is_aligned() {
        assert_eq!(PAGE_SIZE, 4096);
        assert_eq!(Options::default().buffer_pool_size % PAGE_SIZE, 0);
    }

    #[test]
    fn rejects_zero_frame_buffer_pool() {
        let options = Options {
            buffer_pool_size: PAGE_SIZE - 1,
            ..Options::default()
        };
        assert!(matches!(
            options.validate(),
            Err(Error::InvalidArgument(message)) if message.contains("at least one page")
        ));
    }
}
