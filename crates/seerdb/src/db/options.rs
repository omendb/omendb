//! Database configuration options.

use crate::blob::DEFAULT_BLOB_THRESHOLD;
use crate::btree::PAGE_SIZE;
use crate::error::{Error, Result};
use crate::storage::format::{CommitRecord, Lsn};

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

/// The durability class each publication sync uses.
///
/// On Linux both classes call the same fsync family and differ only
/// marginally. On macOS the difference is the whole performance story:
/// Rust std's `sync_data`/`sync_all` map to `F_FULLFSYNC`, a device
/// barrier measured at ~4.2 ms on this host, while a plain `fsync(2)`
/// flushes only to the volatile disk cache at ~0.03 ms. PostgreSQL's
/// macOS builds use plain fsync (or `open_datasync`), which is exactly
/// the KernelBarrier class: safe against process and kernel crash, not
/// against power loss on consumer SSDs that acknowledge flushes early.
///
/// The default keeps the stronger barrier: this engine never trades
/// correctness, and a deployment that can accept kernel-crash-only
/// durability (battery-backed storage, containers on managed hosts,
/// CI) opts in explicitly and sees the ~100x sync-latency difference.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncClass {
    /// Device barrier (macOS `F_FULLFSYNC`): survives power loss even on
    /// consumer SSDs. The strongest available class. Default.
    #[default]
    DeviceBarrier,
    /// Kernel-page-cache barrier (plain `fsync(2)`): survives process
    /// and kernel crash; on power loss the disk cache may lose the last
    /// writes. PostgreSQL's installed macOS default class.
    KernelBarrier,
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

    /// Ack batch commits after one group-synced WAL append, deferring page
    /// materialization and the authority frame to flush/checkpoint/close.
    /// Experimental: single-write `put`/`delete` semantics are unchanged
    /// (they were never durable without flush under default CoW policy);
    /// only envelope-group barriers gain the cheaper ack path.
    pub wal_first_commits: bool,

    /// Unmaterialized WAL bytes tolerated before an automatic synchronous
    /// materialization bounds crash-recovery replay work. Only meaningful
    /// with `wal_first_commits`. Replay costs ~20 us/op CPU, so 2 MiB keeps
    /// reopen under ~500 ms of replay at current throughput. Default:
    /// 2 MiB; 0 disables automatic materialization.
    pub wal_materialize_bytes: u64,

    /// Durability class for every publication sync (WAL, pages, PMT
    /// metadata, MVCC version store). See [`SyncClass`]. Default:
    /// `DeviceBarrier` (macOS `F_FULLFSYNC`, ~4.2 ms/sync on this host);
    /// `KernelBarrier` matches PostgreSQL's installed macOS class at
    /// ~0.03 ms/sync.
    pub sync_class: SyncClass,
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
            wal_first_commits: false,
            wal_materialize_bytes: 2 * 1024 * 1024,
            sync_class: SyncClass::default(),
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
        // One WAL segment's LSN stores a 32-bit byte offset. Keep the
        // configured pending budget below that representable boundary after
        // accounting for the segment marker and commit envelope.
        let framing = (4 + 1 + 8 + 4) as u64 + (4 + 1 + CommitRecord::SERIALIZED_SIZE + 4) as u64;
        if self.max_wal_bytes > Lsn::MAX_OFFSET.saturating_sub(framing) {
            return Err(Error::InvalidArgument(
                "max_wal_bytes exceeds the representable WAL LSN segment".into(),
            ));
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
