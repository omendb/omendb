//! Public buffer-pool contracts shared by frames, guards, and the manager.
//!
//! The manager owns cache state and eviction/write-back algorithms. This
//! module owns the stable identities, error taxonomy, diagnostics snapshot,
//! and detached write-back token exchanged at that boundary.

use crate::btree::node::PAGE_SIZE;
use std::fmt;

/// Identity of one page image in the buffer pool.
///
/// Logical page IDs are stable across out-of-place rewrites, so they are not
/// sufficient as a cache key. `physical_version` comes from the PMT and must
/// change whenever a new physical image is published. Version zero is
/// reserved for the transitional pending/unversioned staging API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCacheKey {
    logical_page_id: u64,
    physical_version: u64,
}

impl PageCacheKey {
    /// Construct an identity for a published PMT page image.
    pub const fn new(logical_page_id: u64, physical_version: u64) -> Self {
        Self {
            logical_page_id,
            physical_version,
        }
    }

    /// Construct the transitional key used by unversioned callers and
    /// pre-publication write staging.
    pub const fn unversioned(logical_page_id: u64) -> Self {
        Self::new(logical_page_id, 0)
    }

    /// Logical page ID component of this cache identity.
    pub const fn logical_page_id(self) -> u64 {
        self.logical_page_id
    }

    /// PMT physical version component of this cache identity.
    pub const fn physical_version(self) -> u64 {
        self.physical_version
    }
}

/// Errors raised when the buffer pool cannot safely satisfy a fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BufferError {
    /// The buffer pool has no frames.
    EmptyPool,
    /// Every frame is pinned by a live guard or an explicit pin.
    AllFramesPinned,
    /// An unpinned dirty frame cannot be discarded without a write-back
    /// callback from the storage engine.
    DirtyPage(u64),
    /// A write-back was attempted while a live guard or explicit pin owns the
    /// frame.
    PinnedPage(u64),
    /// Mutable frame access was requested through a read-only guard.
    ReadOnlyPage(u64),
    /// The frame changed after a write-back image was captured.
    StaleWriteback(u64),
}

impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPool => write!(f, "buffer pool has no frames"),
            Self::AllFramesPinned => write!(f, "all buffer frames are pinned"),
            Self::DirtyPage(page_id) => {
                write!(
                    f,
                    "dirty page {page_id} cannot be evicted before write-back"
                )
            }
            Self::PinnedPage(page_id) => {
                write!(f, "page {page_id} cannot be written back while pinned")
            }
            Self::ReadOnlyPage(page_id) => {
                write!(f, "page {page_id} cannot be mutated through a read guard")
            }
            Self::StaleWriteback(page_id) => {
                write!(f, "write-back image for page {page_id} is stale")
            }
        }
    }
}

impl std::error::Error for BufferError {}

/// Statistics about the buffer pool.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BufferStats {
    /// Total number of frames.
    pub total_frames: usize,
    /// Number of free frames.
    pub free_frames: usize,
    /// Number of pinned frames.
    pub pinned_frames: usize,
    /// Number of dirty frames.
    pub dirty_frames: usize,
    /// Total page reads (cache misses).
    pub reads: u64,
    /// Completed dirty page write-backs.
    pub writes: u64,
    /// Cache hits.
    pub hits: u64,
    /// Write-back tokens requested for dirty resident pages.
    pub writeback_requests: u64,
    /// Write-back requests or completions refused because their safety
    /// preconditions were not satisfied.
    pub writeback_refusals: u64,
    /// Successfully discarded cache copies after streamed device writes.
    pub writeback_discards: u64,
    /// Clock or explicit victim-selection attempts.
    pub eviction_attempts: u64,
    /// Victim selections refused because every candidate was pinned or dirty.
    pub eviction_refusals: u64,
    /// Successfully removed clean frames through eviction.
    pub evictions: u64,
}

/// A stable copy of a dirty frame awaiting durable write-back.
///
/// The frame remains dirty until [`crate::buffer::BufferManager::complete_writeback`]
/// is called after the storage engine confirms that the device write
/// succeeded. Dropping this value therefore preserves the dirty state on I/O
/// failure.
#[derive(Debug)]
pub struct Writeback {
    pub(super) page_key: PageCacheKey,
    pub(super) data: Box<[u8; PAGE_SIZE]>,
}

impl Writeback {
    /// The exact logical/physical page image being written.
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    /// The cache identity of the image being written.
    pub const fn page_key(&self) -> PageCacheKey {
        self.page_key
    }
}
