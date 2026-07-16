//! Buffer frame: a slot in the buffer pool that holds a page.

use crate::btree::node::PAGE_SIZE;
use crate::buffer::PageCacheKey;
use std::sync::Arc;
use std::time::Instant;

/// State of a buffer frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameState {
    /// Frame is empty and available for use.
    Free,
    /// Frame contains a page that has been read from disk.
    Clean,
    /// Frame contains a page that has been modified in memory.
    Dirty,
}

/// A buffer frame holding a single page.
///
/// Each frame has a fixed-size buffer (PAGE_SIZE bytes) and metadata
/// for the buffer manager to track state, access patterns, and pinning.
pub struct Frame {
    /// The page data buffer.
    pub data: Box<[u8; PAGE_SIZE]>,
    /// Logical page and physical version currently stored in this frame.
    pub page_key: Option<PageCacheKey>,
    /// Current state of the frame.
    pub state: FrameState,
    /// Number of active pins (concurrent readers/writers).
    pub pin_count: u32,
    /// Whether this frame is pinned (cannot be evicted).
    pub pinned: bool,
    /// Last access time for LRU eviction.
    pub last_access: Instant,
    /// Whether the frame has been accessed since the last eviction sweep.
    pub referenced: bool,
    /// Ownership token used to derive the number of live page guards.
    guard_token: Arc<()>,
}

impl Frame {
    /// Create a new empty frame.
    pub fn new_empty() -> Self {
        Self {
            data: Box::new([0u8; PAGE_SIZE]),
            page_key: None,
            state: FrameState::Free,
            pin_count: 0,
            pinned: false,
            last_access: Instant::now(),
            referenced: false,
            guard_token: Arc::new(()),
        }
    }

    /// Whether this frame is free (available for use).
    pub fn is_free(&self) -> bool {
        self.state == FrameState::Free
    }

    /// Whether this frame contains a dirty page.
    pub fn is_dirty(&self) -> bool {
        self.state == FrameState::Dirty
    }

    /// Whether an explicit pin or a live page guard prevents eviction.
    pub fn is_pinned(&self) -> bool {
        self.pinned || Arc::strong_count(&self.guard_token) > 1
    }

    /// Mark the frame as containing a clean page.
    pub fn mark_clean(&mut self) {
        self.state = FrameState::Clean;
    }

    /// Mark the frame as containing a dirty page.
    pub fn mark_dirty(&mut self) {
        self.state = FrameState::Dirty;
    }

    /// Pin the frame (prevent eviction).
    pub fn pin(&mut self) {
        self.pin_count += 1;
        self.pinned = true;
        self.last_access = Instant::now();
        self.referenced = true;
    }

    /// Acquire an RAII pin for a page guard.
    pub(crate) fn acquire_guard(&mut self) -> Arc<()> {
        self.last_access = Instant::now();
        self.referenced = true;
        Arc::clone(&self.guard_token)
    }

    /// Unpin the frame (allow eviction when pin_count reaches 0).
    pub fn unpin(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
        if self.pin_count == 0 {
            self.pinned = false;
        }
    }

    /// Load page data into this frame.
    pub fn load(&mut self, page_key: PageCacheKey, data: &[u8; PAGE_SIZE]) {
        self.page_key = Some(page_key);
        self.data.copy_from_slice(data);
        self.state = FrameState::Clean;
        self.last_access = Instant::now();
        self.referenced = true;
    }

    /// Clear the frame (make it free).
    pub fn clear(&mut self) {
        self.page_key = None;
        self.state = FrameState::Free;
        self.pin_count = 0;
        self.pinned = false;
        self.data.fill(0);
    }
}
