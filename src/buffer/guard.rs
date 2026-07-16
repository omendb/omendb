//! Page guards: RAII-based access control for buffer frames.
//!
//! Guards ensure that pages are properly pinned/unpinned and provide
//! controlled access to page data.

use std::sync::Arc;

/// Access level for a page guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardAccess {
    /// Read-only access (shared).
    Read,
    /// Read-write access (exclusive).
    Write,
}

/// RAII guard for a page in the buffer pool.
///
/// The guard holds a frame index, access level, and an ownership token. The
/// token keeps the frame pinned while the guard is alive and is released by
/// `Drop`, allowing multiple guards to coexist without borrowing the buffer
/// manager.
pub struct PageGuard {
    /// Index into the buffer pool's frame array.
    frame_index: usize,
    /// Page ID.
    page_id: u64,
    /// Access level.
    access: GuardAccess,
    /// Shared ownership token for the frame pin.
    pin_token: Option<Arc<()>>,
}

impl PageGuard {
    /// Create a new page guard.
    pub(crate) fn new(
        frame_index: usize,
        page_id: u64,
        access: GuardAccess,
        pin_token: Arc<()>,
    ) -> Self {
        Self {
            frame_index,
            page_id,
            access,
            pin_token: Some(pin_token),
        }
    }

    /// Get the frame index.
    pub fn frame_index(&self) -> usize {
        self.frame_index
    }

    /// Get the page ID.
    pub fn page_id(&self) -> u64 {
        self.page_id
    }

    /// Get the access level.
    pub fn access(&self) -> GuardAccess {
        self.access
    }

    /// Whether this guard has write access.
    pub fn is_writable(&self) -> bool {
        self.access == GuardAccess::Write
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        // Dropping the token is the corresponding unpin operation in the
        // frame. Taking it makes that ownership transition explicit.
        let _ = self.pin_token.take();
    }
}
