//! Buffer pool manager.
//!
//! The buffer manager allocates a fixed pool of page frames and provides
//! methods to fetch, pin, and evict pages. It uses a simple clock algorithm
//! for eviction.

use crate::btree::node::PAGE_SIZE;
use crate::buffer::frame::Frame;
use crate::buffer::guard::{GuardAccess, PageGuard};
use std::collections::HashMap;
use std::fmt;

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
        }
    }
}

impl std::error::Error for BufferError {}

/// Statistics about the buffer pool.
#[derive(Debug, Clone, Default)]
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
    /// Total page writes (evictions of dirty pages).
    pub writes: u64,
    /// Cache hits.
    pub hits: u64,
}

/// Buffer pool manager.
///
/// Manages a fixed-size pool of page frames in memory. Pages are loaded
/// on demand and evicted using a clock algorithm when the pool is full.
pub struct BufferManager {
    /// Array of buffer frames.
    frames: Vec<Frame>,
    /// Map from page ID to frame index (for cache lookup).
    page_map: HashMap<u64, usize>,
    /// Clock hand for eviction (index into frames).
    clock_hand: usize,
    /// Statistics.
    stats: BufferStats,
}

impl BufferManager {
    /// Create a new buffer manager with the given capacity in bytes.
    ///
    /// The capacity is rounded down to a multiple of PAGE_SIZE.
    pub fn new(capacity_bytes: usize) -> Self {
        let num_frames = capacity_bytes / PAGE_SIZE;
        let frames = (0..num_frames).map(|_| Frame::new_empty()).collect();

        Self {
            frames,
            page_map: HashMap::new(),
            clock_hand: 0,
            stats: BufferStats {
                total_frames: num_frames,
                free_frames: num_frames,
                ..Default::default()
            },
        }
    }

    /// Number of frames in the buffer pool.
    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    /// Whether a page currently has a resident frame.
    pub fn is_resident(&self, page_id: u64) -> bool {
        self.page_map
            .get(&page_id)
            .is_some_and(|&frame_idx| !self.frames[frame_idx].is_free())
    }

    /// Get buffer pool statistics.
    pub fn stats(&self) -> BufferStats {
        let mut stats = self.stats.clone();
        stats.free_frames = self.frames.iter().filter(|frame| frame.is_free()).count();
        stats.pinned_frames = self.frames.iter().filter(|frame| frame.is_pinned()).count();
        stats.dirty_frames = self.frames.iter().filter(|frame| frame.is_dirty()).count();
        stats
    }

    /// Fetch a page into the buffer pool and return a guard.
    ///
    /// If the page is already in the pool, return a guard to it (cache hit).
    /// Otherwise, load it from the provided data and return a guard.
    ///
    /// The `access` parameter controls whether the guard allows writes.
    pub fn fetch(
        &mut self,
        page_id: u64,
        data: &[u8; PAGE_SIZE],
        access: GuardAccess,
    ) -> Result<PageGuard, BufferError> {
        // Check if the page is already in the pool.
        if let Some(&frame_idx) = self.page_map.get(&page_id) {
            let frame = &mut self.frames[frame_idx];
            if !frame.is_free() {
                let pin_token = frame.acquire_guard();
                self.stats.hits += 1;
                if access == GuardAccess::Write {
                    frame.mark_dirty();
                }
                return Ok(PageGuard::new(frame_idx, page_id, access, pin_token));
            }
        }

        // Find a free frame or evict one.
        let frame_idx = self.find_free_frame()?;
        // Cache miss successfully found a frame and is now a page read.
        self.stats.reads += 1;

        // Load the page into the frame.
        let frame = &mut self.frames[frame_idx];
        frame.load(page_id, data);
        let pin_token = frame.acquire_guard();
        if access == GuardAccess::Write {
            frame.mark_dirty();
        }

        // Update the page map.
        self.page_map.insert(page_id, frame_idx);
        self.stats.free_frames -= 1;

        Ok(PageGuard::new(frame_idx, page_id, access, pin_token))
    }

    /// Get a reference to the data in a frame.
    pub fn frame_data(&self, guard: &PageGuard) -> &[u8; PAGE_SIZE] {
        &self.frames[guard.frame_index()].data
    }

    /// Get a mutable reference to the data in a frame.
    pub fn frame_data_mut(&mut self, guard: &PageGuard) -> &mut [u8; PAGE_SIZE] {
        &mut self.frames[guard.frame_index()].data
    }

    /// Mark a page as dirty (has been modified).
    pub fn mark_dirty(&mut self, page_id: u64) {
        if let Some(&frame_idx) = self.page_map.get(&page_id) {
            self.frames[frame_idx].mark_dirty();
        }
    }

    /// Release an explicit pin on a page.
    ///
    /// Fetch guards are released by `PageGuard::drop`; this method only
    /// applies to callers that explicitly pinned a frame through the lower
    /// level `Frame` API.
    pub fn unpin(&mut self, page_id: u64) {
        if let Some(&frame_idx) = self.page_map.get(&page_id) {
            self.frames[frame_idx].unpin();
        }
    }

    /// Flush a dirty page to the provided buffer.
    ///
    /// Returns the page data if the page was dirty, None otherwise.
    pub fn flush(&mut self, page_id: u64) -> Option<Box<[u8; PAGE_SIZE]>> {
        if let Some(&frame_idx) = self.page_map.get(&page_id) {
            let frame = &mut self.frames[frame_idx];
            if frame.is_dirty() {
                let data = frame.data.clone();
                frame.mark_clean();
                self.stats.writes += 1;
                return Some(data);
            }
        }
        None
    }

    /// Flush all dirty pages.
    ///
    /// Returns a vector of (page_id, data) for all flushed pages.
    pub fn flush_all(&mut self) -> Vec<(u64, Box<[u8; PAGE_SIZE]>)> {
        let mut flushed = Vec::new();

        for (page_id, &frame_idx) in self.page_map.iter() {
            let frame = &mut self.frames[frame_idx];
            if frame.is_dirty() {
                let data = frame.data.clone();
                frame.mark_clean();
                self.stats.writes += 1;
                flushed.push((*page_id, data));
            }
        }

        flushed
    }

    /// Remove a page from the buffer pool.
    pub fn evict(&mut self, page_id: u64) -> Result<(), BufferError> {
        if let Some(frame_idx) = self.page_map.remove(&page_id) {
            let frame = &self.frames[frame_idx];
            if frame.is_pinned() {
                self.page_map.insert(page_id, frame_idx);
                return Err(BufferError::AllFramesPinned);
            }
            if frame.is_dirty() {
                self.page_map.insert(page_id, frame_idx);
                return Err(BufferError::DirtyPage(page_id));
            }
            self.frames[frame_idx].clear();
            self.stats.free_frames += 1;
        }
        Ok(())
    }

    /// Find a free frame or evict one using the clock algorithm.
    fn find_free_frame(&mut self) -> Result<usize, BufferError> {
        if self.frames.is_empty() {
            return Err(BufferError::EmptyPool);
        }

        // First, look for a free frame.
        for (i, frame) in self.frames.iter().enumerate() {
            if frame.is_free() {
                return Ok(i);
            }
        }

        // No free frames - use clock algorithm to evict.
        self.clock_evict()
    }

    /// Clock algorithm for eviction.
    ///
    /// Scans frames in a circular manner. If a frame is referenced,
    /// clear the reference bit and move on. Otherwise, evict it.
    fn clock_evict(&mut self) -> Result<usize, BufferError> {
        let len = self.frames.len();
        let mut dirty_page = None;

        // Two passes clear reference bits and then select an unreferenced
        // clean victim. Dirty pages are never discarded here.
        for _ in 0..len.saturating_mul(2) {
            let idx = self.clock_hand;
            self.clock_hand = (self.clock_hand + 1) % len;

            let frame = &mut self.frames[idx];

            // Skip pinned frames.
            if frame.is_pinned() {
                continue;
            }

            // If referenced, clear the bit and move on.
            if frame.referenced {
                frame.referenced = false;
                continue;
            }

            if frame.is_dirty() {
                if dirty_page.is_none() {
                    dirty_page = frame.page_id;
                }
                continue;
            }

            // A clean victim can be detached from the page map safely.
            if let Some(old_page_id) = frame.page_id {
                self.page_map.remove(&old_page_id);
            }
            frame.clear();
            self.stats.free_frames += 1;

            return Ok(idx);
        }

        match dirty_page {
            Some(page_id) => Err(BufferError::DirtyPage(page_id)),
            None => Err(BufferError::AllFramesPinned),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_manager_new() {
        let bm = BufferManager::new(4096 * 10); // 10 frames
        assert_eq!(bm.capacity(), 10);
        assert_eq!(bm.stats().total_frames, 10);
        assert_eq!(bm.stats().free_frames, 10);
    }

    #[test]
    fn test_fetch_and_hit() {
        let mut bm = BufferManager::new(4096 * 2);
        let data = [42u8; PAGE_SIZE];

        // First fetch - cache miss.
        let guard = bm.fetch(1, &data, GuardAccess::Read).unwrap();
        assert_eq!(bm.stats().reads, 1);
        assert_eq!(bm.stats().hits, 0);
        drop(guard);

        // Second fetch - cache hit.
        let guard = bm.fetch(1, &data, GuardAccess::Read).unwrap();
        assert_eq!(bm.stats().reads, 1);
        assert_eq!(bm.stats().hits, 1);
        drop(guard);
    }

    #[test]
    fn test_eviction() {
        let mut bm = BufferManager::new(4096 * 2); // Only 2 frames
        let data1 = [1u8; PAGE_SIZE];
        let data2 = [2u8; PAGE_SIZE];
        let data3 = [3u8; PAGE_SIZE];

        // Fill the buffer.
        let g1 = bm.fetch(1, &data1, GuardAccess::Read).unwrap();
        let g2 = bm.fetch(2, &data2, GuardAccess::Read).unwrap();
        assert_eq!(bm.stats().free_frames, 0);

        // Dropping guards allows eviction.
        drop(g1);
        drop(g2);

        // Fetch a new page - should evict one.
        let g3 = bm.fetch(3, &data3, GuardAccess::Read).unwrap();
        assert_eq!(bm.stats().reads, 3);
        drop(g3);
    }

    #[test]
    fn test_dirty_page() {
        let mut bm = BufferManager::new(4096);
        let data = [0u8; PAGE_SIZE];

        let guard = bm.fetch(1, &data, GuardAccess::Write).unwrap();
        bm.mark_dirty(guard.page_id());
        let flushed = bm.flush(guard.page_id());
        assert!(flushed.is_some());
        drop(guard);
    }

    #[test]
    fn test_flush_all() {
        let mut bm = BufferManager::new(4096 * 3);
        let data = [0u8; PAGE_SIZE];

        let g1 = bm.fetch(1, &data, GuardAccess::Write).unwrap();
        let g2 = bm.fetch(2, &data, GuardAccess::Write).unwrap();
        bm.mark_dirty(g1.page_id());
        bm.mark_dirty(g2.page_id());
        drop(g1);
        drop(g2);

        let flushed = bm.flush_all();
        assert_eq!(flushed.len(), 2);
    }

    #[test]
    fn test_guard_drop_releases_pin() {
        let mut bm = BufferManager::new(PAGE_SIZE);
        let data = [1u8; PAGE_SIZE];

        let guard = bm.fetch(1, &data, GuardAccess::Read).unwrap();
        assert_eq!(bm.stats().pinned_frames, 1);
        drop(guard);
        assert_eq!(bm.stats().pinned_frames, 0);

        let guard = bm.fetch(2, &data, GuardAccess::Read).unwrap();
        drop(guard);
    }

    #[test]
    fn test_fetch_refuses_pinned_pool() {
        let mut bm = BufferManager::new(PAGE_SIZE);
        let data = [0u8; PAGE_SIZE];
        let guard = bm.fetch(1, &data, GuardAccess::Read).unwrap();

        assert!(matches!(
            bm.fetch(2, &data, GuardAccess::Read),
            Err(BufferError::AllFramesPinned)
        ));
        drop(guard);
    }

    #[test]
    fn test_fetch_refuses_dirty_eviction_until_flush() {
        let mut bm = BufferManager::new(PAGE_SIZE);
        let data = [0u8; PAGE_SIZE];
        let guard = bm.fetch(1, &data, GuardAccess::Write).unwrap();
        drop(guard);

        assert!(matches!(
            bm.fetch(2, &data, GuardAccess::Read),
            Err(BufferError::DirtyPage(1))
        ));

        assert!(bm.flush(1).is_some());
        let guard = bm.fetch(2, &data, GuardAccess::Read).unwrap();
        drop(guard);
    }
}
