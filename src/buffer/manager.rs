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
}

/// A stable copy of a dirty frame awaiting durable write-back.
///
/// The frame remains dirty until [`BufferManager::complete_writeback`] is
/// called after the storage engine confirms that the device write succeeded.
/// Dropping this value therefore preserves the dirty state on I/O failure.
#[derive(Debug)]
pub struct Writeback {
    page_key: PageCacheKey,
    data: Box<[u8; PAGE_SIZE]>,
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

/// Buffer pool manager.
///
/// Manages a fixed-size pool of page frames in memory. Pages are loaded
/// on demand and evicted using a clock algorithm when the pool is full.
pub struct BufferManager {
    /// Array of buffer frames.
    frames: Vec<Frame>,
    /// Map from logical page plus physical version to frame index.
    page_map: HashMap<PageCacheKey, usize>,
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
        self.is_resident_key(PageCacheKey::unversioned(page_id))
    }

    /// Whether the exact physical page image is resident.
    pub fn is_resident_key(&self, page_key: PageCacheKey) -> bool {
        self.page_map
            .get(&page_key)
            .is_some_and(|&frame_idx| !self.frames[frame_idx].is_free())
    }

    /// Promote a clean pending image to the PMT version that now names it.
    ///
    /// The operation only changes cache identity; it never makes a page
    /// visible to recovery. Manifest publication remains the visibility
    /// barrier owned by the storage engine.
    pub fn rekey(&mut self, from: PageCacheKey, to: PageCacheKey) {
        if from == to {
            return;
        }
        let Some(frame_idx) = self.page_map.remove(&from) else {
            return;
        };
        if self.page_map.contains_key(&to) {
            self.page_map.insert(from, frame_idx);
            return;
        }
        self.frames[frame_idx].page_key = Some(to);
        self.page_map.insert(to, frame_idx);
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
        self.fetch_key(PageCacheKey::unversioned(page_id), data, access)
    }

    /// Fetch an exact logical/physical page image into the buffer pool.
    pub fn fetch_key(
        &mut self,
        page_key: PageCacheKey,
        data: &[u8; PAGE_SIZE],
        access: GuardAccess,
    ) -> Result<PageGuard, BufferError> {
        // Check if the page is already in the pool.
        if let Some(&frame_idx) = self.page_map.get(&page_key) {
            let frame = &mut self.frames[frame_idx];
            if !frame.is_free() {
                let pin_token = frame.acquire_guard();
                self.stats.hits += 1;
                if access == GuardAccess::Write {
                    frame.mark_dirty();
                }
                return Ok(PageGuard::new(frame_idx, page_key, access, pin_token));
            }
        }

        // Find a free frame or evict one.
        let frame_idx = self.find_free_frame()?;
        // Cache miss successfully found a frame and is now a page read.
        self.stats.reads += 1;

        // Load the page into the frame.
        let frame = &mut self.frames[frame_idx];
        frame.load(page_key, data);
        let pin_token = frame.acquire_guard();
        if access == GuardAccess::Write {
            frame.mark_dirty();
        }

        // Update the page map.
        self.page_map.insert(page_key, frame_idx);
        self.stats.free_frames -= 1;

        Ok(PageGuard::new(frame_idx, page_key, access, pin_token))
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
        self.mark_dirty_key(PageCacheKey::unversioned(page_id));
    }

    /// Mark an exact physical page image dirty.
    pub fn mark_dirty_key(&mut self, page_key: PageCacheKey) {
        if let Some(&frame_idx) = self.page_map.get(&page_key) {
            self.frames[frame_idx].mark_dirty();
        }
    }

    /// Release an explicit pin on a page.
    ///
    /// Fetch guards are released by `PageGuard::drop`; this method only
    /// applies to callers that explicitly pinned a frame through the lower
    /// level `Frame` API.
    pub fn unpin(&mut self, page_id: u64) {
        self.unpin_key(PageCacheKey::unversioned(page_id));
    }

    /// Release an explicit pin on an exact physical page image.
    pub fn unpin_key(&mut self, page_key: PageCacheKey) {
        if let Some(&frame_idx) = self.page_map.get(&page_key) {
            self.frames[frame_idx].unpin();
        }
    }

    /// Begin a safe write-back of a dirty page.
    ///
    /// The returned image is detached from the frame, but the frame remains
    /// dirty until [`Self::complete_writeback`] succeeds. This makes a device
    /// error retryable and prevents a failed write from becoming an apparent
    /// clean eviction. Live guards and explicit pins are rejected so a caller
    /// cannot mutate the frame after the image is captured.
    pub fn begin_writeback(&self, page_id: u64) -> Result<Option<Writeback>, BufferError> {
        self.begin_writeback_key(PageCacheKey::unversioned(page_id))
    }

    /// Begin a safe write-back of an exact physical page image.
    pub fn begin_writeback_key(
        &self,
        page_key: PageCacheKey,
    ) -> Result<Option<Writeback>, BufferError> {
        let Some(&frame_idx) = self.page_map.get(&page_key) else {
            return Ok(None);
        };
        let frame = &self.frames[frame_idx];
        if !frame.is_dirty() {
            return Ok(None);
        }
        if frame.is_pinned() {
            return Err(BufferError::PinnedPage(page_key.logical_page_id()));
        }
        Ok(Some(Writeback {
            page_key,
            data: frame.data.clone(),
        }))
    }

    /// Complete a write-back after the device has durably accepted its image.
    pub fn complete_writeback(&mut self, writeback: Writeback) -> Result<(), BufferError> {
        let page_id = writeback.page_key.logical_page_id();
        let Some(&frame_idx) = self.page_map.get(&writeback.page_key) else {
            return Err(BufferError::StaleWriteback(page_id));
        };
        let frame = &mut self.frames[frame_idx];
        if frame.is_pinned() {
            return Err(BufferError::PinnedPage(page_id));
        }
        if !frame.is_dirty() || frame.data.as_ref() != writeback.data.as_ref() {
            return Err(BufferError::StaleWriteback(page_id));
        }
        frame.mark_clean();
        self.stats.writes += 1;
        Ok(())
    }

    /// Begin write-back for all dirty pages.
    ///
    /// No frame is marked clean until each returned token is completed.
    pub fn begin_writeback_all(&self) -> Result<Vec<Writeback>, BufferError> {
        let mut keys: Vec<_> = self
            .page_map
            .iter()
            .filter_map(|(&page_key, &frame_idx)| {
                self.frames[frame_idx].is_dirty().then_some(page_key)
            })
            .collect();
        keys.sort_unstable_by_key(|key| (key.logical_page_id(), key.physical_version()));

        let mut writebacks = Vec::with_capacity(keys.len());
        for page_key in keys {
            if let Some(writeback) = self.begin_writeback_key(page_key)? {
                writebacks.push(writeback);
            }
        }
        Ok(writebacks)
    }

    /// Remove a page from the buffer pool.
    pub fn evict(&mut self, page_id: u64) -> Result<(), BufferError> {
        self.evict_key(PageCacheKey::unversioned(page_id))
    }

    /// Remove an exact physical page image from the buffer pool.
    pub fn evict_key(&mut self, page_key: PageCacheKey) -> Result<(), BufferError> {
        if let Some(frame_idx) = self.page_map.remove(&page_key) {
            let frame = &self.frames[frame_idx];
            if frame.is_pinned() {
                self.page_map.insert(page_key, frame_idx);
                return Err(BufferError::AllFramesPinned);
            }
            if frame.is_dirty() {
                self.page_map.insert(page_key, frame_idx);
                return Err(BufferError::DirtyPage(page_key.logical_page_id()));
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
                    dirty_page = frame.page_key;
                }
                continue;
            }

            // A clean victim can be detached from the page map safely.
            if let Some(old_page_key) = frame.page_key {
                self.page_map.remove(&old_page_key);
            }
            frame.clear();
            self.stats.free_frames += 1;

            return Ok(idx);
        }

        match dirty_page {
            Some(page_key) => Err(BufferError::DirtyPage(page_key.logical_page_id())),
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
    fn test_physical_version_is_part_of_cache_identity() {
        let mut bm = BufferManager::new(PAGE_SIZE * 2);
        let old = [1u8; PAGE_SIZE];
        let new = [2u8; PAGE_SIZE];

        let old_key = PageCacheKey::new(7, 11);
        let new_key = PageCacheKey::new(7, 12);
        let old_guard = bm.fetch_key(old_key, &old, GuardAccess::Read).unwrap();
        assert_eq!(bm.stats().reads, 1);
        drop(old_guard);

        let new_guard = bm.fetch_key(new_key, &new, GuardAccess::Read).unwrap();
        assert_eq!(bm.stats().reads, 2);
        assert_eq!(bm.stats().hits, 0);
        assert_eq!(bm.frame_data(&new_guard), &new);
        assert!(bm.is_resident_key(old_key));
        assert!(bm.is_resident_key(new_key));
        drop(new_guard);
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
        drop(guard);
        let writeback = bm.begin_writeback(1).unwrap().unwrap();
        assert_eq!(writeback.data(), &data);
        bm.complete_writeback(writeback).unwrap();
        assert_eq!(bm.stats().dirty_frames, 0);
    }

    #[test]
    fn test_writeback_all() {
        let mut bm = BufferManager::new(4096 * 3);
        let data = [0u8; PAGE_SIZE];

        let g1 = bm.fetch(1, &data, GuardAccess::Write).unwrap();
        let g2 = bm.fetch(2, &data, GuardAccess::Write).unwrap();
        bm.mark_dirty(g1.page_id());
        bm.mark_dirty(g2.page_id());
        drop(g1);
        drop(g2);

        let writebacks = bm.begin_writeback_all().unwrap();
        assert_eq!(writebacks.len(), 2);
        for writeback in writebacks {
            bm.complete_writeback(writeback).unwrap();
        }
        assert_eq!(bm.stats().dirty_frames, 0);
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

        let writeback = bm.begin_writeback(1).unwrap().unwrap();
        bm.complete_writeback(writeback).unwrap();
        let guard = bm.fetch(2, &data, GuardAccess::Read).unwrap();
        drop(guard);
    }

    #[test]
    fn test_writeback_refuses_live_guard_and_preserves_dirty_state() {
        let mut bm = BufferManager::new(PAGE_SIZE);
        let data = [0u8; PAGE_SIZE];
        let guard = bm.fetch(1, &data, GuardAccess::Write).unwrap();

        assert!(matches!(
            bm.begin_writeback(1),
            Err(BufferError::PinnedPage(1))
        ));
        drop(guard);

        let writeback = bm.begin_writeback(1).unwrap().unwrap();
        drop(writeback);
        assert_eq!(bm.stats().dirty_frames, 1);
        assert!(matches!(
            bm.fetch(2, &data, GuardAccess::Read),
            Err(BufferError::DirtyPage(1))
        ));
    }

    #[test]
    fn test_stale_writeback_cannot_clean_newer_frame_image() {
        let mut bm = BufferManager::new(PAGE_SIZE * 2);
        let data = [0u8; PAGE_SIZE];
        let guard = bm.fetch(1, &data, GuardAccess::Write).unwrap();
        drop(guard);
        let writeback = bm.begin_writeback(1).unwrap().unwrap();

        let guard = bm.fetch(1, &data, GuardAccess::Write).unwrap();
        bm.frame_data_mut(&guard)[0] = 1;
        drop(guard);

        assert!(matches!(
            bm.complete_writeback(writeback),
            Err(BufferError::StaleWriteback(1))
        ));
        assert_eq!(bm.stats().dirty_frames, 1);
    }
}
