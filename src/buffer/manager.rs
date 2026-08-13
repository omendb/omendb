//! Buffer pool manager.
//!
//! The buffer manager allocates a fixed pool of page frames and provides
//! methods to fetch, pin, and evict pages. It uses a simple clock algorithm
//! for eviction.

use super::types::{BufferError, BufferStats, PageCacheKey, Writeback};
use crate::btree::node::PAGE_SIZE;
use crate::buffer::frame::Frame;
use crate::buffer::guard::{GuardAccess, PageGuard};
use std::cell::Cell;
use std::collections::HashMap;

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
    /// Shared-access write-back diagnostics. BufferManager is normally
    /// protected by StorageEngine's mutex, while Cell preserves the existing
    /// shared-access write-back API without introducing contended atomics.
    writeback_requests: Cell<u64>,
    writeback_refusals: Cell<u64>,
}

fn increment_counter(counter: &Cell<u64>) {
    counter.set(counter.get().saturating_add(1));
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
            writeback_requests: Cell::new(0),
            writeback_refusals: Cell::new(0),
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
        stats.writeback_requests = self.writeback_requests.get();
        stats.writeback_refusals = self.writeback_refusals.get();
        stats
    }

    /// Validate the frame/map ownership contract without touching the device.
    ///
    /// `page_map` and each frame are one representation of the same cache
    /// state. Keeping this check here makes the buffer manager the authority
    /// for that relationship instead of making StorageEngine understand frame
    /// internals. The check is intentionally read-only so callers can use it
    /// before a public operation or after a failed write-back.
    pub(crate) fn validate_invariants(&self) -> std::result::Result<(), String> {
        if self.stats.total_frames != self.frames.len() {
            return Err(format!(
                "buffer statistics report {} frames but manager owns {}",
                self.stats.total_frames,
                self.frames.len()
            ));
        }
        if !self.frames.is_empty() && self.clock_hand >= self.frames.len() {
            return Err(format!(
                "buffer clock hand {} exceeds frame count {}",
                self.clock_hand,
                self.frames.len()
            ));
        }

        let mut mapped_frames = vec![false; self.frames.len()];
        for (&page_key, &frame_index) in &self.page_map {
            let Some(frame) = self.frames.get(frame_index) else {
                return Err(format!(
                    "cache key {:?} points outside frame pool at index {}",
                    page_key, frame_index
                ));
            };
            if mapped_frames[frame_index] {
                return Err(format!(
                    "multiple cache keys point to frame {}",
                    frame_index
                ));
            }
            if frame.is_free() {
                return Err(format!(
                    "cache key {:?} points to free frame {}",
                    page_key, frame_index
                ));
            }
            if frame.page_key != Some(page_key) {
                return Err(format!(
                    "frame {} key does not match cache map entry",
                    frame_index
                ));
            }
            mapped_frames[frame_index] = true;
        }

        let free_frames = self
            .frames
            .iter()
            .enumerate()
            .filter(|(index, frame)| frame.is_free() && !mapped_frames[*index])
            .count();
        if free_frames != self.stats.free_frames {
            return Err(format!(
                "buffer statistics report {} free frames but manager has {}",
                self.stats.free_frames, free_frames
            ));
        }

        for (index, frame) in self.frames.iter().enumerate() {
            if frame.is_free() {
                if frame.page_key.is_some() || frame.pin_count != 0 || frame.pinned {
                    return Err(format!("free frame {} retains page or pin state", index));
                }
            } else if frame.page_key.is_none() {
                return Err(format!("resident frame {} has no cache key", index));
            } else if frame.pinned != (frame.pin_count != 0) {
                return Err(format!(
                    "resident frame {} has inconsistent explicit pin metadata",
                    index
                ));
            }
        }

        Ok(())
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

    /// Get mutable access to a frame through a write guard.
    pub fn frame_data_mut(
        &mut self,
        guard: &PageGuard,
    ) -> Result<&mut [u8; PAGE_SIZE], BufferError> {
        if !guard.is_writable() {
            return Err(BufferError::ReadOnlyPage(guard.page_id()));
        }
        Ok(&mut self.frames[guard.frame_index()].data)
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
        increment_counter(&self.writeback_requests);
        if frame.is_pinned() {
            increment_counter(&self.writeback_refusals);
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
            increment_counter(&self.writeback_refusals);
            return Err(BufferError::StaleWriteback(page_id));
        };
        let frame = &mut self.frames[frame_idx];
        if frame.is_pinned() {
            increment_counter(&self.writeback_refusals);
            return Err(BufferError::PinnedPage(page_id));
        }
        if !frame.is_dirty() || frame.data.as_ref() != writeback.data.as_ref() {
            increment_counter(&self.writeback_refusals);
            return Err(BufferError::StaleWriteback(page_id));
        }
        frame.mark_clean();
        self.stats.writes += 1;
        Ok(())
    }

    /// Discard a successfully written staging image without treating the
    /// logical page as clean.
    ///
    /// Storage uses this bounded path when one generation contains more
    /// dirty pages than the pool has frames. The B-tree remains dirty until
    /// manifest publication; only the cache copy is removed so the next page
    /// can reuse the frame. A failed later write or sync therefore leaves the
    /// logical generation retryable while avoiding a dirty-frame eviction.
    pub fn discard_writeback(&mut self, writeback: Writeback) -> Result<(), BufferError> {
        let page_id = writeback.page_key.logical_page_id();
        let Some(&frame_idx) = self.page_map.get(&writeback.page_key) else {
            increment_counter(&self.writeback_refusals);
            return Err(BufferError::StaleWriteback(page_id));
        };
        let frame = &self.frames[frame_idx];
        if frame.is_pinned() {
            increment_counter(&self.writeback_refusals);
            return Err(BufferError::PinnedPage(page_id));
        }
        if !frame.is_dirty() || frame.data.as_ref() != writeback.data.as_ref() {
            increment_counter(&self.writeback_refusals);
            return Err(BufferError::StaleWriteback(page_id));
        }
        self.page_map.remove(&writeback.page_key);
        self.frames[frame_idx].clear();
        self.stats.free_frames += 1;
        self.stats.writeback_discards += 1;
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
        self.stats.eviction_attempts += 1;
        if let Some(frame_idx) = self.page_map.remove(&page_key) {
            let frame = &self.frames[frame_idx];
            if frame.is_pinned() {
                self.page_map.insert(page_key, frame_idx);
                self.stats.eviction_refusals += 1;
                return Err(BufferError::AllFramesPinned);
            }
            if frame.is_dirty() {
                self.page_map.insert(page_key, frame_idx);
                self.stats.eviction_refusals += 1;
                return Err(BufferError::DirtyPage(page_key.logical_page_id()));
            }
            self.frames[frame_idx].clear();
            self.stats.free_frames += 1;
            self.stats.evictions += 1;
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
        self.stats.eviction_attempts += 1;
        match self.clock_evict() {
            Ok(frame_idx) => Ok(frame_idx),
            Err(error) => {
                self.stats.eviction_refusals += 1;
                Err(error)
            }
        }
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
            self.stats.evictions += 1;

            return Ok(idx);
        }

        match dirty_page {
            Some(page_key) => Err(BufferError::DirtyPage(page_key.logical_page_id())),
            None => Err(BufferError::AllFramesPinned),
        }
    }
}

#[cfg(test)]
#[path = "manager_tests.rs"]
mod tests;
