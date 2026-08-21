//! Dirty-page publication and capacity admission for [`StorageEngine`].
//!
//! The parent module remains the authority for the PMT, allocator, device,
//! and reclamation state. This module owns the lifecycle that turns a dirty
//! logical generation into durable page bytes and reports the retired state
//! back to the publication coordinator.

use super::{StorageEngine, capacity_preflight_error};
use crate::btree::{BTree, Node, PAGE_SIZE};
use crate::buffer::{GuardAccess, PageCacheKey};
use crate::error::{Error, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl StorageEngine {
    /// Admit the new data extent required by a logical rebuild before the
    /// active PMT is replaced. The candidate contains only newly allocated
    /// pages, so its flush starts at the current data-file end.
    pub fn preflight_rebuild_capacity(&self, candidate: &BTree) -> Result<()> {
        let page_count = candidate
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| candidate.node(*page_id).is_some())
            .count() as u64;
        if page_count == 0 {
            return Ok(());
        }
        let start = self.device.size()?;
        let required_end = start
            .checked_add(page_count.saturating_mul(PAGE_SIZE as u64))
            .ok_or(Error::DiskFull)?;
        self.device
            .check_write_capacity(required_end - PAGE_SIZE as u64)
            .map_err(capacity_preflight_error)?;
        self.device
            .reserve(required_end)
            .map_err(capacity_preflight_error)?;
        Ok(())
    }

    /// Flush all dirty pages to disk.
    ///
    /// Small generations keep staging images resident until the device sync,
    /// preserving the strongest buffer retry diagnostics. Larger generations
    /// stream one image at a time through the fixed-size pool; the logical
    /// B-tree remains dirty until publication, so a later write or sync error
    /// is still retryable and cannot make an uncommitted generation visible.
    pub fn flush(&mut self) -> Result<()> {
        // A lease may have been released since the last publication. Refresh
        // the allocator view before choosing a reusable physical slot only
        // when retention/read-view state made the cached view stale.
        if self.reclamation_needs_refresh() {
            self.refresh_reclamation()?;
        }
        self.flush_after_reclamation_refresh()
    }

    /// Flush after the caller has already refreshed the reclamation view and
    /// made a reuse decision from it. Keeping this boundary explicit avoids
    /// a second full data-file scan in the durable publication path.
    pub(crate) fn flush_after_reclamation_refresh(&mut self) -> Result<()> {
        // The bootstrap path rewrites the complete logical tree into a new
        // physical generation. It is coarse, but preserves the out-of-place
        // invariant needed by manifest publication.
        let dirty_page_ids = self.btree.dirty_page_ids();
        self.preflight_flush_capacity(&dirty_page_ids)?;
        let stream_writebacks = {
            let buffer = self.buffer_lock()?;
            dirty_page_ids.len() > buffer.capacity()
        };
        let mut retired_offsets = Vec::new();
        let mut retired_cache_keys = Vec::new();
        let pending_capacity = usize::from(!stream_writebacks) * dirty_page_ids.len();
        let mut pending_writebacks = Vec::with_capacity(pending_capacity);
        let mut pending_rekeys = Vec::with_capacity(pending_capacity);

        for page_id in dirty_page_ids {
            let node = self.btree.node(page_id);
            if let Some(node) = node {
                // B-tree mutations do not maintain the persisted checksum in
                // place. Rebuild a temporary node so every page written has a
                // checksum that covers its final bytes.
                let page = *node.as_bytes();
                let mut persisted_node = Node::from_bytes(Box::new(page)).ok_or_else(|| {
                    Error::Corruption(format!("invalid node page {page_id} before flush"))
                })?;
                persisted_node.update_checksum();

                // Every flush creates a new physical version. DB publishes
                // the resulting PMT only after all data is durable.
                if let Some(mapping) = self.pmt.get(page_id as u64) {
                    retired_offsets.push(mapping.offset);
                    retired_cache_keys.push(PageCacheKey::new(page_id as u64, mapping.version));
                } else {
                    #[cfg(test)]
                    eprintln!(
                        "[churn-probe] fresh allocation: page_id={page_id} free_len={} next_offset={}",
                        self.free_offsets.len(),
                        self.next_offset
                    );
                    let _ = &page_id;
                }
                let (offset, reuses_retired_slot) = match self.free_offsets.last() {
                    Some(&offset) => (offset, true),
                    None => (self.next_offset, false),
                };

                // Stage the page through the buffer manager, then write the
                // clean flushed image to the out-of-place device version.
                let page = *persisted_node.as_bytes();
                let pending_key = PageCacheKey::unversioned(page_id as u64);
                let writeback = {
                    let mut buffer = self.buffer_lock()?;
                    let guard = buffer.fetch_key(pending_key, &page, GuardAccess::Write)?;
                    buffer.frame_data_mut(&guard)?.copy_from_slice(&page);
                    drop(guard);
                    buffer.begin_writeback_key(pending_key)?.ok_or_else(|| {
                        Error::Buffer(format!("page {page_id} was not dirty after staging"))
                    })?
                };
                self.device.write_page(offset, writeback.data())?;
                if stream_writebacks {
                    let mut buffer = self.buffer_lock()?;
                    buffer.discard_writeback(writeback)?;
                } else {
                    pending_writebacks.push(writeback);
                }
                self.metrics
                    .physical_page_writes
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .page_bytes_written
                    .fetch_add(PAGE_SIZE as u64, Ordering::Relaxed);
                if reuses_retired_slot {
                    self.free_offsets.pop();
                } else {
                    self.next_offset += PAGE_SIZE as u64;
                }
                Arc::make_mut(&mut self.pmt).insert(page_id as u64, 0, offset);
                let version = self
                    .pmt
                    .get(page_id as u64)
                    .ok_or_else(|| Error::Corruption("PMT insertion was lost".into()))?
                    .version;
                if !stream_writebacks {
                    pending_rekeys.push((pending_key, PageCacheKey::new(page_id as u64, version)));
                }
            }
        }

        // Sync to ensure data is persisted.
        #[cfg(any(test, feature = "fault-injection"))]
        self.device.check_page_range_sync()?;
        self.device.sync()?;
        crate::storage::record_durability_sync();
        {
            let mut buffer = self.buffer_lock()?;
            for writeback in pending_writebacks {
                buffer.complete_writeback(writeback)?;
            }
            for (from, to) in pending_rekeys {
                buffer.rekey(from, to);
            }
        }
        self.metrics.syncs.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .generation_flushes
            .fetch_add(1, Ordering::Relaxed);
        self.pending_reclaimed_offsets = retired_offsets;
        self.pending_reclaimed_cache_keys = retired_cache_keys;

        Ok(())
    }

    /// Admit the full generation before beginning any physical page writes.
    ///
    /// This closes the deterministic capacity/ENOSPC boundary for the active
    /// page set. Reuse of retired slots is always admitted; only newly growing
    /// data-file slots can fail this preflight. Supported filesystems reserve
    /// new data extents with keep-size semantics before page I/O; a final
    /// filesystem ENOSPC during the write itself remains fenced/recoverable.
    fn preflight_flush_capacity(&self, dirty_page_ids: &[u32]) -> Result<()> {
        let mut free_index = self.free_offsets.len();
        let mut next_offset = self.next_offset;
        let mut required_data_end = None;
        for &page_id in dirty_page_ids {
            if self.btree.node(page_id).is_none() {
                continue;
            }
            let offset = if free_index > 0 {
                free_index -= 1;
                self.free_offsets[free_index]
            } else {
                let offset = next_offset;
                next_offset = next_offset
                    .checked_add(PAGE_SIZE as u64)
                    .ok_or(Error::DiskFull)?;
                required_data_end = Some(next_offset);
                offset
            };
            if let Err(error) = self.device.check_write_capacity(offset) {
                self.metrics
                    .capacity_preflight_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(capacity_preflight_error(error));
            }
        }
        if let Some(end) = required_data_end
            && let Err(error) = self.device.reserve(end)
        {
            self.metrics
                .capacity_preflight_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(capacity_preflight_error(error));
        }
        Ok(())
    }
}
