//! Physical page reclamation and generation-transition ownership.
//!
//! `StorageEngine` remains the authority for the PMT, device, and mutable
//! generation state. This module owns the reclamation policy that derives
//! reusable offsets, stages interior relocation, and completes or reserves a
//! generation across the publication barrier.

use super::StorageEngine;
use crate::btree::{BTree, PAGE_SIZE};
use crate::buffer::PageCacheKey;
use crate::error::{Error, Result};
use crate::mvcc::{PMT, PageMapping};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

impl StorageEngine {
    /// Number of physical page slots available for safe reuse.
    pub fn reclaimable_page_count(&self) -> usize {
        self.free_offsets.len()
    }

    /// Return the physical slots selected by the next dirty-page flush.
    ///
    /// The caller persists these candidates before invoking `flush`, because
    /// a failed publication may have written a reused slot before its new
    /// manifest became authoritative.
    pub fn pending_reuse_offsets(&self) -> Vec<u64> {
        let dirty_pages = self
            .btree
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| self.btree.node(*page_id).is_some())
            .count();
        self.free_offsets
            .iter()
            .rev()
            .take(dirty_pages)
            .copied()
            .collect()
    }

    /// Install the physical offsets protected by retained root generations.
    ///
    /// Recomputing from the device extent is intentionally conservative: a
    /// newly retained root can protect pages that were previously considered
    /// free, while a released root becomes reusable at the next flush or
    /// explicit refresh boundary.
    pub fn set_protected_offsets(&mut self, protected_offsets: HashSet<u64>) -> Result<()> {
        *self
            .protected_offsets
            .lock()
            .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))? =
            protected_offsets;
        self.refresh_reclamation()
    }

    /// Refresh the physical reuse view after retention leases or device
    /// state changed without a normal mutation flush.
    pub fn refresh_reclamation(&mut self) -> Result<()> {
        self.refresh_reclaimable_offsets()
    }

    /// Return whether retention or read-view state invalidated the current
    /// free-slot view.
    pub fn reclamation_needs_refresh(&self) -> bool {
        self.reclamation_dirty.load(Ordering::Acquire)
    }

    pub(crate) fn reclamation_dirty_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.reclamation_dirty)
    }

    pub(super) fn protected_offsets_snapshot(&self) -> Result<HashSet<u64>> {
        let mut protected_offsets = self
            .protected_offsets
            .lock()
            .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))?
            .clone();
        let protected_pmts = self
            .protected_pmts
            .lock()
            .map_err(|_| Error::Corruption("storage PMT lease mutex is poisoned".into()))?
            .clone();
        for pmt in protected_pmts {
            protected_offsets.extend(pmt.iter().map(|(_, mapping)| mapping.offset));
        }
        Ok(protected_offsets)
    }

    fn refresh_reclaimable_offsets(&mut self) -> Result<()> {
        let device_size = self.device.size()?;
        let active_offsets: HashSet<_> =
            self.pmt.iter().map(|(_, mapping)| mapping.offset).collect();
        let protected_offsets = self.protected_offsets_snapshot()?;
        let rebuild_reserved_offsets = &self.rebuild_reserved_offsets;
        self.free_offsets = (0..device_size)
            .step_by(PAGE_SIZE)
            .filter(|offset| {
                !active_offsets.contains(offset)
                    && !protected_offsets.contains(offset)
                    && !rebuild_reserved_offsets.contains(offset)
            })
            .collect();
        self.next_offset = device_size;
        self.reclamation_dirty.store(false, Ordering::Release);
        Ok(())
    }

    /// Return the current data size and the size after trimming only trailing
    /// free physical page slots.
    pub fn reclaimable_tail_range(&self) -> Result<(u64, u64)> {
        let before = self.device.size()?;
        let page_size = PAGE_SIZE as u64;
        let mut after = before;
        let mut free_index = self.free_offsets.len();

        while after >= page_size && free_index > 0 {
            let offset = self.free_offsets[free_index - 1];
            if offset != after - page_size {
                break;
            }
            after -= page_size;
            free_index -= 1;
        }

        Ok((before, after))
    }

    /// Trim trailing free page slots after the manifest barrier is complete.
    ///
    /// The caller must first ensure both manifest slots name the active
    /// generation. This method never removes an active PMT mapping and does
    /// not move interior free slots.
    pub fn truncate_reclaimable_tail(&mut self) -> Result<(u64, u64)> {
        if !self.pending_reclaimed_offsets.is_empty() {
            return Err(Error::NeedsRecovery(
                "cannot truncate pages before generation publication".into(),
            ));
        }

        let (before, after) = self.reclaimable_tail_range()?;
        if after == before {
            return Ok((before, after));
        }

        self.device.truncate(after)?;
        let retained = self
            .free_offsets
            .len()
            .saturating_sub(((before - after) / PAGE_SIZE as u64) as usize);
        self.free_offsets.truncate(retained);
        self.next_offset = after;
        Ok((before, after))
    }

    /// Relocate active pages from high offsets into lower unprotected holes.
    ///
    /// This is the interior-compaction half of the maintenance protocol. It
    /// writes copies first and changes the in-memory PMT only after every
    /// copy has been synced. The caller must have mirrored the current
    /// manifest before invoking this method; if a write or sync fails, the
    /// old manifest and all of its source pages remain authoritative after
    /// reopen.
    pub fn relocate_interior_pages(&mut self) -> Result<usize> {
        self.relocate_interior_pages_with_limit(usize::MAX)
    }

    /// Report whether at least one active page can move into a lower hole.
    ///
    /// This planning-only check performs no page reads or writes. Maintenance
    /// uses it to avoid demanding publication-sidecar capacity when a compact
    /// call can only trim an already-free tail.
    pub fn has_relocatable_interior_page(&self) -> Result<bool> {
        if !self.pending_reclaimed_offsets.is_empty() {
            return Err(Error::NeedsRecovery(
                "cannot plan relocation before generation publication".into(),
            ));
        }

        let protected_offsets = self.protected_offsets_snapshot()?;
        let mut free_offsets = self.free_offsets.clone();
        free_offsets.sort_unstable();
        let mut active_pages: Vec<_> = self
            .pmt
            .iter()
            .filter(|(_, mapping)| !protected_offsets.contains(&mapping.offset))
            .map(|(_, mapping)| mapping.offset)
            .collect();
        active_pages.sort_unstable_by_key(|offset| std::cmp::Reverse(*offset));

        let mut free_index = 0;
        for source in active_pages {
            while free_index < free_offsets.len() && free_offsets[free_index] >= source {
                free_index += 1;
            }
            if free_index < free_offsets.len() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Relocate at most `max_pages` active pages into lower unprotected holes.
    ///
    /// The limit bounds the write set and temporary page-image memory for one
    /// maintenance generation. The caller may invoke this method repeatedly;
    /// each successful batch still requires the same manifest publication
    /// barrier before its source pages can be reused.
    pub fn relocate_interior_pages_with_limit(&mut self, max_pages: usize) -> Result<usize> {
        if !self.pending_reclaimed_offsets.is_empty() {
            return Err(Error::NeedsRecovery(
                "cannot relocate pages before generation publication".into(),
            ));
        }
        if max_pages == 0 {
            return Ok(0);
        }

        let protected_offsets = self.protected_offsets_snapshot()?;
        let mut free_offsets = self.free_offsets.clone();
        free_offsets.sort_unstable();

        let mut active_pages: Vec<(u64, PageMapping)> = self
            .pmt
            .iter()
            .filter(|(_, mapping)| !protected_offsets.contains(&mapping.offset))
            .map(|(page_id, mapping)| (page_id, *mapping))
            .collect();
        active_pages.sort_unstable_by_key(|(_, mapping)| std::cmp::Reverse(mapping.offset));

        let mut moves = Vec::new();
        let mut free_index = 0;
        for (page_id, mapping) in active_pages {
            if moves.len() >= max_pages {
                break;
            }
            while free_index < free_offsets.len() && free_offsets[free_index] >= mapping.offset {
                free_index += 1;
            }
            let Some(&target) = free_offsets.get(free_index) else {
                break;
            };
            free_index += 1;

            let node = self.read_node_from_pmt(&self.pmt, page_id)?;
            moves.push((page_id, mapping, target, *node.as_bytes()));
        }

        if moves.is_empty() {
            return Ok(0);
        }

        for (_, _, target, page) in &moves {
            self.device.write_page(*target, page)?;
            self.metrics
                .physical_page_writes
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .page_bytes_written
                .fetch_add(PAGE_SIZE as u64, Ordering::Relaxed);
        }
        self.device.sync()?;
        super::record_durability_sync();
        self.metrics.syncs.fetch_add(1, Ordering::Relaxed);

        let mut retired_offsets = Vec::with_capacity(moves.len());
        let mut retired_cache_keys = Vec::with_capacity(moves.len());
        let targets: HashSet<_> = moves.iter().map(|(_, _, target, _)| *target).collect();
        for (page_id, mapping, target, _) in moves {
            retired_offsets.push(mapping.offset);
            retired_cache_keys.push(PageCacheKey::new(page_id, mapping.version));
            Arc::make_mut(&mut self.pmt).insert(page_id, mapping.file_id, target);
        }
        self.free_offsets.retain(|offset| !targets.contains(offset));
        self.pending_reclaimed_offsets = retired_offsets;
        self.pending_reclaimed_cache_keys = retired_cache_keys;
        Ok(self.pending_reclaimed_offsets.len())
    }

    /// Make the previous generation's retired pages reusable.
    ///
    /// DB calls this only after the new manifest has been durably published.
    /// Before that point, the old generation may still be the authoritative
    /// root after a crash and its physical pages must remain untouched.
    pub fn complete_generation(&mut self) {
        let protected_offsets = self
            .protected_offsets_snapshot()
            .unwrap_or_else(|_| self.pending_reclaimed_offsets.iter().copied().collect());
        let reclaimed_pages = self
            .pending_reclaimed_offsets
            .iter()
            .filter(|offset| !protected_offsets.contains(offset))
            .count() as u64;
        self.metrics
            .reclaimed_pages
            .fetch_add(reclaimed_pages, Ordering::Relaxed);
        self.metrics.reclaimed_bytes.fetch_add(
            reclaimed_pages.saturating_mul(PAGE_SIZE as u64),
            Ordering::Relaxed,
        );
        self.free_offsets.extend(
            self.pending_reclaimed_offsets
                .drain(..)
                .filter(|offset| !protected_offsets.contains(offset)),
        );
        self.free_offsets.extend(
            self.rebuild_reserved_offsets
                .drain()
                .filter(|offset| !protected_offsets.contains(offset)),
        );
        self.free_offsets.sort_unstable();
        self.free_offsets.dedup();
        if let Ok(mut buffer) = self.buffer.lock() {
            for page_key in self.pending_reclaimed_cache_keys.drain(..) {
                let _ = buffer.evict_key(page_key);
            }
        } else {
            self.pending_reclaimed_cache_keys.clear();
        }
        self.lazy_root = (!self.pmt.is_empty()).then_some(self.btree.root_id());
        self.btree.clear_dirty();
    }

    /// Get mutable access to the page mapping table for recovery.
    pub fn pmt_mut(&mut self) -> &mut PMT {
        Arc::make_mut(&mut self.pmt)
    }

    /// Replace the active logical tree as part of a full mark-and-rebuild
    /// maintenance generation.
    ///
    /// The old PMT locations are reserved until the caller publishes the new
    /// manifest and invokes [`Self::complete_generation`]. If maintenance
    /// fails before that barrier, reopening selects the old PMT and the
    /// speculative pages remain unreachable rather than overwriting the old
    /// root.
    pub(crate) fn prepare_logical_rebuild(&mut self, btree: BTree) -> Result<()> {
        if !self.pending_reclaimed_offsets.is_empty()
            || !self.pending_reclaimed_cache_keys.is_empty()
        {
            return Err(Error::NeedsRecovery(
                "logical rebuild has an unpublished retired-page set".into(),
            ));
        }

        self.rebuild_reserved_offsets =
            self.pmt.iter().map(|(_, mapping)| mapping.offset).collect();
        Arc::make_mut(&mut self.pmt).clear();
        self.btree = btree;
        self.lazy_root = None;
        // The accumulated free-offset list must survive the rebuild: the
        // candidate tree allocates fresh page IDs with no prior mappings, so
        // clearing the list here would orphan every free slot above the old
        // tree and force unbounded data-file growth across repeated
        // maintenance cycles. The list stays disjoint from the new active
        // set because those offsets are all assigned at flush time.
        self.next_offset = self.device.size()?;
        Ok(())
    }
}
