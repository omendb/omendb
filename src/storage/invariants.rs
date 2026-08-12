//! Runtime invariants for the storage coordinator.
//!
//! The buffer manager validates its frame/map representation. This module
//! validates the adjacent PMT and physical-generation relationships owned by
//! `StorageEngine`, without reading durable artifacts or deciding whether a
//! generation is authoritative.

use super::StorageEngine;
use crate::btree::PAGE_SIZE;
use crate::error::{Error, Result};
use std::collections::HashSet;

impl StorageEngine {
    /// Validate only relationships needed before an ordinary public handle
    /// operation. This path stays O(1); full PMT/cache scans belong to
    /// explicit verification because they would turn every point read into a
    /// database-wide maintenance pass.
    pub(crate) fn validate_handle_state(&self) -> Result<()> {
        if !self.next_offset.is_multiple_of(PAGE_SIZE as u64) {
            return Err(Error::Corruption(format!(
                "storage allocation frontier {} is not page aligned",
                self.next_offset
            )));
        }
        if self.pending_reclaimed_offsets.len() != self.pending_reclaimed_cache_keys.len() {
            return Err(Error::Corruption(
                "pending reclaimed offsets and cache keys have different lengths".into(),
            ));
        }
        Ok(())
    }

    /// Validate cheap handle-local relationships before a public operation.
    pub(crate) fn validate_runtime_state(&self) -> Result<()> {
        self.validate_handle_state()?;
        self.buffer_lock()?
            .validate_invariants()
            .map_err(Error::Corruption)?;

        let page_size = PAGE_SIZE as u64;
        let mut active_offsets = HashSet::with_capacity(self.pmt.len());
        for (page_id, mapping) in self.pmt.iter() {
            if mapping.file_id != 0 {
                return Err(Error::Corruption(format!(
                    "PMT page {page_id} references unsupported file {}",
                    mapping.file_id
                )));
            }
            if !mapping.offset.is_multiple_of(page_size) {
                return Err(Error::Corruption(format!(
                    "PMT page {page_id} has unaligned offset {}",
                    mapping.offset
                )));
            }
            if mapping.version == 0 {
                return Err(Error::Corruption(format!(
                    "PMT page {page_id} has a reserved zero version"
                )));
            }
            let end = mapping
                .offset
                .checked_add(page_size)
                .ok_or_else(|| Error::Corruption(format!("PMT page {page_id} offset overflows")))?;
            if end > self.next_offset {
                return Err(Error::Corruption(format!(
                    "PMT page {page_id} ends at {end} beyond allocation frontier {}",
                    self.next_offset
                )));
            }
            if !active_offsets.insert(mapping.offset) {
                return Err(Error::Corruption(format!(
                    "PMT maps multiple logical pages to physical offset {}",
                    mapping.offset
                )));
            }
        }

        validate_offset_list(
            "free",
            &self.free_offsets,
            page_size,
            self.next_offset,
            &active_offsets,
            true,
        )?;
        validate_offset_list(
            "pending reclaimed",
            &self.pending_reclaimed_offsets,
            page_size,
            self.next_offset,
            &active_offsets,
            false,
        )?;
        let mut rebuild_reserved_offsets: Vec<_> =
            self.rebuild_reserved_offsets.iter().copied().collect();
        rebuild_reserved_offsets.sort_unstable();
        validate_offset_list(
            "rebuild reserved",
            &rebuild_reserved_offsets,
            page_size,
            self.next_offset,
            &active_offsets,
            true,
        )?;

        Ok(())
    }
}

fn validate_offset_list(
    name: &str,
    offsets: &[u64],
    page_size: u64,
    next_offset: u64,
    active_offsets: &HashSet<u64>,
    require_sorted: bool,
) -> Result<()> {
    let mut previous = None;
    let mut seen = HashSet::with_capacity(offsets.len());
    for &offset in offsets {
        if !offset.is_multiple_of(page_size) {
            return Err(Error::Corruption(format!(
                "{name} offset {offset} is not page aligned"
            )));
        }
        let end = offset
            .checked_add(page_size)
            .ok_or_else(|| Error::Corruption(format!("{name} offset {offset} overflows")))?;
        if end > next_offset {
            return Err(Error::Corruption(format!(
                "{name} offset {offset} exceeds allocation frontier {next_offset}"
            )));
        }
        if !seen.insert(offset) {
            return Err(Error::Corruption(format!(
                "{name} offsets contain duplicate physical page {offset}"
            )));
        }
        if active_offsets.contains(&offset) {
            return Err(Error::Corruption(format!(
                "{name} offset {offset} is still active in the PMT"
            )));
        }
        if require_sorted
            && let Some(previous) = previous
            && offset <= previous
        {
            return Err(Error::Corruption(format!(
                "{name} offsets are not strictly increasing"
            )));
        }
        previous = Some(offset);
    }
    Ok(())
}
