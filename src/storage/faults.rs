//! Feature-gated device fault controls for storage recovery tests.
//!
//! The device remains the effect owner. This module only exposes the
//! `StorageEngine` test controls that route deterministic failures to that
//! device, keeping fault scheduling out of the production coordinator.

use super::StorageEngine;

impl StorageEngine {
    /// Inject one device sync failure for publication-boundary tests.
    pub fn inject_sync_failure(&self) {
        self.device.inject_sync_failure();
    }

    /// Inject one device page-write failure for publication-boundary tests.
    pub fn inject_write_failure(&self) {
        self.device.inject_write_failure();
    }

    /// Inject one failure after a complete device page write.
    pub fn inject_after_write_failure(&self) {
        self.device.inject_after_write_failure();
    }

    /// Inject one failure after the complete page generation is written but
    /// before its device durability sync.
    pub fn inject_page_range_sync_failure(&self) {
        self.device.inject_page_range_sync_failure();
    }

    /// Inject one final-write ENOSPC after a page write may have completed.
    pub fn inject_final_write_disk_full(&self) {
        self.device.inject_final_write_disk_full();
    }

    /// Inject one deterministic disk-full result for recovery tests.
    pub fn inject_disk_full(&self) {
        self.device.inject_disk_full();
    }

    /// Set a persistent device capacity limit for recovery tests.
    pub fn inject_capacity_limit(&self, capacity: u64) {
        self.device.inject_capacity_limit(capacity);
    }
}
