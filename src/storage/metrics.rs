//! Storage-engine diagnostics and cumulative physical-work counters.
//!
//! `StorageEngine` owns the authoritative state and updates these counters at
//! the physical operation boundaries. This module owns their representation
//! and read-only projection so diagnostics do not grow the coordinator's
//! state-definition surface.

use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative physical work performed by one storage-engine handle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageMetrics {
    /// Logical page reads requested through the PMT-backed page seam.
    pub logical_page_reads: u64,
    /// Lazy lookups served by the parsed immutable-node cache.
    pub parsed_page_cache_hits: u64,
    /// Physical page reads issued to the data device.
    pub physical_page_reads: u64,
    /// Physical page writes completed on the data device.
    pub physical_page_writes: u64,
    /// Bytes read from the data device for page operations.
    pub page_bytes_read: u64,
    /// Bytes written to the data device for page operations.
    pub page_bytes_written: u64,
    /// Published generation flushes completed by this handle.
    pub generation_flushes: u64,
    /// Successful data-device sync calls.
    pub syncs: u64,
    /// Physical pages made reusable after a publication barrier.
    pub reclaimed_pages: u64,
    /// Bytes made reusable after a publication barrier.
    pub reclaimed_bytes: u64,
    /// Deterministic capacity preflight failures.
    pub capacity_preflight_failures: u64,
}

#[derive(Debug, Default)]
pub(super) struct StorageCounters {
    pub(super) logical_page_reads: AtomicU64,
    pub(super) parsed_page_cache_hits: AtomicU64,
    pub(super) physical_page_reads: AtomicU64,
    pub(super) physical_page_writes: AtomicU64,
    pub(super) page_bytes_read: AtomicU64,
    pub(super) page_bytes_written: AtomicU64,
    pub(super) generation_flushes: AtomicU64,
    pub(super) syncs: AtomicU64,
    pub(super) reclaimed_pages: AtomicU64,
    pub(super) reclaimed_bytes: AtomicU64,
    pub(super) capacity_preflight_failures: AtomicU64,
}

impl StorageCounters {
    pub(super) fn snapshot(&self) -> StorageMetrics {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        StorageMetrics {
            logical_page_reads: load(&self.logical_page_reads),
            parsed_page_cache_hits: load(&self.parsed_page_cache_hits),
            physical_page_reads: load(&self.physical_page_reads),
            physical_page_writes: load(&self.physical_page_writes),
            page_bytes_read: load(&self.page_bytes_read),
            page_bytes_written: load(&self.page_bytes_written),
            generation_flushes: load(&self.generation_flushes),
            syncs: load(&self.syncs),
            reclaimed_pages: load(&self.reclaimed_pages),
            reclaimed_bytes: load(&self.reclaimed_bytes),
            capacity_preflight_failures: load(&self.capacity_preflight_failures),
        }
    }
}
