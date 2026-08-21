//! Feature-gated failure-injection controls for recovery tests.
//!
//! The fault schedule is deliberately separate from normal DB coordination.
//! Artifact owners consume the thread-local seams, while this module exposes
//! the stable test-facing controls that arm those seams.

pub(super) use super::artifact_io::inject_atomic_rename_failure;
use super::*;
use std::cell::Cell;

thread_local! {
    pub(super) static FAIL_NEXT_ATOMIC_RENAME: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_WAL_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_WAL_AFTER_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_WAL_SYNC: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_WAL_AFTER_SYNC: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_AFTER_MANIFEST: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_WAL_TRUNCATE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_ATOMIC_SHORT_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_ATOMIC_TORN_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_AFTER_BLOB_REWRITE_IMAGE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_SYNC: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_AFTER_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_SHORT_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_TORN_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_CATALOG_AFTER_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_CATALOG_RENAME: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_CATALOG_SHORT_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_CATALOG_TORN_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_CATALOG_DELTA_SHORT_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_CATALOG_DELTA_TORN_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_BLOB_SEGMENT_PRUNE_AFTER_REMOVE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_META_LOG_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_REUSE_PUBLICATION: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_META_LOG_SYNC: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_META_LOG_SHORT_WRITE: Cell<bool> = const { Cell::new(false) };
    pub(super) static FAIL_NEXT_META_LOG_TORN_WRITE: Cell<bool> = const { Cell::new(false) };
}

impl DB {
    /// Inject one device sync failure for the feature-gated fault harness.
    pub fn inject_sync_failure(&self) {
        self.engine.inject_sync_failure();
    }

    /// Inject one device page-write failure for the feature-gated fault harness.
    pub fn inject_write_failure(&self) {
        self.engine.inject_write_failure();
    }

    /// Inject one failure after a complete page write and before publication.
    pub fn inject_after_write_failure(&self) {
        self.engine.inject_after_write_failure();
    }

    /// Inject one failure after the complete page generation is written but
    /// before its device durability sync.
    pub fn inject_page_range_sync_failure(&self) {
        self.engine.inject_page_range_sync_failure();
    }

    /// Inject one final-write ENOSPC after a page write may have completed.
    pub fn inject_final_write_disk_full(&self) {
        self.engine.inject_final_write_disk_full();
    }

    /// Inject one disk-full result for the feature-gated fault harness.
    pub fn inject_disk_full(&self) {
        self.engine.inject_disk_full();
    }

    /// Set a persistent device capacity limit for the feature-gated fault harness.
    pub fn inject_capacity_limit(&self, capacity: u64) {
        self.engine.inject_capacity_limit(capacity);
    }

    /// Inject one atomic artifact rename failure for the feature-gated fault
    /// harness. The next atomic publication on this thread fails before the
    /// rename, leaving the previous artifact available for recovery.
    pub fn inject_atomic_rename_failure(&self) {
        inject_atomic_rename_failure();
    }

    /// Inject one failure before the next WAL append.
    pub fn inject_wal_write_failure(&self) {
        FAIL_NEXT_WAL_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next WAL append but before its sync.
    pub fn inject_wal_after_write_failure(&self) {
        FAIL_NEXT_WAL_AFTER_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure at the next WAL sync boundary.
    pub fn inject_wal_sync_failure(&self) {
        FAIL_NEXT_WAL_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next WAL sync boundary.
    pub fn inject_wal_after_sync_failure(&self) {
        FAIL_NEXT_WAL_AFTER_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure at the next metadata log write boundary.
    pub fn inject_meta_log_write_failure(&self) {
        FAIL_NEXT_META_LOG_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure at the next metadata log sync boundary.
    pub fn inject_meta_log_sync_failure(&self) {
        FAIL_NEXT_META_LOG_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one short metadata-log frame write followed by a failure.
    pub fn inject_meta_log_short_write_failure(&self) {
        FAIL_NEXT_META_LOG_SHORT_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one torn metadata-log append (frame bytes plus trailing
    /// garbage) followed by a failure.
    pub fn inject_meta_log_torn_write_failure(&self) {
        FAIL_NEXT_META_LOG_TORN_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure at the authority-frame sync boundary.
    pub fn inject_manifest_sync_failure(&self) {
        FAIL_NEXT_META_LOG_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure before page write-back of a generation that reuses
    /// physical slots - the reuse-fencing boundary that the former safety
    /// mirror used to own.
    pub fn inject_manifest_mirror_sync_failure(&self) {
        FAIL_NEXT_REUSE_PUBLICATION.with(|failure| failure.set(true));
    }

    /// Inject one failure at the coalesced artifact-directory barrier before
    /// the next user manifest publication.
    pub fn inject_publication_directory_sync_failure(&self) {
        FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next manifest becomes authoritative.
    pub fn inject_after_manifest_failure(&self) {
        FAIL_NEXT_AFTER_MANIFEST.with(|failure| failure.set(true));
    }

    /// Inject one failure after the next WAL file is removed.
    pub fn inject_wal_truncate_failure(&self) {
        FAIL_NEXT_WAL_TRUNCATE.with(|failure| failure.set(true));
    }

    /// Inject one truncated atomic checkpoint image before manifest publish.
    pub fn inject_atomic_short_write_failure(&self) {
        FAIL_NEXT_ATOMIC_SHORT_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one checksum-corrupted atomic checkpoint image before manifest
    /// publish.
    pub fn inject_atomic_torn_write_failure(&self) {
        FAIL_NEXT_ATOMIC_TORN_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure after a mixed-blob rewrite image is durable but
    /// before its maintenance manifest is published.
    pub fn inject_after_blob_rewrite_image_failure(&self) {
        FAIL_NEXT_AFTER_BLOB_REWRITE_IMAGE.with(|failure| failure.set(true));
    }

    /// Inject one failure after a segmented blob suffix is durable but before
    /// its catalog is published.
    pub fn inject_blob_segment_after_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_AFTER_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one partial segmented blob suffix write.
    pub fn inject_blob_segment_short_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_SHORT_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one checksum-corrupted segmented blob suffix write.
    pub fn inject_blob_segment_torn_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_TORN_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure while syncing a segmented blob suffix.
    pub fn inject_blob_segment_sync_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure after a complete segmented-catalog write but before
    /// its sync. This covers delta append and full consolidation; reopen must
    /// treat resulting future state as non-authoritative and retry truncation.
    pub fn inject_blob_segment_catalog_after_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_AFTER_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure while syncing a segmented blob catalog temp file.
    pub fn inject_blob_segment_catalog_sync_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure after a segmented blob catalog temp file is synced
    /// but before it replaces the previous catalog.
    pub fn inject_blob_segment_catalog_rename_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_RENAME.with(|failure| failure.set(true));
    }

    /// Inject one truncated segmented blob catalog image before manifest
    /// publication.
    pub fn inject_blob_segment_catalog_short_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_SHORT_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one checksum-corrupted segmented blob catalog image before
    /// manifest publication.
    pub fn inject_blob_segment_catalog_torn_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_TORN_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one partial segmented catalog-delta append.
    pub fn inject_blob_segment_catalog_delta_short_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_DELTA_SHORT_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one checksum-corrupted segmented catalog-delta append.
    pub fn inject_blob_segment_catalog_delta_torn_write_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_CATALOG_DELTA_TORN_WRITE.with(|failure| failure.set(true));
    }

    /// Inject one failure after an unreferenced segmented blob file is removed.
    pub fn inject_blob_segment_prune_after_remove_failure(&self) {
        FAIL_NEXT_BLOB_SEGMENT_PRUNE_AFTER_REMOVE.with(|failure| failure.set(true));
    }
}
