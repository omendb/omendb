//! Physical compaction lifecycle.
//!
//! This module owns the DB-level orchestration for reclaiming obsolete data
//! pages. The storage engine owns relocation and tail truncation mechanics;
//! `DB` owns the manifest barrier, maintenance publication, capacity
//! preflight, and recovery fencing around those operations.

use super::*;

impl DB {
    /// Reclaim data pages that are no longer referenced by either manifest
    /// slot.
    ///
    /// A pending generation is flushed first. Unprotected active pages are
    /// then copied from high offsets into lower interior holes, published as
    /// a maintenance generation, and finally followed by crash-safe tail
    /// truncation. Retained-root pages are never overwritten or reclaimed.
    pub fn compact(&mut self) -> Result<CompactionReport> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        let result = self.compact_inner(None);
        if result.is_err() && !matches!(&result, Err(Error::CapacityPreflight)) {
            // A maintenance failure can occur after the manifest barrier or
            // after the file length changed. Reopen is the only universally
            // safe way to reconstruct the active generation and allocator.
            self.write_fenced = true;
        }
        result
    }

    /// Reclaim data pages while bounding one maintenance generation.
    ///
    /// At most `max_relocated_pages` active pages are copied into lower
    /// unprotected holes in this call. A zero limit still trims an already
    /// reclaimable tail but performs no interior relocation. Callers can
    /// schedule repeated calls to keep maintenance latency and staging memory
    /// bounded without weakening the manifest publication barrier.
    pub fn compact_with_limit(&mut self, max_relocated_pages: usize) -> Result<CompactionReport> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        let result = self.compact_inner(Some(max_relocated_pages));
        if result.is_err() && !matches!(&result, Err(Error::CapacityPreflight)) {
            self.write_fenced = true;
        }
        result
    }

    /// Publish a PMT relocation without inventing a logical user commit.
    ///
    /// The caller must have written the relocated pages before publishing.
    /// A new generation ID makes the physical checkpoint authoritative while
    /// preserving commit identity and WAL digest.
    fn publish_compaction_generation(&mut self) -> Result<()> {
        let current = self
            .manifest_history
            .latest()
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
        let generation_id = self.next_generation_id;

        if self.blobs.is_segmented() {
            // A compaction generation changes the manifest-selected physical
            // root even when the blob pointers do not change. Advance the
            // segmented catalog frontier with an empty delta (or a bounded
            // consolidation) so the catalog can be validated against the
            // same root generation after reopen.
            self.blobs.set_generation(generation_id.get());
            let blob_bytes = self.write_blob_segments()?;
            self.publication.blob_bytes_written = self
                .publication
                .blob_bytes_written
                .saturating_add(blob_bytes);
        }

        let manifest = Manifest {
            generation_id,
            pmt_checkpoint_id: PmtCheckpointId::new(generation_id.get()),
            root_page_id: self.engine.btree().root_id() as u64,
            ..current
        };
        self.publish_authority_frame(manifest)?;

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_AFTER_MANIFEST.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected post-manifest failure").into());
        }

        self.finish_segmented_blob_publication_cleanup()?;
        self.engine.complete_generation();
        self.generation_id = generation_id;
        self.next_generation_id = GenerationId::new(
            generation_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        Ok(())
    }

    fn compact_inner(&mut self, max_relocated_pages: Option<usize>) -> Result<CompactionReport> {
        self.flush()?;
        self.engine.refresh_reclamation()?;

        let (before, _) = self.engine.reclaimable_tail_range()?;
        let has_reclaimable_pages = self.engine.reclaimable_page_count() > 0;
        let mut manifest_replicated = false;
        let mut relocated_pages = 0;
        if has_reclaimable_pages {
            let will_relocate =
                max_relocated_pages != Some(0) && self.engine.has_relocatable_interior_page()?;
            if will_relocate {
                let metadata_bytes = Self::max_metadata_delta_bytes(
                    self.engine.pmt().len(),
                    self.engine.allocator(),
                )?;
                let blob_bytes = if self.blobs.is_segmented() {
                    Self::blob_publication_size(&self.blobs)?
                } else {
                    0
                };
                self.preflight_maintenance_capacity(0, metadata_bytes, blob_bytes)?;
            }
            // Relocated interior pages are out-of-place copies; the old PMT
            // stays authoritative until the relocation frame publishes.
            manifest_replicated = true;
            relocated_pages = match max_relocated_pages {
                Some(limit) => self.engine.relocate_interior_pages_with_limit(limit)? as u64,
                None => self.engine.relocate_interior_pages()? as u64,
            };
            if relocated_pages > 0 {
                self.publish_compaction_generation()?;
            }
        }

        let (planned_before, planned_after) = self.engine.reclaimable_tail_range()?;
        let (actual_before, actual_after) = self.engine.truncate_reclaimable_tail()?;
        if actual_before != planned_before || actual_after != planned_after {
            return Err(Error::NeedsRecovery(
                "data file changed during compaction planning".into(),
            ));
        }

        Ok(CompactionReport {
            durability: self.durability_status(),
            data_bytes_before: before,
            data_bytes_after: actual_after,
            reclaimed_pages: (before - actual_after) / PAGE_SIZE as u64,
            relocated_pages,
            manifest_replicated,
        })
    }
}
