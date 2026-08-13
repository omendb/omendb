//! Database-level blob reclamation lifecycle.
//!
//! This module owns the maintenance orchestration that turns blob-level
//! deletion metadata into a durable physical generation. `BlobManager` owns
//! live records, pointers, and low-level file/catalog state; `DB` retains the
//! manifest barrier, page publication, capacity admission, and recovery fence.

use super::blob_layout::segmented_catalog_needs_consolidation;
use super::*;

impl DB {
    /// Run garbage collection on blob files.
    ///
    /// Pending mutations are published before reclaiming blobs so an older
    /// durable generation never loses a pointer. Fully dead files are removed
    /// directly; mixed files are compacted by rewriting active B-tree pointers
    /// into a new blob file and sweeping the old file after publication.
    ///
    /// Returns the number of entries reclaimed.
    pub fn gc(&mut self) -> Result<usize> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        self.flush()?;
        if !self
            .retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?
            .is_empty()
        {
            // The current retained read view is copy-backed, but retaining the
            // root still establishes the conservative physical contract. Do
            // not delete blob history until the lease is released.
            return Ok(0);
        }
        if self.blobs.has_reclaimable_files() {
            // Fully-dead file removal changes the active blob image without
            // publishing a new root. Fence the inactive manifest slot first;
            // otherwise it could still name records removed below and become
            // an invalid fallback after a torn newest-slot read.
            self.mirror_current_manifest()?;
            // Admission must precede removal from the in-memory catalog. The
            // current image is an upper bound for the compacted image, so a
            // successful reservation covers the subsequent atomic publish.
            self.admit_blob_image(None, None)?;
        }
        let mut reclaimed = self.blobs.gc();
        if reclaimed > 0 {
            let write_result = if self.blobs.is_segmented() {
                self.publish_blob_rewrite_generation().map(|()| 0)
            } else {
                let blob_path = self.path.join(BLOB_FILE);
                let blob_image = self.blobs.to_bytes();
                self.write_blob_image(&blob_path, &blob_image)
            };
            match write_result {
                Ok(bytes) => {
                    self.publication.blob_bytes_written =
                        self.publication.blob_bytes_written.saturating_add(bytes);
                }
                Err(error) => {
                    self.write_fenced = true;
                    return Err(error);
                }
            }
        }
        if !self.blobs.files_needing_gc().is_empty()
            || segmented_catalog_needs_consolidation(&self.blobs)
        {
            let rewritten = match self.rewrite_mixed_blob_files() {
                Ok(reclaimed) => reclaimed,
                Err(error) if matches!(&error, Error::CapacityPreflight) => return Err(error),
                Err(error) => {
                    self.write_fenced = true;
                    return Err(error);
                }
            };
            reclaimed = reclaimed.saturating_add(rewritten);
        }
        Ok(reclaimed)
    }

    /// Rewrite live blob values into a fresh file and publish their new
    /// pointers as one physical maintenance generation.
    ///
    /// Existing records remain in the candidate blob image but carry deletion
    /// metadata until the new manifest is durable. The prior blob image is
    /// kept under a recovery name across that boundary, so an interrupted
    /// rewrite restores the exact old root image. Once the new root is
    /// authoritative, a second sweep removes the fully dead old files without
    /// changing the logical tree again.
    fn rewrite_mixed_blob_files(&mut self) -> Result<usize> {
        self.engine.ensure_materialized()?;
        let end = vec![u8::MAX; MAX_KEY_SIZE + 1];
        let scan = self
            .engine
            .btree()
            .range_scan(&[], &end)
            .map_err(Error::from)?;

        let mut candidate_blobs = self.blobs.clone();
        candidate_blobs
            .begin_compaction_file()
            .ok_or(Error::DiskFull)?;
        candidate_blobs.mark_all_deleted();
        let mut candidate_tree = self.engine.btree().clone();
        let mut rewritten = 0usize;
        for entry in scan {
            let (key, result) = entry.map_err(Error::from)?;
            let LookupResult::Blob(pointer) = result else {
                continue;
            };
            let value = self.blobs.read(&pointer).ok_or_else(|| {
                Error::Corruption(format!(
                    "active B-tree blob pointer {}:{}:{} is unavailable",
                    pointer.file_id, pointer.offset, pointer.length
                ))
            })?;
            let replacement = candidate_blobs.append(&key, value.to_vec())?;
            candidate_tree
                .upsert_blob(&key, replacement)
                .map_err(Error::from)?;
            rewritten = rewritten.saturating_add(1);
        }
        if rewritten == 0 {
            return Ok(0);
        }

        let blob_bytes = Self::blob_publication_size(&candidate_blobs)?;
        let candidate_page_count = candidate_tree
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| candidate_tree.node(*page_id).is_some())
            .count();
        let candidate_data_bytes = u64::try_from(candidate_page_count)
            .map_err(|_| Error::DiskFull)?
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(Error::DiskFull)?;
        let metadata_bytes = Self::max_metadata_delta_bytes(
            candidate_tree.node_count(),
            candidate_tree.page_allocator(),
        )?;
        self.preflight_maintenance_capacity(candidate_data_bytes, metadata_bytes, blob_bytes)?;
        self.engine.preflight_artifact_capacity(blob_bytes)?;
        self.engine.preflight_rebuild_capacity(&candidate_tree)?;
        if !candidate_blobs.is_segmented() {
            self.reserve_blob_image(blob_bytes)?;
        }
        *self.engine.btree_mut() = candidate_tree;
        self.blobs = candidate_blobs;

        self.mirror_current_manifest()?;
        self.engine.flush()?;
        self.publish_blob_rewrite_generation()?;

        let reclaimed = if self.blobs.has_reclaimable_files() {
            self.admit_blob_image(None, None)?;
            let reclaimed = self.blobs.gc();
            if reclaimed > 0 {
                let blob_bytes = if self.blobs.is_segmented() {
                    self.publish_blob_rewrite_generation()?;
                    0
                } else {
                    let blob_path = self.path.join(BLOB_FILE);
                    let blob_image = self.blobs.to_bytes();
                    self.write_blob_image(&blob_path, &blob_image)?
                };
                self.publication.blob_bytes_written = self
                    .publication
                    .blob_bytes_written
                    .saturating_add(blob_bytes);
            }
            reclaimed
        } else {
            0
        };
        Ok(reclaimed)
    }

    /// Publish a blob-pointer rewrite without inventing a logical user
    /// commit. The data pages, PMT, and blob image are all selected by the new
    /// physical generation before its manifest becomes authoritative.
    pub(super) fn publish_blob_rewrite_generation(&mut self) -> Result<()> {
        let current = self
            .manifest
            .load_latest()?
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
        let generation_id = self.next_generation_id;
        let checkpoint_path = self
            .path
            .join(format!("seerdb.meta.{}", generation_id.get()));
        let checkpoint_bytes = self.save_generation_meta(&checkpoint_path, current)?;
        let meta_path = self.path.join(META_FILE);
        let legacy_meta_bytes = if meta_path.is_file() {
            0
        } else {
            Self::save_meta(&meta_path, self.engine.pmt(), self.engine.allocator())?
        };
        self.publication.metadata_bytes_written = self
            .publication
            .metadata_bytes_written
            .saturating_add(checkpoint_bytes)
            .saturating_add(legacy_meta_bytes);

        self.blobs.set_generation(generation_id.get());
        let blob_path = self.path.join(BLOB_FILE);
        let backup_path = self.path.join(BLOB_REWRITE_BACKUP_FILE);
        let had_blob_image = blob_path.is_file();
        if backup_path.exists() {
            fs::remove_file(&backup_path)?;
        }
        if had_blob_image {
            fs::rename(&blob_path, &backup_path)?;
            sync_directory(&self.path)?;
        }
        let blob_bytes = if self.blobs.is_segmented() {
            self.write_blob_segments()?
        } else {
            let blob_image = self.blobs.to_bytes();
            self.write_blob_image(&blob_path, &blob_image)?
        };
        self.publication.blob_bytes_written = self
            .publication
            .blob_bytes_written
            .saturating_add(blob_bytes);

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_AFTER_BLOB_REWRITE_IMAGE.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected failure after blob rewrite image").into());
        }

        let manifest = Manifest {
            generation_id,
            pmt_checkpoint_id: PmtCheckpointId::new(generation_id.get()),
            root_page_id: self.engine.btree().root_id() as u64,
            ..current
        };
        let mut manifest_history = self.manifest_history.clone();
        manifest_history
            .push(manifest)
            .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        let history_bytes = if self.path.join(MANIFEST_HISTORY_FILE).is_file() {
            self.append_manifest_history(manifest)?
        } else {
            let bytes = manifest_history
                .to_bytes()
                .ok_or_else(|| Error::Wal("manifest history is too large".into()))?;
            self.persist_manifest_history(&manifest_history)?;
            bytes.len() as u64
        };
        self.publication.history_bytes_written = self
            .publication
            .history_bytes_written
            .saturating_add(history_bytes);
        self.manifest_history = manifest_history;
        self.manifest.publish(manifest)?;
        self.publication.manifest_bytes_written = self
            .publication
            .manifest_bytes_written
            .saturating_add(MANIFEST_SLOT_SIZE as u64);
        self.finish_segmented_blob_publication_cleanup()?;
        self.engine.complete_generation();
        self.generation_id = generation_id;
        self.next_generation_id = GenerationId::new(
            generation_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        if !self.blobs.is_segmented() && had_blob_image {
            fs::remove_file(&backup_path)?;
            sync_directory(&self.path)?;
        }
        Ok(())
    }
}
