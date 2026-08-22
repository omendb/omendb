//! Durable root-generation publication coordinator.
//!
//! The coordinator owns publication ordering and timing, while the `DB`
//! handle remains the owner of all mutable state and lower-level artifact
//! helpers.

use super::*;

struct PublicationPreparation {
    parent_manifest: Manifest,
}

impl DB {
    /// Publish a generation after its pages and checkpoints are durable.
    pub(super) fn publish_generation(
        &mut self,
        commit: CommitRecord,
        append_commit: bool,
        recovered_wal_offset: u64,
    ) -> Result<()> {
        let preparation = self.prepare_generation_publication(commit)?;
        let wal_path = self.write_generation_artifacts(
            commit,
            append_commit,
            recovered_wal_offset,
            preparation.parent_manifest,
        )?;
        self.finish_generation_publication(commit, wal_path)
    }

    fn prepare_generation_publication(
        &mut self,
        commit: CommitRecord,
    ) -> Result<PublicationPreparation> {
        let parent_manifest = self
            .manifest_history
            .latest()
            .unwrap_or_else(|| self.bootstrap_manifest());
        if self.engine.reclamation_needs_refresh() {
            self.engine.refresh_reclamation()?;
        }
        let reuse_offsets = self.engine.pending_reuse_offsets();
        let reused_slots = !reuse_offsets.is_empty();
        let _ = reused_slots;
        // No safety mirror is required before page reuse: the authority log is
        // append-only, so recovery falls back to exactly the previous valid
        // publication frame, whose named offsets are excluded from the free
        // set by construction. The fault seam keeps covering this boundary.
        #[cfg(any(test, feature = "fault-injection"))]
        if reused_slots
            && super::faults::FAIL_NEXT_REUSE_PUBLICATION.with(|failure| failure.replace(false))
        {
            return Err(std::io::Error::other("injected reuse publication sync failure").into());
        }
        // Mutation records have already been written to the WAL by the
        // mutation or batch admission path. The commit envelope is appended
        // and forced only after the new out-of-place pages are durable below.
        // The manifest remains the visibility barrier, so forcing the
        // uncommitted mutation prefix before page write-back adds a sync
        // without strengthening recovery: an incomplete publication still
        // reopens the old root, while a durable commit record is enough to
        // replay the complete generation.
        self.write_wal_to_disk(false)?;

        let admission_started = Instant::now();
        self.preflight_publication_capacity()?;
        self.publication_timing.admission_ns = self
            .publication_timing
            .admission_ns
            .saturating_add(elapsed_nanos(admission_started));
        self.publication_timing.admission_ns = self
            .publication_timing
            .admission_ns
            .saturating_add(elapsed_nanos(admission_started));
        let flush_started = Instant::now();
        self.engine.set_write_generation(commit.generation_id.get());
        let flush_result = self.engine.flush_after_reclamation_refresh();
        self.publication_timing.data_flush_ns = self
            .publication_timing
            .data_flush_ns
            .saturating_add(elapsed_nanos(flush_started));
        if let Err(error) = flush_result {
            // Capacity preflight is guaranteed to issue no page I/O, so its
            // reservation can be removed and the mutation remains retryable.
            // Every other error leaves the reservation durable until reopen
            // proves whether this generation reached manifest history.
            let _ = error;
            return Err(error);
        }

        Ok(PublicationPreparation { parent_manifest })
    }

    fn write_generation_artifacts(
        &mut self,
        commit: CommitRecord,
        append_commit: bool,
        recovered_wal_offset: u64,
        parent_manifest: Manifest,
    ) -> Result<PathBuf> {
        let blob_started = Instant::now();
        let blob_bytes = if self.blobs.is_segmented() || self.pending_blob_changes {
            self.blobs.set_generation(commit.generation_id.get());
            if self.blobs.is_segmented() {
                self.write_blob_segments()?
            } else {
                let blob_path = self.path.join(BLOB_FILE);
                let blob_image = self.blobs.to_bytes();
                self.write_blob_image_without_directory_sync(&blob_path, &blob_image)?
            }
        } else {
            0
        };
        self.publication.blob_bytes_written = self
            .publication
            .blob_bytes_written
            .saturating_add(blob_bytes);
        self.publication_timing.blob_write_ns = self
            .publication_timing
            .blob_write_ns
            .saturating_add(elapsed_nanos(blob_started));

        let wal_path = self.path.join(WAL_FILE);
        let wal_offset = if append_commit {
            // The commit record must be durable before the publication frame
            // becomes visible: it completes the generation's mutation prefix.
            let offset = fs::metadata(&wal_path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            self.wal.append(&WalRecord::commit(commit));
            self.write_wal_to_disk(true)?;
            offset
        } else {
            recovered_wal_offset
        };

        let manifest = Manifest {
            database_id: self.database_id,
            history_id: self.history_id,
            generation_id: commit.generation_id,
            commit_id: commit.commit_id,
            page_size: PAGE_SIZE as u32,
            root_page_id: commit.root_page_id,
            pmt_checkpoint_id: PmtCheckpointId::new(commit.generation_id.get()),
            wal_segment: 0,
            wal_offset,
            mutation_count: commit.mutation_count,
            digest: commit.digest,
            format_version: FORMAT_VERSION,
        };
        let metadata_started = Instant::now();
        let (checkpoint_bytes, meta_log_created) = self.append_generation_meta(
            commit.generation_id.get(),
            parent_manifest.pmt_checkpoint_id.get(),
            &manifest.to_bytes(),
        )?;
        self.publication.metadata_bytes_written = self
            .publication
            .metadata_bytes_written
            .saturating_add(checkpoint_bytes);
        self.publication_timing.metadata_write_ns = self
            .publication_timing
            .metadata_write_ns
            .saturating_add(elapsed_nanos(metadata_started));

        // The publication frame IS the visibility barrier. A newly created
        // log file still needs its directory entry made durable before any
        // later ack; an existing log orders frames by its own file sync.
        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_PUBLICATION_DIRECTORY_SYNC.with(|failure| failure.replace(false)) {
            return Err(
                std::io::Error::other("injected publication directory sync failure").into(),
            );
        }
        if meta_log_created {
            let directory_started = Instant::now();
            let directory_result = sync_publication_directory(&self.path);
            self.publication_timing.directory_sync_ns = self
                .publication_timing
                .directory_sync_ns
                .saturating_add(elapsed_nanos(directory_started));
            directory_result?;
        }
        let mut manifest_history = self.manifest_history.clone();
        manifest_history
            .push(manifest)
            .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        self.manifest_history = manifest_history;

        #[cfg(any(test, feature = "fault-injection"))]
        if FAIL_NEXT_AFTER_MANIFEST.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected post-manifest failure").into());
        }

        Ok(wal_path)
    }

    fn finish_generation_publication(
        &mut self,
        commit: CommitRecord,
        wal_path: PathBuf,
    ) -> Result<()> {
        let cleanup_started = Instant::now();
        self.finish_segmented_blob_publication_cleanup()?;

        self.engine.complete_generation();

        // Retain the WAL across publications: the file's reservation stays
        // valid, so the next publication skips the reserve-and-directory
        // barrier entirely. Recovery makes retained records inert (commits at
        // or below the published generation are skipped or validated against
        // the manifest), so only unbounded growth needs bounding: once the
        // retained log passes the reclaim threshold, pay the removal and let
        // the next admission re-reserve.
        if wal_path.exists() {
            let wal_len = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
            if wal_len >= WAL_RETENTION_RECLAIM_BYTES {
                fs::remove_file(&wal_path)?;
                self.wal_reserved_extent = 0;
                // WAL removal is cleanup after the manifest has selected the
                // generation. If the directory entry removal is not durable, a
                // reopen sees the already-published commit and discards the stale
                // WAL; forcing that non-authoritative deletion would add one
                // directory sync to every successful publication.
            }
        }
        self.publication_timing.cleanup_ns = self
            .publication_timing
            .cleanup_ns
            .saturating_add(elapsed_nanos(cleanup_started));

        self.generation_id = commit.generation_id;
        self.commit_id = commit.commit_id;
        self.next_generation_id = GenerationId::new(
            self.next_generation_id.get().max(
                commit
                    .generation_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
            ),
        );
        self.next_commit_id = CommitId::new(
            self.next_commit_id.get().max(
                commit
                    .commit_id
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("commit ID overflow".into()))?,
            ),
        );
        self.pending_mutations = 0;
        self.pending_wal_bytes = 0;
        self.pending_digest = 0;
        self.pending_blob_changes = false;
        Ok(())
    }
}
