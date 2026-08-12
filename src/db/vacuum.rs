//! Resumable logical vacuum lifecycle.
//!
//! This module owns the process-local candidate state and bounded scan
//! lifecycle for logical vacuum. `DB` remains the authority for mutable
//! storage state and the final maintenance publication; the candidate is
//! private to one handle and is never authoritative before that publication.

use super::*;

pub(super) struct VacuumState {
    source_generation: GenerationId,
    source_commit: CommitId,
    cursor: RangeCursor,
    candidate_tree: BTree,
    candidate_blobs: BlobManager,
    scanned_entries: u64,
    live_entries: u64,
    logical_pages_before: u64,
}

impl DB {
    /// Rebuild the active tree from live entries and publish it as a new
    /// maintenance generation.
    ///
    /// This is the first complete logical reclamation primitive: tombstones
    /// and obsolete versions are omitted from the rebuilt tree, while the
    /// old PMT and blob image remain protected until the new manifest is
    /// authoritative. The unbounded convenience method drains the same
    /// resumable cursor used by [`DB::vacuum_step`].
    pub fn vacuum(&mut self) -> Result<VacuumReport> {
        self.check_writable()?;
        loop {
            let progress = self.vacuum_step(usize::MAX)?;
            if progress.complete {
                return Ok(VacuumReport {
                    durability: progress.durability,
                    live_entries: progress.live_entries,
                    logical_pages_before: progress.logical_pages_before,
                    logical_pages_after: progress.logical_pages_after.ok_or_else(|| {
                        Error::Corruption("completed vacuum has no page count".into())
                    })?,
                });
            }
        }
    }

    /// Advance logical reclamation in a bounded call.
    ///
    /// The candidate tree remains private to this handle and is published
    /// only on the final step. A crash or explicit cancellation therefore
    /// leaves the previous manifest and blob image authoritative. The writer
    /// lane remains reserved while a step is pending, so mutations and other
    /// maintenance calls are rejected until completion or cancellation.
    pub fn vacuum_step(&mut self, max_entries: usize) -> Result<VacuumProgress> {
        self.check_writable()?;
        if max_entries == 0 {
            return Err(Error::InvalidArgument(
                "vacuum step must process at least one entry".into(),
            ));
        }
        if self.vacuum.is_none() {
            self.start_vacuum()?;
        }

        let mut state = self.vacuum.take().ok_or_else(|| {
            Error::Corruption("vacuum state disappeared after initialization".into())
        })?;
        if state.source_generation != self.generation_id || state.source_commit != self.commit_id {
            return Err(Error::NeedsRecovery(
                "vacuum source generation changed before publication".into(),
            ));
        }

        let mut complete = false;
        for _ in 0..max_entries {
            let next = state.cursor.next(self.engine.btree());
            let Some(entry) = next else {
                complete = true;
                break;
            };
            let (key, result) = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    return Err(error.into());
                }
            };
            let value = match result {
                LookupResult::Found(value) => value,
                LookupResult::Blob(pointer) => self
                    .blobs
                    .read(&pointer)
                    .map(|value| value.to_vec())
                    .ok_or_else(|| {
                        Error::Corruption(format!(
                            "active B-tree blob pointer {}:{}:{} is unavailable",
                            pointer.file_id, pointer.offset, pointer.length
                        ))
                    })?,
                LookupResult::Deleted | LookupResult::NotFound => continue,
            };
            if state.candidate_blobs.should_separate(value.len()) {
                let pointer = state.candidate_blobs.append(&key, value);
                state.candidate_tree.upsert_blob(&key, pointer)?;
            } else {
                state.candidate_tree.upsert(&key, &value)?;
            }
            state.scanned_entries = state.scanned_entries.saturating_add(1);
            state.live_entries = state.live_entries.saturating_add(1);
        }

        if !complete {
            let progress = self.vacuum_progress(&state, false);
            self.vacuum = Some(state);
            return Ok(progress);
        }

        let candidate_blob_bytes = Self::blob_publication_size(&state.candidate_blobs)?;
        let candidate_page_count =
            u64::try_from(state.candidate_tree.node_count()).map_err(|_| Error::DiskFull)?;
        let candidate_data_bytes = candidate_page_count
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(Error::DiskFull)?;
        let candidate_metadata_bytes =
            Self::full_metadata_bytes_for_candidate(&state.candidate_tree)?;
        if let Err(error) = self.preflight_maintenance_capacity(
            candidate_data_bytes,
            candidate_metadata_bytes,
            candidate_blob_bytes,
        ) {
            self.vacuum = Some(state);
            return Err(error);
        }
        if let Err(error) = self.engine.check_artifact_capacity(candidate_blob_bytes) {
            self.vacuum = Some(state);
            return Err(error);
        }
        if let Err(error) = if state.candidate_blobs.is_segmented() {
            Ok(())
        } else {
            self.reserve_blob_image(candidate_blob_bytes)
        } {
            self.vacuum = Some(state);
            return Err(error);
        }
        if let Err(error) = self
            .engine
            .preflight_rebuild_capacity(&state.candidate_tree)
        {
            self.vacuum = Some(state);
            return Err(error);
        }

        let result = self.finish_vacuum(state);
        if result.is_err() {
            self.write_fenced = true;
        }
        let report = result?;
        Ok(VacuumProgress {
            durability: report.durability,
            scanned_entries: report.live_entries,
            live_entries: report.live_entries,
            logical_pages_before: report.logical_pages_before,
            logical_pages_after: Some(report.logical_pages_after),
            complete: true,
        })
    }

    /// Cancel an in-memory vacuum candidate without changing the durable root.
    pub fn cancel_vacuum(&mut self) -> Result<bool> {
        self.check_writable()?;
        Ok(self.vacuum.take().is_some())
    }

    fn start_vacuum(&mut self) -> Result<()> {
        self.flush()?;
        self.mirror_current_manifest()?;
        self.engine.ensure_materialized()?;
        let end = vec![u8::MAX; MAX_KEY_SIZE + 1];
        let cursor = self
            .engine
            .btree()
            .range_cursor(&[], &end)
            .map_err(Error::from)?;
        self.vacuum = Some(VacuumState {
            source_generation: self.generation_id,
            source_commit: self.commit_id,
            cursor,
            candidate_tree: BTree::new(),
            candidate_blobs: BlobManager::with_threshold_and_mode(
                self.blobs.threshold(),
                self.blobs.is_segmented(),
            ),
            scanned_entries: 0,
            live_entries: 0,
            logical_pages_before: self.engine.pmt().len() as u64,
        });
        Ok(())
    }

    fn vacuum_progress(&self, state: &VacuumState, complete: bool) -> VacuumProgress {
        VacuumProgress {
            durability: self.durability_status(),
            scanned_entries: state.scanned_entries,
            live_entries: state.live_entries,
            logical_pages_before: state.logical_pages_before,
            logical_pages_after: None,
            complete,
        }
    }

    fn finish_vacuum(&mut self, state: VacuumState) -> Result<VacuumReport> {
        let VacuumState {
            candidate_tree,
            candidate_blobs,
            live_entries,
            logical_pages_before,
            ..
        } = state;
        self.engine.prepare_logical_rebuild(candidate_tree)?;
        self.blobs = candidate_blobs;
        self.engine.flush()?;
        self.publish_blob_rewrite_generation()?;
        Ok(VacuumReport {
            durability: self.durability_status(),
            live_entries,
            logical_pages_before,
            logical_pages_after: self.engine.pmt().len() as u64,
        })
    }
}
