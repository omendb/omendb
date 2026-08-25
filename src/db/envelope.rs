//! Pipelined publication: two-phase admit/barrier envelopes.
//!
//! Phase 1 ([`DB::admit_batch`]) validates a batch, admits its WAL
//! reservation, applies it to the live tree/blobs, and stages it as an
//! envelope. No durability work happens here beyond the in-memory WAL
//! append; recovery ignores an unpublished mutation prefix, so a crash
//! before the barrier reopens the previous published generation.
//!
//! Phase 2 ([`DB::publication_barrier`]) publishes the whole staged group
//! as ONE generation: one commit record, one authority frame, one
//! metadata-log sync. The WAL layout stays a valid recovery prefix —
//! `[A-muts, B-muts, C-group-commit]` — because the group commit covers the
//! cumulative pending mutation prefix. Visibility and acks are
//! group-granular: every staged envelope becomes durable together or none
//! does.
//!
//! Membership freezes when the barrier starts: envelopes admitted while a
//! barrier is in flight are impossible (`&mut self`), and arrivals after it
//! returns join the next group. Acks are all-or-nothing per group or the
//! writer fences for recovery.

use super::*;

/// A batch admitted via [`DB::admit_batch`] that awaits publication.
///
/// The envelope carries only its admission-order identity for ack
/// correlation; the barrier builds one commit record for the whole group.
#[derive(Debug, Clone)]
pub struct PendingEnvelope {
    /// Monotonic admission-order identifier within this handle.
    pub envelope_id: u64,
}

impl DB {
    /// Phase 1 of pipelined publication: validate and install a batch into
    /// the live state without any durability work.
    ///
    /// Unlike [`DB::commit_batch_at`], multiple envelopes may be admitted
    /// before publishing. Readers on this handle observe installed-but-
    /// unbarriered state between admit and barrier; callers that need read
    /// stability must retain snapshots across the window.
    pub fn admit_batch(
        &mut self,
        expected_commit: CommitId,
        mutations: &[BatchMutation],
    ) -> Result<PendingEnvelope> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        if self.commit_id != expected_commit {
            return Err(Error::SerializationConflict {
                expected: expected_commit,
                current: self.commit_id,
            });
        }
        if mutations.is_empty() {
            return Err(Error::InvalidArgument(
                "admit_batch requires at least one mutation".into(),
            ));
        }

        let prepared = self.prepare_batch(mutations)?;
        // Buffer the mutation prefix in the WAL without syncing; the barrier
        // owns every durability boundary. Consecutive admissions extend the
        // same prefix, so the layout remains a valid recovery prefix.
        for record in &prepared.records {
            self.wal.append(record);
        }
        *self.engine.btree_mut() = prepared.candidate_tree;
        self.blobs = prepared.candidate_blobs;
        self.pending_blob_changes |= prepared.blob_changed;
        self.pending_blob_frame |= prepared.blob_changed;
        self.pending_mutations = prepared.next_pending_mutations;
        self.pending_wal_bytes = prepared.next_pending_bytes;
        self.pending_digest = prepared.next_digest;

        let envelope_id = self.next_envelope_id;
        self.next_envelope_id = envelope_id
            .checked_add(1)
            .ok_or_else(|| Error::Wal("envelope ID overflow".into()))?;
        let envelope = PendingEnvelope { envelope_id };
        self.pending_envelopes.push(envelope.clone());
        Ok(envelope)
    }

    /// Phase 2 of pipelined publication: publish every admitted envelope as
    /// ONE generation under coalesced barriers and report each envelope's
    /// durability.
    ///
    /// Returns results in admission order; visibility is group-granular, so
    /// every result carries the same post-publication status. An empty group
    /// is a validated no-op. A capacity preflight refusal is retryable: the
    /// envelopes are restored and a later barrier republishes them. Any
    /// other error may have reached durable media and fences the writer for
    /// recovery, exactly like the single-generation publication path.
    pub fn publication_barrier(&mut self) -> Result<Vec<(u64, DurabilityStatus)>> {
        // A fenced writer must not retry: publication may have reached durable
        // media or consumed commit IDs, so only reopen may publish again.
        self.check_writable()?;
        if self.pending_envelopes.is_empty() {
            return Ok(Vec::new());
        }
        let envelopes = std::mem::take(&mut self.pending_envelopes);
        let result = if self.options.wal_first_commits {
            // WAL-first: ack after one group-synced WAL append; pages and
            // authority frame move to materialization (flush/checkpoint/close).
            self.publish_envelope_group_wal_first()
        } else {
            self.publish_envelope_group()
        };
        if let Err(error) = result {
            // Nothing can have been admitted while the group was in flight
            // (`&mut self`), so the pending list is still empty and plain
            // assignment restores the original admission order.
            self.pending_envelopes = envelopes;
            if !matches!(&error, Error::CapacityPreflight) {
                self.write_fenced = true;
            }
            return Err(error);
        }
        if self.options.wal_first_commits && self.materialize_bound_reached() {
            // The bound exists to cap crash-recovery replay; materialize now
            // rather than letting unframed state accumulate further. A failed
            // materialization fences through the normal flush path.
            self.flush()?;
        }
        Ok(envelopes
            .iter()
            .map(|envelope| (envelope.envelope_id, self.durability_status()))
            .collect())
    }

    /// Blob artifacts once for the group: all staged blob changes are
    /// already installed in the live manager.
    fn write_group_blob_artifacts(&mut self, generation: GenerationId) -> Result<()> {
        if self.blobs.is_segmented() || self.pending_blob_frame {
            let blob_started = Instant::now();
            self.blobs.set_generation(generation.get());
            let blob_bytes = if self.blobs.is_segmented() {
                self.write_blob_segments()?
            } else {
                let blob_image = self.blobs.to_bytes();
                self.write_blob_image_without_directory_sync(
                    &self.path.join(BLOB_FILE),
                    &blob_image,
                )?
            };
            self.publication.blob_bytes_written = self
                .publication
                .blob_bytes_written
                .saturating_add(blob_bytes);
            self.publication_timing.blob_write_ns = self
                .publication_timing
                .blob_write_ns
                .saturating_add(elapsed_nanos(blob_started));
            // The blob artifact's directory entry must be durable before any
            // frame can name this generation: an established database's
            // meta.log creation no longer forces it implicitly. The same
            // ordering rule applies to a WAL-first commit sync that will
            // reference these bytes from its replayable mutation prefix.
            let directory_started = Instant::now();
            let directory_result = sync_publication_directory(&self.path);
            self.publication_timing.directory_sync_ns = self
                .publication_timing
                .directory_sync_ns
                .saturating_add(elapsed_nanos(directory_started));
            directory_result?;
        }
        Ok(())
    }

    pub(super) fn publish_envelope_group(&mut self) -> Result<()> {
        // Stage 1: data. One capacity preflight, one dirty-page write pass,
        // and one device sync cover every staged envelope's pages.
        let parent_manifest = self
            .manifest_history
            .latest()
            .unwrap_or_else(|| self.bootstrap_manifest());
        #[cfg(any(test, feature = "fault-injection"))]
        if super::faults::FAIL_NEXT_GROUP_SYNC.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected group data sync failure").into());
        }
        self.write_wal_to_disk(false)?;
        self.preflight_publication_capacity()?;
        if self.engine.reclamation_needs_refresh() {
            self.engine.refresh_reclamation()?;
        }
        let generation = self.next_generation_id;
        self.engine.set_write_generation(generation.get());
        let staged = self.engine.write_dirty_pages()?;
        self.engine.sync_data(staged)?;

        self.write_group_blob_artifacts(generation)?;

        // Stage 2: ONE commit record covering the cumulative pending
        // mutation prefix. Under the default CoW policy it remains buffered;
        // the synced authority frame below is the acknowledgement point.
        // Explicit `sync_writes` still forces the WAL. The offset is the file
        // length BEFORE the commit record: mutation records precede it.
        let wal_path = self.path.join(WAL_FILE);
        let wal_offset = fs::metadata(&wal_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let commit = CommitRecord {
            commit_id: self.next_commit_id,
            generation_id: generation,
            root_page_id: self.engine.btree().root_id() as u64,
            mutation_count: self.pending_mutations,
            digest: self.pending_digest,
        };
        self.next_commit_id = CommitId::new(
            self.next_commit_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("commit ID overflow".into()))?,
        );
        self.next_generation_id = GenerationId::new(
            self.next_generation_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        self.wal.append(&WalRecord::commit(commit));
        self.write_wal_to_disk(false)?;

        // Stage 3: ONE authority frame. After write_dirty_pages the live PMT
        // describes exactly this generation's mappings, so no checkpoint
        // reconstruction or PMT swap is needed. INVARIANT: the frame is only
        // persisted here, after the data sync staged every referenced page
        // offset; the WAL may still be buffered under default policy.
        #[cfg(any(test, feature = "fault-injection"))]
        {
            let remaining = super::faults::FAIL_NEXT_FRAME_APPEND_N.with(|count| {
                let current = count.get();
                if current > 0 {
                    count.set(current - 1);
                }
                current
            });
            if remaining == 1 {
                return Err(std::io::Error::other("injected frame append failure").into());
            }
        }
        let wal_path = self.append_envelope_frame(commit, wal_offset, parent_manifest)?;
        self.finish_generation_publication(commit, wal_path)?;
        Ok(())
    }

    /// WAL-first group publication: blob artifacts (only when a frame has
    /// not yet named them), then ONE cumulative commit envelope synced to
    /// disk as the ack point. Pages and the authority frame move to
    /// materialization (flush/checkpoint/close); recovery replays the
    /// synced prefix ahead of authority via the Greater branch.
    /// Whether unmaterialized WAL bytes crossed the automatic
    /// materialization bound that caps crash-recovery replay work.
    pub(super) fn materialize_bound_reached(&self) -> bool {
        let bound = self.options.wal_materialize_bytes;
        bound != 0 && self.unframed_wal_bytes >= bound
    }

    pub(super) fn publish_envelope_group_wal_first(&mut self) -> Result<()> {
        // A segmented consolidation rewrites the catalog anchor and retires
        // the delta log, which is only safe once the manifest names the new
        // generation in the same publication. Soft barriers leave the
        // manifest behind, so recovery would face an anchor newer than its
        // target with a broken delta chain; escalate to a full publication.
        if self.blobs.is_segmented() && self.segmented_catalog_consolidation_needed() {
            return self.publish_envelope_group();
        }
        let generation = self.next_generation_id;
        self.write_group_blob_artifacts(generation)?;

        let commit = CommitRecord {
            commit_id: self.next_commit_id,
            generation_id: generation,
            root_page_id: self.engine.btree().root_id() as u64,
            mutation_count: self.pending_mutations,
            digest: self.pending_digest,
        };
        self.next_commit_id = CommitId::new(
            self.next_commit_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("commit ID overflow".into()))?,
        );
        self.next_generation_id = GenerationId::new(
            self.next_generation_id
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        self.wal.append(&WalRecord::commit(commit));
        // The sync IS the acknowledgement point under AckPolicy::GroupSync.
        self.write_wal_to_disk(true)?;

        self.generation_id = commit.generation_id;
        self.commit_id = commit.commit_id;
        self.unframed_wal_bytes = self
            .unframed_wal_bytes
            .saturating_add(self.pending_wal_bytes)
            .saturating_add(WAL_COMMIT_RECORD_BYTES);
        self.pending_mutations = 0;
        self.pending_wal_bytes = 0;
        self.pending_digest = 0;
        self.pending_blob_changes = false;
        // pending_blob_frame and unframed_commits survive: materialization
        // must still name these bytes and this state with an authority frame.
        self.unframed_commits = true;
        Ok(())
    }
}
