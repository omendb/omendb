//! Pipelined publication: two-phase admit/barrier envelopes.
//!
//! Phase 1 ([`DB::admit_batch`]) validates a batch, admits its WAL
//! reservation, applies it to the live tree/blobs, and stages it as an
//! envelope. No durability work happens here beyond the in-memory WAL
//! append; recovery ignores an unpublished mutation prefix, so a crash
//! before the barrier reopens the previous published generation.
//!
//! Phase 2 ([`DB::publication_barrier`]) publishes every staged envelope
//! under coalesced barriers: one data-device sync covering all staged page
//! writes, one WAL sync covering all commit records, and one authority-frame
//! append per envelope with its own metadata-log sync. Per-envelope ordering
//! WAL <= data <= frame <= visibility is preserved by these stage barriers.
//!
//! Membership freezes when the barrier starts: envelopes admitted while a
//! barrier is in flight are impossible (`&mut self`), and arrivals after it
//! returns join the next group. Acks are prefix-complete over the frozen
//! list or the writer fences for recovery.

use super::*;
use std::collections::HashSet;

/// A batch admitted via [`DB::admit_batch`] that awaits publication.
#[derive(Debug, Clone)]
pub struct PendingEnvelope {
    /// Monotonic admission-order identifier within this handle.
    pub envelope_id: u64,
    pub(super) commit: CommitRecord,
    /// Page IDs this batch rewrote or created, in distinction from every
    /// earlier unbarriered envelope. The barrier reconstructs per-envelope
    /// PMT checkpoints from these.
    pub(super) changed_pages: Vec<u32>,
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

        let prev_dirty: Vec<u32> = self.engine.btree().dirty_page_ids();
        let prepared = self.prepare_batch(mutations)?;
        // Buffer the mutation prefix in the WAL without syncing; the barrier
        // owns every durability boundary.
        for record in &prepared.records {
            self.wal.append(record);
        }
        // First-touch order over unique page IDs: dirty_page_ids yields each
        // ID once, so the barrier reconstructs checkpoints with exactly one
        // insert per changed page per envelope.
        let prev_dirty_set: HashSet<u32> = prev_dirty.iter().copied().collect();
        let changed_pages = prepared
            .candidate_tree
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| !prev_dirty_set.contains(page_id))
            .collect();
        if self.pending_envelopes.is_empty() {
            // Pages dirtied before this group's first admission (e.g. an
            // unpublished bootstrap or single-mutation prefix) belong to no
            // envelope but every checkpoint in the group must cover them.
            self.group_carry_pages = prev_dirty;
        }
        *self.engine.btree_mut() = prepared.candidate_tree;
        self.blobs = prepared.candidate_blobs;
        self.pending_blob_changes |= prepared.blob_changed;
        self.pending_mutations = prepared.next_pending_mutations;
        self.pending_wal_bytes = prepared.next_pending_bytes;
        self.pending_digest = prepared.next_digest;

        // Each envelope's commit covers the cumulative pending prefix so a
        // group replays as consecutive complete generations.
        let commit = CommitRecord {
            commit_id: self.next_commit_id,
            generation_id: self.next_generation_id,
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

        let envelope_id = self.next_envelope_id;
        self.next_envelope_id = envelope_id
            .checked_add(1)
            .ok_or_else(|| Error::Wal("envelope ID overflow".into()))?;
        let envelope = PendingEnvelope {
            envelope_id,
            commit,
            changed_pages,
        };
        self.pending_envelopes.push(envelope.clone());
        Ok(envelope)
    }

    /// Phase 2 of pipelined publication: publish every admitted envelope
    /// under coalesced barriers and report each envelope's durability.
    ///
    /// Returns results in admission order. An empty group is a validated
    /// no-op. Any error other than a capacity preflight refusal may have
    /// reached durable media and fences the writer for recovery, exactly
    /// like the single-envelope publication path.
    pub fn publication_barrier(&mut self) -> Result<Vec<(u64, DurabilityStatus)>> {
        if self.pending_envelopes.is_empty() {
            return Ok(Vec::new());
        }
        let published = self.publish_pending_envelopes()?;
        Ok(published
            .iter()
            .map(|envelope| (envelope.envelope_id, self.durability_status()))
            .collect())
    }

    fn publish_pending_envelopes(&mut self) -> Result<Vec<PendingEnvelope>> {
        let envelopes = std::mem::take(&mut self.pending_envelopes);
        let last_envelope = envelopes.last().expect("caller checked non-empty");
        let last_generation = last_envelope.commit.generation_id.get();

        // The pre-barrier PMT describes the last published generation; the
        // per-envelope checkpoints below are reconstructed against it.
        let base_pmt = self.engine.clone_pmt_arc();

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
        self.engine.set_write_generation(last_generation);
        let staged = self.engine.write_dirty_pages()?;
        self.engine.sync_data(staged)?;
        let final_pmt = self.engine.clone_pmt_arc();

        // Blob artifacts once for the group: all staged blob changes are
        // already installed in the live manager.
        if self.blobs.is_segmented() || self.pending_blob_changes {
            let blob_started = Instant::now();
            self.blobs.set_generation(last_generation);
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
        }

        // Stage 2: commits. One WAL sync makes every envelope's commit record
        // durable before any frame becomes visible. Offsets advance by record
        // size because buffered records are not visible to metadata queries
        // until the sync lands.
        let wal_path = self.path.join(WAL_FILE);
        let mut next_commit_offset = fs::metadata(&wal_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let mut commit_offsets = Vec::with_capacity(envelopes.len());
        for envelope in &envelopes {
            commit_offsets.push(next_commit_offset);
            let record = WalRecord::commit(envelope.commit);
            next_commit_offset = next_commit_offset.saturating_add(record.to_bytes().len() as u64);
            self.wal.append(&record);
        }
        self.write_wal_to_disk(true)?;

        // Stage 3: authority frames, one per envelope in admission order.
        // INVARIANT: checkpoints are only persisted here, after the group
        // data sync staged every referenced page offset and the WAL sync made
        // every commit record durable. The PMT swap below exists solely to
        // encode each frame against its own generation's mappings; a frame
        // naming non-durable offsets must never reach storage, so no
        // checkpoint materialization may precede stages 1-2.
        //
        // Each frame's checkpoint must resolve to exactly that generation's
        // mappings, but write_dirty_pages produced only the final PMT, so
        // each intermediate checkpoint is rebuilt incrementally from the base
        // PMT plus the first-touch changed-page list of every envelope up to
        // and including this one. One insert per page in admission order
        // reproduces the final version numbers exactly.
        let mut pmt = base_pmt;
        {
            let pmt_ref = Arc::make_mut(&mut pmt);
            for &page_id in &self.group_carry_pages {
                let mapping = final_pmt.get(page_id as u64).ok_or_else(|| {
                    Error::Corruption("staged page mapping missing after flush".into())
                })?;
                pmt_ref.insert(page_id as u64, 0, mapping.offset);
            }
        }
        for (index, envelope) in envelopes.iter().enumerate() {
            {
                let pmt_ref = Arc::make_mut(&mut pmt);
                for &page_id in &envelope.changed_pages {
                    let mapping = final_pmt.get(page_id as u64).ok_or_else(|| {
                        Error::Corruption("staged page mapping missing after flush".into())
                    })?;
                    pmt_ref.insert(page_id as u64, 0, mapping.offset);
                }
            }
            self.engine.swap_pmt(pmt.clone());
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
                    self.engine.swap_pmt(final_pmt.clone());
                    return Err(std::io::Error::other("injected frame append failure").into());
                }
            }
            self.append_envelope_frame(envelope.commit, commit_offsets[index], parent_manifest)?;
        }
        self.engine.swap_pmt(final_pmt);

        self.finish_generation_publication(last_envelope.commit, wal_path)?;
        self.group_carry_pages.clear();
        Ok(envelopes)
    }
}
