//! Maintenance-batch publication: arbitrary durable rewrites without a
//! logical commit.
//!
//! Compaction and blob rewrite already publish new physical generations that
//! preserve logical commit identity. This module generalizes that mechanism
//! to caller-supplied mutations so transactional control-plane work (MVCC
//! record rewrites, status pruning, retention-lease writes, allocator
//! high-water records) can be durable without consuming a logical commit
//! sequence number or fabricating committed-change records.
//!
//! Staging mirrors the envelope-group publication: the candidate is prepared
//! off to the side, installed into the live tree, written as dirty pages with
//! one device sync, and covered by blob artifacts — but no mutation or commit
//! record enters the WAL. The new manifest's PMT checkpoint is the durable
//! record of the rewrite; the previous manifest slot stays authoritative
//! until the new frame is synced; crash recovery atomically selects one
//! frame or the other. The published manifest clones the current logical
//! commit identity (commit ID, commit sequence, LSN, mutation count,
//! digest), so WAL recovery's equal-generation validation still matches the
//! last logical commit envelope and the next logical publication continues
//! from a clean pending state.

use super::*;

/// A fully validated, capacity-preflighted candidate for one maintenance
/// batch. Failure to build one is a certain no-op: nothing outside the
/// candidate was touched.
struct MaintenanceCandidate {
    tree: BTree,
    blobs: BlobManager,
    blob_changed: bool,
}

impl DB {
    /// Commit caller-owned mutations as one maintenance generation without
    /// a logical commit.
    ///
    /// The mutations become durable under a new generation whose manifest
    /// clones the current logical commit identity: `commit_id`, `commit_seq`,
    /// the durable LSN, mutation count, and digest are all preserved, and no
    /// commit sequence number is consumed. Logical readers observe no
    /// visibility event, and the committed-change stream stays untouched.
    ///
    /// Any pending generation is settled first, including WAL-first commits
    /// that still need materialization, so the cloned identity names a fully
    /// published position. Validation and capacity failures leave the writer
    /// retryable and unchanged; any failure after the candidate is installed
    /// may have reached durable media and fences the handle for reopen,
    /// exactly like compaction.
    pub fn commit_maintenance_batch(&mut self, mutations: &[BatchMutation]) -> Result<()> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        // Settle any pending generation, including WAL-first envelope groups
        // and unframed commits, so the cloned identity is the fully
        // materialized head.
        self.flush()?;
        if mutations.is_empty() {
            return Ok(());
        }

        // Phase 1: off-side candidate. Every failure here is a certain
        // no-op with no media effects, so the writer stays retryable.
        let candidate = self.prepare_maintenance_candidate(mutations)?;

        // Phase 2: publication. The candidate is installed into live state
        // first; from that point any failure (except a preflight that issued
        // no I/O) may have reached durable media and must fence.
        let result = self.publish_maintenance_batch(candidate);
        if result.is_err() && !matches!(&result, Err(Error::CapacityPreflight)) {
            self.write_fenced = true;
        }
        result
    }

    fn prepare_maintenance_candidate(
        &mut self,
        mutations: &[BatchMutation],
    ) -> Result<MaintenanceCandidate> {
        let candidate_started = Instant::now();

        // Load every mutation's root-to-leaf path into the sparse overlay
        // BEFORE cloning the candidate, matching the group-commit path.
        // These loads are read-only; a failure here is a certain no-op.
        for mutation in mutations {
            let key = match mutation {
                BatchMutation::Put { key, .. } | BatchMutation::Delete { key } => key,
            };
            self.engine.prepare_mutation(key)?;
        }

        let mut candidate_tree = self.engine.btree().clone();
        let mut candidate_blobs = self.blobs.clone();
        let mut blob_changed = false;
        for mutation in mutations {
            let outcome = match mutation {
                BatchMutation::Put { key, value } => {
                    validate_wal_put_lengths(key, value)?;
                    apply_mutation(
                        Mutation::Put { key, value },
                        &mut candidate_tree,
                        &mut candidate_blobs,
                    )?
                }
                BatchMutation::Delete { key } => {
                    validate_wal_key_length(key)?;
                    apply_mutation(
                        Mutation::Delete { key },
                        &mut candidate_tree,
                        &mut candidate_blobs,
                    )?
                }
            };
            require_blob_deletion(outcome, "maintenance mutation")?;
            blob_changed |= outcome.blob_changed;
        }

        // Maintenance generations carry no WAL reservation: account for the
        // candidate's data extent and sidecar artifacts directly, as compaction
        // does. Every capacity check here is a preflight with no I/O.
        let candidate_page_count =
            u64::try_from(candidate_tree.node_count()).map_err(|_| Error::DiskFull)?;
        let candidate_data_bytes = candidate_page_count
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(Error::DiskFull)?;
        let candidate_metadata_bytes = Self::full_metadata_bytes_for_candidate(&candidate_tree)?;
        let candidate_blob_bytes = Self::blob_publication_size(&candidate_blobs)?;
        self.preflight_maintenance_capacity(
            candidate_data_bytes,
            candidate_metadata_bytes,
            candidate_blob_bytes,
        )?;
        self.engine.check_artifact_capacity(candidate_blob_bytes)?;
        self.engine.preflight_rebuild_capacity(&candidate_tree)?;

        self.publication_timing.candidate_prepare_ns = self
            .publication_timing
            .candidate_prepare_ns
            .saturating_add(elapsed_nanos(candidate_started));

        Ok(MaintenanceCandidate {
            tree: candidate_tree,
            blobs: candidate_blobs,
            blob_changed,
        })
    }

    fn publish_maintenance_batch(&mut self, candidate: MaintenanceCandidate) -> Result<()> {
        let MaintenanceCandidate {
            tree: candidate_tree,
            blobs: candidate_blobs,
            blob_changed,
        } = candidate;

        // Install the candidate as the live state without WAL journaling.
        // The dirty pages are written and synced below; until the new
        // manifest frame is durable, the previous manifest remains
        // authoritative for recovery.
        *self.engine.btree_mut() = candidate_tree;
        self.blobs = candidate_blobs;
        self.pending_blob_changes |= blob_changed;
        self.pending_blob_frame |= blob_changed;

        let generation = self.next_generation_id;

        // Stage 1: data. One capacity preflight, one dirty-page write pass,
        // and one device sync cover the whole maintenance batch, mirroring
        // the envelope-group publication.
        self.preflight_publication_capacity()?;
        if self.engine.reclamation_needs_refresh() {
            self.engine.refresh_reclamation()?;
        }
        self.engine.set_write_generation(generation.get());
        let staged = self.engine.write_dirty_pages()?;
        self.engine.sync_data(staged)?;

        // Stage 2: blob artifacts, exactly as the group barrier writes them.
        self.write_group_blob_artifacts(generation)?;

        // Stage 3: one authority frame cloning the current logical identity.
        // No WAL commit record is appended: the last logical commit envelope
        // remains the WAL tail, and this manifest keeps naming it.
        let current = self
            .manifest_history
            .latest()
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;
        let manifest = Manifest {
            generation_id: generation,
            pmt_checkpoint_id: PmtCheckpointId::new(generation.get()),
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
        self.generation_id = generation;
        self.next_generation_id = GenerationId::new(
            generation
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("generation ID overflow".into()))?,
        );
        // The maintenance batch is accounted as published, not pending: the
        // generation flush above already made every page durable.
        self.pending_mutations = 0;
        self.pending_wal_bytes = 0;
        self.pending_digest = 0;
        self.pending_blob_changes = false;
        self.pending_blob_frame = false;
        // Logical identity is deliberately NOT advanced: commit_id,
        // commit_seq, durable_lsn, and their next reservations are
        // unchanged, which is the entire point of a maintenance generation.
        Ok(())
    }
}
