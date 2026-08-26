//! Atomic batch candidate preparation and durable installation.
//!
//! This module owns the batch boundary between a temporary candidate state
//! and the live database state. Candidate validation and capacity admission
//! happen before the WAL append; installing the candidate happens only after
//! that append succeeds. `DB` remains the owner of all live state and of the
//! generation publication that follows.

use super::*;
use std::time::Instant;

pub(super) struct PreparedBatch {
    pub(super) records: Vec<WalRecord>,
    pub(super) candidate_tree: BTree,
    pub(super) candidate_blobs: BlobManager,
    pub(super) blob_changed: bool,
    pub(super) next_pending_mutations: u64,
    pub(super) next_pending_bytes: u64,
    pub(super) next_digest: u32,
}

impl DB {
    /// Commit multiple byte-key mutations atomically as one durable batch.
    ///
    /// The complete candidate B-tree/blob state is prepared off to the side
    /// before the batch WAL is appended. A validation, capacity, or B-tree
    /// error therefore leaves the current logical state untouched. Once the
    /// WAL batch is durable, [`DB::flush`] publishes all mutations under one
    /// commit envelope; a failure at that boundary fences the writer for
    /// recovery in the same way as a single mutation.
    pub fn commit_batch(&mut self, mutations: &[BatchMutation]) -> Result<DurabilityStatus> {
        let expected_commit = self.commit_id;
        self.commit_batch_at(expected_commit, mutations)
    }

    /// Commit multiple byte-key mutations only if the published commit still
    /// matches the caller's expected base.
    ///
    /// This is the storage boundary for optimistic transaction adapters. The
    /// expected-base check happens before validation, WAL admission, or any
    /// candidate tree/blob work, so a stale caller has no logical side
    /// effects. An empty batch is a validated no-op and returns the current
    /// durability status when the expected base matches.
    pub fn commit_batch_at(
        &mut self,
        expected_commit: CommitId,
        mutations: &[BatchMutation],
    ) -> Result<DurabilityStatus> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        if self.commit_id != expected_commit {
            return Err(Error::SerializationConflict {
                expected: expected_commit,
                current: self.commit_id,
            });
        }
        if mutations.is_empty() {
            return Ok(self.durability_status());
        }
        if self.pending_mutations != 0 {
            return Err(Error::InvalidArgument(
                "commit_batch requires a clean pending generation; flush or discard pending mutations first".into(),
            ));
        }

        let prepared = self.prepare_batch(mutations)?;
        self.install_batch(prepared)
    }

    /// Publish several logical commits as one durable generation.
    ///
    /// Each entry is an independently validated logical batch with its own
    /// assigned commit sequence number. The group installs one chained
    /// candidate state, appends every record, syncs the WAL once, and
    /// publishes a single authority frame whose `commit_seq` advances by
    /// `batches.len()`, so logical visibility order matches assignment order.
    /// Recovery treats the frame as one atomic outcome: either every member
    /// committed or none did. The returned status names the last member's
    /// position.
    pub fn commit_group_at(
        &mut self,
        expected_commit: CommitId,
        batches: &[Vec<BatchMutation>],
    ) -> Result<DurabilityStatus> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        if self.commit_id != expected_commit {
            return Err(Error::SerializationConflict {
                expected: expected_commit,
                current: self.commit_id,
            });
        }
        if self.pending_mutations != 0 {
            return Err(Error::InvalidArgument(
                "commit_group requires a clean pending generation; flush or discard pending mutations first".into(),
            ));
        }
        let members = batches.len();
        if members == 0 {
            return Ok(self.durability_status());
        }

        let candidate_started = Instant::now();
        let mut chain_tree = self.engine.btree().clone();
        let mut chain_blobs = self.blobs.clone();
        let mut chain_blob_changed = false;
        let mut total_bytes = 0u64;
        let mut digest = self.pending_digest;
        let mut all_records = Vec::new();
        for batch in batches {
            let mut records = Vec::with_capacity(batch.len());
            for mutation in batch {
                let record = match mutation {
                    BatchMutation::Put { key, value } => {
                        validate_wal_put_lengths(key, value)?;
                        WalRecord::put(key, value)
                    }
                    BatchMutation::Delete { key } => {
                        validate_wal_key_length(key)?;
                        WalRecord::delete(key)
                    }
                };
                total_bytes = total_bytes
                    .checked_add(record.to_bytes().len() as u64)
                    .ok_or(Error::DiskFull)?;
                records.push(record);
            }
            for mutation in batch {
                let key = match mutation {
                    BatchMutation::Put { key, .. } | BatchMutation::Delete { key } => key,
                };
                self.engine.prepare_mutation(key)?;
            }
            for mutation in batch {
                let outcome = match mutation {
                    BatchMutation::Put { key, value } => apply_mutation(
                        Mutation::Put { key, value },
                        &mut chain_tree,
                        &mut chain_blobs,
                    )?,
                    BatchMutation::Delete { key } => {
                        apply_mutation(Mutation::Delete { key }, &mut chain_tree, &mut chain_blobs)?
                    }
                };
                require_blob_deletion(outcome, "group mutation")?;
                chain_blob_changed |= outcome.blob_changed;
            }
            digest = records.iter().fold(digest, extend_digest);
            all_records.extend(records);
        }

        let required_wal = total_bytes
            .checked_add(WAL_COMMIT_RECORD_BYTES)
            .ok_or(Error::DiskFull)?;
        let available_wal = self
            .options
            .max_wal_bytes
            .saturating_sub(self.pending_wal_bytes);
        if required_wal > available_wal {
            self.wal_admission_failures = self.wal_admission_failures.saturating_add(1);
            return Err(Error::Backpressure {
                required: required_wal,
                available: available_wal,
            });
        }
        let projected_blobs = if chain_blob_changed {
            let projected = Self::blob_publication_size(&chain_blobs)?;
            self.engine.check_artifact_capacity(projected)?;
            Some(projected)
        } else {
            None
        };

        self.publication_timing.candidate_prepare_ns = self
            .publication_timing
            .candidate_prepare_ns
            .saturating_add(elapsed_nanos(candidate_started));

        self.ensure_wal_reservation()?;
        if let Some(projected) = projected_blobs
            && !chain_blobs.is_segmented()
        {
            self.reserve_blob_image(projected)?;
        }

        let next_pending_mutations = u64::try_from(all_records.len())
            .ok()
            .and_then(|count| self.pending_mutations.checked_add(count))
            .ok_or(Error::Wal("mutation count overflow".into()))?;
        let next_pending_bytes = self
            .pending_wal_bytes
            .checked_add(total_bytes)
            .ok_or(Error::Wal("WAL byte count overflow".into()))?;

        for record in &all_records {
            self.wal.append(record);
        }
        if let Err(error) = self.write_wal_to_disk(self.wal.sync_policy() != SyncPolicy::None) {
            self.write_fenced = true;
            return Err(error);
        }

        *self.engine.btree_mut() = chain_tree;
        self.blobs = chain_blobs;
        self.pending_blob_changes = chain_blob_changed;
        self.pending_blob_frame |= chain_blob_changed;
        self.pending_mutations = next_pending_mutations;
        self.pending_wal_bytes = next_pending_bytes;
        self.pending_digest = digest;

        // One authority frame covers all members; its sequence lands on the
        // last member so durability_status reports the logical head.
        self.next_commit_seq = CommitSeq::new(
            self.next_commit_seq
                .get()
                .checked_add(members as u64 - 1)
                .ok_or_else(|| Error::Wal("commit sequence overflow".into()))?,
        );

        if self.options.wal_first_commits {
            if let Err(error) = self.publish_envelope_group_wal_first() {
                self.write_fenced = true;
                return Err(error);
            }
            if self.materialize_bound_reached() {
                self.flush()?;
            }
            return Ok(self.durability_status());
        }
        self.flush()?;
        Ok(self.durability_status())
    }

    pub(super) fn prepare_batch(&mut self, mutations: &[BatchMutation]) -> Result<PreparedBatch> {
        let candidate_started = Instant::now();
        let mut records = Vec::with_capacity(mutations.len());
        let mut mutation_bytes = 0u64;
        for mutation in mutations {
            let record = match mutation {
                BatchMutation::Put { key, value } => {
                    validate_wal_put_lengths(key, value)?;
                    WalRecord::put(key, value)
                }
                BatchMutation::Delete { key } => {
                    validate_wal_key_length(key)?;
                    WalRecord::delete(key)
                }
            };
            mutation_bytes = mutation_bytes
                .checked_add(record.to_bytes().len() as u64)
                .ok_or(Error::DiskFull)?;
            records.push(record);
        }

        let required_wal = mutation_bytes
            .checked_add(WAL_COMMIT_RECORD_BYTES)
            .ok_or(Error::DiskFull)?;
        let available_wal = self
            .options
            .max_wal_bytes
            .saturating_sub(self.pending_wal_bytes);
        if required_wal > available_wal {
            self.wal_admission_failures = self.wal_admission_failures.saturating_add(1);
            return Err(Error::Backpressure {
                required: required_wal,
                available: available_wal,
            });
        }

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
                BatchMutation::Put { key, value } => apply_mutation(
                    Mutation::Put { key, value },
                    &mut candidate_tree,
                    &mut candidate_blobs,
                )?,
                BatchMutation::Delete { key } => apply_mutation(
                    Mutation::Delete { key },
                    &mut candidate_tree,
                    &mut candidate_blobs,
                )?,
            };
            require_blob_deletion(outcome, "batch mutation")?;
            blob_changed |= outcome.blob_changed;
        }

        if blob_changed {
            let projected = Self::blob_publication_size(&candidate_blobs)?;
            self.engine.check_artifact_capacity(projected)?;
        }
        self.publication_timing.candidate_prepare_ns = self
            .publication_timing
            .candidate_prepare_ns
            .saturating_add(elapsed_nanos(candidate_started));

        self.ensure_wal_reservation()?;
        if blob_changed && !candidate_blobs.is_segmented() {
            let projected = Self::blob_publication_size(&candidate_blobs)?;
            self.reserve_blob_image(projected)?;
        }

        let next_pending_mutations = u64::try_from(mutations.len())
            .ok()
            .and_then(|count| self.pending_mutations.checked_add(count))
            .ok_or(Error::Wal("mutation count overflow".into()))?;
        let next_pending_bytes = self
            .pending_wal_bytes
            .checked_add(mutation_bytes)
            .ok_or(Error::Wal("WAL byte count overflow".into()))?;
        let next_digest = records.iter().fold(self.pending_digest, |digest, record| {
            extend_digest(digest, record)
        });

        Ok(PreparedBatch {
            records,
            candidate_tree,
            candidate_blobs,
            blob_changed,
            next_pending_mutations,
            next_pending_bytes,
            next_digest,
        })
    }

    fn install_batch(&mut self, prepared: PreparedBatch) -> Result<DurabilityStatus> {
        for record in &prepared.records {
            self.wal.append(record);
        }
        if let Err(error) = self.write_wal_to_disk(self.wal.sync_policy() != SyncPolicy::None) {
            self.write_fenced = true;
            return Err(error);
        }

        *self.engine.btree_mut() = prepared.candidate_tree;
        self.blobs = prepared.candidate_blobs;
        self.pending_blob_changes = prepared.blob_changed;
        self.pending_blob_frame |= prepared.blob_changed;
        self.pending_mutations = prepared.next_pending_mutations;
        self.pending_wal_bytes = prepared.next_pending_bytes;
        self.pending_digest = prepared.next_digest;
        if self.options.wal_first_commits {
            // Ack after one group-synced WAL append; pages and the authority
            // frame materialize at flush/checkpoint/close.
            if let Err(error) = self.publish_envelope_group_wal_first() {
                self.write_fenced = true;
                return Err(error);
            }
            if self.materialize_bound_reached() {
                self.flush()?;
            }
            return Ok(self.durability_status());
        }
        self.flush()?;
        Ok(self.durability_status())
    }
}
