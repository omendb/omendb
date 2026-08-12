//! Single-mutation admission and journaling for `DB`.
//!
//! This module owns the public put/delete write lifecycle: handle checks, WAL
//! and blob-capacity admission, logical mutation application, and mutation
//! journaling. `DB` remains the mutable-state owner; `mutation.rs` remains the
//! shared logical transition authority, and `wal_admission.rs` owns the WAL
//! mechanics.

use super::*;

impl DB {
    /// Insert a key-value pair.
    ///
    /// The mutation is applied in memory, journaled durably, and included in
    /// the next published root generation.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        validate_wal_put_lengths(key, value)?;
        let record = WalRecord::put(key, value);
        self.admit_wal_record(&record)?;
        self.engine.prepare_mutation(key)?;

        // Mutate memory first, then make the successful mutation durable in
        // the WAL. No page is written before the WAL reaches disk, and an
        // operation that fails never enters a committed WAL batch.
        let previous_blob = match self.engine.lookup(key)? {
            LookupResult::Blob(pointer) => Some(pointer),
            _ => None,
        };
        let appended_value_len = self
            .blobs
            .should_separate(value.len())
            .then_some(value.len());
        let had_previous_blob = previous_blob.is_some();
        if had_previous_blob || appended_value_len.is_some() {
            self.admit_blob_image(previous_blob.as_ref(), appended_value_len)?;
        }
        let outcome = apply_mutation(
            Mutation::Put { key, value },
            self.engine.btree_mut(),
            &mut self.blobs,
        )?;
        require_blob_deletion(outcome, "put")?;

        self.journal_mutation(record)?;
        self.pending_blob_changes |= outcome.blob_changed;

        Ok(())
    }

    /// Delete a key.
    ///
    /// The tombstone is applied in memory, journaled durably, and included in
    /// the next published root generation.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.check_writable()?;
        self.check_maintenance_idle()?;
        validate_wal_key_length(key)?;
        let record = WalRecord::delete(key);
        self.admit_wal_record(&record)?;
        self.engine.prepare_mutation(key)?;

        let previous_blob = match self.engine.lookup(key)? {
            LookupResult::Blob(pointer) => Some(pointer),
            _ => None,
        };
        if previous_blob.is_some() {
            self.admit_blob_image(previous_blob.as_ref(), None)?;
        }
        let outcome = apply_mutation(
            Mutation::Delete { key },
            self.engine.btree_mut(),
            &mut self.blobs,
        )?;
        require_blob_deletion(outcome, "delete")?;
        self.journal_mutation(record)?;
        self.pending_blob_changes |= outcome.blob_changed;
        Ok(outcome.changed)
    }
}
