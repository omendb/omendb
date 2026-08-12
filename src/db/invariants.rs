//! Runtime state invariants for the database coordinator.
//!
//! These checks are intentionally limited to cheap handle-local relationships.
//! Durable artifact and B-tree invariants belong to `diagnostics.rs` and the
//! storage verification owners; this module catches coordinator state drift
//! before a public operation can act on it.

use super::*;

impl DB {
    pub(super) fn validate_runtime_state(&self) -> Result<()> {
        if self.check_only && !self.read_only {
            return Err(Error::Corruption(
                "check-only database must be read-only".into(),
            ));
        }
        if self.read_only && self.lock_file.is_some() {
            return Err(Error::Corruption(
                "read-only database unexpectedly owns the writer lock".into(),
            ));
        }
        if self.is_open && !self.read_only && self.lock_file.is_none() {
            return Err(Error::Corruption(
                "open writable database has no writer lock".into(),
            ));
        }
        if !self.is_open && self.lock_file.is_some() {
            return Err(Error::Corruption(
                "closed database still owns the writer lock".into(),
            ));
        }

        if self.next_commit_id <= self.commit_id {
            return Err(Error::Corruption(
                "next commit identity is not ahead of the published commit".into(),
            ));
        }
        if self.next_generation_id <= self.generation_id {
            return Err(Error::Corruption(
                "next generation identity is not ahead of the published generation".into(),
            ));
        }

        if self.pending_mutations == 0 {
            if self.pending_wal_bytes != 0 {
                return Err(Error::Corruption(
                    "pending WAL bytes exist without pending mutations".into(),
                ));
            }
            if self.pending_digest != 0 {
                return Err(Error::Corruption(
                    "pending digest exists without pending mutations".into(),
                ));
            }
            if self.pending_blob_changes {
                return Err(Error::Corruption(
                    "pending blob changes exist without pending mutations".into(),
                ));
            }
        } else {
            if self.pending_wal_bytes == 0 {
                return Err(Error::Corruption(
                    "pending mutations have no WAL bytes".into(),
                ));
            }
            if self.pending_wal_bytes > self.options.max_wal_bytes {
                return Err(Error::Corruption(
                    "pending WAL bytes exceed the configured admission budget".into(),
                ));
            }
            if self.wal_reserved_extent < self.pending_wal_bytes {
                return Err(Error::Corruption(
                    "pending WAL bytes exceed the reserved WAL extent".into(),
                ));
            }
        }

        Ok(())
    }
}
