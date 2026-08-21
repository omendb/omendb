//! Historical manifest and checkpoint pruning lifecycle.
//!
//! This module owns the DB-level retention policy for history cleanup. The
//! manifest-history and metadata modules retain their durable codecs; `DB`
//! retains current mutable state and publication ordering.

use super::*;
use std::collections::BTreeSet;

impl DB {
    /// Remove historical manifests and PMT checkpoints that are not needed
    /// by the current root or a durable retained snapshot.
    ///
    /// The history sidecar is atomically replaced before any checkpoint is
    /// deleted. A crash during cleanup therefore leaves harmless extra files,
    /// never a history entry that names a missing checkpoint.
    pub fn prune_history(&mut self) -> Result<HistoryPruneReport> {
        self.check_writable()?;
        self.flush()?;
        let recovery_manifests = self.manifest.load_valid_manifests()?;
        let current = recovery_manifests
            .iter()
            .copied()
            .max_by_key(|manifest| (manifest.generation_id, manifest.commit_id))
            .ok_or_else(|| Error::Corruption("database has no valid manifest".into()))?;

        // Both valid slots are recovery roots until a later publication has
        // mirrored the current manifest. Pruning only from `current` can
        // delete the checkpoint needed by the inactive fallback slot, turning
        // a later torn newest-slot recovery into a missing-artifact failure.
        let mut retained = recovery_manifests
            .iter()
            .map(|manifest| manifest.generation_id)
            .collect::<BTreeSet<_>>();
        retained.insert(current.generation_id);
        let state = self
            .retention
            .lock()
            .map_err(|_| Error::Corruption("retention registry mutex is poisoned".into()))?;
        let retained_roots = state
            .all_roots()
            .map(|root| root.manifest)
            .collect::<Vec<_>>();
        retained.extend(retained_roots.iter().map(|manifest| manifest.generation_id));
        drop(state);

        let mut history = self.manifest_history.clone();
        history
            .reconcile_current(current)
            .map_err(|message| Error::Corruption(format!("manifest history {message}")))?;
        let mut retained_checkpoints = BTreeSet::new();
        let mut protected_manifests = recovery_manifests;
        protected_manifests.extend(retained_roots);
        protected_manifests.extend(
            history
                .manifests()
                .iter()
                .copied()
                .filter(|manifest| retained.contains(&manifest.generation_id)),
        );
        for manifest in protected_manifests {
            if manifest.pmt_checkpoint_id.get() != 0 {
                let parsed = DB::read_meta_log(&self.path)?
                    .ok_or_else(|| Error::Corruption("metadata log is missing".into()))?;
                retained_checkpoints.extend(DB::meta_log_ancestors(
                    &parsed,
                    manifest.pmt_checkpoint_id.get(),
                )?);
            }
        }
        let removed_manifests = history.prune_to_generations(&retained) as u64;
        if history != self.manifest_history {
            self.persist_manifest_history(&history)?;
            self.manifest_history = history;
        }

        let (removed_checkpoints, reclaimed_checkpoint_bytes) =
            self.compact_metadata_log(&retained_checkpoints)?;

        Ok(HistoryPruneReport {
            retained_generations: retained.len() as u64,
            removed_manifests,
            removed_checkpoints,
            reclaimed_checkpoint_bytes,
        })
    }
}
