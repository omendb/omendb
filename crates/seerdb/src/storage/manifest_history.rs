//! Durable append-only manifest history.
//!
//! This sidecar retains historical root descriptors independently from the
//! two-slot current-manifest authority. It owns replay, reconciliation, and
//! pruning of historical generations.

use super::valid_page_size;
use super::{CommitId, FORMAT_VERSION, GenerationId, MANIFEST_SLOT_SIZE, Manifest};
use std::collections::BTreeSet;

const MANIFEST_HISTORY_MAGIC: [u8; 8] = *b"SEERHST1";
const MANIFEST_HISTORY_HEADER_SIZE: usize = 8 + 4;
const MANIFEST_HISTORY_ENTRY_SIZE: usize = MANIFEST_SLOT_SIZE + 4;

/// Durable ordered history of published root manifests.
///
/// The alternating `MANIFEST` slots identify only the newest root. This
/// append-only sidecar keeps the older root descriptors needed when a
/// consumer asks to retain a historical commit after later commits have
/// already been published. PMT checkpoints remain separate immutable files.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ManifestHistory {
    manifests: Vec<Manifest>,
}

impl ManifestHistory {
    /// Create an empty manifest history.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return published manifests in generation order.
    pub fn manifests(&self) -> &[Manifest] {
        &self.manifests
    }

    /// Return the newest published manifest, if any.
    pub fn latest(&self) -> Option<Manifest> {
        self.manifests.last().copied()
    }

    /// Find the root descriptor for an exact logical commit.
    pub fn find_commit(&self, commit_id: CommitId) -> Option<Manifest> {
        self.manifests
            .iter()
            .rev()
            .find(|manifest| manifest.commit_id == commit_id)
            .copied()
    }

    /// Replace the history with one manifest after a history fork.
    pub fn reset(&mut self, manifest: Manifest) {
        self.manifests.clear();
        self.manifests.push(manifest);
    }

    /// Append one newer manifest, accepting an idempotent duplicate.
    pub fn push(&mut self, manifest: Manifest) -> std::result::Result<(), &'static str> {
        if manifest.format_version != FORMAT_VERSION || !valid_page_size(manifest.page_size) {
            return Err("invalid manifest format");
        }
        if let Some(current) = self.latest() {
            if current == manifest {
                return Ok(());
            }
            if !manifest.is_newer_than(current) {
                return Err("manifest history is not monotonic");
            }
        }
        self.manifests.push(manifest);
        Ok(())
    }

    /// Reconcile a history sidecar with the authoritative current manifest.
    ///
    /// A sidecar may contain one durable orphan when a crash occurs after its
    /// atomic publication but before the alternating manifest slot. Such an
    /// orphan is discarded here; an older sidecar is advanced with the
    /// authoritative manifest for compatibility with databases created before
    /// this sidecar existed.
    pub fn reconcile_current(
        &mut self,
        current: Manifest,
    ) -> std::result::Result<(), &'static str> {
        if let Some(index) = self
            .manifests
            .iter()
            .position(|manifest| *manifest == current)
        {
            self.manifests.truncate(index + 1);
            return Ok(());
        }

        if let Some(latest) = self.latest()
            && latest.database_id == current.database_id
            && latest.history_id == current.history_id
            && current.is_newer_than(latest)
        {
            self.push(current)?;
            return Ok(());
        }

        // A history fork intentionally changes HistoryId while preserving the
        // root. The current manifest is authoritative in that case.
        if self.latest().is_some_and(|latest| {
            latest.database_id == current.database_id && latest.history_id != current.history_id
        }) {
            self.reset(current);
            return Ok(());
        }

        if self.latest().is_none() {
            self.push(current)?;
            return Ok(());
        }

        Err("manifest history disagrees with authoritative manifest")
    }

    /// Retain only the generations that are still needed by the active
    /// manifest and durable snapshot registry.
    ///
    /// The caller is responsible for atomically persisting the resulting
    /// history before deleting any superseded checkpoint files.
    pub fn prune_to_generations(&mut self, retained: &BTreeSet<GenerationId>) -> usize {
        let before = self.manifests.len();
        self.manifests
            .retain(|manifest| retained.contains(&manifest.generation_id));
        before.saturating_sub(self.manifests.len())
    }

    /// Return the append-only history log header.
    pub fn header_bytes() -> [u8; 12] {
        let mut bytes = [0; 12];
        bytes[..8].copy_from_slice(&MANIFEST_HISTORY_MAGIC);
        bytes[8..].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes
    }

    /// Encode one checksummed append-only history entry.
    pub fn entry_bytes(manifest: Manifest) -> [u8; MANIFEST_HISTORY_ENTRY_SIZE] {
        let manifest_bytes = manifest.to_bytes();
        let checksum = crc32c::crc32c(&manifest_bytes);
        let mut bytes = [0; MANIFEST_HISTORY_ENTRY_SIZE];
        bytes[..MANIFEST_SLOT_SIZE].copy_from_slice(&manifest_bytes);
        bytes[MANIFEST_SLOT_SIZE..].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// Encode the complete append-only history log.
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let total_len = MANIFEST_HISTORY_HEADER_SIZE.checked_add(
            self.manifests
                .len()
                .checked_mul(MANIFEST_HISTORY_ENTRY_SIZE)?,
        )?;
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&Self::header_bytes());
        for manifest in &self.manifests {
            bytes.extend_from_slice(&Self::entry_bytes(*manifest));
        }
        Some(bytes)
    }

    /// Decode and validate complete entries from an append-only history log.
    ///
    /// A partial final entry is ignored because it can result from a crash
    /// during the final append. Complete entries are always checksum-checked.
    pub fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, &'static str> {
        if bytes.len() < MANIFEST_HISTORY_HEADER_SIZE {
            return Err("manifest history is truncated");
        }
        if bytes[..8] != MANIFEST_HISTORY_MAGIC {
            return Err("invalid manifest history magic");
        }
        let version = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .map_err(|_| "invalid manifest history version")?,
        );
        if version != FORMAT_VERSION {
            return Err("unsupported manifest history format");
        }

        let mut history = Self::new();
        let complete_len = bytes.len() - MANIFEST_HISTORY_HEADER_SIZE;
        let complete_len = complete_len / MANIFEST_HISTORY_ENTRY_SIZE * MANIFEST_HISTORY_ENTRY_SIZE;
        for chunk in bytes
            [MANIFEST_HISTORY_HEADER_SIZE..MANIFEST_HISTORY_HEADER_SIZE + complete_len]
            .as_chunks::<{ MANIFEST_HISTORY_ENTRY_SIZE }>()
            .0
        {
            let slot: &[u8; MANIFEST_SLOT_SIZE] = chunk[..MANIFEST_SLOT_SIZE]
                .try_into()
                .map_err(|_| "invalid manifest history entry")?;
            let expected = u32::from_le_bytes(
                chunk[MANIFEST_SLOT_SIZE..]
                    .try_into()
                    .map_err(|_| "invalid manifest history checksum")?,
            );
            if crc32c::crc32c(slot) != expected {
                return Err("manifest history checksum mismatch");
            }
            let manifest = Manifest::from_bytes(slot)
                .map_err(|_| "invalid manifest history manifest")?
                .ok_or("empty manifest history entry")?;
            history.push(manifest)?;
        }
        Ok(history)
    }
}
