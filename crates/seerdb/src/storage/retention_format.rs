//! Durable retained-root registry format.
//!
//! This module owns the checksummed `SEERRET1` envelope and retained-root
//! descriptors. The retention lifecycle owns leases and reclamation state;
//! the manifest format remains owned by `storage::format`.

use super::format::{FORMAT_VERSION, MANIFEST_SLOT_SIZE, Manifest, SnapshotId};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedRoot {
    /// Stable identifier returned to the caller that retained the root.
    pub snapshot_id: SnapshotId,
    /// Immutable generation and checkpoint needed to keep the root live.
    pub manifest: Manifest,
}

/// Checksummed durable registry of root generations that must not be
/// physically reclaimed.
///
/// The registry deliberately stores the complete manifest rather than only a
/// generation number. Recovery can therefore validate the referenced
/// checkpoint and root before allowing any old page to become reusable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionRegistry {
    next_snapshot_id: SnapshotId,
    roots: Vec<RetainedRoot>,
}

const RETENTION_MAGIC: [u8; 8] = *b"SEERRET1";
const RETENTION_HEADER_SIZE: usize = 8 + 4 + 8 + 4;
const RETENTION_CHECKSUM_SIZE: usize = 4;

impl RetentionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            next_snapshot_id: SnapshotId::new(1),
            roots: Vec::new(),
        }
    }

    /// Return all retained roots in snapshot-ID order.
    pub fn roots(&self) -> &[RetainedRoot] {
        &self.roots
    }

    /// Return the next identifier without mutating the registry.
    pub fn next_snapshot_id(&self) -> SnapshotId {
        self.next_snapshot_id
    }

    /// Allocate and insert one retained root.
    pub fn insert(&mut self, manifest: Manifest) -> Option<SnapshotId> {
        let snapshot_id = self.next_snapshot_id;
        self.next_snapshot_id = SnapshotId::new(snapshot_id.get().checked_add(1)?);
        self.roots.push(RetainedRoot {
            snapshot_id,
            manifest,
        });
        Some(snapshot_id)
    }

    /// Remove a retained root, returning its descriptor.
    pub fn remove(&mut self, snapshot_id: SnapshotId) -> Option<RetainedRoot> {
        let index = self
            .roots
            .iter()
            .position(|root| root.snapshot_id == snapshot_id)?;
        Some(self.roots.remove(index))
    }

    /// Whether any retained roots exist.
    pub fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }

    /// Encode the registry with an exact-length checksum envelope.
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let count = u32::try_from(self.roots.len()).ok()?;
        let body_len = RETENTION_HEADER_SIZE
            .checked_add(self.roots.len().checked_mul(8 + MANIFEST_SLOT_SIZE)?)?;
        let total_len = body_len.checked_add(RETENTION_CHECKSUM_SIZE)?;
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&RETENTION_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.next_snapshot_id.get().to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        for root in &self.roots {
            bytes.extend_from_slice(&root.snapshot_id.get().to_le_bytes());
            bytes.extend_from_slice(&root.manifest.to_bytes());
        }
        let checksum = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Some(bytes)
    }

    /// Decode and validate a registry, refusing unknown versions, duplicate
    /// IDs, truncated entries, trailing bytes, and bad checksums.
    pub fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, &'static str> {
        if bytes.len() < RETENTION_HEADER_SIZE + RETENTION_CHECKSUM_SIZE {
            return Err("retention registry is truncated");
        }
        if bytes[..8] != RETENTION_MAGIC {
            return Err("invalid retention registry magic");
        }
        let version = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .map_err(|_| "retention registry version is truncated")?,
        );
        if version != FORMAT_VERSION {
            return Err("unsupported retention registry format version");
        }
        let next_snapshot_id = SnapshotId::new(u64::from_le_bytes(
            bytes[12..20]
                .try_into()
                .map_err(|_| "retention registry next ID is truncated")?,
        ));
        if next_snapshot_id.get() == 0 {
            return Err("retention registry next ID is invalid");
        }
        let count = u32::from_le_bytes(
            bytes[20..24]
                .try_into()
                .map_err(|_| "retention registry count is truncated")?,
        ) as usize;
        let body_len = RETENTION_HEADER_SIZE
            .checked_add(
                count
                    .checked_mul(8 + MANIFEST_SLOT_SIZE)
                    .ok_or("retention registry length overflows")?,
            )
            .ok_or("retention registry length overflows")?;
        let checksum_offset = body_len;
        if bytes.len() != checksum_offset + RETENTION_CHECKSUM_SIZE {
            return Err(if bytes.len() < checksum_offset + RETENTION_CHECKSUM_SIZE {
                "retention registry is truncated"
            } else {
                "retention registry has trailing bytes"
            });
        }
        let expected = u32::from_le_bytes(
            bytes[checksum_offset..]
                .try_into()
                .map_err(|_| "retention registry checksum is truncated")?,
        );
        if expected != crc32c::crc32c(&bytes[..checksum_offset]) {
            return Err("retention registry checksum mismatch");
        }

        let mut roots = Vec::with_capacity(count);
        let mut ids = BTreeSet::new();
        let mut offset = RETENTION_HEADER_SIZE;
        for _ in 0..count {
            let snapshot_id = SnapshotId::new(u64::from_le_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .map_err(|_| "retention registry snapshot ID is truncated")?,
            ));
            if snapshot_id.get() == 0 || !ids.insert(snapshot_id) {
                return Err("retention registry contains a duplicate or invalid ID");
            }
            offset += 8;
            let manifest_bytes: &[u8; MANIFEST_SLOT_SIZE] = bytes
                [offset..offset + MANIFEST_SLOT_SIZE]
                .try_into()
                .map_err(|_| "retention registry manifest is truncated")?;
            let manifest = Manifest::from_bytes(manifest_bytes)
                .map_err(|_| "retention registry contains an invalid manifest")?
                .ok_or("retention registry contains an empty manifest")?;
            roots.push(RetainedRoot {
                snapshot_id,
                manifest,
            });
            offset += MANIFEST_SLOT_SIZE;
        }
        if roots
            .iter()
            .any(|root| root.snapshot_id.get() >= next_snapshot_id.get())
        {
            return Err("retention registry next ID is not beyond retained IDs");
        }
        Ok(Self {
            next_snapshot_id,
            roots,
        })
    }
}

impl Default for RetentionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
