//! Durable format primitives and manifest publication.
//!
//! The format is deliberately independent of the B-tree mutation path. It
//! gives the storage engine stable identities and a fail-closed publication
//! primitive before pages, PMT state, and WAL replay are wired together.

use crate::error::{Error, Result};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(any(test, feature = "fault-injection"))]
use std::cell::Cell;

#[cfg(any(test, feature = "fault-injection"))]
thread_local! {
    static FAIL_NEXT_MANIFEST_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_MIRROR_MANIFEST_SYNC: Cell<bool> = const { Cell::new(false) };
}

/// Current durable format version.
pub const FORMAT_VERSION: u32 = 2;

/// Fixed size of the superblock record.
pub const SUPERBLOCK_SIZE: usize = 4096;

/// Fixed size of each manifest slot.
pub const MANIFEST_SLOT_SIZE: usize = 256;

const SUPERBLOCK_MAGIC: [u8; 8] = *b"SEERDBSB";
const MANIFEST_MAGIC: [u8; 8] = *b"SEERMNF1";
const MANIFEST_SLOT_COUNT: usize = 2;
const MANIFEST_FILE_SIZE: u64 = (MANIFEST_SLOT_SIZE * MANIFEST_SLOT_COUNT) as u64;

/// Stable identity for one database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct DatabaseId([u8; 16]);

impl DatabaseId {
    /// Construct an ID from its persisted bytes.
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Return the persisted representation.
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

macro_rules! numeric_id {
    ($name:ident) => {
        #[doc = "Stable persisted identity represented by a monotonic integer."]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(u64);

        impl $name {
            /// Construct an ID from its persisted integer.
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Return the persisted integer.
            pub const fn get(self) -> u64 {
                self.0
            }
        }
    };
}

numeric_id!(HistoryId);
numeric_id!(GenerationId);
numeric_id!(CommitId);
numeric_id!(PmtCheckpointId);
numeric_id!(SnapshotId);

/// Fixed-format database superblock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Superblock {
    /// Database identity.
    pub database_id: DatabaseId,
    /// Logical history identity.
    pub history_id: HistoryId,
    /// Configured physical page size.
    pub page_size: u32,
    /// Durable format version.
    pub format_version: u32,
}

impl Superblock {
    /// Create a current-format superblock.
    pub fn new(database_id: DatabaseId, history_id: HistoryId, page_size: u32) -> Option<Self> {
        if !valid_page_size(page_size) {
            return None;
        }

        Some(Self {
            database_id,
            history_id,
            page_size,
            format_version: FORMAT_VERSION,
        })
    }

    /// Encode the superblock into its fixed-size sector.
    pub fn to_bytes(self) -> [u8; SUPERBLOCK_SIZE] {
        let mut bytes = [0; SUPERBLOCK_SIZE];
        bytes[0..8].copy_from_slice(&SUPERBLOCK_MAGIC);
        bytes[8..12].copy_from_slice(&self.format_version.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        bytes[16..32].copy_from_slice(&self.database_id.as_bytes());
        bytes[32..40].copy_from_slice(&self.history_id.get().to_le_bytes());
        let checksum = crc32c::crc32c(&bytes[..40]);
        bytes[40..44].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// Decode and validate a superblock.
    pub fn from_bytes(bytes: &[u8; SUPERBLOCK_SIZE]) -> Option<Self> {
        if bytes[0..8] != SUPERBLOCK_MAGIC {
            return None;
        }

        let stored_checksum = u32::from_le_bytes(bytes[40..44].try_into().ok()?);
        if stored_checksum != crc32c::crc32c(&bytes[..40]) {
            return None;
        }

        let format_version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
        let page_size = u32::from_le_bytes(bytes[12..16].try_into().ok()?);
        if format_version != FORMAT_VERSION || !valid_page_size(page_size) {
            return None;
        }

        let database_id = DatabaseId::new(bytes[16..32].try_into().ok()?);
        let history_id = HistoryId::new(u64::from_le_bytes(bytes[32..40].try_into().ok()?));
        Some(Self {
            database_id,
            history_id,
            page_size,
            format_version,
        })
    }
}

/// Commit metadata persisted in a logical WAL commit record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitRecord {
    /// Monotonic durable commit identity.
    pub commit_id: CommitId,
    /// Root generation made visible by this commit.
    pub generation_id: GenerationId,
    /// Root page for the new generation.
    pub root_page_id: u64,
    /// Number of mutations covered by the commit.
    pub mutation_count: u64,
    /// Digest over the transaction's logical mutations.
    pub digest: u32,
}

impl CommitRecord {
    /// Serialized payload size, including the format version.
    pub const SERIALIZED_SIZE: usize = 40;

    /// Encode a commit payload for a WAL record.
    pub fn to_bytes(self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut bytes = [0; Self::SERIALIZED_SIZE];
        bytes[0..4].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.commit_id.get().to_le_bytes());
        bytes[12..20].copy_from_slice(&self.generation_id.get().to_le_bytes());
        bytes[20..28].copy_from_slice(&self.root_page_id.to_le_bytes());
        bytes[28..36].copy_from_slice(&self.mutation_count.to_le_bytes());
        bytes[36..40].copy_from_slice(&self.digest.to_le_bytes());
        bytes
    }

    /// Decode a commit payload, rejecting truncation and future versions.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::SERIALIZED_SIZE {
            return None;
        }

        let format_version = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        if format_version != FORMAT_VERSION {
            return None;
        }

        Some(Self {
            commit_id: CommitId::new(u64::from_le_bytes(bytes[4..12].try_into().ok()?)),
            generation_id: GenerationId::new(u64::from_le_bytes(bytes[12..20].try_into().ok()?)),
            root_page_id: u64::from_le_bytes(bytes[20..28].try_into().ok()?),
            mutation_count: u64::from_le_bytes(bytes[28..36].try_into().ok()?),
            digest: u32::from_le_bytes(bytes[36..40].try_into().ok()?),
        })
    }
}

/// Authoritative root-generation descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Manifest {
    /// Database identity.
    pub database_id: DatabaseId,
    /// Logical history identity.
    pub history_id: HistoryId,
    /// Generation identity used for slot ordering.
    pub generation_id: GenerationId,
    /// Commit made visible by this generation.
    pub commit_id: CommitId,
    /// Physical page size used by this database.
    pub page_size: u32,
    /// Root logical page ID.
    pub root_page_id: u64,
    /// PMT checkpoint identity included by this generation.
    pub pmt_checkpoint_id: PmtCheckpointId,
    /// WAL segment containing the commit record.
    pub wal_segment: u64,
    /// Byte offset of the commit record within the WAL segment.
    pub wal_offset: u64,
    /// Number of mutations covered by the commit.
    pub mutation_count: u64,
    /// Digest over the transaction's logical mutations.
    pub digest: u32,
    /// Durable format version.
    pub format_version: u32,
}

impl Manifest {
    /// Byte offset of the checksum within a manifest slot.
    const CHECKSUM_OFFSET: usize = 252;

    /// Encode a manifest slot with a CRC32C checksum.
    pub fn to_bytes(self) -> [u8; MANIFEST_SLOT_SIZE] {
        let mut bytes = [0; MANIFEST_SLOT_SIZE];
        bytes[0..8].copy_from_slice(&MANIFEST_MAGIC);
        bytes[8..12].copy_from_slice(&self.format_version.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        bytes[16..32].copy_from_slice(&self.database_id.as_bytes());
        bytes[32..40].copy_from_slice(&self.history_id.get().to_le_bytes());
        bytes[40..48].copy_from_slice(&self.generation_id.get().to_le_bytes());
        bytes[48..56].copy_from_slice(&self.commit_id.get().to_le_bytes());
        bytes[56..64].copy_from_slice(&self.root_page_id.to_le_bytes());
        bytes[64..72].copy_from_slice(&self.pmt_checkpoint_id.get().to_le_bytes());
        bytes[72..80].copy_from_slice(&self.wal_segment.to_le_bytes());
        bytes[80..88].copy_from_slice(&self.wal_offset.to_le_bytes());
        bytes[88..96].copy_from_slice(&self.mutation_count.to_le_bytes());
        bytes[96..100].copy_from_slice(&self.digest.to_le_bytes());
        let checksum = crc32c::crc32c(&bytes[..Self::CHECKSUM_OFFSET]);
        bytes[Self::CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// Decode a manifest slot.
    ///
    /// A zero-filled slot returns `Ok(None)`. Any non-empty invalid slot is an
    /// error so a failed first publication cannot be mistaken for a new DB.
    pub fn from_bytes(
        bytes: &[u8; MANIFEST_SLOT_SIZE],
    ) -> std::result::Result<Option<Self>, &'static str> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Ok(None);
        }
        if bytes[0..8] != MANIFEST_MAGIC {
            return Err("invalid manifest magic");
        }

        let stored_checksum = u32::from_le_bytes(
            bytes[Self::CHECKSUM_OFFSET..]
                .try_into()
                .map_err(|_| "invalid manifest checksum")?,
        );
        if stored_checksum != crc32c::crc32c(&bytes[..Self::CHECKSUM_OFFSET]) {
            return Err("manifest checksum mismatch");
        }

        let format_version = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .map_err(|_| "invalid manifest version")?,
        );
        let page_size = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| "invalid manifest page size")?,
        );
        if format_version != FORMAT_VERSION || !valid_page_size(page_size) {
            return Err("unsupported manifest format");
        }

        Ok(Some(Self {
            database_id: DatabaseId::new(
                bytes[16..32]
                    .try_into()
                    .map_err(|_| "invalid database ID")?,
            ),
            history_id: HistoryId::new(u64::from_le_bytes(
                bytes[32..40].try_into().map_err(|_| "invalid history ID")?,
            )),
            generation_id: GenerationId::new(u64::from_le_bytes(
                bytes[40..48]
                    .try_into()
                    .map_err(|_| "invalid generation ID")?,
            )),
            commit_id: CommitId::new(u64::from_le_bytes(
                bytes[48..56].try_into().map_err(|_| "invalid commit ID")?,
            )),
            root_page_id: u64::from_le_bytes(
                bytes[56..64]
                    .try_into()
                    .map_err(|_| "invalid root page ID")?,
            ),
            pmt_checkpoint_id: PmtCheckpointId::new(u64::from_le_bytes(
                bytes[64..72]
                    .try_into()
                    .map_err(|_| "invalid PMT checkpoint ID")?,
            )),
            wal_segment: u64::from_le_bytes(
                bytes[72..80]
                    .try_into()
                    .map_err(|_| "invalid WAL segment")?,
            ),
            wal_offset: u64::from_le_bytes(
                bytes[80..88].try_into().map_err(|_| "invalid WAL offset")?,
            ),
            mutation_count: u64::from_le_bytes(
                bytes[88..96]
                    .try_into()
                    .map_err(|_| "invalid mutation count")?,
            ),
            digest: u32::from_le_bytes(bytes[96..100].try_into().map_err(|_| "invalid digest")?),
            format_version,
            page_size,
        }))
    }

    /// Whether this generation is newer than another valid generation.
    pub fn is_newer_than(self, other: Self) -> bool {
        self.generation_id > other.generation_id
            || (self.generation_id == other.generation_id && self.commit_id > other.commit_id)
    }
}

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
            .chunks_exact(MANIFEST_HISTORY_ENTRY_SIZE)
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

/// Durable registry entry for a retained root generation.
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

/// A two-slot manifest file with fail-closed publication.
pub struct ManifestStore {
    file: File,
}

impl ManifestStore {
    /// Open or create a two-slot manifest file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        let length = file.metadata()?.len();
        match length {
            0 => file.set_len(MANIFEST_FILE_SIZE)?,
            MANIFEST_FILE_SIZE => {}
            _ => {
                return Err(Error::Corruption(format!(
                    "manifest has invalid length {length}"
                )));
            }
        }
        file.seek(SeekFrom::Start(0))?;
        Ok(Self { file })
    }

    /// Open an existing manifest without write permissions.
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let length = file.metadata()?.len();
        if length != MANIFEST_FILE_SIZE {
            return Err(Error::Corruption(format!(
                "manifest has invalid length {length}"
            )));
        }
        Ok(Self { file })
    }

    /// Load the newest valid manifest generation.
    pub fn load_latest(&mut self) -> Result<Option<Manifest>> {
        let mut newest = None;
        let mut saw_invalid = false;

        for slot in 0..MANIFEST_SLOT_COUNT {
            let bytes = self.read_slot(slot)?;
            match Manifest::from_bytes(&bytes) {
                Ok(Some(manifest)) => {
                    if newest.is_none_or(|current| manifest.is_newer_than(current)) {
                        newest = Some(manifest);
                    }
                }
                Ok(None) => {}
                Err(_) => saw_invalid = true,
            }
        }

        if newest.is_none() && saw_invalid {
            return Err(Error::Corruption("no valid manifest generation".into()));
        }
        Ok(newest)
    }

    /// Publish a new manifest into the inactive slot and sync it.
    pub fn publish(&mut self, manifest: Manifest) -> Result<()> {
        let current_slot = self.current_slot()?;
        let target_slot = current_slot.map_or(0, |slot| 1 - slot);
        let bytes = manifest.to_bytes();
        self.write_slot(target_slot, &bytes)?;
        self.file.flush()?;

        self.sync_manifest(false)
    }

    /// Copy the current manifest into the inactive slot before a maintenance
    /// or user generation may reuse pages named by the older slot.
    pub fn publish_mirrored(&mut self, manifest: Manifest) -> Result<()> {
        let current_slot = self.current_slot()?;
        let target_slot = current_slot.map_or(0, |slot| 1 - slot);
        let bytes = manifest.to_bytes();
        self.write_slot(target_slot, &bytes)?;
        self.file.flush()?;

        self.sync_manifest(true)
    }

    fn sync_manifest(&mut self, mirror: bool) -> Result<()> {
        #[cfg(any(test, feature = "fault-injection"))]
        if mirror {
            if FAIL_NEXT_MIRROR_MANIFEST_SYNC.with(|failure| failure.replace(false)) {
                return Err(std::io::Error::other("injected mirror manifest sync failure").into());
            }
        } else if FAIL_NEXT_MANIFEST_SYNC.with(|failure| failure.replace(false)) {
            return Err(std::io::Error::other("injected manifest sync failure").into());
        }

        self.file.sync_all()?;
        Ok(())
    }

    /// Publish identical metadata into both slots.
    ///
    /// This is used when a copied archive becomes a new history. Equal
    /// generation/commit identities otherwise make the normal alternating
    /// publisher continue selecting the same slot.
    pub fn publish_replicated(&mut self, manifest: Manifest) -> Result<()> {
        let bytes = manifest.to_bytes();
        self.write_slot(0, &bytes)?;
        self.write_slot(1, &bytes)?;
        self.file.flush()?;

        self.sync_manifest(false)
    }

    /// Inject one failure at the next manifest sync boundary.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_sync_failure(&self) {
        FAIL_NEXT_MANIFEST_SYNC.with(|failure| failure.set(true));
    }

    /// Inject one failure at the next safety-mirror sync boundary.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_mirror_sync_failure(&self) {
        FAIL_NEXT_MIRROR_MANIFEST_SYNC.with(|failure| failure.set(true));
    }

    fn write_slot(&mut self, slot: usize, bytes: &[u8; MANIFEST_SLOT_SIZE]) -> Result<()> {
        self.file
            .seek(SeekFrom::Start((slot * MANIFEST_SLOT_SIZE) as u64))?;
        self.file.write_all(bytes)?;
        Ok(())
    }

    fn current_slot(&mut self) -> Result<Option<usize>> {
        let mut newest = None;
        let mut saw_invalid = false;

        for slot in 0..MANIFEST_SLOT_COUNT {
            let bytes = self.read_slot(slot)?;
            match Manifest::from_bytes(&bytes) {
                Ok(Some(manifest)) => {
                    if newest.is_none_or(|(_, current)| manifest.is_newer_than(current)) {
                        newest = Some((slot, manifest));
                    }
                }
                Ok(None) => {}
                Err(_) => saw_invalid = true,
            }
        }

        if newest.is_none() && saw_invalid {
            return Err(Error::Corruption("no valid manifest generation".into()));
        }
        Ok(newest.map(|(slot, _)| slot))
    }

    fn read_slot(&mut self, slot: usize) -> Result<[u8; MANIFEST_SLOT_SIZE]> {
        let mut bytes = [0; MANIFEST_SLOT_SIZE];
        self.file
            .seek(SeekFrom::Start((slot * MANIFEST_SLOT_SIZE) as u64))?;
        self.file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

fn valid_page_size(page_size: u32) -> bool {
    page_size >= 512 && page_size.is_power_of_two()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempdir;

    fn database_id() -> DatabaseId {
        DatabaseId::new([7; 16])
    }

    fn manifest(generation_id: u64, commit_id: u64) -> Manifest {
        Manifest {
            database_id: database_id(),
            history_id: HistoryId::new(2),
            generation_id: GenerationId::new(generation_id),
            commit_id: CommitId::new(commit_id),
            page_size: 4096,
            root_page_id: 42,
            pmt_checkpoint_id: PmtCheckpointId::new(9),
            wal_segment: 3,
            wal_offset: 8192,
            mutation_count: 4,
            digest: 0x1234_5678,
            format_version: FORMAT_VERSION,
        }
    }

    #[test]
    fn superblock_roundtrip_and_checksum_validation() {
        let superblock = Superblock::new(database_id(), HistoryId::new(11), 4096).unwrap();
        let bytes = superblock.to_bytes();
        assert_eq!(Superblock::from_bytes(&bytes), Some(superblock));

        let mut corrupt = bytes;
        corrupt[32] ^= 1;
        assert_eq!(Superblock::from_bytes(&corrupt), None);
    }

    #[test]
    fn commit_record_roundtrip() {
        let commit = CommitRecord {
            commit_id: CommitId::new(8),
            generation_id: GenerationId::new(9),
            root_page_id: 10,
            mutation_count: 11,
            digest: 12,
        };
        assert_eq!(CommitRecord::from_bytes(&commit.to_bytes()), Some(commit));
    }

    #[test]
    fn manifest_roundtrip_and_checksum_validation() {
        let expected = manifest(1, 7);
        let bytes = expected.to_bytes();
        assert_eq!(Manifest::from_bytes(&bytes), Ok(Some(expected)));

        let mut corrupt = bytes;
        corrupt[88] ^= 1;
        assert!(Manifest::from_bytes(&corrupt).is_err());
    }

    #[test]
    fn manifest_history_validates_frames_and_ignores_partial_tail() {
        let first = manifest(1, 1);
        let second = manifest(2, 2);
        let mut history = ManifestHistory::new();
        history.push(first).unwrap();
        history.push(second).unwrap();
        let bytes = history.to_bytes().unwrap();
        assert_eq!(ManifestHistory::from_bytes(&bytes).unwrap(), history);

        let mut partial = bytes.clone();
        partial.extend_from_slice(&[0xA5; 17]);
        assert_eq!(ManifestHistory::from_bytes(&partial).unwrap(), history);

        let mut corrupt = bytes;
        corrupt[ManifestHistory::header_bytes().len()] ^= 1;
        assert_eq!(
            ManifestHistory::from_bytes(&corrupt),
            Err("manifest history checksum mismatch")
        );
    }

    #[test]
    fn manifest_store_publishes_and_selects_newest_generation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST");
        let mut store = ManifestStore::open(&path).unwrap();
        assert_eq!(store.load_latest().unwrap(), None);

        store.publish(manifest(1, 1)).unwrap();
        store.publish(manifest(2, 2)).unwrap();
        assert_eq!(store.load_latest().unwrap(), Some(manifest(2, 2)));
    }

    #[cfg(feature = "fault-injection")]
    #[test]
    fn manifest_sync_faults_distinguish_candidate_and_safety_mirror() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST");
        let mut store = ManifestStore::open(&path).unwrap();
        let first = manifest(1, 1);
        let second = manifest(2, 2);

        store.publish(first).unwrap();
        store.inject_sync_failure();
        store.publish_mirrored(first).unwrap();
        assert!(store.publish(second).is_err());

        store.inject_mirror_sync_failure();
        assert!(store.publish_mirrored(first).is_err());
        store.publish(second).unwrap();
    }

    #[test]
    fn manifest_store_falls_back_after_torn_inactive_slot() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST");
        let mut store = ManifestStore::open(&path).unwrap();
        store.publish(manifest(1, 1)).unwrap();
        store.publish(manifest(2, 2)).unwrap();
        drop(store);

        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(MANIFEST_SLOT_SIZE as u64))
            .unwrap();
        file.write_all(&[0xA5; 32]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut reopened = ManifestStore::open(&path).unwrap();
        assert_eq!(reopened.load_latest().unwrap(), Some(manifest(1, 1)));
    }

    #[test]
    fn retention_registry_roundtrip_and_exact_validation() {
        let mut registry = RetentionRegistry::new();
        let first = registry.insert(manifest(3, 3)).unwrap();
        let second = registry.insert(manifest(4, 4)).unwrap();
        let bytes = registry.to_bytes().unwrap();
        assert_eq!(RetentionRegistry::from_bytes(&bytes).unwrap(), registry);

        let removed = registry.remove(first).unwrap();
        assert_eq!(removed.snapshot_id, first);
        assert!(registry.remove(first).is_none());
        assert_eq!(registry.roots()[0].snapshot_id, second);

        let mut truncated = bytes.clone();
        truncated.pop();
        assert_eq!(
            RetentionRegistry::from_bytes(&truncated),
            Err("retention registry is truncated")
        );

        let mut torn = bytes;
        torn[24] ^= 1;
        assert_eq!(
            RetentionRegistry::from_bytes(&torn),
            Err("retention registry checksum mismatch")
        );
    }

    #[test]
    fn retention_registry_rejects_duplicate_and_future_ids() {
        let mut registry = RetentionRegistry::new();
        registry.insert(manifest(1, 1)).unwrap();
        registry.insert(manifest(2, 2)).unwrap();
        let mut bytes = registry.to_bytes().unwrap();
        // The second entry ID starts after the first ID and manifest.
        let second_id_offset = 24 + 8 + MANIFEST_SLOT_SIZE;
        bytes[second_id_offset..second_id_offset + 8].copy_from_slice(&1u64.to_le_bytes());
        let checksum = crc32c::crc32c(&bytes[..bytes.len() - 4]);
        let checksum_offset = bytes.len() - 4;
        bytes[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
        assert_eq!(
            RetentionRegistry::from_bytes(&bytes),
            Err("retention registry contains a duplicate or invalid ID")
        );

        let mut future = registry.to_bytes().unwrap();
        future[12..20].copy_from_slice(&1u64.to_le_bytes());
        let checksum = crc32c::crc32c(&future[..future.len() - 4]);
        let checksum_offset = future.len() - 4;
        future[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
        assert_eq!(
            RetentionRegistry::from_bytes(&future),
            Err("retention registry next ID is not beyond retained IDs")
        );
    }
}
