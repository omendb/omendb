//! Durable format primitives and manifest publication.
//!
//! The format is deliberately independent of the B-tree mutation path. It
//! gives the storage engine stable identities and a fail-closed publication
//! primitive before pages, PMT state, and WAL replay are wired together.

use std::collections::BTreeSet;

pub use super::manifest_store::ManifestStore;
pub use super::retention_format::{RetainedRoot, RetentionRegistry};

/// Current durable format version.
pub const FORMAT_VERSION: u32 = 2;

/// Fixed size of the superblock record.
pub const SUPERBLOCK_SIZE: usize = 4096;

/// Fixed size of each manifest slot.
pub const MANIFEST_SLOT_SIZE: usize = 256;

const SUPERBLOCK_MAGIC: [u8; 8] = *b"SEERDBSB";
const MANIFEST_MAGIC: [u8; 8] = *b"SEERMNF1";

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

/// One publication attempt recorded before page writes begin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReuseAttempt {
    /// Logical commit identity reserved by this generation.
    pub commit_id: CommitId,
    /// Generation that may overwrite the listed physical offsets.
    pub generation_id: GenerationId,
    /// Physical page slots selected for this generation. This may be empty
    /// when the entry exists only to reserve an ambiguous commit identity.
    pub offsets: Vec<u64>,
}

/// Durable ledger of publication attempts whose outcome may be
/// indeterminate after a process or filesystem failure.
///
/// Entries are written before any candidate page reaches the device. A
/// successful publication is removed from the in-memory ledger after its
/// manifest is durable. For reused slots, on-disk cleanup may be deferred until
/// the next publication or reopen; both reconcile entries against authoritative
/// manifest history. An entry absent from history is retained so its reserved
/// identity is never reused; non-empty entries also cause historical reads to
/// fail closed for their offsets.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReuseLedger {
    attempts: Vec<ReuseAttempt>,
}

const REUSE_LEDGER_MAGIC: [u8; 8] = *b"SEERREU1";
const REUSE_LEDGER_HEADER_SIZE: usize = 8 + 4 + 4;
const REUSE_LEDGER_ATTEMPT_HEADER_SIZE: usize = 8 + 8 + 4;
const REUSE_LEDGER_CHECKSUM_SIZE: usize = 4;

impl ReuseLedger {
    /// Create an empty reuse ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return attempts in generation order.
    pub fn attempts(&self) -> &[ReuseAttempt] {
        &self.attempts
    }

    /// Record one generation's candidate offsets.
    pub fn push(&mut self, attempt: ReuseAttempt) -> std::result::Result<(), &'static str> {
        let mut offsets = BTreeSet::new();
        if attempt
            .offsets
            .iter()
            .any(|offset| !offsets.insert(*offset))
        {
            return Err("reuse ledger contains duplicate offsets");
        }
        if let Some(existing) = self
            .attempts
            .iter_mut()
            .find(|existing| existing.generation_id == attempt.generation_id)
        {
            if existing.commit_id != attempt.commit_id {
                return Err("reuse ledger generation has conflicting commit IDs");
            }
            existing.offsets.extend(attempt.offsets);
            existing.offsets.sort_unstable();
            existing.offsets.dedup();
            return Ok(());
        }
        self.attempts.push(attempt);
        self.attempts
            .sort_unstable_by_key(|attempt| attempt.generation_id);
        Ok(())
    }

    /// Remove an attempt after its generation is durably published.
    pub fn remove_generation(&mut self, generation_id: GenerationId) -> bool {
        let before = self.attempts.len();
        self.attempts
            .retain(|attempt| attempt.generation_id != generation_id);
        before != self.attempts.len()
    }

    /// Remove entries reconciled by authoritative history.
    ///
    /// A non-empty attempt remains until its exact generation is published,
    /// because its physical offsets may still overlap an older retained root.
    /// An empty reservation has no physical liveness obligation and can be
    /// removed once a later commit proves that its identity was skipped.
    pub fn prune_published(&mut self, history: &ManifestHistory) -> usize {
        let before = self.attempts.len();
        self.attempts.retain(|attempt| {
            let generation_published = history
                .manifests()
                .iter()
                .any(|manifest| manifest.generation_id == attempt.generation_id);
            let empty_reservation_superseded = attempt.offsets.is_empty()
                && history
                    .manifests()
                    .iter()
                    .any(|manifest| manifest.commit_id > attempt.commit_id);
            !(generation_published || empty_reservation_superseded)
        });
        before.saturating_sub(self.attempts.len())
    }

    /// Encode the exact-length checksummed ledger envelope.
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let count = u32::try_from(self.attempts.len()).ok()?;
        let body_len =
            self.attempts
                .iter()
                .try_fold(REUSE_LEDGER_HEADER_SIZE, |length, attempt| {
                    let offset_bytes = attempt.offsets.len().checked_mul(8)?;
                    length
                        .checked_add(REUSE_LEDGER_ATTEMPT_HEADER_SIZE)?
                        .checked_add(offset_bytes)
                })?;
        let total_len = body_len.checked_add(REUSE_LEDGER_CHECKSUM_SIZE)?;
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&REUSE_LEDGER_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        for attempt in &self.attempts {
            bytes.extend_from_slice(&attempt.commit_id.get().to_le_bytes());
            bytes.extend_from_slice(&attempt.generation_id.get().to_le_bytes());
            bytes.extend_from_slice(&u32::try_from(attempt.offsets.len()).ok()?.to_le_bytes());
            for offset in &attempt.offsets {
                bytes.extend_from_slice(&offset.to_le_bytes());
            }
        }
        let checksum = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Some(bytes)
    }

    /// Decode and validate a reuse ledger envelope.
    pub fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, &'static str> {
        if bytes.len() < REUSE_LEDGER_HEADER_SIZE + REUSE_LEDGER_CHECKSUM_SIZE {
            return Err("reuse ledger is truncated");
        }
        if bytes[..8] != REUSE_LEDGER_MAGIC {
            return Err("invalid reuse ledger magic");
        }
        let version = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .map_err(|_| "reuse ledger version is truncated")?,
        );
        if version != FORMAT_VERSION {
            return Err("unsupported reuse ledger format version");
        }
        let count = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| "reuse ledger count is truncated")?,
        ) as usize;
        let checksum_offset = bytes.len() - REUSE_LEDGER_CHECKSUM_SIZE;
        let expected = u32::from_le_bytes(
            bytes[checksum_offset..]
                .try_into()
                .map_err(|_| "reuse ledger checksum is truncated")?,
        );
        if expected != crc32c::crc32c(&bytes[..checksum_offset]) {
            return Err("reuse ledger checksum mismatch");
        }

        let mut cursor = REUSE_LEDGER_HEADER_SIZE;
        let mut ledger = Self::new();
        for _ in 0..count {
            let header_end = cursor
                .checked_add(REUSE_LEDGER_ATTEMPT_HEADER_SIZE)
                .ok_or("reuse ledger length overflows")?;
            if header_end > checksum_offset {
                return Err("reuse ledger attempt is truncated");
            }
            let commit_id = CommitId::new(u64::from_le_bytes(
                bytes[cursor..cursor + 8]
                    .try_into()
                    .map_err(|_| "reuse ledger commit is truncated")?,
            ));
            let generation_id = GenerationId::new(u64::from_le_bytes(
                bytes[cursor + 8..cursor + 16]
                    .try_into()
                    .map_err(|_| "reuse ledger generation is truncated")?,
            ));
            let offset_count = u32::from_le_bytes(
                bytes[cursor + 16..header_end]
                    .try_into()
                    .map_err(|_| "reuse ledger offset count is truncated")?,
            ) as usize;
            cursor = header_end;
            let offset_bytes = offset_count
                .checked_mul(8)
                .ok_or("reuse ledger offset length overflows")?;
            let offsets_end = cursor
                .checked_add(offset_bytes)
                .ok_or("reuse ledger length overflows")?;
            if offsets_end > checksum_offset {
                return Err("reuse ledger offsets are truncated");
            }
            let mut offsets = Vec::with_capacity(offset_count);
            for chunk in bytes[cursor..offsets_end].chunks_exact(8) {
                offsets.push(u64::from_le_bytes(
                    chunk
                        .try_into()
                        .map_err(|_| "reuse ledger offset is truncated")?,
                ));
            }
            cursor = offsets_end;
            ledger.push(ReuseAttempt {
                commit_id,
                generation_id,
                offsets,
            })?;
        }
        if cursor != checksum_offset {
            return Err("reuse ledger has trailing bytes");
        }
        Ok(ledger)
    }
}

fn valid_page_size(page_size: u32) -> bool {
    page_size >= 512 && page_size.is_power_of_two()
}

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;
