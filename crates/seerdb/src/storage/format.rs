//! Durable format primitives and manifest publication.
//!
//! The format is deliberately independent of the B-tree mutation path. It
//! gives the storage engine stable identities and a fail-closed publication
//! primitive before pages, PMT state, and WAL replay are wired together.

#[path = "manifest_history.rs"]
mod manifest_history;
pub use manifest_history::ManifestHistory;

pub use super::retention_format::{RetainedRoot, RetentionRegistry};

/// Current durable format version.
///
/// Version 3 records the logical commit sequence and the WAL end position in
/// every durable commit envelope. The pre-alpha format has no compatibility
/// promise, so older stores fail closed instead of being silently upgraded.
pub const FORMAT_VERSION: u32 = 3;

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
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
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

numeric_id!(
    HistoryId,
    "Stable persisted identity represented by a monotonic integer."
);
numeric_id!(
    GenerationId,
    "Stable persisted identity represented by a monotonic integer."
);
numeric_id!(
    CommitId,
    "Stable persisted identity represented by a monotonic integer."
);
numeric_id!(
    PmtCheckpointId,
    "Stable persisted identity represented by a monotonic integer."
);
numeric_id!(
    SnapshotId,
    "Stable persisted identity represented by a monotonic integer."
);
numeric_id!(
    TxnId,
    "Logical transaction identity, distinct from the commit sequence assigned on success."
);
numeric_id!(
    CommitSeq,
    "Commit sequence number assigned when a transaction becomes visible."
);

/// Logical write-ahead-log position.
///
/// The high 32 bits identify the retained WAL segment and the low 32 bits
/// identify a byte offset at the end of a committed WAL record. Encoding the
/// segment and offset together makes ordering survive WAL-file reclamation
/// while keeping positions compact in public commit results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(u64);

impl Lsn {
    /// Maximum byte offset representable within one WAL segment.
    pub const MAX_OFFSET: u64 = u32::MAX as u64;

    /// Construct an LSN from its persisted packed representation.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the persisted packed representation.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Construct an LSN from a WAL segment and end offset.
    pub const fn from_wal_position(segment: u64, offset: u64) -> Option<Self> {
        if segment > u32::MAX as u64 || offset > Self::MAX_OFFSET {
            return None;
        }
        Some(Self((segment << 32) | offset))
    }

    /// Return the WAL segment component.
    pub const fn segment(self) -> u64 {
        self.0 >> 32
    }

    /// Return the byte offset component.
    pub const fn offset(self) -> u64 {
        self.0 & Self::MAX_OFFSET
    }
}

/// Logical and durable position returned for a committed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitPosition {
    /// Logical committed visibility order (CSN).
    pub csn: CommitSeq,
    /// Durable WAL end position (LSN).
    pub lsn: Lsn,
}

impl CommitPosition {
    /// Construct a commit position.
    pub const fn new(csn: CommitSeq, lsn: Lsn) -> Self {
        Self { csn, lsn }
    }
}
numeric_id!(TreeId, "First-class ordered keyspace identity.");
numeric_id!(PageVersion, "Version of one logical page mapping.");

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
    /// Monotonic durable commit identity used by the physical generation
    /// publication protocol.
    pub commit_id: CommitId,
    /// Logical committed visibility order (CSN).
    pub commit_seq: CommitSeq,
    /// Durable WAL end position (LSN) for this commit envelope.
    pub lsn: Lsn,
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
    pub const SERIALIZED_SIZE: usize = 56;

    /// Encode a commit payload for a WAL record.
    pub fn to_bytes(self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut bytes = [0; Self::SERIALIZED_SIZE];
        bytes[0..4].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.commit_id.get().to_le_bytes());
        bytes[12..20].copy_from_slice(&self.commit_seq.get().to_le_bytes());
        bytes[20..28].copy_from_slice(&self.lsn.get().to_le_bytes());
        bytes[28..36].copy_from_slice(&self.generation_id.get().to_le_bytes());
        bytes[36..44].copy_from_slice(&self.root_page_id.to_le_bytes());
        bytes[44..52].copy_from_slice(&self.mutation_count.to_le_bytes());
        bytes[52..56].copy_from_slice(&self.digest.to_le_bytes());
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
            commit_seq: CommitSeq::new(u64::from_le_bytes(bytes[12..20].try_into().ok()?)),
            lsn: Lsn::new(u64::from_le_bytes(bytes[20..28].try_into().ok()?)),
            generation_id: GenerationId::new(u64::from_le_bytes(bytes[28..36].try_into().ok()?)),
            root_page_id: u64::from_le_bytes(bytes[36..44].try_into().ok()?),
            mutation_count: u64::from_le_bytes(bytes[44..52].try_into().ok()?),
            digest: u32::from_le_bytes(bytes[52..56].try_into().ok()?),
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
    /// Physical publication identity made visible by this generation.
    pub commit_id: CommitId,
    /// Logical committed visibility order made visible by this generation.
    pub commit_seq: CommitSeq,
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
        bytes[100..108].copy_from_slice(&self.commit_seq.get().to_le_bytes());
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
            commit_seq: CommitSeq::new(u64::from_le_bytes(
                bytes[100..108]
                    .try_into()
                    .map_err(|_| "invalid commit sequence")?,
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

fn valid_page_size(page_size: u32) -> bool {
    page_size >= 512 && page_size.is_power_of_two()
}

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;
