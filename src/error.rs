//! Crate-level error types.

use crate::btree::{BTreeError, InsertError, SplitError};
use crate::buffer::BufferError;
use crate::storage::format::CommitId;

/// Category of a failure reported by the non-mutating integrity checker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CheckFailureKind {
    /// The requested check target is missing or not a database directory.
    #[error("target")]
    Target,
    /// A persisted format or compatibility rule was rejected.
    #[error("format")]
    Format,
    /// The manifest or its selected identity is invalid.
    #[error("manifest")]
    Manifest,
    /// The selected PMT/allocator checkpoint is invalid or mismatched.
    #[error("checkpoint")]
    Checkpoint,
    /// A physical data page is missing, malformed, or has a bad checksum.
    #[error("data page")]
    DataPage,
    /// The logical B-tree graph violates reachability or routing invariants.
    #[error("tree structure")]
    Structure,
    /// Blob metadata or a referenced blob target is invalid.
    #[error("blob")]
    Blob,
    /// The WAL is malformed or has an invalid recovery frontier.
    #[error("wal")]
    Wal,
    /// The checker could not read a required artifact.
    #[error("io")]
    Io,
}

/// Errors that can occur during database operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// I/O error (file operations).
    #[error("io error: {0}")]
    Io(std::io::Error),

    /// The database or filesystem has no space for the requested write.
    #[error("disk full")]
    DiskFull,

    /// The configured WAL admission budget cannot cover this mutation and
    /// its closing commit envelope. The caller may flush and retry.
    #[error("write backpressure: requires {required} WAL bytes, {available} available")]
    Backpressure { required: u64, available: u64 },

    /// Another writable handle owns the database directory.
    #[error("database is busy")]
    DatabaseBusy,

    /// The opened directory is an immutable archive/snapshot.
    #[error("database is read-only")]
    ReadOnly,

    /// Key not found.
    #[error("key not found")]
    NotFound,

    /// Key already exists.
    #[error("duplicate key")]
    DuplicateKey,

    /// Page is full (needs split).
    #[error("page full")]
    PageFull,

    /// Corruption detected (checksum mismatch, invalid data).
    #[error("corruption: {0}")]
    Corruption(String),

    /// A non-mutating integrity check failed with an actionable category.
    #[error("integrity check failed ({kind}): {message}")]
    Check {
        /// Failure category for machine-readable handling.
        kind: CheckFailureKind,
        /// Human-readable detail retained from the failing boundary.
        message: String,
    },

    /// Database is corrupted and needs recovery.
    #[error("database needs recovery: {0}")]
    NeedsRecovery(String),

    /// Invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A retained historical root is no longer available to this handle.
    #[error("snapshot unavailable: {0}")]
    SnapshotUnavailable(String),

    /// The caller attempted to commit against a stale published state.
    #[error(
        "serialization conflict: expected commit {expected:?}, current commit {current:?}"
    )]
    SerializationConflict {
        /// Commit the caller used as its expected base.
        expected: CommitId,
        /// Commit currently published by this writer.
        current: CommitId,
    },

    /// B-tree operation failed.
    #[error("btree error: {0}")]
    BTree(String),

    /// WAL error.
    #[error("wal error: {0}")]
    Wal(String),

    /// Buffer pool could not safely provide a page frame.
    #[error("buffer error: {0}")]
    Buffer(String),
}

/// Result type for database operations.
pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        if error.kind() == std::io::ErrorKind::StorageFull {
            Self::DiskFull
        } else {
            Self::Io(error)
        }
    }
}

impl From<InsertError> for Error {
    fn from(e: InsertError) -> Self {
        match e {
            InsertError::PageFull => Error::PageFull,
            InsertError::WrongNodeType => Error::BTree("wrong node type for operation".into()),
            InsertError::DuplicateKey(_) => Error::DuplicateKey,
        }
    }
}

impl From<SplitError> for Error {
    fn from(e: SplitError) -> Self {
        Error::BTree(format!("split failed: {}", e))
    }
}

impl From<BTreeError> for Error {
    fn from(e: BTreeError) -> Self {
        match e {
            BTreeError::DuplicateKey => Error::DuplicateKey,
            BTreeError::InsertFailed(e) => Error::from(e),
            BTreeError::SplitFailed(e) => Error::from(e),
            BTreeError::PageIdExhausted => Error::BTree("logical page ID exhausted".into()),
            BTreeError::MissingPage(page_id) => {
                Error::Corruption(format!("B-tree page {page_id} is not loaded"))
            }
            BTreeError::Corruption(message) => Error::Corruption(message),
        }
    }
}

impl From<BufferError> for Error {
    fn from(error: BufferError) -> Self {
        Self::Buffer(error.to_string())
    }
}
