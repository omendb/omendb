//! Crate-level error types.

use crate::blob::BlobManagerError;
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
    /// An owner-local runtime or storage-state invariant is invalid.
    #[error("runtime state")]
    Runtime,
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

    /// Capacity admission failed before the generation issued physical page
    /// writes. The caller may restore capacity and retry this operation on
    /// the same handle; no ambiguous media state has been created.
    #[error("capacity preflight refused")]
    CapacityPreflight,

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
    #[error("serialization conflict: expected commit {expected:?}, current commit {current:?}")]
    SerializationConflict {
        /// Commit the caller used as its expected base.
        expected: CommitId,
        /// Commit currently published by this writer.
        current: CommitId,
    },

    /// A bounded maintenance operation owns the serialized writer lane.
    #[error("maintenance is in progress: {0}")]
    MaintenanceInProgress(&'static str),

    /// A batch commit is durable, but releasing its temporary root lease
    /// failed. The transaction is committed; the caller may retry cleanup or
    /// drop the transaction so its lease drop guard can retry it.
    #[error("commit {commit:?} succeeded but transaction cleanup failed: {cleanup}")]
    CommitCleanup {
        /// Durable commit identity published before cleanup failed.
        commit: CommitId,
        /// Cleanup error that should be retried or reported.
        cleanup: Box<Error>,
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

    /// Blob manager could not allocate or append a blob record.
    #[error("blob error: {0}")]
    Blob(#[from] BlobManagerError),
}

/// Result type for database operations.
pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        if matches!(
            error.kind(),
            std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
        ) {
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
            InsertError::EntryTooLarge => {
                Error::InvalidArgument("entry is too large for a B-tree page".into())
            }
            InsertError::WrongNodeType => Error::BTree("wrong node type for operation".into()),
            InsertError::InvalidIndex(index) => {
                Error::InvalidArgument(format!("B-tree entry index {index} is out of bounds"))
            }
            InsertError::ValueSizeMismatch { expected, actual } => Error::InvalidArgument(format!(
                "B-tree replacement size mismatch: expected {expected}, got {actual}"
            )),
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

#[cfg(test)]
mod tests {
    use super::Error;
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn storage_full_and_quota_exceeded_are_typed_as_disk_full() {
        assert!(matches!(
            Error::from(IoError::from(ErrorKind::StorageFull)),
            Error::DiskFull
        ));
        assert!(matches!(
            Error::from(IoError::from(ErrorKind::QuotaExceeded)),
            Error::DiskFull
        ));
    }

    #[test]
    fn unrelated_io_errors_keep_their_source() {
        let error = Error::from(IoError::from(ErrorKind::PermissionDenied));
        assert!(matches!(error, Error::Io(_)));
    }
}
