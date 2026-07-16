//! Crate-level error types.

use crate::btree::{BTreeError, InsertError, SplitError};
use crate::buffer::BufferError;

/// Errors that can occur during database operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// I/O error (file operations).
    #[error("io error: {0}")]
    Io(std::io::Error),

    /// The database or filesystem has no space for the requested write.
    #[error("disk full")]
    DiskFull,

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

    /// Database is corrupted and needs recovery.
    #[error("database needs recovery: {0}")]
    NeedsRecovery(String),

    /// Invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

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
        }
    }
}

impl From<BufferError> for Error {
    fn from(error: BufferError) -> Self {
        Self::Buffer(error.to_string())
    }
}
