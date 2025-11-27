use thiserror::Error;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL error: {0}")]
    Wal(#[from] crate::wal::WALError),

    #[error("SSTable error: {0}")]
    SSTable(#[from] crate::sstable::SSTableError),

    #[error("Compaction error: {0}")]
    Compaction(#[from] crate::compaction::CompactionError),

    #[error("VLog error: {0}")]
    VLog(#[from] crate::vlog::VLogError),

    #[error("Database not opened")]
    NotOpened,

    #[error("Insufficient disk space: {available} bytes available, {required} bytes required")]
    DiskSpaceFull { available: u64, required: u64 },

    #[error("Background thread panic: {thread_name} - database may be in inconsistent state")]
    BackgroundThreadPanic { thread_name: String },

    #[error("Object store error: {0}")]
    ObjectStore(String),

    #[error("Transaction aborted or already committed")]
    TransactionAborted,

    #[error("WAL corruption: {0}")]
    WalCorruption(String),

    #[error("Transaction conflict: {0}")]
    TransactionConflict(crate::transaction::TransactionConflict),
}

pub type Result<T> = std::result::Result<T, DBError>;

/// Result of full database integrity verification.
#[derive(Debug, Clone, Default)]
pub struct VerifyResult {
    pub sstables_verified: u64,
    pub blocks_verified: u64,
    pub sstable_bytes_verified: u64,
    pub vlog_records_verified: u64,
    pub vlog_bytes_verified: u64,
    pub vlog_verified: bool,
}
