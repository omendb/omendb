use std::io;
use thiserror::Error;

use super::block::BlockError;

#[derive(Debug, Error)]
pub enum SSTableError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Invalid SSTable format")]
    InvalidFormat,

    #[error("VLog error: {0}")]
    VLog(String),

    #[error("SSTable corrupted: expected checksum {expected:#x}, got {actual:#x}")]
    Corruption { expected: u32, actual: u32 },

    #[error("Block error: {0}")]
    Block(#[from] BlockError),

    #[error("Buffer pool error: {0}")]
    BufferPool(#[from] crate::buffer::BufferPoolError),
}

pub type Result<T> = std::result::Result<T, SSTableError>;
