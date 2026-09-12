//! Replacement transaction/storage-kernel primitives.
//!
//! This module is temporary migration scaffolding while the current SeerDB
//! implementation remains the semantic/recovery oracle. The types here are not
//! a second permanent backend; they are the new ownership/concurrency model
//! being qualified before cutover.

mod buffer;
mod frame;
mod ids;
mod object;
mod translation;

pub use buffer::{
    BufferError, BufferPool, BufferStats, PageGuard, PageIo, PageIoOperation, PageWriteGuard,
};
pub use frame::{
    FrameMeta, FramePin, FrameState, FrameTransitionError, FrameVersion, FrameWriteLatch,
};
pub use ids::{FrameId, FrameIncarnation, FrameRef, PageId, PageKey, StorageObjectId};
pub use object::{ObjectAuthority, StorageObjectDescriptor};
pub use translation::{PublishResult, TranslationError, TranslationTable};

// Keep the already-proven logical identity domains instead of manufacturing
// vNext-specific duplicates.
pub use crate::storage::format::{CommitSeq, Lsn, TxnId};
