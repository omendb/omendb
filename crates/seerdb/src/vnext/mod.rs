//! Replacement transaction/storage-kernel primitives.
//!
//! This module is temporary migration scaffolding while the current SeerDB
//! implementation remains the semantic/recovery oracle. The types here are not
//! a second permanent backend; they are the new ownership/concurrency model
//! being qualified before cutover.

mod btree;
mod buffer;
mod frame;
mod ids;
mod log;
mod object;
mod recovery;
mod translation;
mod txn;

pub use btree::{BTreeError, BTreeLookup, BTreeObject, RangeCursor};
pub use buffer::{
    BufferError, BufferPool, BufferStats, PageGuard, PageIo, PageIoOperation, PageWriteGuard,
};
pub use frame::{
    FrameMeta, FramePin, FrameState, FrameTransitionError, FrameVersion, FrameWriteLatch,
};
pub use ids::{FrameId, FrameIncarnation, FrameRef, PageId, PageKey, StorageObjectId};
pub use log::{
    mutation_digest, parse_log_prefix, CommitDecision, LogEncodeError, LogParseStatus, LogRecord,
    LoggedMutation, MutationKind,
};
pub use object::{ObjectAuthority, StorageObjectDescriptor};
pub use recovery::{RecoveredTransaction, RecoveryAssembler, RecoveryError};
pub use translation::{PublishResult, TranslationError, TranslationTable};
pub use txn::{Transaction, TransactionError, TransactionPhase};

// Keep the already-proven logical identity domains instead of manufacturing
// vNext-specific duplicates.
pub use crate::storage::format::{CommitPosition, CommitSeq, Lsn, TxnId};
