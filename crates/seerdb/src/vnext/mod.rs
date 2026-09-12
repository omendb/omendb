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
mod log_io;
mod object;
mod recovery;
mod segment_log;
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
    CommitDecision, LogEncodeError, LogParseStatus, LogRecord, LoggedMutation, MutationKind,
    ParsedLogRecord, mutation_digest, parse_log_prefix, parse_log_prefix_frames,
};
pub use log_io::{
    AppendTicket, DurableLog, DurableLogError, LogDevice, LogIoOperation, PrepareLogBatchError,
    PreparedLogBatch,
};
pub use object::{ObjectAuthority, StorageObjectDescriptor};
pub use recovery::{RecoveredTransaction, RecoveryAssembler, RecoveryError};
pub use segment_log::{SegmentedFileLogDevice, SegmentedLogConfig};
pub use translation::{PublishResult, TranslationError, TranslationTable};
pub use txn::{Transaction, TransactionError, TransactionPhase};

// Keep the already-proven logical identity domains instead of manufacturing
// vNext-specific duplicates.
pub use crate::storage::format::{CommitPosition, CommitSeq, Lsn, TxnId};
