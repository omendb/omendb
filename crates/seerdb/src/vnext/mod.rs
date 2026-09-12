//! Replacement SeerDB transaction/storage-kernel architecture.
//!
//! This namespace is temporary migration scaffolding. The existing engine
//! remains the semantic/crash/performance oracle while vNext is qualified;
//! after cutover these modules become the normal SeerDB implementation and the
//! old generation-COW path is deleted.
//!
//! vNext deliberately starts below the ordered-KV abstraction. Transaction,
//! log, buffer, page-lifetime, and recovery services are shared by concrete
//! access methods. Ordered KV remains an important facade over the B-tree
//! access method, but it does not dictate how canonical rows or future search
//! and analytical structures must be laid out.

mod frame;
mod ids;
mod object;
mod translation;

pub use frame::{
    FrameMeta, FramePin, FrameState, FrameTransitionError, FrameVersion, FrameWriteLatch,
};
pub use ids::{FrameId, PageId, PageKey, StorageObjectId};
pub use object::{ObjectAuthority, StorageObjectDescriptor};
pub use translation::{DEFAULT_TRANSLATION_SHARDS, TranslationError, TranslationTable};

// Preserve the ordering domains established by the current engine rather than
// inventing vNext aliases that could accidentally diverge during migration.
pub use crate::storage::format::{CommitSeq, Lsn, TxnId};
