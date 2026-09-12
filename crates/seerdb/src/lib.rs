//! seerdb — OmenDB's transaction and storage kernel for modern hardware
//!
//! SeerDB is being redesigned as the shared kernel beneath OmenDB's physical
//! access methods. It owns transaction ordering, durability, recovery, buffer
//! residency, page/object lifetime, and physical storage services. An ordered
//! transactional KV API remains an important standalone/compatibility facade,
//! but `TreeId + key bytes + opaque value bytes` is no longer the universal
//! internal boundary for canonical rows, search indexes, graph/analytical
//! representations, or future access methods.
//!
//! The current implementation remains available while the replacement is
//! qualified. It combines:
//! - **Out-of-place writes** (LeanStore-inspired): durable page images are not
//!   overwritten in place
//! - **KV separation** (WiscKey-inspired): large values can be stored separately
//! - **SSD-aware layout**: append-oriented placement leaves room for FDP/ZNS
//!   integration and lower device write amplification
//! - **Fixed alpha page format**: the current implementation uses [`PAGE_SIZE`]
//!   pages; page/node sizing remains benchmark-gated before format stability
//! - **Logical MVCC**: fixed snapshots resolve current records through
//!   transaction status and append-oriented before-images, independently of
//!   physical page incarnations
//! - **Explicit durability identities**: logical commit order (CSN) and durable
//!   log position (LSN) remain distinct
//!
//! # vNext architecture
//!
//! The replacement architecture lives temporarily under [`vnext`] while the
//! existing engine serves as a semantic, crash/fault, and performance oracle.
//! vNext moves the narrow waist below ordered KV:
//!
//! ```text
//!                         OmenDB
//!                           |
//!                transaction/storage kernel
//!             /        |        |          \
//!        ordered     canonical  search/    analytical
//!        B-tree       rows       graph     representations
//!             \        |        |          /
//!              +-------+--------+---------+
//!                           |
//!                 buffer / log / MVCC
//! ```
//!
//! The first replacement slice is a shared buffered B-tree over guarded frames,
//! followed by log-authoritative transactions and compact canonical row storage.
//! Physical page materialization becomes asynchronous relative to a durable
//! transaction decision. Specialized access methods then share the same
//! transaction/log/buffer substrate instead of inventing sibling databases.
//!
//! The current group-publication lane and generation-COW storage remain a
//! qualified baseline only. They are deleted after vNext passes the existing
//! recovery/semantic gates and intended-workload performance gates.
//!
//! # Deployment policy
//!
//! One transaction/storage semantics may use different measured physical
//! strategies: autonomous/adaptive local-NVMe logging, quorum logging for HA,
//! async page materialization, and object-storage checkpoints/archive. SeerDB
//! is intentionally not an interchangeable RocksDB/backend wrapper.
//!
//! # Example
//!
//! ```no_run
//! use seerdb::{DB, Options};
//!
//! let mut db = DB::open("./my_db", Options::default()).unwrap();
//! db.put(b"key", b"value").unwrap();
//! let val = db.get(b"key").unwrap();
//! db.delete(b"key").unwrap();
//! db.close().unwrap();
//! ```
//!
//! # References
//!
//! - LeanStore / Umbra / CedarDB: integrated larger-than-memory transaction,
//!   buffer, and physical-layout design
//! - *B-Trees Are Back* (SIGMOD 2025): modern pageable B-tree node layouts
//! - *Moving on From Group Commit* (SIGMOD 2025): autonomous commit on NVMe
//! - *Predictive Translation* (SIGMOD 2026): low-overhead buffer translation
//! - *How to Write to SSDs* (VLDB 2026): DB/SSD out-of-place co-optimization
//! - BtrLog (VLDB 2026): quorum SSD logging plus object-store archival
//! - FoundationDB: ordered transactional KV as a powerful external layering
//!   interface rather than a requirement for every internal physical structure
//! - WiscKey (FAST 2016): key/value separation

#![cfg_attr(test, allow(clippy::disallowed_methods))]

#[cfg(test)]
mod transaction_model;

pub mod allocator;
pub mod blob;
pub mod btree;
pub mod buffer;
pub mod concurrency;
pub mod db;
pub mod error;
pub mod mvcc;
pub mod recovery;
pub mod space;
pub mod storage;
pub mod transactional;
pub mod vnext;

// Re-export main types at crate root.
pub use btree::PAGE_SIZE;
pub use db::{
    BatchMutation, BatchTransaction, BatchTransactionState, BlobStorageMode, CheckReport,
    CompactionReport, DB, DBMetrics, DurabilityStatus, HistoryPruneReport, Options,
    PublicationMetrics, PublicationTimingMetrics, ReadView, RepairAction, RepairReport,
    RestoreReport, RetainedSnapshot, Snapshot, SnapshotReport, VacuumProgress, VacuumReport,
    VerificationReport, WalCheckStatus,
};
pub use error::{CheckFailureKind, Error, Result};
pub use storage::StorageMetrics;
pub use storage::format::{
    CommitId, CommitPosition, CommitSeq, GenerationId, HistoryId, Lsn, PageVersion, SnapshotId,
    TreeId, TxnId,
};
pub use transactional::{
    ChangeGcReport, CommittedChange, Cursor, RetentionLease, SnapshotExport, Transaction,
    TransactionDatabase, TransactionState, VersionGcReport,
};
