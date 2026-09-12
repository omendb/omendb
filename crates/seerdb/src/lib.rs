//! seerdb — High-performance ordered transactional storage for modern hardware
//!
//! SeerDB is OmenDB's ordered transactional KV engine. Its local implementation
//! is currently an out-of-place B-tree engine for RAM + NVMe, while transaction,
//! snapshot, CSN/LSN, and recovery semantics are deliberately independent of one
//! durability transport or physical page policy.
//!
//! The current local engine combines:
//! - **Out-of-place writes** (LeanStore-inspired): pages are never updated in place
//! - **KV separation** (WiscKey-inspired): large values are stored separately
//! - **SSD-aware layout**: append-oriented placement leaves room for FDP/ZNS
//!   integration and lower device write amplification
//! - **Fixed alpha page format**: the current implementation uses [`PAGE_SIZE`]
//!   pages; page/node sizing remains benchmark-gated before format stability
//! - **Logical MVCC**: fixed snapshots resolve current records through transaction
//!   status and append-oriented before-images, independently of physical page
//!   incarnations
//! - **Explicit durability identities**: logical commit order (CSN) and durable
//!   log position (LSN) remain distinct
//!
//! # Architecture
//!
//! The local physical store currently uses an out-of-place B-tree where writes
//! create new page versions instead of overwriting pages in place. A mapping
//! layer tracks durable page locations, garbage collection reclaims superseded
//! storage, and large values can live in append-oriented blob segments.
//!
//! [`TransactionDatabase`] provides multi-transaction logical semantics above
//! that store and returns explicit `{CSN, LSN}` commit positions. The qualified
//! commit implementation currently uses the group-publication lane from OmenDB
//! ADR 0004. That lane is a baseline, not a permanent device policy: ADR 0006
//! explicitly permits autonomous/parallel local-NVMe commit, quorum-replicated
//! logging for HA, asynchronous page materialization, and object-storage-backed
//! checkpoints/archive when measurements and fault qualification justify them.
//!
//! SeerDB is intentionally not an interchangeable RocksDB/LSM/backend wrapper.
//! Deployment profiles may change durability, caching, and I/O mechanisms while
//! retaining one transaction/storage contract.
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
//! - LeanStore / Umbra: memory-efficient larger-than-memory B-tree engines
//! - *B-Trees Are Back* (SIGMOD 2025): modern pageable B-tree node layouts
//! - *Moving on From Group Commit* (SIGMOD 2025): autonomous commit on NVMe
//! - *Predictive Translation* (SIGMOD 2026): low-overhead buffer translation
//! - *How to Write to SSDs* (VLDB 2026): DB/SSD out-of-place co-optimization
//! - BtrLog (VLDB 2026): quorum SSD logging plus object-store archival
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
