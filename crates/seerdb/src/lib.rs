//! seerdb — reusable durable transaction and storage engine for Rust
//!
//! SeerDB can serve independent Rust consumers through an ordered transactional
//! KV library and, as the kernel evolves, reusable storage object, buffer,
//! page/extent, MVCC, logging and recovery primitives. OmenDB is its primary
//! integration and performance consumer, **not** a dependency. OmenDB owns
//! relational row codecs, SQL catalog/constraint semantics and query execution.
//! An ordered byte-KV API is useful, but it must not force all storage objects
//! into one opaque byte-key/value layout.
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
//!        OmenDB      standalone KV      other Rust consumers
//!           \              |              /
//!              SeerDB transaction/storage runtime
//!                           |
//!                 buffer / log / MVCC
//!                           |
//!               ordered trees / page / extents
//! ```
//!
//! The first replacement slice is a buffered B-tree over guarded frames,
//! followed by log-authoritative transactions and generic record/extent
//! facilities. OmenDB's SQL-row layouts and index meanings remain in OmenDB.
//! Physical page materialization becomes asynchronous relative to a durable
//! transaction decision. Integrated consumers may share the same runtime
//! without imposing a universal byte-KV representation.
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
