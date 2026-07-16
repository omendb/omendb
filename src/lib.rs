//! seerdb — High-performance out-of-place B-tree storage engine for NVMe SSDs
//!
//! A storage engine designed from scratch for modern hardware, combining:
//! - **Out-of-place writes** (LeanStore-inspired): pages are never updated in place
//! - **KV separation** (WiscKey-inspired): large values stored separately
//! - **SSD-native design**: FDP/ZNS support for minimal write amplification
//! - **MVCC**: copy-on-write concurrency control
//!
//! # Architecture
//!
//! seerdb uses an out-of-place B-tree where writes create new page versions
//! instead of modifying pages in place. A mapping table tracks page locations,
//! and garbage collection reclaims invalidated pages. Large values are stored
//! separately in an append-only blob log.
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
//! - LeanStore (VLDB 2024, 2026): out-of-place B-tree, SSD-aware buffer management
//! - "How to Write to SSDs" (VLDB 2026): DB-SSD co-optimization, NoWA pattern
//! - WiscKey (FAST 2016): key-value separation for reduced write amplification
//! - Tidehunter (2026): WAL-as-store architecture (reference for I/O patterns)

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

// Re-export main types at crate root.
pub use db::{
    CompactionReport, DB, DBMetrics, DurabilityStatus, Options, Snapshot, SnapshotReport,
    VerificationReport,
};
pub use error::{Error, Result};
pub use storage::StorageMetrics;
