//! Failpoint support for deterministic crash testing.
//!
//! Failpoints allow injecting failures at specific points in the code
//! to test recovery and error handling paths.
//!
//! # Usage
//!
//! In production code, add failpoints at critical locations:
//!
//! ```ignore
//! use crate::fail_point;
//!
//! // In flush.rs after SSTable write
//! fail_point!("flush::after_sstable_write");
//!
//! // In wal.rs after WAL sync
//! fail_point!("wal::after_sync");
//! ```
//!
//! In tests, inject failures:
//!
//! ```ignore
//! fail::cfg("flush::after_sstable_write", "panic").unwrap();
//! // ... test recovery ...
//! fail::remove("flush::after_sstable_write");
//! ```
//!
//! # Available Failpoints
//!
//! | Failpoint | Location | Tests |
//! |-----------|----------|-------|
//! | `flush::after_sstable_write` | After SSTable fsync, before WAL clear | WAL data preserved on crash |
//! | `flush::before_wal_clear` | Just before WAL is cleared | Recovery replays WAL |
//! | `compaction::after_output_write` | After new SSTable, before input deletion | Inputs preserved |
//! | `wal::after_sync` | After WAL sync completes | Data durability |
//!
//! # Feature Flag
//!
//! Failpoints are only compiled when the `failpoints` feature is enabled.
//! In release builds without this feature, all failpoint macros compile to nothing.

/// Failpoint macro - compiles to nothing without the feature
#[cfg(not(feature = "failpoints"))]
#[macro_export]
macro_rules! fail_point {
    ($name:expr) => {};
    ($name:expr, $cond:expr, $e:expr) => {};
}

/// Failpoint macro - delegates to fail crate when enabled
#[cfg(feature = "failpoints")]
#[macro_export]
macro_rules! fail_point {
    ($name:expr) => {
        fail::fail_point!($name);
    };
    ($name:expr, $cond:expr, $e:expr) => {
        fail::fail_point!($name, $cond, $e);
    };
}
