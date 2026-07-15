//! Crash recovery via Write-Ahead Logging (WAL).
//!
//! The WAL is durable before page generations are published. On crash
//! recovery, only mutation prefixes closed by a valid commit envelope are
//! replayed.

mod wal;

pub use wal::{ParseStatus, RecordType, SyncPolicy, WalManager, WalRecord};
