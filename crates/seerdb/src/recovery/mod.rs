//! Crash recovery via Write-Ahead Logging (WAL).
//!
//! The WAL is durable before page generations are published. On crash
//! recovery, only mutation prefixes closed by a valid commit envelope are
//! replayed.

mod record;
mod wal;

pub use record::{ParseStatus, RecordType, WalRecord};
pub use wal::{SyncPolicy, WalManager};
