//! Buffer pool management.
//!
//! The buffer manager pools fixed-size page frames in memory, loading pages
//! on demand and evicting them when the pool is full. Pages are protected
//! by RAII-based guards that control access.

mod frame;
mod guard;
mod manager;

pub use frame::Frame;
pub use guard::{GuardAccess, PageGuard};
pub use manager::{BufferError, BufferManager, BufferStats, PageCacheKey};
