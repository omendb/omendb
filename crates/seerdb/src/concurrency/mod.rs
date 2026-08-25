//! Concurrency control: latches, lock coupling, and optimistic concurrency.
//!
//! This module implements the concurrency primitives needed for the
//! out-of-place B-tree, including hybrid latches and optimistic locking.

mod latch;

pub use latch::HybridLatch;
