//! Multi-version concurrency control.
//!
//! This module separates logical record-version visibility from the Page
//! Mapping Table (PMT), which tracks physical page locations in the
//! out-of-place B-tree. Current records now have an append-oriented logical
//! before-image store; transaction-status indirection and retention-aware GC
//! remain the next integration steps.

mod pmt;
mod version_store;

pub use pmt::{PMT, PageMapping};
pub(crate) use version_store::{
    CurrentRecord, VersionStore, decode_current, encode_current, resolve_commit, visible_current,
};
