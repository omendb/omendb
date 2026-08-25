//! Multi-version concurrency control.
//!
//! This module separates logical record-version visibility from the Page
//! Mapping Table (PMT), which tracks physical page locations in the
//! out-of-place B-tree. The logical record chain is the first physical-MVCC
//! slice; transaction-status indirection and an append-oriented undo store
//! remain later work.

mod pmt;
mod record;

pub use pmt::{PMT, PageMapping};
pub(crate) use record::{
    ValueVersion, decode as decode_record, encode as encode_record, latest as latest_record,
    visible as visible_record,
};
