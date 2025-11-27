//! Scan builder for flexible range queries
//!
//! Provides a builder pattern for constructing range scans over the database.
//!
//! # Example
//!
//! ```rust,no_run
//! use seerdb::{DB, DBOptions};
//!
//! let db = DB::open(DBOptions::default()).unwrap();
//!
//! // Simple range scan
//! for result in db.scan().range(b"a", b"z").iter().unwrap() {
//!     let (key, value) = result.unwrap();
//! }
//!
//! // Prefix scan, keys only, reversed
//! for result in db.scan().prefix(b"user:").keys_only().reverse().iter().unwrap() {
//!     let (key, _) = result.unwrap();
//! }
//! ```

use crate::db::{Result, DB};
use crate::range::{RangeItem, RangeIterator, RangeIteratorRev};
use std::error::Error;

/// Builder for constructing range scans
///
/// Created via [`DB::scan()`]. Configure the scan with method chaining,
/// then call [`iter()`](Self::iter) to execute.
pub struct Scan<'db> {
    db: &'db DB,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    prefix: Option<Vec<u8>>,
    keys_only: bool,
    reverse: bool,
}

impl<'db> Scan<'db> {
    pub(crate) fn new(db: &'db DB) -> Self {
        Self {
            db,
            start: None,
            end: None,
            prefix: None,
            keys_only: false,
            reverse: false,
        }
    }

    /// Set the key range [start, end)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// db.scan().range(b"key00", b"key99").iter()?;
    /// ```
    pub fn range(mut self, start: &[u8], end: &[u8]) -> Self {
        self.start = Some(start.to_vec());
        self.end = Some(end.to_vec());
        self.prefix = None;
        self
    }

    /// Set start key with open end (all keys >= start)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// db.scan().from(b"key50").iter()?; // key50, key51, ...
    /// ```
    pub fn from(mut self, start: &[u8]) -> Self {
        self.start = Some(start.to_vec());
        self.end = None;
        self.prefix = None;
        self
    }

    /// Scan all keys with a given prefix
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// db.scan().prefix(b"user:").iter()?; // user:1, user:2, ...
    /// ```
    pub fn prefix(mut self, prefix: &[u8]) -> Self {
        self.prefix = Some(prefix.to_vec());
        self.start = None;
        self.end = None;
        self
    }

    /// Only return keys, skip reading values
    ///
    /// More efficient when you only need keys, especially with large values.
    pub fn keys_only(mut self) -> Self {
        self.keys_only = true;
        self
    }

    /// Iterate in reverse order (largest to smallest)
    pub fn reverse(mut self) -> Self {
        self.reverse = true;
        self
    }

    /// Execute the scan and return an iterator
    ///
    /// Returns a forward or reverse iterator based on configuration.
    pub fn iter(self) -> Result<ScanIterator> {
        if self.reverse {
            let (start, end) = self.resolve_bounds();
            let iter = if self.keys_only {
                self.db.range_keys_only_rev(&start, end.as_deref())?
            } else {
                self.db.range_rev(&start, end.as_deref())?
            };
            Ok(ScanIterator::Reverse(iter))
        } else {
            let (start, end) = self.resolve_bounds();
            let iter = if self.keys_only {
                self.db.range_keys_only(&start, end.as_deref())?
            } else {
                self.db.range(&start, end.as_deref())?
            };
            Ok(ScanIterator::Forward(iter))
        }
    }

    fn resolve_bounds(&self) -> (Vec<u8>, Option<Vec<u8>>) {
        if let Some(ref prefix) = self.prefix {
            // For prefix scans, compute end as prefix + 1
            let mut end = prefix.clone();
            // Increment last byte, or extend if all 0xFF
            if let Some(last) = end.last_mut() {
                if *last < 0xFF {
                    *last += 1;
                } else {
                    end.push(0x00);
                }
            }
            (prefix.clone(), Some(end))
        } else {
            let start = self.start.clone().unwrap_or_default();
            let end = self.end.clone();
            (start, end)
        }
    }
}

/// Iterator returned by [`Scan::iter()`]
///
/// Abstracts over forward and reverse iterators.
pub enum ScanIterator {
    Forward(RangeIterator),
    Reverse(RangeIteratorRev),
}

impl Iterator for ScanIterator {
    type Item = std::result::Result<RangeItem, Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ScanIterator::Forward(iter) => iter.next(),
            ScanIterator::Reverse(iter) => iter.next(),
        }
    }
}
