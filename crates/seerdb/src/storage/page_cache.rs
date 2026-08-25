//! Bounded cache for validated immutable page decodes.
//!
//! The buffer pool already caches raw page images. Keeping the parsed `Node`
//! separately avoids copying a 4 KiB frame and re-running page decoding and
//! checksum validation on every lazy lookup. PMT physical versions are part
//! of the key, so a newly published image cannot reuse a stale parsed node.

use crate::btree::Node;
use crate::buffer::PageCacheKey;
use std::collections::HashMap;
use std::sync::Arc;

/// Parsed immutable nodes paired with the raw buffer cache.
pub(super) struct ParsedPageCache {
    capacity: usize,
    pages: HashMap<PageCacheKey, Arc<Node>>,
}

impl ParsedPageCache {
    pub(super) fn new(buffer_frames: usize) -> Self {
        Self {
            capacity: buffer_frames.max(1),
            pages: HashMap::new(),
        }
    }

    pub(super) fn get(&self, key: PageCacheKey) -> Option<Arc<Node>> {
        self.pages.get(&key).cloned()
    }

    pub(super) fn insert(&mut self, key: PageCacheKey, node: Arc<Node>) {
        if !self.pages.contains_key(&key) && self.pages.len() >= self.capacity {
            // The raw buffer pool is the authoritative bounded cache. A
            // simple whole-cache reset keeps parsed memory bounded without
            // adding a second eviction policy to the read path.
            self.pages.clear();
        }
        self.pages.insert(key, node);
    }
}
