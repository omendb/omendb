//! Key-restart range cursor for the native vNext B-link tree.
//!
//! Cursor state never stores a frame, slot index, or physical page location
//! across calls. A batch resumes from the last emitted logical key and descends
//! from the current root again, so frame eviction, page movement, root splits
//! and leaf splits cannot make cursor state point at stale storage. Transaction
//! snapshot semantics will later be supplied by the transaction/MVCC layer.

use super::super::BufferPool;
use super::tree_v4::{BTreeError, BTreeLookup, BTreeObject};

/// Resumable forward range cursor over `[start, end)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeCursor {
    start: Vec<u8>,
    end: Vec<u8>,
    last_emitted: Option<Vec<u8>>,
    done: bool,
}

impl RangeCursor {
    /// Construct a cursor over `[start, end)`.
    #[must_use]
    pub fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        let done = start >= end;
        Self {
            start,
            end,
            last_emitted: None,
            done,
        }
    }

    /// Whether the cursor has reached its upper bound or the end of the tree.
    #[must_use]
    pub const fn is_done(&self) -> bool {
        self.done
    }

    /// Last logical key returned by this cursor, if any.
    #[must_use]
    pub fn last_emitted(&self) -> Option<&[u8]> {
        self.last_emitted.as_deref()
    }

    /// Return the next bounded batch.
    ///
    /// Resumption is exclusive of the last emitted key. The implementation
    /// intentionally re-descends by key instead of retaining page/slot state;
    /// this is more robust under splits and eviction and gives the later MVCC
    /// layer a clean place to attach snapshot/restart semantics.
    pub fn next_batch(
        &mut self,
        tree: &BTreeObject,
        buffer: &BufferPool,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, BTreeLookup)>, BTreeError> {
        if self.done || limit == 0 {
            return Ok(Vec::new());
        }

        let resume_after = self.last_emitted.as_deref();
        let seek = resume_after.unwrap_or(self.start.as_slice());
        // Fetch one extra row after the first batch so filtering an inclusive
        // restart key cannot shrink a full batch or falsely signal completion.
        let fetch_limit = limit.saturating_add(usize::from(resume_after.is_some()));
        let mut rows = tree.range(buffer, seek, &self.end, fetch_limit)?;
        let raw_len = rows.len();

        if let Some(last) = resume_after {
            rows.retain(|(key, _)| key.as_slice() > last);
        }
        if rows.len() > limit {
            rows.truncate(limit);
        }

        if let Some((key, _)) = rows.last() {
            self.last_emitted = Some(key.clone());
        }
        if rows.is_empty() || raw_len < fetch_limit {
            self.done = true;
        }

        Ok(rows)
    }
}

impl BTreeObject {
    /// Create a resumable forward cursor over `[start, end)`.
    #[must_use]
    pub fn range_cursor(&self, start: &[u8], end: &[u8]) -> RangeCursor {
        RangeCursor::new(start.to_vec(), end.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{
        ObjectAuthority, PageIo, PageKey, StorageObjectDescriptor, StorageObjectId,
    };
    use std::collections::HashMap;
    use std::io;
    use std::sync::{Arc, RwLock};

    #[derive(Default)]
    struct MemoryPageIo {
        pages: RwLock<HashMap<PageKey, Vec<u8>>>,
    }

    impl PageIo for MemoryPageIo {
        fn read_page(&self, key: PageKey, destination: &mut [u8]) -> io::Result<()> {
            let pages = self
                .pages
                .read()
                .map_err(|_| io::Error::other("page map poisoned"))?;
            let page = pages
                .get(&key)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing page"))?;
            if page.len() != destination.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "page size mismatch",
                ));
            }
            destination.copy_from_slice(page);
            Ok(())
        }

        fn write_page(&self, key: PageKey, source: &[u8]) -> io::Result<()> {
            self.pages
                .write()
                .map_err(|_| io::Error::other("page map poisoned"))?
                .insert(key, source.to_vec());
            Ok(())
        }
    }

    fn descriptor(id: u64) -> StorageObjectDescriptor {
        StorageObjectDescriptor::new(StorageObjectId::new(id), ObjectAuthority::Authoritative)
    }

    #[test]
    fn cursor_batches_do_not_duplicate_restart_key() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(64, 768, device).expect("buffer creates");
        let tree = BTreeObject::create(descriptor(61), &buffer).expect("tree creates");
        for number in 0..200u32 {
            let key = format!("k-{number:04}");
            tree.insert(&buffer, key.as_bytes(), key.as_bytes())
                .expect("insert");
        }

        let mut cursor = tree.range_cursor(b"k-0050", b"k-0075");
        let mut keys = Vec::new();
        while !cursor.is_done() {
            let batch = cursor.next_batch(&tree, &buffer, 7).expect("batch");
            keys.extend(batch.into_iter().map(|(key, _)| key));
        }

        assert_eq!(keys.len(), 25);
        assert_eq!(keys.first().expect("first"), b"k-0050");
        assert_eq!(keys.last().expect("last"), b"k-0074");
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn cursor_restarts_by_key_after_eviction() {
        let device = Arc::new(MemoryPageIo::default());
        // Four frames are intentionally much smaller than the resulting tree.
        let buffer = BufferPool::new(4, 512, device).expect("buffer creates");
        let tree = BTreeObject::create(descriptor(67), &buffer).expect("tree creates");
        for number in 0..240u32 {
            let key = format!("k-{number:04}");
            tree.insert(&buffer, key.as_bytes(), key.as_bytes())
                .expect("insert");
        }

        let mut cursor = tree.range_cursor(b"k-0100", b"k-0160");
        let mut keys = Vec::new();
        while !cursor.is_done() {
            let batch = cursor.next_batch(&tree, &buffer, 5).expect("batch");
            keys.extend(batch.into_iter().map(|(key, _)| key));
        }

        assert_eq!(keys.len(), 60);
        assert_eq!(keys.first().expect("first"), b"k-0100");
        assert_eq!(keys.last().expect("last"), b"k-0159");
        assert!(buffer.stats().expect("stats").evictions > 0);
    }

    #[test]
    fn deleting_restart_key_does_not_skip_successor() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(16, 768, device).expect("buffer creates");
        let tree = BTreeObject::create(descriptor(71), &buffer).expect("tree creates");
        for number in 0..40u32 {
            let key = format!("k-{number:04}");
            tree.insert(&buffer, key.as_bytes(), key.as_bytes())
                .expect("insert");
        }

        let mut cursor = tree.range_cursor(b"k-0010", b"k-0030");
        let first = cursor.next_batch(&tree, &buffer, 5).expect("first batch");
        assert_eq!(first.last().expect("last first batch").0, b"k-0014");
        assert!(tree.delete(&buffer, b"k-0014").expect("delete restart key"));

        let second = cursor.next_batch(&tree, &buffer, 5).expect("second batch");
        assert_eq!(second.first().expect("first second batch").0, b"k-0015");
    }
}
