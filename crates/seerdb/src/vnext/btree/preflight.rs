//! Pure admission checks for native ordered-access-method records.
//!
//! Commit validation needs to reject a logical record that can never fit the
//! configured page size before a durable decision is appended. This module
//! exercises the real v4 leaf builder without touching tree state.

use super::page_v4::{self, LeafEntryOwned, LeafValueOwned};
use super::tree_v4::{BTreeError, BTreeObject};
use super::super::BufferPool;

impl BTreeObject {
    /// Validate that one inline key/value can fit by itself in a leaf for this
    /// buffer pool's configured page size. No page, allocation ID, or tree state
    /// is mutated.
    pub fn preflight_inline_upsert(
        &self,
        buffer: &BufferPool,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), BTreeError> {
        if key.len() > u16::MAX as usize || value.len() > u16::MAX as usize {
            return Err(BTreeError::EntryTooLarge);
        }
        let entry = LeafEntryOwned {
            key: key.to_vec(),
            value: LeafValueOwned::Inline(value.to_vec()),
        };
        page_v4::build_leaf(buffer.page_size(), None, None, &[entry])
            .map(|_| ())
            .map_err(|_| BTreeError::EntryTooLarge)
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

    #[test]
    fn preflight_rejects_unrepresentable_record_without_mutating_tree() {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(4, 384, device).expect("buffer");
        let descriptor =
            StorageObjectDescriptor::new(StorageObjectId::new(1), ObjectAuthority::Authoritative);
        let tree = BTreeObject::create(descriptor, &buffer).expect("tree");

        tree.preflight_inline_upsert(&buffer, b"key", b"small")
            .expect("small value fits");
        assert!(matches!(
            tree.preflight_inline_upsert(&buffer, b"key", &vec![0; 512]),
            Err(BTreeError::EntryTooLarge)
        ));
        assert_eq!(
            tree.lookup(&buffer, b"key").expect("lookup"),
            super::super::BTreeLookup::NotFound
        );
    }
}
