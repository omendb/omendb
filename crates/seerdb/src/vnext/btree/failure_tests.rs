use super::{BTreeError, BTreeLookup, BTreeObject};
use crate::vnext::{
    BufferError, BufferPool, ObjectAuthority, PageIo, PageIoOperation, PageKey,
    StorageObjectDescriptor, StorageObjectId,
};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Default)]
struct FailWritesPageIo {
    pages: RwLock<HashMap<PageKey, Vec<u8>>>,
    failures_remaining: AtomicUsize,
}

impl FailWritesPageIo {
    fn fail_next_write(&self) {
        self.failures_remaining.store(1, Ordering::Release);
    }

    fn allow_writes(&self) {
        self.failures_remaining.store(0, Ordering::Release);
    }
}

impl PageIo for FailWritesPageIo {
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
        if self
            .failures_remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(io::Error::other("injected write failure"));
        }
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
fn failed_root_promotion_cannot_make_older_left_siblings_unreachable() {
    let device = Arc::new(FailWritesPageIo::default());
    // Two frames are enough to materialize a leaf split but force the new-root
    // installation to evict/write one dirty frame. Failing that write creates
    // the precise "split published, parent/root promotion failed" condition.
    let buffer = BufferPool::new(2, 256, device.clone()).expect("buffer creates");
    let tree = BTreeObject::create(descriptor(173), &buffer).expect("tree creates");
    device.fail_next_write();

    let mut failed_at = None;
    for number in 0..200u32 {
        let key = format!("k-{number:04}");
        match tree.insert(&buffer, key.as_bytes(), key.as_bytes()) {
            Ok(()) => {}
            Err(BTreeError::Buffer(BufferError::Io {
                operation: PageIoOperation::Write,
                ..
            })) => {
                failed_at = Some(number);
                break;
            }
            Err(error) => panic!("unexpected insert failure: {error:?}"),
        }
    }
    let failed_at = failed_at.expect("fault must hit root-promotion writeback");

    // The insert that reported the physical failure may already be reachable
    // through the B-link split. Record the actual logical state before retrying
    // instead of assuming transaction atomicity that vNext does not have yet.
    let mut reachable = Vec::new();
    for number in 0..=failed_at {
        let key = format!("k-{number:04}");
        if matches!(
            tree.lookup(&buffer, key.as_bytes()).expect("lookup after fault"),
            BTreeLookup::Found(_)
        ) {
            reachable.push(key);
        }
    }
    assert!(!reachable.is_empty());

    device.allow_writes();
    for number in (failed_at + 1)..(failed_at + 180) {
        let key = format!("k-{number:04}");
        tree.insert(&buffer, key.as_bytes(), key.as_bytes())
            .expect("post-fault inserts must progress");
    }
    assert_ne!(tree.root().get(), 0, "a later split should promote a root");

    for key in reachable {
        assert!(
            matches!(
                tree.lookup(&buffer, key.as_bytes())
                    .expect("pre-fault key remains reachable"),
                BTreeLookup::Found(_)
            ),
            "key {key} became unreachable after delayed root promotion"
        );
    }
}
