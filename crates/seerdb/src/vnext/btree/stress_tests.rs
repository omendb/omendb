use super::{BTreeLookup, BTreeObject};
use crate::btree::{BTree, BTreeError as LegacyBTreeError, LookupResult};
use crate::vnext::{
    BufferPool, ObjectAuthority, PageIo, PageKey, StorageObjectDescriptor, StorageObjectId,
};
use std::collections::{BTreeMap, HashMap};
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

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn index(&mut self, upper: usize) -> usize {
        (self.next() as usize) % upper
    }
}

fn logical_legacy(result: LookupResult) -> Option<Vec<u8>> {
    match result {
        LookupResult::Found(value) => Some(value),
        LookupResult::Blob(_) | LookupResult::Deleted | LookupResult::NotFound => None,
    }
}

fn logical_vnext(result: BTreeLookup) -> Option<Vec<u8>> {
    match result {
        BTreeLookup::Found(value) => Some(value),
        BTreeLookup::Blob(_) | BTreeLookup::Deleted | BTreeLookup::NotFound => None,
    }
}

#[test]
fn randomized_insert_delete_lookup_matches_legacy_across_page_sizes() {
    for (case, page_size) in [384usize, 512, 768, 1024, 2048]
        .into_iter()
        .enumerate()
    {
        let device = Arc::new(MemoryPageIo::default());
        let buffer = BufferPool::new(96, page_size, device).expect("buffer creates");
        let tree = BTreeObject::create(descriptor(100 + case as u64), &buffer)
            .expect("vNext tree creates");
        let mut legacy = BTree::new();
        let mut rng = Lcg::new(0x9e37_79b9_7f4a_7c15 ^ page_size as u64);
        let mut inserted = vec![false; 320];
        let mut deleted = vec![false; 320];

        for step in 0..1_800usize {
            let index = rng.index(inserted.len());
            let key = format!("key-{index:04}-{:08x}", index.wrapping_mul(2_654_435_761));

            match rng.index(5) {
                0 | 1 if !inserted[index] => {
                    let value_len = 1 + rng.index(80);
                    let value = vec![(index as u8).wrapping_add(step as u8); value_len];
                    tree.insert(&buffer, key.as_bytes(), &value)
                        .expect("vNext insert succeeds");
                    legacy
                        .insert(key.as_bytes(), &value)
                        .expect("legacy insert succeeds");
                    inserted[index] = true;
                }
                2 if inserted[index] && !deleted[index] => {
                    assert!(tree.delete(&buffer, key.as_bytes()).expect("vNext delete"));
                    assert!(legacy.delete(key.as_bytes()).expect("legacy delete"));
                    deleted[index] = true;
                }
                _ => {
                    let vnext = logical_vnext(
                        tree.lookup(&buffer, key.as_bytes())
                            .expect("vNext lookup succeeds"),
                    );
                    let old = logical_legacy(
                        legacy
                            .lookup(key.as_bytes())
                            .expect("legacy lookup succeeds"),
                    );
                    assert_eq!(vnext, old, "lookup differs at page_size={page_size} step={step}");
                }
            }

            if step % 97 == 0 {
                for probe in 0..inserted.len() {
                    let probe_key = format!(
                        "key-{probe:04}-{:08x}",
                        probe.wrapping_mul(2_654_435_761)
                    );
                    let vnext = logical_vnext(
                        tree.lookup(&buffer, probe_key.as_bytes())
                            .expect("vNext probe succeeds"),
                    );
                    let old = logical_legacy(
                        legacy
                            .lookup(probe_key.as_bytes())
                            .expect("legacy probe succeeds"),
                    );
                    assert_eq!(
                        vnext, old,
                        "probe differs at page_size={page_size} step={step} key={probe}"
                    );
                }
            }
        }
    }
}

#[test]
fn randomized_ranges_match_ordered_model_under_eviction() {
    let device = Arc::new(MemoryPageIo::default());
    let buffer = BufferPool::new(6, 512, device).expect("buffer creates");
    let tree = BTreeObject::create(descriptor(131), &buffer).expect("tree creates");
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut rng = Lcg::new(0xd1b5_4a32_d192_ed03);

    for step in 0..360usize {
        let key_index = rng.index(1_000);
        let key = format!("r-{key_index:04}").into_bytes();
        if model.contains_key(&key) {
            continue;
        }
        let value = format!("v-{step:04}-{}", rng.next()).into_bytes();
        tree.insert(&buffer, &key, &value).expect("insert succeeds");
        model.insert(key, value);
    }

    for _ in 0..120 {
        let a = rng.index(1_000);
        let b = rng.index(1_000);
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        let start = format!("r-{lo:04}").into_bytes();
        let end = format!("r-{hi:04}").into_bytes();
        let limit = 1 + rng.index(31);
        let expected: Vec<_> = model
            .range(start.clone()..end.clone())
            .take(limit)
            .map(|(key, value)| (key.clone(), BTreeLookup::Found(value.clone())))
            .collect();
        let actual = tree
            .range(&buffer, &start, &end, limit)
            .expect("range succeeds");
        assert_eq!(actual, expected);
    }

    assert!(buffer.stats().expect("stats").evictions > 0);
}

#[test]
fn readers_remain_correct_while_disjoint_writers_force_splits_and_eviction() {
    let device = Arc::new(MemoryPageIo::default());
    let buffer = Arc::new(BufferPool::new(12, 512, device).expect("buffer creates"));
    let tree = Arc::new(BTreeObject::create(descriptor(137), &buffer).expect("tree creates"));

    for number in 0..80u32 {
        let key = format!("base-{number:04}");
        tree.insert(&buffer, key.as_bytes(), key.as_bytes())
            .expect("base insert");
    }

    let mut workers = Vec::new();
    for writer in 0..4u32 {
        let tree = Arc::clone(&tree);
        let buffer = Arc::clone(&buffer);
        workers.push(std::thread::spawn(move || {
            for number in 0..180u32 {
                let key = format!("w{writer}-{number:04}");
                tree.insert(&buffer, key.as_bytes(), key.as_bytes())
                    .expect("writer insert");
            }
        }));
    }
    for reader in 0..4u32 {
        let tree = Arc::clone(&tree);
        let buffer = Arc::clone(&buffer);
        workers.push(std::thread::spawn(move || {
            for round in 0..500u32 {
                let number = (round.wrapping_mul(17).wrapping_add(reader * 11)) % 80;
                let key = format!("base-{number:04}");
                assert_eq!(
                    tree.lookup(&buffer, key.as_bytes())
                        .expect("concurrent lookup"),
                    BTreeLookup::Found(key.as_bytes().to_vec())
                );
                if round % 23 == 0 {
                    let rows = tree
                        .range(&buffer, b"base-0010", b"base-0020", 32)
                        .expect("concurrent range");
                    assert_eq!(rows.len(), 10);
                    assert_eq!(rows.first().expect("first").0, b"base-0010");
                    assert_eq!(rows.last().expect("last").0, b"base-0019");
                }
            }
        }));
    }
    for worker in workers {
        worker.join().expect("worker completes");
    }

    for writer in 0..4u32 {
        for number in 0..180u32 {
            let key = format!("w{writer}-{number:04}");
            assert_eq!(
                tree.lookup(&buffer, key.as_bytes()).expect("final lookup"),
                BTreeLookup::Found(key.as_bytes().to_vec())
            );
        }
    }
    assert!(buffer.stats().expect("stats").evictions > 0);
}

#[test]
fn duplicate_inserts_fail_without_changing_existing_value() {
    let device = Arc::new(MemoryPageIo::default());
    let buffer = BufferPool::new(8, 512, device).expect("buffer creates");
    let tree = BTreeObject::create(descriptor(149), &buffer).expect("tree creates");
    tree.insert(&buffer, b"same", b"first").expect("first insert");
    assert!(matches!(
        tree.insert(&buffer, b"same", b"second"),
        Err(super::BTreeError::DuplicateKey)
    ));
    assert_eq!(
        tree.lookup(&buffer, b"same").expect("lookup"),
        BTreeLookup::Found(b"first".to_vec())
    );
}

#[allow(dead_code)]
fn _assert_legacy_error_is_still_linked(_: LegacyBTreeError) {}
