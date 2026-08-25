#![no_main]

use libfuzzer_sys::fuzz_target;
use seerdb::allocator::PageAllocator;
use seerdb::mvcc::PMT;
use seerdb::storage::format::{MANIFEST_SLOT_SIZE, Manifest, SUPERBLOCK_SIZE, Superblock};

fuzz_target!(|data: &[u8]| {
    let _ = PMT::from_bytes(data);
    let _ = PageAllocator::from_bytes(data);

    let mut manifest = [0u8; MANIFEST_SLOT_SIZE];
    let manifest_len = data.len().min(MANIFEST_SLOT_SIZE);
    manifest[..manifest_len].copy_from_slice(&data[..manifest_len]);
    let _ = Manifest::from_bytes(&manifest);

    let mut superblock = [0u8; SUPERBLOCK_SIZE];
    let superblock_len = data.len().min(SUPERBLOCK_SIZE);
    superblock[..superblock_len].copy_from_slice(&data[..superblock_len]);
    let _ = Superblock::from_bytes(&superblock);
});
