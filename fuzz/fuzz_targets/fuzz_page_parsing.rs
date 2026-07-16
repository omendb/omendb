#![no_main]

use libfuzzer_sys::fuzz_target;
use seerdb::btree::{Node, PAGE_SIZE};

fuzz_target!(|data: &[u8]| {
    let mut page = [0u8; PAGE_SIZE];
    let copy_len = data.len().min(PAGE_SIZE);
    page[..copy_len].copy_from_slice(&data[..copy_len]);
    if let Some(node) = Node::from_bytes(Box::new(page)) {
        let _ = node.verify_checksum();
        let _ = node.count();
        let _ = node.is_leaf();
        let _ = node.key(0);
        let _ = node.value(0);
        let _ = node.child_id(0);
    }
});
