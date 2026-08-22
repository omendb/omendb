use super::*;

#[test]
fn test_empty_node() {
    let node = Node::new_leaf();
    assert_eq!(node.count(), 0);
    assert!(node.is_leaf());
    assert!(!node.is_internal());
    assert!(node.header().is_valid());
}

#[test]
fn test_internal_node() {
    let node = Node::new_internal();
    assert!(node.is_internal());
    assert_eq!(node.count(), 0);
}

#[test]
fn test_insert_and_lookup() {
    let mut node = Node::new_leaf();
    node.insert(b"hello", b"world").unwrap();
    node.insert(b"foo", b"bar").unwrap();
    node.insert(b"aaa", b"bbb").unwrap();

    assert_eq!(node.count(), 3);

    // Keys should be sorted.
    assert_eq!(node.key(0), Some(b"aaa".to_vec()));
    assert_eq!(node.key(1), Some(b"foo".to_vec()));
    assert_eq!(node.key(2), Some(b"hello".to_vec()));

    // Values should be retrievable.
    assert!(matches!(node.value(0), Some(ValueRef::Inline(b"bbb"))));
    assert!(matches!(node.value(1), Some(ValueRef::Inline(b"bar"))));
    assert!(matches!(node.value(2), Some(ValueRef::Inline(b"world"))));
}

#[test]
fn test_search() {
    let mut node = Node::new_leaf();
    node.insert(b"a", b"1").unwrap();
    node.insert(b"c", b"3").unwrap();
    node.insert(b"e", b"5").unwrap();

    assert_eq!(node.search(b"a"), Ok(0));
    assert_eq!(node.search(b"c"), Ok(1));
    assert_eq!(node.search(b"e"), Ok(2));
    assert_eq!(node.search(b"b"), Err(1)); // between a and c
    assert_eq!(node.search(b"d"), Err(2)); // between c and e
    assert_eq!(node.search(b"z"), Err(3)); // after all
}

#[test]
fn test_shared_prefix_keys_roundtrip() {
    let mut node = Node::new_leaf();
    // Shared-prefix keys must remain lossless under the self-contained
    // live-mutation encoding.
    node.insert(b"key_001", b"v1").unwrap();
    node.insert(b"key_002", b"v2").unwrap();
    node.insert(b"key_003", b"v3").unwrap();

    assert_eq!(node.key(0), Some(b"key_001".to_vec()));
    assert_eq!(node.key(1), Some(b"key_002".to_vec()));
    assert_eq!(node.key(2), Some(b"key_003".to_vec()));
    assert_eq!(node.entry_prefix_len(1), 0);
    assert_eq!(node.entry_prefix_len(2), 0);
}

#[test]
fn test_entry_too_large_is_typed() {
    let mut node = Node::new_leaf();
    let key = vec![0xA5; PAGE_SIZE];
    assert!(matches!(
        node.insert(&key, b"value"),
        Err(InsertError::EntryTooLarge)
    ));
}

#[test]
fn test_tombstone() {
    let mut node = Node::new_leaf();
    node.insert(b"alive", b"yes").unwrap();
    node.insert_tombstone(b"dead").unwrap();

    assert_eq!(node.count(), 2);
    assert!(matches!(node.value(0), Some(ValueRef::Inline(_))));
    assert!(matches!(node.value(1), Some(ValueRef::Tombstone)));
}

#[test]
fn test_tombstone_same_key() {
    let mut node = Node::new_leaf();
    node.insert(b"key", b"value").unwrap();

    node.insert_tombstone(b"key").unwrap();
    assert_eq!(node.count(), 1);
    assert_eq!(node.search(b"key"), Ok(0));
    assert!(matches!(node.value(0), Some(ValueRef::Tombstone)));
}

#[test]
fn test_blob_pointer() {
    let mut node = Node::new_leaf();
    node.insert(b"aaa", b"inline").unwrap();
    node.insert_blob(
        b"zzz",
        BlobPointer {
            file_id: 1,
            offset: 4096,
            length: 1024,
        },
    )
    .unwrap();

    assert_eq!(node.count(), 2);
    assert!(matches!(node.value(0), Some(ValueRef::Inline(_))));
    assert!(matches!(
        node.value(1),
        Some(ValueRef::Blob(BlobPointer {
            file_id: 1,
            offset: 4096,
            length: 1024
        }))
    ));
}

#[test]
fn test_internal_node_children() {
    let mut node = Node::new_internal();
    node.set_leftmost_child(7);
    node.insert_child(b"b", 10).unwrap();
    node.insert_child(b"d", 20).unwrap();
    node.insert_child(b"f", 30).unwrap();

    assert_eq!(node.child_id(0), Some(10));
    assert_eq!(node.child_id(1), Some(20));
    assert_eq!(node.child_id(2), Some(30));

    let restored = Node::from_bytes(node.into_bytes()).unwrap();
    assert_eq!(restored.leftmost_child(), 7);
    assert_eq!(restored.child_id(0), Some(10));
}

#[test]
fn test_duplicate_key_rejected() {
    let mut node = Node::new_leaf();
    node.insert(b"key", b"val1").unwrap();
    assert!(matches!(
        node.insert(b"key", b"val2"),
        Err(InsertError::DuplicateKey(_))
    ));
}

#[test]
fn test_split_leaf() {
    let mut node = Node::new_leaf();
    for i in 0..10 {
        let key = format!("key_{:03}", i);
        let val = format!("val_{:03}", i);
        node.insert(key.as_bytes(), val.as_bytes()).unwrap();
    }

    let (median, right) = node.split().unwrap();
    assert_eq!(median, b"key_005");
    assert_eq!(node.count(), 5);
    assert_eq!(right.count(), 5);

    // Left has first 5 keys.
    assert_eq!(node.key(0), Some(b"key_000".to_vec()));
    assert_eq!(node.key(4), Some(b"key_004".to_vec()));

    // Right has last 5 keys.
    assert_eq!(right.key(0), Some(b"key_005".to_vec()));
    assert_eq!(right.key(4), Some(b"key_009".to_vec()));
}

#[test]
fn test_checksum_roundtrip() {
    let mut node = Node::new_leaf();
    node.insert(b"key", b"value").unwrap();
    node.update_checksum();
    assert!(node.verify_checksum());
}

#[test]
fn test_from_bytes_roundtrip() {
    let mut node = Node::new_leaf();
    node.insert(b"hello", b"world").unwrap();

    let bytes = node.into_bytes();
    let restored = Node::from_bytes(bytes).unwrap();
    assert_eq!(restored.count(), 1);
    assert_eq!(restored.key(0), Some(b"hello".to_vec()));
}

#[test]
fn test_invalid_magic() {
    let data = Box::new([0u8; PAGE_SIZE]);
    assert!(Node::from_bytes(data).is_none());
}

#[test]
fn test_rejects_unknown_page_type() {
    let mut node = Node::new_leaf().into_bytes();
    node[8..12].copy_from_slice(&99u32.to_le_bytes());
    assert!(Node::from_bytes(node).is_none());
}

#[test]
fn test_rejects_invalid_slot_bounds() {
    let mut node = Node::new_leaf();
    node.insert(b"key", b"value").unwrap();
    let mut bytes = node.into_bytes();
    // First slot entry lives just past the (48-byte) header.
    bytes[48..50].copy_from_slice(&1u16.to_le_bytes());
    assert!(Node::from_bytes(bytes).is_none());
}

#[test]
fn test_keys_iterator() {
    let mut node = Node::new_leaf();
    node.insert(b"c", b"3").unwrap();
    node.insert(b"a", b"1").unwrap();
    node.insert(b"b", b"2").unwrap();

    let keys: Vec<_> = node.keys().collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

#[test]
fn test_many_inserts() {
    let mut node = Node::new_leaf();
    let mut inserted = Vec::new();

    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        match node.insert(key.as_bytes(), val.as_bytes()) {
            Ok(()) => inserted.push(key),
            Err(InsertError::PageFull) => break,
            Err(e) => panic!("unexpected error: {}", e),
        }
    }

    // Should fit many entries in 4KB.
    assert!(
        inserted.len() > 50,
        "expected >50 entries, got {}",
        inserted.len()
    );

    // Verify all inserted keys are searchable.
    for key in &inserted {
        assert!(node.search(key.as_bytes()).is_ok(), "key {} not found", key);
    }
}

#[test]
fn test_parent_id() {
    let mut node = Node::new_leaf();
    assert_eq!(node.parent_id(), 0);
    node.set_parent_id(42);
    assert_eq!(node.parent_id(), 42);
}

#[test]
fn test_header_roundtrip() {
    let header = NodeHeader {
        magic: page_format::MAGIC,
        version: page_format::PAGE_VERSION,
        page_type: PageType::Leaf,
        count: 5,
        free_space: 3000,
        checksum: 0xDEAD_BEEF_CAFE_BABE,
        parent_id: 123,
        leftmost_child: 456,
        write_generation: 789,
    };
    let bytes = header.to_bytes();
    let restored = NodeHeader::from_bytes(&bytes);
    assert_eq!(restored.magic, header.magic);
    assert_eq!(restored.version, header.version);
    assert_eq!(restored.page_type, header.page_type);
    assert_eq!(restored.count, header.count);
    assert_eq!(restored.free_space, header.free_space);
    assert_eq!(restored.checksum, header.checksum);
    assert_eq!(restored.write_generation, header.write_generation);
    assert_eq!(restored.parent_id, header.parent_id);
    assert_eq!(restored.leftmost_child, header.leftmost_child);
}

#[test]
fn test_tombstone_blob_layout_roundtrip() {
    let mut node = Node::new_leaf();
    for key in [b"key1".as_slice(), b"key2".as_slice(), b"key3".as_slice()] {
        node.insert_blob(
            key,
            BlobPointer {
                file_id: 1,
                offset: 4096,
                length: 2000,
            },
        )
        .unwrap();
    }
    node.insert_tombstone(b"key1").unwrap();
    node.insert_tombstone(b"key2").unwrap();

    assert!(Node::from_bytes(node.into_bytes()).is_some());
}
