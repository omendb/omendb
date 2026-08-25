use std::collections::BTreeMap;

use proptest::prelude::*;

use crate::btree::node::BlobPointer;

use super::*;

#[test]
fn test_btree_insert_and_lookup() {
    let mut tree = BTree::new();

    tree.insert(b"hello", b"world").unwrap();
    tree.insert(b"foo", b"bar").unwrap();
    tree.insert(b"aaa", b"bbb").unwrap();

    assert!(matches!(
        tree.lookup(b"hello").unwrap(),
        LookupResult::Found(value) if value == b"world"
    ));
    assert!(matches!(
        tree.lookup(b"foo").unwrap(),
        LookupResult::Found(value) if value == b"bar"
    ));
    assert!(matches!(
        tree.lookup(b"aaa").unwrap(),
        LookupResult::Found(value) if value == b"bbb"
    ));
    assert!(matches!(
        tree.lookup(b"missing").unwrap(),
        LookupResult::NotFound
    ));
}

#[test]
fn test_btree_duplicate_key() {
    let mut tree = BTree::new();

    tree.insert(b"key", b"val1").unwrap();
    assert!(matches!(
        tree.insert(b"key", b"val2"),
        Err(BTreeError::DuplicateKey)
    ));
}

#[test]
fn test_btree_upsert_resizes_existing_value() {
    let mut tree = BTree::new();

    tree.upsert(b"key", b"short").unwrap();
    tree.upsert(b"key", b"a value with a different size")
        .unwrap();
    assert!(matches!(
        tree.lookup(b"key").unwrap(),
        LookupResult::Found(value) if value == b"a value with a different size"
    ));

    tree.upsert(b"key", b"x").unwrap();
    assert!(matches!(
        tree.lookup(b"key").unwrap(),
        LookupResult::Found(value) if value == b"x"
    ));

    tree.delete(b"key").unwrap();
    tree.upsert(b"key", b"restored").unwrap();
    assert!(matches!(
        tree.lookup(b"key").unwrap(),
        LookupResult::Found(value) if value == b"restored"
    ));
}

#[test]
fn test_btree_clone_isolated_after_mutation() {
    let mut original = BTree::new();
    original.insert(b"key", b"before").unwrap();

    let mut candidate = original.clone();
    candidate.upsert(b"key", b"after").unwrap();
    candidate.insert(b"another", b"value").unwrap();

    assert!(matches!(
        original.lookup(b"key").unwrap(),
        LookupResult::Found(value) if value == b"before"
    ));
    assert!(matches!(
        original.lookup(b"another").unwrap(),
        LookupResult::NotFound
    ));
    assert!(matches!(
        candidate.lookup(b"key").unwrap(),
        LookupResult::Found(value) if value == b"after"
    ));
}

#[test]
fn test_lookup_result_owns_inline_value() {
    let mut tree = BTree::new();
    tree.insert(b"key", b"before").unwrap();

    let value = match tree.lookup(b"key").unwrap() {
        LookupResult::Found(value) => value,
        other => panic!("unexpected lookup result: {other:?}"),
    };
    tree.upsert(b"key", b"after").unwrap();

    assert_eq!(value, b"before");
    assert!(matches!(
        tree.lookup(b"key").unwrap(),
        LookupResult::Found(value) if value == b"after"
    ));
}

#[test]
fn test_btree_delete() {
    let mut tree = BTree::new();

    tree.insert(b"key", b"value").unwrap();
    assert!(matches!(
        tree.lookup(b"key").unwrap(),
        LookupResult::Found(_)
    ));

    tree.delete(b"key").unwrap();
    assert!(matches!(
        tree.lookup(b"key").unwrap(),
        LookupResult::Deleted
    ));
    assert!(tree.range_scan(b"key", b"key~").unwrap().next().is_none());
    assert!(!tree.delete(b"key").unwrap());

    assert!(!tree.delete(b"missing").unwrap());
}

#[test]
fn test_btree_split() {
    let mut tree = BTree::new();

    for i in 0..500 {
        let key = format!("key_{:06}", i);
        let val = format!("val_{:06}", i);
        tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
    }

    for i in 0..500 {
        let key = format!("key_{:06}", i);
        let val = format!("val_{:06}", i);
        assert!(matches!(
            tree.lookup(key.as_bytes()).unwrap(),
            LookupResult::Found(v) if v == val.as_bytes()
        ));
    }

    assert!(tree.node_count() > 1);
}

#[test]
fn test_btree_out_of_order_internal_split_rebuilds_left_half() {
    let keys = 8_192;
    let operations = 38_205;
    let mut tree = BTree::new();
    let mut reference = BTreeMap::new();
    let mut revisions = vec![0usize; keys + 524_288];

    for index in 0..keys {
        let key = format!("qualification-key-{index:08}");
        let value = format!("qualification-value-{index:08}-revision-{:08}", 0);
        tree.upsert(key.as_bytes(), value.as_bytes()).unwrap();
        reference.insert(key.into_bytes(), value.into_bytes());
    }

    let mut state = 20_260_727_u64;
    for operation in 0..operations {
        let random = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state = random;
        let key_index = (random as usize) % (keys + 524_288);
        let key = format!("qualification-key-{key_index:08}");
        match random % 100 {
            0..=44 => {}
            45..=69 => {
                revisions[key_index] = revisions[key_index].saturating_add(1);
                let value = format!(
                    "qualification-value-{key_index:08}-revision-{:08}",
                    revisions[key_index]
                );
                tree.upsert(key.as_bytes(), value.as_bytes())
                    .unwrap_or_else(|error| panic!("operation {operation}: {error:?}"));
                reference.insert(key.into_bytes(), value.into_bytes());
            }
            70..=84 => {
                tree.delete(key.as_bytes())
                    .unwrap_or_else(|error| panic!("operation {operation}: {error:?}"));
                reference.remove(key.as_bytes());
            }
            _ => {}
        }
    }

    let actual = tree
        .range_scan(b"qualification-key-", b"qualification-key-\xFF")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected: Vec<_> = reference
        .into_iter()
        .map(|(key, value)| (key, LookupResult::Found(value)))
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn test_btree_range_scan() {
    let mut tree = BTree::new();

    tree.insert(b"a", b"1").unwrap();
    tree.insert(b"b", b"2").unwrap();
    tree.insert(b"c", b"3").unwrap();
    tree.insert(b"d", b"4").unwrap();
    tree.insert(b"e", b"5").unwrap();

    let results: Vec<_> = tree
        .range_scan(b"b", b"e")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, b"b");
    assert_eq!(results[1].0, b"c");
    assert_eq!(results[2].0, b"d");
}

#[test]
fn test_btree_many_inserts() {
    let mut tree = BTree::new();

    for i in 0..500 {
        let key = format!("key_{:06}", i);
        let val = format!("val_{:06}", i);
        tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
    }

    for i in 0..500 {
        let key = format!("key_{:06}", i);
        assert!(matches!(
            tree.lookup(key.as_bytes()).unwrap(),
            LookupResult::Found(_)
        ));
    }
}

#[test]
fn test_btree_large_range_scan_preserves_all_keys() {
    let mut tree = BTree::new();
    for index in 0..2_000 {
        let key = format!("index/{:04}/{:04}", index % 32, index);
        tree.upsert(key.as_bytes(), b"value").unwrap();
    }

    let entries = tree
        .range_scan(b"index/", b"index/\xFF")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 2_000);
}

#[test]
fn test_btree_large_namespace_batch_preserves_ranges() {
    let mut tree = BTree::new();
    let mut row_prefix = vec![0x10];
    row_prefix.extend_from_slice(&10_u64.to_be_bytes());
    let mut index_prefix = vec![0x20];
    index_prefix.extend_from_slice(&10_u64.to_be_bytes());
    index_prefix.extend_from_slice(&10_u64.to_be_bytes());

    for document_id in 1..=2_000_u64 {
        let mut row_key = row_prefix.clone();
        row_key.extend_from_slice(&10_u64.to_be_bytes());
        row_key.extend_from_slice(&document_id.to_be_bytes());
        tree.upsert(&row_key, b"row").unwrap();

        for index_id in 0..3_u64 {
            let mut index_key = index_prefix.clone();
            index_key[9..17].copy_from_slice(&index_id.to_be_bytes());
            index_key.push(0x04);
            index_key.extend_from_slice(&(document_id % 32).to_be_bytes());
            index_key.extend_from_slice(&10_u64.to_be_bytes());
            index_key.extend_from_slice(&document_id.to_be_bytes());
            tree.upsert(&index_key, b"index").unwrap();
        }
    }

    for document_id in 1..=2_000_u64 {
        let mut row_key = row_prefix.clone();
        row_key.extend_from_slice(&10_u64.to_be_bytes());
        row_key.extend_from_slice(&document_id.to_be_bytes());
        assert!(matches!(tree.lookup(&row_key), Ok(LookupResult::Found(_))));
    }

    let mut row_end = row_prefix.clone();
    row_end.push(u8::MAX);
    let rows = tree
        .range_scan(&row_prefix, &row_end)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 2_000);

    let mut pending = vec![tree.root];
    while let Some(parent_id) = pending.pop() {
        let parent = tree.node(parent_id).unwrap();
        if parent.is_leaf() {
            continue;
        }
        let mut children = vec![parent.leftmost_child() as PageId];
        children.extend((0..parent.count()).map(|index| parent.child_id(index).unwrap() as PageId));
        for child_id in children {
            assert_eq!(tree.node(child_id).unwrap().parent_id(), parent_id);
            pending.push(child_id);
        }
    }
}

#[test]
fn test_btree_sorted_order() {
    let mut tree = BTree::new();

    for i in (0..50).rev() {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
    }

    for i in 0..50 {
        let key = format!("key_{:04}", i);
        assert!(matches!(
            tree.lookup(key.as_bytes()).unwrap(),
            LookupResult::Found(_)
        ));
    }
}

#[test]
fn test_btree_upsert_split_and_leftmost_parent_routing() {
    let mut tree = BTree::new();

    for i in (0..500).rev() {
        let key = format!("key_{i:06}");
        tree.upsert(key.as_bytes(), b"initial")
            .unwrap_or_else(|error| panic!("initial i={i}: {error:?}"));
    }

    for i in 0..500 {
        let key = format!("key_{i:06}");
        let value = format!("updated value with a different size {i}");
        tree.upsert(key.as_bytes(), value.as_bytes()).unwrap();
        assert!(matches!(
            tree.lookup(key.as_bytes()).unwrap(),
            LookupResult::Found(found) if found == value.as_bytes()
        ));
    }

    assert!(tree.node_count() > 2);
}

#[test]
fn test_btree_blob_upsert_splits_full_leaves() {
    let mut tree = BTree::new();

    for index in 0..1_000 {
        let key = format!("blob-key-{index:04}");
        let pointer = BlobPointer {
            file_id: 1,
            offset: index as u64 * 32,
            length: 2_048,
        };
        let result = if index % 2 == 0 {
            tree.insert_blob(key.as_bytes(), pointer)
        } else {
            tree.upsert_blob(key.as_bytes(), pointer)
        };
        result.unwrap_or_else(|error| panic!("blob insert {index} failed: {error:?}"));
    }

    for index in 0..1_000 {
        let key = format!("blob-key-{index:04}");
        let expected = BlobPointer {
            file_id: 1,
            offset: index as u64 * 32,
            length: 2_048,
        };
        assert_eq!(
            tree.lookup(key.as_bytes()).unwrap(),
            LookupResult::Blob(expected)
        );
    }
    assert!(tree.node_count() > 2);
}

#[test]
fn test_btree_range_scan_across_split_leaves() {
    let mut tree = BTree::new();
    for i in 0..500 {
        let key = format!("key_{i:06}");
        let value = format!("value_{i:06}");
        tree.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let results: Vec<_> = tree
        .range_scan(b"key_000050", b"key_000450")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 400);
    assert_eq!(results.first().unwrap().0, b"key_000050");
    assert_eq!(results.last().unwrap().0, b"key_000449");
}

#[test]
fn test_btree_range_scan_falls_back_from_stale_parent_hint() {
    let mut tree = BTree::new();
    for i in 0..500 {
        let key = format!("key_{i:06}");
        tree.insert(key.as_bytes(), b"value").unwrap();
    }

    let leaf_id = tree.find_leaf(b"key_000050").unwrap();
    tree.node_mut(leaf_id).unwrap().set_parent_id(u32::MAX);

    let results: Vec<_> = tree
        .range_scan(b"key_000050", b"key_000450")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 400);
    assert_eq!(results.first().unwrap().0, b"key_000050");
    assert_eq!(results.last().unwrap().0, b"key_000449");
}

#[test]
fn test_range_scan_reports_routing_corruption() {
    let mut tree = BTree::new();
    let mut root = Node::new_internal();
    root.set_leftmost_child(1);
    root.insert_child(b"z", u64::MAX).unwrap();

    let mut leaf = Node::new_leaf();
    leaf.insert(b"a", b"value").unwrap();

    tree.add_node(root, 0);
    tree.add_node(leaf, 1);

    let mut scan = tree.range_scan(b"a", b"zz").unwrap();
    assert!(matches!(
        scan.next(),
        Some(Ok((key, LookupResult::Found(value)))) if key == b"a" && value == b"value"
    ));
    assert!(matches!(
        scan.next(),
        Some(Err(BTreeError::Corruption(message)))
            if message.contains("logical ID width")
    ));
    assert!(scan.next().is_none());
}

proptest! {
    #[test]
    fn prop_btree_mutations_match_reference_model(
        operations in prop::collection::vec(
            (0u8..64, prop::collection::vec(any::<u8>(), 0..48), any::<bool>()),
            1..200
        )
    ) {
        let mut tree = BTree::new();
        let mut reference = BTreeMap::new();

        for (key_id, value, is_write) in operations {
            let key = format!("key-{key_id:03}");
            if is_write {
                tree.upsert(key.as_bytes(), &value).unwrap();
                reference.insert(key.into_bytes(), value);
            } else {
                let expected = reference.remove(key.as_bytes()).is_some();
                prop_assert_eq!(tree.delete(key.as_bytes()).unwrap(), expected);
            }

            for (reference_key, reference_value) in &reference {
                prop_assert!(matches!(
                    tree.lookup(reference_key).unwrap(),
                    LookupResult::Found(value) if value == reference_value.as_slice()
                ));
            }
        }

        let actual: Vec<_> = tree
            .range_scan(b"key-000", b"key-999")
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .filter_map(|(key, value)| match value {
                LookupResult::Found(value) => Some((key, value)),
                LookupResult::Deleted | LookupResult::NotFound => None,
                LookupResult::Blob(pointer) => {
                    panic!("unexpected blob range value: {pointer:?}")
                }
            })
            .collect();
        let expected: Vec<_> = reference.into_iter().collect();
        prop_assert_eq!(actual, expected);
    }
}
