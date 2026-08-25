use seerdb::allocator::PageAllocator;
use seerdb::btree::{BTree, BTreeError, InsertError, Node};

#[test]
fn missing_sparse_page_is_a_typed_mutation_error_without_dirtying_it() {
    let mut tree = BTree::from_sparse_with_allocator(1, 0, PageAllocator::new());

    assert!(matches!(
        tree.insert(b"key", b"value"),
        Err(BTreeError::MissingPage(0))
    ));
    assert!(tree.dirty_page_ids().is_empty());
}

#[test]
fn same_size_replacement_rejects_invalid_index_and_size_without_panicking() {
    let mut node = Node::new_leaf();
    assert!(node.insert(b"key", b"value").is_ok());

    assert!(matches!(
        node.replace_value(1, b"value"),
        Err(InsertError::InvalidIndex(1))
    ));
    assert!(matches!(
        node.replace_value(0, b"different"),
        Err(InsertError::ValueSizeMismatch {
            expected: 5,
            actual: 9
        })
    ));
    assert_eq!(
        node.value(0).and_then(|value| match value {
            seerdb::btree::ValueRef::Inline(value) => Some(value.to_vec()),
            _ => None,
        }),
        Some(b"value".to_vec())
    );
}
