use seerdb::allocator::PageAllocator;
use seerdb::btree::{BTree, BTreeError};

#[test]
fn missing_sparse_page_is_a_typed_mutation_error_without_dirtying_it() {
    let mut tree = BTree::from_sparse_with_allocator(1, 0, PageAllocator::new());

    assert!(matches!(
        tree.insert(b"key", b"value"),
        Err(BTreeError::MissingPage(0))
    ));
    assert!(tree.dirty_page_ids().is_empty());
}
