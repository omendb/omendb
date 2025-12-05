use bytes::Bytes;
use seerdb::{DBOptions, StringAppendOperator};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_merge_in_memory() {
    let dir = tempdir().unwrap();

    let db = DBOptions::default()
        .merge_operator(Arc::new(StringAppendOperator::new(',')))
        .open(dir.path())
        .unwrap();

    // 1. Put base value
    db.put(b"key1", b"val").unwrap();

    // 2. Merge operand
    db.merge(b"key1", b"op1").unwrap();

    // 3. Get (should resolve in memory)
    // "val" + "," + "op1"
    assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("val,op1")));
}

#[test]
fn test_merge_stacking() {
    let dir = tempdir().unwrap();

    let db = DBOptions::default()
        .merge_operator(Arc::new(StringAppendOperator::new(',')))
        .open(dir.path())
        .unwrap();

    // 1. Merge (no base)
    db.merge(b"list", b"item1").unwrap();

    // 2. Merge again
    db.merge(b"list", b"item2").unwrap();

    // 3. Get (should be "item1,item2")
    // Note: First merge on empty/tombstone acts as Put for StringAppendOperator logic
    // Wait, StringAppendOperator logic:
    // if existing_value.is_some() { result.push_str(v) }
    // loop operands { if !result.is_empty { push delimiter } push op }
    // So if base is None: result starts empty. op1 pushed. "item1".
    // Then op2 pushed. "item1,item2". Correct.
    assert_eq!(db.get(b"list").unwrap(), Some(Bytes::from("item1,item2")));
}

#[test]
fn test_merge_flush_and_recovery() {
    let dir = tempdir().unwrap();

    {
        let db = DBOptions::default()
            .memtable_capacity(1024)
            .merge_operator(Arc::new(StringAppendOperator::new(',')))
            .open(dir.path())
            .unwrap();
        db.put(b"key", b"base").unwrap();
        db.merge(b"key", b"op1").unwrap();
        db.flush().unwrap(); // Flush to SSTable

        // Merge on top of SSTable (in new memtable)
        db.merge(b"key", b"op2").unwrap();

        // Check value
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("base,op1,op2")));
    }

    // Reopen (Recovery)
    {
        let db = DBOptions::default()
            .memtable_capacity(1024)
            .merge_operator(Arc::new(StringAppendOperator::new(',')))
            .open(dir.path())
            .unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("base,op1,op2")));
    }
}

#[test]
fn test_merge_with_delete() {
    let dir = tempdir().unwrap();

    let db = DBOptions::default()
        .merge_operator(Arc::new(StringAppendOperator::new(',')))
        .open(dir.path())
        .unwrap();

    db.put(b"key", b"val1").unwrap();
    db.delete(b"key").unwrap();
    db.merge(b"key", b"val2").unwrap();

    // Should act as if base is None (because of delete)
    // So result is "val2"
    assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("val2")));
}
