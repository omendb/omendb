use super::*;
use crate::btree::{LookupResult, PAGE_SIZE};
use crate::space::DeviceOptions;
use std::fs;
use tempfile::tempdir;

#[test]
fn runtime_invariants_cover_pmt_and_generation_state() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    assert!(engine.validate_runtime_state().is_ok());
    engine.btree_mut().insert(b"key", b"value").unwrap();
    assert!(engine.validate_runtime_state().is_ok());
    engine.flush().unwrap();
    assert!(engine.validate_runtime_state().is_ok());

    engine.pmt_mut().insert(9, 0, 1);
    let error = engine.validate_runtime_state().unwrap_err();
    assert!(matches!(error, Error::Corruption(message) if message.contains("unaligned offset")));
}

#[test]
fn buffer_stages_versioned_writeback_without_aliasing_generations() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE * 2),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    engine
        .btree_mut()
        .insert(b"key", b"value")
        .expect("initial insert should fit");
    engine.flush().unwrap();
    let first = engine.buffer_stats();
    assert_eq!(first.reads, 1);
    assert_eq!(first.hits, 0);
    assert_eq!(first.writes, 1);
    assert_eq!(first.dirty_frames, 0);

    engine
        .btree_mut()
        .insert(b"key2", b"updated")
        .expect("second insert should fit");
    engine.flush().unwrap();
    let second = engine.buffer_stats();
    // The new pending image must not alias the old published-version
    // frame. It is a second cache miss until publication can retire the
    // old generation safely.
    assert_eq!(second.reads, 2);
    assert_eq!(second.hits, 0);
    assert_eq!(second.writes, 2);
    assert_eq!(second.dirty_frames, 0);
}

#[test]
fn large_generation_streams_through_small_buffer_pool() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE * 2),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    for index in 0..200 {
        let key = format!("key-{index:04}");
        engine
            .btree_mut()
            .insert(key.as_bytes(), b"value")
            .expect("test key should fit");
    }
    assert!(engine.btree().dirty_page_ids().len() > 2);

    engine.flush().unwrap();
    engine.complete_generation();
    let stats = engine.buffer_stats();
    assert_eq!(stats.dirty_frames, 0);
    assert!(stats.writeback_discards > 0);
    assert_eq!(
        engine.lookup(b"key-0199").unwrap(),
        LookupResult::Found(b"value".to_vec())
    );
}

#[test]
fn streamed_generation_sync_failure_remains_retryable() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE * 2),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    for index in 0..200 {
        let key = format!("key-{index:04}");
        engine
            .btree_mut()
            .insert(key.as_bytes(), b"value")
            .expect("test key should fit");
    }
    engine.inject_sync_failure();

    assert!(matches!(engine.flush(), Err(Error::Io(_))));
    assert!(!engine.btree().dirty_page_ids().is_empty());
    engine.flush().unwrap();
    engine.complete_generation();
    assert_eq!(
        engine.lookup(b"key-0199").unwrap(),
        LookupResult::Found(b"value".to_vec())
    );
}

#[test]
fn failed_device_write_leaves_buffer_image_dirty_for_retry() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE * 2),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    engine.btree_mut().insert(b"key", b"value").unwrap();
    engine.inject_write_failure();
    assert!(matches!(engine.flush(), Err(Error::Io(_))));
    assert_eq!(engine.buffer_stats().dirty_frames, 1);

    engine.flush().unwrap();
    assert_eq!(engine.buffer_stats().dirty_frames, 0);
    assert_eq!(engine.device.size().unwrap(), PAGE_SIZE as u64);
}

#[test]
fn failed_device_sync_leaves_buffer_image_dirty_for_recovery() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE * 2),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    engine.btree_mut().insert(b"key", b"value").unwrap();
    engine.inject_sync_failure();
    assert!(matches!(engine.flush(), Err(Error::Io(_))));
    assert_eq!(engine.buffer_stats().dirty_frames, 1);
}

#[test]
fn load_from_disk_rejects_malformed_page() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data");
    fs::write(&data_path, [0u8; PAGE_SIZE]).unwrap();
    let device = Device::open(
        &data_path,
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE * 2),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    assert!(matches!(
        engine.load_from_disk(),
        Err(Error::Corruption(message)) if message.contains("invalid page")
    ));
}

#[test]
fn flush_writes_only_logically_dirty_pages() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE * 600),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    for index in 0..500 {
        let key = format!("key-{index:06}");
        engine.btree_mut().insert(key.as_bytes(), b"v").unwrap();
    }
    engine.flush().unwrap();
    let first = engine.buffer_stats();
    assert!(first.writes > 1);
    engine.complete_generation();

    engine.btree_mut().upsert(b"key-000250", b"x").unwrap();
    engine.flush().unwrap();
    let second = engine.buffer_stats();
    assert_eq!(second.writes - first.writes, 1);
}

#[test]
fn read_node_uses_pmt_and_buffer_boundary() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE * 2),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    engine.btree_mut().insert(b"key", b"value").unwrap();
    engine.flush().unwrap();
    engine.complete_generation();

    assert!(engine.read_node(0).unwrap().is_leaf());
    assert!(engine.read_node(0).unwrap().is_leaf());
    assert!(matches!(
        engine.read_node(1),
        Err(Error::Corruption(message)) if message.contains("missing page")
    ));
    assert!(engine.metrics().parsed_page_cache_hits >= 1);
}

#[test]
fn reuses_retired_physical_pages_after_generation_completion() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE * 2),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    engine.btree_mut().insert(b"key", b"value-1").unwrap();
    engine.flush().unwrap();
    engine.complete_generation();
    assert_eq!(engine.device.size().unwrap(), PAGE_SIZE as u64);

    engine.btree_mut().upsert(b"key", b"value-2").unwrap();
    engine.flush().unwrap();
    assert_eq!(engine.device.size().unwrap(), (PAGE_SIZE * 2) as u64);
    engine.complete_generation();
    assert_eq!(engine.reclaimable_page_count(), 1);

    engine.btree_mut().upsert(b"key", b"value-3").unwrap();
    engine.flush().unwrap();
    assert_eq!(engine.device.size().unwrap(), (PAGE_SIZE * 2) as u64);
    assert_eq!(engine.reclaimable_page_count(), 0);
}

#[test]
fn empty_buffer_pool_returns_typed_error() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(0),
        PMT::new(),
        PageAllocator::new(),
        device,
    );
    engine
        .btree_mut()
        .insert(b"key", b"value")
        .expect("initial insert should fit");

    assert!(matches!(engine.flush(), Err(Error::Buffer(message)) if message.contains("no frames")));
}

#[test]
fn capacity_preflight_rejects_before_page_io() {
    let dir = tempdir().unwrap();
    let device = Device::open(
        dir.path().join("data"),
        &DeviceOptions {
            use_odirect: false,
            sync_writes: false,
            create: true,
        },
    )
    .unwrap();
    let mut engine = StorageEngine::new(
        BTree::new(),
        BufferManager::new(PAGE_SIZE),
        PMT::new(),
        PageAllocator::new(),
        device,
    );
    engine.btree_mut().insert(b"key", b"value").unwrap();
    engine.inject_capacity_limit(0);

    assert!(matches!(engine.flush(), Err(Error::CapacityPreflight)));
    assert_eq!(engine.device.size().unwrap(), 0);
    let stats = engine.buffer_stats();
    assert_eq!(stats.reads, 0);
    assert_eq!(stats.writes, 0);
}
