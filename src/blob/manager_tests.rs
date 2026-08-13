use super::*;
use std::collections::HashMap;

#[test]
fn test_blob_manager_new() {
    let bm = BlobManager::new();
    assert_eq!(bm.threshold(), DEFAULT_BLOB_THRESHOLD);
    assert_eq!(bm.file_count(), 0);
}

#[test]
fn append_returns_typed_error_when_file_id_space_is_exhausted() {
    let mut bm = BlobManager::new();
    bm.next_file_id = u32::MAX;

    let error = bm.append(b"key", vec![1; 1500]).unwrap_err();

    assert_eq!(error, BlobManagerError::FileIdExhausted);
    assert_eq!(bm.file_count(), 0);
}

#[test]
fn test_blob_manager_rejects_trailing_bytes() {
    let mut bm = BlobManager::new();
    bm.append(b"key", vec![1; 1500]).unwrap();
    let mut bytes = bm.to_bytes();
    bytes.push(0xA5);

    assert!(BlobManager::from_bytes(&bytes).is_none());
}

#[test]
fn test_blob_manager_rejects_corrupt_container_checksum() {
    let mut bm = BlobManager::new();
    bm.append(b"key", vec![1; 1500]).unwrap();
    let mut bytes = bm.to_bytes();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xA5;

    assert!(BlobManager::from_bytes(&bytes).is_none());
}

#[test]
fn test_should_separate() {
    let bm = BlobManager::new();
    assert!(!bm.should_separate(100));
    assert!(!bm.should_separate(1024));
    assert!(bm.should_separate(1025));
}

#[test]
fn test_blob_append_and_read() {
    let mut bm = BlobManager::new();
    let value = vec![42u8; 2000]; // > 1KB threshold

    let ptr = bm.append(b"test_key", value.clone()).unwrap();
    assert_eq!(ptr.length, 2000);

    let read_value = bm.read(&ptr).unwrap();
    assert_eq!(read_value, &value);
}

#[test]
fn test_blob_multiple_appends() {
    let mut bm = BlobManager::new();

    let ptr1 = bm.append(b"key1", vec![1; 1500]).unwrap();
    let ptr2 = bm.append(b"key2", vec![2; 1500]).unwrap();

    assert_eq!(bm.read(&ptr1).unwrap(), &vec![1; 1500]);
    assert_eq!(bm.read(&ptr2).unwrap(), &vec![2; 1500]);
}

#[test]
fn test_blob_file_ids_are_canonicalized_for_indexed_lookup() {
    let mut manager = BlobManager::new();
    let mut high = BlobFile::new(9);
    let (high_offset, high_length) = high.append([0; 8], vec![9; 1500]);
    let mut low = BlobFile::new(2);
    let (low_offset, low_length) = low.append([0; 8], vec![2; 1500]);
    manager.files.push(Arc::new(high));
    manager.files.push(Arc::new(low));

    let restored = BlobManager::from_bytes(&manager.to_bytes()).unwrap();
    assert_eq!(restored.segment_file_ids(), vec![2, 9]);
    assert_eq!(
        restored.read(&BlobPointer {
            file_id: 9,
            offset: high_offset,
            length: high_length,
        }),
        Some(&vec![9; 1500][..])
    );
    assert_eq!(
        restored.read(&BlobPointer {
            file_id: 2,
            offset: low_offset,
            length: low_length,
        }),
        Some(&vec![2; 1500][..])
    );

    let mut segmented = BlobManager::with_threshold_and_mode(1024, true);
    let mut high = BlobFile::new(9);
    high.append([0; 8], vec![9; 1500]);
    let mut low = BlobFile::new(2);
    low.append([0; 8], vec![2; 1500]);
    segmented.files.push(Arc::new(high));
    segmented.files.push(Arc::new(low));
    segmented.set_generation(7);
    let catalog = segmented.to_segment_catalog_bytes();
    let segments = segmented
        .files
        .iter()
        .map(|file| (file.file_id(), file.to_bytes()))
        .collect::<HashMap<_, _>>();
    let restored =
        BlobManager::from_segment_catalog_with_delta_log(&catalog, &segments, &[], None).unwrap();
    assert_eq!(restored.segment_file_ids(), vec![2, 9]);
}

#[test]
fn test_blob_manager_clone_isolated_after_mutation() {
    let mut original = BlobManager::new();
    let pointer = original.append(b"key", vec![1; 1500]).unwrap();

    let mut candidate = original.clone();
    assert!(candidate.mark_deleted(&pointer));
    let candidate_pointer = candidate.append(b"another", vec![2; 1600]).unwrap();

    assert_eq!(original.total_valid_entries(), 1);
    assert_eq!(original.total_deleted_entries(), 0);
    assert_eq!(original.read(&pointer), Some(&vec![1; 1500][..]));
    assert!(original.read(&candidate_pointer).is_none());
    assert_eq!(candidate.total_valid_entries(), 1);
    assert_eq!(candidate.total_deleted_entries(), 1);
}

#[test]
fn test_blob_gc() {
    let mut bm = BlobManager::new();

    let ptr1 = bm.append(b"key1", vec![1; 1500]).unwrap();
    let ptr2 = bm.append(b"key2", vec![2; 1500]).unwrap();
    let ptr3 = bm.append(b"key3", vec![3; 1500]).unwrap();

    assert!(bm.files_needing_gc().is_empty());

    // Mark enough entries as deleted to trigger GC.
    bm.mark_deleted(&ptr1);
    bm.mark_deleted(&ptr2);

    assert!(!bm.files_needing_gc().is_empty());
    assert_eq!(bm.gc(), 0);
    assert_eq!(bm.read(&ptr3), Some(&vec![3; 1500][..]));
}

#[test]
fn test_blob_gc_reclaims_fully_dead_file() {
    let mut bm = BlobManager::new();
    let ptr = bm.append(b"key", vec![1; 1500]).unwrap();
    assert!(bm.mark_deleted(&ptr));

    assert_eq!(bm.gc(), 1);
    assert_eq!(bm.file_count(), 0);
}

#[test]
fn test_blob_rollback_only_removes_unpublished_tail() {
    let mut bm = BlobManager::new();
    let first = bm.append(b"first", vec![1; 1500]).unwrap();
    let second = bm.append(b"second", vec![2; 1600]).unwrap();

    assert!(!bm.rollback_append(&first));
    assert!(bm.rollback_append(&second));
    assert_eq!(
        bm.read(&first).map(|value| (value.len(), value[0])),
        Some((1500, 1))
    );
    assert!(bm.read(&second).is_none());
    assert!(!bm.rollback_append(&second));

    assert!(bm.rollback_append(&first));
    assert_eq!(bm.file_count(), 0);
}

#[test]
fn test_blob_projected_size_matches_serialized_image() {
    let mut bm = BlobManager::new();
    assert_eq!(bm.serialized_size(), Some(bm.to_bytes().len() as u64));

    let projected_append = bm.projected_serialized_size(None, Some(1_500)).unwrap();
    let pointer = bm.append(b"key", vec![1; 1_500]).unwrap();
    assert_eq!(projected_append, bm.to_bytes().len() as u64);
    assert_eq!(bm.serialized_size(), Some(projected_append));

    let projected_delete = bm.projected_serialized_size(Some(&pointer), None).unwrap();
    assert!(bm.mark_deleted(&pointer));
    assert_eq!(projected_delete, bm.to_bytes().len() as u64);
    assert_eq!(bm.serialized_size(), Some(projected_delete));
}

#[test]
fn test_segmented_append_rolls_over_at_target_and_accounts_for_header() {
    let mut manager = BlobManager::with_threshold_and_mode_and_segment_size(1024, true, 64);
    let first = manager.append(b"first", vec![1; 40]).unwrap();
    manager.capture_persisted_state();
    let projected = manager
        .projected_segment_write_size(None, Some(40))
        .expect("segmented projection should fit");

    let second = manager.append(b"second", vec![2; 40]).unwrap();
    assert_eq!(first.file_id, 1);
    assert_eq!(first.offset, 0);
    assert_eq!(second.file_id, 2);
    assert_eq!(second.offset, 0);
    assert_eq!(manager.segment_file_ids(), vec![1, 2]);
    assert_eq!(projected, manager.segment_write_size().unwrap());
}

#[test]
fn test_blob_roundtrip_preserves_deletion_metadata() {
    let mut bm = BlobManager::with_threshold(2048);
    let ptr = bm.append(b"key", vec![1; 1500]).unwrap();
    assert!(bm.mark_deleted(&ptr));

    let restored = BlobManager::from_bytes(&bm.to_bytes()).unwrap();
    assert_eq!(restored.threshold(), 2048);
    assert_eq!(restored.files_needing_gc(), vec![ptr.file_id]);

    let mut restored = restored;
    assert_eq!(restored.gc(), 1);
    assert_eq!(restored.file_count(), 0);
}

#[test]
fn test_blob_manager_accepts_legacy_format() {
    let mut file = BlobFile::new(1);
    file.append([0; 8], vec![1; 1500]);
    let file_data = file.to_bytes();
    let mut legacy = Vec::new();
    legacy.extend_from_slice(&1u32.to_le_bytes());
    legacy.extend_from_slice(&1u32.to_le_bytes());
    legacy.extend_from_slice(&(file_data.len() as u32).to_le_bytes());
    legacy.extend_from_slice(&file_data);

    let restored = BlobManager::from_bytes(&legacy).unwrap();
    assert_eq!(restored.file_count(), 1);
    assert_eq!(restored.total_valid_entries(), 1);
}

#[test]
fn test_segment_catalog_roundtrip_ignores_unpublished_suffix() {
    let mut manager = BlobManager::with_threshold_and_mode(1024, true);
    let pointer = manager.append(b"key", vec![7; 1500]).unwrap();
    manager.set_generation(9);
    let catalog = manager.to_segment_catalog_bytes();
    let mut segment = manager.segment_bytes(pointer.file_id).unwrap();
    segment.extend_from_slice(b"unpublished suffix");
    let segments = HashMap::from([(pointer.file_id, segment)]);

    let restored =
        BlobManager::from_segment_catalog_with_delta_log(&catalog, &segments, &[], None).unwrap();
    assert!(restored.is_segmented());
    assert_eq!(restored.generation_id(), 9);
    assert_eq!(restored.read(&pointer), Some(&vec![7; 1500][..]));
    assert_eq!(restored.persisted_segment_length(pointer.file_id), 1516);
}

#[test]
fn test_segment_catalog_delta_roundtrip_and_torn_suffix() {
    let mut manager = BlobManager::with_threshold_and_mode(1024, true);
    let first = manager.append(b"first", vec![1; 1500]).unwrap();
    manager.set_generation(1);
    let anchor = manager.to_segment_catalog_bytes();
    manager.capture_persisted_state();

    let second = manager.append(b"second", vec![2; 1500]).unwrap();
    manager.set_generation(2);
    let delta = manager.to_segment_catalog_delta_bytes().unwrap();
    let segments = HashMap::from([(first.file_id, manager.segment_bytes(first.file_id).unwrap())]);
    let restored =
        BlobManager::from_segment_catalog_with_delta_log(&anchor, &segments, &delta, Some(2))
            .unwrap();
    assert_eq!(restored.generation_id(), 2);
    assert_eq!(restored.read(&first), Some(&vec![1; 1500][..]));
    assert_eq!(restored.read(&second), Some(&vec![2; 1500][..]));

    let mut torn = delta;
    torn.pop();
    let old = BlobManager::from_segment_catalog_with_delta_log(&anchor, &segments, &torn, Some(1))
        .unwrap();
    assert_eq!(old.generation_id(), 1);
    assert!(
        BlobManager::from_segment_catalog_with_delta_log(&anchor, &segments, &torn, Some(2),)
            .is_none()
    );
}

#[test]
fn test_segment_catalog_replay_bounds_chain_and_skips_future_entry_counts() {
    let mut manager = BlobManager::with_threshold_and_mode(1024, true);
    let first = manager.append(b"first", vec![1; 1500]).unwrap();
    manager.set_generation(1);
    let anchor = manager.to_segment_catalog_bytes();
    manager.capture_persisted_state();

    let second = manager.append(b"second", vec![2; 1500]).unwrap();
    manager.set_generation(2);
    let first_delta = manager.to_segment_catalog_delta_bytes().unwrap();
    manager.capture_persisted_state();

    // This frame is after the manifest-selected generation. Its entry count
    // is intentionally hostile; replay must stop at the future generation
    // before allocating the declared vector.
    manager.set_generation(3);
    let mut future_delta = manager.to_segment_catalog_delta_bytes().unwrap();
    future_delta[36..40].copy_from_slice(&u32::MAX.to_le_bytes());
    let checksum = crc32c::crc32c(&future_delta[..future_delta.len() - 4]);
    let checksum_start = future_delta.len() - 4;
    future_delta[checksum_start..].copy_from_slice(&checksum.to_le_bytes());

    let segments = HashMap::from([(first.file_id, manager.segment_bytes(first.file_id).unwrap())]);
    let mut delta_log = first_delta.clone();
    delta_log.extend_from_slice(&future_delta);
    let restored =
        BlobManager::from_segment_catalog_with_delta_log(&anchor, &segments, &delta_log, Some(2))
            .unwrap();
    assert_eq!(restored.generation_id(), 2);
    assert_eq!(restored.read(&second), Some(&vec![2; 1500][..]));

    let mut long_chain = first_delta;
    for generation in 3..=(2 + super::segment_catalog_format::MAX_SEGMENT_CATALOG_DELTAS as u64) {
        manager.set_generation(generation);
        manager
            .append(format!("key-{generation}").as_bytes(), vec![3; 1500])
            .unwrap();
        let delta = manager.to_segment_catalog_delta_bytes().unwrap();
        long_chain.extend_from_slice(&delta);
        manager.capture_persisted_state();
    }
    let segments = manager
        .segment_file_ids()
        .into_iter()
        .map(|file_id| (file_id, manager.segment_bytes(file_id).unwrap()))
        .collect();
    assert!(
        BlobManager::from_segment_catalog_with_delta_log(
            &anchor,
            &segments,
            &long_chain,
            Some(2 + super::segment_catalog_format::MAX_SEGMENT_CATALOG_DELTAS as u64 - 1),
        )
        .is_some()
    );
    assert!(
        BlobManager::from_segment_catalog_with_delta_log(
            &anchor,
            &segments,
            &long_chain,
            Some(2 + super::segment_catalog_format::MAX_SEGMENT_CATALOG_DELTAS as u64),
        )
        .is_none()
    );
}

#[test]
fn test_blob_manager_rejects_future_format_and_duplicate_ids() {
    let mut manager = BlobManager::new();
    manager.append(b"key", vec![1; 1500]).unwrap();
    let mut future = manager.to_bytes();
    future[8..12].copy_from_slice(&2u32.to_le_bytes());
    assert!(BlobManager::from_bytes(&future).is_none());

    manager.files.push(Arc::new(BlobFile::new(2)));
    let mut duplicate = manager.to_bytes();
    // Header is 32 bytes; skip the first file descriptor and its data.
    let second_file_id = 32 + 4 + 8 + manager.files[0].to_bytes().len() + 4;
    duplicate[second_file_id..second_file_id + 4]
        .copy_from_slice(&manager.files[0].file_id().to_le_bytes());
    assert!(BlobManager::from_bytes(&duplicate).is_none());
}

#[test]
fn test_blob_key_prefix() {
    let prefix = BlobManager::make_key_prefix(b"hello");
    assert_eq!(&prefix, b"hello\0\0\0");

    let prefix = BlobManager::make_key_prefix(b"hello_world!");
    assert_eq!(&prefix, b"hello_wo");
}
