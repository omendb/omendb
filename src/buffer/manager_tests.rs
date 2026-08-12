use super::*;

#[test]
fn test_buffer_manager_new() {
    let bm = BufferManager::new(4096 * 10); // 10 frames
    assert_eq!(bm.capacity(), 10);
    assert_eq!(bm.stats().total_frames, 10);
    assert_eq!(bm.stats().free_frames, 10);
}

#[test]
fn test_fetch_and_hit() {
    let mut bm = BufferManager::new(4096 * 2);
    let data = [42u8; PAGE_SIZE];

    // First fetch - cache miss.
    let guard = bm.fetch(1, &data, GuardAccess::Read).unwrap();
    assert_eq!(bm.stats().reads, 1);
    assert_eq!(bm.stats().hits, 0);
    drop(guard);

    // Second fetch - cache hit.
    let guard = bm.fetch(1, &data, GuardAccess::Read).unwrap();
    assert_eq!(bm.stats().reads, 1);
    assert_eq!(bm.stats().hits, 1);
    drop(guard);
}

#[test]
fn test_physical_version_is_part_of_cache_identity() {
    let mut bm = BufferManager::new(PAGE_SIZE * 2);
    let old = [1u8; PAGE_SIZE];
    let new = [2u8; PAGE_SIZE];

    let old_key = PageCacheKey::new(7, 11);
    let new_key = PageCacheKey::new(7, 12);
    let old_guard = bm.fetch_key(old_key, &old, GuardAccess::Read).unwrap();
    assert_eq!(bm.stats().reads, 1);
    drop(old_guard);

    let new_guard = bm.fetch_key(new_key, &new, GuardAccess::Read).unwrap();
    assert_eq!(bm.stats().reads, 2);
    assert_eq!(bm.stats().hits, 0);
    assert_eq!(bm.frame_data(&new_guard), &new);
    assert!(bm.is_resident_key(old_key));
    assert!(bm.is_resident_key(new_key));
    drop(new_guard);
}

#[test]
fn test_eviction() {
    let mut bm = BufferManager::new(4096 * 2); // Only 2 frames
    let data1 = [1u8; PAGE_SIZE];
    let data2 = [2u8; PAGE_SIZE];
    let data3 = [3u8; PAGE_SIZE];

    // Fill the buffer.
    let g1 = bm.fetch(1, &data1, GuardAccess::Read).unwrap();
    let g2 = bm.fetch(2, &data2, GuardAccess::Read).unwrap();
    assert_eq!(bm.stats().free_frames, 0);

    // Dropping guards allows eviction.
    drop(g1);
    drop(g2);

    // Fetch a new page - should evict one.
    let g3 = bm.fetch(3, &data3, GuardAccess::Read).unwrap();
    assert_eq!(bm.stats().reads, 3);
    assert_eq!(bm.stats().eviction_attempts, 1);
    assert_eq!(bm.stats().evictions, 1);
    drop(g3);
}

#[test]
fn test_dirty_page() {
    let mut bm = BufferManager::new(4096);
    let data = [0u8; PAGE_SIZE];

    let guard = bm.fetch(1, &data, GuardAccess::Write).unwrap();
    bm.mark_dirty(guard.page_id());
    drop(guard);
    let writeback = bm.begin_writeback(1).unwrap().unwrap();
    assert_eq!(writeback.data(), &data);
    bm.complete_writeback(writeback).unwrap();
    let stats = bm.stats();
    assert_eq!(stats.dirty_frames, 0);
    assert_eq!(stats.writeback_requests, 1);
    assert_eq!(stats.writeback_refusals, 0);
}

#[test]
fn test_writeback_all() {
    let mut bm = BufferManager::new(4096 * 3);
    let data = [0u8; PAGE_SIZE];

    let g1 = bm.fetch(1, &data, GuardAccess::Write).unwrap();
    let g2 = bm.fetch(2, &data, GuardAccess::Write).unwrap();
    bm.mark_dirty(g1.page_id());
    bm.mark_dirty(g2.page_id());
    drop(g1);
    drop(g2);

    let writebacks = bm.begin_writeback_all().unwrap();
    assert_eq!(writebacks.len(), 2);
    for writeback in writebacks {
        bm.complete_writeback(writeback).unwrap();
    }
    assert_eq!(bm.stats().dirty_frames, 0);
}

#[test]
fn test_guard_drop_releases_pin() {
    let mut bm = BufferManager::new(PAGE_SIZE);
    let data = [1u8; PAGE_SIZE];

    let guard = bm.fetch(1, &data, GuardAccess::Read).unwrap();
    assert_eq!(bm.stats().pinned_frames, 1);
    drop(guard);
    assert_eq!(bm.stats().pinned_frames, 0);

    let guard = bm.fetch(2, &data, GuardAccess::Read).unwrap();
    drop(guard);
}

#[test]
fn test_fetch_refuses_pinned_pool() {
    let mut bm = BufferManager::new(PAGE_SIZE);
    let data = [0u8; PAGE_SIZE];
    let guard = bm.fetch(1, &data, GuardAccess::Read).unwrap();

    assert!(matches!(
        bm.fetch(2, &data, GuardAccess::Read),
        Err(BufferError::AllFramesPinned)
    ));
    assert_eq!(bm.stats().eviction_attempts, 1);
    assert_eq!(bm.stats().eviction_refusals, 1);
    drop(guard);
}

#[test]
fn test_fetch_refuses_dirty_eviction_until_flush() {
    let mut bm = BufferManager::new(PAGE_SIZE);
    let data = [0u8; PAGE_SIZE];
    let guard = bm.fetch(1, &data, GuardAccess::Write).unwrap();
    drop(guard);

    assert!(matches!(
        bm.fetch(2, &data, GuardAccess::Read),
        Err(BufferError::DirtyPage(1))
    ));
    assert_eq!(bm.stats().eviction_attempts, 1);
    assert_eq!(bm.stats().eviction_refusals, 1);

    let writeback = bm.begin_writeback(1).unwrap().unwrap();
    bm.complete_writeback(writeback).unwrap();
    let guard = bm.fetch(2, &data, GuardAccess::Read).unwrap();
    drop(guard);
}

#[test]
fn test_writeback_refuses_live_guard_and_preserves_dirty_state() {
    let mut bm = BufferManager::new(PAGE_SIZE);
    let data = [0u8; PAGE_SIZE];
    let guard = bm.fetch(1, &data, GuardAccess::Write).unwrap();

    assert!(matches!(
        bm.begin_writeback(1),
        Err(BufferError::PinnedPage(1))
    ));
    assert_eq!(bm.stats().writeback_requests, 1);
    assert_eq!(bm.stats().writeback_refusals, 1);
    drop(guard);

    let writeback = bm.begin_writeback(1).unwrap().unwrap();
    drop(writeback);
    assert_eq!(bm.stats().dirty_frames, 1);
    assert!(matches!(
        bm.fetch(2, &data, GuardAccess::Read),
        Err(BufferError::DirtyPage(1))
    ));
}

#[test]
fn test_stale_writeback_cannot_clean_newer_frame_image() {
    let mut bm = BufferManager::new(PAGE_SIZE * 2);
    let data = [0u8; PAGE_SIZE];
    let guard = bm.fetch(1, &data, GuardAccess::Write).unwrap();
    drop(guard);
    let writeback = bm.begin_writeback(1).unwrap().unwrap();

    let guard = bm.fetch(1, &data, GuardAccess::Write).unwrap();
    bm.frame_data_mut(&guard).unwrap()[0] = 1;
    drop(guard);

    assert!(matches!(
        bm.complete_writeback(writeback),
        Err(BufferError::StaleWriteback(1))
    ));
    let stats = bm.stats();
    assert_eq!(stats.dirty_frames, 1);
    assert_eq!(stats.writeback_requests, 1);
    assert_eq!(stats.writeback_refusals, 1);
}

#[test]
fn test_read_guard_cannot_mutate_frame() {
    let mut bm = BufferManager::new(PAGE_SIZE);
    let data = [7u8; PAGE_SIZE];
    let guard = bm.fetch(1, &data, GuardAccess::Read).unwrap();

    assert!(matches!(
        bm.frame_data_mut(&guard),
        Err(BufferError::ReadOnlyPage(1))
    ));
    assert_eq!(bm.frame_data(&guard), &data);
    drop(guard);
}
