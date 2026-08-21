//! Engine-level reproduction: scattered churn free-list behavior.

use super::*;
use crate::btree::{BTree, PAGE_SIZE};
use crate::space::DeviceOptions;
use tempfile::tempdir;

#[test]
fn engine_free_list_tracks_scattered_churn() {
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
        BufferManager::new(PAGE_SIZE * 64),
        PMT::new(),
        PageAllocator::new(),
        device,
    );

    let value = vec![0xABu8; 128];
    let key = |index: usize| format!("soak-key-{index:08}").into_bytes();

    // Seed 512 keys.
    for chunk in 0..32 {
        for slot in 0..16 {
            let index = chunk * 16 + slot;
            engine.btree_mut().insert(&key(index), &value).unwrap();
        }
        engine.flush().unwrap();
        engine.complete_generation();
    }

    let size_after_seed = engine.device.size().unwrap();
    eprintln!(
        "seed: nodes={} next_offset={} free={} size={}",
        engine.btree().node_count(),
        engine.next_offset,
        engine.free_offsets.len(),
        size_after_seed
    );

    // Scattered churn: golden-ratio scatter, even rounds put, odd delete.
    for round in 0..60 {
        for slot in 0..16 {
            let index = ((round as u64 * 16 + slot as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
                % 512) as usize;
            if round % 2 == 0 {
                engine.btree_mut().upsert(&key(index), &value).unwrap();
            } else {
                engine.btree_mut().delete(&key(index)).unwrap();
            }
        }
        engine.flush().unwrap();
        engine.complete_generation();
        if (round + 1) % 10 == 0 {
            eprintln!(
                "round {}: nodes={} next_offset={} free={} pending={} size={}",
                round + 1,
                engine.btree().node_count(),
                engine.next_offset,
                engine.free_offsets.len(),
                engine.pending_reclaimed_offsets.len(),
                engine.device.size().unwrap()
            );
        }
    }

    assert!(
        engine.btree().node_count() <= 40,
        "node count grew without merges: {}",
        engine.btree().node_count()
    );
}
