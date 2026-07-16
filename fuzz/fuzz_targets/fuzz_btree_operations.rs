#![no_main]

use libfuzzer_sys::fuzz_target;
use seerdb::btree::BTree;

// Exercise the logical mutation path with a bounded, byte-encoded command
// stream. Fuzzing stays focused on state transitions rather than allowing a
// single input to become an unbounded benchmark.
fuzz_target!(|data: &[u8]| {
    let mut tree = BTree::new();
    for command in data.chunks(8).take(256) {
        if command.len() < 3 {
            break;
        }
        let key_len = usize::from(command[1] % 32).min(command.len() - 2);
        let key = &command[2..2 + key_len];
        let value = &command[2 + key_len..];
        match command[0] % 4 {
            0 => {
                let _ = tree.upsert(key, value);
            }
            1 => {
                let _ = tree.delete(key);
            }
            2 => {
                let _ = tree.lookup(key);
            }
            _ => {
                let end = key.iter().copied().chain([0xff]).collect::<Vec<_>>();
                if let Ok(mut scan) = tree.range_scan(key, &end) {
                    let _ = scan.next();
                }
            }
        }
    }
});
