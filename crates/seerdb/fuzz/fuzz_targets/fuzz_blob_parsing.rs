#![no_main]

use libfuzzer_sys::fuzz_target;
use seerdb::blob::BlobManager;

fuzz_target!(|data: &[u8]| {
    let _ = BlobManager::from_bytes(data);
});
