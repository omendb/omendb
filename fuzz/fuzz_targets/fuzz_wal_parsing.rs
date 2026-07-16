#![no_main]

use libfuzzer_sys::fuzz_target;
use seerdb::recovery::WalManager;

fuzz_target!(|data: &[u8]| {
    let (records, _status) = WalManager::parse_records_with_status(data);
    for record in records.iter().take(256) {
        let encoded = record.to_bytes();
        let _ = seerdb::recovery::WalRecord::from_bytes(&encoded);
    }
});
