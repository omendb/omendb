//! Deterministic logical workload and oracle support for the common-KV harness.
//!
//! The generated operations and in-memory oracle are the comparison authority.
//! Backend adapters execute this model but do not define its semantics.

use super::{Config, WorkloadKind};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub(super) enum Operation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Get { key: Vec<u8> },
    Range { start: Vec<u8>, end: Vec<u8> },
}

#[derive(Debug, Clone, Copy)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn index(&mut self, upper: usize) -> usize {
        (self.next() as usize) % upper
    }
}

fn key_for(index: usize) -> Vec<u8> {
    format!("k{index:016}").into_bytes()
}

fn value_for(index: usize, length: usize) -> Vec<u8> {
    (0..length)
        .map(|offset| b'a' + ((index.wrapping_add(offset)) % 26) as u8)
        .collect()
}

pub(super) fn initial_state(config: &Config) -> Vec<(Vec<u8>, Vec<u8>)> {
    if config.workload == WorkloadKind::BatchPut {
        Vec::new()
    } else {
        (0..config.keys)
            .map(|index| (key_for(index), value_for(index, config.value_bytes)))
            .collect()
    }
}

pub(super) fn generate_operations(config: &Config) -> Vec<Operation> {
    let mut rng = Rng::new(config.seed);
    let key_space = config.keys.saturating_mul(2).max(1);
    let total_operations = config.base_operations.saturating_add(config.operations);
    (0..total_operations)
        .map(|operation| match config.workload {
            WorkloadKind::BatchPut => {
                let index = rng.index(key_space);
                Operation::Put {
                    key: key_for(index),
                    value: value_for(config.keys.wrapping_add(operation), config.value_bytes),
                }
            }
            WorkloadKind::PointRead => Operation::Get {
                key: key_for(rng.index(config.keys.max(1))),
            },
            WorkloadKind::RangeRead => {
                let start_index = rng.index(config.keys.max(1));
                let end_index = start_index.saturating_add(config.range_width);
                Operation::Range {
                    start: key_for(start_index),
                    end: key_for(end_index),
                }
            }
            WorkloadKind::Mixed => {
                let index = rng.index(key_space);
                match rng.next() % 100 {
                    0..=54 => Operation::Get {
                        key: key_for(index),
                    },
                    55..=84 => Operation::Put {
                        key: key_for(index),
                        value: value_for(operation.wrapping_add(config.keys), config.value_bytes),
                    },
                    85..=94 => Operation::Delete {
                        key: key_for(index),
                    },
                    _ => {
                        let end_index = index.saturating_add(config.range_width);
                        Operation::Range {
                            start: key_for(index),
                            end: key_for(end_index),
                        }
                    }
                }
            }
        })
        .skip(config.base_operations)
        .collect()
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        result.push_str(&format!("{byte:02x}"));
    }
    result
}

pub(super) fn trace_digest(operations: &[Operation]) -> u64 {
    fn feed_byte(hash: &mut u64, byte: u8) {
        *hash ^= byte as u64;
        *hash = hash.wrapping_mul(0x100000001b3);
    }

    fn feed_bytes(hash: &mut u64, bytes: &[u8]) {
        for byte in (bytes.len() as u64).to_le_bytes() {
            feed_byte(hash, byte);
        }
        for &byte in bytes {
            feed_byte(hash, byte);
        }
    }

    let mut hash = 0xcbf29ce484222325u64;
    for operation in operations {
        match operation {
            Operation::Put { key, value } => {
                feed_byte(&mut hash, b'p');
                feed_bytes(&mut hash, key);
                feed_bytes(&mut hash, value);
            }
            Operation::Delete { key } => {
                feed_byte(&mut hash, b'd');
                feed_bytes(&mut hash, key);
            }
            Operation::Get { key } => {
                feed_byte(&mut hash, b'g');
                feed_bytes(&mut hash, key);
            }
            Operation::Range { start, end } => {
                feed_byte(&mut hash, b'r');
                feed_bytes(&mut hash, start);
                feed_bytes(&mut hash, end);
            }
        }
    }
    hash
}

pub(super) fn render_trace(config: &Config, operations: &[Operation]) -> String {
    let mut output = format!(
        "{{\n  \"format\": \"seerdb-common-kv-trace-v1\",\n  \"workload\": \"{}\",\n  \"durability\": \"{}\",\n  \"keys\": {},\n  \"measured_operations\": {},\n  \"base_operations\": {},\n  \"batch_size\": {},\n  \"value_bytes\": {},\n  \"range_width\": {},\n  \"seed\": {},\n  \"trace_operation_count\": {},\n  \"trace_digest_fnv1a64\": \"{:016x}\",\n  \"trace\": [\n",
        config.workload.name(),
        config.durability.name(),
        config.keys,
        config.operations,
        config.base_operations,
        config.batch_size,
        config.value_bytes,
        config.range_width,
        config.seed,
        operations.len(),
        trace_digest(operations),
    );

    for (index, operation) in operations.iter().enumerate() {
        if index > 0 {
            output.push_str(",\n");
        }
        match operation {
            Operation::Put { key, value } => output.push_str(&format!(
                "    {{\"op\": \"put\", \"key_hex\": \"{}\", \"value_hex\": \"{}\"}}",
                hex_bytes(key),
                hex_bytes(value),
            )),
            Operation::Delete { key } => output.push_str(&format!(
                "    {{\"op\": \"delete\", \"key_hex\": \"{}\"}}",
                hex_bytes(key),
            )),
            Operation::Get { key } => output.push_str(&format!(
                "    {{\"op\": \"get\", \"key_hex\": \"{}\"}}",
                hex_bytes(key),
            )),
            Operation::Range { start, end } => output.push_str(&format!(
                "    {{\"op\": \"range\", \"start_hex\": \"{}\", \"end_hex\": \"{}\"}}",
                hex_bytes(start),
                hex_bytes(end),
            )),
        }
    }
    output.push_str("\n  ]\n}");
    output
}

pub(super) fn apply_oracle(oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>, operation: &Operation) {
    match operation {
        Operation::Put { key, value } => {
            oracle.insert(key.clone(), value.clone());
        }
        Operation::Delete { key } => {
            oracle.remove(key);
        }
        Operation::Get { .. } | Operation::Range { .. } => {}
    }
}

pub(super) fn digest(entries: &[(Vec<u8>, Vec<u8>)]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for (key, value) in entries {
        for byte in (key.len() as u64)
            .to_le_bytes()
            .into_iter()
            .chain(key.iter().copied())
        {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        for byte in (value.len() as u64)
            .to_le_bytes()
            .into_iter()
            .chain(value.iter().copied())
        {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}
