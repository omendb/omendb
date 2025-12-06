//! Quick benchmark for version comparison (~5s total runtime)
//!
//! Run with: cargo run --example quick_bench --release

use std::hint::black_box;
use std::time::{Duration, Instant};

const ITERATIONS: u64 = 1_000_000;
const WARMUP_ITERATIONS: u64 = 10_000;

fn main() {
    println!("seerdb quick benchmark");
    println!("======================\n");

    // Key comparison benchmark (uses SIMD internally)
    bench_key_comparison();

    // Varint decoding benchmark
    bench_varint_decode();

    // Shared prefix benchmark
    bench_shared_prefix();

    println!("\nDone!");
}

fn bench_key_comparison() {
    use seerdb::simd::compare_keys;

    let key_a = b"user:12345:profile:settings";
    let key_b = b"user:12345:profile:settings";
    let key_c = b"user:12345:profile:settingz";
    let key_long_a = b"this_is_a_very_long_key_that_exceeds_32_bytes_for_avx2_testing";
    let key_long_b = b"this_is_a_very_long_key_that_exceeds_32_bytes_for_avx2_testinz";

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        black_box(compare_keys(key_a, key_b));
    }

    // Equal keys (short)
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(compare_keys(key_a, key_b));
    }
    let elapsed = start.elapsed();
    print_result("compare_keys (equal, 28B)", elapsed);

    // Different keys (short)
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(compare_keys(key_a, key_c));
    }
    let elapsed = start.elapsed();
    print_result("compare_keys (diff, 28B)", elapsed);

    // Long keys (AVX2 path)
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(compare_keys(key_long_a, key_long_b));
    }
    let elapsed = start.elapsed();
    print_result("compare_keys (diff, 64B)", elapsed);
}

fn bench_varint_decode() {
    use seerdb::simd::decode_varint;

    let mut buf = [0u8; 32];

    // 1-byte varint
    buf[0] = 0x05;
    for _ in 0..WARMUP_ITERATIONS {
        black_box(decode_varint(&buf));
    }
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(decode_varint(&buf));
    }
    print_result("decode_varint (1-byte)", start.elapsed());

    // 2-byte varint
    buf[0] = 0x85;
    buf[1] = 0x01;
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(decode_varint(&buf));
    }
    print_result("decode_varint (2-byte)", start.elapsed());
}

fn bench_shared_prefix() {
    use seerdb::simd::shared_prefix_len;

    let key_a = b"user:12345:profile:settings:theme";
    let key_b = b"user:12345:profile:settings:color";
    let key_long_a = b"prefix_that_is_shared_for_many_bytes_then_differs_here";
    let key_long_b = b"prefix_that_is_shared_for_many_bytes_then_differs_nope";

    for _ in 0..WARMUP_ITERATIONS {
        black_box(shared_prefix_len(key_a, key_b));
    }

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(shared_prefix_len(key_a, key_b));
    }
    print_result("shared_prefix (27 match)", start.elapsed());

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(shared_prefix_len(key_long_a, key_long_b));
    }
    print_result("shared_prefix (50 match)", start.elapsed());
}

fn print_result(name: &str, elapsed: Duration) {
    let ns_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    let ops_per_sec = (ITERATIONS as f64 / elapsed.as_secs_f64()) / 1_000_000.0;
    println!(
        "{:30} {:>8.2} ns/op  ({:.1} M ops/s)",
        name, ns_per_op, ops_per_sec
    );
}
