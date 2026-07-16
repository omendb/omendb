# SeerDB fuzz targets

These targets are intentionally bounded around parser and logical-state
boundaries. Run them with `cargo-fuzz` and a nightly toolchain so libFuzzer
coverage instrumentation is enabled:

```bash
cargo install cargo-fuzz
cargo +nightly fuzz run fuzz_page_parsing -- -max_total_time=300
cargo +nightly fuzz run fuzz_wal_parsing -- -max_total_time=300
cargo +nightly fuzz run fuzz_blob_parsing -- -max_total_time=300
cargo +nightly fuzz run fuzz_format_parsing -- -max_total_time=300
cargo +nightly fuzz run fuzz_btree_operations -- -max_total_time=300
```

Crash-recovery fuzzing is intentionally separate: it needs a deterministic
fault harness and durable corpus contract before it is added here.
