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
cargo +nightly fuzz run fuzz_crash_recovery -- -max_total_time=300
```

`fuzz_crash_recovery` drives bounded atomic mutations through the deterministic
publication seams, reopens after each injected failure, and accepts only the
old complete generation or the complete new generation. It is a
process-local crash/reopen model, not a substitute for SIGKILL campaigns or
filesystem fault injection; those remain separate evidence gates.
