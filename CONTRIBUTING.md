# Contributing to seerdb

## Getting Started

```bash
git clone https://github.com/omendb/seerdb.git
cd seerdb
rustup override set nightly
cargo build
cargo test --lib
```

## Development Workflow

1. Fork and create a feature branch
2. Make changes with tests
3. Run checks:
   ```bash
   cargo fmt
   cargo clippy --lib
   cargo test --lib
   ```
4. Submit a pull request

## Code Style

- Run `cargo fmt` before committing
- Fix all `cargo clippy` warnings
- Add tests for new functionality
- Keep commits focused and atomic

## Testing

```bash
cargo test --lib                    # Fast unit tests (213 tests)
cargo test                          # Full test suite
cargo test --features failpoints    # Crash injection tests
```

## Benchmarks

```bash
cargo bench --bench ycsb_benchmark
cargo bench --bench mixed_workload
```

## Areas for Contribution

- Performance improvements
- Additional tests (especially edge cases)
- Documentation improvements
- Bug fixes

## Questions?

Open an issue for discussion before large changes.
