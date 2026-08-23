# OmenDB

> **Developer preview.** OmenDB is a work-in-progress embedded relational
> database built around SeerDB. APIs, persistence formats, supported platforms,
> and performance are subject to change.

This repository contains the OmenDB relational layer: typed rows, transactions,
catalog and index management, bounded embedded SQL, sessions, and PostgreSQL
wire work in progress. SeerDB remains the separate physical storage project.

The repository is intentionally small and does not include the private design
notes, research notebooks, task state, or development history used during
development.

## Build and test

```bash
cargo test --all-features --all-targets
cargo clippy --all-features --all-targets -- -D warnings
```

The current release is not published to crates.io or PyPI. The project is
licensed under AGPL-3.0-only.
