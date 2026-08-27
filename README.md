# OmenDB

OmenDB is a server-first relational database system in development, written in Rust and built on [SeerDB](crates/seerdb). It targets PostgreSQL-class single-node OLTP with PostgreSQL ecosystem compatibility at deliberate external boundaries. A direct embedded Rust API remains useful for integration and testing, but it is a secondary deployment surface—not the product boundary.

> **Developer preview:** The relational `0.1.0-alpha.*` line is reserved but not alpha-ready. APIs, persistence formats, supported platforms, and performance are subject to change. The current direct API is a qualification surface; a releasable alpha must satisfy the server-first storage and server gates. PostgreSQL wire support is experimental and feature-gated. See [`docs/architecture.md`](docs/architecture.md) for the design and [`docs/alpha-release-gates.md`](docs/alpha-release-gates.md) for the release contract.

## Quick start

OmenDB is not currently published to crates.io. Use it as a Git dependency or clone this repository. The direct Rust API is currently the smallest working surface, and this example opens the persistent SeerDB-backed engine:

```rust
use omendb::{RelationalBackendConfig, RelationalDatabase};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new("./omendb-data"))?;
    database.execute_sql(
        "CREATE TABLE accounts (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    )?;
    database.execute_sql("INSERT INTO accounts VALUES (1, 'Alice'), (2, 'Bob')")?;
    let result = database.execute_sql("SELECT id, name FROM accounts ORDER BY id")?;
    println!("{:?}", result.rows);
    database.close()?;
    Ok(())
}
```

The target directory must not already exist when creating a database.
`RelationalBackendConfig::new(path)` selects the persistent SeerDB-backed
storage engine — the only backend; SeerDB owns durability end to end.

The SQL layer is deliberately bounded rather than PostgreSQL-compatible. It
supports the tested subset of schema changes, `SELECT`, `INSERT`, `UPDATE`,
`DELETE`, parameters, joins, aggregates, indexes, and constraints. Unsupported
syntax returns `DbError::SqlUnsupported`; use the typed API when you need
control over transaction, snapshot, or backend behavior.

## Deployment roadmap

The roadmap is server-first, with SeerDB and OmenDB advancing as one vertical
slice rather than as isolated rewrites:

1. **SeerDB foundation:** the transaction, MVCC, cursor, change-position, and
   group-commit slices are implemented; page-level multi-writer installation
   remains open.
2. **OmenDB integration:** complete. The direct SeerDB store owns the catalog,
   tables, indexes, and relational transactions.
3. **Server alpha:** in progress. The persistent daemon now supports multiple
   wire sessions, bounded connection admission, authentication policy,
   diagnostics, and clean shutdown/reopen behavior.
4. **Measured acceleration:** add serializable certification, replication/CDC,
   typed OLTP micro-plans, batch execution, and benchmark-led page, buffer,
   WAL, and runtime optimizations.

The current direct Rust API and exploratory wire example are development
surfaces, not the target alpha product. SeerDB is an independent Apache-2.0
Rust crate developed in this monorepo and published on its own version line.
OmenDB owns SQL, schema, row/index encoding, and relational semantics; SeerDB
owns generic ordered-KV storage and transaction durability. Alternate storage
engines are an integration concern, not a first-party engine matrix.

Each direct SQL write is one transaction. Use `execute_sql_batch` (or its
parameterized variant) or a typed transaction with
`RelationalDatabaseTransaction::execute_sql_with_params` when
several statements should share one atomic and durable publication. The
reproducible baseline is available with:

```bash
cargo run --release --example alpha_oltp -- \
  --backend all --rows 512 --operations 1000 --batch-size 1
```

The baseline reports workload metadata and latency but makes no competitive
performance claim; `--batch-size` measures the explicit transaction trade-off.

## PostgreSQL wire server

Run the persistent daemon with:

```bash
cargo run --features pgwire --bin omendbd -- \
  --path ./omendb-data --bind 127.0.0.1:5432
```

The daemon opens or creates the database, accepts multiple PostgreSQL wire
sessions, and closes the durable handle on Ctrl-C. Empty authentication
catalogs use trust mode on loopback only. Provision a SCRAM user through
`pgwire_server::provision_wire_user` before starting the daemon; once a user
exists, startup requires SCRAM authentication. Connection admission is
bounded with `--max-connections`.

The supported SQL and wire surface is deliberate and bounded, not a claim of
PostgreSQL compatibility. The throwaway seeded example remains available for
client experiments:

```bash
cargo run --features pgwire --example pgwire_serve -- 5432
```

## Development

```bash
cargo fmt --all -- --check
cargo test --all-features --all-targets
cargo clippy --all-features --all-targets -- -D warnings
```

SeerDB is an independently versioned Apache-2.0 crate under `crates/seerdb/`; OmenDB remains AGPL-3.0-only. See [`docs/runbook.md`](docs/runbook.md) for operational guidance (health checks, verification, maintenance, and recovery), [`SECURITY.md`](SECURITY.md) for vulnerability reports and [`LICENSE`](LICENSE) for OmenDB licensing terms.
