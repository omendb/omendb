# OmenDB

OmenDB is a server-first relational database system in development, written in Rust and built on [SeerDB](crates/seerdb). It targets PostgreSQL-class single-node OLTP with PostgreSQL ecosystem compatibility at deliberate external boundaries. A direct embedded Rust API remains useful for integration and testing, but it is a secondary deployment surface—not the product boundary.

> **Developer preview:** This tree is targeting a relational `0.1.0-alpha.1` release, but it is not alpha-ready yet. APIs, persistence formats, supported platforms, and performance are subject to change. PostgreSQL wire support is experimental and feature-gated. See [`docs/architecture.md`](docs/architecture.md) for the server-first design and [`docs/alpha-release-gates.md`](docs/alpha-release-gates.md) for the release contract.

## Quick start

OmenDB is not currently published to crates.io. Use it as a Git dependency or clone this repository. The direct Rust API is currently the smallest working surface, and this example uses the temporary/reference backend:

```rust
use std::path::PathBuf;

use omendb::{
    DatabaseConfig, RelationalBackendConfig, RelationalDatabase,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = RelationalDatabase::create(RelationalBackendConfig::Temporary(
        DatabaseConfig {
            directory: PathBuf::from("./omendb-data"),
        },
    ))?;

    database.execute_sql(
        "CREATE TABLE accounts (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    )?;
    database.execute_sql(
        "INSERT INTO accounts VALUES (1, 'Alice'), (2, 'Bob')",
    )?;

    let result = database.execute_sql("SELECT id, name FROM accounts ORDER BY id")?;
    println!("{:?}", result.rows);
    database.close()?;
    Ok(())
}
```

The target directory must not already exist when creating a database. Use
`RelationalBackendConfig::Seer(SeerKernelConfig::new(path))` to select the
persistent SeerDB-backed storage engine. The temporary backend remains a
reference and compatibility path while the server-first storage transaction
architecture is being rebuilt.

The SQL layer is deliberately bounded rather than PostgreSQL-compatible. It
supports the tested subset of schema changes, `SELECT`, `INSERT`, `UPDATE`,
`DELETE`, parameters, joins, aggregates, indexes, and constraints. Unsupported
syntax returns `DbError::SqlUnsupported`; use the typed API when you need
control over transaction, snapshot, or backend behavior.

## Deployment roadmap

The product roadmap is server-first. The planned standalone OmenDB daemon will
provide multi-client concurrency, connection/session lifecycle,
authentication and authorization, configuration, observability, backups, and
operational upgrade procedures in front of the same relational and SeerDB
transaction foundations. The current tree still exposes those foundations
primarily through a direct Rust API; the exploratory wire example below is not
yet the alpha server.

The long-term goal is a competitive OmenDB server for general OLTP workloads,
with PostgreSQL-class deployment capabilities and PostgreSQL ecosystem
compatibility where it is useful. SeerDB is an independent Apache-2.0 Rust
crate developed in this monorepo and published on its own version line. OmenDB
owns SQL, schema, row/index encoding, and relational semantics; SeerDB owns
generic ordered-KV storage and transaction durability. Alternate storage
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

## PostgreSQL wire example

Run the exploratory wire server with:

```bash
cargo run --features pgwire --example pgwire_serve -- 5432
```

It starts a throwaway database on `127.0.0.1`, seeds a `users` table, and
accepts trust authentication by default. Set `PGWIRE_USER` and
`PGWIRE_PASSWORD` to provision a SCRAM user instead. The wire server is for
compatibility checks, not a claim of PostgreSQL protocol or SQL completeness.

## Development

```bash
cargo fmt --all -- --check
cargo test --all-features --all-targets
cargo clippy --all-features --all-targets -- -D warnings
```

SeerDB is an independently versioned Apache-2.0 crate under `crates/seerdb/`; OmenDB remains AGPL-3.0-only. See [`docs/runbook.md`](docs/runbook.md) for operational guidance (health checks, verification, maintenance, and recovery), [`SECURITY.md`](SECURITY.md) for vulnerability reports and [`LICENSE`](LICENSE) for OmenDB licensing terms.
