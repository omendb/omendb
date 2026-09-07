# durable-fs

Class-selected durability primitives shared by the OmenDB-family engines
([omendb](https://github.com/omendb/omendb) / seerdb,
[omendb-olap](https://github.com/omendb/omendb-olap),
[omendb-vector](https://github.com/omendb/omendb-vector)).

Every engine that writes files to local storage needs the same four
things, and each had re-implemented them by hand: a decision about what
"sync" means on this platform, a directory fsync (with the
create-multiple-ancestors subtlety), and an atomic
tmp→fsync→rename→dir-fsync publication. This crate is that layer, one
implementation, extracted from the seerdb engine where it was
battle-tested by the crash matrices.

## What it provides

| Primitive | Purpose |
|---|---|
| `SyncClass` | `DeviceBarrier` (macOS `F_FULLFSYNC`, ~4.2 ms, survives power loss on consumer SSDs) vs `KernelBarrier` (plain `fsync(2)`, ~0.03 ms, process/kernel crash only). Default: the stronger barrier. |
| `sync_file_data(file, class)` | Class-selected data sync. POSIX `fdatasync` explicitly covers the file-size metadata needed to read data back, so append-shaped WAL barriers are correct with either class. |
| `sync_file_all(file, class)` | Class-selected full sync. Publication paths, and any sync that must cover a `set_len`. |
| `fsync_dir(path)` | Make one directory's entry changes durable. Always the strongest barrier — directory syncs sit on publication paths, never per-append. |
| `fsync_dir_chain(path)` | Walk to the root, syncing every ancestor a `create_dir_all` may have created. |
| `atomic_write(path, data)` | Buffered atomic publication: `.tmp` sibling → sync → rename → parent dir fsync. Compose `sync_file_all` + `std::fs::rename` + `fsync_dir` for streaming payloads. |

## Why a class knob

On macOS, Rust std maps both `sync_data` and `sync_all` to
`F_FULLFSYNC` — a full device barrier that costs ~4.2 ms per call, ~100x
plain `fsync(2)` (~0.03 ms, measured 2026-09-06). PostgreSQL's installed
macOS class is the kernel barrier; matching its class took one engine's
measured commit latency from 13.3 ms to 223 µs. The knob exists so a
deployment can make that trade explicitly; the default never trades
correctness.

The two classes double as a durability *ladder*: "strict" (never lose an
acknowledged write) is `DeviceBarrier`; "normal" (process-crash-safe,
power-loss window — the SQLite `synchronous=NORMAL`-in-WAL analogy) is
`KernelBarrier`. Expose the ladder in your product's terms; this crate
keeps the syscall mapping in one place.

On Linux both classes coincide with ordinary fsync semantics, so the
knob is primarily a macOS affordance — but the *name* of the durability
class you are claiming should be explicit everywhere.

## Use

Developed in the OmenDB workspace as an independent Apache-2.0 crate.
SeerDB uses a workspace path dependency. Other repositories can select
`durable-fs` from `https://github.com/omendb/omendb` with a pinned Git revision.

```rust
use durable_fs::{SyncClass, sync_file_data};
```

Run `cargo test -p durable-fs` from the workspace root. The crate supports
Rust 1.88; consumers may require a newer compiler. Directory synchronization
always uses the strongest barrier. Consumers own buffering, policy, metrics,
and failure injection.
