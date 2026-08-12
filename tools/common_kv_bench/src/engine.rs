//! Ordered byte-KV backend adapters used by the comparison harness.
//!
//! The harness owns the logical trace, oracle, timing, and artifact schema in
//! `main.rs`. This module owns only the backend lifecycle and the narrow
//! operations needed to execute that trace against each engine.

use super::{BenchResult, DurabilityMode, EngineKind, Operation, report::SeerCounters};
use std::path::Path;

#[cfg(feature = "fjall")]
pub(super) struct FjallEngine {
    db: fjall::Database,
    keyspace: fjall::Keyspace,
    durable: bool,
}

#[cfg(feature = "rocksdb")]
pub(super) struct RocksDbEngine {
    db: rocksdb::DB,
    durable: bool,
}

pub(super) enum Engine {
    SeerDb {
        db: Box<seerdb::DB>,
    },
    #[cfg(feature = "fjall")]
    Fjall(FjallEngine),
    #[cfg(feature = "rocksdb")]
    RocksDb(RocksDbEngine),
}

impl Engine {
    pub(super) fn create(
        kind: EngineKind,
        path: &Path,
        durability: DurabilityMode,
    ) -> BenchResult<Self> {
        match kind {
            EngineKind::SeerDb => {
                if durability != DurabilityMode::Durable {
                    return Err(
                        "SeerDB common-KV comparison supports durable mode only; buffered mode is not equivalent"
                            .into(),
                    );
                }
                let options = seerdb::Options {
                    // `DB::commit_batch` always forces the generation
                    // publication barrier. Enabling this option would add a
                    // sync per page and compare an extra durability policy.
                    sync_writes: false,
                    blob_threshold: usize::MAX,
                    ..seerdb::Options::default()
                };
                Ok(Self::SeerDb {
                    db: Box::new(seerdb::DB::create(path, options)?),
                })
            }
            EngineKind::Fjall => {
                #[cfg(feature = "fjall")]
                {
                    let db = fjall::Database::builder(path).open()?;
                    let keyspace = db.keyspace("default", fjall::KeyspaceCreateOptions::default)?;
                    Ok(Self::Fjall(FjallEngine {
                        db,
                        keyspace,
                        durable: durability.sync_writes(),
                    }))
                }
                #[cfg(not(feature = "fjall"))]
                {
                    Err("Fjall support is disabled; rebuild with --features fjall".into())
                }
            }
            EngineKind::RocksDb => {
                #[cfg(feature = "rocksdb")]
                {
                    let mut options = rocksdb::Options::default();
                    options.create_if_missing(true);
                    let db = rocksdb::DB::open(&options, path)?;
                    return Ok(Self::RocksDb(RocksDbEngine {
                        db,
                        durable: durability.sync_writes(),
                    }));
                }
                #[cfg(not(feature = "rocksdb"))]
                {
                    Err("RocksDB support is disabled; rebuild with --features rocksdb".into())
                }
            }
        }
    }

    pub(super) fn open_existing(
        kind: EngineKind,
        path: &Path,
        durability: DurabilityMode,
    ) -> BenchResult<Self> {
        match kind {
            EngineKind::SeerDb => {
                if durability != DurabilityMode::Durable {
                    return Err(
                        "SeerDB common-KV comparison supports durable mode only; buffered mode is not equivalent"
                            .into(),
                    );
                }
                let options = seerdb::Options {
                    // See the create path: commit_batch's publication barrier
                    // is the matched durable boundary for this adapter.
                    sync_writes: false,
                    blob_threshold: usize::MAX,
                    ..seerdb::Options::default()
                };
                Ok(Self::SeerDb {
                    db: Box::new(seerdb::DB::open(path, options)?),
                })
            }
            EngineKind::Fjall => {
                #[cfg(feature = "fjall")]
                {
                    let db = fjall::Database::builder(path).open()?;
                    let keyspace = db.keyspace("default", fjall::KeyspaceCreateOptions::default)?;
                    Ok(Self::Fjall(FjallEngine {
                        db,
                        keyspace,
                        durable: durability.sync_writes(),
                    }))
                }
                #[cfg(not(feature = "fjall"))]
                {
                    Err("Fjall support is disabled; rebuild with --features fjall".into())
                }
            }
            EngineKind::RocksDb => {
                #[cfg(feature = "rocksdb")]
                {
                    let mut options = rocksdb::Options::default();
                    options.create_if_missing(false);
                    let db = rocksdb::DB::open(&options, path)?;
                    return Ok(Self::RocksDb(RocksDbEngine {
                        db,
                        durable: durability.sync_writes(),
                    }));
                }
                #[cfg(not(feature = "rocksdb"))]
                {
                    Err("RocksDB support is disabled; rebuild with --features rocksdb".into())
                }
            }
        }
    }

    pub(super) fn write_batch(&mut self, mutations: &[Operation]) -> BenchResult<()> {
        match self {
            Self::SeerDb { db, .. } => {
                let mutations = mutations
                    .iter()
                    .map(|operation| match operation {
                        Operation::Put { key, value } => seerdb::BatchMutation::Put {
                            key: key.clone(),
                            value: value.clone(),
                        },
                        Operation::Delete { key } => {
                            seerdb::BatchMutation::Delete { key: key.clone() }
                        }
                        Operation::Get { .. } | Operation::Range { .. } => {
                            unreachable!("read operation passed to write_batch")
                        }
                    })
                    .collect::<Vec<_>>();
                db.commit_batch(&mutations)?;
            }
            #[cfg(feature = "fjall")]
            Self::Fjall(engine) => {
                let mut batch = engine.db.batch();
                for operation in mutations {
                    match operation {
                        Operation::Put { key, value } => {
                            batch.insert(&engine.keyspace, key.clone(), value.clone())
                        }
                        Operation::Delete { key } => batch.remove(&engine.keyspace, key.clone()),
                        Operation::Get { .. } | Operation::Range { .. } => {
                            unreachable!("read operation passed to write_batch")
                        }
                    }
                }
                batch.commit()?;
                if engine.durable {
                    engine.db.persist(fjall::PersistMode::SyncAll)?;
                }
            }
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(engine) => {
                let mut batch = rocksdb::WriteBatch::default();
                for operation in mutations {
                    match operation {
                        Operation::Put { key, value } => batch.put(key, value),
                        Operation::Delete { key } => batch.delete(key),
                        Operation::Get { .. } | Operation::Range { .. } => {
                            unreachable!("read operation passed to write_batch")
                        }
                    }
                }
                let mut write_options = rocksdb::WriteOptions::default();
                write_options.set_sync(engine.durable);
                engine.db.write_opt(batch, &write_options)?;
            }
        }
        Ok(())
    }

    pub(super) fn get(&self, key: &[u8]) -> BenchResult<Option<Vec<u8>>> {
        match self {
            Self::SeerDb { db, .. } => Ok(db.get(key)?),
            #[cfg(feature = "fjall")]
            Self::Fjall(engine) => Ok(engine.keyspace.get(key)?.map(|value| value.to_vec())),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(engine) => Ok(engine.db.get(key)?),
        }
    }

    pub(super) fn range(&self, start: &[u8], end: &[u8]) -> BenchResult<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            Self::SeerDb { db, .. } => Ok(db.range(start, end)?),
            #[cfg(feature = "fjall")]
            Self::Fjall(engine) => engine
                .keyspace
                .range(start.to_vec()..end.to_vec())
                .map(|guard| {
                    let (key, value) = guard.into_inner()?;
                    Ok((key.to_vec(), value.to_vec()))
                })
                .collect(),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(engine) => {
                use rocksdb::{Direction, IteratorMode};
                let mut entries = Vec::new();
                for item in engine
                    .db
                    .iterator(IteratorMode::From(start, Direction::Forward))
                {
                    let (key, value) = item?;
                    if key.as_ref() >= end {
                        break;
                    }
                    entries.push((key.to_vec(), value.to_vec()));
                }
                Ok(entries)
            }
        }
    }

    pub(super) fn seer_counters(&self) -> Option<SeerCounters> {
        match self {
            Self::SeerDb { db, .. } => db.metrics().ok().map(|metrics| SeerCounters {
                physical_page_writes: metrics.storage.physical_page_writes,
                page_bytes_written: metrics.storage.page_bytes_written,
                generation_flushes: metrics.storage.generation_flushes,
                data_syncs: metrics.storage.syncs,
                wal_bytes_written: metrics.publication.wal_bytes_written,
                metadata_bytes_written: metrics.publication.metadata_bytes_written,
                blob_bytes_written: metrics.publication.blob_bytes_written,
                history_bytes_written: metrics.publication.history_bytes_written,
                manifest_bytes_written: metrics.publication.manifest_bytes_written,
                reclaimed_bytes: metrics.storage.reclaimed_bytes,
                candidate_prepare_ns: metrics.publication_timing.candidate_prepare_ns,
                wal_write_ns: metrics.publication_timing.wal_write_ns,
                admission_ns: metrics.publication_timing.admission_ns,
                data_flush_ns: metrics.publication_timing.data_flush_ns,
                metadata_write_ns: metrics.publication_timing.metadata_write_ns,
                blob_write_ns: metrics.publication_timing.blob_write_ns,
                history_write_ns: metrics.publication_timing.history_write_ns,
                directory_sync_ns: metrics.publication_timing.directory_sync_ns,
                manifest_write_ns: metrics.publication_timing.manifest_write_ns,
                manifest_mirror_ns: metrics.publication_timing.manifest_mirror_ns,
                cleanup_ns: metrics.publication_timing.cleanup_ns,
            }),
            #[cfg(feature = "fjall")]
            Self::Fjall(_) => None,
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(_) => None,
        }
    }

    pub(super) fn close(self) -> BenchResult<()> {
        match self {
            Self::SeerDb { mut db, .. } => db.close()?,
            #[cfg(feature = "fjall")]
            Self::Fjall(_) => {}
            #[cfg(feature = "rocksdb")]
            Self::RocksDb(_) => {}
        }
        Ok(())
    }
}
