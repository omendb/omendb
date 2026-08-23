use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::relational::{
    Catalog, RelationalMutation, RelationalSnapshot, RelationalSnapshotTable, TableDefinition,
    build_snapshot_capture, decode_catalog, decode_row, encode_catalog, encode_row,
    row_identity_bytes,
};
use crate::relational_database::{RelationalBackendKind, RelationalSnapshotCapture};
use crate::{
    AttemptRecord, CommitId, DatabaseConfig, DbError, Key, RelationalBackendConfig,
    RelationalDatabase, Result, SeerKernelConfig, StorageIdentity, TableId,
};

const ARCHIVE_MAGIC: [u8; 4] = *b"DBAR";
const ARCHIVE_VERSION: u32 = 4;
const ARCHIVE_HEADER_BYTES: usize = 100;
const MAX_ARCHIVE_BYTES: usize = 512 * 1024 * 1024;
const MAX_SNAPSHOTS: usize = 1_000_000;
const MAX_ATTEMPTS: usize = 1_000_000;

/// The logical history scope represented by an archive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalArchiveMode {
    /// One current state with no history-preservation claim.
    CurrentState,
    /// The current state plus caller-selected retained snapshots.
    RetainedSnapshots,
    /// Every representable relational commit boundary from an authoritative
    /// source catalog. Histories with reclaimed storage or control-plane
    /// records without an explicit archive policy are refused. Consecutive
    /// boundaries with the same relational state are retained and may map to
    /// one target commit.
    FullHistory,
}

/// How transaction-attempt control-plane records are handled by this archive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalArchiveAttemptDisposition {
    /// The source capture proved that no durable attempt records were present.
    NoAttemptRecords,
    /// Durable attempt records were transferred and remapped to the target
    /// history during restore.
    Transferred,
    /// Durable attempt records were intentionally invalidated by the caller's
    /// explicit archive policy and are not present in the target.
    ExcludedByPolicy,
}

/// Policy applied when a source capture contains durable transaction attempts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationalArchiveAttemptPolicy {
    /// Refuse archive creation until attempts can be mapped to target commits.
    Refuse,
    /// Transfer the captured attempts. Every source attempt commit must be
    /// one of the captured snapshots so restore can prove its source state.
    Transfer,
    /// Explicitly invalidate the captured attempts and report their count.
    ExcludeByPolicy,
}

/// Per-snapshot metadata recorded in a logical archive manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalArchiveSnapshotManifest {
    pub commit: CommitId,
    pub catalog_digest: [u8; 32],
    pub logical_digest: [u8; 32],
    pub table_count: u64,
    pub row_count: u64,
}

/// The versioned manifest for one logical archive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalArchiveManifest {
    pub format_version: u32,
    pub mode: RelationalArchiveMode,
    pub source_backend: RelationalBackendKind,
    pub source_identity: StorageIdentity,
    pub source_head: CommitId,
    pub attempt_disposition: RelationalArchiveAttemptDisposition,
    pub excluded_attempt_count: u64,
    pub snapshots: Vec<RelationalArchiveSnapshotManifest>,
    pub archive_digest: [u8; 32],
}

/// A portable logical archive assembled from a bounded source capture.
///
/// The catalog and rows are authoritative. Secondary indexes are not stored;
/// a future target importer must rebuild and verify them from this payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalArchive {
    pub manifest: RelationalArchiveManifest,
    pub snapshots: Vec<RelationalSnapshot>,
    /// Durable transaction-attempt records included when the manifest uses
    /// [`RelationalArchiveAttemptDisposition::Transferred`].
    pub attempts: Vec<AttemptRecord>,
}

#[derive(Clone, Copy)]
struct ArchiveMetadata {
    source_backend: RelationalBackendKind,
    source_identity: StorageIdentity,
    source_head: CommitId,
    mode: RelationalArchiveMode,
    complete_history: bool,
    attempt_disposition: RelationalArchiveAttemptDisposition,
    excluded_attempt_count: u64,
}

/// One source-to-target commit mapping produced by archive restore.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalArchiveSnapshotMapping {
    pub source: CommitId,
    pub target: CommitId,
}

/// One source-to-target mapping for a transferred durable transaction attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationalArchiveAttemptMapping {
    pub source: AttemptRecord,
    pub target: AttemptRecord,
}

/// Evidence returned after a logical archive is rebuilt and reopened.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalArchiveRestoreReport {
    pub source_identity: StorageIdentity,
    pub target_identity: StorageIdentity,
    pub mode: RelationalArchiveMode,
    pub attempt_disposition: RelationalArchiveAttemptDisposition,
    pub excluded_attempt_count: u64,
    pub mappings: Vec<RelationalArchiveSnapshotMapping>,
    pub attempt_mappings: Vec<RelationalArchiveAttemptMapping>,
    pub history_preserved: bool,
}

impl RelationalArchive {
    /// Assemble an archive from a source-owned bounded capture.
    pub fn from_capture(
        capture: RelationalSnapshotCapture,
        mode: RelationalArchiveMode,
    ) -> Result<Self> {
        Self::from_capture_with_attempt_policy(
            capture,
            mode,
            RelationalArchiveAttemptPolicy::Refuse,
        )
    }

    /// Assemble an archive with an explicit policy for durable attempts.
    pub fn from_capture_with_attempt_policy(
        capture: RelationalSnapshotCapture,
        mode: RelationalArchiveMode,
        attempt_policy: RelationalArchiveAttemptPolicy,
    ) -> Result<Self> {
        let RelationalSnapshotCapture {
            source_backend,
            source_identity,
            source_head,
            complete_history,
            attempts,
            snapshots,
        } = capture;
        if mode == RelationalArchiveMode::FullHistory && !complete_history {
            return Err(DbError::InvalidState(
                "full-history archive requires an authoritative complete source catalog".to_owned(),
            ));
        }
        let attempt_count = u64::try_from(attempts.len())
            .map_err(|_| DbError::InvalidState("too many transaction attempts".to_owned()))?;
        let attempt_disposition = match (attempt_policy, attempt_count) {
            (RelationalArchiveAttemptPolicy::Refuse, 0) => {
                RelationalArchiveAttemptDisposition::NoAttemptRecords
            }
            (RelationalArchiveAttemptPolicy::Refuse, _) => {
                return Err(DbError::InvalidState(
                    "archive creation refuses durable transaction attempts until their target mapping is supported"
                        .to_owned(),
                ));
            }
            (RelationalArchiveAttemptPolicy::Transfer, 0) => {
                RelationalArchiveAttemptDisposition::NoAttemptRecords
            }
            (RelationalArchiveAttemptPolicy::Transfer, _) => {
                validate_transfer_attempts(&attempts, &snapshots)?;
                RelationalArchiveAttemptDisposition::Transferred
            }
            (RelationalArchiveAttemptPolicy::ExcludeByPolicy, 0) => {
                RelationalArchiveAttemptDisposition::NoAttemptRecords
            }
            (RelationalArchiveAttemptPolicy::ExcludeByPolicy, _) => {
                RelationalArchiveAttemptDisposition::ExcludedByPolicy
            }
        };
        let excluded_attempt_count =
            if attempt_disposition == RelationalArchiveAttemptDisposition::ExcludedByPolicy {
                attempt_count
            } else {
                0
            };
        let archive_attempts =
            if attempt_disposition == RelationalArchiveAttemptDisposition::Transferred {
                attempts
            } else {
                Vec::new()
            };
        let payload = encode_payload(&snapshots, &archive_attempts)?;
        build_archive(
            ArchiveMetadata {
                source_backend,
                source_identity,
                source_head,
                mode,
                complete_history,
                attempt_disposition,
                excluded_attempt_count,
            },
            snapshots,
            archive_attempts,
            &payload,
        )
    }

    /// Return the canonical archive bytes, including its integrity header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let payload = encode_payload(&self.snapshots, &self.attempts)?;
        let expected = build_manifest(
            ArchiveMetadata {
                source_backend: self.manifest.source_backend,
                source_identity: self.manifest.source_identity,
                source_head: self.manifest.source_head,
                mode: self.manifest.mode,
                complete_history: self.manifest.mode == RelationalArchiveMode::FullHistory,
                attempt_disposition: self.manifest.attempt_disposition,
                excluded_attempt_count: self.manifest.excluded_attempt_count,
            },
            &self.snapshots,
            &self.attempts,
            &payload,
        )?;
        if expected != self.manifest {
            return Err(DbError::InvalidState(
                "archive manifest does not match its snapshots".to_owned(),
            ));
        }
        encode_file(&self.manifest, &payload)
    }

    /// Write a new archive file through a synced temporary rename.
    ///
    /// Existing destinations are rejected to avoid accidental replacement.
    pub fn write(&self, path: &Path) -> Result<()> {
        if path.exists() {
            return Err(DbError::InvalidState(
                "archive destination already exists".to_owned(),
            ));
        }
        let bytes = self.encode()?;
        if bytes.len() > MAX_ARCHIVE_BYTES {
            return Err(DbError::SnapshotCaptureLimit {
                resource: "archive bytes",
                limit: MAX_ARCHIVE_BYTES,
            });
        }
        let temporary = path.with_file_name(format!(
            ".{}.tmp-{}",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("omendb.archive"),
            std::process::id()
        ));
        let mut file = File::create(&temporary)
            .map_err(|source| io_error("create relational archive", source))?;
        file.write_all(&bytes)
            .map_err(|source| io_error("write relational archive", source))?;
        file.sync_all()
            .map_err(|source| io_error("sync relational archive", source))?;
        fs::rename(&temporary, path)
            .map_err(|source| io_error("publish relational archive", source))?;
        sync_parent(path)
    }

    /// Read and fully validate a logical archive.
    pub fn read(path: &Path) -> Result<Self> {
        let bytes = fs::read(path).map_err(|source| io_error("read relational archive", source))?;
        if bytes.len() > MAX_ARCHIVE_BYTES {
            return Err(archive_corruption("archive exceeds maximum size"));
        }
        decode_file(&bytes)
    }

    /// Rebuild this archive into a fresh selected backend.
    ///
    /// The destination is assembled in a sibling staging directory, verified,
    /// published without replacement, reopened, and verified again. The first
    /// implementation supports selected snapshots with additive catalog
    /// definitions. Transferred attempt records are remapped to the target
    /// history and verified after reopen; the extra target control-plane
    /// publication is kept after the relational history mappings. The default
    /// archive constructor still refuses attempts, while the explicit
    /// exclusion policy reports them as invalidated.
    pub fn restore(
        &self,
        config: RelationalBackendConfig,
    ) -> Result<(RelationalDatabase, RelationalArchiveRestoreReport)> {
        self.encode()?;
        restore_archive(self, config)
    }
}

fn restore_archive(
    archive: &RelationalArchive,
    config: RelationalBackendConfig,
) -> Result<(RelationalDatabase, RelationalArchiveRestoreReport)> {
    let destination = target_directory(&config).to_owned();
    if destination.exists() {
        return Err(DbError::InvalidState(
            "archive restore destination already exists".to_owned(),
        ));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| DbError::InvalidState("archive restore path has no parent".to_owned()))?
        .to_owned();
    fs::create_dir_all(&parent)
        .map_err(|source| io_error("create archive restore parent", source))?;
    let staging = parent.join(format!(".omendb-archive-stage-{}", std::process::id()));
    if staging.exists() {
        return Err(DbError::InvalidState(
            "archive restore staging path already exists".to_owned(),
        ));
    }
    let staging_config = config_with_directory(&config, staging.clone());
    let mut published = false;
    let result = (|| {
        let mut target = RelationalDatabase::create(staging_config)?;
        let (mappings, attempt_mappings) = rebuild_target(&mut target, archive)?;
        target.verify()?;
        let target_identity = target.storage_identity()?;
        target.close()?;
        rename_no_replace(&staging, &destination)
            .map_err(|source| io_error("publish restored archive", source))?;
        published = true;
        sync_parent(&destination).map_err(|error| DbError::MigrationPublished {
            destination: destination.display().to_string(),
            reason: error.to_string(),
        })?;
        let mut reopened = RelationalDatabase::open(config.clone()).map_err(|error| {
            DbError::MigrationPublished {
                destination: destination.display().to_string(),
                reason: error.to_string(),
            }
        })?;
        reopened
            .verify()
            .map_err(|error| DbError::MigrationPublished {
                destination: destination.display().to_string(),
                reason: error.to_string(),
            })?;
        if reopened
            .storage_identity()
            .map_err(|error| DbError::MigrationPublished {
                destination: destination.display().to_string(),
                reason: error.to_string(),
            })?
            != target_identity
        {
            return Err(DbError::MigrationPublished {
                destination: destination.display().to_string(),
                reason: "target identity changed across reopen".to_owned(),
            });
        }
        for mapping in &mappings {
            let lease =
                reopened
                    .retain(mapping.target)
                    .map_err(|error| DbError::MigrationPublished {
                        destination: destination.display().to_string(),
                        reason: error.to_string(),
                    })?;
            let snapshot = archive
                .snapshots
                .iter()
                .find(|snapshot| snapshot.commit == mapping.source)
                .ok_or_else(|| DbError::MigrationPublished {
                    destination: destination.display().to_string(),
                    reason: format!("missing source mapping for commit {}", mapping.source.0),
                })?;
            let verification = verify_target_snapshot(&reopened, snapshot, mapping.target);
            let release = reopened.release(lease);
            if let Err(error) = verification {
                return Err(DbError::MigrationPublished {
                    destination: destination.display().to_string(),
                    reason: error.to_string(),
                });
            }
            release.map_err(|error| DbError::MigrationPublished {
                destination: destination.display().to_string(),
                reason: error.to_string(),
            })?;
        }
        for mapping in &attempt_mappings {
            let resolved = reopened
                .resolve_attempt(mapping.source.attempt)
                .map_err(|error| DbError::MigrationPublished {
                    destination: destination.display().to_string(),
                    reason: error.to_string(),
                })?;
            if resolved != Some(mapping.target) {
                return Err(DbError::MigrationPublished {
                    destination: destination.display().to_string(),
                    reason: format!(
                        "transferred attempt {} did not resolve to its target record",
                        hex_attempt(mapping.source.attempt)
                    ),
                });
            }
        }
        Ok((
            reopened,
            RelationalArchiveRestoreReport {
                source_identity: archive.manifest.source_identity,
                target_identity,
                mode: archive.manifest.mode,
                attempt_disposition: archive.manifest.attempt_disposition,
                excluded_attempt_count: archive.manifest.excluded_attempt_count,
                mappings,
                attempt_mappings,
                history_preserved: archive.manifest.mode != RelationalArchiveMode::CurrentState,
            },
        ))
    })();
    if result.is_err() && !published {
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

fn build_archive(
    metadata: ArchiveMetadata,
    snapshots: Vec<RelationalSnapshot>,
    attempts: Vec<AttemptRecord>,
    payload: &[u8],
) -> Result<RelationalArchive> {
    let manifest = build_manifest(metadata, &snapshots, &attempts, payload)?;
    Ok(RelationalArchive {
        manifest,
        snapshots,
        attempts,
    })
}

fn rebuild_target(
    target: &mut RelationalDatabase,
    archive: &RelationalArchive,
) -> Result<(
    Vec<RelationalArchiveSnapshotMapping>,
    Vec<RelationalArchiveAttemptMapping>,
)> {
    // Keep every reconstructed boundary pinned while later schema/data
    // commits are published. This matters for engines whose physical pages
    // can otherwise be reclaimed or reused before the staged history is
    // verified after reopen.
    let mut history_leases = Vec::with_capacity(archive.snapshots.len());
    let first = archive
        .snapshots
        .first()
        .ok_or_else(|| DbError::InvalidState("archive contains no snapshots".to_owned()))?;
    for table in first.catalog.tables() {
        target.create_table_with_schema_and_primary_key(
            table.clone(),
            first.catalog.primary_key(table.id).map(ToOwned::to_owned),
            Default::default(),
        )?;
    }
    let initial_rows = first
        .tables
        .iter()
        .flat_map(|table| {
            table
                .rows
                .iter()
                .cloned()
                .map(|row| RelationalMutation::Insert {
                    table: table.table,
                    row,
                })
        })
        .collect::<Vec<_>>();
    if !initial_rows.is_empty() {
        target.commit_batch(initial_rows)?;
    }
    for index in first.catalog.indexes() {
        match first.catalog.index_name(index.id) {
            Some(name) => target.create_named_index(index.clone(), name.to_owned())?,
            None => target.create_index(index.clone())?,
        };
    }
    for foreign_key in first.catalog.foreign_keys() {
        match first.catalog.foreign_key_name(foreign_key.id) {
            Some(name) => target.create_named_foreign_key(foreign_key.clone(), name.to_owned())?,
            None => target.create_foreign_key(foreign_key.clone())?,
        };
    }
    let mut mappings = Vec::with_capacity(archive.snapshots.len());
    verify_target_snapshot(target, first, target.commit_id())?;
    history_leases.push(target.retain(target.commit_id())?);
    mappings.push(RelationalArchiveSnapshotMapping {
        source: first.commit,
        target: target.commit_id(),
    });

    for snapshot in &archive.snapshots[1..] {
        advance_catalog(target, &snapshot.catalog)?;
        let mutations = reconcile_rows(target, snapshot)?;
        let target_commit = if mutations.is_empty() {
            target.commit_id()
        } else {
            target.commit_batch(mutations)?
        };
        verify_target_snapshot(target, snapshot, target_commit)?;
        history_leases.push(target.retain(target_commit)?);
        mappings.push(RelationalArchiveSnapshotMapping {
            source: snapshot.commit,
            target: target_commit,
        });
    }
    let attempt_mappings = if archive.attempts.is_empty() {
        Vec::new()
    } else {
        let target_attempts = target.import_attempt_records(&archive.attempts)?;
        archive
            .attempts
            .iter()
            .copied()
            .zip(target_attempts)
            .map(|(source, target)| RelationalArchiveAttemptMapping { source, target })
            .collect()
    };
    Ok((mappings, attempt_mappings))
}

fn advance_catalog(target: &mut RelationalDatabase, desired: &Catalog) -> Result<()> {
    for table in desired.tables() {
        let existing = target
            .catalog()
            .tables()
            .find(|candidate| candidate.id == table.id)
            .cloned();
        match existing {
            Some(existing)
                if existing == *table
                    && target.catalog().primary_key(table.id) == desired.primary_key(table.id) => {}
            Some(existing)
                if target.catalog().primary_key(table.id) == desired.primary_key(table.id)
                    && is_additive_table_change(&existing, table) =>
            {
                for column in table.columns.iter().skip(existing.columns.len()) {
                    target.add_nullable_column(table.id, column.clone())?;
                }
            }
            None => {
                target.create_table_with_schema_and_primary_key(
                    table.clone(),
                    desired.primary_key(table.id).map(ToOwned::to_owned),
                    Default::default(),
                )?;
            }
            Some(_) => {
                return Err(DbError::InvalidState(format!(
                    "archive restore cannot alter table {}",
                    table.id.0
                )));
            }
        }
    }
    if target
        .catalog()
        .tables()
        .any(|table| desired.tables().all(|candidate| candidate.id != table.id))
    {
        return Err(DbError::InvalidState(
            "archive restore cannot remove a table".to_owned(),
        ));
    }

    for index in desired.indexes() {
        let existing = target
            .catalog()
            .indexes()
            .find(|candidate| candidate.id == index.id)
            .cloned();
        match existing {
            Some(existing) if existing == *index => {}
            Some(_) => {
                return Err(DbError::InvalidState(format!(
                    "archive restore cannot alter index {}",
                    index.id.0
                )));
            }
            None => match desired.index_name(index.id) {
                Some(name) => {
                    target.create_named_index(index.clone(), name.to_owned())?;
                }
                None => {
                    target.create_index(index.clone())?;
                }
            },
        }
    }
    if target
        .catalog()
        .indexes()
        .any(|index| desired.indexes().all(|candidate| candidate.id != index.id))
    {
        return Err(DbError::InvalidState(
            "archive restore cannot remove an index".to_owned(),
        ));
    }

    for foreign_key in desired.foreign_keys() {
        let existing = target
            .catalog()
            .foreign_keys()
            .find(|candidate| candidate.id == foreign_key.id)
            .cloned();
        match existing {
            Some(existing) if existing == *foreign_key => {}
            Some(_) => {
                return Err(DbError::InvalidState(format!(
                    "archive restore cannot alter foreign key {}",
                    foreign_key.id.0
                )));
            }
            None => match desired.foreign_key_name(foreign_key.id) {
                Some(name) => {
                    target.create_named_foreign_key(foreign_key.clone(), name.to_owned())?;
                }
                None => {
                    target.create_foreign_key(foreign_key.clone())?;
                }
            },
        }
    }
    if target.catalog().foreign_keys().any(|foreign_key| {
        desired
            .foreign_keys()
            .all(|candidate| candidate.id != foreign_key.id)
    }) {
        return Err(DbError::InvalidState(
            "archive restore cannot remove a foreign key".to_owned(),
        ));
    }
    if target.catalog() != desired {
        return Err(DbError::InvalidState(
            "archive restore catalog generation differs from source".to_owned(),
        ));
    }
    Ok(())
}

fn is_additive_table_change(existing: &TableDefinition, desired: &TableDefinition) -> bool {
    existing.id == desired.id
        && existing.name == desired.name
        && desired.columns.len() > existing.columns.len()
        && desired.columns[..existing.columns.len()] == existing.columns[..]
        && desired.columns[existing.columns.len()..]
            .iter()
            .all(|column| column.nullable)
}

fn reconcile_rows(
    target: &RelationalDatabase,
    snapshot: &RelationalSnapshot,
) -> Result<Vec<RelationalMutation>> {
    let mut mutations = Vec::new();
    for table in &snapshot.tables {
        let definition = snapshot.catalog.table(table.table)?;
        let source_rows = table
            .rows
            .iter()
            .map(|row| Ok((row_identity_bytes(&snapshot.catalog, definition, row)?, row)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let target_catalog = target.catalog_at(target.commit_id())?;
        let target_definition = target_catalog.table(table.table)?;
        let target_rows = target
            .scan(table.table, target.commit_id(), usize::MAX)?
            .into_iter()
            .map(|row| {
                Ok((
                    row_identity_bytes(&target_catalog, target_definition, &row)?,
                    row,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        for (identity, row) in &source_rows {
            match target_rows.get(identity) {
                Some(existing) if *existing == **row => {}
                Some(_) => mutations.push(RelationalMutation::Update {
                    table: table.table,
                    row: (*row).clone(),
                }),
                None => mutations.push(RelationalMutation::Insert {
                    table: table.table,
                    row: (*row).clone(),
                }),
            }
        }
        for (identity, row) in &target_rows {
            if !source_rows.contains_key(identity) {
                mutations.push(RelationalMutation::DeleteRow {
                    table: table.table,
                    row: row.clone(),
                });
            }
        }
    }
    Ok(mutations)
}

fn verify_target_snapshot(
    target: &RelationalDatabase,
    source: &RelationalSnapshot,
    target_commit: CommitId,
) -> Result<()> {
    let catalog = target.catalog_at(target_commit)?;
    if catalog != source.catalog {
        return Err(archive_corruption(
            "target catalog differs from source archive",
        ));
    }
    let mut tables = Vec::with_capacity(source.tables.len());
    for table in &source.tables {
        let rows = target.scan(table.table, target_commit, usize::MAX)?;
        if rows != table.rows {
            return Err(archive_corruption("target rows differ from source archive"));
        }
        tables.push(RelationalSnapshotTable {
            table: table.table,
            rows,
        });
    }
    let rebuilt = build_snapshot_capture(target_commit, catalog, tables)?;
    if rebuilt.catalog_digest != source.catalog_digest
        || rebuilt.logical_digest != source.logical_digest
    {
        return Err(archive_corruption(
            "target snapshot digest differs from source",
        ));
    }
    Ok(())
}

fn target_directory(config: &RelationalBackendConfig) -> &Path {
    match config {
        RelationalBackendConfig::Temporary(config) => &config.directory,
        RelationalBackendConfig::Seer(config) => &config.directory,
    }
}

fn config_with_directory(
    config: &RelationalBackendConfig,
    directory: std::path::PathBuf,
) -> RelationalBackendConfig {
    match config {
        RelationalBackendConfig::Temporary(_config) => {
            RelationalBackendConfig::Temporary(DatabaseConfig { directory })
        }
        RelationalBackendConfig::Seer(config) => RelationalBackendConfig::Seer(SeerKernelConfig {
            directory,
            options: config.options.clone(),
        }),
    }
}

fn build_manifest(
    metadata: ArchiveMetadata,
    snapshots: &[RelationalSnapshot],
    attempts: &[AttemptRecord],
    payload: &[u8],
) -> Result<RelationalArchiveManifest> {
    validate_mode(
        metadata.mode,
        metadata.complete_history,
        metadata.source_head,
        snapshots,
    )?;
    validate_attempt_disposition(
        metadata.attempt_disposition,
        metadata.excluded_attempt_count,
        attempts,
        snapshots,
    )?;
    match metadata.attempt_disposition {
        RelationalArchiveAttemptDisposition::NoAttemptRecords
            if metadata.excluded_attempt_count != 0 =>
        {
            return Err(DbError::InvalidState(
                "archive attempt disposition has a non-zero excluded count".to_owned(),
            ));
        }
        RelationalArchiveAttemptDisposition::ExcludedByPolicy
            if metadata.excluded_attempt_count == 0 =>
        {
            return Err(DbError::InvalidState(
                "archive exclusion policy has no excluded attempts".to_owned(),
            ));
        }
        _ => {}
    }
    if snapshots.len() > MAX_SNAPSHOTS {
        return Err(DbError::SnapshotCaptureLimit {
            resource: "archive snapshots",
            limit: MAX_SNAPSHOTS,
        });
    }
    let mut snapshot_manifests = Vec::with_capacity(snapshots.len());
    let mut previous = None;
    for snapshot in snapshots {
        if previous.is_some_and(|commit| commit >= snapshot.commit) {
            return Err(archive_corruption("snapshots are not strictly ordered"));
        }
        previous = Some(snapshot.commit);
        let row_count = validate_snapshot(snapshot)?;
        snapshot_manifests.push(RelationalArchiveSnapshotManifest {
            commit: snapshot.commit,
            catalog_digest: snapshot.catalog_digest,
            logical_digest: snapshot.logical_digest,
            table_count: snapshot.tables.len() as u64,
            row_count,
        });
    }
    Ok(RelationalArchiveManifest {
        format_version: ARCHIVE_VERSION,
        mode: metadata.mode,
        source_backend: metadata.source_backend,
        source_identity: metadata.source_identity,
        source_head: metadata.source_head,
        attempt_disposition: metadata.attempt_disposition,
        excluded_attempt_count: metadata.excluded_attempt_count,
        snapshots: snapshot_manifests,
        archive_digest: Sha256::digest(payload).into(),
    })
}

fn validate_mode(
    mode: RelationalArchiveMode,
    complete_history: bool,
    source_head: CommitId,
    snapshots: &[RelationalSnapshot],
) -> Result<()> {
    match mode {
        RelationalArchiveMode::CurrentState => {
            if snapshots.len() != 1 || snapshots[0].commit != source_head {
                return Err(DbError::InvalidState(
                    "current-state archive must contain only the source head".to_owned(),
                ));
            }
        }
        RelationalArchiveMode::RetainedSnapshots => {
            if snapshots.is_empty()
                || !snapshots
                    .iter()
                    .any(|snapshot| snapshot.commit == source_head)
            {
                return Err(DbError::InvalidState(
                    "retained-snapshot archive must contain the source head".to_owned(),
                ));
            }
        }
        RelationalArchiveMode::FullHistory => {
            if !complete_history
                || snapshots.is_empty()
                || snapshots.first().map(|snapshot| snapshot.commit) != Some(CommitId(0))
                || snapshots.last().map(|snapshot| snapshot.commit) != Some(source_head)
                || snapshots
                    .windows(2)
                    .any(|pair| pair[0].commit.0.checked_add(1) != Some(pair[1].commit.0))
            {
                return Err(DbError::InvalidState(
                    "full-history archive requires complete ordered relational states".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_attempt_disposition(
    disposition: RelationalArchiveAttemptDisposition,
    excluded_attempt_count: u64,
    attempts: &[AttemptRecord],
    snapshots: &[RelationalSnapshot],
) -> Result<()> {
    match disposition {
        RelationalArchiveAttemptDisposition::NoAttemptRecords => {
            if excluded_attempt_count != 0 || !attempts.is_empty() {
                return Err(archive_corruption(
                    "no-attempt disposition contains attempt records",
                ));
            }
        }
        RelationalArchiveAttemptDisposition::Transferred => {
            if excluded_attempt_count != 0 {
                return Err(archive_corruption(
                    "transferred attempts have an excluded count",
                ));
            }
            validate_transfer_attempts(attempts, snapshots)?;
        }
        RelationalArchiveAttemptDisposition::ExcludedByPolicy => {
            if excluded_attempt_count == 0 || !attempts.is_empty() {
                return Err(archive_corruption(
                    "excluded-attempt disposition has invalid records",
                ));
            }
        }
    }
    Ok(())
}

fn validate_transfer_attempts(
    attempts: &[AttemptRecord],
    snapshots: &[RelationalSnapshot],
) -> Result<()> {
    if attempts.len() > MAX_ATTEMPTS {
        return Err(DbError::SnapshotCaptureLimit {
            resource: "archive attempts",
            limit: MAX_ATTEMPTS,
        });
    }
    let mut previous = None;
    for record in attempts {
        if record.commit.0 == 0 {
            return Err(archive_corruption("attempt record has a zero commit"));
        }
        if previous.is_some_and(|attempt| attempt >= record.attempt) {
            return Err(archive_corruption(
                "attempt records are not strictly ordered",
            ));
        }
        previous = Some(record.attempt);
        if !snapshots
            .iter()
            .any(|snapshot| snapshot.commit == record.commit)
        {
            return Err(DbError::InvalidState(format!(
                "transferred attempt {} requires its source commit {} in the archive",
                hex_attempt(record.attempt),
                record.commit.0
            )));
        }
    }
    Ok(())
}

fn hex_attempt(attempt: crate::TransactionAttemptId) -> String {
    attempt.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn validate_snapshot(snapshot: &RelationalSnapshot) -> Result<u64> {
    let mut previous_table = None;
    let mut row_count = 0_u64;
    for table in &snapshot.tables {
        if previous_table.is_some_and(|previous| previous >= table.table) {
            return Err(archive_corruption(
                "snapshot tables are not strictly ordered",
            ));
        }
        previous_table = Some(table.table);
        let definition = snapshot
            .catalog
            .table(table.table)
            .map_err(|error| archive_corruption(&format!("snapshot table is invalid: {error}")))?;
        let mut previous_identity = None;
        for row in &table.rows {
            row.validate(definition).map_err(|error| {
                archive_corruption(&format!("snapshot row is invalid: {error}"))
            })?;
            let identity =
                row_identity_bytes(&snapshot.catalog, definition, row).map_err(|error| {
                    archive_corruption(&format!("snapshot row identity is invalid: {error}"))
                })?;
            if previous_identity
                .as_deref()
                .is_some_and(|previous| previous >= identity.as_slice())
            {
                return Err(archive_corruption("snapshot rows are not strictly ordered"));
            }
            previous_identity = Some(identity);
            row_count = row_count
                .checked_add(1)
                .ok_or_else(|| archive_corruption("snapshot row count overflows"))?;
        }
    }
    let rebuilt = build_snapshot_capture(
        snapshot.commit,
        snapshot.catalog.clone(),
        snapshot.tables.clone(),
    )?;
    if rebuilt.catalog_digest != snapshot.catalog_digest
        || rebuilt.logical_digest != snapshot.logical_digest
    {
        return Err(archive_corruption("snapshot digest mismatch"));
    }
    Ok(row_count)
}

fn encode_file(manifest: &RelationalArchiveManifest, payload: &[u8]) -> Result<Vec<u8>> {
    let payload_len = u64::try_from(payload.len()).map_err(|_| DbError::SnapshotCaptureLimit {
        resource: "archive bytes",
        limit: MAX_ARCHIVE_BYTES,
    })?;
    let mut header = [0_u8; ARCHIVE_HEADER_BYTES];
    header[..4].copy_from_slice(&ARCHIVE_MAGIC);
    header[4..8].copy_from_slice(&ARCHIVE_VERSION.to_le_bytes());
    header[8] = mode_tag(manifest.mode);
    header[9] = backend_tag(manifest.source_backend);
    header[10] = attempt_disposition_tag(manifest.attempt_disposition);
    header[12..28].copy_from_slice(&manifest.source_identity.database_id);
    header[28..36].copy_from_slice(&manifest.source_identity.history_id.to_le_bytes());
    header[36..44].copy_from_slice(&manifest.source_head.0.to_le_bytes());
    header[44..52].copy_from_slice(&payload_len.to_le_bytes());
    header[52..84].copy_from_slice(&manifest.archive_digest);
    header[84..92].copy_from_slice(&manifest.excluded_attempt_count.to_le_bytes());
    let header_checksum = crc32c::crc32c(&header[..96]);
    header[96..100].copy_from_slice(&header_checksum.to_le_bytes());
    let mut bytes = Vec::with_capacity(ARCHIVE_HEADER_BYTES + payload.len());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

fn decode_file(bytes: &[u8]) -> Result<RelationalArchive> {
    if bytes.len() < ARCHIVE_HEADER_BYTES {
        return Err(archive_corruption("archive header is truncated"));
    }
    let header = &bytes[..ARCHIVE_HEADER_BYTES];
    if header[..4] != ARCHIVE_MAGIC
        || u32::from_le_bytes(header[4..8].try_into().expect("archive version width"))
            != ARCHIVE_VERSION
    {
        return Err(archive_corruption("unsupported archive header"));
    }
    let expected_header = u32::from_le_bytes(header[96..100].try_into().expect("archive checksum"));
    if crc32c::crc32c(&header[..96]) != expected_header {
        return Err(archive_corruption("archive header checksum mismatch"));
    }
    let payload_len = usize::try_from(u64::from_le_bytes(
        header[44..52].try_into().expect("archive length width"),
    ))
    .map_err(|_| archive_corruption("archive payload length overflows"))?;
    if payload_len > MAX_ARCHIVE_BYTES
        || ARCHIVE_HEADER_BYTES
            .checked_add(payload_len)
            .is_none_or(|length| length != bytes.len())
    {
        return Err(archive_corruption("archive payload length mismatch"));
    }
    let payload = &bytes[ARCHIVE_HEADER_BYTES..];
    let archive_digest: [u8; 32] = Sha256::digest(payload).into();
    if archive_digest != header[52..84] {
        return Err(archive_corruption("archive payload digest mismatch"));
    }
    let source_backend = backend_from_tag(header[9])?;
    let mode = mode_from_tag(header[8])?;
    let attempt_disposition = attempt_disposition_from_tag(header[10])?;
    let excluded_attempt_count = u64::from_le_bytes(
        header[84..92]
            .try_into()
            .expect("excluded attempt count width"),
    );
    let mut source_database = [0_u8; 16];
    source_database.copy_from_slice(&header[12..28]);
    let (snapshots, attempts) = decode_payload(payload)?;
    let capture = RelationalSnapshotCapture {
        source_backend,
        source_identity: StorageIdentity {
            database_id: source_database,
            history_id: u64::from_le_bytes(header[28..36].try_into().expect("history width")),
        },
        source_head: CommitId(u64::from_le_bytes(
            header[36..44].try_into().expect("head width"),
        )),
        complete_history: mode == RelationalArchiveMode::FullHistory,
        attempts,
        snapshots,
    };
    if capture.source_identity.history_id == 0 {
        return Err(archive_corruption("archive history ID is zero"));
    }
    let archive = build_archive(
        ArchiveMetadata {
            source_backend: capture.source_backend,
            source_identity: capture.source_identity,
            source_head: capture.source_head,
            mode,
            complete_history: mode == RelationalArchiveMode::FullHistory,
            attempt_disposition,
            excluded_attempt_count,
        },
        capture.snapshots,
        capture.attempts,
        payload,
    )?;
    if archive.manifest.attempt_disposition != attempt_disposition {
        return Err(archive_corruption("archive attempt disposition mismatch"));
    }
    if archive
        .encode()
        .map_err(|error| archive_corruption(&format!("archive is not canonical: {error}")))?
        != bytes
    {
        return Err(archive_corruption("archive encoding is not canonical"));
    }
    Ok(archive)
}

fn encode_payload(snapshots: &[RelationalSnapshot], attempts: &[AttemptRecord]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_u32(&mut bytes, snapshots.len())?;
    for snapshot in snapshots {
        let row_count = validate_snapshot(snapshot)?;
        let catalog = encode_catalog(&snapshot.catalog)?;
        bytes.extend_from_slice(&snapshot.commit.0.to_le_bytes());
        bytes.extend_from_slice(&snapshot.catalog_digest);
        bytes.extend_from_slice(&snapshot.logical_digest);
        put_u64(&mut bytes, snapshot.tables.len())?;
        put_u64(&mut bytes, row_count as usize)?;
        put_bytes(&mut bytes, &catalog)?;
        put_u32(&mut bytes, snapshot.tables.len())?;
        for table in &snapshot.tables {
            bytes.extend_from_slice(&table.table.0.to_le_bytes());
            put_u64(&mut bytes, table.rows.len())?;
            for row in &table.rows {
                bytes.extend_from_slice(&row.primary.0);
                put_bytes(&mut bytes, &encode_row(row)?)?;
            }
        }
    }
    put_u32(&mut bytes, attempts.len())?;
    for record in attempts {
        bytes.extend_from_slice(&record.attempt.0);
        bytes.extend_from_slice(&record.commit.0.to_le_bytes());
        bytes.extend_from_slice(&record.digest);
    }
    Ok(bytes)
}

fn decode_payload(bytes: &[u8]) -> Result<(Vec<RelationalSnapshot>, Vec<AttemptRecord>)> {
    let mut cursor = Cursor::new(bytes);
    let snapshot_count = cursor.u32()? as usize;
    if snapshot_count > MAX_SNAPSHOTS {
        return Err(archive_corruption("archive snapshot count exceeds limit"));
    }
    let mut snapshots = Vec::with_capacity(snapshot_count);
    for _ in 0..snapshot_count {
        let commit = CommitId(cursor.u64()?);
        let catalog_digest = cursor.fixed_32()?;
        let logical_digest = cursor.fixed_32()?;
        let expected_table_count = cursor.count()?;
        let expected_row_count = cursor.count()? as u64;
        let catalog = decode_catalog(cursor.bytes()?)?;
        let table_count = cursor.u32()? as usize;
        if table_count != expected_table_count {
            return Err(archive_corruption("archive table count metadata mismatch"));
        }
        let mut tables = Vec::with_capacity(table_count);
        for _ in 0..table_count {
            let table = TableId(cursor.u64()?);
            let row_count = cursor.count()?;
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let primary = Key(cursor.fixed_16()?);
                rows.push(decode_row(primary, cursor.bytes()?)?);
            }
            tables.push(RelationalSnapshotTable { table, rows });
        }
        let snapshot = build_snapshot_capture(commit, catalog, tables)?;
        let row_count = snapshot
            .tables
            .iter()
            .map(|table| table.rows.len() as u64)
            .sum::<u64>();
        if snapshot.catalog_digest != catalog_digest
            || snapshot.logical_digest != logical_digest
            || row_count != expected_row_count
        {
            return Err(archive_corruption("archive snapshot metadata mismatch"));
        }
        snapshots.push(snapshot);
    }
    let attempt_count = cursor.u32()? as usize;
    if attempt_count > MAX_ATTEMPTS {
        return Err(archive_corruption("archive attempt count exceeds limit"));
    }
    let mut attempts = Vec::with_capacity(attempt_count);
    for _ in 0..attempt_count {
        let attempt = crate::TransactionAttemptId(cursor.fixed_16()?);
        let commit = CommitId(cursor.u64()?);
        let digest = cursor.fixed_32()?;
        attempts.push(AttemptRecord {
            attempt,
            commit,
            digest,
        });
    }
    cursor.finish()?;
    Ok((snapshots, attempts))
}

fn put_u32(bytes: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| DbError::SnapshotCaptureLimit {
        resource: "archive collection length",
        limit: u32::MAX as usize,
    })?;
    bytes.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn put_u64(bytes: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u64::try_from(value).map_err(|_| archive_corruption("archive length overflows"))?;
    bytes.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    put_u64(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn mode_tag(mode: RelationalArchiveMode) -> u8 {
    match mode {
        RelationalArchiveMode::CurrentState => 1,
        RelationalArchiveMode::RetainedSnapshots => 2,
        RelationalArchiveMode::FullHistory => 3,
    }
}

fn mode_from_tag(tag: u8) -> Result<RelationalArchiveMode> {
    match tag {
        1 => Ok(RelationalArchiveMode::CurrentState),
        2 => Ok(RelationalArchiveMode::RetainedSnapshots),
        3 => Ok(RelationalArchiveMode::FullHistory),
        _ => Err(archive_corruption("unknown archive mode")),
    }
}

fn backend_tag(backend: RelationalBackendKind) -> u8 {
    match backend {
        RelationalBackendKind::Temporary => 1,
        RelationalBackendKind::Seer => 2,
    }
}

fn backend_from_tag(tag: u8) -> Result<RelationalBackendKind> {
    match tag {
        1 => Ok(RelationalBackendKind::Temporary),
        2 => Ok(RelationalBackendKind::Seer),
        _ => Err(archive_corruption("unknown archive backend")),
    }
}

fn attempt_disposition_tag(disposition: RelationalArchiveAttemptDisposition) -> u8 {
    match disposition {
        RelationalArchiveAttemptDisposition::NoAttemptRecords => 1,
        RelationalArchiveAttemptDisposition::Transferred => 2,
        RelationalArchiveAttemptDisposition::ExcludedByPolicy => 3,
    }
}

fn attempt_disposition_from_tag(tag: u8) -> Result<RelationalArchiveAttemptDisposition> {
    match tag {
        1 => Ok(RelationalArchiveAttemptDisposition::NoAttemptRecords),
        2 => Ok(RelationalArchiveAttemptDisposition::Transferred),
        3 => Ok(RelationalArchiveAttemptDisposition::ExcludedByPolicy),
        _ => Err(archive_corruption("unknown archive attempt disposition")),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| archive_corruption("archive cursor overflows"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| archive_corruption("archive payload is truncated"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn fixed_16(&mut self) -> Result<[u8; 16]> {
        self.take(16)
            .map(|bytes| bytes.try_into().expect("fixed key width"))
    }

    fn fixed_32(&mut self) -> Result<[u8; 32]> {
        self.take(32)
            .map(|bytes| bytes.try_into().expect("fixed digest width"))
    }

    fn u32(&mut self) -> Result<u32> {
        self.take(4)
            .map(|bytes| u32::from_le_bytes(bytes.try_into().expect("u32 width")))
    }

    fn u64(&mut self) -> Result<u64> {
        self.take(8)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().expect("u64 width")))
    }

    fn count(&mut self) -> Result<usize> {
        let count = usize::try_from(self.u64()?)
            .map_err(|_| archive_corruption("archive collection count overflows"))?;
        if count > MAX_SNAPSHOTS {
            return Err(archive_corruption("archive collection count exceeds limit"));
        }
        Ok(count)
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let length = usize::try_from(self.u64()?)
            .map_err(|_| archive_corruption("archive field length overflows"))?;
        if length > MAX_ARCHIVE_BYTES {
            return Err(archive_corruption("archive field exceeds size limit"));
        }
        self.take(length)
    }

    fn finish(self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(archive_corruption("archive payload has trailing bytes"))
        }
    }
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| DbError::InvalidState("archive path has no parent".to_owned()))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error("sync relational archive directory", source))
}

fn archive_corruption(reason: &str) -> DbError {
    DbError::Corruption {
        artifact: "relational archive",
        reason: reason.to_owned(),
    }
}

fn io_error(operation: &'static str, source: std::io::Error) -> DbError {
    DbError::Io { operation, source }
}

fn rename_no_replace(from: &Path, to: &Path) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let from = CString::new(from.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "source path contains NUL")
        })?;
        let to = CString::new(to.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "target path contains NUL")
        })?;
        // SAFETY: both paths remain alive across the syscall and AT_FDCWD
        // resolves them in the current filesystem namespace.
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let from = CString::new(from.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "source path contains NUL")
        })?;
        let to = CString::new(to.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "target path contains NUL")
        })?;
        // SAFETY: both paths remain alive across the syscall; RENAME_EXCL
        // makes the destination no-replace.
        let result = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (from, to);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "exclusive directory rename is unsupported on this platform",
        ))
    }
}
