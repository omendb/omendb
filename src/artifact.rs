use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::fault::{FaultInjector, FaultPoint};
use crate::{DbError, Result};

pub const PAGE_BYTES: usize = 4096;
const PAGE_HEADER: usize = 24;
const PAGE_PAYLOAD: usize = PAGE_BYTES - PAGE_HEADER;
const PAGE_MAGIC: [u8; 4] = *b"DBPG";
const MANIFEST_MAGIC: [u8; 4] = *b"DBMF";
const MANIFEST_VERSION: u32 = 4;
const MANIFEST_BYTES: usize = 44;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Manifest {
    pub generation: u64,
    pub commit: u64,
    pub logical_len: u64,
    pub payload_checksum: u32,
    pub range_checksum: u32,
}

pub fn data_path(manifest: &Path, generation: u64) -> PathBuf {
    let name = manifest
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("db");
    manifest.with_file_name(format!("{name}.data-{generation:016x}.pages"))
}

pub fn write_pages(
    path: &Path,
    generation: u64,
    payload: &[u8],
    faults: &mut dyn FaultInjector,
) -> Result<u64> {
    let mut file = File::create(path).map_err(|source| io_error("create page artifact", source))?;
    let page_count = payload.len().div_ceil(PAGE_PAYLOAD).max(1);
    for page_no in 0..page_count {
        let start = page_no * PAGE_PAYLOAD;
        let end = payload.len().min(start + PAGE_PAYLOAD);
        let chunk = payload.get(start..end).unwrap_or_default();
        let mut page = [0_u8; PAGE_BYTES];
        page[..4].copy_from_slice(&PAGE_MAGIC);
        page[4..12].copy_from_slice(&generation.to_le_bytes());
        page[12..16].copy_from_slice(&(page_no as u32).to_le_bytes());
        page[16..20].copy_from_slice(&(chunk.len() as u32).to_le_bytes());
        page[PAGE_HEADER..PAGE_HEADER + chunk.len()].copy_from_slice(chunk);
        let checksum = crc32c::crc32c(
            &page[4..20]
                .iter()
                .chain(&page[PAGE_HEADER..])
                .copied()
                .collect::<Vec<_>>(),
        );
        page[20..24].copy_from_slice(&checksum.to_le_bytes());
        write_faultable(&mut file, &page, faults, "write page artifact")?;
    }
    faults.check(FaultPoint::DataSync)?;
    file.sync_all()
        .map_err(|source| io_error("sync page artifact", source))?;
    Ok((page_count * PAGE_BYTES) as u64)
}

pub fn read_pages(path: &Path, generation: u64, logical_len: u64) -> Result<Vec<u8>> {
    let mut file = File::open(path).map_err(|source| io_error("open page artifact", source))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| io_error("read page artifact", source))?;
    if bytes.is_empty() || bytes.len() % PAGE_BYTES != 0 {
        return corruption("page artifact is not a nonzero page multiple");
    }
    let mut payload = Vec::new();
    for (page_no, page) in bytes.as_chunks::<PAGE_BYTES>().0.iter().enumerate() {
        let page_generation = u64::from_le_bytes(page[4..12].try_into().expect("generation width"));
        let stored_page = u32::from_le_bytes(page[12..16].try_into().expect("page width"));
        let length = u32::from_le_bytes(page[16..20].try_into().expect("length width")) as usize;
        let expected = u32::from_le_bytes(page[20..24].try_into().expect("checksum width"));
        let actual = crc32c::crc32c(
            &page[4..20]
                .iter()
                .chain(&page[PAGE_HEADER..])
                .copied()
                .collect::<Vec<_>>(),
        );
        if page[..4] != PAGE_MAGIC
            || page_generation != generation
            || stored_page as usize != page_no
        {
            return corruption("page identity mismatch");
        }
        if length > PAGE_PAYLOAD || actual != expected {
            return corruption("page length or checksum mismatch");
        }
        payload.extend_from_slice(&page[PAGE_HEADER..PAGE_HEADER + length]);
    }
    if payload.len()
        != usize::try_from(logical_len).map_err(|_| corruption_error("logical length overflow"))?
    {
        return corruption("manifest length disagrees with pages");
    }
    Ok(payload)
}

pub fn publish_manifest(
    path: &Path,
    manifest: Manifest,
    faults: &mut dyn FaultInjector,
) -> Result<()> {
    let temporary = path.with_extension("next");
    let mut file =
        File::create(&temporary).map_err(|source| io_error("create manifest", source))?;
    file.write_all(&encode_manifest(manifest))
        .map_err(|source| io_error("write manifest", source))?;
    faults.check(FaultPoint::ManifestSync)?;
    file.sync_all()
        .map_err(|source| io_error("sync manifest", source))?;
    std::fs::rename(&temporary, path).map_err(|source| io_error("publish manifest", source))?;
    sync_parent(path)
}

pub fn read_manifest(path: &Path) -> Result<Option<Manifest>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error("read manifest", source)),
    };
    if bytes.len() != MANIFEST_BYTES
        || bytes[..4] != MANIFEST_MAGIC
        || u32::from_le_bytes(bytes[4..8].try_into().expect("manifest version width"))
            != MANIFEST_VERSION
    {
        return corruption("manifest length or magic mismatch");
    }
    let expected = u32::from_le_bytes(bytes[40..44].try_into().expect("manifest checksum width"));
    if crc32c::crc32c(&bytes[..40]) != expected {
        return corruption("manifest checksum mismatch");
    }
    Ok(Some(Manifest {
        generation: u64::from_le_bytes(bytes[8..16].try_into().expect("generation width")),
        commit: u64::from_le_bytes(bytes[16..24].try_into().expect("commit width")),
        logical_len: u64::from_le_bytes(bytes[24..32].try_into().expect("length width")),
        payload_checksum: u32::from_le_bytes(
            bytes[32..36].try_into().expect("payload checksum width"),
        ),
        range_checksum: u32::from_le_bytes(bytes[36..40].try_into().expect("range checksum width")),
    }))
}

pub fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| DbError::InvalidState("artifact has no parent".to_owned()))?;
    let directory = OpenOptions::new()
        .read(true)
        .open(parent)
        .map_err(|source| io_error("open artifact directory", source))?;
    directory
        .sync_all()
        .map_err(|source| io_error("sync artifact directory", source))
}

fn encode_manifest(manifest: Manifest) -> [u8; MANIFEST_BYTES] {
    let mut bytes = [0_u8; MANIFEST_BYTES];
    bytes[..4].copy_from_slice(&MANIFEST_MAGIC);
    bytes[4..8].copy_from_slice(&MANIFEST_VERSION.to_le_bytes());
    bytes[8..16].copy_from_slice(&manifest.generation.to_le_bytes());
    bytes[16..24].copy_from_slice(&manifest.commit.to_le_bytes());
    bytes[24..32].copy_from_slice(&manifest.logical_len.to_le_bytes());
    bytes[32..36].copy_from_slice(&manifest.payload_checksum.to_le_bytes());
    bytes[36..40].copy_from_slice(&manifest.range_checksum.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..40]);
    bytes[40..44].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn write_faultable(
    file: &mut File,
    bytes: &[u8],
    faults: &mut dyn FaultInjector,
    operation: &'static str,
) -> Result<()> {
    if let Err(error) = faults.check(FaultPoint::ShortWrite) {
        file.write_all(&bytes[..bytes.len().min(7)])
            .map_err(|source| io_error(operation, source))?;
        return Err(error);
    }
    if let Err(error) = faults.check(FaultPoint::TornWrite) {
        file.write_all(&bytes[..bytes.len() / 2])
            .map_err(|source| io_error(operation, source))?;
        return Err(error);
    }
    file.write_all(bytes)
        .map_err(|source| io_error(operation, source))
}

fn io_error(operation: &'static str, source: std::io::Error) -> DbError {
    DbError::Io { operation, source }
}

fn corruption<T>(reason: &str) -> Result<T> {
    Err(corruption_error(reason))
}

fn corruption_error(reason: &str) -> DbError {
    DbError::Corruption {
        artifact: "checkpoint artifact",
        reason: reason.to_owned(),
    }
}
