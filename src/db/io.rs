//! Bounded positional reads and primitive decoding for DB artifact readers.
//!
//! This module owns the platform-specific file-positioning seam used by blob
//! read views and their private recovery fixtures. It does not own artifact
//! layout, publication, or mutable database state.

use super::{Error, Result};
use std::fs::File;

#[cfg(not(any(unix, windows)))]
use std::io::{Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::FileExt as PositionalFileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt as PositionalFileExt;

pub(crate) fn read_exact_at(file: &File, offset: u64, buffer: &mut [u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut filled = 0;
        while filled < buffer.len() {
            let count = file.read_at(&mut buffer[filled..], offset + filled as u64)?;
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "file read reached end of file",
                ));
            }
            filled += count;
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        let mut filled = 0;
        while filled < buffer.len() {
            let count = file.seek_read(&mut buffer[filled..], offset + filled as u64)?;
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "file read reached end of file",
                ));
            }
            filled += count;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let mut cloned = file.try_clone()?;
        cloned.seek(SeekFrom::Start(offset))?;
        cloned.read_exact(buffer)
    }
}

pub(crate) fn decode_u32(bytes: &[u8]) -> Result<u32> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| Error::Corruption("truncated blob integer".into()))?;
    Ok(u32::from_le_bytes(bytes))
}

pub(crate) fn decode_u64(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::Corruption("truncated blob integer".into()))?;
    Ok(u64::from_le_bytes(bytes))
}
