//! Durable reuse-attempt ledger.
//!
//! The ledger reserves publication identities and physical page offsets before
//! candidate writes. It is kept separate from manifest and history codecs so
//! recovery ownership follows the persisted artifact it protects.

use super::{CommitId, FORMAT_VERSION, GenerationId, ManifestHistory};
use std::collections::BTreeSet;

/// One publication attempt recorded before page writes begin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReuseAttempt {
    /// Logical commit identity reserved by this generation.
    pub commit_id: CommitId,
    /// Generation that may overwrite the listed physical offsets.
    pub generation_id: GenerationId,
    /// Physical page slots selected for this generation. This may be empty
    /// when the entry exists only to reserve an ambiguous commit identity.
    pub offsets: Vec<u64>,
}

/// Durable ledger of publication attempts whose outcome may be
/// indeterminate after a process or filesystem failure.
///
/// Entries are written before any candidate page reaches the device. A
/// successful publication is removed from the in-memory ledger after its
/// manifest is durable. For reused slots, on-disk cleanup may be deferred until
/// the next publication or reopen; both reconcile entries against authoritative
/// manifest history. An entry absent from history is retained so its reserved
/// identity is never reused; non-empty entries also cause historical reads to
/// fail closed for their offsets.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReuseLedger {
    attempts: Vec<ReuseAttempt>,
}

const REUSE_LEDGER_MAGIC: [u8; 8] = *b"SEERREU1";
const REUSE_LEDGER_HEADER_SIZE: usize = 8 + 4 + 4;
const REUSE_LEDGER_ATTEMPT_HEADER_SIZE: usize = 8 + 8 + 4;
const REUSE_LEDGER_CHECKSUM_SIZE: usize = 4;

impl ReuseLedger {
    /// Create an empty reuse ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return attempts in generation order.
    pub fn attempts(&self) -> &[ReuseAttempt] {
        &self.attempts
    }

    /// Record one generation's candidate offsets.
    pub fn push(&mut self, attempt: ReuseAttempt) -> std::result::Result<(), &'static str> {
        let mut offsets = BTreeSet::new();
        if attempt
            .offsets
            .iter()
            .any(|offset| !offsets.insert(*offset))
        {
            return Err("reuse ledger contains duplicate offsets");
        }
        if let Some(existing) = self
            .attempts
            .iter_mut()
            .find(|existing| existing.generation_id == attempt.generation_id)
        {
            if existing.commit_id != attempt.commit_id {
                return Err("reuse ledger generation has conflicting commit IDs");
            }
            existing.offsets.extend(attempt.offsets);
            existing.offsets.sort_unstable();
            existing.offsets.dedup();
            return Ok(());
        }
        self.attempts.push(attempt);
        self.attempts
            .sort_unstable_by_key(|attempt| attempt.generation_id);
        Ok(())
    }

    /// Remove an attempt after its generation is durably published.
    pub fn remove_generation(&mut self, generation_id: GenerationId) -> bool {
        let before = self.attempts.len();
        self.attempts
            .retain(|attempt| attempt.generation_id != generation_id);
        before != self.attempts.len()
    }

    /// Remove entries reconciled by authoritative history.
    ///
    /// A non-empty attempt remains until its exact generation is published,
    /// because its physical offsets may still overlap an older retained root.
    /// An empty reservation has no physical liveness obligation and can be
    /// removed once a later commit proves that its identity was skipped.
    pub fn prune_published(&mut self, history: &ManifestHistory) -> usize {
        let before = self.attempts.len();
        self.attempts.retain(|attempt| {
            let generation_published = history
                .manifests()
                .iter()
                .any(|manifest| manifest.generation_id == attempt.generation_id);
            let empty_reservation_superseded = attempt.offsets.is_empty()
                && history
                    .manifests()
                    .iter()
                    .any(|manifest| manifest.commit_id > attempt.commit_id);
            !(generation_published || empty_reservation_superseded)
        });
        before.saturating_sub(self.attempts.len())
    }

    /// Encode the exact-length checksummed ledger envelope.
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let count = u32::try_from(self.attempts.len()).ok()?;
        let body_len =
            self.attempts
                .iter()
                .try_fold(REUSE_LEDGER_HEADER_SIZE, |length, attempt| {
                    let offset_bytes = attempt.offsets.len().checked_mul(8)?;
                    length
                        .checked_add(REUSE_LEDGER_ATTEMPT_HEADER_SIZE)?
                        .checked_add(offset_bytes)
                })?;
        let total_len = body_len.checked_add(REUSE_LEDGER_CHECKSUM_SIZE)?;
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&REUSE_LEDGER_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        for attempt in &self.attempts {
            bytes.extend_from_slice(&attempt.commit_id.get().to_le_bytes());
            bytes.extend_from_slice(&attempt.generation_id.get().to_le_bytes());
            bytes.extend_from_slice(&u32::try_from(attempt.offsets.len()).ok()?.to_le_bytes());
            for offset in &attempt.offsets {
                bytes.extend_from_slice(&offset.to_le_bytes());
            }
        }
        let checksum = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Some(bytes)
    }

    /// Decode and validate a reuse ledger envelope.
    pub fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, &'static str> {
        let (ledger, consumed) = Self::decode_envelope(bytes)?;
        if consumed != bytes.len() {
            return Err("reuse ledger has trailing bytes");
        }
        Ok(ledger)
    }

    /// Recover the newest valid envelope from an append-only ledger file.
    ///
    /// Each persist appends one exact-length checksummed envelope holding the
    /// full ledger state; the last envelope that parses completely wins. A
    /// torn final append (crash between write and sync) is tolerated by
    /// falling back to the previous envelope. Only a bad magic in the first
    /// envelope fails closed: that means the file is not a ledger at all.
    pub fn scan_latest(bytes: &[u8]) -> std::result::Result<Self, &'static str> {
        let mut cursor = 0usize;
        let mut latest = Self::new();
        while bytes.len() >= cursor + REUSE_LEDGER_HEADER_SIZE + REUSE_LEDGER_CHECKSUM_SIZE {
            match Self::decode_envelope(&bytes[cursor..]) {
                Ok((ledger, length)) => {
                    latest = ledger;
                    cursor += length;
                }
                Err(err) => {
                    if cursor == 0 && err == "invalid reuse ledger magic" {
                        return Err(err);
                    }
                    break;
                }
            }
        }
        Ok(latest)
    }

    fn decode_envelope(bytes: &[u8]) -> std::result::Result<(Self, usize), &'static str> {
        if bytes.len() < REUSE_LEDGER_HEADER_SIZE + REUSE_LEDGER_CHECKSUM_SIZE {
            return Err("reuse ledger is truncated");
        }
        if bytes[..8] != REUSE_LEDGER_MAGIC {
            return Err("invalid reuse ledger magic");
        }
        let version = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .map_err(|_| "reuse ledger version is truncated")?,
        );
        if version != FORMAT_VERSION {
            return Err("unsupported reuse ledger format version");
        }
        let count = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| "reuse ledger count is truncated")?,
        ) as usize;

        let mut cursor = REUSE_LEDGER_HEADER_SIZE;
        let mut ledger = Self::new();
        for _ in 0..count {
            let header_end = cursor
                .checked_add(REUSE_LEDGER_ATTEMPT_HEADER_SIZE)
                .ok_or("reuse ledger length overflows")?;
            if header_end > bytes.len() - REUSE_LEDGER_CHECKSUM_SIZE {
                return Err("reuse ledger attempt is truncated");
            }
            let commit_id = CommitId::new(u64::from_le_bytes(
                bytes[cursor..cursor + 8]
                    .try_into()
                    .map_err(|_| "reuse ledger commit is truncated")?,
            ));
            let generation_id = GenerationId::new(u64::from_le_bytes(
                bytes[cursor + 8..cursor + 16]
                    .try_into()
                    .map_err(|_| "reuse ledger generation is truncated")?,
            ));
            let offset_count = u32::from_le_bytes(
                bytes[cursor + 16..header_end]
                    .try_into()
                    .map_err(|_| "reuse ledger offset count is truncated")?,
            ) as usize;
            cursor = header_end;
            let offset_bytes = offset_count
                .checked_mul(8)
                .ok_or("reuse ledger offset length overflows")?;
            let offsets_end = cursor
                .checked_add(offset_bytes)
                .ok_or("reuse ledger length overflows")?;
            if offsets_end > bytes.len() - REUSE_LEDGER_CHECKSUM_SIZE {
                return Err("reuse ledger offsets are truncated");
            }
            let mut offsets = Vec::with_capacity(offset_count);
            for chunk in bytes[cursor..offsets_end].as_chunks::<8>().0 {
                offsets.push(u64::from_le_bytes(*chunk));
            }
            cursor = offsets_end;
            ledger.push(ReuseAttempt {
                commit_id,
                generation_id,
                offsets,
            })?;
        }

        let envelope_len = cursor + REUSE_LEDGER_CHECKSUM_SIZE;
        let expected = u32::from_le_bytes(
            bytes[cursor..envelope_len]
                .try_into()
                .map_err(|_| "reuse ledger checksum is truncated")?,
        );
        if expected != crc32c::crc32c(&bytes[..cursor]) {
            return Err("reuse ledger checksum mismatch");
        }
        Ok((ledger, envelope_len))
    }
}
