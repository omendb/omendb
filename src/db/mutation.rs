//! Shared logical mutation application for the live and recovery paths.

use crate::blob::BlobManager;
use crate::btree::{BTree, LookupResult};
use crate::error::{Error, Result};

/// A byte mutation applied to one candidate B-tree/blob state.
pub(super) enum Mutation<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

/// Effects observed while applying one mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MutationOutcome {
    /// Whether a delete removed a B-tree entry. Puts always report `true`.
    pub changed: bool,
    /// Whether the durable blob image or its deletion metadata changed.
    pub blob_changed: bool,
    /// Whether an old blob pointer, when present, was successfully retired.
    /// The batch path uses this to preserve its fail-closed candidate check.
    pub blob_deletion_succeeded: bool,
}

/// Apply one logical mutation to a candidate state.
///
/// This is the single owner of B-tree/blob replacement semantics. Callers
/// remain responsible for validation, WAL admission, candidate ownership,
/// and publication ordering.
pub(super) fn apply(
    mutation: Mutation<'_>,
    btree: &mut BTree,
    blobs: &mut BlobManager,
) -> Result<MutationOutcome> {
    match mutation {
        Mutation::Put { key, value } => apply_put(key, value, btree, blobs),
        Mutation::Delete { key } => apply_delete(key, btree, blobs),
    }
}

fn apply_put(
    key: &[u8],
    value: &[u8],
    btree: &mut BTree,
    blobs: &mut BlobManager,
) -> Result<MutationOutcome> {
    let previous_blob = match btree.lookup(key)? {
        LookupResult::Blob(pointer) => Some(pointer),
        _ => None,
    };
    let separates = blobs.should_separate(value.len());

    if separates {
        let pointer = blobs.append(key, value.to_vec());
        if let Err(error) = btree.upsert_blob(key, pointer) {
            let _ = blobs.rollback_append(&pointer);
            return Err(error.into());
        }
    } else {
        btree.upsert(key, value)?;
    }

    let blob_deletion_succeeded = previous_blob.is_none_or(|pointer| blobs.mark_deleted(&pointer));
    Ok(MutationOutcome {
        changed: true,
        blob_changed: separates || previous_blob.is_some(),
        blob_deletion_succeeded,
    })
}

fn apply_delete(key: &[u8], btree: &mut BTree, blobs: &mut BlobManager) -> Result<MutationOutcome> {
    let previous_blob = match btree.lookup(key)? {
        LookupResult::Blob(pointer) => Some(pointer),
        _ => None,
    };
    let changed = btree.delete(key)?;
    let blob_changed = changed && previous_blob.is_some();
    let blob_deletion_succeeded = match (blob_changed, previous_blob) {
        (true, Some(pointer)) => blobs.mark_deleted(&pointer),
        _ => true,
    };

    Ok(MutationOutcome {
        changed,
        blob_changed,
        blob_deletion_succeeded,
    })
}

/// Turn a failed old-blob retirement into the same corruption class used by
/// the batch candidate path.
pub(super) fn require_blob_deletion(outcome: MutationOutcome, context: &str) -> Result<()> {
    if outcome.blob_deletion_succeeded {
        Ok(())
    } else {
        Err(Error::Corruption(format!(
            "{context} references a missing or already-deleted blob"
        )))
    }
}
