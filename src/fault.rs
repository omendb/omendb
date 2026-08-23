use std::collections::BTreeSet;

use crate::{DbError, Result};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FaultPoint {
    BeforeWalAppend,
    AfterWalAppend,
    WalSync,
    AfterWalSync,
    DataSync,
    PackedPageSync,
    ManifestMirrorSync,
    SchemaJournalSync,
    ManifestSync,
    AfterManifestPublish,
    WalTruncate,
    DuringRecovery,
    DuringCompaction,
    ShortWrite,
    TornWrite,
}

pub trait FaultInjector {
    fn check(&mut self, point: FaultPoint) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoFaults;

impl FaultInjector for NoFaults {
    fn check(&mut self, _point: FaultPoint) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct FailOnce {
    armed: BTreeSet<FaultPoint>,
    fired: BTreeSet<FaultPoint>,
}

impl FailOnce {
    #[must_use]
    pub fn at(points: impl IntoIterator<Item = FaultPoint>) -> Self {
        Self {
            armed: points.into_iter().collect(),
            fired: BTreeSet::new(),
        }
    }
}

impl FaultInjector for FailOnce {
    fn check(&mut self, point: FaultPoint) -> Result<()> {
        if self.armed.contains(&point) && self.fired.insert(point) {
            return Err(DbError::InjectedFailure(point));
        }
        Ok(())
    }
}
