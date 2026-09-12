//! Concurrent metadata and lifetime state for vNext buffer frames.
//!
//! Page bytes deliberately live in `buffer`; this module proves the state,
//! pinning, anti-ABA, optimistic-version, dirty, writeback, and eviction
//! invariants independently of any particular page representation.

use super::ids::FrameIncarnation;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};

/// Lifecycle state of one fixed buffer-frame slot.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum FrameState {
    Free = 0,
    Loading = 1,
    Resident = 2,
    Writeback = 3,
    Evicting = 4,
}

impl FrameState {
    fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Free,
            1 => Self::Loading,
            2 => Self::Resident,
            3 => Self::Writeback,
            4 => Self::Evicting,
            _ => unreachable!("frame state is only written from FrameState"),
        }
    }
}

/// Stable optimistic image version captured by a reader.
///
/// Even versions are stable. Odd versions are owned by a writer.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FrameVersion(u64);

impl FrameVersion {
    /// Return the raw monotonically increasing version.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Rejected frame-state transition.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum FrameTransitionError {
    #[error("frame state transition expected {expected:?}, found {actual:?}")]
    WrongState {
        expected: FrameState,
        actual: FrameState,
    },
    #[error("frame is pinned")]
    Pinned,
    #[error("frame is dirty")]
    Dirty,
    #[error("frame is clean")]
    Clean,
    #[error("frame already has a writer")]
    WriteBusy,
    #[error("frame incarnation counter is exhausted")]
    IncarnationExhausted,
}

/// Atomic lifecycle metadata for a single frame slot.
pub struct FrameMeta {
    state: AtomicU8,
    pins: AtomicUsize,
    version: AtomicU64,
    incarnation: AtomicU64,
    dirty: AtomicBool,
}

impl FrameMeta {
    /// Construct an unused frame.
    #[must_use]
    pub const fn new_free() -> Self {
        Self {
            state: AtomicU8::new(FrameState::Free as u8),
            pins: AtomicUsize::new(0),
            version: AtomicU64::new(0),
            incarnation: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
        }
    }

    /// Construct a resident frame for tests and isolated metadata users.
    #[must_use]
    pub const fn new_resident() -> Self {
        Self {
            state: AtomicU8::new(FrameState::Resident as u8),
            pins: AtomicUsize::new(0),
            version: AtomicU64::new(0),
            incarnation: AtomicU64::new(1),
            dirty: AtomicBool::new(false),
        }
    }

    /// Return the current lifecycle state.
    #[must_use]
    pub fn state(&self) -> FrameState {
        FrameState::from_raw(self.state.load(Ordering::Acquire))
    }

    /// Return the number of live pins.
    #[must_use]
    pub fn pin_count(&self) -> usize {
        self.pins.load(Ordering::Acquire)
    }

    /// Return whether the resident image has been modified.
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Return the slot's current incarnation, if it has ever hosted a page.
    #[must_use]
    pub fn incarnation(&self) -> Option<FrameIncarnation> {
        FrameIncarnation::new(self.incarnation.load(Ordering::Acquire))
    }

    /// Reserve a free slot for loading a new logical page.
    ///
    /// The returned incarnation is unique for this slot until the process ends.
    pub fn begin_load(&self) -> Result<FrameIncarnation, FrameTransitionError> {
        self.transition(FrameState::Free, FrameState::Loading)?;

        let current = self.incarnation.load(Ordering::Relaxed);
        let Some(next) = current.checked_add(1).and_then(FrameIncarnation::new) else {
            self.state
                .store(FrameState::Free as u8, Ordering::Release);
            return Err(FrameTransitionError::IncarnationExhausted);
        };
        self.incarnation.store(next.get(), Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        Ok(next)
    }

    /// Publish successfully loaded bytes as resident.
    pub fn finish_load(&self) -> Result<(), FrameTransitionError> {
        self.transition(FrameState::Loading, FrameState::Resident)
    }

    /// Abandon a failed load and make the frame reusable.
    pub fn abort_load(&self) -> Result<(), FrameTransitionError> {
        self.dirty.store(false, Ordering::Release);
        self.transition(FrameState::Loading, FrameState::Free)
    }

    /// Pin a resident frame.
    ///
    /// The state and incarnation are rechecked after incrementing the pin count.
    /// If eviction or reuse raced with us, the transient pin is immediately
    /// returned and the caller observes a miss instead of a stale page.
    #[must_use]
    pub fn try_pin(&self) -> Option<FramePin<'_>> {
        if self.state() != FrameState::Resident {
            return None;
        }
        let incarnation = self.incarnation()?;
        self.pins.fetch_add(1, Ordering::AcqRel);
        if self.state() != FrameState::Resident || self.incarnation() != Some(incarnation) {
            self.pins.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(FramePin {
            meta: self,
            incarnation,
        })
    }

    /// Begin conservative writeback of a dirty, unpinned resident image.
    ///
    /// After winning the state CAS we drain only pins that raced between the
    /// pre-CAS zero-pin check and the CAS. A pin that was already established
    /// before the check would have made the transition fail. This closes the
    /// subtle window where writeback could otherwise start while a failed
    /// racing pin was still unwinding.
    pub fn try_begin_writeback(&self) -> Result<(), FrameTransitionError> {
        if !self.is_dirty() {
            return Err(FrameTransitionError::Clean);
        }
        self.begin_exclusive_state(FrameState::Writeback)
    }

    /// Complete successful writeback.
    ///
    /// Dirty is cleared before the frame is made resident again, so no new pin
    /// can observe a stale dirty bit for an already-materialized image.
    pub fn finish_writeback(&self) -> Result<(), FrameTransitionError> {
        let actual = self.state();
        if actual != FrameState::Writeback {
            return Err(FrameTransitionError::WrongState {
                expected: FrameState::Writeback,
                actual,
            });
        }
        self.dirty.store(false, Ordering::Release);
        self.transition(FrameState::Writeback, FrameState::Resident)
    }

    /// Reopen a frame after failed writeback while preserving dirty state.
    pub fn abort_writeback(&self) -> Result<(), FrameTransitionError> {
        self.transition(FrameState::Writeback, FrameState::Resident)
    }

    /// Begin eviction of a clean, unpinned resident frame.
    pub fn try_begin_evict(&self) -> Result<(), FrameTransitionError> {
        if self.is_dirty() {
            return Err(FrameTransitionError::Dirty);
        }
        self.begin_exclusive_state(FrameState::Evicting)
    }

    /// Finish eviction and return the slot to the free list.
    pub fn finish_evict(&self) -> Result<(), FrameTransitionError> {
        self.transition(FrameState::Evicting, FrameState::Free)
    }

    fn begin_exclusive_state(&self, next: FrameState) -> Result<(), FrameTransitionError> {
        if self.pin_count() != 0 {
            return Err(FrameTransitionError::Pinned);
        }
        self.transition(FrameState::Resident, next)?;

        // A pin may have observed Resident immediately before our CAS and then
        // incremented after the zero-pin check. Because state is now nonresident,
        // that pin must fail its post-increment validation and decrement again.
        let mut spins = 0usize;
        while self.pin_count() != 0 {
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
            spins = spins.saturating_add(1);
        }
        Ok(())
    }

    fn transition(
        &self,
        expected: FrameState,
        next: FrameState,
    ) -> Result<(), FrameTransitionError> {
        self.state
            .compare_exchange(
                expected as u8,
                next as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|actual| FrameTransitionError::WrongState {
                expected,
                actual: FrameState::from_raw(actual),
            })
    }
}

impl Default for FrameMeta {
    fn default() -> Self {
        Self::new_free()
    }
}

/// RAII pin keeping one frame incarnation resident.
pub struct FramePin<'a> {
    meta: &'a FrameMeta,
    incarnation: FrameIncarnation,
}

impl FramePin<'_> {
    /// Return the frame incarnation this pin protects.
    #[must_use]
    pub const fn incarnation(&self) -> FrameIncarnation {
        self.incarnation
    }

    /// Capture a stable optimistic image version.
    #[must_use]
    pub fn optimistic_version(&self) -> Option<FrameVersion> {
        let version = self.meta.version.load(Ordering::Acquire);
        (version & 1 == 0).then_some(FrameVersion(version))
    }

    /// Validate that a previously captured stable image did not change.
    #[must_use]
    pub fn validate(&self, version: FrameVersion) -> bool {
        version.0 & 1 == 0
            && self.meta.version.load(Ordering::Acquire) == version.0
            && self.meta.state() == FrameState::Resident
            && self.meta.incarnation() == Some(self.incarnation)
    }

    /// Obtain exclusive optimistic writer ownership.
    ///
    /// Successful acquisition marks the page dirty before mutation can begin.
    pub fn try_write(&self) -> Result<FrameWriteLatch<'_>, FrameTransitionError> {
        if self.meta.state() != FrameState::Resident
            || self.meta.incarnation() != Some(self.incarnation)
        {
            return Err(FrameTransitionError::WrongState {
                expected: FrameState::Resident,
                actual: self.meta.state(),
            });
        }

        let mut version = self.meta.version.load(Ordering::Acquire);
        loop {
            if version & 1 != 0 {
                return Err(FrameTransitionError::WriteBusy);
            }
            match self.meta.version.compare_exchange_weak(
                version,
                version.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.meta.dirty.store(true, Ordering::Release);
                    return Ok(FrameWriteLatch {
                        meta: self.meta,
                        version: FrameVersion(version.wrapping_add(1)),
                    });
                }
                Err(observed) => version = observed,
            }
        }
    }
}

impl Drop for FramePin<'_> {
    fn drop(&mut self) {
        self.meta.pins.fetch_sub(1, Ordering::AcqRel);
    }
}

/// RAII ownership of an odd optimistic frame version.
pub struct FrameWriteLatch<'a> {
    meta: &'a FrameMeta,
    version: FrameVersion,
}

impl FrameWriteLatch<'_> {
    /// Return the odd version owned by this writer.
    #[must_use]
    pub const fn version(&self) -> FrameVersion {
        self.version
    }
}

impl Drop for FrameWriteLatch<'_> {
    fn drop(&mut self) {
        self.meta.version.fetch_add(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_publish_evict_and_reuse_advances_incarnation() {
        let frame = FrameMeta::new_free();
        let first = frame.begin_load().expect("free frame loads");
        assert_eq!(first.get(), 1);
        frame.finish_load().expect("load publishes");
        frame.try_begin_evict().expect("clean frame evicts");
        frame.finish_evict().expect("eviction frees frame");
        let second = frame.begin_load().expect("frame can be reused");
        assert_eq!(second.get(), 2);
    }

    #[test]
    fn live_pin_blocks_writeback_and_eviction() {
        let frame = FrameMeta::new_resident();
        let pin = frame.try_pin().expect("resident frame pins");
        let writer = pin.try_write().expect("writer acquires");
        drop(writer);
        assert_eq!(frame.try_begin_writeback(), Err(FrameTransitionError::Pinned));
        assert_eq!(frame.try_begin_evict(), Err(FrameTransitionError::Dirty));
        drop(pin);
        frame.try_begin_writeback().expect("unpinned dirty frame writes");
        frame.finish_writeback().expect("writeback completes");
        frame.try_begin_evict().expect("clean unpinned frame evicts");
    }

    #[test]
    fn failed_writeback_reopens_resident_and_keeps_dirty() {
        let frame = FrameMeta::new_resident();
        let pin = frame.try_pin().expect("resident frame pins");
        drop(pin.try_write().expect("writer acquires"));
        drop(pin);

        frame.try_begin_writeback().expect("writeback begins");
        frame.abort_writeback().expect("writeback aborts");
        assert_eq!(frame.state(), FrameState::Resident);
        assert!(frame.is_dirty());
    }

    #[test]
    fn writer_invalidates_optimistic_reader() {
        let frame = FrameMeta::new_resident();
        let reader = frame.try_pin().expect("reader pins");
        let before = reader.optimistic_version().expect("stable version");
        let writer_pin = frame.try_pin().expect("writer pins");
        let writer = writer_pin.try_write().expect("writer acquires");
        assert!(reader.optimistic_version().is_none());
        drop(writer);
        assert!(!reader.validate(before));
        let after = reader.optimistic_version().expect("stable after writer");
        assert_eq!(after.get(), before.get() + 2);
        assert!(reader.validate(after));
    }

    #[test]
    fn second_writer_is_rejected() {
        let frame = FrameMeta::new_resident();
        let first_pin = frame.try_pin().expect("first pins");
        let second_pin = frame.try_pin().expect("second pins");
        let first = first_pin.try_write().expect("first writes");
        assert!(matches!(
            second_pin.try_write(),
            Err(FrameTransitionError::WriteBusy)
        ));
        drop(first);
        assert!(second_pin.try_write().is_ok());
    }
}
