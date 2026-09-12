//! Concurrent frame lifecycle and guard metadata for the vNext buffer manager.
//!
//! This module intentionally does not expose page bytes yet. It establishes the
//! state machine that future frame guards must obey before unsafe borrowed page
//! views are introduced.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};

/// Lifecycle of one process-local buffer frame.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum FrameState {
    /// The frame has no page identity or usable contents.
    Free = 0,
    /// I/O is filling a reserved frame; it is not visible to page lookups yet.
    Loading = 1,
    /// The frame contains a resident page and may be pinned by readers/writers.
    Resident = 2,
    /// A stable dirty image is being written out; new pins are temporarily refused.
    Writeback = 3,
    /// The frame has been detached from lookup and is waiting to become free.
    Evicting = 4,
}

impl FrameState {
    fn decode(raw: u8) -> Self {
        match raw {
            0 => Self::Free,
            1 => Self::Loading,
            2 => Self::Resident,
            3 => Self::Writeback,
            4 => Self::Evicting,
            _ => unreachable!("vNext frame state is written only by FrameMeta"),
        }
    }

    const fn may_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Free, Self::Loading)
                | (Self::Loading, Self::Resident)
                | (Self::Loading, Self::Free)
                | (Self::Resident, Self::Writeback)
                | (Self::Resident, Self::Evicting)
                | (Self::Writeback, Self::Resident)
                | (Self::Evicting, Self::Free)
        )
    }
}

/// Stable optimistic version observed while a frame is resident.
///
/// Even values represent quiescent page contents. A writer temporarily makes
/// the version odd and advances it to the next even value on release.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FrameVersion(u64);

impl FrameVersion {
    /// Return the raw version counter for diagnostics and tests.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Why a lifecycle operation could not advance a frame.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum FrameTransitionError {
    /// The frame was not in the state required by the attempted operation.
    #[error("frame state changed: expected {expected:?}, found {actual:?}")]
    WrongState {
        expected: FrameState,
        actual: FrameState,
    },
    /// The requested lifecycle edge is not part of the vNext frame protocol.
    #[error("invalid frame transition from {from:?} to {to:?}")]
    InvalidTransition { from: FrameState, to: FrameState },
    /// A live pin still protects the frame from writeback completion or reuse.
    #[error("frame is still pinned")]
    Pinned,
    /// Writeback was requested for a clean frame.
    #[error("clean frame has nothing to write back")]
    Clean,
    /// Clean eviction was requested for a dirty frame.
    #[error("dirty frame requires writeback before eviction")]
    Dirty,
    /// Another writer owns the optimistic write latch.
    #[error("frame already has an active writer")]
    WriteBusy,
    /// The optimistic version counter cannot advance without wrapping.
    #[error("frame version counter is exhausted")]
    VersionExhausted,
}

/// Concurrent metadata owned by one resident-frame slot.
///
/// Page bytes and page identity are added by the buffer-manager milestone. This
/// type owns only lifecycle synchronization so its races can be tested in
/// isolation.
pub struct FrameMeta {
    state: AtomicU8,
    pins: AtomicUsize,
    version: AtomicU64,
    dirty: AtomicBool,
}

impl FrameMeta {
    /// Construct an unused frame slot.
    #[must_use]
    pub const fn new_free() -> Self {
        Self {
            state: AtomicU8::new(FrameState::Free as u8),
            pins: AtomicUsize::new(0),
            version: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
        }
    }

    /// Construct a resident clean frame for focused tests and bootstrap paths.
    #[must_use]
    pub const fn new_resident() -> Self {
        Self {
            state: AtomicU8::new(FrameState::Resident as u8),
            pins: AtomicUsize::new(0),
            version: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
        }
    }

    /// Return the current lifecycle state.
    #[must_use]
    pub fn state(&self) -> FrameState {
        FrameState::decode(self.state.load(Ordering::Acquire))
    }

    /// Return the current pin count for diagnostics/admission decisions.
    #[must_use]
    pub fn pin_count(&self) -> usize {
        self.pins.load(Ordering::Acquire)
    }

    /// Return whether the resident image contains unmaterialized changes.
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Reserve a free frame for page loading.
    pub fn begin_load(&self) -> Result<(), FrameTransitionError> {
        self.transition(FrameState::Free, FrameState::Loading)
    }

    /// Publish a completely initialized loaded frame to page lookups.
    pub fn finish_load(&self) -> Result<(), FrameTransitionError> {
        self.transition(FrameState::Loading, FrameState::Resident)
    }

    /// Return a failed load reservation to the free pool.
    pub fn abort_load(&self) -> Result<(), FrameTransitionError> {
        self.transition(FrameState::Loading, FrameState::Free)
    }

    /// Pin a resident frame against eviction/writeback transition.
    ///
    /// A concurrent evictor may win between the initial state observation and
    /// pin increment. The second state check detects that race and rolls the pin
    /// back before returning `None`.
    pub fn try_pin(&self) -> Option<FramePin<'_>> {
        if self.state() != FrameState::Resident {
            return None;
        }
        self.pins.fetch_add(1, Ordering::AcqRel);
        if self.state() != FrameState::Resident {
            self.pins.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(FramePin { meta: self })
    }

    /// Freeze an unpinned dirty frame for writeback.
    ///
    /// The later buffer manager may relax this by copying an image under a
    /// page latch, but the first correctness baseline intentionally refuses
    /// writeback while any guard can still mutate or borrow the frame.
    pub fn try_begin_writeback(&self) -> Result<(), FrameTransitionError> {
        let state = self.state();
        if state != FrameState::Resident {
            return Err(FrameTransitionError::WrongState {
                expected: FrameState::Resident,
                actual: state,
            });
        }
        if self.pin_count() != 0 {
            return Err(FrameTransitionError::Pinned);
        }
        if !self.is_dirty() {
            return Err(FrameTransitionError::Clean);
        }
        self.transition(FrameState::Resident, FrameState::Writeback)
    }

    /// Mark a successful stable writeback clean and reopen the frame to pins.
    pub fn finish_writeback(&self) -> Result<(), FrameTransitionError> {
        if self.pin_count() != 0 {
            return Err(FrameTransitionError::Pinned);
        }
        let state = self.state();
        if state != FrameState::Writeback {
            return Err(FrameTransitionError::WrongState {
                expected: FrameState::Writeback,
                actual: state,
            });
        }

        // Clear dirty while pins are still excluded by Writeback state. If a
        // concurrent abort wins the state transition, restore dirty because the
        // writeback no longer proved the resident image durable.
        self.dirty.store(false, Ordering::Release);
        match self.state.compare_exchange(
            FrameState::Writeback as u8,
            FrameState::Resident as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(actual) => {
                self.dirty.store(true, Ordering::Release);
                Err(FrameTransitionError::WrongState {
                    expected: FrameState::Writeback,
                    actual: FrameState::decode(actual),
                })
            }
        }
    }

    /// Reopen a failed writeback while preserving the dirty bit for retry.
    pub fn abort_writeback(&self) -> Result<(), FrameTransitionError> {
        self.transition(FrameState::Writeback, FrameState::Resident)
    }

    /// Detach one clean unpinned frame from page lookup before reuse.
    pub fn try_begin_evict(&self) -> Result<(), FrameTransitionError> {
        let state = self.state();
        if state != FrameState::Resident {
            return Err(FrameTransitionError::WrongState {
                expected: FrameState::Resident,
                actual: state,
            });
        }
        if self.pin_count() != 0 {
            return Err(FrameTransitionError::Pinned);
        }
        if self.is_dirty() {
            return Err(FrameTransitionError::Dirty);
        }
        self.transition(FrameState::Resident, FrameState::Evicting)
    }

    /// Complete eviction after any racing failed pin attempt has drained.
    pub fn finish_evict(&self) -> Result<(), FrameTransitionError> {
        if self.pin_count() != 0 {
            return Err(FrameTransitionError::Pinned);
        }
        self.transition(FrameState::Evicting, FrameState::Free)
    }

    fn transition(
        &self,
        expected: FrameState,
        next: FrameState,
    ) -> Result<(), FrameTransitionError> {
        if !expected.may_transition_to(next) {
            return Err(FrameTransitionError::InvalidTransition {
                from: expected,
                to: next,
            });
        }
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
                actual: FrameState::decode(actual),
            })
    }
}

/// A resident-frame pin. Future page/record references borrow through this
/// lifetime so eviction cannot invalidate them.
pub struct FramePin<'a> {
    meta: &'a FrameMeta,
}

impl FramePin<'_> {
    /// Capture a stable optimistic-read version if no writer owns the frame.
    #[must_use]
    pub fn optimistic_version(&self) -> Option<FrameVersion> {
        if self.meta.state() != FrameState::Resident {
            return None;
        }
        let version = self.meta.version.load(Ordering::Acquire);
        if version & 1 == 1 {
            return None;
        }
        Some(FrameVersion(version))
    }

    /// Verify that an optimistic page read observed one unchanged stable image.
    #[must_use]
    pub fn validate(&self, observed: FrameVersion) -> bool {
        self.meta.state() == FrameState::Resident
            && observed.0 & 1 == 0
            && self.meta.version.load(Ordering::Acquire) == observed.0
    }

    /// Attempt to acquire exclusive mutation ownership of this pinned frame.
    pub fn try_write(&self) -> Result<FrameWriteLatch<'_>, FrameTransitionError> {
        let state = self.meta.state();
        if state != FrameState::Resident {
            return Err(FrameTransitionError::WrongState {
                expected: FrameState::Resident,
                actual: state,
            });
        }
        let version = self.meta.version.load(Ordering::Acquire);
        if version & 1 == 1 {
            return Err(FrameTransitionError::WriteBusy);
        }
        if version >= u64::MAX - 1 {
            return Err(FrameTransitionError::VersionExhausted);
        }
        match self.meta.version.compare_exchange(
            version,
            version + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Exclusive mutation ownership always implies eventual
                // writeback; callers cannot accidentally mutate a clean page
                // and forget to mark it dirty.
                self.meta.dirty.store(true, Ordering::Release);
                Ok(FrameWriteLatch { meta: self.meta })
            }
            Err(_) if self.meta.state() != FrameState::Resident => {
                Err(FrameTransitionError::WrongState {
                    expected: FrameState::Resident,
                    actual: self.meta.state(),
                })
            }
            Err(_) => Err(FrameTransitionError::WriteBusy),
        }
    }
}

impl Drop for FramePin<'_> {
    fn drop(&mut self) {
        let previous = self.meta.pins.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "frame pin count underflow");
    }
}

/// Exclusive writer token for a resident frame.
///
/// Dropping the token advances the optimistic version to the next even value,
/// invalidating every reader that captured the prior version.
pub struct FrameWriteLatch<'a> {
    meta: &'a FrameMeta,
}

impl FrameWriteLatch<'_> {
    /// Mark the frame dirty explicitly. Acquiring a write latch already does
    /// this; the method exists for code whose mutation point wants to state it.
    pub fn mark_dirty(&self) {
        self.meta.dirty.store(true, Ordering::Release);
    }
}

impl Drop for FrameWriteLatch<'_> {
    fn drop(&mut self) {
        let previous = self.meta.version.fetch_add(1, Ordering::Release);
        debug_assert!(previous & 1 == 1, "write latch must own an odd version");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_publish_and_clean_eviction_follow_explicit_edges() {
        let meta = FrameMeta::new_free();
        meta.begin_load().expect("reserve load");
        assert_eq!(meta.state(), FrameState::Loading);
        meta.finish_load().expect("publish load");
        assert_eq!(meta.state(), FrameState::Resident);
        meta.try_begin_evict().expect("detach clean frame");
        assert_eq!(meta.state(), FrameState::Evicting);
        meta.finish_evict().expect("free frame");
        assert_eq!(meta.state(), FrameState::Free);
    }

    #[test]
    fn optimistic_reader_detects_completed_writer() {
        let meta = FrameMeta::new_resident();
        let pin = meta.try_pin().expect("resident pin");
        let before = pin.optimistic_version().expect("stable version");
        assert_eq!(before.get(), 0);
        {
            let writer = pin.try_write().expect("write ownership");
            assert!(meta.is_dirty());
            writer.mark_dirty();
            assert!(!pin.validate(before));
        }
        assert!(!pin.validate(before));
        let after = pin.optimistic_version().expect("new stable version");
        assert_eq!(after.get(), 2);
        assert!(pin.validate(after));
    }

    #[test]
    fn second_writer_is_rejected_while_version_is_odd() {
        let meta = FrameMeta::new_resident();
        let first = meta.try_pin().expect("first pin");
        let second = meta.try_pin().expect("second pin");
        let writer = first.try_write().expect("first writer");
        assert!(matches!(second.try_write(), Err(FrameTransitionError::WriteBusy)));
        drop(writer);
        second.try_write().expect("writer after release");
    }

    #[test]
    fn pinned_dirty_frame_refuses_writeback_until_pin_releases() {
        let meta = FrameMeta::new_resident();
        let pin = meta.try_pin().expect("resident pin");
        {
            let _writer = pin.try_write().expect("write ownership");
        }
        assert_eq!(
            meta.try_begin_writeback(),
            Err(FrameTransitionError::Pinned)
        );
        drop(pin);
        meta.try_begin_writeback().expect("freeze dirty frame");
        assert_eq!(meta.state(), FrameState::Writeback);
        meta.finish_writeback().expect("complete writeback");
        assert_eq!(meta.state(), FrameState::Resident);
        assert!(!meta.is_dirty());
    }

    #[test]
    fn failed_writeback_preserves_dirty_state() {
        let meta = FrameMeta::new_resident();
        let pin = meta.try_pin().expect("resident pin");
        {
            let _writer = pin.try_write().expect("write ownership");
        }
        drop(pin);
        meta.try_begin_writeback().expect("freeze dirty frame");
        meta.abort_writeback().expect("reopen after failed write");
        assert_eq!(meta.state(), FrameState::Resident);
        assert!(meta.is_dirty());
    }

    #[test]
    fn dirty_frame_cannot_be_cleanly_evicted() {
        let meta = FrameMeta::new_resident();
        let pin = meta.try_pin().expect("resident pin");
        {
            let _writer = pin.try_write().expect("write ownership");
        }
        drop(pin);
        assert_eq!(meta.try_begin_evict(), Err(FrameTransitionError::Dirty));
    }
}
