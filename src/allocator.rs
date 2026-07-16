//! Page allocator for tracking free and allocated pages.
//!
//! Manages page ID allocation and free space tracking. Pages are allocated
//! sequentially and tracked in a free list when deallocated.

use std::collections::HashSet;

/// Allocator for page IDs.
///
/// Tracks which pages are allocated and maintains a free list
/// for reuse of deallocated pages.
pub struct PageAllocator {
    /// Next page ID to allocate (sequential allocation).
    next_id: u64,
    /// Free page IDs available for reuse.
    free_list: Vec<u64>,
    /// Set of allocated page IDs (for validation).
    allocated: HashSet<u64>,
}

impl PageAllocator {
    /// Create a new page allocator.
    pub fn new() -> Self {
        Self {
            next_id: 1, // page 0 is reserved for the header
            free_list: Vec::new(),
            allocated: HashSet::new(),
        }
    }

    /// Create a page allocator with a starting ID (for recovery).
    pub fn with_next_id(next_id: u64) -> Self {
        Self {
            next_id,
            free_list: Vec::new(),
            allocated: HashSet::new(),
        }
    }

    /// Allocate a new page ID.
    ///
    /// Prefers reusing freed pages; falls back to sequential allocation.
    pub fn alloc(&mut self) -> u64 {
        let id = if let Some(freed) = self.free_list.pop() {
            freed
        } else {
            let id = self.next_id;
            self.next_id += 1;
            id
        };

        self.allocated.insert(id);
        id
    }

    /// Free a page ID (add to free list).
    ///
    /// Returns true if the page was allocated, false if already free.
    pub fn free(&mut self, page_id: u64) -> bool {
        if self.allocated.remove(&page_id) {
            self.free_list.push(page_id);
            true
        } else {
            false
        }
    }

    /// Check if a page ID is currently allocated.
    pub fn is_allocated(&self, page_id: u64) -> bool {
        self.allocated.contains(&page_id)
    }

    /// Number of currently allocated pages.
    pub fn allocated_count(&self) -> usize {
        self.allocated.len()
    }

    /// Number of free page IDs available for reuse.
    pub fn free_count(&self) -> usize {
        self.free_list.len()
    }

    /// Next page ID that would be allocated (for serialization).
    pub fn next_id(&self) -> u64 {
        self.next_id
    }

    /// Serialize the allocator state.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 4 + self.free_list.len() * 8);
        buf.extend_from_slice(&self.next_id.to_le_bytes());
        buf.extend_from_slice(&(self.free_list.len() as u32).to_le_bytes());
        for &id in &self.free_list {
            buf.extend_from_slice(&id.to_le_bytes());
        }
        buf
    }

    /// Deserialize the allocator state.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 12 {
            return None;
        }

        let next_id = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let free_count = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;

        let expected = 12usize.checked_add(free_count.checked_mul(8)?)?;
        if buf.len() != expected {
            return None;
        }

        let mut free_list = Vec::with_capacity(free_count);
        let mut pos = 12;
        for _ in 0..free_count {
            let id = u64::from_le_bytes([
                buf[pos],
                buf[pos + 1],
                buf[pos + 2],
                buf[pos + 3],
                buf[pos + 4],
                buf[pos + 5],
                buf[pos + 6],
                buf[pos + 7],
            ]);
            free_list.push(id);
            pos += 8;
        }

        Some(Self {
            next_id,
            free_list,
            allocated: HashSet::new(), // rebuilt during recovery
        })
    }
}

impl Default for PageAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_alloc() {
        let mut alloc = PageAllocator::new();
        assert_eq!(alloc.alloc(), 1);
        assert_eq!(alloc.alloc(), 2);
        assert_eq!(alloc.alloc(), 3);
        assert_eq!(alloc.allocated_count(), 3);
    }

    #[test]
    fn test_free_and_reuse() {
        let mut alloc = PageAllocator::new();
        let id1 = alloc.alloc();
        let id2 = alloc.alloc();

        assert!(alloc.free(id1));
        assert_eq!(alloc.free_count(), 1);

        let id3 = alloc.alloc();
        assert_eq!(id3, id1); // should reuse freed page
        assert_eq!(alloc.free_count(), 0);
    }

    #[test]
    fn test_double_free() {
        let mut alloc = PageAllocator::new();
        let id = alloc.alloc();
        assert!(alloc.free(id));
        assert!(!alloc.free(id)); // already freed
    }

    #[test]
    fn test_is_allocated() {
        let mut alloc = PageAllocator::new();
        assert!(!alloc.is_allocated(1));

        let id = alloc.alloc();
        assert!(alloc.is_allocated(id));

        alloc.free(id);
        assert!(!alloc.is_allocated(id));
    }

    #[test]
    fn test_serialization() {
        let mut alloc = PageAllocator::new();
        alloc.alloc(); // 1
        alloc.alloc(); // 2
        alloc.alloc(); // 3
        alloc.free(2);

        let bytes = alloc.to_bytes();
        let restored = PageAllocator::from_bytes(&bytes).unwrap();

        assert_eq!(restored.next_id(), 4);
        assert_eq!(restored.free_count(), 1);
    }

    #[test]
    fn test_deserialization_rejects_trailing_bytes() {
        let alloc = PageAllocator::new();
        let mut bytes = alloc.to_bytes();
        bytes.push(0xA5);

        assert!(PageAllocator::from_bytes(&bytes).is_none());
    }
}
