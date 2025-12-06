//! SIMD-accelerated search for ALEX index nodes.
//!
//! Uses `std::simd` with runtime dispatch via `multiversion`:
//! - `x86_64`: AVX-512 (8 i64) → AVX2 (4 i64) → SSE4.1 (2 i64)
//! - `aarch64`: NEON (2 i64)
//!
//! Note: SVE support will be added when Rust gains native SVE support.

use multiversion::multiversion;
use std::simd::{cmp::SimdPartialEq, LaneCount, Simd, SupportedLaneCount};

/// SIMD-optimized search for key in `Option<i64>` array.
///
/// Searches for first occurrence of `Some(key)` in the slice.
/// Runtime dispatch selects optimal SIMD width for the CPU.
///
/// Returns `Some(index)` if found, `None` otherwise.
#[multiversion(targets("x86_64+avx512f", "x86_64+avx2", "x86_64+sse4.1", "aarch64+neon"))]
pub fn simd_search_i64(keys: &[Option<i64>], key: i64) -> Option<usize> {
    if keys.is_empty() {
        return None;
    }

    // Fast path: check first element (common case after model prediction)
    if keys[0] == Some(key) {
        return Some(0);
    }

    // Try 8-lane (AVX-512), then 4-lane (AVX2), then 2-lane (SSE/NEON), then scalar
    search_simd::<8>(keys, key)
        .or_else(|| search_simd::<4>(keys, key))
        .or_else(|| search_simd::<2>(keys, key))
        .unwrap_or_else(|| search_scalar(keys, key))
}

/// SIMD search with variable lane count.
///
/// Returns `Option<Option<usize>>` for cascade pattern:
/// - `None`: not enough elements for this lane width, try smaller
/// - `Some(None)`: searched all elements, key not found
/// - `Some(Some(idx))`: found key at index
#[inline]
fn search_simd<const N: usize>(keys: &[Option<i64>], key: i64) -> Option<Option<usize>>
where
    LaneCount<N>: SupportedLaneCount,
{
    let len = keys.len();
    if len < N {
        return None; // Not enough elements for this lane count
    }

    let key_vec = Simd::<i64, N>::splat(key);
    let mut i = 0;

    while i + N <= len {
        // Extract N i64 values, using MAX as sentinel for None
        let mut values = [i64::MAX; N];
        for j in 0..N {
            values[j] = keys[i + j].unwrap_or(i64::MAX);
        }

        let vec = Simd::<i64, N>::from_array(values);
        let mask = vec.simd_eq(key_vec);

        if mask.any() {
            // Find which lane matched
            for j in 0..N {
                if keys[i + j] == Some(key) {
                    return Some(Some(i + j));
                }
            }
        }

        i += N;
    }

    // Check remaining elements with scalar
    for (j, &k) in keys[i..len].iter().enumerate() {
        if k == Some(key) {
            return Some(Some(i + j));
        }
    }

    Some(None) // Searched everything, not found
}

/// Scalar fallback search.
#[inline]
fn search_scalar(keys: &[Option<i64>], key: i64) -> Option<usize> {
    keys.iter().position(|&k| k == Some(key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_search_empty() {
        let keys: &[Option<i64>] = &[];
        assert_eq!(simd_search_i64(keys, 42), None);
    }

    #[test]
    fn test_simd_search_not_found() {
        let keys = vec![Some(1), Some(2), Some(3), None, Some(5)];
        assert_eq!(simd_search_i64(&keys, 42), None);
    }

    #[test]
    fn test_simd_search_first_element() {
        let keys = vec![Some(42), Some(2), Some(3)];
        assert_eq!(simd_search_i64(&keys, 42), Some(0));
    }

    #[test]
    fn test_simd_search_middle() {
        let keys = vec![Some(1), Some(2), Some(42), Some(4)];
        assert_eq!(simd_search_i64(&keys, 42), Some(2));
    }

    #[test]
    fn test_simd_search_last() {
        let keys = vec![Some(1), Some(2), Some(3), Some(42)];
        assert_eq!(simd_search_i64(&keys, 42), Some(3));
    }

    #[test]
    fn test_simd_search_with_gaps() {
        let keys = vec![Some(1), None, Some(42), None, Some(5)];
        assert_eq!(simd_search_i64(&keys, 42), Some(2));
    }

    #[test]
    fn test_simd_search_long_array() {
        // Test with array longer than SIMD width
        let mut keys = vec![None; 20];
        keys[15] = Some(42);
        assert_eq!(simd_search_i64(&keys, 42), Some(15));
    }

    #[test]
    fn test_simd_search_very_long_array() {
        // Test with array > 8 elements (AVX-512 width)
        let mut keys = vec![Some(0); 100];
        keys[77] = Some(42);
        assert_eq!(simd_search_i64(&keys, 42), Some(77));
    }

    #[test]
    fn test_simd_search_all_gaps() {
        let keys = vec![None, None, None, None];
        assert_eq!(simd_search_i64(&keys, 42), None);
    }

    #[test]
    fn test_simd_search_consistency() {
        // Verify SIMD matches linear search for various inputs
        let test_cases = vec![
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
            vec![None, Some(1), None, Some(2), None],
            vec![Some(10), Some(20), Some(30), Some(40)],
            vec![Some(1); 10],
            vec![Some(1); 100], // Test larger arrays
        ];

        for keys in test_cases {
            for search_key in [1, 2, 5, 10, 20, 42, 100] {
                let simd_result = simd_search_i64(&keys, search_key);
                let linear_result = keys.iter().position(|&k| k == Some(search_key));
                assert_eq!(
                    simd_result, linear_result,
                    "Mismatch for key={} in {:?}",
                    search_key, keys
                );
            }
        }
    }
}
