//! SIMD-accelerated utilities with runtime CPU feature dispatch.
//!
//! Uses `std::simd` (portable SIMD) for cross-platform acceleration.
//! Runtime dispatch via `multiversion` selects optimal ISA:
//! - `x86_64`: AVX-512 (64 bytes) → AVX2 (32 bytes) → SSE4.1 (16 bytes)
//! - `aarch64`: SVE (variable) → NEON (16 bytes)
//!
//! When the `simd` feature is disabled, falls back to scalar code.

use std::cmp::Ordering;

#[cfg(feature = "simd")]
use multiversion::multiversion;
#[cfg(feature = "simd")]
use std::simd::{cmp::SimdPartialEq, cmp::SimdPartialOrd, LaneCount, Simd, SupportedLaneCount};

/// Compare `user_key` portion of an `InternalKey` against a `user_key`.
///
/// Strips the 8-byte trailer from `internal_key` before comparing.
#[inline]
#[must_use]
pub fn compare_internal_to_user_key(internal_key: &[u8], user_key: &[u8]) -> Ordering {
    let internal_user_len = internal_key.len().saturating_sub(8);
    compare_keys_with_len(internal_key, internal_user_len, user_key, user_key.len())
}

/// Compare two byte slices with explicit lengths.
#[cfg(feature = "simd")]
#[multiversion(targets(
    "x86_64+avx512f",
    "x86_64+avx2",
    "x86_64+sse4.1",
    "aarch64+sve",
    "aarch64+neon"
))]
fn compare_keys_with_len(a: &[u8], len_a: usize, b: &[u8], len_b: usize) -> std::cmp::Ordering {
    // Cascade: try wider SIMD first, fall back to narrower, then scalar.
    // Returns None if key length < lane width, triggering next fallback.
    compare_keys_simd::<32>(a, len_a, b, len_b)
        .or_else(|| compare_keys_simd::<16>(a, len_a, b, len_b))
        .unwrap_or_else(|| compare_keys_scalar(a, len_a, b, len_b))
}

#[cfg(not(feature = "simd"))]
#[inline]
fn compare_keys_with_len(a: &[u8], len_a: usize, b: &[u8], len_b: usize) -> Ordering {
    a[..len_a].cmp(&b[..len_b])
}

/// SIMD key comparison with variable lane count.
#[cfg(feature = "simd")]
#[inline]
fn compare_keys_simd<const N: usize>(
    a: &[u8],
    len_a: usize,
    b: &[u8],
    len_b: usize,
) -> Option<Ordering>
where
    LaneCount<N>: SupportedLaneCount,
{
    let min_len = len_a.min(len_b);
    if min_len < N {
        return None;
    }

    let mut i = 0;
    while i + N <= min_len {
        let a_vec = Simd::<u8, N>::from_slice(&a[i..i + N]);
        let b_vec = Simd::<u8, N>::from_slice(&b[i..i + N]);
        let eq = a_vec.simd_eq(b_vec);

        if !eq.all() {
            // Find first difference
            for j in 0..N {
                let pos = i + j;
                match a[pos].cmp(&b[pos]) {
                    Ordering::Equal => {}
                    other => return Some(other),
                }
            }
        }
        i += N;
    }

    // Handle remaining bytes with scalar
    while i < min_len {
        match a[i].cmp(&b[i]) {
            Ordering::Equal => i += 1,
            other => return Some(other),
        }
    }

    Some(len_a.cmp(&len_b))
}

/// Scalar fallback for key comparison.
#[cfg(feature = "simd")]
#[inline]
fn compare_keys_scalar(a: &[u8], len_a: usize, b: &[u8], len_b: usize) -> Ordering {
    a[..len_a].cmp(&b[..len_b])
}

/// Compare two byte slices.
#[inline]
#[must_use]
pub fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
    compare_keys_with_len(a, a.len(), b, b.len())
}

/// Calculate shared prefix length between two keys.
#[cfg(feature = "simd")]
#[multiversion(targets(
    "x86_64+avx512f",
    "x86_64+avx2",
    "x86_64+sse4.1",
    "aarch64+sve",
    "aarch64+neon"
))]
#[must_use]
pub fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    // Cascade: try wider SIMD first, fall back to narrower, then scalar.
    shared_prefix_simd::<32>(a, b)
        .or_else(|| shared_prefix_simd::<16>(a, b))
        .unwrap_or_else(|| shared_prefix_scalar(a, b))
}

#[cfg(not(feature = "simd"))]
#[inline]
#[must_use]
pub fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// SIMD shared prefix calculation with variable lane count.
#[cfg(feature = "simd")]
#[inline]
fn shared_prefix_simd<const N: usize>(a: &[u8], b: &[u8]) -> Option<usize>
where
    LaneCount<N>: SupportedLaneCount,
{
    let min_len = a.len().min(b.len());
    if min_len < N {
        return None;
    }

    let mut i = 0;
    while i + N <= min_len {
        let a_vec = Simd::<u8, N>::from_slice(&a[i..i + N]);
        let b_vec = Simd::<u8, N>::from_slice(&b[i..i + N]);
        let eq = a_vec.simd_eq(b_vec);

        if eq.all() {
            i += N;
            continue;
        }

        // Find first mismatch
        for j in 0..N {
            if a[i + j] != b[i + j] {
                return Some(i + j);
            }
        }
    }

    // Handle remaining bytes
    while i < min_len && a[i] == b[i] {
        i += 1;
    }

    Some(i)
}

/// Scalar fallback for shared prefix.
#[cfg(feature = "simd")]
#[inline]
fn shared_prefix_scalar(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Decode a varint from a byte slice.
///
/// Returns `(value, bytes_read)` if successful.
#[cfg(feature = "simd")]
#[multiversion(targets(
    "x86_64+avx512f",
    "x86_64+avx2",
    "x86_64+sse4.1",
    "aarch64+sve",
    "aarch64+neon"
))]
#[must_use]
pub fn decode_varint(data: &[u8]) -> Option<(u64, usize)> {
    if data.is_empty() {
        return None;
    }

    // Fast path for single-byte varints (most common)
    if data[0] < 128 {
        return Some((u64::from(data[0]), 1));
    }

    // Fixed 16-lane SIMD: varints are max 10 bytes, wider SIMD provides no benefit
    if data.len() >= 16 {
        let v = Simd::<u8, 16>::from_slice(&data[..16]);
        let mask = v.simd_lt(Simd::<u8, 16>::splat(128));
        let bitmask = mask.to_bitmask();

        if bitmask == 0 {
            return None; // No terminator in first 16 bytes = invalid
        }

        let terminator_pos = bitmask.trailing_zeros() as usize + 1;
        if terminator_pos > 10 {
            return None; // varint64 max is 10 bytes
        }

        // Use slice bounded by SIMD-detected length for faster scalar decode
        return decode_varint_scalar(&data[..terminator_pos]);
    }

    // Scalar fallback for short buffers
    decode_varint_scalar(data)
}

#[cfg(not(feature = "simd"))]
#[inline]
#[must_use]
pub fn decode_varint(data: &[u8]) -> Option<(u64, usize)> {
    decode_varint_scalar(data)
}

/// Scalar varint decoding fallback.
#[inline]
fn decode_varint_scalar(data: &[u8]) -> Option<(u64, usize)> {
    if data.is_empty() {
        return None;
    }
    if data[0] < 128 {
        return Some((u64::from(data[0]), 1));
    }

    let mut value: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        if i >= 10 {
            return None;
        }
        if byte < 128 {
            value |= u64::from(byte) << shift;
            return Some((value, i + 1));
        }
        value |= u64::from(byte & 0x7F) << shift;
        shift += 7;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_keys_equal() {
        assert_eq!(compare_keys(b"hello", b"hello"), Ordering::Equal);
    }

    #[test]
    fn test_compare_keys_less() {
        assert_eq!(compare_keys(b"hello", b"world"), Ordering::Less);
    }

    #[test]
    fn test_compare_keys_greater() {
        assert_eq!(compare_keys(b"world", b"hello"), Ordering::Greater);
    }

    #[test]
    fn test_compare_keys_different_lengths() {
        assert_eq!(compare_keys(b"hello", b"hello world"), Ordering::Less);
        assert_eq!(compare_keys(b"hello world", b"hello"), Ordering::Greater);
    }

    #[test]
    fn test_compare_keys_long() {
        let a = b"this is a very long key that exceeds 16 bytes";
        let b = b"this is a very long key that exceeds 16 bytes";
        assert_eq!(compare_keys(a, b), Ordering::Equal);

        let b = b"this is a very long key that exceeds 16 bytez";
        assert_eq!(compare_keys(a, b), Ordering::Less);
    }

    #[test]
    fn test_compare_keys_very_long() {
        // Test with keys > 32 bytes to exercise AVX2 path
        let a = b"this is an extremely long key that definitely exceeds 32 bytes for AVX2";
        let b = b"this is an extremely long key that definitely exceeds 32 bytes for AVX2";
        assert_eq!(compare_keys(a, b), Ordering::Equal);

        let c = b"this is an extremely long key that definitely exceeds 32 bytes for AVX3";
        assert_eq!(compare_keys(a, c), Ordering::Less);
    }

    #[test]
    fn test_compare_keys_empty() {
        assert_eq!(compare_keys(b"", b""), Ordering::Equal);
        assert_eq!(compare_keys(b"", b"hello"), Ordering::Less);
        assert_eq!(compare_keys(b"hello", b""), Ordering::Greater);
    }

    #[test]
    fn test_compare_keys_consistency() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"", b""),
            (b"a", b"a"),
            (b"a", b"b"),
            (b"hello", b"world"),
            (b"user:123:name", b"user:123:email"),
        ];
        for (a, b) in cases {
            assert_eq!(compare_keys(a, b), a.cmp(b));
        }
    }

    #[test]
    fn test_compare_internal_to_user_key() {
        // InternalKey: user_key + 8-byte trailer
        let internal = b"user_key\x00\x00\x00\x00\x00\x00\x00\x00";
        assert_eq!(
            compare_internal_to_user_key(internal, b"user_key"),
            Ordering::Equal
        );
        assert_eq!(
            compare_internal_to_user_key(internal, b"user_kex"),
            Ordering::Greater
        );
        assert_eq!(
            compare_internal_to_user_key(internal, b"user_kez"),
            Ordering::Less
        );
    }

    #[test]
    fn test_shared_prefix_len() {
        assert_eq!(shared_prefix_len(b"hello", b"world"), 0);
        assert_eq!(shared_prefix_len(b"user:123:name", b"user:123:email"), 9);
        assert_eq!(shared_prefix_len(b"hello", b"hello world"), 5);
        assert_eq!(shared_prefix_len(b"", b"hello"), 0);
    }

    #[test]
    fn test_shared_prefix_len_long() {
        // Test with keys > 32 bytes
        let a = b"prefix_that_is_shared_for_many_bytes_then_differs_here";
        let b = b"prefix_that_is_shared_for_many_bytes_then_differs_nope";
        // Shared: "prefix_that_is_shared_for_many_bytes_then_differs_" (50 chars)
        // First diff at position 50: 'h' vs 'n'
        assert_eq!(shared_prefix_len(a, b), 50);
    }

    #[test]
    fn test_decode_varint() {
        let mut buf = vec![0u8; 32];

        buf[0] = 0x05;
        assert_eq!(decode_varint(&buf), Some((5, 1)));

        buf[0] = 0x85;
        buf[1] = 0x01;
        assert_eq!(decode_varint(&buf), Some((133, 2)));

        buf[0] = 0x80;
        buf[1] = 0x80;
        buf[2] = 0x01;
        assert_eq!(decode_varint(&buf), Some((16384, 3)));

        // All continuation bits = invalid
        for b in buf.iter_mut().take(16) {
            *b = 0x80;
        }
        assert_eq!(decode_varint(&buf), None);
    }
}
