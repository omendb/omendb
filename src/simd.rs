//! SIMD-accelerated utilities with scalar fallbacks.
//!
//! When the `simd` feature is enabled (default), uses portable SIMD for
//! key comparison and varint decoding. Otherwise falls back to scalar code.

use std::cmp::Ordering;

#[cfg(feature = "simd")]
use std::simd::{cmp::SimdPartialEq, cmp::SimdPartialOrd, u8x16};

#[cfg(feature = "simd")]
const SIMD_WIDTH: usize = 16;

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
#[inline]
fn compare_keys_with_len(a: &[u8], len_a: usize, b: &[u8], len_b: usize) -> Ordering {
    #[cfg(feature = "simd")]
    {
        let min_len = len_a.min(len_b);
        let mut i = 0;

        while i + SIMD_WIDTH <= min_len {
            let a_vec = u8x16::from_slice(&a[i..i + SIMD_WIDTH]);
            let b_vec = u8x16::from_slice(&b[i..i + SIMD_WIDTH]);
            let eq = a_vec.simd_eq(b_vec);

            if !eq.all() {
                for j in 0..SIMD_WIDTH {
                    let pos = i + j;
                    match a[pos].cmp(&b[pos]) {
                        Ordering::Equal => {}
                        other => return other,
                    }
                }
            }
            i += SIMD_WIDTH;
        }

        while i < min_len {
            match a[i].cmp(&b[i]) {
                Ordering::Equal => i += 1,
                other => return other,
            }
        }

        len_a.cmp(&len_b)
    }

    #[cfg(not(feature = "simd"))]
    {
        a[..len_a].cmp(&b[..len_b])
    }
}

/// Compare two byte slices.
#[inline]
#[must_use]
pub fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
    compare_keys_with_len(a, a.len(), b, b.len())
}

/// Calculate shared prefix length between two keys.
#[inline]
#[must_use]
pub fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    #[cfg(feature = "simd")]
    {
        let min_len = a.len().min(b.len());
        let mut i = 0;

        while i + SIMD_WIDTH <= min_len {
            let a_vec = u8x16::from_slice(&a[i..i + SIMD_WIDTH]);
            let b_vec = u8x16::from_slice(&b[i..i + SIMD_WIDTH]);
            let eq = a_vec.simd_eq(b_vec);

            if eq.all() {
                i += SIMD_WIDTH;
                continue;
            }

            for j in 0..SIMD_WIDTH {
                if a[i + j] != b[i + j] {
                    return i + j;
                }
            }
        }

        while i < min_len && a[i] == b[i] {
            i += 1;
        }

        i
    }

    #[cfg(not(feature = "simd"))]
    {
        a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
    }
}

/// Decode a varint from a byte slice.
///
/// Returns `(value, bytes_read)` if successful.
#[inline]
#[must_use]
pub fn decode_varint(data: &[u8]) -> Option<(u64, usize)> {
    if data.is_empty() {
        return None;
    }

    // Fast path for single-byte varints
    if data[0] < 128 {
        return Some((data[0] as u64, 1));
    }

    #[cfg(feature = "simd")]
    if data.len() >= 16 {
        let v = u8x16::from_slice(&data[..16]);
        let mask = v.simd_lt(u8x16::splat(128));
        let bitmask = mask.to_bitmask();

        if bitmask == 0 {
            return None;
        }

        let len = bitmask.trailing_zeros() as usize + 1;
        if len > 10 {
            return None;
        }

        let mut value: u64 = 0;
        match len {
            1 => return Some((data[0] as u64, 1)),
            2 => {
                value = (data[0] & 0x7F) as u64;
                value |= (data[1] as u64) << 7;
                return Some((value, 2));
            }
            3 => {
                value = (data[0] & 0x7F) as u64;
                value |= ((data[1] & 0x7F) as u64) << 7;
                value |= (data[2] as u64) << 14;
                return Some((value, 3));
            }
            4 => {
                value = (data[0] & 0x7F) as u64;
                value |= ((data[1] & 0x7F) as u64) << 7;
                value |= ((data[2] & 0x7F) as u64) << 14;
                value |= (data[3] as u64) << 21;
                return Some((value, 4));
            }
            5 => {
                value = (data[0] & 0x7F) as u64;
                value |= ((data[1] & 0x7F) as u64) << 7;
                value |= ((data[2] & 0x7F) as u64) << 14;
                value |= ((data[3] & 0x7F) as u64) << 21;
                value |= (data[4] as u64) << 28;
                return Some((value, 5));
            }
            _ => {
                let mut shift = 0;
                for (i, val) in data.iter().enumerate().take(len) {
                    let byte = *val;
                    if i == len - 1 {
                        value |= (byte as u64) << shift;
                    } else {
                        value |= ((byte & 0x7F) as u64) << shift;
                    }
                    shift += 7;
                }
                return Some((value, len));
            }
        }
    }

    // Scalar fallback
    let mut value: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        if i >= 10 {
            return None;
        }
        if byte < 128 {
            value |= (byte as u64) << shift;
            return Some((value, i + 1));
        }
        value |= ((byte & 0x7F) as u64) << shift;
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
