//! The lexical form of vector literals and the compact form they are stored in.
//!
//! A vector literal lists 32 bit float components in square brackets, separated by commas:
//! `"[0.1,-2.5,0.0003]"`. Its canonical form writes each component as the shortest decimal that
//! reads back to the same float, without spaces. Dictionaries of vector literals store canonical
//! forms packed: a tag character followed by the little endian bytes of the floats, seven bits
//! per character, so that the stored string is valid UTF-8 (ASCII) at about 37 % of the size of
//! the lexical form. Lexical forms that are not canonical (e.g. written by hand with spaces) are
//! stored as they are, so every literal keeps its exact lexical form.

use std::borrow::Cow;
use std::fmt::Write;

/// The first character of a packed vector.
pub const PACKED_TAG: char = '\u{1}';

/// Writes the canonical lexical form of a vector: the shortest representation of each component
/// that reads back to the same float.
pub fn format_vector_into(v: &[f32], out: &mut String) {
    format_floats_into(v.iter().copied(), v.len(), out);
}

fn format_floats_into(v: impl Iterator<Item = f32>, n: usize, out: &mut String) {
    out.reserve(n * 12 + 2);
    out.push('[');
    let mut buf = ryu::Buffer::new();
    for (i, x) in v.enumerate() {
        if i > 0 {
            out.push(',');
        }
        if x.is_finite() {
            out.push_str(buf.format_finite(x));
        } else {
            // Not valid in the lexical space, but keeps the length of the vector.
            write!(out, "{x}").unwrap();
        }
    }
    out.push(']');
}

/// The floats of a packed vector (see [`is_packed`]).
fn packed_floats(s: &str) -> (impl Iterator<Item = f32> + '_, usize) {
    let b = &s.as_bytes()[1..];
    let n_floats = b.len() * 7 / 32;
    let mut acc: u64 = 0;
    let mut bits = 0u32;
    let mut bytes = b.iter();
    let it = (0..n_floats).map(move |_| {
        while bits < 32 {
            acc |= ((*bytes.next().unwrap() & 0x7f) as u64) << bits;
            bits += 7;
        }
        let x = f32::from_bits(acc as u32);
        acc >>= 32;
        bits -= 32;
        x
    });
    (it, n_floats)
}

/// Parses the lexical form of a vector literal into `out`, which is cleared first.
pub fn parse_vector_into(s: &str, out: &mut Vec<f32>) -> Result<(), String> {
    out.clear();
    let t = s.trim();
    let inner = t
        .strip_prefix('[')
        .and_then(|t| t.strip_suffix(']'))
        .ok_or_else(|| format!("a vector literal must be enclosed in square brackets: {s}"))?;
    if inner.trim().is_empty() {
        return Ok(());
    }
    for part in inner.split(',') {
        let part = part.trim();
        let x: f32 = part
            .parse()
            .map_err(|_| format!("invalid vector component {part:?} in {s}"))?;
        out.push(x);
    }
    Ok(())
}

/// The number of characters that `n_bytes` bytes are packed into, seven bits per character.
#[inline]
fn packed_len(n_bytes: usize) -> usize {
    (n_bytes * 8).div_ceil(7)
}

/// Appends the packed form of a vector. NaNs are written as the canonical NaN, as they all have
/// the same lexical form.
pub fn pack_vector_into(v: &[f32], out: &mut String) {
    let n_bytes = v.len() * 4;
    out.reserve(1 + packed_len(n_bytes));
    out.push(PACKED_TAG);
    // Safety: only bytes below 0x80 are pushed, so the string stays valid UTF-8.
    let o = unsafe { out.as_mut_vec() };
    let mut acc: u64 = 0;
    let mut bits = 0u32;
    for x in v {
        let x = if x.is_nan() { f32::NAN } else { *x };
        acc |= (x.to_bits() as u64) << bits;
        bits += 32;
        while bits >= 7 {
            o.push((acc & 0x7f) as u8);
            acc >>= 7;
            bits -= 7;
        }
    }
    if bits > 0 {
        o.push((acc & 0x7f) as u8);
    }
}

pub fn pack_vector(v: &[f32]) -> String {
    let mut s = String::new();
    pack_vector_into(v, &mut s);
    s
}

/// Whether `s` is a packed vector.
#[inline]
pub fn is_packed(s: &str) -> bool {
    let b = s.as_bytes();
    if b.first() != Some(&(PACKED_TAG as u8)) {
        return false;
    }
    let n = b.len() - 1;
    let n_floats = n * 7 / 32;
    packed_len(n_floats * 4) == n && b.is_ascii()
}

/// Unpacks a packed vector (see [`is_packed`]) into `out`, which is cleared first.
pub fn unpack_vector_into(s: &str, out: &mut Vec<f32>) {
    out.clear();
    let (it, n) = packed_floats(s);
    out.reserve(n);
    out.extend(it);
}

/// The vector of a stored vector literal, whether packed or a lexical form.
pub fn stored_to_vector_into(s: &str, out: &mut Vec<f32>) -> Result<(), String> {
    if is_packed(s) {
        unpack_vector_into(s, out);
        Ok(())
    } else {
        parse_vector_into(s, out)
    }
}

/// The form a vector literal with the lexical form `s` is stored in: packed if `s` is canonical
/// (or already packed), otherwise `s` itself.
pub fn lexical_to_stored(s: &str) -> Cow<'_, str> {
    let Some(inner) = s.strip_prefix('[').and_then(|t| t.strip_suffix(']')) else {
        return Cow::Borrowed(s);
    };
    let mut v = Vec::with_capacity(inner.len() / 10 + 1);
    if !inner.is_empty() {
        // Each component must be the canonical form of its float, so it is formatted again
        // and compared, stopping at the first that is not.
        let mut buf = ryu::Buffer::new();
        let mut other = String::new();
        for part in inner.split(',') {
            let Ok(x) = part.parse::<f32>() else {
                return Cow::Borrowed(s);
            };
            let canonical = if x.is_finite() {
                buf.format_finite(x)
            } else {
                other.clear();
                write!(other, "{x}").unwrap();
                other.as_str()
            };
            if canonical != part {
                return Cow::Borrowed(s);
            }
            v.push(x);
        }
    }
    Cow::Owned(pack_vector(&v))
}

/// Writes the lexical form of a stored vector literal.
pub fn stored_to_lexical_into(s: &str, out: &mut String) {
    if is_packed(s) {
        let (it, n) = packed_floats(s);
        format_floats_into(it, n, out);
    } else {
        out.push_str(s);
    }
}

/// The lexical form of a stored vector literal.
pub fn stored_to_lexical(s: &str) -> Cow<'_, str> {
    if is_packed(s) {
        let mut out = String::new();
        stored_to_lexical_into(s, &mut out);
        Cow::Owned(out)
    } else {
        Cow::Borrowed(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lexical(v: &[f32]) -> String {
        let mut s = String::new();
        format_vector_into(v, &mut s);
        s
    }

    #[test]
    fn pack_round_trips() {
        for n in 0..20 {
            let v: Vec<f32> = (0..n).map(|i| (i as f32 - 7.3) * 1.37e-3).collect();
            let p = pack_vector(&v);
            assert!(is_packed(&p), "{n}");
            let mut back = vec![];
            unpack_vector_into(&p, &mut back);
            assert_eq!(
                back.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
                v.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
            );
            assert_eq!(stored_to_lexical(&lexical_to_stored(&lexical(&v))), lexical(&v));
            assert_eq!(lexical_to_stored(&lexical(&v)), p);
        }
    }

    #[test]
    fn special_values() {
        let v = [
            -0.0f32,
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::MIN_POSITIVE,
            f32::MAX,
            1e-45,
        ];
        let l = lexical(&v);
        let s = lexical_to_stored(&l);
        assert!(is_packed(&s));
        assert_eq!(stored_to_lexical(&s), l);
        // All NaNs are packed alike.
        let other_nan = f32::from_bits(0x7fc0_0001);
        assert_eq!(pack_vector(&[other_nan]), pack_vector(&[f32::NAN]));
    }

    #[test]
    fn non_canonical_forms_are_kept() {
        for s in ["[ 1 , 2.5 ]", "[1,2]", "[1.0, 2.0]", "[a]", "abc", "", "[1.0,2.0"] {
            assert_eq!(lexical_to_stored(s), s);
            assert_eq!(stored_to_lexical(s), s);
        }
        assert!(is_packed(&lexical_to_stored("[]")));
        assert_eq!(stored_to_lexical(&lexical_to_stored("[]")), "[]");
        assert!(is_packed(&lexical_to_stored("[1.0,2.0]")));
    }

    #[test]
    fn packed_is_smaller() {
        let v: Vec<f32> = (0..384).map(|i| (i as f32).sin() * 0.123_456_7).collect();
        assert!(pack_vector(&v).len() * 2 < lexical(&v).len());
    }
}
