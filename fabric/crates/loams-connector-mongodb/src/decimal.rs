//! BSON's Decimal128 (IEEE 754-2008 decimal128, binary integer
//! significand), to and from its string form as the BSON Decimal128
//! specification defines it.
//!
//! A string that cannot be stored exactly (more than 34 significant digits,
//! or an exponent out of range, after removing trailing zeros) is refused
//! rather than rounded: a sink must not change a value it was given.

use std::fmt;

/// 16 bytes, little-endian, as BSON stores them.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal128 {
    low: u64,
    high: u64,
}

const EXPONENT_BIAS: i32 = 6176;
const EXPONENT_MAX: i32 = 6111;
const EXPONENT_MIN: i32 = -6176;
const MAX_DIGITS: usize = 34;
/// 10^34 - 1, the largest canonical significand.
const MAX_SIGNIFICAND: u128 = 9_999_999_999_999_999_999_999_999_999_999_999;

impl Decimal128 {
    pub const fn from_parts(high: u64, low: u64) -> Self {
        Self { low, high }
    }

    pub fn parts(&self) -> (u64, u64) {
        (self.high, self.low)
    }

    pub fn from_bytes(b: [u8; 16]) -> Self {
        Self {
            low: u64::from_le_bytes(b[..8].try_into().expect("8")),
            high: u64::from_le_bytes(b[8..].try_into().expect("8")),
        }
    }

    pub fn bytes(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.low.to_le_bytes());
        out[8..].copy_from_slice(&self.high.to_le_bytes());
        out
    }

    fn negative(&self) -> bool {
        self.high >> 63 == 1
    }

    /// Parse the specification's string grammar: an optional sign, then
    /// `Infinity`/`Inf`/`NaN` (any case) or digits with an optional point
    /// and an optional exponent.
    pub fn parse(s: &str) -> Result<Self, String> {
        let bad = || format!("{s:?} is not a decimal");
        let (neg, body) = match s.as_bytes().first() {
            Some(b'-') => (true, &s[1..]),
            Some(b'+') => (false, &s[1..]),
            _ => (false, s),
        };
        let sign = if neg { 1u64 << 63 } else { 0 };
        let lower = body.to_ascii_lowercase();
        if lower == "inf" || lower == "infinity" {
            return Ok(Self::from_parts(sign | 0x7800_0000_0000_0000, 0));
        }
        if lower == "nan" {
            return Ok(Self::from_parts(0x7C00_0000_0000_0000, 0));
        }
        let (mantissa, exp_part) = match body.find(['e', 'E']) {
            Some(i) => (&body[..i], Some(&body[i + 1..])),
            None => (body, None),
        };
        let mut exponent: i64 = match exp_part {
            Some(e) => {
                let digits = e.strip_prefix(['+', '-']).unwrap_or(e);
                if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(bad());
                }
                e.parse::<i64>().map_err(|_| bad())?
            }
            None => 0,
        };
        let (int_part, frac_part) = match mantissa.split_once('.') {
            Some((i, f)) => (i, f),
            None => (mantissa, ""),
        };
        if (int_part.is_empty() && frac_part.is_empty())
            || !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(bad());
        }
        exponent -= frac_part.len() as i64;
        let mut digits: String = format!("{int_part}{frac_part}")
            .trim_start_matches('0')
            .to_owned();
        // Too many digits: only trailing zeros may go, each raising the
        // exponent by one.
        while digits.len() > MAX_DIGITS {
            if !digits.ends_with('0') {
                return Err(format!(
                    "{s:?} has more than {MAX_DIGITS} significant digits"
                ));
            }
            digits.pop();
            exponent += 1;
        }
        let mut significand: u128 = if digits.is_empty() {
            0
        } else {
            digits.parse().map_err(|_| bad())?
        };
        // Clamp the exponent into range without changing the value.
        while exponent > i64::from(EXPONENT_MAX) {
            if significand == 0 {
                exponent = i64::from(EXPONENT_MAX);
                break;
            }
            if significand * 10 > MAX_SIGNIFICAND {
                return Err(format!("{s:?} overflows decimal128"));
            }
            significand *= 10;
            exponent -= 1;
        }
        while exponent < i64::from(EXPONENT_MIN) {
            if significand == 0 {
                exponent = i64::from(EXPONENT_MIN);
                break;
            }
            if !significand.is_multiple_of(10) {
                return Err(format!("{s:?} underflows decimal128"));
            }
            significand /= 10;
            exponent += 1;
        }
        let biased = (exponent + i64::from(EXPONENT_BIAS)) as u64;
        let high = sign | (biased << 49) | ((significand >> 64) as u64 & 0x0001_FFFF_FFFF_FFFF);
        Ok(Self::from_parts(high, significand as u64))
    }
}

impl fmt::Display for Decimal128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.negative() { "-" } else { "" };
        let combination = (self.high >> 58) & 0x1F;
        let (biased, significand) = if combination >> 3 == 0b11 {
            match combination {
                0x1E => return write!(f, "{sign}Infinity"),
                0x1F => return f.write_str("NaN"),
                // A significand of 2^113 or more is non-canonical: zero.
                _ => (((self.high >> 47) & 0x3FFF) as i32, 0u128),
            }
        } else {
            let sig = (u128::from(self.high & 0x0001_FFFF_FFFF_FFFF) << 64) | u128::from(self.low);
            (
                ((self.high >> 49) & 0x3FFF) as i32,
                if sig > MAX_SIGNIFICAND { 0 } else { sig },
            )
        };
        let exponent = biased - EXPONENT_BIAS;
        let digits = significand.to_string();
        let adjusted = exponent + digits.len() as i32 - 1;
        if exponent <= 0 && adjusted >= -6 {
            if exponent == 0 {
                return write!(f, "{sign}{digits}");
            }
            let point = digits.len() as i32 + exponent;
            if point <= 0 {
                write!(f, "{sign}0.{}{digits}", "0".repeat((-point) as usize))
            } else {
                let (a, b) = digits.split_at(point as usize);
                write!(f, "{sign}{a}.{b}")
            }
        } else {
            let (first, rest) = digits.split_at(1);
            let point = if rest.is_empty() {
                String::new()
            } else {
                format!(".{rest}")
            };
            let esign = if adjusted >= 0 { "+" } else { "" };
            write!(f, "{sign}{first}{point}E{esign}{adjusted}")
        }
    }
}

impl fmt::Debug for Decimal128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Decimal128({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(s: &str) -> (u64, u64) {
        Decimal128::parse(s).expect(s).parts()
    }

    /// Values from the BSON corpus (`decimal128-1.json`, `-2`, `-3`).
    #[test]
    fn corpus_values_encode_and_print() {
        let cases: &[(&str, (u64, u64), &str)] = &[
            ("0", (0x3040_0000_0000_0000, 0), "0"),
            ("-0", (0xB040_0000_0000_0000, 0), "-0"),
            ("1", (0x3040_0000_0000_0000, 1), "1"),
            ("-1", (0xB040_0000_0000_0000, 1), "-1"),
            ("0.001234", (0x3034_0000_0000_0000, 1234), "0.001234"),
            (
                "123456789012",
                (0x3040_0000_0000_0000, 123_456_789_012),
                "123456789012",
            ),
            ("0.0000001234", (0x302C_0000_0000_0000, 1234), "1.234E-7"),
            ("1E+3", (0x3046_0000_0000_0000, 1), "1E+3"),
            ("0E+3", (0x3046_0000_0000_0000, 0), "0E+3"),
            ("Infinity", (0x7800_0000_0000_0000, 0), "Infinity"),
            ("-Infinity", (0xF800_0000_0000_0000, 0), "-Infinity"),
            ("NaN", (0x7C00_0000_0000_0000, 0), "NaN"),
            (
                "9.999999999999999999999999999999999E+6144",
                (0x5FFF_ED09_BEAD_87C0, 0x378D_8E63_FFFF_FFFF),
                "9.999999999999999999999999999999999E+6144",
            ),
            ("1E-6176", (0x0000_0000_0000_0000, 1), "1E-6176"),
            ("12.30", (0x303C_0000_0000_0000, 1230), "12.30"),
        ];
        for (input, want, printed) in cases {
            assert_eq!(parts(input), *want, "{input}");
            let d = Decimal128::from_parts(want.0, want.1);
            assert_eq!(d.to_string(), *printed, "{input}");
            assert_eq!(Decimal128::from_bytes(d.bytes()), d);
        }
    }

    #[test]
    fn exponents_clamp_without_changing_the_value() {
        // "1E+6112" fits once the significand absorbs a zero.
        assert_eq!(parts("1E+6112"), parts("10E+6111"));
        // Trailing zeros beyond 34 digits are absorbed by the exponent.
        let many = format!("1{}", "0".repeat(40));
        assert_eq!(parts(&many), parts(&format!("1{}E+7", "0".repeat(33))));
        assert_eq!(parts("0E-9999"), parts("0E-6176"));
    }

    #[test]
    fn inexact_and_malformed_strings_are_refused() {
        for s in [
            "",
            ".",
            "1.2.3",
            "1e",
            "abc",
            "1E+99999",
            "1E-6177",
            "1234567890123456789012345678901234567",
        ] {
            assert!(Decimal128::parse(s).is_err(), "{s}");
        }
    }

    #[test]
    fn non_canonical_significands_read_as_zero() {
        // Combination 0b11xxx that is neither Infinity nor NaN.
        let d = Decimal128::from_parts(0x6C10_0000_0000_0000, 0);
        assert_eq!(d.to_string(), "0");
    }
}
