use tokio_postgres::types::{FromSql, Type};

/// NUMERIC: ndigits | weight | sign | dscale | base-10000 groups ->
/// PostgreSQL-normalized text.
pub struct NumericText(pub String);

impl<'a> FromSql<'a> for NumericText {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() < 8 || !raw.len().is_multiple_of(2) {
            return Err("numeric width".into());
        }
        let be16 = |offset: usize| u16::from_be_bytes([raw[offset], raw[offset + 1]]);
        let ndigits = be16(0) as usize;
        if raw.len() != 8 + ndigits * 2 {
            return Err("numeric group count".into());
        }
        let weight = i16::from_be_bytes([raw[2], raw[3]]) as i64;
        let sign = be16(4);
        let dscale = be16(6) as usize;
        let groups: Vec<u16> = (0..ndigits).map(|i| be16(8 + i * 2)).collect();

        let mut text = String::new();
        if sign == 0x4000 {
            text.push('-');
        }
        if groups.is_empty() {
            text.push('0');
            if dscale > 0 {
                text.push('.');
                for _ in 0..dscale {
                    text.push('0');
                }
            }
            return Ok(NumericText(text));
        }
        // Integer part: groups [0..weight+1) when weight >= 0.
        let int_group_count = usize::try_from(weight + 1).unwrap_or(0);
        let frac_groups = groups.get(int_group_count..).unwrap_or(&[]);
        let mut int_part = String::new();
        // PostgreSQL omits trailing zero groups. Weight still locates them
        // in the integer part (for example 10^32 has one stored group).
        for index in 0..int_group_count {
            let group = groups.get(index).copied().unwrap_or(0);
            if index == 0 {
                int_part.push_str(&group.to_string());
            } else {
                int_part.push_str(&format!("{group:04}"));
            }
        }
        if int_part.is_empty() {
            text.push('0');
        } else {
            text.push_str(&int_part);
        }
        if dscale > 0 {
            // Fraction digits from the groups below 10^0, padded to 4,
            // then fixed to exactly dscale digits.
            let mut frac_digits = String::new();
            let mut remaining_groups = frac_groups.to_vec();
            if weight < 0 {
                // All groups are fractional; leading zero digits sit
                // between the decimal point and the first group.
                let leading_zeros = (-weight - 1) * 4;
                for _ in 0..leading_zeros {
                    frac_digits.push('0');
                }
                remaining_groups = groups.clone();
            }
            for group in &remaining_groups {
                frac_digits.push_str(&format!("{group:04}"));
            }
            while frac_digits.len() < dscale {
                frac_digits.push('0');
            }
            frac_digits.truncate(dscale);
            text.push('.');
            text.push_str(&frac_digits);
        }
        Ok(NumericText(text))
    }
    fn accepts(ty: &Type) -> bool {
        ty == &Type::NUMERIC
    }
}
