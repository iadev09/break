use std::{fmt, str::FromStr};

/// Exact required success fraction. Decisions never use floating-point rounding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Acceptance {
    numerator: u64,
    denominator: u64,
}

impl Acceptance {
    pub fn new(
        numerator: u64,
        denominator: u64,
    ) -> Result<Self, &'static str> {
        if numerator == 0 || denominator == 0 || numerator > denominator {
            return Err(
                "acceptance must be greater than 0 and at most 100 percent",
            );
        }
        Ok(Self { numerator, denominator })
    }

    pub fn accepts(
        self,
        successful: u64,
        attempted: u64,
    ) -> bool {
        attempted > 0
            && successful <= attempted
            && u128::from(successful) * u128::from(self.denominator)
                >= u128::from(attempted) * u128::from(self.numerator)
    }
}

impl FromStr for Acceptance {
    type Err = &'static str;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (whole, fractional) = input.split_once('.').unwrap_or((input, ""));
        if whole.is_empty()
            || !whole.bytes().all(|b| b.is_ascii_digit())
            || !fractional.bytes().all(|b| b.is_ascii_digit())
        {
            return Err("acceptance must be a decimal percentage");
        }
        let precision = u32::try_from(fractional.len())
            .map_err(|_| "acceptance precision is too large")?;
        let scale = 10_u64
            .checked_pow(precision)
            .ok_or("acceptance precision is too large")?;
        let whole = whole
            .parse::<u64>()
            .map_err(|_| "invalid acceptance percentage")?;
        let fraction = if fractional.is_empty() {
            0
        } else {
            fractional
                .parse::<u64>()
                .map_err(|_| "invalid acceptance percentage")?
        };
        let numerator = whole
            .checked_mul(scale)
            .and_then(|v| v.checked_add(fraction))
            .ok_or("acceptance percentage overflows")?;
        let denominator = scale
            .checked_mul(100)
            .ok_or("acceptance precision is too large")?;
        Self::new(numerator, denominator)
    }
}

impl TryFrom<f64> for Acceptance {
    type Error = &'static str;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if !value.is_finite() {
            return Err("acceptance must be a finite percentage");
        }
        value.to_string().parse()
    }
}

impl fmt::Display for Acceptance {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "{:.4}%",
            self.numerator as f64 * 100.0 / self.denominator as f64
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acceptance_observes_exact_boundary_without_rounding() {
        let acceptance: Acceptance = "99.99".parse().unwrap();
        assert!(acceptance.accepts(9_999, 10_000));
        assert!(!acceptance.accepts(9_998, 10_000));
        assert!(!acceptance.accepts(4_999, 5_000));
        assert!(!acceptance.accepts(0, 0));
        assert!(acceptance.accepts(u64::MAX, u64::MAX));
    }

    #[test]
    fn percentages_are_validated_before_traffic() {
        assert!("100".parse::<Acceptance>().unwrap().accepts(1, 1));
        for invalid in ["0", "100.01", "-1", "NaN", "99.9.9", ".99"] {
            assert!(invalid.parse::<Acceptance>().is_err(), "{invalid}");
        }
    }
}
