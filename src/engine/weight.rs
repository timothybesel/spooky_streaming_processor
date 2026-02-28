//! # DBSP weight type
//!
//! `Weight` is a thin `i64` newtype that represents the multiplicity of a
//! record in a Z-set.  The type enforces the DBSP algebra constraint that
//! two weights cannot be **multiplied together** without explicit intent:
//!
//! * `Weight * Weight` is dimensionally invalid (gives "weight²"), so `Mul`
//!   is **intentionally not implemented** for `(Weight, Weight)`.
//! * The join's bilinear product `w_left × w_right` must be expressed as
//!   `Weight::join_product(left, right)` — intentionally verbose, making
//!   the bilinear operation visible in code review.
//! * Scalar multiplication `weight.scale(n)` is provided for batch sizing.

use std::ops::{Add, Neg};

/// The weight (multiplicity) of a record in a Z-set.
///
/// * `+1` — insertion / presence
/// * `-1` — deletion / retraction
/// * Other values arise from joins and batching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Weight(pub i64);

impl Weight {
    /// Unit insertion weight (`+1`).
    pub const INSERT: Self = Weight(1);
    /// Unit deletion weight (`-1`).
    pub const DELETE: Self = Weight(-1);
    /// Zero weight — records with this weight are pruned from ZSets.
    pub const ZERO: Self = Weight(0);

    /// Scale by a plain integer — e.g., for batched insertions.
    #[inline]
    pub fn scale(self, factor: i64) -> Self {
        Weight(self.0 * factor)
    }

    /// Returns `true` if the weight is zero (record can be pruned).
    #[inline]
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Bilinear join weight product: `w_left × w_right`.
    ///
    /// This is the only correct way to multiply two weights.  It is named
    /// explicitly to make the bilinear operation visible in join code.
    #[inline]
    pub fn join_product(left: Weight, right: Weight) -> Weight {
        Weight(left.0 * right.0)
    }
}

impl Add for Weight {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Weight(self.0 + rhs.0)
    }
}

impl Neg for Weight {
    type Output = Self;
    fn neg(self) -> Self {
        Weight(-self.0)
    }
}

// Intentionally NO: `impl Mul<Weight> for Weight`
// Multiplying two weights (weight²) is a dimensional error in DBSP.
// Use `Weight::join_product(a, b)` instead.
