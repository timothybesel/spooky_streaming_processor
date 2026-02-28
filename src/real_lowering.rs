//! # Real SurrealDB AST → Logical Plan (stub)
//!
//! ## Current status: API not yet accessible
//!
//! `surrealdb-core 3.0.0` declares all its SQL AST types as `pub(crate)`:
//!
//! ```text
//! // In surrealdb-core/src/sql/mod.rs:
//! pub(crate) use self::cond::Cond;
//! pub(crate) use self::field::Field;
//! pub(crate) use self::idiom::Idiom;
//! // ... all types are pub(crate), not pub
//! ```
//!
//! External crates cannot reference `surrealdb_core::sql::SelectStatement`,
//! `Cond`, `Field`, `Value`, `Operator`, or any other AST type.
//! The `syn::parse()` function returns an `Ast` that can only be used as a
//! `Debug`-printable blob from outside the crate.
//!
//! ## Migration path
//!
//! Three options to complete this layer:
//!
//! 1. **Use the `surrealdb` client crate** (not `surrealdb-core`): the public
//!    client exposes AST types via feature flags.  Update `Cargo.toml`:
//!    ```toml
//!    surrealdb = { version = "3", features = ["parser"] }
//!    ```
//!
//! 2. **Patch surrealdb-core**: fork the crate and add `pub use` re-exports
//!    for the types needed by this lowering layer.
//!
//! 3. **Wait for the API to stabilize**: if SurrealDB exposes its AST publicly
//!    in a future version, import from there.
//!
//! ## Intended interface (for reference)
//!
//! Once the AST types are accessible, `lower_real_select` should have this
//! signature and follow the patterns documented in
//! `src/ssc_simu/query.rs` (the Debug-printed real AST):
//!
//! ```text
//! pub fn lower_real_select(
//!     stmt: surrealdb_core::sql::SelectStatement,
//! ) -> Result<LogicalPlan, LoweringError>
//! ```
//!
//! The `query.rs` file shows the AST structure for a real SELECT:
//! - `stmt.what` → `Vec<Value>` where `Value::Table(t)` is a base scan
//! - `stmt.cond` → `Option<Cond(Value::Binary { left, op, right })>`
//! - `stmt.expr` → `Fields` with `Field::All` or `Field::Single(Selector)`
//! - `stmt.limit` → `Option<Limit(Value::Literal(Integer(n)))>`
//! - `op` is `Operator::Equal | MoreThan | LessThan | MoreThanOrEqual | ...`
//!
//! The implementation would call `lower_cond` from `lowering.rs` after
//! translating each real AST node to the mock types, or inline the same
//! logic using the real types directly.

use crate::lowering::LoweringError;
use crate::plan::LogicalPlan;

/// Placeholder: real AST lowering is not yet possible because
/// `surrealdb_core` 3.0.0 does not expose its SQL types as public APIs.
///
/// See the module documentation for the migration path.
pub fn lower_real_select_stub() -> Result<LogicalPlan, LoweringError> {
    Err(LoweringError::UnsupportedCondition {
        description: concat!(
            "real AST lowering requires public surrealdb SQL types; ",
            "surrealdb-core 3.0.0 declares all AST types as pub(crate). ",
            "See src/real_lowering.rs module docs for migration options."
        )
        .into(),
    })
}
