//! # Lowering: Mock SurrealDB AST → Logical Plan
//!
//! This module translates a simplified SurrealDB-style SELECT AST into a
//! `LogicalPlan`.  The mock AST types (`MockSelectStmt`, `MockCond`, etc.)
//! mirror the real `surrealdb_core` AST but are simpler to construct in tests.
//!
//! ## The decorrelation trick
//!
//! `col IN (SELECT ...)` is a correlated subquery — DBSP cannot evaluate
//! correlated subqueries incrementally.  The solution is **decorrelation**:
//! detect the `Inside` / `IN` pattern during lowering and rewrite it as a
//! `Join { kind: LeftSemi }` in the logical plan.
//!
//! ## Error handling
//!
//! `lower_select` returns `Result<LogicalPlan, LoweringError>`.  Unrecognised
//! conditions produce an explicit error instead of silently falling back to
//! a full-table scan (C1 fix).

use crate::plan::{JoinType, LogicalPlan, Predicate, Value};

// ─── LoweringError ───────────────────────────────────────────────────────────

/// Errors produced by `lower_select`.
#[derive(Debug, thiserror::Error)]
pub enum LoweringError {
    #[error("unsupported WHERE condition: {description}")]
    UnsupportedCondition { description: String },

    #[error("INSIDE/IN requires a subquery on the right side; got a scalar for column '{col}'")]
    InsideRequiresSubquery { col: String },

    #[error("ambiguous join key: subquery for '{outer_col}' projects no column — use SELECT VALUE <col>")]
    AmbiguousJoinKey { outer_col: String },

    #[error("cross-column predicate '{left_col} {op} {right_col}' is not yet supported")]
    CrossColumnPredicateUnsupported {
        left_col: String,
        op: String,
        right_col: String,
    },

    #[error("query parameter '${name}' must be bound before planning")]
    UnboundParameter { name: String },

    #[error("maximum subquery nesting depth ({max_depth}) exceeded")]
    MaxNestingDepthExceeded { max_depth: usize },

    #[error("FROM clause is empty")]
    EmptyFrom,
}

// ─── Mock SurrealDB AST types ─────────────────────────────────────────────────
//
// These types are kept `pub(crate)` — they are test helpers, not the public
// API boundary.  Production code uses the real `surrealdb_core::sql` types
// via `real_lowering::lower_real_select`.

/// Simplified mock of a SurrealDB `SELECT` statement.
pub(crate) struct MockSelectStmt {
    /// `FROM` target — the outer table name.
    pub(crate) table: String,
    /// Columns named in the `SELECT` list (empty = `SELECT *`).
    pub(crate) select_fields: Vec<String>,
    /// Optional `WHERE` clause.
    pub(crate) cond: Option<MockCond>,
}

/// A condition node in the `WHERE` clause (may be nested).
pub(crate) enum MockCond {
    BinaryOp {
        op: MockOp,
        left: MockExpr,
        right: MockExpr,
    },
    /// Logical AND of two sub-conditions.
    And(Box<MockCond>, Box<MockCond>),
    /// Logical OR of two sub-conditions.
    Or(Box<MockCond>, Box<MockCond>),
    /// Logical NOT of a sub-condition.
    Not(Box<MockCond>),
}

/// Operators in a `WHERE` binary expression.
#[derive(Debug, Clone, Copy)]
pub(crate) enum MockOp {
    /// `col IN (...)` / `col INSIDE (...)` — triggers decorrelation.
    Inside,
    Gt,
    Lt,
    Eq,
    /// `!=` / `NotEqual`
    Ne,
    /// `>=` / `MoreThanEqual`
    Ge,
    /// `<=` / `LessThanEqual`
    Le,
}

/// An expression on either side of a binary operator.
pub(crate) enum MockExpr {
    Column(String),
    Literal(Value),
    /// A full nested SELECT — becomes the right-hand side of a Semi-Join.
    Subquery(Box<MockSelectStmt>),
}

// ─── Public API ──────────────────────────────────────────────────────────────

/// Lower a mock `SELECT` statement to a `LogicalPlan`.
///
/// Returns `Err(LoweringError)` for any condition that cannot be represented
/// in the current `LogicalPlan` IR.  No silent fallbacks.
pub fn lower_select(stmt: MockSelectStmt) -> Result<LogicalPlan, LoweringError> {
    if stmt.table.is_empty() {
        return Err(LoweringError::EmptyFrom);
    }

    let scan = LogicalPlan::Scan { table: stmt.table };

    // Lower the WHERE clause (if present) on top of the base scan.
    let plan = match stmt.cond {
        None => scan,
        Some(cond) => lower_cond(cond, scan)?,
    };

    // Wrap with Project if specific columns were named (not SELECT *).
    let plan = if stmt.select_fields.is_empty() {
        plan
    } else {
        LogicalPlan::Project {
            input: Box::new(plan),
            columns: stmt.select_fields,
        }
    };

    Ok(plan)
}

// ─── Condition lowering ───────────────────────────────────────────────────────

/// Lower a `MockCond` onto a base plan, returning the new plan.
///
/// * `AND` conditions stack as nested `Filter` nodes.
/// * `OR` conditions produce a `Filter { predicate: Or(...) }`.
/// * `Inside(col, subquery)` decorrelates to a `Join { kind: LeftSemi }`.
/// * Any unrecognised shape returns `Err(LoweringError::UnsupportedCondition)`.
fn lower_cond(cond: MockCond, base: LogicalPlan) -> Result<LogicalPlan, LoweringError> {
    match cond {
        // ── Decorrelation: col IN (subquery) → Semi-Join ─────────────────
        MockCond::BinaryOp {
            op: MockOp::Inside,
            left: MockExpr::Column(outer_col),
            right: MockExpr::Subquery(sub_stmt),
        } => {
            let inner_col = sub_stmt
                .select_fields
                .first()
                .cloned()
                .ok_or_else(|| LoweringError::AmbiguousJoinKey {
                    outer_col: outer_col.clone(),
                })?;

            let sub_plan = lower_select(*sub_stmt)?;

            Ok(LogicalPlan::Join {
                left: Box::new(base),
                right: Box::new(sub_plan),
                on: (outer_col, inner_col),
                kind: JoinType::LeftSemi,
            })
        }

        // ── C5 fix: Inside with a scalar is semantically invalid ──────────
        MockCond::BinaryOp {
            op: MockOp::Inside,
            left: MockExpr::Column(col),
            right: MockExpr::Literal(_),
        } => Err(LoweringError::InsideRequiresSubquery { col }),

        // ── Normal scalar predicate → Filter ─────────────────────────────
        MockCond::BinaryOp {
            op,
            left: MockExpr::Column(col),
            right: MockExpr::Literal(val),
        } => {
            let predicate = lower_op(op, col, val)?;
            Ok(LogicalPlan::Filter {
                input: Box::new(base),
                predicate,
            })
        }

        // ── Logical AND: stack two filters ────────────────────────────────
        MockCond::And(left_cond, right_cond) => {
            let plan = lower_cond(*left_cond, base)?;
            lower_cond(*right_cond, plan)
        }

        // ── Logical OR: produce a compound predicate ──────────────────────
        MockCond::Or(left_cond, right_cond) => {
            let left_pred = extract_simple_predicate(*left_cond)?;
            let right_pred = extract_simple_predicate(*right_cond)?;
            Ok(LogicalPlan::Filter {
                input: Box::new(base),
                predicate: Predicate::Or(Box::new(left_pred), Box::new(right_pred)),
            })
        }

        // ── Logical NOT ───────────────────────────────────────────────────
        MockCond::Not(inner) => {
            let pred = extract_simple_predicate(*inner)?;
            Ok(LogicalPlan::Filter {
                input: Box::new(base),
                predicate: Predicate::Not(Box::new(pred)),
            })
        }

        // ── C1 fix: unrecognised shapes → explicit error ──────────────────
        other => Err(LoweringError::UnsupportedCondition {
            description: describe_cond(&other),
        }),
    }
}

/// Lower a binary operator + column + literal to a `Predicate`.
fn lower_op(op: MockOp, col: String, val: Value) -> Result<Predicate, LoweringError> {
    match op {
        MockOp::Gt => Ok(Predicate::Gt(col, val)),
        MockOp::Lt => Ok(Predicate::Lt(col, val)),
        MockOp::Eq => Ok(Predicate::Eq(col, val)),
        MockOp::Ne => Ok(Predicate::Ne(col, val)),
        MockOp::Ge => Ok(Predicate::Ge(col, val)),
        MockOp::Le => Ok(Predicate::Le(col, val)),
        MockOp::Inside => Err(LoweringError::InsideRequiresSubquery { col }),
    }
}

/// Extract a simple `Predicate` from a condition that cannot contain joins.
/// Used for the operands of OR/NOT, which cannot decorrelate subqueries.
fn extract_simple_predicate(cond: MockCond) -> Result<Predicate, LoweringError> {
    match cond {
        MockCond::BinaryOp {
            op,
            left: MockExpr::Column(col),
            right: MockExpr::Literal(val),
        } => lower_op(op, col, val),

        MockCond::And(l, r) => {
            let lp = extract_simple_predicate(*l)?;
            let rp = extract_simple_predicate(*r)?;
            Ok(Predicate::And(Box::new(lp), Box::new(rp)))
        }

        MockCond::Or(l, r) => {
            let lp = extract_simple_predicate(*l)?;
            let rp = extract_simple_predicate(*r)?;
            Ok(Predicate::Or(Box::new(lp), Box::new(rp)))
        }

        MockCond::Not(inner) => {
            let p = extract_simple_predicate(*inner)?;
            Ok(Predicate::Not(Box::new(p)))
        }

        other => Err(LoweringError::UnsupportedCondition {
            description: describe_cond(&other),
        }),
    }
}

/// Human-readable description of a condition — used in error messages.
fn describe_cond(cond: &MockCond) -> String {
    match cond {
        MockCond::BinaryOp { op, left, right } => {
            let l = match left {
                MockExpr::Column(c) => c.clone(),
                MockExpr::Literal(v) => format!("{v:?}"),
                MockExpr::Subquery(_) => "(subquery)".into(),
            };
            let r = match right {
                MockExpr::Column(c) => c.clone(),
                MockExpr::Literal(v) => format!("{v:?}"),
                MockExpr::Subquery(_) => "(subquery)".into(),
            };
            format!("{l} {:?} {r}", op)
        }
        MockCond::And(_, _) => "AND".into(),
        MockCond::Or(_, _) => "OR".into(),
        MockCond::Not(_) => "NOT".into(),
    }
}
