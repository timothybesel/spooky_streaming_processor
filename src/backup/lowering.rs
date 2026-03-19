//! # Lowering: SurrealQL SELECT AST → Logical Plan
//!
//! This module translates a simplified SurrealDB-style SELECT AST into a
//! `LogicalPlan`.  The AST types (`SelectStmt`, `SelectCond`, etc.)
//! mirror the real `surrealdb_core` AST structure but are owned by this crate.
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

// ─── SurrealQL SELECT AST types ──────────────────────────────────────────────

/// A parsed SurrealQL `SELECT` statement.
///
/// Produced by `parser::parse_select` and consumed by `lower_select`.
pub struct SelectStmt {
    /// `FROM` target — the outer table name.
    pub table: String,
    /// Columns named in the `SELECT` list (empty = `SELECT *`).
    pub select_fields: Vec<String>,
    /// Optional `WHERE` clause.
    pub cond: Option<SelectCond>,
}

/// A condition node in the `WHERE` clause (may be nested).
pub enum SelectCond {
    BinaryOp {
        op: SelectOp,
        left: SelectExpr,
        right: SelectExpr,
    },
    /// Logical AND of two sub-conditions.
    And(Box<SelectCond>, Box<SelectCond>),
    /// Logical OR of two sub-conditions.
    Or(Box<SelectCond>, Box<SelectCond>),
    /// Logical NOT of a sub-condition.
    Not(Box<SelectCond>),
}

/// Operators in a `WHERE` binary expression.
#[derive(Debug, Clone, Copy)]
pub enum SelectOp {
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
pub enum SelectExpr {
    Column(String),
    Literal(Value),
    /// A full nested SELECT — becomes the right-hand side of a Semi-Join.
    Subquery(Box<SelectStmt>),
}

// ─── Public API ──────────────────────────────────────────────────────────────

/// Lower a `SelectStmt` to a `LogicalPlan`.
///
/// Returns `Err(LoweringError)` for any condition that cannot be represented
/// in the current `LogicalPlan` IR.  No silent fallbacks.
pub fn lower_select(stmt: SelectStmt) -> Result<LogicalPlan, LoweringError> {
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

/// Lower a `SelectCond` onto a base plan, returning the new plan.
///
/// * `AND` conditions stack as nested `Filter` nodes.
/// * `OR` conditions produce a `Filter { predicate: Or(...) }`.
/// * `Inside(col, subquery)` decorrelates to a `Join { kind: LeftSemi }`.
/// * Any unrecognised shape returns `Err(LoweringError::UnsupportedCondition)`.
fn lower_cond(cond: SelectCond, base: LogicalPlan) -> Result<LogicalPlan, LoweringError> {
    match cond {
        // ── Decorrelation: col IN (subquery) → Semi-Join ─────────────────
        SelectCond::BinaryOp {
            op: SelectOp::Inside,
            left: SelectExpr::Column(outer_col),
            right: SelectExpr::Subquery(sub_stmt),
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
        SelectCond::BinaryOp {
            op: SelectOp::Inside,
            left: SelectExpr::Column(col),
            right: SelectExpr::Literal(_),
        } => Err(LoweringError::InsideRequiresSubquery { col }),

        // ── Normal scalar predicate → Filter ─────────────────────────────
        SelectCond::BinaryOp {
            op,
            left: SelectExpr::Column(col),
            right: SelectExpr::Literal(val),
        } => {
            let predicate = lower_op(op, col, val)?;
            Ok(LogicalPlan::Filter {
                input: Box::new(base),
                predicate,
            })
        }

        // ── Logical AND: stack two filters ────────────────────────────────
        SelectCond::And(left_cond, right_cond) => {
            let plan = lower_cond(*left_cond, base)?;
            lower_cond(*right_cond, plan)
        }

        // ── Logical OR: produce a compound predicate ──────────────────────
        SelectCond::Or(left_cond, right_cond) => {
            let left_pred = extract_simple_predicate(*left_cond)?;
            let right_pred = extract_simple_predicate(*right_cond)?;
            Ok(LogicalPlan::Filter {
                input: Box::new(base),
                predicate: Predicate::Or(Box::new(left_pred), Box::new(right_pred)),
            })
        }

        // ── Logical NOT ───────────────────────────────────────────────────
        SelectCond::Not(inner) => {
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
fn lower_op(op: SelectOp, col: String, val: Value) -> Result<Predicate, LoweringError> {
    match op {
        SelectOp::Gt => Ok(Predicate::Gt(col, val)),
        SelectOp::Lt => Ok(Predicate::Lt(col, val)),
        SelectOp::Eq => Ok(Predicate::Eq(col, val)),
        SelectOp::Ne => Ok(Predicate::Ne(col, val)),
        SelectOp::Ge => Ok(Predicate::Ge(col, val)),
        SelectOp::Le => Ok(Predicate::Le(col, val)),
        SelectOp::Inside => Err(LoweringError::InsideRequiresSubquery { col }),
    }
}

/// Extract a simple `Predicate` from a condition that cannot contain joins.
/// Used for the operands of OR/NOT, which cannot decorrelate subqueries.
fn extract_simple_predicate(cond: SelectCond) -> Result<Predicate, LoweringError> {
    match cond {
        SelectCond::BinaryOp {
            op,
            left: SelectExpr::Column(col),
            right: SelectExpr::Literal(val),
        } => lower_op(op, col, val),

        SelectCond::And(l, r) => {
            let lp = extract_simple_predicate(*l)?;
            let rp = extract_simple_predicate(*r)?;
            Ok(Predicate::And(Box::new(lp), Box::new(rp)))
        }

        SelectCond::Or(l, r) => {
            let lp = extract_simple_predicate(*l)?;
            let rp = extract_simple_predicate(*r)?;
            Ok(Predicate::Or(Box::new(lp), Box::new(rp)))
        }

        SelectCond::Not(inner) => {
            let p = extract_simple_predicate(*inner)?;
            Ok(Predicate::Not(Box::new(p)))
        }

        other => Err(LoweringError::UnsupportedCondition {
            description: describe_cond(&other),
        }),
    }
}

/// Human-readable description of a condition — used in error messages.
fn describe_cond(cond: &SelectCond) -> String {
    match cond {
        SelectCond::BinaryOp { op, left, right } => {
            let l = match left {
                SelectExpr::Column(c) => c.clone(),
                SelectExpr::Literal(v) => format!("{v:?}"),
                SelectExpr::Subquery(_) => "(subquery)".into(),
            };
            let r = match right {
                SelectExpr::Column(c) => c.clone(),
                SelectExpr::Literal(v) => format!("{v:?}"),
                SelectExpr::Subquery(_) => "(subquery)".into(),
            };
            format!("{l} {:?} {r}", op)
        }
        SelectCond::And(_, _) => "AND".into(),
        SelectCond::Or(_, _) => "OR".into(),
        SelectCond::Not(_) => "NOT".into(),
    }
}
