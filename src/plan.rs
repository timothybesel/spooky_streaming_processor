use std::collections::HashMap;

use ordered_float::OrderedFloat;

/// Scalar value type used across records, predicates, and the mock AST.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Int(i64),
    /// IEEE-754 double wrapped in `OrderedFloat` so the type is `Hash + Eq`.
    /// SurrealDB stores numeric values as f64; this variant covers that case.
    Float(OrderedFloat<f64>),
    Str(String),
    Bool(bool),
    Null,
}

// ─── Aggregate expression ────────────────────────────────────────────────────

/// A single aggregate function used inside an `Aggregate` plan node.
#[derive(Debug, Clone, PartialEq)]
pub enum AggExpr {
    Count(String),
    Sum(String),
    Min(String),
    Max(String),
    Avg(String),
}

// ─── Sort direction ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

// ─── Predicate ───────────────────────────────────────────────────────────────

/// A scalar predicate evaluated against a single record.
///
/// All variants are exhaustively matched in `eval` — no silent `_ => false`
/// fallback exists.  Adding a new variant without a matching arm in `eval`
/// produces a compile error.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// `col > literal`
    Gt(String, Value),
    /// `col < literal`
    Lt(String, Value),
    /// `col = literal`
    Eq(String, Value),
    /// `col != literal`
    Ne(String, Value),
    /// `col >= literal`  (MoreThanEqual in SurrealDB AST)
    Ge(String, Value),
    /// `col <= literal`  (LessThanEqual in SurrealDB AST)
    Le(String, Value),
    /// Cross-column equality: `col_a = col_b`
    EqCol(String, String),
    /// `col IN [v1, v2, …]` — literal value list
    InList(String, Vec<Value>),
    /// `col NOT IN [v1, v2, …]` — literal value list
    NotInList(String, Vec<Value>),
    /// Logical AND of two sub-predicates
    And(Box<Predicate>, Box<Predicate>),
    /// Logical OR of two sub-predicates
    Or(Box<Predicate>, Box<Predicate>),
    /// Logical NOT of a sub-predicate
    Not(Box<Predicate>),
}

impl Predicate {
    /// Evaluate this predicate against a record.
    ///
    /// All `Predicate` variants are exhaustively covered here.  Any new
    /// variant added to the enum that is not handled here will produce a
    /// compile error rather than silently returning `false`.
    pub fn eval(&self, record: &HashMap<String, Value>) -> bool {
        match self {
            Predicate::Gt(col, threshold) => match (record.get(col), threshold) {
                (Some(Value::Int(v)), Value::Int(t)) => v > t,
                (Some(Value::Float(v)), Value::Float(t)) => v > t,
                _ => false,
            },
            Predicate::Lt(col, threshold) => match (record.get(col), threshold) {
                (Some(Value::Int(v)), Value::Int(t)) => v < t,
                (Some(Value::Float(v)), Value::Float(t)) => v < t,
                _ => false,
            },
            Predicate::Eq(col, val) => record.get(col) == Some(val),
            Predicate::Ne(col, val) => {
                // A missing field is treated as Null; Null != val → false
                // (consistent with SQL three-valued logic)
                record.get(col).map(|v| v != val).unwrap_or(false)
            }
            Predicate::Ge(col, threshold) => match (record.get(col), threshold) {
                (Some(Value::Int(v)), Value::Int(t)) => v >= t,
                (Some(Value::Float(v)), Value::Float(t)) => v >= t,
                _ => false,
            },
            Predicate::Le(col, threshold) => match (record.get(col), threshold) {
                (Some(Value::Int(v)), Value::Int(t)) => v <= t,
                (Some(Value::Float(v)), Value::Float(t)) => v <= t,
                _ => false,
            },
            Predicate::EqCol(col_a, col_b) => record.get(col_a) == record.get(col_b),
            Predicate::InList(col, values) => {
                record.get(col).map(|v| values.contains(v)).unwrap_or(false)
            }
            Predicate::NotInList(col, values) => {
                record.get(col).map(|v| !values.contains(v)).unwrap_or(false)
            }
            Predicate::And(left, right) => left.eval(record) && right.eval(record),
            Predicate::Or(left, right) => left.eval(record) || right.eval(record),
            Predicate::Not(inner) => !inner.eval(record),
        }
    }
}

// ─── JoinType ────────────────────────────────────────────────────────────────

/// Semantics of a binary equi-join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    /// Standard inner join — emit one output per matching pair.
    Inner,
    /// Semi-join — emit the left record (once) when at least one match exists.
    /// Produced by `col IN (subquery)` decorrelation.
    LeftSemi,
    /// Anti-semi-join — emit the left record when NO match exists.
    /// Produced by `col NOT IN (subquery)` decorrelation.
    LeftAnti,
    /// Cartesian product — every left record matched with every right record.
    /// Produced by implicit `FROM a, b` multi-table syntax.
    Cross,
}

// ─── LogicalPlan ─────────────────────────────────────────────────────────────

/// A node in the **logical** query plan tree.
///
/// This is the intermediate representation (IR) between the SQL/AST surface
/// and the physical push-based execution engine.  Optimization passes work
/// on this tree before it is lowered to a `PhysicalPlan`.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Reads every record from a named base relation.
    Scan { table: String },

    /// Applies a predicate and discards non-matching records.
    Filter {
        input: Box<LogicalPlan>,
        predicate: Predicate,
    },

    /// Retains only the listed columns (physical projection).
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },

    /// Binary equi-join over two sub-plans.
    ///
    /// `on` = `(left_key_column, right_key_column)`.
    /// Decorrelated subqueries produce `kind: LeftSemi` or `kind: LeftAnti`.
    /// Multi-table `FROM` produces `kind: Cross` with `on: ("__cross__", "__cross__")`.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: (String, String),
        kind: JoinType,
    },

    /// Set union of two sub-plans (bag semantics — weights add).
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },

    /// Distinct: deduplicate records (retain only net-weight != 0 entries).
    Distinct { input: Box<LogicalPlan> },

    /// Hash aggregate: GROUP BY + aggregate functions.
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        agg: Vec<AggExpr>,
    },

    /// Sort the output by a list of (column, direction) keys.
    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<(String, SortDir)>,
    },

    /// Limit/offset on an ordered result.
    Limit {
        input: Box<LogicalPlan>,
        count: usize,
        offset: usize,
    },
}
