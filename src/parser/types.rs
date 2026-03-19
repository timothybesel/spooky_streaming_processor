#[derive(Debug, Clone)]
pub enum Field {
    Wildcard,                               // Für das '*'
    Ident(String),                          // Für normale Felder wie 'name', 'email'
    Subquery(Box<SelectStatement>, String), // Für (SELECT ...) AS alias
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub fields: Vec<Field>,
    pub table: String,
    pub where_: Option<WhereExpr>,
}

// ----- WHERE clause ------
#[derive(Debug, Clone)]
pub enum WhereExpr {
    /// `left op right`  e.g. `age > 18`, `name = "alice"`
    BinaryOp {
        op: WhereOp,
        left: WhereOperand,
        right: WhereOperand,
    },
    /// `left AND right`
    And(Box<WhereExpr>, Box<WhereExpr>),
    /// `left OR right`
    Or(Box<WhereExpr>, Box<WhereExpr>),
    /// `NOT expr`  or  `!expr`
    Not(Box<WhereExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhereOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    In,
    Contains,
}

#[derive(Debug, Clone)]
pub enum WhereOperand {
    Column(String),                 // a field name:        age, name, id
    LiteralInt(i64),                // an integer:          18, -5, 0
    LiteralFloat(f64),              // a float:             3.14
    LiteralStr(String),             // a string:            "alice"
    LiteralBool(bool),              // a boolean:           true, false
    LiteralNull,                    // null
    Subquery(Box<SelectStatement>), // a nested SELECT:     (SELECT id FROM order)
    /// `$name` or `$parent.field` — a parameter reference.
    /// Stores the path segments after `$` (e.g. `["auth", "id"]` for `$auth.id`).
    Param(Vec<String>),
    /// A range literal: `18..65`, `18..=65`, `18>..=65`, `..100`, etc.
    Range(Box<RangeBound>),
}

/// A SurrealQL range: `lo..hi`, `lo..=hi`, `lo>..hi`, `lo>..=hi`,
/// with optional open ends (`..hi`, `lo..`, `..`).
#[derive(Debug, Clone)]
pub struct RangeBound {
    pub lo: Option<WhereOperand>,
    /// `>` prefix on lower bound → exclusive
    pub lo_exclusive: bool,
    pub hi: Option<WhereOperand>,
    /// `=` suffix on upper bound → inclusive
    pub hi_inclusive: bool,
}
