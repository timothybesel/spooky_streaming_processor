//! SurrealQL SELECT parser.
//!
//! Grammar implemented:
//! ```text
//! select_stmt := SELECT fields FROM ident
//!                [ WHERE condition ]
//!                [ ORDER BY order_list ]
//!                [ LIMIT integer ]
//!                [ START [AT] integer ]
//!
//! fields      := * | ident (, ident)*
//!
//! condition   := or_expr
//! or_expr     := and_expr (OR and_expr)*
//! and_expr    := not_expr (AND not_expr)*
//! not_expr    := NOT not_expr | atom_cond
//! atom_cond   := ident op literal
//!              | ident IN ( select_stmt )   ← becomes LeftSemi join
//!              | ( condition )
//!
//! op          := = | != | > | >= | < | <=
//!
//! literal     := integer | float | string | true | false | null | none
//! order_list  := order_item (, order_item)*
//! order_item  := ident [ ASC | DESC ]
//!
//! ident       := [a-zA-Z_][a-zA-Z0-9_]*
//!              | `backtick quoted`
//! ```
//!
//! Keywords are case-insensitive. ORDER BY, LIMIT, START are parsed but not
//! yet wired into `SelectStmt` — they are discarded so callers can add them
//! to the struct later without touching this file.

use winnow::{
    ModalResult, Parser,
    ascii::{digit1, multispace0},
    error::ContextError,
    token::take_while,
};
use ordered_float::OrderedFloat;

use crate::lowering::{LoweringError, SelectCond, SelectExpr, SelectOp, SelectStmt, lower_select};
use crate::plan::{LogicalPlan, Value};

// ─── Public error type ────────────────────────────────────────────────────────

/// Errors produced by the SELECT parser.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("syntax error: {0}")]
    Syntax(String),

    #[error(transparent)]
    Lowering(#[from] LoweringError),
}

// ─── Public entry point ───────────────────────────────────────────────────────

/// Parse a SurrealQL `SELECT` string into a `LogicalPlan`.
///
/// The full pipeline is: `&str` → [`SelectStmt`] → [`lower_select`] → `LogicalPlan`.
///
/// Keywords are case-insensitive. Leading / trailing whitespace is ignored.
/// The entire input must be consumed — trailing tokens are a syntax error.
///
/// # Errors
/// - [`ParseError::Syntax`] — grammar mismatch or trailing garbage
/// - [`ParseError::Lowering`] — semantic error (e.g. empty FROM, IN with a scalar)
pub fn parse_select(input: &str) -> Result<LogicalPlan, ParseError> {
    let stmt = parse_select_stmt
        .parse(input.trim())
        .map_err(|e| ParseError::Syntax(e.to_string()))?;
    Ok(lower_select(stmt)?)
}

// ─── Statement ────────────────────────────────────────────────────────────────

fn parse_select_stmt<'s>(input: &mut &'s str) -> ModalResult<SelectStmt> {
    kw("SELECT", input)?;
    let select_fields = parse_field_list(input)?;
    kw("FROM", input)?;
    let table = parse_ident(input)?;

    // Optional WHERE clause.
    let cond = parse_where_opt(input)?;

    // Optional ORDER BY / START / LIMIT — parsed and discarded for now.
    // SurrealQL allows START before LIMIT, so START is tried first.
    // Add fields to SelectStmt and return them here when needed.
    parse_order_by_opt(input)?;
    parse_start_opt(input)?;
    parse_limit_opt(input)?;

    let _ = multispace0.parse_next(input)?;
    Ok(SelectStmt { table, select_fields, cond })
}

// ─── Field list ───────────────────────────────────────────────────────────────

/// `*` → empty Vec (= SELECT *), or `ident, ident, …`.
fn parse_field_list<'s>(input: &mut &'s str) -> ModalResult<Vec<String>> {
    let _ = multispace0.parse_next(input)?;
    let checkpoint = *input;
    if input.starts_with('*') {
        *input = &input[1..];
        return Ok(vec![]);
    }
    *input = checkpoint;
    parse_comma_list(input)
}

/// One or more identifiers separated by commas.
fn parse_comma_list<'s>(input: &mut &'s str) -> ModalResult<Vec<String>> {
    let first = parse_ident(input)?;
    let mut items = vec![first];
    loop {
        let checkpoint = *input;
        let _ = multispace0.parse_next(input)?;
        if !input.starts_with(',') {
            *input = checkpoint;
            break;
        }
        *input = &input[1..]; // consume ','
        match parse_ident(input) {
            Ok(id) => items.push(id),
            Err(_) => { *input = checkpoint; break; }
        }
    }
    Ok(items)
}

// ─── Identifier ───────────────────────────────────────────────────────────────

/// `[a-zA-Z_][a-zA-Z0-9_]*` or `` `backtick quoted` ``.
/// Leading whitespace is skipped.
fn parse_ident<'s>(input: &mut &'s str) -> ModalResult<String> {
    let _ = multispace0.parse_next(input)?;

    // Backtick-quoted identifier (allows reserved words and spaces).
    if input.starts_with('`') {
        *input = &input[1..]; // consume opening `
        let content: &str =
            take_while(1.., |c: char| c != '`').parse_next(input)?;
        if input.starts_with('`') {
            *input = &input[1..]; // consume closing `
            return Ok(content.to_string());
        }
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    let first: &str =
        take_while(1.., |c: char| c.is_ascii_alphabetic() || c == '_').parse_next(input)?;
    let rest: &str =
        take_while(0.., |c: char| c.is_ascii_alphanumeric() || c == '_').parse_next(input)?;
    Ok(format!("{first}{rest}"))
}

// ─── Keyword matching ─────────────────────────────────────────────────────────

/// Case-insensitive keyword that refuses to match if immediately followed by
/// an alphanumeric or `_` (i.e. it's a prefix of a longer identifier).
///
/// Skips leading whitespace.  On failure, **does not** restore whitespace —
/// callers that need backtracking must save a checkpoint before calling.
///
/// All built-in keywords are ASCII so `kw.len()` bytes == chars.
fn kw<'s>(kw: &'static str, input: &mut &'s str) -> ModalResult<()> {
    let _ = multispace0.parse_next(input)?;

    let upper: String = input.chars().take(kw.len()).flat_map(char::to_uppercase).collect();
    if upper != kw {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }
    if input.chars().nth(kw.len()).map_or(false, |c| c.is_alphanumeric() || c == '_') {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }
    *input = &input[kw.len()..];
    Ok(())
}

// ─── WHERE clause ─────────────────────────────────────────────────────────────

fn parse_where_opt<'s>(input: &mut &'s str) -> ModalResult<Option<SelectCond>> {
    let checkpoint = *input;
    if kw("WHERE", input).is_ok() {
        return Ok(Some(parse_or_expr(input)?));
    }
    *input = checkpoint;
    Ok(None)
}

// ── Condition grammar: OR > AND > NOT > atom ──────────────────────────────────

/// `or_expr := and_expr (OR and_expr)*`
fn parse_or_expr<'s>(input: &mut &'s str) -> ModalResult<SelectCond> {
    let first = parse_and_expr(input)?;
    let mut acc = first;
    loop {
        let checkpoint = *input;
        if kw("OR", input).is_ok() {
            match parse_and_expr(input) {
                Ok(right) => acc = SelectCond::Or(Box::new(acc), Box::new(right)),
                Err(_) => { *input = checkpoint; break; }
            }
        } else {
            *input = checkpoint;
            break;
        }
    }
    Ok(acc)
}

/// `and_expr := not_expr (AND not_expr)*`
fn parse_and_expr<'s>(input: &mut &'s str) -> ModalResult<SelectCond> {
    let first = parse_not_expr(input)?;
    let mut acc = first;
    loop {
        let checkpoint = *input;
        if kw("AND", input).is_ok() {
            match parse_not_expr(input) {
                Ok(right) => acc = SelectCond::And(Box::new(acc), Box::new(right)),
                Err(_) => { *input = checkpoint; break; }
            }
        } else {
            *input = checkpoint;
            break;
        }
    }
    Ok(acc)
}

/// `not_expr := NOT not_expr | atom_cond`
fn parse_not_expr<'s>(input: &mut &'s str) -> ModalResult<SelectCond> {
    let checkpoint = *input;
    if kw("NOT", input).is_ok() {
        let inner = parse_not_expr(input)?;
        return Ok(SelectCond::Not(Box::new(inner)));
    }
    *input = checkpoint;
    parse_atom_cond(input)
}

/// `atom_cond := ( condition ) | ident IN ( select_stmt ) | ident op literal`
fn parse_atom_cond<'s>(input: &mut &'s str) -> ModalResult<SelectCond> {
    let _ = multispace0.parse_next(input)?;

    // ── Parenthesised condition: ( or_expr ) ──────────────────────────────
    if input.starts_with('(') {
        let checkpoint = *input;
        *input = &input[1..]; // consume '('
        match parse_or_expr(input) {
            Ok(cond) => {
                let _ = multispace0.parse_next(input)?;
                if input.starts_with(')') {
                    *input = &input[1..]; // consume ')'
                    return Ok(cond);
                }
                *input = checkpoint;
            }
            Err(_) => *input = checkpoint,
        }
    }

    // ── ident op literal  OR  ident IN ( select_stmt ) ───────────────────
    let col = parse_ident(input)?;
    let checkpoint = *input;

    // Try IN keyword.
    if kw("IN", input).is_ok() {
        let _ = multispace0.parse_next(input)?;
        if !input.starts_with('(') {
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        }
        *input = &input[1..]; // consume '('

        // Try subquery first.  On failure, fall back to a scalar literal so
        // that lowering (not the parser) can produce InsideRequiresSubquery.
        let inner_checkpoint = *input;
        let right = if let Ok(sub) = parse_select_stmt(input) {
            SelectExpr::Subquery(Box::new(sub))
        } else {
            *input = inner_checkpoint;
            SelectExpr::Literal(parse_literal(input)?)
        };

        let _ = multispace0.parse_next(input)?;
        if !input.starts_with(')') {
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        }
        *input = &input[1..]; // consume ')'
        return Ok(SelectCond::BinaryOp {
            op: SelectOp::Inside,
            left: SelectExpr::Column(col),
            right,
        });
    }
    *input = checkpoint;

    // Regular comparison: ident op literal.
    let op = parse_op(input)?;
    let val = parse_literal(input)?;
    Ok(SelectCond::BinaryOp {
        op,
        left: SelectExpr::Column(col),
        right: SelectExpr::Literal(val),
    })
}

// ─── Comparison operator ──────────────────────────────────────────────────────

/// `op := != | >= | <= | = | > | <`
///
/// Multi-character operators are tried first to avoid partial matches.
fn parse_op<'s>(input: &mut &'s str) -> ModalResult<SelectOp> {
    let _ = multispace0.parse_next(input)?;
    if input.starts_with("!=") { *input = &input[2..]; return Ok(SelectOp::Ne); }
    if input.starts_with(">=") { *input = &input[2..]; return Ok(SelectOp::Ge); }
    if input.starts_with("<=") { *input = &input[2..]; return Ok(SelectOp::Le); }
    if input.starts_with('=')  { *input = &input[1..]; return Ok(SelectOp::Eq); }
    if input.starts_with('>')  { *input = &input[1..]; return Ok(SelectOp::Gt); }
    if input.starts_with('<')  { *input = &input[1..]; return Ok(SelectOp::Lt); }
    Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
}

// ─── Literal values ───────────────────────────────────────────────────────────

/// `literal := bool | null | float | int | string`
///
/// Float is tried before int because `3.14` starts with digits.
fn parse_literal<'s>(input: &mut &'s str) -> ModalResult<Value> {
    // Each branch checkpoints and restores on failure.
    let checkpoint = *input;

    if let Ok(v) = parse_bool(input) { return Ok(v); }
    *input = checkpoint;

    if let Ok(v) = parse_null(input) { return Ok(v); }
    *input = checkpoint;

    if let Ok(v) = parse_float(input) { return Ok(v); }
    *input = checkpoint;

    if let Ok(v) = parse_int(input) { return Ok(v); }
    *input = checkpoint;

    if let Ok(v) = parse_string_lit(input) { return Ok(v); }
    *input = checkpoint;

    Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
}

fn parse_bool<'s>(input: &mut &'s str) -> ModalResult<Value> {
    let checkpoint = *input;
    if kw("TRUE", input).is_ok()  { return Ok(Value::Bool(true));  }
    *input = checkpoint;
    if kw("FALSE", input).is_ok() { return Ok(Value::Bool(false)); }
    *input = checkpoint;
    Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
}

fn parse_null<'s>(input: &mut &'s str) -> ModalResult<Value> {
    let checkpoint = *input;
    if kw("NULL", input).is_ok() { return Ok(Value::Null); }
    *input = checkpoint;
    if kw("NONE", input).is_ok() { return Ok(Value::Null); }
    *input = checkpoint;
    Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
}

fn parse_int<'s>(input: &mut &'s str) -> ModalResult<Value> {
    let _ = multispace0.parse_next(input)?;
    let checkpoint = *input;
    let sign = if input.starts_with('-') { *input = &input[1..]; "-" } else { "" };
    let digits: &str = match digit1.parse_next(input) {
        Ok(d) => d,
        Err(e) => { *input = checkpoint; return Err(e); }
    };
    let n: i64 = format!("{sign}{digits}")
        .parse()
        .map_err(|_| { *input = checkpoint; winnow::error::ErrMode::Backtrack(ContextError::new()) })?;
    Ok(Value::Int(n))
}

fn parse_float<'s>(input: &mut &'s str) -> ModalResult<Value> {
    let _ = multispace0.parse_next(input)?;
    let checkpoint = *input;
    let sign = if input.starts_with('-') { *input = &input[1..]; "-" } else { "" };
    let int_part: &str = match digit1.parse_next(input) {
        Ok(d) => d,
        Err(e) => { *input = checkpoint; return Err(e); }
    };
    if !input.starts_with('.') { *input = checkpoint; return Err(winnow::error::ErrMode::Backtrack(ContextError::new())); }
    *input = &input[1..]; // consume '.'
    let frac_part: &str = match digit1.parse_next(input) {
        Ok(d) => d,
        Err(e) => { *input = checkpoint; return Err(e); }
    };
    let f: f64 = format!("{sign}{int_part}.{frac_part}")
        .parse()
        .map_err(|_| { *input = checkpoint; winnow::error::ErrMode::Backtrack(ContextError::new()) })?;
    Ok(Value::Float(OrderedFloat(f)))
}

fn parse_string_lit<'s>(input: &mut &'s str) -> ModalResult<Value> {
    let _ = multispace0.parse_next(input)?;
    let quote = if input.starts_with('"') {
        '"'
    } else if input.starts_with('\'') {
        '\''
    } else {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    };
    *input = &input[1..]; // consume opening quote
    let content: &str = take_while(0.., move |c: char| c != quote).parse_next(input)?;
    if input.starts_with(quote) {
        *input = &input[quote.len_utf8()..]; // consume closing quote
        Ok(Value::Str(content.to_string()))
    } else {
        Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
    }
}

// ─── Optional trailing clauses ────────────────────────────────────────────────
//
// These parse (and discard) ORDER BY, LIMIT, and START so the full grammar
// is accepted.  To wire them into the plan:
//   1. Add fields to `SelectStmt`
//   2. Change `()` return types to the appropriate output types
//   3. Store the return values in `parse_select_stmt`

/// `ORDER BY ident [ASC|DESC] (, ident [ASC|DESC])*`
fn parse_order_by_opt<'s>(input: &mut &'s str) -> ModalResult<()> {
    let checkpoint = *input;
    if kw("ORDER", input).is_err() { *input = checkpoint; return Ok(()); }
    if kw("BY", input).is_err()    { *input = checkpoint; return Ok(()); }
    // First sort column (required after ORDER BY).
    parse_ident(input)?;
    try_sort_dir(input)?;
    // Additional columns.
    loop {
        let cp = *input;
        let _ = multispace0.parse_next(input)?;
        if !input.starts_with(',') { *input = cp; break; }
        *input = &input[1..];
        match parse_ident(input) {
            Ok(_) => { try_sort_dir(input)?; }
            Err(_) => { *input = cp; break; }
        }
    }
    Ok(())
}

/// Optional `ASC` or `DESC` after an ORDER BY column — silently ignored.
fn try_sort_dir<'s>(input: &mut &'s str) -> ModalResult<()> {
    let checkpoint = *input;
    if kw("ASC", input).is_ok()  { return Ok(()); }
    *input = checkpoint;
    if kw("DESC", input).is_ok() { return Ok(()); }
    *input = checkpoint;
    Ok(())
}

/// `LIMIT integer`
fn parse_limit_opt<'s>(input: &mut &'s str) -> ModalResult<()> {
    let checkpoint = *input;
    if kw("LIMIT", input).is_err() { *input = checkpoint; return Ok(()); }
    parse_uint(input)?;
    Ok(())
}

/// `START [AT] integer`
fn parse_start_opt<'s>(input: &mut &'s str) -> ModalResult<()> {
    let checkpoint = *input;
    if kw("START", input).is_err() { *input = checkpoint; return Ok(()); }
    let cp = *input;
    if kw("AT", input).is_err() { *input = cp; } // AT is optional
    parse_uint(input)?;
    Ok(())
}

/// Parse an unsigned decimal integer and discard it.
fn parse_uint<'s>(input: &mut &'s str) -> ModalResult<u64> {
    let _ = multispace0.parse_next(input)?;
    let digits: &str = digit1.parse_next(input)?;
    digits.parse::<u64>().map_err(|_| winnow::error::ErrMode::Backtrack(ContextError::new()))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{LogicalPlan, Predicate, Value};

    fn ok(q: &str) -> LogicalPlan {
        parse_select(q).unwrap_or_else(|e| panic!("expected Ok for {q:?}, got {e}"))
    }

    fn err(q: &str) {
        assert!(parse_select(q).is_err(), "expected Err for {q:?}");
    }

    // ── SELECT fields / FROM ─────────────────────────────────────────────────

    #[test]
    fn select_star() {
        assert!(matches!(ok("SELECT * FROM users"), LogicalPlan::Scan { .. }));
    }

    #[test]
    fn select_single_field() {
        assert!(matches!(ok("SELECT name FROM users"), LogicalPlan::Project { .. }));
    }

    #[test]
    fn select_multiple_fields() {
        assert!(matches!(
            ok("SELECT name, age FROM users"),
            LogicalPlan::Project { ref columns, .. } if columns == &["name", "age"]
        ));
    }

    #[test]
    fn case_insensitive_keywords() {
        assert!(matches!(ok("select * from users"), LogicalPlan::Scan { .. }));
        assert!(matches!(ok("SELECT * FROM users"), LogicalPlan::Scan { .. }));
        assert!(matches!(ok("SeLeCt * FrOm users"), LogicalPlan::Scan { .. }));
    }

    #[test]
    fn table_name_case_preserved() {
        assert!(matches!(ok("SELECT * FROM MyTable"), LogicalPlan::Scan { ref table } if table == "MyTable"));
    }

    #[test]
    fn extra_whitespace() {
        assert!(matches!(ok("  SELECT   name ,  age   FROM   orders  "), LogicalPlan::Project { .. }));
    }

    #[test]
    fn backtick_column() {
        assert!(matches!(
            ok("SELECT `name` FROM users"),
            LogicalPlan::Project { ref columns, .. } if columns == &["name"]
        ));
    }

    // ── WHERE conditions ─────────────────────────────────────────────────────

    #[test]
    fn where_gt_int() {
        assert!(matches!(
            ok("SELECT * FROM users WHERE age > 18"),
            LogicalPlan::Filter { predicate: Predicate::Gt(ref col, Value::Int(18)), .. }
            if col == "age"
        ));
    }

    #[test]
    fn where_eq_bool() {
        assert!(matches!(
            ok("SELECT * FROM users WHERE active = true"),
            LogicalPlan::Filter { .. }
        ));
    }

    #[test]
    fn where_float() {
        assert!(matches!(ok("SELECT * FROM users WHERE score > 4.5"), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn where_string() {
        assert!(matches!(ok(r#"SELECT * FROM users WHERE name = "alice""#), LogicalPlan::Filter { .. }));
        assert!(matches!(ok("SELECT * FROM users WHERE name = 'alice'"), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn where_null() {
        assert!(matches!(ok("SELECT * FROM users WHERE deleted = null"), LogicalPlan::Filter { .. }));
        assert!(matches!(ok("SELECT * FROM users WHERE deleted = none"), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn where_all_ops() {
        ok("SELECT * FROM t WHERE a = 1");
        ok("SELECT * FROM t WHERE a != 1");
        ok("SELECT * FROM t WHERE a > 1");
        ok("SELECT * FROM t WHERE a >= 1");
        ok("SELECT * FROM t WHERE a < 1");
        ok("SELECT * FROM t WHERE a <= 1");
    }

    #[test]
    fn where_and() {
        // AND stacks two Filter nodes.
        assert!(matches!(
            ok("SELECT * FROM users WHERE age > 18 AND active = true"),
            LogicalPlan::Filter { input, .. } if matches!(*input, LogicalPlan::Filter { .. })
        ));
    }

    #[test]
    fn where_or() {
        assert!(matches!(
            ok("SELECT * FROM users WHERE age > 18 OR active = false"),
            LogicalPlan::Filter { predicate: Predicate::Or(..), .. }
        ));
    }

    #[test]
    fn where_not() {
        assert!(matches!(
            ok("SELECT * FROM users WHERE NOT active = true"),
            LogicalPlan::Filter { predicate: Predicate::Not(..), .. }
        ));
    }

    #[test]
    fn where_parenthesised() {
        ok("SELECT * FROM users WHERE (age > 18 AND active = true) OR admin = true");
    }

    #[test]
    fn where_lowercase() {
        ok("select * from users where age > 18");
    }

    // ── IN (subquery) ────────────────────────────────────────────────────────

    #[test]
    fn where_in_subquery() {
        assert!(matches!(
            ok("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)"),
            LogicalPlan::Join { .. }
        ));
    }

    #[test]
    fn where_in_scalar_is_lowering_error() {
        // Syntax parses fine; lowering rejects scalar IN argument.
        let e = parse_select("SELECT * FROM users WHERE id IN (42)");
        assert!(matches!(e, Err(ParseError::Lowering(..))));
    }

    // ── ORDER BY / LIMIT / START (accepted, not yet lowered) ─────────────────

    #[test]
    fn order_by_limit() {
        ok("SELECT name FROM users ORDER BY age DESC LIMIT 10");
    }

    #[test]
    fn limit_only() {
        ok("SELECT * FROM users LIMIT 5");
    }

    #[test]
    fn start_at() {
        ok("SELECT * FROM users START AT 20 LIMIT 10");
    }

    #[test]
    fn start_without_at() {
        ok("SELECT * FROM users START 20");
    }

    #[test]
    fn order_by_multi_col() {
        ok("SELECT * FROM users ORDER BY last_name ASC, age DESC");
    }

    // ── Error cases ──────────────────────────────────────────────────────────

    #[test]
    fn missing_select()  { err("FROM users"); }
    #[test]
    fn missing_from()    { err("SELECT * users"); }
    #[test]
    fn missing_where_rhs() { err("SELECT * FROM users WHERE"); }
    #[test]
    fn empty_input()     { err(""); }
    #[test]
    fn trailing_garbage(){ err("SELECT * FROM users GARBAGE"); }
}
