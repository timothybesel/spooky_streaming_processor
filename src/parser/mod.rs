pub mod types;
use types::*;
use winnow::{
    ModalResult, Parser,
    ascii::{digit1, multispace0},
    combinator::{alt, delimited, opt, preceded, separated},
    token::take_while,
};

// ─── Whitespace helper ──────────────────────────────────────────────────────

pub fn lex<'a, O, P>(parser: P) -> impl Parser<&'a str, O, winnow::error::ContextError>
where
    P: Parser<&'a str, O, winnow::error::ContextError>,
{
    delimited(multispace0, parser, multispace0)
}

// ─── Case-insensitive keyword ───────────────────────────────────────────────

/// Match a keyword case-insensitively. Ensures the keyword is not a prefix of
/// a longer identifier (e.g. "ORDER" must not match "ORDERS").
fn kw(expected: &'static str, input: &mut &str) -> ModalResult<()> {
    let _ = multispace0.parse_next(input)?;
    let upper: String = input
        .chars()
        .take(expected.len())
        .flat_map(char::to_uppercase)
        .collect();
    if upper != expected {
        return Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        ));
    }
    // Must not be followed by alphanumeric or underscore
    let next = input.chars().nth(expected.len());
    if next
        .map(|c| c.is_alphanumeric() || c == '_')
        .unwrap_or(false)
    {
        return Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        ));
    }
    *input = &input[expected.len()..];
    Ok(())
}

// ─── Identifiers ────────────────────────────────────────────────────────────

fn parse_ident(input: &mut &str) -> ModalResult<String> {
    let _ = multispace0.parse_next(input)?;
    // Backtick-quoted: `column name`
    if input.starts_with('`') {
        let inner: &str =
            delimited('`', take_while(1.., |c: char| c != '`'), '`').parse_next(input)?;
        return Ok(inner.to_string());
    }
    // Normal: starts with letter or underscore, then alphanumeric or underscore
    let first: &str =
        take_while(1.., |c: char| c.is_ascii_alphabetic() || c == '_').parse_next(input)?;
    let rest: &str =
        take_while(0.., |c: char| c.is_ascii_alphanumeric() || c == '_').parse_next(input)?;
    Ok(format!("{first}{rest}"))
}

// ─── Literals ───────────────────────────────────────────────────────────────

fn parse_literal(input: &mut &str) -> ModalResult<WhereOperand> {
    let _ = multispace0.parse_next(input)?;
    alt((
        parse_literal_bool,
        parse_literal_null,
        parse_literal_string,
        parse_literal_number,
    ))
    .parse_next(input)
}

fn parse_literal_bool(input: &mut &str) -> ModalResult<WhereOperand> {
    alt((
        |i: &mut &str| {
            kw("TRUE", i)?;
            Ok(WhereOperand::LiteralBool(true))
        },
        |i: &mut &str| {
            kw("FALSE", i)?;
            Ok(WhereOperand::LiteralBool(false))
        },
    ))
    .parse_next(input)
}

fn parse_literal_null(input: &mut &str) -> ModalResult<WhereOperand> {
    alt((
        |i: &mut &str| {
            kw("NULL", i)?;
            Ok(WhereOperand::LiteralNull)
        },
        |i: &mut &str| {
            kw("NONE", i)?;
            Ok(WhereOperand::LiteralNull)
        },
    ))
    .parse_next(input)
}

fn parse_literal_string(input: &mut &str) -> ModalResult<WhereOperand> {
    let _ = multispace0.parse_next(input)?;
    alt((
        delimited('"', take_while(0.., |c: char| c != '"'), '"')
            .map(|s: &str| WhereOperand::LiteralStr(s.to_string())),
        delimited('\'', take_while(0.., |c: char| c != '\''), '\'')
            .map(|s: &str| WhereOperand::LiteralStr(s.to_string())),
    ))
    .parse_next(input)
}

/// Parse a number — tries float (has `.`) then falls back to int.
fn parse_literal_number(input: &mut &str) -> ModalResult<WhereOperand> {
    let _ = multispace0.parse_next(input)?;
    let start = *input;

    // Optional sign
    let sign = if input.starts_with('-') {
        *input = &input[1..];
        "-"
    } else {
        ""
    };

    let int_part: &str = digit1.parse_next(input)?;

    // Check for decimal point → float (but NOT `..` which is a range operator)
    if input.starts_with('.') && !input.starts_with("..") {
        *input = &input[1..];
        let frac_part: &str = digit1.parse_next(input)?;
        let f: f64 = format!("{sign}{int_part}.{frac_part}")
            .parse()
            .map_err(|_| {
                *input = start;
                winnow::error::ErrMode::Backtrack(winnow::error::ContextError::new())
            })?;
        Ok(WhereOperand::LiteralFloat(f))
    } else {
        let n: i64 = format!("{sign}{int_part}").parse().map_err(|_| {
            *input = start;
            winnow::error::ErrMode::Backtrack(winnow::error::ContextError::new())
        })?;
        Ok(WhereOperand::LiteralInt(n))
    }
}

// ─── Parameters ─────────────────────────────────────────────────────────────

/// Parse `$name` or `$parent.field.path` — a parameter reference.
fn parse_param(input: &mut &str) -> ModalResult<WhereOperand> {
    let _ = multispace0.parse_next(input)?;
    if !input.starts_with('$') {
        return Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        ));
    }
    *input = &input[1..]; // consume '$'
    // First segment: alphanumeric or underscore
    let first: &str =
        take_while(1.., |c: char| c.is_ascii_alphanumeric() || c == '_').parse_next(input)?;
    let mut segments = vec![first.to_string()];
    // Dotted path segments: .field.subfield
    while input.starts_with('.') && !input.starts_with("..") {
        *input = &input[1..]; // consume '.'
        let seg: &str =
            take_while(1.., |c: char| c.is_ascii_alphanumeric() || c == '_').parse_next(input)?;
        segments.push(seg.to_string());
    }
    Ok(WhereOperand::Param(segments))
}

// ─── Ranges ─────────────────────────────────────────────────────────────────

/// Parse a range like `18..65`, `18..=65`, `18>..65`, `18>..=65`, `..100`, `18..`, `..=100`.
/// The lower bound (if present) must already be parsed — this function is called
/// after we see `..` following a number.
///
/// Called from `parse_range_or_number` when `..` is detected after a number,
/// or from `parse_operand` when input starts with `..`.
fn parse_range_after_lo(
    lo: Option<WhereOperand>,
    lo_exclusive: bool,
    input: &mut &str,
) -> ModalResult<WhereOperand> {
    // We expect `..` has NOT been consumed yet
    if !input.starts_with("..") {
        return Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        ));
    }
    *input = &input[2..]; // consume '..'

    // Check for `=` → upper inclusive
    let hi_inclusive = if input.starts_with('=') {
        *input = &input[1..];
        true
    } else {
        false
    };

    // Try to parse upper bound (optional — `18..` is valid)
    let checkpoint = *input;
    let hi = if let Ok(val) = parse_simple_value(input) {
        Some(val)
    } else {
        *input = checkpoint;
        None
    };

    Ok(WhereOperand::Range(Box::new(RangeBound {
        lo,
        lo_exclusive,
        hi,
        hi_inclusive,
    })))
}

/// Parse a simple value (number, string, bool, null, param) — used as range bounds.
/// Does NOT parse ranges recursively or columns (to avoid ambiguity).
fn parse_simple_value(input: &mut &str) -> ModalResult<WhereOperand> {
    let _ = multispace0.parse_next(input)?;
    alt((
        parse_param,
        parse_literal_bool,
        parse_literal_null,
        parse_literal_string,
        parse_literal_number,
    ))
    .parse_next(input)
}

/// Parse a number that might be followed by `>..` or `..` (range).
/// Returns either a plain number or a Range operand.
fn parse_number_or_range(input: &mut &str) -> ModalResult<WhereOperand> {
    let num = parse_literal_number(input)?;

    // Check for `>..` (exclusive lower bound)
    if input.starts_with(">..") {
        *input = &input[1..]; // consume '>' only, parse_range_after_lo consumes '..'
        return parse_range_after_lo(Some(num), true, input);
    }

    // Check for `..` (inclusive lower bound, range continues)
    if input.starts_with("..") {
        return parse_range_after_lo(Some(num), false, input);
    }

    Ok(num)
}

/// Try parsing a range that starts with `..` (no lower bound), e.g. `..100`, `..=65`.
fn parse_open_range(input: &mut &str) -> ModalResult<WhereOperand> {
    let _ = multispace0.parse_next(input)?;
    if !input.starts_with("..") {
        return Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        ));
    }
    parse_range_after_lo(None, false, input)
}

// ─── Comparison operators ───────────────────────────────────────────────────

/// Parse: `>=  <=  !=  ==  =  >  <`
fn parse_op(input: &mut &str) -> ModalResult<WhereOp> {
    let _ = multispace0.parse_next(input)?;
    // Multi-char operators first
    alt((
        ">=".map(|_| WhereOp::Ge),
        "<=".map(|_| WhereOp::Le),
        "!=".map(|_| WhereOp::Ne),
        "==".map(|_| WhereOp::Eq),
        "=".map(|_| WhereOp::Eq),
        ">".map(|_| WhereOp::Gt),
        "<".map(|_| WhereOp::Lt),
    ))
    .parse_next(input)
}

// ─── WHERE condition parser (recursive descent) ─────────────────────────────
//
// SurrealQL operator reference:
//   Equality:    =  ==  IS  !=  IS NOT
//   Comparison:  >  >=  <  <=
//   Logical:     AND (&&)   OR (||)   NOT (!)
//   Membership:  IN  INSIDE  NOT IN  CONTAINS
//
// Precedence (low → high):
//   or_expr   := and_expr (OR and_expr)*
//   and_expr  := not_expr (AND not_expr)*
//   not_expr  := NOT not_expr | atom_cond
//   atom_cond := ident op operand
//              | ident IN ( select_stmt )
//              | ident CONTAINS literal
//              | ident IS [NOT] literal
//              | ( or_expr )

/// Entry point: parse a full WHERE condition.
pub fn parse_where_expr(input: &mut &str) -> ModalResult<WhereExpr> {
    parse_or_expr(input)
}

/// `or_expr := and_expr ( (OR | ||) and_expr )*`
fn parse_or_expr(input: &mut &str) -> ModalResult<WhereExpr> {
    let first = parse_and_expr(input)?;
    let mut result = first;
    loop {
        let checkpoint = *input;
        // Try OR keyword
        let found_or = kw("OR", input).is_ok() || {
            *input = checkpoint;
            let _ = multispace0.parse_next(input)?;
            if input.starts_with("||") {
                *input = &input[2..];
                true
            } else {
                *input = checkpoint;
                false
            }
        };
        if !found_or {
            break;
        }
        let right = parse_and_expr(input)?;
        result = WhereExpr::Or(Box::new(result), Box::new(right));
    }
    Ok(result)
}

/// `and_expr := not_expr ( (AND | &&) not_expr )*`
fn parse_and_expr(input: &mut &str) -> ModalResult<WhereExpr> {
    let first = parse_not_expr(input)?;
    let mut result = first;
    loop {
        let checkpoint = *input;
        // Try AND keyword
        let found_and = kw("AND", input).is_ok() || {
            *input = checkpoint;
            let _ = multispace0.parse_next(input)?;
            if input.starts_with("&&") {
                *input = &input[2..];
                true
            } else {
                *input = checkpoint;
                false
            }
        };
        if !found_and {
            break;
        }
        let right = parse_not_expr(input)?;
        result = WhereExpr::And(Box::new(result), Box::new(right));
    }
    Ok(result)
}

/// `not_expr := NOT not_expr | !not_expr | atom_cond`
fn parse_not_expr(input: &mut &str) -> ModalResult<WhereExpr> {
    let checkpoint = *input;

    // Try keyword NOT
    if kw("NOT", input).is_ok() {
        let inner = parse_not_expr(input)?;
        return Ok(WhereExpr::Not(Box::new(inner)));
    }
    *input = checkpoint;

    // Try prefix !  (but not != or !!)
    let _ = multispace0.parse_next(input)?;
    if input.starts_with('!') && !input.starts_with("!=") && !input.starts_with("!!") {
        *input = &input[1..];
        let inner = parse_not_expr(input)?;
        return Ok(WhereExpr::Not(Box::new(inner)));
    }
    *input = checkpoint;

    parse_atom_cond(input)
}

/// `atom_cond := ident op operand | ident IN (...) | ident CONTAINS lit | ( or_expr )`
fn parse_atom_cond(input: &mut &str) -> ModalResult<WhereExpr> {
    let _ = multispace0.parse_next(input)?;

    // Parenthesized sub-expression: ( or_expr )
    // We need to distinguish ( condition ) from ( SELECT ... ) used in IN subqueries.
    if input.starts_with('(') {
        let after_paren = input[1..].trim_start();
        if !after_paren.to_uppercase().starts_with("SELECT") {
            *input = &input[1..]; // consume '('
            let expr = parse_or_expr(input)?;
            let _ = multispace0.parse_next(input)?;
            if input.starts_with(')') {
                *input = &input[1..];
                return Ok(expr);
            }
            return Err(winnow::error::ErrMode::Backtrack(
                winnow::error::ContextError::new(),
            ));
        }
    }

    // Column name on the left
    let col = parse_ident(input)?;

    // Save position after parsing col — all keyword-operator attempts reset here.
    let after_col = *input;

    // ── IS NOT / IS ──────────────────────────────────────────────────────
    if kw("IS", input).is_ok() {
        let after_is = *input;
        if kw("NOT", input).is_ok() {
            let val = parse_operand(input)?;
            return Ok(WhereExpr::BinaryOp {
                op: WhereOp::Ne,
                left: WhereOperand::Column(col),
                right: val,
            });
        }
        *input = after_is;
        let val = parse_operand(input)?;
        return Ok(WhereExpr::BinaryOp {
            op: WhereOp::Eq,
            left: WhereOperand::Column(col),
            right: val,
        });
    }
    *input = after_col;

    // ── NOT IN / NOT INSIDE ─────────────────────────────────────────────
    if kw("NOT", input).is_ok() {
        let after_not = *input;
        if kw("IN", input).is_ok() {
            let rhs = parse_in_rhs(input)?;
            return Ok(WhereExpr::Not(Box::new(WhereExpr::BinaryOp {
                op: WhereOp::In,
                left: WhereOperand::Column(col),
                right: rhs,
            })));
        }
        *input = after_not;
        if kw("INSIDE", input).is_ok() {
            let rhs = parse_in_rhs(input)?;
            return Ok(WhereExpr::Not(Box::new(WhereExpr::BinaryOp {
                op: WhereOp::In,
                left: WhereOperand::Column(col),
                right: rhs,
            })));
        }
    }
    *input = after_col;

    // ── IN / INSIDE ──────────────────────────────────────────────────────
    if kw("IN", input).is_ok() {
        let rhs = parse_in_rhs(input)?;
        return Ok(WhereExpr::BinaryOp {
            op: WhereOp::In,
            left: WhereOperand::Column(col),
            right: rhs,
        });
    }
    *input = after_col;
    if kw("INSIDE", input).is_ok() {
        let rhs = parse_in_rhs(input)?;
        return Ok(WhereExpr::BinaryOp {
            op: WhereOp::In,
            left: WhereOperand::Column(col),
            right: rhs,
        });
    }
    *input = after_col;

    // ── CONTAINS ─────────────────────────────────────────────────────────
    if kw("CONTAINS", input).is_ok() {
        let val = parse_operand(input)?;
        return Ok(WhereExpr::BinaryOp {
            op: WhereOp::Contains,
            left: WhereOperand::Column(col),
            right: val,
        });
    }
    *input = after_col;

    // ── Standard comparison: col op operand ──────────────────────────────
    let op = parse_op(input)?;
    let right = parse_operand(input)?;

    Ok(WhereExpr::BinaryOp {
        op,
        left: WhereOperand::Column(col),
        right,
    })
}

/// Parse the right side of IN: `( SELECT ... )` subquery, or a range like `18..=65`.
fn parse_in_rhs(input: &mut &str) -> ModalResult<WhereOperand> {
    let _ = multispace0.parse_next(input)?;

    // Try range first: `age IN 18..=65` (no parens)
    let checkpoint = *input;
    if let Ok(range) = parse_open_range(input) {
        return Ok(range);
    }
    *input = checkpoint;
    if let Ok(range) = parse_number_or_range(input)
        && matches!(range, WhereOperand::Range(_))
    {
        return Ok(range);
    }
    *input = checkpoint;

    // Otherwise: ( SELECT ... ) subquery
    if !input.starts_with('(') {
        return Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        ));
    }
    *input = &input[1..]; // consume '('
    let sub = parse_select_statement(input)?;
    let _ = multispace0.parse_next(input)?;
    if input.starts_with(')') {
        *input = &input[1..];
        Ok(WhereOperand::Subquery(Box::new(sub)))
    } else {
        Err(winnow::error::ErrMode::Backtrack(
            winnow::error::ContextError::new(),
        ))
    }
}

/// Parse a right-hand operand: param, literal, or column reference.
fn parse_operand(input: &mut &str) -> ModalResult<WhereOperand> {
    let _ = multispace0.parse_next(input)?;
    // Try $param first
    let checkpoint = *input;
    if let Ok(param) = parse_param(input) {
        return Ok(param);
    }
    *input = checkpoint;
    // Try literal (bool/null/string/number)
    if let Ok(lit) = parse_literal(input) {
        return Ok(lit);
    }
    *input = checkpoint;
    // Column reference (e.g. `start_date > end_date`)
    let col = parse_ident(input)?;
    Ok(WhereOperand::Column(col))
}

// ─── Field list ─────────────────────────────────────────────────────────────

pub fn parse_field(input: &mut &str) -> ModalResult<Field> {
    alt((
        lex("*").map(|_| Field::Wildcard),
        (
            delimited(lex("("), parse_select_statement, lex(")")),
            opt(preceded(lex("AS"), parse_ident)),
        )
            .map(|(subquery, alias)| {
                Field::Subquery(Box::new(subquery), alias.unwrap_or_default())
            }),
        parse_ident.map(Field::Ident),
    ))
    .parse_next(input)
}

// ─── SELECT statement ───────────────────────────────────────────────────────

pub fn parse_select_statement(input: &mut &str) -> ModalResult<SelectStatement> {
    kw("SELECT", input)?;

    let fields: Vec<Field> = separated(1.., parse_field, lex(",")).parse_next(input)?;

    kw("FROM", input)?;

    let table = parse_ident.parse_next(input)?;

    // Optional WHERE clause
    let where_ = {
        let checkpoint = *input;
        if kw("WHERE", input).is_ok() {
            Some(parse_where_expr(input)?)
        } else {
            *input = checkpoint;
            None
        }
    };

    Ok(SelectStatement {
        fields,
        table,
        where_,
    })
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(q: &str) -> SelectStatement {
        parse_select_statement(&mut q.trim())
            .unwrap_or_else(|e| panic!("failed to parse: {q:?}\nerror: {e:?}"))
    }

    // ── Basic SELECT (no WHERE) ─────────────────────────────────────────

    #[test]
    fn select_star() {
        let stmt = parse_ok("SELECT * FROM user");
        assert_eq!(stmt.table, "user");
        assert!(stmt.where_.is_none());
    }

    #[test]
    fn select_fields() {
        let stmt = parse_ok("SELECT name, age FROM user");
        assert_eq!(stmt.fields.len(), 2);
        assert!(stmt.where_.is_none());
    }

    // ── Simple WHERE conditions ─────────────────────────────────────────

    #[test]
    fn where_gt_int() {
        let stmt = parse_ok("SELECT * FROM user WHERE age > 18");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { op, left, right } => {
                assert_eq!(op, WhereOp::Gt);
                assert!(matches!(left, WhereOperand::Column(c) if c == "age"));
                assert!(matches!(right, WhereOperand::LiteralInt(18)));
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn where_eq_string_double() {
        let stmt = parse_ok(r#"SELECT * FROM user WHERE name = "alice""#);
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { op, right, .. } => {
                assert_eq!(op, WhereOp::Eq);
                assert!(matches!(right, WhereOperand::LiteralStr(s) if s == "alice"));
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn where_eq_string_single() {
        let stmt = parse_ok("SELECT * FROM user WHERE name = 'bob'");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { right, .. } => {
                assert!(matches!(right, WhereOperand::LiteralStr(s) if s == "bob"));
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn where_float() {
        let stmt = parse_ok("SELECT * FROM product WHERE price > 9.99");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { right, .. } => {
                assert!(matches!(right, WhereOperand::LiteralFloat(f) if (f - 9.99).abs() < 0.001));
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn where_bool_true() {
        let stmt = parse_ok("SELECT * FROM user WHERE active = true");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { right, .. } => {
                assert!(matches!(right, WhereOperand::LiteralBool(true)));
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn where_null() {
        let stmt = parse_ok("SELECT * FROM user WHERE deleted = null");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { right, .. } => {
                assert!(matches!(right, WhereOperand::LiteralNull));
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn where_ne() {
        let stmt = parse_ok("SELECT * FROM user WHERE status != 'deleted'");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { op, .. } => assert_eq!(op, WhereOp::Ne),
            other => panic!("expected Ne, got {other:?}"),
        }
    }

    #[test]
    fn where_ge_le() {
        let stmt = parse_ok("SELECT * FROM user WHERE age >= 18 AND age <= 65");
        match stmt.where_.unwrap() {
            WhereExpr::And(left, right) => {
                assert!(matches!(
                    *left,
                    WhereExpr::BinaryOp {
                        op: WhereOp::Ge,
                        ..
                    }
                ));
                assert!(matches!(
                    *right,
                    WhereExpr::BinaryOp {
                        op: WhereOp::Le,
                        ..
                    }
                ));
            }
            other => panic!("expected And(Ge, Le), got {other:?}"),
        }
    }

    // ── Logical operators ───────────────────────────────────────────────

    #[test]
    fn where_and() {
        let stmt = parse_ok("SELECT * FROM user WHERE age > 18 AND active = true");
        assert!(matches!(stmt.where_.unwrap(), WhereExpr::And(_, _)));
    }

    #[test]
    fn where_or() {
        let stmt = parse_ok("SELECT * FROM user WHERE age < 18 OR age > 65");
        assert!(matches!(stmt.where_.unwrap(), WhereExpr::Or(_, _)));
    }

    #[test]
    fn where_and_or_precedence() {
        // AND binds tighter: `a OR b AND c` → `Or(a, And(b, c))`
        let stmt = parse_ok("SELECT * FROM user WHERE x = 1 OR y = 2 AND z = 3");
        match stmt.where_.unwrap() {
            WhereExpr::Or(left, right) => {
                assert!(matches!(*left, WhereExpr::BinaryOp { .. }));
                assert!(matches!(*right, WhereExpr::And(_, _)));
            }
            other => panic!("expected Or(_, And(_, _)), got {other:?}"),
        }
    }

    #[test]
    fn where_not() {
        let stmt = parse_ok("SELECT * FROM user WHERE NOT active = true");
        assert!(matches!(stmt.where_.unwrap(), WhereExpr::Not(_)));
    }

    #[test]
    fn where_bang_not() {
        let stmt = parse_ok("SELECT * FROM user WHERE !active = true");
        assert!(matches!(stmt.where_.unwrap(), WhereExpr::Not(_)));
    }

    #[test]
    fn where_parenthesized() {
        let stmt = parse_ok("SELECT * FROM user WHERE (age > 18 OR age < 5) AND active = true");
        match stmt.where_.unwrap() {
            WhereExpr::And(left, _) => {
                assert!(matches!(*left, WhereExpr::Or(_, _)));
            }
            other => panic!("expected And(Or(..), ..), got {other:?}"),
        }
    }

    #[test]
    fn where_double_amp_and_pipe() {
        // && and || are the symbol alternatives for AND / OR
        let stmt = parse_ok("SELECT * FROM user WHERE age > 18 && active = true || name = 'admin'");
        // (age > 18 AND active = true) OR name = 'admin'
        assert!(matches!(stmt.where_.unwrap(), WhereExpr::Or(_, _)));
    }

    // ── Column vs column ────────────────────────────────────────────────

    #[test]
    fn where_column_vs_column() {
        let stmt = parse_ok("SELECT * FROM user WHERE start_date > end_date");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { right, .. } => {
                assert!(
                    matches!(&right, WhereOperand::Column(c) if c == "end_date"),
                    "expected Column(end_date), got {right:?}"
                );
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    // ── IN subquery ─────────────────────────────────────────────────────

    #[test]
    fn where_in_subquery() {
        let stmt = parse_ok(
            "SELECT * FROM product WHERE id IN (SELECT product_id FROM order WHERE total > 100)",
        );
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp {
                op: WhereOp::In,
                right: WhereOperand::Subquery(sub),
                ..
            } => {
                assert_eq!(sub.table, "order");
                assert!(sub.where_.is_some());
            }
            other => panic!("expected In subquery, got {other:?}"),
        }
    }

    // ── Case insensitivity ──────────────────────────────────────────────

    #[test]
    fn where_case_insensitive() {
        let stmt = parse_ok("select * from users where age > 18 and active = true");
        assert!(matches!(stmt.where_.unwrap(), WhereExpr::And(_, _)));
    }

    // ── Subquery in SELECT field with WHERE ─────────────────────────────

    #[test]
    fn subquery_field_with_where() {
        let stmt = parse_ok(
            "SELECT name, (SELECT product FROM order WHERE price > 10) AS orders FROM user",
        );
        assert_eq!(stmt.fields.len(), 2);
        match &stmt.fields[1] {
            Field::Subquery(sub, alias) => {
                assert_eq!(alias, "orders");
                assert!(sub.where_.is_some());
            }
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    // ── The main.rs example query ───────────────────────────────────────

    #[test]
    fn main_example_query() {
        let stmt = parse_ok(
            "SELECT name, (SELECT product, address FROM order WHERE price > 10.00) AS order FROM user",
        );
        assert_eq!(stmt.table, "user");
        assert_eq!(stmt.fields.len(), 2);
        match &stmt.fields[1] {
            Field::Subquery(sub, alias) => {
                assert_eq!(alias, "order");
                assert_eq!(sub.table, "order");
                assert!(sub.where_.is_some());
            }
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    // ── Parameters ($name, $parent.field) ───────────────────────────────

    #[test]
    fn where_param_simple() {
        let stmt = parse_ok("SELECT * FROM user WHERE id = $user_id");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { right, .. } => {
                assert!(
                    matches!(&right, WhereOperand::Param(p) if p == &["user_id"]),
                    "expected Param([user_id]), got {right:?}"
                );
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn where_param_dotted_path() {
        let stmt = parse_ok("SELECT * FROM user WHERE id = $auth.id");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { right, .. } => {
                assert!(
                    matches!(&right, WhereOperand::Param(p) if p == &["auth", "id"]),
                    "expected Param([auth, id]), got {right:?}"
                );
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn where_param_parent_in_subquery() {
        let stmt = parse_ok(
            "SELECT *, (SELECT * FROM events WHERE host == $parent.id) AS hosted FROM user",
        );
        match &stmt.fields[1] {
            Field::Subquery(sub, alias) => {
                assert_eq!(alias, "hosted");
                match sub.where_.as_ref().unwrap() {
                    WhereExpr::BinaryOp { right, .. } => {
                        assert!(
                            matches!(right, WhereOperand::Param(p) if p == &["parent", "id"]),
                            "expected Param([parent, id]), got {right:?}"
                        );
                    }
                    other => panic!("expected BinaryOp, got {other:?}"),
                }
            }
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    // ── Ranges (IN 18..=65) ─────────────────────────────────────────────

    #[test]
    fn where_in_range_inclusive() {
        // age IN 18..=65  →  both inclusive
        let stmt = parse_ok("SELECT * FROM person WHERE age IN 18..=65");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp {
                op: WhereOp::In,
                right: WhereOperand::Range(r),
                ..
            } => {
                assert!(matches!(r.lo, Some(WhereOperand::LiteralInt(18))));
                assert!(!r.lo_exclusive);
                assert!(matches!(r.hi, Some(WhereOperand::LiteralInt(65))));
                assert!(r.hi_inclusive);
            }
            other => panic!("expected In Range, got {other:?}"),
        }
    }

    #[test]
    fn where_in_range_half_open() {
        // age IN 18..65  →  lower inclusive, upper exclusive
        let stmt = parse_ok("SELECT * FROM person WHERE age IN 18..65");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp {
                op: WhereOp::In,
                right: WhereOperand::Range(r),
                ..
            } => {
                assert!(matches!(r.lo, Some(WhereOperand::LiteralInt(18))));
                assert!(!r.lo_exclusive);
                assert!(matches!(r.hi, Some(WhereOperand::LiteralInt(65))));
                assert!(!r.hi_inclusive);
            }
            other => panic!("expected In Range, got {other:?}"),
        }
    }

    #[test]
    fn where_in_range_exclusive_lower() {
        // age IN 18>..=65  →  lower exclusive, upper inclusive
        let stmt = parse_ok("SELECT * FROM person WHERE age IN 18>..=65");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp {
                op: WhereOp::In,
                right: WhereOperand::Range(r),
                ..
            } => {
                assert!(matches!(r.lo, Some(WhereOperand::LiteralInt(18))));
                assert!(r.lo_exclusive);
                assert!(matches!(r.hi, Some(WhereOperand::LiteralInt(65))));
                assert!(r.hi_inclusive);
            }
            other => panic!("expected In Range, got {other:?}"),
        }
    }

    #[test]
    fn where_in_range_open_ended() {
        // age IN 18..  →  from 18 onwards
        let stmt = parse_ok("SELECT * FROM person WHERE age IN 18..");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp {
                op: WhereOp::In,
                right: WhereOperand::Range(r),
                ..
            } => {
                assert!(matches!(r.lo, Some(WhereOperand::LiteralInt(18))));
                assert!(r.hi.is_none());
            }
            other => panic!("expected In Range, got {other:?}"),
        }
    }

    #[test]
    fn where_in_range_upper_only() {
        // age IN ..=100  →  up to 100 inclusive
        let stmt = parse_ok("SELECT * FROM person WHERE age IN ..=100");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp {
                op: WhereOp::In,
                right: WhereOperand::Range(r),
                ..
            } => {
                assert!(r.lo.is_none());
                assert!(matches!(r.hi, Some(WhereOperand::LiteralInt(100))));
                assert!(r.hi_inclusive);
            }
            other => panic!("expected In Range, got {other:?}"),
        }
    }

    // ── Float in range doesn't break ────────────────────────────────────

    #[test]
    fn where_float_still_works() {
        // Ensure 9.99 is still parsed as float, not as 9 followed by range ..99
        let stmt = parse_ok("SELECT * FROM product WHERE price > 9.99");
        match stmt.where_.unwrap() {
            WhereExpr::BinaryOp { right, .. } => {
                assert!(matches!(right, WhereOperand::LiteralFloat(f) if (f - 9.99).abs() < 0.001));
            }
            other => panic!("expected BinaryOp with float, got {other:?}"),
        }
    }
}
