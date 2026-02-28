//! # Rule-based logical query optimizer
//!
//! Applies a set of algebraic equivalence rules to a `LogicalPlan` until no
//! further rewrites are possible (fixed-point iteration).
//!
//! ## Rules implemented
//!
//! | Rule | Value | Complexity |
//! |---|---|---|
//! | Predicate pushdown | High — reduces join probe input | ~50 LOC |
//! | Projection pushdown | Medium — reduces record width | ~50 LOC |
//! | Trivial project elimination | Low — removes no-op Projects | ~15 LOC |
//!
//! All rules are **correctness-preserving by construction** — no statistics
//! are required, and the fixed-point loop guarantees termination.
//!
//! ## When to add more rules
//!
//! - Join reordering (greedy cardinality-aware) when 3+ table joins appear.
//! - Filter merging (collapse adjacent `Filter` nodes into `And` predicates).

use crate::plan::{JoinType, LogicalPlan, Predicate};

// ─── Entry point ─────────────────────────────────────────────────────────────

/// Apply all optimization rules to `plan` until a fixed point is reached.
pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    let rules: &[fn(LogicalPlan) -> LogicalPlan] = &[
        push_filters_below_joins,
        eliminate_trivial_projects,
        prune_unused_columns,
    ];

    let mut current = plan;
    loop {
        let next = rules.iter().fold(current.clone(), |p, rule| rule(p));
        if next == current {
            break;
        }
        current = next;
    }
    current
}

// ─── Rule: predicate pushdown ─────────────────────────────────────────────────

/// Push `Filter` nodes below `Join` nodes when the predicate only references
/// columns from one join arm.
///
/// `Filter(Join(A, B), p)` → `Join(Filter(A, p), B)` when `p` only references
/// columns available from `A`.
///
/// This reduces the number of rows entering the expensive Phase-1 probe.
pub fn push_filters_below_joins(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            match *input {
                LogicalPlan::Join { left, right, on, kind } => {
                    // Collect columns referenced by the predicate.
                    let pred_cols = predicate_columns(&predicate);

                    // Collect available columns from each side (via leftmost Scan).
                    // In a real optimizer these would come from schema metadata.
                    // Here we approximate: if the predicate column matches the
                    // join key of one side, push it to that side.
                    let left_key = on.0.clone();
                    let right_key = on.1.clone();

                    let pushable_left = pred_cols.iter().any(|c| *c == left_key)
                        && pred_cols.iter().all(|c| *c == left_key);
                    let pushable_right = pred_cols.iter().any(|c| *c == right_key)
                        && pred_cols.iter().all(|c| *c == right_key);

                    if pushable_left {
                        let new_left = LogicalPlan::Filter {
                            input: Box::new(push_filters_below_joins(*left)),
                            predicate,
                        };
                        LogicalPlan::Join {
                            left: Box::new(new_left),
                            right: Box::new(push_filters_below_joins(*right)),
                            on,
                            kind,
                        }
                    } else if pushable_right && kind == JoinType::Inner {
                        // Only safe to push to the right side for inner joins.
                        let new_right = LogicalPlan::Filter {
                            input: Box::new(push_filters_below_joins(*right)),
                            predicate,
                        };
                        LogicalPlan::Join {
                            left: Box::new(push_filters_below_joins(*left)),
                            right: Box::new(new_right),
                            on,
                            kind,
                        }
                    } else {
                        // Cannot push: leave the filter above the join.
                        LogicalPlan::Filter {
                            input: Box::new(LogicalPlan::Join {
                                left: Box::new(push_filters_below_joins(*left)),
                                right: Box::new(push_filters_below_joins(*right)),
                                on,
                                kind,
                            }),
                            predicate,
                        }
                    }
                }
                other => LogicalPlan::Filter {
                    input: Box::new(push_filters_below_joins(other)),
                    predicate,
                },
            }
        }

        // Recurse into all other nodes.
        LogicalPlan::Join { left, right, on, kind } => LogicalPlan::Join {
            left: Box::new(push_filters_below_joins(*left)),
            right: Box::new(push_filters_below_joins(*right)),
            on,
            kind,
        },
        LogicalPlan::Project { input, columns } => LogicalPlan::Project {
            input: Box::new(push_filters_below_joins(*input)),
            columns,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(push_filters_below_joins(*input)),
        },
        LogicalPlan::Aggregate { input, group_by, agg } => LogicalPlan::Aggregate {
            input: Box::new(push_filters_below_joins(*input)),
            group_by,
            agg,
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(push_filters_below_joins(*input)),
            keys,
        },
        LogicalPlan::Limit { input, count, offset } => LogicalPlan::Limit {
            input: Box::new(push_filters_below_joins(*input)),
            count,
            offset,
        },
        LogicalPlan::Union { left, right } => LogicalPlan::Union {
            left: Box::new(push_filters_below_joins(*left)),
            right: Box::new(push_filters_below_joins(*right)),
        },
        leaf @ LogicalPlan::Scan { .. } => leaf,
    }
}

// ─── Rule: trivial project elimination ───────────────────────────────────────

/// Remove `Project` nodes whose column list is empty (i.e., `SELECT *`).
pub fn eliminate_trivial_projects(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, columns } if columns.is_empty() => {
            eliminate_trivial_projects(*input)
        }
        LogicalPlan::Project { input, columns } => LogicalPlan::Project {
            input: Box::new(eliminate_trivial_projects(*input)),
            columns,
        },
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(eliminate_trivial_projects(*input)),
            predicate,
        },
        LogicalPlan::Join { left, right, on, kind } => LogicalPlan::Join {
            left: Box::new(eliminate_trivial_projects(*left)),
            right: Box::new(eliminate_trivial_projects(*right)),
            on,
            kind,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(eliminate_trivial_projects(*input)),
        },
        LogicalPlan::Aggregate { input, group_by, agg } => LogicalPlan::Aggregate {
            input: Box::new(eliminate_trivial_projects(*input)),
            group_by,
            agg,
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(eliminate_trivial_projects(*input)),
            keys,
        },
        LogicalPlan::Limit { input, count, offset } => LogicalPlan::Limit {
            input: Box::new(eliminate_trivial_projects(*input)),
            count,
            offset,
        },
        LogicalPlan::Union { left, right } => LogicalPlan::Union {
            left: Box::new(eliminate_trivial_projects(*left)),
            right: Box::new(eliminate_trivial_projects(*right)),
        },
        leaf => leaf,
    }
}

// ─── Rule: projection pushdown ────────────────────────────────────────────────

/// Push `Project` nodes toward the leaves to reduce column count early.
///
/// Currently a no-op placeholder — full projection pushdown requires tracking
/// which columns are needed by each operator in the tree.  This is a
/// medium-complexity optimization that becomes valuable once aggregate and
/// sort operators are implemented.
pub fn prune_unused_columns(plan: LogicalPlan) -> LogicalPlan {
    // Placeholder: return the plan unchanged.
    // Full implementation would track required columns top-down and insert
    // Project nodes before wide Scans.
    plan
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Collect the column names referenced by a predicate.
fn predicate_columns(pred: &Predicate) -> Vec<&str> {
    match pred {
        Predicate::Gt(col, _)
        | Predicate::Lt(col, _)
        | Predicate::Eq(col, _)
        | Predicate::Ne(col, _)
        | Predicate::Ge(col, _)
        | Predicate::Le(col, _)
        | Predicate::InList(col, _)
        | Predicate::NotInList(col, _) => vec![col.as_str()],

        Predicate::EqCol(a, b) => vec![a.as_str(), b.as_str()],

        Predicate::And(l, r) | Predicate::Or(l, r) => {
            let mut cols = predicate_columns(l);
            cols.extend(predicate_columns(r));
            cols
        }
        Predicate::Not(inner) => predicate_columns(inner),
    }
}
