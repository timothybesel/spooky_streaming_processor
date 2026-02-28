use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use crate::plan::LogicalPlan;

use super::{DispatcherOp, Operator};
use super::filter::FilterOp;
use super::join::{JoinCore, JoinSide, JoinSideOp};
use super::project::ProjectOp;

// ─── Build errors ────────────────────────────────────────────────────────────

/// Errors produced by `compile_plan`.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("the {side} subtree of a Join contains no Scan node; found: {plan_debug}")]
    NoScanInSubtree { side: &'static str, plan_debug: String },

    #[error("duplicate table route: '{table}' already registered in this dispatcher")]
    DuplicateTableRoute { table: String },

    #[error("plan variant '{variant}' is not yet supported by the physical engine")]
    UnsupportedPlanNode { variant: &'static str },
}

// ─── compile_plan ─────────────────────────────────────────────────────────────

/// Recursively compile a `PhysicalPlan` into a push-based physical DAG.
///
/// ### How it works (bottom-up construction)
///
/// The `next` parameter is the **downstream** operator that will receive
/// this sub-plan's output.  We build the graph by wrapping operators from
/// the top of the tree down to the leaves:
///
/// ```text
/// PhysicalPlan::Filter { input: Source("users"), predicate: age > 18 }
///
///   compile_plan(Filter{..}, SinkOp)
///     → FilterOp(age>18, next=SinkOp)           ← wrap first
///     → compile_plan(Source("users"), FilterOp)  ← recurse into input
///     → returns FilterOp (Source is a no-op leaf)
/// ```
///
/// ### Joins
///
/// A `PhysicalPlan::SymmetricHashJoin` has **two** data sources, so
/// `compile_plan` returns a `DispatcherOp` keyed by source-table name.
/// Callers push events into it and it automatically routes each event
/// to the correct pipeline arm.
pub fn compile_plan(
    plan: PhysicalPlan,
    next: Box<dyn Operator>,
) -> Result<Box<dyn Operator>, BuildError> {
    match plan {
        // ── Source ────────────────────────────────────────────────────────
        // Source is the data origin; `next` is what processes records from
        // this table.  Return it unchanged — the caller pushes events.
        PhysicalPlan::Source { .. } => Ok(next),

        // ── Filter ────────────────────────────────────────────────────────
        PhysicalPlan::Filter { input, predicate } => {
            let filter_op = Box::new(FilterOp::new(predicate, next));
            compile_plan(*input, filter_op)
        }

        // ── Project ───────────────────────────────────────────────────────
        PhysicalPlan::Project { input, columns } => {
            let project_op = Box::new(ProjectOp::new(columns, next));
            compile_plan(*input, project_op)
        }

        // ── SymmetricHashJoin ─────────────────────────────────────────────
        PhysicalPlan::SymmetricHashJoin {
            left,
            right,
            left_key,
            right_key,
            kind,
        } => {
            // Walk the physical plan to find the base table for each side
            // so we can key the dispatcher correctly.
            let left_table = find_source_table(&left).ok_or_else(|| BuildError::NoScanInSubtree {
                side: "left",
                plan_debug: format!("{left:?}"),
            })?;
            let right_table =
                find_source_table(&right).ok_or_else(|| BuildError::NoScanInSubtree {
                    side: "right",
                    plan_debug: format!("{right:?}"),
                })?;

            // Shared state: both arms probe/write the same hash tables and
            // funnel their output to the same downstream `next` operator.
            let core = Rc::new(RefCell::new(JoinCore::new(
                left_key, right_key, kind, next,
            )));

            let left_join_op = Box::new(JoinSideOp::new(JoinSide::Left, Rc::clone(&core)));
            let right_join_op = Box::new(JoinSideOp::new(JoinSide::Right, Rc::clone(&core)));

            let left_pipeline = compile_plan(*left, left_join_op)?;
            let right_pipeline = compile_plan(*right, right_join_op)?;

            // Build the dispatch table.
            let mut routes: HashMap<Arc<str>, Box<dyn Operator>> =
                HashMap::with_capacity(2);

            if routes.contains_key(left_table.as_str()) {
                return Err(BuildError::DuplicateTableRoute { table: left_table });
            }
            routes.insert(Arc::from(left_table.as_str()), left_pipeline);

            if routes.contains_key(right_table.as_str()) {
                return Err(BuildError::DuplicateTableRoute { table: right_table });
            }
            routes.insert(Arc::from(right_table.as_str()), right_pipeline);

            Ok(Box::new(DispatcherOp::new(routes)))
        }

        // ── Distinct / HashAggregate / Sort — not yet physical ───────────
        PhysicalPlan::Distinct { .. } => {
            Err(BuildError::UnsupportedPlanNode { variant: "Distinct" })
        }
        PhysicalPlan::HashAggregate { .. } => {
            Err(BuildError::UnsupportedPlanNode { variant: "HashAggregate" })
        }
        PhysicalPlan::Sort { .. } => {
            Err(BuildError::UnsupportedPlanNode { variant: "Sort" })
        }
    }
}

// ─── find_source_table ────────────────────────────────────────────────────────

/// Walk a physical plan tree and return the table name of the first
/// (leftmost) `Source` node.
///
/// Used by `compile_plan` to determine the routing key for each join arm.
pub(crate) fn find_source_table(plan: &PhysicalPlan) -> Option<String> {
    match plan {
        PhysicalPlan::Source { table } => Some(table.clone()),
        PhysicalPlan::Filter { input, .. } => find_source_table(input),
        PhysicalPlan::Project { input, .. } => find_source_table(input),
        PhysicalPlan::SymmetricHashJoin { left, .. } => find_source_table(left),
        PhysicalPlan::Distinct { input } => find_source_table(input),
        PhysicalPlan::HashAggregate { input, .. } => find_source_table(input),
        PhysicalPlan::Sort { input, .. } => find_source_table(input),
    }
}

// ─── Legacy shim ─────────────────────────────────────────────────────────────
// Builds directly from a LogicalPlan without going through PhysicalPlan.
// Used by callers that have not yet been migrated to the full pipeline.

/// Compile a `LogicalPlan` directly into a push-based operator DAG.
///
/// This shim converts each `LogicalPlan` variant to the corresponding
/// `PhysicalPlan` variant inline and delegates to `compile_plan`.
/// Prefer `select_physical_plan` + `compile_plan` for new code.
pub fn build_graph(
    plan: LogicalPlan,
    next: Box<dyn Operator>,
) -> Result<Box<dyn Operator>, BuildError> {
    compile_plan(logical_to_physical(plan)?, next)
}

fn logical_to_physical(plan: LogicalPlan) -> Result<PhysicalPlan, BuildError> {
    match plan {
        LogicalPlan::Scan { table } => Ok(PhysicalPlan::Source { table }),
        LogicalPlan::Filter { input, predicate } => Ok(PhysicalPlan::Filter {
            input: Box::new(logical_to_physical(*input)?),
            predicate,
        }),
        LogicalPlan::Project { input, columns } => Ok(PhysicalPlan::Project {
            input: Box::new(logical_to_physical(*input)?),
            columns,
        }),
        LogicalPlan::Join {
            left,
            right,
            on: (left_key, right_key),
            kind,
        } => Ok(PhysicalPlan::SymmetricHashJoin {
            left: Box::new(logical_to_physical(*left)?),
            right: Box::new(logical_to_physical(*right)?),
            left_key,
            right_key,
            kind,
        }),
        LogicalPlan::Distinct { input } => Ok(PhysicalPlan::Distinct {
            input: Box::new(logical_to_physical(*input)?),
        }),
        LogicalPlan::Aggregate { input, group_by, agg } => Ok(PhysicalPlan::HashAggregate {
            input: Box::new(logical_to_physical(*input)?),
            group_by,
            agg,
        }),
        LogicalPlan::Sort { input, keys } => Ok(PhysicalPlan::Sort {
            input: Box::new(logical_to_physical(*input)?),
            keys,
            limit: None,
        }),
        LogicalPlan::Limit { input, count, offset } => {
            // Merge Limit into a Sort if the parent is Sort, otherwise wrap.
            let physical_input = logical_to_physical(*input)?;
            match physical_input {
                PhysicalPlan::Sort { input, keys, .. } => Ok(PhysicalPlan::Sort {
                    input,
                    keys,
                    limit: Some(count),
                }),
                other => {
                    // A Limit without Sort is a plain offset+count — fold into Sort
                    // with no keys (order-preserving pass-through).
                    Ok(PhysicalPlan::Sort {
                        input: Box::new(other),
                        keys: vec![],
                        limit: Some(count + offset),
                    })
                }
            }
        }
        LogicalPlan::Union { .. } => {
            Err(BuildError::UnsupportedPlanNode { variant: "Union" })
        }
    }
}
