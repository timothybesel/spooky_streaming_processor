//! # Physical Plan
//!
//! `PhysicalPlan` sits between `LogicalPlan` and the physical operator DAG.
//!
//! ## Layer responsibilities
//!
//! | Layer | Responsibility |
//! |---|---|
//! | `LogicalPlan` | Algebraic query intent (filter, join, aggregate) |
//! | `PhysicalPlan` | Algorithm selection (SymmetricHashJoin, HashAggregate) |
//! | Operator DAG | Push-based execution (built by `compile_plan`) |
//!
//! `select_physical_plan` chooses the physical algorithm for each logical
//! operator.  Currently only one algorithm exists per operator type, so the
//! mapping is trivial.  When alternative algorithms are added (e.g.,
//! sort-merge join alongside symmetric hash join), the selection logic lives
//! here, optionally using cost estimates.

use crate::plan::{AggExpr, JoinType, LogicalPlan, Predicate, SortDir};

// ─── PlanError ───────────────────────────────────────────────────────────────

/// Errors produced by `select_physical_plan`.
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("logical plan variant '{variant}' has no physical implementation yet")]
    NoPhysicalImpl { variant: &'static str },
}

// ─── PhysicalPlan ────────────────────────────────────────────────────────────

/// A physical query plan node.
///
/// Each variant represents a specific algorithm chosen for the logical
/// operation.  The physical layer is the last tree-shaped representation
/// before the plan is compiled into a flat push-based operator DAG by
/// `compile_plan`.
#[derive(Debug)]
pub enum PhysicalPlan {
    /// Base table scan — events arrive from an external source.
    Source { table: String },

    /// Row-level predicate filter.
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Predicate,
    },

    /// Column projection — drops unlisted columns.
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<String>,
    },

    /// Symmetric Hash Join — incremental equi-join algorithm.
    ///
    /// Each side maintains a hash table indexed by its join key.
    /// Both sides probe the opposite table on every incoming event.
    SymmetricHashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_key: String,
        right_key: String,
        kind: JoinType,
    },

    /// Deduplication — retains only records with net weight != 0.
    Distinct { input: Box<PhysicalPlan> },

    /// Hash aggregate — groups by key columns and applies aggregate functions.
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<String>,
        agg: Vec<AggExpr>,
    },

    /// Sort with optional limit (TOP-N).
    Sort {
        input: Box<PhysicalPlan>,
        keys: Vec<(String, SortDir)>,
        limit: Option<usize>,
    },
}

// ─── select_physical_plan ────────────────────────────────────────────────────

/// Convert a `LogicalPlan` into a `PhysicalPlan` by choosing algorithms.
///
/// Currently a 1-to-1 mapping since only one algorithm per operator is
/// implemented.  Returns `PlanError::NoPhysicalImpl` for operators not yet
/// supported by the engine (Distinct, Aggregate, Sort).
pub fn select_physical_plan(logical: LogicalPlan) -> Result<PhysicalPlan, PlanError> {
    match logical {
        LogicalPlan::Scan { table } => Ok(PhysicalPlan::Source { table }),

        LogicalPlan::Filter { input, predicate } => Ok(PhysicalPlan::Filter {
            input: Box::new(select_physical_plan(*input)?),
            predicate,
        }),

        LogicalPlan::Project { input, columns } => Ok(PhysicalPlan::Project {
            input: Box::new(select_physical_plan(*input)?),
            columns,
        }),

        LogicalPlan::Join {
            left,
            right,
            on: (left_key, right_key),
            kind,
        } => Ok(PhysicalPlan::SymmetricHashJoin {
            left: Box::new(select_physical_plan(*left)?),
            right: Box::new(select_physical_plan(*right)?),
            left_key,
            right_key,
            kind,
        }),

        LogicalPlan::Union { .. } => Err(PlanError::NoPhysicalImpl { variant: "Union" }),

        LogicalPlan::Distinct { input } => Ok(PhysicalPlan::Distinct {
            input: Box::new(select_physical_plan(*input)?),
        }),

        LogicalPlan::Aggregate { input, group_by, agg } => Ok(PhysicalPlan::HashAggregate {
            input: Box::new(select_physical_plan(*input)?),
            group_by,
            agg,
        }),

        LogicalPlan::Sort { input, keys } => Ok(PhysicalPlan::Sort {
            input: Box::new(select_physical_plan(*input)?),
            keys,
            limit: None,
        }),

        LogicalPlan::Limit { input, count, offset } => {
            // Merge Limit into an enclosing Sort if present.
            let physical = select_physical_plan(*input)?;
            match physical {
                PhysicalPlan::Sort { input, keys, .. } => Ok(PhysicalPlan::Sort {
                    input,
                    keys,
                    limit: Some(count),
                }),
                other => Ok(PhysicalPlan::Sort {
                    input: Box::new(other),
                    keys: vec![],
                    limit: Some(count + offset),
                }),
            }
        }
    }
}
