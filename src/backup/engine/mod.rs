use std::collections::HashMap;
use std::sync::Arc;

use crate::plan::Value;

pub mod batching;
pub mod builder;
pub mod filter;
pub mod join;
pub mod project;
pub mod sink;
pub mod weight;

pub use weight::Weight;

/// A database row: a map of column name → value.
pub type Record = HashMap<String, Value>;

/// A streaming change event.
///
/// `weight = Weight::INSERT` (+1) represents an **insertion**;
/// `weight = Weight::DELETE` (-1) represents a **deletion/retraction**.
/// Other non-zero values arise from join weight products or batching.
///
/// The `source` field is an `Arc<str>` to avoid a heap allocation when
/// routing through `DispatcherOp` (the same interned string is shared by all
/// events from the same table).
pub struct Event {
    /// Source table name — used by `DispatcherOp` to route to the right pipeline.
    pub source: Arc<str>,
    pub record: Record,
    /// DBSP weight: +1 insert, -1 delete, or a join-product weight.
    pub weight: Weight,
}

/// Core trait for every push-based physical operator.
///
/// The DAG is assembled bottom-up: each operator wraps the next one in a
/// `Box<dyn Operator>` and calls `next.on_event(...)` for every output it produces.
pub trait Operator {
    fn on_event(&mut self, event: Event);
}

/// Entry-point operator that routes events to sub-pipelines by source table.
///
/// `compile_plan` returns a `DispatcherOp` whenever the plan has more than one
/// data source (i.e., when a Join is present).
pub struct DispatcherOp {
    routes: HashMap<Arc<str>, Box<dyn Operator>>,
}

impl DispatcherOp {
    pub(crate) fn new(routes: HashMap<Arc<str>, Box<dyn Operator>>) -> Self {
        DispatcherOp { routes }
    }
}

impl Operator for DispatcherOp {
    fn on_event(&mut self, event: Event) {
        match self.routes.get_mut(&event.source) {
            Some(op) => op.on_event(event),
            None => eprintln!("[Dispatcher] unknown source '{}'", event.source),
        }
    }
}
