use super::{Event, Operator};

/// Projection operator — retains only the named columns in each record.
///
/// Columns not in the list are dropped from the record before forwarding
/// the event downstream.  The weight is preserved unchanged.
pub struct ProjectOp {
    columns: Vec<String>,
    next: Box<dyn Operator>,
}

impl ProjectOp {
    pub fn new(columns: Vec<String>, next: Box<dyn Operator>) -> Self {
        ProjectOp { columns, next }
    }
}

impl Operator for ProjectOp {
    fn on_event(&mut self, mut event: Event) {
        event.record.retain(|col, _| self.columns.contains(col));
        self.next.on_event(event);
    }
}
