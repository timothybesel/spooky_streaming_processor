use crate::plan::Predicate;
use super::{Event, Operator};

/// Stateless filter operator.
///
/// Evaluates `predicate` against each incoming record; records that pass are
/// forwarded to `next`, others are silently dropped.
pub struct FilterOp {
    predicate: Predicate,
    next: Box<dyn Operator>,
}

impl FilterOp {
    pub fn new(predicate: Predicate, next: Box<dyn Operator>) -> Self {
        FilterOp { predicate, next }
    }
}

impl Operator for FilterOp {
    fn on_event(&mut self, event: Event) {
        if self.predicate.eval(&event.record) {
            self.next.on_event(event);
        }
    }
}
