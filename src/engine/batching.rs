//! # Batching operator
//!
//! `BatchingOp` accumulates incoming events into a ZSet buffer and flushes
//! consolidated batches downstream.
//!
//! ## Why this matters (DBSP math)
//!
//! With consolidation rate `c` (fraction of a batch whose weights cancel):
//! - Event-at-a-time probe work is proportional to `B` (full batch size).
//! - After batching and consolidation, probe work is proportional to `B × (1-c)`.
//! - Throughput gain: `1/(1-c)` — at c=50% → 2×, at c=90% → 10×.
//!
//! Any pipeline with high retraction rates (e.g., CDC streams with frequent
//! updates) benefits strongly from placing a `BatchingOp` before expensive
//! join probes.
//!
//! ## Pre-requisites
//!
//! Requires correct weight arithmetic in join.rs (`Weight::join_product`) so
//! that non-unit weights from consolidated batches propagate correctly.
//!
//! ## Buffer design
//!
//! `Record = HashMap<String, Value>` does not implement `Hash`, so we use a
//! `Vec<(Record, i64)>` buffer with linear record equality search.
//! For typical flush thresholds (≤10k records) this is acceptable; for larger
//! thresholds, use a canonical sorted-key map.

use std::sync::Arc;

use super::{Event, Operator, Record, Weight};

/// Accumulates events into a ZSet buffer and flushes consolidated batches.
///
/// Records with net weight 0 (an insert followed by a delete for the same
/// record) are pruned before flushing — this is the consolidation step.
pub struct BatchingOp {
    /// The ZSet buffer: list of (record, net_weight) pairs.
    /// Records are deduplicated by equality; existing entries are updated
    /// in-place rather than appended.
    buffer: Vec<(Record, i64)>,
    /// Flush when buffer length reaches this many distinct records.
    flush_threshold: usize,
    /// Source label forwarded on flushed events.
    source: Arc<str>,
    next: Box<dyn Operator>,
}

impl BatchingOp {
    pub fn new(source: Arc<str>, flush_threshold: usize, next: Box<dyn Operator>) -> Self {
        BatchingOp {
            buffer: Vec::new(),
            flush_threshold,
            source,
            next,
        }
    }

    /// Flush all buffered events downstream, skipping zero-weight records.
    pub fn flush(&mut self) {
        let source = self.source.clone();
        for (record, raw_weight) in self.buffer.drain(..) {
            if raw_weight != 0 {
                self.next.on_event(Event {
                    source: source.clone(),
                    record,
                    weight: Weight(raw_weight),
                });
            }
        }
    }
}

impl Operator for BatchingOp {
    fn on_event(&mut self, event: Event) {
        // Update in-place if the same record was already buffered.
        if let Some(entry) = self.buffer.iter_mut().find(|(r, _)| r == &event.record) {
            entry.1 += event.weight.0;
        } else {
            self.buffer.push((event.record, event.weight.0));
        }

        if self.buffer.len() >= self.flush_threshold {
            self.flush();
        }
    }
}
