use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::plan::{JoinType, Value};
use super::{Event, Operator, Record, Weight};

/// The inner hash table type: join-key → list of (record, net_weight) pairs.
type JoinMap = HashMap<Value, Vec<(Arc<Record>, i64)>>;

// ─── Shared state ────────────────────────────────────────────────────────────

/// Mutable core shared between the left and right arms of one join.
///
/// Each side maintains its own hash table indexed by its join key.
/// Records are stored as `(Arc<Record>, i64)` pairs — the `Arc` allows the
/// probe phase to hold references without cloning the full record on every
/// match (Phase 3.1 optimization), and the `i64` stores the net weight so
/// that retractions can be tracked and zero-weight records pruned (C8 fix).
///
/// The downstream operator (`next`) is also stored here so both arms can
/// emit to it without duplicating the pointer.
pub(crate) struct JoinCore {
    left_key: String,
    right_key: String,
    kind: JoinType,
    /// left_key_value → list of (record, net_weight) pairs.
    left_map: JoinMap,
    /// right_key_value → list of (record, net_weight) pairs.
    right_map: JoinMap,
    next: Box<dyn Operator>,
}

impl JoinCore {
    pub(crate) fn new(
        left_key: String,
        right_key: String,
        kind: JoinType,
        next: Box<dyn Operator>,
    ) -> Self {
        JoinCore {
            left_key,
            right_key,
            kind,
            left_map: HashMap::new(),
            right_map: HashMap::new(),
            next,
        }
    }
}

/// Shared mutable join state — intentionally `Rc<RefCell>`, not `Arc<Mutex>`.
///
/// This engine is **single-threaded** by design.  Do NOT add `unsafe impl Send`
/// on this type — the `Rc` is genuinely unsafe across threads.
/// To add multi-threading, replace with `Arc<Mutex<JoinCore>>` and add
/// `+ Send + Sync` bounds to the `Operator` trait throughout.
pub(crate) type SharedJoin = Rc<RefCell<JoinCore>>;

// ─── Symmetric Hash Join operator ────────────────────────────────────────────

/// Which input arm this operator instance represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinSide {
    Left,
    Right,
}

/// One input arm of a **Symmetric Hash Join**.
///
/// Both `Left` and `Right` instances share a single `JoinCore` via
/// `Rc<RefCell<JoinCore>>`.
///
/// ### Incremental update protocol (three phases, no double-borrow)
///
/// 1. **Probe** – borrow `core` immutably, scan the opposite side's hash
///    table, and collect all matching merged records into a local `Vec`.
///    Release the borrow.
///
/// 2. **Insert** – borrow `core` mutably, update this side's hash table.
///    If the arriving record already exists (retraction pattern), update its
///    net weight in-place and prune entries where weight == 0.
///    Release the borrow.
///
/// 3. **Emit** – borrow `core` mutably, push every output collected in
///    step 1 to `core.next`.  Release the borrow.
///
/// Splitting into three phases ensures `RefCell` is never borrowed more than
/// once at a time.
pub(crate) struct JoinSideOp {
    side: JoinSide,
    core: SharedJoin,
}

impl JoinSideOp {
    pub(crate) fn new(side: JoinSide, core: SharedJoin) -> Self {
        JoinSideOp { side, core }
    }
}

impl Operator for JoinSideOp {
    fn on_event(&mut self, event: Event) {
        // ── Phase 1: Probe the opposite side ─────────────────────────────
        // `core` is borrowed immutably and released at end of this block.
        let outputs: Vec<Event> = {
            let core = self.core.borrow();

            let (own_key, probe_map): (&str, &JoinMap) =
                match self.side {
                    JoinSide::Left => (&core.left_key, &core.right_map),
                    JoinSide::Right => (&core.right_key, &core.left_map),
                };

            // For Cross joins all records share a single sentinel key so
            // every left record probes every right record.
            let join_val = if core.kind == JoinType::Cross {
                Value::Null
            } else {
                event.record.get(own_key).cloned().unwrap_or(Value::Null)
            };

            match (probe_map.get(&join_val), &core.kind) {
                // ── Inner join ────────────────────────────────────────────
                (Some(matches), JoinType::Inner) | (Some(matches), JoinType::Cross) => matches
                    .iter()
                    .filter(|(_, stored_w)| *stored_w != 0)
                    .map(|(other, stored_w)| {
                        let mut merged = event.record.clone();
                        merged.extend(other.as_ref().clone());
                        Event {
                            source: event.source.clone(),
                            record: merged,
                            // Correct bilinear weight: arriving × stored (C3 fix)
                            weight: Weight::join_product(event.weight, Weight(*stored_w)),
                        }
                    })
                    .collect(),

                // ── Semi join (col IN subquery) ───────────────────────────
                // Emit the left record exactly once if at least one live match exists.
                (Some(matches), JoinType::LeftSemi) => {
                    let has_live_match = matches.iter().any(|(_, w)| *w != 0);
                    if has_live_match && self.side == JoinSide::Left {
                        vec![Event {
                            source: event.source.clone(),
                            record: event.record.clone(),
                            weight: event.weight,
                        }]
                    } else {
                        vec![]
                    }
                }

                // ── Anti-semi join (col NOT IN subquery) ──────────────────
                // Some match found → suppress the left record.
                (Some(_), JoinType::LeftAnti) => vec![],
                // No match found.
                (None, JoinType::LeftAnti) if self.side == JoinSide::Left => {
                    // Left side with no right match → emit (NOT IN satisfied).
                    vec![Event {
                        source: event.source.clone(),
                        record: event.record.clone(),
                        weight: event.weight,
                    }]
                }
                // No match found, but this is the right side of LeftAnti — no output.
                (None, JoinType::LeftAnti) => vec![],

                // No match in non-anti joins → no output.
                (None, _) => vec![],
            }
        }; // ← immutable borrow of core released here

        // ── Phase 2: Insert / update this side's hash table ──────────────
        {
            let mut core = self.core.borrow_mut();

            // For Cross joins use sentinel key so all records probe each other.
            let key = if core.kind == JoinType::Cross {
                Value::Null
            } else {
                let own_key = match self.side {
                    JoinSide::Left => core.left_key.clone(),
                    JoinSide::Right => core.right_key.clone(),
                };
                event.record.get(&own_key).cloned().unwrap_or(Value::Null)
            };

            let map = match self.side {
                JoinSide::Left => &mut core.left_map,
                JoinSide::Right => &mut core.right_map,
            };

            let bucket = map.entry(key.clone()).or_default();

            // Update in-place if this exact record was already stored, otherwise push.
            // This enables correct retraction handling (C8 fix):
            // A +1 followed by a -1 for the same record results in net weight 0,
            // which is then pruned, keeping the map from growing without bound.
            let record_arc = Arc::new(event.record);
            if let Some(entry) = bucket.iter_mut().find(|(r, _)| **r == *record_arc) {
                entry.1 += event.weight.0;
            } else {
                bucket.push((record_arc, event.weight.0));
            }

            // Prune zero-weight entries (retraction cleanup).
            bucket.retain(|(_, w)| *w != 0);
            if bucket.is_empty() {
                map.remove(&key);
            }
        } // ← mutable borrow released here

        // ── Phase 3: Emit joined outputs downstream ───────────────────────
        {
            let mut core = self.core.borrow_mut();
            for out in outputs {
                core.next.on_event(out);
            }
        }
    }
}
