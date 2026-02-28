# Architecture: `spooky_streaming_processor`

> **Revision**: 2026-02-28
> **Language**: Rust, edition 2024
> **Crate type**: binary
> **Version**: 0.1.0

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Layer Map](#2-layer-map)
3. [The DBSP Mathematical Model](#3-the-dbsp-mathematical-model)
4. [Query Pipeline](#4-query-pipeline)
5. [Key Design Decisions](#5-key-design-decisions)
6. [Operator Catalog](#6-operator-catalog)
7. [The Symmetric Hash Join](#7-the-symmetric-hash-join)
8. [Weight Arithmetic](#8-weight-arithmetic)
9. [Error Handling Strategy](#9-error-handling-strategy)
10. [Data Flow](#10-data-flow)
11. [Technical Debt and Known Limitations](#11-technical-debt-and-known-limitations)
12. [Getting Started (Contributor View)](#12-getting-started-contributor-view)

---

## 1. Project Overview

### What Is DBSP?

**DBSP** (Database Stream Processing) is a computational model for evaluating relational queries **incrementally** over streams of changes. The core insight is that standard relational algebra — SELECT, JOIN, PROJECT, FILTER — can be reformulated over *Z-sets* (integer-weighted multisets) such that every operator consumes only the *delta* of its input (the change since last evaluation) and produces only the *delta* of its output.

This is in contrast to:
- **Batch query engines** (e.g., DuckDB, SparkSQL): re-scan all data on every query.
- **Naive streaming systems**: emit all intermediate results on every event, then recompute.

In DBSP, a DELETE event is simply a record with weight `-1`. The math of the algebra guarantees that operators applied to a stream of deltas produce the correct stream of output deltas for the full result relation. No checkpointing or full re-evaluation is needed.

The theoretical foundation is developed in:
> Budiu, M. et al. "DBSP: Automatic Incremental View Maintenance for Rich Query Languages" (VLDB 2023).

### What This Project Implements

`spooky_streaming_processor` is a prototype Rust implementation of a DBSP streaming query engine. It is a complete vertical slice from SQL text parsing through to live event evaluation:

- A **mock AST** that mirrors the SurrealDB query language shape, with a working lowering pass to a logical plan IR.
- A **rule-based logical optimizer** (predicate pushdown, trivial projection elimination, fixed-point iteration).
- A **physical planner** that selects the concrete algorithm for each logical operator.
- A **push-based operator DAG** built at compile time from the physical plan, consisting of composable `Box<dyn Operator>` nodes.
- A **symmetric hash join** implementing the incremental ΔA⋈B_old + A_old⋈ΔB formula with correct bilinear weight arithmetic.
- A **batching operator** that consolidates Z-set buffers before forwarding, enabling high retraction-rate throughput gains.
- A **SurrealDB parser integration** via `surrealdb-core 3.0.0` for real SQL text parsing (with a documented limitation that the resulting AST types are `pub(crate)` and cannot be further inspected).

### Who It Is For

- **Researchers** exploring incremental query evaluation, DBSP algebra implementations, and streaming join algorithms.
- **Engineers** building or extending a streaming query layer on top of SurrealDB's parser.
- **Contributors** who want to add new operators (aggregation, sort, distinct) or improve optimizer rules.

---

## 2. Layer Map

Every source file belongs to one named layer. Reading this table first gives a mental model for navigation.

| Module path | Layer | Responsibility |
|---|---|---|
| `src/plan.rs` | IR | Core data types: `Value`, `Predicate`, `JoinType`, `LogicalPlan`, `AggExpr`, `SortDir`. These are the lingua franca shared by all layers. |
| `src/lowering.rs` | Frontend / Lowering | Translates a mock SurrealDB-style AST (`MockSelectStmt`) to `LogicalPlan`. Decorrelates `IN (subquery)` into `LeftSemi` joins. Owns `LoweringError`. |
| `src/real_lowering.rs` | Frontend / Lowering (stub) | Intended destination for real `surrealdb_core::sql::SelectStatement` lowering. Currently a stub; see §11 for why. |
| `src/optimizer.rs` | Optimizer | Rule-based, fixed-point algebraic optimizer over `LogicalPlan`. Implements predicate pushdown, trivial project elimination, and a placeholder projection pushdown. |
| `src/physical_plan.rs` | Physical Planner | `PhysicalPlan` enum and `select_physical_plan()`. Chooses the concrete algorithm for each logical operator (currently 1-to-1). Owns `PlanError`. |
| `src/engine/mod.rs` | Engine / Runtime | Defines `Record`, `Event`, `Weight` (re-exported), the `Operator` trait, and `DispatcherOp`. |
| `src/engine/weight.rs` | Engine / Runtime | `Weight(i64)` newtype. Implements `Add`, `Neg`, `scale()`, `join_product()`. Deliberately omits `Mul<Weight> for Weight`. |
| `src/engine/builder.rs` | Engine / Compiler | `compile_plan()`: recursively compiles a `PhysicalPlan` into a `Box<dyn Operator>` DAG. Also contains `build_graph()` legacy shim and `BuildError`. |
| `src/engine/filter.rs` | Engine / Operator | `FilterOp`: stateless, linear DBSP filter. |
| `src/engine/project.rs` | Engine / Operator | `ProjectOp`: stateless, linear DBSP projection. |
| `src/engine/join.rs` | Engine / Operator | `JoinCore`, `JoinSideOp`, `SharedJoin`: bilinear symmetric hash join with three-phase protocol. |
| `src/engine/batching.rs` | Engine / Operator | `BatchingOp`: Z-set consolidation buffer; flushes when threshold is reached. |
| `src/engine/sink.rs` | Engine / Output | `SinkOp`: terminal operator; pretty-prints events to stdout. |
| `src/main.rs` | Binary entrypoint | Three demo functions wired together: `demo_engine()`, `demo_lowering()`, `demo_real_parser()`. |

### Dependency direction (must be acyclic)

```
plan.rs
  ↑
lowering.rs   real_lowering.rs
  ↑
optimizer.rs
  ↑
physical_plan.rs
  ↑
engine/{weight, filter, project, join, batching, sink, builder, mod}
  ↑
main.rs
```

No module below in this ordering imports from one above it.

---

## 3. The DBSP Mathematical Model

### Z-Sets

A **Z-set** is a function from records to integers:

```
Z = { record → weight  |  weight ∈ ℤ }
```

- `weight = +1`: the record is present with multiplicity one (a standard INSERT).
- `weight = -1`: the record is being retracted (a DELETE).
- `weight = +2`: the record is present with multiplicity two (two concurrent INSERT events for the same tuple, before consolidation).
- `weight = 0`: the record is absent (insert + delete cancelled; can be pruned).

In Rust, a single element of a Z-set is represented as an `Event`:

```rust
// src/engine/mod.rs
pub struct Event {
    pub source: Arc<str>,
    pub record: Record,        // HashMap<String, Value>
    pub weight: Weight,        // Weight(i64)
}
```

The `Weight` type enforces correct Z-set algebra at the type level (see §8).

### Linear Operators

An operator `f` is **linear** if:

```
f(Δ) = Δf        (the output delta is a linear function of the input delta)
```

This means: to compute the incremental result of `f`, just apply `f` to the delta. There is no need to maintain state.

**Filter** and **Project** are both linear:

- `Filter(predicate, Δ)` passes each event in `Δ` unchanged if `predicate` evaluates to `true`; otherwise drops it. Weight is preserved.
- `Project(columns, Δ)` removes all columns not in `columns` from each event's record. Weight is preserved.

From `src/engine/filter.rs`:

```rust
impl Operator for FilterOp {
    fn on_event(&mut self, event: Event) {
        if self.predicate.eval(&event.record) {
            self.next.on_event(event);   // weight passes through unchanged
        }
    }
}
```

From `src/engine/project.rs`:

```rust
impl Operator for ProjectOp {
    fn on_event(&mut self, mut event: Event) {
        event.record.retain(|col, _| self.columns.contains(col));
        self.next.on_event(event);       // weight passes through unchanged
    }
}
```

### Bilinear Operators: Join

Join is **bilinear**: it is linear in each of its two arguments separately. Concretely, the incremental join formula is:

```
Δ(A ⋈ B) = ΔA ⋈ B_old + A_old ⋈ ΔB
```

Where:
- `ΔA` is the incoming delta on the left input.
- `ΔB` is the incoming delta on the right input.
- `A_old` and `B_old` are the accumulated (historical) Z-sets on each side.

For a pair `(a, b)` that matches on the join key, the output weight is the **product** of the two input weights:

```
w_out = w_left × w_right
```

This is mathematically correct because Z-sets use multiplication to express the join's Cartesian product of multiplicities. A `+1` insert matched against a `-1` delete produces a `-1` output (a retraction of the previously-joined row). Two `+1` inserts produce a `+1` output.

In Rust, this product is expressed as:

```rust
// src/engine/weight.rs
pub fn join_product(left: Weight, right: Weight) -> Weight {
    Weight(left.0 * right.0)
}
```

And called in the join probe phase:

```rust
// src/engine/join.rs  — inside Phase 1
weight: Weight::join_product(event.weight, Weight(*stored_w)),
```

The `Mul<Weight> for Weight` trait is deliberately absent; see §8.

---

## 4. Query Pipeline

The full compilation pipeline for a query, from SQL text to a live operator DAG, consists of seven distinct transformation stages.

### Stage 1: SQL Text → Opaque AST

```rust
// src/main.rs, demo_real_parser()
use surrealdb_core::syn;
let ast = syn::parse(query)?;   // returns surrealdb_core::syn::Ast
```

`surrealdb_core::syn::parse()` tokenises and parses a SurrealDB SQL string. The returned `Ast` type can be `Debug`-printed but its fields cannot be accessed from external crates because all inner SQL types are declared `pub(crate)` in `surrealdb-core 3.0.0`. See §11 for the consequence.

### Stage 2: Mock AST Construction (current workaround)

Because the real AST is opaque, the working pipeline uses a mock AST defined in `src/lowering.rs`:

```rust
// src/lowering.rs
pub(crate) struct MockSelectStmt {
    pub(crate) table: String,
    pub(crate) select_fields: Vec<String>,
    pub(crate) cond: Option<MockCond>,
}
```

`MockCond` mirrors `surrealdb_core::sql::Cond` with operators like `MockOp::Inside` (SQL `IN`), `MockOp::Gt`, `MockOp::Eq`, logical `And`, `Or`, `Not`, and arbitrary nested `Subquery`.

`MockExpr` covers the expression shapes: `Column(name)`, `Literal(Value)`, and `Subquery(Box<MockSelectStmt>)`.

### Stage 3: Mock AST → `LogicalPlan` (Lowering)

```rust
// src/lowering.rs
pub fn lower_select(stmt: MockSelectStmt) -> Result<LogicalPlan, LoweringError>
```

`lower_select` walks the mock AST and produces a `LogicalPlan` tree. The key non-trivial transformation is **subquery decorrelation**:

```
WHERE col IN (SELECT inner_col FROM t WHERE ...)
→
LogicalPlan::Join {
    left:  Scan(outer_table),
    right: lower_select(subquery),
    on:    (col, inner_col),
    kind:  JoinType::LeftSemi,
}
```

DBSP cannot evaluate correlated subqueries incrementally — each arriving event would require a fresh evaluation of the subquery against the full relation. Decorrelation removes the correlation by expressing the `IN` check as a join, which is incrementally evaluable.

Logical AND conditions stack as nested `Filter` nodes:

```
WHERE age > 18 AND active = true
→
Filter(
  input: Filter(input: Scan, predicate: Gt("age", 18)),
  predicate: Eq("active", true)
)
```

Logical OR conditions combine into a compound `Predicate::Or` inside a single `Filter`.

Any condition shape not recognised returns an explicit `LoweringError` — there are no silent fallbacks.

### Stage 4: Logical Plan Optimization

```rust
// src/optimizer.rs
pub fn optimize(plan: LogicalPlan) -> LogicalPlan
```

The optimizer applies a set of algebraic equivalence rules in a fixed-point loop:

```rust
let rules: &[fn(LogicalPlan) -> LogicalPlan] = &[
    push_filters_below_joins,
    eliminate_trivial_projects,
    prune_unused_columns,
];

loop {
    let next = rules.iter().fold(current.clone(), |p, rule| rule(p));
    if next == current { break; }
    current = next;
}
```

The loop terminates because: (a) each rule only moves nodes strictly closer to the leaves, and (b) `LogicalPlan` is a finite tree with no cycles.

**Predicate pushdown** (`push_filters_below_joins`):

```
Filter(Join(A, B), p) → Join(Filter(A, p), B)
```

This fires when all columns in `p` appear in exactly one join arm's key. The check is conservative: it only pushes when predicate columns exactly match the join key column. Pushing to the right side of a non-inner join is forbidden (it would change outer-join semantics).

**Trivial project elimination** (`eliminate_trivial_projects`):

Removes `Project { columns: [] }` nodes, which represent `SELECT *` — a no-op.

**Projection pushdown** (`prune_unused_columns`):

Currently a no-op placeholder that returns the plan unchanged. The plumbing exists for future implementation.

### Stage 5: `LogicalPlan` → `PhysicalPlan`

```rust
// src/physical_plan.rs
pub fn select_physical_plan(logical: LogicalPlan) -> Result<PhysicalPlan, PlanError>
```

This stage selects the concrete physical algorithm for each logical operator. Currently the mapping is 1-to-1:

| Logical | Physical |
|---|---|
| `Scan` | `Source` |
| `Filter` | `Filter` |
| `Project` | `Project` |
| `Join` (any kind) | `SymmetricHashJoin` |
| `Distinct` | `Distinct` (engine unimplemented) |
| `Aggregate` | `HashAggregate` (engine unimplemented) |
| `Sort` | `Sort` (engine unimplemented) |
| `Limit` | Folded into `Sort { limit: Some(n) }` |
| `Union` | `PlanError::NoPhysicalImpl` |

When alternative algorithms exist (e.g., sort-merge join), the cost-based selection logic would live here.

### Stage 6: `PhysicalPlan` → Operator DAG (Compilation)

```rust
// src/engine/builder.rs
pub fn compile_plan(
    plan: PhysicalPlan,
    next: Box<dyn Operator>,
) -> Result<Box<dyn Operator>, BuildError>
```

`compile_plan` is a recursive bottom-up DAG assembler. It takes the **downstream** operator `next` as a parameter, wraps it in the operator for the current plan node, and recurses into the input sub-plan:

```
compile_plan(Filter { input: Source("users"), predicate: age>18 }, SinkOp)
  → filter = FilterOp::new(age>18, SinkOp)          // wrap downstream first
  → compile_plan(Source("users"), filter)             // recurse
  → returns filter                                    // Source is a routing leaf
```

For joins, the function creates a shared `Rc<RefCell<JoinCore>>` and wires two `JoinSideOp` instances to it before building two separate pipelines and combining them into a `DispatcherOp`:

```rust
let core = Rc::new(RefCell::new(JoinCore::new(left_key, right_key, kind, next)));
let left_join_op  = Box::new(JoinSideOp::new(JoinSide::Left,  Rc::clone(&core)));
let right_join_op = Box::new(JoinSideOp::new(JoinSide::Right, Rc::clone(&core)));
let left_pipeline  = compile_plan(*left,  left_join_op)?;
let right_pipeline = compile_plan(*right, right_join_op)?;
// routes: { "users" → left_pipeline, "orders" → right_pipeline }
Ok(Box::new(DispatcherOp::new(routes)))
```

### Stage 7: Live Event Evaluation

```rust
// src/main.rs
graph.on_event(Event {
    source: Arc::from("users"),
    record: rec(&[("id", Value::Int(1)), ("age", Value::Int(30))]),
    weight: Weight::INSERT,
});
```

Each call to `on_event` pushes a single Z-set delta through the entire compiled DAG. Because the DAG is push-based, the event propagates from the entry point (`DispatcherOp`) through filters, joins, projections, and terminates at `SinkOp` which prints the output diff.

---

## 5. Key Design Decisions

### Decision 1: Push-Based DAG (Not Pull/Volcano)

**Decision**: Operators call `next.on_event()` to push output downstream. There is no `pull()` or `next()` iterator that the parent calls.

**Why**: DBSP's computational model is naturally push-based. A streaming event arrives from an external source (Kafka, CDC, a network socket), and the system must react to it. Pull-based iterators require the sink to drive execution, which means the pipeline must poll continuously even when no events are arriving. Push means the event triggers exactly the computation necessary.

**Trade-offs**: Push makes backpressure harder (a slow sink cannot signal the source to pause). It also prevents lazy evaluation across operator boundaries. For a prototype engine that processes discrete events, these trade-offs are acceptable.

**Alternatives considered**: The Volcano/iterator model used by most DBMS engines (DuckDB, Postgres). It was rejected because it conflicts with the "event arrives, react" execution model.

### Decision 2: `Rc<RefCell<JoinCore>>` — Intentionally Single-Threaded

**Decision**: The shared join state is `Rc<RefCell<JoinCore>>`, not `Arc<Mutex<JoinCore>>`.

**Why**: `Rc` avoids the atomic reference count overhead on every clone; `RefCell` avoids the syscall overhead of a mutex on every borrow. The engine is explicitly single-threaded — events are processed one at a time in a single-threaded loop.

**The comment in `src/engine/join.rs` is a deliberate guard rail**:

```rust
/// This engine is **single-threaded** by design.  Do NOT add `unsafe impl Send`
/// on this type — the `Rc` is genuinely unsafe across threads.
/// To add multi-threading, replace with `Arc<Mutex<JoinCore>>` and add
/// `+ Send + Sync` bounds to the `Operator` trait throughout.
pub(crate) type SharedJoin = Rc<RefCell<JoinCore>>;
```

**Trade-offs**: Single-threaded means the engine cannot scale across CPU cores. Multi-table, multi-pipeline parallelism requires the `Arc<Mutex>` path.

**Alternatives considered**: `Arc<Mutex>` with `+ Send + Sync` bounds on `Operator`. This is the correct path for multi-threaded execution but was deferred to avoid premature complexity.

### Decision 3: Three-Phase Join Protocol

**Decision**: `JoinSideOp::on_event` is split into three sequential phases (Probe, Insert, Emit) with explicit borrow release between each phase.

**Why**: `Rc<RefCell<T>>` panics at runtime if borrowed twice simultaneously. A naive join implementation that (a) borrows the core to probe, and then (b) calls `next.on_event()` while the probe borrow is held, will panic if `next` is itself a `JoinSideOp` that tries to borrow the same core.

The three phases guarantee:

1. **Phase 1 (Probe)**: immutable borrow, collect results into a local `Vec`. Borrow released.
2. **Phase 2 (Insert)**: mutable borrow, update hash table. Borrow released.
3. **Phase 3 (Emit)**: mutable borrow, drain the `Vec` and call `next.on_event()`. Borrow released.

At no point are two borrows of the same `RefCell` alive simultaneously.

**Trade-offs**: An extra allocation of a `Vec<Event>` per event processed. For typical event sizes this is negligible.

**Alternatives considered**: Using `unsafe` to hold a raw pointer to the inner data. Rejected because it eliminates the safety guarantees that motivated using Rust in the first place.

### Decision 4: `Arc<Record>` in `JoinCore` Maps

**Decision**: Records stored in `left_map` and `right_map` are wrapped in `Arc<Record>`:

```rust
type JoinMap = HashMap<Value, Vec<(Arc<Record>, i64)>>;
```

**Why**: During a join probe, the probe side clones the arriving record and merges it with every matching stored record. Without `Arc`, each merge would require a full `HashMap<String, Value>` clone per match. With `Arc`, the stored record is shared by reference; only the merge into the output record (`event.record.clone()` then `merged.extend(other.as_ref().clone())`) performs a clone, and that clone is necessary to produce the merged output tuple.

For a join with `N` left records matching `M` right records, this reduces allocations from `O(N*M * avg_record_width)` to `O(N*M)` for the output records, but avoids duplicating the stored side.

**Trade-offs**: `Arc` adds a heap allocation and an atomic reference count per stored record. For large joins with high key cardinality, this is dominated by the savings.

**Alternatives considered**: Storing `Record` by value. Simple but leads to O(NM) clone work per join match across the stored side.

### Decision 5: `Weight(i64)` Newtype

**Decision**: Weight is a newtype over `i64` with deliberately restricted arithmetic. See §8 for the full analysis.

### Decision 6: `LoweringError` / `BuildError` / `PlanError` Result Types

**Decision**: Every fallible transformation returns `Result<T, SpecificError>` with a typed error enum. No function silently ignores an unrecognised input and falls back to a default.

**Why**: Silent fallbacks (returning a full-table scan for an unrecognised WHERE condition, for example) are a common source of correctness bugs in query engines. A planning error should always be visible to the caller.

**Trade-offs**: More boilerplate error types, and callers must handle `Result` everywhere. This is the correct trade-off for a query engine where silent wrong answers are worse than explicit failures.

**Alternatives considered**: A single catch-all `anyhow::Error`. Rejected because `anyhow` errors are not matchable by callers — a test cannot write `assert!(matches!(e, LoweringError::InsideRequiresSubquery { .. }))` against an `anyhow::Error`.

### Decision 7: `surrealdb-core` for Parsing, Not the Full `surrealdb` Client

**Decision**: `Cargo.toml` depends on `surrealdb-core = { version = "3.0.0", default-features = false }`, not the full `surrealdb` crate.

**Why**: The `surrealdb` client crate brings in a large dependency tree (TLS, HTTP, WebSocket, etc.) that is unnecessary for a local query engine. `surrealdb-core` is the parser-only kernel.

**Trade-offs**: `surrealdb-core 3.0.0` declares all SQL AST types as `pub(crate)`. This blocks access to the parsed AST from external crates. The full `surrealdb` crate exposes AST types via feature flags. See §11 for the migration path.

---

## 6. Operator Catalog

Every physical operator implements the `Operator` trait:

```rust
// src/engine/mod.rs
pub trait Operator {
    fn on_event(&mut self, event: Event);
}
```

All operators are `Box<dyn Operator>` in the compiled DAG. Below is the complete catalog.

### `DispatcherOp`

**File**: `src/engine/mod.rs`

**Type**: Entry-point router. Not a DBSP algebraic operator; it is the multiplexer created when a join is present.

**State**: `HashMap<Arc<str>, Box<dyn Operator>>` — routing table from source table name to pipeline.

**Behaviour**: On `on_event`, looks up `event.source` in the routing table and calls `op.on_event(event)`. If the source is not registered, prints a warning to stderr and drops the event.

**Invariants**: Every table that appears in a `Scan` node in the compiled plan must have a route registered. `compile_plan` ensures this when building the join dispatcher.

### `FilterOp`

**File**: `src/engine/filter.rs`

**DBSP class**: Linear operator.

**State**: None (stateless). Holds a `Predicate` and a `Box<dyn Operator>` (`next`).

**Behaviour**: Evaluates `predicate.eval(&event.record)`. If `true`, forwards the event unchanged (same record, same weight). If `false`, drops the event.

**Invariants**: The weight of a passing event is always identical to the weight of the arriving event. The `Predicate::eval` function is total — it never panics on missing columns (returns `false` for type mismatches and missing fields).

### `ProjectOp`

**File**: `src/engine/project.rs`

**DBSP class**: Linear operator.

**State**: None (stateless). Holds a `Vec<String>` (column whitelist) and `next`.

**Behaviour**: Calls `event.record.retain(|col, _| self.columns.contains(col))`, then forwards the event. Columns not in the list are dropped in place. Weight is preserved.

**Invariants**: Columns listed in `columns` that do not exist in the record are silently ignored (the `retain` call is a no-op for them). The operator does not fail on schema mismatches.

### `JoinSideOp`

**File**: `src/engine/join.rs`

**DBSP class**: Bilinear operator (one side of a symmetric hash join pair).

**State**: Shared `Rc<RefCell<JoinCore>>` containing `left_map`, `right_map`, join key strings, join kind, and `next`.

**Behaviour**: Three-phase protocol on each `on_event` — see §7 for the full protocol.

**Supports**: `JoinType::Inner`, `JoinType::Cross`, `JoinType::LeftSemi`, `JoinType::LeftAnti`.

**Invariants**:
- The `RefCell` is never double-borrowed (guaranteed by the three-phase protocol structure).
- Records with net weight 0 are pruned from the hash map after every update.
- For Cross joins, all records use `Value::Null` as the sentinel join key, ensuring every left record probes every right record.

### `BatchingOp`

**File**: `src/engine/batching.rs`

**DBSP class**: Consolidation buffer (auxiliary, not a relational operator).

**State**: `Vec<(Record, i64)>` buffer, a `flush_threshold: usize`, a `source: Arc<str>`, and `next`.

**Behaviour**: On `on_event`, searches the buffer for an existing entry with the same record (linear equality scan). If found, adds the incoming weight to the existing entry's weight. If not found, appends `(record, weight)`. When `buffer.len() >= flush_threshold`, calls `flush()`.

`flush()` drains the buffer and emits each entry with non-zero weight as an `Event` downstream.

**Throughput model**: With consolidation rate `c` (fraction of inserts cancelled by matching deletes before flush), the effective event volume entering downstream operators is reduced by a factor of `1/(1-c)`.

**Invariants**: Records with net weight 0 after consolidation are never forwarded downstream.

**Known limitation**: Uses `Vec` linear search because `Record = HashMap<String, Value>` does not implement `Hash`. For flush thresholds above ~10,000 distinct records, this becomes a bottleneck. See §11.

### `SinkOp`

**File**: `src/engine/sink.rs`

**DBSP class**: Materialized view (terminal consumer).

**State**: `label: String`. No accumulation state.

**Behaviour**: On `on_event`, sorts the record's columns alphabetically, formats each as `column=value`, and prints:

```
[OUTPUT] weight=+1  { age=Int(30), id=Int(1), name=Str("Alice") }
```

**Invariants**: Alphabetical column sort makes output deterministic regardless of `HashMap` iteration order. This is important for testing.

---

## 7. The Symmetric Hash Join

The symmetric hash join is the only stateful relational operator currently implemented. It is the engine's most complex component.

### Mathematical Basis

Standard batch join: `A ⋈ B`

Incremental (DBSP) join formula:

```
Δ(A ⋈ B) = ΔA ⋈ B_old  +  A_old ⋈ ΔB
```

Interpretation:
- When a new tuple arrives on the left (`ΔA`), it must be matched against all currently-known right tuples (`B_old`).
- When a new tuple arrives on the right (`ΔB`), it must be matched against all currently-known left tuples (`A_old`).
- The incoming delta is added to the history after matching, making `B_old` and `A_old` grow over time.

### Data Structures

```rust
// src/engine/join.rs
type JoinMap = HashMap<Value, Vec<(Arc<Record>, i64)>>;

pub(crate) struct JoinCore {
    left_key:  String,
    right_key: String,
    kind:      JoinType,
    left_map:  JoinMap,    // B_old for the left arm
    right_map: JoinMap,    // A_old for the right arm
    next:      Box<dyn Operator>,
}
```

`JoinMap` maps a join-key value to a list of `(Arc<Record>, net_weight)` pairs. The `net_weight` is an `i64` accumulator: each insert adds `+1` (or whatever the arriving weight is) and each retraction adds `-1`.

### Three-Phase Protocol

The protocol is executed in `JoinSideOp::on_event` for every arriving event.

#### Phase 1: Probe

```rust
let outputs: Vec<Event> = {
    let core = self.core.borrow();          // immutable borrow

    let (own_key, probe_map) = match self.side {
        JoinSide::Left  => (&core.left_key,  &core.right_map),
        JoinSide::Right => (&core.right_key, &core.left_map),
    };

    let join_val = event.record.get(own_key).cloned().unwrap_or(Value::Null);

    match (probe_map.get(&join_val), &core.kind) {
        (Some(matches), JoinType::Inner) => matches
            .iter()
            .filter(|(_, w)| *w != 0)
            .map(|(other, stored_w)| {
                let mut merged = event.record.clone();
                merged.extend(other.as_ref().clone());
                Event {
                    source: event.source.clone(),
                    record: merged,
                    weight: Weight::join_product(event.weight, Weight(*stored_w)),
                }
            })
            .collect(),
        // ... other join kinds
    }
};   // <-- immutable borrow released here
```

For an inner join, every stored record that matches the join key is paired with the arriving record. The merged record is the union of both records' columns. The output weight is `w_arriving × w_stored`.

For a semi-join (`LeftSemi`), only the left side emits, and it emits the left record (not merged) exactly once if at least one live match exists.

For an anti-semi-join (`LeftAnti`), the left record is emitted only when no match is found.

#### Phase 2: Insert / Update Hash Table

```rust
{
    let mut core = self.core.borrow_mut();   // mutable borrow

    let key = event.record.get(&own_key).cloned().unwrap_or(Value::Null);
    let map = match self.side {
        JoinSide::Left  => &mut core.left_map,
        JoinSide::Right => &mut core.right_map,
    };

    let bucket = map.entry(key.clone()).or_default();
    let record_arc = Arc::new(event.record);

    // Update in-place or append
    if let Some(entry) = bucket.iter_mut().find(|(r, _)| **r == *record_arc) {
        entry.1 += event.weight.0;
    } else {
        bucket.push((record_arc, event.weight.0));
    }

    // Prune zero-weight entries
    bucket.retain(|(_, w)| *w != 0);
    if bucket.is_empty() {
        map.remove(&key);
    }
}   // <-- mutable borrow released here
```

The arriving record is added to this side's hash map (Phase 2 is the update of `A_old` or `B_old`). If the exact same record was already stored (detected by value equality), the weight is updated in-place rather than creating a duplicate entry. This is the retraction mechanism: a `+1` insert followed by a `-1` delete for the same record results in net weight `0`, which is then pruned from the bucket.

Pruning zero-weight entries prevents unbounded map growth in long-running pipelines with frequent updates.

#### Phase 3: Emit

```rust
{
    let mut core = self.core.borrow_mut();   // mutable borrow
    for out in outputs {
        core.next.on_event(out);
    }
}   // <-- mutable borrow released here
```

The collected output events from Phase 1 are forwarded to the downstream operator. Using a separate mutable borrow for this phase (rather than holding the Phase 1 borrow) ensures that `core.next.on_event()` cannot accidentally re-enter the same `RefCell`.

### Join Kind Handling

| `JoinType` | Phase 1 behaviour |
|---|---|
| `Inner` | Emit merged record for every stored match with non-zero weight. Weight = `join_product(w_arriving, w_stored)`. |
| `Cross` | Same as Inner, but all records use `Value::Null` as the sentinel key so every left record matches every right record. |
| `LeftSemi` | Left side only: emit the left record (unmerged) once if any live match exists. Right side: no emission. |
| `LeftAnti` | Left side: emit the left record if no match exists. Right side: no emission. |

---

## 8. Weight Arithmetic

### Why `Weight(i64)` Is a Newtype

Raw `i64` arithmetic can silently compute `w₁ * w₂` anywhere. In DBSP, multiplying two weights together is a **dimensional error**: the result is a "weight²" quantity that has no meaning in the Z-set algebra. The only correct place to multiply two weights is in a bilinear operator (the join).

By defining `Weight` as a newtype:

```rust
// src/engine/weight.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Weight(pub i64);
```

and deliberately **not** implementing `Mul<Weight> for Weight`, any accidental `w1 * w2` expression fails at compile time with "cannot multiply `Weight` by `Weight`".

### Implemented Operations

| Operation | Trait / Method | Use case |
|---|---|---|
| `w1 + w2` | `impl Add for Weight` | Accumulating weights in a Z-set (batching, retraction tracking). |
| `-w` | `impl Neg for Weight` | Inverting a weight for retraction. |
| `w.scale(n: i64)` | Method | Scaling by a plain integer (batch sizing, not weight-times-weight). |
| `Weight::join_product(a, b)` | Static method | The **only** correct way to multiply two weights. Named verbosely to surface in code review. |
| `w.is_zero()` | Method | Check if a record can be pruned from a Z-set. |

### What Is Intentionally Absent

```rust
// src/engine/weight.rs, line 68-70
// Intentionally NO: `impl Mul<Weight> for Weight`
// Multiplying two weights (weight²) is a dimensional error in DBSP.
// Use `Weight::join_product(a, b)` instead.
```

`impl Mul<i64> for Weight` is also absent. Use `scale()` instead:

```rust
// Correct: scale by a batch factor
let batch_weight = Weight::INSERT.scale(10);   // Weight(10)

// Compile error if someone writes:
// let bad = Weight::INSERT * Weight::DELETE;   // ERROR: cannot multiply Weight by Weight
```

### Constants

```rust
pub const INSERT: Self = Weight(1);    // standard insertion
pub const DELETE: Self = Weight(-1);   // standard deletion / retraction
pub const ZERO:   Self = Weight(0);    // prunable record
```

---

## 9. Error Handling Strategy

The engine uses three typed error enums, each owned by the layer that can produce it.

### `LoweringError` — `src/lowering.rs`

Produced by `lower_select()`. Fires when the mock AST cannot be represented in the `LogicalPlan` IR.

| Variant | Condition | Example |
|---|---|---|
| `UnsupportedCondition { description }` | A `MockCond` shape has no mapping to `LogicalPlan`. | A binary op with two column references on both sides. |
| `InsideRequiresSubquery { col }` | `col IN <scalar>` — the right side of `IN` must be a subquery. | `WHERE id IN 42` |
| `AmbiguousJoinKey { outer_col }` | The subquery in `col IN (SELECT ...)` projects no columns. The join key cannot be inferred. | `WHERE id IN (SELECT FROM Products)` (no SELECT list) |
| `CrossColumnPredicateUnsupported` | A binary condition compares two columns against each other (not a column vs. literal). | `WHERE a.id = b.id` in a WHERE clause |
| `UnboundParameter { name }` | A query parameter (`$name`) appears without a binding. | `WHERE age > $min_age` without binding `$min_age`. |
| `MaxNestingDepthExceeded { max_depth }` | Recursion depth for nested subqueries exceeds a limit. | Pathological deeply-nested subqueries. |
| `EmptyFrom` | The `FROM` clause is missing a table name. | `MockSelectStmt { table: "", .. }` |

### `PlanError` — `src/physical_plan.rs`

Produced by `select_physical_plan()`. Fires when a `LogicalPlan` node has no physical implementation.

| Variant | Condition |
|---|---|
| `NoPhysicalImpl { variant }` | The logical variant is in the enum but has no corresponding physical algorithm yet. Currently fires for `Union`. |

### `BuildError` — `src/engine/builder.rs`

Produced by `compile_plan()` and `build_graph()`. Fires when the physical plan cannot be compiled into an operator DAG.

| Variant | Condition |
|---|---|
| `NoScanInSubtree { side, plan_debug }` | A join arm's sub-plan contains no `Source` node, so the dispatcher cannot determine the routing key. |
| `DuplicateTableRoute { table }` | Two join arms resolve to the same table name, creating an ambiguous dispatcher entry. |
| `UnsupportedPlanNode { variant }` | A `PhysicalPlan` variant has no operator implementation yet. Fires for `Distinct`, `HashAggregate`, `Sort`. |

### Error Propagation Pattern

All three error types implement `thiserror::Error` and produce human-readable messages. The chain from `main.rs`:

```rust
let _physical = match select_physical_plan(plan.clone()) {
    Ok(p)  => p,
    Err(e) => { eprintln!("Plan error: {e}"); return; }
};

let mut graph = match build_graph(plan, sink) {
    Ok(g)  => g,
    Err(e) => { eprintln!("Build error: {e}"); return; }
};
```

Errors are handled at the top of each demo function. There are no `.unwrap()` calls in production pipeline paths.

---

## 10. Data Flow

This section traces a single `Event` from entry to output through the full compiled DAG for the query in `demo_engine()`:

```sql
SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 18
```

The compiled DAG structure for this query:

```
DispatcherOp {
  "users"  → FilterOp(age > 18)
                 → JoinSideOp(Left,  core)
  "orders" → JoinSideOp(Right, core)
}

core = JoinCore {
  left_key:  "id",
  right_key: "user_id",
  kind:      Inner,
  left_map:  {},     // grows as users events arrive
  right_map: {},     // grows as orders events arrive
  next:      SinkOp("OUTPUT"),
}
```

### Event: INSERT users Alice (id=1, age=30)

1. `graph.on_event(Event { source: "users", record: {id:1, name:"Alice", age:30}, weight: +1 })`.
2. `DispatcherOp.on_event`: looks up `"users"` → routes to `FilterOp`.
3. `FilterOp.on_event`: evaluates `age > 18` → `30 > 18` → `true`. Forwards event unchanged.
4. `JoinSideOp(Left).on_event`:
   - **Phase 1**: borrows `core`. `probe_map = core.right_map`. `join_val = Value::Int(1)`. `right_map.get(Int(1))` → `None`. No output.
   - **Phase 2**: borrows `core` mutably. Key = `Int(1)`. Appends `(Arc({id:1, name:"Alice", age:30}), +1)` to `left_map[Int(1)]`.
   - **Phase 3**: no outputs to emit.
5. `left_map = { Int(1): [(Arc(Alice), 1)] }`. `right_map = {}`. No output to sink.

### Event: INSERT users Bob (id=2, age=15) — filtered

1. `DispatcherOp` routes to `FilterOp`.
2. `FilterOp`: evaluates `age > 18` → `15 > 18` → `false`. **Event dropped.** Nothing reaches `JoinSideOp`. `left_map` unchanged.

### Event: INSERT orders #100 (user_id=1, item=Book)

1. `DispatcherOp` routes to `JoinSideOp(Right)` (no filter on the orders pipeline).
2. `JoinSideOp(Right).on_event`:
   - **Phase 1**: borrows `core`. `probe_map = core.left_map`. `join_val = Value::Int(1)`. `left_map.get(Int(1))` → `[(Arc(Alice), 1)]`. Inner join matches:
     - merged = `{order_id:100, user_id:1, item:"Book", id:1, name:"Alice", age:30}`.
     - weight = `join_product(+1, Weight(+1))` = `+1`.
     - Output: `Event { record: merged, weight: +1 }`.
   - **Phase 2**: borrows `core` mutably. Appends `(Arc({order_id:100, user_id:1, item:"Book"}), +1)` to `right_map[Int(1)]`.
   - **Phase 3**: borrows `core` mutably. Calls `next.on_event(merged_event)`.
3. `SinkOp.on_event`: prints:
   ```
   [OUTPUT] weight=+1  { age=Int(30), id=Int(1), item=Str("Book"), name=Str("Alice"), order_id=Int(100), user_id=Int(1) }
   ```

### Event: DELETE orders #100 (user_id=1, item=Book)

1. Routes to `JoinSideOp(Right)`, weight = `-1`.
2. **Phase 1**: `left_map.get(Int(1))` → Alice. Merged with weight `join_product(-1, +1)` = `-1`. Output retraction event.
3. **Phase 2**: finds `(Arc(order_id:100...), 1)` in `right_map[Int(1)]`. Updates weight: `1 + (-1) = 0`. Prunes the entry (weight == 0). Bucket is empty; removes `Int(1)` from `right_map`.
4. **Phase 3**: calls `SinkOp.on_event` with weight `-1`:
   ```
   [OUTPUT] weight=-1  { age=Int(30), id=Int(1), item=Str("Book"), name=Str("Alice"), order_id=Int(100), user_id=Int(1) }
   ```

This is the correct DBSP retraction: the downstream view is updated to remove the previously-joined row.

---

## 11. Technical Debt and Known Limitations

### TD-1: `real_lowering.rs` Is a Stub

**File**: `src/real_lowering.rs`

**Status**: `lower_real_select_stub()` always returns `Err(LoweringError::UnsupportedCondition { ... })`.

**Root cause**: `surrealdb-core 3.0.0` declares all SQL AST types (`SelectStatement`, `Cond`, `Field`, `Idiom`, `Operator`, etc.) as `pub(crate)`. External crates cannot reference these types, so there is no valid Rust signature for a function that accepts a parsed statement.

**Migration options** (documented in the module):
1. Switch to `surrealdb = { version = "3", features = ["parser"] }` — the public client crate exposes AST types via feature flags.
2. Fork `surrealdb-core` and add `pub use` re-exports for the needed types.
3. Wait for the SurrealDB team to expose the AST in a future release.

### TD-2: `Distinct`, `HashAggregate`, `Sort` Return `BuildError::UnsupportedPlanNode`

**File**: `src/engine/builder.rs`

These `PhysicalPlan` variants are recognised by `compile_plan` but have no operator implementation. Any plan containing them fails at DAG compilation time:

```rust
PhysicalPlan::Distinct { .. }      => Err(BuildError::UnsupportedPlanNode { variant: "Distinct" }),
PhysicalPlan::HashAggregate { .. } => Err(BuildError::UnsupportedPlanNode { variant: "HashAggregate" }),
PhysicalPlan::Sort { .. }          => Err(BuildError::UnsupportedPlanNode { variant: "Sort" }),
```

### TD-3: `Union` Returns `PlanError::NoPhysicalImpl`

**File**: `src/physical_plan.rs`

`LogicalPlan::Union` has no physical algorithm. Plans involving `UNION` fail at the physical planning stage:

```rust
LogicalPlan::Union { .. } => Err(PlanError::NoPhysicalImpl { variant: "Union" }),
```

Union in DBSP is trivially implemented as weight addition (Z-sets are additive). This is a straightforward addition once operator infrastructure is in place.

### TD-4: `LeftAnti` Emits No Retractions When the Right Side Grows

**File**: `src/engine/join.rs`

The current `LeftAnti` implementation emits a left record when the right side has no match. However, if a matching right record later arrives, the anti-join should **retract** the previously-emitted left record. This retraction is not emitted.

The Phase 1 code for `LeftAnti` on the right side simply returns an empty vec:

```rust
(Some(_), JoinType::LeftAnti) => vec![],
```

A correct implementation would need to look up the left side's accumulated records and emit retractions for each previously-emitted left record that is now matched.

This is a known correctness gap for `NOT IN` semantics in a long-running streaming scenario.

### TD-5: Predicate Pushdown Only Handles Exact Key-Column Matches

**File**: `src/optimizer.rs`

The `push_filters_below_joins` rule determines whether a predicate can be pushed by checking if all predicate columns exactly match the join key column of one side:

```rust
let pushable_left = pred_cols.iter().any(|c| *c == left_key)
    && pred_cols.iter().all(|c| *c == left_key);
```

This misses cases like: a filter on a non-key column that still only references one join side. A correct implementation requires schema metadata (the set of columns available from each sub-plan) rather than just comparing to the join key.

### TD-6: No Benchmark Suite

There is no `benches/` directory. Criterion benchmarks for the join operator (varying input size, key cardinality, retraction rate) and the batching operator (varying flush threshold and consolidation rate) do not exist.

### TD-7: `BatchingOp` Uses `Vec` Linear Search

**File**: `src/engine/batching.rs`

```rust
if let Some(entry) = self.buffer.iter_mut().find(|(r, _)| r == &event.record) {
```

Because `Record = HashMap<String, Value>` does not implement `Hash`, the buffer cannot use a `HashMap` for O(1) lookup. The linear scan is O(N) per event where N is the number of distinct buffered records. For flush thresholds above approximately 10,000 records this becomes a bottleneck.

A fix requires either: (a) a canonical serialization of `Record` to use as a hash key, or (b) a custom `Hash` implementation over sorted `(String, Value)` pairs.

### TD-8: `prune_unused_columns` Is a No-Op

**File**: `src/optimizer.rs`

```rust
pub fn prune_unused_columns(plan: LogicalPlan) -> LogicalPlan {
    // Placeholder: return the plan unchanged.
    plan
}
```

Full projection pushdown requires tracking which columns are needed by each operator (top-down analysis). This is a medium-complexity optimization that matters when wide tables (many columns) are joined.

---

## 12. Getting Started (Contributor View)

### What to Read First

1. `src/plan.rs` — understand `Value`, `Predicate`, `JoinType`, and `LogicalPlan`. These types are the vocabulary used everywhere.
2. `src/engine/mod.rs` — understand `Record`, `Event`, `Weight`, and the `Operator` trait. These are the runtime primitives.
3. `src/engine/weight.rs` — understand the `Weight` newtype and why `Mul` is absent before touching any arithmetic.
4. `src/engine/join.rs` — the most complex file; understand the three-phase protocol before modifying join behaviour.
5. `src/engine/builder.rs` — understand `compile_plan()` before adding a new operator.
6. `src/main.rs` — the three demos show the full pipeline end-to-end.

### How to Run

```bash
cargo run
```

The binary executes three demo functions in sequence and prints their output to stdout. No arguments are required. No external database is needed.

Expected output sections:

```
╔══════════════════════════════════════════════════════════╗
║  Demo 1 – DBSP Push-based Engine                        ║
╚══════════════════════════════════════════════════════════╝
...
╔══════════════════════════════════════════════════════════╗
║  Demo 2 – AST Lowering (decorrelation + AND/OR)         ║
╚══════════════════════════════════════════════════════════╝
...
╔══════════════════════════════════════════════════════════╗
║  Demo 3 – Real SurrealDB parser output                  ║
╚══════════════════════════════════════════════════════════╝
```

### How to Add a New Physical Operator

Use `DistinctOp` as a worked example. Follow these steps:

**Step 1**: Create `src/engine/distinct.rs`.

```rust
use std::collections::HashMap;
use super::{Event, Operator, Record, Weight};

/// Stateful DBSP distinct operator.
/// Maintains a count map; emits +1 when count goes 0→1, emits -1 when count goes 1→0.
pub struct DistinctOp {
    counts: HashMap<Record, i64>,
    next: Box<dyn Operator>,
}
```

Implement `Operator for DistinctOp`. Note: `Record = HashMap<String, Value>` does not implement `Hash`, so you will need a canonical key (see TD-7 for context — the same issue applies here).

**Step 2**: Register the module in `src/engine/mod.rs`.

```rust
pub mod distinct;
```

**Step 3**: Handle the variant in `src/engine/builder.rs`.

Replace:
```rust
PhysicalPlan::Distinct { .. } => Err(BuildError::UnsupportedPlanNode { variant: "Distinct" }),
```

With:
```rust
PhysicalPlan::Distinct { input } => {
    let distinct_op = Box::new(DistinctOp::new(next));
    compile_plan(*input, distinct_op)
}
```

**Step 4**: Verify the `PhysicalPlan::Distinct` variant in `src/physical_plan.rs` already maps from `LogicalPlan::Distinct` (it does — `select_physical_plan` already handles it).

**Step 5**: Add a test case in `src/main.rs` or a new integration test that exercises the distinct operator with insert/delete events and verifies the retraction semantics.

### How to Add a New Optimization Rule

1. Write a pure function `fn my_rule(plan: LogicalPlan) -> LogicalPlan` in `src/optimizer.rs`.
2. The function must be a structural transformation that returns a plan `PartialEq`-equal to its input when no rewrite applies (this is the fixed-point termination condition).
3. Add it to the `rules` slice in `optimize()`.
4. Add a unit test that constructs a `LogicalPlan` where the rule fires and asserts the output matches the expected rewritten plan.

### How to Migrate to the Real SurrealDB AST

See TD-1 and `src/real_lowering.rs` module documentation. The recommended path is:

Update `Cargo.toml`:
```toml
surrealdb = { version = "3", features = ["parser"] }
```

Then implement `lower_real_select(stmt: surrealdb::sql::SelectStatement) -> Result<LogicalPlan, LoweringError>` following the same pattern as `lower_select` in `src/lowering.rs`. The module documentation in `src/real_lowering.rs` includes the expected AST field names based on Debug output from `demo_real_parser()`.

### Dependencies

| Crate | Version | Purpose |
|---|---|---|
| `surrealdb-core` | 3.0.0 | SQL parser (`syn::parse`). AST types are `pub(crate)`. |
| `serde` | 1.0.228 | Derive macros for serialization (CBOR integration). |
| `cbor4ii` | 1.2.2 | CBOR serialization/deserialization (future wire format for events). |
| `thiserror` | 2 | Derive macros for typed error enums. |
| `ordered-float` | 4 | `OrderedFloat<f64>` wrapper providing `Hash + Eq` for `Value::Float`. |

All dependencies are open-source with permissive licenses.

---

*This document was generated from a complete read of all source files in the `spooky_streaming_processor` crate. It reflects the codebase as of 2026-02-28.*
