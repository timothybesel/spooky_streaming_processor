# spooky_streaming_processor

Recomputing a full query result on every database change is expensive. This crate
implements **DBSP (Database Stream Processing)** — a mathematical framework for
incremental view maintenance in which only the *delta* caused by each insert or
delete is recomputed, never the full result set.

## What Is DBSP?

DBSP is a formal algebra for streaming computations over **Z-sets**: multisets
whose elements carry integer *weights*. A weight of `+1` means a record is
present; `-1` is a retraction (deletion). Intermediate weights arise when join
outputs are themselves retracted.

The core guarantee: every relational operator (filter, join, project, aggregate)
has an *incremental* form that accepts a delta stream and produces a delta stream.
When a single row is inserted into one side of a join, only the rows that actually
match need to be emitted — not a re-scan of the whole relation.

| Traditional query evaluation | DBSP incremental evaluation |
|---|---|
| Re-execute `SELECT ...` on every change | Maintain state; process only the delta |
| Join is O(N x M) per event | Join is O(matches) per delta event |
| DELETE requires identifying the old result | Retract with `weight = -1`; algebra cancels |

Reference: Frank McSherry et al., *"DBSP: Automatic Incremental View Maintenance
for Rich Query Languages"* (VLDB 2023).

## The Pipeline

```
MockSelectStmt          (mock SurrealDB AST — real types are pub(crate) in v3)
    |
    | lower_select()    src/lowering.rs
    v
LogicalPlan             (algebraic IR: Scan, Filter, Join, Project, ...)
    |
    | optimize()        src/optimizer.rs  — predicate pushdown, project elimination
    v
LogicalPlan (optimized)
    |
    | select_physical_plan()   src/physical_plan.rs — algorithm selection
    v
PhysicalPlan            (SymmetricHashJoin, HashAggregate, Sort, ...)
    |
    | compile_plan() / build_graph()   src/engine/builder.rs
    v
Box<dyn Operator>       (push-based DAG of FilterOp, JoinSideOp, ProjectOp, ...)
    |
    | on_event(Event)   — one call per streaming delta
    v
SinkOp                  (prints weight-annotated rows to stdout)
```

Each layer has a single responsibility:

| Layer | File | Responsibility |
|---|---|---|
| Lowering | `src/lowering.rs` | SQL/AST to `LogicalPlan`; decorrelates subqueries |
| Logical IR | `src/plan.rs` | Algebraic query tree |
| Optimizer | `src/optimizer.rs` | Algebraic rewrites (predicate pushdown, ...) |
| Physical planner | `src/physical_plan.rs` | Algorithm selection |
| Engine builder | `src/engine/builder.rs` | Compiles physical plan to operator DAG |
| Engine operators | `src/engine/` | Push-based execution; weight algebra |

## Quick Start

**Prerequisites**: Rust toolchain (edition 2024, i.e. Rust 1.85+).

```bash
git clone <repo>
cd spooky_streaming_processor
cargo run
```

`cargo run` executes three self-contained demos:

1. **Demo 1 — DBSP engine**: builds a `JOIN ... WHERE age > 18` plan manually,
   streams inserts and a retraction, and prints weight-annotated output rows.
2. **Demo 2 — AST lowering**: shows `col IN (subquery)` decorrelation into a
   `LeftSemi` join and AND/OR condition lowering.
3. **Demo 3 — Real SurrealDB parser**: parses a correlated subquery with
   `surrealdb_core::syn::parse` and prints the raw Debug AST. Inspection of
   parsed types from external crates is not yet possible — see
   [Current Limitations](#current-limitations).

### Trust and constraints

- This crate modifies **no files and no system state**. All computation is
  in-memory.
- The engine is **single-threaded by design** (`Rc<RefCell>` in the join
  operator). Do not share a compiled graph across threads; the type system
  enforces this.
- `cargo run` prints to stdout only; no network, no disk I/O.

## Usage

### Building a Plan Manually

Construct a `LogicalPlan` directly and compile it to an operator DAG:

```rust
use spooky_streaming_processor::engine::{Event, Weight, builder::build_graph, sink::SinkOp};
use spooky_streaming_processor::plan::{JoinType, LogicalPlan, Predicate, Value};
use std::sync::Arc;

// SELECT * FROM users JOIN orders ON users.id = orders.user_id
// WHERE users.age > 18
let plan = LogicalPlan::Join {
    left: Box::new(LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan { table: "users".into() }),
        predicate: Predicate::Gt("age".into(), Value::Int(18)),
    }),
    right: Box::new(LogicalPlan::Scan { table: "orders".into() }),
    on: ("id".into(), "user_id".into()),
    kind: JoinType::Inner,
};

let sink = Box::new(SinkOp::new("output"));
let mut graph = build_graph(plan, sink).unwrap();
```

`build_graph` returns `Result<Box<dyn Operator>, BuildError>`. Errors are
explicit — there are no silent fallbacks.

### Using the Full Pipeline

For new code, go through the optimizer and physical planner explicitly:

```rust
use spooky_streaming_processor::optimizer::optimize;
use spooky_streaming_processor::physical_plan::select_physical_plan;
use spooky_streaming_processor::engine::builder::compile_plan;
use spooky_streaming_processor::engine::sink::SinkOp;
use spooky_streaming_processor::plan::{LogicalPlan, Predicate, Value};

let plan = LogicalPlan::Filter {
    input: Box::new(LogicalPlan::Scan { table: "events".into() }),
    predicate: Predicate::Gt("score".into(), Value::Int(100)),
};

let optimized = optimize(plan);
let physical  = select_physical_plan(optimized).unwrap();
let mut graph = compile_plan(physical, Box::new(SinkOp::new("out"))).unwrap();
```

### Lowering from a Mock AST

`lower_select` converts a `MockSelectStmt` to a `LogicalPlan`.
`col IN (subquery)` is decorrelated into a `LeftSemi` join automatically:

```rust
use spooky_streaming_processor::lowering::{
    lower_select, LoweringError, MockCond, MockExpr, MockOp, MockSelectStmt,
};
use spooky_streaming_processor::plan::Value;

// SELECT SupplierName FROM Suppliers
// WHERE SupplierID IN (SELECT SupplierID FROM Products WHERE Price < 20)
let ast = MockSelectStmt {
    table: "Suppliers".into(),
    select_fields: vec!["SupplierName".into()],
    cond: Some(MockCond::BinaryOp {
        op: MockOp::Inside,
        left: MockExpr::Column("SupplierID".into()),
        right: MockExpr::Subquery(Box::new(MockSelectStmt {
            table: "Products".into(),
            select_fields: vec!["SupplierID".into()],
            cond: Some(MockCond::BinaryOp {
                op: MockOp::Lt,
                left: MockExpr::Column("Price".into()),
                right: MockExpr::Literal(Value::Int(20)),
            }),
        })),
    }),
};

match lower_select(ast) {
    Ok(plan) => println!("{plan:#?}"),
    Err(e)   => eprintln!("lowering error: {e}"),
}
// -> LogicalPlan::Join { kind: LeftSemi, ... }
```

Unrecognised conditions return an explicit error rather than silently falling
back to a full-table scan:

```rust
// This returns Err(LoweringError::InsideRequiresSubquery), not a wrong plan.
let bad = MockSelectStmt {
    table: "users".into(),
    select_fields: vec![],
    cond: Some(MockCond::BinaryOp {
        op: MockOp::Inside,
        left: MockExpr::Column("id".into()),
        right: MockExpr::Literal(Value::Int(42)),  // scalar, not subquery
    }),
};
assert!(matches!(
    lower_select(bad),
    Err(LoweringError::InsideRequiresSubquery { .. })
));
```

### Streaming Events

Push `Event` values into the compiled DAG one at a time. The `source` field
routes each event to the correct pipeline arm inside `DispatcherOp`:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use spooky_streaming_processor::engine::{Event, Weight};
use spooky_streaming_processor::plan::Value;

let rec = |pairs: &[(&str, Value)]| -> HashMap<String, Value> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
};

// INSERT users: Alice (id=1, age=30)
graph.on_event(Event {
    source: Arc::from("users"),
    record: rec(&[
        ("id",   Value::Int(1)),
        ("name", Value::Str("Alice".into())),
        ("age",  Value::Int(30)),
    ]),
    weight: Weight::INSERT,  // +1
});

// INSERT users: Bob (id=2, age=15) — filtered by age > 18, produces no output
graph.on_event(Event {
    source: Arc::from("users"),
    record: rec(&[
        ("id",   Value::Int(2)),
        ("name", Value::Str("Bob".into())),
        ("age",  Value::Int(15)),
    ]),
    weight: Weight::INSERT,
});

// INSERT orders: #100 (user_id=1) — matches Alice; emits a joined row
graph.on_event(Event {
    source: Arc::from("orders"),
    record: rec(&[
        ("order_id", Value::Int(100)),
        ("user_id",  Value::Int(1)),
        ("item",     Value::Str("Book".into())),
    ]),
    weight: Weight::INSERT,
});
// SinkOp prints:
// [output] weight=+1  { age=Int(30), id=Int(1), item=Str("Book"), name=Str("Alice"), order_id=Int(100), user_id=Int(1) }
```

Both join sides may arrive in any order. Inserting Alice before her orders or
after them produces the same final result — the Symmetric Hash Join maintains
state on both sides.

### Retractions

Send the same record with `Weight::DELETE` (`-1`) to retract a prior result.
The join's bilinear weight product propagates the retraction through the entire
downstream DAG:

```rust
// DELETE order #100 — retracts the Alice x Book join result
graph.on_event(Event {
    source: Arc::from("orders"),
    record: rec(&[
        ("order_id", Value::Int(100)),
        ("user_id",  Value::Int(1)),
        ("item",     Value::Str("Book".into())),
    ]),
    weight: Weight::DELETE,  // -1
});
// SinkOp prints:
// [output] weight=-1  { age=Int(30), id=Int(1), item=Str("Book"), name=Str("Alice"), order_id=Int(100), user_id=Int(1) }
```

The negative-weight event cancels the prior positive-weight event at any
downstream consumer that accumulates weights. Records with net weight 0 are
pruned from the join's internal hash tables automatically.

### Batching

For high-retraction workloads (e.g. CDC streams with frequent updates), insert
a `BatchingOp` before an expensive join to consolidate inserts and deletes for
the same record before they are probed:

```rust
use spooky_streaming_processor::engine::batching::BatchingOp;
use std::sync::Arc;

// Flush every 1 000 distinct records; cancelling insert/delete pairs
// are pruned before they ever reach the join probe.
let batched_sink = Box::new(BatchingOp::new(
    Arc::from("orders"),
    1_000,
    Box::new(SinkOp::new("out")),
));
```

At a retraction rate `c`, `BatchingOp` reduces join probe work by a factor of
`1 / (1 - c)`: 50% retraction rate gives a 2x throughput gain; 90% gives 10x.
Call `flush()` manually at end-of-batch to drain any remaining buffered events.

## Supported Operators

### Logical IR (`LogicalPlan`, `src/plan.rs`)

| Variant | Description |
|---|---|
| `Scan { table }` | Read all records from a named base relation |
| `Filter { predicate }` | Discard records not matching a `Predicate` |
| `Project { columns }` | Retain only the named columns |
| `Join { on, kind }` | Binary equi-join — Inner, LeftSemi, LeftAnti, Cross |
| `Union` | Set union with bag semantics (weights add) |
| `Distinct` | Deduplicate; retain entries with net weight != 0 |
| `Aggregate { group_by, agg }` | Hash aggregate (Count, Sum, Min, Max, Avg) |
| `Sort { keys }` | Sort by (column, direction) list |
| `Limit { count, offset }` | Merged into an enclosing `Sort` during planning |

### Predicate variants (`Predicate`, `src/plan.rs`)

`Gt`, `Lt`, `Eq`, `Ne`, `Ge`, `Le`, `EqCol` (cross-column equality),
`InList`, `NotInList`, `And`, `Or`, `Not`.

All variants are exhaustively matched in `Predicate::eval`. Adding a new variant
without a corresponding arm in `eval` is a compile error, not a silent bug.

### Physical algorithms (`PhysicalPlan`, `src/physical_plan.rs`)

| Physical variant | Implements |
|---|---|
| `Source` | Base table read (data arrives via `on_event`) |
| `Filter` | Stateless row predicate (`FilterOp`) |
| `Project` | Column pruning (`ProjectOp`) |
| `SymmetricHashJoin` | Incremental equi-join; both sides may arrive in any order |
| `Distinct` | Logical node — not yet compiled (see [Current Limitations](#current-limitations)) |
| `HashAggregate` | Logical node — not yet compiled |
| `Sort` | Logical node — not yet compiled |

### Optimizer rules (`src/optimizer.rs`)

The optimizer runs all rules in a fixed-point loop until no rule changes the
plan. No statistics are required; all rules are correctness-preserving by
construction.

| Rule | Effect |
|---|---|
| Predicate pushdown | `Filter(Join(A, B), p)` becomes `Join(Filter(A, p), B)` when `p` references only columns from `A`, reducing join input size |
| Trivial project elimination | Removes `Project` nodes with an empty column list (`SELECT *`) |
| Projection pushdown | Placeholder — currently a no-op (see [Current Limitations](#current-limitations)) |

## Current Limitations

These are known, documented gaps — not silent failures. Every unsupported path
returns an explicit typed error.

| Limitation | Detail |
|---|---|
| `surrealdb-core` 3.0.0 AST types are `pub(crate)` | `SelectStatement`, `Cond`, `Field`, and all other SQL types cannot be referenced from external crates. The parsed AST is Debug-printable only. See `src/real_lowering.rs` for three migration paths. |
| `Distinct`, `HashAggregate`, `Sort` not compiled | `compile_plan` returns `BuildError::UnsupportedPlanNode` for these variants. The `LogicalPlan` and `PhysicalPlan` nodes exist; only the physical operator implementations are missing. |
| `Union` not implemented | Both `select_physical_plan` and `build_graph` return an error for `LogicalPlan::Union`. |
| Correlated subqueries not supported | `$parent.field` references produce `LoweringError::UnboundParameter`. Decorrelation handles only the `col IN (SELECT ...)` pattern. |
| Single-threaded only | `JoinCore` is wrapped in `Rc<RefCell<...>>`, not `Arc<Mutex<...>>`. The `Operator` trait does not require `Send`. Adding multi-threading requires replacing `Rc<RefCell>` throughout `src/engine/` and adding `+ Send + Sync` bounds to `Operator`. The comment in `src/engine/join.rs` documents exactly what to change. |
| No persistence | All join state and buffered events are held in heap memory. There is no WAL, snapshot, or recovery mechanism. |
| Projection pushdown is a no-op | `prune_unused_columns` in `src/optimizer.rs` returns the plan unchanged. The placeholder is correct; it provides no column-width reduction yet. |

## Architecture

The codebase is a single binary crate (also usable as a library). Each module
boundary is enforced by a `Result<…, ExplicitError>` — there are no panics on
invalid input, no `unwrap` on user data, and no `_ => unreachable!()` in
predicate evaluation.

```
src/
  main.rs              — three runnable demos (demo_engine, demo_lowering, demo_real_parser)
  plan.rs              — Value, Predicate, JoinType, AggExpr, SortDir, LogicalPlan
  lowering.rs          — MockSelectStmt -> LogicalPlan; LoweringError (7 variants)
  real_lowering.rs     — stub; documents pub(crate) blocker in surrealdb-core 3.0.0
  optimizer.rs         — fixed-point rule engine; predicate pushdown; project elimination
  physical_plan.rs     — PhysicalPlan enum; select_physical_plan; PlanError
  engine/
    mod.rs             — Event, Record, Operator trait, DispatcherOp
    weight.rs          — Weight newtype; join_product; Mul<Weight,Weight> intentionally absent
    join.rs            — JoinCore (Rc<RefCell> shared state), JoinSideOp (three-phase protocol)
    filter.rs          — FilterOp (stateless)
    project.rs         — ProjectOp (column pruning, weight preserved)
    batching.rs        — BatchingOp (ZSet consolidation; throughput scaling for high retraction)
    sink.rs            — SinkOp (terminal; sorts columns; prints weight-annotated rows)
    builder.rs         — compile_plan, build_graph, find_source_table; BuildError (3 variants)
```

### Key design decisions

**No silent fallbacks.** Every error path in `lower_select`, `select_physical_plan`,
and `compile_plan` returns a typed error. An unsupported condition or plan node
never silently becomes a full-table scan.

**Weight algebra enforced at compile time.** `Mul<Weight, Weight>` is
intentionally not implemented in `src/engine/weight.rs`. The bilinear join
product `w_left * w_right` must be written as `Weight::join_product(a, b)`,
making the DBSP-algebra operation visible in code review and preventing
accidental weight squaring.

**Single-threaded by explicit choice.** `Rc<RefCell<JoinCore>>` in
`src/engine/join.rs` documents the threading model. The comment beside the
type alias `SharedJoin` describes exactly what to change for multi-threading.

**Three-phase join protocol.** Each `on_event` call in `JoinSideOp` proceeds
as: (1) probe the opposite side's hash table and collect outputs into a local
`Vec` while holding an immutable borrow, (2) update this side's hash table
under a mutable borrow, (3) emit collected outputs under a mutable borrow.
This three-phase split prevents a second `RefCell` borrow while the first is
live — a correctness requirement, not a performance choice.

**Fixed-point optimizer.** Rules run in a loop until no rule changes the plan.
This guarantees termination without requiring rules to be manually ordered or
composed.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for a deeper narrative and
[`docs/API.md`](docs/API.md) for the full public API reference.

## API Reference

### `engine::Event`

```rust
pub struct Event {
    pub source: Arc<str>,   // source table name — routes through DispatcherOp
    pub record: Record,     // HashMap<String, Value>
    pub weight: Weight,     // +1 insert, -1 delete, or join-product weight
}
```

### `engine::Weight`

```rust
pub struct Weight(pub i64);

impl Weight {
    pub const INSERT: Self;                                        // +1
    pub const DELETE: Self;                                        // -1
    pub const ZERO:   Self;                                        //  0
    pub fn scale(self, factor: i64) -> Self;                       // scalar multiply
    pub fn is_zero(self) -> bool;
    pub fn join_product(left: Weight, right: Weight) -> Weight;    // bilinear product
}
// Add and Neg are implemented.
// Mul<Weight, Weight> is intentionally absent — weight^2 is a DBSP type error.
```

### `engine::Operator`

```rust
pub trait Operator {
    fn on_event(&mut self, event: Event);
}
```

Implement this trait to add a new physical operator. Wrap `Box<dyn Operator>`
as the `next` field and forward processed events to it.

### `engine::builder::build_graph`

```rust
pub fn build_graph(
    plan: LogicalPlan,
    next: Box<dyn Operator>,
) -> Result<Box<dyn Operator>, BuildError>
```

Convenience shim: converts a `LogicalPlan` directly to a push-based operator
DAG without an explicit physical planning step. Returns a `DispatcherOp` when
the plan contains a join. Prefer `select_physical_plan` + `compile_plan` for
new code.

### `engine::builder::compile_plan`

```rust
pub fn compile_plan(
    plan: PhysicalPlan,
    next: Box<dyn Operator>,
) -> Result<Box<dyn Operator>, BuildError>
```

Compiles a `PhysicalPlan` bottom-up into a push-based operator DAG. Returns a
`DispatcherOp` (keyed by source-table name) when a join is present.

### `engine::builder::BuildError`

```rust
pub enum BuildError {
    NoScanInSubtree { side: &'static str, plan_debug: String },
    DuplicateTableRoute { table: String },
    UnsupportedPlanNode { variant: &'static str },
}
```

### `lowering::lower_select`

```rust
pub fn lower_select(stmt: MockSelectStmt) -> Result<LogicalPlan, LoweringError>
```

Converts a mock SurrealDB AST to a `LogicalPlan`. `col IN (subquery)` is
decorrelated to `LogicalPlan::Join { kind: JoinType::LeftSemi }`.

### `lowering::LoweringError`

```rust
pub enum LoweringError {
    UnsupportedCondition { description: String },
    InsideRequiresSubquery { col: String },
    AmbiguousJoinKey { outer_col: String },
    CrossColumnPredicateUnsupported { left_col, op, right_col: String },
    UnboundParameter { name: String },
    MaxNestingDepthExceeded { max_depth: usize },
    EmptyFrom,
}
```

### `optimizer::optimize`

```rust
pub fn optimize(plan: LogicalPlan) -> LogicalPlan
```

Applies all algebraic rewrite rules to a fixed point. Currently applies
predicate pushdown, trivial project elimination, and a projection pushdown
placeholder.

### `physical_plan::select_physical_plan`

```rust
pub fn select_physical_plan(logical: LogicalPlan) -> Result<PhysicalPlan, PlanError>
```

Selects a physical algorithm for each logical operator. `Union` returns
`PlanError::NoPhysicalImpl`.

### `engine::sink::SinkOp`

```rust
pub struct SinkOp { ... }
impl SinkOp {
    pub fn new(label: impl Into<String>) -> Self;
}
```

Terminal operator. Prints each event to stdout as:

```
[label] weight=+1  { col=Value, ... }
```

Columns are sorted alphabetically for deterministic output.

### `engine::batching::BatchingOp`

```rust
pub struct BatchingOp { ... }
impl BatchingOp {
    pub fn new(source: Arc<str>, flush_threshold: usize, next: Box<dyn Operator>) -> Self;
    pub fn flush(&mut self);
}
```

Accumulates events into a ZSet buffer. Cancelling insert/delete pairs for the
same record are consolidated (net weight = 0, then pruned) before flushing.
`flush()` must be called manually at end-of-batch to drain remaining buffered
events.

## License

This project does not yet have a LICENSE file. The absence of a license means
the code is not available for use, modification, or distribution under any open
source terms. Add a `LICENSE` file (MIT, Apache-2.0, or similar) before
publishing or sharing the crate.
