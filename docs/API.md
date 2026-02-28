# spooky_streaming_processor — API Reference

**Version**: 0.1.0
**Edition**: Rust 2024
**Type**: Incremental streaming query engine (DBSP implementation)
**Audience**: Engineers embedding this engine or extending it with new operators

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Module: `plan` — Logical IR Types](#module-plan--logical-ir-types)
   - [`enum Value`](#enum-value)
   - [`type Record`](#type-record)
   - [`enum AggExpr`](#enum-aggexpr)
   - [`enum SortDir`](#enum-sortdir)
   - [`enum Predicate`](#enum-predicate)
   - [`enum JoinType`](#enum-jointype)
   - [`enum LogicalPlan`](#enum-logicalplan)
3. [Module: `engine` — Core Streaming Runtime](#module-engine--core-streaming-runtime)
   - [`struct Event`](#struct-event)
   - [`trait Operator`](#trait-operator)
   - [`struct DispatcherOp`](#struct-dispatcherop)
4. [Module: `engine::weight` — Weight Newtype](#module-engineweight--weight-newtype)
   - [`struct Weight`](#struct-weight)
5. [Module: `engine::filter` — Filter Operator](#module-enginefilter--filter-operator)
   - [`struct FilterOp`](#struct-filterop)
6. [Module: `engine::project` — Projection Operator](#module-engineproject--projection-operator)
   - [`struct ProjectOp`](#struct-projectop)
7. [Module: `engine::sink` — Sink Operator](#module-enginesink--sink-operator)
   - [`struct SinkOp`](#struct-sinkop)
8. [Module: `engine::batching` — Batching Operator](#module-enginebatching--batching-operator)
   - [`struct BatchingOp`](#struct-batchingop)
9. [Module: `engine::join` — Symmetric Hash Join](#module-enginejoin--symmetric-hash-join)
   - [`struct JoinSideOp`](#struct-joinsideop)
10. [Module: `lowering` — Mock AST to Logical Plan](#module-lowering--mock-ast-to-logical-plan)
    - [`enum LoweringError`](#enum-loweringerror)
    - [`struct MockSelectStmt`](#struct-mockselectstmt)
    - [`enum MockCond`](#enum-mockcond)
    - [`enum MockOp`](#enum-mockop)
    - [`enum MockExpr`](#enum-mockexpr)
    - [`fn lower_select`](#fn-lower_select)
11. [Module: `optimizer` — Optimization Passes](#module-optimizer--optimization-passes)
    - [`fn optimize`](#fn-optimize)
    - [`fn push_filters_below_joins`](#fn-push_filters_below_joins)
    - [`fn eliminate_trivial_projects`](#fn-eliminate_trivial_projects)
    - [`fn prune_unused_columns`](#fn-prune_unused_columns)
12. [Module: `physical_plan` — Physical Plan Selection](#module-physical_plan--physical-plan-selection)
    - [`enum PlanError`](#enum-planerror)
    - [`enum PhysicalPlan`](#enum-physicalplan)
    - [`fn select_physical_plan`](#fn-select_physical_plan)
13. [Module: `engine::builder` — DAG Compilation](#module-enginebuilder--dag-compilation)
    - [`enum BuildError`](#enum-builderror)
    - [`fn compile_plan`](#fn-compile_plan)
    - [`fn build_graph`](#fn-build_graph)
14. [Module: `real_lowering` — Stub](#module-real_lowering--stub)
    - [`fn lower_real_select_stub`](#fn-lower_real_select_stub)
15. [Error Reference](#error-reference)
16. [End-to-End Pipeline Example](#end-to-end-pipeline-example)

---

## Architecture Overview

The engine is a push-based, incremental query processor modeled on the DBSP (Database Stream Processing) algebraic framework. The query pipeline has four layers:

```
SQL text / mock AST
        │
        ▼ lowering.rs
  LogicalPlan  (plan.rs)
        │
        ▼ optimizer.rs  (fixed-point rule application)
  LogicalPlan  (optimized)
        │
        ▼ physical_plan.rs
  PhysicalPlan (algorithm selection)
        │
        ▼ engine/builder.rs
  Box<dyn Operator>  (push-based DAG)
        │
        ▼ caller pushes Event structs
  stdout / downstream consumer
```

Events carry a **weight** (`Weight`, a signed `i64`): `+1` means insertion, `-1` means deletion (retraction). The weight algebra ensures that batched updates remain correct under Z-set semantics.

**Threading model**: The engine is deliberately single-threaded. `JoinCore` uses `Rc<RefCell<...>>` and is not `Send`. Do not add `unsafe impl Send` to any engine type.

---

## Module: `plan` — Logical IR Types

Source: `src/plan.rs`

This module defines the algebraic intermediate representation (IR) shared by the lowering pass, the optimizer, and the physical plan selector. All types implement `Debug + Clone + PartialEq`; `Clone + PartialEq` is required by the optimizer's fixed-point loop.

---

### `enum Value`

Scalar value type used across records, predicates, and the mock AST.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Int(i64),
    Float(OrderedFloat<f64>),
    Str(String),
    Bool(bool),
    Null,
}
```

#### Variants

| Variant | Inner type | Description |
|---|---|---|
| `Int(i64)` | `i64` | 64-bit signed integer. |
| `Float(OrderedFloat<f64>)` | `OrderedFloat<f64>` | IEEE-754 double wrapped in `ordered_float::OrderedFloat` to satisfy `Hash + Eq`. Use this for any numeric value that originates as an `f64` (e.g., from SurrealDB). |
| `Str(String)` | `String` | UTF-8 string. |
| `Bool(bool)` | `bool` | Boolean. |
| `Null` | — | SQL NULL / absent value. |

#### Invariants

- `Float` uses `ordered_float::OrderedFloat<f64>` (crate `ordered-float = "4"`). `NaN` values compare equal to themselves via `OrderedFloat`'s total ordering. Code that constructs `Value::Float` must not rely on IEEE NaN-inequality semantics.
- `Value` implements `Hash`, so it can be used as a key in `HashMap` (used in join hash tables).

#### Example

```rust
use spooky_streaming_processor::plan::Value;
use ordered_float::OrderedFloat;

let int_val   = Value::Int(42);
let float_val = Value::Float(OrderedFloat(3.14));
let str_val   = Value::Str("hello".into());
let bool_val  = Value::Bool(true);
let null_val  = Value::Null;

// Value implements Eq, so comparison is straightforward:
assert_eq!(Value::Int(1), Value::Int(1));
assert_ne!(Value::Int(1), Value::Null);
```

---

### `type Record`

```rust
// defined in src/engine/mod.rs
pub type Record = HashMap<String, Value>;
```

A database row: a map from column name to `Value`. Used as the payload of every `Event` and as the operand of `Predicate::eval`.

#### Invariants

- Column names are case-sensitive strings.
- A missing key is treated as `Value::Null` by `Predicate::eval` for comparison operators (`Eq`, `Ne`, `Gt`, `Lt`, `Ge`, `Le`). Specifically, a missing key causes most comparisons to return `false` (consistent with SQL three-valued logic).
- `Record` does not implement `Hash`, so batching uses a `Vec<(Record, i64)>` buffer with linear equality search rather than a hash map.

---

### `enum AggExpr`

A single aggregate function used inside an `Aggregate` logical plan node.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum AggExpr {
    Count(String),
    Sum(String),
    Min(String),
    Max(String),
    Avg(String),
}
```

#### Variants

| Variant | Parameter | Semantics |
|---|---|---|
| `Count(col)` | Column name | Count of non-null values in `col`. |
| `Sum(col)` | Column name | Arithmetic sum of `col` values. |
| `Min(col)` | Column name | Minimum value in `col`. |
| `Max(col)` | Column name | Maximum value in `col`. |
| `Avg(col)` | Column name | Arithmetic mean of `col` values. |

#### Notes

`AggExpr` is part of the `LogicalPlan::Aggregate` and `PhysicalPlan::HashAggregate` nodes. Physical execution of aggregates is not yet implemented by `compile_plan`; passing a `HashAggregate` node returns `BuildError::UnsupportedPlanNode`.

---

### `enum SortDir`

Sort direction for a sort key column.

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}
```

| Variant | Meaning |
|---|---|
| `Asc` | Ascending order (smallest first). |
| `Desc` | Descending order (largest first). |

Used in `LogicalPlan::Sort { keys: Vec<(String, SortDir)> }`.

---

### `enum Predicate`

A scalar predicate evaluated against a single `Record`. All variants are exhaustively matched in `Predicate::eval`. Adding a new variant without a matching `eval` arm produces a **compile error** — there is no silent `_ => false` fallback.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Gt(String, Value),
    Lt(String, Value),
    Eq(String, Value),
    Ne(String, Value),
    Ge(String, Value),
    Le(String, Value),
    EqCol(String, String),
    InList(String, Vec<Value>),
    NotInList(String, Vec<Value>),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}
```

#### Variants

| Variant | Signature | Description |
|---|---|---|
| `Gt(col, val)` | `col > val` | True if the record's `col` field is greater than `val`. Only `Int`/`Int` and `Float`/`Float` comparisons succeed; mismatched types return `false`. |
| `Lt(col, val)` | `col < val` | True if `col < val`. Same type restrictions as `Gt`. |
| `Eq(col, val)` | `col = val` | True if the record's `col` field equals `val` (any `Value` variant, including `Null`). A missing key is treated as `Null`. |
| `Ne(col, val)` | `col != val` | True if `col` is present and not equal to `val`. A **missing** key returns `false` (SQL NULL != x is NULL, not true). |
| `Ge(col, val)` | `col >= val` | True if `col >= val`. Numeric types only. |
| `Le(col, val)` | `col <= val` | True if `col <= val`. Numeric types only. |
| `EqCol(col_a, col_b)` | `col_a = col_b` | True if both columns exist and are equal. Missing key on either side returns `false`. |
| `InList(col, values)` | `col IN [...]` | True if the record's `col` value is in `values`. Missing key returns `false`. |
| `NotInList(col, values)` | `col NOT IN [...]` | True if the record's `col` value is **not** in `values`. Missing key returns `false`. |
| `And(left, right)` | `left AND right` | Short-circuit logical AND. |
| `Or(left, right)` | `left OR right` | Short-circuit logical OR. |
| `Not(inner)` | `NOT inner` | Logical negation. |

#### `Predicate::eval`

```rust
pub fn eval(&self, record: &HashMap<String, Value>) -> bool
```

Evaluates the predicate against `record`. Returns `true` if the predicate passes.

**Parameters**

| Name | Type | Description |
|---|---|---|
| `record` | `&HashMap<String, Value>` | The row to evaluate against. |

**Return**: `true` if the predicate holds for `record`, `false` otherwise.

**Invariants**

- `Predicate` is exhaustively matched; no variant can silently fall through to `false`.
- Numeric comparisons (`Gt`, `Lt`, `Ge`, `Le`) return `false` for type mismatches (e.g., comparing `Int` with `Float`).
- `Ne` returns `false` for a missing key (SQL NULL semantics).
- `And` and `Or` short-circuit: if the left side determines the result, the right side is not evaluated.

#### Example

```rust
use std::collections::HashMap;
use spooky_streaming_processor::plan::{Predicate, Value};

let mut record = HashMap::new();
record.insert("age".into(), Value::Int(25));
record.insert("active".into(), Value::Bool(true));

// Simple comparison
let pred = Predicate::Gt("age".into(), Value::Int(18));
assert!(pred.eval(&record));

// Compound: age > 18 AND active = true
let compound = Predicate::And(
    Box::new(Predicate::Gt("age".into(), Value::Int(18))),
    Box::new(Predicate::Eq("active".into(), Value::Bool(true))),
);
assert!(compound.eval(&record));

// InList
let list_pred = Predicate::InList(
    "age".into(),
    vec![Value::Int(25), Value::Int(30)],
);
assert!(list_pred.eval(&record));

// Missing key returns false for Ne
let missing = Predicate::Ne("score".into(), Value::Int(0));
assert!(!missing.eval(&record));
```

---

### `enum JoinType`

Semantics of a binary equi-join in the logical plan.

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftSemi,
    LeftAnti,
    Cross,
}
```

#### Variants

| Variant | SQL Equivalent | Description |
|---|---|---|
| `Inner` | `JOIN ... ON` | Standard inner join. Emits one output record per matching `(left, right)` pair. |
| `LeftSemi` | `col IN (subquery)` | Semi-join. Emits the left record exactly once if at least one matching right record exists. Produced by decorrelating `col IN (subquery)` in the lowering pass. |
| `LeftAnti` | `col NOT IN (subquery)` | Anti-semi-join. Emits the left record when **no** matching right record exists. Produced by decorrelating `col NOT IN (subquery)`. |
| `Cross` | `FROM a, b` | Cartesian product. Every left record matches every right record. The join engine uses a `Value::Null` sentinel key so all records share the same hash bucket. |

---

### `enum LogicalPlan`

A node in the **logical** query plan tree — the IR between the SQL/AST surface and the physical execution engine. Optimization passes rewrite this tree; `select_physical_plan` then converts it to a `PhysicalPlan`.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    Scan { table: String },
    Filter { input: Box<LogicalPlan>, predicate: Predicate },
    Project { input: Box<LogicalPlan>, columns: Vec<String> },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: (String, String),
        kind: JoinType,
    },
    Union { left: Box<LogicalPlan>, right: Box<LogicalPlan> },
    Distinct { input: Box<LogicalPlan> },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        agg: Vec<AggExpr>,
    },
    Sort { input: Box<LogicalPlan>, keys: Vec<(String, SortDir)> },
    Limit { input: Box<LogicalPlan>, count: usize, offset: usize },
}
```

#### Variants

| Variant | Fields | Description |
|---|---|---|
| `Scan` | `table: String` | Reads all events from a named base relation. This is the leaf of every plan tree. |
| `Filter` | `input`, `predicate: Predicate` | Evaluates `predicate` against each record; non-matching records are discarded. |
| `Project` | `input`, `columns: Vec<String>` | Retains only the named columns. An empty `columns` list means `SELECT *` and is eliminated by `eliminate_trivial_projects`. |
| `Join` | `left`, `right`, `on: (String, String)`, `kind: JoinType` | Binary equi-join. `on.0` is the left column name; `on.1` is the right column name. For `Cross` joins, `on` is conventionally `("__cross__", "__cross__")`. |
| `Union` | `left`, `right` | Bag-union of two sub-plans (weights add). **No physical implementation exists**; `select_physical_plan` returns `PlanError::NoPhysicalImpl` for this variant. |
| `Distinct` | `input` | Deduplication: retains only records whose net weight is non-zero. |
| `Aggregate` | `input`, `group_by: Vec<String>`, `agg: Vec<AggExpr>` | Hash aggregate with GROUP BY. |
| `Sort` | `input`, `keys: Vec<(String, SortDir)>` | Orders the result by the given keys. |
| `Limit` | `input`, `count: usize`, `offset: usize` | Applies row count limit and offset. When directly above a `Sort`, the physical planner merges this into `PhysicalPlan::Sort { limit: Some(count) }`. |

#### Invariants

- `LogicalPlan: Clone + PartialEq` — this is required by the optimizer's fixed-point loop, which clones the plan each iteration and compares for equality to detect convergence.
- `Union` has no physical implementation. If a plan containing `Union` reaches `select_physical_plan` or `build_graph`, an error is returned.
- Join `on` keys are column **names**, not expressions. Both sides must have a column with that name at runtime; missing keys produce `Value::Null` during probing.

#### Example

```rust
use spooky_streaming_processor::plan::{JoinType, LogicalPlan, Predicate, Value};

// SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 18
let plan = LogicalPlan::Join {
    left: Box::new(LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan { table: "users".into() }),
        predicate: Predicate::Gt("age".into(), Value::Int(18)),
    }),
    right: Box::new(LogicalPlan::Scan { table: "orders".into() }),
    on: ("id".into(), "user_id".into()),
    kind: JoinType::Inner,
};
```

---

## Module: `engine` — Core Streaming Runtime

Source: `src/engine/mod.rs`

This module re-exports the `Weight` type and defines the core push-model primitives: `Record`, `Event`, the `Operator` trait, and `DispatcherOp`.

---

### `struct Event`

A streaming change event — the unit of data that flows through the operator DAG.

```rust
pub struct Event {
    pub source: Arc<str>,
    pub record: Record,
    pub weight: Weight,
}
```

#### Fields

| Field | Type | Description |
|---|---|---|
| `source` | `Arc<str>` | The name of the source table this event originates from. Used by `DispatcherOp` to route the event to the correct pipeline arm. `Arc<str>` is used to avoid a heap allocation per event when the same interned string is shared across all events from the same table. |
| `record` | `Record` | The row payload (`HashMap<String, Value>`). |
| `weight` | `Weight` | DBSP multiplicity: `Weight::INSERT` (+1) for an insertion, `Weight::DELETE` (-1) for a retraction/deletion. Non-unit weights arise from join weight products and batching. |

#### Invariants

- A weight of `Weight::ZERO` (0) is valid but means the event has no effect. `BatchingOp` prunes zero-weight records before flushing. The join hash tables also prune entries where net weight reaches zero.
- `event.source` must match a key registered in `DispatcherOp::routes` exactly (pointer-equal `Arc<str>` after interning). An unknown source name causes a warning to `stderr` and the event is silently dropped.

#### Example

```rust
use std::collections::HashMap;
use std::sync::Arc;
use spooky_streaming_processor::engine::{Event, Weight};
use spooky_streaming_processor::plan::Value;

let mut record = HashMap::new();
record.insert("id".into(), Value::Int(1));
record.insert("name".into(), Value::Str("Alice".into()));

let event = Event {
    source: Arc::from("users"),
    record,
    weight: Weight::INSERT,
};
```

---

### `trait Operator`

Core trait for every push-based physical operator in the DAG.

```rust
pub trait Operator {
    fn on_event(&mut self, event: Event);
}
```

#### Method: `on_event`

Called for each incoming event. Implementors process the event (apply a predicate, project columns, probe a join table, etc.) and call `next.on_event(...)` for each output event produced.

| Parameter | Type | Description |
|---|---|---|
| `event` | `Event` | The incoming change event to process. |

#### Invariants

- `Operator` is not `Send` or `Sync` — the engine is single-threaded.
- Operators are composed by wrapping: each operator holds a `Box<dyn Operator>` reference to the downstream operator. The DAG is built bottom-up by `compile_plan`.
- `on_event` may produce zero, one, or many downstream events for a single input event (e.g., a join emits one event per matching pair).

#### Implementing a Custom Operator

```rust
use spooky_streaming_processor::engine::{Event, Operator};

struct CountingOp {
    count: u64,
    next: Box<dyn Operator>,
}

impl Operator for CountingOp {
    fn on_event(&mut self, event: Event) {
        self.count += 1;
        self.next.on_event(event);
    }
}
```

---

### `struct DispatcherOp`

Entry-point operator that routes incoming events to sub-pipelines based on the event's `source` name.

```rust
pub struct DispatcherOp {
    routes: HashMap<Arc<str>, Box<dyn Operator>>,
}
```

`compile_plan` returns a `DispatcherOp` whenever the compiled plan contains a join (i.e., when there are two or more data sources). Callers push all events — regardless of which table they come from — into the single `DispatcherOp` entry point.

#### Constructor

```rust
// pub(crate) — created internally by compile_plan
pub(crate) fn new(routes: HashMap<Arc<str>, Box<dyn Operator>>) -> Self
```

`DispatcherOp` is constructed internally by `compile_plan`; callers do not create it directly. The `routes` map is keyed by interned table names (`Arc<str>`).

#### Routing Behavior

When `on_event` is called:
- If `event.source` matches a registered route key, the event is forwarded to that route's operator.
- If `event.source` does not match any route, a warning is printed to `stderr` and the event is dropped.

#### Invariants

- Route keys must be the same interned `Arc<str>` values used in `event.source`. `compile_plan` creates the keys with `Arc::from(table_name.as_str())`. When pushing events manually, create source names with the same `Arc::from("table_name")` call.
- Routing is by exact string equality — no prefix matching, no wildcards.

#### Example

```rust
use std::sync::Arc;
use std::collections::HashMap;
use spooky_streaming_processor::engine::{Event, Weight};
use spooky_streaming_processor::engine::sink::SinkOp;
use spooky_streaming_processor::engine::builder::build_graph;
use spooky_streaming_processor::plan::{JoinType, LogicalPlan, Value};

let plan = LogicalPlan::Join {
    left: Box::new(LogicalPlan::Scan { table: "users".into() }),
    right: Box::new(LogicalPlan::Scan { table: "orders".into() }),
    on: ("id".into(), "user_id".into()),
    kind: JoinType::Inner,
};

let sink = Box::new(SinkOp::new("OUTPUT"));
let mut dispatcher = build_graph(plan, sink).expect("build failed");

// Events for both tables are pushed into the single dispatcher:
let mut user_record = HashMap::new();
user_record.insert("id".into(), Value::Int(1));
dispatcher.on_event(Event {
    source: Arc::from("users"),   // must match the registered route key
    record: user_record,
    weight: Weight::INSERT,
});
```

---

## Module: `engine::weight` — Weight Newtype

Source: `src/engine/weight.rs`

`Weight` is a thin `i64` newtype representing the multiplicity of a record in a Z-set. The type enforces DBSP algebra constraints by intentionally omitting `Mul<Weight, Weight>`.

---

### `struct Weight`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Weight(pub i64);
```

The inner `i64` is public for read access but should not be manipulated directly for arithmetic operations. Use the provided methods and constants instead.

#### Constants

| Constant | Value | Meaning |
|---|---|---|
| `Weight::INSERT` | `Weight(1)` | Standard insertion weight. Use for new records entering the stream. |
| `Weight::DELETE` | `Weight(-1)` | Standard deletion/retraction weight. Use when a record is removed or updated (as a delete-then-insert pair). |
| `Weight::ZERO` | `Weight(0)` | Zero weight. Records with this weight have no effect and are pruned by the runtime. |

#### Methods

##### `Weight::scale`

```rust
pub fn scale(self, factor: i64) -> Weight
```

Multiplies the weight by a plain integer scalar. Used for batched insertions where a single record represents `factor` copies.

| Parameter | Type | Description |
|---|---|---|
| `self` | `Weight` | The weight to scale. |
| `factor` | `i64` | Integer multiplier. |

**Returns**: A new `Weight` whose inner value is `self.0 * factor`.

```rust
use spooky_streaming_processor::engine::Weight;

let w = Weight::INSERT.scale(5);
assert_eq!(w.0, 5);
```

##### `Weight::is_zero`

```rust
pub fn is_zero(self) -> bool
```

Returns `true` if the weight is zero. Use this to prune records that cancel out (an insert followed by a delete for the same record).

```rust
use spooky_streaming_processor::engine::Weight;

assert!(Weight::ZERO.is_zero());
assert!(!(Weight::INSERT + Weight::DELETE).is_zero() == false);
let cancel = Weight::INSERT + Weight::DELETE;
assert!(cancel.is_zero());
```

##### `Weight::join_product`

```rust
pub fn join_product(left: Weight, right: Weight) -> Weight
```

The bilinear join weight product: `w_left * w_right`. This is the **only correct way** to multiply two weights together.

| Parameter | Type | Description |
|---|---|---|
| `left` | `Weight` | Weight of the arriving event. |
| `right` | `Weight` | Weight of the stored (probed) record. |

**Returns**: `Weight(left.0 * right.0)`.

**Invariant**: `Mul<Weight, Weight>` is intentionally **not implemented**. Writing `left_weight * right_weight` is a **compile error**. This prevents accidental "weight squared" errors, which are dimensional violations in the DBSP algebra. Always call `Weight::join_product(left, right)` in join code.

```rust
use spooky_streaming_processor::engine::Weight;

// Correct: explicit join product
let out_weight = Weight::join_product(Weight::INSERT, Weight::DELETE);
assert_eq!(out_weight.0, -1);  // +1 * -1 = -1

// Compile error — Mul<Weight, Weight> is not implemented:
// let bad = Weight::INSERT * Weight::DELETE;
```

#### Implemented Traits

| Trait | Behavior |
|---|---|
| `Add` | `w1 + w2` — adds the inner `i64` values. Used to accumulate weights for the same record. |
| `Neg` | `-w` — negates the inner `i64`. Turns an insert into a delete and vice versa. |
| `PartialOrd`, `Ord` | Ordered by the inner `i64` value. |

**Not implemented**: `Mul<Weight, Weight>` — intentionally omitted. Use `Weight::join_product` for join weight arithmetic.

---

## Module: `engine::filter` — Filter Operator

Source: `src/engine/filter.rs`

---

### `struct FilterOp`

Stateless filter operator. Evaluates a `Predicate` against each incoming record; records that pass are forwarded downstream; others are silently dropped.

```rust
pub struct FilterOp {
    predicate: Predicate,
    next: Box<dyn Operator>,
}
```

#### Constructor

```rust
pub fn new(predicate: Predicate, next: Box<dyn Operator>) -> Self
```

| Parameter | Type | Description |
|---|---|---|
| `predicate` | `Predicate` | The predicate to evaluate on each event's record. |
| `next` | `Box<dyn Operator>` | The downstream operator that receives passing events. |

#### Behavior

On each `on_event` call:
1. Calls `predicate.eval(&event.record)`.
2. If `true`, calls `self.next.on_event(event)`.
3. If `false`, the event is dropped (no downstream call).

The `weight` and `source` fields of the event are passed through unchanged.

#### Example

```rust
use spooky_streaming_processor::engine::filter::FilterOp;
use spooky_streaming_processor::engine::sink::SinkOp;
use spooky_streaming_processor::engine::{Event, Operator, Weight};
use spooky_streaming_processor::plan::{Predicate, Value};
use std::collections::HashMap;
use std::sync::Arc;

let sink = Box::new(SinkOp::new("filtered-output"));
let mut filter = FilterOp::new(
    Predicate::Gt("age".into(), Value::Int(18)),
    sink,
);

let mut record = HashMap::new();
record.insert("age".into(), Value::Int(25));

filter.on_event(Event {
    source: Arc::from("users"),
    record,
    weight: Weight::INSERT,
});
// Output: [filtered-output] weight=+1  { age=Int(25) }
```

---

## Module: `engine::project` — Projection Operator

Source: `src/engine/project.rs`

---

### `struct ProjectOp`

Projection operator — retains only the named columns in each record, dropping all others. The event weight and source are passed through unchanged.

```rust
pub struct ProjectOp {
    columns: Vec<String>,
    next: Box<dyn Operator>,
}
```

#### Constructor

```rust
pub fn new(columns: Vec<String>, next: Box<dyn Operator>) -> Self
```

| Parameter | Type | Description |
|---|---|---|
| `columns` | `Vec<String>` | Column names to retain. All other columns are removed from `event.record`. |
| `next` | `Box<dyn Operator>` | Downstream operator. |

#### Behavior

Calls `event.record.retain(|col, _| self.columns.contains(col))` then forwards the modified event. The operation is in-place on the `Event`'s record.

#### Invariants

- Column names not present in the incoming record are silently ignored (not an error).
- If `columns` is empty, all columns are dropped, producing an empty record on every event.
- The optimizer's `eliminate_trivial_projects` rule removes `Project` nodes that have an empty column list from the logical plan before physical compilation.

#### Example

```rust
use spooky_streaming_processor::engine::project::ProjectOp;
use spooky_streaming_processor::engine::sink::SinkOp;
use spooky_streaming_processor::engine::{Event, Operator, Weight};
use spooky_streaming_processor::plan::Value;
use std::collections::HashMap;
use std::sync::Arc;

let sink = Box::new(SinkOp::new("projected"));
let mut project = ProjectOp::new(vec!["id".into(), "name".into()], sink);

let mut record = HashMap::new();
record.insert("id".into(), Value::Int(1));
record.insert("name".into(), Value::Str("Alice".into()));
record.insert("age".into(), Value::Int(30));  // will be dropped

project.on_event(Event {
    source: Arc::from("users"),
    record,
    weight: Weight::INSERT,
});
// Output: [projected] weight=+1  { id=Int(1), name=Str("Alice") }
```

---

## Module: `engine::sink` — Sink Operator

Source: `src/engine/sink.rs`

---

### `struct SinkOp`

Terminal operator that prints every received event to **stdout**. Used as the leaf of every operator DAG in demos and tests.

```rust
pub struct SinkOp {
    label: String,
}
```

#### Constructor

```rust
pub fn new(label: impl Into<String>) -> Self
```

| Parameter | Type | Description |
|---|---|---|
| `label` | `impl Into<String>` | A logical label printed with each output line to identify which pipeline produced the event. |

#### Output Format

Each event is printed as:

```
[<label>] weight=<+/-N>  { col1=<value>, col2=<value>, ... }
```

Columns are sorted alphabetically for **deterministic output** regardless of `HashMap` iteration order.

#### Example

```rust
use spooky_streaming_processor::engine::sink::SinkOp;
use spooky_streaming_processor::engine::{Event, Operator, Weight};
use spooky_streaming_processor::plan::Value;
use std::collections::HashMap;
use std::sync::Arc;

let mut sink = SinkOp::new("demo");

let mut record = HashMap::new();
record.insert("id".into(), Value::Int(42));
record.insert("name".into(), Value::Str("Bob".into()));

sink.on_event(Event {
    source: Arc::from("users"),
    record,
    weight: Weight::DELETE,
});
// Stdout: [demo] weight=-1  { id=Int(42), name=Str("Bob") }
```

---

## Module: `engine::batching` — Batching Operator

Source: `src/engine/batching.rs`

`BatchingOp` accumulates incoming events into a ZSet buffer and flushes consolidated batches downstream. Consolidation eliminates insert/delete pairs for the same record, reducing the work done by downstream operators (especially join probes).

**Throughput benefit**: With consolidation rate `c` (fraction of buffered records whose weights cancel to zero), downstream probe work reduces from `B` to `B × (1-c)`. At `c = 50%` this is a 2x improvement; at `c = 90%` it is 10x.

---

### `struct BatchingOp`

```rust
pub struct BatchingOp {
    buffer: Vec<(Record, i64)>,
    flush_threshold: usize,
    source: Arc<str>,
    next: Box<dyn Operator>,
}
```

#### Constructor

```rust
pub fn new(source: Arc<str>, flush_threshold: usize, next: Box<dyn Operator>) -> Self
```

| Parameter | Type | Description |
|---|---|---|
| `source` | `Arc<str>` | Source name stamped on all flushed events. Must match the table name used in `Event::source` for downstream routing. |
| `flush_threshold` | `usize` | Number of distinct buffered records that triggers an automatic flush. When the buffer reaches this size, `flush()` is called automatically at the end of `on_event`. |
| `next` | `Box<dyn Operator>` | Downstream operator that receives the consolidated batch. |

#### Method: `flush`

```rust
pub fn flush(&mut self)
```

Drains the internal buffer, emitting one event per record with non-zero net weight. Records whose net weight has reached zero (because they received both an insert and a delete) are pruned and produce no downstream event.

After `flush()`, the buffer is empty.

**Invariant**: Callers must call `flush()` explicitly at end-of-stream or end-of-epoch if events remain in the buffer. Automatic flushing only triggers when the buffer reaches `flush_threshold` distinct records.

#### Buffer Design

The buffer is `Vec<(Record, i64)>` — a linear list of `(record, net_weight)` pairs. When an event arrives for a record already in the buffer, the weight is accumulated in-place (no duplicate entries). For flush thresholds up to approximately 10,000 records the linear search is acceptable; for larger thresholds, a canonically keyed map would be more efficient.

#### Example

```rust
use std::collections::HashMap;
use std::sync::Arc;
use spooky_streaming_processor::engine::batching::BatchingOp;
use spooky_streaming_processor::engine::sink::SinkOp;
use spooky_streaming_processor::engine::{Event, Operator, Weight};
use spooky_streaming_processor::plan::Value;

let sink = Box::new(SinkOp::new("output"));
let mut batcher = BatchingOp::new(Arc::from("users"), 100, sink);

// Simulate an insert followed by a delete for the same record:
let mut record = HashMap::new();
record.insert("id".into(), Value::Int(1));

batcher.on_event(Event {
    source: Arc::from("users"),
    record: record.clone(),
    weight: Weight::INSERT,
});
batcher.on_event(Event {
    source: Arc::from("users"),
    record: record.clone(),
    weight: Weight::DELETE,
});

// The net weight is 0 — flushing will not emit this record.
batcher.flush();
// No output: the record was consolidated away.
```

---

## Module: `engine::join` — Symmetric Hash Join

Source: `src/engine/join.rs`

The Symmetric Hash Join is the incremental equi-join algorithm used by this engine. Both sides maintain hash tables indexed by their respective join key. Each incoming event on either side immediately probes the opposite table and emits join results — making the join fully incremental, with no need to buffer all data before producing output.

`JoinSideOp` is `pub(crate)` — it is created exclusively by `compile_plan` and is not part of the public embedding API.

---

### `struct JoinSideOp`

One input arm of a Symmetric Hash Join. Both the left and right arms share a single `JoinCore` through `Rc<RefCell<JoinCore>>`.

```rust
// pub(crate)
pub(crate) struct JoinSideOp {
    side: JoinSide,
    core: Rc<RefCell<JoinCore>>,
}
```

#### Threading Invariant

`JoinCore` is wrapped in `Rc<RefCell<...>>`, not `Arc<Mutex<...>>`. The engine is **single-threaded by design**. Do **not** add `unsafe impl Send` to `JoinSideOp` or `JoinCore`. To add multi-threading support, replace with `Arc<Mutex<JoinCore>>` and add `+ Send + Sync` bounds throughout the `Operator` trait.

#### Three-Phase Incremental Update Protocol

Every call to `JoinSideOp::on_event` executes three phases, each acquiring and releasing the `RefCell` borrow separately. This avoids the double-borrow panic that would occur if probing and emitting were interleaved while the core was already mutably borrowed.

**Phase 1 — Probe** (immutable borrow):
- Borrow `JoinCore` immutably.
- Read the **opposite** side's hash table.
- Collect all matching records (with their net weights) into a local `Vec<Event>`.
- Release the borrow before phase 2.

**Phase 2 — Insert** (mutable borrow):
- Borrow `JoinCore` mutably.
- Update **this** side's hash table with the arriving event.
- If the record already exists in the table, accumulate the weight in-place.
- Prune entries whose net weight reaches zero (retraction cleanup).
- Remove the hash bucket entirely if it becomes empty.
- Release the borrow before phase 3.

**Phase 3 — Emit** (mutable borrow):
- Borrow `JoinCore` mutably.
- Forward all events collected in phase 1 to `core.next`.
- Release the borrow.

This three-phase structure means `RefCell` is never borrowed more than once at a time, making join execution panic-free.

#### Join Semantics by Type

| `JoinType` | Probe behavior |
|---|---|
| `Inner` | Emit one merged event per live matching record in the opposite table. Weight = `join_product(arriving, stored)`. |
| `Cross` | Same as `Inner` but uses `Value::Null` as the sentinel key so every left record matches every right record. |
| `LeftSemi` | Only the **left** arm can emit. Emits the arriving left record (with its own weight, unmodified) if any live match exists in the right table. |
| `LeftAnti` | Left arm emits when **no** live match exists in the right table. Right arm never emits. |

#### Weight Arithmetic in Joins

For `Inner` and `Cross` joins, the output weight of a matched pair is computed as:

```rust
Weight::join_product(event.weight, Weight(stored_weight))
```

This is the correct bilinear product. `Mul<Weight, Weight>` is not implemented; using `*` on two `Weight` values is a compile error.

---

## Module: `lowering` — Mock AST to Logical Plan

Source: `src/lowering.rs`

This module translates a simplified SurrealDB-style SELECT AST into a `LogicalPlan`. The mock AST types (`MockSelectStmt`, `MockCond`, etc.) mirror the real `surrealdb_core` AST but are simpler to construct in tests and demos.

**Decorrelation**: `col IN (SELECT ...)` is rewritten as `Join { kind: LeftSemi }` during lowering. DBSP cannot evaluate correlated subqueries incrementally; decorrelation makes the subquery into a join that the engine can handle.

**Note on visibility**: All mock AST types are `pub(crate)`. They are test helpers, not the public API boundary. Production code is intended to use real `surrealdb_core::sql` types via `real_lowering::lower_real_select` (currently blocked; see `real_lowering`).

---

### `enum LoweringError`

Errors produced by `lower_select`. Uses `thiserror` for `Display` formatting.

```rust
#[derive(Debug, thiserror::Error)]
pub enum LoweringError {
    UnsupportedCondition { description: String },
    InsideRequiresSubquery { col: String },
    AmbiguousJoinKey { outer_col: String },
    CrossColumnPredicateUnsupported { left_col: String, op: String, right_col: String },
    UnboundParameter { name: String },
    MaxNestingDepthExceeded { max_depth: usize },
    EmptyFrom,
}
```

#### Variants

| Variant | Fields | Trigger Condition |
|---|---|---|
| `UnsupportedCondition` | `description: String` | The WHERE clause shape is not supported: the condition is not a scalar `BinaryOp`, an `Inside+Subquery`, `And`, `Or`, or `Not`. |
| `InsideRequiresSubquery` | `col: String` | `col INSIDE literal` — `INSIDE` / `IN` requires a subquery on the right-hand side; a literal value is semantically invalid. |
| `AmbiguousJoinKey` | `outer_col: String` | The subquery in `col IN (subquery)` has an empty `select_fields` list, making it impossible to determine the join key column. Use `SELECT <col> FROM ...` rather than `SELECT * FROM ...` in the subquery. |
| `CrossColumnPredicateUnsupported` | `left_col`, `op`, `right_col` | A cross-column comparison (`col_a op col_b`) was encountered; only scalar predicates are currently supported. |
| `UnboundParameter` | `name: String` | A query parameter reference (`$param`) was encountered; parameters must be bound before planning. |
| `MaxNestingDepthExceeded` | `max_depth: usize` | Subquery nesting exceeded the maximum allowed depth. |
| `EmptyFrom` | — | The `FROM` clause table name is an empty string. |

---

### `struct MockSelectStmt`

Simplified mock of a SurrealDB `SELECT` statement. `pub(crate)` — internal test helper.

```rust
pub(crate) struct MockSelectStmt {
    pub(crate) table: String,
    pub(crate) select_fields: Vec<String>,
    pub(crate) cond: Option<MockCond>,
}
```

#### Fields

| Field | Type | Description |
|---|---|---|
| `table` | `String` | The `FROM` target — the outer table name. Must be non-empty; an empty string causes `LoweringError::EmptyFrom`. |
| `select_fields` | `Vec<String>` | Columns named in the `SELECT` list. An empty list means `SELECT *` (no `Project` node is generated). When used as the right-hand side of an `IN` subquery, `select_fields[0]` becomes the right join key; an empty list here causes `LoweringError::AmbiguousJoinKey`. |
| `cond` | `Option<MockCond>` | Optional `WHERE` clause. `None` means no filtering. |

---

### `enum MockCond`

A condition node in the `WHERE` clause. `pub(crate)`.

```rust
pub(crate) enum MockCond {
    BinaryOp { op: MockOp, left: MockExpr, right: MockExpr },
    And(Box<MockCond>, Box<MockCond>),
    Or(Box<MockCond>, Box<MockCond>),
    Not(Box<MockCond>),
}
```

#### Variants

| Variant | Description |
|---|---|
| `BinaryOp { op, left, right }` | A binary comparison expression. The `op`, `left`, and `right` determine whether lowering produces a `Filter`, a `Join { kind: LeftSemi }`, or an error. |
| `And(left, right)` | Logical AND. Lowered as two stacked `Filter` nodes (left applied first, right applied to the result). |
| `Or(left, right)` | Logical OR. Lowered as a single `Filter { predicate: Predicate::Or(...) }`. The operands must be simple scalar predicates (not subqueries). |
| `Not(inner)` | Logical NOT. Lowered as `Filter { predicate: Predicate::Not(...) }`. The operand must be a simple scalar predicate. |

---

### `enum MockOp`

Binary operator in a `WHERE` expression. `pub(crate)`.

```rust
#[derive(Debug, Clone, Copy)]
pub(crate) enum MockOp {
    Inside,
    Gt,
    Lt,
    Eq,
    Ne,
    Ge,
    Le,
}
```

#### Variants

| Variant | SQL | Description |
|---|---|---|
| `Inside` | `IN` / `INSIDE` | Triggers subquery decorrelation. Right-hand side must be `MockExpr::Subquery`; a `Literal` produces `LoweringError::InsideRequiresSubquery`. |
| `Gt` | `>` | Greater than. |
| `Lt` | `<` | Less than. |
| `Eq` | `=` | Equal. |
| `Ne` | `!=` | Not equal. |
| `Ge` | `>=` (`MoreThanEqual` in SurrealDB AST) | Greater than or equal. |
| `Le` | `<=` (`LessThanEqual` in SurrealDB AST) | Less than or equal. |

---

### `enum MockExpr`

An expression on either side of a binary operator. `pub(crate)`.

```rust
pub(crate) enum MockExpr {
    Column(String),
    Literal(Value),
    Subquery(Box<MockSelectStmt>),
}
```

#### Variants

| Variant | Description |
|---|---|
| `Column(name)` | A reference to a column in the current row. Must appear on the left side of a binary op for scalar predicates. |
| `Literal(val)` | A literal scalar value. Must appear on the right side of a scalar binary op. |
| `Subquery(stmt)` | A nested `SELECT` statement. Only valid on the right side of `MockOp::Inside`. |

---

### `fn lower_select`

```rust
pub fn lower_select(stmt: MockSelectStmt) -> Result<LogicalPlan, LoweringError>
```

Lower a mock `SELECT` statement to a `LogicalPlan`. No silent fallbacks — any unrecognized condition shape returns an explicit error.

#### Parameters

| Name | Type | Description |
|---|---|---|
| `stmt` | `MockSelectStmt` | The mock AST to lower. |

#### Returns

`Ok(LogicalPlan)` on success. The structure of the returned plan:

1. A base `LogicalPlan::Scan { table }`.
2. If `stmt.cond` is `Some`, a `Filter` (scalar predicate) or `Join { kind: LeftSemi }` (decorrelated subquery) is placed above the scan.
3. If `stmt.select_fields` is non-empty, a `LogicalPlan::Project` is placed at the top.

#### Errors

| Error variant | Trigger |
|---|---|
| `LoweringError::EmptyFrom` | `stmt.table` is an empty string. |
| `LoweringError::InsideRequiresSubquery` | `INSIDE` / `IN` operator with a `Literal` on the right side. |
| `LoweringError::AmbiguousJoinKey` | `col IN (subquery)` where the subquery has no `select_fields`. |
| `LoweringError::UnsupportedCondition` | Any other unrecognized condition shape. |

#### Example: Subquery Decorrelation

```rust
// SELECT SupplierName FROM Suppliers
// WHERE SupplierID IN (SELECT SupplierID FROM Products WHERE Price < 20)
use spooky_streaming_processor::lowering::{
    MockSelectStmt, MockCond, MockOp, MockExpr, lower_select,
};
use spooky_streaming_processor::plan::Value;

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

let plan = lower_select(ast).expect("lowering should succeed");
// plan is: Project(Join(Scan("Suppliers"), Filter(Scan("Products"), Lt), LeftSemi))
```

#### Example: AND Condition

```rust
use spooky_streaming_processor::lowering::{
    MockSelectStmt, MockCond, MockOp, MockExpr, lower_select,
};
use spooky_streaming_processor::plan::Value;

// SELECT * FROM users WHERE age > 18 AND active = true
let ast = MockSelectStmt {
    table: "users".into(),
    select_fields: vec![],
    cond: Some(MockCond::And(
        Box::new(MockCond::BinaryOp {
            op: MockOp::Gt,
            left: MockExpr::Column("age".into()),
            right: MockExpr::Literal(Value::Int(18)),
        }),
        Box::new(MockCond::BinaryOp {
            op: MockOp::Eq,
            left: MockExpr::Column("active".into()),
            right: MockExpr::Literal(Value::Bool(true)),
        }),
    )),
};

let plan = lower_select(ast).expect("lowering should succeed");
// plan is: Filter(Filter(Scan("users"), Gt("age", 18)), Eq("active", true))
```

#### Example: Error Path

```rust
use spooky_streaming_processor::lowering::{
    MockSelectStmt, MockCond, MockOp, MockExpr, lower_select, LoweringError,
};
use spooky_streaming_processor::plan::Value;

// INSIDE with a scalar — should fail
let bad = MockSelectStmt {
    table: "users".into(),
    select_fields: vec![],
    cond: Some(MockCond::BinaryOp {
        op: MockOp::Inside,
        left: MockExpr::Column("id".into()),
        right: MockExpr::Literal(Value::Int(42)),
    }),
};

match lower_select(bad) {
    Err(LoweringError::InsideRequiresSubquery { col }) => {
        assert_eq!(col, "id");
    }
    other => panic!("unexpected: {other:?}"),
}
```

---

## Module: `optimizer` — Optimization Passes

Source: `src/optimizer.rs`

A rule-based algebraic optimizer that rewrites a `LogicalPlan` tree until no further rewrites are possible (fixed-point iteration). All rules are correctness-preserving; no statistical cost model is required.

The fixed-point loop requires `LogicalPlan: Clone + PartialEq`: it clones the plan each iteration and compares with the previous iteration to detect convergence.

---

### `fn optimize`

```rust
pub fn optimize(plan: LogicalPlan) -> LogicalPlan
```

Apply all optimization rules to `plan` until a fixed point is reached.

#### Parameters

| Name | Type | Description |
|---|---|---|
| `plan` | `LogicalPlan` | The plan to optimize. Consumed by value. |

#### Returns

The optimized `LogicalPlan`. Guaranteed to terminate because each rule either leaves the plan unchanged or strictly reduces it in size/depth, preventing infinite loops.

#### Rules Applied (in order, each iteration)

1. `push_filters_below_joins`
2. `eliminate_trivial_projects`
3. `prune_unused_columns`

The loop continues until a full pass over all rules produces no change.

#### Example

```rust
use spooky_streaming_processor::plan::{JoinType, LogicalPlan, Predicate, Value};
use spooky_streaming_processor::optimizer::optimize;

let plan = LogicalPlan::Filter {
    input: Box::new(LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { table: "users".into() }),
        right: Box::new(LogicalPlan::Scan { table: "orders".into() }),
        on: ("id".into(), "user_id".into()),
        kind: JoinType::Inner,
    }),
    predicate: Predicate::Gt("id".into(), Value::Int(0)),
};

// The optimizer pushes the filter below the join (to the left side,
// since "id" is the left join key).
let optimized = optimize(plan);
```

---

### `fn push_filters_below_joins`

```rust
pub fn push_filters_below_joins(plan: LogicalPlan) -> LogicalPlan
```

Push `Filter` nodes below `Join` nodes when the filter predicate references only columns from one join arm. This reduces the number of rows entering the expensive join probe phase.

#### Rewrite Rule

```
Filter(Join(A, B), p)
  where p only references left_key columns
→ Join(Filter(A, p), B)
```

For `Inner` joins, filters referencing only right key columns can also be pushed to the right arm. For non-`Inner` joins (`LeftSemi`, `LeftAnti`, `Cross`), pushing to the right side is unsafe and is not performed.

#### Parameters

| Name | Type | Description |
|---|---|---|
| `plan` | `LogicalPlan` | The plan tree to transform. |

#### Returns

The transformed `LogicalPlan`. If no push-down is applicable, the plan is returned unchanged.

#### Notes

The current implementation approximates column availability by checking whether the predicate references the join key column. A full optimizer would use schema metadata (available column sets) for each sub-tree.

---

### `fn eliminate_trivial_projects`

```rust
pub fn eliminate_trivial_projects(plan: LogicalPlan) -> LogicalPlan
```

Remove `Project` nodes whose `columns` list is empty (representing `SELECT *`). Such nodes are no-ops and can be eliminated.

#### Rewrite Rule

```
Project(input, columns=[])  →  input
```

#### Parameters

| Name | Type | Description |
|---|---|---|
| `plan` | `LogicalPlan` | The plan tree to transform. |

#### Returns

The transformed `LogicalPlan` with empty `Project` nodes removed.

---

### `fn prune_unused_columns`

```rust
pub fn prune_unused_columns(plan: LogicalPlan) -> LogicalPlan
```

Projection pushdown — push `Project` nodes toward the leaves to reduce record width as early as possible. **Currently a no-op placeholder.** The function returns the plan unchanged.

#### Status

Not yet implemented. Full implementation would require tracking which columns are needed by each operator top-down and inserting `Project` nodes before wide `Scan` nodes. This optimization becomes valuable once aggregate and sort operators are physically implemented.

---

## Module: `physical_plan` — Physical Plan Selection

Source: `src/physical_plan.rs`

`PhysicalPlan` sits between `LogicalPlan` and the executable operator DAG. `select_physical_plan` chooses the specific algorithm for each logical operator.

| Layer | Role |
|---|---|
| `LogicalPlan` | Algebraic query intent (what to compute) |
| `PhysicalPlan` | Algorithm selection (how to compute it) |
| Operator DAG | Push-based execution (built by `compile_plan`) |

---

### `enum PlanError`

Errors produced by `select_physical_plan`.

```rust
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    NoPhysicalImpl { variant: &'static str },
}
```

#### Variants

| Variant | Field | Trigger |
|---|---|---|
| `NoPhysicalImpl` | `variant: &'static str` | The logical plan variant has no physical algorithm. Currently triggered by `LogicalPlan::Union`. |

---

### `enum PhysicalPlan`

A physical query plan node — the last tree-shaped representation before the plan is compiled into a flat push-based operator DAG by `compile_plan`.

```rust
#[derive(Debug)]
pub enum PhysicalPlan {
    Source { table: String },
    Filter { input: Box<PhysicalPlan>, predicate: Predicate },
    Project { input: Box<PhysicalPlan>, columns: Vec<String> },
    SymmetricHashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_key: String,
        right_key: String,
        kind: JoinType,
    },
    Distinct { input: Box<PhysicalPlan> },
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<String>,
        agg: Vec<AggExpr>,
    },
    Sort {
        input: Box<PhysicalPlan>,
        keys: Vec<(String, SortDir)>,
        limit: Option<usize>,
    },
}
```

#### Variants

| Variant | Fields | Algorithm |
|---|---|---|
| `Source` | `table: String` | Base table scan. Events arrive externally. |
| `Filter` | `input`, `predicate` | Row-level predicate evaluation. |
| `Project` | `input`, `columns` | Column projection — drops unlisted columns. |
| `SymmetricHashJoin` | `left`, `right`, `left_key`, `right_key`, `kind` | Incremental equi-join using symmetric hash tables on both sides. |
| `Distinct` | `input` | Deduplication by net weight. Physical operator not yet implemented in `compile_plan`. |
| `HashAggregate` | `input`, `group_by`, `agg` | Hash-partitioned GROUP BY. Physical operator not yet implemented in `compile_plan`. |
| `Sort` | `input`, `keys`, `limit: Option<usize>` | Ordering with optional TOP-N limit. `Limit` from the logical plan is merged into the enclosing `Sort` node. Physical operator not yet implemented in `compile_plan`. |

#### Note on `Limit` Merging

`LogicalPlan::Limit` does not have a corresponding `PhysicalPlan::Limit` variant. Instead, `select_physical_plan` merges the limit into the enclosing `Sort`:

- `Sort` above `Limit`: `Sort { limit: Some(count) }`.
- `Limit` without `Sort`: wrapped in a `Sort { keys: [], limit: Some(count + offset) }`.

---

### `fn select_physical_plan`

```rust
pub fn select_physical_plan(logical: LogicalPlan) -> Result<PhysicalPlan, PlanError>
```

Convert a `LogicalPlan` into a `PhysicalPlan` by choosing algorithms for each logical operator.

#### Parameters

| Name | Type | Description |
|---|---|---|
| `logical` | `LogicalPlan` | The (typically optimized) logical plan. Consumed by value. |

#### Returns

`Ok(PhysicalPlan)` on success.

#### Errors

| Error | Trigger |
|---|---|
| `PlanError::NoPhysicalImpl { variant: "Union" }` | The logical plan contains a `LogicalPlan::Union` node. No physical union algorithm is implemented. |

#### Example

```rust
use spooky_streaming_processor::plan::{JoinType, LogicalPlan};
use spooky_streaming_processor::physical_plan::select_physical_plan;

let logical = LogicalPlan::Join {
    left: Box::new(LogicalPlan::Scan { table: "users".into() }),
    right: Box::new(LogicalPlan::Scan { table: "orders".into() }),
    on: ("id".into(), "user_id".into()),
    kind: JoinType::Inner,
};

let physical = select_physical_plan(logical).expect("should succeed");
// Returns PhysicalPlan::SymmetricHashJoin { left_key: "id", right_key: "user_id", ... }
```

---

## Module: `engine::builder` — DAG Compilation

Source: `src/engine/builder.rs`

This module provides `compile_plan` (primary) and `build_graph` (convenience shim) for compiling a plan tree into an executable push-based operator DAG.

---

### `enum BuildError`

Errors produced by `compile_plan` and `build_graph`.

```rust
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    NoScanInSubtree { side: &'static str, plan_debug: String },
    DuplicateTableRoute { table: String },
    UnsupportedPlanNode { variant: &'static str },
}
```

#### Variants

| Variant | Fields | Trigger |
|---|---|---|
| `NoScanInSubtree` | `side: &'static str` ("left" or "right"), `plan_debug: String` | One arm of a `SymmetricHashJoin` contains no `Source` node. `compile_plan` cannot determine the routing key for the `DispatcherOp`. |
| `DuplicateTableRoute` | `table: String` | Both arms of a join resolve to the same source table name. The dispatcher cannot distinguish their events. |
| `UnsupportedPlanNode` | `variant: &'static str` | A `PhysicalPlan` node with no physical operator implementation was encountered: `Distinct`, `HashAggregate`, or `Sort`. |

---

### `fn compile_plan`

```rust
pub fn compile_plan(
    plan: PhysicalPlan,
    next: Box<dyn Operator>,
) -> Result<Box<dyn Operator>, BuildError>
```

Recursively compile a `PhysicalPlan` into a push-based physical operator DAG.

#### Parameters

| Name | Type | Description |
|---|---|---|
| `plan` | `PhysicalPlan` | The physical plan to compile. Consumed by value. |
| `next` | `Box<dyn Operator>` | The **downstream** operator that will receive this sub-plan's output. The DAG is built bottom-up: `next` is the operator that processes records produced by `plan`. |

#### Returns

`Ok(Box<dyn Operator>)` — the root operator of the compiled DAG. For single-source plans, this is the operator corresponding to the `Source` leaf (or the first operator above it). For join plans, this is a `Box<dyn Operator>` wrapping a `DispatcherOp`.

#### Errors

| Error | Trigger |
|---|---|
| `BuildError::NoScanInSubtree` | A join arm resolves to no `Source` node. |
| `BuildError::DuplicateTableRoute` | Both join arms have the same source table name. |
| `BuildError::UnsupportedPlanNode { variant: "Distinct" }` | Plan contains `PhysicalPlan::Distinct`. |
| `BuildError::UnsupportedPlanNode { variant: "HashAggregate" }` | Plan contains `PhysicalPlan::HashAggregate`. |
| `BuildError::UnsupportedPlanNode { variant: "Sort" }` | Plan contains `PhysicalPlan::Sort`. |

#### Bottom-Up Construction

The `next` parameter is the downstream consumer. Compilation wraps operators from the top of the tree downward:

```
compile_plan(Filter { input: Source("users"), predicate: age > 18 }, SinkOp)

  Step 1: Create FilterOp(age>18, next=SinkOp)
  Step 2: compile_plan(Source("users"), FilterOp)
          → Source is a no-op leaf; returns FilterOp unchanged

Result: FilterOp  (caller pushes events directly into this)
```

#### Join Compilation

For `SymmetricHashJoin`, `compile_plan`:

1. Locates the `Source` table name in each arm (using `find_source_table`).
2. Creates a shared `JoinCore` (wrapped in `Rc<RefCell<...>>`).
3. Creates `JoinSideOp` instances for left and right, both sharing the `JoinCore`.
4. Recursively compiles each arm, passing the appropriate `JoinSideOp` as `next`.
5. Assembles a `DispatcherOp` routing left-table events to the left pipeline and right-table events to the right pipeline.
6. Returns the `DispatcherOp` as the entry point.

#### Example

```rust
use spooky_streaming_processor::engine::builder::compile_plan;
use spooky_streaming_processor::engine::sink::SinkOp;
use spooky_streaming_processor::engine::{Event, Operator, Weight};
use spooky_streaming_processor::physical_plan::PhysicalPlan;
use spooky_streaming_processor::plan::{JoinType, Predicate, Value};
use std::collections::HashMap;
use std::sync::Arc;

// PhysicalPlan: Filter(Source("users"), age > 18) → SinkOp
let plan = PhysicalPlan::Filter {
    input: Box::new(PhysicalPlan::Source { table: "users".into() }),
    predicate: Predicate::Gt("age".into(), Value::Int(18)),
};

let sink = Box::new(SinkOp::new("output"));
let mut root = compile_plan(plan, sink).expect("compile failed");

let mut record = HashMap::new();
record.insert("age".into(), Value::Int(25));

root.on_event(Event {
    source: Arc::from("users"),
    record,
    weight: Weight::INSERT,
});
// Output: [output] weight=+1  { age=Int(25) }
```

---

### `fn build_graph`

```rust
pub fn build_graph(
    plan: LogicalPlan,
    next: Box<dyn Operator>,
) -> Result<Box<dyn Operator>, BuildError>
```

Compile a `LogicalPlan` directly into a push-based operator DAG, bypassing the `PhysicalPlan` layer. This is a **convenience shim** for callers that have not yet been migrated to the full `select_physical_plan` + `compile_plan` pipeline.

#### Parameters

| Name | Type | Description |
|---|---|---|
| `plan` | `LogicalPlan` | The logical plan to compile. Consumed by value. |
| `next` | `Box<dyn Operator>` | Downstream operator. |

#### Returns

Same as `compile_plan`.

#### Errors

All `BuildError` variants from `compile_plan`, plus:

| Error | Trigger |
|---|---|
| `BuildError::UnsupportedPlanNode { variant: "Union" }` | `LogicalPlan::Union` has no physical implementation. |

#### When to Prefer `build_graph` vs `select_physical_plan` + `compile_plan`

- Use `build_graph` in tests and simple demos where the full pipeline overhead is unnecessary.
- Use `select_physical_plan` + `compile_plan` in production code where you need cost-based or alternative algorithm selection.

#### Example

```rust
use spooky_streaming_processor::engine::builder::build_graph;
use spooky_streaming_processor::engine::sink::SinkOp;
use spooky_streaming_processor::engine::{Event, Operator, Weight};
use spooky_streaming_processor::plan::{JoinType, LogicalPlan, Predicate, Value};
use std::collections::HashMap;
use std::sync::Arc;

let plan = LogicalPlan::Filter {
    input: Box::new(LogicalPlan::Scan { table: "users".into() }),
    predicate: Predicate::Ge("score".into(), Value::Int(90)),
};

let mut root = build_graph(plan, Box::new(SinkOp::new("out"))).expect("failed");

let mut rec = HashMap::new();
rec.insert("score".into(), Value::Int(95));
root.on_event(Event { source: Arc::from("users"), record: rec, weight: Weight::INSERT });
```

---

## Module: `real_lowering` — Stub

Source: `src/real_lowering.rs`

---

### `fn lower_real_select_stub`

```rust
pub fn lower_real_select_stub() -> Result<LogicalPlan, LoweringError>
```

A placeholder function that always returns `Err(LoweringError::UnsupportedCondition)` explaining why real AST lowering is not yet possible.

#### Why This Exists

`surrealdb-core 3.0.0` declares all SQL AST types (`SelectStatement`, `Cond`, `Field`, `Operator`, `Value`, etc.) as `pub(crate)`. External crates cannot reference these types. The `syn::parse()` function returns an `Ast` that can only be used as a `Debug`-printable blob from outside the crate.

#### Migration Path

Three options are documented in the module source:

1. **Use the `surrealdb` client crate** (not `surrealdb-core`) — update `Cargo.toml`:
   ```toml
   surrealdb = { version = "3", features = ["parser"] }
   ```

2. **Patch `surrealdb-core`** — fork the crate and add `pub use` re-exports for the AST types needed by this lowering layer.

3. **Wait for the API to stabilize** — if SurrealDB exposes its AST publicly in a future release, import from there.

#### Intended Interface (for reference)

Once AST types are accessible, the intended signature is:

```rust
// Not yet callable — blocked by pub(crate) AST types in surrealdb-core 3.0.0
pub fn lower_real_select(
    stmt: surrealdb_core::sql::SelectStatement,
) -> Result<LogicalPlan, LoweringError>
```

---

## Error Reference

Complete table of all error types, variants, and their trigger conditions.

| Error Type | Variant | Trigger Condition |
|---|---|---|
| `LoweringError` | `UnsupportedCondition { description }` | WHERE clause shape not in: scalar `BinaryOp`, `Inside+Subquery`, `And`, `Or`, `Not`. |
| `LoweringError` | `InsideRequiresSubquery { col }` | `col INSIDE literal` — scalar on right-hand side of `INSIDE`/`IN`. |
| `LoweringError` | `AmbiguousJoinKey { outer_col }` | Subquery in `col IN (subquery)` has no `select_fields` — join key cannot be determined. |
| `LoweringError` | `CrossColumnPredicateUnsupported { left_col, op, right_col }` | Cross-column comparison (`col_a op col_b`) encountered; only scalar predicates supported. |
| `LoweringError` | `UnboundParameter { name }` | Query parameter `$param` was not bound before planning. |
| `LoweringError` | `MaxNestingDepthExceeded { max_depth }` | Subquery nesting exceeded the configured maximum depth. |
| `LoweringError` | `EmptyFrom` | The `FROM` clause table name is an empty string. |
| `PlanError` | `NoPhysicalImpl { variant }` | `LogicalPlan::Union` reached `select_physical_plan` — no physical union algorithm exists. |
| `BuildError` | `NoScanInSubtree { side, plan_debug }` | A join arm (left or right) contains no `Source` / `Scan` node. |
| `BuildError` | `DuplicateTableRoute { table }` | Both arms of a join resolve to the same source table name. |
| `BuildError` | `UnsupportedPlanNode { variant }` | `Distinct`, `HashAggregate`, `Sort`, or `Union` node reached `compile_plan` — no physical operator implemented. |

---

## End-to-End Pipeline Example

The following example walks through the complete four-layer pipeline: AST lowering → optimization → physical plan selection → DAG compilation → streaming event processing.

```rust
use std::collections::HashMap;
use std::sync::Arc;

use spooky_streaming_processor::engine::{Event, Operator, Weight};
use spooky_streaming_processor::engine::builder::build_graph;
use spooky_streaming_processor::engine::sink::SinkOp;
use spooky_streaming_processor::lowering::{
    lower_select, MockSelectStmt, MockCond, MockOp, MockExpr,
};
use spooky_streaming_processor::optimizer::optimize;
use spooky_streaming_processor::physical_plan::select_physical_plan;
use spooky_streaming_processor::plan::{JoinType, LogicalPlan, Predicate, Value};

fn main() {
    // ── Step 1: Build a LogicalPlan manually (or via lower_select) ───────────
    //
    // Query: SELECT * FROM users JOIN orders ON users.id = orders.user_id
    //        WHERE users.age > 18
    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan { table: "users".into() }),
            predicate: Predicate::Gt("age".into(), Value::Int(18)),
        }),
        right: Box::new(LogicalPlan::Scan { table: "orders".into() }),
        on: ("id".into(), "user_id".into()),
        kind: JoinType::Inner,
    };

    // ── Step 2: Optimize ─────────────────────────────────────────────────────
    // The optimizer pushes the Filter below the Join since "age" is on the
    // left side. (Already placed there manually above, so this is a no-op here.)
    let plan = optimize(plan);

    // ── Step 3: Select physical plan ─────────────────────────────────────────
    let physical = select_physical_plan(plan.clone())
        .expect("no Union in plan; should not fail");

    // ── Step 4: Compile to a push-based operator DAG ─────────────────────────
    let sink = Box::new(SinkOp::new("OUTPUT"));
    let mut graph = build_graph(plan, sink).expect("compilation should succeed");
    // graph is a DispatcherOp routing "users" and "orders" events.

    // ── Step 5: Push streaming events ────────────────────────────────────────
    // Helper to build a Record from key-value pairs:
    let rec = |pairs: &[(&str, Value)]| -> HashMap<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    };

    // INSERT users: Alice (passes age > 18 filter)
    graph.on_event(Event {
        source: Arc::from("users"),
        record: rec(&[
            ("id", Value::Int(1)),
            ("name", Value::Str("Alice".into())),
            ("age", Value::Int(30)),
        ]),
        weight: Weight::INSERT,
    });

    // INSERT users: Bob (age=15, filtered out — no join output)
    graph.on_event(Event {
        source: Arc::from("users"),
        record: rec(&[
            ("id", Value::Int(2)),
            ("name", Value::Str("Bob".into())),
            ("age", Value::Int(15)),
        ]),
        weight: Weight::INSERT,
    });

    // INSERT orders: order for Alice
    graph.on_event(Event {
        source: Arc::from("orders"),
        record: rec(&[
            ("order_id", Value::Int(100)),
            ("user_id", Value::Int(1)),
            ("item", Value::Str("Book".into())),
        ]),
        weight: Weight::INSERT,
    });
    // Output: [OUTPUT] weight=+1  { age=Int(30), id=Int(1), item=Str("Book"),
    //                               name=Str("Alice"), order_id=Int(100), user_id=Int(1) }

    // DELETE order: retract order 100
    graph.on_event(Event {
        source: Arc::from("orders"),
        record: rec(&[
            ("order_id", Value::Int(100)),
            ("user_id", Value::Int(1)),
            ("item", Value::Str("Book".into())),
        ]),
        weight: Weight::DELETE,
    });
    // Output: [OUTPUT] weight=-1  { age=Int(30), id=Int(1), item=Str("Book"),
    //                               name=Str("Alice"), order_id=Int(100), user_id=Int(1) }
}
```

### Expected stdout

```
[OUTPUT] weight=+1  { age=Int(30), id=Int(1), item=Str("Book"), name=Str("Alice"), order_id=Int(100), user_id=Int(1) }
[OUTPUT] weight=-1  { age=Int(30), id=Int(1), item=Str("Book"), name=Str("Alice"), order_id=Int(100), user_id=Int(1) }
```

Bob's INSERT produces no output because `age=15` fails the `age > 18` filter before reaching the join. The retraction (DELETE) produces a `-1` weight event — the downstream consumer subtracts this record from its materialized view.
