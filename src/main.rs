mod engine;
mod lowering;
mod optimizer;
mod physical_plan;
mod plan;
mod real_lowering;

use std::collections::HashMap;
use std::sync::Arc;

use surrealdb_core::syn;

use engine::builder::build_graph;
use engine::sink::SinkOp;
use engine::{Event, Weight};
use lowering::{LoweringError, MockCond, MockExpr, MockOp, MockSelectStmt, lower_select};
use optimizer::optimize;
use physical_plan::select_physical_plan;
use plan::{JoinType, LogicalPlan, Predicate, Value};

// ─── Demo 1: DBSP engine ──────────────────────────────────────────────────────

/// Demonstrates the push-based engine with a manually constructed plan for:
///
///   SELECT * FROM users JOIN orders ON users.id = orders.user_id
///   WHERE users.age > 18
fn demo_engine() {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║  Demo 1 – DBSP Push-based Engine                        ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("Query: SELECT * FROM users JOIN orders");
    println!("       ON users.id = orders.user_id WHERE users.age > 18\n");

    // ── Step 1: Build the Logical Plan ────────────────────────────────────
    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".into(),
            }),
            predicate: Predicate::Gt("age".into(), Value::Int(18)),
        }),
        right: Box::new(LogicalPlan::Scan {
            table: "orders".into(),
        }),
        on: ("id".into(), "user_id".into()),
        kind: JoinType::Inner,
    };

    println!("── Logical Plan ─────────────────────────────────────────────");
    println!("{plan:#?}\n");

    // ── Step 2: Optimize ─────────────────────────────────────────────────
    let plan = optimize(plan);

    // ── Step 3: Select physical plan ─────────────────────────────────────
    let _physical = match select_physical_plan(plan.clone()) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Plan error: {e}");
            return;
        }
    };

    // ── Step 4: Compile to a physical DAG ────────────────────────────────
    let sink = Box::new(SinkOp::new("OUTPUT"));
    let mut graph = match build_graph(plan, sink) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("Build error: {e}");
            return;
        }
    };

    // ── Step 5: Simulate streaming events ────────────────────────────────
    println!("── Streaming Events ─────────────────────────────────────────");

    let rec = |pairs: &[(&str, Value)]| -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    };

    println!("→ INSERT users: Alice (id=1, age=30)");
    graph.on_event(Event {
        source: Arc::from("users"),
        record: rec(&[
            ("id", Value::Int(1)),
            ("name", Value::Str("Alice".into())),
            ("age", Value::Int(30)),
        ]),
        weight: Weight::INSERT,
    });

    println!("→ INSERT users: Bob   (id=2, age=15)  ← filtered by age>18");
    graph.on_event(Event {
        source: Arc::from("users"),
        record: rec(&[
            ("id", Value::Int(2)),
            ("name", Value::Str("Bob".into())),
            ("age", Value::Int(15)),
        ]),
        weight: Weight::INSERT,
    });

    println!("→ INSERT orders: #100 (user_id=1, item=Book)");
    graph.on_event(Event {
        source: Arc::from("orders"),
        record: rec(&[
            ("order_id", Value::Int(100)),
            ("user_id", Value::Int(1)),
            ("item", Value::Str("Book".into())),
        ]),
        weight: Weight::INSERT,
    });

    println!("→ INSERT orders: #101 (user_id=1, item=Pen)");
    graph.on_event(Event {
        source: Arc::from("orders"),
        record: rec(&[
            ("order_id", Value::Int(101)),
            ("user_id", Value::Int(1)),
            ("item", Value::Str("Pen".into())),
        ]),
        weight: Weight::INSERT,
    });

    println!("→ INSERT users: Carol (id=1, age=25)  ← late arrival");
    graph.on_event(Event {
        source: Arc::from("users"),
        record: rec(&[
            ("id", Value::Int(1)),
            ("name", Value::Str("Carol".into())),
            ("age", Value::Int(25)),
        ]),
        weight: Weight::INSERT,
    });

    println!("→ DELETE orders: #100 (user_id=1, item=Book)  ← retraction");
    graph.on_event(Event {
        source: Arc::from("orders"),
        record: rec(&[
            ("order_id", Value::Int(100)),
            ("user_id", Value::Int(1)),
            ("item", Value::Str("Book".into())),
        ]),
        weight: Weight::DELETE,
    });

    println!();
}

// ─── Demo 2: AST Lowering ─────────────────────────────────────────────────────

/// Demonstrates the lowering pass: a mock SurrealDB AST is converted into a
/// flat `LogicalPlan` where the `IN (subquery)` becomes a `LeftSemi` join.
fn demo_lowering() {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║  Demo 2 – AST Lowering (decorrelation + AND/OR)         ║");
    println!("╚══════════════════════════════════════════════════════════╝");

    // ── IN (subquery) decorrelation ───────────────────────────────────────
    println!("Query: SELECT SupplierName FROM Suppliers");
    println!("       WHERE SupplierID IN");
    println!("         (SELECT SupplierID FROM Products WHERE Price < 20)\n");

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
        Ok(plan) => {
            println!("── Lowered LogicalPlan ─────────────────────────────────────");
            println!("{plan:#?}\n");
        }
        Err(e) => eprintln!("Lowering error: {e}"),
    }

    // ── AND condition ─────────────────────────────────────────────────────
    println!("── AND condition: WHERE age > 18 AND active = true ──────────");
    let and_ast = MockSelectStmt {
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
    match lower_select(and_ast) {
        Ok(plan) => println!("{plan:#?}\n"),
        Err(e) => eprintln!("Lowering error: {e}"),
    }

    // ── Error path: Inside with scalar ────────────────────────────────────
    println!("── Error: INSIDE with scalar (should fail) ──────────────────");
    let bad_ast = MockSelectStmt {
        table: "users".into(),
        select_fields: vec![],
        cond: Some(MockCond::BinaryOp {
            op: MockOp::Inside,
            left: MockExpr::Column("id".into()),
            right: MockExpr::Literal(Value::Int(42)),
        }),
    };
    match lower_select(bad_ast) {
        Ok(_) => eprintln!("BUG: should have returned error"),
        Err(LoweringError::InsideRequiresSubquery { col }) => {
            println!("✓ Correct error: InsideRequiresSubquery {{ col: {col:?} }}\n")
        }
        Err(e) => eprintln!("Unexpected error: {e}"),
    }
}

// ─── Demo 3: Real SurrealDB parser ───────────────────────────────────────────

/// Parses a query with the actual `surrealdb_core` parser and prints the AST.
///
/// Note: the `surrealdb_core` 3.0.0 SQL types are `pub(crate)` only, so the
/// parsed AST cannot be inspected or lowered from this crate.
/// See `src/real_lowering.rs` for the migration path.
fn demo_real_parser() {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║  Demo 3 – Real SurrealDB parser output                  ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("(AST types are pub(crate) in surrealdb-core 3.0.0 — ");
    println!(" inspection from external crates is not yet possible)\n");

    let query = "SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author, (SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author FROM comment WHERE thread=$parent.id ORDER BY created_at desc LIMIT 10) AS comments FROM thread WHERE id = $id LIMIT 1;";

    println!("SQL: {query}\n");

    match syn::parse(query) {
        Ok(ast) => println!("{ast:#?}"),
        Err(e) => eprintln!("Parse error: {e:?}"),
    }
    println!();
}

fn main() {
    demo_engine();
    demo_lowering();
    demo_real_parser();
}
