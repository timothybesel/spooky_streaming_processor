mod engine;
mod lowering;
mod optimizer;
mod parser;
mod physical_plan;
mod plan;
mod real_lowering;

use std::collections::HashMap;
use std::sync::Arc;

use engine::builder::build_graph;
use engine::sink::SinkOp;
use engine::{Event, Weight};
use lowering::{lower_select, LoweringError, SelectCond, SelectExpr, SelectOp, SelectStmt};
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
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
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

    let ast = SelectStmt {
        table: "Suppliers".into(),
        select_fields: vec!["SupplierName".into()],
        cond: Some(SelectCond::BinaryOp {
            op: SelectOp::Inside,
            left: SelectExpr::Column("SupplierID".into()),
            right: SelectExpr::Subquery(Box::new(SelectStmt {
                table: "Products".into(),
                select_fields: vec!["SupplierID".into()],
                cond: Some(SelectCond::BinaryOp {
                    op: SelectOp::Lt,
                    left: SelectExpr::Column("Price".into()),
                    right: SelectExpr::Literal(Value::Int(20)),
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
    let and_ast = SelectStmt {
        table: "users".into(),
        select_fields: vec![],
        cond: Some(SelectCond::And(
            Box::new(SelectCond::BinaryOp {
                op: SelectOp::Gt,
                left: SelectExpr::Column("age".into()),
                right: SelectExpr::Literal(Value::Int(18)),
            }),
            Box::new(SelectCond::BinaryOp {
                op: SelectOp::Eq,
                left: SelectExpr::Column("active".into()),
                right: SelectExpr::Literal(Value::Bool(true)),
            }),
        )),
    };
    match lower_select(and_ast) {
        Ok(plan) => println!("{plan:#?}\n"),
        Err(e) => eprintln!("Lowering error: {e}"),
    }

    // ── Error path: Inside with scalar ────────────────────────────────────
    println!("── Error: INSIDE with scalar (should fail) ──────────────────");
    let bad_ast = SelectStmt {
        table: "users".into(),
        select_fields: vec![],
        cond: Some(SelectCond::BinaryOp {
            op: SelectOp::Inside,
            left: SelectExpr::Column("id".into()),
            right: SelectExpr::Literal(Value::Int(42)),
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

// ─── Demo 3: SurrealQL SELECT parser ─────────────────────────────────────────

/// Demonstrates the new winnow-based SELECT parser:
///   parse_select(query_str) → LogicalPlan
fn demo_parser() {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║  Demo 3 – SurrealQL SELECT Parser (winnow)              ║");
    println!("╚══════════════════════════════════════════════════════════╝");

    let queries = [
        "SELECT * FROM users",
        "SELECT name, age FROM users",
        "select * from orders",
        "SELECT id FROM order_items",
    ];

    for q in &queries {
        print!("  {q:<45}  →  ");
        match parser::parse_select(q) {
            Ok(plan) => println!("Ok  {plan:?}"),
            Err(e) => println!("Err {e}"),
        }
    }

    println!("\n── Error cases ──────────────────────────────────────────────");
    let bad = ["FROM users", "SELECT * WHERE age > 18", ""];
    for q in &bad {
        print!("  {q:<45}  →  ");
        match parser::parse_select(q) {
            Ok(plan) => println!("Ok  {plan:?}  (unexpected!)"),
            Err(e) => println!("Err (expected): {e}"),
        }
    }
    println!();
}

fn main() {
    demo_engine();
    demo_lowering();
    demo_parser();
}
