mod parser;
use parser::*;

fn main() {
    let q1 =
        "SELECT name, (SELECT product, address FROM order WHERE price > 10.00) AS order FROM user";
    println!("Q1: {:?}\n", parse_select_statement(&mut q1.trim()));

    let q2 = "SELECT * FROM user WHERE age > 18 AND active = true";
    println!("Q2: {:?}\n", parse_select_statement(&mut q2.trim()));

    let q3 = "SELECT * FROM user WHERE (age >= 18 OR role = 'admin') AND deleted = null";
    println!("Q3: {:?}\n", parse_select_statement(&mut q3.trim()));

    // Parameter reference: $auth.id
    let q4 = "SELECT * FROM post WHERE author = $auth.id";
    println!("Q4: {:?}\n", parse_select_statement(&mut q4.trim()));

    // $parent in subquery
    let q5 = "SELECT *, (SELECT * FROM event WHERE host = $parent.id) AS hosted FROM user";
    println!("Q5: {:?}\n", parse_select_statement(&mut q5.trim()));

    // Range: IN 18..=65
    let q6 = "SELECT * FROM user WHERE age IN 18..=65";
    println!("Q6: {:?}\n", parse_select_statement(&mut q6.trim()));
}
