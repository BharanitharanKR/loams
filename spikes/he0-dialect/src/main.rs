//! HE0 Task 10 spike: the ClickHouse dialect on DataFusion.
//!
//! `he0-dialect-spike parse <title> <file-or-dir>...` prints the parse-rate
//! report; `he0-dialect-spike plan <file> <plans-out>` plans each query raw
//! (DataFusion 54.1 as shipped, plus stub tables and functions) and through
//! the spike's proto-frontend, and prints the planning table.

mod corpus;
mod ctx;
mod parse_rate;
mod rewrite;

use datafusion_expr::LogicalPlan;
use datafusion_optimizer::{Analyzer, Optimizer, OptimizerContext};
use datafusion_sql::planner::{ParserOptions, SqlToRel};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use std::fmt::Write as _;
use std::path::PathBuf;

fn main() {
    std::panic::set_hook(Box::new(|i| eprintln!("panic: {i}")));
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("parse") => {
            let title = args.get(2).cloned().unwrap_or_default();
            let mut entries = Vec::new();
            for p in &args[3..] {
                entries.extend(corpus::load(&PathBuf::from(p)).expect("read corpus"));
            }
            let rows = parse_rate::run(entries);
            print!("{}", parse_rate::report(&rows, &title));
            let ft = parse_rate::feature_table(&rows);
            if ft.lines().count() > 2 {
                println!("\n### Features with a failing or lossy statement (0.63)\n\n{ft}");
            }
        }
        Some("plan") => {
            let mut entries = corpus::load(&PathBuf::from(&args[2])).expect("read corpus");
            let summary = args.get(4).is_some_and(|a| a == "summary");
            if summary {
                entries.retain(|e| {
                    let u = e.sql.trim_start().to_uppercase();
                    u.starts_with("SELECT") || u.starts_with("WITH") || u.starts_with('(')
                });
            }
            let out = PathBuf::from(&args[3]);
            std::fs::create_dir_all(&out).expect("plans dir");
            let mut table = String::from("| id | DataFusion 54.1 as shipped | with the proto-frontend | rewrites applied |\n|---|---|---|---|\n");
            let (mut raw_ok, mut raw_silent, mut fe_ok) = (0, 0, 0);
            for e in &entries {
                let sql = e.sql.clone();
                let raw = std::panic::catch_unwind(|| plan_raw(&sql)).unwrap_or_else(|_| "**panics** (DataFusion)".into());
                let sql = e.sql.clone();
                let (fe, applied, plan) = std::panic::catch_unwind(|| plan_frontend(&sql))
                    .unwrap_or_else(|_| ("**panics** (DataFusion)".into(), String::new(), None));
                if raw.starts_with("plans") {
                    raw_ok += 1;
                }
                if raw.starts_with("**silent") {
                    raw_silent += 1;
                }
                if fe.starts_with("plans") {
                    fe_ok += 1;
                }
                if let Some(p) = plan {
                    std::fs::write(out.join(format!("{}.txt", e.id.replace('/', "__"))), format!("{}\n\n{p}\n", e.sql)).expect("write plan");
                }
                let _ = writeln!(table, "| `{}` | {} | {} | {} |", e.id, cell(&raw), cell(&fe), cell(&applied));
            }
            let n = entries.len();
            if summary {
                let fails: Vec<&str> = table.lines().skip(2).filter(|l| !l.contains("| plans |")).collect();
                let head: Vec<&str> = table.lines().take(2).collect();
                println!("{}\n{}\n", head.join("\n"), fails.join("\n"));
            } else {
                println!("{table}");
            }
            println!("Raw: {raw_ok}/{n} plan correctly, {raw_silent}/{n} plan but silently drop a construct. Proto-frontend: {fe_ok}/{n} plan.");
        }
        _ => eprintln!("usage: he0-dialect-spike parse <title> <paths>... | plan <file> <plans-dir>"),
    }
}

fn cell(s: &str) -> String {
    s.replace('|', "\\|").replace('\n', " ")
}

fn opts() -> ParserOptions {
    ParserOptions { enable_ident_normalization: false, ..ParserOptions::default() }
}

fn analyse(plan: LogicalPlan, ctx: &ctx::Ctx) -> Result<LogicalPlan, String> {
    use datafusion_expr::planner::ContextProvider;
    let analysed = Analyzer::new()
        .execute_and_check(plan, ctx.options(), |_, _| {})
        .map_err(|e| format!("analyzer: {}", first_line(&e.to_string())))?;
    Optimizer::new()
        .optimize(analysed, &ctx::OptCfg { inner: OptimizerContext::new(), reg: ctx }, |_, _| {})
        .map_err(|e| format!("optimizer: {}", first_line(&e.to_string())))
}

fn first_line(s: &str) -> String {
    let l = s.lines().next().unwrap_or("").to_string();
    l.chars().take(140).collect()
}

/// DataFusion as shipped: parse, plan, analyse. A plan that succeeds while
/// the AST holds a construct the planner ignores is reported as silent.
fn plan_raw(sql: &str) -> String {
    let stmt = match Parser::parse_sql(&ClickHouseDialect {}, sql) {
        Ok(mut v) => v.remove(0),
        Err(e) => return format!("parse fails: {}", first_line(&e.to_string())),
    };
    let det = rewrite::detect(&stmt);
    let ctx = ctx::Ctx::new(false);
    let planner = SqlToRel::new_with_options(&ctx, opts());
    match planner.sql_statement_to_plan(stmt).map_err(|e| first_line(&e.to_string())).and_then(|p| analyse(p, &ctx)) {
        Ok(_) if !det.silent.is_empty() => {
            format!("**silently drops {}**", det.silent.iter().cloned().collect::<Vec<_>>().join(", "))
        }
        Ok(_) => "plans".to_string(),
        Err(e) => format!("fails: {e}"),
    }
}

fn plan_frontend(sql: &str) -> (String, String, Option<String>) {
    let (sql, params) = rewrite::substitute_params(sql);
    let mut stmt = match Parser::parse_sql(&ClickHouseDialect {}, &sql) {
        Ok(mut v) => v.remove(0),
        Err(e) => return (format!("parse fails: {}", first_line(&e.to_string())), String::new(), None),
    };
    let rw = rewrite::rewrite(&mut stmt);
    let det = rewrite::detect(&stmt);
    let mut applied: Vec<String> = rw.applied.iter().cloned().collect();
    if params {
        applied.insert(0, "query parameters substituted (the front's job)".into());
    }
    if det.silent.iter().any(|s| s.starts_with("FINAL")) {
        applied.push("FINAL → ReplacingFinal extension node (RelationPlanner)".into());
    }
    if !det.front_owned.is_empty() {
        applied.push(format!("{} left to the front", det.front_owned.iter().cloned().collect::<Vec<_>>().join(", ")));
    }
    let mut applied = applied.join("; ");
    if !rw.notes.is_empty() {
        let _ = write!(applied, ". Notes: {}", rw.notes.iter().cloned().collect::<Vec<_>>().join("; "));
    }
    let ctx = ctx::Ctx::new(true);
    let planner = SqlToRel::new_with_options(&ctx, opts());
    let rewritten = stmt.to_string();
    match planner.sql_statement_to_plan(stmt).map_err(|e| first_line(&e.to_string())).and_then(|p| analyse(p, &ctx)) {
        Ok(p) => {
            let leftover: Vec<String> = det.silent.iter().filter(|s| !s.starts_with("FINAL")).cloned().collect();
            let status = if leftover.is_empty() { "plans".to_string() } else { format!("**silently drops {}**", leftover.join(", ")) };
            (status, applied, Some(format!("-- rewritten:\n{rewritten}\n\n-- optimized logical plan:\n{}", p.display_indent())))
        }
        Err(e) => (format!("fails: {e}"), applied, Some(format!("-- rewritten:\n{rewritten}\n\n-- error: {e}"))),
    }
}
