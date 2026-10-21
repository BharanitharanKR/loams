//! Parse rate: every statement through sqlparser's `ClickHouseDialect` at
//! 0.62.0 (what DataFusion 54.1 links) and 0.63.0 (the House's pin), with
//! checks that a "parsed" statement kept what the text said.

use crate::corpus::Entry;
use std::collections::BTreeMap;
use std::fmt::Write as _;

/// Keywords whose loss on a round trip means the parser dropped meaning.
const KEEP: &[&str] = &[
    "FINAL", "PREWHERE", "TOTALS", "ROLLUP", "SAMPLE", "SETTINGS", "FORMAT", "ARRAY JOIN",
    "GLOBAL", "ANY", "ASOF", "WITH FILL", "INTERPOLATE", "QUALIFY", "EXCEPT", "COLUMNS", "SEMI",
    "TTL", "CODEC", "MATERIALIZED", "ALIAS", "ON CLUSTER", "PARTITION BY", "ENGINE",
];

#[derive(Debug, Clone)]
pub struct Outcome {
    pub ok: bool,
    pub error: Option<String>,
    pub lossy: Vec<String>,
}

macro_rules! checker {
    ($modname:ident, $sp:ident) => {
        pub mod $modname {
            use super::{Outcome, KEEP};
            use $sp::ast::{JoinOperator, SetExpr, Statement, TableFactor};
            use $sp::dialect::ClickHouseDialect;
            use $sp::parser::Parser;

            pub fn check(sql: &str) -> Outcome {
                let d = ClickHouseDialect {};
                let stmts = match Parser::parse_sql(&d, sql) {
                    Ok(s) => s,
                    Err(e) => {
                        return Outcome { ok: false, error: Some(e.to_string()), lossy: vec![] };
                    }
                };
                let mut lossy = Vec::new();
                let printed: String =
                    stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("; ");
                match Parser::parse_sql(&d, &printed) {
                    Ok(again) if again == stmts => {}
                    Ok(_) => lossy.push("roundtrip_ast_differs".to_string()),
                    Err(_) => lossy.push("roundtrip_reparse_fails".to_string()),
                }
                let src = super::strip_literals(sql).to_uppercase();
                let out = super::strip_literals(&printed).to_uppercase();
                for kw in KEEP {
                    if super::count_kw(&src, kw) > super::count_kw(&out, kw) {
                        lossy.push(format!("dropped:{kw}"));
                    }
                }
                for s in &stmts {
                    final_and_array_join(s, &mut lossy);
                }
                Outcome { ok: true, error: None, lossy }
            }

            fn final_and_array_join(s: &Statement, lossy: &mut Vec<String>) {
                let Statement::Query(q) = s else { return };
                let mut stack = vec![q.body.as_ref()];
                while let Some(body) = stack.pop() {
                    match body {
                        SetExpr::Select(sel) => {
                            for twj in &sel.from {
                                let mut factors = vec![&twj.relation];
                                factors.extend(twj.joins.iter().map(|j| &j.relation));
                                for f in factors {
                                    if let TableFactor::Table { alias: Some(a), .. } = f {
                                        if a.name.value.eq_ignore_ascii_case("final") {
                                            lossy.push("final_parsed_as_alias".to_string());
                                        }
                                    }
                                }
                            }
                            let has_aj = sel.from.first().is_some_and(|t| {
                                t.joins.iter().any(|j| {
                                    matches!(
                                        j.join_operator,
                                        JoinOperator::ArrayJoin
                                            | JoinOperator::LeftArrayJoin
                                            | JoinOperator::InnerArrayJoin
                                    )
                                })
                            });
                            if has_aj && sel.from.len() > 1 {
                                lossy.push("array_join_list_parsed_as_cross_join".to_string());
                            }
                        }
                        SetExpr::SetOperation { left, right, .. } => {
                            stack.push(left);
                            stack.push(right);
                        }
                        SetExpr::Query(q) => stack.push(q.body.as_ref()),
                        _ => {}
                    }
                }
            }
        }
    };
}

checker!(v062, sqlparser);
checker!(v063, sqlparser063);

pub fn strip_literals(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_str = false;
    let mut prev = '\0';
    for c in s.chars() {
        if c == '\'' && prev != '\\' {
            in_str = !in_str;
            out.push(' ');
        } else if !in_str {
            out.push(c);
        }
        prev = c;
    }
    out
}

pub fn count_kw(hay: &str, kw: &str) -> usize {
    let words: Vec<&str> = hay
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .collect();
    let k: Vec<&str> = kw.split(' ').collect();
    if words.len() < k.len() {
        return 0;
    }
    (0..=words.len() - k.len()).filter(|&i| words[i..i + k.len()] == k[..]).count()
}

pub struct Row {
    pub entry: Entry,
    pub v062: Outcome,
    pub v063: Outcome,
}

pub fn run(entries: Vec<Entry>) -> Vec<Row> {
    entries
        .into_iter()
        .map(|e| {
            let a = v062::check(&e.sql);
            let b = v063::check(&e.sql);
            Row { entry: e, v062: a, v063: b }
        })
        .collect()
}

fn category(id: &str) -> String {
    if let Some((dir, _)) = id.split_once('/') {
        return format!("hs1:{dir}");
    }
    let c = id.split('_').next().unwrap_or("other");
    // plan20.sql's q01..q20 are SELECTs.
    if c.len() == 3 && c.starts_with('q') && c[1..].chars().all(|d| d.is_ascii_digit()) {
        return "sel".to_string();
    }
    c.to_string()
}

pub fn report(rows: &[Row], title: &str) -> String {
    let mut s = String::new();
    let n = rows.len();
    let ok62 = rows.iter().filter(|r| r.v062.ok).count();
    let ok63 = rows.iter().filter(|r| r.v063.ok).count();
    let clean63 = rows.iter().filter(|r| r.v063.ok && r.v063.lossy.is_empty()).count();
    let clean62 = rows.iter().filter(|r| r.v062.ok && r.v062.lossy.is_empty()).count();
    let _ = writeln!(s, "## {title}\n");
    let _ = writeln!(s, "| | statements | parse 0.62 | parse 0.63 | faithful 0.62 | faithful 0.63 |");
    let _ = writeln!(s, "|---|---|---|---|---|---|");
    let mut cats: BTreeMap<String, Vec<&Row>> = BTreeMap::new();
    for r in rows {
        cats.entry(category(&r.entry.id)).or_default().push(r);
    }
    for (c, rs) in &cats {
        let k = rs.len();
        let _ = writeln!(
            s,
            "| {c} | {k} | {} | {} | {} | {} |",
            rs.iter().filter(|r| r.v062.ok).count(),
            rs.iter().filter(|r| r.v063.ok).count(),
            rs.iter().filter(|r| r.v062.ok && r.v062.lossy.is_empty()).count(),
            rs.iter().filter(|r| r.v063.ok && r.v063.lossy.is_empty()).count(),
        );
    }
    let pct = |a: usize| if n == 0 { 0.0 } else { 100.0 * a as f64 / n as f64 };
    let _ = writeln!(
        s,
        "| **all** | **{n}** | **{ok62} ({:.1} %)** | **{ok63} ({:.1} %)** | **{clean62} ({:.1} %)** | **{clean63} ({:.1} %)** |\n",
        pct(ok62),
        pct(ok63),
        pct(clean62),
        pct(clean63)
    );
    let _ = writeln!(s, "### Failures and lossy parses (0.63; 0.62 where it differs)\n");
    let _ = writeln!(s, "| id | 0.63 | 0.62 |");
    let _ = writeln!(s, "|---|---|---|");
    for r in rows {
        let a = describe(&r.v063);
        let b = describe(&r.v062);
        if !(r.v063.ok && r.v063.lossy.is_empty() && r.v062.ok && r.v062.lossy.is_empty()) {
            let b = if a == b { "same".to_string() } else { b };
            let _ = writeln!(s, "| `{}` | {} | {} |", r.entry.id, a, b);
        }
    }
    s
}

fn describe(o: &Outcome) -> String {
    if !o.ok {
        let e = o.error.clone().unwrap_or_default().replace('|', "\\|");
        let e: String = e.chars().take(110).collect();
        return format!("fails: {e}");
    }
    if o.lossy.is_empty() {
        "ok".to_string()
    } else {
        format!("lossy: {}", o.lossy.join(", "))
    }
}

pub fn feature_table(rows: &[Row]) -> String {
    let mut by: BTreeMap<String, (usize, usize, usize)> = BTreeMap::new();
    for r in rows {
        for f in &r.entry.features {
            let e = by.entry(f.clone()).or_default();
            e.0 += 1;
            if r.v063.ok {
                e.1 += 1;
            }
            if r.v063.ok && r.v063.lossy.is_empty() {
                e.2 += 1;
            }
        }
    }
    let mut s = String::from("| feature | statements | parse 0.63 | faithful 0.63 |\n|---|---|---|---|\n");
    for (f, (n, ok, clean)) in by {
        if clean < n {
            let _ = writeln!(s, "| `{f}` | {n} | {ok} | {clean} |");
        }
    }
    s
}
