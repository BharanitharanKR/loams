//! The spike's proto-frontend: an AST pass over sqlparser 0.62 that turns
//! ClickHouse constructs into SQL DataFusion plans, plus a detector for the
//! constructs DataFusion's planner ignores without an error.

use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    GroupByWithModifier, Ident, JoinOperator, LimitClause, ObjectName, ObjectNamePart, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, Visit, VisitMut, Visitor, VisitorMut,
    visit_expressions_mut,
};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;

// ------------------------------------------------------------------ detector

/// Constructs present in the AST that DataFusion 54.1's `SqlToRel` drops
/// without an error (read from its source: `select.prewhere` is never read,
/// `GroupByExpr::Expressions(exprs, _)` discards the modifiers, and
/// `FunctionArgs::try_new` destructures `Function { .. }` without
/// `parameters`), or that the front owns.
#[derive(Default)]
pub struct Detect {
    pub silent: BTreeSet<String>,
    pub front_owned: BTreeSet<String>,
}

impl Visitor for Detect {
    type Break = ();
    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        if q.settings.is_some() {
            self.front_owned.insert("SETTINGS".into());
        }
        if q.format_clause.is_some() {
            self.front_owned.insert("FORMAT".into());
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_select(&mut self, s: &Select) -> ControlFlow<()> {
        if s.prewhere.is_some() {
            self.silent.insert("PREWHERE".into());
        }
        if let GroupByExpr::Expressions(_, m) = &s.group_by {
            for m in m {
                self.silent.insert(format!("GROUP BY {m}"));
            }
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { alias, sample, .. } = t {
            if alias.as_ref().is_some_and(|a| a.name.value.eq_ignore_ascii_case("final")) {
                self.silent.insert("FINAL (read as a table alias)".into());
            }
            if sample.is_some() {
                self.silent.insert("SAMPLE".into());
            }
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = e {
            if let FunctionArguments::List(p) = &f.parameters {
                if !p.args.is_empty() {
                    self.silent.insert(format!("parameters of {}(...)(...)", fname(f)));
                }
            }
        }
        ControlFlow::Continue(())
    }
}

pub fn detect(stmt: &Statement) -> Detect {
    let mut d = Detect::default();
    let _ = stmt.visit(&mut d);
    d
}

// ------------------------------------------------------------------ helpers

fn fname(f: &Function) -> String {
    f.name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.clone()).unwrap_or_default()
}

fn set_fname(f: &mut Function, n: &str) {
    f.name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(n))]);
}

fn args_mut(f: &mut Function) -> Option<&mut Vec<FunctionArg>> {
    match &mut f.args {
        FunctionArguments::List(l) => Some(&mut l.args),
        _ => None,
    }
}

fn arg_expr(a: &FunctionArg) -> Option<Expr> {
    match a {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
        _ => None,
    }
}

fn parse_expr(s: &str) -> Expr {
    Parser::new(&ClickHouseDialect {})
        .try_with_sql(s)
        .and_then(|mut p| p.parse_expr())
        .unwrap_or_else(|e| panic!("internal rewrite produced bad SQL `{s}`: {e}"))
}

fn parse_query(s: &str) -> Query {
    match Parser::parse_sql(&ClickHouseDialect {}, s) {
        Ok(mut v) => match v.remove(0) {
            Statement::Query(q) => *q,
            other => panic!("not a query: {other}"),
        },
        Err(e) => panic!("internal rewrite produced bad SQL `{s}`: {e}"),
    }
}

/// Aggregates whose `-If` form becomes `agg(args) FILTER (WHERE cond)`.
const IF_BASES: &[&str] = &[
    "count", "sum", "avg", "min", "max", "uniq", "uniqExact", "uniqCombined", "any", "anyLast",
    "argMax", "argMin", "quantile", "quantileExact", "quantileTDigest", "groupArray",
];

/// ClickHouse higher-order functions and DataFusion's equivalents (lambda
/// first in ClickHouse, last in DataFusion).
const HOFS: &[(&str, &str)] =
    &[("arrayMap", "array_transform"), ("arrayFilter", "array_filter"), ("arrayExists", "array_any_match")];

// ------------------------------------------------------------------ rewriter

#[derive(Default)]
pub struct Rewrite {
    pub applied: BTreeSet<String>,
    pub notes: BTreeSet<String>,
}

impl Rewrite {
    fn function(&mut self, f: &mut Function) {
        let name = fname(f);
        // -If combinator.
        if let Some(base) = name.strip_suffix("If") {
            if IF_BASES.contains(&base) {
                if let Some(args) = args_mut(f) {
                    if let Some(cond) = args.pop().and_then(|a| arg_expr(&a)) {
                        if base == "count" && args.is_empty() {
                            args.push(FunctionArg::Unnamed(FunctionArgExpr::Wildcard));
                        }
                        f.filter = Some(Box::new(cond));
                        set_fname(f, base);
                        self.applied.insert("-If → FILTER (WHERE …)".into());
                    }
                }
            }
        }
        // Parametric aggregates: quantile(0.9)(x) → quantile(0.9, x).
        if let FunctionArguments::List(p) = &f.parameters {
            if !p.args.is_empty() {
                let params = p.args.clone();
                f.parameters = FunctionArguments::None;
                if let Some(args) = args_mut(f) {
                    let mut all = params;
                    all.append(args);
                    *args = all;
                }
                self.applied.insert("parameters → leading arguments".into());
            }
        }
        let name = fname(f);
        // arrayJoin(x) → unnest(x).
        if name == "arrayJoin" {
            set_fname(f, "unnest");
            self.applied.insert("arrayJoin() → unnest()".into());
        }
        // DataFusion lowercases unquoted function names; ClickHouse's are
        // case-sensitive (toStartOfDay, argMax). Quote camelCase names.
        if f.name.0.len() == 1 && name.chars().any(|c| c.is_ascii_uppercase()) && !HOFS.iter().any(|(ch, _)| *ch == name) {
            if let Some(ObjectNamePart::Identifier(id)) = f.name.0.first_mut() {
                if id.quote_style.is_none() {
                    id.quote_style = Some('"');
                    self.applied.insert("camelCase function names quoted".into());
                }
            }
        }
        // Higher-order functions.
        if let Some((_, df)) = HOFS.iter().find(|(ch, _)| *ch == name) {
            if let Some(args) = args_mut(f) {
                if args.len() == 2 {
                    args.swap(0, 1);
                    set_fname(f, df);
                    self.applied.insert(format!("{name}(λ, a) → {df}(a, λ)"));
                } else {
                    self.notes.insert(format!("{name} over {} arrays: no DataFusion equivalent", args.len() - 1));
                }
            }
        }
    }

    fn select(&mut self, s: &mut Select) {
        // PREWHERE folds into WHERE (same rows; HE1 keeps it as a hint).
        if let Some(pw) = s.prewhere.take() {
            s.selection = Some(match s.selection.take() {
                Some(w) => parse_expr(&format!("({pw}) AND ({w})")),
                None => pw,
            });
            self.applied.insert("PREWHERE → WHERE".into());
        }
        // GROUP BY modifiers.
        if let GroupByExpr::Expressions(exprs, mods) = &mut s.group_by {
            if mods.contains(&GroupByWithModifier::Totals) {
                let keys = exprs.clone();
                *exprs = vec![Expr::GroupingSets(vec![keys, vec![]])];
                mods.retain(|m| *m != GroupByWithModifier::Totals);
                self.applied.insert("WITH TOTALS → GROUPING SETS ((keys), ())".into());
                self.notes.insert("WITH TOTALS: totals row must bypass ORDER BY/LIMIT and go to the format's totals slot".into());
            }
            if mods.contains(&GroupByWithModifier::Rollup) {
                let keys = exprs.clone();
                *exprs = vec![Expr::Rollup(keys.into_iter().map(|e| vec![e]).collect())];
                mods.retain(|m| *m != GroupByWithModifier::Rollup);
                self.applied.insert("WITH ROLLUP → ROLLUP(...)".into());
            }
        }
        // Aliases visible everywhere (ClickHouse scoping).
        let aliases: HashMap<String, Expr> = s
            .projection
            .iter()
            .filter_map(|i| match i {
                SelectItem::ExprWithAlias { expr, alias } => Some((alias.value.clone(), expr.clone())),
                _ => None,
            })
            .collect();
        if !aliases.is_empty() {
            let mut used = false;
            if let Some(w) = s.selection.as_mut() {
                used |= substitute(w, &aliases, None);
            }
            for item in s.projection.iter_mut() {
                if let SelectItem::ExprWithAlias { expr, alias } = item {
                    let own = alias.value.clone();
                    used |= substitute(expr, &aliases, Some(&own));
                } else if let SelectItem::UnnamedExpr(expr) = item {
                    used |= substitute(expr, &aliases, None);
                }
            }
            if used {
                self.applied.insert("select aliases substituted in WHERE / SELECT".into());
            }
        }
        // ARRAY JOIN → a derived table with unnest() in its projection.
        self.array_join(s);
    }

    fn array_join(&mut self, s: &mut Select) {
        let Some(first) = s.from.first() else { return };
        let ajs: Vec<_> = first
            .joins
            .iter()
            .filter(|j| {
                matches!(j.join_operator, JoinOperator::ArrayJoin | JoinOperator::LeftArrayJoin | JoinOperator::InnerArrayJoin)
            })
            .cloned()
            .collect();
        if ajs.is_empty() {
            return;
        }
        if ajs.iter().any(|j| matches!(j.join_operator, JoinOperator::LeftArrayJoin)) {
            self.notes.insert("LEFT ARRAY JOIN planned as ARRAY JOIN: unnest() drops empty arrays; needs an Unnest option".into());
        }
        let mut items = Vec::new();
        let mut factors: Vec<TableFactor> = ajs.iter().map(|j| j.relation.clone()).collect();
        // sqlparser parses `ARRAY JOIN a AS x, b AS y` as a FROM list.
        for extra in s.from.iter().skip(1) {
            factors.push(extra.relation.clone());
            self.notes.insert("ARRAY JOIN list re-assembled from a FROM list (sqlparser misparse)".into());
        }
        for f in &factors {
            match f {
                TableFactor::Table { name, alias, args, .. } => {
                    let e = match args {
                        Some(a) => format!("{name}({})", a.args.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", ")),
                        None => name.to_string(),
                    };
                    let al = alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| name.to_string());
                    items.push(format!("unnest({e}) AS {al}"));
                }
                other => {
                    self.notes.insert(format!("ARRAY JOIN over {other}: not handled"));
                    return;
                }
            }
        }
        let base = &first.relation;
        let base_alias = match base {
            TableFactor::Table { name, alias, .. } => {
                alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| name.to_string())
            }
            _ => "__aj".to_string(),
        };
        let inner = format!("SELECT *, {} FROM {base}", items.join(", "));
        let mut q = parse_query(&inner);
        // The new projection's function calls get the same treatment.
        let _ = VisitMut::visit(&mut q, self);
        let derived = TableFactor::Derived {
            lateral: false,
            subquery: Box::new(q),
            alias: Some(sqlparser::ast::TableAlias { explicit: true, name: Ident::new(base_alias), columns: vec![], at: None }),
            sample: None,
        };
        let mut twj = first.clone();
        twj.relation = derived;
        twj.joins.retain(|j| {
            !matches!(j.join_operator, JoinOperator::ArrayJoin | JoinOperator::LeftArrayJoin | JoinOperator::InnerArrayJoin)
        });
        s.from = vec![twj];
        self.applied.insert("ARRAY JOIN → derived table with unnest()".into());
    }

    fn limit_by(&mut self, q: &mut Query) {
        let Some(LimitClause::LimitOffset { limit, offset, limit_by }) = &q.limit_clause else { return };
        if limit_by.is_empty() {
            return;
        }
        let SetExpr::Select(sel) = q.body.as_ref() else {
            self.notes.insert("LIMIT BY over a set operation: not handled".into());
            return;
        };
        let n = limit.as_ref().map(|e| e.to_string()).unwrap_or_else(|| "1".into());
        let m = offset.as_ref().map(|o| o.value.to_string()).unwrap_or_else(|| "0".into());
        let aliases: HashMap<String, Expr> = sel
            .projection
            .iter()
            .filter_map(|i| match i {
                SelectItem::ExprWithAlias { expr, alias } => Some((alias.value.clone(), expr.clone())),
                _ => None,
            })
            .collect();
        let resolve = |e: &Expr| {
            let mut e = e.clone();
            substitute(&mut e, &aliases, None);
            e
        };
        let by: Vec<String> = limit_by.iter().map(|e| resolve(e).to_string()).collect();
        let mut order = Vec::new();
        let mut outer_order = Vec::new();
        let mut hidden = Vec::new();
        if let Some(ob) = &q.order_by {
            if let sqlparser::ast::OrderByKind::Expressions(es) = &ob.kind {
                for (i, o) in es.iter().enumerate() {
                    let e = resolve(&o.expr);
                    let opts = o.options.to_string();
                    order.push(format!("{e}{}{opts}", if opts.is_empty() { "" } else { " " }));
                    hidden.push(e.clone());
                    outer_order.push(format!("__lb_o{i}{}{opts}", if opts.is_empty() { "" } else { " " }));
                }
            }
        }
        let mut inner = (**sel).clone();
        let win = format!(
            "row_number() OVER (PARTITION BY {}{})",
            by.join(", "),
            if order.is_empty() { String::new() } else { format!(" ORDER BY {}", order.join(", ")) }
        );
        inner.projection.push(SelectItem::ExprWithAlias { expr: parse_expr(&win), alias: Ident::new("__lb_rn") });
        for (i, h) in hidden.iter().enumerate() {
            inner.projection.push(SelectItem::ExprWithAlias { expr: h.clone(), alias: Ident::new(format!("__lb_o{i}")) });
        }
        let inner_sql = inner.to_string();
        let mut excluded = vec!["__lb_rn".to_string()];
        excluded.extend((0..hidden.len()).map(|i| format!("__lb_o{i}")));
        let outer = format!(
            "SELECT * EXCEPT ({}) FROM ({inner_sql}) AS __lb WHERE __lb_rn > {m} AND __lb_rn <= {m} + {n}{}",
            excluded.join(", "),
            if outer_order.is_empty() { String::new() } else { format!(" ORDER BY {}", outer_order.join(", ")) }
        );
        let mut nq = parse_query(&outer);
        nq.with = q.with.take();
        *q = nq;
        self.applied.insert("LIMIT n BY k → row_number() window + filter".into());
    }
}

/// Replaces identifiers that name a select alias with the aliased
/// expression. `own` is the alias of the item being rewritten (no self
/// reference). Returns whether anything changed.
fn substitute(e: &mut Expr, aliases: &HashMap<String, Expr>, own: Option<&str>) -> bool {
    let mut changed = false;
    let _ = visit_expressions_mut(e, |x| {
        if let Expr::Identifier(id) = x {
            if Some(id.value.as_str()) != own {
                if let Some(r) = aliases.get(&id.value) {
                    // An alias that shadows a column of the same name in its
                    // own definition (`f(x) AS x`) is left alone.
                    if !matches!(r, Expr::Identifier(i) if i.value == id.value) {
                        *x = Expr::Nested(Box::new(r.clone()));
                        changed = true;
                    }
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    changed
}

impl VisitorMut for Rewrite {
    type Break = ();
    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        // CAST(x, 'T') → CAST(x AS T).
        if let Expr::Function(f) = e {
            if fname(f).eq_ignore_ascii_case("cast") {
                let args: Vec<Expr> = match &f.args {
                    FunctionArguments::List(l) => l.args.iter().filter_map(arg_expr).collect(),
                    _ => vec![],
                };
                if let [x, Expr::Value(v)] = args.as_slice() {
                    if let sqlparser::ast::Value::SingleQuotedString(t) = &v.value {
                        if let Ok(dt) = Parser::new(&ClickHouseDialect {}).try_with_sql(t).and_then(|mut p| p.parse_data_type()) {
                            *e = Expr::Cast {
                                kind: sqlparser::ast::CastKind::Cast,
                                expr: Box::new(x.clone()),
                                data_type: dt,
                                array: false,
                                format: None,
                            };
                            self.applied.insert("CAST(x, 'T') → CAST(x AS T)".into());
                            return ControlFlow::Continue(());
                        }
                    }
                }
            }
        }
        if let Expr::Function(f) = e {
            self.function(f);
        }
        ControlFlow::Continue(())
    }
    fn post_visit_select(&mut self, s: &mut Select) -> ControlFlow<()> {
        self.select(s);
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        self.limit_by(q);
        ControlFlow::Continue(())
    }
}

pub fn rewrite(stmt: &mut Statement) -> Rewrite {
    let mut r = Rewrite::default();
    let _ = stmt.visit(&mut r);
    r
}

/// The front substitutes `{name: Type}` query parameters before parsing
/// (ClickHouse's HTTP `param_<name>`); the spike uses typed dummy values.
pub fn substitute_params(sql: &str) -> (String, bool) {
    let mut out = String::new();
    let mut rest = sql;
    let mut any = false;
    while let Some(start) = rest.find('{') {
        let Some(end) = rest[start..].find('}') else { break };
        let inner = &rest[start + 1..start + end];
        if let Some((name, ty)) = inner.split_once(':') {
            if name.trim().chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                let ty = ty.trim();
                let lit = if ty.starts_with("String") || ty.starts_with("LowCardinality") || ty == "UUID" {
                    "'p'".to_string()
                } else if ty.starts_with("DateTime") || ty.starts_with("Date") {
                    format!("CAST('2024-01-01 00:00:00' AS {ty})")
                } else if ty.starts_with("Array(String") {
                    // Rendered as a tuple: `x IN {ids: Array(String)}`.
                    "('a', 'b')".to_string()
                } else if ty == "Identifier" {
                    "events".to_string()
                } else if ty.starts_with("UInt") || ty.starts_with("Int") {
                    "10".to_string()
                } else if ty.starts_with("Float") {
                    "1.5".to_string()
                } else {
                    format!("CAST(NULL AS {ty})")
                };
                out.push_str(&rest[..start]);
                out.push_str(&lit);
                rest = &rest[start + end + 1..];
                any = true;
                continue;
            }
        }
        out.push_str(&rest[..start + 1]);
        rest = &rest[start + 1..];
    }
    out.push_str(rest);
    (out, any)
}
