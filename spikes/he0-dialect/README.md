# HE0 Task 10 spike: the ClickHouse dialect on DataFusion

**Never merged.** Branch `he0-t10-dialect-spike`, kept for reference. The report
is `docs/plans/he0-dialect-spike-report.md` on `dev`.

A workspace of its own (DataFusion 54.1.0, arrow 58, sqlparser 0.62.0 and
0.63.0), so no DataFusion dependency enters `fabric/` or the engine workspace.

- `corpus/tier0.sql`: 175 hand-written statements in the shapes of design 75
  §7.3's Tier 0 list. `corpus/plan20.sql`: the 20 planning queries. No
  application source, no ClickBench text.
- `src/parse_rate.rs`: parses each statement with `ClickHouseDialect` at 0.62
  and 0.63 and checks the parse kept what the text said (round trip, keyword
  survival, `FINAL` read as an alias, `ARRAY JOIN` lists read as cross joins).
- `src/ctx.rs`: stub tables, stub ClickHouse functions (planning only), a
  ClickHouse `TypePlanner`, a `RelationPlanner` that plans `FROM t FINAL` as a
  `ReplacingFinal` extension node.
- `src/rewrite.rs`: the proto-frontend (an AST pass) and the detector for
  constructs DataFusion's planner drops without an error.
- `results/`: the outputs the report quotes. `plans/`: each of the 20 queries,
  rewritten, with its optimized logical plan.

```
cargo build                       # from this directory; output in the shared target
B=/mnt/Projects/rust-cache/target/debug/he0-dialect-spike
$B parse "Tier 0" corpus/tier0.sql corpus/plan20.sql
$B plan corpus/plan20.sql plans
$B plan corpus/tier0.sql <dir> summary
cargo tree --features iceberg-check -e normal -d   # one arrow (58) with iceberg 0.10.1
```

`version-check/df55-iceberg0101/` is a `cargo tree`-only manifest showing that
DataFusion 55.2 beside iceberg 0.10.1 brings two Arrow majors (58 and 59).
