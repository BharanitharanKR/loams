//! Performance benches against the real systems (CN2 Task 27). Each runs
//! the family's conformance harness through the kit's harness bench and
//! writes its report for `connectors-bench`, which checks it against
//! `conformance/connectors/bench/baselines.toml`. Gated on
//! `LOAMS_CONNECTORS_BENCH` and the system's stack, so the PR shards skip
//! them.

use loams_connector_postgres::Target;
use loams_connector_postgres::harness::PgHarness;
use loams_flow_conformance::bench::{
    BenchPlan, BenchReport, bench_enabled, run_harness_bench, write_report,
};

async fn bench(h: PgHarness, id: &str, direction: &str, system: &str) {
    let points = run_harness_bench(&h, &BenchPlan::default())
        .await
        .unwrap_or_else(|e| panic!("{id} {direction} bench: {e}"));
    let report = BenchReport::new(id, direction, system, points);
    eprintln!("{report:#?}");
    write_report(&report).expect("the report is written");
}

#[tokio::test(flavor = "multi_thread")]
async fn bench_postgresql_cdc() {
    if !bench_enabled("bench_postgresql_cdc", &["LOAMS_CONNECTORS_P2_STACK"]) {
        return;
    }
    let h = PgHarness::cdc(Target::Postgres, "bench_cdc");
    bench(h, "postgresql", "source", "PostgreSQL 17 (pgoutput)").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bench_postgresql_sink() {
    if !bench_enabled("bench_postgresql_sink", &["LOAMS_CONNECTORS_P2_STACK"]) {
        return;
    }
    let h = PgHarness::sink(Target::Postgres, "bench_sink", true);
    bench(h, "postgresql", "sink", "PostgreSQL 17 (exactly-once sink)").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bench_pgvector_batch() {
    if !bench_enabled("bench_pgvector_batch", &["LOAMS_CONNECTORS_P2_STACK"]) {
        return;
    }
    let h = PgHarness::batch(Target::Pgvector, "bench_batch");
    bench(h, "pgvector", "source", "PostgreSQL 17 with pgvector").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bench_cockroachdb_cdc() {
    if !bench_enabled("bench_cockroachdb_cdc", &["LOAMS_CONNECTORS_CI_ONLY_STACK"]) {
        return;
    }
    let h = PgHarness::cdc(Target::Cockroach, "bench_feed");
    bench(h, "cockroachdb", "source", "CockroachDB (changefeed)").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bench_aurora_cdc() {
    if !bench_enabled("bench_aurora_cdc", &["LOAMS_AURORA_POSTGRES_URL"]) {
        return;
    }
    let h = PgHarness::cdc(Target::Aurora, "bench_aurora");
    bench(h, "aurora", "source", "Amazon Aurora PostgreSQL").await;
}
