//! Performance benches against the real systems (CN2 Task 27). Each runs
//! the search harness (a source index copied to a sink index through the
//! connector) through the kit's harness bench and writes its report for
//! `connectors-bench`, which checks it against
//! `conformance/connectors/bench/baselines.toml`. Gated on
//! `LOAMS_CONNECTORS_BENCH` and the system's stack, so the PR shards skip
//! them.

use loams_connector_search::conformance::SearchHarness;
use loams_flow_conformance::bench::{
    BenchPlan, BenchReport, bench_enabled, run_harness_bench, write_report,
};

async fn bench(h: SearchHarness, id: &str, system: &str) {
    let points = run_harness_bench(&h, &BenchPlan::default())
        .await
        .unwrap_or_else(|e| panic!("{id} bench: {e}"));
    let report = BenchReport::new(id, "roundtrip", system, points);
    eprintln!("{report:#?}");
    write_report(&report).expect("the report is written");
}

#[tokio::test(flavor = "multi_thread")]
async fn bench_opensearch() {
    if !bench_enabled("bench_opensearch", &["LOAMS_CONNECTORS_P2_STACK"]) {
        return;
    }
    bench(SearchHarness::opensearch(), "opensearch", "OpenSearch 3").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bench_qdrant() {
    if !bench_enabled("bench_qdrant", &["LOAMS_CONNECTORS_P2_STACK"]) {
        return;
    }
    bench(SearchHarness::qdrant(), "qdrant", "Qdrant").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bench_elasticsearch() {
    if !bench_enabled("bench_elasticsearch", &["LOAMS_CONNECTORS_CI_ONLY_STACK"]) {
        return;
    }
    bench(
        SearchHarness::elasticsearch(),
        "elasticsearch",
        "Elasticsearch 9",
    )
    .await;
}
