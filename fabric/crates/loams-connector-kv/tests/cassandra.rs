//! Cassandra, against real Apache Cassandra 5 in `connectors-p2` (OR-2).
//! Without `LOAMS_CONNECTORS_P2_STACK` every test here skips: nothing is
//! pulled or run locally.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::{declared, run_suites, skip, up};
use loams_connector_kv::cassandra::{CassandraFactory, RangePos};
use loams_connector_kv::harness::ChainHarness;
use loams_connector_kv::harness::cassandra::{Cassandra, contact};
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorFactory, EventBatch, InstanceKey, Scrubber, SplitState,
    StaticSecrets,
};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use serde_json::json;

const P2: &str = "LOAMS_CONNECTORS_P2_STACK";

#[tokio::test(flavor = "multi_thread")]
async fn cassandra_suite_against_cassandra() {
    let h = ChainHarness::new(Cassandra::new());
    run_suites("cassandra", &h, &declared("cassandra"), up(P2)).await;
}

async fn table_with_rows(n: i32) -> (Session, String) {
    let s = SessionBuilder::new()
        .known_node(contact())
        .build()
        .await
        .expect("connect");
    let t = format!("tok_{}", loams_connector_kv::harness::run_tag());
    for cql in [
        "CREATE KEYSPACE IF NOT EXISTS loams_h WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            .to_owned(),
        format!("CREATE TABLE IF NOT EXISTS loams_h.{t} (id int PRIMARY KEY, v text)"),
    ] {
        s.query_unpaged(cql, ()).await.expect("ddl");
    }
    let p = s
        .prepare(format!("INSERT INTO loams_h.{t} (id, v) VALUES (?, ?)"))
        .await
        .expect("prepare");
    for i in 0..n {
        s.execute_unpaged(&p, (i, format!("v{i}")))
            .await
            .expect("insert");
    }
    (s, t)
}

fn ctx(table: &str, extra: serde_json::Value) -> ConnectorContext {
    let mut cfg = json!({ "contact_points": [contact()], "keyspace": "loams_h", "table": table, "consistency": "one" });
    if let (Some(o), serde_json::Value::Object(e)) = (cfg.as_object_mut(), extra) {
        o.extend(e);
    }
    ConnectorContext::new(
        InstanceKey::new(1, "cass"),
        cfg,
        Arc::new(StaticSecrets::new()),
        Scrubber::new(),
    )
}

fn ids(batch: &EventBatch) -> Vec<i64> {
    match batch {
        EventBatch::Events(v) => v
            .iter()
            .filter_map(|e| {
                serde_json::from_slice::<serde_json::Value>(&e.data).ok()?["id"].as_i64()
            })
            .collect(),
        EventBatch::Arrow { .. } => Vec::new(),
    }
}

/// The token ranges partition the table: read split by split, every row
/// arrives exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn cassandra_token_range_partitions() {
    if !up(P2) {
        return skip(
            "cassandra_token_range_partitions",
            "LOAMS_CONNECTORS_P2_STACK is not set",
        );
    }
    let (s, t) = table_with_rows(300).await;
    let c = ctx(&t, json!({ "splits": 8, "page_size": 50 }));
    let (mut en, sf) = CassandraFactory.source(&c).expect("source").expect("some");
    let splits = en
        .discover(&Vec::<SplitState>::new())
        .await
        .expect("splits");
    assert_eq!(splits.len(), 8);
    let mut seen = Vec::new();
    for split in &splits {
        let mut src = sf.create(split).expect("create");
        src.open(split, None).await.expect("open");
        loop {
            let b = src
                .next(Budget {
                    max_rows: 1000,
                    max_bytes: 8 << 20,
                })
                .await
                .expect("next");
            let Some(b) = b else { break };
            seen.extend(ids(&b.events));
            if RangePos::from_position(&b.position).is_some_and(|p| p.done) {
                break;
            }
        }
    }
    let distinct: BTreeSet<i64> = seen.iter().copied().collect();
    assert_eq!(seen.len(), 300, "no row twice");
    assert_eq!(distinct.len(), 300, "every row");
    let _ = s.query_unpaged(format!("DROP TABLE loams_h.{t}"), ()).await;
}

/// A source reopened from a page's checkpointed paging state reads on from
/// the next page: nothing lost, nothing twice.
#[tokio::test(flavor = "multi_thread")]
async fn cassandra_paging_state_resumes() {
    if !up(P2) {
        return skip(
            "cassandra_paging_state_resumes",
            "LOAMS_CONNECTORS_P2_STACK is not set",
        );
    }
    let (s, t) = table_with_rows(40).await;
    let c = ctx(&t, json!({ "splits": 1, "page_size": 5 }));
    let (mut en, sf) = CassandraFactory.source(&c).expect("source").expect("some");
    let split = en.discover(&[]).await.expect("splits").remove(0);
    let budget = Budget {
        max_rows: 5,
        max_bytes: 8 << 20,
    };
    let mut first = sf.create(&split).expect("create");
    first.open(&split, None).await.expect("open");
    let mut seen = Vec::new();
    let mut pos = None;
    for _ in 0..3 {
        let b = first.next(budget).await.expect("next").expect("a page");
        seen.extend(ids(&b.events));
        pos = Some(b.position);
    }
    let pos = pos.expect("a position");
    assert!(RangePos::from_position(&pos).is_some_and(|p| !p.done && p.paging.is_some()));
    drop(first);
    let mut second = sf.create(&split).expect("create");
    second.open(&split, Some(pos)).await.expect("reopen");
    loop {
        let Some(b) = second.next(budget).await.expect("next") else {
            break;
        };
        seen.extend(ids(&b.events));
        if RangePos::from_position(&b.position).is_some_and(|p| p.done) {
            break;
        }
    }
    let distinct: BTreeSet<i64> = seen.iter().copied().collect();
    assert_eq!(seen.len(), 40, "no row twice: {seen:?}");
    assert_eq!(distinct.len(), 40, "every row");
    let _ = s.query_unpaged(format!("DROP TABLE loams_h.{t}"), ()).await;
}
