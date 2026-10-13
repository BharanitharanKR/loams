//! The target profiles against their real systems: CockroachDB
//! (`connectors-ci-only`), pgvector (`connectors-p2`) and Aurora PostgreSQL
//! (nightly). Each test is gated on its system's stack or account.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Int64Type};
use arrow_schema::DataType;
use common::{ci_only, eventually, fold, gate, opts, p2, spec};
use loams_connector_postgres::harness::{Endpoint, PgHarness};
use loams_connector_postgres::{PostgresFactory, Target};
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorFactory, EventBatch, FabricReader, InstanceKey, Pipeline,
    PipelineConfig, Scrubber, SinkHandle, StaticSecrets,
};
use loams_flow_conformance::{ConnectorHarness, RunEnv, Suite, run_with};
use serde_json::json;

const SETTLE: Duration = Duration::from_secs(90);

/// A sinkless changefeed's stream folds to the table, after inserts,
/// updates and deletes; the kit's kill-restart and cdc suites pass.
#[tokio::test(flavor = "multi_thread")]
async fn cockroach_changefeed_cdc_converges() {
    if !ci_only() {
        return;
    }
    let h = PgHarness::cdc(Target::Cockroach, "feed");
    h.reset().await.expect("reset");
    h.seed(100).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("initial scan");
    let t = h.table().to_owned();
    h.exec(&format!(
        "UPDATE {t} SET v = 'u' WHERE id < 20; DELETE FROM {t} WHERE id >= 90; \
         INSERT INTO {t} (id, v) VALUES (500, 'n'), (501, 'n')"
    ))
    .await
    .expect("workload");
    let admin = h.admin().await.expect("admin");
    let want: std::collections::BTreeMap<i64, Option<String>> = admin
        .query(format!("SELECT id, v FROM {t}").as_str(), &[])
        .await
        .expect("select")
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, Option<String>>(1)))
        .collect();
    let ok = eventually(SETTLE, || async { fold(&h.delivered().await) == want }).await;
    h.stop().await.expect("stop");
    assert!(ok, "the changefeed never folded to the table");
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("cockroachdb"),
        None,
        &h,
        &[Suite::KillRestart, Suite::Cdc],
    )
    .await;
    assert!(report.green(), "{report}");
}

/// CockroachDB has no binary `COPY TO` (R0.20): the server refuses it, and
/// the batch source reads with `SELECT … AS OF SYSTEM TIME` instead.
#[tokio::test(flavor = "multi_thread")]
async fn cockroach_batch_uses_select_fallback() {
    if !ci_only() {
        return;
    }
    let h = PgHarness::batch(Target::Cockroach, "batch");
    h.reset().await.expect("reset");
    let admin = h.admin().await.expect("admin");
    let copy = admin
        .copy_out(
            format!(
                "COPY (SELECT * FROM {}) TO STDOUT (FORMAT binary)",
                h.table()
            )
            .as_str(),
        )
        .await;
    assert!(
        copy.is_err(),
        "CockroachDB accepted a binary COPY TO; R0.20 can be revisited"
    );
    h.seed(1_000).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settle");
    h.stop().await.expect("stop");
    let mut rows = 0;
    for b in h.delivered().await {
        if let EventBatch::Arrow { batch, envelope } = b {
            assert!(envelope.ty.ends_with(".rows.v1"));
            rows += batch.num_rows();
        }
    }
    assert_eq!(rows, 1_000);
    let copies = h.read_back().await.expect("read back");
    assert!(copies.iter().all(|o| o.copies == 1));
}

fn ctx(ep: &Endpoint, name: &str, extra: serde_json::Value) -> ConnectorContext {
    let mut config = ep.config();
    if let (Some(c), Some(e)) = (config.as_object_mut(), extra.as_object()) {
        c.extend(e.clone());
    }
    ConnectorContext::new(
        InstanceKey::new(1, name),
        config,
        Arc::new(ep.secrets()),
        Scrubber::new(),
    )
}

/// `vector(3)` reads as `FixedSizeList<Float32>(3)` on the bulk path and
/// writes back through the sink unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn pgvector_roundtrip_fixed_size_list() {
    if !p2() {
        return;
    }
    let ep = Endpoint::from_env(Target::Pgvector).expect("endpoint");
    let src = format!("loams_pv_src_{}", std::process::id());
    let dst = format!("loams_pv_dst_{}", std::process::id());
    let admin_h = PgHarness::batch(Target::Pgvector, "pv");
    admin_h
        .exec(&format!(
            "CREATE EXTENSION IF NOT EXISTS vector; DROP TABLE IF EXISTS {src}; DROP TABLE IF EXISTS {dst}; \
             CREATE TABLE {src} (id bigint PRIMARY KEY, emb vector(3)); \
             CREATE TABLE {dst} (id bigint PRIMARY KEY, emb vector(3)); \
             INSERT INTO {src} VALUES (1, '[1,2,3]'), (2, '[0.5,-1,2.25]'), (3, NULL)"
        ))
        .await
        .expect("setup");
    // Read: the batch source over the bulk path.
    let read_ctx = ctx(&ep, "pv-read", json!({"tables": [src]}));
    let factory = PostgresFactory::new(Target::Pgvector);
    let (enumerator, sources) = factory.source(&read_ctx).expect("source").expect("some");
    let fabric = MemoryFabric::new();
    let handle = Pipeline::source(read_ctx.instance.clone(), PipelineConfig::default())
        .enumerator(enumerator)
        .sources(Arc::from(sources))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .start()
        .await
        .expect("start");
    assert!(eventually(SETTLE, || async { !fabric.is_empty() }).await);
    handle.stop().await;
    let mut reader = fabric.reader("t");
    let (batch, _) = reader
        .poll(Budget {
            max_rows: 100,
            max_bytes: 1 << 20,
        })
        .await
        .expect("poll")
        .expect("a batch");
    let EventBatch::Arrow { batch, envelope } = batch else {
        panic!("not the bulk path");
    };
    let emb = batch.column_by_name("emb").expect("emb");
    assert!(
        matches!(emb.data_type(), DataType::FixedSizeList(item, 3) if item.data_type() == &DataType::Float32)
    );
    let list = emb.as_fixed_size_list();
    let ids = batch
        .column_by_name("id")
        .expect("id")
        .as_primitive::<Int64Type>();
    let row2 = (0..batch.num_rows())
        .find(|i| ids.value(*i) == 2)
        .expect("row 2");
    let v = list.value(row2);
    assert_eq!(
        v.as_primitive::<Float32Type>().values().to_vec(),
        vec![0.5, -1.0, 2.25]
    );
    // Write: the same Arrow batch through the sink.
    let write_ctx = ctx(&ep, "pv-write", json!({"table": dst}));
    let SinkHandle::Transactional(mut sink) = factory
        .sink(&write_ctx)
        .expect("sink")
        .expect("some")
        .create()
        .expect("create")
    else {
        panic!("the sink is exactly-once by default");
    };
    sink.recover().await.expect("recover");
    sink.write(EventBatch::Arrow { batch, envelope })
        .await
        .expect("write");
    let mut cp = loams_connector_sdk::Checkpoint::new(write_ctx.instance.clone(), 1);
    cp.splits
        .insert("fabric".into(), loams_connector_sdk::Position::from_u64(1));
    let token = sink.prepare(&cp).await.expect("prepare");
    sink.commit(token).await.expect("commit");
    let admin = admin_h.admin().await.expect("admin");
    let same = admin
        .query(
            format!("SELECT count(*) FROM {src} s JOIN {dst} d USING (id) WHERE s.emb IS NOT DISTINCT FROM d.emb").as_str(),
            &[],
        )
        .await
        .expect("compare");
    assert_eq!(same[0].get::<_, i64>(0), 3);
    admin_h
        .exec(&format!("DROP TABLE {src}; DROP TABLE {dst}"))
        .await
        .expect("cleanup");
}

/// Aurora PostgreSQL with an RDS IAM token instead of a password (nightly,
/// a real cluster whose user is granted `rds_iam`).
#[tokio::test(flavor = "multi_thread")]
async fn aurora_iam_token_nightly() {
    if !gate(&[
        "LOAMS_AURORA_POSTGRES_URL",
        "LOAMS_AWS_ACCESS_KEY_ID",
        "LOAMS_AWS_SECRET_ACCESS_KEY",
        "LOAMS_AWS_REGION",
    ]) {
        return;
    }
    let url = std::env::var("LOAMS_AURORA_POSTGRES_URL").expect("url");
    let ep = Endpoint::from_url(&url).expect("a postgres:// URL");
    let var = |k: &str| std::env::var(k).expect(k);
    let secrets = StaticSecrets::new()
        .with("aws-key", var("LOAMS_AWS_ACCESS_KEY_ID").into_bytes())
        .with(
            "aws-secret",
            var("LOAMS_AWS_SECRET_ACCESS_KEY").into_bytes(),
        );
    let config = json!({
        "host": ep.host, "port": ep.port, "user": ep.user, "database": ep.database,
        "auth": "iam", "sslmode": "require",
        "aws": {"region": var("LOAMS_AWS_REGION"), "access_key_id": "aws-key", "secret_access_key": "aws-secret"},
        "tables": ["pg_catalog.pg_namespace"], "key": ["oid"],
    });
    let ctx = ConnectorContext::new(
        InstanceKey::new(1, "aurora-iam"),
        config,
        Arc::new(secrets),
        Scrubber::new(),
    );
    let cfg = PostgresFactory::new(Target::Aurora)
        .config(&ctx)
        .expect("config");
    let client = loams_connector_postgres::conn::connect(&cfg, &ctx)
        .await
        .expect("an IAM login");
    let one: i32 = client
        .query_one("SELECT 1", &[])
        .await
        .expect("query")
        .get(0);
    assert_eq!(one, 1);
    // The nightly also runs the source suites on the real cluster.
    let h = PgHarness::cdc(Target::Aurora, "aurora");
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("aurora"),
        None,
        &h,
        &[Suite::KillRestart, Suite::Cdc],
    )
    .await;
    assert!(report.green(), "{report}");
}
