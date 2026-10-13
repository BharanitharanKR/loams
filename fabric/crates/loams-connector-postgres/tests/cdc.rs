//! Native Postgres CDC against real PostgreSQL 17 (compose `pgvector`,
//! `connectors-p2`). Gated on `LOAMS_CONNECTORS_P2_STACK`.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use common::{eventually, fold, opts, p2, spec};
use loams_connector_postgres::decode::CdcPos;
use loams_connector_postgres::harness::PgHarness;
use loams_connector_postgres::{PostgresFactory, Target, slot};
use loams_flow_conformance::{ConnectorHarness, KillTarget, RunEnv, Suite, run_with};

const SETTLE: Duration = Duration::from_secs(90);

async fn slot_name(h: &PgHarness) -> String {
    PostgresFactory::new(Target::Postgres)
        .config(&h.context().expect("ctx"))
        .expect("cfg")
        .slot_name()
}

async fn confirmed(admin: &tokio_postgres::Client, slot: &str) -> u64 {
    slot::slot_info(admin, slot)
        .await
        .ok()
        .flatten()
        .and_then(|i| i.confirmed_flush)
        .unwrap_or(0)
}

async fn source_table(h: &PgHarness) -> BTreeMap<i64, Option<String>> {
    let admin = h.admin().await.expect("admin");
    admin
        .query(format!("SELECT id, v FROM {}", h.table()).as_str(), &[])
        .await
        .expect("select")
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, Option<String>>(1)))
        .collect()
}

/// A random workload of 10,000 operations; folding what reached the Fabric
/// gives the source table (the Fluss `_current` table's stand-in until FL1,
/// R10.8).
#[tokio::test(flavor = "multi_thread")]
async fn pg_cdc_converges() {
    if !p2() {
        return;
    }
    let h = PgHarness::cdc(Target::Postgres, "converge");
    h.reset().await.expect("reset");
    h.seed(500).await.expect("seed");
    h.start().await.expect("start");
    // splitmix64, so the workload is reproducible.
    let mut x: u64 = 0x5eed;
    let mut next = move || {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let t = h.table().to_owned();
    let mut sql = String::new();
    for i in 0..10_000u64 {
        let id = next() % 2_000;
        match next() % 3 {
            0 => sql.push_str(&format!(
                "INSERT INTO {t} (id, v) VALUES ({id}, 'i{i}') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;"
            )),
            1 => sql.push_str(&format!("UPDATE {t} SET v = 'u{i}' WHERE id = {id};")),
            _ => sql.push_str(&format!("DELETE FROM {t} WHERE id = {id};")),
        }
        if i % 250 == 249 {
            h.exec(&sql).await.expect("workload");
            sql.clear();
        }
    }
    let want = source_table(&h).await;
    let ok = eventually(SETTLE, || async { fold(&h.delivered().await) == want }).await;
    h.stop().await.expect("stop");
    assert!(
        ok,
        "the folded change stream never equalled the source table"
    );
}

/// Rows written while the snapshot runs arrive exactly once: through the
/// snapshot if committed before the consistent point, through the stream
/// if after.
#[tokio::test(flavor = "multi_thread")]
async fn pg_cdc_snapshot_then_stream_no_gap_no_overlap() {
    if !p2() {
        return;
    }
    let h = std::sync::Arc::new(PgHarness::cdc(Target::Postgres, "handoff"));
    h.reset().await.expect("reset");
    h.seed(5_000).await.expect("seed");
    let writer = {
        let h = h.clone();
        tokio::spawn(async move {
            for i in 0..200 {
                let id = 10_000 + i;
                h.exec(&format!(
                    "INSERT INTO {} (id, v) VALUES ({id}, 'w')",
                    h.table()
                ))
                .await
                .expect("insert");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    h.start().await.expect("start");
    writer.await.expect("writer");
    let want = source_table(&h).await;
    let ok = eventually(SETTLE, || async {
        let copies = h.read_back().await.expect("read back");
        copies.len() == want.len()
    })
    .await;
    h.stop().await.expect("stop");
    assert!(ok, "not every row arrived");
    let copies = h.read_back().await.expect("read back");
    let over: Vec<_> = copies.iter().filter(|o| o.copies != 1).collect();
    assert!(over.is_empty(), "rows delivered more than once: {over:?}");
    assert!(matches!(
        h.cdc_position().await,
        Some(CdcPos::Stream { .. })
    ));
}

/// The slot's confirmed flush LSN never passes data the Fabric has not
/// acknowledged.
#[tokio::test(flavor = "multi_thread")]
async fn pg_cdc_flush_lsn_only_after_fabric_ack() {
    if !p2() {
        return;
    }
    let h = PgHarness::cdc(Target::Postgres, "flush")
        .with_config(serde_json::json!({"heartbeat_ms": 0}));
    h.reset().await.expect("reset");
    h.seed(10).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settle");
    let slot = slot_name(&h).await;
    let admin = h.admin().await.expect("admin");
    let before: String = admin
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .expect("lsn")
        .get(0);
    let before = loams_connector_postgres::replication::parse_lsn(&before).expect("lsn");
    let fabric = h.fabric().await;
    fabric.hold();
    h.exec(&format!(
        "INSERT INTO {} (id, v) VALUES (1000, 'held')",
        h.table()
    ))
    .await
    .expect("insert");
    tokio::time::sleep(Duration::from_secs(4)).await;
    let held = slot::slot_info(&admin, &slot)
        .await
        .expect("slot")
        .expect("exists");
    assert!(
        held.confirmed_flush.expect("flush") <= before,
        "the slot confirmed WAL the Fabric has not acknowledged"
    );
    fabric.release();
    let moved = eventually(SETTLE, || async {
        slot::slot_info(&admin, &slot)
            .await
            .ok()
            .flatten()
            .and_then(|i| i.confirmed_flush)
            .is_some_and(|f| f > before)
    })
    .await;
    h.stop().await.expect("stop");
    assert!(
        moved,
        "the flush LSN did not advance after the Fabric's ack"
    );
}

/// The kit's kill-restart, dup-check and cdc suites against real Postgres.
#[tokio::test(flavor = "multi_thread")]
async fn pg_cdc_kill_restart_resumes() {
    if !p2() {
        return;
    }
    let h = PgHarness::cdc(Target::Postgres, "kill");
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("postgresql"),
        None,
        &h,
        &[Suite::KillRestart, Suite::DupCheck, Suite::Cdc],
    )
    .await;
    assert!(report.green(), "{report}");
    // And by hand: rows written while the task is dead arrive after restart.
    h.reset().await.expect("reset");
    h.seed(50).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settle");
    h.kill(KillTarget::Task).await.expect("kill");
    h.seed(50).await.expect("seed while down");
    h.start().await.expect("restart");
    h.settle(SETTLE).await.expect("resumed");
    h.stop().await.expect("stop");
}

/// `StopInstance` keeps the slot; `DeleteInstance` drops it and the
/// Loams-owned publication.
#[tokio::test(flavor = "multi_thread")]
async fn pg_cdc_slot_dropped_on_delete() {
    if !p2() {
        return;
    }
    let h = PgHarness::cdc(Target::Postgres, "delete");
    h.reset().await.expect("reset");
    h.seed(5).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settle");
    h.stop().await.expect("stop");
    let slot = slot_name(&h).await;
    let admin = h.admin().await.expect("admin");
    assert!(
        slot::slot_info(&admin, &slot)
            .await
            .expect("slot")
            .is_some(),
        "stop keeps the slot"
    );
    let ctx = h.context().expect("ctx");
    PostgresFactory::new(Target::Postgres)
        .delete_instance(&ctx)
        .await
        .expect("delete");
    assert!(
        slot::slot_info(&admin, &slot)
            .await
            .expect("slot")
            .is_none(),
        "delete drops the slot"
    );
    let (publication, _) = PostgresFactory::new(Target::Postgres)
        .config(&ctx)
        .expect("cfg")
        .publication_name();
    let pubs = admin
        .query(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .expect("publications");
    assert!(pubs.is_empty(), "delete drops the publication");
}

/// On a quiet database the heartbeat keeps the slot moving.
#[tokio::test(flavor = "multi_thread")]
async fn pg_cdc_idle_heartbeat_advances_slot() {
    if !p2() {
        return;
    }
    let h = PgHarness::cdc(Target::Postgres, "heartbeat");
    h.reset().await.expect("reset");
    h.seed(1).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settle");
    let slot = slot_name(&h).await;
    let admin = h.admin().await.expect("admin");
    let first = confirmed(&admin, &slot).await;
    let moved = eventually(Duration::from_secs(30), || async {
        confirmed(&admin, &slot).await > first
    })
    .await;
    h.stop().await.expect("stop");
    assert!(moved, "the slot did not advance on an idle database");
}

/// `ALTER TABLE … ADD COLUMN` mid-stream is a schema change, and later rows
/// carry the column.
#[tokio::test(flavor = "multi_thread")]
async fn pg_cdc_ddl_add_column_evolves() {
    if !p2() {
        return;
    }
    let h = PgHarness::cdc(Target::Postgres, "ddl");
    h.reset().await.expect("reset");
    h.seed(3).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settle");
    h.exec(&format!(
        "ALTER TABLE {0} ADD COLUMN note text; INSERT INTO {0} (id, v, note) VALUES (50, 'x', 'hello')",
        h.table()
    ))
    .await
    .expect("ddl");
    let ok = eventually(SETTLE, || async {
        h.changes().await.iter().any(|e| {
            let d: serde_json::Value = serde_json::from_slice(&e.data).unwrap_or_default();
            d["after"]["note"] == "hello"
        })
    })
    .await;
    let evolved = h.delivered().await.iter().any(|b| match b {
        loams_connector_sdk::EventBatch::Events(v) => v.iter().any(|e| {
            e.ty.ends_with(".schema-change.v1")
                && String::from_utf8_lossy(&e.data).contains("\"note\"")
        }),
        _ => false,
    });
    h.stop().await.expect("stop");
    assert!(ok, "the row after the DDL did not carry the new column");
    assert!(evolved, "no schema-change event for the added column");
}
