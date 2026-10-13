//! GCS and Azure Blob through the listing source: the high-water key
//! resumes after a stop, on `InMemory` always, on fake-gcs-server and
//! Azurite in PR CI, and on real accounts nightly. Also the decoded-rows
//! path (CSV through the SDK's codec) and its mid-object resume.

mod common;

use std::sync::Arc;

use common::*;
use loams_connector_objects::config::SourceConfig;
use loams_connector_objects::list::{
    ListPosition, ListingSourceFactory, PartialPos, SourceSettings, StoreLister,
};
use loams_connector_objects::store::{Backend, StoreHandle};
use loams_connector_sdk::{
    Budget, EnvelopeBuilder, EventBatch, SourceFactory, Split, StaticSecrets,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

async fn gcs_stores() -> Vec<(Arc<dyn ObjectStore>, String, &'static str)> {
    let mut out = Vec::new();
    if real(
        "LOAMS_CONNECTORS_P2_STACK",
        &["LOAMS_FAKE_GCS_ENDPOINT", "LOAMS_FAKE_GCS_BUCKET"],
        "fake-gcs-server",
    ) {
        let bucket = env("LOAMS_FAKE_GCS_BUCKET");
        let s = real_store(
            Backend::Gcs,
            serde_json::json!({"bucket": bucket, "endpoint": env("LOAMS_FAKE_GCS_ENDPOINT"), "allow_http": true}),
            StaticSecrets::new(),
        )
        .await;
        out.push((s, bucket, "fake-gcs-server"));
    }
    if real(
        "LOAMS_CONNECTORS_NIGHTLY",
        &["LOAMS_GCS_BUCKET", "LOAMS_GCS_SERVICE_ACCOUNT_KEY"],
        "a real GCS bucket",
    ) {
        let bucket = env("LOAMS_GCS_BUCKET");
        let s = real_store(
            Backend::Gcs,
            serde_json::json!({"bucket": bucket, "auth": {"service_account_key": "sa"}}),
            StaticSecrets::new().with("sa", env("LOAMS_GCS_SERVICE_ACCOUNT_KEY")),
        )
        .await;
        out.push((s, bucket, "GCS"));
    }
    out
}

async fn azure_stores() -> Vec<(Arc<dyn ObjectStore>, String, &'static str)> {
    let mut out = Vec::new();
    for (stack, prefix, what) in [
        ("LOAMS_CONNECTORS_P2_STACK", "LOAMS_AZURITE", "Azurite"),
        (
            "LOAMS_CONNECTORS_NIGHTLY",
            "LOAMS_AZURE",
            "a real Azure account",
        ),
    ] {
        let vars: Vec<String> = ["ACCOUNT", "CONTAINER", "ACCESS_KEY"]
            .iter()
            .map(|v| format!("{prefix}_{v}"))
            .collect();
        let refs: Vec<&str> = vars.iter().map(String::as_str).collect();
        if !real(stack, &refs, what) {
            continue;
        }
        let endpoint = std::env::var(format!("{prefix}_ENDPOINT")).ok();
        let container = env(&vars[1]);
        let s = real_store(
            Backend::Azure,
            serde_json::json!({
                "bucket": container, "account": env(&vars[0]), "endpoint": endpoint,
                "allow_http": endpoint.as_deref().is_some_and(|e| e.starts_with("http://")),
                "auth": {"access_key": "key"}
            }),
            StaticSecrets::new().with("key", env(&vars[2])),
        )
        .await;
        out.push((s, container, what));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gcs_and_blob_high_water_resume() {
    listing_high_water_resume(Arc::new(InMemory::new()), "mem", "in").await;
    let mut stores = gcs_stores().await;
    stores.extend(azure_stores().await);
    for (store, bucket, what) in stores {
        eprintln!("high-water resume on {what}");
        listing_high_water_resume(store, &bucket, &run_prefix("hw")).await;
    }
}

#[tokio::test]
async fn decoded_rows_resume_mid_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut csv = String::from("id,name\n");
    for i in 0..25 {
        csv.push_str(&format!("{i},n{i}\n"));
    }
    store
        .put(&Path::from("in/rows.csv"), csv.into_bytes().into())
        .await
        .expect("put");
    let cfg: SourceConfig = serde_json::from_value(serde_json::json!({
        "decode": "csv",
        "columns": [{"name": "id", "type": "bigint", "nullable": false}, {"name": "name", "type": "text"}]
    }))
    .expect("config");
    let factory = ListingSourceFactory::new(
        Arc::new(StoreLister::new(
            Arc::new(StoreHandle::ready(store.clone())),
            "b",
        )),
        SourceSettings::from_config(&cfg).expect("settings"),
        EnvelopeBuilder::new("s3", &instance()),
    );
    let split = Split::new("in", serde_json::json!({"prefix": "in"}));
    let budget = Budget {
        max_rows: 10,
        max_bytes: 1 << 20,
    };

    let mut src = factory.create(&split).expect("source");
    src.open(&split, None).await.expect("open");
    let mut ranges = Vec::new();
    let mut positions = Vec::new();
    while let Some(b) = src.next(budget).await.expect("next") {
        let EventBatch::Arrow { batch, envelope } = b.events else {
            panic!("rows come as Arrow batches");
        };
        assert!(batch.num_rows() <= 10);
        ranges.push(envelope.id.rsplit('/').next().expect("range").to_owned());
        positions.push(b.position);
    }
    assert_eq!(ranges, ["0-9", "10-19", "20-24"]);
    let after_first = ListPosition::from_position(&positions[0]).expect("position");
    let version = after_first
        .obj
        .as_ref()
        .expect("mid-object")
        .version
        .clone();
    assert_eq!(
        after_first.obj,
        Some(PartialPos {
            key: "in/rows.csv".into(),
            version,
            row: 10
        })
    );
    assert_eq!(
        ListPosition::from_position(&positions[2]).expect("position"),
        ListPosition {
            hw: Some("in/rows.csv".into()),
            obj: None
        }
    );

    // A restart from the first batch's position emits rows 10.. only.
    let mut again = factory.create(&split).expect("source");
    again
        .open(&split, Some(positions[0].clone()))
        .await
        .expect("open");
    let b = again.next(budget).await.expect("next").expect("a batch");
    let EventBatch::Arrow { envelope, .. } = b.events else {
        panic!("Arrow");
    };
    assert!(envelope.id.ends_with("/10-19"), "{}", envelope.id);
}

#[tokio::test]
async fn oversized_object_is_poison_and_skipped() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(&Path::from("p/a-big"), vec![b'x'; 2048].into())
        .await
        .expect("put");
    store
        .put(&Path::from("p/b-small"), b"ok".to_vec().into())
        .await
        .expect("put");
    let cfg: SourceConfig =
        serde_json::from_value(serde_json::json!({"decode": "bytes", "max_object_bytes": 1024}))
            .expect("config");
    let factory = ListingSourceFactory::new(
        Arc::new(StoreLister::new(Arc::new(StoreHandle::ready(store)), "b")),
        SourceSettings::from_config(&cfg).expect("settings"),
        EnvelopeBuilder::new("s3", &instance()),
    );
    let split = Split::new("p", serde_json::json!({"prefix": "p"}));
    let budget = Budget {
        max_rows: 10,
        max_bytes: 1 << 20,
    };
    let mut src = factory.create(&split).expect("source");
    src.open(&split, None).await.expect("open");
    let err = src.next(budget).await.expect_err("poison");
    assert_eq!(err.class, loams_connector_sdk::ErrorClass::Poison);
    let b = src
        .next(budget)
        .await
        .expect("next")
        .expect("the small one");
    let EventBatch::Events(evs) = b.events else {
        panic!("events")
    };
    assert_eq!(evs.len(), 1);
    assert_eq!(&evs[0].data[..], b"ok");
}
