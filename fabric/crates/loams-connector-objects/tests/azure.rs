//! ADLS Gen2's DFS rename (R0.21). Azurite has no hierarchical namespace,
//! so the rename runs nightly against a real HNS account; PR CI checks the
//! request the client sends against a local endpoint (which never counts
//! toward a suite) until fixtures recorded from the real account exist.

mod common;

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use common::*;
use http_body_util::Full;
use hyper::service::service_fn;
use loams_connector_objects::adls_gen2::{DfsAuth, DfsClient, RenameOutcome};
use loams_connector_objects::config::SinkFormat;
use loams_connector_objects::sink::Publish;
use loams_connector_objects::store::Backend;
use loams_connector_sdk::{AllowAll, Secret, StaticSecrets};

type Seen = Arc<Mutex<Vec<(String, String, http::HeaderMap)>>>;

/// A local endpoint that answers the first rename with 201 and every later
/// one with 409 `PathAlreadyExists`; directory creation succeeds.
async fn fake_dfs() -> (String, Seen) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let seen: Seen = Arc::default();
    let s = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let s = s.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                    let s = s.clone();
                    async move {
                        let rename = req.headers().contains_key("x-ms-rename-source");
                        let mut seen = s.lock().expect("lock");
                        seen.push((
                            req.method().to_string(),
                            req.uri().to_string(),
                            req.headers().clone(),
                        ));
                        let renames = seen
                            .iter()
                            .filter(|(_, _, h)| h.contains_key("x-ms-rename-source"))
                            .count();
                        let resp = if rename && renames > 1 {
                            http::Response::builder()
                                .status(409)
                                .header("x-ms-error-code", "PathAlreadyExists")
                                .body(Full::new(Bytes::new()))
                        } else {
                            http::Response::builder()
                                .status(201)
                                .body(Full::new(Bytes::new()))
                        };
                        Ok::<_, std::convert::Infallible>(resp.expect("response"))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(tcp), svc)
                    .await;
            });
        }
    });
    (format!("http://{addr}"), seen)
}

#[tokio::test]
async fn dfs_rename_request_shape() {
    let (endpoint, seen) = fake_dfs().await;
    // A base64 account key (not a real one).
    let key = Secret::new("a2V5LWZvci10ZXN0cw==");
    let dfs = DfsClient::new(
        Arc::new(AllowAll),
        endpoint,
        "fs",
        DfsAuth::SharedKey {
            account: "acct".into(),
            key,
        },
    );
    assert_eq!(
        dfs.rename("base/_staging/a b.tmp", "base/a b.parquet")
            .await
            .expect("rename"),
        RenameOutcome::Renamed
    );
    assert_eq!(
        dfs.rename("base/_staging/x.tmp", "base/x.parquet")
            .await
            .expect("rename"),
        RenameOutcome::DestinationExists
    );
    let seen = seen.lock().expect("lock");
    // Parent directory, then the rename.
    let (m, uri, _) = &seen[0];
    assert_eq!(
        (m.as_str(), uri.as_str()),
        ("PUT", "/fs/base?resource=directory")
    );
    let (m, uri, h) = &seen[1];
    assert_eq!(
        (m.as_str(), uri.as_str()),
        ("PUT", "/fs/base/a%20b.parquet?mode=legacy")
    );
    assert_eq!(h["x-ms-rename-source"], "/fs/base/_staging/a%20b.tmp");
    assert_eq!(h["if-none-match"], "*");
    assert_eq!(
        h["x-ms-version"],
        loams_connector_objects::adls_gen2::DFS_VERSION
    );
    let auth = h["authorization"].to_str().expect("ascii");
    assert!(auth.starts_with("SharedKey acct:"), "{auth}");
    assert!(
        !auth.contains("a2V5LWZvci10ZXN0cw=="),
        "the key itself was sent"
    );
}

/// The sink publishing through the DFS rename on a real HNS account.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adls_hns_rename_nightly() {
    if !real(
        "LOAMS_CONNECTORS_NIGHTLY",
        &[
            "LOAMS_ADLS_ACCOUNT",
            "LOAMS_ADLS_FILESYSTEM",
            "LOAMS_ADLS_ACCESS_KEY",
        ],
        "the ADLS Gen2 HNS rename",
    ) {
        return;
    }
    let account = env("LOAMS_ADLS_ACCOUNT");
    let fs = env("LOAMS_ADLS_FILESYSTEM");
    let store = real_store(
        Backend::Azure,
        serde_json::json!({"bucket": fs, "account": account, "auth": {"access_key": "key"}}),
        StaticSecrets::new().with("key", env("LOAMS_ADLS_ACCESS_KEY")),
    )
    .await;
    let dfs = Arc::new(DfsClient::new(
        Arc::new(AllowAll),
        DfsClient::default_endpoint(&account),
        fs,
        DfsAuth::SharedKey {
            account,
            key: Secret::new(env("LOAMS_ADLS_ACCESS_KEY")),
        },
    ));
    let prefix = run_prefix("adls");
    let fabric = loams_connector_sdk::testing::memory::MemoryFabric::new();
    push_events(&fabric, 0, 30);
    let sink = sink_config(SinkFormat::Ndjson, 1 << 30, 0);
    let h = start_sink_with(
        &fabric,
        store.clone(),
        &sink,
        &prefix,
        1,
        Publish::DfsRename(dfs),
    )
    .await;
    eventually("published", || fabric.committed(GROUP) == 30).await;
    let status = h.stop().await;
    assert!(status.last_error.is_none(), "{:?}", status.last_error);
    let ids = delivered_ids(&*store, &prefix).await;
    assert_eq!(ids.len(), 30);
    assert_contiguous(&published(&*store, &prefix).await);
}
