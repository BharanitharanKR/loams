//! `loams dev` with Loams Live on the embedded store (LV1 plan Task 23): Live
//! runs by default with no PD, its data under `<data_dir>/live/`, and the
//! data survives a restart.
//!
//! Run with `cargo test -p loams --test live_dev` (the `live` feature is in
//! the default set).

#![cfg(feature = "live")]

use std::io::BufRead;
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;

/// A running `loams dev`: its HTTP and Live addresses. Killed on drop.
struct Dev {
    child: Child,
    http: SocketAddr,
    live: SocketAddr,
}

impl Drop for Dev {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn addr_after(line: &str, prefix: &str) -> Option<SocketAddr> {
    line.strip_prefix(prefix)?.trim().parse().ok()
}

/// `loams dev` on `data_dir`, every listener on an ephemeral loopback port,
/// with no PD configured.
fn start(data_dir: &Path) -> Dev {
    start_with(data_dir, &[])
}

/// [`start`] with `extra` flags.
fn start_with(data_dir: &Path, extra: &[&str]) -> Dev {
    let mut child = Command::new(env!("CARGO_BIN_EXE_loams"))
        .args([
            "dev",
            "--listen",
            "127.0.0.1:0",
            "--flight-sql-listen",
            "127.0.0.1:0",
            "--es-listen",
            "127.0.0.1:0",
            "--no-qdrant",
            "--live-listen",
            "127.0.0.1:0",
            "--data-dir",
        ])
        .arg(data_dir)
        .args(extra)
        .env_remove("LOAMS_TEST_PD")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn loams");
    let stdout = child.stdout.take().expect("stdout");
    let mut lines = std::io::BufReader::new(stdout).lines();
    let (mut http, mut live, mut seen) = (None, None, Vec::new());
    while http.is_none() {
        let line = lines
            .next()
            .unwrap_or_else(|| panic!("loams exited before listening; printed {seen:?}"))
            .expect("read stdout");
        live = live.or_else(|| addr_after(&line, "loams live listening on http://"));
        http = addr_after(&line, "loams listening on http://");
        seen.push(line);
    }
    std::thread::spawn(move || for _ in lines {});
    let live = live.unwrap_or_else(|| panic!("no Live line before the HTTP line: {seen:?}"));
    Dev {
        child,
        http: http.expect("the HTTP address"),
        live,
    }
}

/// Stops `dev` with SIGTERM and waits for it to exit.
fn stop(mut dev: Dev) {
    let sent = Command::new("kill")
        .args(["-TERM", &dev.child.id().to_string()])
        .status()
        .expect("kill");
    assert!(sent.success());
    for _ in 0..200 {
        if dev.child.try_wait().expect("wait").is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("loams did not stop within 10 s of SIGTERM");
}

/// A Connect JSON unary call: the status and the response body.
async fn connect(addr: SocketAddr, path: &str, body: &Value) -> (u16, Value) {
    let response = reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("content-type", "application/json")
        .json(body)
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .expect("a response");
    let status = response.status().as_u16();
    let text = response.text().await.expect("a body");
    (
        status,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}

fn object(fields: Value) -> Value {
    json!({ "objectValue": { "fields": fields } })
}

/// `_system:insert` of `{ n }` into `notes`: the new document's id and the
/// commit timestamp.
async fn insert(live: SocketAddr, n: i64) -> (Value, Value) {
    let args = object(json!({
        "table": { "stringValue": "notes" },
        "fields": object(json!({ "n": { "int64Value": n.to_string() } })),
    }));
    let (status, body) = connect(
        live,
        "/loams.live.v1.LiveService/Mutate",
        &json!({ "function": "_system:insert", "args": args }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    (body["result"].clone(), body["commitTs"].clone())
}

/// `_system:get` of `id` at timestamp `ts` (a Query without one reads at
/// the latest subscription tick, which may not have reached a commit just
/// made): the document's `n`.
async fn get_n(live: SocketAddr, id: &Value, ts: &Value) -> Value {
    let (status, body) = connect(
        live,
        "/loams.live.v1.LiveService/Query",
        &json!({
            "function": "_system:get",
            "args": object(json!({ "id": id })),
            "ts": ts,
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    body["result"]["objectValue"]["fields"]["n"]["int64Value"].clone()
}

/// With no PD anywhere, `loams dev` serves `GetInstance` and Live on the
/// embedded store under `<data_dir>/live/`: an insert then a get round-trip.
#[tokio::test(flavor = "multi_thread")]
async fn dev_starts_live_without_pd() {
    let dir = TempDir::new().expect("a temp dir");
    let dev = start(dir.path());
    let (status, info) = connect(
        dev.http,
        "/loams.instance.v1.InstanceService/GetInstance",
        &json!({}),
    )
    .await;
    assert_eq!(status, 200, "{info}");
    let (id, ts) = insert(dev.live, 7).await;
    assert!(id["stringValue"].is_string(), "an id: {id}");
    assert_eq!(get_n(dev.live, &id, &ts).await, json!("7"));
    assert!(
        dir.path().join("live").join("store.redb").exists(),
        "the store is <data_dir>/live/store.redb"
    );
    drop(dev);
}

/// The embedded store keeps Live's data across a restart of `loams dev`.
#[tokio::test(flavor = "multi_thread")]
async fn data_survives_restart_on_embedded() {
    let dir = TempDir::new().expect("a temp dir");
    let dev = start(dir.path());
    let (id, ts) = insert(dev.live, 41).await;
    stop(dev);
    let dev = start(dir.path());
    assert_eq!(get_n(dev.live, &id, &ts).await, json!("41"));
    let (other, _) = insert(dev.live, 42).await;
    assert_ne!(other, id);
    drop(dev);
}

/// The same across a SIGKILL: a committed insert is durable without a clean
/// shutdown (Task 23 review item 12).
#[tokio::test(flavor = "multi_thread")]
async fn data_survives_sigkill_on_embedded() {
    let dir = TempDir::new().expect("a temp dir");
    let mut dev = start(dir.path());
    let (id, ts) = insert(dev.live, 43).await;
    dev.child.kill().expect("SIGKILL");
    dev.child.wait().expect("reaped");
    drop(dev);
    let dev = start(dir.path());
    assert_eq!(get_n(dev.live, &id, &ts).await, json!("43"));
    drop(dev);
}

/// LV1 plan Task 5: `loams live-worker`, the hidden subcommand the server
/// starts isolated workers with, serves a bundle's functions from inside
/// its sandbox, which kills it with `SIGSYS` when it opens a file.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn live_worker_subcommand_serves_functions_sandboxed() {
    use loams_live_js::{Isolation, JsConfig, Probe, WorkerCommand, WorkerPool};

    const BUNDLE: &str = r#"
import { query, mutation } from "loams:server";
export const notes = {
  add: mutation(async (ctx, { body }) => await ctx.db.insert("notes", { body })),
  count: query(async (ctx) => (await ctx.db.query("notes").collect()).length),
};
"#;
    let pool = WorkerPool::new(WorkerCommand::new(env!("CARGO_BIN_EXE_loams")).arg("live-worker"));
    let handle = pool
        .spawn(
            "dev",
            BUNDLE.as_bytes(),
            JsConfig {
                isolation: Isolation::Isolated,
                contexts: 1,
                ..JsConfig::default()
            },
        )
        .await
        .expect("loams live-worker loads the bundle");
    assert_eq!(handle.probe(Probe::Ping).await.expect("a ping"), "");

    let store = loams_live::testing::TestStore::embedded(option_env!("CARGO_TARGET_TMPDIR")).await;
    let runner = loams_live::Runner::open(store.store(), &store.live_config("worker"))
        .await
        .expect("the runner opens");
    let add = handle.function("notes:add").expect("notes:add");
    let body = loams_live::LiveValue::Object(
        [("body".to_string(), loams_live::LiveValue::Str("hi".into()))].into(),
    );
    runner
        .mutate(add, body, None)
        .await
        .expect("a mutation runs in the worker");
    let count = handle.function("notes:count").expect("notes:count");
    let at = runner.store().now().await.expect("now");
    let counted = runner
        .query(
            &*count,
            loams_live::LiveValue::Object(Default::default()),
            at,
        )
        .await
        .expect("a query runs in the worker");
    assert_eq!(counted.result, loams_live::LiveValue::F64(1.0));

    match handle.probe(Probe::OpenFile).await {
        Err(e @ loams_live::LiveError::WorkerCrashed(_)) => {
            assert!(e.to_string().contains("SIGSYS"), "{e}");
        }
        other => panic!("the sandbox kills the worker, not {other:?}"),
    }
}

const NOTES: &str = r#"
import { query, mutation } from "loams:server";
export const notes = {
  add: mutation(async (ctx, { body }) => await ctx.db.insert("notes", { body })),
  count: query(async (ctx) => (await ctx.db.query("notes").collect()).length),
};
"#;

/// `notes:count` at `ts`.
async fn count_notes(live: SocketAddr, ts: &Value) -> Option<f64> {
    let (status, body) = connect(
        live,
        "/loams.live.v1.LiveService/Query",
        &json!({ "function": "notes:count", "args": object(json!({})), "ts": ts }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    body["result"]["doubleValue"].as_f64()
}

/// The processes whose parent is `pid` and whose command line names
/// `live-worker`.
#[cfg(target_os = "linux")]
fn live_workers_of(pid: u32) -> usize {
    let mut found = 0;
    for entry in std::fs::read_dir("/proc").expect("/proc") {
        let path = entry.expect("an entry").path();
        let Ok(stat) = std::fs::read_to_string(path.join("stat")) else {
            continue;
        };
        // `pid (comm) state ppid …`; comm may hold spaces, so split after
        // its closing parenthesis.
        let ppid = stat
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.split_whitespace().nth(1))
            .and_then(|p| p.parse::<u32>().ok());
        let cmdline = std::fs::read(path.join("cmdline")).unwrap_or_default();
        if ppid == Some(pid) && String::from_utf8_lossy(&cmdline).contains("live-worker") {
            found += 1;
        }
    }
    found
}

/// LV1 plan Task 6: `loams dev` deploys a bundle over the Live admin API
/// (`LiveAdminService`, Task 10) and runs
/// its functions, in process by default and, with `--live-isolation
/// isolated`, in `loams live-worker` children of the server. The deployment
/// survives a restart: its bundle is in the data directory's bucket.
#[tokio::test(flavor = "multi_thread")]
async fn dev_serves_deployed_functions() {
    let modes: &[(&[&str], bool)] = if cfg!(target_os = "linux") {
        &[(&[], false), (&["--live-isolation", "isolated"], true)]
    } else {
        &[(&[], false)]
    };
    for &(extra, isolated) in modes {
        let dir = TempDir::new().expect("a temp dir");
        let dev = start_with(dir.path(), extra);
        use base64::Engine as _;
        let (status, body) = connect(
            dev.live,
            "/loams.live.v1.LiveAdminService/Deploy",
            &json!({
                "bundle": base64::engine::general_purpose::STANDARD.encode(NOTES),
                "schema": {},
            }),
        )
        .await;
        assert_eq!(status, 200, "{extra:?}: {body}");
        assert!(body["deployment"]["id"].is_string(), "{body}");
        assert_eq!(
            body["deployment"]["state"], "DEPLOYMENT_STATE_ACTIVE",
            "{body}"
        );
        // `loams dev` runs with authentication off: the loopback principal
        // deployed it (LV1 plan Task 10).
        assert_eq!(body["deployment"]["createdBy"], "dev-loopback", "{body}");
        let args = object(json!({ "body": { "stringValue": "hi" } }));
        let (status, body) = connect(
            dev.live,
            "/loams.live.v1.LiveService/Mutate",
            &json!({ "function": "notes:add", "args": args }),
        )
        .await;
        assert_eq!(status, 200, "{extra:?}: {body}");
        let ts = body["commitTs"].clone();
        assert_eq!(count_notes(dev.live, &ts).await, Some(1.0));
        #[cfg(target_os = "linux")]
        assert_eq!(
            live_workers_of(dev.child.id()) > 0,
            isolated,
            "{extra:?}: workers run only when isolated"
        );
        #[cfg(not(target_os = "linux"))]
        let _ = isolated;
        stop(dev);

        let dev = start_with(dir.path(), extra);
        assert_eq!(
            count_notes(dev.live, &ts).await,
            Some(1.0),
            "{extra:?}: the deployment serves after a restart"
        );
        drop(dev);
    }
}
