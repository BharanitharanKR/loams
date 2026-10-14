//! The compute runner (the release stack's `pg-runner`): `pg-control`
//! drives computes through [`RemoteRuntime`], and the runner, the only
//! process holding the container runtime's socket, checks every call
//! against its [`RunnerPolicy`] before its own runtime sees it.
//!
//! These run the real HTTP server and client over loopback, with an
//! in-memory runtime behind the server that records what reached it.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use loams_pg_control::ids::ComputeId;
use loams_pg_control::model::{ComputeBinding, Cu, EndpointRec, EndpointType};
use loams_pg_control::runtime::remote::RemoteRuntime;
use loams_pg_control::runtime::runner::{RunnerPolicy, RunnerServer, read_or_create_token};
use loams_pg_control::runtime::{
    BackendAddr, ComputeHandle, ComputeLaunch, ComputeRuntime, ResizeOutcome, RuntimeError,
    RuntimeStatus,
};
use loams_pg_control::secrets::Secret;

const PRJ: &str = "prj-01JZ0000000000000000000000";
const BR: &str = "br-01JZ0000000000000000000009";
const EP: &str = "ep-01JZ0000000000000000000001";
const CMP: &str = "cmp-01JZ0000000000000000000003";
const ALLOWED: &str = "ghcr.io/neondatabase/compute-node-v17@sha256:13ab146d3e7bbabb25a8532f315ac443e7512351d1ede0bab586def5c70e26c3";
const OTHER: &str = "docker.io/library/alpine@sha256:1111111111111111111111111111111111111111111111111111111111111111";
const RUNNER_TOKEN: &str = "runner-token-sentinel";
const UPCALL_TOKEN: &str = "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJjbXAifQ.c2lnbmF0dXJl";

/// What the fake runtime answers next, and what reached it.
#[derive(Default)]
struct Fake {
    calls: Mutex<Vec<String>>,
    launches: Mutex<Vec<ComputeLaunch>>,
    fail: Mutex<Option<RuntimeError>>,
}

impl Fake {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
    fn record(&self, call: String) -> Result<(), RuntimeError> {
        self.calls.lock().unwrap().push(call);
        match self.fail.lock().unwrap().take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl ComputeRuntime for Fake {
    async fn ensure_address(
        &self,
        ns: &str,
        ep: &EndpointRec,
    ) -> Result<BackendAddr, RuntimeError> {
        self.record(format!(
            "ensure_address {ns} {} {}",
            ep.id,
            ep.backend_addr.as_deref().unwrap_or("-")
        ))?;
        Ok(BackendAddr::new("127.0.0.1", 55500))
    }
    async fn remove_address(&self, _ns: &str, ep: &EndpointRec) -> Result<(), RuntimeError> {
        self.record(format!("remove_address {}", ep.id))
    }
    async fn start(&self, c: &ComputeLaunch) -> Result<ComputeHandle, RuntimeError> {
        self.record(format!("start {}", c.compute_id))?;
        self.launches.lock().unwrap().push(c.clone());
        Ok(ComputeHandle {
            compute_id: c.compute_id,
            ctl_url: format!("http://{}:3080", c.compute_id.to_string().to_lowercase()),
            address: c.address.clone(),
        })
    }
    async fn bind_address(
        &self,
        _ns: &str,
        ep: &EndpointRec,
        c: &ComputeHandle,
    ) -> Result<(), RuntimeError> {
        self.record(format!("bind_address {} {}", ep.id, c.compute_id))
    }
    async fn stop(&self, ns: &str, compute_id: &ComputeId) -> Result<(), RuntimeError> {
        self.record(format!("stop {ns} {compute_id}"))
    }
    async fn status(
        &self,
        _ns: &str,
        compute_id: &ComputeId,
    ) -> Result<RuntimeStatus, RuntimeError> {
        self.record(format!("status {compute_id}"))?;
        Ok(RuntimeStatus::Exited { code: Some(3) })
    }
    async fn resize(
        &self,
        _ns: &str,
        compute_id: &ComputeId,
        cu: Cu,
    ) -> Result<ResizeOutcome, RuntimeError> {
        self.record(format!("resize {compute_id} {}", cu.0))?;
        Ok(ResizeOutcome::Refused("no room".into()))
    }
}

struct Runner {
    fake: Arc<Fake>,
    remote: RemoteRuntime,
    url: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for Runner {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

async fn runner_with(client_token: &str) -> Runner {
    let fake = Arc::new(Fake::default());
    let policy = RunnerPolicy::new([ALLOWED.to_owned()]).unwrap();
    let server = RunnerServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        fake.clone(),
        policy,
        Secret::new(RUNNER_TOKEN.to_owned()),
    )
    .await
    .unwrap();
    let url = format!("http://{}", server.local_addr());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(server.serve(async move {
        let _ = stopped.await;
    }));
    let remote =
        RemoteRuntime::new(url.parse().unwrap(), Secret::new(client_token.to_owned())).unwrap();
    Runner {
        fake,
        remote,
        url,
        stop: Some(stop),
    }
}

async fn runner() -> Runner {
    runner_with(RUNNER_TOKEN).await
}

fn endpoint() -> EndpointRec {
    let mut ep = EndpointRec::new(PRJ, EP, BR, EndpointType::ReadWrite);
    ep.backend_addr = Some("127.0.0.1:55500".into());
    ep
}

fn launch() -> ComputeLaunch {
    ComputeLaunch {
        compute_id: CMP.parse().unwrap(),
        binding: Some(ComputeBinding {
            namespace: "acme".into(),
            project_id: PRJ.into(),
            endpoint_id: EP.into(),
        }),
        address: Some(BackendAddr::new("127.0.0.1", 55500)),
        image: ALLOWED.into(),
        cu: Cu(2),
        upcall_token: Secret::new(UPCALL_TOKEN.into()),
    }
}

#[tokio::test]
async fn remote_round_trips_every_call() {
    let r = runner().await;
    let ep = endpoint();
    let id: ComputeId = CMP.parse().unwrap();

    let addr = r.remote.ensure_address("acme", &ep).await.unwrap();
    assert_eq!(addr, BackendAddr::new("127.0.0.1", 55500));
    let handle = r.remote.start(&launch()).await.unwrap();
    assert_eq!(handle.compute_id, id);
    assert_eq!(
        handle.ctl_url,
        format!("http://{}:3080", CMP.to_lowercase())
    );
    assert_eq!(handle.address, Some(addr));
    r.remote.bind_address("acme", &ep, &handle).await.unwrap();
    assert_eq!(
        r.remote.status("acme", &id).await.unwrap(),
        RuntimeStatus::Exited { code: Some(3) }
    );
    assert_eq!(
        r.remote.resize("acme", &id, Cu(4)).await.unwrap(),
        ResizeOutcome::Refused("no room".into())
    );
    r.remote.stop("acme", &id).await.unwrap();
    r.remote.remove_address("acme", &ep).await.unwrap();

    assert_eq!(
        r.fake.calls(),
        [
            format!("ensure_address acme {EP} 127.0.0.1:55500"),
            format!("start {CMP}"),
            format!("bind_address {EP} {CMP}"),
            format!("status {CMP}"),
            format!("resize {CMP} 4"),
            format!("stop acme {CMP}"),
            format!("remove_address {EP}"),
        ]
    );
    // The launch arrives whole, its token included.
    let got = r.fake.launches.lock().unwrap()[0].clone();
    let want = launch();
    assert_eq!(got.binding, want.binding);
    assert_eq!(got.address, want.address);
    assert_eq!(got.image, want.image);
    assert_eq!(got.cu, want.cu);
    assert_eq!(got.upcall_token.expose(), want.upcall_token.expose());
}

#[tokio::test]
async fn remote_errors_keep_their_kind() {
    let r = runner().await;
    let id: ComputeId = CMP.parse().unwrap();
    for e in [
        RuntimeError::InvalidArgument("bad".into()),
        RuntimeError::PortsExhausted {
            start: 55500,
            end: 56000,
        },
        RuntimeError::Unavailable("compose failed".into()),
        RuntimeError::Timeout {
            what: "docker compose up".into(),
            after: Duration::from_secs(300),
        },
    ] {
        *r.fake.fail.lock().unwrap() = Some(e.clone());
        assert_eq!(r.remote.stop("acme", &id).await.unwrap_err(), e);
    }
}

#[tokio::test]
async fn runner_refuses_unlisted_images() {
    let r = runner().await;
    for image in [
        OTHER.to_owned(),
        "ghcr.io/neondatabase/compute-node-v17:latest".to_owned(),
        format!(
            "ghcr.io/example/neon-archive/compute-node-v17@{}",
            ALLOWED.split_once('@').unwrap().1
        ),
    ] {
        let mut c = launch();
        c.image = image.clone();
        let e = r.remote.start(&c).await.unwrap_err();
        assert!(
            matches!(&e, RuntimeError::InvalidArgument(m) if m.contains("not one the runner runs")),
            "{image}: {e:?}"
        );
    }
    assert!(r.fake.calls().is_empty(), "{:?}", r.fake.calls());
}

#[tokio::test]
async fn runner_refuses_a_wrong_or_missing_token() {
    let r = runner_with("not-the-token").await;
    let e = r.remote.start(&launch()).await.unwrap_err();
    assert!(
        matches!(&e, RuntimeError::Unavailable(m) if m.contains("refused the token")),
        "{e:?}"
    );
    let e = r
        .remote
        .ensure_address("acme", &endpoint())
        .await
        .unwrap_err();
    assert!(matches!(e, RuntimeError::Unavailable(_)), "{e:?}");

    let anonymous = reqwest::Client::new()
        .post(format!("{}/v1/stop", r.url))
        .json(&serde_json::json!({ "namespace": "acme", "compute_id": CMP }))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(r.fake.calls().is_empty(), "{:?}", r.fake.calls());
}

#[tokio::test]
async fn runner_refuses_malformed_launches_and_endpoints() {
    let r = runner().await;
    let mut cases: Vec<(&str, ComputeLaunch)> = Vec::new();
    let mut c = launch();
    c.binding = None;
    cases.push(("an unbound compute", c));
    let mut c = launch();
    c.binding.as_mut().unwrap().namespace = "../etc".into();
    cases.push(("a namespace that is no DNS label", c));
    let mut c = launch();
    c.binding.as_mut().unwrap().project_id = "prj-\"quoted\"".into();
    cases.push(("a project id", c));
    let mut c = launch();
    c.binding.as_mut().unwrap().endpoint_id = "ep-x\ny".into();
    cases.push(("an endpoint id", c));
    let mut c = launch();
    c.cu = Cu(0);
    cases.push(("no CPU", c));
    let mut c = launch();
    c.cu = Cu(65);
    cases.push(("over 16 CU", c));
    let mut c = launch();
    c.upcall_token = Secret::new(format!("{UPCALL_TOKEN}\nLD_PRELOAD=/tmp/x.so"));
    cases.push(("a token that leaves its line", c));
    let mut c = launch();
    c.upcall_token = Secret::new(String::new());
    cases.push(("an empty token", c));
    for (what, c) in cases {
        let e = r.remote.start(&c).await.unwrap_err();
        assert!(
            matches!(e, RuntimeError::InvalidArgument(_)),
            "{what}: {e:?}"
        );
    }

    let mut ep = endpoint();
    ep.id = "ep-not-an-id".into();
    let e = r.remote.ensure_address("acme", &ep).await.unwrap_err();
    assert!(matches!(e, RuntimeError::InvalidArgument(_)), "{e:?}");
    let e = r
        .remote
        .ensure_address("../etc", &endpoint())
        .await
        .unwrap_err();
    assert!(matches!(e, RuntimeError::InvalidArgument(_)), "{e:?}");
    let e = r
        .remote
        .stop("Acme Corp", &CMP.parse().unwrap())
        .await
        .unwrap_err();
    assert!(matches!(e, RuntimeError::InvalidArgument(_)), "{e:?}");
    let mut ep = endpoint();
    ep.project_id = "acme".into();
    let e = r.remote.remove_address("acme", &ep).await.unwrap_err();
    assert!(matches!(e, RuntimeError::InvalidArgument(_)), "{e:?}");

    let garbage = reqwest::Client::new()
        .post(format!("{}/v1/start", r.url))
        .bearer_auth(RUNNER_TOKEN)
        .body("{not json")
        .send()
        .await
        .unwrap();
    assert_eq!(garbage.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(r.fake.calls().is_empty(), "{:?}", r.fake.calls());
}

#[tokio::test]
async fn health_needs_no_token_and_says_nothing_else() {
    let r = runner().await;
    let resp = reqwest::get(format!("{}/health", r.url)).await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let unknown = reqwest::get(format!("{}/v1/images", r.url)).await.unwrap();
    assert_eq!(unknown.status(), reqwest::StatusCode::NOT_FOUND);
}

#[test]
fn remote_needs_an_http_url() {
    for url in ["unix:///var/run/docker.sock", "file:///tmp/x", "ftp://x/"] {
        let e = RemoteRuntime::new(url.parse().unwrap(), Secret::new("t".into())).unwrap_err();
        assert!(matches!(e, RuntimeError::InvalidArgument(_)), "{url}");
    }
    let remote = RemoteRuntime::new(
        "http://pg-runner:7691".parse().unwrap(),
        Secret::new(RUNNER_TOKEN.into()),
    )
    .unwrap();
    assert!(!format!("{remote:?}").contains(RUNNER_TOKEN));
}

#[test]
fn policy_takes_only_runnable_images() {
    assert!(RunnerPolicy::new(["ghcr.io/x/compute:latest".to_owned()]).is_err());
    assert!(
        RunnerPolicy::new([format!(
            "ghcr.io/example/neon-archive/compute-node-v17@{}",
            ALLOWED.split_once('@').unwrap().1
        )])
        .is_err()
    );
    assert!(RunnerPolicy::new(Vec::<String>::new()).is_err());
    let images: BTreeMap<u32, String> = loams_pg_control::runtime::images::pinned().unwrap();
    let policy = RunnerPolicy::new(images.values().cloned()).unwrap();
    assert!(policy.images().all(|i| images.values().any(|p| p == i)));
}

#[test]
fn the_token_file_is_created_once_and_private() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let path = dir.path().join("runner").join("token");
    assert!(read_or_create_token(&path, false).is_err());
    let first = read_or_create_token(&path, true).unwrap();
    assert!(first.expose().len() >= 43, "{}", first.expose().len());
    let again = read_or_create_token(&path, true).unwrap();
    assert_eq!(first.expose(), again.expose());
    assert_eq!(
        read_or_create_token(&path, false).unwrap().expose(),
        first.expose()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "{mode:o}");
    }
    std::fs::write(&path, "  \n").unwrap();
    assert!(read_or_create_token(&path, true).is_err());
}
