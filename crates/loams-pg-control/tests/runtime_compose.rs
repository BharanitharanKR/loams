//! The compose runtime (PG2 Task 10), against a fake command runner that
//! keeps the containers a real `docker compose` would: every command is an
//! argument array, and its effect is recorded.
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use loams_pg_control::model::{
    ComputeBinding, Cu, DesiredState, EndpointRec, EndpointState, EndpointType, PoolMode,
};
use loams_pg_control::runtime::compose::{
    CommandOutput, CommandRunner, ComposeBin, ComposeConfig, ComposeRuntime, CtlReach,
    DEFAULT_PORT_RANGE, Invocation,
};
use loams_pg_control::runtime::{
    BackendAddr, COMMAND_TIMEOUT, ComputeLaunch, ComputeRuntime, ResizeOutcome, RuntimeError,
    RuntimeStatus,
};
use loams_pg_control::secrets::Secret;

const PRJ: &str = "prj-01JZ0000000000000000000000";
const EP_A: &str = "ep-01JZ0000000000000000000001";
const EP_B: &str = "ep-01JZ0000000000000000000002";
const CMP_A: &str = "cmp-01JZ0000000000000000000003";
const CMP_B: &str = "cmp-01JZ0000000000000000000004";
const CMP_A2: &str = "cmp-01JZ0000000000000000000005";
const IMAGE: &str = "ghcr.io/neondatabase/compute-node-v17@sha256:13ab146d3e7bbabb25a8532f315ac443e7512351d1ede0bab586def5c70e26c3";
const TOKEN: &str = "upcall-token-sentinel";

/// What the fake knows of one compose project's containers.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Container {
    Running,
    Exited(i32),
}

/// A fake `docker compose`: records each call, and keeps the containers of
/// each project the way `up`, `rm` and `ps` would.
#[derive(Default)]
struct Fake {
    calls: Mutex<Vec<Vec<String>>>,
    containers: Mutex<BTreeMap<(String, String), Container>>,
    fail_up: Mutex<bool>,
}

impl Fake {
    fn calls(&self) -> Vec<Vec<String>> {
        self.calls.lock().unwrap().clone()
    }
    /// The subcommands called, as `<sub> <service>`.
    fn subs(&self) -> Vec<String> {
        self.calls()
            .iter()
            .map(|c| format!("{} {}", c[6], service(c)))
            .collect()
    }
    fn exit(&self, project: &str, service: &str, code: i32) {
        self.containers
            .lock()
            .unwrap()
            .insert((project.into(), service.into()), Container::Exited(code));
    }
}

/// The service a call names: `port <service> <port>`, else the last
/// argument.
fn service(argv: &[String]) -> &str {
    if argv[6] == "port" {
        &argv[7]
    } else {
        argv.last().unwrap()
    }
}

#[async_trait]
impl CommandRunner for Fake {
    async fn run(&self, inv: &Invocation) -> Result<CommandOutput, RuntimeError> {
        let mut argv = vec![inv.program.to_string_lossy().into_owned()];
        argv.extend(inv.args.iter().cloned());
        assert_eq!(inv.timeout, COMMAND_TIMEOUT);
        self.calls.lock().unwrap().push(argv.clone());
        // docker compose -f <file> -p <project> <sub> … <service>
        assert_eq!(&argv[..2], ["/usr/bin/docker", "compose"], "{argv:?}");
        assert_eq!(argv[2], "-f");
        assert!(Path::new(&argv[3]).is_file(), "{argv:?}");
        assert_eq!(argv[4], "-p");
        let key = (argv[5].clone(), service(&argv).to_owned());
        let ok = |stdout: &str| {
            Ok(CommandOutput {
                code: Some(0),
                stdout: stdout.into(),
                stderr: String::new(),
            })
        };
        let mut containers = self.containers.lock().unwrap();
        match argv[6].as_str() {
            "up" => {
                assert_eq!(&argv[7..9], ["-d", "--no-build"]);
                if *self.fail_up.lock().unwrap() {
                    return Ok(CommandOutput {
                        code: Some(1),
                        stdout: String::new(),
                        stderr: "Error: port is already allocated".into(),
                    });
                }
                containers.insert(key, Container::Running);
                ok("")
            }
            "rm" => {
                assert_eq!(&argv[7..9], ["-s", "-f"]);
                containers.remove(&key);
                ok("")
            }
            "ps" => {
                assert_eq!(&argv[7..10], ["-a", "--format", "json"]);
                match containers.get(&key) {
                    None => ok(""),
                    Some(Container::Running) => ok(&format!(
                        "{{\"Service\":\"{}\",\"State\":\"running\",\"ExitCode\":0}}\n",
                        key.1
                    )),
                    Some(Container::Exited(code)) => ok(&format!(
                        "{{\"Service\":\"{}\",\"State\":\"exited\",\"ExitCode\":{code}}}\n",
                        key.1
                    )),
                }
            }
            "port" => {
                assert_eq!(argv.len(), 9, "{argv:?}");
                assert_eq!(argv[8], "3080");
                ok("127.0.0.1:49153\n")
            }
            other => panic!("unexpected subcommand {other}"),
        }
    }
}

fn docker() -> ComposeBin {
    ComposeBin {
        program: PathBuf::from("/usr/bin/docker"),
        args: vec!["compose".into()],
    }
}

fn config(dir: &Path) -> ComposeConfig {
    ComposeConfig {
        network: Some("loams-neon_default".into()),
        extra_hosts: vec!["host.docker.internal:host-gateway".into()],
        ..ComposeConfig::new(
            docker(),
            dir.into(),
            "http://host.docker.internal:7690".into(),
        )
    }
}

fn open(dir: &Path, fake: &Arc<Fake>) -> ComposeRuntime {
    ComposeRuntime::open(config(dir), fake.clone()).unwrap()
}

fn endpoint(id: &str) -> EndpointRec {
    EndpointRec {
        project_id: PRJ.into(),
        id: id.into(),
        branch_id: "br-01JZ0000000000000000000009".into(),
        kind: EndpointType::ReadWrite,
        min_cu: Cu(1),
        max_cu: Cu(4),
        suspend_timeout_s: 300,
        pool_mode: PoolMode::Transaction,
        pg_settings: BTreeMap::new(),
        desired: DesiredState::Running,
        state: EndpointState::Idle,
        compute_id: None,
        backend_addr: None,
        failure: None,
    }
}

fn launch(compute: &str, ep: &str, addr: &BackendAddr, cu: Cu) -> ComputeLaunch {
    ComputeLaunch {
        compute_id: compute.parse().unwrap(),
        binding: Some(ComputeBinding {
            namespace: "acme".into(),
            project_id: PRJ.into(),
            endpoint_id: ep.into(),
        }),
        address: Some(addr.clone()),
        image: IMAGE.into(),
        cu,
        upcall_token: Secret::new(TOKEN.into()),
    }
}

/// The two computes of `acme` produce `tests/fixtures/compose/acme.yaml`,
/// byte for byte, whichever starts first. `LOAMS_BLESS=1` rewrites it.
#[tokio::test]
async fn compose_file_golden() {
    let mut files = Vec::new();
    for order in [[CMP_A, CMP_B], [CMP_B, CMP_A]] {
        let dir = tempfile::tempdir().unwrap();
        let fake = Arc::new(Fake::default());
        let rt = open(dir.path(), &fake);
        let a = rt.ensure_address(&endpoint(EP_A)).await.unwrap();
        let b = rt.ensure_address(&endpoint(EP_B)).await.unwrap();
        for c in order {
            let l = if c == CMP_A {
                launch(CMP_A, EP_A, &a, Cu(4))
            } else {
                launch(CMP_B, EP_B, &b, Cu(1))
            };
            rt.start(&l).await.unwrap();
        }
        files.push(std::fs::read_to_string(dir.path().join("acme.yaml")).unwrap());
    }
    assert_eq!(files[0], files[1], "the file depends on the start order");
    let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compose/acme.yaml");
    if std::env::var_os("LOAMS_BLESS").is_some() {
        std::fs::create_dir_all(golden.parent().unwrap()).unwrap();
        std::fs::write(&golden, &files[0]).unwrap();
    }
    assert_eq!(files[0], std::fs::read_to_string(&golden).unwrap());
    // The upcall token is never in the compose file.
    assert!(!files[0].contains(TOKEN));
}

/// A second start of the same compute runs no second `up`, and answers the
/// same handle.
#[tokio::test]
async fn start_is_idempotent_by_compute_id() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let rt = open(dir.path(), &fake);
    let addr = rt.ensure_address(&endpoint(EP_A)).await.unwrap();
    let l = launch(CMP_A, EP_A, &addr, Cu(4));
    let first = rt.start(&l).await.unwrap();
    let second = rt.start(&l).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(first.ctl_url, "http://127.0.0.1:49153");
    let ups = fake.subs().iter().filter(|s| s.starts_with("up ")).count();
    assert_eq!(ups, 1, "{:?}", fake.subs());
    // Every call is an argument array: the binary, then compose's own
    // arguments, then the file and the project.
    let up = fake.calls().into_iter().find(|c| c[6] == "up").unwrap();
    assert_eq!(
        up,
        [
            "/usr/bin/docker".to_string(),
            "compose".into(),
            "-f".into(),
            dir.path().join("acme.yaml").display().to_string(),
            "-p".into(),
            "loams-pg-acme".into(),
            "up".into(),
            "-d".into(),
            "--no-build".into(),
            "cmp-01jz0000000000000000000003".into(),
        ]
    );
    // A compute whose `up` failed is started again on the next call.
    let other = rt.ensure_address(&endpoint(EP_B)).await.unwrap();
    *fake.fail_up.lock().unwrap() = true;
    let lb = launch(CMP_B, EP_B, &other, Cu(4));
    let err = rt.start(&lb).await.unwrap_err();
    assert!(matches!(err, RuntimeError::Unavailable(_)), "{err}");
    assert!(
        err.to_string().contains("port is already allocated"),
        "{err}"
    );
    *fake.fail_up.lock().unwrap() = false;
    rt.start(&lb).await.unwrap();
    assert_eq!(
        rt.status(&CMP_B.parse().unwrap()).await.unwrap(),
        RuntimeStatus::Running
    );
}

/// Stopping a compute the runtime does not know (never started, already
/// stopped, or stopped before a restart) is not an error, and runs nothing.
#[tokio::test]
async fn stop_missing_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let rt = open(dir.path(), &fake);
    rt.stop(&CMP_A.parse().unwrap()).await.unwrap();
    assert!(fake.calls().is_empty());

    let addr = rt.ensure_address(&endpoint(EP_A)).await.unwrap();
    rt.start(&launch(CMP_A, EP_A, &addr, Cu(4))).await.unwrap();
    rt.stop(&CMP_A.parse().unwrap()).await.unwrap();
    assert_eq!(
        fake.subs().last().unwrap(),
        "rm cmp-01jz0000000000000000000003"
    );
    assert_eq!(
        rt.status(&CMP_A.parse().unwrap()).await.unwrap(),
        RuntimeStatus::Missing
    );
    let calls = fake.calls().len();
    rt.stop(&CMP_A.parse().unwrap()).await.unwrap();
    assert_eq!(fake.calls().len(), calls);
    // The compute's env file (its token) is gone with it.
    assert!(
        !dir.path()
            .join("env")
            .join("cmp-01jz0000000000000000000003.env")
            .exists()
    );
    // The namespace's last compute took its compose file and state with it.
    assert!(!dir.path().join("acme.yaml").exists());
    assert!(!dir.path().join("state").join("acme.json").exists());
}

/// An endpoint keeps its host port: across calls, across a restart of the
/// runtime, and when the record already names it. Other endpoints get
/// other ports; a removed endpoint's port is free again.
#[tokio::test]
async fn port_is_stable_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let a = {
        let rt = open(dir.path(), &fake);
        let a = rt.ensure_address(&endpoint(EP_A)).await.unwrap();
        assert_eq!(rt.ensure_address(&endpoint(EP_A)).await.unwrap(), a);
        a
    };
    assert_eq!(a, BackendAddr::new("127.0.0.1", DEFAULT_PORT_RANGE.start));
    let rt = open(dir.path(), &fake);
    assert_eq!(rt.ensure_address(&endpoint(EP_A)).await.unwrap(), a);
    let b = rt.ensure_address(&endpoint(EP_B)).await.unwrap();
    assert_ne!(a.port, b.port);

    // A record that names its address keeps it, even one the runtime's
    // table lost.
    let fresh = tempfile::tempdir().unwrap();
    let rt2 = open(fresh.path(), &fake);
    let mut rec = endpoint(EP_B);
    rec.backend_addr = Some(b.to_string());
    assert_eq!(rt2.ensure_address(&rec).await.unwrap(), b);
    // …and the port is then taken for any other endpoint.
    let c = rt2.ensure_address(&endpoint(EP_A)).await.unwrap();
    assert_ne!(c.port, b.port);
    // A record naming another endpoint's port is refused.
    let mut clash = endpoint("ep-01JZ0000000000000000000007");
    clash.backend_addr = Some(c.to_string());
    let err = rt2.ensure_address(&clash).await.unwrap_err();
    assert!(matches!(err, RuntimeError::InvalidArgument(_)), "{err}");

    // Removing the address frees the port.
    rt.remove_address(&endpoint(EP_A)).await.unwrap();
    rt.remove_address(&endpoint(EP_A)).await.unwrap();
    let d = rt
        .ensure_address(&endpoint("ep-01JZ0000000000000000000008"))
        .await
        .unwrap();
    assert_eq!(d, a);
}

/// A token file no recorded compute names (a crash between writing it and
/// the state) is removed when the runtime opens; a recorded one stays.
#[tokio::test]
async fn open_removes_tokens_of_unrecorded_computes() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    {
        let rt = open(dir.path(), &fake);
        let addr = rt.ensure_address(&endpoint(EP_A)).await.unwrap();
        rt.start(&launch(CMP_A, EP_A, &addr, Cu(4))).await.unwrap();
    }
    let env = dir.path().join("env");
    std::fs::write(env.join("cmp-01jz0000000000000000000009.env"), "x").unwrap();
    let rt = open(dir.path(), &fake);
    assert!(!env.join("cmp-01jz0000000000000000000009.env").exists());
    assert!(env.join("cmp-01jz0000000000000000000003.env").exists());
    // The reopened runtime still knows the compute, and stops it.
    assert_eq!(
        rt.status(&CMP_A.parse().unwrap()).await.unwrap(),
        RuntimeStatus::Running
    );
    rt.stop(&CMP_A.parse().unwrap()).await.unwrap();
    assert!(!env.join("cmp-01jz0000000000000000000003.env").exists());
}

/// The range runs out.
#[tokio::test]
async fn ports_run_out() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let cfg = ComposeConfig {
        port_range: 55500..55502,
        ..config(dir.path())
    };
    let rt = ComposeRuntime::open(cfg, fake).unwrap();
    rt.ensure_address(&endpoint(EP_A)).await.unwrap();
    rt.ensure_address(&endpoint(EP_B)).await.unwrap();
    let err = rt
        .ensure_address(&endpoint("ep-01JZ0000000000000000000007"))
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeError::PortsExhausted { .. }), "{err}");
}

/// AP1e Task 21's order: `docker compose`, `podman compose`, then the
/// standalone `docker-compose`.
#[test]
fn compose_bin_detection_order() {
    let on = |present: &'static [&'static str]| {
        move |bin: &str| {
            present
                .contains(&bin)
                .then(|| PathBuf::from(format!("/usr/bin/{bin}")))
        }
    };
    let all = ComposeBin::detect(on(&["docker", "podman", "docker-compose"])).unwrap();
    assert_eq!(all.program, PathBuf::from("/usr/bin/docker"));
    assert_eq!(all.args, ["compose"]);
    let podman = ComposeBin::detect(on(&["podman", "docker-compose"])).unwrap();
    assert_eq!(podman.program, PathBuf::from("/usr/bin/podman"));
    assert_eq!(podman.args, ["compose"]);
    let standalone = ComposeBin::detect(on(&["docker-compose"])).unwrap();
    assert_eq!(standalone.program, PathBuf::from("/usr/bin/docker-compose"));
    assert!(standalone.args.is_empty());
    assert!(ComposeBin::detect(on(&[])).is_none());
}

/// Starting a new compute for an endpoint removes the endpoint's previous
/// compute first: both would publish the endpoint's port, and a replaced
/// read-write compute must not keep running.
#[tokio::test]
async fn a_new_compute_replaces_the_endpoints_previous_one() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let rt = open(dir.path(), &fake);
    let addr = rt.ensure_address(&endpoint(EP_A)).await.unwrap();
    rt.start(&launch(CMP_A, EP_A, &addr, Cu(4))).await.unwrap();
    let new = rt.start(&launch(CMP_A2, EP_A, &addr, Cu(4))).await.unwrap();
    assert_eq!(
        fake.subs(),
        [
            "ps cmp-01jz0000000000000000000003",
            "up cmp-01jz0000000000000000000003",
            "port cmp-01jz0000000000000000000003",
            "rm cmp-01jz0000000000000000000003",
            "ps cmp-01jz0000000000000000000005",
            "up cmp-01jz0000000000000000000005",
            "port cmp-01jz0000000000000000000005",
        ]
    );
    assert_eq!(new.address, Some(addr.clone()));
    let mut ep = endpoint(EP_A);
    ep.backend_addr = Some(addr.to_string());
    rt.bind_address(&ep, &new).await.unwrap();
    assert_eq!(
        rt.status(&CMP_A.parse().unwrap()).await.unwrap(),
        RuntimeStatus::Missing
    );
}

/// `status` reads the container's state; `resize` is not offered on
/// compose.
#[tokio::test]
async fn status_and_resize() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let rt = open(dir.path(), &fake);
    let id = CMP_A.parse().unwrap();
    assert_eq!(rt.status(&id).await.unwrap(), RuntimeStatus::Missing);
    let addr = rt.ensure_address(&endpoint(EP_A)).await.unwrap();
    rt.start(&launch(CMP_A, EP_A, &addr, Cu(4))).await.unwrap();
    assert_eq!(rt.status(&id).await.unwrap(), RuntimeStatus::Running);
    fake.exit("loams-pg-acme", "cmp-01jz0000000000000000000003", 137);
    assert_eq!(
        rt.status(&id).await.unwrap(),
        RuntimeStatus::Exited { code: Some(137) }
    );
    assert_eq!(
        rt.resize(&id, Cu(8)).await.unwrap(),
        ResizeOutcome::Unsupported
    );
}

/// What `start` refuses before it runs anything: an image not pinned by
/// digest, a namespace that cannot name a compose project, an unbound
/// compute, no address, and a port outside the range. `bind_address`
/// refuses an address the compute was not started with.
#[tokio::test]
async fn bad_launches_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let rt = open(dir.path(), &fake);
    let addr = rt.ensure_address(&endpoint(EP_A)).await.unwrap();
    let good = launch(CMP_A, EP_A, &addr, Cu(4));
    let mut cases = Vec::new();
    let mut l = good.clone();
    l.image = "ghcr.io/neondatabase/compute-node-v17:latest".into();
    cases.push(l);
    let mut l = good.clone();
    l.binding.as_mut().unwrap().namespace = "Acme Corp".into();
    cases.push(l);
    let mut l = good.clone();
    l.binding = None;
    cases.push(l);
    let mut l = good.clone();
    l.address = None;
    cases.push(l);
    let mut l = good.clone();
    l.address = Some(BackendAddr::new("127.0.0.1", 5432));
    cases.push(l);
    for l in cases {
        let err = rt.start(&l).await.unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidArgument(_)), "{err}");
    }
    assert!(fake.calls().is_empty(), "{:?}", fake.calls());

    let handle = rt.start(&good).await.unwrap();
    let mut other = endpoint(EP_B);
    other.backend_addr = Some(BackendAddr::new("127.0.0.1", addr.port + 1).to_string());
    let err = rt.bind_address(&other, &handle).await.unwrap_err();
    assert!(matches!(err, RuntimeError::InvalidArgument(_)), "{err}");
}

/// The upcall token goes only to the compute's env file, mode 0600, and
/// the runtime's files are private to its user.
#[cfg(unix)]
#[tokio::test]
async fn the_token_is_only_in_a_private_env_file() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let rt = open(dir.path(), &fake);
    let addr = rt.ensure_address(&endpoint(EP_A)).await.unwrap();
    rt.start(&launch(CMP_A, EP_A, &addr, Cu(4))).await.unwrap();
    let env = dir
        .path()
        .join("env")
        .join("cmp-01jz0000000000000000000003.env");
    assert_eq!(
        std::fs::read_to_string(&env).unwrap(),
        format!("NEON_CONTROL_PLANE_TOKEN={TOKEN}\n")
    );
    let mut holders = BTreeSet::new();
    for entry in walk(dir.path()) {
        let mode = std::fs::metadata(&entry).unwrap().permissions().mode() & 0o777;
        if entry.is_dir() {
            assert_eq!(mode, 0o700, "{}", entry.display());
        } else {
            assert_eq!(mode, 0o600, "{}", entry.display());
            if std::fs::read_to_string(&entry).unwrap().contains(TOKEN) {
                holders.insert(entry);
            }
        }
    }
    assert_eq!(holders, BTreeSet::from([env]));
    for call in fake.calls() {
        assert!(!call.iter().any(|a| a.contains(TOKEN)), "{call:?}");
    }
    // Debug never shows the token.
    let l = launch(CMP_A, EP_A, &addr, Cu(4));
    assert!(!format!("{l:?}").contains(TOKEN));
}

#[cfg(unix)]
fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(walk(&p));
        }
        out.push(p);
    }
    out
}

/// The real runner: a command that outlives its timeout is killed and
/// reported as a timeout; a missing binary is `Unavailable`.
#[tokio::test]
async fn the_runner_times_out_and_kills() {
    use loams_pg_control::runtime::compose::TokioCommandRunner;
    let started = std::time::Instant::now();
    let err = TokioCommandRunner
        .run(&Invocation {
            program: PathBuf::from("sleep"),
            args: vec!["30".into()],
            timeout: Duration::from_millis(200),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeError::Timeout { .. }), "{err}");
    assert!(started.elapsed() < Duration::from_secs(10));
    let out = TokioCommandRunner
        .run(&Invocation {
            program: PathBuf::from("sh"),
            args: vec!["-c".into(), "echo out; echo err >&2; exit 3".into()],
            timeout: Duration::from_secs(10),
        })
        .await
        .unwrap();
    assert_eq!(out.code, Some(3));
    assert_eq!(out.stdout, "out\n");
    assert_eq!(out.stderr, "err\n");
    let err = TokioCommandRunner
        .run(&Invocation {
            program: PathBuf::from("/nonexistent/loams-compose"),
            args: vec![],
            timeout: Duration::from_secs(1),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, RuntimeError::Unavailable(_)), "{err}");
}

/// In network reach (`pg-control` on the compose network), `compute_ctl`
/// is the service's name, and no port is published for it.
#[tokio::test]
async fn network_reach_uses_the_service_name() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let cfg = ComposeConfig {
        ctl_reach: CtlReach::Network,
        ..config(dir.path())
    };
    let rt = ComposeRuntime::open(cfg, fake.clone()).unwrap();
    let addr = rt.ensure_address(&endpoint(EP_A)).await.unwrap();
    let h = rt.start(&launch(CMP_A, EP_A, &addr, Cu(4))).await.unwrap();
    assert_eq!(h.ctl_url, "http://cmp-01jz0000000000000000000003:3080");
    assert!(!fake.subs().iter().any(|s| s.starts_with("port ")));
    let file = std::fs::read_to_string(dir.path().join("acme.yaml")).unwrap();
    assert!(!file.contains(":3080\""), "{file}");
}
