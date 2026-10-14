//! The compose runtime (design §46 §7.2; PG2 Task 10): each compute is a
//! service of a compose project per Loams namespace, run with `docker
//! compose`, `podman compose` or `docker-compose` (detected in that order,
//! as AP1e Task 21 does).
//!
//! **Files** under [`ComposeConfig::project_dir`] (`<data>/pg/compose`),
//! all private to the user (directories 0700, files 0600):
//!
//! | File | Holds |
//! |---|---|
//! | `<ns>.yaml` | the compose file of namespace `<ns>` (project `loams-pg-<ns>`), generated deterministically from its state |
//! | `state/<ns>.json` | the namespace's computes: what the compose file is rendered from |
//! | `state/ports.json` | each endpoint's host port |
//! | `env/<service>.env` | one compute's `NEON_CONTROL_PLANE_TOKEN`, the only place its token is written |
//!
//! The state is written before the compose file, and [`ComposeRuntime::open`]
//! renders every compose file again, so the files always follow the state.
//!
//! **Commands** are argument arrays (never a shell string): `up -d
//! --no-build <service>`, `rm -s -f <service>`, `ps -a --format json
//! <service>` and `port <service> 3080`, each with `-f <ns>.yaml -p
//! loams-pg-<ns>`, killed after [`COMMAND_TIMEOUT`]. Commands of one
//! namespace run one at a time; namespaces do not wait for each other.
//!
//! **Addresses.** Each endpoint gets a host port from
//! [`ComposeConfig::port_range`] (`55500..56000`), kept in
//! `state/ports.json` and recorded by the reconciler in `E/` (an endpoint
//! whose record names its port keeps it). Its compute publishes Postgres
//! there, on [`ComposeConfig::publish_ip`]. On the compose network the
//! compute also answers to the alias `ep-<endpoint ulid>` (lower case), so
//! a container on that network (PgDog) reaches the endpoint without the
//! host. Compose fixes a container's published ports when it starts, so a
//! compute is bound to its endpoint's address at [`start`], and starting a
//! new compute for an endpoint removes the endpoint's previous one first
//! (two computes cannot publish one port, and a replaced read-write compute
//! must not keep running). [`bind_address`] only checks the binding. An
//! unbound warm-pool compute has no address, so the compose runtime does
//! not start one: compose has no warm pool (R14.2).
//!
//! **Fitting the compose stacks.** The computes join
//! [`ComposeConfig::network`], the network of the storage stack
//! (`deploy/loams-postgres-dev`'s `loams-neon_default`, or the preview
//! release's `loams_default`, which includes it), so the spec's
//! `pageserver:6400` and `safekeeper1:5454` resolve. `compute_ctl` reaches
//! the upcall listener at [`ComposeConfig::control_plane_uri`].
//! `pg-control` reaches `compute_ctl` per [`CtlReach`]: on the host
//! through a loopback port the container publishes, or on the compose
//! network by the service's name.
//!
//! [`start`]: ComputeRuntime::start
//! [`bind_address`]: ComputeRuntime::bind_address

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{
    BackendAddr, COMMAND_TIMEOUT, COMPUTE_CTL_PORT, COMPUTE_PG_PORT, ComputeHandle, ComputeLaunch,
    ComputeRuntime, ResizeOutcome, RuntimeError, RuntimeStatus, check_upcall_token,
    compute_ctl_args, is_pinned,
};
use crate::ids::ComputeId;
use crate::model::{Cu, EndpointRec};

/// The host ports endpoints get (§46 §7.2; Task 10).
pub const DEFAULT_PORT_RANGE: Range<u16> = 55500..56000;

/// The format of the state files.
const STATE_FORMAT: u8 = 1;

/// The most of a failed command's stderr an error repeats.
const STDERR_TAIL: usize = 2000;

/// A compose command line's start: the binary and the arguments that put
/// it in compose mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeBin {
    pub program: PathBuf,
    pub args: Vec<String>,
}

impl ComposeBin {
    /// The first of `docker compose`, `podman compose` and `docker-compose`
    /// whose binary `which` finds (AP1e Task 21's order).
    pub fn detect(which: impl Fn(&str) -> Option<PathBuf>) -> Option<Self> {
        let compose = |program| ComposeBin {
            program,
            args: vec!["compose".into()],
        };
        which("docker")
            .map(compose)
            .or_else(|| which("podman").map(compose))
            .or_else(|| {
                which("docker-compose").map(|program| ComposeBin {
                    program,
                    args: Vec::new(),
                })
            })
    }

    /// [`detect`](Self::detect) over the directories of `PATH`.
    pub fn from_path() -> Option<Self> {
        let path = std::env::var_os("PATH")?;
        Self::detect(|bin| {
            std::env::split_paths(&path)
                .map(|dir| dir.join(bin))
                .find(|p| is_executable(p))
        })
    }
}

fn is_executable(p: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        p.is_file()
    }
}

/// How `pg-control` reaches a compute's `compute_ctl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CtlReach {
    /// `pg-control` runs on the host (single-node mode, the desktop): each
    /// compute publishes `compute_ctl` on a loopback port the runtime asks
    /// compose for.
    #[default]
    Host,
    /// `pg-control` runs on the compose network: `http://<service>:3080`.
    Network,
}

/// The compose runtime's settings.
#[derive(Debug, Clone)]
pub struct ComposeConfig {
    pub bin: ComposeBin,
    /// `<data>/pg/compose`.
    pub project_dir: PathBuf,
    pub port_range: Range<u16>,
    /// The host address endpoints' ports are published on.
    pub publish_ip: IpAddr,
    /// The external network the computes join: the storage stack's.
    /// `None`: each namespace's project gets its own default network.
    pub network: Option<String>,
    /// `compute_ctl`'s `--control-plane-uri`: the upcall listener as the
    /// computes reach it.
    pub control_plane_uri: String,
    /// `extra_hosts` of every compute (`host.docker.internal:host-gateway`
    /// when the upcall listener is on the host).
    pub extra_hosts: Vec<String>,
    pub ctl_reach: CtlReach,
}

impl ComposeConfig {
    /// The defaults: [`DEFAULT_PORT_RANGE`] on 127.0.0.1, no network or
    /// extra hosts, `compute_ctl` reached from the host.
    pub fn new(bin: ComposeBin, project_dir: PathBuf, control_plane_uri: String) -> Self {
        ComposeConfig {
            bin,
            project_dir,
            port_range: DEFAULT_PORT_RANGE,
            publish_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            network: None,
            control_plane_uri,
            extra_hosts: Vec::new(),
            ctl_reach: CtlReach::Host,
        }
    }

    /// The host of the endpoints' addresses: the publish address, or
    /// loopback when that is unspecified.
    fn address_host(&self) -> String {
        if self.publish_ip.is_unspecified() {
            Ipv4Addr::LOCALHOST.to_string()
        } else {
            self.publish_ip.to_string()
        }
    }
}

/// One command to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invocation {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub timeout: Duration,
}

/// What a command printed, and how it ended (`None`: killed by a signal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// Runs commands ([`TokioCommandRunner`]; tests fake it).
#[async_trait]
pub trait CommandRunner: Send + Sync + 'static {
    /// Runs `inv` to its end or its timeout.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Timeout`] when it outlived `inv.timeout` (it is
    /// killed); [`RuntimeError::Unavailable`] when it could not start. A
    /// command that ran and failed is an `Ok` with its code.
    async fn run(&self, inv: &Invocation) -> Result<CommandOutput, RuntimeError>;
}

/// [`CommandRunner`] over `tokio::process`: no shell, stdin closed, the
/// child killed when its timeout passes.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioCommandRunner;

#[async_trait]
impl CommandRunner for TokioCommandRunner {
    async fn run(&self, inv: &Invocation) -> Result<CommandOutput, RuntimeError> {
        let what = describe(inv);
        let child = tokio::process::Command::new(&inv.program)
            .args(&inv.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| RuntimeError::Unavailable(format!("starting {what}: {e}")))?;
        // On a timeout the child is dropped, which kills it.
        match tokio::time::timeout(inv.timeout, child.wait_with_output()).await {
            Err(_) => Err(RuntimeError::Timeout {
                what,
                after: inv.timeout,
            }),
            Ok(Err(e)) => Err(RuntimeError::Unavailable(format!("running {what}: {e}"))),
            Ok(Ok(out)) => Ok(CommandOutput {
                code: out.status.code(),
                stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            }),
        }
    }
}

/// The program and its subcommand words (the arguments up to the first
/// flag or path), for errors.
fn describe(inv: &Invocation) -> String {
    let mut out = inv.program.file_name().map_or_else(
        || inv.program.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    for a in inv.args.iter().take_while(|a| !a.starts_with('-')) {
        out.push(' ');
        out.push_str(a);
    }
    out
}

/// One compute of a namespace, as the state file keeps it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Service {
    project_id: String,
    endpoint_id: String,
    image: String,
    cu: Cu,
    /// The endpoint's host port.
    port: u16,
}

/// `state/<ns>.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct NamespaceState {
    format: u8,
    /// Compute id (canonical) → its service.
    computes: BTreeMap<String, Service>,
}

/// `state/ports.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PortsState {
    format: u8,
    /// Endpoint id → host port.
    endpoints: BTreeMap<String, u16>,
}

#[derive(Debug, Default)]
struct State {
    ports: PortsState,
    namespaces: BTreeMap<String, NamespaceState>,
}

/// The compose runtime. See the module doc.
pub struct ComposeRuntime {
    config: ComposeConfig,
    runner: Arc<dyn CommandRunner>,
    state: Mutex<State>,
    /// One per namespace: its commands and file writes run one at a time.
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl std::fmt::Debug for ComposeRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComposeRuntime")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ComposeRuntime {
    /// Opens the runtime over `config.project_dir`: creates its
    /// directories, reads the state, and renders every namespace's compose
    /// file from it.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::InvalidArgument`] for an empty port range or a
    /// control-plane URI that is not `http(s)://`;
    /// [`RuntimeError::Unavailable`] when the files cannot be read or
    /// written, or a state file does not parse.
    pub fn open(
        config: ComposeConfig,
        runner: Arc<dyn CommandRunner>,
    ) -> Result<Self, RuntimeError> {
        if config.port_range.is_empty() {
            return Err(RuntimeError::InvalidArgument(
                "the port range is empty".into(),
            ));
        }
        if !(config.control_plane_uri.starts_with("http://")
            || config.control_plane_uri.starts_with("https://"))
        {
            return Err(RuntimeError::InvalidArgument(
                "the control-plane URI must be http:// or https://".into(),
            ));
        }
        let dir = &config.project_dir;
        for d in [dir.clone(), dir.join("state"), dir.join("env")] {
            private_dir(&d)?;
        }
        let mut state = State::default();
        let ports = dir.join("state").join("ports.json");
        if ports.exists() {
            state.ports = read_json(&ports)?;
        }
        for entry in fs::read_dir(dir.join("state")).map_err(|e| io(&dir.join("state"), &e))? {
            let path = entry.map_err(|e| io(&dir.join("state"), &e))?.path();
            let Some(ns) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".json"))
                .filter(|n| *n != "ports")
            else {
                continue;
            };
            check_namespace(ns).map_err(|_| {
                RuntimeError::Unavailable(format!("{}: not a namespace's state", path.display()))
            })?;
            let ns_state: NamespaceState = read_json(&path)?;
            state.namespaces.insert(ns.to_owned(), ns_state);
        }
        let rt = ComposeRuntime {
            config,
            runner,
            state: Mutex::new(State::default()),
            locks: Mutex::new(HashMap::new()),
        };
        for (ns, ns_state) in &state.namespaces {
            rt.write_compose_file(ns, ns_state)?;
        }
        // A token whose compute the state does not name (a crash between
        // the two writes) is removed.
        let env_dir = rt.config.project_dir.join("env");
        for entry in fs::read_dir(&env_dir).map_err(|e| io(&env_dir, &e))? {
            let path = entry.map_err(|e| io(&env_dir, &e))?.path();
            let known = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".env"))
                .is_some_and(|svc| {
                    state
                        .namespaces
                        .values()
                        .any(|n| n.computes.keys().any(|id| service_name(id) == svc))
                });
            if !known {
                remove_file(&path)?;
            }
        }
        *rt.lock_state() = state;
        Ok(rt)
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn ns_lock(&self, ns: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.entry(ns.to_owned()).or_default().clone()
    }

    fn compose_file(&self, ns: &str) -> PathBuf {
        self.config.project_dir.join(format!("{ns}.yaml"))
    }

    fn state_file(&self, ns: &str) -> PathBuf {
        self.config
            .project_dir
            .join("state")
            .join(format!("{ns}.json"))
    }

    fn env_file(&self, service: &str) -> PathBuf {
        self.config
            .project_dir
            .join("env")
            .join(format!("{service}.env"))
    }

    /// Writes the namespace's state, then its compose file; a namespace
    /// with no compute loses both.
    fn persist(&self, ns: &str, ns_state: &NamespaceState) -> Result<(), RuntimeError> {
        if ns_state.computes.is_empty() {
            for p in [self.state_file(ns), self.compose_file(ns)] {
                remove_file(&p)?;
            }
            return Ok(());
        }
        write_private(&self.state_file(ns), &to_json(ns_state)?)?;
        self.write_compose_file(ns, ns_state)
    }

    fn write_compose_file(&self, ns: &str, ns_state: &NamespaceState) -> Result<(), RuntimeError> {
        write_private(
            &self.compose_file(ns),
            render(&self.config, ns, ns_state).as_bytes(),
        )
    }

    fn persist_ports(&self, ports: &PortsState) -> Result<(), RuntimeError> {
        write_private(
            &self.config.project_dir.join("state").join("ports.json"),
            &to_json(ports)?,
        )
    }

    /// `compose -f <ns>.yaml -p loams-pg-<ns> <args…>`, which must succeed.
    async fn compose(&self, ns: &str, args: &[&str]) -> Result<CommandOutput, RuntimeError> {
        let mut all = self.config.bin.args.clone();
        all.push("-f".into());
        all.push(self.compose_file(ns).display().to_string());
        all.push("-p".into());
        all.push(project_name(ns));
        all.extend(args.iter().map(|a| (*a).to_owned()));
        let inv = Invocation {
            program: self.config.bin.program.clone(),
            args: all,
            timeout: COMMAND_TIMEOUT,
        };
        let out = self.runner.run(&inv).await?;
        if out.code == Some(0) {
            return Ok(out);
        }
        let code = out
            .code
            .map_or_else(|| "a signal".to_owned(), |c| format!("exit {c}"));
        Err(RuntimeError::Unavailable(format!(
            "{} {} failed ({code}): {}",
            describe(&inv),
            args.first().copied().unwrap_or_default(),
            tail(out.stderr.trim(), STDERR_TAIL)
        )))
    }

    async fn ps(&self, ns: &str, service: &str) -> Result<RuntimeStatus, RuntimeError> {
        let out = self
            .compose(ns, &["ps", "-a", "--format", "json", service])
            .await?;
        parse_ps(&out.stdout, service)
    }

    async fn handle(
        &self,
        ns: &str,
        id: ComputeId,
        service: &str,
        address: BackendAddr,
    ) -> Result<ComputeHandle, RuntimeError> {
        let ctl_url = match self.config.ctl_reach {
            CtlReach::Network => format!("http://{service}:{COMPUTE_CTL_PORT}"),
            CtlReach::Host => {
                let port = COMPUTE_CTL_PORT.to_string();
                let out = self.compose(ns, &["port", service, &port]).await?;
                format!("http://127.0.0.1:{}", parse_port(&out.stdout)?)
            }
        };
        Ok(ComputeHandle {
            compute_id: id,
            ctl_url,
            address: Some(address),
        })
    }

    /// The namespace holding compute `id`, with its service.
    fn find(&self, id: &str) -> Option<(String, Service)> {
        self.lock_state()
            .namespaces
            .iter()
            .find_map(|(ns, s)| s.computes.get(id).map(|svc| (ns.clone(), svc.clone())))
    }

    /// Removes compute `id` of `ns`: its container, then its state and env
    /// file. Called under the namespace's lock.
    async fn remove(&self, ns: &str, id: &str) -> Result<(), RuntimeError> {
        let service = service_name(id);
        self.compose(ns, &["rm", "-s", "-f", &service]).await?;
        let mut ns_state = self
            .lock_state()
            .namespaces
            .get(ns)
            .cloned()
            .unwrap_or_default();
        ns_state.computes.remove(id);
        self.persist(ns, &ns_state)?;
        {
            let mut st = self.lock_state();
            if ns_state.computes.is_empty() {
                st.namespaces.remove(ns);
            } else {
                st.namespaces.insert(ns.to_owned(), ns_state);
            }
        }
        remove_file(&self.env_file(&service))
    }
}

#[async_trait]
impl ComputeRuntime for ComposeRuntime {
    // Ports are the host's, one range for every namespace: the compose
    // runtime keys addresses by endpoint alone, and finds a compute's
    // namespace in its own state.
    async fn ensure_address(
        &self,
        _ns: &str,
        ep: &EndpointRec,
    ) -> Result<BackendAddr, RuntimeError> {
        let range = self.config.port_range.clone();
        let mut st = self.lock_state();
        let held_by = |ports: &PortsState, port: u16| {
            ports
                .endpoints
                .iter()
                .find(|(e, p)| **p == port && **e != ep.id)
                .map(|(e, _)| e.clone())
        };
        let port = if let Some(recorded) = &ep.backend_addr {
            let port = BackendAddr::parse(recorded)?.port;
            if !range.contains(&port) {
                return Err(RuntimeError::InvalidArgument(format!(
                    "endpoint {}'s port {port} is outside {}..{}",
                    ep.id, range.start, range.end
                )));
            }
            if let Some(other) = held_by(&st.ports, port) {
                return Err(RuntimeError::InvalidArgument(format!(
                    "endpoint {}'s port {port} is endpoint {other}'s",
                    ep.id
                )));
            }
            port
        } else if let Some(port) = st.ports.endpoints.get(&ep.id) {
            *port
        } else {
            range
                .clone()
                .find(|p| held_by(&st.ports, *p).is_none())
                .ok_or(RuntimeError::PortsExhausted {
                    start: range.start,
                    end: range.end,
                })?
        };
        if st.ports.endpoints.get(&ep.id) != Some(&port) {
            let mut ports = st.ports.clone();
            ports.format = STATE_FORMAT;
            ports.endpoints.insert(ep.id.clone(), port);
            self.persist_ports(&ports)?;
            st.ports = ports;
        }
        Ok(BackendAddr::new(self.config.address_host(), port))
    }

    async fn remove_address(&self, _ns: &str, ep: &EndpointRec) -> Result<(), RuntimeError> {
        let mut st = self.lock_state();
        if st
            .namespaces
            .values()
            .any(|n| n.computes.values().any(|s| s.endpoint_id == ep.id))
        {
            return Err(RuntimeError::InvalidArgument(format!(
                "endpoint {} still has a compute; stop it first",
                ep.id
            )));
        }
        if st.ports.endpoints.contains_key(&ep.id) {
            let mut ports = st.ports.clone();
            ports.endpoints.remove(&ep.id);
            self.persist_ports(&ports)?;
            st.ports = ports;
        }
        Ok(())
    }

    async fn start(&self, c: &ComputeLaunch) -> Result<ComputeHandle, RuntimeError> {
        let invalid = |why: String| Err(RuntimeError::InvalidArgument(why));
        let Some(binding) = &c.binding else {
            return invalid(
                "the compose runtime starts bound computes only: an unbound compute has no \
                 port to publish, so compose has no warm pool (R14.2)"
                    .into(),
            );
        };
        let ns = c.namespace()?;
        check_namespace(ns)?;
        if !is_pinned(&c.image) {
            return invalid(format!("image {:?} is not pinned by digest", c.image));
        }
        check_upcall_token(c.upcall_token.expose())?;
        if c.cu.0 == 0 {
            return invalid("a compute needs at least 0.25 CU".into());
        }
        let Some(address) = &c.address else {
            return invalid("a compose compute needs its endpoint's address".into());
        };
        let expected = self
            .lock_state()
            .ports
            .endpoints
            .get(&binding.endpoint_id)
            .map(|p| BackendAddr::new(self.config.address_host(), *p));
        if expected.as_ref() != Some(address) {
            return invalid(format!(
                "{address} is not endpoint {}'s address (ensure_address)",
                binding.endpoint_id
            ));
        }
        let id = c.compute_id.to_string();
        let service = service_name(&id);

        let lock = self.ns_lock(ns);
        let _held = lock.lock().await;
        let known = self
            .lock_state()
            .namespaces
            .get(ns)
            .is_some_and(|s| s.computes.contains_key(&id));
        if !known {
            // The endpoint's previous computes publish its port: remove
            // them first.
            let previous: Vec<String> = self
                .lock_state()
                .namespaces
                .get(ns)
                .map(|s| {
                    s.computes
                        .iter()
                        .filter(|(_, svc)| svc.endpoint_id == binding.endpoint_id)
                        .map(|(other, _)| other.clone())
                        .collect()
                })
                .unwrap_or_default();
            for other in previous {
                self.remove(ns, &other).await?;
            }
            let env = self.env_file(&service);
            write_private(
                &env,
                format!("NEON_CONTROL_PLANE_TOKEN={}\n", c.upcall_token.expose()).as_bytes(),
            )?;
            // Recorded under the state lock that `ensure_address` and
            // `remove_address` take, after checking again that the port is
            // still the endpoint's: a `remove_address` since the check above
            // could have freed it for another endpoint.
            let mut st = self.lock_state();
            let recorded = match st.ports.endpoints.get(&binding.endpoint_id) {
                Some(port) if *port == address.port => {
                    let mut ns_state = st.namespaces.get(ns).cloned().unwrap_or_default();
                    ns_state.format = STATE_FORMAT;
                    ns_state.computes.insert(
                        id.clone(),
                        Service {
                            project_id: binding.project_id.clone(),
                            endpoint_id: binding.endpoint_id.clone(),
                            image: c.image.clone(),
                            cu: c.cu,
                            port: address.port,
                        },
                    );
                    // The state on disk first.
                    self.persist(ns, &ns_state).map(|()| ns_state)
                }
                _ => Err(RuntimeError::InvalidArgument(format!(
                    "{address} is no longer endpoint {}'s address",
                    binding.endpoint_id
                ))),
            };
            match recorded {
                Ok(ns_state) => {
                    st.namespaces.insert(ns.to_owned(), ns_state);
                }
                Err(e) => {
                    drop(st);
                    // The token goes with a compute that was never recorded.
                    let _ = remove_file(&env);
                    return Err(e);
                }
            }
        }
        if self.ps(ns, &service).await? == RuntimeStatus::Missing {
            self.compose(ns, &["up", "-d", "--no-build", &service])
                .await?;
        }
        self.handle(ns, c.compute_id, &service, address.clone())
            .await
    }

    async fn bind_address(
        &self,
        _ns: &str,
        ep: &EndpointRec,
        c: &ComputeHandle,
    ) -> Result<(), RuntimeError> {
        let recorded = ep
            .backend_addr
            .as_deref()
            .map(BackendAddr::parse)
            .transpose()?;
        if recorded.is_some() && recorded == c.address {
            return Ok(());
        }
        Err(RuntimeError::InvalidArgument(format!(
            "compute {} was not started with endpoint {}'s address; compose publishes a \
             compute's address when it starts",
            c.compute_id, ep.id
        )))
    }

    async fn stop(&self, _ns: &str, compute_id: &ComputeId) -> Result<(), RuntimeError> {
        let id = compute_id.to_string();
        let Some((ns, _)) = self.find(&id) else {
            return Ok(());
        };
        let lock = self.ns_lock(&ns);
        let _held = lock.lock().await;
        if self.find(&id).is_none() {
            return Ok(());
        }
        self.remove(&ns, &id).await
    }

    async fn status(
        &self,
        _ns: &str,
        compute_id: &ComputeId,
    ) -> Result<RuntimeStatus, RuntimeError> {
        let id = compute_id.to_string();
        let Some((ns, _)) = self.find(&id) else {
            return Ok(RuntimeStatus::Missing);
        };
        self.ps(&ns, &service_name(&id)).await
    }

    async fn resize(&self, _: &str, _: &ComputeId, _: Cu) -> Result<ResizeOutcome, RuntimeError> {
        // A container's limits change only by recreating it, which is a
        // restart; computes are replaced instead (§46 §7.4: a move happens
        // only on restart).
        Ok(ResizeOutcome::Unsupported)
    }
}

/// A namespace names a compose project and files: a DNS label
/// (`[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?`), as the Kubernetes runtime's
/// `loams-pg-<ns>` needs too.
pub(crate) fn check_namespace(ns: &str) -> Result<(), RuntimeError> {
    let b = ns.as_bytes();
    let edge = |c: &u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    let ok = !b.is_empty()
        && b.len() <= 63
        && b.first().is_some_and(edge)
        && b.last().is_some_and(edge)
        && b.iter().all(|c| edge(c) || *c == b'-');
    if ok {
        Ok(())
    } else {
        Err(RuntimeError::InvalidArgument(format!(
            "namespace {ns:?} cannot name a compose project (a DNS label is needed)"
        )))
    }
}

/// The compose project of namespace `ns`.
fn project_name(ns: &str) -> String {
    format!("loams-pg-{ns}")
}

/// A compute's service: its id in lower case (a DNS name on the network).
fn service_name(compute_id: &str) -> String {
    compute_id.to_ascii_lowercase()
}

/// `0.25 CU` steps as compose's `cpus` (1 CU = 1 vCPU).
fn cpus(cu: Cu) -> String {
    let whole = cu.0 / 4;
    match cu.0 % 4 {
        0 => whole.to_string(),
        1 => format!("{whole}.25"),
        2 => format!("{whole}.5"),
        _ => format!("{whole}.75"),
    }
}

/// A YAML double-quoted scalar, with compose's `$` interpolation escaped.
/// JSON's string escapes are a subset of YAML's.
fn q(s: &str) -> String {
    serde_json::Value::String(s.replace('$', "$$")).to_string()
}

/// `ip:host_port:container_port` (an IPv6 address in brackets).
fn port_mapping(ip: IpAddr, host_port: Option<u16>, container_port: u16) -> String {
    let ip = match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{v6}]"),
    };
    let host = host_port.map(|p| p.to_string()).unwrap_or_default();
    format!("{ip}:{host}:{container_port}")
}

/// The compose file of `ns`: a pure function of the config and the state,
/// in key order.
fn render(config: &ComposeConfig, ns: &str, state: &NamespaceState) -> String {
    let mut out = String::new();
    let mut line = |s: String| {
        out.push_str(&s);
        out.push('\n');
    };
    line(format!(
        "# Generated by loams pg-control (PG2 Task 10) from state/{ns}.json: the computes of \
         namespace {ns}."
    ));
    line("# Rewritten on every start and stop; do not edit.".into());
    line(format!("name: {}", q(&project_name(ns))));
    line("services:".into());
    for (id, svc) in &state.computes {
        let service = service_name(id);
        let args = compute_ctl_args(id, &config.control_plane_uri);
        line(format!("  {}:", q(&service)));
        line(format!("    image: {}", q(&svc.image)));
        line("    restart: \"no\"".into());
        line(format!(
            "    entrypoint: [{}]",
            q("/usr/local/bin/compute_ctl")
        ));
        line("    command:".into());
        for a in &args {
            line(format!("      - {}", q(a)));
        }
        line("    env_file:".into());
        line(format!("      - {}", q(&format!("env/{service}.env"))));
        line("    labels:".into());
        for (k, v) in [
            ("loams.dev/compute", id.as_str()),
            ("loams.dev/endpoint", &svc.endpoint_id),
            ("loams.dev/namespace", ns),
            ("loams.dev/project", &svc.project_id),
        ] {
            line(format!("      {}: {}", q(k), q(v)));
        }
        line(format!("    cpus: {}", q(&cpus(svc.cu))));
        line(format!("    mem_limit: {}", q(&format!("{}g", svc.cu.0))));
        line("    ports:".into());
        line(format!(
            "      - {}",
            q(&port_mapping(
                config.publish_ip,
                Some(svc.port),
                COMPUTE_PG_PORT
            ))
        ));
        if config.ctl_reach == CtlReach::Host {
            line(format!(
                "      - {}",
                q(&port_mapping(
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                    None,
                    COMPUTE_CTL_PORT
                ))
            ));
        }
        line("    networks:".into());
        line("      default:".into());
        line("        aliases:".into());
        line(format!(
            "          - {}",
            q(&svc.endpoint_id.to_ascii_lowercase())
        ));
        if !config.extra_hosts.is_empty() {
            line("    extra_hosts:".into());
            for h in &config.extra_hosts {
                line(format!("      - {}", q(h)));
            }
        }
    }
    if let Some(network) = &config.network {
        line("networks:".into());
        line("  default:".into());
        line(format!("    name: {}", q(network)));
        line("    external: true".into());
    }
    out
}

/// `compose ps --format json`: one JSON object per line (compose 2.21 and
/// later) or one array (earlier); no output when there is no container.
fn parse_ps(stdout: &str, service: &str) -> Result<RuntimeStatus, RuntimeError> {
    #[derive(Deserialize)]
    struct Row {
        #[serde(rename = "Service", default)]
        service: Option<String>,
        #[serde(rename = "State", default)]
        state: String,
        #[serde(rename = "ExitCode", default)]
        exit_code: Option<i32>,
    }
    let text = stdout.trim();
    let bad = |e: serde_json::Error| {
        RuntimeError::Unavailable(format!("compose ps answered unreadable JSON: {e}"))
    };
    let rows: Vec<Row> = if text.is_empty() {
        Vec::new()
    } else if text.starts_with('[') {
        serde_json::from_str(text).map_err(bad)?
    } else {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).map_err(bad))
            .collect::<Result<_, _>>()?
    };
    let Some(row) = rows
        .into_iter()
        .find(|r| r.service.as_deref().is_none_or(|s| s == service))
    else {
        return Ok(RuntimeStatus::Missing);
    };
    Ok(match row.state.as_str() {
        "running" => RuntimeStatus::Running,
        "exited" | "dead" => RuntimeStatus::Exited {
            code: row.exit_code,
        },
        _ => RuntimeStatus::Pending,
    })
}

/// `compose port`'s answer, `host:port` (the last line).
fn parse_port(stdout: &str) -> Result<u16, RuntimeError> {
    stdout
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .and_then(|l| l.trim().rsplit_once(':'))
        .and_then(|(_, p)| p.parse().ok())
        .ok_or_else(|| {
            RuntimeError::Unavailable(format!(
                "compose port answered {:?}, not host:port",
                tail(stdout.trim(), 200)
            ))
        })
}

/// The last `max` bytes of `s`, on a character boundary.
fn tail(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut start = s.len() - max;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

fn io(path: &Path, e: &std::io::Error) -> RuntimeError {
    RuntimeError::Unavailable(format!("{}: {e}", path.display()))
}

fn to_json<T: Serialize>(v: &T) -> Result<Vec<u8>, RuntimeError> {
    let mut out = serde_json::to_vec_pretty(v)
        .map_err(|e| RuntimeError::Unavailable(format!("encoding the state: {e}")))?;
    out.push(b'\n');
    Ok(out)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, RuntimeError> {
    let bytes = fs::read(path).map_err(|e| io(path, &e))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| RuntimeError::Unavailable(format!("{}: {e}", path.display())))
}

/// Creates `dir` (mode 0700 on Unix) when it is missing.
fn private_dir(dir: &Path) -> Result<(), RuntimeError> {
    if dir.is_dir() {
        return Ok(());
    }
    let mut b = fs::DirBuilder::new();
    b.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(0o700);
    }
    b.create(dir).map_err(|e| io(dir, &e))
}

/// Replaces `path` with `bytes` atomically: a 0600 temporary file, fsync,
/// rename, fsync of the directory.
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), RuntimeError> {
    let dir = path.parent().unwrap_or(Path::new("."));
    let mut tmp_name = path.file_name().unwrap_or_default().to_os_string();
    tmp_name.push(".tmp");
    let tmp = dir.join(tmp_name);
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(&tmp).map_err(|e| io(&tmp, &e))?;
    f.write_all(bytes).map_err(|e| io(&tmp, &e))?;
    f.sync_all().map_err(|e| io(&tmp, &e))?;
    drop(f);
    fs::rename(&tmp, path).map_err(|e| io(path, &e))?;
    #[cfg(unix)]
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io(dir, &e))?;
    Ok(())
}

fn remove_file(path: &Path) -> Result<(), RuntimeError> {
    match fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(io(path, &e)),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpus_are_quarter_steps() {
        assert_eq!(cpus(Cu(1)), "0.25");
        assert_eq!(cpus(Cu(2)), "0.5");
        assert_eq!(cpus(Cu(3)), "0.75");
        assert_eq!(cpus(Cu(4)), "1");
        assert_eq!(cpus(Cu(9)), "2.25");
    }

    #[test]
    fn namespaces_must_be_dns_labels() {
        for ok in ["acme", "a", "a-1", &"a".repeat(63)] {
            assert!(check_namespace(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "Acme",
            "-a",
            "a-",
            "a_b",
            "a.b",
            "../x",
            &"a".repeat(64),
        ] {
            assert!(check_namespace(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn ps_output_in_both_shapes() {
        let line = "{\"Service\":\"s\",\"State\":\"running\",\"ExitCode\":0}\n";
        assert_eq!(parse_ps(line, "s").unwrap(), RuntimeStatus::Running);
        let arr = "[{\"Service\":\"s\",\"State\":\"exited\",\"ExitCode\":1}]";
        assert_eq!(
            parse_ps(arr, "s").unwrap(),
            RuntimeStatus::Exited { code: Some(1) }
        );
        let created = "{\"Service\":\"s\",\"State\":\"created\"}";
        assert_eq!(parse_ps(created, "s").unwrap(), RuntimeStatus::Pending);
        assert_eq!(parse_ps("", "s").unwrap(), RuntimeStatus::Missing);
        assert_eq!(parse_ps(line, "other").unwrap(), RuntimeStatus::Missing);
        assert!(parse_ps("not json", "s").is_err());
    }

    #[test]
    fn port_answers() {
        assert_eq!(parse_port("127.0.0.1:49153\n").unwrap(), 49153);
        assert_eq!(parse_port("0.0.0.0:1\n[::]:1\n").unwrap(), 1);
        assert!(parse_port("").is_err());
        assert!(parse_port("no port").is_err());
    }

    #[test]
    fn quoting_escapes_interpolation() {
        assert_eq!(q("a$b\"c"), "\"a$$b\\\"c\"");
    }

    #[test]
    fn tails_respect_char_boundaries() {
        assert_eq!(tail("héllo", 4), "llo");
        assert_eq!(tail("abc", 10), "abc");
    }
}
