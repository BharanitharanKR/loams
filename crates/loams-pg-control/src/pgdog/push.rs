//! Pushing a namespace's PgDog config (design §46 §8.2; PG2 Task 21).
//!
//! [`PgDogPusher::push`] reads the namespace's records, renders the two
//! files ([`super::render`]), and for each replica of the namespace
//! ([`PgDogAdmin::instances`]):
//! 1. sends them to the replica's [`ConfigSink`]: its `pgdog-config-sync`
//!    sidecar over mutual TLS on Kubernetes ([`SidecarSink`]), or a bind
//!    mount on compose ([`DirSink`]);
//! 2. waits until the sink answers the rendered generation (at most
//!    [`PgDogPusher::ack_timeout`]);
//! 3. sends `RELOAD` through the replica's admin database.
//!
//! The generation each replica applied is kept here (R21.3: RT1's
//! `ConfigPush` machine is not built). [`PgDogPusher::run`] coalesces the
//! pushes a namespace is asked for to at most one per
//! [`PgDogPusher::debounce`] (500 ms). [`PgDogPusher::resync`] is the drift
//! check: a replica whose sink answers another generation than the records
//! render (a PgDog restarted onto an older persisted copy, a lost push), or
//! that this pusher has not reloaded at that generation (a failed `RELOAD`,
//! a restarted `pg-control`), is pushed again (R21.4: the pinned PgDog's
//! `SHOW DATABASES` and `SHOW USERS` answer nothing to compare).

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use rustls::pki_types::ServerName;
use tokio::sync::mpsc;

use super::render::{
    NamespaceRecords, RenderSettings, RenderedConfig, RoleCredential, RoleKey, render_namespace,
};
use super::{InstanceId, PgDogAdmin};
use crate::store::PgControlStore;

/// Why a push did not complete (for logs; the next push or resync retries).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PushError(pub String);

/// Where a replica's files go.
#[async_trait]
pub trait ConfigSink: Send + Sync + 'static {
    /// Writes the two files for `instance` of `namespace`.
    async fn send(
        &self,
        namespace: &str,
        instance: &InstanceId,
        config: &RenderedConfig,
    ) -> Result<(), PushError>;
    /// The generation `instance` holds now, if any.
    async fn generation(
        &self,
        namespace: &str,
        instance: &InstanceId,
    ) -> Result<Option<u64>, PushError>;
}

fn sync_config(c: &RenderedConfig) -> loams_pgdog_sync::Config {
    loams_pgdog_sync::Config {
        generation: c.generation,
        pgdog_toml: c.pgdog_toml.expose().clone(),
        users_toml: c.users_toml.expose().clone(),
    }
}

/// Compose and single node: PgDog reads `<root>/<namespace>/`, a bind
/// mount; the files are written in place, atomically.
#[derive(Debug, Clone)]
pub struct DirSink {
    pub root: PathBuf,
}

#[async_trait]
impl ConfigSink for DirSink {
    async fn send(
        &self,
        namespace: &str,
        _instance: &InstanceId,
        config: &RenderedConfig,
    ) -> Result<(), PushError> {
        let dir = self.root.join(namespace);
        let c = sync_config(config);
        tokio::task::spawn_blocking(move || loams_pgdog_sync::write_config(&dir, &c))
            .await
            .map_err(|e| PushError(e.to_string()))?
            .map_err(|e| PushError(format!("writing the config: {e}")))
    }

    async fn generation(
        &self,
        namespace: &str,
        _instance: &InstanceId,
    ) -> Result<Option<u64>, PushError> {
        Ok(loams_pgdog_sync::read_generation(
            &self.root.join(namespace),
        ))
    }
}

/// Kubernetes: each replica's `pgdog-config-sync` at
/// `https://<instance>:<port>`, over mutual TLS (`pg-control`'s client
/// certificate; the sidecar's, checked against the same CA).
#[derive(Clone)]
pub struct SidecarSink {
    pub port: u16,
    pub tls: Arc<rustls::ClientConfig>,
    pub timeout: Duration,
}

impl std::fmt::Debug for SidecarSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SidecarSink")
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl SidecarSink {
    /// A sink presenting `cert_pem`/`key_pem` (`pg-control`'s client
    /// certificate) and trusting only `ca_pem` for the sidecars'.
    ///
    /// # Errors
    ///
    /// [`PushError`] for unreadable PEM or a config rustls refuses.
    pub fn new(
        port: u16,
        cert_pem: &[u8],
        key_pem: &[u8],
        ca_pem: &[u8],
        timeout: Duration,
    ) -> Result<Self, PushError> {
        use rustls::pki_types::pem::PemObject as _;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let tls = |e: &dyn std::fmt::Display| PushError(format!("sidecar tls: {e}"));
        let mut roots = rustls::RootCertStore::empty();
        for ca in CertificateDer::pem_slice_iter(ca_pem) {
            roots.add(ca.map_err(|e| tls(&e))?).map_err(|e| tls(&e))?;
        }
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
            .collect::<Result<_, _>>()
            .map_err(|e| tls(&e))?;
        let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|e| tls(&e))?;
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| tls(&e))?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| tls(&e))?;
        Ok(SidecarSink {
            port,
            tls: Arc::new(config),
            timeout,
        })
    }

    async fn call(
        &self,
        instance: &InstanceId,
        method: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<(u16, Bytes), PushError> {
        let fail = |what: &str, e: &dyn std::fmt::Display| {
            PushError(format!("{} {what}: {e}", instance.0))
        };
        let exchange = async {
            let tcp = tokio::net::TcpStream::connect((instance.0.as_str(), self.port))
                .await
                .map_err(|e| fail("connect", &e))?;
            let name = ServerName::try_from(instance.0.clone()).map_err(|e| fail("name", &e))?;
            let tls = tokio_rustls::TlsConnector::from(self.tls.clone())
                .connect(name, tcp)
                .await
                .map_err(|e| fail("tls", &e))?;
            let (mut send, conn) =
                hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tls))
                    .await
                    .map_err(|e| fail("http", &e))?;
            tokio::spawn(conn);
            let req = hyper::Request::builder()
                .method(method)
                .uri(path)
                .header("host", instance.0.as_str())
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(body)))
                .map_err(|e| fail("request", &e))?;
            let resp = send
                .send_request(req)
                .await
                .map_err(|e| fail("request", &e))?;
            let status = resp.status().as_u16();
            let body = resp
                .into_body()
                .collect()
                .await
                .map_err(|e| fail("answer", &e))?
                .to_bytes();
            Ok((status, body))
        };
        tokio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| fail("call", &"timed out"))?
    }
}

#[async_trait]
impl ConfigSink for SidecarSink {
    async fn send(
        &self,
        _namespace: &str,
        instance: &InstanceId,
        config: &RenderedConfig,
    ) -> Result<(), PushError> {
        let body =
            serde_json::to_vec(&sync_config(config)).map_err(|e| PushError(e.to_string()))?;
        match self.call(instance, "PUT", "/v1/config", body).await? {
            (200, _) => Ok(()),
            (status, _) => Err(PushError(format!("{} answered {status}", instance.0))),
        }
    }

    async fn generation(
        &self,
        _namespace: &str,
        instance: &InstanceId,
    ) -> Result<Option<u64>, PushError> {
        match self
            .call(instance, "GET", "/v1/generation", Vec::new())
            .await?
        {
            (200, body) => serde_json::from_slice::<loams_pgdog_sync::Generation>(&body)
                .map(|g| g.generation)
                .map_err(|e| PushError(e.to_string())),
            (status, _) => Err(PushError(format!("{} answered {status}", instance.0))),
        }
    }
}

/// Where the roles' credentials come from (Task 24 derives them from the
/// credential store). Until then, none: `users.toml` lists no user.
#[async_trait]
pub trait CredentialSource: Send + Sync + 'static {
    async fn credentials(
        &self,
        records: &NamespaceRecords,
    ) -> Result<BTreeMap<RoleKey, RoleCredential>, PushError>;
}

/// No credentials (before Task 24).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoCredentials;

#[async_trait]
impl CredentialSource for NoCredentials {
    async fn credentials(
        &self,
        _: &NamespaceRecords,
    ) -> Result<BTreeMap<RoleKey, RoleCredential>, PushError> {
        Ok(BTreeMap::new())
    }
}

/// What one push did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pushed {
    pub generation: u64,
    /// The replicas that applied it (sent, acknowledged, reloaded).
    pub applied: Vec<InstanceId>,
    /// The replicas that did not, with why.
    pub failed: Vec<(InstanceId, String)>,
}

/// Renders and pushes namespaces' PgDog configs (see the module docs).
pub struct PgDogPusher<S> {
    pub store: S,
    pub settings: RenderSettings,
    pub credentials: Arc<dyn CredentialSource>,
    pub sink: Arc<dyn ConfigSink>,
    pub admin: Arc<dyn PgDogAdmin>,
    /// How long a replica has to answer the pushed generation (5 s).
    pub ack_timeout: Duration,
    /// How often it is asked meanwhile (100 ms).
    pub ack_poll: Duration,
    /// At most one push per namespace this often (500 ms).
    pub debounce: Duration,
    applied: Mutex<HashMap<(String, InstanceId), u64>>,
}

impl<S> std::fmt::Debug for PgDogPusher<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgDogPusher")
            .field("ack_timeout", &self.ack_timeout)
            .field("debounce", &self.debounce)
            .finish_non_exhaustive()
    }
}

impl<S: PgControlStore> PgDogPusher<S> {
    pub fn new(
        store: S,
        settings: RenderSettings,
        credentials: Arc<dyn CredentialSource>,
        sink: Arc<dyn ConfigSink>,
        admin: Arc<dyn PgDogAdmin>,
    ) -> Self {
        PgDogPusher {
            store,
            settings,
            credentials,
            sink,
            admin,
            ack_timeout: Duration::from_secs(5),
            ack_poll: Duration::from_millis(100),
            debounce: Duration::from_millis(500),
            applied: Mutex::new(HashMap::new()),
        }
    }

    /// The generation `instance` of `namespace` last applied through this
    /// pusher.
    pub fn applied(&self, namespace: &str, instance: &InstanceId) -> Option<u64> {
        self.applied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(namespace.to_owned(), instance.clone()))
            .copied()
    }

    /// The namespace's config as its records render now.
    ///
    /// # Errors
    ///
    /// [`PushError`]: the store, the credentials or the renderer.
    pub async fn render(&self, namespace: &str) -> Result<RenderedConfig, PushError> {
        let mut records = NamespaceRecords::read(&self.store, namespace)
            .await
            .map_err(|e| PushError(format!("reading the records: {e}")))?;
        records.credentials = self.credentials.credentials(&records).await?;
        render_namespace(&records, &self.settings).map_err(|e| PushError(e.to_string()))
    }

    /// One replica: send, wait for the generation, `RELOAD`.
    async fn apply(
        &self,
        namespace: &str,
        instance: &InstanceId,
        config: &RenderedConfig,
    ) -> Result<(), PushError> {
        self.sink.send(namespace, instance, config).await?;
        let deadline = tokio::time::Instant::now() + self.ack_timeout;
        loop {
            if self.sink.generation(namespace, instance).await? == Some(config.generation) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(PushError(format!(
                    "{} did not apply generation {} within {:?}",
                    instance.0, config.generation, self.ack_timeout
                )));
            }
            tokio::time::sleep(self.ack_poll).await;
        }
        self.admin
            .reload(instance)
            .await
            .map_err(|e| PushError(e.to_string()))?;
        self.applied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((namespace.to_owned(), instance.clone()), config.generation);
        Ok(())
    }

    /// Renders `namespace` and pushes it to each of its replicas.
    ///
    /// # Errors
    ///
    /// [`PushError`] when nothing could be rendered or the replicas could
    /// not be listed; a replica that failed is in [`Pushed::failed`].
    pub async fn push(&self, namespace: &str) -> Result<Pushed, PushError> {
        let config = self.render(namespace).await?;
        let instances = self
            .admin
            .instances(namespace)
            .await
            .map_err(|e| PushError(e.to_string()))?;
        let mut out = Pushed {
            generation: config.generation,
            applied: Vec::new(),
            failed: Vec::new(),
        };
        for instance in instances {
            match self.apply(namespace, &instance, &config).await {
                Ok(()) => out.applied.push(instance),
                Err(e) => {
                    tracing::warn!(namespace, instance = %instance.0, error = %e, "PgDog config not applied");
                    out.failed.push((instance, e.0));
                }
            }
        }
        if !out.applied.is_empty() {
            tracing::info!(
                namespace,
                generation = out.generation,
                replicas = out.applied.len(),
                "PgDog config pushed"
            );
        }
        Ok(out)
    }

    /// The drift check: pushes again when a replica holds another
    /// generation than the records render. Answers whether it pushed.
    ///
    /// # Errors
    ///
    /// As [`push`](Self::push).
    pub async fn resync(&self, namespace: &str) -> Result<bool, PushError> {
        let config = self.render(namespace).await?;
        let instances = self
            .admin
            .instances(namespace)
            .await
            .map_err(|e| PushError(e.to_string()))?;
        let mut drifted = false;
        for instance in &instances {
            let held = self
                .sink
                .generation(namespace, instance)
                .await
                .ok()
                .flatten();
            let applied = self.applied(namespace, instance);
            if held != Some(config.generation) || applied != Some(config.generation) {
                tracing::info!(namespace, instance = %instance.0, ?held, ?applied, rendered = config.generation, "PgDog config drifted");
                drifted = true;
            }
        }
        if drifted {
            self.push(namespace).await?;
        }
        Ok(drifted)
    }
}

impl<S: PgControlStore> PgDogPusher<S> {
    /// Pushes the namespaces `kicks` names, each at most once per
    /// [`debounce`](Self::debounce): kicks that arrive while a push is due
    /// fold into it. Runs until `shutdown` resolves or the kicks end.
    pub async fn run(
        self: Arc<Self>,
        mut kicks: mpsc::UnboundedReceiver<String>,
        shutdown: impl std::future::Future<Output = ()>,
    ) {
        let mut due: BTreeMap<String, tokio::time::Instant> = BTreeMap::new();
        tokio::pin!(shutdown);
        loop {
            let next = due.values().min().copied();
            tokio::select! {
                () = &mut shutdown => return,
                kick = kicks.recv() => match kick {
                    Some(ns) => {
                        due.entry(ns).or_insert_with(|| tokio::time::Instant::now() + self.debounce);
                    }
                    None => return,
                },
                () = async {
                    match next {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {
                    let now = tokio::time::Instant::now();
                    let ready: Vec<String> = due
                        .iter()
                        .filter(|(_, at)| **at <= now)
                        .map(|(ns, _)| ns.clone())
                        .collect();
                    for ns in ready {
                        due.remove(&ns);
                        if let Err(e) = self.push(&ns).await {
                            tracing::warn!(namespace = %ns, error = %e, "PgDog push failed");
                        }
                    }
                }
            }
        }
    }
}
