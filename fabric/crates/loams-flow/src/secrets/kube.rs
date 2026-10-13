//! `KubeSecretStore` (OR-21): Kubernetes `Secret`s in the instance's
//! namespace, read with one `GET /api/v1/namespaces/<ns>/secrets/<secret>`
//! through the SDK's HTTP client and the pod's service-account token. No
//! Kubernetes client crate is linked for one read (R25.6); Task 26's fleet
//! may move it onto `kube` behind the same [`SecretStore`] trait.
//!
//! Also here: what a fallback pod needs to read the same `Secret`s as files
//! ([`secret_mounts`], [`config_with_secret_files`]) and the one rendered
//! object allowed to carry secret bytes ([`render_secret`]).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use http::{HeaderValue, Request, StatusCode};
use loams_connector_sdk::http::{HttpClient, client};
use loams_connector_sdk::{AllowAll, Secret, SecretRef};
use serde::Deserialize;
use serde_json::{Value, json};
use zeroize::{Zeroize, Zeroizing};

use super::{SecretError, SecretStore, parse_ref};
use crate::manifest::ConnectorSpec;

/// The in-cluster service-account files.
pub const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// The default Kubernetes namespace of a Loams namespace (R25.5).
pub const DEFAULT_NAMESPACE_TEMPLATE: &str = "loams-{ns}";

/// Where the API token comes from.
pub enum TokenSource {
    /// A file read on every request, so a rotated bound token is picked up
    /// (the in-cluster `token` file).
    File(PathBuf),
    /// A fixed token.
    Static(Secret),
}

impl fmt::Debug for TokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenSource::File(p) => f.debug_tuple("File").field(p).finish(),
            TokenSource::Static(_) => f.write_str("Static(<redacted>)"),
        }
    }
}

pub struct KubeSecretStore {
    api: String,
    token: TokenSource,
    client: HttpClient,
    namespace_template: String,
}

impl fmt::Debug for KubeSecretStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KubeSecretStore")
            .field("api", &self.api)
            .field("token", &self.token)
            .field("namespace_template", &self.namespace_template)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct SecretDoc {
    #[serde(default)]
    data: BTreeMap<String, String>,
}

impl Drop for SecretDoc {
    fn drop(&mut self) {
        for v in self.data.values_mut() {
            v.zeroize();
        }
    }
}

impl KubeSecretStore {
    /// A store over the API server at `api` (`https://host:port`, or `http`
    /// for tests). The API server is the runtime's own trusted endpoint, not
    /// an instance's: its connection is not under an instance's egress
    /// policy.
    pub fn new(api: &str, token: TokenSource) -> Result<Self, SecretError> {
        let api = api.trim_end_matches('/').to_owned();
        if !(api.starts_with("https://") || api.starts_with("http://")) {
            return Err(SecretError::Unavailable(format!(
                "the Kubernetes API URL must be http(s): {api}"
            )));
        }
        Ok(Self {
            api,
            token,
            client: client(Arc::new(AllowAll)),
            namespace_template: DEFAULT_NAMESPACE_TEMPLATE.to_owned(),
        })
    }

    /// The pod's own API server, token and cluster CA
    /// (`KUBERNETES_SERVICE_HOST`, `KUBERNETES_SERVICE_PORT` and
    /// [`SERVICE_ACCOUNT_DIR`]).
    pub fn in_cluster() -> Result<Self, SecretError> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST")
            .map_err(|_| SecretError::Unavailable("KUBERNETES_SERVICE_HOST is not set".into()))?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
        let host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host
        };
        let dir = PathBuf::from(SERVICE_ACCOUNT_DIR);
        let ca = std::fs::read(dir.join("ca.crt"))
            .map_err(|e| SecretError::Unavailable(format!("reading the cluster CA: {e}")))?;
        Self::new(
            &format!("https://{host}:{port}"),
            TokenSource::File(dir.join("token")),
        )?
        .with_ca_pem(&ca)
    }

    /// Trust only the PEM certificates in `pem` (the cluster CA).
    pub fn with_ca_pem(mut self, pem: &[u8]) -> Result<Self, SecretError> {
        use rustls::pki_types::CertificateDer;
        use rustls::pki_types::pem::PemObject as _;
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(pem) {
            let cert =
                cert.map_err(|e| SecretError::Unavailable(format!("the cluster CA: {e}")))?;
            roots
                .add(cert)
                .map_err(|e| SecretError::Unavailable(format!("the cluster CA: {e}")))?;
        }
        if roots.is_empty() {
            return Err(SecretError::Unavailable(
                "the cluster CA holds no certificate".into(),
            ));
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| SecretError::Unavailable(e.to_string()))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        self.client = self.client.with_tls_config(Arc::new(tls));
        Ok(self)
    }

    /// The Kubernetes namespace of Loams namespace `ns`: the template with
    /// `{ns}` replaced.
    pub fn with_namespace_template(mut self, template: &str) -> Self {
        self.namespace_template = template.to_owned();
        self
    }

    pub fn kube_namespace(&self, ns: u64) -> String {
        self.namespace_template.replace("{ns}", &ns.to_string())
    }

    async fn token(&self) -> Result<Zeroizing<String>, SecretError> {
        match &self.token {
            TokenSource::Static(s) => s
                .expose_str()
                .map(|t| Zeroizing::new(t.trim().to_owned()))
                .ok_or_else(|| SecretError::Unavailable("the API token is not UTF-8".into())),
            TokenSource::File(p) => {
                let raw = Zeroizing::new(tokio::fs::read_to_string(p).await.map_err(|e| {
                    SecretError::Unavailable(format!("reading the API token: {}", e.kind()))
                })?);
                Ok(Zeroizing::new(raw.trim().to_owned()))
            }
        }
    }
}

#[async_trait]
impl SecretStore for KubeSecretStore {
    async fn resolve(&self, ns: u64, r: &SecretRef) -> Result<Secret, SecretError> {
        let (secret, key) = parse_ref(r)?;
        let kns = self.kube_namespace(ns);
        let url = format!("{}/api/v1/namespaces/{kns}/secrets/{secret}", self.api);
        let token = self.token().await?;
        let mut auth = HeaderValue::from_str(&format!("Bearer {}", token.as_str()))
            .map_err(|_| SecretError::Unavailable("the API token is not a header value".into()))?;
        auth.set_sensitive(true);
        let req = Request::get(&url)
            .header(http::header::ACCEPT, "application/json")
            .header(http::header::AUTHORIZATION, auth)
            .body(Bytes::new())
            .map_err(|e| SecretError::Unavailable(format!("building the request: {e}")))?;
        let resp = self
            .client
            .send(req)
            .await
            .map_err(|e| SecretError::Unavailable(e.message))?;
        let not_found = || SecretError::NotFound {
            namespace: ns,
            name: r.name.clone(),
        };
        match resp.status() {
            StatusCode::OK => {}
            StatusCode::NOT_FOUND => return Err(not_found()),
            s @ (StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) => {
                return Err(SecretError::Denied {
                    namespace: ns,
                    name: r.name.clone(),
                    reason: format!("the API server answered {s}"),
                });
            }
            s => {
                return Err(SecretError::Unavailable(format!(
                    "the API server answered {s}"
                )));
            }
        }
        // The body is never put in an error: it holds every key's value.
        let doc: SecretDoc = serde_json::from_slice(resp.body()).map_err(|_| {
            SecretError::Unavailable("the API server's Secret could not be parsed".into())
        })?;
        let encoded = doc.data.get(&key).ok_or_else(not_found)?;
        let bytes = Zeroizing::new(STANDARD.decode(encoded.as_bytes()).map_err(|_| {
            SecretError::Unavailable(format!("key {key:?} of the Secret is not base64"))
        })?);
        Ok(Secret::new(bytes.as_slice()))
    }
}

/// The volumes and mounts that give a fallback pod its secrets as files,
/// and where each reference lands.
#[derive(Debug, Clone, PartialEq)]
pub struct SecretMounts {
    /// Pod `volumes`: one per `Secret`, holding only the referenced keys,
    /// mode 0400.
    pub volumes: Vec<Value>,
    /// Container `volumeMounts`: `<root>/<secret>`, read-only.
    pub volume_mounts: Vec<Value>,
    /// Reference name → file path.
    pub files: BTreeMap<String, String>,
}

/// [`SecretMounts`] for `refs` under `mount_root`. Deterministic: volumes
/// are named `secret-<n>` in the order of the sorted `Secret` names.
pub fn secret_mounts(refs: &[SecretRef], mount_root: &str) -> Result<SecretMounts, SecretError> {
    let root = mount_root.trim_end_matches('/');
    let mut by_secret: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut files = BTreeMap::new();
    for r in refs {
        let (secret, key) = parse_ref(r)?;
        files.insert(r.name.clone(), format!("{root}/{secret}/{key}"));
        by_secret.entry(secret).or_default().insert(key);
    }
    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();
    for (i, (secret, keys)) in by_secret.iter().enumerate() {
        let name = format!("secret-{i}");
        let items: Vec<Value> = keys
            .iter()
            .map(|k| json!({ "key": k, "path": k }))
            .collect();
        volumes.push(json!({
            "name": name,
            "secret": { "secretName": secret, "defaultMode": 0o400, "items": items, "optional": false }
        }));
        volume_mounts.push(json!({
            "name": name,
            "mountPath": format!("{root}/{secret}"),
            "readOnly": true
        }));
    }
    Ok(SecretMounts {
        volumes,
        volume_mounts,
        files,
    })
}

/// An instance config as a fallback renderer may write it into a
/// ConfigMap: each of the manifest's `secrets` paths, which holds a secret
/// reference, becomes `{"file": "<mounted path>"}`. A secret path whose
/// reference is not in `mounts` is an error, so nothing is rendered with a
/// dangling (or literal) value.
pub fn config_with_secret_files(
    spec: &ConnectorSpec,
    config: &Value,
    mounts: &SecretMounts,
) -> Result<Value, SecretError> {
    let mut out = config.clone();
    for path in &spec.secrets {
        let pointer: String = path
            .split('.')
            .map(|p| format!("/{}", p.replace('~', "~0").replace('/', "~1")))
            .collect();
        let Some(cur) = out.pointer_mut(&pointer) else {
            continue;
        };
        let name = cur.as_str().ok_or_else(|| SecretError::InvalidName {
            name: path.clone(),
            reason: "a secret path must hold a secret reference".into(),
        })?;
        let file = mounts
            .files
            .get(name)
            .ok_or_else(|| SecretError::InvalidName {
                name: path.clone(),
                reason: "the reference is not mounted".into(),
            })?;
        *cur = json!({ "file": file });
    }
    Ok(out)
}

/// A Kubernetes `Secret`: the only rendered object that may carry secret
/// bytes (Global Constraints).
pub fn render_secret(k8s_namespace: &str, name: &str, data: &BTreeMap<String, Secret>) -> Value {
    let data: serde_json::Map<String, Value> = data
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(STANDARD.encode(v.expose()))))
        .collect();
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {
            "name": name,
            "namespace": k8s_namespace,
            "labels": { "app.kubernetes.io/managed-by": "loams-flow" }
        },
        "data": data
    })
}
