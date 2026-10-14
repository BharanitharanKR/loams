//! The Kubernetes runtime (design §46 §7.2, §15; PG2 Task 13, feature
//! `kubernetes`): each compute is a Pod, each endpoint's address a
//! ClusterIP Service, in the namespace `loams-pg-<ns>` of the compute's
//! Loams namespace.
//!
//! **Objects** (all labelled `app.kubernetes.io/managed-by:
//! loams-pg-control`), rendered by the pure functions [`compute_pod`],
//! [`endpoint_service`], [`computes_service`], [`compute_secret`] and
//! [`role`], so a record set always renders the same bytes:
//!
//! | Object | Name | What |
//! |---|---|---|
//! | Pod | `cmp-<ulid>` (the compute id, lower case) | `compute_ctl` and Postgres, `restartPolicy: Never` (replace, never restart), labels `loams.dev/{org,namespace,project,endpoint,compute}` (a warm-pool Pod gets its project and endpoint when it is bound, Task 14), Argo CD told to ignore it |
//! | Secret | the Pod's name | the upcall token (`token`), and with a CA the compute's certificate (`tls.crt`, `tls.key`, `ca.crt`) |
//! | Service | `ep-<ulid>` (the endpoint id, lower case) | port 5432 to the compute's [`COMPUTE_PG_PORT`]; its selector names the endpoint's current compute ([`ComputeRuntime::bind_address`]), and nothing before |
//! | Service | `computes` (headless) | DNS for every compute Pod (`hostname` and `subdomain`), so `compute_ctl` is `http://cmp-<ulid>.computes.loams-pg-<ns>.svc.<domain>:3080` from the moment the Pod exists |
//!
//! **The Pod's security** (§46 §15.1): non-root (uid and gid 1000, the
//! compute image's `postgres`), a read-only root filesystem with `emptyDir`
//! volumes for `/var/db/postgres` and `/tmp`, every capability dropped, no
//! privilege escalation, `seccompProfile: RuntimeDefault`, no
//! ServiceAccount token, no service links, and an optional
//! `runtimeClassName` (gVisor or Kata, Q645). The token reaches
//! `compute_ctl` from the Secret, never from the Pod's spec. CPU and memory
//! requests equal their limits (1 CU is 1 vCPU and 4 GiB); `resize` asks
//! the Pod's `resize` subresource for new ones in place.
//!
//! **RBAC** ([`role`]): a Role in each `loams-pg-*` namespace over Pods
//! (and their `resize`), Services and Secrets only; Task 48's chart ships
//! it with a RoleBinding to `pg-control`'s ServiceAccount.
//!
//! [`ComputeRuntime::bind_address`]: super::ComputeRuntime::bind_address

use std::fmt;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use k8s_openapi::api::core::v1::{Pod, Secret as KubeSecret, Service};
use k8s_openapi::api::rbac::v1::Role;
use kube::api::{Api, DeleteParams, Patch, PatchParams, PostParams};
use serde_json::json;

use super::certs::ComputeCa;
use super::compose::check_namespace;
use super::{
    BackendAddr, COMPUTE_CTL_PORT, COMPUTE_PG_PORT, ComputeHandle, ComputeLaunch, ComputeRuntime,
    ResizeOutcome, RuntimeError, RuntimeStatus, check_upcall_token, compute_ctl_args, is_pinned,
};
use crate::ids::{ComputeId, EndpointId};
use crate::model::{Cu, EndpointRec};

/// The server-side apply field manager, as the secret store's.
pub const FIELD_MANAGER: &str = "loams-pg-control";

/// The port of an endpoint's Service: Postgres's own.
pub const SERVICE_PORT: u16 = 5432;

/// The headless Service that gives every compute Pod a DNS name.
pub const COMPUTES_SERVICE: &str = "computes";

/// The compute image's `postgres` user and group.
pub const COMPUTE_UID: i64 = 1000;

/// Where a compute's certificate is mounted.
pub const TLS_DIR: &str = "/etc/loams-compute/tls";

/// What every compute object carries as `app.kubernetes.io/component`.
pub const COMPONENT: &str = "postgres-compute";

/// The selector value of an endpoint Service no compute serves yet.
const UNBOUND: &str = "none";

/// How long a stopped Pod has to exit (it was `/terminate`d first).
const STOP_GRACE_S: u32 = 10;

/// The Kubernetes runtime's settings.
#[derive(Debug, Clone)]
pub struct KubeConfig {
    /// `compute_ctl`'s `--control-plane-uri`: the upcall Service as the
    /// computes reach it.
    pub control_plane_uri: String,
    /// The cluster's DNS domain (`cluster.local`).
    pub cluster_domain: String,
    /// The Pods' `runtimeClassName`, when a sandbox is used (Q645).
    pub runtime_class: Option<String>,
    /// The `loams.dev/org` label (R12.6: empty until MT4's tenants).
    pub org: String,
}

impl KubeConfig {
    pub fn new(control_plane_uri: impl Into<String>) -> Self {
        KubeConfig {
            control_plane_uri: control_plane_uri.into(),
            cluster_domain: "cluster.local".into(),
            runtime_class: None,
            org: String::new(),
        }
    }
}

/// The Kubernetes namespace of Loams namespace `ns`: `loams-pg-<ns>`.
///
/// # Errors
///
/// [`RuntimeError::InvalidArgument`] when `ns` is not a DNS label short
/// enough to make one.
pub fn namespace_for(ns: &str) -> Result<String, RuntimeError> {
    check_namespace(ns)?;
    let name = format!("loams-pg-{ns}");
    if name.len() > 63 {
        return Err(RuntimeError::InvalidArgument(format!(
            "namespace {ns:?} is too long for a Kubernetes namespace ({name})"
        )));
    }
    Ok(name)
}

/// A compute's Pod and Secret name: its id in lower case.
pub fn pod_name(id: &ComputeId) -> String {
    id.to_string().to_ascii_lowercase()
}

/// An endpoint's Service name: its id in lower case.
///
/// # Errors
///
/// [`RuntimeError::InvalidArgument`] for an id that is not canonical.
pub fn service_name(endpoint_id: &str) -> Result<String, RuntimeError> {
    endpoint_id.parse::<EndpointId>().map_err(|_| {
        RuntimeError::InvalidArgument(format!("{endpoint_id:?} is not an endpoint id"))
    })?;
    Ok(endpoint_id.to_ascii_lowercase())
}

/// An endpoint's address: its Service's name in the cluster's DNS.
fn address(config: &KubeConfig, k8s_ns: &str, service: &str) -> BackendAddr {
    BackendAddr::new(
        format!("{service}.{k8s_ns}.svc.{}", config.cluster_domain),
        SERVICE_PORT,
    )
}

/// `compute_ctl`'s external API, through the headless Service.
fn ctl_url(config: &KubeConfig, k8s_ns: &str, pod: &str) -> String {
    format!(
        "http://{pod}.{COMPUTES_SERVICE}.{k8s_ns}.svc.{}:{COMPUTE_CTL_PORT}",
        config.cluster_domain
    )
}

/// `0.25 CU` steps as CPU millicores and memory (1 CU = 1 vCPU, 4 GiB).
fn resources(cu: Cu) -> serde_json::Value {
    let amounts = json!({
        "cpu": format!("{}m", u32::from(cu.0) * 250),
        "memory": format!("{}Gi", cu.0),
    });
    json!({ "requests": amounts, "limits": amounts })
}

fn from_json<T: serde::de::DeserializeOwned>(v: serde_json::Value, what: &str) -> T {
    serde_json::from_value(v)
        .unwrap_or_else(|e| panic!("the {what} template is a valid {what}: {e}"))
}

/// The compute's Pod. An unbound warm-pool compute's (Task 14) has empty
/// `loams.dev/project` and `loams.dev/endpoint` labels until
/// [`bind_address`](ComputeRuntime::bind_address) sets them.
///
/// # Panics
///
/// Never: the template is a valid Pod.
pub fn compute_pod(config: &KubeConfig, k8s_ns: &str, launch: &ComputeLaunch, tls: bool) -> Pod {
    let id = launch.compute_id.to_string();
    let name = pod_name(&launch.compute_id);
    let (project, endpoint) = launch.binding.as_ref().map_or_else(
        || (String::new(), String::new()),
        |b| (b.project_id.clone(), b.endpoint_id.clone()),
    );
    let namespace = launch.namespace.clone();
    let mut mounts = vec![
        json!({ "name": "data", "mountPath": "/var/db/postgres" }),
        json!({ "name": "tmp", "mountPath": "/tmp" }),
    ];
    let mut volumes = vec![
        json!({ "name": "data", "emptyDir": {} }),
        json!({ "name": "tmp", "emptyDir": {} }),
    ];
    if tls {
        mounts.push(json!({ "name": "tls", "mountPath": TLS_DIR, "readOnly": true }));
        volumes.push(json!({
            "name": "tls",
            "secret": {
                "secretName": name,
                "defaultMode": 0o440,
                "items": [
                    { "key": "tls.crt", "path": "tls.crt" },
                    { "key": "tls.key", "path": "tls.key" },
                    { "key": "ca.crt", "path": "ca.crt" },
                ],
            },
        }));
    }
    let mut spec = json!({
        "hostname": name,
        "subdomain": COMPUTES_SERVICE,
        "restartPolicy": "Never",
        "automountServiceAccountToken": false,
        "enableServiceLinks": false,
        "terminationGracePeriodSeconds": STOP_GRACE_S,
        "securityContext": {
            "runAsNonRoot": true,
            "runAsUser": COMPUTE_UID,
            "runAsGroup": COMPUTE_UID,
            "fsGroup": COMPUTE_UID,
            "seccompProfile": { "type": "RuntimeDefault" },
        },
        "containers": [{
            "name": "compute",
            "image": launch.image,
            "command": ["/usr/local/bin/compute_ctl"],
            "args": compute_ctl_args(&id, &config.control_plane_uri),
            "env": [{
                "name": "NEON_CONTROL_PLANE_TOKEN",
                "valueFrom": { "secretKeyRef": { "name": name, "key": "token" } },
            }],
            "ports": [
                { "name": "postgres", "containerPort": COMPUTE_PG_PORT },
                { "name": "http", "containerPort": COMPUTE_CTL_PORT },
            ],
            "resources": resources(launch.cu),
            "resizePolicy": [
                { "resourceName": "cpu", "restartPolicy": "NotRequired" },
                { "resourceName": "memory", "restartPolicy": "NotRequired" },
            ],
            "securityContext": {
                "allowPrivilegeEscalation": false,
                "readOnlyRootFilesystem": true,
                "capabilities": { "drop": ["ALL"] },
            },
            "volumeMounts": mounts,
        }],
        "volumes": volumes,
    });
    if let Some(class) = &config.runtime_class {
        spec["runtimeClassName"] = json!(class);
    }
    from_json(
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": k8s_ns,
                "labels": {
                    "app.kubernetes.io/managed-by": FIELD_MANAGER,
                    "app.kubernetes.io/component": COMPONENT,
                    "loams.dev/org": config.org,
                    "loams.dev/namespace": namespace,
                    "loams.dev/project": project,
                    "loams.dev/endpoint": endpoint,
                    "loams.dev/compute": id,
                },
                "annotations": {
                    "argocd.argoproj.io/compare-options": "IgnoreExtraneous",
                },
            },
            "spec": spec,
        }),
        "Pod",
    )
}

/// The compute's Secret: its upcall token, and its certificate when the
/// runtime has a CA.
///
/// # Panics
///
/// Never: the template is a valid Secret.
pub fn compute_secret(
    k8s_ns: &str,
    launch: &ComputeLaunch,
    tls: Option<(&str, &str, &str)>,
) -> KubeSecret {
    let b64 = |s: &str| json!(STANDARD.encode(s.as_bytes()));
    let mut data = json!({ "token": b64(launch.upcall_token.expose()) });
    if let Some((cert, key, ca)) = tls {
        data["tls.crt"] = b64(cert);
        data["tls.key"] = b64(key);
        data["ca.crt"] = b64(ca);
    }
    from_json(
        json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {
                "name": pod_name(&launch.compute_id),
                "namespace": k8s_ns,
                "labels": {
                    "app.kubernetes.io/managed-by": FIELD_MANAGER,
                    "app.kubernetes.io/component": COMPONENT,
                    "loams.dev/compute": launch.compute_id.to_string(),
                },
            },
            "data": data,
        }),
        "Secret",
    )
}

/// An endpoint's Service, selecting no compute until one is bound.
///
/// # Errors
///
/// [`RuntimeError::InvalidArgument`] for an endpoint id that is not
/// canonical.
pub fn endpoint_service(
    config: &KubeConfig,
    k8s_ns: &str,
    ns: &str,
    ep: &EndpointRec,
) -> Result<Service, RuntimeError> {
    let name = service_name(&ep.id)?;
    Ok(from_json(
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": name,
                "namespace": k8s_ns,
                "labels": {
                    "app.kubernetes.io/managed-by": FIELD_MANAGER,
                    "app.kubernetes.io/component": "postgres-endpoint",
                    "loams.dev/org": config.org,
                    "loams.dev/namespace": ns,
                    "loams.dev/project": ep.project_id,
                    "loams.dev/endpoint": ep.id,
                },
                "annotations": {
                    "argocd.argoproj.io/compare-options": "IgnoreExtraneous",
                },
            },
            "spec": {
                "type": "ClusterIP",
                "selector": {
                    "app.kubernetes.io/component": COMPONENT,
                    "loams.dev/compute": UNBOUND,
                },
                "ports": [{
                    "name": "postgres",
                    "port": SERVICE_PORT,
                    "targetPort": COMPUTE_PG_PORT,
                    "protocol": "TCP",
                }],
            },
        }),
        "Service",
    ))
}

/// The headless Service that names every compute Pod of a namespace.
///
/// # Panics
///
/// Never: the template is a valid Service.
pub fn computes_service(k8s_ns: &str) -> Service {
    from_json(
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": COMPUTES_SERVICE,
                "namespace": k8s_ns,
                "labels": {
                    "app.kubernetes.io/managed-by": FIELD_MANAGER,
                    "app.kubernetes.io/component": COMPONENT,
                },
                "annotations": {
                    "argocd.argoproj.io/compare-options": "IgnoreExtraneous",
                },
            },
            "spec": {
                "clusterIP": "None",
                "publishNotReadyAddresses": true,
                "selector": { "app.kubernetes.io/component": COMPONENT },
                "ports": [{ "name": "http", "port": COMPUTE_CTL_PORT, "protocol": "TCP" }],
            },
        }),
        "Service",
    )
}

/// The Role `pg-control` needs in `k8s_ns`: Pods (with `resize`; `patch`
/// labels a warm-pool Pod with its endpoint when it is bound, R14.5),
/// Services and Secrets, nothing else (§46 §7.2).
///
/// # Panics
///
/// Never: the template is a valid Role.
pub fn role(k8s_ns: &str) -> Role {
    from_json(
        json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {
                "name": "loams-pg-control",
                "namespace": k8s_ns,
                "labels": { "app.kubernetes.io/managed-by": FIELD_MANAGER },
            },
            "rules": [
                {
                    "apiGroups": [""],
                    "resources": ["pods"],
                    "verbs": ["get", "list", "watch", "create", "patch", "delete"],
                },
                {
                    "apiGroups": [""],
                    "resources": ["pods/resize"],
                    "verbs": ["patch"],
                },
                {
                    "apiGroups": [""],
                    "resources": ["services"],
                    "verbs": ["get", "list", "create", "patch", "delete"],
                },
                {
                    "apiGroups": [""],
                    "resources": ["secrets"],
                    "verbs": ["get", "create", "patch", "delete"],
                },
            ],
        }),
        "Role",
    )
}

/// The Kubernetes runtime. See the module doc.
pub struct KubeRuntime {
    client: kube::Client,
    config: KubeConfig,
    ca: Option<ComputeCa>,
}

impl fmt::Debug for KubeRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KubeRuntime")
            .field("config", &self.config)
            .field("ca", &self.ca.is_some())
            .finish_non_exhaustive()
    }
}

/// What a failed call tells: the API's code and reason, or the client's
/// error (never an object's data).
fn api_error(what: &str, e: &kube::Error) -> RuntimeError {
    match e {
        kube::Error::Api(s) if s.code == 400 || s.code == 422 => {
            RuntimeError::InvalidArgument(format!("{what}: {} {}: {}", s.code, s.reason, s.message))
        }
        kube::Error::Api(s) => {
            RuntimeError::Unavailable(format!("{what}: {} {}: {}", s.code, s.reason, s.message))
        }
        other => RuntimeError::Unavailable(format!("{what}: {other}")),
    }
}

fn is_status(e: &kube::Error, code: u16) -> bool {
    matches!(e, kube::Error::Api(s) if s.code == code)
}

impl KubeRuntime {
    /// The runtime over `client`. With `ca`, every compute gets a
    /// certificate for its endpoint's Service names (R10.7).
    pub fn new(client: kube::Client, config: KubeConfig, ca: Option<ComputeCa>) -> Self {
        KubeRuntime { client, config, ca }
    }

    /// The runtime through the client the environment configures: the
    /// in-cluster ServiceAccount, or `KUBECONFIG`.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Unavailable`] when no client can be configured.
    pub async fn from_environment(
        config: KubeConfig,
        ca: Option<ComputeCa>,
    ) -> Result<Self, RuntimeError> {
        let client = kube::Client::try_default()
            .await
            .map_err(|e| RuntimeError::Unavailable(format!("no Kubernetes client: {e}")))?;
        Ok(Self::new(client, config, ca))
    }

    fn pods(&self, k8s_ns: &str) -> Api<Pod> {
        Api::namespaced(self.client.clone(), k8s_ns)
    }

    fn services(&self, k8s_ns: &str) -> Api<Service> {
        Api::namespaced(self.client.clone(), k8s_ns)
    }

    fn secrets(&self, k8s_ns: &str) -> Api<KubeSecret> {
        Api::namespaced(self.client.clone(), k8s_ns)
    }

    /// Creates `obj` unless it exists.
    async fn create_once<K>(&self, api: &Api<K>, obj: &K, what: &str) -> Result<(), RuntimeError>
    where
        K: kube::Resource + Clone + serde::de::DeserializeOwned + serde::Serialize + fmt::Debug,
    {
        match api.create(&PostParams::default(), obj).await {
            Ok(_) => Ok(()),
            Err(e) if is_status(&e, 409) => Ok(()),
            Err(e) => Err(api_error(what, &e)),
        }
    }

    /// The compute's certificate, its key and the CA's, for the endpoint's
    /// Service names; an unbound warm-pool compute's for its own Pod's
    /// names, which no other compute has (R14.5).
    fn certificate(
        &self,
        k8s_ns: &str,
        launch: &ComputeLaunch,
    ) -> Result<Option<(String, String, String)>, RuntimeError> {
        let Some(ca) = &self.ca else {
            return Ok(None);
        };
        let domain = &self.config.cluster_domain;
        let names = match &launch.binding {
            Some(binding) => {
                let service = service_name(&binding.endpoint_id)?;
                vec![
                    format!("{service}.{k8s_ns}.svc.{domain}"),
                    format!("{service}.{k8s_ns}.svc"),
                    format!("{service}.{k8s_ns}"),
                ]
            }
            None => {
                let pod = pod_name(&launch.compute_id);
                vec![
                    format!("{pod}.{COMPUTES_SERVICE}.{k8s_ns}.svc.{domain}"),
                    format!("{pod}.{COMPUTES_SERVICE}.{k8s_ns}.svc"),
                ]
            }
        };
        let cert = ca
            .issue(&names, std::time::SystemTime::now())
            .map_err(|e| {
                RuntimeError::Unavailable(format!("issuing the compute's certificate: {e}"))
            })?;
        Ok(Some((
            cert.cert_pem,
            cert.key_pem.expose().clone(),
            ca.cert_pem().to_owned(),
        )))
    }
}

#[async_trait]
impl ComputeRuntime for KubeRuntime {
    async fn ensure_address(
        &self,
        ns: &str,
        ep: &EndpointRec,
    ) -> Result<BackendAddr, RuntimeError> {
        let k8s_ns = namespace_for(ns)?;
        let service = endpoint_service(&self.config, &k8s_ns, ns, ep)?;
        let name = service_name(&ep.id)?;
        let addr = address(&self.config, &k8s_ns, &name);
        if let Some(recorded) = &ep.backend_addr
            && BackendAddr::parse(recorded)? != addr
        {
            return Err(RuntimeError::InvalidArgument(format!(
                "endpoint {}'s address {recorded} is not its Service's ({addr})",
                ep.id
            )));
        }
        self.create_once(
            &self.services(&k8s_ns),
            &service,
            "creating the endpoint's Service",
        )
        .await?;
        Ok(addr)
    }

    async fn remove_address(&self, ns: &str, ep: &EndpointRec) -> Result<(), RuntimeError> {
        let k8s_ns = namespace_for(ns)?;
        let name = service_name(&ep.id)?;
        match self
            .services(&k8s_ns)
            .delete(&name, &DeleteParams::default())
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if is_status(&e, 404) => Ok(()),
            Err(e) => Err(api_error("deleting the endpoint's Service", &e)),
        }
    }

    async fn start(&self, c: &ComputeLaunch) -> Result<ComputeHandle, RuntimeError> {
        let k8s_ns = namespace_for(c.namespace()?)?;
        if !is_pinned(&c.image) {
            return Err(RuntimeError::InvalidArgument(format!(
                "image {:?} is not pinned by digest",
                c.image
            )));
        }
        if c.cu.0 == 0 {
            return Err(RuntimeError::InvalidArgument(
                "a compute needs at least 0.25 CU".into(),
            ));
        }
        check_upcall_token(c.upcall_token.expose())?;
        // An unbound warm-pool compute (Task 14) has no address yet:
        // `bind_address` points its endpoint's Service at it.
        if let Some(binding) = &c.binding {
            let service = service_name(&binding.endpoint_id)?;
            let expected = address(&self.config, &k8s_ns, &service);
            if c.address.as_ref() != Some(&expected) {
                return Err(RuntimeError::InvalidArgument(format!(
                    "the launch's address is not endpoint {}'s Service ({expected})",
                    binding.endpoint_id
                )));
            }
        }
        let name = pod_name(&c.compute_id);
        let pods = self.pods(&k8s_ns);
        let existing = pods
            .get_opt(&name)
            .await
            .map_err(|e| api_error("reading the compute's Pod", &e))?;
        if existing.is_none() {
            self.create_once(
                &self.services(&k8s_ns),
                &computes_service(&k8s_ns),
                "creating the computes' headless Service",
            )
            .await?;
            let tls = self.certificate(&k8s_ns, c)?;
            let secret = compute_secret(
                &k8s_ns,
                c,
                tls.as_ref()
                    .map(|(cert, key, ca)| (cert.as_str(), key.as_str(), ca.as_str())),
            );
            // Applied: a retry after a crash between the two writes
            // replaces the token the first try wrote.
            self.secrets(&k8s_ns)
                .patch(
                    &name,
                    &PatchParams::apply(FIELD_MANAGER).force(),
                    &Patch::Apply(&secret),
                )
                .await
                .map_err(|e| api_error("writing the compute's Secret", &e))?;
            let pod = compute_pod(&self.config, &k8s_ns, c, tls.is_some());
            self.create_once(&pods, &pod, "creating the compute's Pod")
                .await?;
        }
        Ok(ComputeHandle {
            compute_id: c.compute_id,
            ctl_url: ctl_url(&self.config, &k8s_ns, &name),
            address: c.address.clone(),
        })
    }

    async fn bind_address(
        &self,
        ns: &str,
        ep: &EndpointRec,
        c: &ComputeHandle,
    ) -> Result<(), RuntimeError> {
        let k8s_ns = namespace_for(ns)?;
        let name = service_name(&ep.id)?;
        // The Pod names its endpoint: a warm-pool Pod (Task 14) was started
        // without one. A no-op for a Pod started for its endpoint.
        let labels = json!({
            "metadata": { "labels": {
                "loams.dev/project": ep.project_id,
                "loams.dev/endpoint": ep.id,
            } },
        });
        self.pods(&k8s_ns)
            .patch(
                &pod_name(&c.compute_id),
                &PatchParams::default(),
                &Patch::Merge(&labels),
            )
            .await
            .map_err(|e| api_error("labelling the compute's Pod with its endpoint", &e))?;
        let patch = json!({
            "spec": { "selector": { "loams.dev/compute": c.compute_id.to_string() } },
        });
        self.services(&k8s_ns)
            .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .map_err(|e| api_error("pointing the endpoint's Service at the compute", &e))?;
        Ok(())
    }

    async fn stop(&self, ns: &str, compute_id: &ComputeId) -> Result<(), RuntimeError> {
        let k8s_ns = namespace_for(ns)?;
        let name = pod_name(compute_id);
        let dp = DeleteParams {
            grace_period_seconds: Some(STOP_GRACE_S),
            ..DeleteParams::default()
        };
        match self.pods(&k8s_ns).delete(&name, &dp).await {
            Ok(_) => {}
            Err(e) if is_status(&e, 404) => {}
            Err(e) => return Err(api_error("deleting the compute's Pod", &e)),
        }
        match self
            .secrets(&k8s_ns)
            .delete(&name, &DeleteParams::default())
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if is_status(&e, 404) => Ok(()),
            Err(e) => Err(api_error("deleting the compute's Secret", &e)),
        }
    }

    async fn status(
        &self,
        ns: &str,
        compute_id: &ComputeId,
    ) -> Result<RuntimeStatus, RuntimeError> {
        let k8s_ns = namespace_for(ns)?;
        let pod = self
            .pods(&k8s_ns)
            .get_opt(&pod_name(compute_id))
            .await
            .map_err(|e| api_error("reading the compute's Pod", &e))?;
        Ok(pod.map_or(RuntimeStatus::Missing, |p| pod_status(&p)))
    }

    async fn resize(
        &self,
        ns: &str,
        compute_id: &ComputeId,
        cu: Cu,
    ) -> Result<ResizeOutcome, RuntimeError> {
        let k8s_ns = namespace_for(ns)?;
        let name = pod_name(compute_id);
        let pods = self.pods(&k8s_ns);
        if pods
            .get_opt(&name)
            .await
            .map_err(|e| api_error("reading the compute's Pod", &e))?
            .is_none()
        {
            return Err(RuntimeError::InvalidArgument(format!(
                "compute {compute_id} has no Pod"
            )));
        }
        let patch = json!({
            "spec": { "containers": [{ "name": "compute", "resources": resources(cu) }] },
        });
        match pods
            .patch_subresource(
                "resize",
                &name,
                &PatchParams::default(),
                &Patch::Strategic(&patch),
            )
            .await
        {
            Ok(_) => Ok(ResizeOutcome::Applied),
            // The Pod exists, so a 404 is the subresource's: an API server
            // without in-place resize (before Kubernetes 1.33's beta).
            Err(e) if is_status(&e, 404) || is_status(&e, 405) => Ok(ResizeOutcome::Unsupported),
            Err(kube::Error::Api(s)) if matches!(s.code, 409 | 422) => Ok(ResizeOutcome::Refused(
                format!("{} {}", s.reason, s.message),
            )),
            Err(e) => Err(api_error("resizing the compute's Pod", &e)),
        }
    }
}

/// What the runtime sees of a Pod: being deleted or ended is `Exited`
/// (with the container's exit code when it has one), `Running` once its
/// container runs, `Pending` before.
pub fn pod_status(pod: &Pod) -> RuntimeStatus {
    let terminated_code = || {
        pod.status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.name == "compute"))
            .and_then(|c| c.state.as_ref())
            .and_then(|s| s.terminated.as_ref())
            .map(|t| t.exit_code)
    };
    if pod.metadata.deletion_timestamp.is_some() {
        return RuntimeStatus::Exited {
            code: terminated_code(),
        };
    }
    let phase = pod
        .status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .unwrap_or("Pending");
    match phase {
        "Running" => match terminated_code() {
            Some(code) => RuntimeStatus::Exited { code: Some(code) },
            None => RuntimeStatus::Running,
        },
        "Succeeded" | "Failed" => RuntimeStatus::Exited {
            code: terminated_code(),
        },
        _ => RuntimeStatus::Pending,
    }
}
