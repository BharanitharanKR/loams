//! The connector fleet on Kubernetes (CN2 Task 26, OR-16).
//!
//! * **Native connectors** run as tasks inside the `loams-fabric flow` pods
//!   (a Deployment the `loams-connectors` chart installs). Which pod runs which
//!   instance, and which pod reads which split, is decided by fenced leases
//!   ([`lease`]); [`native`] holds the scheduler that takes them and the
//!   Deployment's scale.
//! * **Fallbacks** (OR-4) run in pods of their own, which [`KubeRuntime`]
//!   renders: `loams-connect` (Camel) as one Deployment per namespace group
//!   with each instance's routes in a ConfigMap ([`connect`]); Debezium Server
//!   as one StatefulSet with a PVC per CDC instance ([`debezium`], Q350); the
//!   Iggy connectors runtime as one Deployment per namespace group ([`iggy`]).
//!
//! Rendering is deterministic (the golden tests in `tests/kube.rs`), and every
//! object is labelled with [`labels`]. Fallback pods get only the `Secret` keys
//! their instances reference, read-only and mode 0400, mount no
//! service-account token, and are confined by a NetworkPolicy rendered from
//! their namespace's [`EgressPolicy`] (R25.4, R25.9).
//!
//! The objects are `serde_json::Value`s in the Kubernetes API's JSON shape:
//! no Kubernetes client crate is linked (R25.6, R26.2). Applying them
//! (server-side apply through the API) is CN1 Task 3's supervisor's job (R26.1).

pub mod connect;
pub mod debezium;
pub mod iggy;
pub mod lease;
pub mod native;

use std::collections::{BTreeMap, BTreeSet};

use loams_connector_sdk::{InstanceKey, Scrubber, SecretRef};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::egress::{ClusterNetworks, EgressPolicy, render_network_policy};
use crate::manifest::RuntimeKind;
use crate::secrets::SecretError;
use crate::secrets::kube::{SecretMounts, secret_mounts};
use crate::validate::Direction;

/// The labels every rendered object carries (Task 26 Interfaces).
pub mod labels {
    /// The Loams namespace id.
    pub const NAMESPACE: &str = "loams.dev/namespace";
    /// The instance name, on per-instance objects.
    pub const INSTANCE: &str = "loams.dev/instance";
    /// The connector id, on per-instance objects.
    pub const CONNECTOR: &str = "loams.dev/connector";
    /// `camel`, `debezium` or `iggy` (the manifest's runtime slug).
    pub const RUNTIME: &str = "loams.dev/runtime";
    /// `"true"` on every fallback object.
    pub const FALLBACK: &str = "loams.dev/fallback";
    /// The namespace group, on per-group objects (R26.4).
    pub const GROUP: &str = "loams.dev/group";
    /// `app.kubernetes.io/managed-by`.
    pub const MANAGED_BY: &str = "app.kubernetes.io/managed-by";
}

/// The largest ConfigMap the API server stores.
pub const CONFIGMAP_MAX_BYTES: usize = 1 << 20;

/// Why the fleet could not be rendered. Never carries a file's contents or
/// a secret.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RenderError {
    #[error(
        "instance {instance}: runtime `{runtime}` is not a fallback; native instances run inside the flow pods"
    )]
    NotAFallback { instance: String, runtime: String },
    #[error("{what}: {reason}")]
    Invalid { what: String, reason: String },
    #[error(
        "instance {instance}: file `{file}` holds a resolved secret; reference it by its mounted path"
    )]
    SecretInConfig { instance: String, file: String },
    #[error(transparent)]
    Secret(#[from] SecretError),
}

fn invalid(what: impl Into<String>, reason: impl Into<String>) -> RenderError {
    RenderError::Invalid {
        what: what.into(),
        reason: reason.into(),
    }
}

// ------------------------------------------------------------------ config

/// The fleet's configuration: the chart renders it into the `fleet.yaml` of
/// the `<release>-fleet` ConfigMap, and `loams-fabric flow` reads it (R26.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetConfig {
    pub flow: FlowConfig,
    /// The Kubernetes namespace of a Loams namespace: `{ns}` is replaced by
    /// its id. The same template `KubeSecretStore` reads secrets with (R25.5).
    pub namespace_template: String,
    /// The Loams namespaces this fleet serves; the chart grants `flow` its
    /// Role in each one's Kubernetes namespace.
    pub served_namespaces: Vec<u64>,
    /// The cluster's pod and Service CIDRs and the control plane (R25.3).
    pub cluster: ClusterNetworks,
    /// Where fallback pods reach the Fabric (R26.5).
    pub fabric: FabricEndpoint,
    pub fallback: FallbackConfig,
}

/// The `flow` Deployment and how far it scales.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowConfig {
    pub k8s_namespace: String,
    pub deployment: String,
    pub min_replicas: u32,
    pub max_replicas: u32,
    /// The memory one `flow` pod gives native connectors (Task 27's budgets
    /// sum per instance against it).
    pub pod_memory_bytes: u64,
    pub max_instances_per_pod: u32,
}

/// The Fabric's pods, which fallback pods send to and read from
/// (`ingest`, Iggy).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FabricEndpoint {
    pub k8s_namespace: String,
    pub pod_labels: BTreeMap<String, String>,
    pub ports: Vec<u16>,
}

/// What every fallback pod shares, and each fallback runtime's own settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackConfig {
    /// A service account bound to nothing; its token is never mounted.
    pub service_account: String,
    /// Where secrets are mounted: `<root>/<secret>/<key>` (R25.5).
    pub secret_mount_root: String,
    pub camel: RuntimeConfig,
    pub debezium: RuntimeConfig,
    pub iggy: RuntimeConfig,
}

/// One fallback runtime's image and pod settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    pub image: ImageRef,
    /// The image's non-root user; also the pod's group and `fsGroup`, so the
    /// mode-0400 secret files are readable by it alone (R26.6).
    pub uid: u32,
    pub metrics_port: u16,
    pub metrics_path: String,
    pub resources: Resources,
    /// The PVC of each Debezium Server StatefulSet (Q350).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<Storage>,
}

/// An image pinned by digest (Global Constraints "Pins").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageRef {
    pub repository: String,
    /// `sha256:<64 hex>`.
    pub digest: String,
}

impl ImageRef {
    /// `<repository>@<digest>`.
    pub fn reference(&self) -> String {
        format!("{}@{}", self.repository, self.digest)
    }

    fn check(&self, what: &str) -> Result<(), RenderError> {
        let hex = self.digest.strip_prefix("sha256:").unwrap_or("");
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid(
                format!("{what} image"),
                "must be pinned by digest (sha256:<64 lowercase hex>), not a tag",
            ));
        }
        if self.repository.is_empty() || self.repository.contains('@') {
            return Err(invalid(format!("{what} image"), "bad repository"));
        }
        Ok(())
    }
}

/// Container resources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub cpu_request: String,
    pub memory_request: String,
    pub memory_limit: String,
}

/// A PVC's size and class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    pub size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

impl FleetConfig {
    /// Parses the chart's `fleet.yaml`.
    pub fn from_yaml(text: &str) -> Result<Self, RenderError> {
        serde_norway::from_str(text).map_err(|e| invalid("fleet.yaml", e.to_string()))
    }

    /// The Kubernetes namespace of Loams namespace `ns`.
    pub fn k8s_namespace(&self, ns: u64) -> String {
        self.namespace_template.replace("{ns}", &ns.to_string())
    }

    fn runtime(&self, kind: RuntimeKind) -> Option<&RuntimeConfig> {
        match kind {
            RuntimeKind::Camel => Some(&self.fallback.camel),
            RuntimeKind::Debezium => Some(&self.fallback.debezium),
            RuntimeKind::Iggy => Some(&self.fallback.iggy),
            RuntimeKind::Native | RuntimeKind::Openapi => None,
        }
    }

    fn validate(&self) -> Result<(), RenderError> {
        if !self.namespace_template.contains("{ns}") {
            return Err(invalid("namespace_template", "must contain {ns}"));
        }
        for ns in &self.served_namespaces {
            check_dns_label("served namespace", &self.k8s_namespace(*ns))?;
        }
        check_dns_label("flow.k8s_namespace", &self.flow.k8s_namespace)?;
        check_dns_label("flow.deployment", &self.flow.deployment)?;
        if self.flow.min_replicas == 0 || self.flow.min_replicas > self.flow.max_replicas {
            return Err(invalid("flow", "need 1 <= min_replicas <= max_replicas"));
        }
        if self.flow.pod_memory_bytes == 0 || self.flow.max_instances_per_pod == 0 {
            return Err(invalid("flow", "pod budgets must be positive"));
        }
        check_dns_label("fabric.k8s_namespace", &self.fabric.k8s_namespace)?;
        if self.fabric.pod_labels.is_empty() || self.fabric.ports.is_empty() {
            return Err(invalid("fabric", "needs pod labels and ports"));
        }
        check_dns_label("fallback.service_account", &self.fallback.service_account)?;
        if !self.fallback.secret_mount_root.starts_with('/') {
            return Err(invalid("fallback.secret_mount_root", "must be absolute"));
        }
        for (name, rt) in [
            ("camel", &self.fallback.camel),
            ("debezium", &self.fallback.debezium),
            ("iggy", &self.fallback.iggy),
        ] {
            rt.image.check(name)?;
            if rt.uid == 0 {
                return Err(invalid(
                    format!("{name}.uid"),
                    "fallback pods never run as root",
                ));
            }
            if !rt.metrics_path.starts_with('/') {
                return Err(invalid(format!("{name}.metrics_path"), "must be absolute"));
            }
        }
        if self.fallback.debezium.storage.is_none() {
            return Err(invalid(
                "debezium.storage",
                "Debezium Server needs a PVC (Q350)",
            ));
        }
        Ok(())
    }
}

// --------------------------------------------------------------- instances

/// A fallback instance as `KubeRuntime` receives it: Task 22's renderer has
/// already turned the instance into the runtime's own files, which name
/// secrets only by their mounted path ([`KubeRuntime::secret_mounts`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallbackInstance {
    pub key: InstanceKey,
    pub connector: String,
    /// `camel`, `debezium` or `iggy`.
    pub runtime: RuntimeKind,
    pub directions: Vec<Direction>,
    /// The namespace group (Camel and Iggy); `None` is the instance's own
    /// namespace's group, `ns-<id>`.
    pub group: Option<String>,
    /// File name → contents. Every name starts with `<instance>.`.
    pub files: BTreeMap<String, String>,
    /// The secrets the files reference.
    pub secrets: Vec<SecretRef>,
}

impl FallbackInstance {
    fn group_name(&self) -> String {
        self.group
            .clone()
            .unwrap_or_else(|| format!("ns-{}", self.key.namespace))
    }

    fn what(&self) -> String {
        format!("instance {}/{}", self.key.namespace, self.key.name)
    }

    /// The labels of this instance's own objects.
    fn labels(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (labels::NAMESPACE.to_owned(), self.key.namespace.to_string()),
            (labels::INSTANCE.to_owned(), self.key.name.clone()),
            (labels::CONNECTOR.to_owned(), self.connector.clone()),
            (labels::RUNTIME.to_owned(), self.runtime.slug().to_owned()),
            (labels::FALLBACK.to_owned(), "true".to_owned()),
        ])
    }
}

/// A namespace group's instances of one runtime, checked to share one
/// Kubernetes namespace and one egress policy.
pub(crate) struct Group<'a> {
    pub name: String,
    pub runtime: RuntimeKind,
    pub k8s_namespace: String,
    pub namespaces: BTreeSet<u64>,
    pub policy: EgressPolicy,
    pub instances: Vec<&'a FallbackInstance>,
}

impl Group<'_> {
    /// The labels of the group's workload and its pods.
    pub fn labels(&self) -> BTreeMap<String, String> {
        let mut l = self.selector();
        l.insert(labels::FALLBACK.to_owned(), "true".to_owned());
        if self.namespaces.len() == 1
            && let Some(ns) = self.namespaces.first()
        {
            l.insert(labels::NAMESPACE.to_owned(), ns.to_string());
        }
        l
    }

    /// What the workload and its NetworkPolicy select.
    pub fn selector(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (labels::GROUP.to_owned(), self.name.clone()),
            (labels::RUNTIME.to_owned(), self.runtime.slug().to_owned()),
        ])
    }

    pub fn secrets(&self) -> Vec<SecretRef> {
        let mut seen = BTreeSet::new();
        self.instances
            .iter()
            .flat_map(|i| i.secrets.iter())
            .filter(|r| seen.insert(r.name.clone()))
            .cloned()
            .collect()
    }
}

// ----------------------------------------------------------------- runtime

/// Renders the fallback fleet (Task 26 Interfaces).
#[derive(Debug, Clone)]
pub struct KubeRuntime {
    config: FleetConfig,
}

impl KubeRuntime {
    /// Refuses a configuration with an image not pinned by digest, a bad
    /// name, or a Debezium runtime without storage.
    pub fn new(config: FleetConfig) -> Result<Self, RenderError> {
        config.validate()?;
        Ok(Self { config })
    }

    pub fn config(&self) -> &FleetConfig {
        &self.config
    }

    /// Where an instance's secrets land in its pod; Task 22's renderers
    /// write these paths into the files they hand back.
    pub fn secret_mounts(&self, refs: &[SecretRef]) -> Result<SecretMounts, RenderError> {
        Ok(secret_mounts(
            refs,
            &self.config.fallback.secret_mount_root,
        )?)
    }

    /// The objects for `instances`, sorted (ConfigMaps, Services,
    /// NetworkPolicies, Deployments, StatefulSets; then by namespace and
    /// name). `policies` holds each Loams namespace's egress policy; a
    /// namespace without one gets [`EgressPolicy::for_cluster`].
    pub fn render(
        &self,
        instances: &[FallbackInstance],
        policies: &BTreeMap<u64, EgressPolicy>,
    ) -> Result<Vec<Value>, RenderError> {
        let groups = self.groups(instances, policies)?;
        let mut out = Vec::new();
        for g in &groups {
            match g.runtime {
                RuntimeKind::Camel => connect::render(self, g, &mut out)?,
                RuntimeKind::Iggy => iggy::render(self, g, &mut out)?,
                RuntimeKind::Debezium => {
                    for i in &g.instances {
                        debezium::render(self, g, i, &mut out)?;
                    }
                }
                RuntimeKind::Native | RuntimeKind::Openapi => {}
            }
        }
        out.sort_by_key(sort_key);
        Ok(out)
    }

    /// [`render`](Self::render), after refusing any file that holds a
    /// secret `scrubber` has seen resolved (Global Constraints "No secret").
    pub fn render_checked(
        &self,
        instances: &[FallbackInstance],
        policies: &BTreeMap<u64, EgressPolicy>,
        scrubber: &Scrubber,
    ) -> Result<Vec<Value>, RenderError> {
        for i in instances {
            for (file, text) in &i.files {
                if scrubber.contains(text) {
                    return Err(RenderError::SecretInConfig {
                        instance: format!("{}/{}", i.key.namespace, i.key.name),
                        file: file.clone(),
                    });
                }
            }
        }
        let out = self.render(instances, policies)?;
        if out.iter().any(|o| scrubber.contains(&o.to_string())) {
            return Err(invalid("rendered objects", "hold a resolved secret"));
        }
        Ok(out)
    }

    /// The `flow` Deployment's `Scale` (`autoscaling/v1`), which the fleet's
    /// leader writes to `deployments/<name>/scale` (R26.7).
    pub fn render_flow_scale(&self, replicas: u32) -> Value {
        json!({
            "apiVersion": "autoscaling/v1",
            "kind": "Scale",
            "metadata": {
                "name": self.config.flow.deployment,
                "namespace": self.config.flow.k8s_namespace,
            },
            "spec": { "replicas": replicas }
        })
    }

    fn groups<'a>(
        &self,
        instances: &'a [FallbackInstance],
        policies: &BTreeMap<u64, EgressPolicy>,
    ) -> Result<Vec<Group<'a>>, RenderError> {
        let mut seen = BTreeSet::new();
        let mut by_group: BTreeMap<(String, String), Vec<&'a FallbackInstance>> = BTreeMap::new();
        for i in instances {
            self.check_instance(i)?;
            if !seen.insert(i.key.clone()) {
                return Err(invalid(i.what(), "listed twice"));
            }
            // Debezium instances are one StatefulSet each; their group is
            // their namespace's.
            let group = match i.runtime {
                RuntimeKind::Debezium => format!("ns-{}", i.key.namespace),
                _ => i.group_name(),
            };
            check_dns_label("group", &group)?;
            if group.len() > 30 {
                return Err(invalid(format!("group {group}"), "at most 30 characters"));
            }
            by_group
                .entry((i.runtime.slug().to_owned(), group))
                .or_default()
                .push(i);
        }
        let mut out = Vec::new();
        for ((_, name), mut list) in by_group {
            list.sort_by(|a, b| a.key.cmp(&b.key));
            let runtime = list[0].runtime;
            let namespaces: BTreeSet<u64> = list.iter().map(|i| i.key.namespace).collect();
            let k8s: BTreeSet<String> = namespaces
                .iter()
                .map(|n| self.config.k8s_namespace(*n))
                .collect();
            if k8s.len() != 1 {
                return Err(invalid(
                    format!("group {name}"),
                    "its namespaces map to different Kubernetes namespaces, so its pod could not mount their secrets",
                ));
            }
            let policy_of = |n: &u64| {
                policies
                    .get(n)
                    .cloned()
                    .unwrap_or_else(|| EgressPolicy::for_cluster(&self.config.cluster))
            };
            let policy = namespaces.iter().next().map(policy_of).unwrap_or_default();
            if namespaces.iter().any(|n| policy_of(n) != policy) {
                return Err(invalid(
                    format!("group {name}"),
                    "its namespaces have different egress policies",
                ));
            }
            out.push(Group {
                name,
                runtime,
                k8s_namespace: k8s.into_iter().next().unwrap_or_default(),
                namespaces,
                policy,
                instances: list,
            });
        }
        Ok(out)
    }

    fn check_instance(&self, i: &FallbackInstance) -> Result<(), RenderError> {
        if self.config.runtime(i.runtime).is_none() {
            return Err(RenderError::NotAFallback {
                instance: format!("{}/{}", i.key.namespace, i.key.name),
                runtime: i.runtime.slug().to_owned(),
            });
        }
        check_label_value(&i.what(), &i.key.name)?;
        check_label_value(&i.what(), &i.connector)?;
        if i.directions.is_empty() {
            return Err(invalid(i.what(), "declares no direction"));
        }
        if i.runtime == RuntimeKind::Debezium && i.directions != [Direction::Source] {
            return Err(invalid(i.what(), "Debezium Server runs sources only"));
        }
        if i.files.is_empty() {
            return Err(invalid(i.what(), "has no rendered files"));
        }
        let prefix = format!("{}.", i.key.name);
        let mut size = 0;
        for (name, text) in &i.files {
            let rest = name.strip_prefix(&prefix).unwrap_or("");
            if rest.is_empty()
                || name.len() > 253
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
            {
                return Err(invalid(
                    i.what(),
                    format!("file `{name}` must be `{prefix}<name>` of [-._a-zA-Z0-9]"),
                ));
            }
            size += name.len() + text.len();
        }
        if size > CONFIGMAP_MAX_BYTES {
            return Err(invalid(i.what(), "its files exceed a ConfigMap's 1 MiB"));
        }
        // Every reference parses (and so cannot leave its namespace).
        self.secret_mounts(&i.secrets)?;
        Ok(())
    }

    // Shared pieces the runtime modules build with.

    pub(crate) fn runtime_config(&self, kind: RuntimeKind) -> &RuntimeConfig {
        match kind {
            RuntimeKind::Debezium => &self.config.fallback.debezium,
            RuntimeKind::Iggy => &self.config.fallback.iggy,
            _ => &self.config.fallback.camel,
        }
    }

    /// The pod spec every fallback workload shares: no token, non-root,
    /// read-only root, all capabilities dropped, `tmp` as the only writable
    /// scratch, secrets mounted read-only.
    pub(crate) fn pod_spec(
        &self,
        kind: RuntimeKind,
        container: &str,
        env: Vec<Value>,
        mut volumes: Vec<Value>,
        mut mounts: Vec<Value>,
        secrets: &[SecretRef],
    ) -> Result<Value, RenderError> {
        let rt = self.runtime_config(kind);
        let sm = self.secret_mounts(secrets)?;
        volumes.extend(sm.volumes);
        mounts.extend(sm.volume_mounts);
        volumes.push(json!({ "name": "tmp", "emptyDir": { "sizeLimit": "256Mi" } }));
        mounts.push(json!({ "name": "tmp", "mountPath": "/tmp", "readOnly": false }));
        Ok(json!({
            "serviceAccountName": self.config.fallback.service_account,
            "automountServiceAccountToken": false,
            "enableServiceLinks": false,
            "securityContext": {
                "runAsNonRoot": true,
                "runAsUser": rt.uid,
                "runAsGroup": rt.uid,
                "fsGroup": rt.uid,
                "seccompProfile": { "type": "RuntimeDefault" }
            },
            "containers": [{
                "name": container,
                "image": rt.image.reference(),
                "imagePullPolicy": "IfNotPresent",
                "env": env,
                "ports": [{ "name": "metrics", "containerPort": rt.metrics_port, "protocol": "TCP" }],
                "resources": {
                    "requests": { "cpu": rt.resources.cpu_request, "memory": rt.resources.memory_request },
                    "limits": { "memory": rt.resources.memory_limit }
                },
                "securityContext": {
                    "allowPrivilegeEscalation": false,
                    "readOnlyRootFilesystem": true,
                    "capabilities": { "drop": ["ALL"] }
                },
                "volumeMounts": mounts
            }],
            "volumes": volumes
        }))
    }

    /// The group's (or instance's) NetworkPolicy: Task 25's
    /// [`render_network_policy`] plus a rule to the Fabric's pods (R26.5).
    pub(crate) fn network_policy(
        &self,
        policy: &EgressPolicy,
        k8s_namespace: &str,
        name: &str,
        selector: &BTreeMap<String, String>,
        object_labels: &BTreeMap<String, String>,
    ) -> Value {
        let mut np = render_network_policy(policy, k8s_namespace, name, selector);
        let fabric = &self.config.fabric;
        let ports: Vec<Value> = fabric
            .ports
            .iter()
            .map(|p| json!({ "protocol": "TCP", "port": p }))
            .collect();
        if let Some(egress) = np["spec"]["egress"].as_array_mut() {
            egress.push(json!({
                "to": [{
                    "namespaceSelector": { "matchLabels": { "kubernetes.io/metadata.name": fabric.k8s_namespace } },
                    "podSelector": { "matchLabels": fabric.pod_labels }
                }],
                "ports": ports
            }));
        }
        if let Some(l) = np["metadata"]["labels"].as_object_mut() {
            for (k, v) in object_labels {
                l.insert(k.clone(), Value::String(v.clone()));
            }
        }
        np
    }

    /// One instance's ConfigMap of rendered files.
    pub(crate) fn config_map(
        &self,
        name: &str,
        k8s_namespace: &str,
        instance: &FallbackInstance,
        group: Option<&str>,
    ) -> Value {
        let mut labels = instance.labels();
        labels.insert(labels::MANAGED_BY.to_owned(), "loams-flow".to_owned());
        if let Some(g) = group {
            labels.insert(labels::GROUP.to_owned(), g.to_owned());
        }
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "namespace": k8s_namespace, "labels": labels },
            "data": instance.files
        })
    }
}

/// A projected volume of the group's per-instance ConfigMaps for one Loams
/// namespace, and its mount at `<dir>/<ns>`.
pub(crate) fn projected_config(
    volume: &str,
    dir: &str,
    ns: u64,
    maps: &[(String, &FallbackInstance)],
) -> (Value, Value) {
    let sources: Vec<Value> = maps
        .iter()
        .filter(|(_, i)| i.key.namespace == ns)
        .map(|(name, i)| {
            let items: Vec<Value> = i
                .files
                .keys()
                .map(|k| json!({ "key": k, "path": k }))
                .collect();
            json!({ "configMap": { "name": name, "items": items } })
        })
        .collect();
    (
        json!({ "name": volume, "projected": { "defaultMode": 0o444, "sources": sources } }),
        json!({ "name": volume, "mountPath": format!("{dir}/{ns}"), "readOnly": true }),
    )
}

/// A stable hash of everything a pod reads from ConfigMaps, so a changed
/// file rolls the pod.
pub(crate) fn config_hash<'a>(files: impl IntoIterator<Item = (&'a String, &'a String)>) -> String {
    let mut h = Fnv::new();
    for (k, v) in files {
        h.write(k.as_bytes());
        h.write(&[0]);
        h.write(v.as_bytes());
        h.write(&[0]);
    }
    format!("{:016x}", h.0)
}

/// FNV-1a 64: a stable, dependency-free hash for names and annotations (not
/// a security boundary).
pub(crate) struct Fnv(pub u64);

impl Fnv {
    pub fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    pub fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
    }
}

/// `<prefix>-<ns>-<instance>` as a DNS-1123 label of at most `max`
/// characters. An instance name that does not survive lower-casing (or a
/// name too long) gets a hash of the original instead, so two instances never
/// share an object (R26.4).
pub(crate) fn object_name(prefix: &str, ns: u64, instance: &str, max: usize) -> String {
    let dns: String = instance
        .chars()
        .map(|c| match c {
            'a'..='z' | '0'..='9' | '-' => c,
            'A'..='Z' => c.to_ascii_lowercase(),
            _ => '-',
        })
        .collect();
    let base = format!("{prefix}-{ns}-{dns}");
    if dns == instance && base.len() <= max {
        return base;
    }
    let mut h = Fnv::new();
    h.write(instance.as_bytes());
    let suffix = format!("{:08x}", h.0 as u32);
    let keep = max.saturating_sub(suffix.len() + 1).min(base.len());
    let head = base[..keep].trim_end_matches('-');
    format!("{head}-{suffix}")
}

fn check_dns_label(what: &str, s: &str) -> Result<(), RenderError> {
    let ok = !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !s.starts_with('-')
        && !s.ends_with('-');
    if ok {
        Ok(())
    } else {
        Err(invalid(what, format!("`{s}` is not a DNS-1123 label")))
    }
}

/// A Kubernetes label value: at most 63 of `[-._a-zA-Z0-9]`, alphanumeric at
/// both ends.
fn check_label_value(what: &str, s: &str) -> Result<(), RenderError> {
    let alnum = |c: Option<char>| c.is_some_and(|c| c.is_ascii_alphanumeric());
    let ok = !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
        && alnum(s.chars().next())
        && alnum(s.chars().last());
    if ok {
        Ok(())
    } else {
        Err(invalid(
            what,
            format!("`{s}` is not a Kubernetes label value"),
        ))
    }
}

fn sort_key(v: &Value) -> (u8, String, String) {
    let rank = match v["kind"].as_str().unwrap_or("") {
        "ConfigMap" => 0,
        "Service" => 1,
        "NetworkPolicy" => 2,
        "Deployment" => 3,
        "StatefulSet" => 4,
        _ => 5,
    };
    (
        rank,
        v["metadata"]["namespace"].as_str().unwrap_or("").to_owned(),
        v["metadata"]["name"].as_str().unwrap_or("").to_owned(),
    )
}

/// Objects as a multi-document YAML stream (the golden format).
pub fn to_yaml(objects: &[Value]) -> String {
    let mut out = String::new();
    for o in objects {
        out.push_str("---\n");
        out.push_str(&serde_norway::to_string(o).unwrap_or_default());
    }
    out
}

/// A multi-document YAML stream (`helm template`'s output, or [`to_yaml`]'s)
/// as objects; empty documents are skipped.
pub fn from_yaml_stream(text: &str) -> Result<Vec<Value>, RenderError> {
    let mut out = Vec::new();
    for doc in serde_norway::Deserializer::from_str(text) {
        let v = Value::deserialize(doc).map_err(|e| invalid("YAML stream", e.to_string()))?;
        if !v.is_null() {
            out.push(v);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_names() {
        assert_eq!(
            object_name("loams-connect", 7, "orders", 63),
            "loams-connect-7-orders"
        );
        let a = object_name("loams-connect", 7, "Orders", 63);
        let b = object_name("loams-connect", 7, "orders", 63);
        assert_ne!(a, b);
        assert!(a.starts_with("loams-connect-7-orders-"), "{a}");
        let long = object_name("loams-debezium", 7, &"x".repeat(63), 52);
        assert!(long.len() <= 52, "{long}");
        assert!(check_dns_label("n", &long).is_ok());
        let dotted = object_name("p", 1, "a.b_c", 63);
        assert!(check_dns_label("n", &dotted).is_ok(), "{dotted}");
    }

    #[test]
    fn label_values() {
        assert!(check_label_value("x", "orders.v2_eu-1").is_ok());
        for bad in ["", "-a", "a-", "a/b", &"a".repeat(64)] {
            assert!(check_label_value("x", bad).is_err(), "{bad}");
        }
    }
}
