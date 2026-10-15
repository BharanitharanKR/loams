//! `loams-connect` (Camel) fallbacks: one Deployment per namespace group,
//! each instance's routes in a ConfigMap of its own, projected into
//! `/etc/loams-connect/routes/<namespace>/<instance>.<direction>.yaml` (the
//! Fallback runtime contract).

use serde_json::{Value, json};

use super::{Group, KubeRuntime, RenderError, config_hash, labels, object_name, projected_config};
use crate::manifest::RuntimeKind;

/// Where the routes are mounted.
pub const ROUTES_DIR: &str = "/etc/loams-connect/routes";
/// The Deployment's name prefix (`loams-connect-<group>`).
pub const PREFIX: &str = "loams-connect";

pub(crate) fn render(
    rt: &KubeRuntime,
    g: &Group<'_>,
    out: &mut Vec<Value>,
) -> Result<(), RenderError> {
    group_workload(
        rt,
        g,
        out,
        PREFIX,
        "loams-connect",
        ROUTES_DIR,
        "routes",
        vec![
            // Camel's own masking of secret-looking properties in its logs
            // (R25.7); Task 22's renderer also sets `camel.main.logMask`.
            json!({ "name": "CAMEL_MAIN_LOGMASK", "value": "true" }),
            json!({ "name": "LOAMS_CONNECT_ROUTES_DIR", "value": ROUTES_DIR }),
        ],
    )
}

/// The per-group shape Camel and the Iggy connectors runtime share: a
/// ConfigMap per instance, one Deployment for the group, and the group's
/// NetworkPolicy.
#[allow(clippy::too_many_arguments)]
pub(crate) fn group_workload(
    rt: &KubeRuntime,
    g: &Group<'_>,
    out: &mut Vec<Value>,
    prefix: &str,
    container: &str,
    dir: &str,
    volume_prefix: &str,
    env: Vec<Value>,
) -> Result<(), RenderError> {
    debug_assert!(matches!(g.runtime, RuntimeKind::Camel | RuntimeKind::Iggy));
    let maps: Vec<(String, &super::FallbackInstance)> = g
        .instances
        .iter()
        .map(|i| (object_name(prefix, i.key.namespace, &i.key.name, 253), *i))
        .collect();
    for (name, i) in &maps {
        out.push(rt.config_map(name, &g.k8s_namespace, i, Some(&g.name)));
    }

    let mut volumes = Vec::new();
    let mut mounts = Vec::new();
    for ns in &g.namespaces {
        let (v, m) = projected_config(&format!("{volume_prefix}-{ns}"), dir, *ns, &maps);
        volumes.push(v);
        mounts.push(m);
    }
    let pod = rt.pod_spec(g.runtime, container, env, volumes, mounts, &g.secrets())?;
    let name = format!("{prefix}-{}", g.name);
    let group_labels = g.labels();
    let mut obj_labels = group_labels.clone();
    obj_labels.insert(labels::MANAGED_BY.to_owned(), "loams-flow".to_owned());
    let hash = config_hash(g.instances.iter().flat_map(|i| i.files.iter()));
    out.push(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": name, "namespace": g.k8s_namespace, "labels": obj_labels },
        "spec": {
            "replicas": 1,
            // One writer per instance: the old pod stops before the new one
            // starts (OR-16).
            "strategy": { "type": "Recreate" },
            "selector": { "matchLabels": g.selector() },
            "template": {
                "metadata": {
                    "labels": group_labels,
                    "annotations": { "loams.dev/config-hash": hash }
                },
                "spec": pod
            }
        }
    }));
    out.push(rt.network_policy(
        &g.policy,
        &g.k8s_namespace,
        &format!("{name}-egress"),
        &g.selector(),
        &g.labels(),
    ));
    Ok(())
}
