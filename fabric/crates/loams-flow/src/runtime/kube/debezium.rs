//! Debezium Server fallbacks (Oracle CDC, and the opt-in CDC fallbacks of
//! OR-4): one StatefulSet per CDC instance, with a PVC for its offsets and
//! schema history (Q350), its properties in a ConfigMap, a headless Service
//! and its own NetworkPolicy.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::{FallbackInstance, Group, KubeRuntime, RenderError, config_hash, labels, object_name};
use crate::manifest::RuntimeKind;

/// Where the instance's files are mounted, each without its `<instance>.`
/// prefix (`application.properties`).
pub const CONFIG_DIR: &str = "/debezium/config";
/// The PVC's mount: offsets and schema history.
pub const DATA_DIR: &str = "/debezium/data";
/// The object name prefix (`loams-debezium-<ns>-<instance>`).
pub const PREFIX: &str = "loams-debezium";
/// A StatefulSet's name stays short enough for its pods' and revisions'
/// labels.
const MAX_NAME: usize = 52;

pub(crate) fn render(
    rt: &KubeRuntime,
    g: &Group<'_>,
    i: &FallbackInstance,
    out: &mut Vec<Value>,
) -> Result<(), RenderError> {
    let ns = &g.k8s_namespace;
    let name = object_name(PREFIX, i.key.namespace, &i.key.name, MAX_NAME);
    let cfg = rt.runtime_config(RuntimeKind::Debezium);
    let storage = cfg.storage.clone().unwrap_or(super::Storage {
        size: "10Gi".into(),
        storage_class: None,
    });

    let mut obj_labels = i.labels();
    obj_labels.insert(labels::MANAGED_BY.to_owned(), "loams-flow".to_owned());
    let selector = BTreeMap::from([
        (labels::NAMESPACE.to_owned(), i.key.namespace.to_string()),
        (labels::INSTANCE.to_owned(), i.key.name.clone()),
        (labels::RUNTIME.to_owned(), i.runtime.slug().to_owned()),
    ]);

    out.push(rt.config_map(&name, ns, i, None));
    out.push(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": name, "namespace": ns, "labels": obj_labels },
        "spec": {
            "clusterIP": "None",
            "selector": selector,
            "ports": [{ "name": "metrics", "port": cfg.metrics_port, "targetPort": "metrics", "protocol": "TCP" }]
        }
    }));

    let prefix = format!("{}.", i.key.name);
    let items: Vec<Value> = i
        .files
        .keys()
        .map(|k| json!({ "key": k, "path": k.strip_prefix(&prefix).unwrap_or(k) }))
        .collect();
    let volumes = vec![json!({
        "name": "config",
        "configMap": { "name": name, "items": items, "defaultMode": 0o444 }
    })];
    let mounts = vec![
        json!({ "name": "config", "mountPath": CONFIG_DIR, "readOnly": true }),
        json!({ "name": "data", "mountPath": DATA_DIR, "readOnly": false }),
    ];
    let pod = rt.pod_spec(
        RuntimeKind::Debezium,
        "debezium-server",
        Vec::new(),
        volumes,
        mounts,
        &i.secrets,
    )?;
    let mut claim = json!({
        "metadata": { "name": "data", "labels": obj_labels },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": { "requests": { "storage": storage.size } }
        }
    });
    if let Some(class) = &storage.storage_class {
        claim["spec"]["storageClassName"] = json!(class);
    }
    out.push(json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": { "name": name, "namespace": ns, "labels": obj_labels },
        "spec": {
            "replicas": 1,
            "serviceName": name,
            "selector": { "matchLabels": selector },
            // Offsets outlive a pod and a scale-down; deleting the instance
            // deletes its PVC explicitly (R26.8).
            "persistentVolumeClaimRetentionPolicy": { "whenDeleted": "Retain", "whenScaled": "Retain" },
            "template": {
                "metadata": {
                    "labels": obj_labels,
                    "annotations": { "loams.dev/config-hash": config_hash(i.files.iter()) }
                },
                "spec": pod
            },
            "volumeClaimTemplates": [claim]
        }
    }));
    out.push(rt.network_policy(
        &g.policy,
        ns,
        &format!("{name}-egress"),
        &selector,
        &obj_labels,
    ));
    Ok(())
}
