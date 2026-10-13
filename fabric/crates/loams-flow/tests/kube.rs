//! CN2 Task 26: the connector fleet on Kubernetes. `KubeRuntime`'s rendered
//! fallback objects (golden), the fenced instance and split leases, the
//! `flow` Deployment's scale, and the `loams-connectors` Helm chart (golden,
//! linted when `helm` is available).
//!
//! Golden files live under `tests/golden/kube/`. `LOAMS_UPDATE_GOLDEN=1`
//! rewrites them; review the diff.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use loams_connector_sdk::testing::memory::MemoryCheckpointStore;
use loams_connector_sdk::{
    Checkpoint, CheckpointError, CheckpointStore, InstanceKey, Position, SecretRef, SplitId,
};
use loams_flow::egress::{ClusterNetworks, EgressPolicy, IpNet};
use loams_flow::manifest::{RuntimeKind, repository_root};
use loams_flow::runtime::kube::lease::{
    FencedCheckpointStore, LeaseClient, LeaseError, LeaseStore, MemoryLeaseStore,
    instance_lease_key, split_checkpoint_key,
};
use loams_flow::runtime::kube::native::{FlowScale, NativeScheduler, desired_replicas};
use loams_flow::runtime::kube::{
    FallbackInstance, FleetConfig, KubeRuntime, RenderError, labels, to_yaml,
};
use loams_flow::validate::Direction;
use serde_json::Value;

// ---------------------------------------------------------------- fixtures

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn golden_dir() -> PathBuf {
    crate_dir().join("tests/golden/kube")
}

fn chart_dir() -> PathBuf {
    repository_root().join("deploy/helm/loams-connectors")
}

/// The fleet configuration the chart renders with `ci/golden-values.yaml`
/// (`helm_lint_and_template_golden` checks that the two agree).
fn fleet() -> FleetConfig {
    let text = std::fs::read_to_string(crate_dir().join("tests/fixtures/kube/fleet.yaml")).unwrap();
    FleetConfig::from_yaml(&text).unwrap()
}

fn net(s: &str) -> IpNet {
    s.parse().unwrap()
}

fn files(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn refs(names: &[&str]) -> Vec<SecretRef> {
    names.iter().map(|n| SecretRef::new(*n)).collect()
}

/// Two Camel instances in namespace 7 (Oracle batch, a JMS sink), a Debezium
/// Oracle CDC instance in namespace 7, and two Iggy-plugin instances in
/// namespace 9. File contents stand in for Task 22's renderers: they name
/// secrets only by their mounted path.
fn instances() -> Vec<FallbackInstance> {
    vec![
        FallbackInstance {
            key: InstanceKey::new(7, "orders-oracle"),
            connector: "oracle".into(),
            runtime: RuntimeKind::Camel,
            directions: vec![Direction::Source],
            group: None,
            files: files(&[(
                "orders-oracle.source.yaml",
                "- route:\n    id: loams-7-orders-oracle-source\n    from:\n      uri: sql:select * from orders\n      parameters:\n        dataSource: '#oracle'\n        passwordFile: /var/run/loams/secrets/oracle-creds/password\n",
            )]),
            secrets: refs(&["oracle-creds/password", "oracle-creds/user"]),
        },
        FallbackInstance {
            key: InstanceKey::new(7, "billing-jms"),
            connector: "jms".into(),
            runtime: RuntimeKind::Camel,
            directions: vec![Direction::Sink],
            group: None,
            files: files(&[(
                "billing-jms.sink.yaml",
                "- route:\n    id: loams-7-billing-jms-sink\n    from:\n      uri: iggy:loams-7/billing\n    steps:\n      - to: jms:queue:billing\n",
            )]),
            secrets: refs(&["jms-broker"]),
        },
        FallbackInstance {
            key: InstanceKey::new(7, "erp-cdc"),
            connector: "debezium-oracle".into(),
            runtime: RuntimeKind::Debezium,
            directions: vec![Direction::Source],
            group: None,
            files: files(&[(
                "erp-cdc.application.properties",
                "debezium.source.connector.class=io.debezium.connector.oracle.OracleConnector\ndebezium.source.database.password=${file:/var/run/loams/secrets/erp-oracle/password}\ndebezium.source.offset.storage.file.filename=/debezium/data/offsets.dat\n",
            )]),
            secrets: refs(&["erp-oracle/password"]),
        },
        FallbackInstance {
            key: InstanceKey::new(9, "es-legacy"),
            connector: "elasticsearch".into(),
            runtime: RuntimeKind::Iggy,
            directions: vec![Direction::Sink],
            group: None,
            files: files(&[(
                "es-legacy.sink.toml",
                "[plugin]\nname = \"elasticsearch_sink\"\n[config]\nurl = \"https://es.example.com:9200\"\npassword_file = \"/var/run/loams/secrets/es/password\"\n",
            )]),
            secrets: refs(&["es/password"]),
        },
        FallbackInstance {
            key: InstanceKey::new(9, "ch-legacy"),
            connector: "clickhouse".into(),
            runtime: RuntimeKind::Iggy,
            directions: vec![Direction::Sink],
            group: None,
            files: files(&[(
                "ch-legacy.sink.toml",
                "[plugin]\nname = \"clickhouse_sink\"\n[config]\nurl = \"https://ch.example.com:8443\"\n",
            )]),
            secrets: vec![],
        },
    ]
}

/// Namespace 7's admin allowlisted one partner network; namespace 9 keeps
/// the cluster default.
fn policies(fleet: &FleetConfig) -> BTreeMap<u64, EgressPolicy> {
    let mut p7 = EgressPolicy::for_cluster(&fleet.cluster);
    p7.allow_cidrs.push(net("10.20.30.0/24"));
    BTreeMap::from([(7, p7)])
}

fn check_golden(name: &str, actual: &str) {
    let path = golden_dir().join(name);
    if std::env::var_os("LOAMS_UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{}: {e} (LOAMS_UPDATE_GOLDEN=1 writes it)", path.display()));
    assert!(
        expected == actual,
        "{name} differs from its golden file (LOAMS_UPDATE_GOLDEN=1 rewrites it)\n--- golden\n{expected}\n--- rendered\n{actual}"
    );
}

fn of_kind<'a>(objects: &'a [Value], kind: &str) -> Vec<&'a Value> {
    objects.iter().filter(|o| o["kind"] == kind).collect()
}

fn label<'a>(obj: &'a Value, key: &str) -> Option<&'a str> {
    obj["metadata"]["labels"][key].as_str()
}

// ------------------------------------------------------------- rendering

#[test]
fn render_golden_per_runtime() {
    let fleet = fleet();
    let runtime = KubeRuntime::new(fleet.clone()).unwrap();
    let policies = policies(&fleet);
    let all = instances();

    for (kind, file) in [
        (RuntimeKind::Camel, "connect.yaml"),
        (RuntimeKind::Debezium, "debezium.yaml"),
        (RuntimeKind::Iggy, "iggy.yaml"),
    ] {
        let subset: Vec<FallbackInstance> =
            all.iter().filter(|i| i.runtime == kind).cloned().collect();
        let rendered = runtime.render(&subset, &policies).unwrap();
        check_golden(file, &to_yaml(&rendered));

        // Deterministic: the same input renders the same bytes, whatever the
        // order the instances arrive in.
        let mut reversed = subset.clone();
        reversed.reverse();
        assert_eq!(rendered, runtime.render(&reversed, &policies).unwrap());
    }

    // The native side: the flow Deployment's scale for the native fleet.
    let scale = FlowScale::from(&fleet);
    let budgets = vec![64 << 20; 40];
    let replicas = desired_replicas(&scale, &budgets);
    check_golden(
        "native.yaml",
        &to_yaml(&[runtime.render_flow_scale(replicas)]),
    );
}

#[test]
fn rendered_objects_carry_the_fleet_labels() {
    let fleet = fleet();
    let rendered = KubeRuntime::new(fleet.clone())
        .unwrap()
        .render(&instances(), &policies(&fleet))
        .unwrap();
    for obj in &rendered {
        let kind = obj["kind"].as_str().unwrap();
        assert_eq!(label(obj, labels::FALLBACK), Some("true"), "{kind}");
        let runtime = label(obj, labels::RUNTIME).unwrap();
        assert!(["camel", "debezium", "iggy"].contains(&runtime), "{kind}");
        assert!(
            label(obj, labels::NAMESPACE).is_some(),
            "{kind}: namespace label"
        );
        // Per-instance objects name their instance and connector; the
        // per-group workloads name their group (R26.4).
        let per_instance = kind == "ConfigMap" || runtime == "debezium";
        if per_instance {
            assert!(label(obj, labels::INSTANCE).is_some(), "{kind}");
            assert!(label(obj, labels::CONNECTOR).is_some(), "{kind}");
        } else {
            assert!(label(obj, labels::GROUP).is_some(), "{kind}");
        }
    }
    // Every Debezium instance is its own StatefulSet with a PVC (Q350); each
    // Camel and Iggy namespace group is one Deployment.
    let sts = of_kind(&rendered, "StatefulSet");
    assert_eq!(sts.len(), 1);
    assert_eq!(
        sts[0]["spec"]["volumeClaimTemplates"][0]["spec"]["accessModes"][0],
        "ReadWriteOnce"
    );
    let deployments = of_kind(&rendered, "Deployment");
    assert_eq!(
        deployments.len(),
        2,
        "one Camel group (ns 7), one Iggy group (ns 9)"
    );
    for d in deployments {
        // A fallback group is single-writer: the old pod stops before the new
        // one starts.
        assert_eq!(d["spec"]["strategy"]["type"], "Recreate");
        assert_eq!(d["spec"]["replicas"], 1);
    }
}

/// R25.9's requirements on the fallback pods, and Task 25's NetworkPolicy
/// per pod selector.
#[test]
fn fallback_pods_meet_the_security_requirements() {
    let fleet = fleet();
    let rendered = KubeRuntime::new(fleet.clone())
        .unwrap()
        .render(&instances(), &policies(&fleet))
        .unwrap();
    let workloads: Vec<&Value> = rendered
        .iter()
        .filter(|o| o["kind"] == "Deployment" || o["kind"] == "StatefulSet")
        .collect();
    assert_eq!(workloads.len(), 3);
    let policies: Vec<&Value> = of_kind(&rendered, "NetworkPolicy");
    for w in workloads {
        let ns = w["metadata"]["namespace"].as_str().unwrap();
        let pod = &w["spec"]["template"];
        let spec = &pod["spec"];
        assert_eq!(spec["automountServiceAccountToken"], false);
        assert_eq!(
            spec["serviceAccountName"],
            fleet.fallback.service_account.as_str()
        );
        assert_eq!(spec["enableServiceLinks"], false);
        assert_eq!(spec["securityContext"]["runAsNonRoot"], true);
        for c in spec["containers"].as_array().unwrap() {
            let image = c["image"].as_str().unwrap();
            assert!(image.contains("@sha256:"), "{image} is pinned by digest");
            let sc = &c["securityContext"];
            assert_eq!(sc["allowPrivilegeEscalation"], false);
            assert_eq!(sc["readOnlyRootFilesystem"], true);
            assert_eq!(sc["capabilities"]["drop"][0], "ALL");
            for m in c["volumeMounts"].as_array().unwrap() {
                assert_eq!(
                    m["readOnly"],
                    m["name"] != "tmp" && m["name"] != "data",
                    "{m}"
                );
            }
        }
        // Only the referenced keys, read-only, mode 0400.
        for v in spec["volumes"].as_array().unwrap() {
            if let Some(secret) = v.get("secret") {
                assert_eq!(secret["defaultMode"], 0o400);
                assert!(!secret["items"].as_array().unwrap().is_empty());
            }
        }
        // A NetworkPolicy in the same namespace selects these pods.
        let pod_labels = pod["metadata"]["labels"].as_object().unwrap();
        let selected = policies.iter().any(|p| {
            p["metadata"]["namespace"] == ns
                && p["spec"]["podSelector"]["matchLabels"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .all(|(k, v)| pod_labels.get(k) == Some(v))
        });
        assert!(selected, "{} has no NetworkPolicy", w["metadata"]["name"]);
    }
    // The NetworkPolicy carries the namespace's own policy: namespace 7's
    // allowlisted CIDR re-opened, the cluster's pod CIDR denied, and the
    // Fabric's pods reachable (R26.5).
    let ns7 = policies
        .iter()
        .find(|p| p["metadata"]["name"] == "loams-connect-ns-7-egress")
        .unwrap();
    let text = ns7.to_string();
    assert!(text.contains("10.20.30.0/24"));
    assert!(text.contains("10.244.0.0/16"));
    assert!(text.contains("loams-fabric"), "{text}");
}

#[test]
fn no_secret_in_rendered_kube_objects_task_26() {
    // The renderer sees references only; a config file that carries a
    // resolved secret's bytes is refused, and the Scrubber-driven check finds
    // nothing in what it renders from references.
    let fleet = fleet();
    let runtime = KubeRuntime::new(fleet.clone()).unwrap();
    let secret = "hunter2-very-secret-value";
    let scrubber = loams_connector_sdk::Scrubber::new();
    scrubber.add(&loams_connector_sdk::Secret::new(secret.as_bytes()));
    let rendered = runtime.render(&instances(), &policies(&fleet)).unwrap();
    for obj in &rendered {
        assert!(!scrubber.contains(&obj.to_string()));
    }
    let mut leaky = instances();
    leaky[0].files.insert(
        "orders-oracle.source.yaml".into(),
        format!("password: {secret}\n"),
    );
    let err = runtime
        .render_checked(&leaky, &policies(&fleet), &scrubber)
        .unwrap_err();
    assert!(matches!(err, RenderError::SecretInConfig { .. }), "{err}");
    assert!(!err.to_string().contains(secret));
}

#[test]
fn render_refuses_what_it_cannot_run_safely() {
    let fleet = fleet();
    let runtime = KubeRuntime::new(fleet.clone()).unwrap();
    let p = policies(&fleet);
    let base = instances();

    let refuse = |mutate: &dyn Fn(&mut Vec<FallbackInstance>)| {
        let mut list = base.clone();
        mutate(&mut list);
        runtime.render(&list, &p).unwrap_err()
    };

    // Native instances run inside the flow pods, never as rendered objects.
    let e = refuse(&|l| l[0].runtime = RuntimeKind::Native);
    assert!(matches!(e, RenderError::NotAFallback { .. }), "{e}");
    // Debezium Server is a source only.
    let e = refuse(&|l| l[2].directions = vec![Direction::Sink]);
    assert!(matches!(e, RenderError::Invalid { .. }), "{e}");
    // A file must be the instance's own (the projected route directory is
    // shared by the group).
    let e = refuse(&|l| {
        l[0].files = files(&[("billing-jms.sink.yaml", "x")]);
    });
    assert!(matches!(e, RenderError::Invalid { .. }), "{e}");
    // Names must be Kubernetes label values.
    let e = refuse(&|l| l[0].key.name = "Orders_".into());
    assert!(matches!(e, RenderError::Invalid { .. }), "{e}");
    // Two namespaces with different egress policies cannot share a group.
    let e = refuse(&|l| {
        l[3].group = Some("shared".into());
        l[3].runtime = RuntimeKind::Camel;
        l[0].group = Some("shared".into());
    });
    assert!(matches!(e, RenderError::Invalid { .. }), "{e}");
    // A ConfigMap is at most 1 MiB.
    let e = refuse(&|l| {
        l[0].files
            .insert("orders-oracle.source.yaml".into(), "x".repeat(1 << 20));
    });
    assert!(matches!(e, RenderError::Invalid { .. }), "{e}");
    // The same instance twice.
    let e = refuse(&|l| {
        let dup = l[0].clone();
        l.push(dup);
    });
    assert!(matches!(e, RenderError::Invalid { .. }), "{e}");

    // An image without a digest is refused when the fleet is configured.
    let mut tagged = fleet.clone();
    tagged.fallback.debezium.image.digest = "3.7.0.Final".into();
    assert!(KubeRuntime::new(tagged).is_err());
}

// ---------------------------------------------------------------- leases

const TTL: u64 = 10_000;

/// Review Focus 12: two `flow` processes contend for one instance; at no
/// instant do both run it, and when the holder dies the other takes over
/// with a newer epoch.
#[tokio::test]
async fn two_flow_processes_one_instance_runner() {
    let store: Arc<dyn LeaseStore> = Arc::new(MemoryLeaseStore::new());
    let instance = InstanceKey::new(7, "orders");
    let mut a = NativeScheduler::new(LeaseClient::new(store.clone(), "flow-a", TTL));
    let mut b = NativeScheduler::new(LeaseClient::new(store.clone(), "flow-b", TTL));
    let list = vec![instance.clone()];
    let no_splits = BTreeMap::new();

    let mut runner_epochs = Vec::new();
    let mut now = 0;
    // Both alive: interleaved ticks, every second, for a minute.
    for step in 0..60 {
        let (first, second) = if step % 2 == 0 {
            (&mut a, &mut b)
        } else {
            (&mut b, &mut a)
        };
        first.tick(&list, &no_splits, now).await.unwrap();
        second.tick(&list, &no_splits, now).await.unwrap();
        let runners: Vec<u64> = [&a, &b]
            .iter()
            .filter_map(|s| s.runs(&instance, now))
            .collect();
        assert_eq!(
            runners.len(),
            1,
            "exactly one runner at t={now}: {runners:?}"
        );
        runner_epochs.push(runners[0]);
        now += 1_000;
    }
    assert!(
        runner_epochs.iter().all(|e| *e == runner_epochs[0]),
        "no flapping"
    );
    let first_epoch = runner_epochs[0];
    let holder_is_a = a.runs(&instance, now - 1_000).is_some();

    // The holder dies (stops ticking). Until its lease expires nobody runs
    // the instance a second time; after, the survivor does, with epoch + 1.
    let (dead, alive) = if holder_is_a {
        (&a, &mut b)
    } else {
        (&b, &mut a)
    };
    let died_at = now;
    while now < died_at + TTL + 2_000 {
        alive.tick(&list, &no_splits, now).await.unwrap();
        let both = dead.runs(&instance, now).is_some() && alive.runs(&instance, now).is_some();
        assert!(!both, "two runners at t={now}");
        now += 1_000;
    }
    let epoch = alive
        .runs(&instance, now - 1_000)
        .expect("the survivor took over");
    assert_eq!(epoch, first_epoch + 1);
    assert!(dead.runs(&instance, now).is_none());
}

/// A stale holder's renewal is refused, and so is its checkpoint write, even
/// before its successor has written one (the lease check runs first).
#[tokio::test]
async fn stale_lease_holder_fenced() {
    let leases: Arc<dyn LeaseStore> = Arc::new(MemoryLeaseStore::new());
    let instance = InstanceKey::new(7, "orders");
    let key = instance_lease_key(&instance);
    let old = LeaseClient::new(leases.clone(), "flow-a", TTL);
    let new = LeaseClient::new(leases.clone(), "flow-b", TTL);

    let stale = old.acquire(&key, 0).await.unwrap().expect("free lease");
    assert_eq!(stale.epoch, 1);
    // flow-b cannot take an unexpired lease.
    assert!(new.acquire(&key, 5_000).await.unwrap().is_none());
    // flow-a stalls past its expiry; flow-b takes over.
    let fresh = new
        .acquire(&key, TTL + 1)
        .await
        .unwrap()
        .expect("expired lease");
    assert_eq!(fresh.epoch, 2);

    let renewed = old.renew(&stale, TTL + 2).await;
    assert!(
        matches!(renewed, Err(LeaseError::Fenced { .. })),
        "{renewed:?}"
    );
    assert!(
        old.release(&stale).await.is_err(),
        "a stale holder cannot release"
    );

    let inner = Arc::new(MemoryCheckpointStore::new());
    let checkpoints = FencedCheckpointStore::new(inner.clone(), leases.clone());
    let mut cp = Checkpoint::new(instance.clone(), stale.epoch);
    cp.splits
        .insert(SplitId::new("p0"), Position::new(vec![1u8], "offset 1"));
    let err = checkpoints.save(&cp).await.unwrap_err();
    assert!(
        matches!(
            err,
            CheckpointError::Fenced {
                stored: 2,
                attempted: 1
            }
        ),
        "{err:?}"
    );
    assert!(inner.get(&instance).is_none(), "nothing was written");

    // The holder's own write goes through, and the stale one stays fenced
    // by the inner store's epoch check too.
    let mut cp2 = Checkpoint::new(instance.clone(), fresh.epoch);
    cp2.splits
        .insert(SplitId::new("p0"), Position::new(vec![2u8], "offset 2"));
    checkpoints.save(&cp2).await.unwrap();
    assert!(checkpoints.save(&cp).await.is_err());
    assert_eq!(checkpoints.load(&instance).await.unwrap().unwrap().epoch, 2);

    // Split checkpoints are fenced by their split lease.
    let split = SplitId::new("p3");
    let skey = split_checkpoint_key(&instance, &split);
    let s_old = old
        .acquire(
            &loams_flow::runtime::kube::lease::split_lease_key(&instance, &split),
            0,
        )
        .await
        .unwrap()
        .unwrap();
    let _s_new = new
        .acquire(
            &loams_flow::runtime::kube::lease::split_lease_key(&instance, &split),
            TTL + 1,
        )
        .await
        .unwrap()
        .unwrap();
    let mut scp = Checkpoint::new(skey, s_old.epoch);
    scp.splits
        .insert(split, Position::new(vec![3u8], "offset 3"));
    assert!(matches!(
        checkpoints.save(&scp).await,
        Err(CheckpointError::Fenced { .. })
    ));
}

/// OR-16: a native source's splits spread across the flow pods; when a pod
/// is lost its splits move to the survivors once its leases expire, none is
/// held twice, and each moved split's epoch goes up.
#[tokio::test]
async fn splits_rebalance_on_pod_loss() {
    let store: Arc<dyn LeaseStore> = Arc::new(MemoryLeaseStore::new());
    let instance = InstanceKey::new(7, "clicks");
    let instances = vec![instance.clone()];
    let splits: BTreeMap<InstanceKey, Vec<SplitId>> = BTreeMap::from([(
        instance.clone(),
        (0..9).map(|i| SplitId::new(format!("p{i}"))).collect(),
    )]);
    let mut pods: Vec<NativeScheduler> = ["flow-0", "flow-1", "flow-2"]
        .iter()
        .map(|h| NativeScheduler::new(LeaseClient::new(store.clone(), *h, TTL)))
        .collect();

    let mut now = 0;
    for _ in 0..5 {
        for p in &mut pods {
            p.tick(&instances, &splits, now).await.unwrap();
        }
        now += 1_000;
    }
    let held = |pods: &[NativeScheduler], now: u64| -> Vec<BTreeSet<SplitId>> {
        pods.iter()
            .map(|p| p.splits_held(&instance, now).into_iter().collect())
            .collect()
    };
    let before = held(&pods, now);
    assert_eq!(
        before.iter().map(BTreeSet::len).collect::<Vec<_>>(),
        [3, 3, 3]
    );
    assert_disjoint_and_complete(&before, 9);
    let lost: BTreeSet<SplitId> = before[2].clone();
    let lost_epochs: BTreeMap<SplitId, u64> = lost
        .iter()
        .map(|s| (s.clone(), pods[2].split_epoch(&instance, s).unwrap()))
        .collect();

    // flow-2 is lost.
    let survivors = &mut pods[..2];
    let lost_at = now;
    while now < lost_at + TTL + 5_000 {
        for p in survivors.iter_mut() {
            p.tick(&instances, &splits, now).await.unwrap();
        }
        // Never two holders of one split while the lost pod's leases live.
        let mut all = held(survivors, now);
        if now < lost_at + TTL - 1_000 {
            all.push(lost.clone());
        }
        assert_disjoint(&all);
        now += 1_000;
    }
    let after = held(survivors, now);
    let mut sizes: Vec<usize> = after.iter().map(BTreeSet::len).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, [4, 5]);
    assert_disjoint_and_complete(&after, 9);
    for s in &lost {
        let p = survivors
            .iter()
            .find(|p| p.split_epoch(&instance, s).is_some())
            .unwrap();
        assert_eq!(p.split_epoch(&instance, s).unwrap(), lost_epochs[s] + 1);
    }
    // The instance itself has exactly one runner among the survivors.
    assert_eq!(
        survivors
            .iter()
            .filter(|p| p.runs(&instance, now).is_some())
            .count(),
        1
    );
}

fn assert_disjoint(sets: &[BTreeSet<SplitId>]) {
    let mut seen = BTreeSet::new();
    for s in sets {
        for id in s {
            assert!(seen.insert(id.clone()), "{id} held twice");
        }
    }
}

fn assert_disjoint_and_complete(sets: &[BTreeSet<SplitId>], n: usize) {
    assert_disjoint(sets);
    assert_eq!(sets.iter().map(BTreeSet::len).sum::<usize>(), n);
}

#[test]
fn flow_replicas_follow_instances_and_budgets() {
    let scale = FlowScale {
        min_replicas: 2,
        max_replicas: 8,
        pod_memory_bytes: 1 << 30,
        max_instances_per_pod: 32,
    };
    let mib = |n: u64| n << 20;
    assert_eq!(desired_replicas(&scale, &[]), 2, "the floor");
    assert_eq!(
        desired_replicas(&scale, &vec![mib(64); 40]),
        3,
        "by memory: 2.5 GiB"
    );
    assert_eq!(
        desired_replicas(&scale, &vec![mib(1); 100]),
        4,
        "by count: 100 / 32"
    );
    assert_eq!(
        desired_replicas(&scale, &vec![mib(512); 100]),
        8,
        "the ceiling"
    );
}

// ------------------------------------------------------------ Helm chart

fn helm() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("LOAMS_HELM") {
        return Some(PathBuf::from(p));
    }
    let ok = Command::new("helm")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    ok.then(|| PathBuf::from("helm"))
}

fn run(cmd: &mut Command) -> String {
    let out = cmd.output().expect("helm runs");
    assert!(
        out.status.success(),
        "{cmd:?} failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn helm_template(helm: &Path, values: &Path) -> String {
    run(Command::new(helm)
        .args(["template", "loams-connectors"])
        .arg(chart_dir())
        .args([
            "--namespace",
            "loams-system",
            "--kube-version",
            "1.33.0",
            "-f",
        ])
        .arg(values))
}

const HELM_GOLDEN: &str = "helm-golden-values.yaml";

/// `helm lint --strict` and `helm template` against the golden file when
/// `helm` is on `PATH` (or named by `LOAMS_HELM`); the structural checks on
/// the golden objects run either way.
#[test]
fn helm_lint_and_template_golden() {
    let values = chart_dir().join("ci/golden-values.yaml");
    match helm() {
        Some(helm) => {
            run(Command::new(&helm)
                .args(["lint", "--strict"])
                .arg(chart_dir())
                .arg("-f")
                .arg(&values));
            check_golden(HELM_GOLDEN, &helm_template(&helm, &values));
            // Without image digests the chart refuses to render.
            let out = Command::new(&helm)
                .args(["template", "x"])
                .arg(chart_dir())
                .output()
                .unwrap();
            assert!(!out.status.success(), "the default values carry no digest");
            let err = String::from_utf8_lossy(&out.stderr);
            assert!(err.contains("digest"), "{err}");
        }
        None => eprintln!(
            "helm_lint_and_template_golden: helm not found (set LOAMS_HELM); \
             checked the committed golden objects only"
        ),
    }

    let text = std::fs::read_to_string(golden_dir().join(HELM_GOLDEN)).unwrap();
    let objects: Vec<Value> = loams_flow::runtime::kube::from_yaml_stream(&text).unwrap();
    check_chart_objects(&objects);
}

fn check_chart_objects(objects: &[Value]) {
    let find = |kind: &str, name: &str| -> &Value {
        objects
            .iter()
            .find(|o| o["kind"] == kind && o["metadata"]["name"] == name)
            .unwrap_or_else(|| panic!("{kind}/{name} is rendered"))
    };

    // The fleet configuration the chart hands `loams-fabric flow` is the one
    // `KubeRuntime` reads, cluster networks included (R25.3).
    let cm = find("ConfigMap", "loams-connectors-fleet");
    let fleet_yaml = cm["data"]["fleet.yaml"].as_str().unwrap();
    let chart_fleet = FleetConfig::from_yaml(fleet_yaml).unwrap();
    assert_eq!(
        chart_fleet,
        fleet(),
        "chart's fleet.yaml = tests/fixtures/kube/fleet.yaml"
    );
    assert_eq!(
        chart_fleet.cluster,
        ClusterNetworks {
            pod_cidrs: vec![net("10.244.0.0/16")],
            service_cidrs: vec![net("10.96.0.0/12")],
            control_plane: vec![net("10.0.0.10/32")],
            control_plane_hosts: vec!["loams-api.loams-system.svc".into()],
        }
    );
    KubeRuntime::new(chart_fleet.clone()).expect("the chart's fleet config is valid");

    // The flow Deployment: pinned image, its own service account (which
    // needs a token for the API server), the fleet config mounted.
    let flow = find("Deployment", "loams-connectors-flow");
    let pod = &flow["spec"]["template"]["spec"];
    let image = pod["containers"][0]["image"].as_str().unwrap();
    assert!(image.contains("@sha256:"), "{image}");
    assert_eq!(pod["serviceAccountName"], "loams-connectors-flow");
    assert_eq!(pod["securityContext"]["runAsNonRoot"], true);
    assert!(
        flow["spec"]["template"]["metadata"]["annotations"]["loams.dev/fleet-config-hash"]
            .is_string()
    );
    assert_eq!(flow["spec"]["replicas"], 2);

    // R25.9: `get` on secrets only, per served namespace; never list or
    // watch, never cluster-wide.
    for o in objects {
        if o["kind"] == "ClusterRole" {
            assert!(
                !o.to_string().contains("secrets"),
                "no cluster-wide secret access"
            );
        }
        if o["kind"] == "Role" {
            for rule in o["rules"].as_array().unwrap() {
                let resources: Vec<&str> = rule["resources"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| r.as_str().unwrap())
                    .collect();
                if resources.contains(&"secrets") {
                    assert_eq!(resources, ["secrets"], "secrets in a rule of their own");
                    assert_eq!(rule["verbs"], serde_json::json!(["get"]));
                }
            }
        }
    }
    for ns in ["loams-7", "loams-9"] {
        let role = objects
            .iter()
            .find(|o| o["kind"] == "Role" && o["metadata"]["namespace"] == ns)
            .unwrap_or_else(|| panic!("a Role in {ns}"));
        assert!(role.to_string().contains("\"secrets\""));
        let binding = objects
            .iter()
            .find(|o| o["kind"] == "RoleBinding" && o["metadata"]["namespace"] == ns)
            .unwrap();
        assert_eq!(binding["subjects"][0]["name"], "loams-connectors-flow");
        assert_eq!(binding["subjects"][0]["namespace"], "loams-system");
        // The fallback service account: no token, no bindings.
        let sa = objects
            .iter()
            .find(|o| {
                o["kind"] == "ServiceAccount"
                    && o["metadata"]["namespace"] == ns
                    && o["metadata"]["name"] == chart_fleet.fallback.service_account.as_str()
            })
            .unwrap();
        assert_eq!(sa["automountServiceAccountToken"], false);
        // Fallback pods: default-deny egress until KubeRuntime's policy
        // opens it, ingress only for metrics.
        let np = objects
            .iter()
            .find(|o| {
                o["kind"] == "NetworkPolicy"
                    && o["metadata"]["namespace"] == ns
                    && o["metadata"]["name"] == "loams-connectors-fallback-default"
            })
            .unwrap();
        assert_eq!(
            np["spec"]["podSelector"]["matchLabels"][labels::FALLBACK],
            "true"
        );
        assert_eq!(np["spec"]["egress"], serde_json::json!([]));
    }
    for o in objects {
        if o["kind"] == "RoleBinding" || o["kind"] == "ClusterRoleBinding" {
            assert_ne!(
                o["subjects"][0]["name"],
                chart_fleet.fallback.service_account.as_str(),
                "the fallback service account is bound to nothing"
            );
        }
    }

    // R24.1: the scrape configuration for flow and for each fallback runtime.
    let sm = find("ServiceMonitor", "loams-connectors-flow");
    assert_eq!(sm["spec"]["endpoints"][0]["port"], "metrics");
    for runtime in ["camel", "debezium", "iggy"] {
        let pm = find("PodMonitor", &format!("loams-connectors-{runtime}"));
        assert_eq!(
            pm["spec"]["selector"]["matchLabels"][labels::RUNTIME],
            runtime
        );
        assert_eq!(
            pm["spec"]["selector"]["matchLabels"][labels::FALLBACK],
            "true"
        );
        assert_eq!(
            pm["spec"]["namespaceSelector"]["matchNames"],
            serde_json::json!(["loams-7", "loams-9"])
        );
    }

    // A PodDisruptionBudget keeps a drain from taking every flow pod at once.
    let pdb = find("PodDisruptionBudget", "loams-connectors-flow");
    assert_eq!(pdb["spec"]["maxUnavailable"], 1);

    // No Secret is rendered by the chart (secrets are the user's).
    assert!(objects.iter().all(|o| o["kind"] != "Secret"));
}
