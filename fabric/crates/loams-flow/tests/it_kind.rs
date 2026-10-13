//! CN2 Task 26's kind test: the mixed fleet on a real Kubernetes cluster.
//! `#[ignore]`d, run by the `connectors-kind` CI job.
//!
//! It needs a kind cluster whose current kubeconfig context is the one to
//! use, `helm` and `kubectl` on `PATH`, and `LOAMS_KIND_FLEET_VALUES`: a values
//! file for `deploy/helm/loams-connectors` naming images loaded into the
//! cluster. Without it the test says so and returns.
//!
//! What runs today (R26.10): the chart installs, the flow pods become
//! ready, every fallback workload `KubeRuntime` renders for the mixed fleet
//! (a Debezium Oracle CDC fallback beside Camel and Iggy groups) is applied
//! and rolls out, and each pod, flow and fallback, is killed and comes back.
//! The native Kafka, Postgres CDC and S3 sink instances and the no-loss check
//! need the family crates (Tasks 9, 10, 13), `NativeRuntime` and FL1's Fabric;
//! the test grows those assertions as they land.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use loams_connector_sdk::{InstanceKey, SecretRef};
use loams_flow::manifest::{RuntimeKind, repository_root};
use loams_flow::runtime::kube::{FallbackInstance, FleetConfig, KubeRuntime, to_yaml};
use loams_flow::validate::Direction;

const RELEASE_NS: &str = "loams-system";

fn sh(cmd: &mut Command) -> String {
    let out = cmd.output().expect("the command runs");
    assert!(
        out.status.success(),
        "{cmd:?}:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn kubectl(args: &[&str]) -> String {
    sh(Command::new("kubectl").args(args))
}

fn apply(yaml: &str) {
    let mut child = Command::new("kubectl")
        .args([
            "apply",
            "--server-side",
            "--field-manager=loams-flow",
            "-f",
            "-",
        ])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(child.stdin.as_mut().unwrap(), yaml.as_bytes()).unwrap();
    assert!(child.wait().unwrap().success(), "kubectl apply");
}

fn wait_ready(ns: &str, selector: &str) {
    kubectl(&[
        "-n",
        ns,
        "wait",
        "--for=condition=Ready",
        "pod",
        "-l",
        selector,
        "--timeout=300s",
    ]);
}

fn files(name: &str, text: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(name.to_owned(), text.to_owned())])
}

#[test]
#[ignore = "needs a kind cluster (CI job connectors-kind)"]
fn it_kind_mixed_fleet() {
    let Some(values) = std::env::var_os("LOAMS_KIND_FLEET_VALUES").map(PathBuf::from) else {
        eprintln!("it_kind_mixed_fleet: LOAMS_KIND_FLEET_VALUES is not set; skipped");
        return;
    };
    let chart = repository_root().join("deploy/helm/loams-connectors");
    for ns in [RELEASE_NS, "loams-7"] {
        let _ = Command::new("kubectl")
            .args(["create", "namespace", ns])
            .output();
    }
    sh(Command::new("helm")
        .args(["upgrade", "--install", "loams-connectors"])
        .arg(&chart)
        .args(["--namespace", RELEASE_NS, "--wait", "--timeout", "5m", "-f"])
        .arg(&values));
    wait_ready(RELEASE_NS, "app.kubernetes.io/component=flow");

    // The fleet config the chart installed, as flow reads it.
    let text = kubectl(&[
        "-n",
        RELEASE_NS,
        "get",
        "configmap",
        "loams-connectors-fleet",
        "-o",
        "jsonpath={.data.fleet\\.yaml}",
    ]);
    let runtime = KubeRuntime::new(FleetConfig::from_yaml(&text).unwrap()).unwrap();
    // A throwaway secret for the CDC instance's mount (kind only).
    let _ = Command::new("kubectl")
        .args([
            "-n",
            "loams-7",
            "create",
            "secret",
            "generic",
            "erp-oracle",
            "--from-literal=password=kind-only",
        ])
        .output();

    let fleet = vec![
        FallbackInstance {
            key: InstanceKey::new(7, "erp-cdc"),
            connector: "debezium-oracle".into(),
            runtime: RuntimeKind::Debezium,
            directions: vec![Direction::Source],
            group: None,
            files: files(
                "erp-cdc.application.properties",
                "debezium.source.connector.class=io.debezium.connector.oracle.OracleConnector\n",
            ),
            secrets: vec![SecretRef::new("erp-oracle/password")],
        },
        FallbackInstance {
            key: InstanceKey::new(7, "billing-jms"),
            connector: "jms".into(),
            runtime: RuntimeKind::Camel,
            directions: vec![Direction::Sink],
            group: None,
            files: files("billing-jms.sink.yaml", "[]\n"),
            secrets: vec![],
        },
        FallbackInstance {
            key: InstanceKey::new(7, "es-legacy"),
            connector: "elasticsearch".into(),
            runtime: RuntimeKind::Iggy,
            directions: vec![Direction::Sink],
            group: None,
            files: files("es-legacy.sink.toml", "\n"),
            secrets: vec![],
        },
    ];
    let objects = runtime.render(&fleet, &BTreeMap::new()).unwrap();
    apply(&to_yaml(&objects));
    wait_ready("loams-7", "loams.dev/fallback=true");

    // Kill each pod, flow and fallback; each comes back.
    for (ns, selector) in [
        (RELEASE_NS, "app.kubernetes.io/component=flow"),
        ("loams-7", "loams.dev/runtime=debezium"),
        ("loams-7", "loams.dev/runtime=camel"),
        ("loams-7", "loams.dev/runtime=iggy"),
    ] {
        kubectl(&["-n", ns, "delete", "pod", "-l", selector, "--wait=true"]);
        wait_ready(ns, selector);
    }
}
