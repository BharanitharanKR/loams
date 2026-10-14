//! The Kubernetes runtime against a real API server and kubelet (PG2 Task
//! 13, feature `kubernetes`). Ignored by default; `pg2-e2e.yml`'s kind job
//! runs it:
//!
//! ```text
//! kind create cluster --name loams-pg
//! docker pull <compute-v17 image> && kind load docker-image <image> --name loams-pg
//! cargo test -p loams-pg-control --features kubernetes --test it_kind -- --ignored
//! ```
//!
//! The runtime creates the endpoint's Service and the compute's Secret and
//! Pod in `loams-pg-it`. The kubelet admits the Pod as rendered (non-root
//! uid 1000, read-only root filesystem, every capability dropped,
//! `RuntimeDefault` seccomp) and its container starts. Binding moves the
//! Service's selector to the compute, and `stop` and `remove_address`
//! clean up. No storage runs in the cluster, so the compute gets no spec
//! (its upcall URI answers nothing) and this does not run `SELECT 1`
//! through the Service; that waits for the storage stack in the cluster
//! (Task 48's chart). The Pod's settings were run end to end, with
//! `SELECT 1`, on compose (R13.5). `IT_KIND_IMAGE` overrides the image.
#![allow(clippy::unwrap_used)]

use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::{Namespace, Pod, Service};
use kube::api::{Api, DeleteParams, PostParams};
use loams_pg_control::ids::ComputeId;
use loams_pg_control::model::{ComputeBinding, Cu, EndpointRec, EndpointType};
use loams_pg_control::runtime::kubernetes::{KubeConfig, KubeRuntime, pod_name};
use loams_pg_control::runtime::{ComputeLaunch, ComputeRuntime, RuntimeStatus, images};
use loams_pg_control::secrets::Secret;
use serde_json::json;

const NS: &str = "it";
const K8S_NS: &str = "loams-pg-it";
const PRJ: &str = "prj-01JZ0000000000000000000000";
const BR: &str = "br-01JZ0000000000000000000009";
const EP: &str = "ep-01JZ0000000000000000000001";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a kind cluster (KUBECONFIG) with the compute image loaded"]
async fn it_kind_start_bind_and_stop() {
    let client = kube::Client::try_default().await.unwrap();
    let namespaces: Api<Namespace> = Api::all(client.clone());
    let ns: Namespace = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Namespace", "metadata": { "name": K8S_NS },
    }))
    .unwrap();
    match namespaces.create(&PostParams::default(), &ns).await {
        Ok(_) => {}
        Err(kube::Error::Api(s)) if s.code == 409 => {}
        Err(e) => panic!("creating {K8S_NS}: {e}"),
    }

    let image = std::env::var("IT_KIND_IMAGE")
        .unwrap_or_else(|_| images::pinned().unwrap().remove(&17).unwrap());
    let rt = KubeRuntime::new(
        client.clone(),
        KubeConfig::new("http://pg-control-upcall.loams-postgres.svc.cluster.local:7690"),
        None,
    );
    let ep = EndpointRec::new(PRJ, EP, BR, EndpointType::ReadWrite);
    let addr = rt.ensure_address(NS, &ep).await.unwrap();
    let id = ComputeId::new();
    let launch = ComputeLaunch {
        compute_id: id,
        binding: Some(ComputeBinding {
            namespace: NS.into(),
            project_id: PRJ.into(),
            endpoint_id: EP.into(),
        }),
        address: Some(addr.clone()),
        image,
        cu: Cu(1),
        upcall_token: Secret::new("it.kind.token".into()),
    };
    let handle = rt.start(&launch).await.unwrap();

    // The kubelet admits the Pod and its container starts: it leaves
    // Pending, and its container ran (no CreateContainerConfigError, which
    // a refused security setting gives).
    let pods: Api<Pod> = Api::namespaced(client.clone(), K8S_NS);
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let status = rt.status(NS, &id).await.unwrap();
        let pod = pods.get(&pod_name(&id)).await.unwrap();
        let waiting = pod
            .status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .and_then(|cs| cs.first())
            .and_then(|c| c.state.as_ref())
            .and_then(|s| s.waiting.as_ref())
            .and_then(|w| w.reason.clone());
        assert_ne!(
            waiting.as_deref(),
            Some("CreateContainerConfigError"),
            "{:?}",
            pod.status
        );
        if status != RuntimeStatus::Pending {
            assert!(
                matches!(
                    status,
                    RuntimeStatus::Running | RuntimeStatus::Exited { .. }
                ),
                "{status:?}"
            );
            break;
        }
        assert!(Instant::now() < deadline, "still pending: {:?}", pod.status);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let mut bound = ep.clone();
    bound.backend_addr = Some(addr.to_string());
    rt.bind_address(NS, &bound, &handle).await.unwrap();
    let services: Api<Service> = Api::namespaced(client.clone(), K8S_NS);
    let service = services.get(&EP.to_lowercase()).await.unwrap();
    let selector = service.spec.unwrap().selector.unwrap();
    assert_eq!(
        selector.get("loams.dev/compute").map(String::as_str),
        Some(id.to_string().as_str())
    );

    rt.stop(NS, &id).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(120);
    while rt.status(NS, &id).await.unwrap() != RuntimeStatus::Missing {
        assert!(Instant::now() < deadline, "the Pod is still there");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    rt.remove_address(NS, &bound).await.unwrap();
    assert!(
        services
            .get_opt(&EP.to_lowercase())
            .await
            .unwrap()
            .is_none()
    );
    let _ = namespaces.delete(K8S_NS, &DeleteParams::default()).await;
}
