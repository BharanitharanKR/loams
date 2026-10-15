//! The Kubernetes runtime (PG2 Task 13, feature `kubernetes`): the objects
//! it renders, checked against goldens, and what it asks a stand-in of the
//! Kubernetes API for.
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path as FsPath;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use loams_pg_control::ids::ComputeId;
use loams_pg_control::model::{ComputeBinding, Cu, EndpointRec, EndpointType};
use loams_pg_control::runtime::certs::ComputeCa;
use loams_pg_control::runtime::kubernetes::{
    KubeConfig, KubeRuntime, compute_pod, compute_secret, computes_service, endpoint_service, role,
};
use loams_pg_control::runtime::{
    BackendAddr, ComputeLaunch, ComputeRuntime, ResizeOutcome, RuntimeError, RuntimeStatus,
};
use loams_pg_control::secrets::Secret;
use serde_json::{Value, json};

const PRJ: &str = "prj-01JZ0000000000000000000000";
const BR: &str = "br-01JZ0000000000000000000009";
const EP: &str = "ep-01JZ0000000000000000000001";
const CMP: &str = "cmp-01JZ0000000000000000000003";
const CMP2: &str = "cmp-01JZ0000000000000000000004";
const IMAGE: &str = "ghcr.io/neondatabase/compute-node-v17@sha256:13ab146d3e7bbabb25a8532f315ac443e7512351d1ede0bab586def5c70e26c3";
const TOKEN: &str = "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJjbXAifQ.dG9rZW4tc2VudGluZWw";
const K8S_NS: &str = "loams-pg-acme";
const ADDR: &str = "ep-01jz0000000000000000000001.loams-pg-acme.svc.cluster.local";

fn config() -> KubeConfig {
    KubeConfig::new("http://pg-control-upcall.loams-postgres.svc.cluster.local:7690")
}

fn endpoint() -> EndpointRec {
    EndpointRec::new(PRJ, EP, BR, EndpointType::ReadWrite)
}

fn launch(id: &str) -> ComputeLaunch {
    ComputeLaunch {
        compute_id: id.parse().unwrap(),
        namespace: "acme".into(),
        binding: Some(ComputeBinding {
            namespace: "acme".into(),
            project_id: PRJ.into(),
            endpoint_id: EP.into(),
        }),
        address: Some(BackendAddr::new(ADDR, 5432)),
        image: IMAGE.into(),
        cu: Cu(2),
        upcall_token: Secret::new(TOKEN.into()),
    }
}

/// `value` against `tests/fixtures/kube/<name>`; `LOAMS_BLESS=1` rewrites it.
fn golden(name: &str, value: &Value) {
    let text = serde_json::to_string_pretty(value).unwrap() + "\n";
    let path = FsPath::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/kube")
        .join(name);
    if std::env::var_os("LOAMS_BLESS").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &text).unwrap();
    }
    assert_eq!(text, std::fs::read_to_string(&path).unwrap(), "{name}");
}

/// The compute's Pod, with a sandbox runtime class and its certificate
/// mounted, is `tests/fixtures/kube/pod.json`; its spec never holds the
/// token, and it runs locked down.
#[test]
fn pod_golden() {
    let mut config = config();
    config.runtime_class = Some("gvisor".into());
    let pod = serde_json::to_value(compute_pod(&config, K8S_NS, &launch(CMP), true)).unwrap();
    golden("pod.json", &pod);
    assert!(!pod.to_string().contains(TOKEN));
    let spec = &pod["spec"];
    assert_eq!(spec["restartPolicy"], "Never");
    assert_eq!(spec["automountServiceAccountToken"], false);
    assert_eq!(spec["securityContext"]["runAsNonRoot"], true);
    assert_eq!(
        spec["securityContext"]["seccompProfile"]["type"],
        "RuntimeDefault"
    );
    let c = &spec["containers"][0];
    assert_eq!(c["securityContext"]["readOnlyRootFilesystem"], true);
    assert_eq!(c["securityContext"]["allowPrivilegeEscalation"], false);
    assert_eq!(c["securityContext"]["capabilities"]["drop"], json!(["ALL"]));
    assert_eq!(c["resources"]["requests"], c["resources"]["limits"]);
    assert_eq!(c["resources"]["limits"]["cpu"], "500m");
    assert_eq!(c["resources"]["limits"]["memory"], "2Gi");
    assert!(spec.get("hostNetwork").is_none() && spec.get("hostPID").is_none());
    for v in spec["volumes"].as_array().unwrap() {
        assert!(v.get("hostPath").is_none(), "{v}");
    }
    let labels = &pod["metadata"]["labels"];
    for key in ["org", "namespace", "project", "endpoint", "compute"] {
        assert!(labels.get(format!("loams.dev/{key}")).is_some(), "{key}");
    }
    assert_eq!(
        pod["metadata"]["annotations"]["argocd.argoproj.io/compare-options"],
        "IgnoreExtraneous"
    );
}

/// The endpoint's Service and the computes' headless one are
/// `tests/fixtures/kube/service.json`.
#[test]
fn service_golden() {
    let services = json!([
        serde_json::to_value(endpoint_service(&config(), K8S_NS, "acme", &endpoint()).unwrap())
            .unwrap(),
        serde_json::to_value(computes_service(K8S_NS)).unwrap(),
    ]);
    golden("service.json", &services);
}

/// The Role reaches Pods (and their resize), Services and Secrets of its
/// namespace, and reads Pods' metrics, nothing else:
/// `tests/fixtures/kube/role.json`.
#[test]
fn rbac_allows_only_pods_services_secrets() {
    let role = serde_json::to_value(role(K8S_NS)).unwrap();
    golden("role.json", &role);
    let mut resources = BTreeSet::new();
    for rule in role["rules"].as_array().unwrap() {
        if rule["apiGroups"] == json!(["metrics.k8s.io"]) {
            // The autoscaler reads Pods' CPU, and only reads it (R15.2).
            assert_eq!(rule["resources"], json!(["pods"]), "{rule}");
            assert_eq!(rule["verbs"], json!(["get"]), "{rule}");
            continue;
        }
        assert_eq!(rule["apiGroups"], json!([""]), "{rule}");
        for r in rule["resources"].as_array().unwrap() {
            resources.insert(r.as_str().unwrap().to_owned());
        }
        for v in rule["verbs"].as_array().unwrap() {
            assert_ne!(v, "*", "{rule}");
            assert_ne!(v, "escalate");
        }
        assert!(rule.get("resourceNames").is_none());
    }
    assert_eq!(
        resources,
        ["pods", "pods/resize", "services", "secrets"]
            .map(str::to_owned)
            .into()
    );
}

/// The Secret holds the token (and the certificate with a CA), base64 as
/// `data`.
#[test]
fn secret_holds_the_token() {
    use base64::Engine as _;
    let secret = serde_json::to_value(compute_secret(K8S_NS, &launch(CMP), None)).unwrap();
    let token = base64::engine::general_purpose::STANDARD
        .decode(secret["data"]["token"].as_str().unwrap())
        .unwrap();
    assert_eq!(token, TOKEN.as_bytes());
    assert_eq!(secret["metadata"]["name"], CMP.to_lowercase());
    assert!(secret["data"].get("tls.key").is_none());
}

// A stand-in of the Kubernetes API: Pods, Services, Secrets and the
// Pods' resize subresource, kept as JSON.

#[derive(Default)]
struct Api {
    /// (namespace, resource, name) → object.
    objects: BTreeMap<(String, String, String), Value>,
    /// `METHOD resource name content-type`.
    calls: Vec<String>,
    /// What the resize subresource answers.
    resize: u16,
}

type Shared = Arc<Mutex<Api>>;

fn status(code: u16, reason: &str, message: &str) -> Response {
    let body = json!({
        "kind": "Status", "apiVersion": "v1", "metadata": {},
        "status": if code < 300 { "Success" } else { "Failure" },
        "message": message, "reason": reason, "code": code,
    });
    (
        StatusCode::from_u16(code).unwrap(),
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

fn ct(headers: &HeaderMap) -> String {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned()
}

/// RFC 7386 merge patch.
fn merge(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(t), Value::Object(p)) => {
            for (k, v) in p {
                if v.is_null() {
                    t.remove(k);
                } else {
                    merge(t.entry(k.clone()).or_insert(Value::Null), v);
                }
            }
        }
        (t, p) => *t = p.clone(),
    }
}

async fn create(
    State(s): State<Shared>,
    Path((ns, res)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut api = s.lock().unwrap();
    let mut obj: Value = serde_json::from_slice(&body).unwrap();
    let name = obj["metadata"]["name"].as_str().unwrap().to_owned();
    api.calls
        .push(format!("POST {res} {name} {}", ct(&headers)));
    let key = (ns, res.clone(), name.clone());
    if api.objects.contains_key(&key) {
        return status(
            409,
            "AlreadyExists",
            &format!("{res} {name:?} already exists"),
        );
    }
    obj["metadata"]["resourceVersion"] = json!("1");
    api.objects.insert(key, obj.clone());
    (StatusCode::CREATED, axum::Json(obj)).into_response()
}

async fn read(
    State(s): State<Shared>,
    Path((ns, res, name)): Path<(String, String, String)>,
) -> Response {
    let mut api = s.lock().unwrap();
    api.calls.push(format!("GET {res} {name}"));
    match api.objects.get(&(ns, res.clone(), name.clone())) {
        Some(o) => (StatusCode::OK, axum::Json(o.clone())).into_response(),
        None => status(404, "NotFound", &format!("{res} {name:?} not found")),
    }
}

async fn update(
    State(s): State<Shared>,
    Path((ns, res, name)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut api = s.lock().unwrap();
    let ct = ct(&headers);
    api.calls.push(format!("PATCH {res} {name} {ct}"));
    let patch: Value = serde_json::from_slice(&body).unwrap();
    let key = (ns, res.clone(), name.clone());
    if ct.starts_with("application/apply-patch") {
        let mut obj = patch;
        obj["metadata"]["resourceVersion"] = json!("1");
        api.objects.insert(key, obj.clone());
        return (StatusCode::OK, axum::Json(obj)).into_response();
    }
    match api.objects.get_mut(&key) {
        Some(o) => {
            merge(o, &patch);
            (StatusCode::OK, axum::Json(o.clone())).into_response()
        }
        None => status(404, "NotFound", &format!("{res} {name:?} not found")),
    }
}

async fn remove(
    State(s): State<Shared>,
    Path((ns, res, name)): Path<(String, String, String)>,
) -> Response {
    let mut api = s.lock().unwrap();
    api.calls.push(format!("DELETE {res} {name}"));
    match api.objects.remove(&(ns, res.clone(), name.clone())) {
        Some(o) => (StatusCode::OK, axum::Json(o)).into_response(),
        None => status(404, "NotFound", &format!("{res} {name:?} not found")),
    }
}

async fn pod_metrics(
    State(s): State<Shared>,
    Path((ns, name)): Path<(String, String)>,
) -> Response {
    let mut api = s.lock().unwrap();
    api.calls.push(format!("GET podmetrics {name}"));
    match api.objects.get(&(ns, "podmetrics".into(), name.clone())) {
        Some(o) => (StatusCode::OK, axum::Json(o.clone())).into_response(),
        None => status(404, "NotFound", &format!("podmetrics {name:?} not found")),
    }
}

async fn resize(
    State(s): State<Shared>,
    Path((ns, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut api = s.lock().unwrap();
    api.calls
        .push(format!("PATCH pods/resize {name} {}", ct(&headers)));
    match api.resize {
        404 => status(
            404,
            "NotFound",
            "the server could not find the requested resource",
        ),
        422 => status(422, "Invalid", "resize is not allowed for this pod"),
        _ => {
            let patch: Value = serde_json::from_slice(&body).unwrap();
            let pod = api.objects.get_mut(&(ns, "pods".into(), name)).unwrap();
            pod["spec"]["containers"][0]["resources"] =
                patch["spec"]["containers"][0]["resources"].clone();
            (StatusCode::OK, axum::Json(pod.clone())).into_response()
        }
    }
}

/// A stand-in API server, and a runtime on it.
async fn stand_in(ca: Option<ComputeCa>) -> (Shared, KubeRuntime) {
    let shared: Shared = Arc::default();
    let app = Router::new()
        .route("/api/v1/namespaces/{ns}/pods/{name}/resize", patch(resize))
        .route(
            "/apis/metrics.k8s.io/v1beta1/namespaces/{ns}/pods/{name}",
            get(pod_metrics),
        )
        .route(
            "/api/v1/namespaces/{ns}/{res}/{name}",
            get(read).patch(update).delete(remove),
        )
        .route("/api/v1/namespaces/{ns}/{res}", post(create))
        .with_state(shared.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    let client =
        kube::Client::try_from(kube::Config::new(format!("http://{addr}").parse().unwrap()))
            .unwrap();
    (shared, KubeRuntime::new(client, config(), ca))
}

fn object(api: &Shared, res: &str, name: &str) -> Option<Value> {
    api.lock()
        .unwrap()
        .objects
        .get(&(K8S_NS.into(), res.into(), name.to_lowercase()))
        .cloned()
}

fn calls(api: &Shared) -> Vec<String> {
    api.lock().unwrap().calls.clone()
}

/// The address is the endpoint's Service, created once; a start writes
/// the Secret, then the Pod, and a second start of the same compute
/// creates nothing.
#[tokio::test]
async fn start_is_idempotent_by_compute_id() {
    let (api, rt) = stand_in(None).await;
    let addr = rt.ensure_address("acme", &endpoint()).await.unwrap();
    assert_eq!(addr, BackendAddr::new(ADDR, 5432));
    let mut recorded = endpoint();
    recorded.backend_addr = Some(addr.to_string());
    assert_eq!(rt.ensure_address("acme", &recorded).await.unwrap(), addr);
    let service = object(&api, "services", EP).unwrap();
    assert_eq!(service["spec"]["selector"]["loams.dev/compute"], "none");

    let first = rt.start(&launch(CMP)).await.unwrap();
    let second = rt.start(&launch(CMP)).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first.ctl_url,
        format!(
            "http://{}.computes.loams-pg-acme.svc.cluster.local:3080",
            CMP.to_lowercase()
        )
    );
    let posts: Vec<String> = calls(&api)
        .into_iter()
        .filter(|c| c.starts_with("POST pods") || c.starts_with("PATCH secrets"))
        .collect();
    assert_eq!(posts.len(), 2, "{posts:?}");
    assert!(
        posts[0].starts_with("PATCH secrets"),
        "the Secret first: {posts:?}"
    );
    assert!(posts[0].contains("apply-patch"), "{posts:?}");
    let pod = object(&api, "pods", CMP).unwrap();
    assert!(!pod.to_string().contains(TOKEN));
    assert!(object(&api, "services", "computes").is_some());
}

/// A launch whose address is not its endpoint's Service, an unbound one
/// with an address, one whose namespace is not its binding's, an unpinned
/// image, a namespace too long for `loams-pg-<ns>` and a
/// recorded address of another kind are refused, and nothing is created.
#[tokio::test]
async fn bad_launches_are_refused() {
    let (api, rt) = stand_in(None).await;
    let mut cases = Vec::new();
    let mut l = launch(CMP);
    l.address = Some(BackendAddr::new("127.0.0.1", 55500));
    cases.push(l);
    let mut l = launch(CMP);
    l.binding = None;
    cases.push(l);
    let mut l = launch(CMP);
    l.namespace = "other".into();
    cases.push(l);
    let mut l = launch(CMP);
    l.image = "ghcr.io/neondatabase/compute-node-v17:latest".into();
    cases.push(l);
    let mut l = launch(CMP);
    l.binding.as_mut().unwrap().namespace = "a".repeat(60);
    l.namespace = "a".repeat(60);
    cases.push(l);
    let mut l = launch(CMP);
    l.upcall_token = Secret::new(format!("{TOKEN}\nX=1"));
    cases.push(l);
    for l in cases {
        let e = rt.start(&l).await.unwrap_err();
        assert!(matches!(e, RuntimeError::InvalidArgument(_)), "{e}");
    }
    let mut ep = endpoint();
    ep.backend_addr = Some("127.0.0.1:55500".into());
    let e = rt.ensure_address("acme", &ep).await.unwrap_err();
    assert!(matches!(e, RuntimeError::InvalidArgument(_)), "{e}");
    assert!(calls(&api).is_empty(), "{:?}", calls(&api));
}

/// Binding moves the Service's selector to the compute; binding the next
/// compute moves it again.
#[tokio::test]
async fn bind_moves_selector() {
    let (api, rt) = stand_in(None).await;
    let addr = rt.ensure_address("acme", &endpoint()).await.unwrap();
    let mut ep = endpoint();
    ep.backend_addr = Some(addr.to_string());
    for id in [CMP, CMP2] {
        let handle = rt.start(&launch(id)).await.unwrap();
        rt.bind_address("acme", &ep, &handle).await.unwrap();
        let service = object(&api, "services", EP).unwrap();
        assert_eq!(service["spec"]["selector"]["loams.dev/compute"], id);
        assert_eq!(
            service["spec"]["selector"]["app.kubernetes.io/component"],
            "postgres-compute"
        );
        let pod = object(&api, "pods", id).unwrap();
        assert_eq!(pod["metadata"]["labels"]["loams.dev/compute"], id);
    }
}

/// `resize` asks the Pod's resize subresource: Applied when the API takes
/// it, Unsupported when the server has no such subresource, Refused when
/// it refuses the resize, and an error for a compute with no Pod.
#[tokio::test]
async fn resize_unsupported_maps_to_outcome() {
    let (api, rt) = stand_in(None).await;
    rt.ensure_address("acme", &endpoint()).await.unwrap();
    rt.start(&launch(CMP)).await.unwrap();
    let id: ComputeId = CMP.parse().unwrap();
    api.lock().unwrap().resize = 404;
    assert_eq!(
        rt.resize("acme", &id, Cu(4)).await.unwrap(),
        ResizeOutcome::Unsupported
    );
    api.lock().unwrap().resize = 422;
    assert!(matches!(
        rt.resize("acme", &id, Cu(4)).await.unwrap(),
        ResizeOutcome::Refused(_)
    ));
    api.lock().unwrap().resize = 200;
    assert_eq!(
        rt.resize("acme", &id, Cu(8)).await.unwrap(),
        ResizeOutcome::Applied
    );
    let pod = object(&api, "pods", CMP).unwrap();
    assert_eq!(
        pod["spec"]["containers"][0]["resources"]["limits"]["cpu"],
        "2000m"
    );
    assert!(
        calls(&api)
            .iter()
            .any(|c| c.starts_with("PATCH pods/resize") && c.contains("strategic-merge-patch"))
    );
    let other: ComputeId = CMP2.parse().unwrap();
    assert!(matches!(
        rt.resize("acme", &other, Cu(4)).await,
        Err(RuntimeError::InvalidArgument(_))
    ));
}

/// A warm-pool compute (Task 14) starts with no binding and no address in
/// its namespace, its Pod naming no project or endpoint; binding it labels
/// the Pod with its endpoint and points the endpoint's Service at it.
#[tokio::test]
async fn unbound_pod_starts_and_bind_labels_it() {
    let (api, rt) = stand_in(None).await;
    let mut warm = launch(CMP);
    warm.binding = None;
    warm.address = None;
    let handle = rt.start(&warm).await.unwrap();
    assert_eq!(handle.address, None);
    let pod = object(&api, "pods", CMP).unwrap();
    let labels = &pod["metadata"]["labels"];
    assert_eq!(labels["loams.dev/namespace"], "acme");
    assert_eq!(labels["loams.dev/project"], "");
    assert_eq!(labels["loams.dev/endpoint"], "");

    let addr = rt.ensure_address("acme", &endpoint()).await.unwrap();
    let mut ep = endpoint();
    ep.backend_addr = Some(addr.to_string());
    rt.bind_address("acme", &ep, &handle).await.unwrap();
    let pod = object(&api, "pods", CMP).unwrap();
    assert_eq!(pod["metadata"]["labels"]["loams.dev/project"], PRJ);
    assert_eq!(pod["metadata"]["labels"]["loams.dev/endpoint"], EP);
    let service = object(&api, "services", EP).unwrap();
    assert_eq!(service["spec"]["selector"]["loams.dev/compute"], CMP);
}

/// With a CA, a warm-pool compute's certificate names its own Pod (the
/// endpoint it will serve is not known yet), never an endpoint's Service.
#[tokio::test]
async fn unbound_certificate_names_the_pod() {
    use base64::Engine as _;
    let ca = ComputeCa::generate(SystemTime::now()).unwrap();
    let (api, rt) = stand_in(Some(ca)).await;
    let mut warm = launch(CMP);
    warm.binding = None;
    warm.address = None;
    rt.start(&warm).await.unwrap();
    let secret = object(&api, "secrets", CMP).unwrap();
    let pem = base64::engine::general_purpose::STANDARD
        .decode(secret["data"]["tls.crt"].as_str().unwrap())
        .unwrap();
    let (_, pem) = x509_parser::pem::parse_x509_pem(&pem).unwrap();
    let cert = pem.parse_x509().unwrap();
    let names: Vec<String> = cert
        .subject_alternative_name()
        .unwrap()
        .unwrap()
        .value
        .general_names
        .iter()
        .map(|n| n.to_string())
        .collect();
    let pod = CMP.to_lowercase();
    assert!(
        names
            .iter()
            .any(|n| n.contains(&format!("{pod}.computes.loams-pg-acme.svc.cluster.local"))),
        "{names:?}"
    );
    assert!(names.iter().all(|n| !n.contains("ep-")), "{names:?}");
}

/// What the Pod says is the compute's status; stop removes the Pod and its
/// Secret and is idempotent; remove_address deletes the Service.
#[tokio::test]
async fn status_stop_and_remove() {
    let (api, rt) = stand_in(None).await;
    let id: ComputeId = CMP.parse().unwrap();
    assert_eq!(
        rt.status("acme", &id).await.unwrap(),
        RuntimeStatus::Missing
    );
    rt.stop("acme", &id).await.unwrap();
    rt.ensure_address("acme", &endpoint()).await.unwrap();
    rt.start(&launch(CMP)).await.unwrap();
    assert_eq!(
        rt.status("acme", &id).await.unwrap(),
        RuntimeStatus::Pending
    );
    let key = (K8S_NS.to_owned(), "pods".to_owned(), CMP.to_lowercase());
    api.lock().unwrap().objects.get_mut(&key).unwrap()["status"] = json!({ "phase": "Running" });
    assert_eq!(
        rt.status("acme", &id).await.unwrap(),
        RuntimeStatus::Running
    );
    api.lock().unwrap().objects.get_mut(&key).unwrap()["status"] = json!({
        "phase": "Failed",
        "containerStatuses": [{
            "name": "compute", "image": IMAGE, "imageID": "", "ready": false,
            "restartCount": 0, "state": { "terminated": { "exitCode": 3 } },
        }],
    });
    assert_eq!(
        rt.status("acme", &id).await.unwrap(),
        RuntimeStatus::Exited { code: Some(3) }
    );
    rt.stop("acme", &id).await.unwrap();
    assert!(object(&api, "pods", CMP).is_none());
    assert!(object(&api, "secrets", CMP).is_none());
    rt.stop("acme", &id).await.unwrap();
    rt.remove_address("acme", &endpoint()).await.unwrap();
    rt.remove_address("acme", &endpoint()).await.unwrap();
    assert!(object(&api, "services", EP).is_none());
}

/// With a CA, the compute's Secret holds a certificate for its endpoint's
/// Service names, and the Pod mounts it.
#[tokio::test]
async fn certificate_mounted_with_a_ca() {
    let ca = ComputeCa::generate(SystemTime::now()).unwrap();
    let ca_pem = ca.cert_pem().to_owned();
    let (api, rt) = stand_in(Some(ca)).await;
    rt.ensure_address("acme", &endpoint()).await.unwrap();
    rt.start(&launch(CMP)).await.unwrap();
    let secret = object(&api, "secrets", CMP).unwrap();
    use base64::Engine as _;
    let read = |k: &str| {
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(secret["data"][k].as_str().unwrap())
                .unwrap(),
        )
        .unwrap()
    };
    assert_eq!(read("ca.crt"), ca_pem);
    assert!(read("tls.crt").starts_with("-----BEGIN CERTIFICATE-----"));
    assert!(read("tls.key").contains("PRIVATE KEY"));
    let pod = object(&api, "pods", CMP).unwrap();
    assert!(
        pod["spec"]["volumes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["name"] == "tls")
    );
}

/// The autoscaler's CPU (Task 15): the compute container's usage from the
/// Pod's metrics, as a fraction of the compute's size; an error without
/// metrics.
#[tokio::test]
async fn pod_cpu_from_the_metrics_api() {
    use loams_pg_control::autoscale::ComputeMetrics;
    use loams_pg_control::model::{ComputeRec, ComputeStatus};
    use loams_pg_control::runtime::kubernetes::cpu_cores;

    assert_eq!(cpu_cores("250m"), Some(0.25));
    assert_eq!(cpu_cores("1500000000n"), Some(1.5));
    assert_eq!(cpu_cores("2"), Some(2.0));
    assert_eq!(cpu_cores("x"), None);
    let (api, rt) = stand_in(None).await;
    let compute = ComputeRec {
        id: CMP.into(),
        binding: None,
        pool: None,
        bound_at_ms: None,
        spec_version: 0,
        status: ComputeStatus::Running,
        jwt_key_id: None,
        ctl_url: None,
        cu: Cu(2),
        created_at_ms: 0,
        last_active_ms: None,
    };
    assert!(rt.sample("acme", &compute).await.is_err(), "no metrics yet");
    api.lock().unwrap().objects.insert(
        (K8S_NS.into(), "podmetrics".into(), CMP.to_lowercase()),
        serde_json::from_str::<Value>(
            &std::fs::read_to_string(
                FsPath::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/kube/pod_metrics.json"),
            )
            .unwrap(),
        )
        .unwrap(),
    );
    let s = rt.sample("acme", &compute).await.unwrap();
    // 0.4 cores of a 0.5 CU (0.5 vCPU) compute.
    assert!((s.cpu.unwrap() - 0.8).abs() < 1e-9, "{s:?}");
    assert_eq!(s.lfc_size, None);
}
