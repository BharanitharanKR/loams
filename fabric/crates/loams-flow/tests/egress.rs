//! CN2 Task 25: the egress guard (Review Focus 9, SSRF), the NetworkPolicy
//! rendered from it for fallback pods, and the refusal of malicious overlay
//! content.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{EgressCheck, ErrorClass, Resolve};
use loams_flow::ConnectorSpec;
use loams_flow::egress::{
    ClusterNetworks, EgressGuard, EgressPolicy, IpNet, render_network_policy,
};
use loams_flow::overlay::check_overlay;
use loams_flow::registry::Registry;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn registry() -> Registry {
    Registry::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../connectors/registry"))
        .unwrap()
}

fn spec(id: &str) -> ConnectorSpec {
    registry().get(id).unwrap().clone()
}

fn net(s: &str) -> IpNet {
    s.parse().unwrap()
}

fn cluster() -> ClusterNetworks {
    ClusterNetworks {
        pod_cidrs: vec![net("10.244.0.0/16"), net("fd00:10:244::/56")],
        service_cidrs: vec![net("10.96.0.0/12")],
        control_plane: vec![net("10.0.0.10/32")],
        control_plane_hosts: vec!["loams-api.loams-system.svc".into()],
    }
}

fn sock(s: &str) -> SocketAddr {
    s.parse().unwrap()
}

#[test]
fn egress_guard_refuses_metadata_loopback_and_cluster() {
    let policy = EgressPolicy::for_cluster(&cluster());

    // After DNS, at connect: every default-denied address.
    for addr in [
        "127.0.0.1:5432",
        "127.8.9.10:80",
        "[::1]:443",
        "169.254.169.254:80", // AWS, GCP and Azure instance metadata
        "[fd00:ec2::254]:80", // AWS IMDS over IPv6
        "[fe80::1]:80",
        "0.0.0.0:80",
        "[::ffff:127.0.0.1]:80", // IPv4-mapped loopback
        "[::ffff:169.254.169.254]:80",
        "[64:ff9b::a9fe:a9fe]:80", // NAT64 of 169.254.169.254
        "10.244.3.7:9092",         // a pod
        "[fd00:10:244::7]:9092",   // a pod, IPv6
        "10.96.0.1:443",           // the Kubernetes API Service
        "10.0.0.10:8443",          // a Loams control-plane Service
    ] {
        let a = sock(addr);
        assert!(policy.check_addr(&a).is_err(), "{addr} must be refused");
        assert!(
            EgressCheck::check_addr(&policy, &a).is_err(),
            "{addr} must be refused through the SDK hook"
        );
    }
    for addr in [
        "93.184.215.14:443",
        "[2606:2800:21f:cb07::1]:443",
        "192.168.10.5:5432",
    ] {
        assert!(policy.check_addr(&sock(addr)).is_ok(), "{addr} is allowed");
    }

    // Before DNS: names and literals.
    for host in [
        "localhost",
        "LOCALHOST.",
        "db.localhost",
        "metadata.google.internal",
        "169.254.169.254",
        "[::1]",
        "kubernetes.default.svc",
        "kubernetes.default.svc.cluster.local",
        "postgres.other-tenant.svc.cluster.local",
        "loams-api.loams-system.svc",
        "2130706433", // 127.0.0.1 as one decimal number
        "0x7f.0.0.1",
        "127.1",
    ] {
        assert!(policy.check_host(host).is_err(), "{host} must be refused");
    }
    assert!(policy.check_host("db.example.com").is_ok());

    // At validation: every host a config names, by spec.
    let pg = spec("postgresql");
    for config in [
        json!({"host": "169.254.169.254", "database": "x", "password": "pg-creds/password"}),
        json!({"host": "localhost", "database": "x"}),
        json!({"dsn": "postgres://u:hunter2@kubernetes.default.svc:5432/x"}),
        json!({"dsn": "postgres://u@[::1]:5432/x"}),
    ] {
        let err = policy.check_config(&pg, &config).expect_err("refused");
        let text = err.to_string();
        assert!(!text.contains("hunter2"), "no userinfo in errors: {text}");
    }
    policy
        .check_config(&pg, &json!({"host": "db.example.com", "database": "x"}))
        .unwrap();
    // A secret path holds a secret *name*, never a host, and is not read.
    policy
        .check_config(
            &pg,
            &json!({"host": "db.example.com", "password": "localhost"}),
        )
        .unwrap();

    let kafka = spec("kafka");
    assert!(
        policy
            .check_config(
                &kafka,
                &json!({"brokers": "b1.example.com:9092,127.0.0.1:9092", "topics": ["t"]})
            )
            .is_err()
    );
    let http = spec("http");
    for url in [
        "http://169.254.169.254/latest/meta-data/",
        "file:///etc/passwd",
        "http://[fd00:ec2::254]/",
        "https://metadata.google.internal/computeMetadata/v1/",
    ] {
        assert!(
            policy.check_config(&http, &json!({"url": url})).is_err(),
            "{url} must be refused"
        );
    }
    policy
        .check_config(&http, &json!({"url": "https://api.example.com/v1/items"}))
        .unwrap();

    // The namespace admin's allowlist lifts the default for what it names.
    let mut admin = EgressPolicy::for_cluster(&cluster());
    admin.allow_cidrs.push(net("10.96.12.0/24"));
    admin
        .allow_hosts
        .push("pg.shared-db.svc.cluster.local".into());
    assert!(admin.check_addr(&sock("10.96.12.40:5432")).is_ok());
    assert!(admin.check_addr(&sock("10.96.13.40:5432")).is_err());
    assert!(admin.check_host("pg.shared-db.svc.cluster.local").is_ok());
    assert!(
        admin
            .check_host("other.shared-db.svc.cluster.local")
            .is_err()
    );
    // An allowlisted name never lifts the address check (DNS rebinding).
    assert!(admin.check_addr(&sock("169.254.169.254:80")).is_err());
}

/// A resolver whose answers change from call to call: a DNS rebinding
/// attacker's name server.
struct Rebinding {
    answers: Mutex<Vec<IpAddr>>,
    calls: AtomicUsize,
}

#[async_trait]
impl Resolve for Rebinding {
    async fn resolve(&self, _host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let ip = self.answers.lock().unwrap().remove(0);
        Ok(vec![SocketAddr::new(ip, port)])
    }
}

async fn serve(status: &'static str, extra: String, hits: Arc<AtomicUsize>) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            hits.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 2048];
            let _ = s.read(&mut buf).await;
            let resp = format!(
                "HTTP/1.1 {status}\r\n{extra}content-length: 2\r\nconnection: close\r\n\r\nok"
            );
            let _ = s.write_all(resp.as_bytes()).await;
        }
    });
    port
}

#[tokio::test]
async fn egress_guard_rechecks_after_dns() {
    // The test's own server is on 127.0.0.1, so the admin allows exactly that
    // address; the rest of 127.0.0.0/8 stays denied.
    let mut policy = EgressPolicy::for_cluster(&cluster());
    policy.allow_cidrs.push(net("127.0.0.1/32"));
    let hits = Arc::new(AtomicUsize::new(0));
    let port = serve("200 OK", String::new(), hits.clone()).await;

    // Validation sees only a name, and the name is fine.
    let http = spec("http");
    policy
        .check_config(
            &http,
            &json!({"url": format!("http://rebind.example.com:{port}/")}),
        )
        .unwrap();

    let resolver = Arc::new(Rebinding {
        answers: Mutex::new(vec![
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
            "169.254.169.254".parse().unwrap(),
        ]),
        calls: AtomicUsize::new(0),
    });
    let guard: Arc<dyn EgressCheck> =
        Arc::new(EgressGuard::new(policy).with_resolver(resolver.clone()));

    // First connection: the name resolves to the allowed address.
    let mut s = loams_connector_sdk::net::connect("rebind.example.com", port, &*guard)
        .await
        .unwrap();
    s.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    let mut answer = Vec::new();
    s.read_to_end(&mut answer).await.unwrap();
    assert!(answer.starts_with(b"HTTP/1.1 200"));
    // Then the name server rebinds it: each connection resolves afresh and
    // is refused after DNS, before a packet is sent.
    for _ in 0..2 {
        let err = loams_connector_sdk::net::connect("rebind.example.com", port, &*guard)
            .await
            .expect_err("rebound address refused");
        assert_eq!(err.class, ErrorClass::Fatal);
        assert!(err.message.contains("egress refused"), "{}", err.message);
    }
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 3);
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    // The HTTP client goes through the same hook.
    let req = http::Request::get(format!("http://rebind.example.com:{port}/"))
        .body(Bytes::new())
        .unwrap();
    let resolver = Arc::new(Rebinding {
        answers: Mutex::new(vec!["10.96.0.1".parse().unwrap()]),
        calls: AtomicUsize::new(0),
    });
    let mut p = EgressPolicy::for_cluster(&cluster());
    p.allow_cidrs.push(net("127.0.0.1/32"));
    let guard = Arc::new(EgressGuard::new(p).with_resolver(resolver));
    let err = loams_connector_sdk::http::client(guard)
        .send(req)
        .await
        .expect_err("the Kubernetes API Service is refused");
    assert!(err.message.contains("egress refused"));
}

#[tokio::test]
async fn redirect_rechecked() {
    let mut policy = EgressPolicy::for_cluster(&cluster());
    policy.allow_cidrs.push(net("127.0.0.1/32"));
    let guard: Arc<dyn EgressCheck> = Arc::new(EgressGuard::new(policy));
    let client = loams_connector_sdk::http::client(guard);

    let final_hits = Arc::new(AtomicUsize::new(0));
    let final_port = serve("200 OK", String::new(), final_hits.clone()).await;

    for (location, refused) in [
        (
            "http://169.254.169.254/latest/meta-data/iam/".to_owned(),
            true,
        ),
        (
            "http://metadata.google.internal/computeMetadata/v1/".to_owned(),
            true,
        ),
        ("http://127.0.0.2:80/".to_owned(), true),
        ("http://[::1]:80/".to_owned(), true),
        (format!("http://127.0.0.1:{final_port}/next"), false),
    ] {
        let hits = Arc::new(AtomicUsize::new(0));
        let port = serve(
            "302 Found",
            format!("location: {location}\r\n"),
            hits.clone(),
        )
        .await;
        let req = http::Request::get(format!("http://127.0.0.1:{port}/start"))
            .header("authorization", "Bearer abc")
            .body(Bytes::new())
            .unwrap();
        // Without following, the redirect comes back as it is.
        let resp = client.send(req).await.unwrap();
        assert_eq!(resp.status(), 302);
        let req = http::Request::get(format!("http://127.0.0.1:{port}/start"))
            .body(Bytes::new())
            .unwrap();
        let out = client.send_following(req, 5).await;
        if refused {
            let err = out.expect_err(&location);
            assert!(
                err.message.contains("egress refused"),
                "{location}: {}",
                err.message
            );
        } else {
            assert_eq!(out.unwrap().status(), 200);
        }
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }
    assert_eq!(final_hits.load(Ordering::SeqCst), 1);

    // A redirect loop is bounded, and each hop was a new, checked request.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            counted.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 2048];
            let _ = s.read(&mut buf).await;
            let resp = "HTTP/1.1 307 Temporary Redirect\r\nlocation: /again\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
            let _ = s.write_all(resp.as_bytes()).await;
        }
    });
    let req = http::Request::get(format!("http://127.0.0.1:{port}/"))
        .body(Bytes::new())
        .unwrap();
    let err = client.send_following(req, 3).await.expect_err("loop");
    assert!(err.message.contains("more than 3"), "{}", err.message);
    assert_eq!(hits.load(Ordering::SeqCst), 4);
}

#[test]
fn network_policy_rendered_from_policy() {
    let mut policy = EgressPolicy::for_cluster(&cluster());
    policy.allow_cidrs.push(net("10.96.12.0/24"));
    let labels = BTreeMap::from([
        ("loams.dev/namespace".to_owned(), "7".to_owned()),
        ("loams.dev/instance".to_owned(), "orders-cdc".to_owned()),
    ]);
    let np = render_network_policy(&policy, "loams-7", "loams-egress-orders-cdc", &labels);
    let golden: Value =
        serde_json::from_str(include_str!("fixtures/network_policy.golden.json")).unwrap();
    assert_eq!(np, golden, "{}", serde_json::to_string_pretty(&np).unwrap());

    // Every denied CIDR the policy holds is excepted from the open block of
    // its family, and every allowed one is opened explicitly.
    let egress = np["spec"]["egress"].as_array().unwrap();
    let except: Vec<&str> = egress
        .iter()
        .flat_map(|r| r["to"].as_array().unwrap())
        .filter_map(|t| t["ipBlock"]["except"].as_array())
        .flatten()
        .map(|v| v.as_str().unwrap())
        .collect();
    for d in &policy.deny_cidrs {
        assert!(
            except.contains(&d.trunc().to_string().as_str()),
            "{d} excepted"
        );
    }
    assert!(
        egress
            .iter()
            .any(|r| r["to"][0]["ipBlock"]["cidr"] == "10.96.12.0/24")
    );
    // Rendering is deterministic.
    assert_eq!(
        np,
        render_network_policy(&policy, "loams-7", "loams-egress-orders-cdc", &labels)
    );
}

#[test]
fn overlay_refuses_bean_exec_file_schemes() {
    let refused = [
        (
            "exec",
            "- route:\n    from: { uri: \"timer:t\" }\n    steps:\n      - to: \"exec:/bin/sh?args=-c id\"\n",
        ),
        (
            "file",
            "- route:\n    from: { uri: \"file:/var/run/secrets?noop=true\" }\n    steps: []\n",
        ),
        (
            "bean",
            "- route:\n    from: { uri: \"timer:t\" }\n    steps:\n      - to: \"bean:runtime?method=exec\"\n",
        ),
        (
            "#bean",
            "- route:\n    from: { uri: \"kafka:t?headerDeserializer=#bean:evil\" }\n",
        ),
        (
            "#class",
            "- route:\n    from: { uri: \"timer:t\" }\n    steps:\n      - to: \"http:x?httpClientConfigurer=#class:com.evil.Hook\"\n",
        ),
        (
            "beans type",
            "- beans:\n    - name: x\n      type: com.evil.Gadget\n",
        ),
        (
            "groovy",
            "- route:\n    from: { uri: \"timer:t\" }\n    steps:\n      - setBody:\n          groovy: \"'id'.execute().text\"\n",
        ),
        (
            "env",
            "- route:\n    from: { uri: \"timer:t\" }\n    steps:\n      - setBody:\n          simple: \"{{env:AWS_SECRET_ACCESS_KEY}}\"\n",
        ),
        ("FILE upper", "- route:\n    from: { uri: \"FILE:/etc\" }\n"),
    ];
    for (what, yaml) in refused {
        let err = check_overlay(yaml).expect_err(what);
        assert!(!err.is_empty(), "{what}");
    }
    let clean = "- route:\n    id: loams-7-orders-sink\n    from: { uri: \"iggy:orders?consumerGroup=flow-orders\" }\n    steps:\n      - setHeader:\n          name: ce-type\n          constant: io.loams.dev.flow.http.delivery.v1\n      - setBody:\n          jq: \".data\"\n      - to: \"https://api.example.com/hook\"\n";
    check_overlay(clean).unwrap();
}
