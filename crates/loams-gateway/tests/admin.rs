//! The admin listener (GW1 Task 1) and the static config.
#![allow(clippy::unwrap_used)]

mod common;

use std::net::SocketAddr;

use loams_gateway::config::{GatewayConfig, TlsMode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    let status = out[9..12].parse().unwrap();
    let body = out.split("\r\n\r\n").nth(1).unwrap_or_default().to_owned();
    (status, body)
}

#[test]
fn config_defaults_match_the_design() {
    let c = GatewayConfig::from_toml(
        "[routes]\npath = \"/r.toml\"\n[pg]\ntls_mode = \"passthrough\"\n",
    )
    .unwrap();
    assert_eq!(c.admin.listen, "0.0.0.0:9187".parse().unwrap());
    assert_eq!(c.routes.poll_interval_ms, 1000);
    assert!(c.routes.require_generation);
    assert_eq!(c.pg.listen, "0.0.0.0:5432".parse().unwrap());
    assert_eq!(c.pg.dedicated_ports.0, 16000..=17999);
    assert_eq!(c.pg.tls_mode, TlsMode::Passthrough);
    assert_eq!(c.pg.handshake_timeout_ms, 10_000);
    assert_eq!(c.pg.max_connections, 20_000);
    assert_eq!(c.pg.drain_timeout_ms, 60_000);
}

#[test]
fn config_rejects_unknown_fields() {
    assert!(GatewayConfig::from_toml("[routes]\npath = \"/r\"\nbogus = 1\n").is_err());
    // auto (the default) terminates direct TLS, so it needs a certificate.
    let e = GatewayConfig::from_toml("[routes]\npath = \"/r\"\n").unwrap_err();
    assert!(e.to_string().contains("certs"), "{e}");
    assert!(
        GatewayConfig::from_toml(
            "[routes]\npath = \"/r\"\n[pg]\ntls_mode = \"passthrough\"\ndedicated_ports = \"9-1\"\n"
        )
        .is_err()
    );
}

#[tokio::test]
async fn readyz_waits_for_routes() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let path = dir.path().join("routes.toml");
    let config = format!(
        "[admin]\nlisten = \"127.0.0.1:0\"\n[routes]\npath = \"{}\"\npoll_interval_ms = 20\n[pg]\nlisten = \"127.0.0.1:0\"\ntls_mode = \"passthrough\"\n",
        path.display()
    );
    let g = loams_gateway::Gateway::start(GatewayConfig::from_toml(&config).unwrap())
        .await
        .unwrap();
    assert_eq!(get(g.admin_addr(), "/healthz").await.0, 200);
    assert_eq!(get(g.admin_addr(), "/readyz").await.0, 503);
    let routes = common::ns("acme", "127.0.0.1:6432".parse().unwrap(), None);
    std::fs::write(&path, loams_gateway::routes::with_generation(&routes)).unwrap();
    let mut rx = g.routes();
    rx.wait_for(|t| t.is_some()).await.unwrap();
    assert_eq!(get(g.admin_addr(), "/readyz").await.0, 200);
    assert_eq!(get(g.admin_addr(), "/nope").await.0, 404);
    g.shutdown().await;
}

#[tokio::test]
async fn routes_endpoint_shows_generation_only() {
    let ca = common::Ca::new("pgdog ca");
    let routes = common::ns("acme", "127.0.0.1:6432".parse().unwrap(), None);
    let h = common::start(&ca, &routes, common::Options::default()).await;
    let (status, body) = get(h.gateway.admin_addr(), "/v1/routes").await;
    assert_eq!(status, 200);
    let v: serde_json::Value = serde_json::from_str(body.trim()).unwrap();
    assert_eq!(
        v["generation"],
        loams_gateway::routes::generation_of(&routes)
    );
    assert_eq!(v["pg_namespaces"], 1);
    assert!(!body.contains("acme"), "no hostnames or upstreams: {body}");
    assert!(!body.contains("6432"), "no hostnames or upstreams: {body}");
}
