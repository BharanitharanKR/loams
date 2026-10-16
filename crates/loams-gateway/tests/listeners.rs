//! Dedicated ports, reload and the connection cap (GW1 Task 6), and the
//! drain (Task 8).
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::*;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

async fn dedicated(h: &Harness, port: u16) -> std::net::SocketAddr {
    let _ = h;
    std::net::SocketAddr::from(([127, 0, 0, 1], port))
}

/// Waits until a dedicated port accepts.
async fn wait_bound(port: u16) {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("port {port} never bound");
}

#[tokio::test]
async fn dedicated_port_routes_without_sni() {
    let ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&ca, &[WILDCARD], "acme").await;
    let port = free_port().await;
    let h = start(&ca, &ns("acme", pg.addr, Some(port)), Options::default()).await;
    wait_bound(port).await;
    let mut c = sslrequest_client(dedicated(&h, port).await, "acme.gw.pg.test", false, &ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut c).await, "acme:ping");
}

#[tokio::test]
async fn dedicated_port_ignores_sni() {
    let ca = Ca::new("pgdog ca");
    let acme = fake_pgdog(&ca, &[WILDCARD], "acme").await;
    let beta = fake_pgdog(&ca, &[WILDCARD], "beta").await;
    let port = free_port().await;
    let routes = ns("acme", acme.addr, Some(port)) + &ns("beta", beta.addr, None);
    let h = start(&ca, &routes, Options::default()).await;
    wait_bound(port).await;
    // SNI names beta, but the port is acme's.
    let mut c = sslrequest_client(dedicated(&h, port).await, "beta.gw.pg.test", true, &ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut c).await, "acme:ping");
}

#[tokio::test]
async fn new_port_bound_on_reload() {
    let ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&ca, &[WILDCARD], "acme").await;
    let h = start(&ca, &ns("acme", pg.addr, None), Options::default()).await;
    let port = free_port().await;
    h.set_routes(&ns("acme", pg.addr, Some(port))).await;
    wait_bound(port).await;
    let mut c = sslrequest_client(dedicated(&h, port).await, "acme.gw.pg.test", false, &ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut c).await, "acme:ping");
}

#[tokio::test]
async fn removed_port_stops_accepting() {
    let ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&ca, &[WILDCARD], "acme").await;
    let port = free_port().await;
    let h = start(&ca, &ns("acme", pg.addr, Some(port)), Options::default()).await;
    wait_bound(port).await;
    h.set_routes(&ns("acme", pg.addr, None)).await;
    let mut refused = false;
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_err() {
            refused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(refused, "port {port} still accepts after leaving the table");
}

#[tokio::test]
async fn reload_keeps_live_connection() {
    let ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&ca, &[WILDCARD], "acme").await;
    let port = free_port().await;
    let h = start(&ca, &ns("acme", pg.addr, Some(port)), Options::default()).await;
    wait_bound(port).await;
    let mut shared = sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &ca.pem)
        .await
        .unwrap();
    let mut on_port =
        sslrequest_client(dedicated(&h, port).await, "acme.gw.pg.test", false, &ca.pem)
            .await
            .unwrap();
    assert_eq!(ping(&mut shared).await, "acme:ping");
    // The namespace and its port leave the table.
    h.set_routes("").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(ping(&mut shared).await, "acme:ping");
    assert_eq!(ping(&mut on_port).await, "acme:ping");
    // New connections are refused.
    assert!(
        sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &ca.pem)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn connection_cap_enforced() {
    let ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&ca, &[WILDCARD], "acme").await;
    let opts = Options {
        max_connections: 1,
        ..Options::default()
    };
    let h = start(&ca, &ns("acme", pg.addr, None), opts).await;
    let mut first = sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut first).await, "acme:ping");
    assert!(
        sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &ca.pem)
            .await
            .is_err(),
        "a second connection over the cap is closed"
    );
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut again = sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut again).await, "acme:ping");
}

#[tokio::test]
async fn cancel_on_dedicated_port_reaches_every_replica() {
    let ca = Ca::new("pgdog ca");
    let one = fake_pgdog(&ca, &[WILDCARD], "one").await;
    let two = fake_pgdog(&ca, &[WILDCARD], "two").await;
    let port = free_port().await;
    let routes = format!(
        "[[pg.namespaces]]\nname = \"acme\"\nhostnames = [\"acme.gw.pg.test\"]\nport = {port}\nupstreams = [\"{}\", \"{}\"]\n",
        one.addr, two.addr
    );
    let h = start(&ca, &routes, Options::default()).await;
    wait_bound(port).await;
    let packet = [0, 0, 0, 16, 0x04, 0xd2, 0x16, 0x2e, 0, 0, 0, 7, 0, 0, 0, 9];
    let mut s = TcpStream::connect(dedicated(&h, port).await).await.unwrap();
    s.write_all(&packet).await.unwrap();
    for _ in 0..100 {
        if one.cancels.lock().unwrap().len() == 1 && two.cancels.lock().unwrap().len() == 1 {
            assert_eq!(one.cancels.lock().unwrap()[0], packet);
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the cancel did not reach both replicas");
}

#[tokio::test]
async fn sigterm_drains_then_exits() {
    // Shutdown stops accepting at once and waits for live relays, up to the
    // drain timeout.
    let ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&ca, &[WILDCARD], "acme").await;
    let opts = Options {
        drain_timeout_ms: 500,
        ..Options::default()
    };
    let h = start(&ca, &ns("acme", pg.addr, None), opts).await;
    let addr = h.gateway.pg_addr();
    let mut live = sslrequest_client(addr, "acme.gw.pg.test", true, &ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut live).await, "acme:ping");
    let started = std::time::Instant::now();
    let Harness { gateway, dir, .. } = h;
    gateway.shutdown().await;
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(400),
        "waited for the live relay ({took:?})"
    );
    assert!(
        took < Duration::from_secs(3),
        "gave up at the drain timeout ({took:?})"
    );
    assert!(
        TcpStream::connect(addr).await.is_err(),
        "no longer accepting"
    );
    drop(dir);
}
