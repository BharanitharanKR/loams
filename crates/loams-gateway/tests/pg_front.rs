//! The Postgres front door against a fake PgDog (GW1 Tasks 3–5): the
//! splice, the terminate path, refusals and limits.
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const ALPN: &[u8] = b"postgresql";

/// Reads to the end and returns what came back.
async fn read_all(s: &mut TcpStream) -> Vec<u8> {
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut out)).await;
    out
}

#[tokio::test]
async fn sslrequest_spliced_by_sni() {
    let up_ca = Ca::new("pgdog ca");
    let acme = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let other = fake_pgdog(&up_ca, &[WILDCARD], "other").await;
    let h = start(
        &up_ca,
        &(ns("acme", acme.addr, None) + &ns("other", other.addr, None)),
        Options::default(),
    )
    .await;
    let mut a = sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &up_ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut a).await, "acme:ping");
    let mut o = sslrequest_client(h.gateway.pg_addr(), "OTHER.gw.pg.test", true, &up_ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut o).await, "other:ping");
}

#[tokio::test]
async fn splice_presents_upstream_certificate() {
    // On the splice path the TLS session is PgDog's: a client that offers
    // ALPN postgresql gets none, because PgDog negotiates none, whereas the
    // gateway's own TLS would select it.
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    let mut c = sslrequest_client_alpn(
        h.gateway.pg_addr(),
        "acme.gw.pg.test",
        true,
        &up_ca.pem,
        Some(ALPN),
    )
    .await
    .unwrap();
    assert_eq!(
        c.get_ref().1.alpn_protocol(),
        None,
        "spliced, not terminated"
    );
    assert_eq!(ping(&mut c).await, "acme:ping");
}

#[tokio::test]
async fn unknown_sni_gets_unrecognized_name() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    let err = sslrequest_client(h.gateway.pg_addr(), "nope.gw.pg.test", true, &up_ca.pem)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("UnrecognisedName"), "{err}");
    // No SNI and no default namespace: refused too.
    let err = sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", false, &up_ca.pem)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("UnrecognisedName"), "{err}");
    assert_eq!(*pg.connections.lock().unwrap(), 0);
}

#[tokio::test]
async fn no_sni_uses_default_namespace() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "dev").await;
    let routes = format!(
        "[pg]\ndefault_namespace = \"dev\"\n\n{}",
        ns("dev", pg.addr, None)
    );
    let h = start(&up_ca, &routes, Options::default()).await;
    let mut c = sslrequest_client(h.gateway.pg_addr(), "dev.gw.pg.test", false, &up_ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut c).await, "dev:ping");
}

#[tokio::test]
async fn plaintext_startup_refused() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    let mut s = TcpStream::connect(h.gateway.pg_addr()).await.unwrap();
    // StartupMessage, protocol 3.0, user=app.
    let mut body = vec![0, 3, 0, 0];
    body.extend_from_slice(b"user\0app\0\0");
    let mut msg = ((body.len() + 4) as u32).to_be_bytes().to_vec();
    msg.extend_from_slice(&body);
    s.write_all(&msg).await.unwrap();
    let reply = read_all(&mut s).await;
    assert_eq!(reply[0], b'E');
    let text = String::from_utf8_lossy(&reply);
    assert!(text.contains("FATAL") && text.contains("28000"), "{text}");
    assert!(text.contains("requires TLS"), "{text}");
    assert_eq!(*pg.connections.lock().unwrap(), 0);
}

#[tokio::test]
async fn gssenc_answered_n_then_ssl() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    let mut s = TcpStream::connect(h.gateway.pg_addr()).await.unwrap();
    s.write_all(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x30])
        .await
        .unwrap();
    assert_eq!(s.read_u8().await.unwrap(), b'N');
    s.write_all(&SSL_REQUEST).await.unwrap();
    assert_eq!(s.read_u8().await.unwrap(), b'S');
}

#[tokio::test]
async fn bytes_after_sslrequest_closed() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    let mut s = TcpStream::connect(h.gateway.pg_addr()).await.unwrap();
    let mut pipelined = SSL_REQUEST.to_vec();
    pipelined.extend_from_slice(&[0x16, 3, 1, 0, 5, 1, 2, 3, 4, 5]);
    s.write_all(&pipelined).await.unwrap();
    assert!(
        read_all(&mut s).await.is_empty(),
        "no S for a pipelined SSLRequest"
    );
}

#[tokio::test]
async fn oversized_client_hello_refused() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    let mut s = TcpStream::connect(h.gateway.pg_addr()).await.unwrap();
    // A handshake record that claims to be large, then endless filler.
    s.write_all(&[0x16, 3, 1, 0x40, 0x00]).await.unwrap();
    let filler = vec![1u8; 4096];
    let mut written = 0;
    while written < 80 * 1024 {
        if s.write_all(&filler).await.is_err() {
            break;
        }
        written += filler.len();
    }
    let _ = read_all(&mut s).await;
    assert_eq!(*pg.connections.lock().unwrap(), 0);
}

#[tokio::test]
async fn handshake_deadline() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let opts = Options {
        handshake_timeout_ms: 300,
        ..Options::default()
    };
    let h = start(&up_ca, &ns("acme", pg.addr, None), opts).await;
    let mut s = TcpStream::connect(h.gateway.pg_addr()).await.unwrap();
    s.write_all(&[0, 0]).await.unwrap(); // half an opening, then silence
    let started = std::time::Instant::now();
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
        .await
        .unwrap()
        .unwrap_or(0);
    assert_eq!(n, 0, "closed");
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn cancel_on_shared_port_dropped() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    let mut s = TcpStream::connect(h.gateway.pg_addr()).await.unwrap();
    s.write_all(&[0, 0, 0, 16, 0x04, 0xd2, 0x16, 0x2e, 0, 0, 0, 1, 0, 0, 0, 2])
        .await
        .unwrap();
    let _ = read_all(&mut s).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(pg.cancels.lock().unwrap().is_empty());
}

#[tokio::test]
async fn upstream_n_closes() {
    let up_ca = Ca::new("pgdog ca");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg = fake_pgdog_on(listener, &up_ca, &[WILDCARD], "acme", b'N').await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    assert!(
        sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &up_ca.pem)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn first_healthy_upstream_is_used() {
    let up_ca = Ca::new("pgdog ca");
    let first = fake_pgdog(&up_ca, &[WILDCARD], "first").await;
    let second = fake_pgdog(&up_ca, &[WILDCARD], "second").await;
    let routes = format!(
        "[[pg.namespaces]]\nname = \"acme\"\nhostnames = [\"acme.gw.pg.test\"]\nupstreams = [\"{}\", \"{}\"]\n",
        first.addr, second.addr
    );
    let h = start(&up_ca, &routes, Options::default()).await;
    for _ in 0..3 {
        let mut c = sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &up_ca.pem)
            .await
            .unwrap();
        assert_eq!(ping(&mut c).await, "first:ping");
    }
}

#[tokio::test]
async fn failed_upstream_is_ejected_then_retried() {
    let up_ca = Ca::new("pgdog ca");
    let dead = free_port().await;
    let live = fake_pgdog(&up_ca, &[WILDCARD], "live").await;
    let routes = format!(
        "[[pg.namespaces]]\nname = \"acme\"\nhostnames = [\"acme.gw.pg.test\"]\nupstreams = [\"127.0.0.1:{dead}\", \"{}\"]\n",
        live.addr
    );
    let h = start(&up_ca, &routes, Options::default()).await;
    let mut c = sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &up_ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut c).await, "live:ping");
}

#[tokio::test]
async fn direct_tls_terminated_and_reencrypted() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    // The client sees the gateway's certificate and ALPN postgresql.
    let mut c = direct_client(
        h.gateway.pg_addr(),
        "acme.gw.pg.test",
        &up_ca.pem,
        Some(ALPN),
    )
    .await
    .unwrap();
    assert_eq!(c.get_ref().1.alpn_protocol(), Some(ALPN));
    assert_eq!(ping(&mut c).await, "acme:ping");
}

#[tokio::test]
async fn direct_without_alpn_refused() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    let err = direct_client(h.gateway.pg_addr(), "acme.gw.pg.test", &up_ca.pem, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("NoApplicationProtocol"), "{err}");
    let err = direct_client(
        h.gateway.pg_addr(),
        "acme.gw.pg.test",
        &up_ca.pem,
        Some(b"h2"),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("NoApplicationProtocol"), "{err}");
}

#[tokio::test]
async fn upstream_certificate_verified() {
    // PgDog's certificate is from a CA the gateway does not trust: the
    // gateway refuses before the client's TLS completes.
    let trusted = Ca::new("trusted ca");
    let rogue = Ca::new("rogue ca");
    let pg = fake_pgdog(&rogue, &[WILDCARD], "rogue").await;
    let h = start(&trusted, &ns("acme", pg.addr, None), Options::default()).await;
    let err = direct_client(
        h.gateway.pg_addr(),
        "acme.gw.pg.test",
        &trusted.pem,
        Some(ALPN),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("InternalError"), "{err}");
}

#[tokio::test]
async fn terminate_refused_without_pgdog_certificate() {
    // The gateway's certificate is not PgDog's: SCRAM channel binding would
    // fail at PgDog, so the gateway refuses up front.
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let opts = Options {
        own_gateway_cert: true,
        ..Options::default()
    };
    let h = start(&up_ca, &ns("acme", pg.addr, None), opts).await;
    let err = direct_client(
        h.gateway.pg_addr(),
        "acme.gw.pg.test",
        &up_ca.pem,
        Some(ALPN),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("InternalError"), "{err}");
}

#[tokio::test]
async fn terminate_mode_terminates_sslrequest() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let opts = Options {
        tls_mode: "terminate",
        ..Options::default()
    };
    let h = start(&up_ca, &ns("acme", pg.addr, None), opts).await;
    let mut c = sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &up_ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut c).await, "acme:ping");
}

#[tokio::test]
async fn passthrough_mode_refuses_direct() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let opts = Options {
        tls_mode: "passthrough",
        ..Options::default()
    };
    let h = start(&up_ca, &ns("acme", pg.addr, None), opts).await;
    let err = direct_client(
        h.gateway.pg_addr(),
        "acme.gw.pg.test",
        &up_ca.pem,
        Some(ALPN),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("NoApplicationProtocol"), "{err}");
    let mut c = sslrequest_client(h.gateway.pg_addr(), "acme.gw.pg.test", true, &up_ca.pem)
        .await
        .unwrap();
    assert_eq!(ping(&mut c).await, "acme:ping");
}

#[tokio::test]
async fn wildcard_certificate_selected() {
    let up_ca = Ca::new("pgdog ca");
    let pg = fake_pgdog(&up_ca, &[WILDCARD], "acme").await;
    let h = start(&up_ca, &ns("acme", pg.addr, None), Options::default()).await;
    // The gateway's only certificate is the wildcard (PgDog's); verify-full
    // of the namespace hostname passes against it, and an uncovered name is
    // refused before PgDog is dialled.
    let mut c = direct_client(
        h.gateway.pg_addr(),
        "acme.gw.pg.test",
        &up_ca.pem,
        Some(ALPN),
    )
    .await
    .unwrap();
    assert_eq!(ping(&mut c).await, "acme:ping");
    let before = *pg.connections.lock().unwrap();
    let routes = ns("acme", pg.addr, None)
        + "[[pg.namespaces]]\nname = \"odd\"\nhostnames = [\"odd.example.org\"]\n"
        + &format!("upstreams = [\"{}\"]\n", pg.addr);
    h.set_routes(&routes).await;
    let err = direct_client(
        h.gateway.pg_addr(),
        "odd.example.org",
        &up_ca.pem,
        Some(ALPN),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("UnrecognisedName"), "{err}");
    assert_eq!(*pg.connections.lock().unwrap(), before);
}
