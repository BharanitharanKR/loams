//! pgdog-config-sync (PG2 Task 21): mutual TLS, atomic writes, the
//! generation route, and the persisted copy at start.
#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use loams_pgdog_sync::{
    Config, GENERATION, PGDOG_TOML, Server, USERS_TOML, load_persisted, read_generation,
    server_tls, write_config,
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};

struct Pki {
    ca: String,
    server_cert: String,
    server_key: String,
    client: (String, String),
    stranger: (String, String),
}

fn ca(name: &str) -> (KeyPair, CertificateParams) {
    let mut p = CertificateParams::new(vec![]).unwrap();
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.distinguished_name.push(rcgen::DnType::CommonName, name);
    (KeyPair::generate().unwrap(), p)
}

fn ca_pem(key: &KeyPair, params: &CertificateParams) -> String {
    params.clone().self_signed(key).unwrap().pem()
}

fn leaf(names: Vec<String>, ca_key: &KeyPair, ca_params: &CertificateParams) -> (String, String) {
    let k = KeyPair::generate().unwrap();
    let c = CertificateParams::new(names)
        .unwrap()
        .signed_by(&k, &rcgen::Issuer::new(ca_params.clone(), ca_key))
        .unwrap();
    (c.pem(), k.serialize_pem())
}

fn pki() -> Pki {
    let (ca_key, ca_params) = ca("pg-control CA");
    let (server_cert, server_key) = leaf(vec!["localhost".into()], &ca_key, &ca_params);
    let client = leaf(vec!["pg-control".into()], &ca_key, &ca_params);
    let (other_key, other_params) = ca("another CA");
    let stranger = leaf(vec!["pg-control".into()], &other_key, &other_params);
    Pki {
        ca: ca_pem(&ca_key, &ca_params),
        server_cert,
        server_key,
        client,
        stranger,
    }
}

async fn serve(dir: &Path, pki: &Pki) -> SocketAddr {
    let tls = server_tls(
        pki.server_cert.as_bytes(),
        pki.server_key.as_bytes(),
        pki.ca.as_bytes(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = Server {
        dir: dir.to_path_buf(),
        tls: Arc::new(tls),
    };
    tokio::spawn(server.serve(listener, std::future::pending()));
    addr
}

fn client_tls(pki: &Pki, identity: Option<&(String, String)>) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    for c in CertificateDer::pem_slice_iter(pki.ca.as_bytes()) {
        roots.add(c.unwrap()).unwrap();
    }
    let b = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots);
    Arc::new(match identity {
        Some((cert, key)) => b
            .with_client_auth_cert(
                CertificateDer::pem_slice_iter(cert.as_bytes())
                    .collect::<Result<_, _>>()
                    .unwrap(),
                PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap(),
            )
            .unwrap(),
        None => b.with_no_client_auth(),
    })
}

/// One request; `Err` when the connection or handshake fails.
async fn call(
    addr: SocketAddr,
    tls: Arc<rustls::ClientConfig>,
    method: &str,
    path: &str,
    body: String,
) -> Result<(u16, String), String> {
    let tcp = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| e.to_string())?;
    let tls = tokio_rustls::TlsConnector::from(tls)
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .map_err(|e| e.to_string())?;
    let (mut send, conn) = hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tls))
        .await
        .map_err(|e| e.to_string())?;
    tokio::spawn(conn);
    let req = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", "localhost")
        .body(Full::new(Bytes::from(body)))
        .unwrap();
    let resp = send.send_request(req).await.map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| e.to_string())?
        .to_bytes();
    Ok((status, String::from_utf8_lossy(&body).into_owned()))
}

fn config(generation: u64, fill: char) -> Config {
    Config {
        generation,
        pgdog_toml: std::iter::repeat_n(fill, 200_000).collect(),
        users_toml: format!("# users {generation}"),
    }
}

/// Only a client with a certificate from pg-control's CA is served.
#[tokio::test]
async fn sync_requires_mtls() {
    let dir = tempfile::tempdir().unwrap();
    let pki = pki();
    let addr = serve(dir.path(), &pki).await;
    let body = serde_json::to_string(&config(7, 'a')).unwrap();
    assert!(
        call(
            addr,
            client_tls(&pki, None),
            "PUT",
            "/v1/config",
            body.clone()
        )
        .await
        .is_err()
    );
    assert!(
        call(
            addr,
            client_tls(&pki, Some(&pki.stranger)),
            "PUT",
            "/v1/config",
            body.clone()
        )
        .await
        .is_err()
    );
    assert_eq!(read_generation(dir.path()), None, "nothing written");
    let (status, answer) = call(
        addr,
        client_tls(&pki, Some(&pki.client)),
        "PUT",
        "/v1/config",
        body,
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "{answer}");
    let (status, answer) = call(
        addr,
        client_tls(&pki, Some(&pki.client)),
        "GET",
        "/v1/generation",
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!((status, answer.as_str()), (200, r#"{"generation":7}"#));
    assert_eq!(
        std::fs::read_to_string(dir.path().join(USERS_TOML)).unwrap(),
        "# users 7"
    );
    let (status, _) = call(
        addr,
        client_tls(&pki, Some(&pki.client)),
        "PUT",
        "/v1/config",
        "{}".into(),
    )
    .await
    .unwrap();
    assert_eq!(status, 400);
}

/// A reader of the files while new ones are written sees each file whole:
/// one config's or the next's, never a mix inside a file.
#[test]
fn sync_writes_atomically() {
    let dir = tempfile::tempdir().unwrap();
    write_config(dir.path(), &config(1, 'a')).unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let reader = {
        let (dir, done) = (dir.path().to_path_buf(), done.clone());
        std::thread::spawn(move || {
            let mut reads = 0;
            while !done.load(Ordering::SeqCst) {
                let text = std::fs::read_to_string(dir.join(PGDOG_TOML)).unwrap();
                assert_eq!(text.len(), 200_000, "a partial file");
                let first = text.chars().next().unwrap();
                assert!(text.chars().all(|c| c == first), "a mixed file");
                reads += 1;
            }
            reads
        })
    };
    for i in 0..200u64 {
        let fill = if i % 2 == 0 { 'b' } else { 'c' };
        write_config(dir.path(), &config(i + 2, fill)).unwrap();
    }
    done.store(true, Ordering::SeqCst);
    assert!(reader.join().unwrap() > 0);
    assert_eq!(read_generation(dir.path()), Some(201));
    assert!(!dir.path().join(format!(".{PGDOG_TOML}.tmp")).exists());
}

/// A restarted sidecar (an emptied directory) loads the persisted copy, so
/// PgDog serves the last config before pg-control pushes again; a directory
/// that already holds a generation is left alone.
#[test]
fn pgdog_restart_loads_persisted_copy() {
    let persisted = tempfile::tempdir().unwrap();
    std::fs::write(persisted.path().join(PGDOG_TOML), "[general]\n").unwrap();
    std::fs::write(persisted.path().join(USERS_TOML), "# users\n").unwrap();
    std::fs::write(persisted.path().join(GENERATION), "42").unwrap();
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        load_persisted(dir.path(), persisted.path()).unwrap(),
        Some(42)
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join(PGDOG_TOML)).unwrap(),
        "[general]\n"
    );
    write_config(dir.path(), &config(43, 'z')).unwrap();
    assert_eq!(
        load_persisted(dir.path(), persisted.path()).unwrap(),
        Some(43)
    );
    let empty = tempfile::tempdir().unwrap();
    let fresh = tempfile::tempdir().unwrap();
    assert_eq!(load_persisted(fresh.path(), empty.path()).unwrap(), None);
}
