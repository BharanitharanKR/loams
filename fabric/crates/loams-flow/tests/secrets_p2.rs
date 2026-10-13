//! CN2 Task 25: production secret stores (OR-21's `KubeSecretStore`, the dev
//! `FileSecretStore`), the fallback log filter, and no secret in a rendered
//! Kubernetes object other than a `Secret` (Review Focus 8).

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use loams_connector_sdk::{
    ErrorClass, Scrubber, ScrubbingResolver, Secret, SecretRef, SecretResolver,
};
use loams_flow::egress::{ClusterNetworks, EgressPolicy, render_network_policy};
use loams_flow::registry::Registry;
use loams_flow::secrets::file::FileSecretStore;
use loams_flow::secrets::kube::{
    KubeSecretStore, TokenSource, config_with_secret_files, render_secret, secret_mounts,
};
use loams_flow::secrets::logs::{FallbackLogFilter, mask_known_secret_options};
use loams_flow::secrets::{NamespaceResolver, SecretError, SecretStore};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PG_PASSWORD: &str = "s3cr3t/pg+value";
const API_TOKEN: &str = "tok_live_9f8e7d6c5b4a";

fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("secrets_p2-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A Kubernetes API server that serves one namespace's Secrets to one
/// service-account token.
async fn fake_kube_api(token: &'static str, rotated: &'static str, hits: Arc<AtomicUsize>) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            hits.fetch_add(1, Ordering::SeqCst);
            let mut buf = vec![0u8; 8192];
            let n = s.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            let line = req.lines().next().unwrap_or_default().to_owned();
            let authed = req.contains(&format!("authorization: Bearer {token}\r\n"))
                || req.contains(&format!("authorization: Bearer {rotated}\r\n"));
            let (status, body) = if !authed {
                (
                    "403 Forbidden",
                    r#"{"kind":"Status","code":403}"#.to_owned(),
                )
            } else if line.starts_with("GET /api/v1/namespaces/loams-7/secrets/pg-creds ") {
                (
                    "200 OK",
                    json!({
                        "kind": "Secret", "apiVersion": "v1",
                        "metadata": {"name": "pg-creds", "namespace": "loams-7"},
                        "type": "Opaque",
                        "data": {
                            "password": STANDARD.encode(PG_PASSWORD),
                            "user": STANDARD.encode("app"),
                        }
                    })
                    .to_string(),
                )
            } else if line.starts_with("GET /api/v1/namespaces/loams-7/secrets/api-token ") {
                (
                    "200 OK",
                    json!({"kind": "Secret", "data": {"value": STANDARD.encode(API_TOKEN)}})
                        .to_string(),
                )
            } else {
                (
                    "404 Not Found",
                    r#"{"kind":"Status","code":404}"#.to_owned(),
                )
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(resp.as_bytes()).await;
        }
    });
    port
}

#[tokio::test]
async fn kube_secret_store_reads_secret() {
    let dir = scratch("kube");
    let token_file = dir.join("token");
    std::fs::write(&token_file, "sa-token-1\n").unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let port = fake_kube_api("sa-token-1", "sa-token-2", hits.clone()).await;

    let store = KubeSecretStore::new(
        &format!("http://127.0.0.1:{port}"),
        TokenSource::File(token_file.clone()),
    )
    .unwrap();
    assert_eq!(store.kube_namespace(7), "loams-7");

    let s = store
        .resolve(7, &SecretRef::new("pg-creds/password"))
        .await
        .unwrap();
    assert_eq!(s.expose(), PG_PASSWORD.as_bytes());
    // A bare name reads the key `value`.
    let s = store
        .resolve(7, &SecretRef::new("api-token"))
        .await
        .unwrap();
    assert_eq!(s.expose(), API_TOKEN.as_bytes());

    // Bound service-account tokens rotate: the file is read per request.
    std::fs::write(&token_file, "sa-token-2").unwrap();
    store
        .resolve(7, &SecretRef::new("pg-creds/user"))
        .await
        .unwrap();

    // Missing secret, missing key, another namespace, bad names.
    assert!(matches!(
        store.resolve(7, &SecretRef::new("nope/password")).await,
        Err(SecretError::NotFound { .. })
    ));
    assert!(matches!(
        store.resolve(7, &SecretRef::new("pg-creds/missing")).await,
        Err(SecretError::NotFound { .. })
    ));
    assert!(matches!(
        store.resolve(8, &SecretRef::new("pg-creds/password")).await,
        Err(SecretError::NotFound { .. })
    ));
    for bad in [
        "../kube-system/x",
        "pg-creds/../../x",
        "PG/x",
        "",
        "a/b/c",
        "pg-creds/",
    ] {
        assert!(
            matches!(
                store.resolve(7, &SecretRef::new(bad)).await,
                Err(SecretError::InvalidName { .. })
            ),
            "{bad:?} refused before any request"
        );
    }
    let before = hits.load(Ordering::SeqCst);

    // A revoked token is `Denied`, which the SDK sees as `Auth`.
    std::fs::write(&token_file, "revoked").unwrap();
    let err = store
        .resolve(7, &SecretRef::new("pg-creds/password"))
        .await
        .unwrap_err();
    assert!(matches!(err, SecretError::Denied { .. }));
    assert!(!format!("{err} {err:?}").contains("revoked"));
    assert_eq!(hits.load(Ordering::SeqCst), before + 1);
    std::fs::write(&token_file, "sa-token-1").unwrap();

    // Native connectors read it through the SDK's `SecretResolver`.
    let resolver: Arc<dyn SecretResolver> = Arc::new(NamespaceResolver::new(Arc::new(store), 7));
    let s = resolver
        .resolve(&SecretRef::new("pg-creds/password"))
        .await
        .unwrap();
    assert_eq!(s.expose(), PG_PASSWORD.as_bytes());
    let err = resolver.resolve(&SecretRef::new("nope")).await.unwrap_err();
    assert_eq!(err.class, ErrorClass::Auth);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn file_secret_store_reads_owner_only_files() {
    let dir = scratch("file");
    let secret_dir = dir.join("7/pg-creds");
    std::fs::create_dir_all(&secret_dir).unwrap();
    std::fs::write(secret_dir.join("password"), PG_PASSWORD).unwrap();
    std::fs::write(secret_dir.join("loose"), "world-readable").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            secret_dir.join("password"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        std::fs::set_permissions(
            secret_dir.join("loose"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
    }
    let store = FileSecretStore::new(&dir);
    let s = store
        .resolve(7, &SecretRef::new("pg-creds/password"))
        .await
        .unwrap();
    assert_eq!(s.expose(), PG_PASSWORD.as_bytes());
    #[cfg(unix)]
    assert!(matches!(
        store.resolve(7, &SecretRef::new("pg-creds/loose")).await,
        Err(SecretError::Denied { .. })
    ));
    assert!(matches!(
        store.resolve(8, &SecretRef::new("pg-creds/password")).await,
        Err(SecretError::NotFound { .. })
    ));
    assert!(matches!(
        store.resolve(7, &SecretRef::new("../7/pg-creds")).await,
        Err(SecretError::InvalidName { .. })
    ));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn no_secret_in_fallback_logs() {
    let dir = scratch("logs");
    let secret_dir = dir.join("7/pg-creds");
    std::fs::create_dir_all(&secret_dir).unwrap();
    std::fs::write(secret_dir.join("password"), PG_PASSWORD).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            secret_dir.join("password"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }
    // The supervisor resolves the instance's secrets through a scrubbing
    // resolver, which records them for its line filter.
    let scrubber = Scrubber::new();
    let resolver = ScrubbingResolver::new(
        Arc::new(NamespaceResolver::new(
            Arc::new(FileSecretStore::new(&dir)),
            7,
        )),
        scrubber.clone(),
    );
    resolver
        .resolve(&SecretRef::new("pg-creds/password"))
        .await
        .unwrap();

    // Known option names are masked even when the value is not a resolved
    // secret (a fallback that echoes its own config).
    assert_eq!(
        mask_known_secret_options("database.password=abc123 user=app"),
        "database.password=**** user=app"
    );
    assert_eq!(
        mask_known_secret_options(r#"{"clientSecret": "xyz", "region": "eu-west-1"}"#),
        r#"{"clientSecret": "****", "region": "eu-west-1"}"#
    );
    assert_eq!(
        mask_known_secret_options(
            "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";"
        ),
        "sasl.jaas.config=**** required username=\"u\" password=\"****\";"
    );
    assert_eq!(
        mask_known_secret_options("connecting to mongodb://app:pw@db:27017/x"),
        "connecting to mongodb://app:****@db:27017/x"
    );

    let filter = FallbackLogFilter::new(scrubber.clone());
    let b64 = STANDARD.encode(PG_PASSWORD);
    let log = format!(
        "INFO Started route loams-7-orders-sink\n\
         DEBUG jdbc:postgresql://db:5432/x user=app password={PG_PASSWORD}\n\
         ERROR authentication failed for {PG_PASSWORD} at db:5432\n\
         TRACE secret volume bytes {b64}\n\
         WARN retrying https://db.example.com/q?pw=s3cr3t%2Fpg%2Bvalue\n\
         INFO Stopped route loams-7-orders-sink\n"
    );
    let mut out = Vec::new();
    filter.pump(log.as_bytes(), &mut out).await.unwrap();
    let out = String::from_utf8(out).unwrap();
    assert!(!scrubber.contains(&out), "a secret reached the log:\n{out}");
    assert_eq!(filter.dropped(), 3, "{out}");
    let kept: Vec<&str> = out.lines().collect();
    assert_eq!(
        kept,
        [
            "INFO Started route loams-7-orders-sink",
            "DEBUG jdbc:postgresql://db:5432/x user=app password=****",
            "INFO Stopped route loams-7-orders-sink",
        ]
    );
    let _ = std::fs::remove_dir_all(dir);
}

fn walk_strings<'a>(v: &'a Value, out: &mut Vec<&'a str>) {
    match v {
        Value::String(s) => out.push(s),
        Value::Array(a) => a.iter().for_each(|x| walk_strings(x, out)),
        Value::Object(o) => o.iter().for_each(|(k, x)| {
            out.push(k);
            walk_strings(x, out);
        }),
        _ => {}
    }
}

#[test]
fn no_secret_in_rendered_kube_objects() {
    let scrubber = Scrubber::new();
    scrubber.add(&Secret::new(PG_PASSWORD));
    scrubber.add(&Secret::new(API_TOKEN));

    let registry =
        Registry::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../connectors/registry"))
            .unwrap();
    let pg = registry.get("postgresql").unwrap();
    let config = json!({
        "host": "db.example.com", "database": "orders", "user": "app",
        "password": "pg-creds/password",
        "tls": {"key_pem": "pg-creds/tls_key"}
    });
    let refs = [
        SecretRef::new("pg-creds/password"),
        SecretRef::new("pg-creds/tls_key"),
        SecretRef::new("api-token"),
    ];
    let mounts = secret_mounts(&refs, "/etc/loams/secrets").unwrap();
    let rendered_config = config_with_secret_files(pg, &config, &mounts).unwrap();
    assert_eq!(
        rendered_config["password"],
        json!({"file": "/etc/loams/secrets/pg-creds/password"})
    );
    assert_eq!(
        rendered_config["tls"]["key_pem"],
        json!({"file": "/etc/loams/secrets/pg-creds/tls_key"})
    );
    let np = render_network_policy(
        &EgressPolicy::for_cluster(&ClusterNetworks::default()),
        "loams-7",
        "loams-egress-orders",
        &BTreeMap::from([("loams.dev/instance".to_owned(), "orders".to_owned())]),
    );
    let objects = [
        json!({"apiVersion": "v1", "kind": "ConfigMap",
               "metadata": {"name": "orders", "namespace": "loams-7"},
               "data": {"config.json": rendered_config.to_string()}}),
        json!({"kind": "PodSpecFragment", "volumes": mounts.volumes, "volumeMounts": mounts.volume_mounts}),
        np,
    ];
    for o in &objects {
        let mut strings = Vec::new();
        walk_strings(o, &mut strings);
        for s in strings {
            assert!(
                !scrubber.contains(s),
                "secret in a {} object: {s}",
                o["kind"]
            );
        }
        assert!(!scrubber.contains(&o.to_string()));
    }
    // Mounted read-only, owner-read only.
    for m in &mounts.volume_mounts {
        assert_eq!(m["readOnly"], true);
    }
    for v in &mounts.volumes {
        assert_eq!(v["secret"]["defaultMode"], 0o400);
    }
    assert_eq!(mounts.volumes.len(), 2, "one volume per Secret");

    // The only object that may carry the bytes is a `Secret`.
    let secret = render_secret(
        "loams-7",
        "pg-creds",
        &BTreeMap::from([("password".to_owned(), Secret::new(PG_PASSWORD))]),
    );
    assert_eq!(secret["kind"], "Secret");
    assert!(scrubber.contains(&secret.to_string()));
}
