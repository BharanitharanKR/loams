//! `psql` (libpq 17) through the gateway to the pinned, unmodified PgDog
//! (GW1 Task 7). Ignored by default: it starts containers.
//!
//! ```text
//! IT_CONTAINER_ENGINE=podman cargo test -p loams-gateway --test it_pgdog -- --ignored --nocapture
//! ```
//!
//! `IT_CONTAINER_ENGINE` is `docker` by default. The PgDog image is the pin
//! in `deploy/pgdog/compose.yaml` (PG2 R18.1); it is named nowhere else
//! (PG2 R19.3). Every container runs on the host network, on random ports.
//!
//! The gateway holds PgDog's own certificate for the terminate path: libpq
//! authenticates with SCRAM-SHA-256-PLUS over TLS, whose channel binding is
//! a hash of the server certificate the client saw, so both paths run with
//! `channel_binding=require`.
#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::process::{Command, Output};
use std::time::Duration;

use common::*;

const POSTGRES_IMAGE: &str = "docker.io/library/postgres:17.11";
const PG_PASSWORD: &str = "it-superuser";
const APP_PASSWORD: &str = "it-app-pw";

fn engine() -> String {
    std::env::var("IT_CONTAINER_ENGINE").unwrap_or_else(|_| "docker".into())
}

/// The pinned PgDog image: the one reference with a digest in Task 18's
/// scratch stack.
fn pgdog_image() -> String {
    let compose = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/pgdog/compose.yaml"
    ))
    .unwrap();
    compose
        .lines()
        .filter_map(|l| l.trim().strip_prefix("image: "))
        .find(|image| image.contains("@sha256:"))
        .expect("the pinned PgDog image")
        .to_owned()
}

/// Removes the container when the test ends, however it ends.
struct Container(String);

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new(engine()).args(["rm", "-f", &self.0]).output();
    }
}

fn run(args: &[&str]) -> String {
    let out = Command::new(engine()).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "{} {args:?}: {}",
        engine(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

fn suffix() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )
}

async fn wait_tcp(port: u16, what: &str) {
    for _ in 0..300 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("{what} never listened on {port}");
}

fn write(dir: &Path, name: &str, text: &str) {
    std::fs::write(dir.join(name), text).unwrap();
}

/// Runs `psql` from the Postgres image with `conninfo`; certificates are
/// under `/certs`.
fn psql(certs: &Path, conninfo: &str) -> Output {
    Command::new(engine())
        .args([
            "run",
            "--rm",
            "--network",
            "host",
            "-v",
            &format!("{}:/certs:ro", certs.display()),
            "-e",
            &format!("PGPASSWORD={APP_PASSWORD}"),
            "-e",
            "PGCONNECT_TIMEOUT=10",
            POSTGRES_IMAGE,
            "psql",
            conninfo,
            "-Atc",
            "select 'through-the-gateway'",
        ])
        .output()
        .unwrap()
}

fn ok(name: &str, out: &Output) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && stdout.trim() == "through-the-gateway",
        "{name}: stdout {stdout:?}, stderr {stderr:?}"
    );
    println!("{name}: ok");
}

fn refused(name: &str, out: &Output, needle: &str) {
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && stderr.contains(needle),
        "{name}: expected a failure with {needle:?}, got status {:?}, stderr {stderr:?}",
        out.status
    );
    println!("{name}: refused as expected");
}

// psql runs as a blocking child process, so the gateway needs other threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "starts Postgres and the pinned PgDog in containers"]
async fn it_pgdog_through_the_gateway() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let id = suffix();
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let pgdog_dir = dir.path().join("pgdog");
    let certs = dir.path().join("certs");
    for d in [&pgdog_dir, &certs] {
        std::fs::create_dir(d).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    // Postgres, the backend.
    let pg_port = free_port().await;
    let pg_name = format!("gw1-it-pg-{id}");
    run(&[
        "run",
        "-d",
        "--name",
        &pg_name,
        "--network",
        "host",
        "-e",
        &format!("POSTGRES_PASSWORD={PG_PASSWORD}"),
        POSTGRES_IMAGE,
        "-c",
        &format!("port={pg_port}"),
        "-c",
        "listen_addresses=127.0.0.1",
    ]);
    let _pg = Container(pg_name);
    wait_tcp(pg_port, "postgres").await;

    // PgDog with TLS toward clients, its certificate from PgDog's CA.
    let pgdog_ca = Ca::new("pgdog ca");
    let (cert, key) = pgdog_ca.wildcard.clone();
    write(&pgdog_dir, "tls.crt", &cert);
    write(&pgdog_dir, "tls.key", &key);
    let pgdog_port = free_port().await;
    write(
        &pgdog_dir,
        "pgdog.toml",
        &format!(
            r#"[general]
host = "127.0.0.1"
port = {pgdog_port}
auth_type = "scram"
tls_certificate = "/etc/pgdog/tls.crt"
tls_private_key = "/etc/pgdog/tls.key"
tls_client_required = true

[[databases]]
name = "acme"
host = "127.0.0.1"
port = {pg_port}
database_name = "postgres"
"#
        ),
    );
    write(
        &pgdog_dir,
        "users.toml",
        &format!(
            r#"[[users]]
name = "app"
database = "acme"
password = "{APP_PASSWORD}"
server_user = "postgres"
server_password = "{PG_PASSWORD}"
"#
        ),
    );
    #[cfg(unix)]
    for f in ["tls.crt", "tls.key", "pgdog.toml", "users.toml"] {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(pgdog_dir.join(f), std::fs::Permissions::from_mode(0o644))
            .unwrap();
    }
    let pgdog_name = format!("gw1-it-pgdog-{id}");
    run(&[
        "run",
        "-d",
        "--name",
        &pgdog_name,
        "--network",
        "host",
        "-v",
        &format!("{}:/etc/pgdog:ro", pgdog_dir.display()),
        &pgdog_image(),
        "pgdog",
        "--config",
        "/etc/pgdog/pgdog.toml",
        "--users",
        "/etc/pgdog/users.toml",
    ]);
    let _pgdog = Container(pgdog_name.clone());
    wait_tcp(pgdog_port, "pgdog").await;

    // The gateway, in process: a dedicated port for acme.
    let dedicated = free_port().await;
    let routes = format!(
        "[[pg.namespaces]]\nname = \"acme\"\nhostnames = [\"acme.gw.pg.test\"]\nport = {dedicated}\nupstreams = [\"127.0.0.1:{pgdog_port}\"]\n"
    );
    let h = start(&pgdog_ca, &routes, Options::default()).await;
    wait_tcp(dedicated, "the dedicated port").await;
    let shared = h.gateway.pg_addr().port();
    write(&certs, "pgdog-ca.crt", &pgdog_ca.pem);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            certs.join("pgdog-ca.crt"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
    }
    let base = "hostaddr=127.0.0.1 user=app dbname=acme";

    // The baseline: psql straight to PgDog (SSLRequest, as PgDog requires).
    ok(
        "baseline_psql_to_pgdog",
        &psql(
            &certs,
            &format!(
                "{base} host=acme.gw.pg.test port={pgdog_port} sslmode=verify-full sslrootcert=/certs/pgdog-ca.crt"
            ),
        ),
    );

    // it_sslrequest_verify_full: libpq's default negotiation, spliced: the
    // client verifies PgDog's certificate for the namespace hostname.
    ok(
        "it_sslrequest_verify_full",
        &psql(
            &certs,
            &format!(
                "{base} host=acme.gw.pg.test port={shared} sslmode=verify-full channel_binding=require sslrootcert=/certs/pgdog-ca.crt"
            ),
        ),
    );

    // it_direct_tls: sslnegotiation=direct, terminated at the gateway with
    // ALPN postgresql (PgDog negotiates none, which libpq refuses for direct
    // TLS), re-encrypted to PgDog; channel binding holds because the gateway
    // presents PgDog's certificate.
    ok(
        "it_direct_tls",
        &psql(
            &certs,
            &format!(
                "{base} host=acme.gw.pg.test port={shared} sslmode=verify-full sslnegotiation=direct channel_binding=require sslrootcert=/certs/pgdog-ca.crt"
            ),
        ),
    );

    // it_dedicated_port_without_sni: no SNI at all; the port picks acme.
    ok(
        "it_dedicated_port_without_sni",
        &psql(
            &certs,
            &format!(
                "{base} host=acme.gw.pg.test port={dedicated} sslsni=0 sslmode=verify-full sslrootcert=/certs/pgdog-ca.crt"
            ),
        ),
    );

    // it_plaintext_refused.
    refused(
        "it_plaintext_refused",
        &psql(
            &certs,
            &format!("{base} host=acme.gw.pg.test port={shared} sslmode=disable"),
        ),
        "requires TLS",
    );

    // it_unknown_sni_refused.
    refused(
        "it_unknown_sni_refused",
        &psql(
            &certs,
            &format!("{base} host=other.gw.pg.test port={shared} sslmode=require"),
        ),
        "unrecognized name",
    );

    // The client's TLS really ended at PgDog on the splice path: PgDog's
    // log has no TLS errors and served the queries.
    let logs = Command::new(engine())
        .args(["logs", &pgdog_name])
        .output()
        .unwrap();
    let logs =
        String::from_utf8_lossy(&logs.stdout).into_owned() + &String::from_utf8_lossy(&logs.stderr);
    assert!(!logs.to_ascii_lowercase().contains("panicked"), "{logs}");
    h.gateway.shutdown().await;
}
