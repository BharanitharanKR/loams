//! An `s3://` store takes its credentials, endpoint and region from the
//! process environment (`AWS_*`), as the release stack's `engine.env` sets
//! them (release blocker B1: before, the environment was ignored and every
//! write went to the instance metadata service and timed out).
//!
//! The workspace forbids `unsafe`, so a test cannot set its own environment:
//! each test runs [`child`] again in a fresh process with the environment it
//! wants, and checks what that process did.
//!
//! - [`s3_store_reads_the_environment_against_a_fake_endpoint`] always runs,
//!   against a minimal HTTP server standing in for S3.
//! - [`s3_store_reads_the_environment_against_rustfs`] runs when
//!   `LOAMS_TEST_S3_URL=http://<host>:<port>/<bucket>` (with
//!   `LOAMS_TEST_S3_KEY` and `LOAMS_TEST_S3_SECRET`) names a real
//!   S3-compatible store; `scripts/git/rustfs.sh start` prints them.

// Test setup: a failure here is a broken test environment.
#![allow(clippy::unwrap_used)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use loams_store::{Store, StoreError};

const CHILD_MODE: &str = "LOAMS_STORE_S3_CHILD";
const CHILD_URL: &str = "LOAMS_STORE_S3_CHILD_URL";
const NO_OPTIONS: [(&str, &str); 0] = [];

/// Runs [`child`] in a new process with exactly `env` (plus the mode and
/// bucket URL), and returns its combined output; panics when it fails.
fn run_child(mode: &str, bucket_url: &str, env: &[(&str, &str)]) -> String {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([
        "--exact",
        "s3_env::child",
        "--nocapture",
        "--test-threads=1",
    ])
    .env_clear()
    .env(CHILD_MODE, mode)
    .env(CHILD_URL, bucket_url);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "child failed:\n{text}");
    assert!(
        text.contains("child: done"),
        "child did not run its checks:\n{text}"
    );
    text
}

/// The child half: does nothing unless run by [`run_child`].
#[tokio::test]
async fn child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let url = std::env::var(CHILD_URL).unwrap();
    // No options at all: everything comes from the environment.
    let store = Store::from_url(&url, NO_OPTIONS).unwrap();
    match mode.as_str() {
        "fake" => {
            tokio::time::timeout(
                Duration::from_secs(30),
                store.put("objects/k1", Bytes::from_static(b"hello")),
            )
            .await
            .expect("put timed out (metadata service?)")
            .unwrap();
        }
        "real" => {
            let key = format!(
                "s3-env/{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let ops = async {
                let v1 = store
                    .put_if_absent(&key, Bytes::from_static(b"v1"))
                    .await
                    .unwrap();
                let err = store
                    .put_if_absent(&key, Bytes::from_static(b"again"))
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, StoreError::AlreadyExists { .. }),
                    "got {err:?}"
                );
                store
                    .put_if_match(&key, Bytes::from_static(b"v2"), &v1)
                    .await
                    .unwrap();
                assert_eq!(store.get(&key).await.unwrap().0, Bytes::from_static(b"v2"));
                let err = store
                    .put_if_match(&key, Bytes::from_static(b"v3"), &v1)
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, StoreError::PreconditionFailed { .. }),
                    "got {err:?}"
                );
                store.delete(&key).await.unwrap();
            };
            tokio::time::timeout(Duration::from_secs(60), ops)
                .await
                .expect("store operations timed out (metadata service?)");
        }
        other => panic!("unknown child mode {other}"),
    }
    println!("child: done");
}

/// One request the fake endpoint received.
#[derive(Debug, Clone)]
struct Seen {
    method: String,
    path: String,
    authorization: String,
}

/// A minimal S3 stand-in on 127.0.0.1: answers every request `200 OK` with
/// an `ETag`, and records it.
fn fake_s3() -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(conn) = conn else { continue };
            let log = Arc::clone(&log);
            std::thread::spawn(move || serve(conn, &log));
        }
    });
    (format!("http://{addr}"), seen)
}

fn serve(conn: TcpStream, log: &Mutex<Vec<Seen>>) {
    let mut reader = BufReader::new(conn.try_clone().unwrap());
    let mut conn = conn;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_string();
        let path = parts.next().unwrap_or_default().to_string();
        let mut len = 0usize;
        let mut authorization = String::new();
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).unwrap_or(0) == 0 {
                return;
            }
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':') {
                let value = value.trim();
                if name.eq_ignore_ascii_case("content-length") {
                    len = value.parse().unwrap_or(0);
                } else if name.eq_ignore_ascii_case("authorization") {
                    authorization = value.to_string();
                }
            }
        }
        let mut body = vec![0; len];
        if reader.read_exact(&mut body).is_err() {
            return;
        }
        log.lock().unwrap().push(Seen {
            method,
            path,
            authorization,
        });
        let reply = "HTTP/1.1 200 OK\r\nETag: \"fake-etag\"\r\nContent-Length: 0\r\n\r\n";
        if conn.write_all(reply.as_bytes()).is_err() {
            return;
        }
    }
}

#[test]
fn s3_store_reads_the_environment_against_a_fake_endpoint() {
    let (endpoint, seen) = fake_s3();
    run_child(
        "fake",
        "s3://b1-bucket/stack",
        &[
            ("AWS_ACCESS_KEY_ID", "AKIDLOAMSTEST"),
            ("AWS_SECRET_ACCESS_KEY", "not-a-real-secret"),
            ("AWS_REGION", "us-east-1"),
            // http:// alone allows plain HTTP: no AWS_ALLOW_HTTP needed.
            ("AWS_ENDPOINT_URL", &endpoint),
        ],
    );
    let seen = seen.lock().unwrap().clone();
    let put = seen
        .iter()
        .find(|s| s.method == "PUT")
        .unwrap_or_else(|| panic!("no PUT reached the endpoint: {seen:?}"));
    // Path style: the bucket is the first path segment, then the URL's prefix.
    assert_eq!(put.path, "/b1-bucket/stack/objects/k1");
    // Signed with the environment's access key, for the environment's region.
    assert!(
        put.authorization.contains("Credential=AKIDLOAMSTEST/")
            && put.authorization.contains("/us-east-1/s3/"),
        "unexpected Authorization: {}",
        put.authorization
    );
}

#[test]
fn s3_store_reads_the_environment_against_rustfs() {
    let Ok(url) = std::env::var("LOAMS_TEST_S3_URL") else {
        eprintln!(
            "LOAMS_TEST_S3_URL not set: s3_store_reads_the_environment_against_rustfs skipped"
        );
        return;
    };
    let (endpoint, bucket) = url.rsplit_once('/').expect("<endpoint>/<bucket>");
    let key = std::env::var("LOAMS_TEST_S3_KEY").expect("LOAMS_TEST_S3_KEY");
    let secret = std::env::var("LOAMS_TEST_S3_SECRET").expect("LOAMS_TEST_S3_SECRET");
    run_child(
        "real",
        &format!("s3://{bucket}/loams-store-it"),
        &[
            ("AWS_ACCESS_KEY_ID", &key),
            ("AWS_SECRET_ACCESS_KEY", &secret),
            ("AWS_REGION", "us-east-1"),
            ("AWS_ENDPOINT_URL", endpoint),
        ],
    );
}
