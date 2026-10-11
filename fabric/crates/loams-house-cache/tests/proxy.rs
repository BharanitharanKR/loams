//! `house-cache` over a real socket, as a worker reaches it (HS1 Task 9).
//!
//! The upstream is an in-memory store, and, when `LOAMS_HOUSE_S3_ENDPOINT` (with
//! `LOAMS_HOUSE_S3_KEY`, `LOAMS_HOUSE_S3_SECRET` and `LOAMS_HOUSE_S3_BUCKET`)
//! names an S3-compatible server such as RustFS, that server too.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use loams_cache::{RangeCache, RangeCacheConfig};
use loams_house::{ForwardSlot, ForwardTarget};
use loams_house_cache::{Grant, HouseCache, OneStore, StoreOptions, Upstream};
use loams_store::Store;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// A response off the wire.
#[derive(Debug)]
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Reply {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// One request on its own connection (`Connection: close`).
async fn send<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
) -> Reply {
    let mut head =
        format!("{method} {target} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.expect("write");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a head");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("a status");
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_string(), v.trim().to_string()))
        .collect();
    Reply {
        status,
        headers,
        body: raw[split + 4..].to_vec(),
    }
}

async fn tcp(addr: SocketAddr, method: &str, target: &str, headers: &[(&str, &str)]) -> Reply {
    let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    send(stream, method, target, headers).await
}

/// Deterministic bytes that are not all alike.
fn data(len: usize, seed: u8) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect::<Vec<u8>>(),
    )
}

async fn memory_upstream(block: u64) -> Arc<Upstream> {
    let store = Store::in_memory();
    let cache = RangeCache::new(
        store.clone(),
        RangeCacheConfig {
            block_size: block,
            memory_bytes: 64 << 20,
            disk: None,
        },
    )
    .await
    .expect("cache");
    Arc::new(Upstream::new("bucket", "", store, cache))
}

/// What every test puts in its store (keys relative to the store's root).
async fn fill(store: &Store) -> (Bytes, Bytes) {
    let a = data(300_000, 7);
    let b = Bytes::from_static(b"hello, house-cache");
    store.put("ns/1/a.bin", a.clone()).await.expect("put");
    store.put("ns/1/dir/b.txt", b.clone()).await.expect("put");
    store.put("ns/1/dir/c.txt", b.clone()).await.expect("put");
    store
        .put("ns/2/secret.bin", Bytes::from_static(b"other"))
        .await
        .expect("put");
    (a, b)
}

/// Checks GET (whole and ranged), HEAD and LIST through the proxy against the
/// store itself.
async fn check_surface(upstream: Arc<Upstream>, key_prefix: &str) {
    let (a, b) = fill(upstream.store()).await;
    let proxy = HouseCache::new(Arc::new(OneStore(Arc::clone(&upstream))));
    let mut grant = Grant::for_namespace("1");
    grant.list = true;
    proxy.grant("w1", grant);
    let addr = proxy.endpoint("w1").expect("endpoint");
    let bucket = upstream.bucket().to_string();
    let object = |key: &str| format!("/{bucket}/{key_prefix}{key}");

    // HEAD: the store's size, a validator, ranges accepted.
    let head = tcp(addr, "HEAD", &object("ns/1/a.bin"), &[]).await;
    let info = upstream.store().head("ns/1/a.bin").await.expect("head");
    assert_eq!(head.status, 200);
    assert_eq!(
        head.header("content-length"),
        Some(info.size.to_string().as_str())
    );
    assert_eq!(head.header("accept-ranges"), Some("bytes"));
    assert!(head.header("last-modified").is_some());
    assert!(head.body.is_empty());

    // GET, whole and in ranges, byte for byte.
    let whole = tcp(addr, "GET", &object("ns/1/a.bin"), &[]).await;
    assert_eq!(whole.status, 200);
    assert_eq!(whole.body, a);
    let part = tcp(
        addr,
        "GET",
        &object("ns/1/a.bin"),
        &[("Range", "bytes=1000-200000")],
    )
    .await;
    assert_eq!(part.status, 206);
    assert_eq!(part.body, a.slice(1000..200_001));
    assert_eq!(
        part.header("content-range"),
        Some("bytes 1000-200000/300000")
    );
    let suffix = tcp(
        addr,
        "GET",
        &object("ns/1/a.bin"),
        &[("Range", "bytes=-10")],
    )
    .await;
    assert_eq!(suffix.body, a.slice(299_990..));
    let past = tcp(
        addr,
        "GET",
        &object("ns/1/a.bin"),
        &[("Range", "bytes=300000-")],
    )
    .await;
    assert_eq!(past.status, 416);
    assert!(past.text().contains("<Code>InvalidRange</Code>"));
    let small = tcp(addr, "GET", &object("ns/1/dir/b.txt"), &[]).await;
    assert_eq!(small.body, b);
    let missing = tcp(addr, "GET", &object("ns/1/nope"), &[]).await;
    assert_eq!(missing.status, 404);
    assert!(
        missing.text().contains("<Code>NoSuchKey</Code>"),
        "{}",
        missing.text()
    );

    // ListObjectsV2: the keys under the prefix, with and without a delimiter,
    // and in pages.
    let prefix = format!("{key_prefix}ns/1/");
    let list = |extra: &str| {
        format!(
            "/{bucket}?list-type=2&prefix={}{extra}",
            prefix.replace('/', "%2F")
        )
    };
    let all = tcp(addr, "GET", &list(""), &[]).await;
    assert_eq!(all.status, 200, "{}", all.text());
    let text = all.text();
    for key in ["ns/1/a.bin", "ns/1/dir/b.txt", "ns/1/dir/c.txt"] {
        assert!(
            text.contains(&format!("<Key>{key_prefix}{key}</Key>")),
            "{text}"
        );
    }
    assert!(!text.contains("secret"), "{text}");
    assert!(text.contains("<Size>300000</Size>"));
    let folded = tcp(addr, "GET", &list("&delimiter=%2F"), &[]).await.text();
    assert!(
        folded.contains(&format!("<Prefix>{key_prefix}ns/1/dir/</Prefix>")),
        "{folded}"
    );
    assert!(!folded.contains("b.txt"), "{folded}");
    let page = tcp(addr, "GET", &list("&max-keys=1"), &[]).await.text();
    assert!(page.contains("<IsTruncated>true</IsTruncated>"), "{page}");
    let token = page
        .split("<NextContinuationToken>")
        .nth(1)
        .and_then(|rest| rest.split('<').next())
        .expect("a token")
        .to_string();
    let next = tcp(
        addr,
        "GET",
        &list(&format!(
            "&max-keys=1&continuation-token={}",
            token.replace('/', "%2F")
        )),
        &[],
    )
    .await
    .text();
    assert!(
        next.contains(&format!("<Key>{key_prefix}ns/1/dir/b.txt</Key>")),
        "{next}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_range_head_list_match_upstream() {
    check_surface(memory_upstream(64 * 1024).await, "").await;

    // The same against an S3-compatible server (RustFS), when one is named.
    let Ok(endpoint) = std::env::var("LOAMS_HOUSE_S3_ENDPOINT") else {
        eprintln!("skipped the RustFS half: LOAMS_HOUSE_S3_ENDPOINT is not set");
        return;
    };
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} is needed"));
    let bucket = var("LOAMS_HOUSE_S3_BUCKET");
    let run = format!("run-{}", std::process::id());
    let options = StoreOptions(vec![
        ("aws_endpoint".into(), endpoint),
        ("aws_access_key_id".into(), var("LOAMS_HOUSE_S3_KEY")),
        ("aws_secret_access_key".into(), var("LOAMS_HOUSE_S3_SECRET")),
        ("aws_region".into(), "us-east-1".into()),
        ("aws_allow_http".into(), "true".into()),
        ("aws_virtual_hosted_style_request".into(), "false".into()),
    ]);
    let upstream = Upstream::open(
        &format!("s3://{bucket}/{run}"),
        options,
        RangeCacheConfig {
            block_size: 64 * 1024,
            memory_bytes: 64 << 20,
            disk: None,
        },
    )
    .await
    .expect("the S3 upstream opens");
    check_surface(Arc::new(upstream), &format!("{run}/")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_refuses_foreign_prefix() {
    let upstream = memory_upstream(4096).await;
    fill(upstream.store()).await;
    let proxy = HouseCache::new(Arc::new(OneStore(upstream)));
    proxy.grant("w1", Grant::for_namespace("1"));
    let addr = proxy.endpoint("w1").expect("endpoint");

    let refused = [
        ("GET", "/bucket/ns/2/secret.bin"),
        ("HEAD", "/bucket/ns/2/secret.bin"),
        ("GET", "/bucket/ns/1/../2/secret.bin"),
        ("GET", "/bucket/ns/1/%2E%2E/2/secret.bin"),
        ("GET", "/bucket/ns/1//x"),
        ("GET", "/bucket/ns/10/x"),
        ("GET", "/other/ns/1/a.bin"),
        ("PUT", "/bucket/ns/1/new"),
        ("DELETE", "/bucket/ns/1/a.bin"),
        // Listing is not granted (HS1 R1.2).
        ("GET", "/bucket?list-type=2&prefix=ns%2F1%2F"),
    ];
    for (method, target) in refused {
        let reply = tcp(addr, method, target, &[]).await;
        assert_eq!(reply.status, 403, "{method} {target}: {}", reply.text());
        if method != "HEAD" {
            assert!(
                reply.text().contains("<Code>AccessDenied</Code>"),
                "{}",
                reply.text()
            );
        }
    }
    let metrics = proxy.metrics();
    assert_eq!(metrics.namespaces["1"].refused, refused.len() as u64);
    assert_eq!(metrics.namespaces["1"].bytes_served, 0);

    // A granted listing still stays inside the namespace.
    let mut grant = Grant::for_namespace("1");
    grant.list = true;
    proxy.grant("w1", grant);
    let foreign = tcp(addr, "GET", "/bucket?list-type=2&prefix=ns%2F2%2F", &[]).await;
    assert_eq!(foreign.status, 403);
    let wide = tcp(addr, "GET", "/bucket?list-type=2&prefix=ns%2F", &[]).await;
    assert_eq!(wide.status, 403);

    // A worker with no grant reads nothing.
    let stranger = proxy.endpoint("w2").expect("endpoint");
    let reply = tcp(stranger, "GET", "/bucket/ns/1/a.bin", &[]).await;
    assert_eq!(reply.status, 403);
    assert_eq!(proxy.metrics().refused_unbound, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_bytes_are_verified() {
    let upstream = memory_upstream(4096).await;
    let (a, _) = fill(upstream.store()).await;
    let proxy = HouseCache::new(Arc::new(OneStore(Arc::clone(&upstream))));
    proxy.grant("w1", Grant::for_namespace("1"));
    let addr = proxy.endpoint("w1").expect("endpoint");
    let range = [("Range", "bytes=0-4095")];

    let first = tcp(addr, "GET", "/bucket/ns/1/a.bin", &range).await;
    assert_eq!(first.body, a.slice(0..4096));
    assert_eq!(upstream.cache().stats().misses, 1);

    // The cached block goes bad (its checksum no longer matches)…
    let mut bad = vec![0xAAu8; 4096];
    bad.extend_from_slice(&[0, 0, 0, 0]);
    upstream
        .cache()
        .insert_raw_for_test("ns/1/a.bin", 0, Bytes::from(bad));
    // …and is never served: it is fetched again.
    let second = tcp(addr, "GET", "/bucket/ns/1/a.bin", &range).await;
    assert_eq!(second.status, 206);
    assert_eq!(second.body, a.slice(0..4096));
    let stats = upstream.cache().stats();
    assert_eq!(stats.checksum_failures, 1);
    assert_eq!(stats.misses, 2);
    let third = tcp(addr, "GET", "/bucket/ns/1/a.bin", &range).await;
    assert_eq!(third.body, a.slice(0..4096));
    assert_eq!(upstream.cache().stats().hits, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_survives_worker_recycle() {
    let upstream = memory_upstream(64 * 1024).await;
    let (a, _) = fill(upstream.store()).await;
    let proxy = HouseCache::new(Arc::new(OneStore(Arc::clone(&upstream))));

    proxy.bound("w1", "1");
    let first = proxy.endpoint("w1").expect("endpoint");
    assert_eq!(tcp(first, "GET", "/bucket/ns/1/a.bin", &[]).await.body, a);
    let cold = upstream.cache().stats();
    assert!(cold.misses > 0);

    // The worker is recycled: its identity goes, a new worker of the same
    // namespace comes, and the blocks are still there.
    proxy.gone("w1");
    proxy.bound("w2", "1");
    let second = proxy.endpoint("w2").expect("endpoint");
    assert_eq!(tcp(second, "GET", "/bucket/ns/1/a.bin", &[]).await.body, a);
    let warm = upstream.cache().stats();
    assert_eq!(warm.misses, cold.misses, "no block fetched again");
    assert!(warm.hits >= cold.misses);
    // The old identity reads nothing (and its endpoint is closed).
    assert!(proxy.grant_of("w1").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connections_are_known_by_their_channel() {
    // The forwarder's path: a socket pair per connection, attributed to the worker
    // whose channel it came from.
    let upstream = memory_upstream(64 * 1024).await;
    let (_, b) = fill(upstream.store()).await;
    let proxy = HouseCache::new(Arc::new(OneStore(upstream)));
    proxy.bound("w1", "1");
    proxy.bound("w2", "2");
    for (worker, status) in [("w1", 200), ("w2", 403)] {
        let (ours, theirs) = std::os::unix::net::UnixStream::pair().expect("pair");
        proxy.accept(worker, theirs, ForwardSlot::unbounded());
        ours.set_nonblocking(true).expect("nonblocking");
        let stream = tokio::net::UnixStream::from_std(ours).expect("tokio stream");
        let reply = send(stream, "GET", "/bucket/ns/1/dir/b.txt", &[]).await;
        assert_eq!(reply.status, status, "{worker}");
        if status == 200 {
            assert_eq!(reply.body, b);
        }
    }
}
