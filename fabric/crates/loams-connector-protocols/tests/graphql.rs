//! GraphQL: Relay-cursor queries and mutations against a synthetic fixture
//! of the GitHub GraphQL API's documented shapes; `graphql-transport-ws`
//! subscriptions against a loopback WebSocket peer; and (nightly) the real
//! GitHub API.
#![allow(clippy::unwrap_used)]

mod common;

use common::*;
use loams_connector_protocols::graphql::Graphql;
use loams_connector_protocols::ws::{accept_for, parse_frame};
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorFactory, ErrorClass, EventBatch, Position, SinkHandle,
    Source,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn source(ctx: &ConnectorContext, pos: Option<Position>) -> Box<dyn Source> {
    let (mut en, factory) = Graphql.source(ctx).unwrap().unwrap();
    let split = en.discover(&[]).await.unwrap().remove(0);
    let mut s = factory.create(&split).unwrap();
    s.open(&split, pos).await.unwrap();
    s
}

#[tokio::test]
async fn graphql_cursor_pagination_resumes() {
    let (_, scenario, proxy) = replay("graphql/issues.json").await;
    let config = with_proxy(&scenario["config"], &proxy.base_url());
    let ctx = ctx(config, &scenario["secrets"]);

    let mut src = source(&ctx, None).await;
    let first = src.next(BUDGET).await.unwrap().unwrap();
    let page1 = events(&first);
    assert_eq!(page1.len(), 2);
    assert_eq!(page1[0].ty, "io.loams.dev.flow.graphql.item.v1");
    assert_eq!(page1[0].subject.as_deref(), Some("I_synthetic1"));
    assert_eq!(first.position.display, "after Y3Vyc29yOnYyOpHOc3ludGgy");
    // A crash here: a new source opened at the saved position asks for the
    // next page only.
    drop(src);
    let mut resumed = source(&ctx, Some(first.position)).await;
    let rest = drain(&mut *resumed).await;
    assert_eq!(rest.len(), 1);
    assert_eq!(data(&rest[0])["number"], 3);
    // Item ids are content digests: a re-read of an unchanged item is the
    // same event.
    let mut again = source(&ctx, None).await;
    let all = drain(&mut *again).await;
    assert_eq!(all[0].id, page1[0].id);
    assert_eq!(all.len(), 3);
    let sent = proxy.received();
    assert_eq!(
        sent[0].headers.get("authorization").map(String::as_str),
        Some("Bearer synthetic-ghp-0000000000000000")
    );
    proxy.finish().unwrap();
}

#[tokio::test]
async fn graphql_mutation_sink_and_errors() {
    let (_, scenario, proxy) = replay("graphql/issues.json").await;
    let config = with_proxy(&scenario["config"], &proxy.base_url());
    let ctx = ctx(config, &scenario["secrets"]);
    let SinkHandle::Plain(mut sink) = Graphql.sink(&ctx).unwrap().unwrap().create().unwrap() else {
        panic!("plain");
    };
    let ok = event(
        "m1",
        None,
        &json!({"input": {"subjectId": "I_synthetic1", "body": "Synced by Loams"}}),
    );
    assert_eq!(
        sink.write(EventBatch::Events(vec![ok])).await.unwrap().rows,
        1
    );
    let bad = event(
        "m2",
        None,
        &json!({"input": {"subjectId": "I_missing", "body": "Synced by Loams"}}),
    );
    let err = sink.write(EventBatch::Events(vec![bad])).await.unwrap_err();
    assert_eq!(
        err.class,
        ErrorClass::Poison,
        "a GraphQL error on a mutation is the event's"
    );
    assert!(err.message.contains("I_missing"));
    proxy.finish().unwrap();
}

/// A one-connection `graphql-transport-ws` peer on loopback: ack, then two
/// `next` messages and `complete`.
async fn ws_peer() -> (String, tokio::task::JoinHandle<Vec<Value>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = s.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
        }
        let head = String::from_utf8_lossy(&buf).into_owned();
        let key = head
            .lines()
            .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
            .unwrap()
            .trim()
            .to_owned();
        assert!(head.contains("Authorization: Bearer synthetic-token"));
        let resp = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Accept: {}\r\nSec-WebSocket-Protocol: graphql-transport-ws\r\n\r\n",
            accept_for(&key)
        );
        s.write_all(resp.as_bytes()).await.unwrap();
        let send = |v: Value| {
            let p = v.to_string().into_bytes();
            let mut f = vec![0x81, p.len() as u8];
            f.extend(p);
            f
        };
        let mut got = Vec::new();
        let mut rbuf: Vec<u8> = Vec::new();
        loop {
            while let Some((_, op, payload, used)) = parse_frame(&rbuf) {
                rbuf.drain(..used);
                if op != 1 {
                    continue;
                }
                let m: Value = serde_json::from_slice(&payload).unwrap();
                got.push(m.clone());
                match m["type"].as_str() {
                    Some("connection_init") => {
                        s.write_all(&send(json!({"type": "connection_ack"})))
                            .await
                            .unwrap();
                    }
                    Some("subscribe") => {
                        for n in 1..=2 {
                            s.write_all(&send(json!({"id": "1", "type": "next",
                                "payload": {"data": {"issueOpened": {"number": n}}}})))
                                .await
                                .unwrap();
                        }
                        s.write_all(&send(json!({"id": "1", "type": "complete"})))
                            .await
                            .unwrap();
                        return got;
                    }
                    _ => {}
                }
            }
            let n = s.read(&mut chunk).await.unwrap();
            if n == 0 {
                return got;
            }
            rbuf.extend_from_slice(&chunk[..n]);
        }
    });
    (format!("http://{addr}/graphql"), task)
}

#[tokio::test]
async fn graphql_ws_subscription_streams() {
    let (url, peer) = ws_peer().await;
    let ctx = ctx(
        json!({"url": url, "auth": {"bearer": "tok"},
               "subscription": {"query": "subscription { issueOpened { number } }"}}),
        &json!({"tok": "synthetic-token"}),
    );
    let mut src = source(&ctx, None).await;
    let mut got = Vec::new();
    while let Some(b) = src
        .next(Budget {
            max_rows: 10,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap()
    {
        got.extend(events(&b));
    }
    assert_eq!(got.len(), 2);
    assert_eq!(data(&got[1]), json!({"issueOpened": {"number": 2}}));
    assert_eq!(got[0].ty, "io.loams.dev.flow.graphql.item.v1");
    let sent = peer.await.unwrap();
    assert_eq!(sent[0]["type"], "connection_init");
    assert_eq!(
        sent[0]["payload"]["Authorization"],
        "Bearer synthetic-token"
    );
    assert_eq!(
        sent[1]["payload"]["query"],
        "subscription { issueOpened { number } }"
    );
}

/// Nightly: the real GitHub GraphQL API (`LOAMS_GITHUB_TOKEN`), one page of
/// a public repository's issues.
#[tokio::test]
async fn graphql_against_github() {
    let Some(v) = env_all(&["LOAMS_GITHUB_TOKEN"]) else {
        return skip("graphql_against_github", "LOAMS_GITHUB_TOKEN unset");
    };
    let ctx = ctx(
        json!({"url": "https://api.github.com/graphql", "auth": {"bearer": "t"},
          "query": {"query": "query($first: Int!, $after: String) { repository(owner: \"rust-lang\", name: \"rust\") { issues(first: $first, after: $after) { pageInfo { hasNextPage endCursor } nodes { id number } } } }",
                    "connection": "/data/repository/issues", "page_size": 5}}),
        &json!({"t": v[0]}),
    );
    let mut src = source(&ctx, None).await;
    let b = src
        .next(Budget {
            max_rows: 5,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(events(&b).len(), 5);
}
