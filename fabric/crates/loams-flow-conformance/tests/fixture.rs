//! The record-and-replay fixture proxy.
// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use loams_connector_sdk::{AllowAll, Scrubber, Secret};
use loams_flow_conformance::fixture::{Body, RecordedRequest, RecordedResponse};
use loams_flow_conformance::{Exchange, Fixture, FixtureError, FixtureProxy, ScrubRules};

fn exchange(
    method: &str,
    path: &str,
    body: serde_json::Value,
    resp: serde_json::Value,
) -> Exchange {
    Exchange {
        request: RecordedRequest {
            method: method.to_owned(),
            path: path.to_owned(),
            headers: BTreeMap::new(),
            body: if body.is_null() {
                Body::Empty
            } else {
                Body::Json(body)
            },
        },
        response: RecordedResponse {
            status: 200,
            headers: BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
            body: Body::Json(resp),
        },
    }
}

async fn send(
    base: &str,
    method: &str,
    path: &str,
    body: &str,
    auth: Option<&str>,
) -> (u16, String) {
    let client = loams_connector_sdk::http::client(Arc::new(AllowAll));
    let mut b = Request::builder()
        .method(method)
        .uri(format!("{base}{path}"))
        .header("content-type", "application/json");
    if let Some(a) = auth {
        b = b.header("authorization", a);
    }
    let resp = client
        .send(b.body(Bytes::from(body.to_owned())).unwrap())
        .await
        .expect("the proxy answers");
    (
        resp.status().as_u16(),
        String::from_utf8_lossy(resp.body()).into_owned(),
    )
}

#[tokio::test]
async fn fixture_replay_rejects_unrecorded_request() {
    let fixture = Fixture {
        recorded_from: "HubSpot".to_owned(),
        exchanges: vec![
            exchange(
                "POST",
                "/crm/v3/objects/contacts/search?limit=2&after=10",
                serde_json::json!({"filterGroups": [], "sorts": ["id"]}),
                serde_json::json!({"results": [{"id": "1"}]}),
            ),
            exchange(
                "GET",
                "/crm/v3/objects/contacts?after=20",
                serde_json::Value::Null,
                serde_json::json!({"results": []}),
            ),
        ],
    };
    let proxy = FixtureProxy::replay(fixture, ScrubRules::default())
        .await
        .unwrap();
    let base = proxy.base_url();

    // Matched by method, path (query order does not matter) and canonical
    // body (key order does not matter); headers are not matched.
    let (status, body) = send(
        &base,
        "POST",
        "/crm/v3/objects/contacts/search?after=10&limit=2",
        r#"{"sorts":["id"],"filterGroups":[]}"#,
        Some("Bearer whatever"),
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("\"id\":\"1\""), "{body}");
    assert!(proxy.unmatched().is_empty());

    // A request nobody recorded: another body, another path, another method.
    let (status, _) = send(
        &base,
        "POST",
        "/crm/v3/objects/contacts/search?after=10&limit=2",
        r#"{"sorts":["name"],"filterGroups":[]}"#,
        None,
    )
    .await;
    assert_eq!(status, 500);
    let (status, _) = send(&base, "DELETE", "/crm/v3/objects/contacts/1", "", None).await;
    assert_eq!(status, 500);
    assert_eq!(proxy.unmatched().len(), 2);
    match proxy.finish() {
        Err(FixtureError::Unmatched(list)) => {
            assert_eq!(list.len(), 2);
            assert!(
                list[1].starts_with("DELETE /crm/v3/objects/contacts/1"),
                "{list:?}"
            );
        }
        other => panic!("replay must fail on an unrecorded request: {other:?}"),
    }

    // A fixture that does not name the real system it came from is refused.
    let anonymous = Fixture {
        recorded_from: String::new(),
        exchanges: vec![],
    };
    assert!(matches!(
        FixtureProxy::replay(anonymous, ScrubRules::default()).await,
        Err(FixtureError::NoSystem)
    ));
}

/// A stand-in upstream for the recording test only: it plays the role of the
/// real system's HTTP endpoint on loopback, and returns credentials the way
/// an OAuth token endpoint does.
async fn upstream() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let svc =
                hyper::service::service_fn(|req: Request<hyper::body::Incoming>| async move {
                    let auth = req
                        .headers()
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_owned();
                    let _ = req.into_body().collect().await;
                    let body = serde_json::json!({
                        "access_token": "tok-7f3a9c",
                        "nested": {"password": "upstream-pw-1188"},
                        "echo": auth,
                        "data": [1, 2, 3]
                    });
                    Ok::<_, Infallible>(
                        Response::builder()
                            .header("content-type", "application/json")
                            .header("set-cookie", "session=cookie-5521")
                            .body(Full::new(Bytes::from(body.to_string())))
                            .unwrap(),
                    )
                });
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    (format!("http://{addr}"), task)
}

#[tokio::test]
async fn fixture_recording_scrubs_secrets() {
    let (upstream, task) = upstream().await;
    // The connector resolved this secret; it must vanish wherever it shows
    // up, even in a field no rule names (the upstream echoes it back).
    let scrubber = Scrubber::new();
    scrubber.add(&Secret::new("s3cr3t-bearer-0042"));
    let rules = ScrubRules {
        json_pointers: {
            let mut p = ScrubRules::default().json_pointers;
            p.push("/nested/password".to_owned());
            p
        },
        ..ScrubRules::default()
    }
    .with_values(scrubber.clone());
    let client = loams_connector_sdk::http::client(Arc::new(AllowAll));
    let proxy = FixtureProxy::record(&upstream, "Salesforce", rules.clone(), client)
        .await
        .unwrap();
    let (status, body) = send(
        &proxy.base_url(),
        "POST",
        "/services/oauth2/token?client_secret=cs-9931&grant_type=client_credentials",
        r#"{"password":"hunter2-pw-77","user":"svc"}"#,
        Some("Bearer s3cr3t-bearer-0042"),
    )
    .await;
    // The connector under test sees the real response.
    assert_eq!(status, 200);
    assert!(body.contains("tok-7f3a9c"));
    let fixture = proxy.into_fixture();
    task.abort();

    assert_eq!(fixture.recorded_from, "Salesforce");
    assert_eq!(fixture.exchanges.len(), 1);
    let text = fixture.to_json();
    for secret in [
        "s3cr3t-bearer-0042",
        "cs-9931",
        "hunter2-pw-77",
        "tok-7f3a9c",
        "upstream-pw-1188",
        "cookie-5521",
    ] {
        assert!(
            !text.contains(secret),
            "{secret} survived recording:\n{text}"
        );
    }
    // What is not a secret is kept.
    assert!(text.contains("grant_type=client_credentials"));
    assert!(text.contains("\"user\": \"svc\""));

    // The scrubbed recording still replays the same request.
    let replay = FixtureProxy::replay(fixture, rules).await.unwrap();
    let (status, body) = send(
        &replay.base_url(),
        "POST",
        "/services/oauth2/token?grant_type=client_credentials&client_secret=cs-9931",
        r#"{"user":"svc","password":"hunter2-pw-77"}"#,
        None,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("<scrubbed>"));
    replay.finish().unwrap();
}
