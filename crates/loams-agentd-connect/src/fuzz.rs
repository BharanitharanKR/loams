//! Invariants checked by the fuzz targets (`fuzz/fuzz_targets`, SD1 Task 30)
//! and, on seed inputs, by the unit tests below. Each panics only when an
//! invariant breaks.
//!
//! - [`connect_decode`]: untrusted bytes as the body of a call to any
//!   `loams.agentd.v1` method, in any of Connect's codecs, through the real
//!   router and handlers over a fake engine. Nothing panics or hangs, and a
//!   handler only ever calls its own ndjson twin, with params that parse as
//!   that method's `Params`.
//! - [`ndjson_params`]: untrusted JSON as the typed params of any ndjson
//!   method (what each engine arm parses): decoding never panics, and what
//!   decodes re-encodes to the same value.
//! - [`jwt`]: untrusted bytes as the remote listener's bearer token, checked
//!   against a fixed key set: nothing panics, and only a token signed by the
//!   issuer's key for the enrolled subject, audience and issuer is accepted.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use loams_agentd_proto::rpc::{Method, MethodVisitor, method_spec, visit_methods};
use loams_agentd_rpc::{RpcError, RpcReply, RpcService};

use crate::services::{Agentd, HostInfo};
use crate::twins::{CONNECT_METHODS, Twin};

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime")
    })
}

// ---------------------------------------------------------------------------
// connect_decode
// ---------------------------------------------------------------------------

/// The engine behind the fuzzed router: it records each call and refuses it,
/// so no handler reaches a filesystem, a process or the network.
#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<(String, serde_json::Value)>>,
}

#[async_trait]
impl RpcService for Recorder {
    async fn handle(&self, method: &str, params: serde_json::Value) -> Result<RpcReply, RpcError> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((method.to_owned(), params));
        Err(RpcError::BadParams("refused by the fuzz engine".into()))
    }
}

fn host() -> HostInfo {
    HostInfo {
        protocol: 1,
        version: "0.0.0".into(),
        build_sha: "fuzz".into(),
        mode: loams_agentd_proto::daemon::DaemonMode::Child,
        pid: 1,
        started_at_ms: 0,
        features: vec![crate::FEATURE.into()],
    }
}

/// Whether `params` parse as the params of the ndjson method `name`.
struct ParsesAs<'a> {
    name: &'a str,
    params: &'a serde_json::Value,
    parsed: Option<bool>,
}

impl MethodVisitor for ParsesAs<'_> {
    fn visit<M: Method>(&mut self) {
        if M::NAME == self.name {
            self.parsed = Some(serde_json::from_value::<M::Params>(self.params.clone()).is_ok());
        }
    }
}

/// The codecs a Connect, gRPC or gRPC-Web client may name.
const CONTENT_TYPES: &[&str] = &[
    "application/proto",
    "application/json",
    "application/connect+proto",
    "application/connect+json",
    "application/grpc",
    "application/grpc-web+proto",
    "application/json; charset=utf-8",
    "text/plain",
];

/// The largest response body read back.
const MAX_RESPONSE: usize = 1 << 20;

/// `data[0]` picks a method, `data[1]` a codec, a GET and a compression
/// header; the rest is the request body (or a GET's `message` query).
/// Answers how many engine calls the handler made.
pub fn connect_decode(data: &[u8]) -> usize {
    let [method, codec, body @ ..] = data else {
        return 0;
    };
    let row = &CONNECT_METHODS[usize::from(*method) % CONNECT_METHODS.len()];
    let content_type = CONTENT_TYPES[usize::from(*codec) % CONTENT_TYPES.len()];
    let get = codec & 0x40 != 0;
    let gzip = codec & 0x80 != 0;

    let engine = Arc::new(Recorder::default());
    let agentd = Agentd::new(engine.clone(), host());
    let mut service = agentd.router().into_axum_service();

    let mut builder = http::Request::builder().header(http::header::HOST, "127.0.0.1:1");
    let request = if get {
        // Connect's GET form: the message in the query.
        let encoded: String = body.iter().map(|b| format!("%{b:02X}")).collect();
        let codec = if content_type.contains("json") {
            "json"
        } else {
            "proto"
        };
        builder = builder.method(http::Method::GET).uri(format!(
            "{}?connect=v1&encoding={codec}&message={encoded}",
            row.path()
        ));
        builder.body(axum::body::Body::empty())
    } else {
        builder = builder
            .method(http::Method::POST)
            .uri(row.path())
            .header(http::header::CONTENT_TYPE, content_type)
            .header("connect-protocol-version", "1");
        if gzip {
            builder = builder
                .header("content-encoding", "gzip")
                .header("connect-content-encoding", "gzip");
        }
        builder.body(axum::body::Body::from(body.to_vec()))
    };
    let Ok(request) = request else {
        return 0;
    };

    runtime().block_on(async {
        let served = tokio::time::timeout(Duration::from_secs(10), async {
            std::future::poll_fn(|cx| {
                <_ as tower::Service<http::Request<axum::body::Body>>>::poll_ready(&mut service, cx)
            })
            .await
            .expect("the router is always ready");
            let response = tower::Service::call(&mut service, request)
                .await
                .expect("the router never fails");
            // Reading the body runs a streaming handler to its end.
            let _ = axum::body::to_bytes(axum::body::Body::new(response.into_body()), MAX_RESPONSE)
                .await;
        })
        .await;
        assert!(served.is_ok(), "{} hung", row.path());
    });

    let calls = engine
        .calls
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let twins: Vec<&str> = row.twin.ndjson().iter().map(|spec| spec.name).collect();
    if matches!(row.twin, Twin::Gap { .. }) {
        assert!(
            calls.is_empty(),
            "{} is a gap but called the engine",
            row.path()
        );
    }
    if let Some((first, params)) = calls.first() {
        assert!(
            twins.contains(&first.as_str()),
            "{} called {first}, not its twin {twins:?}",
            row.path()
        );
        match row.twin {
            Twin::Mutate(op) => assert_eq!(
                params.get("op").and_then(serde_json::Value::as_str),
                Some(op),
                "{} sent another Mutate op",
                row.path()
            ),
            Twin::Command(kind) => assert_eq!(
                params
                    .pointer("/command/kind")
                    .and_then(serde_json::Value::as_str),
                Some(kind),
                "{} queued another command",
                row.path()
            ),
            _ => {}
        }
    }
    for (name, params) in &calls {
        assert!(
            method_spec(name).is_some(),
            "{name} is not an ndjson method"
        );
        let mut check = ParsesAs {
            name,
            params,
            parsed: None,
        };
        visit_methods(&mut check);
        assert_eq!(
            check.parsed,
            Some(true),
            "{}: the params sent to {name} do not parse as its Params: {params}",
            row.path()
        );
    }
    calls.len()
}

// ---------------------------------------------------------------------------
// ndjson_params
// ---------------------------------------------------------------------------

struct Roundtrip<'a> {
    index: usize,
    seen: usize,
    json: &'a [u8],
}

impl MethodVisitor for Roundtrip<'_> {
    fn visit<M: Method>(&mut self) {
        let mine = self.seen == self.index;
        self.seen += 1;
        if !mine {
            return;
        }
        let Ok(params) = serde_json::from_slice::<M::Params>(self.json) else {
            return;
        };
        let first = serde_json::to_value(&params).expect("decoded params encode");
        let again: M::Params = serde_json::from_value(first.clone())
            .unwrap_or_else(|e| panic!("{}: encoded params do not decode: {e}", M::NAME));
        let second = serde_json::to_value(&again).expect("decoded params encode");
        assert_eq!(first, second, "{}: params round trip", M::NAME);
    }
}

/// `data[0]` picks an ndjson method; the rest is its params as JSON.
pub fn ndjson_params(data: &[u8]) {
    let [method, json @ ..] = data else {
        return;
    };
    let count = loams_agentd_proto::rpc::METHODS.len();
    let mut visit = Roundtrip {
        index: usize::from(*method) % count,
        seen: 0,
        json,
    };
    visit_methods(&mut visit);
}

// ---------------------------------------------------------------------------
// jwt
// ---------------------------------------------------------------------------

/// The fixed issuer, audience and subject the `jwt` target verifies against.
pub const JWT_ISSUER: &str = "https://auth.example.test/application/o/loams/";
pub const JWT_AUDIENCE: &str = "loams-agentd";
pub const JWT_SUBJECT: &str = "fuzz-subject";
/// The `kid` of the issuer's Ed25519 key in the fixed set.
pub const JWT_KID: &str = "fuzz-ed25519";
/// A seed that is not secret: the target's key only signs its corpus.
const JWT_SEED: [u8; 32] = [7; 32];

fn jwt_keys() -> &'static jsonwebtoken::jwk::JwkSet {
    static KEYS: OnceLock<jsonwebtoken::jwk::JwkSet> = OnceLock::new();
    KEYS.get_or_init(|| {
        use ring::signature::KeyPair;
        let pair = ring::signature::Ed25519KeyPair::from_seed_unchecked(&JWT_SEED)
            .expect("a fixed Ed25519 seed");
        let x = base64url(pair.public_key().as_ref());
        serde_json::from_value(serde_json::json!({ "keys": [
            { "kty": "OKP", "crv": "Ed25519", "kid": JWT_KID, "use": "sig", "x": x },
            // An HMAC key: a token may not pick it (alg families must match).
            { "kty": "oct", "kid": "fuzz-oct", "k": base64url(b"not a signing key for tokens") },
        ]}))
        .expect("a fixed JWKS")
    })
}

fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | u32::from(*b) << (16 - 8 * i));
        let chars = chunk.len() + 1;
        for i in 0..chars {
            out.push(char::from(ALPHABET[(n >> (18 - 6 * i) & 63) as usize]));
        }
    }
    out
}

fn jwt_options() -> crate::oidc::OidcOptions {
    crate::oidc::OidcOptions::new(JWT_ISSUER, JWT_AUDIENCE, JWT_SUBJECT)
}

/// A token for `claims` signed with the fixed issuer key (EdDSA): the
/// corpus' valid seed and the tests' accepted case.
pub fn jwt_signed(claims: &serde_json::Value) -> String {
    let header = serde_json::json!({ "alg": "EdDSA", "typ": "JWT", "kid": JWT_KID });
    let input = format!(
        "{}.{}",
        base64url(header.to_string().as_bytes()),
        base64url(claims.to_string().as_bytes())
    );
    let pair = ring::signature::Ed25519KeyPair::from_seed_unchecked(&JWT_SEED)
        .expect("a fixed Ed25519 seed");
    let signature = pair.sign(input.as_bytes());
    format!("{input}.{}", base64url(signature.as_ref()))
}

/// Untrusted bytes as a bearer token. Whatever verifies carries the enrolled
/// subject, audience and issuer, with the `exp` the verifier answered.
pub fn jwt(data: &[u8]) {
    let Ok(token) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(header) = crate::oidc::token_header(token) else {
        return;
    };
    let jwk = crate::oidc::find_key(jwt_keys(), header.kid.as_deref(), header.alg);
    let Ok(exp) = crate::oidc::verify_with_key(&jwt_options(), &header, jwk, token) else {
        return;
    };
    // Read the claims again without the verifier's checks, and hold them to
    // what an accepted token must say.
    let mut insecure = jsonwebtoken::Validation::new(header.alg);
    insecure.insecure_disable_signature_validation();
    insecure.validate_aud = false;
    insecure.validate_exp = false;
    insecure.required_spec_claims.clear();
    let claims = jsonwebtoken::decode::<serde_json::Value>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(&[]),
        &insecure,
    )
    .expect("an accepted token decodes")
    .claims;
    assert_eq!(
        header.alg,
        jsonwebtoken::Algorithm::EdDSA,
        "only the Ed25519 key signs"
    );
    assert_eq!(claims["sub"], JWT_SUBJECT, "accepted another subject");
    assert_eq!(
        claims["iss"].as_str().map(|iss| iss.trim_end_matches('/')),
        Some(JWT_ISSUER.trim_end_matches('/')),
        "accepted another issuer"
    );
    let aud_ok = match &claims["aud"] {
        serde_json::Value::String(aud) => aud == JWT_AUDIENCE,
        serde_json::Value::Array(auds) => auds.iter().any(|aud| aud == JWT_AUDIENCE),
        _ => false,
    };
    assert!(aud_ok, "accepted another audience");
    assert_eq!(claims["exp"].as_u64(), Some(exp), "answered another exp");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_decode_holds_for_every_method_and_codec() {
        let mut reached = std::collections::BTreeSet::new();
        for method in 0..CONNECT_METHODS.len() {
            let method = u8::try_from(method).expect("fewer than 256 methods");
            for codec in [0u8, 1, 2, 3, 4, 5, 0x40, 0x41, 0x80, 0x81] {
                for body in [
                    &b""[..],
                    b"{}",
                    b"\x00\x00\x00\x00\x02{}",
                    b"\x0a\x03abc\x12\x01-",
                    b"{\"chatId\":\"../../x\",\"path\":\"/etc/passwd\",\"limit\":\"99999999999\"}",
                    b"\xff\xff\xff\xff\xff",
                ] {
                    let mut data = vec![method, codec];
                    data.extend_from_slice(body);
                    if connect_decode(&data) > 0 {
                        reached.insert(method);
                    }
                }
            }
        }
        // The seeds get past decoding and mapping into the engine for most
        // methods (a gap never calls it; some need fields the seeds lack).
        let twinned = CONNECT_METHODS
            .iter()
            .filter(|row| !matches!(row.twin, Twin::Gap { .. }))
            .count();
        assert!(
            reached.len() * 2 > twinned,
            "only {} of {twinned} methods reached the engine",
            reached.len()
        );
    }

    #[test]
    fn ndjson_params_hold_on_every_fixture() {
        let dir = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../loams-agentd-proto/fixtures"
        );
        let mut fixtures: Vec<Vec<u8>> = std::fs::read_dir(dir)
            .expect("the ndjson fixtures")
            .map(|entry| std::fs::read(entry.expect("a fixture").path()).expect("a fixture"))
            .collect();
        fixtures.extend([
            b"".to_vec(),
            b"{}".to_vec(),
            b"null".to_vec(),
            b"[]".to_vec(),
        ]);
        let count = loams_agentd_proto::rpc::METHODS.len();
        for method in 0..count {
            let method = u8::try_from(method).expect("fewer than 256 methods");
            for json in &fixtures {
                let mut data = vec![method];
                data.extend_from_slice(json);
                ndjson_params(&data);
            }
        }
    }

    #[test]
    fn jwt_accepts_only_the_enrolled_subject() {
        let claims = |sub: &str, aud: &str| {
            serde_json::json!({
                "iss": JWT_ISSUER, "aud": aud, "sub": sub,
                "exp": 4_102_444_800u64, "iat": 0,
            })
        };
        let valid = jwt_signed(&claims(JWT_SUBJECT, JWT_AUDIENCE));
        let header = crate::oidc::token_header(&valid).unwrap();
        let jwk = crate::oidc::find_key(jwt_keys(), header.kid.as_deref(), header.alg);
        assert_eq!(
            crate::oidc::verify_with_key(&jwt_options(), &header, jwk, &valid),
            Ok(4_102_444_800)
        );
        jwt(valid.as_bytes());
        for refused in [
            jwt_signed(&claims("someone-else", JWT_AUDIENCE)),
            jwt_signed(&claims(JWT_SUBJECT, "another-audience")),
            valid.replace(".eyJ", ".eyK"),
            format!("{valid}x"),
        ] {
            let header = crate::oidc::token_header(&refused);
            let verdict = header.and_then(|header| {
                let jwk = crate::oidc::find_key(jwt_keys(), header.kid.as_deref(), header.alg);
                crate::oidc::verify_with_key(&jwt_options(), &header, jwk, &refused)
            });
            assert!(verdict.is_err(), "{refused}");
            jwt(refused.as_bytes());
        }
        for seed in [
            &b""[..],
            b"a.b.c",
            b"eyJhbGciOiJub25lIn0.eyJzdWIiOiJmdXp6LXN1YmplY3QifQ.",
            b"eyJhbGciOiJIUzI1NiIsImtpZCI6ImZ1enotb2N0In0.e30.AAAA",
            b"\xff",
        ] {
            jwt(seed);
        }
    }

    #[test]
    fn base64url_matches_the_rfc_vectors() {
        assert_eq!(base64url(b""), "");
        assert_eq!(base64url(b"f"), "Zg");
        assert_eq!(base64url(b"fo"), "Zm8");
        assert_eq!(base64url(b"foo"), "Zm9v");
        assert_eq!(base64url(b"foob"), "Zm9vYg");
        assert_eq!(base64url(&[0xfb, 0xff]), "-_8");
    }
}
