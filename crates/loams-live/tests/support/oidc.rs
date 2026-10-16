//! A stand-in OIDC issuer for the end-user suites (LV1 plan Task 16):
//! discovery and a JWKS with generated Ed25519 keys, on loopback.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use loams_live::apps::TrustedIssuer;
use loams_live::pb;
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use serde_json::{Value, json};

/// The audience the suites' apps accept.
pub const AUDIENCE: &str = "shop-web";

/// One Ed25519 signing key: its signer and its public JWK.
pub struct Key {
    pub kid: String,
    pub signer: EncodingKey,
    pub jwk: Value,
}

impl Key {
    pub fn generate(kid: &str) -> Key {
        let pkcs8 =
            Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).expect("test fixture");
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("test fixture");
        let x = data_encoding::BASE64URL_NOPAD.encode(pair.public_key().as_ref());
        Key {
            kid: kid.into(),
            signer: EncodingKey::from_ed_der(pkcs8.as_ref()),
            jwk: json!({"kty": "OKP", "crv": "Ed25519", "x": x, "kid": kid, "alg": "EdDSA", "use": "sig"}),
        }
    }
}

/// An OIDC issuer on loopback: `/.well-known/openid-configuration` and a
/// JWKS that can be swapped, counting JWKS fetches.
pub struct Issuer {
    pub url: String,
    pub published: Arc<std::sync::Mutex<Vec<Value>>>,
    pub jwks_fetches: Arc<AtomicUsize>,
    pub key: Key,
    pub task: tokio::task::JoinHandle<()>,
}

impl Issuer {
    pub async fn start() -> Issuer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test fixture");
        let url = format!(
            "http://{}/application/o/shop/",
            listener.local_addr().expect("test fixture")
        );
        let key = Key::generate("k1");
        let published = Arc::new(std::sync::Mutex::new(vec![key.jwk.clone()]));
        let jwks_fetches = Arc::new(AtomicUsize::new(0));
        let discovery = json!({"issuer": url, "jwks_uri": format!("{url}jwks/")});
        let keys = published.clone();
        let fetches = jwks_fetches.clone();
        let app = axum::Router::new()
            .route(
                "/application/o/shop/.well-known/openid-configuration",
                axum::routing::get(move || async move { axum::Json(discovery) }),
            )
            .route(
                "/application/o/shop/jwks/",
                axum::routing::get(move || {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    let keys = keys.lock().expect("test fixture").clone();
                    async move { axum::Json(json!({ "keys": keys })) }
                }),
            );
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test fixture");
        });
        Issuer {
            url,
            published,
            jwks_fetches,
            key,
            task,
        }
    }

    pub fn trusted(&self) -> TrustedIssuer {
        TrustedIssuer {
            issuer: self.url.clone(),
            audiences: vec![AUDIENCE.into()],
            jwks_uri: None,
        }
    }

    pub fn trusted_pb(&self) -> pb::TrustedIssuer {
        self.trusted().to_proto()
    }

    /// A token for `sub`, signed with `key`, its claims edited by `edit`.
    pub fn token_with(&self, key: &Key, sub: &str, edit: impl FnOnce(&mut Value)) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test fixture")
            .as_secs();
        let mut claims = json!({
            "iss": self.url, "aud": AUDIENCE, "sub": sub, "exp": now + 300, "iat": now,
            "email": format!("{sub}@example.com"), "name": "Ada", "plan": "pro",
        });
        edit(&mut claims);
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(key.kid.clone());
        jsonwebtoken::encode(&header, &claims, &key.signer).expect("test fixture")
    }

    pub fn token(&self, sub: &str) -> String {
        self.token_with(&self.key, sub, |_| {})
    }
}

impl Drop for Issuer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
