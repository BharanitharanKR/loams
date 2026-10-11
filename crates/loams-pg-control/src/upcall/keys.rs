//! `pg-control`'s Ed25519 key and the compute tokens it signs (design §46
//! §4.3; PG2 Task 8).
//!
//! One key signs two kinds of token, both JWTs (`alg: EdDSA`, the key's
//! `kid` in the header) whose claims name one compute:
//! - **the upcall token** ([`ComputeKeys::issue_upcall_token`], `aud`
//!   [`UPCALL_AUDIENCE`]): the compute's own, handed to it at start
//!   (`NEON_CONTROL_PLANE_TOKEN`); `compute_ctl` sends it with the spec
//!   upcall, and the listener accepts nothing else there;
//! - **the `compute_ctl` token** ([`ComputeKeys::issue_compute_ctl_token`],
//!   `aud` `compute`): short-lived, for `pg-control`'s own calls to that
//!   compute's `compute_ctl` (`/configure`, `/terminate`), which verifies it
//!   against the JWKS in `compute_ctl_config` ([`ComputeKeys::jwks`]) and
//!   checks only its `compute_id` (`compute_tools/src/http/middleware/
//!   authorize.rs`). The upcall refuses it: its audience is not the upcall's.
//!
//! A compute's record (`C/`) names the `kid` its upcall token was signed
//! with, so a token signed with any other key the listener trusts (an older
//! one kept for rotation, [`ComputeKeys::trust`]) is refused for it.
//!
//! The private key never shows: `Debug` prints the `kid`s only, and
//! [`ComputeKeys::pkcs8`] is a [`Secret`].

use std::fmt;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ids::ComputeId;
use crate::secrets::Secret;

/// The `aud` of an upcall token: only `pg-control`'s upcall listener takes
/// it.
pub const UPCALL_AUDIENCE: &str = "loams-pg-control";

/// How long an upcall token lives. A compute keeps the token it started
/// with, and `compute_ctl` reads its spec again whenever it reconfigures
/// itself (after losing its pageserver), so the token must outlive the
/// compute: a compute older than this is replaced (Task 11). The record's
/// `kid` and status revoke it sooner.
pub const COMPUTE_TOKEN_TTL: Duration = Duration::from_secs(90 * 24 * 3600);

/// How long a token for one `compute_ctl` call lives.
pub const CTL_TOKEN_TTL: Duration = Duration::from_secs(10 * 60);

/// Clock skew allowed on `exp`.
const LEEWAY_S: u64 = 60;

/// Why a key could not be made or used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("the system random source failed")]
    Random,
    #[error("not an Ed25519 key in PKCS#8")]
    InvalidKey,
    #[error("signing a token failed")]
    Sign,
}

/// Why an upcall token was refused. The text names no part of the token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    #[error("the token is malformed")]
    Malformed,
    #[error("the token names a key this listener does not trust")]
    UnknownKey,
    #[error("the token's signature, audience or claims are invalid")]
    Invalid,
    #[error("the token has expired")]
    Expired,
}

/// A verified upcall token: the compute it names and the key that signed
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpcallClaims {
    pub compute_id: String,
    pub kid: String,
}

/// The claims `pg-control` signs (`compute_api::requests::ComputeClaims`,
/// plus `exp` and `iat`).
#[derive(Serialize, Deserialize)]
struct Claims {
    compute_id: String,
    aud: Vec<String>,
    exp: u64,
    iat: u64,
}

struct Trusted {
    kid: String,
    /// The public key, base64url (the JWK's `x`).
    x: String,
    key: DecodingKey,
}

/// `pg-control`'s signing key, and the keys it accepts upcall tokens from:
/// its own first, then those added by [`trust`](Self::trust).
pub struct ComputeKeys {
    pkcs8: Secret<Vec<u8>>,
    signing: EncodingKey,
    trusted: Vec<Trusted>,
}

impl fmt::Debug for ComputeKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ComputeKeys")
            .field("kid", &self.kid())
            .field(
                "trusted",
                &self
                    .trusted
                    .iter()
                    .map(|t| t.kid.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// `pgc-` and the first 8 bytes of the public key's SHA-256, in hex.
fn kid_of(public: &[u8]) -> String {
    format!("pgc-{}", hex::encode(&Sha256::digest(public)[..8]))
}

fn secs(ms: u64) -> u64 {
    ms / 1000
}

impl ComputeKeys {
    /// A new key from the system's random source.
    ///
    /// # Errors
    ///
    /// [`KeyError::Random`] when the random source fails.
    pub fn generate() -> Result<Self, KeyError> {
        let doc =
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).map_err(|_| KeyError::Random)?;
        Self::from_pkcs8(Secret::new(doc.as_ref().to_vec()))
    }

    /// The key `der` (PKCS#8, v1 or v2), as [`pkcs8`](Self::pkcs8) gives it
    /// back.
    ///
    /// # Errors
    ///
    /// [`KeyError::InvalidKey`] for anything but an Ed25519 key.
    pub fn from_pkcs8(der: Secret<Vec<u8>>) -> Result<Self, KeyError> {
        let pair = Ed25519KeyPair::from_pkcs8_maybe_unchecked(der.expose())
            .map_err(|_| KeyError::InvalidKey)?;
        let public = pair.public_key().as_ref();
        let x = URL_SAFE_NO_PAD.encode(public);
        let key = DecodingKey::from_ed_components(&x).map_err(|_| KeyError::InvalidKey)?;
        let trusted = vec![Trusted {
            kid: kid_of(public),
            x,
            key,
        }];
        Ok(ComputeKeys {
            signing: EncodingKey::from_ed_der(der.expose()),
            pkcs8: der,
            trusted,
        })
    }

    /// The private key, PKCS#8, to keep where the next start reads it.
    pub fn pkcs8(&self) -> &Secret<Vec<u8>> {
        &self.pkcs8
    }

    /// Also accept tokens `older` signed (and publish its public key), while
    /// computes started under it still run: a key rotation.
    #[must_use]
    pub fn trust(mut self, older: &ComputeKeys) -> Self {
        for t in &older.trusted {
            if !self.trusted.iter().any(|mine| mine.kid == t.kid) {
                self.trusted.push(Trusted {
                    kid: t.kid.clone(),
                    x: t.x.clone(),
                    key: t.key.clone(),
                });
            }
        }
        self
    }

    /// The signing key's id, which a compute's record keeps.
    pub fn kid(&self) -> &str {
        &self.trusted[0].kid
    }

    /// The JWKS for `compute_ctl_config.jwks`: every trusted public key,
    /// in the shape `compute_ctl` reads (`jsonwebtoken::jwk::JwkSet`).
    pub fn jwks(&self) -> serde_json::Value {
        let keys: Vec<serde_json::Value> = self
            .trusted
            .iter()
            .map(|t| {
                serde_json::json!({
                    "kty": "OKP",
                    "use": "sig",
                    "key_ops": ["verify"],
                    "alg": "EdDSA",
                    "kid": t.kid,
                    "crv": "Ed25519",
                    "x": t.x,
                })
            })
            .collect();
        serde_json::json!({ "keys": keys })
    }

    fn sign(
        &self,
        compute: &ComputeId,
        aud: &str,
        now_ms: u64,
        ttl: Duration,
    ) -> Result<Secret<String>, KeyError> {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(self.kid().to_owned());
        let claims = Claims {
            compute_id: compute.to_string(),
            aud: vec![aud.to_owned()],
            iat: secs(now_ms),
            exp: secs(now_ms).saturating_add(ttl.as_secs()),
        };
        jsonwebtoken::encode(&header, &claims, &self.signing)
            .map(Secret::new)
            .map_err(|_| KeyError::Sign)
    }

    /// The upcall token of `compute`, valid `ttl` from `now_ms` (usually
    /// [`COMPUTE_TOKEN_TTL`]). Its record keeps [`kid`](Self::kid).
    ///
    /// # Errors
    ///
    /// [`KeyError::Sign`].
    pub fn issue_upcall_token(
        &self,
        compute: &ComputeId,
        now_ms: u64,
        ttl: Duration,
    ) -> Result<Secret<String>, KeyError> {
        self.sign(compute, UPCALL_AUDIENCE, now_ms, ttl)
    }

    /// A token for a call to `compute`'s `compute_ctl` (usually
    /// [`CTL_TOKEN_TTL`]).
    ///
    /// # Errors
    ///
    /// [`KeyError::Sign`].
    pub fn issue_compute_ctl_token(
        &self,
        compute: &ComputeId,
        now_ms: u64,
        ttl: Duration,
    ) -> Result<Secret<String>, KeyError> {
        self.sign(
            compute,
            loams_postgres::compute_ctl::COMPUTE_AUDIENCE,
            now_ms,
            ttl,
        )
    }

    /// Checks an upcall token at `now_ms`: signed by a trusted key (named by
    /// its `kid`), for [`UPCALL_AUDIENCE`], not expired.
    ///
    /// # Errors
    ///
    /// [`TokenError`]; none says more than which check failed.
    pub fn verify_upcall_token(
        &self,
        token: &str,
        now_ms: u64,
    ) -> Result<UpcallClaims, TokenError> {
        let header = jsonwebtoken::decode_header(token).map_err(|_| TokenError::Malformed)?;
        if header.alg != Algorithm::EdDSA {
            return Err(TokenError::Invalid);
        }
        let kid = header.kid.ok_or(TokenError::UnknownKey)?;
        let trusted = self
            .trusted
            .iter()
            .find(|t| t.kid == kid)
            .ok_or(TokenError::UnknownKey)?;
        let mut validation = Validation::new(Algorithm::EdDSA);
        // `exp` is checked below against the listener's clock.
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.validate_aud = true;
        validation.set_audience(&[UPCALL_AUDIENCE]);
        validation.set_required_spec_claims(&["exp", "aud"]);
        let data = jsonwebtoken::decode::<Claims>(token, &trusted.key, &validation)
            .map_err(|_| TokenError::Invalid)?;
        if data.claims.exp.saturating_add(LEEWAY_S) <= secs(now_ms) {
            return Err(TokenError::Expired);
        }
        Ok(UpcallClaims {
            compute_id: data.claims.compute_id,
            kid,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_round_trips_through_pkcs8() {
        let a = ComputeKeys::generate().expect("a key");
        let b = ComputeKeys::from_pkcs8(Secret::new(a.pkcs8().expose().clone())).expect("same");
        assert_eq!(a.kid(), b.kid());
        assert_eq!(a.jwks(), b.jwks());
        let c = ComputeKeys::generate().expect("a key");
        assert_ne!(a.kid(), c.kid());
        assert!(ComputeKeys::from_pkcs8(Secret::new(vec![0; 48])).is_err());
    }

    #[test]
    fn upcall_tokens_verify_and_expire() {
        let keys = ComputeKeys::generate().expect("a key");
        let id = ComputeId::new();
        let now = 1_791_590_400_000;
        let t = keys
            .issue_upcall_token(&id, now, Duration::from_secs(60))
            .expect("signed");
        let claims = keys.verify_upcall_token(t.expose(), now).expect("valid");
        assert_eq!(claims.compute_id, id.to_string());
        assert_eq!(claims.kid, keys.kid());
        // Within the leeway, then past it.
        assert!(keys.verify_upcall_token(t.expose(), now + 100_000).is_ok());
        assert_eq!(
            keys.verify_upcall_token(t.expose(), now + 121_000),
            Err(TokenError::Expired)
        );
        let ctl = keys
            .issue_compute_ctl_token(&id, now, Duration::from_secs(60))
            .expect("signed");
        assert_eq!(
            keys.verify_upcall_token(ctl.expose(), now),
            Err(TokenError::Invalid)
        );
        assert_eq!(
            keys.verify_upcall_token("a.b.c", now),
            Err(TokenError::Malformed)
        );
    }

    #[test]
    fn a_rotated_key_still_verifies_its_tokens() {
        let old = ComputeKeys::generate().expect("a key");
        let id = ComputeId::new();
        let t = old
            .issue_upcall_token(&id, 0, Duration::from_secs(3600))
            .expect("signed");
        let new = ComputeKeys::generate().expect("a key");
        assert_eq!(
            new.verify_upcall_token(t.expose(), 0),
            Err(TokenError::UnknownKey)
        );
        let new = new.trust(&old);
        assert_eq!(
            new.verify_upcall_token(t.expose(), 0).expect("trusted").kid,
            old.kid()
        );
        assert_eq!(new.jwks()["keys"].as_array().map(Vec::len), Some(2));
        assert_ne!(new.kid(), old.kid());
    }
}
