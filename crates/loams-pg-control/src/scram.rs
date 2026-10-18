//! Role passwords as Postgres's SCRAM-SHA-256 verifiers (RFC 5802, RFC
//! 7677; PG2 Task 24): what a compute spec carries for a role, so
//! `compute_ctl` sets `rolpassword` to it (`ALTER ROLE … PASSWORD
//! 'SCRAM-SHA-256$…'`) and the compute never sees the password.
//!
//! - **The format** is Postgres's own (`pg_authid.rolpassword`):
//!   `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`, base64.
//! - **Stable.** A spec is a pure function of its records (§28 §5.3), so a
//!   role's verifier must not change from one spec to the next: its salt is
//!   derived from the role's [`SecretRef`], which is fresh for every
//!   password (a reset names a new one), so each password still gets its
//!   own salt.
//! - **Cached.** [`RoleVerifiers`] reads a role's password from the
//!   [`SecretStore`] and derives its verifier once per reference: the
//!   secret under a reference never changes.
//! - The password never leaves this module except as its verifier, and
//!   neither appears in a log line or a `Debug`.

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use ring::{digest, hmac, pbkdf2};

use crate::secrets::{SecretError, SecretRef, SecretStore};

/// Postgres's default `scram_iterations`.
pub const ITERATIONS: NonZeroU32 = NonZeroU32::new(4096).expect("4096 is not zero");

/// The salt's length, as Postgres makes it (`SCRAM_DEFAULT_SALT_LEN`).
pub const SALT_LEN: usize = 16;

/// The verifier prefix `compute_ctl` checks for (anything else it takes
/// for an MD5 hash).
pub const PREFIX: &str = "SCRAM-SHA-256$";

/// The SCRAM-SHA-256 verifier of `password` with `salt`.
pub fn verifier(password: &[u8], salt: &[u8], iterations: NonZeroU32) -> String {
    let mut salted = zeroize::Zeroizing::new([0u8; digest::SHA256_OUTPUT_LEN]);
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iterations,
        salt,
        password,
        salted.as_mut(),
    );
    let key = hmac::Key::new(hmac::HMAC_SHA256, salted.as_ref());
    let client_key = hmac::sign(&key, b"Client Key");
    let stored_key = digest::digest(&digest::SHA256, client_key.as_ref());
    let server_key = hmac::sign(&key, b"Server Key");
    let b64 = base64::engine::general_purpose::STANDARD;
    format!(
        "{PREFIX}{iterations}:{}${}:{}",
        b64.encode(salt),
        b64.encode(stored_key.as_ref()),
        b64.encode(server_key.as_ref()),
    )
}

/// The salt of the password stored under `r`: the first [`SALT_LEN`]
/// bytes of a SHA-256 of the reference, so the same password under the
/// same reference gives the same verifier.
pub fn salt_for(r: &SecretRef) -> [u8; SALT_LEN] {
    let mut ctx = digest::Context::new(&digest::SHA256);
    ctx.update(b"loams-pg-role-scram-salt\0");
    ctx.update(r.as_str().as_bytes());
    let d = ctx.finish();
    let mut salt = [0u8; SALT_LEN];
    salt.copy_from_slice(&d.as_ref()[..SALT_LEN]);
    salt
}

/// The verifier of the password stored under `r`.
pub fn role_verifier(password: &[u8], r: &SecretRef) -> String {
    verifier(password, &salt_for(r), ITERATIONS)
}

/// Role verifiers from the [`SecretStore`], each derived once per
/// reference. Specs (the upcall listener's and the endpoint reconciler's)
/// read them through one shared instance.
pub struct RoleVerifiers {
    secrets: Arc<dyn SecretStore>,
    cache: Mutex<HashMap<SecretRef, Arc<str>>>,
}

impl RoleVerifiers {
    pub fn new(secrets: Arc<dyn SecretStore>) -> Self {
        RoleVerifiers {
            secrets,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// The verifier of the role password under `r`.
    ///
    /// # Errors
    ///
    /// The secret store's: [`SecretError::NotFound`] for a password that is
    /// gone (deleted, or a reset moved the role on).
    pub async fn get(&self, r: &SecretRef) -> Result<Arc<str>, SecretError> {
        if let Some(v) = self.lock().get(r) {
            return Ok(v.clone());
        }
        let secret = self.secrets.get(r).await?;
        let v: Arc<str> = role_verifier(secret.expose(), r).into();
        let mut cache = self.lock();
        // A bound on what a long-lived process keeps: references of reset
        // or deleted roles are never asked for again.
        if cache.len() >= 65_536 {
            cache.clear();
        }
        cache.insert(r.clone(), v.clone());
        Ok(v)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<SecretRef, Arc<str>>> {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl fmt::Debug for RoleVerifiers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoleVerifiers")
            .field("cached", &self.lock().len())
            .finish_non_exhaustive()
    }
}

/// Two handles are the same source only when they are the same instance.
impl PartialEq for RoleVerifiers {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for RoleVerifiers {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Against a verifier computed independently (Python's hashlib:
    /// PBKDF2-HMAC-SHA256, then HMAC "Client Key"/"Server Key").
    #[test]
    fn verifier_matches_reference() {
        let salt: Vec<u8> = (0u8..16).collect();
        assert_eq!(
            verifier(b"pencil", &salt, ITERATIONS),
            "SCRAM-SHA-256$4096:AAECAwQFBgcICQoLDA0ODw==$zHCdol2044/ZyWzPLi7oxApCkamKw9Z+E4U/QApd/5Y=:dd5peBOitVnLNFu7VmwP+HiDaaw4OUCv396eVCWhYiE="
        );
    }

    #[test]
    fn salt_follows_the_reference() {
        let a = SecretRef::parse("pg-role-a-1").unwrap();
        let b = SecretRef::parse("pg-role-a-2").unwrap();
        assert_eq!(salt_for(&a), salt_for(&a));
        assert_ne!(salt_for(&a), salt_for(&b));
        assert_eq!(role_verifier(b"pw", &a), role_verifier(b"pw", &a));
        assert_ne!(role_verifier(b"pw", &a), role_verifier(b"pw", &b));
        assert!(!role_verifier(b"pw-plain", &a).contains("pw-plain"));
    }
}
