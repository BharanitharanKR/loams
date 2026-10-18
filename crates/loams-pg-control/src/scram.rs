//! Role passwords as Postgres's SCRAM-SHA-256 verifiers (RFC 5802, RFC
//! 7677; PG2 Task 24): what a compute spec carries for a role, so
//! `compute_ctl` sets `rolpassword` to it (`ALTER ROLE … PASSWORD
//! 'SCRAM-SHA-256$…'`) and the compute never sees the password.
//!
//! - **The format** is Postgres's own (`pg_authid.rolpassword`):
//!   `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`, base64.
//! - **Stable.** A spec is a pure function of its records (§28 §5.3), so a
//!   role's verifier must not change from one spec to the next: its salt is
//!   an HMAC, keyed by the stack's random [`SaltKey`], of the role's
//!   [`SecretRef`], which is fresh for every password (a reset names a new
//!   one). So each password gets its own salt, and the same reference and
//!   password give different salts on different stacks.
//! - **The salt key** is [`SALT_KEY_LEN`] bytes from the OS generator,
//!   stored once in the [`SecretStore`] under [`SALT_KEY_REF`] (with
//!   `create`, so instances that start together share one) and read back
//!   on every start, as the signing key is. It is a new secret, not a change
//!   to any record: roles keep their records, and a role whose verifier was
//!   made before the key existed gets its new one on the next push, from
//!   the password the store already holds.
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

use rand::TryRngCore;

use crate::secrets::{Secret, SecretError, SecretRef, SecretStore};

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

/// Where every `pg-control` instance finds the salt key in its secret
/// store. Not a role reference, so the sweep of unreferenced role secrets
/// never deletes it.
pub const SALT_KEY_REF: &str = "pg-control-scram-salt-key";

/// The salt key's length in bytes.
pub const SALT_KEY_LEN: usize = 32;

/// The stack's key for role salts: random, made once, kept in the secret
/// store. Its `Debug` prints nothing of it.
#[derive(Clone)]
pub struct SaltKey(hmac::Key);

impl SaltKey {
    /// A key from `bytes` (at least [`SALT_KEY_LEN`] of them).
    ///
    /// # Errors
    ///
    /// [`SecretError::Corrupt`] for a shorter one.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SecretError> {
        if bytes.len() < SALT_KEY_LEN {
            return Err(SecretError::Corrupt(format!(
                "the SCRAM salt key {SALT_KEY_REF} is {} bytes, not {SALT_KEY_LEN}",
                bytes.len()
            )));
        }
        Ok(SaltKey(hmac::Key::new(hmac::HMAC_SHA256, bytes)))
    }

    /// A fresh key from the OS generator, as bytes to store.
    ///
    /// # Errors
    ///
    /// [`SecretError::Unavailable`] when the generator fails.
    pub fn generate() -> Result<Secret<Vec<u8>>, SecretError> {
        let mut bytes = vec![0u8; SALT_KEY_LEN];
        rand::rngs::OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(|e| SecretError::Unavailable(format!("the OS random generator: {e}")))?;
        Ok(Secret::new(bytes))
    }
}

impl fmt::Debug for SaltKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SaltKey([redacted])")
    }
}

/// The salt key `secrets` holds, created there first if it holds none. The
/// key is always the one read back from the store, so of instances that
/// start together all use the one `create` kept. A stored key that is too
/// short is an error, never replaced.
///
/// # Errors
///
/// The store's, except that a key missing right after its `create` is
/// [`SecretError::Unavailable`] (a [`SecretError::NotFound`] from here
/// would read as a role's missing password).
pub async fn load_or_create_salt_key(secrets: &dyn SecretStore) -> Result<SaltKey, SecretError> {
    let r = SecretRef::parse(SALT_KEY_REF)?;
    let not_found = |e: SecretError| match e {
        SecretError::NotFound(_) => {
            SecretError::Unavailable(format!("the SCRAM salt key {SALT_KEY_REF} is missing"))
        }
        e => e,
    };
    match secrets.get(&r).await {
        Ok(stored) => return SaltKey::from_bytes(stored.expose()),
        Err(SecretError::NotFound(_)) => {}
        Err(e) => return Err(e),
    }
    if secrets.create(&r, SaltKey::generate()?).await? {
        tracing::info!(secret_ref = %r, "stored a new SCRAM salt key");
    }
    let stored = secrets.get(&r).await.map_err(not_found)?;
    SaltKey::from_bytes(stored.expose())
}

/// The salt of the password stored under `r`: the first [`SALT_LEN`]
/// bytes of an HMAC-SHA-256, under the stack's `key`, of the reference. The
/// same password under the same reference on the same stack gives the same
/// verifier; on another stack, another.
pub fn salt_for(key: &SaltKey, r: &SecretRef) -> [u8; SALT_LEN] {
    let mut ctx = hmac::Context::with_key(&key.0);
    ctx.update(b"loams-pg-role-scram-salt\0");
    ctx.update(r.as_str().as_bytes());
    let tag = ctx.sign();
    let mut salt = [0u8; SALT_LEN];
    salt.copy_from_slice(&tag.as_ref()[..SALT_LEN]);
    salt
}

/// The verifier of the password stored under `r`.
pub fn role_verifier(password: &[u8], key: &SaltKey, r: &SecretRef) -> String {
    verifier(password, &salt_for(key, r), ITERATIONS)
}

/// Role verifiers from the [`SecretStore`], each derived once per
/// reference. Specs (the upcall listener's and the endpoint reconciler's)
/// read them through one shared instance.
pub struct RoleVerifiers {
    secrets: Arc<dyn SecretStore>,
    key: tokio::sync::OnceCell<SaltKey>,
    cache: Mutex<HashMap<SecretRef, Arc<str>>>,
}

impl RoleVerifiers {
    pub fn new(secrets: Arc<dyn SecretStore>) -> Self {
        RoleVerifiers {
            secrets,
            key: tokio::sync::OnceCell::new(),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// The stack's salt key, loaded (or created) on first use and kept.
    ///
    /// # Errors
    ///
    /// [`load_or_create_salt_key`]'s; a later call tries again.
    pub async fn salt_key(&self) -> Result<&SaltKey, SecretError> {
        self.key
            .get_or_try_init(|| load_or_create_salt_key(self.secrets.as_ref()))
            .await
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
        let key = self.salt_key().await?;
        let secret = self.secrets.get(r).await?;
        let v: Arc<str> = role_verifier(secret.expose(), key, r).into();
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

    fn key() -> SaltKey {
        SaltKey::from_bytes(SaltKey::generate().unwrap().expose()).unwrap()
    }

    #[test]
    fn salt_follows_the_reference() {
        let k = key();
        let a = SecretRef::parse("pg-role-a-1").unwrap();
        let b = SecretRef::parse("pg-role-a-2").unwrap();
        assert_eq!(salt_for(&k, &a), salt_for(&k, &a));
        assert_ne!(salt_for(&k, &a), salt_for(&k, &b));
        assert_eq!(role_verifier(b"pw", &k, &a), role_verifier(b"pw", &k, &a));
        assert_ne!(role_verifier(b"pw", &k, &a), role_verifier(b"pw", &k, &b));
        assert!(!role_verifier(b"pw-plain", &k, &a).contains("pw-plain"));
    }

    /// Two stacks (two salt keys) with the same reference and password make
    /// different verifiers.
    #[test]
    fn salt_differs_between_stacks() {
        let (k1, k2) = (key(), key());
        let r = SecretRef::parse("pg-role-a-1").unwrap();
        assert_ne!(salt_for(&k1, &r), salt_for(&k2, &r));
        assert_ne!(role_verifier(b"pw", &k1, &r), role_verifier(b"pw", &k2, &r));
        assert!(SaltKey::from_bytes(&[0u8; SALT_KEY_LEN - 1]).is_err());
        assert_eq!(format!("{k1:?}"), "SaltKey([redacted])");
    }

    /// What the server does on login (RFC 5802 §3): from the client's
    /// password and the verifier's salt and iterations, the client key, whose
    /// hash must be the verifier's StoredKey.
    fn login(password: &[u8], verifier: &str) -> bool {
        let b64 = base64::engine::general_purpose::STANDARD;
        let rest = verifier.strip_prefix(PREFIX).unwrap();
        let (params, keys) = rest.split_once('$').unwrap();
        let (iters, salt) = params.split_once(':').unwrap();
        let (stored, _server) = keys.split_once(':').unwrap();
        let iters: NonZeroU32 = iters.parse().unwrap();
        let salt = b64.decode(salt).unwrap();
        let mut salted = [0u8; digest::SHA256_OUTPUT_LEN];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            iters,
            &salt,
            password,
            &mut salted,
        );
        let key = hmac::Key::new(hmac::HMAC_SHA256, &salted);
        let client_key = hmac::sign(&key, b"Client Key");
        b64.encode(digest::digest(&digest::SHA256, client_key.as_ref()).as_ref()) == stored
    }

    #[derive(Default)]
    struct Mem(Mutex<HashMap<String, Vec<u8>>>);

    #[async_trait::async_trait]
    impl SecretStore for Mem {
        async fn put(&self, r: &SecretRef, s: Secret<Vec<u8>>) -> Result<(), SecretError> {
            self.0
                .lock()
                .unwrap()
                .insert(r.as_str().into(), s.expose().clone());
            Ok(())
        }
        async fn create(&self, r: &SecretRef, s: Secret<Vec<u8>>) -> Result<bool, SecretError> {
            let mut m = self.0.lock().unwrap();
            if m.contains_key(r.as_str()) {
                return Ok(false);
            }
            m.insert(r.as_str().into(), s.expose().clone());
            Ok(true)
        }
        async fn get(&self, r: &SecretRef) -> Result<Secret<Vec<u8>>, SecretError> {
            self.0
                .lock()
                .unwrap()
                .get(r.as_str())
                .map(|v| Secret::new(v.clone()))
                .ok_or_else(|| SecretError::NotFound(r.to_string()))
        }
        async fn delete(&self, r: &SecretRef) -> Result<(), SecretError> {
            self.0.lock().unwrap().remove(r.as_str());
            Ok(())
        }
        async fn list(&self) -> Result<Vec<SecretRef>, SecretError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .keys()
                .filter_map(|k| SecretRef::parse(k).ok())
                .collect())
        }
    }

    /// Two stacks, each with two roles under the same password: four
    /// different verifiers; each stable across passes and across a restart
    /// (a new cache on the same store); each one a password login accepts.
    #[tokio::test]
    async fn verifiers_are_per_stack_stable_and_log_in() {
        let password = b"pw-same-everywhere";
        let a = SecretRef::parse("pg-role-a-1").unwrap();
        let b = SecretRef::parse("pg-role-a-2").unwrap();
        let mut all = Vec::new();
        for _stack in 0..2 {
            let store: Arc<Mem> = Arc::default();
            for r in [&a, &b] {
                store.put(r, Secret::new(password.to_vec())).await.unwrap();
            }
            let v = RoleVerifiers::new(store.clone());
            let (va, vb) = (v.get(&a).await.unwrap(), v.get(&b).await.unwrap());
            assert_ne!(va, vb);
            assert_eq!(v.get(&a).await.unwrap(), va, "stable across passes");
            // A restart reads the stored key back.
            let restarted = RoleVerifiers::new(store.clone());
            assert_eq!(
                restarted.get(&a).await.unwrap(),
                va,
                "stable across restarts"
            );
            assert!(login(password, &va) && login(password, &vb));
            assert!(!login(b"wrong", &va));
            // The key is not a role secret: the sweep leaves it.
            let key_ref = SecretRef::parse(SALT_KEY_REF).unwrap();
            assert!(store.list().await.unwrap().contains(&key_ref));
            assert_eq!(key_ref.role_parts(), None);
            all.extend([va, vb]);
        }
        let distinct: std::collections::HashSet<_> = all.iter().collect();
        assert_eq!(distinct.len(), 4, "same password, different verifiers");
    }

    /// Instances that start together end on one key.
    #[tokio::test]
    async fn racing_instances_share_one_key() {
        let store: Arc<Mem> = Arc::default();
        let r = SecretRef::parse("pg-role-a-1").unwrap();
        store.put(&r, Secret::new(b"pw".to_vec())).await.unwrap();
        let (x, y) = (
            RoleVerifiers::new(store.clone()),
            RoleVerifiers::new(store.clone()),
        );
        let (vx, vy) = tokio::join!(x.get(&r), y.get(&r));
        assert_eq!(vx.unwrap(), vy.unwrap());
    }
}
