//! `pg-control`'s signing key across instances and restarts (PG2 Task 9,
//! R8.9): every instance reads the one key the secret store holds, the
//! first start creates it there exactly once, and nothing ever signs with a
//! key of its own that the store does not hold.

#[allow(unused_macros)]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::MemorySecrets;
use loams_pg_control::ids::ComputeId;
use loams_pg_control::secrets::{
    FileSecretStore, KeySource, Secret, SecretError, SecretRef, SecretStore,
};
use loams_pg_control::store::conformance::{Factory, local_factory};
use loams_pg_control::upcall::{SIGNING_KEY_REF, SigningKeyError, load_or_create_signing_key};

const NOW: u64 = 1_791_590_400_000;

/// The backend `common`'s store helpers use (unused here).
#[allow(dead_code)]
fn factory() -> Factory {
    local_factory(env!("CARGO_TARGET_TMPDIR"))
}

fn signing_ref() -> SecretRef {
    SecretRef::parse(SIGNING_KEY_REF).expect("a valid reference")
}

#[tokio::test]
async fn two_instances_share_one_signing_key() {
    let secrets = Arc::new(MemorySecrets::default());
    let r = signing_ref();
    // Two instances start at once against one store: both read the key
    // that won the create, so each accepts the other's tokens.
    let (a, b) = tokio::join!(
        load_or_create_signing_key(secrets.as_ref(), &r),
        load_or_create_signing_key(secrets.as_ref(), &r)
    );
    let (a, b) = (a.expect("instance a"), b.expect("instance b"));
    assert_eq!(a.kid(), b.kid());
    assert_eq!(secrets.len(), 1, "one key stored");

    let compute = ComputeId::new();
    let from_a = a
        .issue_upcall_token(&compute, NOW, Duration::from_secs(600))
        .expect("signed by a");
    let from_b = b
        .issue_upcall_token(&compute, NOW, Duration::from_secs(600))
        .expect("signed by b");
    assert_eq!(
        b.verify_upcall_token(from_a.expose(), NOW)
            .expect("b accepts a's token")
            .kid,
        a.kid()
    );
    assert!(a.verify_upcall_token(from_b.expose(), NOW).is_ok());
}

#[tokio::test]
async fn a_restart_keeps_verifying() {
    let dir = tempfile::Builder::new()
        .prefix("signing-key-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("a temporary directory");
    let open = || {
        FileSecretStore::open(
            dir.path().join("secrets.age"),
            &KeySource::File(dir.path().join("identity")),
        )
        .expect("the store")
    };
    let compute = ComputeId::new();
    let (kid, token) = {
        let store = open();
        let keys = load_or_create_signing_key(&store, &signing_ref())
            .await
            .expect("first start");
        let token = keys
            .issue_upcall_token(&compute, NOW, Duration::from_secs(600))
            .expect("signed");
        (keys.kid().to_owned(), token)
    };
    // The process is gone; the next start reads the same key back.
    let again = load_or_create_signing_key(&open(), &signing_ref())
        .await
        .expect("after a restart");
    assert_eq!(again.kid(), kid);
    assert_eq!(
        again
            .verify_upcall_token(token.expose(), NOW)
            .expect("verified")
            .compute_id,
        compute.to_string()
    );
}

#[tokio::test]
async fn a_bad_stored_key_refuses_to_start() {
    // A key that does not parse is never replaced by a fresh one: computes
    // started under the real key would be locked out.
    let secrets = MemorySecrets::default();
    let r = signing_ref();
    secrets
        .put(&r, Secret::new(b"not a pkcs8 document".to_vec()))
        .await
        .expect("put");
    let e = load_or_create_signing_key(&secrets, &r)
        .await
        .expect_err("refused");
    assert!(matches!(e, SigningKeyError::Key(_)), "{e}");
    assert_eq!(
        secrets.value(&r).as_deref(),
        Some(&b"not a pkcs8 document"[..]),
        "left as it was"
    );
}

#[tokio::test]
async fn an_unavailable_store_refuses_to_start() {
    let secrets = MemorySecrets::default();
    secrets.fail_puts(true);
    let e = load_or_create_signing_key(&secrets, &signing_ref())
        .await
        .expect_err("refused");
    assert!(
        matches!(e, SigningKeyError::Store(SecretError::Unavailable(_))),
        "{e}"
    );
    assert_eq!(secrets.len(), 0);
}

#[tokio::test]
async fn create_keeps_the_first_secret() {
    let secrets = MemorySecrets::default();
    let r = signing_ref();
    assert!(
        secrets
            .create(&r, Secret::new(b"first".to_vec()))
            .await
            .expect("created")
    );
    assert!(
        !secrets
            .create(&r, Secret::new(b"second".to_vec()))
            .await
            .expect("not replaced")
    );
    assert_eq!(secrets.value(&r).as_deref(), Some(&b"first"[..]));
}
