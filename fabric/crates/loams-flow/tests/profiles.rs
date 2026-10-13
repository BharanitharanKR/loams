//! CN2 plan Task 7: target and HTTP-API profiles (`connectors/profiles/<id>.yaml`),
//! their schema, the module rule of R-CN2-2, secrets by reference only, and a
//! target's effective capabilities (its family's, minus `narrows`).
//!
//! Each case writes its profiles into a fresh directory under cargo's
//! `CARGO_TARGET_TMPDIR`, so the repository's `connectors/profiles/` is only read,
//! never written.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

use loams_flow::manifest::{ConnectorSpec, Delivery, Format, load_manifest, load_manifest_value};
use loams_flow::profile::{
    FAMILY_MODULES, Profile, ProfileError, ProfileKind, check_manifest_profile,
    effective_capabilities, load_profile, load_profiles, resolve,
};
use loams_flow::validate::ManifestSchema;
use serde_json::Value;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn connectors_root() -> PathBuf {
    repo_root().join("connectors")
}

fn kafka() -> ConnectorSpec {
    load_manifest(
        &connectors_root().join("registry/kafka.yaml"),
        &connectors_root(),
    )
    .expect("kafka.yaml loads")
}

/// A fresh, empty directory for one case.
fn scratch(name: &str) -> PathBuf {
    static N: AtomicU32 = AtomicU32::new(0);
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "profiles-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, AtomicOrdering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write(dir: &Path, file: &str, text: &str) -> PathBuf {
    let path = dir.join(file);
    std::fs::write(&path, text).unwrap();
    path
}

/// A target profile on the Kafka family with the given `narrows`.
fn target_yaml(id: &str, narrows: &[&str]) -> String {
    let narrows = narrows
        .iter()
        .map(|n| format!("\"{n}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "apiVersion: loams.flow/v1\nkind: Profile\nid: {id}\nprofile_kind: target\nmodule: kafka\n\
         system:\n  name: Redpanda\n  service: {{ compose: redpanda }}\n\
         defaults: {{ client_id: loams }}\nnarrows: [{narrows}]\n"
    )
}

/// An HTTP-API profile, HubSpot-shaped (the Shared contracts' example), with
/// `extra` appended under `http:`.
fn http_yaml(id: &str, extra: &str) -> String {
    format!(
        "apiVersion: loams.flow/v1\nkind: Profile\nid: {id}\nprofile_kind: http-api\nmodule: http\n\
         system:\n  name: HubSpot\n  service: {{ fixture: conformance/connectors/fixtures/hubspot }}\n\
         http:\n  base_url: \"https://api.hubspot.com\"\n\
         \x20 auth: {{ scheme: oauth2, header: Authorization, token_url: \"https://api.hubapi.com/oauth/v1/token\" }}\n\
         \x20 resources:\n    contacts:\n\
         \x20     list: {{ method: GET, path: \"/crm/v3/objects/contacts\", items: \"/results\", cursor: {{ next: \"/paging/next/after\", param: \"after\" }} }}\n\
         \x20     upsert: {{ method: POST, path: \"/crm/v3/objects/contacts/batch/upsert\", batch: 100, key: \"/id\", body: \"/inputs\" }}\n\
         \x20     async_job: null\n\
         \x20 rate_limit: {{ requests: 100, per_seconds: 10, honour_headers: [\"Retry-After\", \"X-HubSpot-RateLimit-Remaining\"] }}\n\
         \x20 webhook: {{ scheme: hmac-sha256, header: \"X-HubSpot-Signature-v3\" }}\n{extra}"
    )
}

fn invalid_problems(result: Result<Profile, ProfileError>) -> Vec<String> {
    match result {
        Err(ProfileError::Invalid { problems, .. }) => problems,
        other => panic!("expected an invalid profile, got {other:?}"),
    }
}

/// A target profile serves its family's capabilities minus the paths it narrows,
/// and every narrowing is explained by a `narrows:` note on the manifest.
#[test]
fn target_inherits_family_minus_narrows() {
    let spec = kafka();
    let dir = scratch("narrows");
    let path = write(
        &dir,
        "redpanda-lite.yaml",
        &target_yaml("redpanda-lite", &["sink.idempotent", "formats.avro"]),
    );
    let profile = load_profile(&path).expect("the target profile loads");
    assert_eq!(profile.kind, ProfileKind::Target);
    assert!(FAMILY_MODULES.contains(&profile.module.as_str()));

    // No profile: the manifest's own capabilities.
    assert_eq!(effective_capabilities(&spec, None), spec.capabilities);

    // The profile: the same, minus the two narrowed paths, and nothing else.
    let mut expected = spec.capabilities.clone();
    expected.sink.as_mut().unwrap().idempotent = false;
    expected.formats.retain(|f| *f != Format::Avro);
    let effective = effective_capabilities(&spec, Some(&profile));
    assert_eq!(effective, expected);
    assert_ne!(effective, spec.capabilities);

    // Narrowing a whole direction removes it; narrowing the transaction away from an
    // exactly-once sink takes the claim down to what the rest still supports
    // (R-CN2-7: an idempotent sink is effectively once).
    let mut txn = spec.clone();
    txn.capabilities.sink.as_mut().unwrap().transactional = true;
    txn.capabilities.delivery.sink = Delivery::ExactlyOnce;
    let path = write(
        &dir,
        "no-txn.yaml",
        &target_yaml("no-txn", &["sink.transactional", "source"]),
    );
    let no_txn = load_profile(&path).unwrap();
    let effective = effective_capabilities(&txn, Some(&no_txn));
    assert!(effective.source.is_none());
    assert!(!effective.sink.as_ref().unwrap().transactional);
    assert_eq!(effective.delivery.sink, Delivery::EffectivelyOnce);

    // A narrowing must be explained on the manifest that uses the profile.
    let mut uses = spec.clone();
    uses.runtime.profile = Some("redpanda-lite".to_string());
    let errors = check_manifest_profile(&uses, &profile);
    assert!(
        errors
            .iter()
            .any(|e| e.path == "notes" && e.message.contains("sink.idempotent")),
        "an unexplained narrowing must be refused, got {errors:?}"
    );
    uses.notes = vec![
        "narrows: sink.idempotent — the target's producer is not idempotent".to_string(),
        "narrows: formats.avro — no schema registry on the target".to_string(),
    ];
    assert_eq!(check_manifest_profile(&uses, &profile), Vec::new());

    // A narrowing that names no capability is refused when the profile loads.
    let path = write(
        &dir,
        "bad-path.yaml",
        &target_yaml("bad-path", &["sink.teleport"]),
    );
    let problems = invalid_problems(load_profile(&path));
    assert!(
        problems.iter().any(|p| p.contains("sink.teleport")),
        "{problems:?}"
    );

    // The module rule (R-CN2-2): a target names a family module; an HTTP-API profile
    // runs on `http` and carries an `http` block; a target carries none.
    let path = write(
        &dir,
        "not-a-family.yaml",
        &target_yaml("not-a-family", &[]).replace("module: kafka", "module: redis"),
    );
    assert!(!invalid_problems(load_profile(&path)).is_empty());
    let path = write(
        &dir,
        "http-on-kafka.yaml",
        &http_yaml("http-on-kafka", "").replace("module: http", "module: kafka"),
    );
    assert!(!invalid_problems(load_profile(&path)).is_empty());
    let path = write(
        &dir,
        "target-with-http.yaml",
        &(target_yaml("target-with-http", &[])
            + "http:\n  base_url: \"https://x.example\"\n  auth: { scheme: bearer }\n"),
    );
    assert!(!invalid_problems(load_profile(&path)).is_empty());
    let path = write(&dir, "hubspot-like.yaml", &http_yaml("hubspot-like", ""));
    let http = load_profile(&path).expect("the HTTP-API profile loads");
    assert_eq!(http.kind, ProfileKind::HttpApi);
    assert_eq!(http.module, "http");

    // A target's family must be the crate the manifest's native runtime names.
    let mut wrong_family = uses.clone();
    wrong_family.runtime.reference = "loams_connector_postgres::Postgres".to_string();
    let errors = check_manifest_profile(&wrong_family, &profile);
    assert!(
        errors.iter().any(|e| e.path == "runtime.profile"),
        "{errors:?}"
    );

    // The repository's own profiles all load, each under its own id.
    let all = load_profiles(&connectors_root().join("profiles")).expect("connectors/profiles");
    let redpanda = all.get("redpanda").expect("the redpanda target profile");
    assert_eq!(redpanda.kind, ProfileKind::Target);
    assert_eq!(redpanda.module, "kafka");
    for (id, p) in &all {
        assert_eq!(id, &p.id);
    }
}

/// A profile never holds a credential: `auth` says how to authenticate and the
/// instance config names the secrets, which the runtime resolves.
#[test]
fn profile_secrets_by_reference_only() {
    let dir = scratch("secrets");

    // A credential in `auth` is not a field the schema knows, and is a secret by name.
    let path = write(
        &dir,
        "inline-token.yaml",
        &http_yaml("inline-token", "").replace(
            "auth: { scheme: oauth2,",
            "auth: { scheme: oauth2, client_secret: \"s3cr3t-value\",",
        ),
    );
    let problems = invalid_problems(load_profile(&path));
    assert!(
        problems.iter().any(|p| p.contains("client_secret")),
        "{problems:?}"
    );
    assert!(
        problems.iter().all(|p| !p.contains("s3cr3t-value")),
        "a refusal must not echo the value: {problems:?}"
    );

    // A secret-named key anywhere, defaults included.
    let path = write(
        &dir,
        "default-password.yaml",
        &target_yaml("default-password", &[]).replace(
            "defaults: { client_id: loams }",
            "defaults: { sasl: { password: hunter2-hunter2 } }",
        ),
    );
    let problems = invalid_problems(load_profile(&path));
    assert!(
        problems
            .iter()
            .any(|p| p.contains("defaults.sasl.password")),
        "{problems:?}"
    );

    // A credential header among the fixed headers.
    let path = write(
        &dir,
        "auth-header.yaml",
        &http_yaml(
            "auth-header",
            "  headers: { Accept: application/json, Authorization: \"Bearer abc123abc\" }\n",
        ),
    );
    let problems = invalid_problems(load_profile(&path));
    assert!(
        problems.iter().any(|p| p.contains("Authorization")),
        "{problems:?}"
    );

    // Credentials in the base URL.
    let path = write(
        &dir,
        "userinfo.yaml",
        &http_yaml("userinfo", "").replace(
            "https://api.hubspot.com",
            "https://admin:pa55word@api.hubspot.com",
        ),
    );
    let problems = invalid_problems(load_profile(&path));
    assert!(
        problems.iter().any(|p| p.contains("base_url")),
        "{problems:?}"
    );
    assert!(problems.iter().all(|p| !p.contains("pa55word")));

    // Names are not values: a resource called `tokens`, a `token_url` and a
    // `*-Remaining` header to honour are all fine.
    let path = write(
        &dir,
        "vault-like.yaml",
        &http_yaml("vault-like", "").replace("    contacts:\n", "    tokens:\n"),
    );
    let p = load_profile(&path).expect("resource names are not secrets");
    assert!(p.http.unwrap().resources.contains_key("tokens"));

    // Every profile in the repository passes the same rule.
    load_profiles(&connectors_root().join("profiles")).expect("connectors/profiles");
}

/// A profile id that names no profile is refused everywhere it can be named: by
/// the loader for a manifest's `runtime.profile`, by `resolve` for an instance, and
/// a profile file must be named for its id and be of a known kind.
#[test]
fn unknown_profile_refused() {
    let schema = ManifestSchema::embedded().unwrap();
    let text = std::fs::read_to_string(connectors_root().join("registry/kafka.yaml")).unwrap();
    let kafka: Value = serde_norway::from_str(&text).unwrap();

    let mut value = kafka.clone();
    value["runtime"]["profile"] = Value::String("no-such-profile".to_string());
    let errors = load_manifest_value("kafka.yaml", &value, &connectors_root(), &schema)
        .err()
        .unwrap_or_default();
    assert!(
        errors.iter().any(|e| e.path == "runtime.profile"),
        "{errors:?}"
    );

    let profiles: BTreeMap<String, Profile> =
        load_profiles(&connectors_root().join("profiles")).unwrap();
    assert!(resolve(&profiles, "redpanda").is_ok());
    match resolve(&profiles, "no-such-profile") {
        Err(ProfileError::Unknown { id }) => assert_eq!(id, "no-such-profile"),
        other => panic!("expected Unknown, got {other:?}"),
    }

    // A manifest naming a profile that exists but does not validate is refused by
    // the loader too, with the profile's own problems.
    let root = scratch("root");
    std::fs::create_dir_all(root.join("profiles")).unwrap();
    std::fs::create_dir_all(root.join("schemas")).unwrap();
    std::fs::copy(
        connectors_root().join("schemas/kafka.config.json"),
        root.join("schemas/kafka.config.json"),
    )
    .unwrap();
    write(
        &root.join("profiles"),
        "broken.yaml",
        &target_yaml("broken", &[]).replace("module: kafka", "module: no-such-family"),
    );
    let mut value = kafka.clone();
    value["runtime"]["profile"] = Value::String("broken".to_string());
    let errors = load_manifest_value("kafka.yaml", &value, &root, &schema)
        .err()
        .unwrap_or_default();
    assert!(
        errors.iter().any(|e| e.path == "runtime.profile"),
        "{errors:?}"
    );
    // ...and accepted once the profile is a valid target on the manifest's family.
    write(
        &root.join("profiles"),
        "fixed.yaml",
        &target_yaml("fixed", &[]),
    );
    value["runtime"]["profile"] = Value::String("fixed".to_string());
    let spec = load_manifest_value("kafka.yaml", &value, &root, &schema)
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(spec.runtime.profile.as_deref(), Some("fixed"));

    // A file whose id is not its name, and a kind that is not a profile kind.
    let dir = scratch("names");
    write(&dir, "alias.yaml", &target_yaml("redpanda", &[]));
    match load_profiles(&dir) {
        Err(ProfileError::Invalid { file, problems }) => {
            assert!(file.contains("alias.yaml"));
            assert!(
                problems.iter().any(|p| p.contains("redpanda")),
                "{problems:?}"
            );
        }
        other => panic!("expected a name mismatch, got {other:?}"),
    }
    let dir = scratch("kinds");
    let path = write(
        &dir,
        "twin.yaml",
        &target_yaml("twin", &[]).replace("profile_kind: target", "profile_kind: twin"),
    );
    assert!(!invalid_problems(load_profile(&path)).is_empty());
}
