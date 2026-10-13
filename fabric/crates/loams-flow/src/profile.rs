//! Target and HTTP-API profiles (CN2 plan Task 7, Shared contracts "Profiles"):
//! `connectors/profiles/<id>.yaml`, checked against
//! `connectors/schema/profile.schema.json` and the rules the schema cannot
//! express.
//!
//! * A **target profile** lets one family implementation serve a
//!   protocol-compatible system (Redpanda on the Kafka family), with the
//!   target's own manifest, narrowing and suite (OR-2). Its
//!   [`effective_capabilities`] are the manifest's minus the profile's
//!   `narrows`, and each narrowing is explained by a `narrows: <path> …` note on
//!   the manifest ([`check_manifest_profile`]).
//! * An **HTTP-API profile** describes a REST API declaratively; the `http`
//!   family's profile engine runs it (OR-11).
//! * R-CN2-2's module rule: a target's `module` is a family module
//!   ([`FAMILY_MODULES`]) and an HTTP-API profile's is `http`; a target carries no
//!   `http` block. Where the manifest's native runtime already names a family
//!   crate (`loams_connector_<family>::…`), it must be the profile's.
//! * Secrets by reference only (Review Focus 8): no key anywhere in a profile
//!   names a credential, no fixed header carries one, and `base_url` has no user
//!   information. A refusal names the key, never the value.
//!
//! The types are the SDK's ([`loams_connector_sdk::profile`]), because a
//! `ConnectorContext` carries the profile an instance runs with (plan ruling
//! R4.4).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde_json::Value;

pub use loams_connector_sdk::profile::{HttpProfile, Profile, ProfileKind, SystemRef};

use crate::manifest::{Capabilities, ConnectorSpec, Delivery, Format, ManifestError};

/// The family modules a target profile may run on: the `loams-connector-<family>`
/// crates of the plan's file structure (OR-6). `profile.schema.json`'s `module`
/// enum is the same list (a unit test keeps the two equal).
pub const FAMILY_MODULES: &[&str] = &[
    "http",
    "kafka",
    "postgres",
    "mysql",
    "sqlserver",
    "objects",
    "kv",
    "search",
    "mongodb",
    "analytics",
    "lakehouse",
    "brokers",
    "cloud",
    "protocols",
];

/// The `http` family: every HTTP-API profile's module.
pub const HTTP_MODULE: &str = "http";

/// Why a profile could not be loaded or found.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProfileError {
    /// The file is not there or not readable.
    #[error("{file}: cannot read the profile: {message}")]
    Read {
        /// The profile's file name.
        file: String,
        /// The I/O error.
        message: String,
    },
    /// The profile breaks its schema or a rule; every problem is listed.
    #[error("{file}: invalid profile: {}", problems.join("; "))]
    Invalid {
        /// The profile's file name.
        file: String,
        /// Each problem, naming the field (never a secret's value).
        problems: Vec<String>,
    },
    /// No profile has this id.
    #[error("no profile {id:?} under connectors/profiles/ (R-CN2-2)")]
    Unknown {
        /// The id asked for.
        id: String,
    },
    /// `profile.schema.json` itself does not load.
    #[error("the profile schema does not load: {0}")]
    Schema(String),
}

/// `connectors/schema/profile.schema.json` compiled once.
#[derive(Debug)]
pub struct ProfileSchema {
    validator: jsonschema::Validator,
}

impl ProfileSchema {
    /// The compiled schema at `path`.
    pub fn load(path: &Path) -> Result<Self, ProfileError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| ProfileError::Schema(format!("{}: {e}", path.display())))?;
        let document: Value = serde_json::from_str(&text)
            .map_err(|e| ProfileError::Schema(format!("{}: {e}", path.display())))?;
        let validator = jsonschema::validator_for(&document)
            .map_err(|e| ProfileError::Schema(format!("{}: {e}", path.display())))?;
        Ok(Self { validator })
    }

    /// Every violation of the schema, with instance values masked: a refused
    /// profile may hold a credential, and the refusal must not repeat it.
    pub fn violations(&self, value: &Value) -> Vec<String> {
        self.validator
            .iter_errors(value)
            .map(|error| {
                let at = dotted(&error.instance_path().to_string());
                let at = if at.is_empty() {
                    "profile".to_string()
                } else {
                    at
                };
                format!("{at}: {}", error.masked())
            })
            .collect()
    }
}

/// The profile schema's path in this repository.
pub fn default_profile_schema_path() -> PathBuf {
    crate::manifest::repository_root().join("connectors/schema/profile.schema.json")
}

/// The profiles directory in this repository.
pub fn default_profiles_dir() -> PathBuf {
    crate::manifest::repository_root().join("connectors/profiles")
}

fn default_schema() -> Result<&'static ProfileSchema, ProfileError> {
    static SCHEMA: OnceLock<Result<ProfileSchema, ProfileError>> = OnceLock::new();
    SCHEMA
        .get_or_init(|| ProfileSchema::load(&default_profile_schema_path()))
        .as_ref()
        .map_err(Clone::clone)
}

/// The profile at `path`, checked against the repository's profile schema and
/// the rules of this module. Its id must be its file name.
pub fn load_profile(path: &Path) -> Result<Profile, ProfileError> {
    load_profile_with_schema(path, default_schema()?)
}

/// [`load_profile`] against an already-compiled schema.
pub fn load_profile_with_schema(
    path: &Path,
    schema: &ProfileSchema,
) -> Result<Profile, ProfileError> {
    let file = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string());
    let text = std::fs::read_to_string(path).map_err(|e| ProfileError::Read {
        file: file.clone(),
        message: e.to_string(),
    })?;
    let invalid = |problems: Vec<String>| ProfileError::Invalid {
        file: file.clone(),
        problems,
    };
    let value: Value =
        serde_norway::from_str(&text).map_err(|e| invalid(vec![format!("not valid YAML: {e}")]))?;

    let mut problems = schema.violations(&value);
    problems.extend(secret_problems(&value));
    if let Some(id) = value.get("id").and_then(Value::as_str) {
        let stem = path.file_stem().map(|s| s.to_string_lossy().into_owned());
        if stem.as_deref() != Some(id) {
            problems.push(format!(
                "id: the id {id:?} is not the file name (expected {id}.yaml)"
            ));
        }
    }
    if let Some(narrows) = value.get("narrows").and_then(Value::as_array) {
        for path in narrows.iter().filter_map(Value::as_str) {
            if let Err(message) = narrow(&mut probe_capabilities(), path) {
                problems.push(format!("narrows: {message}"));
            }
        }
    }
    if !problems.is_empty() {
        return Err(invalid(problems));
    }
    let profile: Profile = serde_json::from_value(value)
        .map_err(|e| invalid(vec![format!("does not deserialise: {e}")]))?;
    let mut problems = Vec::new();
    if !FAMILY_MODULES.contains(&profile.module.as_str()) {
        problems.push(format!(
            "module: {:?} is not a family module",
            profile.module
        ));
    }
    match profile.kind {
        ProfileKind::HttpApi if profile.module != HTTP_MODULE || profile.http.is_none() => problems
            .push("module: an http-api profile runs on `http` and has an `http` block".into()),
        ProfileKind::Target if profile.http.is_some() => {
            problems.push("http: only an http-api profile has an `http` block".into())
        }
        _ => {}
    }
    if problems.is_empty() {
        Ok(profile)
    } else {
        Err(invalid(problems))
    }
}

/// Every `*.yaml` under `dir`, by id. The first profile that does not load is
/// the error.
pub fn load_profiles(dir: &Path) -> Result<BTreeMap<String, Profile>, ProfileError> {
    let schema = default_schema()?;
    let entries = std::fs::read_dir(dir).map_err(|e| ProfileError::Read {
        file: dir.display().to_string(),
        message: e.to_string(),
    })?;
    let mut paths: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "yaml"))
        .collect();
    paths.sort();
    let mut out = BTreeMap::new();
    for path in paths {
        let profile = load_profile_with_schema(&path, schema)?;
        out.insert(profile.id.clone(), profile);
    }
    Ok(out)
}

/// The profile `id` names, or [`ProfileError::Unknown`].
pub fn resolve<'a>(
    profiles: &'a BTreeMap<String, Profile>,
    id: &str,
) -> Result<&'a Profile, ProfileError> {
    profiles
        .get(id)
        .ok_or_else(|| ProfileError::Unknown { id: id.to_string() })
}

/// The manifest's capabilities as the profile's target has them: the family's
/// (the manifest's own), minus every path in `narrows`. A delivery claim the
/// narrowed sink can no longer make comes down to the strongest one it still
/// can (R-CN2-7): `exactly_once` needs `transactional`, `effectively_once` needs
/// `idempotent` or `upsert`, and `at_least_once` is the floor (OR-7).
pub fn effective_capabilities(spec: &ConnectorSpec, p: Option<&Profile>) -> Capabilities {
    let mut caps = spec.capabilities.clone();
    let Some(p) = p else {
        return caps;
    };
    for path in &p.narrows {
        // Validated when the profile loaded; a path that names nothing narrows nothing.
        let _ = narrow(&mut caps, path);
    }
    if let Some(sink) = &caps.sink {
        let dedup = sink.idempotent || sink.upsert;
        let delivery = &mut caps.delivery.sink;
        if *delivery == Delivery::ExactlyOnce && !sink.transactional {
            *delivery = if dedup {
                Delivery::EffectivelyOnce
            } else {
                Delivery::AtLeastOnce
            };
        }
        if *delivery == Delivery::EffectivelyOnce && !dedup && !sink.transactional {
            *delivery = Delivery::AtLeastOnce;
        }
    }
    caps
}

/// Narrow `caps` by one capability path: `source` or `sink` (the direction),
/// `source.<flag>` or `sink.<flag>` (a boolean), `bulk.arrow`, or
/// `formats.<slug>`. An `Err` says why the path names no capability.
pub fn narrow(caps: &mut Capabilities, path: &str) -> Result<(), String> {
    let unknown = || format!("{path:?} names no capability");
    let (head, tail) = match path.split_once('.') {
        Some((h, t)) => (h, Some(t)),
        None => (path, None),
    };
    match (head, tail) {
        ("source", None) => caps.source = None,
        ("sink", None) => caps.sink = None,
        ("source", Some(flag)) => {
            // A direction the manifest lacks has nothing to narrow, but the
            // flag must still be one a source has.
            let mut probe = probe_capabilities().source;
            let target = caps.source.as_mut().or(probe.as_mut());
            *target
                .and_then(|s| source_flag(s, flag))
                .ok_or_else(unknown)? = false;
        }
        ("sink", Some(flag)) => {
            let mut probe = probe_capabilities().sink;
            let target = caps.sink.as_mut().or(probe.as_mut());
            *target
                .and_then(|s| sink_flag(s, flag))
                .ok_or_else(unknown)? = false;
        }
        ("bulk", Some("arrow")) => caps.bulk.arrow = false,
        ("formats", Some(slug)) => {
            let format = Format::from_slug(slug).ok_or_else(unknown)?;
            caps.formats.retain(|f| *f != format);
        }
        _ => return Err(unknown()),
    }
    Ok(())
}

/// A full set of capabilities, to check a `narrows` path against when no
/// manifest is at hand.
fn probe_capabilities() -> Capabilities {
    use crate::manifest::{
        Backpressure, BulkCaps, DeliveryCaps, Evolution, Ordering, RegistryRequirement, SchemaCaps,
        SinkCaps, SourceCaps,
    };
    Capabilities {
        source: Some(SourceCaps {
            streaming: true,
            batch: true,
            cdc: true,
            webhook: true,
            resumable: true,
            position: String::new(),
        }),
        sink: Some(SinkCaps {
            streaming: true,
            batch: true,
            transactional: true,
            upsert: true,
            delete: true,
            idempotent: true,
            reply: true,
        }),
        delivery: DeliveryCaps {
            source: Delivery::AtLeastOnce,
            sink: Delivery::AtLeastOnce,
        },
        ordering: Ordering::ALL[0],
        formats: Format::ALL.to_vec(),
        schema: SchemaCaps {
            registry: RegistryRequirement::ALL[0],
            evolution: Evolution::ALL[0],
        },
        bulk: BulkCaps {
            arrow: true,
            max_batch_rows: 1,
        },
        backpressure: Backpressure::ALL[0],
    }
}

fn source_flag<'a>(s: &'a mut crate::manifest::SourceCaps, flag: &str) -> Option<&'a mut bool> {
    Some(match flag {
        "streaming" => &mut s.streaming,
        "batch" => &mut s.batch,
        "cdc" => &mut s.cdc,
        "webhook" => &mut s.webhook,
        "resumable" => &mut s.resumable,
        _ => return None,
    })
}

fn sink_flag<'a>(s: &'a mut crate::manifest::SinkCaps, flag: &str) -> Option<&'a mut bool> {
    Some(match flag {
        "streaming" => &mut s.streaming,
        "batch" => &mut s.batch,
        "transactional" => &mut s.transactional,
        "upsert" => &mut s.upsert,
        "delete" => &mut s.delete,
        "idempotent" => &mut s.idempotent,
        "reply" => &mut s.reply,
        _ => return None,
    })
}

/// R-CN2-2's rules between a manifest and the profile its `runtime.profile`
/// names: a family crate named by the manifest's native runtime is the
/// profile's module, and every narrowing has a `narrows: <path> …` note.
pub fn check_manifest_profile(spec: &ConnectorSpec, profile: &Profile) -> Vec<ManifestError> {
    let mut errors = Vec::new();
    let family_crate = format!("loams_connector_{}", profile.module);
    let refs = [
        Some(spec.runtime.reference.as_str()),
        spec.runtime.source.as_ref().map(|o| o.reference.as_str()),
        spec.runtime.sink.as_ref().map(|o| o.reference.as_str()),
    ];
    for reference in refs.into_iter().flatten() {
        let krate = reference.split("::").next().unwrap_or_default();
        if krate.starts_with("loams_connector_") && krate != family_crate {
            errors.push(ManifestError::new(
                "runtime.profile",
                format!(
                    "the profile {:?} runs on the {} family, but the runtime names {krate} \
                     (R-CN2-2)",
                    profile.id, profile.module
                ),
            ));
        }
    }
    for path in &profile.narrows {
        let prefix = format!("narrows: {path}");
        let explained = spec.notes.iter().any(|note| {
            note.strip_prefix(&prefix).is_some_and(|rest| {
                rest.is_empty()
                    || rest.starts_with(|c: char| c.is_whitespace() || ":,;".contains(c))
            })
        });
        if !explained {
            errors.push(ManifestError::new(
                "notes",
                format!(
                    "the profile {:?} narrows {path}, and no `narrows: {path} …` note says why",
                    profile.id
                ),
            ));
        }
    }
    errors
}

/// Keys a profile may not have: anything that names a credential. Compared
/// case-insensitively, with `-` read as `_`.
const SECRET_KEY_PARTS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "access_key",
    "credential",
    "authorization",
    "cookie",
    "session",
];

/// Keys that look like secrets by name and are not: they hold a URL.
const NOT_SECRET_KEYS: &[&str] = &["token_url"];

fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase().replace('-', "_");
    !NOT_SECRET_KEYS.contains(&k.as_str()) && SECRET_KEY_PARTS.iter().any(|p| k.contains(p))
}

/// `{auth.<field>}`: a reference to a secret the instance config names.
fn is_secret_ref(value: &Value) -> bool {
    value
        .as_str()
        .and_then(|s| s.strip_prefix("{auth."))
        .and_then(|s| s.strip_suffix('}'))
        .is_some_and(|f| {
            !f.is_empty()
                && f.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
}

/// Every place a profile holds (or could hold) a credential, by path.
fn secret_problems(value: &Value) -> Vec<String> {
    let mut problems = Vec::new();
    walk_keys(value, "", &mut problems);
    if let Some(url) = value.pointer("/http/base_url").and_then(Value::as_str) {
        let authority = url.split("://").nth(1).unwrap_or_default();
        let authority = authority.split('/').next().unwrap_or_default();
        if authority.contains('@') {
            problems.push(
                "http.base_url: a URL with user information holds a credential; \
                 name it in the instance config's secrets instead"
                    .to_string(),
            );
        }
    }
    problems
}

fn walk_keys(value: &Value, at: &str, problems: &mut Vec<String>) {
    let Value::Object(map) = value else {
        if let Value::Array(items) = value {
            for (i, item) in items.iter().enumerate() {
                walk_keys(item, &format!("{at}.{i}"), problems);
            }
        }
        return;
    };
    for (key, child) in map {
        let here = if at.is_empty() {
            key.clone()
        } else {
            format!("{at}.{key}")
        };
        // A credential-named key whose value is a secret reference
        // (`{auth.<field>}`, resolved by the runtime) holds no credential
        // (CN2 Task 20: PostHog's `api_key` body field).
        if is_secret_ref(child) {
            continue;
        }
        // Resource names are names, not values (Vault has `tokens`).
        let is_resource_name = at == "http.resources";
        if !is_resource_name && is_secret_key(key) {
            problems.push(format!(
                "{here}: a profile never holds a credential (Review Focus 8); the instance \
                 config names its secrets and the runtime resolves them"
            ));
            continue;
        }
        walk_keys(child, &here, problems);
    }
}

fn dotted(pointer: &str) -> String {
    pointer
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn family_modules_match_the_schema() {
        let text = std::fs::read_to_string(default_profile_schema_path()).expect("schema");
        let schema: Value = serde_json::from_str(&text).expect("json");
        let listed: Vec<&str> = schema["properties"]["module"]["enum"]
            .as_array()
            .expect("module enum")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert_eq!(listed, FAMILY_MODULES);
    }

    #[test]
    fn secret_keys_by_name() {
        for key in [
            "password",
            "client_secret",
            "X-Api-Key",
            "Authorization",
            "access_token",
        ] {
            assert!(is_secret_key(key), "{key}");
        }
        for key in [
            "token_url",
            "header",
            "scheme",
            "base_url",
            "items",
            "cursor",
        ] {
            assert!(!is_secret_key(key), "{key}");
        }
    }

    /// CN2 Task 20 (R20.4): a credential-named body field may hold a secret
    /// *reference*, never a value.
    #[test]
    fn secret_references_are_not_credentials() {
        let ok = serde_json::json!({"http": {"resources": {"events": {"create": {
            "envelope": {"api_key": "{auth.api_key}"}}}}}});
        assert!(secret_problems(&ok).is_empty());
        for value in ["phc_live_value", "{auth.}", "{auth.API}", "x{auth.api_key}"] {
            let bad = serde_json::json!({"envelope": {"api_key": value}});
            assert_eq!(secret_problems(&bad).len(), 1, "{value}");
        }
    }
}
