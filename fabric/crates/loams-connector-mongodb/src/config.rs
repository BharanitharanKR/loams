//! An instance's configuration (the keys the guide's table lists). Secrets
//! are named, never given: `password`, `tls_cert` and `tls_key` hold secret
//! names that the instance's `SecretResolver` turns into values at connect
//! time (Global Constraints, "No secret").

use std::time::Duration;

use loams_connector_sdk::schema::SchemaPolicy;
use loams_connector_sdk::{ConnectorError, InstanceKey, Profile};
use serde_json::Value;

/// The checkpoint collection's default name, in the sink's database.
pub const CHECKPOINTS: &str = "_loams_checkpoints";

/// What the source reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// A snapshot of the collections, in `_id`-range splits.
    Batch,
    /// A change stream, after an optional snapshot.
    Cdc,
}

/// How an authenticated connection proves who it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMechanism {
    /// No authentication.
    None,
    /// `SCRAM-SHA-256` with `username` and the `password` secret (`basic`).
    ScramSha256,
    /// `MONGODB-X509`: the TLS client certificate is the identity (`mtls`).
    X509,
}

/// `fullDocument` for update events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullDocument {
    /// No document for updates, only the update description.
    Off,
    /// The document as it is when the event is read (may be newer).
    UpdateLookup,
    /// The post-image (6.0+, `changeStreamPreAndPostImages` on the
    /// collection), when available.
    WhenAvailable,
    /// The post-image, or the stream fails.
    Required,
}

impl FullDocument {
    pub fn server_value(self) -> &'static str {
        match self {
            FullDocument::Off => "default",
            FullDocument::UpdateLookup => "updateLookup",
            FullDocument::WhenAvailable => "whenAvailable",
            FullDocument::Required => "required",
        }
    }
}

/// `fullDocumentBeforeChange` (pre-images, 6.0+).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeforeChange {
    Off,
    WhenAvailable,
    Required,
}

impl BeforeChange {
    pub fn server_value(self) -> &'static str {
        match self {
            BeforeChange::Off => "off",
            BeforeChange::WhenAvailable => "whenAvailable",
            BeforeChange::Required => "required",
        }
    }
}

/// How the sink writes a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    /// Replace (or patch, for a change event's update description) by key,
    /// inserting when absent; delete on `loamsop = d`. Idempotent.
    Upsert,
    /// Insert every event as a new document. Not idempotent: exactly once
    /// only inside the sink's transaction.
    Insert,
}

/// TLS settings. `ca_pem` is a certificate (public); `cert` and `key` are
/// secret names.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TlsConfig {
    pub ca_pem: Option<String>,
    pub cert: Option<String>,
    pub key: Option<String>,
    pub server_name: Option<String>,
}

/// The sink's settings (present when the config names a `collection`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkConfig {
    pub collection: String,
    pub write: Write,
    /// The fields a document is matched on (`["_id"]` by default).
    pub key: Vec<String>,
    pub apply_deletes: bool,
    /// Exactly once through a transaction with the checkpoint (replica
    /// sets); `false` writes each batch on its own (effectively once for
    /// `upsert`, at least once for `insert`).
    pub transactional: bool,
    pub checkpoint_collection: String,
}

#[derive(Debug, Clone)]
pub struct MongoConfig {
    pub hosts: Vec<(String, u16)>,
    pub replica_set: Option<String>,
    pub database: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub auth_source: String,
    pub auth_mechanism: AuthMechanism,
    pub tls: Option<TlsConfig>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    // Source.
    pub mode: Mode,
    pub collections: Vec<String>,
    pub max_splits: usize,
    pub split_rows: u64,
    pub snapshot: bool,
    pub full_document: FullDocument,
    pub before_change: BeforeChange,
    pub heartbeat: Duration,
    pub await_ms: u64,
    pub window_interval: Duration,
    pub schema: SchemaPolicy,
    // Sink.
    pub sink: Option<SinkConfig>,
}

fn fatal(m: impl Into<String>) -> ConnectorError {
    ConnectorError::fatal(format!("config: {}", m.into()))
}

fn opt_str(c: &Value, k: &str) -> Result<Option<String>, ConnectorError> {
    match c.get(k) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if !s.is_empty() => Ok(Some(s.clone())),
        Some(Value::String(_)) => Err(fatal(format!("{k} is empty"))),
        Some(_) => Err(fatal(format!("{k} must be a string"))),
    }
}

fn opt_u64(c: &Value, k: &str, default: u64) -> Result<u64, ConnectorError> {
    match c.get(k) {
        None | Some(Value::Null) => Ok(default),
        Some(v) => v
            .as_u64()
            .ok_or_else(|| fatal(format!("{k} must be a non-negative integer"))),
    }
}

fn opt_bool(c: &Value, k: &str, default: bool) -> Result<bool, ConnectorError> {
    match c.get(k) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Bool(b)) => Ok(*b),
        Some(_) => Err(fatal(format!("{k} must be true or false"))),
    }
}

fn str_list(c: &Value, k: &str) -> Result<Vec<String>, ConnectorError> {
    match c.get(k) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(a)) => a
            .iter()
            .map(|v| match v {
                Value::String(s) if !s.is_empty() => Ok(s.clone()),
                _ => Err(fatal(format!("{k} must be a list of non-empty strings"))),
            })
            .collect(),
        Some(_) => Err(fatal(format!("{k} must be a list of strings"))),
    }
}

/// A database or collection name the server would accept.
fn check_name(kind: &str, s: &str) -> Result<(), ConnectorError> {
    let bad = s.is_empty()
        || s.contains('\0')
        || (kind == "database" && s.contains(['/', '\\', '.', ' ', '"', '$']))
        || (kind == "collection" && (s.starts_with("system.") || s.contains('$')));
    if bad {
        return Err(fatal(format!("{s:?} is not a valid {kind} name")));
    }
    Ok(())
}

/// `host`, `host:port`, or `[v6]:port`.
pub fn parse_host(s: &str) -> Result<(String, u16), ConnectorError> {
    let bad = || fatal(format!("host {s:?} must be host or host:port"));
    if s.contains('@') || s.contains("://") || s.contains('/') {
        // A URI could carry credentials; hosts are names only.
        return Err(fatal(format!(
            "host {s:?}: give host names in `hosts`, not a connection URI"
        )));
    }
    if let Some(rest) = s.strip_prefix('[') {
        let (h, tail) = rest.split_once(']').ok_or_else(bad)?;
        let port = match tail.strip_prefix(':') {
            Some(p) => p.parse().map_err(|_| bad())?,
            None if tail.is_empty() => 27017,
            None => return Err(bad()),
        };
        return Ok((h.to_owned(), port));
    }
    match s.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() && !h.contains(':') => {
            Ok((h.to_owned(), p.parse().map_err(|_| bad())?))
        }
        None if !s.is_empty() => Ok((s.to_owned(), 27017)),
        _ => Err(bad()),
    }
}

/// Merge a target profile's `defaults` under the user's config.
pub fn merge_defaults(config: &Value, profile: Option<&Profile>) -> Value {
    let mut out = profile
        .map(|p| p.defaults.clone())
        .filter(Value::is_object)
        .unwrap_or_else(|| Value::Object(Default::default()));
    if let (Value::Object(o), Value::Object(c)) = (&mut out, config) {
        for (k, v) in c {
            o.insert(k.clone(), v.clone());
        }
    }
    out
}

impl MongoConfig {
    pub fn parse(c: &Value) -> Result<Self, ConnectorError> {
        if !c.is_object() {
            return Err(fatal("the instance config must be an object"));
        }
        let mut hosts = Vec::new();
        for h in str_list(c, "hosts")? {
            hosts.push(parse_host(&h)?);
        }
        if let Some(h) = opt_str(c, "host")? {
            let (host, port) = parse_host(&h)?;
            let port = match c.get("port") {
                Some(Value::Number(n)) => n
                    .as_u64()
                    .and_then(|p| u16::try_from(p).ok())
                    .ok_or_else(|| fatal("port must be 1-65535"))?,
                _ => port,
            };
            hosts.push((host, port));
        }
        if hosts.is_empty() {
            return Err(fatal("hosts (or host) is required"));
        }
        let database = opt_str(c, "database")?.ok_or_else(|| fatal("database is required"))?;
        check_name("database", &database)?;
        let username = opt_str(c, "username")?;
        let password = opt_str(c, "password")?;
        let tls = match c.get("tls") {
            None | Some(Value::Null) | Some(Value::Bool(false)) => None,
            Some(Value::Bool(true)) => Some(TlsConfig::default()),
            Some(t @ Value::Object(_)) => Some(TlsConfig {
                ca_pem: opt_str(t, "ca_pem")?,
                cert: opt_str(t, "cert")?,
                key: opt_str(t, "key")?,
                server_name: opt_str(t, "server_name")?,
            }),
            Some(_) => return Err(fatal("tls must be true, false or an object")),
        };
        if let Some(t) = &tls
            && t.cert.is_some() != t.key.is_some()
        {
            return Err(fatal("tls.cert and tls.key go together"));
        }
        let auth_mechanism = match opt_str(c, "auth_mechanism")?.as_deref() {
            None if username.is_some() => AuthMechanism::ScramSha256,
            None => AuthMechanism::None,
            Some("SCRAM-SHA-256") => AuthMechanism::ScramSha256,
            Some("MONGODB-X509") => AuthMechanism::X509,
            Some(other) => {
                return Err(fatal(format!(
                    "auth_mechanism {other:?}: SCRAM-SHA-256 or MONGODB-X509"
                )));
            }
        };
        match auth_mechanism {
            AuthMechanism::ScramSha256 if username.is_none() || password.is_none() => {
                return Err(fatal("SCRAM-SHA-256 needs username and password"));
            }
            AuthMechanism::X509 if tls.as_ref().is_none_or(|t| t.cert.is_none()) => {
                return Err(fatal("MONGODB-X509 needs tls.cert and tls.key"));
            }
            _ => {}
        }
        let auth_source = match auth_mechanism {
            AuthMechanism::X509 => "$external".to_owned(),
            _ => opt_str(c, "auth_source")?.unwrap_or_else(|| "admin".to_owned()),
        };
        let mode = match opt_str(c, "mode")?.as_deref() {
            None | Some("batch") => Mode::Batch,
            Some("cdc") => Mode::Cdc,
            Some(other) => return Err(fatal(format!("mode {other:?}: batch or cdc"))),
        };
        let collections = str_list(c, "collections")?;
        for coll in &collections {
            check_name("collection", coll)?;
        }
        let snapshot = match opt_str(c, "snapshot")?.as_deref() {
            None | Some("initial") => true,
            Some("never") => false,
            Some(other) => return Err(fatal(format!("snapshot {other:?}: initial or never"))),
        };
        let full_document = match opt_str(c, "full_document")?.as_deref() {
            None | Some("update_lookup") => FullDocument::UpdateLookup,
            Some("off") => FullDocument::Off,
            Some("when_available") => FullDocument::WhenAvailable,
            Some("required") => FullDocument::Required,
            Some(other) => {
                return Err(fatal(format!(
                    "full_document {other:?}: update_lookup, when_available, required or off"
                )));
            }
        };
        let before_change = match opt_str(c, "before_change")?.as_deref() {
            None | Some("off") => BeforeChange::Off,
            Some("when_available") => BeforeChange::WhenAvailable,
            Some("required") => BeforeChange::Required,
            Some(other) => {
                return Err(fatal(format!(
                    "before_change {other:?}: off, when_available or required"
                )));
            }
        };
        let sink = match opt_str(c, "collection")? {
            None => None,
            Some(collection) => {
                check_name("collection", &collection)?;
                let write = match opt_str(c, "write")?.as_deref() {
                    None | Some("upsert") => Write::Upsert,
                    Some("insert") => Write::Insert,
                    Some(other) => {
                        return Err(fatal(format!("write {other:?}: upsert or insert")));
                    }
                };
                let mut key = str_list(c, "key")?;
                if key.is_empty() {
                    key.push("_id".to_owned());
                }
                let checkpoint_collection =
                    opt_str(c, "checkpoint_collection")?.unwrap_or_else(|| CHECKPOINTS.to_owned());
                check_name("collection", &checkpoint_collection)?;
                Some(SinkConfig {
                    collection,
                    write,
                    key,
                    apply_deletes: opt_bool(c, "apply_deletes", true)?,
                    transactional: opt_bool(c, "transactional", true)?,
                    checkpoint_collection,
                })
            }
        };
        let max_splits = opt_u64(c, "max_splits", 8)?.clamp(1, 1024) as usize;
        Ok(Self {
            hosts,
            replica_set: opt_str(c, "replica_set")?,
            database,
            username,
            password,
            auth_source,
            auth_mechanism,
            tls,
            connect_timeout: Duration::from_millis(opt_u64(c, "connect_timeout_ms", 10_000)?),
            request_timeout: Duration::from_millis(opt_u64(c, "request_timeout_ms", 60_000)?),
            mode,
            collections,
            max_splits,
            split_rows: opt_u64(c, "split_rows", 1_000_000)?.max(1),
            snapshot,
            full_document,
            before_change,
            heartbeat: Duration::from_millis(opt_u64(c, "heartbeat_ms", 10_000)?),
            await_ms: opt_u64(c, "await_ms", 1000)?.clamp(1, 60_000),
            window_interval: Duration::from_millis(opt_u64(c, "window_ms", 30_000)?),
            schema: SchemaPolicy::from_config(c)?,
            sink,
        })
    }

    /// The Loams-written id of an instance in this database's checkpoint
    /// collection.
    pub fn instance_id(instance: &InstanceKey) -> String {
        format!("{}/{}", instance.namespace, instance.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_minimal_source_and_a_minimal_sink() {
        let c = MongoConfig::parse(&json!({"hosts": ["db:27018", "db2"], "database": "app"}))
            .expect("source");
        assert_eq!(
            c.hosts,
            vec![("db".to_owned(), 27018), ("db2".to_owned(), 27017)]
        );
        assert_eq!(
            (c.mode, c.auth_mechanism),
            (Mode::Batch, AuthMechanism::None)
        );
        assert!(c.sink.is_none() && c.snapshot);
        let s = MongoConfig::parse(&json!({
            "host": "db", "port": 1234, "database": "app", "collection": "out",
            "username": "u", "password": "db-password",
        }))
        .expect("sink");
        assert_eq!(s.hosts, vec![("db".to_owned(), 1234)]);
        assert_eq!(s.auth_mechanism, AuthMechanism::ScramSha256);
        let sink = s.sink.expect("sink");
        assert_eq!(sink.key, vec!["_id".to_owned()]);
        assert!(sink.transactional && sink.apply_deletes);
        assert_eq!(sink.write, Write::Upsert);
        assert_eq!(sink.checkpoint_collection, "_loams_checkpoints");
    }

    #[test]
    fn bad_configs_are_fatal_with_a_reason() {
        for (c, why) in [
            (json!({"database": "app"}), "hosts"),
            (json!({"hosts": ["h"]}), "database"),
            (
                json!({"hosts": ["mongodb://u:p@h"], "database": "a"}),
                "URI",
            ),
            (json!({"hosts": ["h:x"], "database": "a"}), "host:port"),
            (json!({"hosts": ["h"], "database": "a.b"}), "database name"),
            (
                json!({"hosts": ["h"], "database": "a", "username": "u"}),
                "password",
            ),
            (
                json!({"hosts": ["h"], "database": "a", "auth_mechanism": "MONGODB-X509"}),
                "tls.cert",
            ),
            (
                json!({"hosts": ["h"], "database": "a", "tls": {"cert": "c"}}),
                "together",
            ),
            (
                json!({"hosts": ["h"], "database": "a", "mode": "stream"}),
                "batch or cdc",
            ),
            (
                json!({"hosts": ["h"], "database": "a", "collection": "system.x"}),
                "collection name",
            ),
            (
                json!({"hosts": ["h"], "database": "a", "evolution": "loose"}),
                "evolution",
            ),
        ] {
            let err = MongoConfig::parse(&c).expect_err(why);
            assert!(err.message.contains(why), "{why}: {}", err.message);
        }
    }

    #[test]
    fn ipv6_hosts_and_x509() {
        assert_eq!(
            parse_host("[::1]:27019").expect("v6"),
            ("::1".to_owned(), 27019)
        );
        assert_eq!(
            parse_host("[fe80::1]").expect("v6"),
            ("fe80::1".to_owned(), 27017)
        );
        let c = MongoConfig::parse(&json!({
            "hosts": ["h"], "database": "a", "auth_mechanism": "MONGODB-X509",
            "tls": {"cert": "client-cert", "key": "client-key", "ca_pem": "-----BEGIN"},
        }))
        .expect("x509");
        assert_eq!(c.auth_source, "$external");
        assert_eq!(c.tls.expect("tls").cert.as_deref(), Some("client-cert"));
    }

    #[test]
    fn profile_defaults_sit_under_the_users_config() {
        let mut p = Profile {
            id: "t".into(),
            kind: loams_connector_sdk::ProfileKind::Target,
            module: "mongodb".into(),
            system: loams_connector_sdk::SystemRef {
                name: "x".into(),
                service: json!({}),
            },
            defaults: json!({"database": "d", "tls": true}),
            narrows: vec![],
            http: None,
        };
        let merged = merge_defaults(&json!({"database": "mine"}), Some(&p));
        assert_eq!(merged, json!({"database": "mine", "tls": true}));
        p.defaults = Value::Null;
        assert_eq!(merge_defaults(&json!({"a": 1}), Some(&p)), json!({"a": 1}));
    }
}
