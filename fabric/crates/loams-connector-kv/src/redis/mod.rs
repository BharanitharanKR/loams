//! Redis and Valkey (RESP2), one implementation for three manifests:
//!
//! * `redis` — the ★ connector (CN1 Task 13's semantics, built here directly
//!   on the SDK because CN1 never built it, R0.2): Streams through a consumer
//!   group, and a sink that `XADD`s, `SET`s, `HSET`s or `PUBLISH`es.
//! * `valkey` — the same code against real Valkey, as the target profile
//!   `connectors/profiles/valkey.yaml` (OR-2): keyspace reads by `SCAN` and
//!   typed reads, keyed writes, and Streams.
//! * `redis-streams` — the Streams source and the `XADD` sink as their own
//!   manifest, tested against Valkey and Redis 8 (`ci-service`).
//!
//! Every connection is opened by `sdk::net` (egress-checked after DNS) and
//! handed to the `redis` crate as a stream; TLS (`rediss://`) is the SDK's
//! rustls. The password is a secret *name* in the config.

pub mod keys;
pub mod sink;
pub mod streams;

use std::sync::Arc;

use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, SinkFactory, SourceFactory, SplitEnumerator,
};
use redis::aio::MultiplexedConnection;

use crate::config::{self, Config};

/// The manifest ids this module serves.
pub const IDS: &[&str] = &["redis", "valkey", "redis-streams"];

/// Where and how to connect, parsed from the config. Never holds the
/// password itself: that is resolved at connect time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub db: i64,
}

impl Endpoint {
    /// Parse `redis://host[:port][/db]` or `rediss://…`. User information in
    /// the URL is refused: credentials go by secret name (`user`,
    /// `password`), never in a URL that could be logged.
    pub fn parse(url: &str) -> Result<Endpoint, ConnectorError> {
        let (tls, rest) = if let Some(r) = url.strip_prefix("rediss://") {
            (true, r)
        } else if let Some(r) = url.strip_prefix("redis://") {
            (false, r)
        } else {
            return Err(ConnectorError::fatal(
                "config: `url` must start with redis:// or rediss://",
            ));
        };
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        if authority.contains('@') {
            return Err(ConnectorError::fatal(
                "config: `url` must not carry credentials; name them in `user` and `password`",
            ));
        }
        let (host, port) = split_host_port(authority, 6379)?;
        let db = if path.is_empty() {
            0
        } else {
            path.parse::<i64>().map_err(|_| {
                ConnectorError::fatal("config: the database in `url` must be a number")
            })?
        };
        Ok(Endpoint {
            host,
            port,
            tls,
            db,
        })
    }
}

/// `host[:port]` or `[v6][:port]`.
pub(crate) fn split_host_port(
    authority: &str,
    default: u16,
) -> Result<(String, u16), ConnectorError> {
    if authority.is_empty() {
        return Err(ConnectorError::fatal("config: the address has no host"));
    }
    if let Some(rest) = authority.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err(ConnectorError::fatal(
                "config: an IPv6 address is missing `]`",
            ));
        };
        let host = rest[..end].to_owned();
        let port = match rest[end + 1..].strip_prefix(':') {
            Some(p) => parse_port(p)?,
            None => default,
        };
        return Ok((host, port));
    }
    match authority.rsplit_once(':') {
        Some((h, p)) => Ok((h.to_owned(), parse_port(p)?)),
        None => Ok((authority.to_owned(), default)),
    }
}

fn parse_port(p: &str) -> Result<u16, ConnectorError> {
    p.parse::<u16>()
        .map_err(|_| ConnectorError::fatal("config: the port must be a number below 65536"))
}

/// Open a connection for `ctx`: the config's `url`, `user`, `password`
/// (secret name) and `database`.
pub async fn connect(
    ctx: &ConnectorContext,
    cfg: &Config,
) -> Result<MultiplexedConnection, ConnectorError> {
    let ep = Endpoint::parse(&cfg.required_str("url")?)?;
    let db = match cfg.u64("database")? {
        Some(d) => i64::try_from(d)
            .map_err(|_| ConnectorError::fatal("config: `database` is too large"))?,
        None => ep.db,
    };
    let mut info = redis::RedisConnectionInfo::default()
        .set_db(db)
        .set_lib_name("loams-connector-kv", env!("CARGO_PKG_VERSION"));
    if let Some(user) = cfg.str("user")? {
        info = info.set_username(user);
    }
    if let Some(pw) = config::secret(ctx, cfg, "password").await? {
        info = info.set_password(config::secret_text(&pw, "password")?);
    }
    let result = if ep.tls {
        let stream =
            loams_connector_sdk::net::connect_tls(&ep.host, ep.port, &*ctx.egress, None).await?;
        MultiplexedConnection::new_with_config(&info, stream, connection_config())
            .await
            .map(|(c, driver)| {
                tokio::spawn(driver);
                c
            })
    } else {
        let stream = loams_connector_sdk::net::connect(&ep.host, ep.port, &*ctx.egress).await?;
        MultiplexedConnection::new_with_config(&info, stream, connection_config())
            .await
            .map(|(c, driver)| {
                tokio::spawn(driver);
                c
            })
    };
    result.map_err(|e| classify(&e, &format!("connecting to {}:{}", ep.host, ep.port)))
}

/// The `redis` crate's connection settings. Its default response timeout
/// (500 ms) is shorter than a blocking `XREADGROUP`, so it is raised to a
/// minute; the Streams source caps `block_ms` below it.
fn connection_config() -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::default()
        .set_response_timeout(Some(std::time::Duration::from_secs(60)))
        .set_connection_timeout(Some(std::time::Duration::from_secs(10)))
}

/// The longest `XREADGROUP … BLOCK` the Streams source issues.
pub const MAX_BLOCK: std::time::Duration = std::time::Duration::from_secs(30);

/// A Redis error as an SDK error class. The message names the operation and
/// the server's error code, never the command's arguments.
pub fn classify(e: &redis::RedisError, what: &str) -> ConnectorError {
    use redis::ErrorKind;
    let code = e.code().unwrap_or("");
    let msg = format!("{what}: {}", e.kind_name());
    match e.kind() {
        ErrorKind::AuthenticationFailed => ConnectorError::auth(msg),
        _ if matches!(code, "NOAUTH" | "WRONGPASS" | "NOPERM") => ConnectorError::auth(msg),
        _ if code == "LOADING" || code == "BUSY" || code == "TRYAGAIN" || code == "MASTERDOWN" => {
            ConnectorError::retryable(msg)
        }
        _ if e.is_io_error()
            || e.is_timeout()
            || e.is_connection_dropped()
            || e.is_connection_refusal() =>
        {
            ConnectorError::retryable(msg)
        }
        _ => ConnectorError::fatal(msg),
    }
}

trait KindName {
    fn kind_name(&self) -> String;
}

impl KindName for redis::RedisError {
    /// The error's category and server code (`WRONGTYPE`, `NOGROUP`), not its
    /// detail, which can quote the command.
    fn kind_name(&self) -> String {
        match self.code() {
            Some(code) => format!("server error {code}"),
            None => format!("{:?}", self.kind()),
        }
    }
}

/// Source and sink modes. `None` = not set in the config: the default is
/// Streams when a stream is configured (or the manifest is
/// `redis-streams`), else the keyspace.
fn streams_mode(id: &str, cfg: &Config, key: &str) -> Result<bool, ConnectorError> {
    match cfg.str(key)?.as_deref() {
        Some("stream") | Some("xadd") => Ok(true),
        Some(_) => Ok(false),
        None => Ok(id == "redis-streams"
            || cfg.get("stream").is_some()
            || cfg.get("streams").is_some()),
    }
}

/// The factory for one of [`IDS`]. A Valkey instance carries the `valkey`
/// target profile in its context; the code path is the same.
#[derive(Debug, Clone, Copy)]
pub struct RedisFactory {
    id: &'static str,
}

impl RedisFactory {
    /// `id` must be one of [`IDS`].
    pub fn new(id: &'static str) -> Self {
        debug_assert!(IDS.contains(&id));
        Self { id }
    }
}

impl ConnectorFactory for RedisFactory {
    fn id(&self) -> &'static str {
        self.id
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Config::of(ctx);
        if streams_mode(self.id, &cfg, "source_mode")? {
            let s = streams::StreamsSettings::from_config(self.id, ctx, &cfg)?;
            Ok(Some((
                Box::new(streams::StreamsEnumerator::new(s.streams.clone())),
                Box::new(streams::StreamsSourceFactory::new(ctx.clone(), cfg, s)),
            )))
        } else {
            let s = keys::ScanSettings::from_config(&cfg)?;
            Ok(Some((
                Box::new(keys::ScanEnumerator),
                Box::new(keys::ScanSourceFactory::new(self.id, ctx.clone(), cfg, s)),
            )))
        }
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = Config::of(ctx);
        let mode = match cfg.str("sink_mode")? {
            Some(m) => m,
            None if streams_mode(self.id, &cfg, "sink_mode")? => "xadd".to_owned(),
            None => "set".to_owned(),
        };
        let settings = match mode.as_str() {
            "xadd" => sink::WriteMode::Xadd {
                stream: cfg.required_str("stream")?,
                max_len: cfg.u64("max_len")?,
            },
            "set" => sink::WriteMode::Set {
                key_template: key_template(&cfg)?,
                ttl_seconds: cfg.u64("ttl_seconds")?,
            },
            "hset" => sink::WriteMode::Hset {
                key_template: key_template(&cfg)?,
                field_template: cfg.str("field_template")?,
            },
            "publish" => sink::WriteMode::Publish {
                channel: cfg.required_str("channel")?,
            },
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: `sink_mode` {other:?} is not one of xadd, set, hset, publish"
                )));
            }
        };
        Ok(Some(Box::new(sink::RedisSinkFactory::new(
            ctx.clone(),
            cfg,
            settings,
        ))))
    }
}

/// The key template of `set` and `hset`: `key` (the upsert key the manifest
/// rule names), or CN1's `key_template`, or `{subject}`.
fn key_template(cfg: &Config) -> Result<String, ConnectorError> {
    Ok(match cfg.str("key")? {
        Some(k) => k,
        None => cfg
            .str("key_template")?
            .unwrap_or_else(|| "{subject}".to_owned()),
    })
}

/// The three factories of this module.
pub fn factories() -> Vec<Arc<dyn ConnectorFactory>> {
    IDS.iter()
        .map(|id| Arc::new(RedisFactory::new(id)) as Arc<dyn ConnectorFactory>)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_parse_and_refuse_inline_credentials() {
        assert_eq!(
            Endpoint::parse("redis://valkey:6380/2").expect("url"),
            Endpoint {
                host: "valkey".into(),
                port: 6380,
                tls: false,
                db: 2
            }
        );
        assert_eq!(
            Endpoint::parse("rediss://[::1]").expect("url"),
            Endpoint {
                host: "::1".into(),
                port: 6379,
                tls: true,
                db: 0
            }
        );
        let e = Endpoint::parse("redis://admin:hunter2@h:1").expect_err("credentials refused");
        assert!(!e.message.contains("hunter2"));
        assert!(Endpoint::parse("http://h").is_err());
    }

    #[test]
    fn mode_defaults_follow_the_manifest_and_config() {
        let none = Config::new(serde_json::json!({}));
        let stream = Config::new(serde_json::json!({ "stream": "s" }));
        assert!(streams_mode("redis-streams", &none, "source_mode").expect("mode"));
        assert!(!streams_mode("valkey", &none, "source_mode").expect("mode"));
        assert!(streams_mode("valkey", &stream, "source_mode").expect("mode"));
        let keys = Config::new(serde_json::json!({ "stream": "s", "source_mode": "keys" }));
        assert!(!streams_mode("redis", &keys, "source_mode").expect("mode"));
    }
}
