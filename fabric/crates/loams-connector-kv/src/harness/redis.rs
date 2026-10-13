//! The Valkey and Redis harnesses: `valkey` (keyspace → keyed writes, under
//! the `valkey` target profile) and `redis-streams` (a stream → `XADD` to a
//! second stream), each against the real server of its compose service.

use std::sync::Arc;

use async_trait::async_trait;
use loams_connector_sdk::{
    ConnectorContext, ConnectorFactory, InstanceKey, Profile, Scrubber, StaticSecrets,
};
use loams_flow_conformance::{HarnessError, Observed, ServiceKind};
use redis::Value;
use redis::aio::MultiplexedConnection;
use serde_json::{Value as Json, json};

use super::{KvSystem, run_tag, unavailable};
use crate::config::Config;
use crate::redis::RedisFactory;

/// Valkey 8 in `connectors-p2` (deploy/fabric/compose.yaml, host port 6380).
pub const VALKEY: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "valkey",
    system: "Valkey",
}];

/// Redis 8 in `connectors-ci-only` (host port 6379; OR-3).
pub const REDIS: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "redis",
    system: "Redis",
}];

/// The Valkey URL: `LOAMS_VALKEY_URL`, else the compose service's host port.
pub fn valkey_url() -> String {
    std::env::var("LOAMS_VALKEY_URL").unwrap_or_else(|_| "redis://127.0.0.1:6380".to_owned())
}

/// The Redis URL: `LOAMS_REDIS_URL`, else the compose service's host port.
pub fn redis_url() -> String {
    std::env::var("LOAMS_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned())
}

/// The secret name the `basic` auth check resolves, and its value.
const PASSWORD_SECRET: &str = "kv-harness-password";
const PASSWORD: &str = "loams-harness-acl-pass";

/// A direct connection, as the connector opens one.
pub async fn direct(
    url: &str,
    extra: Json,
    secrets: StaticSecrets,
) -> Result<MultiplexedConnection, HarnessError> {
    let mut cfg = json!({ "url": url });
    if let (Some(o), Json::Object(e)) = (cfg.as_object_mut(), extra) {
        o.extend(e);
    }
    let ctx = ConnectorContext::new(
        InstanceKey::new(1, "kv-harness"),
        cfg.clone(),
        Arc::new(secrets),
        Scrubber::new(),
    );
    crate::redis::connect(&ctx, &Config::new(cfg))
        .await
        .map_err(|e| unavailable("connecting", e))
}

async fn q(c: &mut MultiplexedConnection, cmd: &mut redis::Cmd) -> Result<Value, HarnessError> {
    cmd.query_async(c)
        .await
        .map_err(|e| HarnessError::failed(format!("redis: {e}")))
}

/// Delete every key matching `pattern`.
async fn delete_matching(c: &mut MultiplexedConnection, pattern: &str) -> Result<(), HarnessError> {
    let mut cursor = 0u64;
    loop {
        let reply = q(
            c,
            redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(500),
        )
        .await?;
        let (next, keys) = crate::redis::keys::parse_scan_reply(&reply)
            .map_err(|e| HarnessError::failed(e.to_string()))?;
        if !keys.is_empty() {
            q(c, redis::cmd("DEL").arg(&keys)).await?;
        }
        if next == 0 {
            return Ok(());
        }
        cursor = next;
    }
}

/// Every key matching `pattern`.
async fn keys_matching(
    c: &mut MultiplexedConnection,
    pattern: &str,
) -> Result<Vec<String>, HarnessError> {
    let mut out = Vec::new();
    let mut cursor = 0u64;
    loop {
        let reply = q(
            c,
            redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(500),
        )
        .await?;
        let (next, keys) = crate::redis::keys::parse_scan_reply(&reply)
            .map_err(|e| HarnessError::failed(e.to_string()))?;
        out.extend(keys);
        if next == 0 {
            return Ok(out);
        }
        cursor = next;
    }
}

/// `none`: connect without credentials and move a record. `basic`: create an
/// ACL user limited to `prefix`, connect as it with the password resolved
/// from the secret store, move a record, and drop the user.
async fn authenticate(url: &str, prefix: &str, method: &str) -> Result<(), HarnessError> {
    let key = format!("{prefix}:auth");
    match method {
        "none" => {
            let mut c = direct(url, json!({}), StaticSecrets::new()).await?;
            q(&mut c, redis::cmd("SET").arg(&key).arg("ok")).await?;
            q(&mut c, redis::cmd("DEL").arg(&key)).await?;
            Ok(())
        }
        "basic" => {
            let mut admin = direct(url, json!({}), StaticSecrets::new()).await?;
            let user = format!("{prefix}-user").replace(':', "-");
            q(
                &mut admin,
                redis::cmd("ACL")
                    .arg("SETUSER")
                    .arg(&user)
                    .arg("on")
                    .arg(format!(">{PASSWORD}"))
                    .arg(format!("~{prefix}:*"))
                    .arg("+@all"),
            )
            .await?;
            let secrets = StaticSecrets::new().with(PASSWORD_SECRET, PASSWORD);
            let moved = async {
                let mut c = direct(
                    url,
                    json!({ "user": user, "password": PASSWORD_SECRET }),
                    secrets,
                )
                .await?;
                q(&mut c, redis::cmd("SET").arg(&key).arg("ok")).await?;
                let v = q(&mut c, redis::cmd("GET").arg(&key)).await?;
                q(&mut c, redis::cmd("DEL").arg(&key)).await?;
                if v == Value::BulkString(b"ok".to_vec()) {
                    Ok(())
                } else {
                    Err(HarnessError::failed(
                        "the ACL user read back a different value",
                    ))
                }
            }
            .await;
            q(&mut admin, redis::cmd("ACL").arg("DELUSER").arg(&user)).await?;
            moved
        }
        other => Err(HarnessError::Unsupported(format!("auth {other}"))),
    }
}

/// `valkey`: `SCAN` the `…:src:*` keys, write each to `…:dst:<src key>`.
#[derive(Debug)]
pub struct ValkeyKeys {
    url: String,
    prefix: String,
    profile: Option<Profile>,
}

impl ValkeyKeys {
    pub fn new() -> Self {
        let path = loams_flow::manifest::repository_root().join("connectors/profiles/valkey.yaml");
        Self {
            url: valkey_url(),
            prefix: format!("loams-h:{}", run_tag()),
            profile: loams_flow::profile::load_profile(&path).ok(),
        }
    }
}

impl Default for ValkeyKeys {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl KvSystem for ValkeyKeys {
    fn id(&self) -> &'static str {
        "valkey"
    }

    fn services(&self) -> &'static [ServiceKind] {
        VALKEY
    }

    fn factory(&self) -> Arc<dyn ConnectorFactory> {
        Arc::new(RedisFactory::new("valkey"))
    }

    fn profile(&self) -> Option<Profile> {
        self.profile.clone()
    }

    fn source_config(&self) -> Json {
        json!({ "url": self.url, "scan_match": format!("{}:src:*", self.prefix), "scan_count": 50 })
    }

    fn sink_config(&self) -> Option<Json> {
        Some(
            json!({ "url": self.url, "sink_mode": "set", "key_template": format!("{}:dst:{{subject}}", self.prefix) }),
        )
    }

    async fn ready(&self) -> Result<(), HarnessError> {
        let mut c = direct(&self.url, json!({}), StaticSecrets::new()).await?;
        q(&mut c, &mut redis::cmd("PING")).await.map(|_| ())
    }

    async fn clear(&self) -> Result<(), HarnessError> {
        let mut c = direct(&self.url, json!({}), StaticSecrets::new()).await?;
        delete_matching(&mut c, &format!("{}:*", self.prefix)).await
    }

    async fn write_input(&self, from: usize, to: usize) -> Result<Vec<String>, HarnessError> {
        let mut c = direct(&self.url, json!({}), StaticSecrets::new()).await?;
        let mut pipe = redis::pipe();
        for i in from..to {
            pipe.cmd("SET")
                .arg(format!("{}:src:{i}", self.prefix))
                .arg(format!("value-{i}"));
        }
        let _: Vec<Value> = pipe
            .query_async(&mut c)
            .await
            .map_err(|e| HarnessError::failed(format!("seeding: {e}")))?;
        Ok((from..to).map(|i| i.to_string()).collect())
    }

    async fn read_target(&self) -> Result<Vec<Observed>, HarnessError> {
        let mut c = direct(&self.url, json!({}), StaticSecrets::new()).await?;
        let dst = format!("{}:dst:{}:src:", self.prefix, self.prefix);
        Ok(keys_matching(&mut c, &format!("{}:dst:*", self.prefix))
            .await?
            .into_iter()
            .filter_map(|k| k.strip_prefix(&dst).map(str::to_owned))
            .map(|id| Observed { id, copies: 1 })
            .collect())
    }

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        authenticate(&self.url, &self.prefix, method).await
    }
}

/// `redis-streams`: `XADD`ed entries of `…:in` → `XADD` to `…:out`, against
/// Valkey or Redis.
#[derive(Debug)]
pub struct Streams {
    url: String,
    prefix: String,
    services: &'static [ServiceKind],
}

impl Streams {
    /// Against Valkey (`connectors-p2`).
    pub fn valkey() -> Self {
        Self {
            url: valkey_url(),
            prefix: format!("loams-h:{}", run_tag()),
            services: VALKEY,
        }
    }

    /// Against Redis 8 (`connectors-ci-only`).
    pub fn redis() -> Self {
        Self {
            url: redis_url(),
            prefix: format!("loams-h:{}", run_tag()),
            services: REDIS,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn input(&self) -> String {
        format!("{}:in", self.prefix)
    }

    pub fn output(&self) -> String {
        format!("{}:out", self.prefix)
    }
}

#[async_trait]
impl KvSystem for Streams {
    fn id(&self) -> &'static str {
        "redis-streams"
    }

    fn services(&self) -> &'static [ServiceKind] {
        self.services
    }

    fn factory(&self) -> Arc<dyn ConnectorFactory> {
        Arc::new(RedisFactory::new("redis-streams"))
    }

    fn source_config(&self) -> Json {
        json!({ "url": self.url, "stream": self.input(), "block_ms": 200, "count": 50 })
    }

    fn sink_config(&self) -> Option<Json> {
        Some(json!({ "url": self.url, "stream": self.output() }))
    }

    async fn ready(&self) -> Result<(), HarnessError> {
        let mut c = direct(&self.url, json!({}), StaticSecrets::new()).await?;
        q(&mut c, &mut redis::cmd("PING")).await.map(|_| ())
    }

    async fn clear(&self) -> Result<(), HarnessError> {
        let mut c = direct(&self.url, json!({}), StaticSecrets::new()).await?;
        q(
            &mut c,
            redis::cmd("DEL").arg(self.input()).arg(self.output()),
        )
        .await
        .map(|_| ())
    }

    async fn write_input(&self, from: usize, to: usize) -> Result<Vec<String>, HarnessError> {
        let mut c = direct(&self.url, json!({}), StaticSecrets::new()).await?;
        let mut pipe = redis::pipe();
        for i in from..to {
            pipe.cmd("XADD").arg(self.input()).arg("*").arg("n").arg(i);
        }
        let ids: Vec<Value> = pipe
            .query_async(&mut c)
            .await
            .map_err(|e| HarnessError::failed(format!("seeding: {e}")))?;
        ids.into_iter()
            .map(|v| match v {
                Value::BulkString(b) => {
                    Ok(format!("{}/{}", self.input(), String::from_utf8_lossy(&b)))
                }
                Value::SimpleString(s) => Ok(format!("{}/{s}", self.input())),
                other => Err(HarnessError::failed(format!("XADD replied {other:?}"))),
            })
            .collect()
    }

    async fn read_target(&self) -> Result<Vec<Observed>, HarnessError> {
        let mut c = direct(&self.url, json!({}), StaticSecrets::new()).await?;
        let reply = q(
            &mut c,
            redis::cmd("XRANGE").arg(self.output()).arg("-").arg("+"),
        )
        .await?;
        let entries = match &reply {
            Value::Array(_) => {
                crate::redis::streams::parse_read_reply(&Value::Array(vec![Value::Array(vec![
                    Value::BulkString(self.output().into_bytes()),
                    reply.clone(),
                ])]))
                .map_err(|e| HarnessError::failed(e.to_string()))?
            }
            _ => Vec::new(),
        };
        let mut counts = std::collections::BTreeMap::<String, u32>::new();
        for e in entries {
            let id = e
                .fields
                .unwrap_or_default()
                .into_iter()
                .find(|(k, _)| k == b"ce_id")
                .map(|(_, v)| String::from_utf8_lossy(&v).into_owned());
            if let Some(id) = id {
                *counts.entry(id).or_default() += 1;
            }
        }
        Ok(counts
            .into_iter()
            .map(|(id, copies)| Observed { id, copies })
            .collect())
    }

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        authenticate(&self.url, &self.prefix, method).await
    }
}
