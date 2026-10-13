//! The DynamoDB and DynamoDB Streams harnesses, against floci 2.1.0 in PR
//! CI (`connectors-p2`, R0.23) or a real AWS account in the nightly job
//! (`LOAMS_AWS_ACCESS_KEY_ID`, `LOAMS_AWS_SECRET_ACCESS_KEY`,
//! `LOAMS_AWS_REGION`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus,
};
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorFactory, InstanceKey, Scrubber, StaticSecrets,
};
use loams_flow_conformance::{
    CdcReport, ConnectorHarness, HarnessError, KillTarget, Observed, ServiceKind,
};
use serde_json::{Value as Json, json};

use super::{ChainHarness, KvSystem, env_set, run_tag, unavailable};
use crate::config::Config;
use crate::ddbstreams::DynamoDbStreamsFactory;
use crate::dynamodb::{AwsDynamo, DynamoApi, DynamoDbFactory, Item, Write};

/// floci for DynamoDB, or a real account nightly.
pub const DYNAMODB: &[ServiceKind] = &[
    ServiceKind::Emulator {
        compose_service: "floci",
        system: "Amazon DynamoDB",
        vendor: "floci",
    },
    ServiceKind::Nightly {
        env: &[
            "LOAMS_AWS_ACCESS_KEY_ID",
            "LOAMS_AWS_SECRET_ACCESS_KEY",
            "LOAMS_AWS_REGION",
        ],
        system: "Amazon DynamoDB",
    },
];

/// floci for DynamoDB Streams, or a real account nightly.
pub const STREAMS: &[ServiceKind] = &[
    ServiceKind::Emulator {
        compose_service: "floci",
        system: "Amazon DynamoDB Streams",
        vendor: "floci",
    },
    ServiceKind::Nightly {
        env: &[
            "LOAMS_AWS_ACCESS_KEY_ID",
            "LOAMS_AWS_SECRET_ACCESS_KEY",
            "LOAMS_AWS_REGION",
        ],
        system: "Amazon DynamoDB Streams",
    },
];

/// Where the harness talks to: AWS when the nightly secrets are set, else
/// floci when the `connectors-p2` stack is up.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub endpoint_url: Option<String>,
    pub region: String,
    access_key_id: String,
    secret_access_key: String,
}

impl Endpoint {
    pub fn from_env() -> Option<Endpoint> {
        let var = |v: &str| std::env::var(v).ok().filter(|s| !s.is_empty());
        // The nightly job has both stacks up and the account's secrets: the
        // real account wins there, so `stable` is earned against AWS.
        if let (Some(region), Some(ak), Some(sk)) = (
            var("LOAMS_AWS_REGION"),
            var("LOAMS_AWS_ACCESS_KEY_ID"),
            var("LOAMS_AWS_SECRET_ACCESS_KEY"),
        ) {
            return Some(Endpoint {
                endpoint_url: None,
                region,
                access_key_id: ak,
                secret_access_key: sk,
            });
        }
        env_set("LOAMS_CONNECTORS_P2_STACK").then(|| Endpoint {
            endpoint_url: Some(
                var("LOAMS_FLOCI_URL").unwrap_or_else(|| "http://127.0.0.1:4566".to_owned()),
            ),
            region: "us-east-1".to_owned(),
            access_key_id: "test".to_owned(),
            secret_access_key: "test".to_owned(),
        })
    }

    /// The config keys every instance gets (secrets by name).
    pub fn config(&self) -> Json {
        let mut c = json!({
            "region": self.region,
            "access_key_id": "kv-harness-aws-ak",
            "secret_access_key": "kv-harness-aws-sk",
        });
        if let Some(e) = &self.endpoint_url {
            c["endpoint_url"] = json!(e);
        }
        c
    }

    pub fn secrets(&self) -> StaticSecrets {
        StaticSecrets::new()
            .with("kv-harness-aws-ak", self.access_key_id.clone())
            .with("kv-harness-aws-sk", self.secret_access_key.clone())
    }

    pub async fn client(&self) -> Result<AwsDynamo, HarnessError> {
        let cfg = self.config();
        let ctx = ConnectorContext::new(
            InstanceKey::new(1, "kv-harness"),
            cfg.clone(),
            Arc::new(self.secrets()),
            Scrubber::new(),
        );
        AwsDynamo::connect(&ctx, &Config::new(cfg))
            .await
            .map_err(|e| unavailable("DynamoDB client", e))
    }
}

fn merge(mut a: Json, b: Json) -> Json {
    if let (Some(o), Json::Object(extra)) = (a.as_object_mut(), b) {
        o.extend(extra);
    }
    a
}

fn need() -> Result<Endpoint, HarnessError> {
    Endpoint::from_env().ok_or_else(|| {
        HarnessError::Unavailable(
            "neither LOAMS_CONNECTORS_P2_STACK (floci) nor the LOAMS_AWS_* nightly secrets are set"
                .into(),
        )
    })
}

/// Drop and create `table` (key `id`, a string), with a stream when
/// `stream` is set, and wait until it is active.
pub async fn recreate(c: &AwsDynamo, table: &str, stream: bool) -> Result<(), HarnessError> {
    let client = c.client();
    if client
        .describe_table()
        .table_name(table)
        .send()
        .await
        .is_ok()
    {
        let _ = client.delete_table().table_name(table).send().await;
        for _ in 0..120 {
            if client
                .describe_table()
                .table_name(table)
                .send()
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    let attr = AttributeDefinition::builder()
        .attribute_name("id")
        .attribute_type(ScalarAttributeType::S)
        .build()
        .map_err(|e| HarnessError::failed(e.to_string()))?;
    let key = KeySchemaElement::builder()
        .attribute_name("id")
        .key_type(KeyType::Hash)
        .build()
        .map_err(|e| HarnessError::failed(e.to_string()))?;
    let mut req = client
        .create_table()
        .table_name(table)
        .attribute_definitions(attr)
        .key_schema(key)
        .billing_mode(BillingMode::PayPerRequest);
    if stream {
        req = req.stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .map_err(|e| HarnessError::failed(e.to_string()))?,
        );
    }
    req.send()
        .await
        .map_err(|e| HarnessError::failed(format!("CreateTable: {e:?}")))?;
    for _ in 0..240 {
        let d = client.describe_table().table_name(table).send().await;
        if d.as_ref()
            .ok()
            .and_then(|o| o.table())
            .and_then(|t| t.table_status())
            .is_some_and(|s| *s == TableStatus::Active)
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(HarnessError::failed(format!(
        "table {table} never became active"
    )))
}

fn item(i: usize) -> Item {
    HashMap::from([
        ("id".to_owned(), AttributeValue::S(i.to_string())),
        ("n".to_owned(), AttributeValue::N(i.to_string())),
    ])
}

async fn put_items(c: &AwsDynamo, table: &str, items: Vec<Item>) -> Result<(), HarnessError> {
    for chunk in items.chunks(25) {
        let mut writes: Vec<Write> = chunk.iter().cloned().map(Write::Put).collect();
        for _ in 0..20 {
            writes = c
                .batch_write(table, writes)
                .await
                .map_err(|e| HarnessError::failed(e.to_string()))?;
            if writes.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}

async fn scan_ids(c: &AwsDynamo, table: &str) -> Result<Vec<String>, HarnessError> {
    let mut out = Vec::new();
    let mut start = None;
    loop {
        let page = c
            .scan(table, 0, 1, start, 500, true)
            .await
            .map_err(|e| HarnessError::failed(e.to_string()))?;
        for i in &page.items {
            if let Some(AttributeValue::S(id)) = i.get("id") {
                out.push(id.clone());
            }
        }
        match page.last_evaluated_key {
            Some(k) => start = Some(k),
            None => return Ok(out),
        }
    }
}

/// `dynamodb`: a parallel scan of `…-src` → `BatchWriteItem` into `…-dst`.
#[derive(Debug)]
pub struct DynamoDb {
    tag: String,
}

impl DynamoDb {
    pub fn new() -> Self {
        Self { tag: run_tag() }
    }

    fn src(&self) -> String {
        format!("loams-h-{}-src", self.tag)
    }

    fn dst(&self) -> String {
        format!("loams-h-{}-dst", self.tag)
    }
}

impl Default for DynamoDb {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl KvSystem for DynamoDb {
    fn id(&self) -> &'static str {
        "dynamodb"
    }

    fn services(&self) -> &'static [ServiceKind] {
        DYNAMODB
    }

    fn factory(&self) -> Arc<dyn ConnectorFactory> {
        Arc::new(DynamoDbFactory::new())
    }

    fn secrets(&self) -> StaticSecrets {
        Endpoint::from_env()
            .map(|e| e.secrets())
            .unwrap_or_default()
    }

    fn source_config(&self) -> Json {
        let base = Endpoint::from_env()
            .map(|e| e.config())
            .unwrap_or_else(|| json!({}));
        merge(
            base,
            json!({ "table": self.src(), "segments": 3, "page_size": 20, "consistent_read": true }),
        )
    }

    fn sink_config(&self) -> Option<Json> {
        let base = Endpoint::from_env()
            .map(|e| e.config())
            .unwrap_or_else(|| json!({}));
        Some(merge(base, json!({ "table": self.dst() })))
    }

    async fn ready(&self) -> Result<(), HarnessError> {
        let c = need()?.client().await?;
        c.client()
            .list_tables()
            .limit(1)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| unavailable("ListTables", format!("{e:?}")))
    }

    async fn clear(&self) -> Result<(), HarnessError> {
        let c = need()?.client().await?;
        recreate(&c, &self.src(), false).await?;
        recreate(&c, &self.dst(), false).await
    }

    async fn write_input(&self, from: usize, to: usize) -> Result<Vec<String>, HarnessError> {
        let c = need()?.client().await?;
        put_items(&c, &self.src(), (from..to).map(item).collect()).await?;
        Ok((from..to).map(|i| i.to_string()).collect())
    }

    async fn read_target(&self) -> Result<Vec<Observed>, HarnessError> {
        let c = need()?.client().await?;
        Ok(scan_ids(&c, &self.dst())
            .await?
            .into_iter()
            .map(|id| Observed { id, copies: 1 })
            .collect())
    }

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        if method != "iam" {
            return Err(HarnessError::Unsupported(format!("auth {method}")));
        }
        // SigV4 with the keys resolved from the secret store, through the
        // connector's own client.
        let c = need()?.client().await?;
        c.key_schema(&self.src())
            .await
            .map(|_| ())
            .map_err(|e| HarnessError::failed(e.to_string()))
    }
}

/// `dynamodb-streams`: the stream of `…-cdc`, read into the Fabric (the
/// target of a source-only connector).
#[derive(Debug)]
pub struct DynamoDbStreams {
    tag: String,
}

impl DynamoDbStreams {
    pub fn new() -> Self {
        Self { tag: run_tag() }
    }

    pub fn table(&self) -> String {
        format!("loams-h-{}-cdc", self.tag)
    }
}

impl Default for DynamoDbStreams {
    fn default() -> Self {
        Self::new()
    }
}

/// The item id a change event carries (its `keys.id`).
fn change_id(ev: &CloudEvent) -> Option<String> {
    let v: Json = serde_json::from_slice(&ev.data).ok()?;
    v["keys"]["id"].as_str().map(str::to_owned)
}

#[async_trait]
impl KvSystem for DynamoDbStreams {
    fn id(&self) -> &'static str {
        "dynamodb-streams"
    }

    fn services(&self) -> &'static [ServiceKind] {
        STREAMS
    }

    fn factory(&self) -> Arc<dyn ConnectorFactory> {
        Arc::new(DynamoDbStreamsFactory::new())
    }

    fn secrets(&self) -> StaticSecrets {
        Endpoint::from_env()
            .map(|e| e.secrets())
            .unwrap_or_default()
    }

    fn source_config(&self) -> Json {
        let base = Endpoint::from_env()
            .map(|e| e.config())
            .unwrap_or_else(|| json!({}));
        merge(
            base,
            json!({ "table": self.table(), "poll_interval_ms": 200 }),
        )
    }

    fn sink_config(&self) -> Option<Json> {
        None
    }

    fn fabric_id(&self, ev: &CloudEvent) -> Option<String> {
        // One insert per seeded item: its INSERT change is the record.
        (ev.extensions.get("loamsop").map(String::as_str) == Some("c"))
            .then(|| change_id(ev))
            .flatten()
    }

    async fn ready(&self) -> Result<(), HarnessError> {
        DynamoDb::new().ready().await
    }

    async fn clear(&self) -> Result<(), HarnessError> {
        let c = need()?.client().await?;
        recreate(&c, &self.table(), true).await
    }

    async fn write_input(&self, from: usize, to: usize) -> Result<Vec<String>, HarnessError> {
        let c = need()?.client().await?;
        put_items(&c, &self.table(), (from..to).map(item).collect()).await?;
        Ok((from..to).map(|i| i.to_string()).collect())
    }

    async fn read_target(&self) -> Result<Vec<Observed>, HarnessError> {
        Err(HarnessError::Unsupported("the Fabric is the target".into()))
    }

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        DynamoDb {
            tag: self.tag.clone(),
        }
        .authenticate_table(method, &self.table())
        .await
    }

    async fn cdc(&self, h: &ChainHarness<Self>) -> Result<CdcReport, HarnessError> {
        let c = need()?.client().await?;
        let t = self.table();
        h.start().await?;
        // Inserts, an update, a delete, and an item with a new attribute (a
        // schemaless table's "DDL", ruling R14.6).
        put_items(&c, &t, vec![item(1), item(2), item(3)]).await?;
        let mut updated = item(2);
        updated.insert("n".into(), AttributeValue::N("20".into()));
        put_items(&c, &t, vec![updated]).await?;
        c.batch_write(
            &t,
            vec![Write::Delete(HashMap::from([(
                "id".to_owned(),
                AttributeValue::S("3".into()),
            )]))],
        )
        .await
        .map_err(|e| HarnessError::failed(e.to_string()))?;
        let mut widened = item(4);
        widened.insert("added_attribute".into(), AttributeValue::S("x".into()));
        put_items(&c, &t, vec![widened]).await?;
        let fabric = h.fabric().await;
        let seen = |fabric: &loams_connector_sdk::testing::memory::MemoryFabric| {
            let mut ops: BTreeMap<(String, String), bool> = BTreeMap::new();
            let mut ddl = false;
            for ev in fabric.events() {
                let op = ev.extensions.get("loamsop").cloned().unwrap_or_default();
                let id = change_id(&ev).unwrap_or_default();
                if ev.data.windows(15).any(|w| w == b"added_attribute") {
                    ddl = true;
                }
                ops.insert((id, op), true);
            }
            (ops, ddl)
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let (ops, ddl) = loop {
            let (ops, ddl) = seen(&fabric);
            let has = |id: &str, op: &str| ops.contains_key(&(id.to_owned(), op.to_owned()));
            if (has("1", "c") && has("2", "u") && has("3", "d") && ddl)
                || tokio::time::Instant::now() >= deadline
            {
                break (ops, ddl);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let has = |id: &str, op: &str| ops.contains_key(&(id.to_owned(), op.to_owned()));
        // Kill, write more, restart from the checkpoint: nothing lost.
        let before = fabric.len();
        h.kill(KillTarget::Task).await?;
        put_items(&c, &t, vec![item(5), item(6)]).await?;
        h.start().await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let resumed = loop {
            let (ops, _) = seen(&fabric);
            let ok = ops.contains_key(&("5".to_owned(), "c".to_owned()))
                && ops.contains_key(&("6".to_owned(), "c".to_owned()));
            if ok || tokio::time::Instant::now() >= deadline {
                break ok && fabric.len() >= before + 2;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        h.stop().await?;
        Ok(CdcReport {
            inserts: has("1", "c"),
            updates: has("2", "u"),
            deletes: has("3", "d"),
            ddl,
            resumed_without_loss: resumed,
        })
    }
}

impl DynamoDb {
    async fn authenticate_table(&self, method: &str, table: &str) -> Result<(), HarnessError> {
        if method != "iam" {
            return Err(HarnessError::Unsupported(format!("auth {method}")));
        }
        let c = need()?.client().await?;
        c.key_schema(table)
            .await
            .map(|_| ())
            .map_err(|e| HarnessError::failed(e.to_string()))
    }
}
