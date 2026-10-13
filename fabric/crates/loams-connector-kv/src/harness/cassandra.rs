//! The Cassandra harness: token-range reads of `src_<tag>` → upserts into
//! `dst_<tag>`, in the keyspace `loams_h` of real Apache Cassandra 5
//! (`connectors-p2`, host port 9042).

use std::sync::Arc;

use async_trait::async_trait;
use loams_connector_sdk::ConnectorFactory;
use loams_flow_conformance::{HarnessError, Observed, ServiceKind};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use serde_json::{Value as Json, json};

use super::{KvSystem, run_tag, unavailable};
use crate::cassandra::CassandraFactory;

pub const CASSANDRA: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "cassandra",
    system: "Apache Cassandra",
}];

/// The contact point: `LOAMS_CASSANDRA_CONTACT`, else the compose service's
/// host port.
pub fn contact() -> String {
    std::env::var("LOAMS_CASSANDRA_CONTACT").unwrap_or_else(|_| "127.0.0.1:9042".to_owned())
}

const KEYSPACE: &str = "loams_h";

#[derive(Debug)]
pub struct Cassandra {
    tag: String,
}

impl Cassandra {
    pub fn new() -> Self {
        Self { tag: run_tag() }
    }

    fn src(&self) -> String {
        format!("src_{}", self.tag)
    }

    fn dst(&self) -> String {
        format!("dst_{}", self.tag)
    }

    async fn session(&self) -> Result<Session, HarnessError> {
        SessionBuilder::new()
            .known_node(contact())
            .build()
            .await
            .map_err(|e| unavailable("connecting to Cassandra", e))
    }
}

impl Default for Cassandra {
    fn default() -> Self {
        Self::new()
    }
}

async fn run(s: &Session, cql: String) -> Result<(), HarnessError> {
    s.query_unpaged(cql, ())
        .await
        .map(|_| ())
        .map_err(|e| HarnessError::failed(format!("CQL: {e}")))
}

#[async_trait]
impl KvSystem for Cassandra {
    fn id(&self) -> &'static str {
        "cassandra"
    }

    fn services(&self) -> &'static [ServiceKind] {
        CASSANDRA
    }

    fn factory(&self) -> Arc<dyn ConnectorFactory> {
        Arc::new(CassandraFactory)
    }

    fn source_config(&self) -> Json {
        json!({
            "contact_points": [contact()], "keyspace": KEYSPACE, "table": self.src(),
            "consistency": "one", "splits": 4, "page_size": 20
        })
    }

    fn sink_config(&self) -> Option<Json> {
        Some(
            json!({ "contact_points": [contact()], "keyspace": KEYSPACE, "table": self.dst(), "consistency": "one" }),
        )
    }

    async fn ready(&self) -> Result<(), HarnessError> {
        self.session().await.map(|_| ())
    }

    async fn clear(&self) -> Result<(), HarnessError> {
        let s = self.session().await?;
        run(
            &s,
            format!("CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"),
        )
        .await?;
        for t in [self.src(), self.dst()] {
            run(
                &s,
                format!("CREATE TABLE IF NOT EXISTS {KEYSPACE}.{t} (id int PRIMARY KEY, v text)"),
            )
            .await?;
            run(&s, format!("TRUNCATE {KEYSPACE}.{t}")).await?;
        }
        Ok(())
    }

    async fn write_input(&self, from: usize, to: usize) -> Result<Vec<String>, HarnessError> {
        let s = self.session().await?;
        let p = s
            .prepare(format!(
                "INSERT INTO {KEYSPACE}.{} (id, v) VALUES (?, ?)",
                self.src()
            ))
            .await
            .map_err(|e| HarnessError::failed(format!("prepare: {e}")))?;
        for i in from..to {
            let id = i32::try_from(i).map_err(|_| HarnessError::failed("too many records"))?;
            s.execute_unpaged(&p, (id, format!("value-{i}")))
                .await
                .map_err(|e| HarnessError::failed(format!("insert: {e}")))?;
        }
        Ok((from..to).map(|i| i.to_string()).collect())
    }

    async fn read_target(&self) -> Result<Vec<Observed>, HarnessError> {
        let s = self.session().await?;
        let rows = s
            .query_unpaged(format!("SELECT id FROM {KEYSPACE}.{}", self.dst()), ())
            .await
            .map_err(|e| HarnessError::failed(format!("select: {e}")))?
            .into_rows_result()
            .map_err(|e| HarnessError::failed(format!("select: {e}")))?;
        let ids = rows
            .rows::<(i32,)>()
            .map_err(|e| HarnessError::failed(format!("select: {e}")))?
            .map(|r| {
                r.map(|(id,)| Observed {
                    id: id.to_string(),
                    copies: 1,
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| HarnessError::failed(format!("select: {e}")))?;
        Ok(ids)
    }

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        match method {
            "none" => {
                let s = self.session().await?;
                run(&s, "SELECT release_version FROM system.local".to_owned()).await
            }
            other => Err(HarnessError::Unsupported(format!(
                "auth {other}: the CI Cassandra runs AllowAllAuthenticator"
            ))),
        }
    }
}
