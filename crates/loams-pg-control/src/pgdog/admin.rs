//! PgDog's admin database (design §46 §8.2; PG2 Task 21), as
//! [`PgDogAdmin`]: `RELOAD`, and `SHOW POOLS` read by column name (R18.7).
//!
//! The pinned PgDog's `SHOW DATABASES` answers one empty column, so a
//! replica's databases are the distinct `database` of its pools. A
//! namespace's replicas come from a [`PgDogDirectory`]: on compose, a fixed
//! address; on Kubernetes, the namespace's PgDog Pods (Task 22). Each
//! [`InstanceId`] is the replica's host; the admin port and login are this
//! client's. The admin connection is plaintext until Task 23.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_postgres::SimpleQueryMessage;

use super::{InstanceId, PgDogAdmin, PgDogError, PoolRow};
use crate::secrets::Secret;

/// Where a namespace's PgDog replicas are.
pub trait PgDogDirectory: Send + Sync + 'static {
    fn instances(&self, namespace: &str) -> Vec<InstanceId>;
}

/// A fixed list per namespace (compose, single node, tests).
#[derive(Debug, Clone, Default)]
pub struct StaticDirectory(pub BTreeMap<String, Vec<InstanceId>>);

impl PgDogDirectory for StaticDirectory {
    fn instances(&self, namespace: &str) -> Vec<InstanceId> {
        self.0.get(namespace).cloned().unwrap_or_default()
    }
}

/// How to log into a replica's admin database.
#[derive(Clone)]
pub struct AdminLogin {
    pub port: u16,
    pub user: String,
    pub password: Secret<String>,
    pub connect_timeout: Duration,
}

impl std::fmt::Debug for AdminLogin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminLogin")
            .field("port", &self.port)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

/// [`PgDogAdmin`] over each replica's admin database.
#[derive(Clone)]
pub struct PgDogAdminClient {
    pub login: AdminLogin,
    pub directory: Arc<dyn PgDogDirectory>,
}

impl std::fmt::Debug for PgDogAdminClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgDogAdminClient")
            .field("login", &self.login)
            .finish_non_exhaustive()
    }
}

fn err(instance: &InstanceId, what: &str, e: &dyn std::fmt::Display) -> PgDogError {
    PgDogError(format!("{} {what}: {e}", instance.0))
}

impl PgDogAdminClient {
    async fn query(
        &self,
        instance: &InstanceId,
        sql: &str,
    ) -> Result<Vec<SimpleQueryMessage>, PgDogError> {
        let mut config = tokio_postgres::Config::new();
        config
            .host(&instance.0)
            .port(self.login.port)
            .user(&self.login.user)
            .password(self.login.password.expose())
            .dbname("admin")
            .connect_timeout(self.login.connect_timeout);
        let (client, conn) = tokio::time::timeout(
            self.login.connect_timeout,
            config.connect(tokio_postgres::NoTls),
        )
        .await
        .map_err(|_| err(instance, "connect", &"timed out"))?
        .map_err(|e| err(instance, "connect", &e))?;
        let task = tokio::spawn(conn);
        let out = client
            .simple_query(sql)
            .await
            .map_err(|e| err(instance, sql, &e));
        drop(client);
        task.abort();
        out
    }
}

/// The rows of `SHOW POOLS`, read by column name.
pub(crate) fn pool_rows(messages: &[SimpleQueryMessage]) -> Vec<PoolRow> {
    let num = |row: &tokio_postgres::SimpleQueryRow, col: &str| -> u64 {
        row.try_get(col)
            .ok()
            .flatten()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(PoolRow {
                database: row
                    .try_get("database")
                    .ok()
                    .flatten()
                    .unwrap_or("")
                    .to_owned(),
                user: row.try_get("user").ok().flatten().unwrap_or("").to_owned(),
                cl_waiting: num(row, "cl_waiting"),
                sv_active: num(row, "sv_active"),
            }),
            _ => None,
        })
        .collect()
}

#[async_trait]
impl PgDogAdmin for PgDogAdminClient {
    async fn instances(&self, namespace: &str) -> Result<Vec<InstanceId>, PgDogError> {
        Ok(self.directory.instances(namespace))
    }

    async fn reload(&self, instance: &InstanceId) -> Result<(), PgDogError> {
        self.query(instance, "RELOAD").await.map(|_| ())
    }

    async fn show_pools(&self, instance: &InstanceId) -> Result<Vec<PoolRow>, PgDogError> {
        Ok(pool_rows(&self.query(instance, "SHOW POOLS").await?))
    }

    async fn show_databases(&self, instance: &InstanceId) -> Result<Vec<String>, PgDogError> {
        let names: BTreeSet<String> = self
            .show_pools(instance)
            .await?
            .into_iter()
            .map(|p| p.database)
            .collect();
        Ok(names.into_iter().collect())
    }
}
