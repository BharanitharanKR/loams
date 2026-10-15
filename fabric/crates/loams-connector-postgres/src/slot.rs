//! Publications and replication slots: creation, the lag metrics, the
//! idle heartbeat and the drop on `DeleteInstance` (Review Focus 6: an
//! abandoned slot fills the source's disk).

use std::time::Duration;

use loams_connector_sdk::ConnectorError;
use tokio_postgres::Client;

use crate::config::TableName;
use crate::conn::classify;
use crate::decode::HEARTBEAT_PREFIX;
use crate::replication::{Lsn, parse_lsn};
use crate::types::quote_ident;

/// Make sure the publication covers `tables`. A Loams-owned publication is
/// created, or re-pointed when the configured tables changed; a user-named
/// one must already exist.
pub async fn ensure_publication(
    client: &Client,
    name: &str,
    owned: bool,
    tables: &[TableName],
) -> Result<(), ConnectorError> {
    let exists = !client
        .query(
            "SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&name],
        )
        .await
        .map_err(|e| classify(&e, "reading publications"))?
        .is_empty();
    if !owned {
        return if exists {
            Ok(())
        } else {
            Err(ConnectorError::fatal(format!(
                "publication {name} does not exist"
            )))
        };
    }
    if tables.is_empty() {
        return Err(ConnectorError::fatal("config: tables is required for CDC"));
    }
    let list = tables
        .iter()
        .map(TableName::quoted)
        .collect::<Vec<_>>()
        .join(", ");
    let sql = if exists {
        format!("ALTER PUBLICATION {} SET TABLE {list}", quote_ident(name))
    } else {
        format!("CREATE PUBLICATION {} FOR TABLE {list}", quote_ident(name))
    };
    client
        .batch_execute(&sql)
        .await
        .map_err(|e| classify(&e, &format!("publication {name}")))
}

/// The tables a publication covers.
pub async fn publication_tables(
    client: &Client,
    name: &str,
) -> Result<Vec<TableName>, ConnectorError> {
    let rows = client
        .query(
            "SELECT schemaname::text, tablename::text FROM pg_catalog.pg_publication_tables \
             WHERE pubname = $1 ORDER BY 1, 2",
            &[&name],
        )
        .await
        .map_err(|e| classify(&e, "reading publication tables"))?;
    Ok(rows
        .iter()
        .map(|r| TableName {
            schema: r.get(0),
            name: r.get(1),
        })
        .collect())
}

/// A slot as `pg_replication_slots` shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotInfo {
    pub confirmed_flush: Option<Lsn>,
    pub active_pid: Option<i32>,
    /// `reserved`, `extended`, `unreserved` or `lost` (PG 13+).
    pub wal_status: Option<String>,
}

pub async fn slot_info(client: &Client, slot: &str) -> Result<Option<SlotInfo>, ConnectorError> {
    let rows = client
        .query(
            "SELECT confirmed_flush_lsn::text, active_pid, wal_status::text \
             FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| classify(&e, "reading replication slots"))?;
    Ok(rows.first().map(|r| SlotInfo {
        confirmed_flush: r.get::<_, Option<String>>(0).as_deref().and_then(parse_lsn),
        active_pid: r.get(1),
        wal_status: r.get(2),
    }))
}

/// Drop `slot`, ending the session that holds it if any (a crashed
/// predecessor's walsender may linger until `wal_sender_timeout`).
/// `Ok(false)` when there was no such slot.
pub async fn drop_slot(client: &Client, slot: &str) -> Result<bool, ConnectorError> {
    for _ in 0..20 {
        let Some(info) = slot_info(client, slot).await? else {
            return Ok(false);
        };
        if let Some(pid) = info.active_pid {
            client
                .execute("SELECT pg_terminate_backend($1)", &[&pid])
                .await
                .map_err(|e| classify(&e, "ending the slot's session"))?;
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        match client
            .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await
        {
            Ok(_) => return Ok(true),
            Err(e) => {
                let ce = classify(&e, "dropping the slot");
                if !ce.class.is_transient() {
                    return Err(ce);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Err(ConnectorError::retryable(format!(
        "slot {slot} stayed active"
    )))
}

/// Drop a Loams-owned publication.
pub async fn drop_publication(client: &Client, name: &str) -> Result<(), ConnectorError> {
    client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {}", quote_ident(name)))
        .await
        .map_err(|e| classify(&e, "dropping the publication"))
}

/// Slot lag, for metrics (Task 24 exports them): the WAL the slot holds
/// back, and how much more WAL may be written before the server
/// invalidates it (`safe_wal_size`, under `max_slot_wal_keep_size`; `None`
/// when unlimited).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SlotLag {
    pub lag_bytes: i64,
    pub headroom_bytes: Option<i64>,
    /// Seconds since the last commit the source delivered (from the stream).
    pub lag_seconds: Option<f64>,
}

impl SlotLag {
    pub fn lag_seconds_since(commit_micros_pg: i64) -> f64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as i64)
            - crate::types::PG_EPOCH_MICROS;
        ((now - commit_micros_pg).max(0) as f64) / 1e6
    }
}

pub async fn slot_lag(client: &Client, slot: &str) -> Result<Option<SlotLag>, ConnectorError> {
    let rows = client
        .query(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::int8, safe_wal_size::int8 \
             FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| classify(&e, "reading slot lag"))?;
    Ok(rows.first().map(|r| SlotLag {
        lag_bytes: r.get::<_, Option<i64>>(0).unwrap_or(0),
        headroom_bytes: r.get(1),
        lag_seconds: None,
    }))
}

/// The idle heartbeat: a non-transactional logical message, which the slot
/// decodes and the source confirms, so the slot advances on a database
/// whose captured tables are quiet.
pub async fn heartbeat(client: &Client) -> Result<(), ConnectorError> {
    client
        .execute(
            "SELECT pg_logical_emit_message(false, $1, ''::text)",
            &[&HEARTBEAT_PREFIX],
        )
        .await
        .map(|_| ())
        .map_err(|e| classify(&e, "heartbeat"))
}

/// The OIDs of types without a fixed OID that the codecs know by name.
pub async fn custom_types(client: &Client) -> Result<Vec<(u32, String)>, ConnectorError> {
    let rows = client
        .query(
            "SELECT oid::int8, typname::text FROM pg_catalog.pg_type WHERE typname IN ('vector')",
            &[],
        )
        .await
        .map_err(|e| classify(&e, "reading types"))?;
    Ok(rows
        .iter()
        .map(|r| (r.get::<_, i64>(0) as u32, r.get::<_, String>(1)))
        .collect())
}
