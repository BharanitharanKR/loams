//! `loams migrate verify`'s store probes (design §54 §10.4, UP1 Task 7):
//! after `migrate`, a read of each product that touches its migrated formats,
//! reported per product as `ok`, `empty` or `failed`. The engine's own probes
//! (`/ready`, `GetInstance`) are added by the `loams` binary, which speaks
//! HTTP; this module reads the stores only.

use std::collections::BTreeMap;

use loams_common::meta::{COLLECTION_POINTER_PREFIX, Consistency, SYSTEM_NAMESPACE};
use serde::Serialize;

use crate::cx::Cx;
use crate::formats;
use crate::ledger::Ledger;

/// One product's result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeState {
    /// The product has data and it reads.
    Ok,
    /// The product has no data (or its store is not part of this stack).
    Empty,
    /// The product's data does not read.
    Failed,
}

/// One product's result with what was looked at.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Probe {
    pub state: ProbeState,
    pub detail: String,
}

impl Probe {
    pub fn ok(detail: impl Into<String>) -> Self {
        Probe {
            state: ProbeState::Ok,
            detail: detail.into(),
        }
    }

    pub fn empty(detail: impl Into<String>) -> Self {
        Probe {
            state: ProbeState::Empty,
            detail: detail.into(),
        }
    }

    pub fn failed(detail: impl Into<String>) -> Self {
        Probe {
            state: ProbeState::Failed,
            detail: detail.into(),
        }
    }
}

/// Every product's result, by product name.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct VerifyReport {
    pub products: BTreeMap<String, Probe>,
}

impl VerifyReport {
    /// Whether no product failed.
    pub fn passed(&self) -> bool {
        self.products
            .values()
            .all(|p| p.state != ProbeState::Failed)
    }
}

/// Probes every store `cx` holds; `release` is what the stack record must
/// name.
pub async fn verify_stores(cx: &Cx, release: &str) -> VerifyReport {
    let mut report = VerifyReport::default();
    report
        .products
        .insert("stack".to_string(), stack(cx, release).await);
    report
        .products
        .insert("collections".to_string(), collections(cx).await);
    report
        .products
        .insert("streams".to_string(), streams(cx).await);
    report.products.insert("durable".to_string(), durable(cx));
    report.products.insert("house".to_string(), house(cx));
    report
}

async fn stack(cx: &Cx, release: &str) -> Probe {
    let ledger = match Ledger::open_existing(cx.meta.clone()).await {
        Ok(Some(ledger)) => ledger,
        Ok(None) => return Probe::failed("no _system namespace: the stack is not stamped"),
        Err(e) => return Probe::failed(e.to_string()),
    };
    match ledger.stack().await {
        Ok(Some(record)) if record.release == release => {
            match record.stored().readable_by(formats::REGISTRY) {
                Ok(()) => Probe::ok(format!("stack record at {release}")),
                Err(e) => Probe::failed(e.to_string()),
            }
        }
        Ok(Some(record)) => Probe::failed(format!(
            "the stack record names {}, not {release}",
            record.release
        )),
        Ok(None) => Probe::failed("no stack record"),
        Err(e) => Probe::failed(e.to_string()),
    }
}

/// Lists every collection and opens the manifest each pointer names.
async fn collections(cx: &Cx) -> Probe {
    let l = Consistency::Linearizable;
    let namespaces = match cx.meta.namespaces(l).await {
        Ok(n) => n,
        Err(e) => return Probe::failed(format!("namespaces: {e}")),
    };
    let mut collections = 0;
    let mut opened = 0;
    for ns in namespaces.iter().filter(|n| n.name != SYSTEM_NAMESPACE) {
        match cx.meta.collections(l, Some(ns.id)).await {
            Ok(c) => collections += c.len(),
            Err(e) => return Probe::failed(format!("collections of {}: {e}", ns.name)),
        }
        let pointers = match cx
            .meta
            .pointers_with_prefix(l, ns.id, COLLECTION_POINTER_PREFIX)
            .await
        {
            Ok(p) => p,
            Err(e) => return Probe::failed(format!("pointers of {}: {e}", ns.name)),
        };
        let Some(bucket) = &cx.bucket else {
            continue;
        };
        for (key, pointer) in pointers {
            if let Err(e) = crate::adopt::open_manifest(bucket, &pointer.value).await {
                return Probe::failed(format!("{}/{key}: {}", ns.name, e.detail));
            }
            opened += 1;
        }
    }
    if collections == 0 {
        Probe::empty("no collections")
    } else {
        Probe::ok(format!(
            "{collections} collections, {opened} manifests opened"
        ))
    }
}

async fn streams(cx: &Cx) -> Probe {
    match cx.meta.streams(Consistency::Linearizable, None).await {
        Ok(s) if s.is_empty() => Probe::empty("no streams"),
        Ok(s) => Probe::ok(format!("{} streams", s.len())),
        Err(e) => Probe::failed(format!("streams: {e}")),
    }
}

fn durable(cx: &Cx) -> Probe {
    let Ok(durable) = cx.durable_cx() else {
        return Probe::empty("no data directory");
    };
    if !durable.exists() {
        return Probe::empty("no durable store");
    }
    match durable.applied_migrations() {
        Ok(applied) if applied.is_empty() => Probe::failed("the durable store has no schema"),
        Ok(applied) => Probe::ok(format!("{} Resonate migrations applied", applied.len())),
        Err(e) => Probe::failed(e.to_string()),
    }
}

fn house(cx: &Cx) -> Probe {
    let Ok(house) = cx.house_cx() else {
        return Probe::empty("no House directory");
    };
    let catalog = house.dir().join("catalog.sqlite");
    if !catalog.exists() {
        return Probe::empty("no House catalog");
    }
    if let Err(e) = formats::check_dir_stamp(house.dir(), formats::HOUSE_DIR) {
        return Probe::failed(e.to_string());
    }
    let read =
        rusqlite::Connection::open_with_flags(&catalog, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .and_then(|conn| {
                conn.query_row("SELECT count(*) FROM tables", [], |row| {
                    row.get::<_, i64>(0)
                })
            });
    match read {
        Ok(tables) => Probe::ok(format!("{tables} lake tables")),
        Err(e) => Probe::failed(format!("catalog.sqlite: {e}")),
    }
}
