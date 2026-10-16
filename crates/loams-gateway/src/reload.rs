//! Route sources and hot reload (design §57 §8.2, D1018).
//!
//! The file is read at once, then every poll interval (compared by digest,
//! which also catches a Kubernetes ConfigMap's symlink swap) and at once on
//! `SIGHUP`. A valid table replaces the old one atomically; an invalid file
//! is logged and ignored, so the old table stays in force.

use std::future::Future;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::routes::{RouteError, RouteTable};

/// The current table; `None` until the first valid file is read.
pub type RouteRx = watch::Receiver<Option<Arc<RouteTable>>>;
pub type RouteTx = watch::Sender<Option<Arc<RouteTable>>>;

/// A fresh channel with no table.
pub fn channel() -> (RouteTx, RouteRx) {
    watch::channel(None)
}

#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    #[error("routes file {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    Route(#[from] RouteError),
}

/// Reads and validates the routes file once.
pub fn load_file(
    path: &Path,
    dedicated: &RangeInclusive<u16>,
    require_generation: bool,
) -> Result<RouteTable, ReloadError> {
    let text = std::fs::read_to_string(path).map_err(|source| ReloadError::Read {
        path: path.to_owned(),
        source,
    })?;
    Ok(RouteTable::parse(&text, dedicated, require_generation)?)
}

/// Watches the routes file until `shutdown` resolves.
pub async fn watch_file(
    path: PathBuf,
    every: Duration,
    dedicated: RangeInclusive<u16>,
    require_generation: bool,
    tx: RouteTx,
    shutdown: impl Future<Output = ()>,
) {
    let mut last: Option<[u8; 32]> = None;
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut hup = hangup();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tick.tick() => {}
            _ = recv(&mut hup) => {
                tracing::info!(path = %path.display(), "SIGHUP: re-reading routes");
                last = None;
            }
        }
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(e) => {
                // A missing file before the first table is normal at start
                // (pg-control has not written it yet); say so once.
                if last != Some([0; 32]) {
                    tracing::warn!(path = %path.display(), error = %e, "routes file unreadable");
                    last = Some([0; 32]);
                }
                continue;
            }
        };
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if last == Some(digest) {
            continue;
        }
        last = Some(digest);
        let parsed = std::str::from_utf8(&bytes)
            .map_err(|e| RouteError::Parse(e.to_string()))
            .and_then(|text| RouteTable::parse(text, &dedicated, require_generation));
        match parsed {
            Ok(table) => {
                tracing::info!(
                    generation = %table.generation,
                    pg_namespaces = table.pg.namespaces.len(),
                    "routes loaded"
                );
                tx.send_replace(Some(Arc::new(table)));
            }
            Err(e) => {
                tracing::error!(path = %path.display(), error = %e, "routes file invalid; keeping the current table");
            }
        }
    }
}

#[cfg(unix)]
type Hangup = Option<tokio::signal::unix::Signal>;
#[cfg(not(unix))]
type Hangup = ();

#[cfg(unix)]
fn hangup() -> Hangup {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).ok()
}

#[cfg(not(unix))]
fn hangup() -> Hangup {}

#[cfg(unix)]
async fn recv(hup: &mut Hangup) {
    match hup {
        Some(s) => {
            s.recv().await;
        }
        None => std::future::pending().await,
    }
}

#[cfg(not(unix))]
async fn recv(_: &mut Hangup) {
    std::future::pending().await
}
