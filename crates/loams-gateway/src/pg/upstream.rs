//! PgDog upstreams (design §57 §7.6, D1017): resolved at connect time,
//! tried in order, with passive ejection; then the gateway's own
//! `SSLRequest`, which must be answered `S`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::routes::PgNamespace;
use crate::sniff::pg::SSL_REQUEST;

/// How long a failed address is skipped.
pub const EJECT_FOR: Duration = Duration::from_secs(2);

/// Why no PgDog of a namespace could be used.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    /// No upstream host resolved to an address.
    #[error("no upstream address resolved for namespace {0}")]
    Unresolved(String),
    /// Every address failed to connect or to negotiate TLS.
    #[error("every upstream of namespace {0} failed")]
    AllFailed(String),
    /// PgDog answered `SSLRequest` with `N`.
    #[error("PgDog at {0} refused TLS")]
    NoTls(SocketAddr),
    /// PgDog answered `SSLRequest` with something else.
    #[error("PgDog at {addr} answered SSLRequest with {answer:#04x}")]
    Protocol { addr: SocketAddr, answer: u8 },
    /// A socket error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Addresses that recently failed, and until when they are skipped.
#[derive(Debug, Default)]
pub struct Ejections {
    until: Mutex<HashMap<SocketAddr, Instant>>,
}

impl Ejections {
    fn is_ejected(&self, addr: &SocketAddr, now: Instant) -> bool {
        self.lock().get(addr).is_some_and(|until| *until > now)
    }

    pub fn eject(&self, addr: SocketAddr, now: Instant) {
        self.lock().insert(addr, now + EJECT_FOR);
    }

    fn clear(&self, addr: &SocketAddr) {
        self.lock().remove(addr);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<SocketAddr, Instant>> {
        self.until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Every address of the namespace's upstreams: entries in their order,
/// each host's addresses sorted, duplicates dropped.
pub async fn resolve(ns: &PgNamespace) -> Vec<SocketAddr> {
    let mut out: Vec<SocketAddr> = Vec::new();
    for upstream in &ns.upstreams {
        match tokio::net::lookup_host(upstream.as_str()).await {
            Ok(addrs) => {
                let mut addrs: Vec<SocketAddr> = addrs.collect();
                addrs.sort();
                for a in addrs {
                    if !out.contains(&a) {
                        out.push(a);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(namespace = %ns.name, %upstream, error = %e, "upstream did not resolve")
            }
        }
    }
    out
}

/// The addresses in the order they are tried: healthy ones first, in
/// order; ejected ones after, so a namespace whose every replica failed
/// recently is still tried.
pub fn order(addrs: Vec<SocketAddr>, ejections: &Ejections, now: Instant) -> Vec<SocketAddr> {
    let (healthy, ejected): (Vec<_>, Vec<_>) = addrs
        .into_iter()
        .partition(|a| !ejections.is_ejected(a, now));
    healthy.into_iter().chain(ejected).collect()
}

/// Connects to the namespace's first reachable PgDog and negotiates TLS
/// with `SSLRequest` (the only way PgDog does TLS, §57 §3).
pub async fn connect_pgdog(
    ns: &PgNamespace,
    ejections: &Ejections,
    connect_timeout: Duration,
) -> Result<(TcpStream, SocketAddr), UpstreamError> {
    let addrs = resolve(ns).await;
    if addrs.is_empty() {
        return Err(UpstreamError::Unresolved(ns.name.clone()));
    }
    for addr in order(addrs, ejections, Instant::now()) {
        let stream = match tokio::time::timeout(connect_timeout, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                tracing::warn!(namespace = %ns.name, %addr, error = %e, "PgDog connect failed; ejecting");
                ejections.eject(addr, Instant::now());
                continue;
            }
            Err(_) => {
                tracing::warn!(namespace = %ns.name, %addr, "PgDog connect timed out; ejecting");
                ejections.eject(addr, Instant::now());
                continue;
            }
        };
        // A replica that accepts TCP but then hangs, resets, or answers
        // anything but `S` is ejected and the next one is tried; one that
        // answers `N` is skipped, never used (no downgrade).
        let mut stream = stream;
        let _ = stream.set_nodelay(true);
        match tokio::time::timeout(connect_timeout, ssl_request(&mut stream, addr)).await {
            Ok(Ok(())) => {
                ejections.clear(&addr);
                return Ok((stream, addr));
            }
            Ok(Err(e)) => {
                tracing::warn!(namespace = %ns.name, %addr, error = %e, "PgDog SSLRequest failed; ejecting");
            }
            Err(_) => {
                tracing::warn!(namespace = %ns.name, %addr, "PgDog SSLRequest timed out; ejecting");
            }
        }
        ejections.eject(addr, Instant::now());
    }
    Err(UpstreamError::AllFailed(ns.name.clone()))
}

/// Sends `SSLRequest` and requires `S`; `N` is never accepted (no
/// downgrade, D1015).
pub async fn ssl_request(stream: &mut TcpStream, addr: SocketAddr) -> Result<(), UpstreamError> {
    stream.write_all(&SSL_REQUEST).await?;
    let answer = stream.read_u8().await?;
    match answer {
        b'S' => Ok(()),
        b'N' => Err(UpstreamError::NoTls(addr)),
        answer => Err(UpstreamError::Protocol { addr, answer }),
    }
}

/// Sends a plaintext `CancelRequest` to every address of the namespace;
/// only the PgDog that issued the key acts on it (§57 §7.7).
pub async fn broadcast_cancel(
    ns: &PgNamespace,
    packet: &[u8; 16],
    connect_timeout: Duration,
) -> usize {
    let mut sent = 0;
    for addr in resolve(ns).await {
        let send = async {
            let mut s = TcpStream::connect(addr).await?;
            s.write_all(packet).await?;
            s.shutdown().await
        };
        match tokio::time::timeout(connect_timeout, send).await {
            Ok(Ok(())) => sent += 1,
            Ok(Err(e)) => {
                tracing::debug!(namespace = %ns.name, %addr, error = %e, "cancel not delivered")
            }
            Err(_) => tracing::debug!(namespace = %ns.name, %addr, "cancel timed out"),
        }
    }
    sent
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn healthy_first_ejected_after() {
        let e = Ejections::default();
        let now = Instant::now();
        e.eject(addr(1), now);
        assert_eq!(
            order(vec![addr(1), addr(2)], &e, now),
            vec![addr(2), addr(1)]
        );
        // Ejection ends.
        assert_eq!(
            order(vec![addr(1), addr(2)], &e, now + EJECT_FOR * 2),
            vec![addr(1), addr(2)]
        );
    }

    #[test]
    fn all_ejected_still_tries() {
        let e = Ejections::default();
        let now = Instant::now();
        e.eject(addr(1), now);
        e.eject(addr(2), now);
        assert_eq!(order(vec![addr(1), addr(2)], &e, now).len(), 2);
    }
}
