//! The connection observer (design §57 §11, D1021): the only way
//! per-namespace numbers leave the process. A trait with no wire format and
//! a no-op default; metrics carry no tenant label.

use std::fmt::Debug;
use std::time::Duration;

/// How a Postgres connection was carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Path {
    /// TLS end to end between the client and PgDog.
    Spliced,
    /// TLS terminated at the gateway and re-encrypted to PgDog.
    Terminated,
}

/// Sees each routed connection open and close.
pub trait ConnectionObserver: Send + Sync + Debug {
    /// A connection to `namespace` starts relaying over `path`.
    fn on_open(&self, _namespace: &str, _path: Path) {}
    /// That connection ended after relaying `bytes_in` from the client and
    /// `bytes_out` to it, `duration` after it was accepted.
    fn on_close(&self, _namespace: &str, _bytes_in: u64, _bytes_out: u64, _duration: Duration) {}
}

/// Observes nothing.
#[derive(Debug, Default)]
pub struct NoopObserver;

impl ConnectionObserver for NoopObserver {}
