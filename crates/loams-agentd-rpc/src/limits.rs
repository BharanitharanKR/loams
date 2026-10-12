//! The RPC server's limits (design §50 §7).

use std::time::Duration;

/// What one server accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The largest WebSocket message (and frame) read, in bytes. A larger one
    /// closes the connection.
    pub max_frame_bytes: usize,
    /// Streams open at once on one connection; one more is refused with
    /// `limit_exceeded`.
    pub max_streams: usize,
    /// Connections open at once with the main token; one more upgrade is
    /// answered 429.
    pub max_owner_connections: usize,
    /// Added after each failed authentication, before the next attempt from
    /// anyone is checked.
    pub auth_failure_delay: Duration,
    /// The largest upgrade request head, in bytes.
    pub max_request_head: usize,
    /// How long a client may take to send its upgrade request, and how long
    /// it may then wait for its token check.
    pub handshake_timeout: Duration,
    /// Connections that have not finished authenticating, at once; more are
    /// closed without an answer, so queued failures cannot pile up.
    pub max_pending_upgrades: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_streams: 64,
            max_owner_connections: 8,
            auth_failure_delay: Duration::from_millis(250),
            max_request_head: 16 * 1024,
            handshake_timeout: Duration::from_secs(10),
            max_pending_upgrades: 32,
        }
    }
}
