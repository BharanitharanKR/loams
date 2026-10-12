//! Idle-session migration: graceful compute replacement (plan SQ1 Task 6;
//! §47 §5.3, §14).
//!
//! When a pool member goes away (scale-in, a rolling upgrade, a pod
//! replacement), the control plane asks its sessions to move through their
//! lease ([`crate::upstream::Close::Migrate`]). A session moves only between
//! commands, with no transaction or cursor open
//! ([`crate::codec::response::ResponseTracker::movable`]):
//!
//! 1. the relay stops forwarding client commands (it holds the upstream
//!    writer) and runs `SHOW SESSION_STATES` on the old member itself;
//! 2. it logs in to a new member as the same `ri_<role>` (with its own
//!    password: no TiDB session token is needed), with no database;
//! 3. it runs `SET NAMES utf8mb4` (the states are UTF-8 whatever the
//!    login's collation), `SET SESSION sql_mode = ''` (so the backslash
//!    escapes below hold whatever the global `sql_mode` is) and `SET
//!    SESSION_STATES '<json>'`, which restores the session's variables (its
//!    character sets and `sql_mode` included), current database, prepared
//!    statements (same ids) and bindings; a session that switched
//!    multi-statements away from what its login gives gets `COM_SET_OPTION`
//!    again;
//! 4. it switches the relay to the new connection and closes the old one.
//!
//! A session TiDB will not move (ERR 8146: an open transaction, unfetched
//! cursor rows, bound long data, locks, temporary tables) stays on its
//! member and moves at a later idle point, after its next command. Sessions
//! that never become movable are drained: the control plane closes them
//! through the lease ([`crate::upstream::Close::WhenIdle`], `Now`) when the
//! member must go. A migration that fails once the old member was asked
//! (`SHOW SESSION_STATES` errors otherwise, no new member logs in, `SET
//! SESSION_STATES` fails) closes the session with ERR 1053.
//!
//! What a client can notice: `CONNECTION_ID()` changes, and so does the
//! member-specific state TiDB does not migrate (`@@hostname`, `@@port`,
//! the process list).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio_rustls::TlsConnector;

use crate::auth::Role;
use crate::codec::Reader;
use crate::codec::command::ErrPacket;
use crate::codec::handshake::{Capabilities, upstream_capabilities};
use crate::upstream::{ClientContext, CredentialStore, Upstream, UpstreamMember, connect};
use crate::wire::PacketIo;

/// TiDB's `ErrCannotMigrateSession`.
pub const ER_CANNOT_MIGRATE_SESSION: u16 = 8146;

/// The largest response the gate reads for itself (`SHOW SESSION_STATES`
/// with many prepared statements).
const MAX_RESPONSE: usize = 64 << 20;

/// What the relay needs to log in to another member as the session's
/// internal user.
pub struct Reconnect {
    /// TLS to TiDB.
    pub tls: TlsConnector,
    /// The internal passwords.
    pub credentials: Arc<dyn CredentialStore>,
    /// The session's branch and role.
    pub branch: String,
    /// The role: the session logs in as `ri_<role>`.
    pub role: Role,
    /// The client as the first login saw it (the database is taken from
    /// the session states, not from here).
    pub client: ClientContext,
    /// The static upstream profile.
    pub profile: Capabilities,
    /// The deadline of each step (login, `SHOW` and `SET`).
    pub timeout: Duration,
}

impl fmt::Debug for Reconnect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reconnect")
            .field("branch", &self.branch)
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

impl Reconnect {
    /// The capabilities of the session's upstream connections.
    pub fn capabilities(&self) -> Capabilities {
        upstream_capabilities(self.client.agreed, self.profile)
    }
}

/// Why a migration failed (the session is then closed with 1053).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MigrateError {
    /// TiDB answered a statement of the migration with an error.
    #[error("{what} failed ({code})")]
    Server {
        /// The statement.
        what: &'static str,
        /// TiDB's error code.
        code: u16,
    },
    /// No new member accepted the login.
    #[error("no member to move to: {0}")]
    NoMember(String),
    /// A step passed its deadline.
    #[error("{0} timed out")]
    Timeout(&'static str),
    /// A malformed answer or an I/O error.
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// The result of a migration that did not fail.
pub enum Outcome {
    /// The session runs on the new member now; the old one is closed.
    Moved(Upstream),
    /// TiDB will not move the session yet (ERR 8146, with TiDB's reason);
    /// it stays.
    Stayed(Upstream, String),
}

impl fmt::Debug for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Outcome::Moved(_) => "Moved",
            Outcome::Stayed(..) => "Stayed",
        })
    }
}

fn protocol(e: impl fmt::Display) -> MigrateError {
    MigrateError::Protocol(e.to_string())
}

/// The member a connection is on.
pub fn member_addr(upstream: &Upstream) -> Option<std::net::SocketAddr> {
    upstream.get_ref().0.peer_addr().ok()
}

/// A text-protocol query's rows (each column `None` for NULL), or TiDB's
/// error. `io` must be between commands.
async fn query(
    io: &mut PacketIo<Upstream>,
    sql: &str,
    caps: Capabilities,
) -> Result<Result<Vec<Vec<Option<Vec<u8>>>>, ErrPacket>, MigrateError> {
    let mut payload = Vec::with_capacity(1 + sql.len());
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    io.set_next_seq(0);
    io.write(&payload).await.map_err(protocol)?;
    let first = io.read().await.map_err(protocol)?;
    match first.first() {
        Some(0x00) => return Ok(Ok(Vec::new())),
        Some(0xff) => return Ok(Err(ErrPacket::decode(&first).map_err(protocol)?)),
        _ => {}
    }
    let columns = Reader::new(&first)
        .lenenc("column count")
        .map_err(protocol)?;
    if columns == 0 || columns > 4096 {
        return Err(protocol(format_args!("{columns} columns")));
    }
    for _ in 0..columns {
        io.read().await.map_err(protocol)?;
    }
    if !caps.contains(Capabilities::DEPRECATE_EOF) {
        io.read().await.map_err(protocol)?;
    }
    let mut rows = Vec::new();
    loop {
        let p = io.read().await.map_err(protocol)?;
        match p.first() {
            Some(0xfe) if p.len() < 0xff_ffff => return Ok(Ok(rows)),
            Some(0xff) => return Ok(Err(ErrPacket::decode(&p).map_err(protocol)?)),
            _ => {}
        }
        let mut r = Reader::new(&p);
        let mut row = Vec::new();
        for _ in 0..columns {
            if r.peek() == Some(0xfb) {
                r.bytes(1, "null").map_err(protocol)?;
                row.push(None);
            } else {
                row.push(Some(
                    r.lenenc_bytes(MAX_RESPONSE, "column")
                        .map_err(protocol)?
                        .to_vec(),
                ));
            }
        }
        rows.push(row);
    }
}

/// A statement that answers OK (or an error).
async fn execute(
    io: &mut PacketIo<Upstream>,
    what: &'static str,
    sql: &str,
    caps: Capabilities,
) -> Result<(), MigrateError> {
    match query(io, sql, caps).await? {
        Ok(_) => Ok(()),
        Err(e) => Err(MigrateError::Server { what, code: e.code }),
    }
}

/// `sql` as a single-quoted string literal under backslash escapes (the
/// escaping TiDB's own `DecodeSessionStates` uses for prepared statements).
pub fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

async fn timed<T>(
    what: &'static str,
    limit: Duration,
    f: impl Future<Output = Result<T, MigrateError>>,
) -> Result<T, MigrateError> {
    tokio::time::timeout(limit, f)
        .await
        .map_err(|_| MigrateError::Timeout(what))?
}

/// Moves the session on `old` (between commands, its writer held by the
/// caller) to the first of `to` that accepts it. `multi_statements` is the
/// session's current multi-statement option.
pub async fn migrate(
    old: Upstream,
    to: &[UpstreamMember],
    r: &Reconnect,
    multi_statements: bool,
) -> Result<Outcome, MigrateError> {
    let caps = r.capabilities();
    let mut io = PacketIo::new(old, MAX_RESPONSE);
    let rows = timed(
        "SHOW SESSION_STATES",
        r.timeout,
        query(&mut io, "SHOW SESSION_STATES", caps),
    )
    .await?;
    let states = match rows {
        Ok(rows) => match rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next())
        {
            Some(Some(json)) => match String::from_utf8(json) {
                Ok(json) => json,
                // A `character_set_results` that is not UTF-8 turned
                // non-ASCII state into other bytes: nothing changed on the
                // old member, so the session stays (and is drained).
                Err(_) => {
                    return Ok(Outcome::Stayed(
                        io.into_inner().map_err(protocol)?,
                        "session states are not UTF-8 (character_set_results)".into(),
                    ));
                }
            },
            _ => return Err(protocol("SHOW SESSION_STATES returned no states")),
        },
        Err(e) if e.code == ER_CANNOT_MIGRATE_SESSION => {
            return Ok(Outcome::Stayed(
                io.into_inner().map_err(protocol)?,
                e.message,
            ));
        }
        Err(e) => {
            return Err(MigrateError::Server {
                what: "SHOW SESSION_STATES",
                code: e.code,
            });
        }
    };
    let password = r
        .credentials
        .internal_password(&r.branch, r.role)
        .await
        .ok_or_else(|| MigrateError::NoMember("no internal credential".into()))?;
    let client = ClientContext {
        database: None,
        ..r.client.clone()
    };
    let mut new = None;
    let mut last = String::from("no members");
    for member in to {
        let login = connect(
            member,
            &r.tls,
            &client,
            r.role.internal_user(),
            &password,
            r.profile,
        );
        match tokio::time::timeout(r.timeout, login).await {
            Ok(Ok(u)) => {
                new = Some(u);
                break;
            }
            Ok(Err(e)) => last = format!("{}: {e}", member.addr),
            Err(_) => last = format!("{}: login timed out", member.addr),
        }
    }
    drop(password);
    let new = new.ok_or(MigrateError::NoMember(last))?;
    let mut nio = PacketIo::new(new, MAX_RESPONSE);
    timed("SET SESSION_STATES", r.timeout, async {
        // The states are UTF-8 text; the login's collation may not be.
        execute(&mut nio, "SET NAMES", "SET NAMES utf8mb4", caps).await?;
        execute(&mut nio, "SET sql_mode", "SET SESSION sql_mode = ''", caps).await?;
        let set = format!("SET SESSION_STATES {}", quote(&states));
        execute(&mut nio, "SET SESSION_STATES", &set, caps).await?;
        if multi_statements != caps.contains(Capabilities::MULTI_STATEMENTS) {
            // The session switched multi-statements away from what the
            // login gives. COM_SET_OPTION (0 on, 1 off): EOF or ERR.
            let option = if multi_statements { 0x00 } else { 0x01 };
            nio.set_next_seq(0);
            nio.write(&[0x1b, option, 0x00]).await.map_err(protocol)?;
            let p = nio.read().await.map_err(protocol)?;
            if p.first() == Some(&0xff) {
                let e = ErrPacket::decode(&p).map_err(protocol)?;
                return Err(MigrateError::Server {
                    what: "COM_SET_OPTION",
                    code: e.code,
                });
            }
        }
        Ok(())
    })
    .await?;
    let new = nio.into_inner().map_err(protocol)?;
    // The old member: COM_QUIT, then close (TiDB ends the session on EOF
    // anyway). Its errors no longer matter.
    let mut old = io.into_inner().map_err(protocol)?;
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        let _ = old.write_all(&[1, 0, 0, 0, 0x01]).await;
        let _ = old.shutdown().await;
    })
    .await;
    Ok(Outcome::Moved(new))
}
