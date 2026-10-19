//! `GetConnectionInfo` (§46 §8, D709; PG2 Tasks 20 and 25, R1.11): where
//! and as whom to connect to a branch, never with a password.
//!
//! Until PgDog fronts the computes (Task 20), clients reach an endpoint's
//! compute directly at the address the runtime gave it (`backend_addr`: a
//! published host port under compose, a Service under Kubernetes), and the
//! database is the compute's own name for it, not PgDog's routed one.
//!
//! - **Defaults.** No branch: the project's default branch. No endpoint:
//!   the branch's read-write endpoint (`read_only`: its first read-only
//!   one). No database: the branch's first database, else `postgres`. No
//!   role: the owner of that database, else the branch's first role.
//! - **A read.** It changes nothing: an endpoint that has no address yet
//!   (never started) answers `failed_precondition`, naming `StartEndpoint`.
//!   Waking a suspended endpoint (D709 step 5) waits for PgDog's waker.

use serde::{Deserialize, Serialize};

use super::{NeonRead, PgService, ServiceError};
use crate::model::{BranchScope, DatabaseRec, EndpointRec, EndpointType, RoleRec};

/// The database every compute has.
pub const DEFAULT_DATABASE: &str = "postgres";

/// `GetConnectionInfoRequest`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetConnectionInfo {
    pub namespace: String,
    pub project_id: String,
    pub branch_id: String,
    pub endpoint_id: String,
    pub role: String,
    pub database: String,
    pub read_only: bool,
}

/// How clients reach the computes (the connection's TLS).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectConfig {
    /// libpq's `sslmode` for the answer.
    pub sslmode: String,
    /// The CA bundle that signs the computes' certificates, PEM, when they
    /// have any.
    pub ca_pem: Option<String>,
}

impl Default for ConnectConfig {
    /// Computes the compose runtime starts serve no TLS of their own: a
    /// client that asks for it falls back to plaintext on the local host.
    fn default() -> Self {
        ConnectConfig {
            sslmode: "prefer".into(),
            ca_pem: None,
        }
    }
}

/// `ConnectionInfo`: no password field (R1.11).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub sslmode: String,
    pub ca_pem: String,
    /// The same as a libpq URI, without a password.
    pub uri: String,
}

/// `host:port`, `[v6]:port` → (host, port).
fn split_addr(addr: &str) -> Option<(String, u16)> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse().ok()?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    (!host.is_empty()).then(|| (host.to_owned(), port))
}

/// RFC 3986 percent-encoding of everything but the unreserved characters.
fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The libpq URI of `info` (no password).
fn uri(info: &ConnectionInfo) -> String {
    let host = if info.host.contains(':') {
        format!("[{}]", info.host)
    } else {
        info.host.clone()
    };
    format!(
        "postgresql://{}@{host}:{}/{}?sslmode={}",
        pct(&info.user),
        info.port,
        pct(&info.database),
        pct(&info.sslmode)
    )
}

/// The endpoint a request without one takes: the branch's read-write
/// endpoint, or (`read_only`) its first read-only one, in id order.
fn pick_endpoint<'a>(
    endpoints: &'a [crate::store::Versioned<EndpointRec>],
    branch_id: &str,
    read_only: bool,
) -> Option<&'a EndpointRec> {
    let kind = if read_only {
        EndpointType::ReadOnly
    } else {
        EndpointType::ReadWrite
    };
    endpoints
        .iter()
        .map(|e| &e.record)
        .find(|e| e.branch_id == branch_id && e.kind == kind)
}

impl<N: NeonRead> PgService<N> {
    /// `GetConnectionInfo`.
    ///
    /// # Errors
    ///
    /// `project_not_found`; `not_found` for the branch, endpoint, role or
    /// database; `invalid_argument` for an endpoint of another branch, or
    /// a read-write one with `read_only`; `failed_precondition` when the
    /// branch has no such endpoint, no role, or the endpoint no address.
    pub async fn connection_info(
        &self,
        req: GetConnectionInfo,
    ) -> Result<ConnectionInfo, ServiceError> {
        let scope = self
            .scope(&req.namespace, &req.project_id, &req.branch_id)
            .await?;
        let branch_id = scope.branch_id().to_owned();
        let endpoint = if req.endpoint_id.is_empty() {
            let all = self.all_endpoints(&req.project_id).await?;
            pick_endpoint(&all, &branch_id, req.read_only)
                .cloned()
                .ok_or_else(|| {
                    ServiceError::failed_precondition(format!(
                        "branch {branch_id} has no {} endpoint: CreateEndpoint first",
                        if req.read_only {
                            "read-only"
                        } else {
                            "read-write"
                        }
                    ))
                })?
        } else {
            let e = self
                .endpoint(&req.project_id, &req.endpoint_id, "endpoint_id")
                .await?
                .record;
            if e.branch_id != branch_id {
                return Err(ServiceError::invalid(
                    "endpoint_id",
                    format!("endpoint {} is not on branch {branch_id}", e.id),
                ));
            }
            if req.read_only && e.kind != EndpointType::ReadOnly {
                return Err(ServiceError::invalid(
                    "read_only",
                    format!("endpoint {} is read-write", e.id),
                ));
            }
            e
        };
        let (host, port) = endpoint
            .backend_addr
            .as_deref()
            .and_then(split_addr)
            .ok_or_else(|| {
                ServiceError::failed_precondition(format!(
                    "endpoint {} has no address yet: StartEndpoint first",
                    endpoint.id
                ))
            })?;

        let branch_scope = BranchScope {
            branch_id: branch_id.clone(),
        };
        let databases = self.all::<DatabaseRec>(&branch_scope).await?;
        let roles = self.all::<RoleRec>(&branch_scope).await?;
        let database = match req.database.as_str() {
            "" => databases.first().map(|d| d.record.clone()),
            name => match databases.iter().find(|d| d.record.name == name) {
                Some(d) => Some(d.record.clone()),
                None if name == DEFAULT_DATABASE => None,
                None => return Err(ServiceError::not_found("database", name)),
            },
        };
        let user = match req.role.as_str() {
            "" => database
                .as_ref()
                .map(|d| d.owner.clone())
                .or_else(|| roles.first().map(|r| r.record.name.clone()))
                .ok_or_else(|| {
                    ServiceError::failed_precondition(format!(
                        "branch {branch_id} has no role: CreateRole first"
                    ))
                })?,
            name => {
                if !roles.iter().any(|r| r.record.name == name) {
                    return Err(ServiceError::not_found("role", name));
                }
                name.to_owned()
            }
        };
        let mut info = ConnectionInfo {
            host,
            port,
            database: database.map_or_else(|| DEFAULT_DATABASE.to_owned(), |d| d.name),
            user,
            sslmode: self.config.connect.sslmode.clone(),
            ca_pem: self.config.connect.ca_pem.clone().unwrap_or_default(),
            uri: String::new(),
        };
        info.uri = uri(&info);
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses() {
        assert_eq!(
            split_addr("127.0.0.1:55432"),
            Some(("127.0.0.1".into(), 55432))
        );
        assert_eq!(split_addr("[::1]:5432"), Some(("::1".into(), 5432)));
        assert_eq!(split_addr("host"), None);
        assert_eq!(split_addr(":5432"), None);
        assert_eq!(split_addr("h:port"), None);
    }

    #[test]
    fn uri_is_encoded_and_has_no_password() {
        let info = ConnectionInfo {
            host: "::1".into(),
            port: 5432,
            database: "my db".into(),
            user: "a@b".into(),
            sslmode: "prefer".into(),
            ..ConnectionInfo::default()
        };
        assert_eq!(
            uri(&info),
            "postgresql://a%40b@[::1]:5432/my%20db?sslmode=prefer"
        );
    }
}
