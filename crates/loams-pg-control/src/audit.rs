//! Audit events of `loams.postgres.v1` (D221, §41: every control-plane
//! mutation names its principal and the `act` chain; PG2 Task 9).
//!
//! An [`AuditEvent`] is emitted for every mutation, whether it applied or
//! was refused, and for every call refused before it reached the service
//! (unauthenticated, permission denied). It names who, what RPC, on which
//! namespace and project, the targets the request named (a branch id, a
//! role or database name), the idempotency key and how it ended (a reason,
//! never a message). It never holds a request or answer body, so no
//! password, credential or token can be in it.
//!
//! [`AuditSink`] is the seam: [`LogAuditSink`] writes each event as one JSON
//! log line on the `loams_audit` target, which the OTel log pipeline
//! carries to the operator's store (D221's "OTel logs to a Loams stream");
//! [`MemoryAuditSink`] keeps them for tests.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::Serialize;
use ulid::Ulid;

use crate::authz::{Actor, PrincipalKind};

/// The `tracing` target of [`LogAuditSink`]'s lines.
pub const AUDIT_TARGET: &str = "loams_audit";

/// How a call ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "result")]
pub enum AuditOutcome {
    Ok,
    /// Refused or failed, with the registered reason.
    Refused {
        reason: String,
    },
}

/// One audited call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditEvent {
    /// A ULID.
    pub id: String,
    /// Ms since the Unix epoch.
    pub time_ms: u64,
    /// `/loams.postgres.v1.PostgresService/CreateBranch`.
    pub rpc: String,
    /// Empty when the caller was not authenticated.
    pub principal: String,
    pub principal_kind: Option<PrincipalKind>,
    /// The actors for the principal, nearest first.
    pub act: Vec<Actor>,
    /// The API key's id, when one authenticated the call.
    pub credential: Option<String>,
    pub namespace: String,
    pub project_id: Option<String>,
    /// What the request named besides the project: `branch_id`, `role`,
    /// `database`, `name`. Names and ids only.
    pub target: BTreeMap<String, String>,
    pub idempotency_key: Option<String>,
    /// The operation a long-running call answered.
    pub operation_id: Option<String>,
    pub outcome: AuditOutcome,
}

impl AuditEvent {
    /// An event for `rpc` now, ending `outcome`; the caller fills the rest.
    pub fn new(rpc: impl Into<String>, time_ms: u64, outcome: AuditOutcome) -> Self {
        AuditEvent {
            id: Ulid::generate().to_string(),
            time_ms,
            rpc: rpc.into(),
            principal: String::new(),
            principal_kind: None,
            act: Vec::new(),
            credential: None,
            namespace: String::new(),
            project_id: None,
            target: BTreeMap::new(),
            idempotency_key: None,
            operation_id: None,
            outcome,
        }
    }
}

/// Where audit events go.
pub trait AuditSink: Send + Sync + 'static {
    fn emit(&self, event: &AuditEvent);
}

/// One JSON line per event on the [`AUDIT_TARGET`] log target.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogAuditSink;

impl AuditSink for LogAuditSink {
    fn emit(&self, event: &AuditEvent) {
        match serde_json::to_string(event) {
            Ok(json) => tracing::info!(target: AUDIT_TARGET, event = %json, "audit"),
            Err(e) => {
                tracing::error!(target: AUDIT_TARGET, error = %e, rpc = %event.rpc, "an audit event does not encode")
            }
        }
    }
}

/// Keeps every event, for tests.
#[derive(Debug, Default)]
pub struct MemoryAuditSink(Mutex<Vec<AuditEvent>>);

impl MemoryAuditSink {
    /// Every event so far, in order.
    pub fn events(&self) -> Vec<AuditEvent> {
        self.0.lock().map(|e| e.clone()).unwrap_or_default()
    }
}

impl AuditSink for MemoryAuditSink {
    fn emit(&self, event: &AuditEvent) {
        if let Ok(mut events) = self.0.lock() {
            events.push(event.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_event_is_one_json_object_without_bodies() {
        let mut e = AuditEvent::new(
            "/loams.postgres.v1.PostgresService/CreateRole",
            1,
            AuditOutcome::Refused {
                reason: "permission_denied".into(),
            },
        );
        e.principal = "alice".into();
        e.principal_kind = Some(PrincipalKind::User);
        e.act = vec![Actor {
            subject: "bot".into(),
            kind: PrincipalKind::Agent,
        }];
        e.target.insert("role".into(), "app".into());
        let json: serde_json::Value = serde_json::to_value(&e).expect("encodes");
        assert_eq!(json["outcome"]["result"], "refused");
        assert_eq!(json["outcome"]["reason"], "permission_denied");
        assert_eq!(json["act"][0]["kind"], "agent");
        assert_eq!(json["principal_kind"], "user");
        let sink = MemoryAuditSink::default();
        sink.emit(&e);
        assert_eq!(sink.events(), vec![e]);
    }
}
