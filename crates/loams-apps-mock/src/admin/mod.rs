//! The console API on Connect: `loams.auth.v1` and `loams.admin.v1` (API2
//! Tasks 6 and 7), served beside the console's `/api/v1/*` REST routes from
//! the same seed.
//!
//! **One seed, two surfaces.** [`ConsoleApi`] holds the console seed that
//! [`crate::console`] turns into REST answers, resolved once at start, so a
//! REST route and its RPC answer the same record with the same times. Each
//! handler reads the seed record the REST route answers and turns it into the
//! proto message through its proto3 JSON mapping: the field names are the
//! REST contract's (proto3 JSON accepts the original `snake_case` names), and
//! [`fix`] rewrites the few values the two spell differently, which are the
//! enums (`"owner"` is `ORG_ROLE_OWNER`) and the contract's `limits`, which
//! the proto does not carry.
//!
//! **The mock's semantics are the REST mock's.** A create answers the seed's
//! example object and the next list does not include it; an update, suspend
//! or resume answers the seed record with the change applied, but does not
//! keep it. What is real beyond that:
//!
//! - `KeyService/CreateKey` answers a key's secret once per
//!   `idempotency_key`; a replay is `failed_precondition`, reason
//!   `secret_already_issued`.
//! - Every mutation made over Connect appends an event to the audit log
//!   ([`journal::Journal`]), which `ListAuditEvents` lists and
//!   `WatchAuditEvents` streams, with resume cursors and heartbeats.
//! - `AuthService/GetSession` answers `unauthenticated` when the mock runs
//!   signed out (`--signed-out`), unless the call carries a session: the
//!   cookie `CreateSession` and `CompleteSetup` set, or a bearer token.
//!
//! No admin RPC checks a role here: the REST routes it mirrors do not either.
//! The server's `connect_admin.rs`, with the `Authorizer` and
//! `member_cannot_create_key`, is the server half of Task 7.

// The generated traits return `impl Encodable<_>`; these impls name the
// concrete message type, which is the intended refinement.
#![allow(refining_impl_trait)]

mod agents;
mod auth;
pub(crate) mod journal;
mod keys;
mod org;
mod projects;

use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use connectrpc::{ConnectError, ErrorCode, Router};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::refuse;

pub(crate) use agents::Agents;
pub(crate) use auth::Auth;
pub(crate) use journal::Audit;
pub(crate) use keys::Keys;
pub(crate) use org::Org;
pub(crate) use projects::Projects;

/// The cookie `CreateSession` and `CompleteSetup` set. Its value is fake and
/// documented as such, like the mock's bearer tokens.
pub(crate) const SESSION_COOKIE: &str = "loams_session";

/// The most a list answers in one page, and the page size a request with
/// `page_size` 0 gets.
const MAX_PAGE: usize = 1_000;
const DEFAULT_PAGE: usize = 100;

/// The console API's state: the resolved console seed and what the Connect
/// mutations changed.
#[derive(Debug)]
pub(crate) struct ConsoleApi {
    /// The console seed, resolved once (the REST routes use the same value).
    pub(crate) seed: Value,
    /// Whether `GetSession` answers without a credential (`--signed-out`
    /// makes it false, as it makes `GET /api/v1/session` answer 401).
    pub(crate) signed_in: bool,
    /// The audit log the seed starts with plus every Connect mutation.
    pub(crate) journal: journal::Journal,
    /// `CreateKey` idempotency keys whose secret has been answered.
    issued: Mutex<HashSet<String>>,
}

impl ConsoleApi {
    pub(crate) fn new(seed: Value, signed_in: bool, heartbeat: Duration) -> anyhow::Result<Self> {
        let events = seed
            .get("audit")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|mut event| {
                fix::audit_event(&mut event);
                from_json(event)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| anyhow::anyhow!("the console seed's audit log: {err}"))?;
        Ok(Self {
            journal: journal::Journal::new(events, heartbeat),
            seed,
            signed_in,
            issued: Mutex::new(HashSet::new()),
        })
    }

    /// The seed value under `key`.
    fn get(&self, key: &str) -> Result<&Value, ConnectError> {
        self.seed
            .get(key)
            .ok_or_else(|| internal(format!("the console seed has no `{key}`")))
    }

    /// The array under `key`.
    fn items(&self, key: &str) -> Result<Vec<Value>, ConnectError> {
        Ok(self.get(key)?.as_array().cloned().unwrap_or_default())
    }

    /// The array under `key`, then `entry` (`keys` → `docs-search/production`).
    fn entry_items(&self, key: &str, entry: &str) -> Result<Vec<Value>, ConnectError> {
        Ok(self
            .get(key)?
            .get(entry)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    /// One of the seed's example objects (what a create answers).
    fn example(&self, name: &str) -> Result<Value, ConnectError> {
        self.get("examples")?
            .get(name)
            .cloned()
            .ok_or_else(|| internal(format!("the console seed has no example `{name}`")))
    }

    fn issued(&self) -> MutexGuard<'_, HashSet<String>> {
        self.issued.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Registers every console API service on `router`.
pub(crate) fn register(api: &Arc<ConsoleApi>, router: Router) -> Router {
    use crate::proto::loams::admin::v1::{
        AgentServiceExt as _, AuditServiceExt as _, KeyServiceExt as _, OrgServiceExt as _,
        ProjectServiceExt as _,
    };
    use crate::proto::loams::auth::v1::AuthServiceExt as _;

    let router = Arc::new(Auth(api.clone())).register(router);
    let router = Arc::new(Org(api.clone())).register(router);
    let router = Arc::new(Projects(api.clone())).register(router);
    let router = Arc::new(Agents(api.clone())).register(router);
    let router = Arc::new(Keys(api.clone())).register(router);
    Arc::new(Audit(api.clone())).register(router)
}

/// A seed record as a proto message, through the proto3 JSON mapping.
pub(crate) fn from_json<T: DeserializeOwned>(value: Value) -> Result<T, ConnectError> {
    serde_json::from_value(value).map_err(|err| {
        internal(format!(
            "a console seed record does not fit its message ({}): {err}",
            std::any::type_name::<T>()
        ))
    })
}

/// `internal`: the seed and the protos disagree, which is a bug in the mock.
fn internal(message: String) -> ConnectError {
    refuse(ErrorCode::Internal, "internal", message)
}

/// `not_found` with the registered reason and a message naming the resource.
pub(crate) fn not_found(kind: &str, name: &str) -> ConnectError {
    refuse(
        ErrorCode::NotFound,
        "not_found",
        format!("no {kind} `{name}`"),
    )
}

/// `invalid_argument` naming the field.
pub(crate) fn invalid(field: &str, message: &str) -> ConnectError {
    refuse(
        ErrorCode::InvalidArgument,
        "invalid_argument",
        format!("{field}: {message}"),
    )
}

/// One AIP-158 page of `items`. The token is the offset of the page's first
/// item, which is all a fixed list needs.
pub(crate) fn page<T>(
    items: Vec<T>,
    page_size: i32,
    page_token: &str,
) -> Result<(Vec<T>, String), ConnectError> {
    let size = match usize::try_from(page_size) {
        Ok(0) => DEFAULT_PAGE,
        Ok(n) => n.min(MAX_PAGE),
        Err(_) => return Err(invalid("page_size", "must not be negative")),
    };
    let start = if page_token.is_empty() {
        0
    } else {
        page_token
            .strip_prefix('o')
            .and_then(|n| n.parse::<usize>().ok())
            .filter(|n| *n <= items.len())
            .ok_or_else(|| invalid("page_token", "not a token this list issued"))?
    };
    let end = (start + size).min(items.len());
    let next = if end < items.len() {
        format!("o{end}")
    } else {
        String::new()
    };
    Ok((
        items.into_iter().skip(start).take(end - start).collect(),
        next,
    ))
}

/// The rewrites between the REST contract's JSON and the proto3 JSON mapping.
///
/// Field names already agree (the protos use the contract's), and proto3 JSON
/// reads `null` as the default, so only the enums differ: the contract spells
/// them in lower case (`"service_account"`) and proto3 JSON wants the value's
/// full name (`SUBJECT_KIND_SERVICE_ACCOUNT`).
pub(crate) mod fix {
    use serde_json::Value;

    /// `value[key] = PREFIX_<VALUE>` when it is a string.
    fn enumerate(value: &mut Value, key: &str, prefix: &str) {
        if let Some(slot) = value.get_mut(key)
            && let Some(name) = slot.as_str()
        {
            *slot = Value::String(format!("{prefix}_{}", name.to_ascii_uppercase()));
        }
    }

    fn each(value: &mut Value, key: &str, f: impl Fn(&mut Value)) {
        if let Some(items) = value.get_mut(key).and_then(Value::as_array_mut) {
            items.iter_mut().for_each(f);
        }
    }

    /// A `SubjectRef` under `key`, when there is one (it may be `null`).
    pub(crate) fn subject(value: &mut Value, key: &str) {
        if let Some(subject) = value.get_mut(key) {
            enumerate(subject, "kind", "SUBJECT_KIND");
        }
    }

    pub(crate) fn member(value: &mut Value) {
        enumerate(value, "role", "ORG_ROLE");
    }

    pub(crate) fn invitation(value: &mut Value) {
        enumerate(value, "role", "ORG_ROLE");
        subject(value, "invited_by");
    }

    pub(crate) fn team_detail(value: &mut Value) {
        each(value, "projects", |grant| {
            enumerate(grant, "role", "PROJECT_ROLE")
        });
    }

    pub(crate) fn grant(value: &mut Value) {
        enumerate(value, "role", "PROJECT_ROLE");
        subject(value, "subject");
    }

    /// An environment's activity: the contract's `limits` (always `null` in
    /// the seed) has no proto field.
    pub(crate) fn activity(value: &mut Value) {
        if let Some(map) = value.as_object_mut() {
            map.remove("limits");
        }
    }

    pub(crate) fn agent(value: &mut Value) {
        enumerate(value, "status", "AGENT_STATUS");
        subject(value, "owner");
    }

    pub(crate) fn token(value: &mut Value) {
        enumerate(value, "flow", "TOKEN_FLOW");
        subject(value, "acting_for");
    }

    pub(crate) fn api_key(value: &mut Value) {
        subject(value, "principal");
    }

    pub(crate) fn audit_event(value: &mut Value) {
        enumerate(value, "outcome", "AUDIT_OUTCOME");
        subject(value, "actor");
        subject(value, "acting_for");
    }

    pub(crate) fn session(value: &mut Value) {
        enumerate(value, "role", "ORG_ROLE");
    }

    pub(crate) fn sign_in(value: &mut Value) {
        each(value, "oidc", |provider| {
            enumerate(provider, "kind", "OIDC_PROVIDER_KIND");
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pages_walk_the_whole_list_and_refuse_a_foreign_token() {
        let items: Vec<u32> = (0..5).collect();
        let (first, next) = page(items.clone(), 2, "").unwrap();
        assert_eq!((first, next.as_str()), (vec![0, 1], "o2"));
        let (second, next) = page(items.clone(), 2, &next).unwrap();
        assert_eq!((second, next.as_str()), (vec![2, 3], "o4"));
        let (last, next) = page(items.clone(), 2, &next).unwrap();
        assert_eq!((last, next.as_str()), (vec![4], ""));
        assert_eq!(page(items.clone(), 0, "").unwrap().0.len(), 5);
        assert!(page(items.clone(), 2, "o9").is_err());
        assert!(page(items.clone(), 2, "zz").is_err());
        assert!(page(items, -1, "").is_err());
    }

    #[test]
    fn fix_spells_enums_as_proto3_json_does() {
        let mut agent = serde_json::json!({
            "status": "suspended",
            "owner": {"kind": "service_account", "id": "sa_1", "name": "ci"},
        });
        fix::agent(&mut agent);
        assert_eq!(agent["status"], "AGENT_STATUS_SUSPENDED");
        assert_eq!(agent["owner"]["kind"], "SUBJECT_KIND_SERVICE_ACCOUNT");
        // A null subject stays null, which proto3 JSON reads as unset.
        let mut token = serde_json::json!({"flow": "federation", "acting_for": null});
        fix::token(&mut token);
        assert_eq!(token["acting_for"], Value::Null);
    }
}
