//! Authorization (LV1 plan Task 15, design §45 §5.2): what a [`Caller`]
//! may do in an app.
//!
//! Two checks, in this order:
//!
//! 1. **The app fence.** A credential bound to an environment reaches only
//!    the app of that environment (the app's `namespace`); any other app is
//!    `PERMISSION_DENIED`, reason `live_app_mismatch`. A credential bound
//!    to no environment (the self-hosted default) reaches every app.
//! 2. **Scopes and actions.** Each call is an [`Action`] on a
//!    [`Resource`], and needs the action's scope (`live:call`, `live:read`,
//!    `live:write`, `live:deploy`, `live:admin`); a read or write of one
//!    table is also allowed by `live:read:<table>` or `live:write:<table>`.
//!    A credential that names no scopes has them all. A refusal is
//!    `PERMISSION_DENIED`, reason `permission_denied`.
//!
//! App end users and anonymous callers (Task 16, §45 §5.3) are not
//! platform principals: they may only `call`, and only public functions;
//! every other action is `permission_denied` whatever their scopes.
//!
//! Calls map to actions as follows: a deployed function is `call` (an
//! internal one also needs a platform principal); `_system:*` queries are `read` and
//! mutations `write`, of the table they name (`_system:query`,
//! `_system:insert`) or of the whole app (the id-addressed ones and
//! `_system:tables`); every `LiveAdminService` deployment call is `deploy`;
//! the app directory calls are `admin`, and creating or listing apps also
//! needs a credential bound to no environment. Deploys, activations,
//! directory writes and `_system:*` writes write an audit line
//! (`audit = true`) until the engine has an audit log.

use std::fmt;

use crate::apps::AppRecord;
use crate::auth::Caller;
use crate::txn::{FnKind, Function, Visibility};
use crate::{LiveError, LiveValue};

/// The reason of a call outside the credential's environment.
pub const APP_MISMATCH: &str = "live_app_mismatch";

/// The reason of a call the credential's scopes do not allow.
pub const SCOPE_MISSING: &str = "permission_denied";

/// What a call does (§45 §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    /// Run a deployed function.
    Call,
    /// Read documents through `_system:*`.
    Read,
    /// Write documents through `_system:*`.
    Write,
    /// Deploy, activate, list deployments, read index status and logs.
    Deploy,
    /// Manage apps.
    Admin,
}

impl Action {
    /// The scope that allows the action.
    pub fn scope(self) -> &'static str {
        match self {
            Action::Call => "live:call",
            Action::Read => "live:read",
            Action::Write => "live:write",
            Action::Deploy => "live:deploy",
            Action::Admin => "live:admin",
        }
    }
}

/// What an action is on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resource {
    /// The cluster's app directory (creating and listing apps).
    Directory,
    /// One app: `live_app:<app>`.
    App(String),
    /// One table of an app: `live_table:<app>/<table>`.
    Table { app: String, table: String },
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Resource::Directory => f.write_str("live_apps"),
            Resource::App(app) => write!(f, "live_app:{app}"),
            Resource::Table { app, table } => write!(f, "live_table:{app}/{table}"),
        }
    }
}

/// Decides whether a caller may take an action on a resource (the D66
/// shape: `check(principal, action, resource)`). [`ScopeAuthorizer`] is the
/// one Live uses; the engine's role bindings may replace it.
pub trait Authorizer: Send + Sync {
    fn check(&self, caller: &Caller, action: Action, resource: &Resource) -> Result<(), LiveError>;
}

/// Allows what the caller's scopes allow.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScopeAuthorizer;

impl Authorizer for ScopeAuthorizer {
    fn check(&self, caller: &Caller, action: Action, resource: &Resource) -> Result<(), LiveError> {
        if !caller.is_platform() && action != Action::Call {
            return Err(LiveError::PermissionDenied {
                reason: SCOPE_MISSING,
                message: format!(
                    "{} on {resource} needs a platform principal; app end users call only \
                     public functions",
                    action.scope()
                ),
            });
        }
        if caller.has_scope(action.scope()) {
            return Ok(());
        }
        if let Resource::Table { table, .. } = resource
            && matches!(action, Action::Read | Action::Write)
            && caller.has_scope(&format!("{}:{table}", action.scope()))
        {
            return Ok(());
        }
        Err(LiveError::PermissionDenied {
            reason: SCOPE_MISSING,
            message: format!(
                "{} needs the scope {} on {resource}",
                caller.id,
                action.scope()
            ),
        })
    }
}

/// Refuses `caller` on `app` when the credential is bound to another
/// environment.
pub fn fence(caller: &Caller, app: &AppRecord) -> Result<(), LiveError> {
    match &caller.environment {
        Some(env) if *env != app.namespace => Err(LiveError::PermissionDenied {
            reason: APP_MISMATCH,
            message: format!(
                "this credential is bound to the environment {env}; the Live app {} belongs to \
                 another",
                app.name
            ),
        }),
        _ => Ok(()),
    }
}

/// The action and resource of calling `f` in `app` with `args`.
pub fn of_function(app: &str, f: &dyn Function, args: &LiveValue) -> (Action, Resource) {
    if !f.name().starts_with("_system:") {
        return (Action::Call, Resource::App(app.to_string()));
    }
    let action = match f.kind() {
        FnKind::Query => Action::Read,
        FnKind::Mutation => Action::Write,
    };
    let table = match args {
        LiveValue::Object(fields) => match fields.get("table") {
            Some(LiveValue::Str(table)) => Some(table.clone()),
            _ => None,
        },
        _ => None,
    };
    let resource = match table {
        Some(table) => Resource::Table {
            app: app.to_string(),
            table,
        },
        None => Resource::App(app.to_string()),
    };
    (action, resource)
}

/// Checks a call of `f` by `caller` in `app`: the scope, and for an
/// internal function a platform principal. Writes through `_system:*` are
/// audited.
pub fn check_call(
    authz: &dyn Authorizer,
    caller: &Caller,
    app: &str,
    f: &dyn Function,
    args: &LiveValue,
) -> Result<(), LiveError> {
    let (action, resource) = of_function(app, f, args);
    authz.check(caller, action, &resource)?;
    if f.visibility() == Visibility::Internal && !is_platform(caller) {
        return Err(LiveError::PermissionDenied {
            reason: SCOPE_MISSING,
            message: format!("{} is internal: only platform principals call it", f.name()),
        });
    }
    if action == Action::Write {
        audit(caller, action, &resource, f.name());
    }
    Ok(())
}

/// Whether the caller is a platform principal, not an app end user or an
/// anonymous caller (Task 16).
pub fn is_platform(caller: &Caller) -> bool {
    caller.is_platform()
}

/// One audit line (`audit = true`), until the engine has an audit log.
pub fn audit(caller: &Caller, action: Action, resource: &Resource, what: &str) {
    tracing::info!(
        audit = true,
        caller = %caller.id,
        kind = caller.kind.as_str(),
        action = action.scope(),
        resource = %resource,
        what,
        "Live audit"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::CallerKind;

    fn caller(scopes: &[&str]) -> Caller {
        Caller {
            scopes: Some(scopes.iter().map(|s| s.to_string()).collect()),
            ..Caller::new("apikey:01", CallerKind::ApiKey)
        }
    }

    #[test]
    fn table_scopes_allow_one_table() {
        let c = caller(&["live:read:notes"]);
        let notes = Resource::Table {
            app: "a".into(),
            table: "notes".into(),
        };
        let other = Resource::Table {
            app: "a".into(),
            table: "users".into(),
        };
        assert!(ScopeAuthorizer.check(&c, Action::Read, &notes).is_ok());
        assert!(ScopeAuthorizer.check(&c, Action::Read, &other).is_err());
        assert!(ScopeAuthorizer.check(&c, Action::Write, &notes).is_err());
        assert!(
            ScopeAuthorizer
                .check(&c, Action::Read, &Resource::App("a".into()))
                .is_err()
        );
        let all = Caller::new("dev-loopback", CallerKind::DevLoopback);
        assert!(
            ScopeAuthorizer
                .check(&all, Action::Admin, &Resource::Directory)
                .is_ok()
        );
        // An anonymous caller (or end user) names no scopes, yet only calls.
        let anonymous = Caller::anonymous();
        let app = Resource::App("a".into());
        assert!(
            ScopeAuthorizer
                .check(&anonymous, Action::Call, &app)
                .is_ok()
        );
        for action in [Action::Read, Action::Write, Action::Deploy, Action::Admin] {
            assert!(ScopeAuthorizer.check(&anonymous, action, &notes).is_err());
        }
    }
}
