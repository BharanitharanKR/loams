//! Browser origins (LV1 plan Task 18, design §45 §6.4).
//!
//! Each app lists the browser origins its own pages are served from
//! (`allowed_origins`). A CORS preflight cannot carry the `loams-live-app`
//! header's value, so the engine's layer answers it for the **union** of the
//! active apps' origins ([`AppOrigins`], kept current by the app
//! [`Engines`](crate::engines::Engines) from the directory). The actual
//! request names its app, and [`check`] then holds its `Origin` to **that**
//! app's list: anything else is `PERMISSION_DENIED`, reason
//! `live_origin_not_allowed`. A platform principal on an origin the engine
//! itself allows (the console's, marked [`ConsoleOrigin`] by the engine's
//! layer) is let through as well, so the console keeps working on every app.
//! Credentials are bearer headers, never cookies:
//! `Access-Control-Allow-Credentials` is never sent.

use std::collections::BTreeSet;
use std::sync::{Arc, PoisonError, RwLock};

use connectrpc::RequestContext;

use crate::apps::{AppRecord, AppState};
use crate::auth::Caller;
use crate::error::LiveError;

/// The reason of a browser call from an origin the addressed app does not
/// list.
pub const ORIGIN_NOT_ALLOWED: &str = "live_origin_not_allowed";

/// The union of the active apps' allowed origins. Cheap to clone; every
/// clone sees the same set.
#[derive(Clone, Default)]
pub struct AppOrigins {
    set: Arc<RwLock<Arc<BTreeSet<String>>>>,
}

impl std::fmt::Debug for AppOrigins {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.snapshot().iter()).finish()
    }
}

impl AppOrigins {
    /// Whether an active app lists `origin` (a trailing `/` ignored).
    pub fn contains(&self, origin: &str) -> bool {
        self.snapshot().contains(normalize(origin))
    }

    /// The current set.
    pub fn snapshot(&self) -> Arc<BTreeSet<String>> {
        self.set
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replaces the set with the origins of the active apps in `apps`.
    pub fn replace<'a>(&self, apps: impl IntoIterator<Item = &'a AppRecord>) {
        let set: BTreeSet<String> = apps
            .into_iter()
            .filter(|app| app.state == AppState::Active)
            .flat_map(|app| app.settings.allowed_origins.iter())
            .map(|o| normalize(o).to_owned())
            .collect();
        *self.set.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(set);
    }
}

/// The extension the engine's layer puts on a request whose `Origin` the
/// engine's own CORS allows (the console's origins).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsoleOrigin;

fn normalize(origin: &str) -> &str {
    origin.trim_end_matches('/')
}

/// Holds a browser request's `Origin` to the addressed app's list (see the
/// module docs). A request without `Origin` (not a browser's) passes.
pub fn check(ctx: &RequestContext, app: &AppRecord, caller: &Caller) -> Result<(), LiveError> {
    let Some(origin) = ctx.header("origin") else {
        return Ok(());
    };
    let origin = origin
        .to_str()
        .map_err(|_| LiveError::invalid("the Origin header is not text"))?;
    check_origin(
        origin,
        app,
        caller,
        ctx.extensions().get::<ConsoleOrigin>().is_some(),
    )
}

/// [`check`] on the request's `origin`; `console` when the engine's layer
/// marked it [`ConsoleOrigin`].
pub fn check_origin(
    origin: &str,
    app: &AppRecord,
    caller: &Caller,
    console: bool,
) -> Result<(), LiveError> {
    let listed = app
        .settings
        .allowed_origins
        .iter()
        .any(|o| normalize(o) == normalize(origin));
    if listed || (console && caller.is_platform()) {
        return Ok(());
    }
    Err(LiveError::PermissionDenied {
        reason: ORIGIN_NOT_ALLOWED,
        message: format!(
            "the origin {origin} is not among the Live app {}'s allowed origins",
            app.name
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::AppSettings;

    fn app(name: &str, state: AppState, origins: &[&str]) -> AppRecord {
        AppRecord {
            id: 1,
            name: name.to_owned(),
            namespace: String::new(),
            keyspace: format!("loams_live_{name}"),
            state,
            settings: AppSettings {
                allowed_origins: origins.iter().map(|o| (*o).to_owned()).collect(),
                ..AppSettings::default()
            },
            cursor_key_ref: String::new(),
            created_at_ms: 0,
        }
    }

    #[test]
    fn the_union_holds_active_apps_only() {
        let origins = AppOrigins::default();
        let seen = origins.clone();
        origins.replace(&[
            app("shop", AppState::Active, &["https://shop.example/"]),
            app("old", AppState::Disabled, &["https://old.example"]),
        ]);
        assert!(seen.contains("https://shop.example"));
        assert!(seen.contains("https://shop.example/"));
        assert!(!seen.contains("https://old.example"));
    }

    #[test]
    fn an_origin_is_held_to_the_addressed_app() {
        let shop = app("shop", AppState::Active, &["https://shop.example"]);
        let user = Caller::anonymous();
        let admin = Caller::unauthenticated();
        assert!(check_origin("https://shop.example", &shop, &user, false).is_ok());
        let err = check_origin("https://blog.example", &shop, &user, true).unwrap_err();
        assert!(
            matches!(err, LiveError::PermissionDenied { reason, .. } if reason == ORIGIN_NOT_ALLOWED)
        );
        assert!(check_origin("https://console.example", &shop, &admin, true).is_ok());
        assert!(check_origin("https://console.example", &shop, &admin, false).is_err());
    }
}
