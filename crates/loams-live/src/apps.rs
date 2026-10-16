//! The app directory (design §45 §7, D690; LV1 plan Task 13): which Live
//! apps a cluster has, where each one's data lives, and their settings.
//!
//! The directory lives in the system keyspace [`SYSTEM_KEYSPACE`] and is
//! read and written through the store seam like any Live data. Its keys,
//! relative to the system store's root:
//!
//! ```text
//! counter = 0x00 ‖ name           ("next_id" → u32 BE, "changes" → u64 BE)
//! app     = 0x01 ‖ id:u32 BE      → AppRecord
//! name    = 0x02 ‖ name           → id:u32 BE
//! intent  = 0x03 ‖ id:u32 BE      → AppIntent (a create in progress)
//! ```
//!
//! Every write bumps the `changes` counter, so directory writes serialize on
//! it, and nodes poll it to drop their cached records
//! ([`Apps::sync`]). A new app gets the keyspace `loams_live_a<id>`, which
//! [`Apps::create`] ensures **before** it writes the record: an intent
//! names the keyspace first, the keyspace is ensured, and one transaction
//! then writes the record and removes the intent. A create that fails after
//! the keyspace leaves no record and an intent, which [`Apps::janitor_at`]
//! reports as an orphan keyspace. The server's own app (`--live-app`) is
//! [`adopt`](Apps::adopt)ed into the directory with the keyspace it already
//! uses.
//!
//! Deletion is soft: [`Apps::delete`] marks the app deleting (every call
//! refused) and disables its keyspace in PD (TiKV); once the retention has
//! passed, the janitor deletes the app's data, archives its keyspace and
//! removes its record.
//!
//! [`Keyspaces`] maps keyspace names to stores. In production each name is
//! its own keyspace ([`Layout::Dedicated`]); the test suites map every name
//! to a root under the test keyspace ([`Layout::Rooted`]), since a test
//! cluster has only its fixed keyspaces.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use buffa::Message;
use loams_kv::{CommitMode, KeyspaceState, Store, StoreConfig, Txn, TxnOptions};

use crate::txn::lift;
use crate::{Limits, LiveError, catalog, pb};

/// The keyspace of the app directory (design §45 §7).
pub const SYSTEM_KEYSPACE: &str = "loams_live_system";

/// The keyspace prefix of an app created through the directory: app 7
/// lives in `loams_live_a7`.
pub const APP_KEYSPACE_PREFIX: &str = "loams_live_a";

/// The request header that names the app a call addresses (design §45
/// §6.2). Without it, the call goes to the server's default app.
pub const APP_HEADER: &str = "loams-live-app";

/// How many apps one cluster may have until LV1 measures it (design §45
/// §15, P14: at least 1 000 dedicated keyspaces expected).
pub const DEFAULT_MAX_APPS: u32 = 1_000;

/// How long a deleted app's data is kept before the janitor purges it.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// How old an intent must be before the janitor reports its keyspace as an
/// orphan (a create still running is not one).
pub const DEFAULT_ORPHAN_AFTER: Duration = Duration::from_secs(10 * 60);

/// Where every app's pagination cursor key lives: a record of the app's own
/// catalog (LV1 plan Task 4).
pub const CURSOR_KEY_REF: &str = "catalog";

/// The longest namespace accepted.
pub const MAX_NAMESPACE_BYTES: usize = 256;

/// The most issuers and allowed origins one app may list.
pub const MAX_SETTINGS_ENTRIES: usize = 64;

/// The page size of [`Apps::list`] when the caller names none, and the
/// largest one.
pub const DEFAULT_PAGE: usize = 100;
pub const MAX_PAGE: usize = 1_000;

/// Keys deleted per transaction when a deleted app is purged.
const PURGE_BATCH: usize = 512;

/// The first byte of a [`Layout::Rooted`] keyspace's root under the base
/// root. Live's own keys start below it (`0x01`–`0x05`, `0xA0`).
const ROOTED_TAG: u8 = 0xFE;

const COUNTER: u8 = 0x00;
const APP: u8 = 0x01;
const NAME: u8 = 0x02;
const INTENT: u8 = 0x03;

fn counter_key(name: &[u8]) -> Vec<u8> {
    [&[COUNTER][..], name].concat()
}

fn app_key(id: u32) -> Vec<u8> {
    [&[APP][..], &id.to_be_bytes()].concat()
}

fn name_key(name: &str) -> Vec<u8> {
    [&[NAME][..], name.as_bytes()].concat()
}

fn intent_key(id: u32) -> Vec<u8> {
    [&[INTENT][..], &id.to_be_bytes()].concat()
}

/// The keyspace of the app with id `id`.
pub fn app_keyspace(id: u32) -> String {
    format!("{APP_KEYSPACE_PREFIX}{id}")
}

/// How [`Keyspaces`] maps a keyspace name to a store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Layout {
    /// Each name is a keyspace of its own (production): created through PD
    /// on TiKV, inside the one file on the embedded store.
    #[default]
    Dedicated,
    /// Every name is a root under the base store's root, in the base
    /// keyspace (the test suites: a test cluster has only fixed keyspaces).
    Rooted,
}

/// Maps keyspace names to stores: the base store's own keyspace is the base
/// store itself; every other name follows the [`Layout`].
#[derive(Debug, Clone)]
pub struct Keyspaces {
    base: StoreConfig,
    layout: Layout,
}

impl Keyspaces {
    /// Keyspaces beside `base` (the server's default app's store).
    pub fn new(base: StoreConfig, layout: Layout) -> Self {
        Keyspaces { base, layout }
    }

    /// The layout.
    pub fn layout(&self) -> Layout {
        self.layout
    }

    fn is_base(&self, keyspace: &str) -> bool {
        keyspace == self.base.keyspace()
    }

    /// The store configuration of `keyspace`.
    pub fn config(&self, keyspace: &str) -> StoreConfig {
        if self.is_base(keyspace) {
            return self.base.clone();
        }
        match self.layout {
            Layout::Dedicated => self.base.with_keyspace(keyspace),
            Layout::Rooted => {
                let mut root = self.base.root().to_vec();
                root.push(ROOTED_TAG);
                root.extend_from_slice(keyspace.as_bytes());
                root.push(0);
                self.base.with_root(root)
            }
        }
    }

    /// Creates `keyspace` when its backend needs that first (TiKV, through
    /// PD; idempotent).
    pub async fn ensure(&self, keyspace: &str) -> Result<(), LiveError> {
        if self.layout == Layout::Rooted || self.is_base(keyspace) {
            return Ok(());
        }
        self.config(keyspace)
            .ensure_keyspace()
            .await
            .map_err(|e| LiveError::Internal(format!("ensuring the keyspace {keyspace}: {e}")))
    }

    /// Moves `keyspace` to `state` in PD (TiKV, dedicated keyspaces only;
    /// nothing elsewhere).
    pub async fn set_state(&self, keyspace: &str, state: KeyspaceState) -> Result<(), LiveError> {
        if self.layout == Layout::Rooted || self.is_base(keyspace) {
            return Ok(());
        }
        self.config(keyspace)
            .set_keyspace_state(state)
            .await
            .map_err(|e| {
                LiveError::Internal(format!(
                    "setting the keyspace {keyspace} {}: {e}",
                    state.as_pd()
                ))
            })
    }

    /// Opens the store of `keyspace`.
    pub async fn open(&self, keyspace: &str) -> Result<Store, LiveError> {
        Store::open(self.config(keyspace))
            .await
            .map_err(|e| LiveError::Internal(format!("opening the keyspace {keyspace}: {e}")))
    }
}

/// Where an app is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppState {
    /// Calls are served.
    Active,
    /// Every call is refused (`live_app_disabled`).
    Disabled,
    /// Deleted: refused like a disabled app, and purged at `purge_after_ms`
    /// (Unix ms).
    Deleting { purge_after_ms: u64 },
}

impl AppState {
    /// The wire state.
    pub fn to_proto(self) -> pb::AppState {
        match self {
            AppState::Active => pb::AppState::APP_STATE_ACTIVE,
            AppState::Disabled => pb::AppState::APP_STATE_DISABLED,
            AppState::Deleting { .. } => pb::AppState::APP_STATE_DELETING,
        }
    }

    /// The state's name, as the API spells it.
    pub fn name(self) -> &'static str {
        match self {
            AppState::Active => "active",
            AppState::Disabled => "disabled",
            AppState::Deleting { .. } => "deleting",
        }
    }
}

/// Per-app limits: each one only tightens the server's [`Limits`]
/// ([`AppLimits::apply`]); `None` keeps the server's.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct AppLimits {
    pub max_js_cpu_ms: Option<u64>,
    pub mutation_deadline_ms: Option<u64>,
    pub max_result_bytes: Option<u64>,
    pub max_scanned_docs: Option<u64>,
    pub max_written_docs: Option<u64>,
    pub max_written_bytes: Option<u64>,
}

impl AppLimits {
    fn fields(&self) -> [(&'static str, Option<u64>); 6] {
        [
            ("max_js_cpu_ms", self.max_js_cpu_ms),
            ("mutation_deadline_ms", self.mutation_deadline_ms),
            ("max_result_bytes", self.max_result_bytes),
            ("max_scanned_docs", self.max_scanned_docs),
            ("max_written_docs", self.max_written_docs),
            ("max_written_bytes", self.max_written_bytes),
        ]
    }

    fn check(&self) -> Result<(), LiveError> {
        for (name, value) in self.fields() {
            if value == Some(0) {
                return Err(LiveError::invalid(format!(
                    "limits.{name} must be positive when set"
                )));
            }
        }
        Ok(())
    }

    /// `server` tightened by these limits: each set limit is the smaller of
    /// the two.
    pub fn apply(&self, server: &Limits) -> Limits {
        fn min_usize(server: usize, app: Option<u64>) -> usize {
            app.map_or(server, |a| {
                server.min(usize::try_from(a).unwrap_or(usize::MAX))
            })
        }
        fn min_ms(server: Duration, app: Option<u64>) -> Duration {
            app.map_or(server, |a| server.min(Duration::from_millis(a)))
        }
        Limits {
            max_js_cpu: min_ms(server.max_js_cpu, self.max_js_cpu_ms),
            mutation_deadline: min_ms(server.mutation_deadline, self.mutation_deadline_ms),
            max_result_bytes: min_usize(server.max_result_bytes, self.max_result_bytes),
            max_scanned_docs: min_usize(server.max_scanned_docs, self.max_scanned_docs),
            max_written_docs: min_usize(server.max_written_docs, self.max_written_docs),
            max_written_bytes: min_usize(server.max_written_bytes, self.max_written_bytes),
            ..server.clone()
        }
    }

    /// The wire limits.
    pub fn to_proto(&self) -> pb::AppLimits {
        pb::AppLimits {
            max_js_cpu_ms: self.max_js_cpu_ms,
            mutation_deadline_ms: self.mutation_deadline_ms,
            max_result_bytes: self.max_result_bytes,
            max_scanned_docs: self.max_scanned_docs,
            max_written_docs: self.max_written_docs,
            max_written_bytes: self.max_written_bytes,
            ..Default::default()
        }
    }

    /// From the wire.
    pub fn from_proto(p: &pb::AppLimits) -> Self {
        AppLimits {
            max_js_cpu_ms: p.max_js_cpu_ms,
            mutation_deadline_ms: p.mutation_deadline_ms,
            max_result_bytes: p.max_result_bytes,
            max_scanned_docs: p.max_scanned_docs,
            max_written_docs: p.max_written_docs,
            max_written_bytes: p.max_written_bytes,
        }
    }
}

/// An OIDC issuer whose tokens identify the app's end users (design §45
/// §5.3), verified by [`EndUsers`](crate::auth::EndUsers) (LV1 Task 16).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TrustedIssuer {
    pub issuer: String,
    pub audiences: Vec<String>,
    pub jwks_uri: Option<String>,
}

/// Whether `url` is https, or http to a loopback host (tests and a local
/// issuer).
pub(crate) fn https_or_loopback(url: &str) -> bool {
    if url.starts_with("https://") {
        return true;
    }
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or_default(),
        None => authority
            .rsplit_once(':')
            .map_or(authority, |(host, _port)| host),
    };
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

impl TrustedIssuer {
    fn check(&self) -> Result<(), LiveError> {
        if !https_or_loopback(&self.issuer) {
            return Err(LiveError::invalid(format!(
                "issuer {:?}: an issuer is an https URL (http only on loopback)",
                self.issuer
            )));
        }
        if self.audiences.is_empty() || self.audiences.iter().any(String::is_empty) {
            return Err(LiveError::invalid(format!(
                "issuer {:?}: name at least one audience, none empty",
                self.issuer
            )));
        }
        if let Some(uri) = &self.jwks_uri
            && !https_or_loopback(uri)
        {
            return Err(LiveError::invalid(format!(
                "issuer {:?}: jwks_uri is an https URL (http only on loopback)",
                self.issuer
            )));
        }
        Ok(())
    }

    /// The wire issuer.
    pub fn to_proto(&self) -> pb::TrustedIssuer {
        pb::TrustedIssuer {
            issuer: self.issuer.clone(),
            audiences: self.audiences.clone(),
            jwks_uri: self.jwks_uri.clone(),
            ..Default::default()
        }
    }

    /// From the wire.
    pub fn from_proto(p: &pb::TrustedIssuer) -> Self {
        TrustedIssuer {
            issuer: p.issuer.clone(),
            audiences: p.audiences.clone(),
            jwks_uri: p.jwks_uri.clone(),
        }
    }
}

/// An app's settings, which [`Apps::update`] changes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct AppSettings {
    pub limits: AppLimits,
    pub issuers: Vec<TrustedIssuer>,
    /// Browser origins (`scheme://host[:port]`) allowed to call the app.
    pub allowed_origins: Vec<String>,
    pub allow_anonymous: bool,
}

impl AppSettings {
    fn check(&self) -> Result<(), LiveError> {
        self.limits.check()?;
        if self.issuers.len() > MAX_SETTINGS_ENTRIES
            || self.allowed_origins.len() > MAX_SETTINGS_ENTRIES
        {
            return Err(LiveError::invalid(format!(
                "an app lists at most {MAX_SETTINGS_ENTRIES} issuers and {MAX_SETTINGS_ENTRIES} \
                 allowed origins"
            )));
        }
        for issuer in &self.issuers {
            issuer.check()?;
        }
        for origin in &self.allowed_origins {
            check_origin(origin)?;
        }
        Ok(())
    }
}

/// An origin is `scheme://host[:port]`: no path, no wildcard.
fn check_origin(origin: &str) -> Result<(), LiveError> {
    let bad = || {
        LiveError::invalid(format!(
            "allowed origin {origin:?}: use scheme://host[:port], with no path or wildcard"
        ))
    };
    let rest = origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"))
        .ok_or_else(bad)?;
    if rest.is_empty()
        || rest.contains(['/', '*', '?', '#', ' ', '@'])
        || origin.len() > MAX_NAMESPACE_BYTES
    {
        return Err(bad());
    }
    Ok(())
}

/// One app of the directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppRecord {
    /// Cluster-unique and permanent.
    pub id: u32,
    pub name: String,
    /// The org and environment it belongs to (Q639), free text until LV1
    /// Task 15.
    pub namespace: String,
    pub keyspace: String,
    pub state: AppState,
    pub settings: AppSettings,
    /// Where its cursor key lives ([`CURSOR_KEY_REF`]).
    pub cursor_key_ref: String,
    /// Unix ms (the store's clock).
    pub created_at_ms: u64,
}

impl AppRecord {
    /// The app as the admin API shows it.
    pub fn to_proto(&self) -> pb::App {
        pb::App {
            id: self.id,
            name: self.name.clone(),
            namespace: self.namespace.clone(),
            keyspace: self.keyspace.clone(),
            state: self.state.to_proto().into(),
            purge_after_ms: match self.state {
                AppState::Deleting { purge_after_ms } => purge_after_ms,
                _ => 0,
            },
            limits: buffa::MessageField::some(self.settings.limits.to_proto()),
            issuers: self
                .settings
                .issuers
                .iter()
                .map(TrustedIssuer::to_proto)
                .collect(),
            allowed_origins: self.settings.allowed_origins.clone(),
            allow_anonymous: self.settings.allow_anonymous,
            created_at_ms: self.created_at_ms,
            ..Default::default()
        }
    }

    fn encode(&self) -> Vec<u8> {
        pb::AppRecord {
            format: crate::RECORD_FORMAT,
            app: buffa::MessageField::some(self.to_proto()),
            cursor_key_ref: self.cursor_key_ref.clone(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn decode(bytes: &[u8]) -> Result<Self, LiveError> {
        let corrupt = |what: String| LiveError::Corrupt(format!("app record: {what}"));
        let p = pb::AppRecord::decode_from_slice(bytes).map_err(|e| corrupt(e.to_string()))?;
        if !crate::record_format_readable(p.format) {
            return Err(corrupt(format!(
                "format {} (this release reads {})",
                p.format,
                crate::record_format_range()
            )));
        }
        let app = p.app.as_option().ok_or_else(|| corrupt("no app".into()))?;
        let state = match app.state.as_known() {
            Some(pb::AppState::APP_STATE_ACTIVE) => AppState::Active,
            Some(pb::AppState::APP_STATE_DISABLED) => AppState::Disabled,
            Some(pb::AppState::APP_STATE_DELETING) => AppState::Deleting {
                purge_after_ms: app.purge_after_ms,
            },
            _ => return Err(corrupt(format!("unknown state {:?}", app.state))),
        };
        Ok(AppRecord {
            id: app.id,
            name: app.name.clone(),
            namespace: app.namespace.clone(),
            keyspace: app.keyspace.clone(),
            state,
            settings: AppSettings {
                limits: app
                    .limits
                    .as_option()
                    .map(AppLimits::from_proto)
                    .unwrap_or_default(),
                issuers: app.issuers.iter().map(TrustedIssuer::from_proto).collect(),
                allowed_origins: app.allowed_origins.clone(),
                allow_anonymous: app.allow_anonymous,
            },
            cursor_key_ref: p.cursor_key_ref,
            created_at_ms: app.created_at_ms,
        })
    }
}

/// What [`Apps::create`] creates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NewApp {
    pub name: String,
    pub namespace: String,
    pub settings: AppSettings,
}

/// What [`Apps::update`] changes; `None` keeps the field.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppUpdate {
    /// `Active` or `Disabled`.
    pub state: Option<AppState>,
    pub limits: Option<AppLimits>,
    pub issuers: Option<Vec<TrustedIssuer>>,
    pub allowed_origins: Option<Vec<String>>,
    pub allow_anonymous: Option<bool>,
}

/// A page of [`Apps::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppPage {
    /// By id.
    pub apps: Vec<AppRecord>,
    /// The id the next page starts after; `None` on the last page.
    pub next: Option<u32>,
}

/// What one [`Apps::janitor_at`] run did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirectoryReport {
    /// The deleted apps whose data it purged and whose records it removed.
    pub purged: Vec<AppRecord>,
    /// Keyspaces a create ensured but no app owns: a create that failed
    /// after its keyspace (or is older than the orphan age and still
    /// running). Reported, never deleted.
    pub orphans: Vec<String>,
}

/// The directory's limits and ages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppsOptions {
    /// Apps the cluster may have (deleting ones count until purged).
    pub max_apps: u32,
    /// How long a deleted app is kept.
    pub retention: Duration,
    /// How old an intent is before it counts as an orphan.
    pub orphan_after: Duration,
}

impl Default for AppsOptions {
    fn default() -> Self {
        AppsOptions {
            max_apps: DEFAULT_MAX_APPS,
            retention: DEFAULT_RETENTION,
            orphan_after: DEFAULT_ORPHAN_AFTER,
        }
    }
}

/// The app directory. Cheap to clone.
#[derive(Clone)]
pub struct Apps {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Apps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Apps")
            .field("store", &self.inner.store)
            .field("options", &self.inner.options)
            .finish_non_exhaustive()
    }
}

struct Inner {
    store: Store,
    keyspaces: Keyspaces,
    options: AppsOptions,
    cache: Mutex<Cache>,
}

/// Records read by name, valid while the `changes` counter is `version`.
#[derive(Default)]
struct Cache {
    version: Option<u64>,
    by_name: HashMap<String, AppRecord>,
}

fn txn_options(op: &'static str) -> TxnOptions {
    TxnOptions {
        // Two-phase commit, as Live's own transactions (R1 plan row T7-1).
        commit_mode: Some(CommitMode::TwoPc),
        ..TxnOptions::new(op)
    }
}

fn decode_u32(bytes: &[u8], what: &str) -> Result<u32, LiveError> {
    bytes
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| LiveError::Corrupt(format!("app directory: {what} has {} bytes", bytes.len())))
}

fn decode_u64(bytes: &[u8], what: &str) -> Result<u64, LiveError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| LiveError::Corrupt(format!("app directory: {what} has {} bytes", bytes.len())))
}

/// Bumps the `changes` counter in `txn`: every directory write does, so
/// they conflict with each other and nodes see that the directory moved.
async fn bump(txn: &mut Txn) -> Result<(), LiveError> {
    let key = counter_key(b"changes");
    let now = match txn.get(&key).await? {
        Some(bytes) => decode_u64(&bytes, "the change counter")?,
        None => 0,
    };
    txn.put(&key, (now + 1).to_be_bytes().to_vec()).await?;
    Ok(())
}

async fn load_by_id(txn: &mut Txn, id: u32) -> Result<Option<AppRecord>, LiveError> {
    match txn.get(&app_key(id)).await? {
        Some(bytes) => AppRecord::decode(&bytes).map(Some),
        None => Ok(None),
    }
}

async fn load_by_name(txn: &mut Txn, name: &str) -> Result<Option<AppRecord>, LiveError> {
    match txn.get(&name_key(name)).await? {
        Some(id) => load_by_id(txn, decode_u32(&id, "a name record")?).await,
        None => Ok(None),
    }
}

/// The number of app records.
async fn count(txn: &mut Txn) -> Result<usize, LiveError> {
    let mut n = 0;
    let mut from = vec![APP];
    let end = [APP + 1];
    loop {
        let page = txn.scan(&from, Some(&end), MAX_PAGE).await?;
        n += page.len();
        match page.last() {
            Some((key, _)) if page.len() == MAX_PAGE => {
                from = key.clone();
                from.push(0);
            }
            _ => return Ok(n),
        }
    }
}

fn check_namespace(namespace: &str) -> Result<(), LiveError> {
    if namespace.len() > MAX_NAMESPACE_BYTES || namespace.chars().any(char::is_control) {
        return Err(LiveError::invalid(format!(
            "namespace: at most {MAX_NAMESPACE_BYTES} bytes and no control characters"
        )));
    }
    Ok(())
}

impl Apps {
    /// The directory in the system keyspace of `keyspaces` (ensured
    /// first).
    pub async fn open(keyspaces: Keyspaces, options: AppsOptions) -> Result<Apps, LiveError> {
        keyspaces.ensure(SYSTEM_KEYSPACE).await?;
        let store = keyspaces.open(SYSTEM_KEYSPACE).await?;
        Ok(Apps::with_store(store, keyspaces, options))
    }

    /// The directory on `store` (the system keyspace's store, opened by the
    /// caller: a test puts a fault plan on it).
    pub fn with_store(store: Store, keyspaces: Keyspaces, options: AppsOptions) -> Apps {
        Apps {
            inner: Arc::new(Inner {
                store,
                keyspaces,
                options,
                cache: Mutex::new(Cache::default()),
            }),
        }
    }

    /// The directory's store.
    pub fn store(&self) -> &Store {
        &self.inner.store
    }

    /// The keyspaces the apps live in.
    pub fn keyspaces(&self) -> &Keyspaces {
        &self.inner.keyspaces
    }

    /// The options.
    pub fn options(&self) -> &AppsOptions {
        &self.inner.options
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, Cache> {
        self.inner
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Forgets every cached record (after a write of this node).
    fn invalidate(&self) {
        let mut cache = self.cache();
        cache.version = None;
        cache.by_name.clear();
    }

    async fn now_ms(&self) -> Result<u64, LiveError> {
        self.inner
            .store
            .now()
            .await
            .map(loams_kv::Ts::physical_ms)
            .map_err(|e| LiveError::Internal(format!("a timestamp: {e}")))
    }

    async fn run<T: Send + 'static, F>(&self, op: &'static str, body: F) -> Result<T, LiveError>
    where
        F: for<'t> FnMut(
                &'t mut Txn,
            ) -> futures::future::BoxFuture<
                't,
                Result<Result<T, LiveError>, loams_kv::TxnError>,
            > + Send,
    {
        let done = self.inner.store.run(txn_options(op), body).await?.value;
        self.invalidate();
        done
    }

    /// Creates the app `new.name`: allocates its id and writes an intent,
    /// ensures its keyspace `loams_live_a<id>`, then writes its record and
    /// removes the intent in one transaction. Refused when the name is
    /// taken (`live_app_exists`) or the cluster has
    /// [`max_apps`](AppsOptions::max_apps) apps.
    pub async fn create(&self, new: NewApp) -> Result<AppRecord, LiveError> {
        catalog::check_name("app", &new.name)?;
        check_namespace(&new.namespace)?;
        new.settings.check()?;
        let max_apps = self.inner.options.max_apps;
        let started_at_ms = self.now_ms().await?;
        // 1. The id and the intent.
        let name = new.name.clone();
        let intent = self
            .run("live.apps.reserve", move |txn| {
                let name = name.clone();
                Box::pin(async move {
                    let reserved = async {
                        refuse_taken(txn, &name).await?;
                        refuse_full(txn, max_apps).await?;
                        let key = counter_key(b"next_id");
                        let id = match txn.get(&key).await? {
                            Some(bytes) => decode_u32(&bytes, "the next id")?,
                            None => 1,
                        };
                        let next = id.checked_add(1).ok_or_else(|| {
                            LiveError::Internal("the app ids are exhausted".into())
                        })?;
                        txn.put(&key, next.to_be_bytes().to_vec()).await?;
                        let intent = pb::AppIntent {
                            format: crate::RECORD_FORMAT,
                            id,
                            name: name.clone(),
                            keyspace: app_keyspace(id),
                            started_at_ms,
                            ..Default::default()
                        };
                        txn.put(&intent_key(id), intent.encode_to_vec()).await?;
                        Ok(intent)
                    };
                    lift(reserved.await)
                })
            })
            .await?;
        // 2. The keyspace, before any record names it.
        self.inner.keyspaces.ensure(&intent.keyspace).await?;
        // 3. The record, the name and the intent's removal, at once.
        let record = AppRecord {
            id: intent.id,
            name: new.name,
            namespace: new.namespace,
            keyspace: intent.keyspace.clone(),
            state: AppState::Active,
            settings: new.settings,
            cursor_key_ref: CURSOR_KEY_REF.to_string(),
            created_at_ms: started_at_ms,
        };
        let write = record.clone();
        self.run("live.apps.create", move |txn| {
            let record = write.clone();
            Box::pin(async move {
                let created = async {
                    refuse_taken(txn, &record.name).await?;
                    refuse_full(txn, max_apps).await?;
                    txn.delete(&intent_key(record.id)).await?;
                    txn.put(&app_key(record.id), record.encode()).await?;
                    txn.put(&name_key(&record.name), record.id.to_be_bytes().to_vec())
                        .await?;
                    bump(txn).await
                };
                lift(created.await)
            })
        })
        .await?;
        tracing::info!(app = %record.name, id = record.id, keyspace = %record.keyspace, "created a Live app");
        Ok(record)
    }

    /// The app `name`, writing its record with the keyspace `keyspace` when
    /// the directory has none: the server's own app (`--live-app`), whose
    /// keyspace predates the directory. An existing record is returned as
    /// it is.
    pub async fn adopt(&self, name: &str, keyspace: &str) -> Result<AppRecord, LiveError> {
        catalog::check_name("app", name)?;
        if let Some(record) = self.get_by_name(name).await? {
            return Ok(record);
        }
        let created_at_ms = self.now_ms().await?;
        let (name, keyspace) = (name.to_string(), keyspace.to_string());
        self.run("live.apps.adopt", move |txn| {
            let (name, keyspace) = (name.clone(), keyspace.clone());
            Box::pin(async move {
                let adopted = async {
                    if let Some(record) = load_by_name(txn, &name).await? {
                        return Ok(record);
                    }
                    let key = counter_key(b"next_id");
                    let id = match txn.get(&key).await? {
                        Some(bytes) => decode_u32(&bytes, "the next id")?,
                        None => 1,
                    };
                    txn.put(&key, (id + 1).to_be_bytes().to_vec()).await?;
                    let record = AppRecord {
                        id,
                        name,
                        namespace: String::new(),
                        keyspace,
                        state: AppState::Active,
                        settings: AppSettings::default(),
                        cursor_key_ref: CURSOR_KEY_REF.to_string(),
                        created_at_ms,
                    };
                    txn.put(&app_key(id), record.encode()).await?;
                    txn.put(&name_key(&record.name), id.to_be_bytes().to_vec())
                        .await?;
                    bump(txn).await?;
                    Ok(record)
                };
                lift(adopted.await)
            })
        })
        .await
    }

    /// The app `name`, from this node's cache when it has it.
    pub async fn get_by_name(&self, name: &str) -> Result<Option<AppRecord>, LiveError> {
        if let Some(record) = self.cache().by_name.get(name) {
            return Ok(Some(record.clone()));
        }
        let wanted = name.to_string();
        let found = self
            .inner
            .store
            .run(txn_options("live.apps.get"), move |txn| {
                let name = wanted.clone();
                Box::pin(async move { lift(load_by_name(txn, &name).await) })
            })
            .await?
            .value?;
        if let Some(record) = &found {
            self.cache()
                .by_name
                .insert(record.name.clone(), record.clone());
        }
        Ok(found)
    }

    /// The app whose namespace is `namespace` (the app bound to an
    /// environment, LV1 plan Task 15); the lowest id when several are.
    pub async fn find_by_namespace(&self, namespace: &str) -> Result<Option<AppRecord>, LiveError> {
        let mut after = None;
        loop {
            let page = self.list(after, MAX_PAGE).await?;
            if let Some(found) = page.apps.into_iter().find(|a| a.namespace == namespace) {
                return Ok(Some(found));
            }
            match page.next {
                Some(next) => after = Some(next),
                None => return Ok(None),
            }
        }
    }

    /// A page of apps by id: up to `limit` (100 when 0, at most 1 000)
    /// after the id `after`.
    pub async fn list(&self, after: Option<u32>, limit: usize) -> Result<AppPage, LiveError> {
        let limit = match limit {
            0 => DEFAULT_PAGE,
            n => n.min(MAX_PAGE),
        };
        self.inner
            .store
            .run(txn_options("live.apps.list"), move |txn| {
                Box::pin(async move {
                    let listed = async {
                        let from = match after {
                            Some(id) => {
                                let mut k = app_key(id);
                                k.push(0);
                                k
                            }
                            None => vec![APP],
                        };
                        let pairs = txn.scan(&from, Some(&[APP + 1]), limit + 1).await?;
                        let more = pairs.len() > limit;
                        let apps = pairs
                            .iter()
                            .take(limit)
                            .map(|(_, v)| AppRecord::decode(v))
                            .collect::<Result<Vec<_>, _>>()?;
                        let next = if more {
                            apps.last().map(|a| a.id)
                        } else {
                            None
                        };
                        Ok(AppPage { apps, next })
                    };
                    lift(listed.await)
                })
            })
            .await?
            .value
    }

    /// Changes the app `name`'s settings or state. Refused for an unknown
    /// app (`NOT_FOUND`), and for a deleting one.
    pub async fn update(&self, name: &str, update: AppUpdate) -> Result<AppRecord, LiveError> {
        if matches!(update.state, Some(AppState::Deleting { .. })) {
            return Err(LiveError::invalid(
                "UpdateApp sets the state ACTIVE or DISABLED; DeleteApp deletes",
            ));
        }
        let wanted = name.to_string();
        let record = self
            .run("live.apps.update", move |txn| {
                let (name, update) = (wanted.clone(), update.clone());
                Box::pin(async move {
                    let updated = async {
                        let mut record = load_by_name(txn, &name)
                            .await?
                            .ok_or_else(|| not_found(&name))?;
                        if let AppState::Deleting { .. } = record.state {
                            return Err(LiveError::FailedPrecondition(format!(
                                "the app {name} is deleted"
                            )));
                        }
                        if let Some(state) = update.state {
                            record.state = state;
                        }
                        let settings = &mut record.settings;
                        if let Some(limits) = update.limits {
                            settings.limits = limits;
                        }
                        if let Some(issuers) = update.issuers {
                            settings.issuers = issuers;
                        }
                        if let Some(origins) = update.allowed_origins {
                            settings.allowed_origins = origins;
                        }
                        if let Some(anonymous) = update.allow_anonymous {
                            settings.allow_anonymous = anonymous;
                        }
                        settings.check()?;
                        txn.put(&app_key(record.id), record.encode()).await?;
                        bump(txn).await?;
                        Ok(record)
                    };
                    lift(updated.await)
                })
            })
            .await?;
        tracing::info!(app = %record.name, state = record.state.name(), "updated a Live app");
        Ok(record)
    }

    /// Deletes the app `name` softly: it is deleting (every call refused)
    /// until the retention has passed, and its keyspace is disabled (TiKV).
    /// Deleting a deleting app returns it unchanged.
    pub async fn delete(&self, name: &str) -> Result<AppRecord, LiveError> {
        let purge_after_ms = self.now_ms().await?.saturating_add(
            u64::try_from(self.inner.options.retention.as_millis()).unwrap_or(u64::MAX),
        );
        let wanted = name.to_string();
        let record = self
            .run("live.apps.delete", move |txn| {
                let name = wanted.clone();
                Box::pin(async move {
                    let deleted = async {
                        let mut record = load_by_name(txn, &name)
                            .await?
                            .ok_or_else(|| not_found(&name))?;
                        if let AppState::Deleting { .. } = record.state {
                            return Ok(record);
                        }
                        record.state = AppState::Deleting { purge_after_ms };
                        txn.put(&app_key(record.id), record.encode()).await?;
                        bump(txn).await?;
                        Ok(record)
                    };
                    lift(deleted.await)
                })
            })
            .await?;
        self.inner
            .keyspaces
            .set_state(&record.keyspace, KeyspaceState::Disabled)
            .await?;
        tracing::info!(app = %record.name, id = record.id, "deleted a Live app; its data is purged after the retention");
        Ok(record)
    }

    /// The directory's change counter (0 before its first write).
    pub async fn version(&self) -> Result<u64, LiveError> {
        self.inner
            .store
            .run(txn_options("live.apps.version"), |txn| {
                Box::pin(async move {
                    let read = async {
                        match txn.get(&counter_key(b"changes")).await? {
                            Some(bytes) => decode_u64(&bytes, "the change counter"),
                            None => Ok(0),
                        }
                    };
                    lift(read.await)
                })
            })
            .await?
            .value
    }

    /// Reads the change counter and drops this node's cached records when
    /// the directory moved since the last call; returns whether it moved.
    pub async fn sync(&self) -> Result<bool, LiveError> {
        let version = self.version().await?;
        let mut cache = self.cache();
        if cache.version == Some(version) {
            return Ok(false);
        }
        cache.version = Some(version);
        cache.by_name.clear();
        Ok(true)
    }

    /// [`janitor_at`](Self::janitor_at) at the store's clock.
    pub async fn janitor_once(&self) -> Result<DirectoryReport, LiveError> {
        let now = self.now_ms().await?;
        self.janitor_at(now).await
    }

    /// Purges every deleting app whose retention ended by `now_ms` (its
    /// data deleted, its keyspace archived, its record and name removed),
    /// and reports the keyspaces of intents older than the orphan age.
    pub async fn janitor_at(&self, now_ms: u64) -> Result<DirectoryReport, LiveError> {
        let mut report = DirectoryReport::default();
        let mut after = None;
        loop {
            let page = self.list(after, MAX_PAGE).await?;
            for record in &page.apps {
                if let AppState::Deleting { purge_after_ms } = record.state
                    && purge_after_ms <= now_ms
                {
                    self.purge(record).await?;
                    report.purged.push(record.clone());
                }
            }
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        let orphan_after =
            u64::try_from(self.inner.options.orphan_after.as_millis()).unwrap_or(u64::MAX);
        let intents = self
            .inner
            .store
            .run(txn_options("live.apps.intents"), |txn| {
                Box::pin(async move {
                    let read = async {
                        let pairs = txn.scan(&[INTENT], Some(&[INTENT + 1]), MAX_PAGE).await?;
                        pairs
                            .iter()
                            .map(|(_, v)| {
                                pb::AppIntent::decode_from_slice(v)
                                    .map_err(|e| LiveError::Corrupt(format!("app intent: {e}")))
                            })
                            .collect::<Result<Vec<_>, _>>()
                    };
                    lift(read.await)
                })
            })
            .await?
            .value?;
        for intent in intents {
            if intent.started_at_ms.saturating_add(orphan_after) <= now_ms {
                tracing::warn!(
                    keyspace = %intent.keyspace,
                    app = %intent.name,
                    "a Live app create stopped after ensuring its keyspace: no app owns it"
                );
                report.orphans.push(intent.keyspace);
            }
        }
        Ok(report)
    }

    /// Deletes a deleting app's data, archives its keyspace and removes its
    /// record and name.
    async fn purge(&self, record: &AppRecord) -> Result<(), LiveError> {
        let store = self.inner.keyspaces.open(&record.keyspace).await?;
        loop {
            let deleted = store
                .run(txn_options("live.apps.purge"), |txn| {
                    Box::pin(async move {
                        let pairs = txn.scan(&[], Some(&[ROOTED_TAG]), PURGE_BATCH).await?;
                        for (key, _) in &pairs {
                            txn.delete(key).await?;
                        }
                        Ok(pairs.len())
                    })
                })
                .await?
                .value;
            if deleted < PURGE_BATCH {
                break;
            }
        }
        drop(store);
        self.inner
            .keyspaces
            .set_state(&record.keyspace, KeyspaceState::Archived)
            .await?;
        let (id, name) = (record.id, record.name.clone());
        self.run("live.apps.purge_record", move |txn| {
            let name = name.clone();
            Box::pin(async move {
                let removed = async {
                    txn.delete(&app_key(id)).await?;
                    // A name reused since (impossible while the record
                    // exists) is left alone.
                    if let Some(bytes) = txn.get(&name_key(&name)).await?
                        && decode_u32(&bytes, "a name record")? == id
                    {
                        txn.delete(&name_key(&name)).await?;
                    }
                    bump(txn).await
                };
                lift(removed.await)
            })
        })
        .await?;
        tracing::info!(app = %record.name, id = record.id, keyspace = %record.keyspace, "purged a deleted Live app");
        Ok(())
    }
}

fn not_found(name: &str) -> LiveError {
    LiveError::NotFound(format!("no Live app is named {name}"))
}

async fn refuse_taken(txn: &mut Txn, name: &str) -> Result<(), LiveError> {
    if txn.get(&name_key(name)).await?.is_some() {
        return Err(LiveError::AppExists(format!(
            "a Live app is already named {name}"
        )));
    }
    Ok(())
}

async fn refuse_full(txn: &mut Txn, max_apps: u32) -> Result<(), LiveError> {
    let apps = count(txn).await?;
    if apps >= usize::try_from(max_apps).unwrap_or(usize::MAX) {
        return Err(LiveError::limit(
            "max_apps_per_cluster",
            format!("the cluster has {apps} Live apps, its limit"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_limits_only_tighten() {
        let server = Limits::default();
        let app = AppLimits {
            max_js_cpu_ms: Some(100),
            max_scanned_docs: Some(u64::MAX),
            ..AppLimits::default()
        };
        let limits = app.apply(&server);
        assert_eq!(limits.max_js_cpu, Duration::from_millis(100));
        assert_eq!(limits.max_scanned_docs, server.max_scanned_docs);
        assert_eq!(limits.max_written_docs, server.max_written_docs);
        assert!(
            AppLimits {
                max_written_docs: Some(0),
                ..AppLimits::default()
            }
            .check()
            .is_err()
        );
    }

    #[test]
    fn origins_are_scheme_host_port() {
        for ok in ["https://app.example.com", "http://localhost:5173"] {
            check_origin(ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        for bad in [
            "app.example.com",
            "https://",
            "https://*.example.com",
            "https://a.example.com/path",
            "ftp://a",
        ] {
            assert!(check_origin(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn issuers_need_https_and_an_audience() {
        let issuer = TrustedIssuer {
            issuer: "https://auth.example.com/application/o/app/".into(),
            audiences: vec!["app".into()],
            jwks_uri: None,
        };
        issuer.check().expect("a valid issuer");
        assert!(
            TrustedIssuer {
                issuer: "http://auth.example.com".into(),
                ..issuer.clone()
            }
            .check()
            .is_err()
        );
        assert!(
            TrustedIssuer {
                audiences: vec![],
                ..issuer
            }
            .check()
            .is_err()
        );
        for ok in [
            "http://127.0.0.1:9000/o/",
            "http://localhost/",
            "http://[::1]:80",
        ] {
            assert!(https_or_loopback(ok), "{ok}");
        }
        for bad in [
            "http://localhost.evil.example/",
            "http://127.0.0.1.evil.example",
            "http://localhost@evil.example/",
            "ftp://localhost/",
        ] {
            assert!(!https_or_loopback(bad), "{bad}");
        }
    }

    #[test]
    fn records_round_trip() {
        let record = AppRecord {
            id: 7,
            name: "chat".into(),
            namespace: "acme/prod".into(),
            keyspace: app_keyspace(7),
            state: AppState::Deleting { purge_after_ms: 42 },
            settings: AppSettings {
                limits: AppLimits {
                    max_result_bytes: Some(1024),
                    ..AppLimits::default()
                },
                issuers: vec![TrustedIssuer {
                    issuer: "https://id.example.com".into(),
                    audiences: vec!["chat".into()],
                    jwks_uri: Some("https://id.example.com/jwks".into()),
                }],
                allowed_origins: vec!["https://chat.example.com".into()],
                allow_anonymous: true,
            },
            cursor_key_ref: CURSOR_KEY_REF.into(),
            created_at_ms: 1,
        };
        assert_eq!(
            AppRecord::decode(&record.encode()).expect("decodes"),
            record
        );
        assert_eq!(app_keyspace(7), "loams_live_a7");
    }

    #[test]
    fn rooted_keyspaces_live_under_the_base_root() {
        let base = StoreConfig::Embedded(loams_kv::EmbeddedConfig {
            root: vec![9, 9],
            ..loams_kv::EmbeddedConfig::new("/data/live/store.redb", "loams_live_dev")
        });
        let rooted = Keyspaces::new(base.clone(), Layout::Rooted);
        assert_eq!(rooted.config("loams_live_dev").root(), &[9, 9]);
        let other = rooted.config("loams_live_a1");
        assert_eq!(other.keyspace(), "loams_live_dev");
        assert_eq!(other.root(), b"\x09\x09\xFEloams_live_a1\x00");
        let dedicated = Keyspaces::new(base, Layout::Dedicated);
        let other = dedicated.config("loams_live_a1");
        assert_eq!(other.keyspace(), "loams_live_a1");
        assert_eq!(other.root(), &[9, 9]);
    }
}
