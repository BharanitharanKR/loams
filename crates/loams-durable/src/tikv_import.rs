//! Copies a SQLite durable store into the TiKV one (0.1.0 → 0.1.1).
//!
//! 0.1.0 kept Durable in Resonate's SQLite store (`durable/default.db` in
//! `loams-data`); from 0.1.1 the release stack keeps it in TiKV through
//! Resonate's blob backend ([`crate::tikv`], #796). The upgrade's
//! `0002-durable-tikv` migration calls [`import_sqlite`] once, before the
//! engine starts, so nothing serves from TiKV while it runs.
//!
//! The import writes the blob backend's own documents directly rather than
//! replaying requests: a replay would stamp every promise with the import's
//! time and re-dispatch every pending task, while the documents keep
//! timestamps, task versions, leases and fired callbacks as they were.
//!
//! - one [`OriginDoc`] per origin (the id before its first `:`), with its
//!   promises, tasks, unfired callbacks (on the awaited promise) and fired
//!   ones (in the awaiter task's `resumes`), and listeners;
//! - one timer object per origin at [`min_deadline`];
//! - one [`ScheduleDoc`] and one timer object per schedule.
//!
//! It first clears the target's documents, schedules and timers, so a rerun
//! (after a crash, or a rollback from the upgrade snapshot and a second
//! upgrade) starts from the SQLite file again. The SQLite file is only read.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use resonate_core::types::{PromiseState, PromiseValue, TaskState};
use resonate_server_blob::applier::KeySpace;
use resonate_server_blob::codec;
use resonate_server_blob::kernel::state::{
    OriginDoc, PromiseDoc, TaskDoc, check_invariants, min_deadline,
};
use resonate_server_blob::schedules::{SCHEDULE_FORMAT_VERSION, ScheduleDoc};
use resonate_server_blob::store::Store;

use crate::tikv::{TikvStore, connect, key_space};
use crate::{DurableError, DurableStore};

/// What an import read and wrote.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct ImportReport {
    pub origins: u64,
    pub promises: u64,
    pub tasks: u64,
    pub callbacks: u64,
    pub listeners: u64,
    pub schedules: u64,
    /// Callbacks whose awaiter is in another origin, or has no task: the
    /// blob backend cannot hold them (its callbacks live in one origin's
    /// document), so they are left out and counted here.
    pub dropped_callbacks: u64,
}

/// The SQLite store, read into the blob backend's shapes.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SqliteState {
    pub origins: BTreeMap<String, OriginDoc>,
    pub schedules: BTreeMap<String, ScheduleDoc>,
    pub report: ImportReport,
}

/// The origin of a promise id: everything before its first `:`.
fn origin_of(id: &str) -> &str {
    id.split(':').next().unwrap_or(id)
}

fn bad(what: &str, e: impl std::fmt::Display) -> DurableError {
    DurableError::Config(format!("the SQLite durable store's {what}: {e}"))
}

fn promise_state(s: &str) -> Result<PromiseState, DurableError> {
    serde_json::from_value(serde_json::Value::String(s.to_owned()))
        .map_err(|e| bad("promise state", format!("{s:?}: {e}")))
}

fn task_state(s: &str) -> Result<TaskState, DurableError> {
    serde_json::from_value(serde_json::Value::String(s.to_owned()))
        .map_err(|e| bad("task state", format!("{s:?}: {e}")))
}

/// A header map as the SQLite backend reads it (`row_to_promise`): JSON, or
/// empty when it does not parse.
fn headers<M: serde::de::DeserializeOwned + Default>(text: Option<String>) -> Option<M> {
    text.map(|h| serde_json::from_str(&h).unwrap_or_default())
}

fn tags(text: &str) -> BTreeMap<String, String> {
    serde_json::from_str(text).unwrap_or_default()
}

/// Reads the whole SQLite store at `path`, read-only.
pub fn read_sqlite(path: &Path) -> Result<SqliteState, DurableError> {
    let conn = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| bad(&path.display().to_string(), e))?;
    let mut state = SqliteState::default();

    let mut stmt = conn
        .prepare(
            "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, \
             timeout_at, created_at, settled_at, task_state, task_version, retry_timeout_at, \
             lease_timeout_at, ttl, pid FROM promises ORDER BY id",
        )
        .map_err(|e| bad("promises", e))?;
    let mut rows = stmt.query([]).map_err(|e| bad("promises", e))?;
    while let Some(row) = rows.next().map_err(|e| bad("promises", e))? {
        let get = |e: rusqlite::Error| bad("promises", e);
        let id: String = row.get(0).map_err(get)?;
        let promise = PromiseDoc {
            state: promise_state(&row.get::<_, String>(1).map_err(get)?)?,
            param: PromiseValue {
                headers: headers(row.get(2).map_err(get)?),
                data: row.get(3).map_err(get)?,
            },
            value: PromiseValue {
                headers: headers(row.get(4).map_err(get)?),
                data: row.get(5).map_err(get)?,
            },
            tags: tags(&row.get::<_, String>(6).map_err(get)?),
            timeout_at: row.get(7).map_err(get)?,
            created_at: row.get(8).map_err(get)?,
            settled_at: row.get(9).map_err(get)?,
            callbacks: Vec::new(),
            listeners: Vec::new(),
        };
        let doc = state.origins.entry(origin_of(&id).to_owned()).or_default();
        doc.clock = doc
            .clock
            .max(promise.created_at)
            .max(promise.settled_at.unwrap_or(0));
        if let Some(task) = row.get::<_, Option<String>>(10).map_err(get)? {
            doc.tasks.insert(
                id.clone(),
                TaskDoc {
                    state: task_state(&task)?,
                    version: row.get(11).map_err(get)?,
                    retry_at: row.get(12).map_err(get)?,
                    lease_at: row.get(13).map_err(get)?,
                    ttl: row.get(14).map_err(get)?,
                    pid: row.get(15).map_err(get)?,
                    resumes: BTreeSet::new(),
                },
            );
            state.report.tasks += 1;
        }
        doc.promises.insert(id, promise);
        state.report.promises += 1;
    }
    drop(rows);
    drop(stmt);

    // Callbacks in registration order (rowid): a settlement fans out to
    // awaiters in that order.
    let mut stmt = conn
        .prepare("SELECT awaited_id, awaiter_id, ready FROM callbacks ORDER BY rowid")
        .map_err(|e| bad("callbacks", e))?;
    let mut rows = stmt.query([]).map_err(|e| bad("callbacks", e))?;
    while let Some(row) = rows.next().map_err(|e| bad("callbacks", e))? {
        let get = |e: rusqlite::Error| bad("callbacks", e);
        let awaited: String = row.get(0).map_err(get)?;
        let awaiter: String = row.get(1).map_err(get)?;
        let ready: bool = row.get(2).map_err(get)?;
        let placed = if ready {
            // A fired callback is the awaiter task's pending resume.
            state
                .origins
                .get_mut(origin_of(&awaiter))
                .and_then(|doc| doc.tasks.get_mut(&awaiter))
                .map(|task| task.resumes.insert(awaited.clone()))
                .is_some()
        } else {
            // An unfired one waits on the awaited promise; the blob backend
            // keeps it there, so both must be in one origin's document.
            match state.origins.get_mut(origin_of(&awaited)) {
                Some(doc) if doc.promises.contains_key(&awaiter) => {
                    match doc.promises.get_mut(&awaited) {
                        Some(p) => {
                            if !p.callbacks.contains(&awaiter) {
                                p.callbacks.push(awaiter.clone());
                            }
                            true
                        }
                        None => false,
                    }
                }
                _ => false,
            }
        };
        if placed {
            state.report.callbacks += 1;
        } else {
            tracing::warn!(
                %awaited,
                %awaiter,
                ready,
                "durable import: a callback the TiKV store cannot hold is left out"
            );
            state.report.dropped_callbacks += 1;
        }
    }
    drop(rows);
    drop(stmt);

    let mut stmt = conn
        .prepare("SELECT promise_id, address FROM listeners ORDER BY rowid")
        .map_err(|e| bad("listeners", e))?;
    let mut rows = stmt.query([]).map_err(|e| bad("listeners", e))?;
    while let Some(row) = rows.next().map_err(|e| bad("listeners", e))? {
        let get = |e: rusqlite::Error| bad("listeners", e);
        let id: String = row.get(0).map_err(get)?;
        let address: String = row.get(1).map_err(get)?;
        if let Some(p) = state
            .origins
            .get_mut(origin_of(&id))
            .and_then(|doc| doc.promises.get_mut(&id))
            && !p.listeners.contains(&address)
        {
            p.listeners.push(address);
            state.report.listeners += 1;
        }
    }
    drop(rows);
    drop(stmt);

    let mut stmt = conn
        .prepare(
            "SELECT id, cron, promise_id, promise_timeout, promise_param_headers, \
             promise_param_data, promise_tags, created_at, next_run_at, last_run_at \
             FROM schedules ORDER BY id",
        )
        .map_err(|e| bad("schedules", e))?;
    let mut rows = stmt.query([]).map_err(|e| bad("schedules", e))?;
    while let Some(row) = rows.next().map_err(|e| bad("schedules", e))? {
        let get = |e: rusqlite::Error| bad("schedules", e);
        let id: String = row.get(0).map_err(get)?;
        state.schedules.insert(
            id,
            ScheduleDoc {
                v: SCHEDULE_FORMAT_VERSION,
                cron: row.get(1).map_err(get)?,
                promise_id: row.get(2).map_err(get)?,
                promise_timeout: row.get(3).map_err(get)?,
                promise_param_headers: headers(row.get(4).map_err(get)?),
                promise_param_data: row.get(5).map_err(get)?,
                promise_tags: tags(&row.get::<_, String>(6).map_err(get)?),
                created_at: row.get(7).map_err(get)?,
                next_run_at: row.get(8).map_err(get)?,
                last_run_at: row.get(9).map_err(get)?,
            },
        );
        state.report.schedules += 1;
    }

    for (origin, doc) in &mut state.origins {
        doc.timer_at = min_deadline(doc);
        check_invariants(doc).map_err(|e| bad(&format!("origin {origin}"), e))?;
    }
    state.report.origins = state.origins.len() as u64;
    Ok(state)
}

/// The blob prefixes an import owns: documents, schedules and timers.
fn owned_prefixes(keys: &KeySpace) -> [String; 3] {
    [
        format!("{}/", keys.doc_prefix()),
        format!("{}/", keys.sched_prefix()),
        format!("{}/", keys.timer_prefix()),
    ]
}

/// Deletes every document, schedule and timer under `keys` in `store`.
pub async fn clear(store: &dyn Store, keys: &KeySpace) -> Result<(), DurableError> {
    for prefix in owned_prefixes(keys) {
        store
            .delete_prefix(&prefix)
            .await
            .map_err(|e| DurableError::Start(format!("clearing {prefix}: {e}")))?;
    }
    Ok(())
}

/// Replaces `store`'s durable state under `keys` with `state`.
pub async fn write(
    store: &dyn Store,
    keys: &KeySpace,
    state: &SqliteState,
) -> Result<ImportReport, DurableError> {
    let fail = |what: &str, e: &dyn std::fmt::Display| {
        DurableError::Start(format!("writing the TiKV durable store's {what}: {e}"))
    };
    clear(store, keys).await?;
    for (origin, doc) in &state.origins {
        store
            .put(&keys.doc_key(origin), codec::encode(doc, origin))
            .await
            .map_err(|e| fail(&format!("origin {origin}"), &e))?;
        if let Some(at) = doc.timer_at {
            store
                .put(&keys.timer_key(origin, at), Vec::new())
                .await
                .map_err(|e| fail(&format!("timer of {origin}"), &e))?;
        }
    }
    for (id, schedule) in &state.schedules {
        let body = serde_json::to_vec(schedule).map_err(|e| fail(&format!("schedule {id}"), &e))?;
        store
            .put(&keys.sched_key(id), body)
            .await
            .map_err(|e| fail(&format!("schedule {id}"), &e))?;
        store
            .put(&keys.sched_timer_key(id, schedule.next_run_at), Vec::new())
            .await
            .map_err(|e| fail(&format!("timer of schedule {id}"), &e))?;
    }
    Ok(state.report.clone())
}

/// Reads back what [`write`] wrote and compares it with `state`: every
/// origin's document decodes to the same document, and every schedule is
/// there.
pub async fn verify(
    store: &dyn Store,
    keys: &KeySpace,
    state: &SqliteState,
) -> Result<(), DurableError> {
    let fail = |what: String| DurableError::Start(format!("durable import check: {what}"));
    for (origin, want) in &state.origins {
        let Some((bytes, _)) = store
            .get(&keys.doc_key(origin))
            .await
            .map_err(|e| fail(format!("origin {origin}: {e}")))?
        else {
            return Err(fail(format!("origin {origin} is missing")));
        };
        let got =
            codec::decode(&bytes, origin).map_err(|e| fail(format!("origin {origin}: {e:?}")))?;
        if got.promises != want.promises || got.tasks != want.tasks {
            return Err(fail(format!(
                "origin {origin} differs from the SQLite store"
            )));
        }
    }
    for id in state.schedules.keys() {
        if store
            .get(&keys.sched_key(id))
            .await
            .map_err(|e| fail(format!("schedule {id}: {e}")))?
            .is_none()
        {
            return Err(fail(format!("schedule {id} is missing")));
        }
    }
    Ok(())
}

/// The TiKV store `target` names (a [`DurableStore::Tikv`]).
async fn open(target: &DurableStore) -> Result<TikvStore, DurableError> {
    let DurableStore::Tikv { pd, keyspace, root } = target else {
        return Err(DurableError::Config(format!(
            "the durable import writes to a TiKV store, not {target}"
        )));
    };
    let tikv = connect(pd.clone(), keyspace.clone(), root.clone())
        .await
        .map_err(DurableError::Start)?;
    Ok(TikvStore::new(tikv))
}

/// Where [`import_once`] records what it imported: outside the documents,
/// schedules and timers the blob server reads.
pub const MARKER: &str = "loams/sqlite-import";

/// A digest of what the SQLite store holds: every origin's document and
/// every schedule, as the import writes them. Not of the file's bytes: a
/// checkpoint of its write-ahead log changes those and nothing it holds.
pub fn fingerprint(state: &SqliteState) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for (origin, doc) in &state.origins {
        hash.update(b"o");
        hash.update((origin.len() as u64).to_be_bytes());
        hash.update(origin.as_bytes());
        let body = codec::encode(doc, origin);
        hash.update((body.len() as u64).to_be_bytes());
        hash.update(&body);
    }
    for (id, schedule) in &state.schedules {
        hash.update(b"s");
        hash.update((id.len() as u64).to_be_bytes());
        hash.update(id.as_bytes());
        let body = serde_json::to_vec(schedule).unwrap_or_default();
        hash.update((body.len() as u64).to_be_bytes());
        hash.update(&body);
    }
    hash.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Imports the SQLite store at `sqlite` into `store` unless [`MARKER`] says
/// what it holds (by [`fingerprint`]) was imported already. A file that
/// changed since (it served again after a rollback) is imported again, and
/// replaces what the store held. Returns what it imported, or `None`.
pub async fn import_once_into(
    store: &dyn Store,
    keys: &KeySpace,
    sqlite: &Path,
) -> Result<Option<ImportReport>, DurableError> {
    let state = read_sqlite(sqlite)?;
    let fingerprint = fingerprint(&state);
    let marker = store
        .get(MARKER)
        .await
        .map_err(|e| DurableError::Start(format!("reading the durable import marker: {e}")))?;
    if let Some((bytes, _)) = marker {
        let seen: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        if seen["fingerprint"] == fingerprint.as_str() {
            return Ok(None);
        }
    }
    let report = write(store, keys, &state).await?;
    verify(store, keys, &state).await?;
    let record = serde_json::json!({
        "fingerprint": fingerprint,
        "source": sqlite.display().to_string(),
        "report": report,
    });
    store
        .put(MARKER, record.to_string().into_bytes())
        .await
        .map_err(|e| DurableError::Start(format!("writing the durable import marker: {e}")))?;
    Ok(Some(report))
}

/// [`import_once_into`] the TiKV store `target` names, as the engine does on
/// start when its data directory still has 0.1.0's SQLite store.
pub async fn import_once(
    sqlite: &Path,
    target: &DurableStore,
) -> Result<Option<ImportReport>, DurableError> {
    let store = open(target).await?;
    let report = import_once_into(&store, &key_space(), sqlite).await?;
    match &report {
        Some(report) => tracing::info!(
            from = %sqlite.display(),
            to = %target,
            origins = report.origins,
            promises = report.promises,
            tasks = report.tasks,
            schedules = report.schedules,
            dropped_callbacks = report.dropped_callbacks,
            "copied the SQLite durable store into TiKV"
        ),
        None => tracing::debug!(
            from = %sqlite.display(),
            "the SQLite durable store was copied into TiKV already"
        ),
    }
    Ok(report)
}

/// The reverse of [`write`]: every document and schedule under `keys` in
/// `store`, in the shapes [`read_sqlite`] returns.
pub async fn read_blob(store: &dyn Store, keys: &KeySpace) -> Result<SqliteState, DurableError> {
    let fail =
        |what: String| DurableError::Start(format!("reading the TiKV durable store: {what}"));
    let mut state = SqliteState::default();
    let doc_keys = store
        .list(&keys.doc_prefix(), usize::MAX)
        .await
        .map_err(|e| fail(e.to_string()))?;
    for key in doc_keys {
        let Some(origin) = keys.origin_of_doc_key(&key) else {
            continue;
        };
        let Some((bytes, _)) = store.get(&key).await.map_err(|e| fail(e.to_string()))? else {
            continue;
        };
        let doc = codec::decode(&bytes, &origin).map_err(|e| fail(format!("{key}: {e:?}")))?;
        state.report.promises += doc.promises.len() as u64;
        state.report.tasks += doc.tasks.len() as u64;
        state.report.callbacks += doc
            .promises
            .values()
            .map(|p| p.callbacks.len() as u64)
            .sum::<u64>()
            + doc
                .tasks
                .values()
                .map(|t| t.resumes.len() as u64)
                .sum::<u64>();
        state.report.listeners += doc
            .promises
            .values()
            .map(|p| p.listeners.len() as u64)
            .sum::<u64>();
        state.origins.insert(origin, doc);
    }
    let sched_keys = store
        .list(&keys.sched_prefix(), usize::MAX)
        .await
        .map_err(|e| fail(e.to_string()))?;
    for key in sched_keys {
        let Some(id) = keys.id_of_sched_key(&key) else {
            continue;
        };
        let Some((bytes, _)) = store.get(&key).await.map_err(|e| fail(e.to_string()))? else {
            continue;
        };
        let schedule: ScheduleDoc =
            serde_json::from_slice(&bytes).map_err(|e| fail(format!("{key}: {e}")))?;
        state.schedules.insert(id, schedule);
        state.report.schedules += 1;
    }
    state.report.origins = state.origins.len() as u64;
    Ok(state)
}

fn json_or_null<T: serde::Serialize>(value: Option<&T>) -> Option<String> {
    value.map(|v| serde_json::to_string(v).unwrap_or_else(|_| "{}".into()))
}

/// Replaces the rows of the SQLite store at `sqlite` with `state`, in one
/// transaction. The file must exist with Resonate's schema (0.1.0's store):
/// its `_sqlx_migrations` row stays, so 0.1.0 opens it as its own.
pub fn write_sqlite(sqlite: &Path, state: &SqliteState) -> Result<ImportReport, DurableError> {
    let mut conn = rusqlite::Connection::open_with_flags(
        sqlite,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| bad(&sqlite.display().to_string(), e))?;
    let tx = conn.transaction().map_err(|e| bad("a transaction", e))?;
    for table in ["callbacks", "listeners", "promises", "schedules"] {
        tx.execute(&format!("DELETE FROM {table}"), [])
            .map_err(|e| bad(table, e))?;
    }
    {
        let mut promise = tx
            .prepare(
                "INSERT INTO promises (id, state, param_headers, param_data, value_headers, \
                 value_data, tags, timeout_at, created_at, settled_at, task_state, task_version, \
                 retry_timeout_at, lease_timeout_at, ttl, pid) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            )
            .map_err(|e| bad("promises", e))?;
        let mut callback = tx
            .prepare("INSERT INTO callbacks (awaited_id, awaiter_id, ready) VALUES (?1, ?2, ?3)")
            .map_err(|e| bad("callbacks", e))?;
        let mut listener = tx
            .prepare("INSERT INTO listeners (promise_id, address) VALUES (?1, ?2)")
            .map_err(|e| bad("listeners", e))?;
        for doc in state.origins.values() {
            for (id, p) in &doc.promises {
                let task = doc.tasks.get(id);
                promise
                    .execute(rusqlite::params![
                        id,
                        p.state.as_str(),
                        json_or_null(p.param.headers.as_ref()),
                        p.param.data,
                        json_or_null(p.value.headers.as_ref()),
                        p.value.data,
                        serde_json::to_string(&p.tags).unwrap_or_else(|_| "{}".into()),
                        p.timeout_at,
                        p.created_at,
                        p.settled_at,
                        task.map(|t| t.state.as_str()),
                        task.map_or(0, |t| t.version),
                        task.and_then(|t| t.retry_at),
                        task.and_then(|t| t.lease_at),
                        task.and_then(|t| t.ttl),
                        task.and_then(|t| t.pid.clone()),
                    ])
                    .map_err(|e| bad(&format!("promise {id}"), e))?;
            }
        }
        // Callbacks after every promise (they reference both ends), unfired
        // ones in registration order.
        for doc in state.origins.values() {
            for (id, p) in &doc.promises {
                for awaiter in &p.callbacks {
                    callback
                        .execute(rusqlite::params![id, awaiter, false])
                        .map_err(|e| bad(&format!("callback {id} <- {awaiter}"), e))?;
                }
                for address in &p.listeners {
                    listener
                        .execute(rusqlite::params![id, address])
                        .map_err(|e| bad(&format!("listener of {id}"), e))?;
                }
            }
            for (id, t) in &doc.tasks {
                for awaited in &t.resumes {
                    callback
                        .execute(rusqlite::params![awaited, id, true])
                        .map_err(|e| bad(&format!("callback {awaited} <- {id}"), e))?;
                }
            }
        }
        let mut schedule = tx
            .prepare(
                "INSERT INTO schedules (id, cron, promise_id, promise_timeout, \
                 promise_param_headers, promise_param_data, promise_tags, created_at, \
                 next_run_at, last_run_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )
            .map_err(|e| bad("schedules", e))?;
        for (id, s) in &state.schedules {
            schedule
                .execute(rusqlite::params![
                    id,
                    s.cron,
                    s.promise_id,
                    s.promise_timeout,
                    json_or_null(s.promise_param_headers.as_ref()),
                    s.promise_param_data,
                    serde_json::to_string(&s.promise_tags).unwrap_or_else(|_| "{}".into()),
                    s.created_at,
                    s.next_run_at,
                    s.last_run_at,
                ])
                .map_err(|e| bad(&format!("schedule {id}"), e))?;
        }
    }
    tx.commit().map_err(|e| bad("the commit", e))?;
    Ok(state.report.clone())
}

/// `loams durable export-sqlite`: copies the TiKV store `source` back into
/// the SQLite store at `sqlite`, so the release before 0.1.1 (which reads
/// only SQLite) starts on what 0.1.1 wrote (`loams upgrade rollback
/// --keep-writes`). Without the file it is created with Resonate's schema.
pub async fn export_sqlite(
    source: &DurableStore,
    sqlite: &Path,
) -> Result<ImportReport, DurableError> {
    if !sqlite.exists() {
        // 0.1.0 never served Durable here: give the file Resonate's schema
        // (what 0.1.0 would have), then fill it.
        if let Some(dir) = sqlite.parent() {
            std::fs::create_dir_all(dir).map_err(|e| bad(&dir.display().to_string(), e))?;
        }
        crate::DurableServer::migrate(DurableStore::Sqlite {
            path: sqlite.to_path_buf(),
        })
        .await?;
    }
    let store = open(source).await?;
    let state = read_blob(&store, &key_space()).await?;
    let report = write_sqlite(sqlite, &state)?;
    let back = read_sqlite(sqlite)?;
    // Promises, tasks and schedules; a document's clock, generation and timer
    // are the blob server's bookkeeping, which SQLite does not keep.
    let same = back.schedules == state.schedules
        && back.origins.len() == state.origins.len()
        && back.origins.iter().all(|(origin, doc)| {
            state
                .origins
                .get(origin)
                .is_some_and(|want| doc.promises == want.promises && doc.tasks == want.tasks)
        });
    if !same {
        return Err(DurableError::Start(
            "the SQLite store does not read back what was exported".into(),
        ));
    }
    tracing::info!(
        from = %source,
        to = %sqlite.display(),
        promises = report.promises,
        schedules = report.schedules,
        "copied the TiKV durable store back into SQLite"
    );
    Ok(report)
}
