//! The registry of every persisted Loams format (design §54 §4, D924; UP1
//! Task 1).
//!
//! Each format a Loams crate writes to a store that survives a restart is
//! listed once in [`REGISTRY`] with the version it writes (`current`) and the
//! oldest version it reads (`min_read`). Readers check a found version with
//! [`check`], which accepts `min_read..=current`, so release N+1 reads release
//! N's data (Rule 1). A crate that owns a format asserts its constant against
//! the registry in a `format_constants_match_registry` test, so a bump that
//! forgets the registry fails CI.
//!
//! The registry lives here, not in `loams-migrate`, so every crate that owns a
//! format can use it without a dependency cycle; `loams-migrate` re-exports
//! it.
//!
//! Formats that carried no version before UP1 (the Raft log, pointer values,
//! the graph catalog document, Live's journal, Git's pack and head hint, the
//! House directory, the auth principal directory) are registered at version 1 with the rule "absent means
//! 1": a reader that finds no version treats it as 1, and nothing is
//! rewritten.

use std::cell::RefCell;
use std::fmt;

/// A format's registry id: `<area>.<what>`, for example
/// `collection.manifest`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FormatId(pub &'static str);

impl FormatId {
    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for FormatId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// Which store holds a format (design §54 §3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Store {
    /// The bucket's objects and mutable bucket documents (S1, S2).
    Bucket,
    /// The metastore (S3): TiKV keyspace `loams_meta` or embedded Raft.
    Meta,
    /// Loams Live's keyspace (S5).
    Live,
    /// Durable execution's SQLite store (S11).
    Durable,
    /// The House data directory (S10).
    House,
    /// `pg-control`'s and `pg-runner`'s records (S9).
    PgControl,
    /// The stack directory on the host (S13).
    Host,
}

impl Store {
    pub fn as_str(&self) -> &'static str {
        match self {
            Store::Bucket => "bucket",
            Store::Meta => "meta",
            Store::Live => "live",
            Store::Durable => "durable",
            Store::House => "house",
            Store::PgControl => "pg_control",
            Store::Host => "host",
        }
    }
}

/// One persisted format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FormatSpec {
    pub id: FormatId,
    pub store: Store,
    /// The version this release writes.
    pub current: u16,
    /// The oldest version this release reads; `current - 1` unless a release
    /// says otherwise, and `current` while no older version exists.
    pub min_read: u16,
}

impl FormatSpec {
    /// Whether `found` is in `min_read..=current`.
    pub fn reads(&self, found: u16) -> bool {
        (self.min_read..=self.current).contains(&found)
    }
}

const fn spec(id: &'static str, store: Store, current: u16, min_read: u16) -> FormatSpec {
    FormatSpec {
        id: FormatId(id),
        store,
        current,
        min_read,
    }
}

// ---- Registry ids, one constant per format ----

pub const LOG_WAL: FormatId = FormatId("log.wal");
pub const LOG_SEGMENT: FormatId = FormatId("log.segment");
pub const COLLECTION_MANIFEST: FormatId = FormatId("collection.manifest");
pub const COLLECTION_PK_DELTA: FormatId = FormatId("collection.pk_delta");
pub const COLLECTION_DEAD_LETTERS: FormatId = FormatId("collection.dead_letters");
pub const COLLECTION_RECORD: FormatId = FormatId("collection.record");
pub const COLLECTION_PK_VALUE: FormatId = FormatId("collection.pk_value");
/// Quickwit's split footer trailer (`QWFT`): written by `loams-quickwit`,
/// read there and by `loams-text` and `loams-hot`.
pub const TEXT_SPLIT_TRAILER: FormatId = FormatId("text.split_trailer");
pub const TEXT_DELETE_BITMAP: FormatId = FormatId("text.delete_bitmap");
pub const TEXT_CHECKSUMS: FormatId = FormatId("text.checksums");
pub const HOT_ARTIFACT: FormatId = FormatId("hot.artifact");
pub const HOT_OWNER: FormatId = FormatId("hot.owner");
pub const HNSW_FLAT: FormatId = FormatId("hnsw.flat");
pub const LINK_COUNTER: FormatId = FormatId("link.counter");
pub const GIT_SEGMENT: FormatId = FormatId("git.segment");
pub const GIT_CHECKPOINT: FormatId = FormatId("git.checkpoint");
pub const GIT_CATALOG_DOC: FormatId = FormatId("git.catalog_doc");
pub const GIT_PACK: FormatId = FormatId("git.pack");
pub const GIT_HEAD_HINT: FormatId = FormatId("git.head_hint");
pub const META_RAFT_SNAPSHOT: FormatId = FormatId("meta.raft_snapshot");
pub const META_RAFT_LOG: FormatId = FormatId("meta.raft_log");
pub const META_TIKV_RECORD: FormatId = FormatId("meta.tikv_record");
pub const META_POINTER: FormatId = FormatId("meta.pointer");
pub const GRAPH_MANIFEST: FormatId = FormatId("graph.manifest");
pub const GRAPH_CHANGESET: FormatId = FormatId("graph.changeset");
pub const GRAPH_PORTABLE: FormatId = FormatId("graph.portable");
pub const GRAPH_RECOVERY_STAMP: FormatId = FormatId("graph.recovery_stamp");
pub const GRAPH_CATALOG_DOC: FormatId = FormatId("graph.catalog_doc");
pub const LIVE_RECORD: FormatId = FormatId("live.record");
pub const LIVE_JOURNAL: FormatId = FormatId("live.journal");
pub const DURABLE_SQLITE: FormatId = FormatId("durable.sqlite");
pub const HOUSE_DIR: FormatId = FormatId("house.dir");
pub const HOUSE_CATALOG: FormatId = FormatId("house.catalog");
/// The auth principal directory: unversioned JSON in the bucket
/// (`system/directory/`), named by pointer `principal-directory` in the
/// `_system` namespace (`crates/loams/src/auth/directory.rs`).
pub const AUTH_DIRECTORY: FormatId = FormatId("auth.directory");
pub const PG_CONTROL_RECORD: FormatId = FormatId("pg_control.record");
pub const PG_CONTROL_COMPOSE_STATE: FormatId = FormatId("pg_control.compose_state");
pub const PG_CONTROL_SECRETS_FILE: FormatId = FormatId("pg_control.secrets_file");
pub const PG_CONTROL_CA: FormatId = FormatId("pg_control.ca");

/// Formats whose readers keep a wider window than N−1, by an earlier
/// release's decision: the Raft snapshot decoder reads versions 5–8 (M1.3).
pub const WIDER_READ: &[FormatId] = &[META_RAFT_SNAPSHOT];

/// Every persisted Loams format (UP1 Task 0, ruling T0-1).
pub static REGISTRY: &[FormatSpec] = &[
    spec(LOG_WAL.0, Store::Bucket, 1, 1),
    spec(LOG_SEGMENT.0, Store::Bucket, 1, 1),
    spec(COLLECTION_MANIFEST.0, Store::Bucket, 1, 1),
    spec(COLLECTION_PK_DELTA.0, Store::Bucket, 1, 1),
    spec(COLLECTION_DEAD_LETTERS.0, Store::Bucket, 1, 1),
    spec(COLLECTION_RECORD.0, Store::Bucket, 1, 1),
    spec(COLLECTION_PK_VALUE.0, Store::Bucket, 1, 1),
    spec(TEXT_SPLIT_TRAILER.0, Store::Bucket, 1, 1),
    spec(TEXT_DELETE_BITMAP.0, Store::Bucket, 1, 1),
    spec(TEXT_CHECKSUMS.0, Store::Bucket, 1, 1),
    spec(HOT_ARTIFACT.0, Store::Bucket, 1, 1),
    spec(HOT_OWNER.0, Store::Bucket, 1, 1),
    spec(HNSW_FLAT.0, Store::Bucket, 1, 1),
    spec(LINK_COUNTER.0, Store::Bucket, 1, 1),
    spec(GIT_SEGMENT.0, Store::Bucket, 1, 1),
    spec(GIT_CHECKPOINT.0, Store::Bucket, 1, 1),
    spec(GIT_CATALOG_DOC.0, Store::Bucket, 1, 1),
    spec(GIT_PACK.0, Store::Bucket, 1, 1),
    spec(GIT_HEAD_HINT.0, Store::Bucket, 1, 1),
    spec(META_RAFT_SNAPSHOT.0, Store::Meta, 8, 5),
    spec(META_RAFT_LOG.0, Store::Meta, 1, 1),
    spec(META_TIKV_RECORD.0, Store::Meta, 1, 1),
    spec(META_POINTER.0, Store::Meta, 1, 1),
    spec(GRAPH_MANIFEST.0, Store::Bucket, 1, 1),
    spec(GRAPH_CHANGESET.0, Store::Bucket, 1, 1),
    spec(GRAPH_PORTABLE.0, Store::Bucket, 1, 1),
    spec(GRAPH_RECOVERY_STAMP.0, Store::Bucket, 1, 1),
    spec(GRAPH_CATALOG_DOC.0, Store::Meta, 1, 1),
    spec(LIVE_RECORD.0, Store::Live, 1, 1),
    spec(LIVE_JOURNAL.0, Store::Live, 1, 1),
    spec(DURABLE_SQLITE.0, Store::Durable, 1, 1),
    spec(HOUSE_DIR.0, Store::House, 1, 1),
    spec(HOUSE_CATALOG.0, Store::House, 1, 1),
    spec(AUTH_DIRECTORY.0, Store::Bucket, 1, 1),
    spec(PG_CONTROL_RECORD.0, Store::PgControl, 1, 1),
    spec(PG_CONTROL_COMPOSE_STATE.0, Store::PgControl, 1, 1),
    spec(PG_CONTROL_SECRETS_FILE.0, Store::PgControl, 1, 1),
    spec(PG_CONTROL_CA.0, Store::PgControl, 1, 1),
];

/// A found version outside a format's readable range.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "format_unreadable: {id} version {found} is outside the readable range {min_read}..={current}"
)]
pub struct Unreadable {
    pub id: FormatId,
    pub found: u16,
    pub min_read: u16,
    pub current: u16,
}

impl Unreadable {
    /// Newer data than this binary reads (the fix is `loams upgrade`), as
    /// opposed to older data (restore a snapshot or upgrade this binary).
    pub fn is_newer(&self) -> bool {
        self.found > self.current
    }
}

thread_local! {
    static OVERRIDES: RefCell<Vec<FormatSpec>> = const { RefCell::new(Vec::new()) };
}

/// The registry's spec of `id`, honouring a [`with_override`] in force on
/// this thread. Panics on an unregistered id: that is a bug in the caller,
/// caught by its crate's `format_constants_match_registry` test.
pub fn spec_of(id: FormatId) -> FormatSpec {
    if let Some(over) = OVERRIDES.with(|o| o.borrow().iter().rev().find(|s| s.id == id).copied()) {
        return over;
    }
    *REGISTRY
        .iter()
        .find(|s| s.id == id)
        .unwrap_or_else(|| panic!("format {id} is not in loams_common::formats::REGISTRY"))
}

/// Accepts `found` when it is in `id`'s `min_read..=current`.
pub fn check(id: FormatId, found: u16) -> Result<(), Unreadable> {
    let spec = spec_of(id);
    if spec.reads(found) {
        Ok(())
    } else {
        Err(Unreadable {
            id,
            found,
            min_read: spec.min_read,
            current: spec.current,
        })
    }
}

/// [`check`] for formats whose version field is wider than `u16` on the
/// wire; a value above `u16::MAX` is never readable.
pub fn check_wide(id: FormatId, found: u64) -> Result<(), Unreadable> {
    match u16::try_from(found) {
        Ok(found) => check(id, found),
        Err(_) => {
            let spec = spec_of(id);
            Err(Unreadable {
                id,
                found: u16::MAX,
                min_read: spec.min_read,
                current: spec.current,
            })
        }
    }
}

/// Runs `f` with `spec` replacing its registry entry on this thread: tests
/// use it to check that a reader accepts `current - 1` when `min_read` allows
/// it, while every format is still at version 1.
pub fn with_override<T>(spec: FormatSpec, f: impl FnOnce() -> T) -> T {
    struct Pop;
    impl Drop for Pop {
        fn drop(&mut self) {
            OVERRIDES.with(|o| {
                o.borrow_mut().pop();
            });
        }
    }
    OVERRIDES.with(|o| o.borrow_mut().push(spec));
    let _pop = Pop;
    f()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_is_scoped_to_the_closure() {
        let s = spec(LOG_WAL.0, Store::Bucket, 2, 1);
        with_override(s, || assert!(check(LOG_WAL, 2).is_ok()));
        assert!(check(LOG_WAL, 2).is_err());
    }

    #[test]
    fn wide_versions_above_u16_are_refused() {
        assert!(check_wide(GRAPH_MANIFEST, 1).is_ok());
        assert!(check_wide(GRAPH_MANIFEST, u64::from(u16::MAX) + 1).is_err());
    }
}
