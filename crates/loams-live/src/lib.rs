//! Loams Live, the reactive document database on TiKV (design §20; R1 plan
//! Tasks 8–13).
//!
//! This crate holds the data model of one Live app in one keyspace (Task 8):
//! [`LiveValue`] and its protobuf and index encodings, [`DocId`] and its
//! checksummed text form, tables and indexes ([`TableDef`], [`IndexDef`] and
//! the catalog operations in [`catalog`]), the key layout of §20 §4.3
//! ([`AppKeys`]), and the document operations with index maintenance in
//! [`docs`], which run inside a `loams-kv` transaction and return the
//! [`WriteRecord`]s the commit journal needs. [`Limits`] holds R1's document
//! and mutation limits. The sharded, sequenced commit journal (Task 9), its
//! [`Tailer`] and its [`Janitor`] are in [`journal`]. [`LiveTxn`], its
//! [`ReadSet`], the [`Function`] trait and the mutation and query
//! [`Runner`] (Task 10) are in [`txn`]; the built-in `_system:*` functions
//! are in [`system`], their argument shapes in [`query`]. The subscription
//! manager (Task 11), [`Subscriptions`], and its read-set index,
//! [`ReadSetIndex`], are in [`subs`] and [`readset`]. Sessions and their
//! versioned Transitions (Task 12) are in [`session`], and the connect-rust
//! sync service, [`LiveServer`], in [`service`]. Pagination cursors (LV1
//! Task 4) are in [`cursor`], and the validators shared by function
//! arguments and schemas in [`validate`]. Versioned deployments, their
//! bundle store and their activation (LV1 Task 6) are in [`deploy`], with
//! the [`AppEngine`] that serves an app's active deployment; the schema a
//! deployment declares, with its table validators (LV1 Task 8), is in
//! [`schema`], and the leased table scans that check them in [`backfill`].
//! The admin service, `LiveAdminService` (LV1 Task 10), is in [`admin`],
//! and the function logs it tails in [`logs`]. The app directory (LV1
//! Task 13) is in [`apps`], and the per-app engines a server starts on
//! demand in [`engines`].

pub mod admin;
pub mod apps;
pub mod auth;
pub mod authz;
pub mod backfill;
pub mod catalog;
mod config;
pub mod cursor;
pub mod deploy;
pub mod docs;
pub mod engines;
mod error;
pub mod forward;
pub mod ids;
pub mod journal;
pub mod keys;
mod limits;
pub mod logs;
pub mod origins;
pub mod query;
pub mod readset;
pub mod schema;
pub mod service;
pub mod session;
pub mod subs;
pub mod system;
pub mod testing;
pub mod txn;
pub mod validate;
mod value;

/// The `loams.live.v1` protobuf messages.
pub use loams_live_proto::loams::live::v1 as pb;

pub use apps::{APP_HEADER, AppRecord, AppState, Apps, Keyspaces, Layout, SYSTEM_KEYSPACE};
pub use catalog::{IndexDef, IndexSpec, TableDef, TableValidation};
pub use config::{
    DEFAULT_IDLE_AFTER, DEFAULT_JANITOR_INTERVAL, DEFAULT_JOURNAL_SHARDS, DEFAULT_LISTEN,
    Isolation, KEYSPACE_PREFIX, LiveConfig, Tenancy, check_isolation, check_listen, keyspace_of,
    store_path,
};
pub use deploy::AppEngine;
pub use docs::{Doc, IndexRange, Order, Reads, WriteRecord};
pub use engines::{AppRuntime, Engines};
pub use error::{LIVE_REASONS, LiveError, scrub};
pub use ids::{DocId, IndexId, TableId};
pub use journal::{Batch, Checkpoint, Janitor, JanitorReport, Journal, Tailer};
pub use keys::{AppKeys, KeyRange};
pub use limits::Limits;
/// The store a [`LiveConfig`] names (LV1 row T20-9).
pub use loams_kv::{Backend, EmbeddedConfig, StoreConfig};
pub use origins::{AppOrigins, ConsoleOrigin};
pub use readset::{ReadSetIndex, SubId};
pub use service::{LiveHandle, LiveRoutes, LiveServer};
pub use session::{ClientState, SessionConfig, Sessions, Version};
pub use subs::{SubKey, SubResult, SubsConfig, SubsStats, Subscriptions, Target, Tick};
pub use txn::{
    CallCtx, CallOutput, FnKind, Function, Identity, LiveTxn, LogLevel, LogLine, Mutated, Page,
    Queried, ReadSet, Runner, RunnerOptions, Usage, Visibility,
};
pub use value::{LiveValue, fields_from_proto, fields_to_proto};

/// The `format` every Live record on the store carries (`DocumentRecord`,
/// `TableDef`, `TableJobRecord`, `AppDef`, `DeploymentRecord`,
/// `IdempotencyRecord`), registry id `live.record` (UP1 Task 1).
pub const RECORD_FORMAT: u32 = 1;

/// Whether this release reads a Live record of `format`: the registry's
/// range, not only [`RECORD_FORMAT`].
pub fn record_format_readable(format: u32) -> bool {
    loams_common::formats::check_wide(loams_common::formats::LIVE_RECORD, u64::from(format)).is_ok()
}

/// The record formats this release reads, for error messages.
pub(crate) fn record_format_range() -> String {
    let spec = loams_common::formats::spec_of(loams_common::formats::LIVE_RECORD);
    format!("{}..={}", spec.min_read, spec.current)
}

/// UP1 Task 1: Live's record readers check the registry's readable range.
#[cfg(test)]
mod format_tests {
    use loams_common::formats::{self, FormatSpec};

    use super::*;

    fn document(format: u32) -> pb::DocumentRecord {
        pb::DocumentRecord {
            format,
            ..Default::default()
        }
    }

    fn id() -> DocId {
        DocId {
            table: TableId(1),
            bytes: [1; crate::ids::DOC_ID_BYTES],
        }
    }

    #[test]
    fn format_constants_match_registry() {
        let current = formats::spec_of(formats::LIVE_RECORD).current;
        assert_eq!(RECORD_FORMAT, u32::from(current));
    }

    #[test]
    fn reader_accepts_previous_version() {
        let id = id();
        assert!(Doc::from_record(id, document(RECORD_FORMAT)).is_ok());
        assert!(Doc::from_record(id, document(RECORD_FORMAT - 1)).is_err());
        let spec = formats::spec_of(formats::LIVE_RECORD);
        let spec = FormatSpec {
            min_read: spec.current - 1,
            ..spec
        };
        formats::with_override(spec, || {
            Doc::from_record(id, document(RECORD_FORMAT - 1))
                .expect("N-1 opens once min_read allows it");
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        let id = id();
        let err = Doc::from_record(id, document(RECORD_FORMAT + 1)).unwrap_err();
        assert!(
            err.to_string().contains("this release reads 1..=1"),
            "{err}"
        );
    }
}
