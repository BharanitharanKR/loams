//! Iceberg table metadata for the Loams House's lake tables (HS1 Task 10, design
//! [§49](../../../../docs/design/49-loams-house-production.md) §5).
//!
//! The House keeps every table it writes as an Iceberg v2 table (v3 when a column
//! needs nanoseconds) in the namespace's catalog. This crate is the part of that
//! which is Iceberg itself:
//!
//! * the spec types the House builds a table from ([`spec`]: schemas, partition
//!   specs, sort orders, table metadata) and the identifiers and commit types the
//!   REST catalog protocol speaks ([`NamespaceIdent`], [`TableIdent`],
//!   [`TableRequirement`], [`TableUpdate`]), re-exported from iceberg-rust 0.10.1;
//! * [`metadata`]: a new table's first metadata, a commit applied to the current
//!   metadata (its requirements checked, its updates applied, the metadata log
//!   kept), and the names of metadata files — what the House's own catalogs (the
//!   in-memory one and the SQLite one of `--single-node`) do where a REST catalog
//!   does it server-side;
//! * [`append`]: a fast append of data files as one snapshot (HS1 Task 11's
//!   minimum, which Task 12's write path extends), and [`files`]: the live
//!   files of a snapshot, from its manifest list and manifests.
//!
//! # arrow 58 stays here (R0.3)
//!
//! iceberg 0.10.1 is built on arrow 58 while the `fabric/` workspace is on arrow 59.
//! Nothing of iceberg's Arrow API is re-exported: a signature of this crate never
//! names an Arrow type, so the two majors are linked side by side and never meet.
//! `loams-house`'s `arrow_58_stays_in_the_lake` checks the dependency graph.

pub mod append;
pub mod files;
pub mod metadata;

pub use iceberg::spec;
pub use iceberg::{
    Error as IcebergError, ErrorKind, NamespaceIdent, TableCreation, TableIdent, TableRequirement,
    TableUpdate,
};
pub use metadata::{LakeError, MetadataFile};
