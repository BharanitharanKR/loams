//! The format registry. [`loams_common::formats`] is its only definition;
//! this module re-exports it and defines nothing of its own (§54 §4.1), so
//! the crates that own formats use it without depending on `loams-migrate`.

pub use loams_common::formats::*;
