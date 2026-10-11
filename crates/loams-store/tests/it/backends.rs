//! The stores a backend-sensitive test runs on: in memory, through
//! `FaultyStore` (a wrapper whose listing order `Store` cannot know), and on
//! a `file://` directory under cargo's target tmpdir (not the RAM `/tmp`).

// Test setup: a failure here is a broken test environment.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use loams_store::{FaultyStore, Store};
use object_store::memory::InMemory;

/// A store and, for `file://`, the directory that must outlive it.
pub struct Backend {
    pub name: &'static str,
    pub store: Store,
    pub dir: Option<tempfile::TempDir>,
}

pub fn all() -> Vec<Backend> {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let url = format!("file://{}", dir.path().join("bucket").display());
    let file = Store::from_url(&url, [("", ""); 0]).unwrap();
    vec![
        Backend {
            name: "memory",
            store: Store::in_memory(),
            dir: None,
        },
        Backend {
            name: "faulty-memory",
            store: Store::new(Arc::new(FaultyStore::new(Arc::new(InMemory::new())))),
            dir: None,
        },
        Backend {
            name: "file",
            store: file,
            dir: Some(dir),
        },
    ]
}

/// Every regular file under `dir`, relative to it, sorted: what a `file://`
/// store left on disk, staging files included.
pub fn files_on_disk(dir: &std::path::Path) -> Vec<String> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.push(path.strip_prefix(root).unwrap().display().to_string());
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}
