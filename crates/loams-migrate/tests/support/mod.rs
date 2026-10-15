//! Test-only migrations and a one-node metastore for the runner's tests.
//!
//! [`FileMigration`] migrates a directory the way a real migration must:
//! it copies `v1/<n>` to `v2.partial/<n>` (expand, two items per call, the
//! cursor is the next item), then renames `v2.partial` to `v2` and points
//! `CURRENT` at it (one atomic switch each). Until the switch, `CURRENT`
//! says `v1` and `v1/` is untouched.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use loams_common::meta::MetaStore;
use loams_meta::{MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock};
use loams_migrate::formats::{FormatId, REGISTRY, Store};
use loams_migrate::ledger::LEASE_TTL;
use loams_migrate::{
    Cursor, Cx, Estimate, Ledger, Migration, MigrationError, MigrationLease, StackRecord, Step,
    UpgradePath,
};
use loams_store::Store as ObjectStore;
use tempfile::TempDir;

/// A one-node embedded Raft metastore and a data directory.
pub struct Stack {
    pub meta: Arc<dyn MetaStore>,
    pub data: TempDir,
    _node: MetaNode,
    _router: Router,
    _dir: TempDir,
}

impl Stack {
    pub async fn new() -> Stack {
        let router = Router::new();
        let dir = TempDir::new().expect("temp dir");
        let config = MetaConfig::new(1, dir.path(), ObjectStore::in_memory());
        let node = MetaNode::start(config, &router).await.expect("start node");
        node.initialize(vec![1]).await.expect("initialize");
        node.wait_for_leader(Duration::from_secs(20))
            .await
            .expect("a leader");
        let client = MetaClient::new(
            node.clone(),
            vec![node.clone()],
            Arc::new(SystemClock),
            MetaClientConfig::default(),
        );
        Stack {
            meta: Arc::new(client),
            data: TempDir::new().expect("data dir"),
            _node: node,
            _router: router,
            _dir: dir,
        }
    }

    pub fn cx(&self) -> Cx {
        let mut cx = Cx::new(self.meta.clone());
        cx.data_dir = Some(self.data.path().to_path_buf());
        cx
    }

    /// Writes the stack record at `release`, as a finished run would.
    pub async fn stamp(&self, release: &str) {
        let ledger = Ledger::open(self.meta.clone()).await.expect("ledger");
        let lease = MigrationLease::acquire(self.meta.clone(), "test-stamp", LEASE_TTL)
            .await
            .expect("lease");
        let record = StackRecord::current(release, REGISTRY, SystemTime::now(), None);
        ledger.set_stack(&lease, &record).await.expect("stamp");
        lease.release().await.expect("release");
    }

    pub async fn ledger(&self) -> Ledger {
        Ledger::open(self.meta.clone()).await.expect("ledger")
    }
}

/// The release train in `tests/fixtures/paths/<name>.toml`.
pub fn path(name: &str) -> UpgradePath {
    let file = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/paths")
        .join(format!("{name}.toml"));
    UpgradePath::parse(&fs::read_to_string(file).expect("read path")).expect("parse path")
}

/// See the module documentation.
pub struct FileMigration {
    pub id: &'static str,
    pub class: loams_migrate::migration::Class,
    pub items: u32,
    /// Fail (`injected`) when about to copy this item; `u32::MAX`: never.
    pub fail_at: AtomicU32,
    /// How many items were copied, over every run.
    pub copies: AtomicU32,
}

impl FileMigration {
    pub fn new(id: &'static str, items: u32) -> Self {
        FileMigration {
            id,
            class: loams_migrate::migration::Class::Reversible,
            items,
            fail_at: AtomicU32::new(u32::MAX),
            copies: AtomicU32::new(0),
        }
    }

    pub fn point_of_no_return(mut self) -> Self {
        self.class = loams_migrate::migration::Class::PointOfNoReturn;
        self
    }

    pub fn dir(&self, cx: &Cx) -> PathBuf {
        cx.data_dir.as_ref().expect("a data dir").join(self.id)
    }

    /// Writes the old data: `v1/<n>` and `CURRENT` = `v1`.
    pub fn seed(&self, data: &Path) {
        let dir = data.join(self.id);
        fs::create_dir_all(dir.join("v1")).expect("mkdir");
        for n in 0..self.items {
            fs::write(dir.join("v1").join(n.to_string()), format!("item-{n}")).expect("write");
        }
        fs::write(dir.join("CURRENT"), "v1").expect("write");
    }

    /// What a reader of the current version sees: `CURRENT` and its files.
    pub fn read_current(&self, data: &Path) -> (String, BTreeMap<String, String>) {
        let dir = data.join(self.id);
        let current = fs::read_to_string(dir.join("CURRENT")).expect("CURRENT");
        (current.clone(), read_dir(&dir.join(current)))
    }
}

/// Every file under `dir` (relative path → contents).
pub fn read_dir(dir: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries {
        let path = entry.expect("entry").path();
        let name = path
            .file_name()
            .expect("name")
            .to_string_lossy()
            .to_string();
        if path.is_dir() {
            for (k, v) in read_dir(&path) {
                out.insert(format!("{name}/{k}"), v);
            }
        } else {
            out.insert(
                name,
                String::from_utf8_lossy(&fs::read(&path).expect("read")).into_owned(),
            );
        }
    }
    out
}

fn io(e: std::io::Error) -> MigrationError {
    MigrationError::new("io", e.to_string())
}

#[async_trait]
impl Migration for FileMigration {
    fn id(&self) -> &'static str {
        self.id
    }

    fn store(&self) -> Store {
        Store::Host
    }

    fn formats(&self) -> &'static [(FormatId, u16, u16)] {
        &[]
    }

    fn class(&self) -> loams_migrate::migration::Class {
        self.class
    }

    fn estimate(&self, _cx: &Cx) -> Result<Estimate, MigrationError> {
        Ok(Estimate {
            rows: u64::from(self.items),
            bytes: 0,
            seconds: 1,
        })
    }

    async fn apply(&self, cx: &mut Cx, cursor: Option<Cursor>) -> Result<Step, MigrationError> {
        let dir = self.dir(cx);
        if fs::read_to_string(dir.join("CURRENT")).map_err(io)? == "v2" {
            return Ok(Step::Done);
        }
        let partial = dir.join("v2.partial");
        fs::create_dir_all(&partial).map_err(io)?;
        let start = cursor.map_or(0, |c| u32::from_be_bytes(c.try_into().unwrap_or([0; 4])));
        let end = (start + 2).min(self.items);
        for n in start..end {
            if self.fail_at.load(Ordering::SeqCst) == n {
                return Err(MigrationError::new("injected", format!("item {n}")));
            }
            let old = fs::read_to_string(dir.join("v1").join(n.to_string())).map_err(io)?;
            fs::write(partial.join(n.to_string()), old.to_uppercase()).map_err(io)?;
            self.copies.fetch_add(1, Ordering::SeqCst);
        }
        if end < self.items {
            return Ok(Step::Continue(end.to_be_bytes().to_vec()));
        }
        // The switch: one rename for the data, one for the pointer.
        cx.before_switch()?;
        if !dir.join("v2").exists() {
            fs::rename(&partial, dir.join("v2")).map_err(io)?;
        }
        fs::write(dir.join("CURRENT.tmp"), "v2").map_err(io)?;
        fs::rename(dir.join("CURRENT.tmp"), dir.join("CURRENT")).map_err(io)?;
        Ok(Step::Done)
    }

    async fn verify(&self, cx: &Cx) -> Result<(), MigrationError> {
        let dir = self.dir(cx);
        let files = read_dir(&dir.join("v2"));
        let current = fs::read_to_string(dir.join("CURRENT")).map_err(io)?;
        if current == "v2" && files.len() == self.items as usize {
            Ok(())
        } else {
            Err(MigrationError::new(
                "verify",
                format!("{current}, {} files", files.len()),
            ))
        }
    }

    async fn abandon(&self, cx: &mut Cx) -> Result<(), MigrationError> {
        let partial = self.dir(cx).join("v2.partial");
        match fs::remove_dir_all(partial) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io(e)),
        }
    }
}
