//! Retention, orphan files, restore and reconciliation (HS1 Task 15, §49 §11),
//! on the House's own catalog over an in-memory bucket.
//!
//! The tables here are written the way the House's write path commits them
//! (fast appends and `replace` snapshots of `loams-house-lake`), with small
//! stand-in data files, so the tests need no chDB worker. Time is the
//! snapshots' own: commits are stamped with chosen times and the sweeps are
//! asked "as of" a later one.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use loams_house::catalog::{
    DROPPED, HouseCatalog, Lake, LakeRoot, LakeTable, LocalCatalog, MemCatalog,
};
use loams_house::ddl::PROP_ENGINE;
use loams_house::retention::{
    self, ORPHAN_AGE, RestorePoint, check_retention, expire_table, purge_dropped, reconcile,
    remove_orphans, restore_table, sweep_namespace,
};
use loams_house_lake::append::{NewDataFile, fast_append};
use loams_house_lake::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, NestedField, PrimitiveType,
    Schema, Struct, TableMetadata, Type,
};
use loams_house_lake::{TableCreation, TableIdent, files, rewrite};
use loams_store::Store;
use proptest::prelude::*;

const NS: u64 = 7;
const DAY: i64 = 24 * 3600 * 1000;

struct Fx {
    lake: Arc<Lake>,
    store: Store,
    root: LakeRoot,
}

impl Fx {
    fn mem() -> Self {
        let store = Store::in_memory();
        let root = LakeRoot::local();
        let catalog = Arc::new(MemCatalog::new(store.clone(), root.clone()));
        Self::over(catalog, store, root)
    }

    fn over(catalog: Arc<dyn HouseCatalog>, store: Store, root: LakeRoot) -> Self {
        let lake = Arc::new(Lake::new(catalog, root.clone()).with_store(store.clone()));
        Self { lake, store, root }
    }

    fn key(&self, location: &str) -> String {
        self.root
            .relative(location)
            .expect("in the store")
            .to_string()
    }

    async fn exists(&self, location: &str) -> bool {
        self.store.head(&self.key(location)).await.is_ok()
    }

    async fn put(&self, location: &str, bytes: Bytes) {
        self.store
            .put(&self.key(location), bytes)
            .await
            .expect("put");
    }

    async fn get(&self, location: &str) -> Bytes {
        self.store.get(&self.key(location)).await.expect("get").0
    }

    async fn create(&self, database: &str, name: &str) -> TableIdent {
        if database != "default" {
            self.lake
                .create_database(NS, database, None, true)
                .await
                .expect("database");
        }
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "k", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        let creation = TableCreation::builder()
            .name(name.to_string())
            .location(self.root.table_location(NS, database, name))
            .schema(schema)
            .properties(HashMap::from([(
                PROP_ENGINE.to_string(),
                "MergeTree".to_string(),
            )]))
            .build();
        self.lake
            .create_table(NS, database, creation, false)
            .await
            .expect("table")
            .expect("new")
            .ident
    }

    async fn load(&self, ident: &TableIdent) -> LakeTable {
        self.lake.catalog().load_table(ident).await.expect("load")
    }

    /// A data file written but not committed (a failed write).
    async fn upload(&self, table: &TableMetadata, rows: u64) -> NewDataFile {
        let location = format!(
            "{}/data/{}.parquet",
            table.location(),
            uuid::Uuid::new_v4().simple()
        );
        self.put(&location, Bytes::from(vec![7u8; rows as usize]))
            .await;
        NewDataFile {
            location,
            record_count: rows,
            size_bytes: rows,
            partition: Struct::empty(),
        }
    }

    /// Appends `n` files, stamped now; the new snapshot.
    async fn append(&self, ident: &TableIdent, n: u64) -> i64 {
        let at_ms = tick().await;
        let table = self.load(ident).await;
        let mut new = Vec::new();
        for i in 0..n {
            new.push(self.upload(&table.metadata, i + 1).await);
        }
        let parent = match table.metadata.current_snapshot() {
            Some(s) => Some(self.get(s.manifest_list()).await),
            None => None,
        };
        let built = fast_append(&table.metadata, parent.as_deref(), new, at_ms)
            .await
            .expect("append");
        for (location, bytes) in built.files {
            self.put(&location, bytes).await;
        }
        self.lake
            .catalog()
            .commit(ident, built.requirements, built.updates)
            .await
            .expect("commit");
        built.snapshot_id
    }

    /// The live files of a snapshot.
    async fn live(&self, metadata: &TableMetadata, snapshot_id: i64) -> Vec<String> {
        let snapshot = metadata.snapshot_by_id(snapshot_id).expect("snapshot");
        let list = self.get(snapshot.manifest_list()).await;
        let mut out = Vec::new();
        for m in files::manifests(&list, FormatVersion::V2).expect("list") {
            let bytes = self.get(&m.location).await;
            out.extend(
                files::live_files(&bytes, &m)
                    .expect("manifest")
                    .into_iter()
                    .map(|f| f.location),
            );
        }
        out.sort();
        out
    }

    /// Compacts every live file into one, stamped now, as compaction commits.
    async fn compact(&self, ident: &TableIdent) -> Option<i64> {
        let at_ms = tick().await;
        let table = self.load(ident).await;
        let current = table.metadata.current_snapshot()?;
        let live = self.live(&table.metadata, current.snapshot_id()).await;
        if live.is_empty() {
            return None;
        }
        let list = self.get(current.manifest_list()).await;
        let mut manifests = Vec::new();
        for m in rewrite::parent_manifests(&list, FormatVersion::V2).expect("list") {
            let bytes = self.get(&m.manifest_path).await;
            manifests.push((m, bytes));
        }
        let merged = self.upload(&table.metadata, 3).await;
        let data = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(merged.location)
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(table.metadata.default_partition_spec().spec_id())
            .record_count(3)
            .file_size_in_bytes(3)
            .build()
            .expect("data file");
        let removed: HashSet<String> = live.into_iter().collect();
        let built = rewrite::replace(
            &table.metadata,
            manifests,
            &removed,
            vec![data],
            Vec::new(),
            at_ms,
        )
        .await
        .expect("replace");
        for (location, bytes) in built.files {
            self.put(&location, bytes).await;
        }
        self.lake
            .catalog()
            .commit(ident, built.requirements, built.updates)
            .await
            .expect("commit");
        Some(built.snapshot_id)
    }

    /// Every file every snapshot of the table lists, and its metadata files.
    async fn all_referenced(&self, ident: &TableIdent) -> HashSet<String> {
        let table = self.load(ident).await;
        let mut out = HashSet::new();
        out.insert(table.metadata_location.clone());
        for snapshot in table.metadata.snapshots() {
            out.insert(snapshot.manifest_list().to_string());
            let list = self.get(snapshot.manifest_list()).await;
            for m in files::manifests(&list, FormatVersion::V2).expect("list") {
                out.insert(m.location.clone());
                let bytes = self.get(&m.location).await;
                for f in files::live_files(&bytes, &m).expect("manifest") {
                    out.insert(f.location);
                }
            }
        }
        out
    }

    async fn stored_under(&self, location: &str) -> HashSet<String> {
        let key = self.key(location);
        self.store
            .list(&format!("{key}/"))
            .await
            .expect("list")
            .into_iter()
            .map(|info| format!("{}/{}", self.root, info.path))
            .collect()
    }
}

fn now() -> i64 {
    retention::now_ms()
}

/// Now, a little later than the last call: commits are stamped with the wall
/// clock (Iceberg refuses a change older than the table's last one), so the
/// tests space them out and ask the sweeps about later times instead.
async fn tick() -> i64 {
    tokio::time::sleep(Duration::from_millis(15)).await;
    now()
}

fn snapshot_ms(metadata: &TableMetadata, id: i64) -> i64 {
    metadata
        .snapshot_by_id(id)
        .expect("snapshot")
        .timestamp_ms()
}

#[tokio::test]
async fn expired_snapshots_free_files_only_when_unreferenced() {
    let fx = Fx::mem();
    let t = fx.create("default", "events").await;
    // Two appends, a compaction that replaces their files, then (after the
    // cutoff the expiry is asked about) one more append.
    let s1 = fx.append(&t, 2).await;
    let s2 = fx.append(&t, 1).await;
    let before = fx.load(&t).await;
    let old_files = fx.live(&before.metadata, s2).await;
    let s3 = fx.compact(&t).await.expect("compacted");
    let cutoff = tick().await;
    let s4 = fx.append(&t, 1).await;
    let compacted = fx.load(&t).await;
    let kept_files = fx.live(&compacted.metadata, s4).await;
    let s1_list = before
        .metadata
        .snapshot_by_id(s1)
        .expect("s1")
        .manifest_list()
        .to_string();
    let lake = &fx.lake;
    let at = tick().await;
    let retention_to = |cutoff: i64| Duration::from_millis((at - cutoff) as u64);

    // A retention reaching back past the first snapshot: nothing goes.
    let table = fx.load(&t).await;
    let first = snapshot_ms(&table.metadata, s1);
    let none = expire_table(lake, &table, retention_to(first), at)
        .await
        .expect("expiry");
    assert_eq!(none.snapshots, 0);

    // s1 and s2 are older than the cutoff and expire; s3 was current at the
    // cutoff and stays (time travel to then still reads), s4 is current.
    let table = fx.load(&t).await;
    let expired = expire_table(lake, &table, retention_to(cutoff), at)
        .await
        .expect("expiry");
    assert_eq!(expired.snapshots, 2);
    let after = fx.load(&t).await;
    let mut kept: Vec<i64> = after
        .metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect();
    kept.sort_unstable();
    let mut want = vec![s3, s4];
    want.sort_unstable();
    assert_eq!(kept, want);
    // The appends' files are gone (only s1 and s2 listed them as live), as is
    // s1's manifest list; every file the kept snapshots list is still there.
    for file in &old_files {
        assert!(!fx.exists(file).await, "{file} was freed");
    }
    assert!(!fx.exists(&s1_list).await);
    for file in fx.all_referenced(&t).await {
        assert!(fx.exists(&file).await, "{file} is still referenced");
    }
    for file in &kept_files {
        assert!(fx.exists(file).await);
    }
    assert!(expired.files >= old_files.len() + 2, "{expired:?}");

    // A table's own retention property is honoured, but never under 24 h.
    let short = fx.create("default", "short").await;
    fx.append(&short, 1).await;
    fx.append(&short, 1).await;
    fx.lake
        .catalog()
        .commit(
            &short,
            Vec::new(),
            vec![loams_house_lake::TableUpdate::SetProperties {
                updates: HashMap::from([(
                    retention::RETENTION_PROPERTY.to_string(),
                    "1000".to_string(),
                )]),
            }],
        )
        .await
        .expect("property");
    let table = fx.load(&short).await;
    let early = expire_table(lake, &table, lake.retention(), now() + 23 * 3600 * 1000)
        .await
        .expect("expiry");
    assert_eq!(early.snapshots, 0, "the 24 h floor holds");
    let table = fx.load(&short).await;
    let late = expire_table(lake, &table, lake.retention(), now() + DAY + 60_000)
        .await
        .expect("expiry");
    assert_eq!(late.snapshots, 1);

    assert!(check_retention(Duration::from_secs(23 * 3600)).is_err());
}

#[tokio::test]
async fn orphans_and_purges() {
    let fx = Fx::mem();
    let t = fx.create("default", "t").await;
    fx.append(&t, 2).await;
    let table = fx.load(&t).await;
    // A write whose commit never happened.
    let lost = fx.upload(&table.metadata, 5).await;
    // Too young: kept.
    assert_eq!(
        remove_orphans(&fx.lake, &t, ORPHAN_AGE, now())
            .await
            .expect("orphans"),
        Some(0)
    );
    assert!(fx.exists(&lost.location).await);
    // Three days on: deleted; everything referenced stays.
    let later = now() + 3 * DAY + 1_000;
    assert_eq!(
        remove_orphans(&fx.lake, &t, ORPHAN_AGE, later)
            .await
            .expect("orphans"),
        Some(1)
    );
    assert!(!fx.exists(&lost.location).await);
    for file in fx.all_referenced(&t).await {
        assert!(fx.exists(&file).await, "{file}");
    }

    // A dropped table waits for UNDROP for the retention, then its files go.
    let gone = fx.create("default", "gone").await;
    fx.append(&gone, 2).await;
    let location = fx.load(&gone).await.metadata.location().to_string();
    fx.lake
        .drop_table(NS, "default", "gone", false)
        .await
        .expect("drop");
    assert_eq!(
        purge_dropped(&fx.lake, NS, fx.lake.retention(), now())
            .await
            .expect("purge"),
        0
    );
    assert!(!fx.stored_under(&location).await.is_empty());
    let swept = sweep_namespace(&fx.lake, NS, now() + 8 * DAY)
        .await
        .expect("sweep");
    assert_eq!(swept.purged_tables, 1);
    assert!(fx.stored_under(&location).await.is_empty());
    assert!(
        fx.lake
            .catalog()
            .list_tables(&Lake::dropped_namespace(NS))
            .await
            .expect("dropped")
            .is_empty()
    );
    assert!(fx.lake.undrop_table(NS, "default", "gone").await.is_err());
    // The live table came through the sweep whole.
    for file in fx.all_referenced(&t).await {
        assert!(fx.exists(&file).await, "{file}");
    }
    assert_eq!(fx.lake.namespaces().await.expect("namespaces"), vec![NS]);
}

#[derive(Debug, Clone)]
enum Step {
    Append(u64),
    Compact,
    FailedWrite,
    Restore(usize),
    Sweep,
    Wait(i64),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => (1u64..4).prop_map(Step::Append),
        2 => Just(Step::Compact),
        1 => Just(Step::FailedWrite),
        1 => (0usize..16).prop_map(Step::Restore),
        2 => Just(Step::Sweep),
        2 => (0i64..4 * DAY).prop_map(Step::Wait),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// Whatever commits, compactions, failed writes, restores and sweeps
    /// happen, in any order and at any pace, no file a retained snapshot or
    /// the current metadata names is ever deleted.
    #[test]
    fn orphan_gc_never_deletes_referenced_files(steps in prop::collection::vec(step(), 1..24)) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async move {
            let fx = Fx::mem();
            let t = fx.create("default", "p").await;
            // How far past the wall clock the sweeps are asked about.
            let mut ahead = 0i64;
            let mut snapshots: Vec<i64> = Vec::new();
            for step in steps {
                match step {
                    Step::Append(n) => {
                        snapshots.push(fx.append(&t, n).await);
                    }
                    Step::Compact => {
                        if let Some(id) = fx.compact(&t).await {
                            snapshots.push(id);
                        }
                    }
                    Step::FailedWrite => {
                        let table = fx.load(&t).await;
                        fx.upload(&table.metadata, 2).await;
                    }
                    Step::Restore(i) => {
                        let table = fx.load(&t).await;
                        let alive: Vec<i64> = snapshots
                            .iter()
                            .copied()
                            .filter(|id| table.metadata.snapshot_by_id(*id).is_some())
                            .collect();
                        if !alive.is_empty() {
                            let id = alive[i % alive.len()];
                            restore_table(
                                &fx.lake,
                                NS,
                                "default",
                                "p",
                                RestorePoint::Snapshot(id),
                                "",
                                now(),
                            )
                            .await
                            .expect("restore");
                        }
                    }
                    Step::Sweep => {
                        sweep_namespace(&fx.lake, NS, now() + ahead).await.expect("sweep");
                    }
                    Step::Wait(ms) => ahead += ms,
                }
                for file in fx.all_referenced(&t).await {
                    assert!(fx.exists(&file).await, "{file} is referenced and was deleted");
                }
            }
        });
    }
}

#[tokio::test]
async fn restore_table_to_snapshot_and_timestamp() {
    let fx = Fx::mem();
    let t = fx.create("default", "r").await;
    let s1 = fx.append(&t, 1).await;
    let s2 = fx.append(&t, 1).await;
    let s3 = fx.append(&t, 1).await;
    let made = fx.load(&t).await;
    let (t0, t2) = (
        snapshot_ms(&made.metadata, s1),
        snapshot_ms(&made.metadata, s2),
    );
    assert_eq!(fx.load(&t).await.metadata.current_snapshot_id(), Some(s3));

    let restored = restore_table(
        &fx.lake,
        NS,
        "default",
        "r",
        RestorePoint::Snapshot(s1),
        "",
        now(),
    )
    .await
    .expect("restore");
    assert_eq!(restored.snapshot_id, s1);
    assert!(!restored.repeated);
    assert!(restored.operation_id.starts_with("op-"));
    assert_eq!(fx.load(&t).await.metadata.current_snapshot_id(), Some(s1));
    let table = fx.load(&t).await;
    assert_eq!(fx.live(&table.metadata, s1).await.len(), 1);

    // By time: what was current just after s2 was made.
    let restored = restore_table(
        &fx.lake,
        NS,
        "default",
        "r",
        RestorePoint::Timestamp(t2 + 1),
        "",
        now(),
    )
    .await
    .expect("restore");
    assert_eq!(restored.snapshot_id, s2);
    assert_eq!(fx.load(&t).await.metadata.current_snapshot_id(), Some(s2));

    // A later append goes on from the restored snapshot.
    let s4 = fx.append(&t, 1).await;
    let table = fx.load(&t).await;
    assert_eq!(
        table
            .metadata
            .snapshot_by_id(s4)
            .and_then(|s| s.parent_snapshot_id()),
        Some(s2)
    );

    // Unknown snapshots and times before the first write are 36.
    let err = restore_table(
        &fx.lake,
        NS,
        "default",
        "r",
        RestorePoint::Snapshot(42),
        "",
        now(),
    )
    .await
    .expect_err("unknown");
    assert_eq!(err.code(), 36);
    let err = restore_table(
        &fx.lake,
        NS,
        "default",
        "r",
        RestorePoint::Timestamp(t0 - DAY),
        "",
        now(),
    )
    .await
    .expect_err("too early");
    assert_eq!(err.code(), 36);
    let err = restore_table(
        &fx.lake,
        NS,
        "default",
        "missing",
        RestorePoint::Snapshot(s1),
        "",
        now(),
    )
    .await
    .expect_err("no table");
    assert_eq!(err.code(), 60);
}

#[tokio::test]
async fn restore_is_idempotent_by_key() {
    let fx = Fx::mem();
    let t = fx.create("default", "i").await;
    let s1 = fx.append(&t, 1).await;
    fx.append(&t, 1).await;
    let t0 = now();
    let key = "01J9Z3Q4W5E6R7T8Y9U0I1O2P3";
    let first = restore_table(
        &fx.lake,
        NS,
        "default",
        "i",
        RestorePoint::Snapshot(s1),
        key,
        t0 + 2_000,
    )
    .await
    .expect("restore");
    // New rows land after the restore; a repeat of the call must not roll
    // them back.
    let s3 = fx.append(&t, 1).await;
    let again = restore_table(
        &fx.lake,
        NS,
        "default",
        "i",
        RestorePoint::Snapshot(s1),
        key,
        t0 + 4_000,
    )
    .await
    .expect("repeat");
    assert!(again.repeated);
    assert_eq!(again.operation_id, first.operation_id);
    assert_eq!(again.snapshot_id, s1);
    assert_eq!(
        fx.load(&t).await.metadata.current_snapshot_id(),
        Some(s3),
        "the repeat changed nothing"
    );
    // After 24 h the key is forgotten, and the next restore drops it.
    let later = restore_table(
        &fx.lake,
        NS,
        "default",
        "i",
        RestorePoint::Snapshot(s1),
        key,
        t0 + 2_000 + DAY + 1,
    )
    .await
    .expect("restore");
    assert!(!later.repeated);
    assert_ne!(later.operation_id, first.operation_id);
    let other = restore_table(
        &fx.lake,
        NS,
        "default",
        "i",
        RestorePoint::Snapshot(s3),
        "another",
        t0 + 2_000 + 3 * DAY,
    )
    .await
    .expect("restore");
    assert!(!other.repeated);
    let properties = fx.load(&t).await.metadata.properties().clone();
    assert!(!properties.contains_key(&format!("{}{key}", retention::RESTORE_KEY_PREFIX)));
    assert!(properties.contains_key(&format!("{}another", retention::RESTORE_KEY_PREFIX)));
    assert!(
        restore_table(
            &fx.lake,
            NS,
            "default",
            "i",
            RestorePoint::Snapshot(s1),
            "not a key",
            t0,
        )
        .await
        .is_err()
    );
}

/// A catalog restored to an earlier point: the bucket holds newer metadata
/// files than its pointers, and tables made since are unknown to it.
#[tokio::test]
async fn dr_register_reconciles_newer_metadata() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("house-dr-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(dir.join("bucket")).expect("bucket");
    let store = Store::from_url(
        &format!("file://{}", dir.join("bucket").display()),
        Vec::<(String, String)>::new(),
    )
    .expect("store");
    let root = LakeRoot::local();
    let open = || {
        Arc::new(
            LocalCatalog::open(&dir.join("catalog.sqlite"), store.clone(), root.clone())
                .expect("opens"),
        ) as Arc<dyn HouseCatalog>
    };
    let fx = Fx::over(open(), store.clone(), root.clone());
    let a = fx.create("default", "a").await;
    fx.create("sales", "d").await;
    fx.append(&a, 1).await;

    // The backup: the catalog's database as it is now.
    std::fs::copy(dir.join("catalog.sqlite"), dir.join("backup.sqlite")).expect("backup");

    // After it: more rows in `a`, a new table `b`, and `sales.d` dropped.
    let s2 = fx.append(&a, 2).await;
    let b = fx.create("default", "b").await;
    let sb = fx.append(&b, 1).await;
    fx.lake
        .drop_table(NS, "sales", "d", false)
        .await
        .expect("drop");

    // The catalog is restored to the backup.
    drop(fx);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(dir.join(format!("catalog.sqlite{suffix}")));
    }
    std::fs::copy(dir.join("backup.sqlite"), dir.join("catalog.sqlite")).expect("restore");
    let fx = Fx::over(open(), store, root);
    assert_ne!(fx.load(&a).await.metadata.current_snapshot_id(), Some(s2));
    assert!(
        fx.lake
            .load(NS, "default", "b")
            .await
            .expect("load")
            .is_none()
    );

    // The orphan sweep leaves a table with newer metadata alone.
    assert_eq!(
        remove_orphans(&fx.lake, &a, ORPHAN_AGE, now() + 10 * DAY)
            .await
            .expect("orphans"),
        None
    );

    let reconciled = reconcile(&fx.lake, NS).await.expect("reconcile");
    assert!(reconciled.skipped.is_empty(), "{reconciled:?}");
    assert_eq!(fx.load(&a).await.metadata.current_snapshot_id(), Some(s2));
    let b_back = fx
        .lake
        .load(NS, "default", "b")
        .await
        .expect("load")
        .expect("b is registered");
    assert_eq!(b_back.ident, b);
    assert_eq!(b_back.metadata.current_snapshot_id(), Some(sb));
    // `sales.d` was dropped after the backup: it is a dropped table again,
    // and UNDROP brings it back.
    assert!(
        fx.lake
            .load(NS, "sales", "d")
            .await
            .expect("load")
            .is_none()
    );
    let dropped = fx
        .lake
        .catalog()
        .list_tables(&Lake::dropped_namespace(NS))
        .await
        .expect("dropped");
    assert_eq!(dropped.len(), 1);
    assert_eq!(
        dropped[0].namespace().as_ref().last().map(String::as_str),
        Some(DROPPED)
    );
    fx.lake
        .undrop_table(NS, "sales", "d")
        .await
        .expect("undrop");
    assert_eq!(reconciled.updated.len(), 2, "{reconciled:?}");
    assert_eq!(reconciled.registered.len(), 1, "{reconciled:?}");

    // A second run finds nothing to do.
    let again = reconcile(&fx.lake, NS).await.expect("reconcile");
    assert!(
        again.updated.is_empty() && again.registered.is_empty(),
        "{again:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
