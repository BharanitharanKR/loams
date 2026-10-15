//! The store contexts (UP1 Task 6): each writes beside the old data and
//! switches with one atomic step; until the switch, the old data reads
//! unchanged.

mod support;

use bytes::Bytes;
use loams_common::meta::Consistency;
use loams_migrate::cx::{HostChange, HouseCx};
use loams_store::Store as ObjectStore;
use support::Stack;
use tempfile::TempDir;

fn file_url(dir: &std::path::Path) -> String {
    format!(
        "file://{}/",
        dir.canonicalize().expect("canonical").display()
    )
}

#[tokio::test]
async fn every_store_context_writes_beside_then_switches() {
    let stack = Stack::new().await;
    let mut cx = stack.cx();
    let ns = stack.meta.create_namespace("app").await.expect("namespace");

    // Bucket: a new object, then the pointer CAS.
    let bucket_dir = TempDir::new().expect("bucket");
    cx.bucket = Some(
        ObjectStore::from_url(&file_url(bucket_dir.path()), Vec::<(String, String)>::new())
            .expect("store"),
    );
    {
        let bucket = cx.bucket_cx().expect("bucket cx");
        bucket
            .write_object_new("objs/v1", Bytes::from_static(b"one"))
            .await
            .expect("v1");
        let v1 = bucket
            .cas_pointer(ns, "head", None, "objs/v1", None)
            .await
            .expect("point at v1");
        bucket
            .write_object_new("objs/v2", Bytes::from_static(b"two"))
            .await
            .expect("v2 beside");
        // Before the switch, readers still see v1.
        let head = bucket
            .read_pointer(ns, "head")
            .await
            .expect("read")
            .expect("head");
        assert_eq!(head.value, "objs/v1");
        assert_eq!(bucket.read_object("objs/v1").await.expect("v1"), "one");
        // An object is never rewritten; the same bytes again is a resume.
        bucket
            .write_object_new("objs/v2", Bytes::from_static(b"two"))
            .await
            .expect("same bytes");
        assert_eq!(
            bucket
                .write_object_new("objs/v1", Bytes::from_static(b"other"))
                .await
                .unwrap_err()
                .code,
            "object_exists"
        );
        bucket
            .cas_pointer(ns, "head", Some(v1), "objs/v2", None)
            .await
            .expect("switch");
        let head = bucket
            .read_pointer(ns, "head")
            .await
            .expect("read")
            .expect("head");
        assert_eq!(head.value, "objs/v2");
    }

    // Live: new keys and the switch key in one transaction.
    cx.live = Some(loams_kv::testing::embedded().await);
    {
        let live = cx.live_cx().expect("live cx");
        live.txn("test.seed", |txn| {
            Box::pin(async move {
                txn.put(b"old/1", b"a".to_vec()).await?;
                txn.put(b"current", b"old".to_vec()).await
            })
        })
        .await
        .expect("seed");
        live.txn("test.migrate", |txn| {
            Box::pin(async move {
                txn.put(b"new/1", b"A".to_vec()).await?;
                txn.put(b"current", b"new".to_vec()).await
            })
        })
        .await
        .expect("switch");
        let pairs = live.scan(b"", None, 100).await.expect("scan");
        let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, [b"current".as_slice(), b"new/1", b"old/1"]);
    }

    // Durable: a copy beside, changed, then one rename.
    let durable_dir = stack.data.path().join("durable");
    std::fs::create_dir_all(&durable_dir).expect("mkdir");
    let db = durable_dir.join("default.db");
    {
        let conn = rusqlite::Connection::open(&db).expect("db");
        conn.execute_batch("CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('old');")
            .expect("seed");
    }
    {
        let durable = cx.durable_cx().expect("durable cx");
        let new = durable.copy_beside().expect("copy");
        new.execute_batch("UPDATE t SET v = 'new'; PRAGMA user_version = 2;")
            .expect("change the copy");
        drop(new);
        // Before the switch the current database is untouched.
        let current: String = durable
            .conn()
            .expect("conn")
            .query_row("SELECT v FROM t", [], |r| r.get(0))
            .expect("read");
        assert_eq!(current, "old");
        assert_eq!(durable.user_version().expect("version"), 0);
        durable.swap_file().expect("swap");
        let current: String = durable
            .conn()
            .expect("conn")
            .query_row("SELECT v FROM t", [], |r| r.get(0))
            .expect("read");
        assert_eq!(current, "new");
        assert_eq!(durable.user_version().expect("version"), 2);
        assert!(!durable.new_path().exists());
    }

    // pg-control: a new record file beside, then one rename.
    let pg_dir = TempDir::new().expect("pg");
    cx.pg_control_dir = Some(pg_dir.path().to_path_buf());
    {
        let pg = cx.pg_control_cx().expect("pg cx");
        std::fs::write(pg.records(), b"old records").expect("seed");
        pg.write_new(b"new records").expect("beside");
        assert_eq!(std::fs::read(pg.records()).expect("read"), b"old records");
        pg.swap_file().expect("swap");
        assert_eq!(std::fs::read(pg.records()).expect("read"), b"new records");
    }

    // Metastore: new keys, then one CAS; a moved pointer writes nothing.
    {
        let meta = cx.meta_cx();
        meta.write_new(ns, "m/new", "x", None)
            .await
            .expect("beside");
        let head = stack
            .meta
            .pointer(Consistency::Linearizable, ns, "head")
            .await
            .expect("read")
            .expect("head");
        assert_eq!(
            meta.switch(ns, "head", Some(head.version + 7), "objs/v3", None)
                .await
                .unwrap_err()
                .code,
            "pointer_moved"
        );
        assert_eq!(
            meta.scan_prefix(ns, "head").await.expect("scan")[0].1.value,
            "objs/v2",
            "nothing written by the refused switch"
        );
    }
}

#[test]
fn house_swap_is_atomic_rename() {
    let root = TempDir::new().expect("root");
    let dir = root.path().join("house");
    std::fs::create_dir_all(dir.join("spool")).expect("mkdir");
    std::fs::write(dir.join("catalog.sqlite"), b"old").expect("old");
    let house = HouseCx::new(dir.clone());
    assert_eq!(house.stamp().expect("stamp"), 1, "absent reads as 1");
    let sibling = house.sibling();
    std::fs::create_dir_all(&sibling).expect("sibling");
    std::fs::write(sibling.join("catalog.sqlite"), b"new").expect("new");
    HouseCx::write_stamp(&sibling, 2).expect("stamp the new version");
    // Until the switch, the current directory is the old one, unstamped.
    assert_eq!(
        std::fs::read(dir.join("catalog.sqlite")).expect("read"),
        b"old"
    );
    house.swap_dir().expect("exchange");
    assert_eq!(
        std::fs::read(dir.join("catalog.sqlite")).expect("read"),
        b"new"
    );
    assert_eq!(
        std::fs::read_to_string(dir.join("FORMAT")).expect("stamp"),
        "house.dir 2\n"
    );
    // The old version now sits at the sibling's path, whole.
    assert_eq!(
        std::fs::read(sibling.join("catalog.sqlite")).expect("old kept"),
        b"old"
    );
    assert!(sibling.join("spool").is_dir());
    house.discard_sibling().expect("discard");
    assert!(!sibling.exists());
}

#[tokio::test]
async fn meta_switch_is_one_cas() {
    let stack = Stack::new().await;
    let cx = stack.cx();
    let ns = stack.meta.create_namespace("app").await.expect("namespace");
    let meta = cx.meta_cx();
    let v1 = meta
        .switch(ns, "head", None, "a", None)
        .await
        .expect("create");
    // A retry that finds its own value succeeds.
    assert_eq!(
        meta.switch(ns, "head", None, "a", None)
            .await
            .expect("retry"),
        v1
    );
    let v2 = meta
        .switch(ns, "head", Some(v1), "b", None)
        .await
        .expect("switch");
    assert_eq!(v2, v1 + 1);
    // A stale expectation changes nothing.
    assert!(meta.switch(ns, "head", Some(v1), "c", None).await.is_err());
    let all = meta.scan_prefix(ns, "").await.expect("scan");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].1.value, "b");
}

#[test]
fn host_cx_writes_nothing_on_the_host() {
    let stack_dir = TempDir::new().expect("stack dir");
    std::fs::write(
        stack_dir.path().join(".env"),
        "LOAMS_VERSION=0.1.0-preview.2\n",
    )
    .expect("env");
    let before = support::read_dir(stack_dir.path());
    let mut host = loams_migrate::cx::HostCx::default();
    let change = HostChange::Recreate {
        service: "safekeeper1".to_string(),
        reason: "give it a volume".to_string(),
    };
    host.request(change.clone());
    host.request(change.clone());
    host.request(HostChange::Env {
        name: "LOAMS_UPGRADE_SNAPSHOT_DIR".to_string(),
        default: String::new(),
    });
    assert_eq!(
        host.changes().len(),
        2,
        "a change asked twice is listed once"
    );
    assert_eq!(host.changes()[0], change);
    assert_eq!(support::read_dir(stack_dir.path()), before);
    let json = serde_json::to_value(host.changes()).expect("json");
    assert_eq!(json[0]["kind"], "recreate");
}
