//! SFTP: the sink writes to a temporary name and renames, and the source
//! polls by path. The logic runs on a local directory with SFTP v3's rename
//! semantics in every build, and against `atmoz/sftp` when the CI job
//! provides it.

mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use common::*;
use loams_connector_objects::config::{SinkConfig, SinkFormat, SourceConfig};
use loams_connector_objects::list::{Listed, ListingSourceFactory, SourceSettings};
use loams_connector_objects::sftp::{
    LocalFs, RemoteFs, SftpConfig, SftpLister, SftpSinkFactory, SshFs,
};
use loams_connector_sdk::{
    AllowAll, Budget, CloudEvent, ConnectorError, EnvelopeBuilder, EventBatch, Sink, SinkFactory,
    SinkHandle, SourceFactory, Split, StaticSecrets,
};

/// Records every operation, and fails the next rename when asked.
struct Recording {
    inner: Arc<dyn RemoteFs>,
    ops: Mutex<Vec<String>>,
    fail_rename: Mutex<bool>,
}

impl Recording {
    fn new(inner: Arc<dyn RemoteFs>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            ops: Mutex::default(),
            fail_rename: Mutex::new(false),
        })
    }
    fn log(&self, s: String) {
        self.ops.lock().expect("lock").push(s);
    }
}

#[async_trait]
impl RemoteFs for Recording {
    async fn list(&self, dir: &str, recursive: bool) -> Result<Vec<Listed>, ConnectorError> {
        self.inner.list(dir, recursive).await
    }
    async fn read(&self, path: &str) -> Result<Option<Bytes>, ConnectorError> {
        self.inner.read(path).await
    }
    async fn write(&self, path: &str, data: &[u8]) -> Result<(), ConnectorError> {
        self.log(format!("write {path}"));
        self.inner.write(path, data).await
    }
    async fn rename_new(&self, from: &str, to: &str) -> Result<bool, ConnectorError> {
        if std::mem::take(&mut *self.fail_rename.lock().expect("lock")) {
            return Err(ConnectorError::retryable("injected: connection lost"));
        }
        self.log(format!("rename {from} -> {to}"));
        self.inner.rename_new(from, to).await
    }
    async fn remove(&self, path: &str) -> Result<(), ConnectorError> {
        self.inner.remove(path).await
    }
    async fn mkdirs(&self, dir: &str) -> Result<(), ConnectorError> {
        self.inner.mkdirs(dir).await
    }
}

fn events(from: u64, n: u64) -> EventBatch {
    EventBatch::Events(
        (from..from + n)
            .map(|i| {
                CloudEvent::new(format!("e/{i}"), "/t", "t")
                    .with_data("application/json", format!("{{\"i\":{i}}}"))
            })
            .collect(),
    )
}

fn plain(h: SinkHandle) -> Box<dyn Sink> {
    match h {
        SinkHandle::Plain(s) => s,
        SinkHandle::Transactional(_) => panic!("the SFTP sink is plain"),
    }
}

async fn atomic_rename_body(fs: Arc<dyn RemoteFs>, prefix: &str) {
    let rec = Recording::new(fs);
    let cfg = SinkConfig {
        format: SinkFormat::Ndjson,
        ..SinkConfig::default()
    };
    let factory = SftpSinkFactory::new(rec.clone(), cfg.clone(), prefix, &instance());
    let mut sink = plain(factory.create().expect("sink"));
    sink.write(events(0, 10)).await.expect("write");
    sink.flush().await.expect("flush");

    // The connection drops between the upload and the rename: nothing is
    // published, and the retried flush publishes the batch once.
    sink.write(events(10, 5)).await.expect("write");
    *rec.fail_rename.lock().expect("lock") = true;
    assert!(sink.flush().await.is_err());
    let base = format!("{}/{}/fabric", prefix.trim_matches('/'), instance().name);
    let base = base.trim_start_matches('/').to_owned();
    let visible: Vec<String> = rec
        .list(&base, false)
        .await
        .expect("list")
        .into_iter()
        .map(|l| l.key)
        .filter(|k| !k.rsplit('/').next().is_some_and(|n| n.starts_with('.')))
        .collect();
    assert_eq!(
        visible.len(),
        1,
        "a partial publish is visible: {visible:?}"
    );
    sink.flush().await.expect("retried flush");

    let names: Vec<String> = {
        let mut v: Vec<String> = rec
            .list(&base, false)
            .await
            .expect("list")
            .into_iter()
            .map(|l| l.key)
            .collect();
        v.sort();
        v
    };
    assert_eq!(names.len(), 2, "no temporary file is left: {names:?}");
    assert_contiguous(&names);
    let ops = rec.ops.lock().expect("lock").clone();
    for n in &names {
        assert!(
            !ops.iter().any(|o| o == &format!("write {n}")),
            "{n} was written in place: {ops:?}"
        );
        assert!(
            ops.iter().any(|o| o.ends_with(&format!("-> {n}"))),
            "{ops:?}"
        );
    }
    let body = rec.read(&names[1]).await.expect("read").expect("exists");
    assert_eq!(
        body.split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .count(),
        5
    );

    // A second sink on the same prefix continues the sequence.
    let mut next = plain(factory.create().expect("sink"));
    next.write(events(15, 3)).await.expect("write");
    next.flush().await.expect("flush");
    let mut all: Vec<String> = rec
        .list(&base, false)
        .await
        .expect("list")
        .into_iter()
        .map(|l| l.key)
        .collect();
    all.sort();
    assert_eq!(assert_contiguous(&all), 18);
}

async fn listing_body(fs: Arc<dyn RemoteFs>, host: &str, dir: &str) {
    fs.mkdirs(&format!("{dir}/sub")).await.expect("mkdir");
    for (name, body) in [
        ("b.json", "2"),
        ("a.json", "1"),
        (".hidden", "x"),
        ("sub/c.json", "3"),
    ] {
        fs.write(&format!("{dir}/{name}"), body.as_bytes())
            .await
            .expect("write");
    }
    let cfg: SourceConfig =
        serde_json::from_value(serde_json::json!({"decode": "bytes"})).expect("cfg");
    let factory = ListingSourceFactory::new(
        Arc::new(SftpLister::new(fs.clone(), host, true)),
        SourceSettings::from_config(&cfg).expect("settings"),
        EnvelopeBuilder::new("sftp", &instance()),
    );
    let split = Split::new(dir, serde_json::json!({"prefix": dir}));
    let budget = Budget {
        max_rows: 10,
        max_bytes: 1 << 20,
    };
    let mut src = factory.create(&split).expect("source");
    src.open(&split, None).await.expect("open");
    let b = src.next(budget).await.expect("next").expect("batch");
    let EventBatch::Events(evs) = b.events else {
        panic!("events")
    };
    let subjects: Vec<&str> = evs.iter().filter_map(|e| e.subject.as_deref()).collect();
    assert_eq!(
        subjects,
        [
            format!("{dir}/a.json"),
            format!("{dir}/b.json"),
            format!("{dir}/sub/c.json")
        ]
    );
    assert!(evs[0].id.starts_with(&format!("{host}/{dir}/a.json@")));
    assert_eq!(&evs[0].data[..], b"1");
    assert!(
        src.next(budget).await.expect("next").is_none(),
        "a bounded listing ends"
    );

    // Resuming from the position: only a new file above the high-water
    // path is read.
    fs.write(&format!("{dir}/z.json"), b"4")
        .await
        .expect("write");
    let mut again = factory.create(&split).expect("source");
    again.open(&split, Some(b.position)).await.expect("open");
    let b = again.next(budget).await.expect("next").expect("batch");
    let EventBatch::Events(evs) = b.events else {
        panic!("events")
    };
    assert_eq!(evs.len(), 1);
    assert_eq!(
        evs[0].subject.as_deref(),
        Some(format!("{dir}/z.json").as_str())
    );
}

fn real_sftp() -> Option<Arc<dyn RemoteFs>> {
    if !real(
        "LOAMS_CONNECTORS_P2_STACK",
        &[
            "LOAMS_SFTP_HOST",
            "LOAMS_SFTP_USER",
            "LOAMS_SFTP_PASSWORD",
            "LOAMS_SFTP_HOST_KEY",
        ],
        "the atmoz/sftp suite",
    ) {
        return None;
    }
    let cfg = SftpConfig::parse(
        &serde_json::json!({
            "host": env("LOAMS_SFTP_HOST"),
            "port": env("LOAMS_SFTP_PORT").parse::<u16>().unwrap_or(22),
            "username": env("LOAMS_SFTP_USER"),
            "root": env("LOAMS_SFTP_ROOT"),
            "host_key": env("LOAMS_SFTP_HOST_KEY"),
            "auth": {"password": "pw"}
        }),
        None,
    )
    .expect("config");
    let secrets = StaticSecrets::new().with("pw", env("LOAMS_SFTP_PASSWORD"));
    Some(Arc::new(
        SshFs::new(&cfg, Arc::new(secrets), Arc::new(AllowAll)).expect("ssh fs"),
    ))
}

#[tokio::test]
async fn sftp_sink_atomic_rename() {
    let dir = tempfile::tempdir().expect("tempdir");
    atomic_rename_body(Arc::new(LocalFs::new(dir.path())), "out").await;
    if let Some(fs) = real_sftp() {
        atomic_rename_body(fs, &run_prefix("sftp-sink")).await;
    }
}

#[tokio::test]
async fn sftp_listing_source_polls_by_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    listing_body(Arc::new(LocalFs::new(dir.path())), "local", "in").await;
    if let Some(fs) = real_sftp() {
        listing_body(fs, &env("LOAMS_SFTP_HOST"), &run_prefix("sftp-list")).await;
    }
}
