//! SFTP (SSH 2, SFTP v3 over `russh` and `russh-sftp`): a polling source
//! and a sink that writes to a temporary name and renames (CN2 Task 13).
//!
//! The source is the family's [`ListingSource`](crate::list::ListingSource)
//! over [`SftpLister`]: files under the configured directory in path order,
//! a high-water path per split, ids `"<host>/<path>@<mtime>-<size>"`.
//!
//! The sink is a plain (at-least-once) sink: each Fabric batch becomes one
//! file, written to a hidden temporary name and published with SFTP's
//! `RENAME`, which refuses an existing destination, so a reader never sees a
//! partial file. Names follow the object sink's,
//! `<prefix>/<instance>/<split>/<first>-<last>.<ext>`.
//!
//! The server's host key must be configured (`host_key`, an OpenSSH public
//! key line): an unknown key is refused. Keys are Ed25519 or ECDSA (R13.6).

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use loams_connector_sdk::pipeline::FABRIC_SPLIT;
use loams_connector_sdk::{
    ConnectorError, EgressCheck, EventBatch, InstanceKey, SecretRef, SecretResolver, Sink, SinkAck,
    SinkFactory, SinkHandle,
};
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::config::{SinkConfig, SourceConfig, merged};
use crate::list::{Listed, Lister};
use crate::sink::{encode_file, events_to_batch, parse_range};

/// An SFTP instance's config.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SftpConfig {
    pub host: String,
    #[serde(default = "ssh_port")]
    pub port: u16,
    pub username: String,
    /// The directory the source reads and the sink writes under.
    #[serde(default)]
    pub root: String,
    /// The source descends into subdirectories.
    #[serde(default)]
    pub recursive: bool,
    /// The server's public key, as in `known_hosts` or `ssh-keyscan`
    /// output: `ssh-ed25519 AAAA…` (a host name before it is ignored).
    pub host_key: String,
    #[serde(default)]
    pub auth: SftpAuthNames,
    /// The sink's prefix under `root`.
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub prefixes: Vec<String>,
    #[serde(default)]
    pub source: SourceConfig,
    #[serde(default)]
    pub sink: SinkConfig,
    // Keys other families' configs share, accepted and unused here.
    #[serde(default)]
    pub pipeline: serde_json::Value,
    #[serde(default)]
    pub evolution: serde_json::Value,
    #[serde(default)]
    pub lossy: serde_json::Value,
}

fn ssh_port() -> u16 {
    22
}

/// Secret names for SFTP authentication: a password, or a private key
/// (OpenSSH PEM) with an optional passphrase.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SftpAuthNames {
    pub password: Option<String>,
    pub private_key: Option<String>,
    pub private_key_passphrase: Option<String>,
}

impl SftpConfig {
    pub fn parse(
        config: &serde_json::Value,
        defaults: Option<&serde_json::Value>,
    ) -> Result<Self, ConnectorError> {
        let cfg: SftpConfig = serde_json::from_value(merged(config, defaults))
            .map_err(|e| ConnectorError::fatal(format!("sftp config: {e}")))?;
        if cfg.auth.password.is_none() && cfg.auth.private_key.is_none() {
            return Err(ConnectorError::fatal(
                "sftp: auth.password or auth.private_key is required",
            ));
        }
        parse_host_key(&cfg.host_key)?;
        Ok(cfg)
    }

    pub fn source_prefixes(&self) -> Vec<String> {
        let list = if self.prefixes.is_empty() {
            vec![self.prefix.clone()]
        } else {
            self.prefixes.clone()
        };
        let mut out: Vec<String> = list
            .iter()
            .map(|p| crate::config::normalise_prefix(p))
            .collect();
        out.sort();
        out.dedup();
        out
    }
}

/// The key of a `known_hosts` or `ssh-keyscan` line, or of a bare
/// `<algorithm> <base64>` public key.
pub fn parse_host_key(line: &str) -> Result<russh::keys::PublicKey, ConnectorError> {
    let words: Vec<&str> = line.split_whitespace().collect();
    let at = words
        .iter()
        .position(|w| w.starts_with("ssh-") || w.starts_with("ecdsa-"))
        .ok_or_else(|| ConnectorError::fatal("sftp: host_key is not an OpenSSH public key"))?;
    let b64 = words
        .get(at + 1)
        .ok_or_else(|| ConnectorError::fatal("sftp: host_key has no key data"))?;
    russh::keys::parse_public_key_base64(b64)
        .map_err(|e| ConnectorError::fatal(format!("sftp: host_key: {e}")))
}

/// Join SFTP paths with `/`, ignoring empty parts.
pub fn join(parts: &[&str]) -> String {
    let joined: Vec<&str> = parts
        .iter()
        .flat_map(|p| p.split('/'))
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();
    let lead = parts.first().is_some_and(|p| p.starts_with('/'));
    format!("{}{}", if lead { "/" } else { "" }, joined.join("/"))
}

fn parent(path: &str) -> Option<&str> {
    path.rsplit_once('/')
        .map(|(p, _)| p)
        .filter(|p| !p.is_empty())
}

/// The file operations the SFTP source and sink need. Paths are relative to
/// the instance's `root`.
#[async_trait]
pub trait RemoteFs: Send + Sync {
    /// Files under `dir` (recursively when asked), with keys relative to the
    /// root. A missing directory lists nothing.
    async fn list(&self, dir: &str, recursive: bool) -> Result<Vec<Listed>, ConnectorError>;
    async fn read(&self, path: &str) -> Result<Option<Bytes>, ConnectorError>;
    /// Create or truncate `path` and write `data`.
    async fn write(&self, path: &str, data: &[u8]) -> Result<(), ConnectorError>;
    /// Rename, refusing an existing destination: `Ok(false)` if it exists.
    async fn rename_new(&self, from: &str, to: &str) -> Result<bool, ConnectorError>;
    async fn remove(&self, path: &str) -> Result<(), ConnectorError>;
    async fn mkdirs(&self, dir: &str) -> Result<(), ConnectorError>;
}

/// A local directory as a [`RemoteFs`]: the stand-in for unit tests of the
/// source and sink logic (it never counts toward a suite, OR-2), with
/// SFTP v3's rename semantics (an existing destination is refused, through
/// `link` + `unlink`).
#[derive(Debug, Clone)]
pub struct LocalFs {
    root: PathBuf,
}

impl LocalFs {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.root.join(rel.trim_start_matches('/'))
    }
}

fn io_err(what: &str, e: std::io::Error) -> ConnectorError {
    ConnectorError::retryable(format!("{what}: {e}"))
}

#[async_trait]
impl RemoteFs for LocalFs {
    async fn list(&self, dir: &str, recursive: bool) -> Result<Vec<Listed>, ConnectorError> {
        let mut out = Vec::new();
        let mut dirs = vec![dir.trim_matches('/').to_owned()];
        while let Some(d) = dirs.pop() {
            let mut rd = match tokio::fs::read_dir(self.path(&d)).await {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io_err("list", e)),
            };
            while let Some(entry) = rd.next_entry().await.map_err(|e| io_err("list", e))? {
                let name = entry.file_name().to_string_lossy().into_owned();
                let rel = join(&[&d, &name]);
                let meta = entry.metadata().await.map_err(|e| io_err("stat", e))?;
                if meta.is_dir() {
                    if recursive {
                        dirs.push(rel);
                    }
                    continue;
                }
                let mtime = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map_or(0, |d| d.as_secs());
                out.push(Listed {
                    key: rel,
                    size: meta.len(),
                    etag: Some(format!("{mtime}-{}", meta.len())),
                    last_modified: None,
                });
            }
        }
        Ok(out)
    }

    async fn read(&self, path: &str) -> Result<Option<Bytes>, ConnectorError> {
        match tokio::fs::read(self.path(path)).await {
            Ok(b) => Ok(Some(Bytes::from(b))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err("read", e)),
        }
    }

    async fn write(&self, path: &str, data: &[u8]) -> Result<(), ConnectorError> {
        tokio::fs::write(self.path(path), data)
            .await
            .map_err(|e| io_err("write", e))
    }

    async fn rename_new(&self, from: &str, to: &str) -> Result<bool, ConnectorError> {
        match tokio::fs::hard_link(self.path(from), self.path(to)).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
            Err(e) => return Err(io_err("rename", e)),
        }
        tokio::fs::remove_file(self.path(from))
            .await
            .map_err(|e| io_err("rename", e))?;
        Ok(true)
    }

    async fn remove(&self, path: &str) -> Result<(), ConnectorError> {
        match tokio::fs::remove_file(self.path(path)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err("remove", e)),
        }
    }

    async fn mkdirs(&self, dir: &str) -> Result<(), ConnectorError> {
        tokio::fs::create_dir_all(self.path(dir))
            .await
            .map_err(|e| io_err("mkdir", e))
    }
}

/// Accepts exactly the configured host key.
struct HostKeyCheck {
    expected: russh::keys::PublicKey,
}

impl russh::client::Handler for HostKeyCheck {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(match server {
            russh::keys::PublicKeyOrCertificate::PublicKey { key, .. } => {
                key.key_data() == self.expected.key_data()
            }
            russh::keys::PublicKeyOrCertificate::Certificate(_) => false,
        })
    }
}

struct Connected {
    // Dropping the handle closes the connection; it lives as long as the
    // SFTP session.
    _ssh: russh::client::Handle<HostKeyCheck>,
    sftp: russh_sftp::client::SftpSession,
}

/// A [`RemoteFs`] over SSH: connects through the SDK's `net::connect`
/// (egress-checked after DNS), on first use, and again after an error.
pub struct SshFs {
    host: String,
    port: u16,
    username: String,
    root: String,
    host_key: russh::keys::PublicKey,
    auth: SftpAuthNames,
    secrets: Arc<dyn SecretResolver>,
    egress: Arc<dyn EgressCheck>,
    conn: Mutex<Option<Arc<Connected>>>,
}

impl std::fmt::Debug for SshFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshFs")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

fn sftp_err(what: &str, e: russh_sftp::client::error::Error) -> ConnectorError {
    use russh_sftp::client::error::Error as E;
    use russh_sftp::protocol::StatusCode as S;
    match &e {
        E::Status(s) if s.status_code == S::PermissionDenied => {
            ConnectorError::auth(format!("sftp {what}: {e}"))
        }
        _ => ConnectorError::retryable(format!("sftp {what}: {e}")),
    }
}

fn is_no_such_file(e: &russh_sftp::client::error::Error) -> bool {
    matches!(e, russh_sftp::client::error::Error::Status(s)
        if s.status_code == russh_sftp::protocol::StatusCode::NoSuchFile)
}

impl SshFs {
    pub fn new(
        cfg: &SftpConfig,
        secrets: Arc<dyn SecretResolver>,
        egress: Arc<dyn EgressCheck>,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            host: cfg.host.clone(),
            port: cfg.port,
            username: cfg.username.clone(),
            root: cfg.root.clone(),
            host_key: parse_host_key(&cfg.host_key)?,
            auth: cfg.auth.clone(),
            secrets,
            egress,
            conn: Mutex::new(None),
        })
    }

    fn full(&self, rel: &str) -> String {
        join(&[&self.root, rel])
    }

    async fn secret(&self, name: &Option<String>) -> Result<Option<String>, ConnectorError> {
        let Some(name) = name else { return Ok(None) };
        let s = self.secrets.resolve(&SecretRef::new(name)).await?;
        Ok(Some(
            s.expose_str()
                .ok_or_else(|| ConnectorError::auth(format!("secret {name:?} is not UTF-8")))?
                .to_owned(),
        ))
    }

    async fn session(&self) -> Result<Arc<Connected>, ConnectorError> {
        let mut guard = self.conn.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let tcp = loams_connector_sdk::net::connect(&self.host, self.port, &*self.egress).await?;
        let config = Arc::new(russh::client::Config {
            inactivity_timeout: Some(std::time::Duration::from_secs(120)),
            ..Default::default()
        });
        let mut ssh = russh::client::connect_stream(
            config,
            tcp,
            HostKeyCheck {
                expected: self.host_key.clone(),
            },
        )
        .await
        .map_err(|e| match e {
            russh::Error::UnknownKey => ConnectorError::auth(format!(
                "sftp: {} presented an unexpected host key",
                self.host
            )),
            other => ConnectorError::retryable(format!("ssh connect: {other}")),
        })?;
        let ok = if let Some(pem) = self.secret(&self.auth.private_key).await? {
            let pass = self.secret(&self.auth.private_key_passphrase).await?;
            let key = russh::keys::decode_secret_key(&pem, pass.as_deref())
                .map_err(|_| ConnectorError::auth("sftp: the private key does not decode"))?;
            ssh.authenticate_publickey(
                &self.username,
                russh::keys::PrivateKeyWithHashAlg::new(Arc::new(key), None),
            )
            .await
            .map_err(|e| ConnectorError::retryable(format!("ssh auth: {e}")))?
            .success()
        } else if let Some(pw) = self.secret(&self.auth.password).await? {
            ssh.authenticate_password(&self.username, pw)
                .await
                .map_err(|e| ConnectorError::retryable(format!("ssh auth: {e}")))?
                .success()
        } else {
            false
        };
        if !ok {
            return Err(ConnectorError::auth(format!(
                "sftp: {} refused the credentials of {}",
                self.host, self.username
            )));
        }
        let channel = ssh
            .channel_open_session()
            .await
            .map_err(|e| ConnectorError::retryable(format!("ssh channel: {e}")))?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| ConnectorError::retryable(format!("ssh subsystem: {e}")))?;
        let sftp = russh_sftp::client::SftpSession::new(channel.into_stream())
            .await
            .map_err(|e| sftp_err("session", e))?;
        let c = Arc::new(Connected { _ssh: ssh, sftp });
        *guard = Some(c.clone());
        Ok(c)
    }

    /// Run `f` on the session; drop the session after a failure so the next
    /// call reconnects.
    async fn with<T, F, Fut>(&self, f: F) -> Result<T, ConnectorError>
    where
        F: FnOnce(Arc<Connected>) -> Fut,
        Fut: std::future::Future<Output = Result<T, ConnectorError>>,
    {
        let c = self.session().await?;
        let r = f(c).await;
        if r.as_ref().is_err_and(|e| e.class.is_transient()) {
            *self.conn.lock().await = None;
        }
        r
    }
}

#[async_trait]
impl RemoteFs for SshFs {
    async fn list(&self, dir: &str, recursive: bool) -> Result<Vec<Listed>, ConnectorError> {
        let root = self.full("");
        let start = self.full(dir);
        self.with(|c| async move {
            let mut out = Vec::new();
            let mut dirs = vec![start];
            while let Some(d) = dirs.pop() {
                let rd = match c.sftp.read_dir(d.clone()).await {
                    Ok(rd) => rd,
                    Err(e) if is_no_such_file(&e) => continue,
                    Err(e) => return Err(sftp_err("readdir", e)),
                };
                for entry in rd {
                    let name = entry.file_name();
                    if name == "." || name == ".." {
                        continue;
                    }
                    let path = join(&[&d, &name]);
                    let meta = entry.metadata();
                    if meta.is_dir() {
                        if recursive {
                            dirs.push(path);
                        }
                        continue;
                    }
                    let rel = path
                        .strip_prefix(&root)
                        .unwrap_or(&path)
                        .trim_start_matches('/')
                        .to_owned();
                    let size = meta.size.unwrap_or(0);
                    out.push(Listed {
                        key: rel,
                        size,
                        etag: Some(format!("{}-{size}", meta.mtime.unwrap_or(0))),
                        last_modified: None,
                    });
                }
            }
            Ok(out)
        })
        .await
    }

    async fn read(&self, path: &str) -> Result<Option<Bytes>, ConnectorError> {
        let p = self.full(path);
        self.with(|c| async move {
            match c.sftp.read(p).await {
                Ok(b) => Ok(Some(Bytes::from(b))),
                Err(e) if is_no_such_file(&e) => Ok(None),
                Err(e) => Err(sftp_err("read", e)),
            }
        })
        .await
    }

    async fn write(&self, path: &str, data: &[u8]) -> Result<(), ConnectorError> {
        use russh_sftp::protocol::OpenFlags;
        let p = self.full(path);
        self.with(|c| async move {
            let mut f = c
                .sftp
                .open_with_flags(
                    p,
                    OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
                )
                .await
                .map_err(|e| sftp_err("open", e))?;
            f.write_all(data)
                .await
                .map_err(|e| ConnectorError::retryable(format!("sftp write: {e}")))?;
            f.shutdown()
                .await
                .map_err(|e| ConnectorError::retryable(format!("sftp close: {e}")))?;
            Ok(())
        })
        .await
    }

    async fn rename_new(&self, from: &str, to: &str) -> Result<bool, ConnectorError> {
        let (from, to) = (self.full(from), self.full(to));
        self.with(|c| async move {
            // SFTP v3 RENAME fails when the destination exists (OpenSSH
            // answers SSH_FX_FAILURE), which is the no-overwrite publish.
            match c.sftp.rename(from, to.clone()).await {
                Ok(()) => Ok(true),
                Err(e) => match c.sftp.try_exists(to).await {
                    Ok(true) => Ok(false),
                    _ => Err(sftp_err("rename", e)),
                },
            }
        })
        .await
    }

    async fn remove(&self, path: &str) -> Result<(), ConnectorError> {
        let p = self.full(path);
        self.with(|c| async move {
            match c.sftp.remove_file(p).await {
                Ok(()) => Ok(()),
                Err(e) if is_no_such_file(&e) => Ok(()),
                Err(e) => Err(sftp_err("remove", e)),
            }
        })
        .await
    }

    async fn mkdirs(&self, dir: &str) -> Result<(), ConnectorError> {
        let full = self.full(dir);
        self.with(|c| async move {
            let lead = full.starts_with('/');
            let mut at = String::new();
            for seg in full.split('/').filter(|s| !s.is_empty()) {
                at = if at.is_empty() && !lead {
                    seg.to_owned()
                } else {
                    format!("{at}/{seg}")
                };
                if !c.sftp.try_exists(at.clone()).await.unwrap_or(false) {
                    // A concurrent creator is fine: check again on failure.
                    if let Err(e) = c.sftp.create_dir(at.clone()).await
                        && !c.sftp.try_exists(at.clone()).await.unwrap_or(false)
                    {
                        return Err(sftp_err("mkdir", e));
                    }
                }
            }
            Ok(())
        })
        .await
    }
}

/// A [`Lister`] over a [`RemoteFs`]: lists a whole directory (SFTP has no
/// ordered, offset listing), then sorts and filters by key.
pub struct SftpLister {
    fs: Arc<dyn RemoteFs>,
    host: String,
    recursive: bool,
}

impl std::fmt::Debug for SftpLister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpLister")
            .field("host", &self.host)
            .field("recursive", &self.recursive)
            .finish_non_exhaustive()
    }
}

impl SftpLister {
    pub fn new(fs: Arc<dyn RemoteFs>, host: impl Into<String>, recursive: bool) -> Self {
        Self {
            fs,
            host: host.into(),
            recursive,
        }
    }
}

#[async_trait]
impl Lister for SftpLister {
    fn bucket(&self) -> &str {
        &self.host
    }

    async fn list_after(
        &self,
        prefix: &str,
        after: Option<&str>,
    ) -> Result<BoxStream<'static, Result<Listed, ConnectorError>>, ConnectorError> {
        let mut items = self.fs.list(prefix, self.recursive).await?;
        items.retain(|l| {
            !l.key.rsplit('/').next().is_some_and(|n| n.starts_with('.'))
                && after.is_none_or(|a| l.key.as_str() > a)
        });
        items.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(futures::stream::iter(items.into_iter().map(Ok)).boxed())
    }

    async fn fetch(&self, key: &str) -> Result<Option<Bytes>, ConnectorError> {
        self.fs.read(key).await
    }
}

/// Makes the SFTP sink.
pub struct SftpSinkFactory {
    fs: Arc<dyn RemoteFs>,
    config: SinkConfig,
    base: String,
}

impl std::fmt::Debug for SftpSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpSinkFactory")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl SftpSinkFactory {
    pub fn new(
        fs: Arc<dyn RemoteFs>,
        config: SinkConfig,
        prefix: &str,
        instance: &InstanceKey,
    ) -> Self {
        Self {
            fs,
            config,
            base: join(&[prefix, &instance.name, FABRIC_SPLIT]),
        }
    }
}

impl SinkFactory for SftpSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(SftpSink {
            fs: self.fs.clone(),
            config: self.config.clone(),
            base: self.base.clone(),
            next_seq: None,
            pending: Vec::new(),
        })))
    }
}

/// One file per Fabric batch, published by rename.
pub struct SftpSink {
    fs: Arc<dyn RemoteFs>,
    config: SinkConfig,
    base: String,
    next_seq: Option<u64>,
    pending: Vec<RecordBatch>,
}

impl std::fmt::Debug for SftpSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpSink")
            .field("base", &self.base)
            .field("next_seq", &self.next_seq)
            .finish_non_exhaustive()
    }
}

impl SftpSink {
    /// The next sequence number: one after the newest published file.
    async fn next_seq(&mut self) -> Result<u64, ConnectorError> {
        if let Some(n) = self.next_seq {
            return Ok(n);
        }
        let n = self
            .fs
            .list(&self.base, false)
            .await?
            .iter()
            .filter_map(|l| l.key.rsplit('/').next().and_then(parse_range))
            .map(|(_, last)| last + 1)
            .max()
            .unwrap_or(0);
        self.next_seq = Some(n);
        Ok(n)
    }
}

#[async_trait]
impl Sink for SftpSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let bytes = batch.size_bytes() as u64;
        let rb = match batch {
            EventBatch::Events(e) if e.is_empty() => return Ok(SinkAck::default()),
            EventBatch::Events(e) => events_to_batch(&e)?,
            EventBatch::Arrow { batch, .. } => batch,
        };
        if let Some(p) = self.pending.first()
            && p.schema().fields() != rb.schema().fields()
        {
            return Err(ConnectorError::poison(
                "a batch with another schema arrived before the flush",
            ));
        }
        // Encode now, so a batch the format cannot take is refused (and
        // dead-lettered) at write time, not at the flush.
        encode_file(
            self.config.format,
            self.config.compression,
            &self.config.raw_separator,
            std::slice::from_ref(&rb),
        )?;
        let rows = rb.num_rows() as u64;
        self.pending.push(rb);
        Ok(SinkAck { rows, bytes })
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        if self.pending.is_empty() {
            return Ok(SinkAck::default());
        }
        let rows: u64 = self.pending.iter().map(|b| b.num_rows() as u64).sum();
        let first = self.next_seq().await?;
        let last = first + rows - 1;
        let body = encode_file(
            self.config.format,
            self.config.compression,
            &self.config.raw_separator,
            &self.pending,
        )?;
        let name = format!("{first:020}-{last:020}.{}", self.config.format.extension());
        let final_path = join(&[&self.base, &name]);
        let tmp = join(&[&self.base, &format!(".{name}.tmp")]);
        if let Some(dir) = parent(&final_path) {
            self.fs.mkdirs(dir).await?;
        }
        self.fs.write(&tmp, &body).await?;
        if !self.fs.rename_new(&tmp, &final_path).await? {
            // Only a second writer on this prefix (or lost state) gets here.
            let _ = self.fs.remove(&tmp).await;
            self.next_seq = None;
            return Err(ConnectorError::fatal(format!(
                "{final_path} already exists: another writer shares this prefix"
            )));
        }
        self.pending.clear();
        self.next_seq = Some(last + 1);
        Ok(SinkAck {
            rows,
            bytes: body.len() as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_join_without_doubled_slashes() {
        assert_eq!(join(&["/upload/", "a", "/b/c"]), "/upload/a/b/c");
        assert_eq!(join(&["", "a"]), "a");
        assert_eq!(parent("a/b/c.txt"), Some("a/b"));
        assert_eq!(parent("c.txt"), None);
    }

    #[test]
    fn host_keys_parse_from_keyscan_lines() {
        let line =
            "sftp ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl";
        assert!(parse_host_key(line).is_ok());
        assert!(parse_host_key("nonsense").is_err());
    }

    #[test]
    fn config_needs_credentials_and_a_host_key() {
        let base = serde_json::json!({
            "host": "sftp", "username": "loams",
            "host_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
        });
        assert!(SftpConfig::parse(&base, None).is_err());
        let mut ok = base.clone();
        ok["auth"] = serde_json::json!({"password": "sftp-password"});
        let cfg = SftpConfig::parse(&ok, None).expect("parses");
        assert_eq!(cfg.port, 22);
    }
}
