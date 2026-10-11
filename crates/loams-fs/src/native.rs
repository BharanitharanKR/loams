use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use bytes::Bytes;

use crate::{Fs, FsCaps, FsError, FsMeta, FsPath, WriteMode, check_range};

/// How many times a write retries after a concurrent [`Fs::remove`] pruned
/// the directory it had just created.
const PARENT_RACE_RETRIES: usize = 8;

/// An [`Fs`] on a local directory (§36 §17.1): Kubernetes volumes, a
/// Lambda's `/tmp`, a laptop's cache directory.
///
/// `NativeFs` owns `root`. Files live under `root/files/`; a write is
/// staged as a whole file in `root/staging/` and then linked or renamed into
/// place, so no reader sees part of it. Staging files a crash left behind
/// are removed when the root is opened, so one process at a time owns a
/// root (open it once and clone the handle). With `survives_restart`, every
/// write fsyncs the file and the directories it changed before it returns.
/// Removing a file also removes the directories it leaves empty.
#[derive(Clone, Debug)]
pub struct NativeFs {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    files: PathBuf,
    staging: PathBuf,
    survives_restart: bool,
}

impl NativeFs {
    /// Opens (creating it if needed) the backend at `root`, and clears its
    /// staging directory.
    pub fn new(root: impl Into<PathBuf>, survives_restart: bool) -> Result<Self, FsError> {
        let root = root.into();
        let files = root.join("files");
        let staging = root.join("staging");
        let backend = |e: io::Error| FsError::Backend(format!("{}: {e}", root.display()));
        fs::create_dir_all(&files).map_err(backend)?;
        fs::create_dir_all(&staging).map_err(backend)?;
        for entry in fs::read_dir(&staging).map_err(backend)? {
            let entry = entry.map_err(backend)?;
            match fs::remove_file(entry.path()) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(backend(e)),
            }
        }
        if survives_restart {
            sync_dir(&root).map_err(backend)?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                files,
                staging,
                survives_restart,
            }),
        })
    }

    /// Runs `op` on the blocking pool.
    async fn blocking<T, F>(&self, op: F) -> Result<T, FsError>
    where
        T: Send + 'static,
        F: FnOnce(&Inner) -> Result<T, FsError> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || op(&inner))
            .await
            .map_err(|e| FsError::Backend(format!("blocking task failed: {e}")))?
    }
}

impl Inner {
    /// Where `path` lives on disk. The root is a directory, never a file.
    fn host(&self, path: &FsPath) -> Result<PathBuf, FsError> {
        if path.is_root() {
            return Err(FsError::Unsupported("the root is a directory, not a file"));
        }
        // An `FsPath` has no empty, `.` or `..` segment and is relative, so
        // it cannot leave `files`.
        Ok(self.files.join(path.as_str()))
    }

    /// Creates the missing directories above `host`, fsyncing the parent of
    /// each one created when writes must survive a restart.
    fn create_parents(&self, host: &Path) -> io::Result<()> {
        let Some(parent) = host.parent() else {
            return Ok(());
        };
        if parent.is_dir() {
            return Ok(());
        }
        let mut missing = Vec::new();
        let mut dir = parent;
        while !dir.is_dir() && dir != self.files {
            missing.push(dir);
            match dir.parent() {
                Some(up) => dir = up,
                None => break,
            }
        }
        for dir in missing.into_iter().rev() {
            match fs::create_dir(dir) {
                Ok(()) => {
                    if self.survives_restart
                        && let Some(up) = dir.parent()
                    {
                        sync_dir(up)?;
                    }
                }
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Writes `data` to a new staging file and returns its path.
    fn stage(&self, data: &[u8]) -> io::Result<PathBuf> {
        let staged = self
            .staging
            .join(ulid::Ulid::generate().to_string().to_lowercase());
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged)?;
        let written = file.write_all(data).and_then(|()| {
            if self.survives_restart {
                file.sync_all()
            } else {
                Ok(())
            }
        });
        if let Err(e) = written {
            let _ = fs::remove_file(&staged);
            return Err(e);
        }
        Ok(staged)
    }

    /// Runs `place` (which moves something to `host`) after creating
    /// `host`'s parents, again if a concurrent removal pruned them, and then
    /// fsyncs `host`'s directory when writes must survive a restart.
    fn place(&self, host: &Path, mut place: impl FnMut() -> io::Result<()>) -> io::Result<()> {
        let mut tries = 0;
        loop {
            self.create_parents(host)?;
            match place() {
                Err(e) if e.kind() == ErrorKind::NotFound && tries < PARENT_RACE_RETRIES => {
                    tries += 1;
                }
                Err(e) => return Err(e),
                Ok(()) => break,
            }
        }
        if self.survives_restart
            && let Some(parent) = host.parent()
        {
            sync_dir(parent)?;
        }
        Ok(())
    }

    /// Removes the directories above `host` that are now empty, up to (and
    /// not including) `files`. A directory that is not empty stops it.
    fn prune(&self, host: &Path) {
        let mut dir = host.parent();
        while let Some(d) = dir {
            if d == self.files || !d.starts_with(&self.files) || fs::remove_dir(d).is_err() {
                break;
            }
            dir = d.parent();
        }
    }
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn io_error(path: &FsPath, e: io::Error) -> FsError {
    match e.kind() {
        ErrorKind::NotFound | ErrorKind::IsADirectory | ErrorKind::NotADirectory => {
            FsError::NotFound(path.clone())
        }
        ErrorKind::AlreadyExists => FsError::AlreadyExists(path.clone()),
        _ => FsError::Backend(format!("{path}: {e}")),
    }
}

fn meta_of(meta: &fs::Metadata) -> FsMeta {
    let modified_unix_ms = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    FsMeta {
        len: meta.len(),
        modified_unix_ms,
    }
}

/// Every regular file under `dir`, as `(path, meta)`, in no order. Names
/// that are not UTF-8 or not valid `FsPath`s cannot have been written
/// through an `Fs` and are skipped.
fn walk(dir: &Path, prefix: &FsPath, out: &mut Vec<(FsPath, FsMeta)>) -> io::Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(path) = prefix.join(&name) else {
            continue;
        };
        // A file removed since the listing began is simply not listed.
        let meta = match fs::symlink_metadata(entry.path()) {
            Ok(meta) => meta,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        if meta.is_dir() {
            walk(&entry.path(), &path, out)?;
        } else if meta.is_file() {
            out.push((path, meta_of(&meta)));
        }
    }
    Ok(())
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl Fs for NativeFs {
    fn caps(&self) -> FsCaps {
        FsCaps {
            append: true,
            atomic_rename: true,
            max_file: u64::MAX,
            survives_restart: self.inner.survives_restart,
        }
    }

    async fn read(&self, path: &FsPath) -> Result<Bytes, FsError> {
        let path = path.clone();
        self.blocking(move |inner| {
            let host = inner
                .host(&path)
                .map_err(|_| FsError::NotFound(path.clone()))?;
            fs::read(host)
                .map(Bytes::from)
                .map_err(|e| io_error(&path, e))
        })
        .await
    }

    async fn read_range(&self, path: &FsPath, range: Range<u64>) -> Result<Bytes, FsError> {
        let path = path.clone();
        self.blocking(move |inner| {
            let host = inner
                .host(&path)
                .map_err(|_| FsError::NotFound(path.clone()))?;
            let mut file = File::open(host).map_err(|e| io_error(&path, e))?;
            let meta = file.metadata().map_err(|e| io_error(&path, e))?;
            if !meta.is_file() {
                return Err(FsError::NotFound(path));
            }
            check_range(&path, &range, meta.len())?;
            let want = usize::try_from(range.end - range.start)
                .map_err(|_| FsError::TooLarge(range.end - range.start))?;
            let mut buf = vec![0; want];
            file.seek(SeekFrom::Start(range.start))
                .and_then(|_| file.read_exact(&mut buf))
                .map_err(|e| io_error(&path, e))?;
            Ok(Bytes::from(buf))
        })
        .await
    }

    async fn write(&self, path: &FsPath, data: Bytes, mode: WriteMode) -> Result<(), FsError> {
        let path = path.clone();
        self.blocking(move |inner| {
            let host = inner.host(&path)?;
            let staged = inner.stage(&data).map_err(|e| io_error(&path, e))?;
            let placed = match mode {
                // rename(2) replaces the target atomically.
                WriteMode::Overwrite => inner.place(&host, || fs::rename(&staged, &host)),
                // link(2) fails if the target exists, and the target appears
                // with all of its bytes.
                WriteMode::CreateNew => inner.place(&host, || fs::hard_link(&staged, &host)),
            };
            if mode == WriteMode::CreateNew || placed.is_err() {
                let _ = fs::remove_file(&staged);
            }
            placed.map_err(|e| io_error(&path, e))
        })
        .await
    }

    async fn append(&self, path: &FsPath, data: Bytes) -> Result<u64, FsError> {
        let path = path.clone();
        self.blocking(move |inner| {
            let host = inner.host(&path)?;
            let mut opened = None;
            inner
                .place(&host, || {
                    opened = Some(OpenOptions::new().create(true).append(true).open(&host)?);
                    Ok(())
                })
                .map_err(|e| io_error(&path, e))?;
            let Some(mut file) = opened else {
                return Err(FsError::Backend(format!("{path}: append opened nothing")));
            };
            file.write_all(&data).map_err(|e| io_error(&path, e))?;
            if inner.survives_restart {
                file.sync_data().map_err(|e| io_error(&path, e))?;
            }
            Ok(file.metadata().map_err(|e| io_error(&path, e))?.len())
        })
        .await
    }

    async fn rename(&self, from: &FsPath, to: &FsPath) -> Result<(), FsError> {
        let (from, to) = (from.clone(), to.clone());
        self.blocking(move |inner| {
            let source = inner
                .host(&from)
                .map_err(|_| FsError::NotFound(from.clone()))?;
            let target = inner.host(&to)?;
            if !source.is_file() {
                return Err(FsError::NotFound(from));
            }
            inner
                .place(&target, || fs::rename(&source, &target))
                .map_err(|e| io_error(&from, e))?;
            if inner.survives_restart
                && let Some(parent) = source.parent()
            {
                sync_dir(parent).map_err(|e| io_error(&from, e))?;
            }
            inner.prune(&source);
            Ok(())
        })
        .await
    }

    async fn remove(&self, path: &FsPath) -> Result<(), FsError> {
        let path = path.clone();
        self.blocking(move |inner| {
            let Ok(host) = inner.host(&path) else {
                return Ok(());
            };
            match fs::remove_file(&host) {
                Ok(()) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        ErrorKind::NotFound | ErrorKind::IsADirectory | ErrorKind::NotADirectory
                    ) =>
                {
                    return Ok(());
                }
                Err(e) => return Err(io_error(&path, e)),
            }
            if inner.survives_restart
                && let Some(parent) = host.parent()
            {
                sync_dir(parent).map_err(|e| io_error(&path, e))?;
            }
            inner.prune(&host);
            Ok(())
        })
        .await
    }

    async fn stat(&self, path: &FsPath) -> Result<Option<FsMeta>, FsError> {
        let path = path.clone();
        self.blocking(move |inner| {
            let Ok(host) = inner.host(&path) else {
                return Ok(None);
            };
            match fs::metadata(host) {
                Ok(meta) if meta.is_file() => Ok(Some(meta_of(&meta))),
                Ok(_) => Ok(None),
                Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
                    Ok(None)
                }
                Err(e) => Err(io_error(&path, e)),
            }
        })
        .await
    }

    async fn list(
        &self,
        dir: &FsPath,
        after: Option<&FsPath>,
        limit: usize,
    ) -> Result<Vec<(FsPath, FsMeta)>, FsError> {
        let (dir, after) = (dir.clone(), after.cloned());
        self.blocking(move |inner| {
            let mut found = Vec::new();
            if dir.is_root() {
                walk(&inner.files, &FsPath::root(), &mut found)
            } else {
                let host = inner.files.join(dir.as_str());
                match fs::symlink_metadata(&host) {
                    Ok(meta) if meta.is_file() => {
                        found.push((dir.clone(), meta_of(&meta)));
                        Ok(())
                    }
                    _ => walk(&host, &dir, &mut found),
                }
            }
            .map_err(|e| io_error(&dir, e))?;
            if let Some(after) = &after {
                found.retain(|(path, _)| path > after);
            }
            found.sort_by(|a, b| a.0.cmp(&b.0));
            found.truncate(limit);
            Ok(found)
        })
        .await
    }
}
