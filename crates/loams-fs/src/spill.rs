use std::sync::Arc;

use crate::{Fs, FsError, FsPath, FsPathError};

/// How many files [`SpillDir`]'s cleanup lists per round.
const CLEANUP_PAGE: usize = 1024;

/// A per-request scratch directory under an [`Fs`] root (§36 §17), such as
/// the spill buffer of a push. Its files are removed by [`SpillDir::close`],
/// or, if it is dropped unclosed, by a task spawned on the current tokio
/// runtime.
///
/// Each one is `<parent>/spill-<ulid>`, so concurrent requests never share
/// one. A crash leaves its files behind; the cache's own eviction or the
/// owner's cleanup of `parent` collects them.
#[derive(Debug)]
pub struct SpillDir {
    fs: Arc<dyn Fs>,
    dir: FsPath,
    closed: bool,
}

impl SpillDir {
    /// A new, empty spill directory under `parent`.
    pub fn new(fs: Arc<dyn Fs>, parent: &FsPath) -> Result<Self, FsPathError> {
        let name = format!(
            "spill-{}",
            ulid::Ulid::generate().to_string().to_lowercase()
        );
        Ok(Self {
            fs,
            dir: parent.join(&name)?,
            closed: false,
        })
    }

    /// The directory itself.
    pub fn dir(&self) -> &FsPath {
        &self.dir
    }

    /// The path of `name` (one or more segments) inside the directory.
    pub fn path(&self, name: &str) -> Result<FsPath, FsPathError> {
        self.dir.join(name)
    }

    /// Removes every file in the directory now.
    pub async fn close(mut self) -> Result<(), FsError> {
        self.closed = true;
        remove_all(self.fs.as_ref(), &self.dir).await
    }
}

impl Drop for SpillDir {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        let (fs, dir) = (Arc::clone(&self.fs), self.dir.clone());
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(e) = remove_all(fs.as_ref(), &dir).await {
                        tracing::warn!(dir = %dir, error = %e, "removing a dropped spill directory failed");
                    }
                });
            }
            Err(_) => {
                tracing::warn!(dir = %self.dir, "a spill directory was dropped outside a tokio runtime; its files stay");
            }
        }
    }
}

async fn remove_all(fs: &dyn Fs, dir: &FsPath) -> Result<(), FsError> {
    loop {
        let page = fs.list(dir, None, CLEANUP_PAGE).await?;
        if page.is_empty() {
            return Ok(());
        }
        for (path, _) in page {
            fs.remove(&path).await?;
        }
    }
}
