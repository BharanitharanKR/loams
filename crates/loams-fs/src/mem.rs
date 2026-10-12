use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::ops::{Bound, Range};
use std::sync::{Mutex, MutexGuard, PoisonError};

use bytes::{Bytes, BytesMut};

use crate::{Fs, FsCaps, FsError, FsMeta, FsPath, WriteMode, check_range, now_unix_ms};

/// An in-memory [`Fs`]: for tests, and the conformance suite's reference.
/// Every operation is atomic under one lock.
#[derive(Debug)]
pub struct MemFs {
    max_file: u64,
    files: Mutex<BTreeMap<FsPath, MemFile>>,
}

#[derive(Clone, Debug)]
struct MemFile {
    data: Bytes,
    modified_unix_ms: i64,
}

impl MemFile {
    fn new(data: Bytes) -> Self {
        Self {
            data,
            modified_unix_ms: now_unix_ms(),
        }
    }

    fn meta(&self) -> FsMeta {
        FsMeta {
            len: self.data.len() as u64,
            modified_unix_ms: self.modified_unix_ms,
        }
    }
}

impl MemFs {
    /// An empty in-memory backend whose files hold at most `max_file` bytes.
    pub fn new(max_file: u64) -> Self {
        Self {
            max_file,
            files: Mutex::new(BTreeMap::new()),
        }
    }

    fn files(&self) -> MutexGuard<'_, BTreeMap<FsPath, MemFile>> {
        self.files.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn check_len(&self, len: u64) -> Result<(), FsError> {
        if len > self.max_file {
            return Err(FsError::TooLarge(len));
        }
        Ok(())
    }
}

/// The root names the whole backend, never a file.
fn file_path(path: &FsPath) -> Result<&FsPath, FsError> {
    if path.is_root() {
        return Err(FsError::Unsupported("the root is a directory, not a file"));
    }
    Ok(path)
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl Fs for MemFs {
    fn caps(&self) -> FsCaps {
        FsCaps {
            append: true,
            atomic_rename: true,
            max_file: self.max_file,
            survives_restart: false,
        }
    }

    async fn read(&self, path: &FsPath) -> Result<Bytes, FsError> {
        self.files()
            .get(path)
            .map(|file| file.data.clone())
            .ok_or_else(|| FsError::NotFound(path.clone()))
    }

    async fn read_range(&self, path: &FsPath, range: Range<u64>) -> Result<Bytes, FsError> {
        let data = self.read(path).await?;
        check_range(path, &range, data.len() as u64)?;
        // In range, so both ends fit in usize.
        Ok(data.slice(range.start as usize..range.end as usize))
    }

    async fn write(&self, path: &FsPath, data: Bytes, mode: WriteMode) -> Result<(), FsError> {
        let path = file_path(path)?;
        self.check_len(data.len() as u64)?;
        let mut files = self.files();
        match (files.entry(path.clone()), mode) {
            (Entry::Occupied(_), WriteMode::CreateNew) => Err(FsError::AlreadyExists(path.clone())),
            (Entry::Occupied(mut file), WriteMode::Overwrite) => {
                file.insert(MemFile::new(data));
                Ok(())
            }
            (Entry::Vacant(slot), _) => {
                slot.insert(MemFile::new(data));
                Ok(())
            }
        }
    }

    async fn append(&self, path: &FsPath, data: Bytes) -> Result<u64, FsError> {
        let path = file_path(path)?;
        let mut files = self.files();
        let old = files
            .get(path)
            .map(|file| file.data.clone())
            .unwrap_or_default();
        let len = old.len() as u64 + data.len() as u64;
        self.check_len(len)?;
        let mut joined = BytesMut::with_capacity(old.len() + data.len());
        joined.extend_from_slice(&old);
        joined.extend_from_slice(&data);
        files.insert(path.clone(), MemFile::new(joined.freeze()));
        Ok(len)
    }

    async fn rename(&self, from: &FsPath, to: &FsPath) -> Result<(), FsError> {
        let to = file_path(to)?;
        let mut files = self.files();
        let file = files
            .remove(from)
            .ok_or_else(|| FsError::NotFound(from.clone()))?;
        files.insert(to.clone(), file);
        Ok(())
    }

    async fn remove(&self, path: &FsPath) -> Result<(), FsError> {
        self.files().remove(path);
        Ok(())
    }

    async fn stat(&self, path: &FsPath) -> Result<Option<FsMeta>, FsError> {
        Ok(self.files().get(path).map(MemFile::meta))
    }

    async fn list(
        &self,
        dir: &FsPath,
        after: Option<&FsPath>,
        limit: usize,
    ) -> Result<Vec<(FsPath, FsMeta)>, FsError> {
        let files = self.files();
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        Ok(files
            .range((lower, Bound::Unbounded))
            .filter(|(path, _)| path.is_under(dir))
            .take(limit)
            .map(|(path, file)| (path.clone(), file.meta()))
            .collect())
    }
}
