//! Object store handles: the backend-agnostic surface every Loams crate
//! writes through, and the object metadata it reads back.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, ObjectStoreScheme, PutMode, PutOptions,
    PutPayload, UpdateVersion, WriteMultipart,
};

use crate::error::{StoreError, map_err};

/// Parses `path` as an [`object_store::path::Path`] without percent-encoding it.
///
/// `Path::from` percent-encodes its input, so a literal `%` would not round-trip
/// between `put` and a path later returned by `list`. `Path::parse` treats the
/// string as already-decoded segments (rejecting `.` and `..` segments and
/// ASCII control characters), which is what every `Store` method needs.
fn parse_path(path: &str) -> Result<Path, StoreError> {
    Path::parse(path).map_err(|e| StoreError::InvalidPath(format!("{path}: {e}")))
}

/// Opaque version of an object, used for compare-and-swap writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectVersion {
    /// The backend's entity tag, when it has one.
    pub e_tag: Option<String>,
    /// The backend's own version marker, when it has one.
    pub version: Option<String>,
}

/// Metadata about a stored object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    /// The object's key, as [`Store::list`] and [`Store::head`] report it.
    pub path: String,
    /// The object's size in bytes.
    pub size: u64,
    /// The version a compare-and-swap write must present to overwrite it.
    pub version: ObjectVersion,
    /// When the object was last written, in ms since the Unix epoch, as the
    /// backend reports it.
    pub last_modified_ms: u64,
}

/// Handle to an object store. Cheap to clone.
#[derive(Clone, Debug)]
pub struct Store {
    inner: Arc<dyn ObjectStore>,
    /// Whether the backend lists keys in lexicographic order, so a paged
    /// listing can stop after its page (GT1 Task 0, T0-3).
    sorted_listing: bool,
}

impl Store {
    /// The smallest part [`Store::put_multipart`] sends, except the last: S3
    /// and the stores that copy its API refuse smaller parts.
    pub const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

    /// How many parts of one [`Store::put_multipart`] may be uploading before
    /// it reads more of the body.
    pub const PARTS_IN_FLIGHT: usize = 4;

    /// Wraps an existing object store.
    ///
    /// `object_store` promises no listing order, so the store is assumed to
    /// list in any order (see [`Store::with_sorted_listing`]).
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            sorted_listing: false,
        }
    }

    /// Declares whether the backend lists keys in lexicographic order.
    ///
    /// [`Store::list_page`] is correct either way; with a sorted backend it
    /// reads only one page of the listing instead of the whole tail.
    /// [`Store::from_url`] and [`Store::in_memory`] set this themselves.
    pub fn with_sorted_listing(mut self, sorted: bool) -> Self {
        self.sorted_listing = sorted;
        self
    }

    /// An in-memory store, for tests and `loams dev`.
    ///
    /// Its contents vanish with the process. A component that keeps durable
    /// local state pointing into the store (such as a metastore's snapshot
    /// pointer) will find those objects missing after a restart; use a
    /// `file://` store where state must survive restarts.
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemory::new())).with_sorted_listing(true)
    }

    /// Opens a store from a URL such as `s3://bucket/prefix`, `gs://bucket`,
    /// `az://container`, `file:///abs/dir` or `memory:///`.
    ///
    /// Settings come from the process environment first, as the backends'
    /// own `from_env()` read them: `AWS_*` for S3 (`AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`,
    /// `AWS_ENDPOINT_URL` or `AWS_ENDPOINT`, `AWS_ALLOW_HTTP`,
    /// `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`, ...), `GOOGLE_*` and
    /// `SERVICE_ACCOUNT` for GCS, `AZURE_*` and `IDENTITY_ENDPOINT` for Azure.
    /// Then the URL, then `options`: backend-specific keys (for example
    /// `aws_region`, case-insensitive) that override the environment. An S3
    /// endpoint given as `http://` allows plain HTTP unless `AWS_ALLOW_HTTP`
    /// or `aws_allow_http` says otherwise.
    ///
    /// Every backend is durable once a write returns: `file://` stores fsync the
    /// written file and its directory before `put` returns, as S3, GCS and Azure
    /// do implicitly. Callers such as the metastore rely on this.
    pub fn from_url<I, K, V>(url: &str, options: I) -> Result<Self, StoreError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        Self::from_url_with_env(url, crate::backend::process_env(), options)
    }

    /// [`Store::from_url`], reading the backend's settings from `env` instead
    /// of the process environment.
    pub fn from_url_with_env<E, I, K, V>(url: &str, env: E, options: I) -> Result<Self, StoreError>
    where
        E: IntoIterator<Item = (String, String)>,
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let parsed = url::Url::parse(url).map_err(|e| StoreError::InvalidUrl(e.to_string()))?;
        // GCS and Azure list keys in lexicographic order, and so does the
        // in-memory store. A local directory and WebDAV do not, and neither
        // does every S3 target (S3 Express One Zone directory buckets list
        // unordered; S3-compatible stores vary), so an S3 URL is unsorted
        // unless the caller opts in with `with_sorted_listing(true)`.
        let sorted = matches!(
            ObjectStoreScheme::parse(&parsed),
            Ok((
                ObjectStoreScheme::Memory
                    | ObjectStoreScheme::GoogleCloudStorage
                    | ObjectStoreScheme::MicrosoftAzure,
                _
            ))
        );
        if parsed.scheme() == "file" {
            let dir = parsed
                .to_file_path()
                .map_err(|()| StoreError::InvalidUrl(url.to_string()))?;
            std::fs::create_dir_all(&dir)
                .map_err(|e| StoreError::InvalidUrl(format!("{url}: {e}")))?;
            let fs = object_store::local::LocalFileSystem::new_with_prefix(&dir)
                .map_err(|e| map_err(url, e))?
                .with_fsync(true);
            return Ok(Self::new(Arc::new(fs)));
        }
        let (scheme, prefix) =
            ObjectStoreScheme::parse(&parsed).map_err(|e| map_err(url, e.into()))?;
        let (store, prefix) = if crate::backend::is_cloud(&scheme) {
            let prefix = Path::parse(prefix).map_err(|e| map_err(url, e.into()))?;
            let store =
                crate::backend::build(&scheme, url, env, options).map_err(|e| map_err(url, e))?;
            (store, prefix)
        } else {
            object_store::parse_url_opts(&parsed, options).map_err(|e| map_err(url, e))?
        };
        let store: Arc<dyn ObjectStore> = if prefix.as_ref().is_empty() {
            Arc::from(store)
        } else {
            Arc::new(object_store::prefix::PrefixStore::new(store, prefix))
        };
        Ok(Self::new(store).with_sorted_listing(sorted))
    }

    /// The underlying object store, for integrations that need it directly.
    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.inner
    }

    /// Writes `data` only if no object exists at `path`.
    ///
    /// Returns [`StoreError::AlreadyExists`] if an object is already present.
    pub async fn put_if_absent(
        &self,
        path: &str,
        data: Bytes,
    ) -> Result<ObjectVersion, StoreError> {
        self.put_with_mode(path, data, PutMode::Create).await
    }

    /// Writes `data` only if the current object at `path` has version `expected`.
    ///
    /// Returns [`StoreError::PreconditionFailed`] if the object changed, and
    /// [`StoreError::NotSupported`] on backends without conditional updates.
    pub async fn put_if_match(
        &self,
        path: &str,
        data: Bytes,
        expected: &ObjectVersion,
    ) -> Result<ObjectVersion, StoreError> {
        let mode = PutMode::Update(UpdateVersion {
            e_tag: expected.e_tag.clone(),
            version: expected.version.clone(),
        });
        self.put_with_mode(path, data, mode).await
    }

    /// Writes `data`, replacing any existing object.
    pub async fn put(&self, path: &str, data: Bytes) -> Result<ObjectVersion, StoreError> {
        self.put_with_mode(path, data, PutMode::Overwrite).await
    }

    async fn put_with_mode(
        &self,
        path: &str,
        data: Bytes,
        mode: PutMode,
    ) -> Result<ObjectVersion, StoreError> {
        let opts = PutOptions {
            mode,
            ..Default::default()
        };
        let result = self
            .inner
            .put_opts(&parse_path(path)?, PutPayload::from(data), opts)
            .await
            .map_err(|e| map_err(path, e))?;
        Ok(ObjectVersion {
            e_tag: result.e_tag,
            version: result.version,
        })
    }

    /// Reads a whole object.
    pub async fn get(&self, path: &str) -> Result<(Bytes, ObjectInfo), StoreError> {
        let result = self
            .inner
            .get(&parse_path(path)?)
            .await
            .map_err(|e| map_err(path, e))?;
        let info = to_info(&result.meta);
        let bytes = result.bytes().await.map_err(|e| map_err(path, e))?;
        Ok((bytes, info))
    }

    /// Reads bytes `range` of an object.
    ///
    /// An empty or inverted range returns empty bytes without a request.
    pub async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes, StoreError> {
        if range.start >= range.end {
            return Ok(Bytes::new());
        }
        self.inner
            .get_range(&parse_path(path)?, range)
            .await
            .map_err(|e| map_err(path, e))
    }

    /// Reads bytes `range` of an object in one request, with the metadata
    /// of the whole object (its size included) from the same response.
    ///
    /// `range` must be non-empty: the backend refuses an empty one. A range
    /// that runs past the end is cut at the end, so a caller that expects a
    /// length checks it.
    pub async fn get_range_with_info(
        &self,
        path: &str,
        range: Range<u64>,
    ) -> Result<(Bytes, ObjectInfo), StoreError> {
        let options = GetOptions {
            range: Some(GetRange::Bounded(range)),
            ..GetOptions::default()
        };
        let result = self
            .inner
            .get_opts(&parse_path(path)?, options)
            .await
            .map_err(|e| map_err(path, e))?;
        let info = to_info(&result.meta);
        let bytes = result.bytes().await.map_err(|e| map_err(path, e))?;
        Ok((bytes, info))
    }

    /// Reads object metadata.
    pub async fn head(&self, path: &str) -> Result<ObjectInfo, StoreError> {
        let meta = self
            .inner
            .head(&parse_path(path)?)
            .await
            .map_err(|e| map_err(path, e))?;
        Ok(to_info(&meta))
    }

    /// Deletes an object. Deleting a missing object succeeds.
    pub async fn delete(&self, path: &str) -> Result<(), StoreError> {
        match self.inner.delete(&parse_path(path)?).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(map_err(path, e)),
        }
    }

    /// Writes `body` to `path` as a multipart upload, replacing any existing
    /// object, and returns the new object's version.
    ///
    /// Parts are `part_size` bytes (raised to [`Store::MIN_PART_SIZE`]), the
    /// last one shorter. The next body chunk is read only once fewer than
    /// [`Store::PARTS_IN_FLIGHT`] parts are uploading, so memory stays near
    /// that many parts plus one chunk.
    /// If the call is cancelled (its future dropped) before it finishes,
    /// the upload is aborted in a task spawned on the current runtime.
    /// The object appears only when the upload completes. A failed body
    /// ([`StoreError::Body`]) or a failed part aborts the upload, so the call
    /// leaves no object and no parts behind. A failed completion is aborted
    /// too, but its outcome is unknown, as with any lost acknowledgement: the
    /// object may exist.
    pub async fn put_multipart(
        &self,
        path: &str,
        mut body: BoxStream<'static, io::Result<Bytes>>,
        part_size: usize,
    ) -> Result<ObjectVersion, StoreError> {
        let location = parse_path(path)?;
        let upload = self
            .inner
            .put_multipart(&location)
            .await
            .map_err(|e| map_err(path, e))?;
        let mut upload = AbortOnDrop(Some(WriteMultipart::new_with_chunk_size(
            upload,
            part_size.max(Self::MIN_PART_SIZE),
        )));
        let failure = loop {
            let chunk = match body.next().await {
                None => break None,
                Some(Ok(chunk)) => chunk,
                Some(Err(e)) => break Some(StoreError::Body(e)),
            };
            let Some(writer) = upload.0.as_mut() else {
                break None;
            };
            if let Err(e) = writer.wait_for_capacity(Self::PARTS_IN_FLIGHT).await {
                break Some(map_err(path, e));
            }
            writer.put(chunk);
        };
        // From here the upload is finished or aborted explicitly, so the
        // drop guard has nothing left to do.
        let Some(upload) = upload.0.take() else {
            return Err(StoreError::Backend(object_store::Error::Generic {
                store: "loams-store",
                source: "multipart upload state lost".into(),
            }));
        };
        match failure {
            None => {
                let result = upload.finish().await.map_err(|e| map_err(path, e))?;
                Ok(ObjectVersion {
                    e_tag: result.e_tag,
                    version: result.version,
                })
            }
            Some(err) => {
                // The upload's own failure is the one to report; an abort that
                // also fails leaves parts the store's lifecycle rules expire.
                if let Err(abort) = upload.abort().await {
                    tracing::warn!(path, error = %abort, "aborting a failed multipart upload also failed");
                }
                Err(err)
            }
        }
    }

    /// Copies the object at `from` to `to`, replacing any object there.
    ///
    /// Distinct paths use the store's server-side copy. Copying an object
    /// onto itself refreshes its last-modified time instead: neither S3 (which
    /// refuses a self-copy without a metadata change) nor a local directory
    /// (whose self-copy is a no-op) does that with a copy, so the object is
    /// read and its bytes written back (GT1 Task 0, T0-4). An overwrite that
    /// races the refresh can be lost, so refresh only content-named objects.
    pub async fn copy(&self, from: &str, to: &str) -> Result<(), StoreError> {
        if from == to {
            let (data, _) = self.get(from).await?;
            self.put(to, data).await?;
            return Ok(());
        }
        let source = parse_path(from)?;
        let target = parse_path(to)?;
        self.inner
            .copy(&source, &target)
            .await
            .map_err(|e| map_err(from, e))
    }

    /// Lists up to `limit` objects under `prefix` (recursively) whose paths
    /// sort strictly after `after`, sorted by path.
    ///
    /// Passing the last path of one page as `after` resumes with the next;
    /// `after` need not name an object. Memory holds at most `limit` entries
    /// on any backend; a backend that lists in order is read only as far as
    /// the page (see [`Store::with_sorted_listing`]).
    pub async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectInfo>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(parse_path(prefix)?)
        };
        let mut listed = match after {
            Some(after) => self
                .inner
                .list_with_offset(prefix_path.as_ref(), &parse_path(after)?),
            None => self.inner.list(prefix_path.as_ref()),
        };
        // A max-heap of the `limit` smallest paths seen so far.
        let mut page: BinaryHeap<ByPath> = BinaryHeap::new();
        while let Some(meta) = listed.try_next().await.map_err(|e| map_err(prefix, e))? {
            page.push(ByPath(to_info(&meta)));
            if page.len() > limit {
                page.pop();
            }
            if self.sorted_listing && page.len() == limit {
                break;
            }
        }
        Ok(page.into_sorted_vec().into_iter().map(|e| e.0).collect())
    }

    /// Lists all objects under `prefix` (recursively), sorted by path.
    ///
    /// An empty `prefix` lists the whole store.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, StoreError> {
        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(parse_path(prefix)?)
        };
        let mut infos: Vec<ObjectInfo> = self
            .inner
            .list(prefix_path.as_ref())
            .map_ok(|meta| to_info(&meta))
            .try_collect()
            .await
            .map_err(|e| map_err(prefix, e))?;
        infos.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(infos)
    }
}

/// Aborts a multipart upload that is dropped before it finished, for
/// example when the [`Store::put_multipart`] future is cancelled, so no
/// parts are left behind for the store's lifecycle rules.
struct AbortOnDrop(Option<WriteMultipart>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        let Some(upload) = self.0.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(e) = upload.abort().await {
                        tracing::warn!(error = %e, "aborting a cancelled multipart upload failed");
                    }
                });
            }
            Err(_) => {
                tracing::warn!(
                    "a multipart upload was cancelled outside a tokio runtime; its parts stay"
                );
            }
        }
    }
}

/// An [`ObjectInfo`] ordered by its path alone, for [`Store::list_page`]'s heap.
struct ByPath(ObjectInfo);

impl PartialEq for ByPath {
    fn eq(&self, other: &Self) -> bool {
        self.0.path == other.0.path
    }
}

impl Eq for ByPath {}

impl PartialOrd for ByPath {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ByPath {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.path.cmp(&other.0.path)
    }
}

fn to_info(meta: &object_store::ObjectMeta) -> ObjectInfo {
    ObjectInfo {
        path: meta.location.to_string(),
        size: meta.size,
        version: ObjectVersion {
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        },
        last_modified_ms: u64::try_from(meta.last_modified.timestamp_millis()).unwrap_or(0),
    }
}
