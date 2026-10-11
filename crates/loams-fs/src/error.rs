use crate::FsPath;

/// Errors of an [`Fs`](crate::Fs) (§36 §17.2).
#[derive(Debug, thiserror::Error)]
pub enum FsError {
    /// There is no file at the path.
    #[error("not found: {0}")]
    NotFound(FsPath),
    /// A `CreateNew` write found a file at the path.
    #[error("already exists: {0}")]
    AlreadyExists(FsPath),
    /// The backend cannot do this (append or rename on a blob-only backend,
    /// a file operation on the root, a path the host cannot represent).
    #[error("unsupported on this backend: {0}")]
    Unsupported(&'static str),
    /// The file would exceed the backend's `max_file`.
    #[error("too large: {0} bytes")]
    TooLarge(u64),
    /// Any other failure, including a `read_range` outside the file.
    #[error("backend: {0}")]
    Backend(String),
}
