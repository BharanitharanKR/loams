use crate::FsPath;

/// Errors of an [`Fs`](crate::Fs) (§36 §17.2).
#[derive(Debug, thiserror::Error)]
pub enum FsError {
    #[error("not found: {0}")]
    NotFound(FsPath),
    #[error("already exists: {0}")]
    AlreadyExists(FsPath),
    #[error("unsupported on this backend: {0}")]
    Unsupported(&'static str),
    #[error("too large: {0} bytes")]
    TooLarge(u64),
    #[error("backend: {0}")]
    Backend(String),
}
