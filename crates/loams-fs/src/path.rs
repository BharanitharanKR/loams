use std::fmt;
use std::str::FromStr;

/// The longest [`FsPath`], in bytes: R2's key limit (§36 §17.2).
pub const MAX_PATH_BYTES: usize = 1024;

/// A path inside an [`Fs`](crate::Fs): relative, `/`-separated, with no
/// empty, `.` or `..` segments, at most [`MAX_PATH_BYTES`] bytes.
///
/// [`FsPath::root`] (the empty path) names the whole backend; it is a
/// directory for [`Fs::list`](crate::Fs::list), never a file.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FsPath(String);

/// Why a string is not an [`FsPath`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FsPathError {
    /// The path is longer than [`MAX_PATH_BYTES`].
    #[error("path is {0} bytes, over the {MAX_PATH_BYTES}-byte limit")]
    TooLong(usize),
    /// A segment is empty, `.` or `..` (so also an absolute path).
    #[error("path {0:?} has an empty, `.` or `..` segment")]
    BadSegment(String),
    /// The path contains a NUL byte.
    #[error("path {0:?} contains a NUL byte")]
    Nul(String),
}

impl FsPath {
    /// Validates `path`.
    pub fn new(path: impl Into<String>) -> Result<Self, FsPathError> {
        let path = path.into();
        if path.len() > MAX_PATH_BYTES {
            return Err(FsPathError::TooLong(path.len()));
        }
        if path.contains('\0') {
            return Err(FsPathError::Nul(path));
        }
        if path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        {
            return Err(FsPathError::BadSegment(path));
        }
        Ok(Self(path))
    }

    /// The empty path: the whole backend, as a directory to list.
    pub fn root() -> Self {
        Self(String::new())
    }

    /// Whether this is [`FsPath::root`].
    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    /// `self/child`, where `child` is one or more segments.
    pub fn join(&self, child: &str) -> Result<Self, FsPathError> {
        if self.is_root() {
            Self::new(child)
        } else {
            Self::new(format!("{}/{child}", self.0))
        }
    }

    /// Whether this path is `dir` or lies under it, by whole segments.
    pub fn is_under(&self, dir: &FsPath) -> bool {
        dir.is_root()
            || self.0 == dir.0
            || (self.0.starts_with(&dir.0) && self.0.as_bytes().get(dir.0.len()) == Some(&b'/'))
    }

    /// The path as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FsPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for FsPath {
    type Err = FsPathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}
