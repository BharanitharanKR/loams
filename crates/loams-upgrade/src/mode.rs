//! Unix mode bits of the stack directory's private files, portably: Windows
//! has none (a new file inherits its directory's ACL), so there setting a mode
//! does nothing and a file reads as 0644. The upgrade CLI runs against a
//! Linux or macOS compose stack; this keeps `loams` building everywhere.

use std::fs::{Metadata, OpenOptions};
use std::path::Path;

#[cfg(unix)]
pub(crate) fn set_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
pub(crate) fn set_mode(_path: &Path, _mode: u32) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
pub(crate) fn mode_of(meta: &Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    meta.permissions().mode() & 0o7777
}

#[cfg(not(unix))]
pub(crate) fn mode_of(_meta: &Metadata) -> u32 {
    0o644
}

/// `options` creating files with `mode`.
#[cfg(unix)]
pub(crate) fn create_mode(options: &mut OpenOptions, mode: u32) -> &mut OpenOptions {
    use std::os::unix::fs::OpenOptionsExt as _;
    options.mode(mode)
}

#[cfg(not(unix))]
pub(crate) fn create_mode(options: &mut OpenOptions, _mode: u32) -> &mut OpenOptions {
    options
}
