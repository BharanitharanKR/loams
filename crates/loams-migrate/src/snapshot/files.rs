//! File helpers of snapshots: private directories (0700) and files (0600),
//! recursive copies, digests and whole-directory replacement.

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::manifest::Component;
use super::{SnapshotError, io};

pub(crate) fn private_dir(dir: &Path) -> Result<(), SnapshotError> {
    fs::create_dir_all(dir).map_err(|e| io(dir.display(), e))?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(|e| io(dir.display(), e))
}

/// `dir` and its missing parents, each 0700 when this creates it.
pub(crate) fn private_dir_all(dir: &Path) -> Result<(), SnapshotError> {
    if dir.exists() {
        return Ok(());
    }
    if let Some(parent) = dir.parent() {
        private_dir_all(parent)?;
    }
    private_dir(dir)
}

pub(crate) fn private_file(path: &Path) -> Result<(), SnapshotError> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|e| io(path.display(), e))
}

pub(crate) fn write_private(path: &Path, bytes: &[u8]) -> Result<(), SnapshotError> {
    fs::write(path, bytes).map_err(|e| io(path.display(), e))?;
    private_file(path)
}

pub(crate) fn non_empty(dir: &Path) -> bool {
    fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_some())
}

/// Every regular file under `root` as (path relative to `root` with `/`,
/// absolute path), sorted.
pub(crate) fn walk(root: &Path) -> Result<Vec<(String, PathBuf)>, SnapshotError> {
    let mut out = Vec::new();
    walk_into(root, root, &mut out)?;
    out.sort();
    Ok(out)
}

fn walk_into(
    root: &Path,
    dir: &Path,
    out: &mut Vec<(String, PathBuf)>,
) -> Result<(), SnapshotError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io(dir.display(), e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| io(dir.display(), e))?;
        let path = entry.path();
        let kind = entry.file_type().map_err(|e| io(path.display(), e))?;
        if kind.is_dir() {
            walk_into(root, &path, out)?;
        } else if kind.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|e| io(path.display(), e))?
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            out.push((relative, path));
        }
    }
    Ok(())
}

/// The bytes of every file under `path` (a file or a directory).
pub(crate) fn size_of(path: &Path) -> u64 {
    match fs::metadata(path) {
        Ok(meta) if meta.is_file() => meta.len(),
        Ok(_) => walk(path)
            .unwrap_or_default()
            .iter()
            .filter_map(|(_, p)| fs::metadata(p).ok())
            .map(|m| m.len())
            .sum(),
        Err(_) => 0,
    }
}

/// The digest of a component: every file's relative path and contents, in
/// path order.
pub(crate) fn digest(root: &Path) -> Result<(u64, String), SnapshotError> {
    let mut hash = Sha256::new();
    let mut bytes = 0;
    for (relative, path) in walk(root)? {
        let data = fs::read(&path).map_err(|e| io(path.display(), e))?;
        bytes += data.len() as u64;
        hash.update(relative.as_bytes());
        hash.update([0]);
        hash.update((data.len() as u64).to_be_bytes());
        hash.update(&data);
    }
    Ok((bytes, hex(&hash.finalize())))
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// Describes the component at `<dir>/<path>`.
pub(crate) fn describe(
    store: &str,
    method: &str,
    dir: &Path,
    path: &str,
) -> Result<Component, SnapshotError> {
    let (bytes, sha256) = digest(&dir.join(path))?;
    Ok(Component {
        store: store.to_string(),
        method: method.to_string(),
        path: path.to_string(),
        bytes,
        sha256,
    })
}

/// Copies directory `from` to `to` (private), and describes it.
pub(crate) fn copy_component(
    store: &str,
    from: &Path,
    to: &Path,
) -> Result<Component, SnapshotError> {
    private_dir(to)?;
    for (relative, path) in walk(from)? {
        let target = to.join(&relative);
        if let Some(parent) = target.parent() {
            private_dir_all(parent)?;
        }
        fs::copy(&path, &target).map_err(|e| io(path.display(), e))?;
        private_file(&target)?;
    }
    let dir = to.parent().unwrap_or(to);
    let name = to
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    describe(store, "copy", dir, &name)
}

/// Replaces directory `target` with a copy of `source`: the copy is built
/// beside it, the current directory is moved aside, the copy takes its
/// place, and only then is the old one removed.
pub(crate) fn replace_dir(source: &Path, target: &Path) -> Result<(), SnapshotError> {
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let staged = target.with_file_name(format!("{name}.restore-new"));
    let old = target.with_file_name(format!("{name}.restore-old"));
    let _ = fs::remove_dir_all(&staged);
    fs::create_dir_all(&staged).map_err(|e| io(staged.display(), e))?;
    for (relative, path) in walk(source)? {
        let to = staged.join(&relative);
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent).map_err(|e| io(parent.display(), e))?;
        }
        fs::copy(&path, &to).map_err(|e| io(path.display(), e))?;
    }
    let _ = fs::remove_dir_all(&old);
    if target.exists() {
        fs::rename(target, &old).map_err(|e| io(target.display(), e))?;
    }
    fs::rename(&staged, target).map_err(|e| io(target.display(), e))?;
    let _ = fs::remove_dir_all(&old);
    Ok(())
}
