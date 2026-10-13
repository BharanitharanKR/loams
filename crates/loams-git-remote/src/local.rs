//! The local repository: `git` run as a process (D394), the record of the
//! packs already fetched, pack files written atomically, and the pushing
//! principal.

use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use bytes::Bytes;
use loams_git::{ObjectId, Principal};
use sha2::{Digest, Sha256};

use crate::HelperError;

/// The local repository git runs the helper for (`GIT_DIR`). `git
/// ls-remote` outside a repository runs the helper with none: then only
/// `list` works.
#[derive(Debug)]
pub struct Local {
    dirs: Option<Dirs>,
}

#[derive(Debug)]
struct Dirs {
    git_dir: PathBuf,
    /// `$GIT_DIR/loams/<remote>/`: the helper's own files for this remote.
    remote_dir: PathBuf,
}

impl Local {
    /// The repository in `GIT_DIR` (git sets it for every helper), with a
    /// directory for `remote`'s files. A remote name that is not a plain
    /// name (a URL fetched directly) is replaced by a hash of it.
    pub fn open(remote: &str) -> Self {
        let Some(git_dir) = std::env::var_os("GIT_DIR").map(PathBuf::from) else {
            return Self { dirs: None };
        };
        let plain = !remote.is_empty()
            && !remote.starts_with('.')
            && remote
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
        let dir_name = if plain {
            remote.to_string()
        } else {
            format!(
                "url-{}",
                &hex::encode(Sha256::digest(remote.as_bytes()))[..16]
            )
        };
        let remote_dir = git_dir.join("loams").join(dir_name);
        Self {
            dirs: Some(Dirs {
                git_dir,
                remote_dir,
            }),
        }
    }

    fn dirs(&self) -> Result<&Dirs, HelperError> {
        self.dirs.as_ref().ok_or_else(|| {
            HelperError::Protocol("GIT_DIR is not set: run the helper through git".into())
        })
    }

    /// `objects/pack/` (or `$GIT_OBJECT_DIRECTORY/pack/`).
    fn pack_dir(&self) -> Result<PathBuf, HelperError> {
        let git_dir = &self.dirs()?.git_dir;
        Ok(std::env::var_os("GIT_OBJECT_DIRECTORY")
            .map_or_else(|| git_dir.join("objects"), PathBuf::from)
            .join("pack"))
    }

    /// The file listing the checksums of the packs this remote's objects
    /// came in (Ruling 4).
    fn packs_record(&self) -> Result<PathBuf, HelperError> {
        Ok(self.dirs()?.remote_dir.join("packs"))
    }

    /// The packs fetched (or pushed) before.
    pub fn recorded_packs(&self) -> Result<HashSet<String>, HelperError> {
        match std::fs::read_to_string(self.packs_record()?) {
            Ok(text) => Ok(text.lines().map(str::to_string).collect()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HashSet::new()),
            Err(err) => Err(HelperError::io("read the fetched-packs record", err)),
        }
    }

    /// Adds `checksums` to the record, rewriting it atomically.
    pub fn record_packs(&self, checksums: &[String]) -> Result<(), HelperError> {
        if checksums.is_empty() {
            return Ok(());
        }
        let mut all = self.recorded_packs()?;
        all.extend(checksums.iter().cloned());
        let mut lines: Vec<String> = all.into_iter().collect();
        lines.sort();
        let mut text = lines.join("\n");
        text.push('\n');
        let remote_dir = &self.dirs()?.remote_dir;
        std::fs::create_dir_all(remote_dir)
            .map_err(|err| HelperError::io("create the helper's directory", err))?;
        let tmp = remote_dir.join(format!("packs.tmp-{}", std::process::id()));
        let record = self.packs_record()?;
        std::fs::write(&tmp, text)
            .and_then(|()| std::fs::rename(&tmp, record))
            .map_err(|err| HelperError::io("write the fetched-packs record", err))
    }

    /// Whether git already has the pack (`pack-<checksum>.idx`).
    pub fn has_pack(&self, checksum: &str) -> Result<bool, HelperError> {
        Ok(self
            .pack_dir()?
            .join(format!("pack-{checksum}.idx"))
            .exists())
    }

    /// A temporary file in `objects/pack/`, which git ignores and removes
    /// when stale (`tmp_pack_*`).
    pub fn temp_pack_file(&self, kind: &str) -> Result<(PathBuf, std::fs::File), HelperError> {
        let dir = self.pack_dir()?;
        std::fs::create_dir_all(&dir).map_err(|err| HelperError::io("create objects/pack", err))?;
        let path = dir.join(format!(
            "tmp_pack_loams_{kind}_{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        ));
        let file = std::fs::File::create(&path)
            .map_err(|err| HelperError::io("create a temporary pack file", err))?;
        Ok((path, file))
    }

    /// Moves a downloaded pack and idx into place: the pack first, so git
    /// never sees an idx without its pack.
    pub fn install_pack(&self, checksum: &str, pack: &Path, idx: &Path) -> Result<(), HelperError> {
        let dir = self.pack_dir()?;
        std::fs::rename(pack, dir.join(format!("pack-{checksum}.pack")))
            .and_then(|()| std::fs::rename(idx, dir.join(format!("pack-{checksum}.idx"))))
            .map_err(|err| HelperError::io(format!("install pack {checksum}"), err))
    }

    /// Runs `git <args>` with `stdin`, returning its stdout. A non-zero
    /// exit is an error carrying its stderr.
    pub fn git(&self, args: &[&str], stdin: Option<&[u8]>) -> Result<Vec<u8>, HelperError> {
        let what = args.first().copied().unwrap_or("").to_string();
        let mut child = Command::new("git")
            .args(args)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| HelperError::Git {
                what: what.clone(),
                reason: err.to_string(),
            })?;
        if let (Some(input), Some(mut pipe)) = (stdin, child.stdin.take()) {
            pipe.write_all(input).map_err(|err| HelperError::Git {
                what: what.clone(),
                reason: err.to_string(),
            })?;
        }
        let out = child.wait_with_output().map_err(|err| HelperError::Git {
            what: what.clone(),
            reason: err.to_string(),
        })?;
        if out.status.success() {
            Ok(out.stdout)
        } else {
            Err(HelperError::Git {
                what,
                reason: String::from_utf8_lossy(&out.stderr).trim().to_string(),
            })
        }
    }

    /// The object `rev` names, unpeeled (an annotated tag stays a tag).
    pub fn rev_parse(&self, rev: &str) -> Result<ObjectId, HelperError> {
        let out = self.git(&["rev-parse", "--verify", "--end-of-options", rev], None)?;
        parse_oid(String::from_utf8_lossy(&out).trim())
    }

    /// Which of `oids` the local repository has.
    pub fn present(&self, oids: &[ObjectId]) -> Result<HashSet<ObjectId>, HelperError> {
        if oids.is_empty() {
            return Ok(HashSet::new());
        }
        let input: String = oids.iter().map(|oid| format!("{oid}\n")).collect();
        let out = self.git(
            &["cat-file", "--batch-check=%(objectname)"],
            Some(input.as_bytes()),
        )?;
        // A missing object prints `<oid> missing`; a present one its name.
        Ok(String::from_utf8_lossy(&out)
            .lines()
            .filter(|line| !line.ends_with(" missing"))
            .filter_map(|line| parse_oid(line).ok())
            .collect())
    }

    /// Whether `old` is an ancestor of `new` (Ruling 5).
    pub fn is_ancestor(&self, old: &ObjectId, new: &ObjectId) -> Result<bool, HelperError> {
        let status = Command::new("git")
            .args(["merge-base", "--is-ancestor"])
            .arg(old.to_string())
            .arg(new.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|err| HelperError::Git {
                what: "merge-base".into(),
                reason: err.to_string(),
            })?;
        // 1 is "not an ancestor"; anything else is a failure.
        match status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(HelperError::Git {
                what: "merge-base".into(),
                reason: format!("merge-base --is-ancestor {old} {new} failed ({status})"),
            }),
        }
    }

    /// The pack of every object reachable from `tips` and not from
    /// `exclude` (which must all be present locally), completed so it
    /// needs no base outside itself, and its idx: `git pack-objects --revs
    /// --thin` into `git index-pack --fix-thin`. `None` when it would hold
    /// no object.
    pub fn pack(
        &self,
        tips: &[ObjectId],
        exclude: &[ObjectId],
        progress: bool,
    ) -> Result<Option<(Bytes, Bytes)>, HelperError> {
        if tips.is_empty() {
            return Ok(None);
        }
        let work = self.dirs()?.remote_dir.join(format!(
            "push-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        ));
        std::fs::create_dir_all(&work)
            .map_err(|err| HelperError::io("create the push directory", err))?;
        let result = self.pack_into(&work, tips, exclude, progress);
        // Best effort: a leftover directory holds only this push's copy.
        let _ = std::fs::remove_dir_all(&work);
        result
    }

    fn pack_into(
        &self,
        work: &Path,
        tips: &[ObjectId],
        exclude: &[ObjectId],
        progress: bool,
    ) -> Result<Option<(Bytes, Bytes)>, HelperError> {
        let failed = |what: &str, reason: String| HelperError::Git {
            what: what.to_string(),
            reason,
        };
        let mut input = String::new();
        for tip in tips {
            input.push_str(&format!("{tip}\n"));
        }
        for base in exclude {
            input.push_str(&format!("^{base}\n"));
        }
        let mut objects = Command::new("git")
            .args(["pack-objects", "--revs", "--stdout", "--thin"])
            .arg(if progress { "--progress" } else { "-q" })
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|err| failed("pack-objects", err.to_string()))?;
        let pack_out = objects
            .stdout
            .take()
            .ok_or_else(|| failed("pack-objects", "no stdout".into()))?;
        let pack_path = work.join("push.pack");
        let index = Command::new("git")
            .args(["index-pack", "--stdin", "--fix-thin"])
            .arg(&pack_path)
            .stdin(Stdio::from(pack_out))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| failed("index-pack", err.to_string()))?;
        // pack-objects reads every revision before it writes, and index-pack
        // drains its output meanwhile, so writing all input first is safe.
        let written = objects
            .stdin
            .take()
            .ok_or_else(|| failed("pack-objects", "no stdin".into()))
            .and_then(|mut pipe| {
                pipe.write_all(input.as_bytes())
                    .map_err(|err| failed("pack-objects", err.to_string()))
            });
        let objects_status = objects
            .wait()
            .map_err(|err| failed("pack-objects", err.to_string()))?;
        let index_out = index
            .wait_with_output()
            .map_err(|err| failed("index-pack", err.to_string()))?;
        written?;
        if !objects_status.success() {
            return Err(failed(
                "pack-objects",
                format!("exited with {objects_status}"),
            ));
        }
        if !index_out.status.success() {
            return Err(failed(
                "index-pack",
                String::from_utf8_lossy(&index_out.stderr)
                    .trim()
                    .to_string(),
            ));
        }
        let read = |path: PathBuf| {
            std::fs::read(&path)
                .map(Bytes::from)
                .map_err(|err| HelperError::io(format!("read {}", path.display()), err))
        };
        let pack = read(pack_path)?;
        let idx = read(work.join("push.idx"))?;
        // A pack's object count is the big-endian u32 at bytes 8..12.
        let count = pack
            .get(8..12)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
            .ok_or_else(|| failed("pack-objects", "a pack without a header".into()))?;
        Ok((count > 0).then_some((pack, idx)))
    }

    /// Who pushes (Ruling 6, ruling T51.4): `git config user.email` under
    /// `LOAMS_GIT_UNAUTHENTICATED_TESTS=1`; otherwise the subject of the
    /// bucket credentials the helper uses (the S3 access key id, the Azure
    /// storage account), and for a local directory the local user.
    pub fn principal(&self, scheme: &str) -> Result<Principal, HelperError> {
        let env = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let raw = if env("LOAMS_GIT_UNAUTHENTICATED_TESTS").as_deref() == Some("1") {
            let out = self.git(&["config", "user.email"], None).map_err(|_| {
                HelperError::Protocol(
                    "LOAMS_GIT_UNAUTHENTICATED_TESTS=1 needs git config user.email".into(),
                )
            })?;
            String::from_utf8_lossy(&out).trim().to_string()
        } else {
            let subject = match scheme {
                "s3" | "s3a" => env("AWS_ACCESS_KEY_ID"),
                "az" | "adl" | "azure" | "abfs" | "abfss" => env("AZURE_STORAGE_ACCOUNT_NAME"),
                _ => None,
            };
            subject.unwrap_or_else(|| {
                let user = env("USER")
                    .or_else(|| env("LOGNAME"))
                    .unwrap_or_else(|| "unknown".into());
                format!("local:{user}")
            })
        };
        Principal::parse(&sanitize_principal(&raw))
            .map_err(|err| HelperError::Protocol(format!("pushing principal: {err}")))
    }
}

/// `raw` as one path segment of printable ASCII (the `Principal` rules):
/// anything else becomes `_`.
fn sanitize_principal(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_graphic() && c != '/' && c != '\\' {
                c
            } else {
                '_'
            }
        })
        .take(256)
        .collect();
    match cleaned.as_str() {
        "" | "." | ".." => "unknown".into(),
        _ => cleaned,
    }
}

/// Parses 40 hex digits.
pub fn parse_oid(hex: &str) -> Result<ObjectId, HelperError> {
    ObjectId::from_hex(hex.as_bytes())
        .map_err(|err| HelperError::Protocol(format!("object id {hex:?}: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principals_are_one_printable_segment() {
        assert_eq!(sanitize_principal("t@example.com"), "t@example.com");
        assert_eq!(sanitize_principal("a/b c\\d"), "a_b_c_d");
        assert_eq!(sanitize_principal(".."), "unknown");
        assert_eq!(sanitize_principal(""), "unknown");
        assert_eq!(sanitize_principal(&"x".repeat(300)).len(), 256);
    }
}
