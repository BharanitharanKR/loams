//! `FileSecretStore`: the dev store (CN1's `deploy/fabric/secrets/`, mode
//! 0600), and the layout a fallback pod sees once its `Secret`s are mounted.
//! A reference `<secret>/<key>` in namespace `ns` is the file
//! `<root>/<ns>/<secret>/<key>`. On Unix a file readable by its group or by
//! others is refused, so a secret checked out with the wrong mode fails
//! loudly rather than working.

use std::fmt;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use loams_connector_sdk::{Secret, SecretRef};
use zeroize::Zeroizing;

use super::{SecretError, SecretStore, parse_ref};

pub struct FileSecretStore {
    root: PathBuf,
}

impl FileSecretStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    /// The file a reference names (after [`parse_ref`] has checked it, so
    /// it cannot leave `<root>/<ns>/`).
    pub fn path_of(&self, ns: u64, r: &SecretRef) -> Result<PathBuf, SecretError> {
        let (secret, key) = parse_ref(r)?;
        Ok(self.root.join(ns.to_string()).join(secret).join(key))
    }
}

impl fmt::Debug for FileSecretStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSecretStore")
            .field("root", &self.root)
            .finish()
    }
}

#[async_trait]
impl SecretStore for FileSecretStore {
    async fn resolve(&self, ns: u64, r: &SecretRef) -> Result<Secret, SecretError> {
        let path = self.path_of(ns, r)?;
        let meta = match tokio::fs::metadata(&path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(SecretError::NotFound {
                    namespace: ns,
                    name: r.name.clone(),
                });
            }
            Err(e) => return Err(SecretError::Unavailable(e.kind().to_string())),
        };
        if !meta.is_file() {
            return Err(SecretError::NotFound {
                namespace: ns,
                name: r.name.clone(),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = meta.permissions().mode();
            if mode & 0o077 != 0 {
                return Err(SecretError::Denied {
                    namespace: ns,
                    name: r.name.clone(),
                    reason: format!(
                        "the file's mode is {:o}; it must be readable by its owner only",
                        mode & 0o777
                    ),
                });
            }
        }
        let bytes = Zeroizing::new(
            tokio::fs::read(&path)
                .await
                .map_err(|e| SecretError::Unavailable(e.kind().to_string()))?,
        );
        // A trailing newline from `echo` is not part of the secret.
        let end = bytes
            .iter()
            .rposition(|b| *b != b'\n' && *b != b'\r')
            .map_or(0, |i| i + 1);
        Ok(Secret::new(&bytes[..end]))
    }
}
