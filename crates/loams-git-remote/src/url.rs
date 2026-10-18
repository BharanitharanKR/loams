//! Addresses (§36 §6.2): `loams::<store-url>/ns/<ns>/repos/<repo>`, and how
//! one resolves to a repository in the bucket.
//!
//! git strips `loams::` and hands the helper the rest. Its path ends in
//! `ns/<ns>/repos/<repo>`; everything before that is the `object_store`
//! URL of the bucket (`file:///…`, `s3://bucket/prefix`, `gs://…`,
//! `az://…`), opened with the backend's credentials from the environment.
//! `<repo>` is a repository id, or a name the catalog maps to one (ruling
//! T51.2). `<ns>` is the namespace's numeric id: the bucket keys everything
//! by namespace id and keeps no namespace names (those are in the engine's
//! metadata store, which a serverless helper cannot read), so a name there
//! is refused with a message that says so.

use loams_git::paths::RepoPaths;
use loams_git::{
    BucketRepoCatalog, NamespaceId, RepoCatalog, RepoId, RepoLifecycle, RepoName, StoreWalStore,
    WalStore,
};
use loams_store::{Store, StoreError};

use crate::HelperError;

/// A parsed address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Address {
    /// The address as git gave it, for messages.
    pub raw: String,
    /// The bucket's URL: everything before `/ns/<ns>/repos/<repo>`.
    pub store_url: String,
    /// The namespace.
    pub ns: NamespaceId,
    /// The last segment: a repository id or name.
    pub repo: String,
}

impl Address {
    /// Parses `<store-url>/ns/<ns>/repos/<repo>` (a trailing `/` is
    /// allowed). Anything else is `BadAddress`.
    pub fn parse(raw: &str) -> Result<Self, HelperError> {
        let bad = || HelperError::BadAddress {
            address: raw.to_string(),
        };
        let trimmed = raw.trim_end_matches('/');
        // The last four segments are `ns`, `<ns>`, `repos` and `<repo>`.
        let mut parts = trimmed.rsplitn(5, '/');
        let repo = parts.next().ok_or_else(bad)?;
        let repos = parts.next().ok_or_else(bad)?;
        let ns = parts.next().ok_or_else(bad)?;
        let ns_word = parts.next().ok_or_else(bad)?;
        let store_url = parts.next().ok_or_else(bad)?;
        if repos != "repos" || ns_word != "ns" || RepoId::parse(repo).is_err() {
            return Err(bad());
        }
        // `scheme://` alone (no bucket, no directory) names nothing.
        let has_location = store_url
            .split_once("://")
            .is_some_and(|(scheme, rest)| !scheme.is_empty() && !rest.is_empty());
        if !has_location {
            return Err(bad());
        }
        let numeric = !ns.is_empty() && ns.bytes().all(|b| b.is_ascii_digit());
        let ns = match ns.parse::<u64>().ok().filter(|_| numeric) {
            Some(id) => NamespaceId(id),
            // A namespace name (what the console and the APIs show): say
            // why it cannot work rather than "expected …".
            None if RepoName::parse(ns).is_ok() => {
                return Err(HelperError::NamespaceName {
                    address: raw.to_string(),
                    ns: ns.to_string(),
                });
            }
            None => return Err(bad()),
        };
        Ok(Self {
            raw: raw.to_string(),
            store_url: store_url.to_string(),
            ns,
            repo: repo.to_string(),
        })
    }

    /// The URL's scheme (`file`, `s3`, …).
    pub fn scheme(&self) -> &str {
        self.store_url
            .split_once("://")
            .map_or("", |(scheme, _)| scheme)
    }

    /// Opens the bucket, with the backend's settings and credentials from
    /// the environment (`AWS_*`, `GOOGLE_*`, `AZURE_*`).
    pub fn open_store(&self) -> Result<Store, HelperError> {
        Store::from_url(&self.store_url, std::env::vars()).map_err(|err| HelperError::Store {
            what: format!("open {}", self.store_url),
            reason: err.to_string(),
        })
    }

    /// The repository the address names, or `None` when there is none (or
    /// it is being deleted). An id with a checkpoint wins; otherwise the
    /// segment is looked up as a catalog name (ruling T51.2).
    pub async fn resolve(&self, store: &Store) -> Result<Option<RepoId>, HelperError> {
        let segment = RepoId::parse(&self.repo).map_err(|_| HelperError::BadAddress {
            address: self.raw.clone(),
        })?;
        if self.has_checkpoint(store, &segment).await? {
            return match store
                .head(&RepoPaths::deleting_marker(self.ns, &segment))
                .await
            {
                Ok(_) => Ok(None),
                Err(StoreError::NotFound { .. }) => Ok(Some(segment)),
                Err(err) => Err(HelperError::store(
                    "read the repository's catalog state",
                    err,
                )),
            };
        }
        let Ok(name) = RepoName::parse(&self.repo) else {
            return Ok(None);
        };
        let catalog = BucketRepoCatalog::new(store.clone());
        let record = catalog
            .get(self.ns, &name)
            .await
            .map_err(|err| HelperError::Store {
                what: "read the repository catalog".into(),
                reason: err.to_string(),
            })?;
        Ok(record
            .filter(|record| record.state == RepoLifecycle::Active)
            .map(|record| record.repo_id))
    }

    /// The id the address names when it names none yet: a push with
    /// `LOAMS_GIT_CREATE_ON_PUSH=1` creates the repository under it.
    pub fn new_repo_id(&self) -> Result<RepoId, HelperError> {
        RepoId::parse(&self.repo).map_err(|_| HelperError::BadAddress {
            address: self.raw.clone(),
        })
    }

    async fn has_checkpoint(&self, store: &Store, repo: &RepoId) -> Result<bool, HelperError> {
        let wal = StoreWalStore::new(store.clone(), RepoPaths::new(self.ns, repo));
        let checkpoint = wal
            .latest_checkpoint(loams_git::Seq(u64::MAX))
            .await
            .map_err(|err| HelperError::Store {
                what: "read the repository's checkpoint".into(),
                reason: err.to_string(),
            })?;
        Ok(checkpoint.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_store_url_namespace_and_repo() {
        let a = Address::parse("file:///data/loams/ns/7/repos/r01jtest000000000000000000")
            .expect("parse");
        assert_eq!(a.store_url, "file:///data/loams");
        assert_eq!(a.ns, NamespaceId(7));
        assert_eq!(a.repo, "r01jtest000000000000000000");
        assert_eq!(a.scheme(), "file");
        let b = Address::parse("s3://bucket/prefix/ns/1/repos/web/").expect("parse");
        assert_eq!(b.store_url, "s3://bucket/prefix");
        assert_eq!(b.repo, "web");
        let c = Address::parse("s3://bucket/ns/12/repos/web").expect("parse");
        assert_eq!(c.store_url, "s3://bucket");
        assert_eq!(c.ns, NamespaceId(12));
    }

    #[test]
    fn refuses_anything_else() {
        for raw in [
            "",
            "file:///data",
            "file:///data/ns/-1/repos/web",
            "file:///data/ns/Bad/repos/web",
            "file:///data/ns/1/repo/web",
            "file:///data/nss/1/repos/web",
            "file:///data/ns/1/repos/Web",
            "file:///data/ns/1/repos/web/extra",
            "/ns/1/repos/web",
            "s3:///ns/1/repos/web",
            "s3://ns/1/repos/web",
        ] {
            let err = Address::parse(raw).expect_err(raw);
            assert_eq!(
                err.to_string(),
                format!("loams: expected <store-url>/ns/<namespace-id>/repos/<repo>, got {raw}")
            );
        }
    }

    #[test]
    fn a_namespace_name_says_the_url_wants_its_id() {
        for raw in [
            "s3://loams-engine/stack/ns/e2e/repos/e2e-sample",
            "file:///data/ns/x/repos/web",
        ] {
            let err = Address::parse(raw).expect_err(raw);
            assert!(
                matches!(&err, HelperError::NamespaceName { address, .. } if address == raw),
                "{err}"
            );
            let text = err.to_string();
            assert!(text.contains("numeric id"), "{text}");
            assert!(text.contains("namespace_id"), "{text}");
        }
    }
}
