//! `loams-connector-objects`: the object-storage connector family (CN2
//! Task 13, OR-6), one implementation over `object_store` (S3, GCS, Azure)
//! plus SFTP over `russh-sftp`.
//!
//! | Manifest | Runtime ref | Store |
//! |---|---|---|
//! | `s3` ★ | [`s3`] | S3 (the native sink replaces Iggy's `s3_sink`, OR-8) |
//! | `cloudflare-r2` | [`s3`] + profile | R2's S3 API (`region: auto`) |
//! | `minio-rustfs` | [`s3`] + profile | RustFS and MinIO |
//! | `gcs` | [`gcs`] | GCS JSON API |
//! | `azure-blob` | [`azure_blob`] | Azure Blob REST |
//! | `adls-gen2` | [`adls_gen2`] | Blob REST, and DFS REST for rename (R0.21) |
//! | `sftp` | [`sftp`] | SSH 2 / SFTP v3 |
//!
//! * [`list`] — the listing source: a split per prefix, a high-water key,
//!   ids `"<bucket>/<key>@<etag>"`, objects as metadata, bytes, or rows
//!   decoded through the SDK's codecs (CSV, NDJSON) and Parquet.
//! * [`sink`] — the rolling sink: Parquet, NDJSON or raw files rolled by
//!   size or time, deterministic names and a conditional publish, staged with
//!   the checkpoint so a crash loses and duplicates nothing (R13.2).
//! * [`notify`] — S3, GCS and Event Grid notifications as `object` and
//!   `object-deleted` events.
//! * [`sftp`] — the SFTP polling source and the write-then-rename sink.
//! * [`store`] — building a store whose connections pass the egress check.

use std::sync::Arc;

use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder, SinkFactory,
    SourceFactory, SplitEnumerator,
};

pub mod adls_gen2;
pub mod config;
pub mod list;
pub mod notify;
pub mod sftp;
pub mod sink;
pub mod store;

use config::ObjectsConfig;
use list::{ListingEnumerator, ListingSourceFactory, SourceSettings, StoreLister};
use sink::{ObjectSinkFactory, Publish, SinkSettings};
use store::{Backend, StoreHandle};

/// Which store a factory drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Store(Backend),
    /// Azure Blob, publishing through the DFS rename when `hns` is set.
    Adls,
    Sftp,
}

/// The family's [`ConnectorFactory`]: one per manifest id.
#[derive(Debug, Clone, Copy)]
pub struct ObjectsFactory {
    id: &'static str,
    kind: Kind,
}

impl ObjectsFactory {
    pub const fn new(id: &'static str, kind: Kind) -> Self {
        Self { id, kind }
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }
}

fn defaults(ctx: &ConnectorContext) -> Option<&serde_json::Value> {
    ctx.profile.as_ref().map(|p| &p.defaults)
}

impl ObjectsFactory {
    fn store_parts(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<(ObjectsConfig, Arc<StoreHandle>), ConnectorError> {
        let cfg = ObjectsConfig::parse(&ctx.config, defaults(ctx))?;
        let backend = match self.kind {
            Kind::Store(b) => b,
            Kind::Adls => Backend::Azure,
            Kind::Sftp => return Err(ConnectorError::fatal("not an object store")),
        };
        let store = Arc::new(StoreHandle::lazy(
            backend,
            cfg.clone(),
            ctx.secrets.clone(),
            ctx.egress.clone(),
        ));
        Ok((cfg, store))
    }
}

impl ConnectorFactory for ObjectsFactory {
    fn id(&self) -> &'static str {
        self.id
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let envelope = EnvelopeBuilder::new(self.id, &ctx.instance);
        let (prefixes, lister, settings): (_, Arc<dyn list::Lister>, _) = match self.kind {
            Kind::Sftp => {
                let cfg = sftp::SftpConfig::parse(&ctx.config, defaults(ctx))?;
                let fs = Arc::new(sftp::SshFs::new(
                    &cfg,
                    ctx.secrets.clone(),
                    ctx.egress.clone(),
                )?);
                (
                    cfg.source_prefixes(),
                    Arc::new(sftp::SftpLister::new(fs, cfg.host.clone(), cfg.recursive)),
                    SourceSettings::from_config(&cfg.source)?,
                )
            }
            _ => {
                let (cfg, store) = self.store_parts(ctx)?;
                (
                    cfg.source_prefixes(),
                    Arc::new(StoreLister::new(store, cfg.bucket.clone())),
                    SourceSettings::from_config(&cfg.source)?,
                )
            }
        };
        Ok(Some((
            Box::new(ListingEnumerator::new(prefixes)),
            Box::new(ListingSourceFactory::new(lister, settings, envelope)),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        if self.kind == Kind::Sftp {
            let cfg = sftp::SftpConfig::parse(&ctx.config, defaults(ctx))?;
            let fs = Arc::new(sftp::SshFs::new(
                &cfg,
                ctx.secrets.clone(),
                ctx.egress.clone(),
            )?);
            return Ok(Some(Box::new(sftp::SftpSinkFactory::new(
                fs,
                cfg.sink.clone(),
                &cfg.prefix,
                &ctx.instance,
            ))));
        }
        let (cfg, store) = self.store_parts(ctx)?;
        let publish = if self.kind == Kind::Adls && cfg.hns {
            let account = cfg
                .account
                .clone()
                .ok_or_else(|| ConnectorError::fatal("account is required for ADLS Gen2"))?;
            let endpoint = cfg
                .dfs_endpoint
                .clone()
                .unwrap_or_else(|| adls_gen2::DfsClient::default_endpoint(&account));
            store::check_endpoint(&endpoint, &*ctx.egress)?;
            Publish::DfsRename(Arc::new(adls_gen2::DfsClient::lazy(
                ctx.egress.clone(),
                endpoint,
                cfg.bucket.clone(),
                account,
                cfg.auth.clone(),
                ctx.secrets.clone(),
            )))
        } else {
            Publish::ConditionalPut
        };
        let settings = SinkSettings::new(&cfg.sink, &cfg.prefix, &ctx.instance, publish);
        Ok(Some(Box::new(ObjectSinkFactory::new(
            store,
            settings,
            ctx.instance.clone(),
        ))))
    }
}

/// S3, and the S3 targets R2 and MinIO/RustFS (target profiles).
pub mod s3 {
    use super::{Kind, ObjectsFactory};
    use crate::store::Backend;

    /// The ★ `s3` connector.
    pub const S3: ObjectsFactory = ObjectsFactory::new("s3", Kind::Store(Backend::S3));
    /// `cloudflare-r2`: the S3 code with the `cloudflare-r2` profile.
    pub const CLOUDFLARE_R2: ObjectsFactory =
        ObjectsFactory::new("cloudflare-r2", Kind::Store(Backend::S3));
    /// `minio-rustfs`: the S3 code with the `minio-rustfs` profile.
    pub const MINIO_RUSTFS: ObjectsFactory =
        ObjectsFactory::new("minio-rustfs", Kind::Store(Backend::S3));
}

/// Google Cloud Storage.
pub mod gcs {
    use super::{Kind, ObjectsFactory};
    use crate::store::Backend;

    pub const GCS: ObjectsFactory = ObjectsFactory::new("gcs", Kind::Store(Backend::Gcs));
}

/// Azure Blob Storage.
pub mod azure_blob {
    use super::{Kind, ObjectsFactory};
    use crate::store::Backend;

    pub const AZURE_BLOB: ObjectsFactory =
        ObjectsFactory::new("azure-blob", Kind::Store(Backend::Azure));
}

/// `adls-gen2` (the DFS client is in [`adls_gen2`]).
pub const ADLS_GEN2: ObjectsFactory = ObjectsFactory::new("adls-gen2", Kind::Adls);
/// `sftp`.
pub const SFTP: ObjectsFactory = ObjectsFactory::new("sftp", Kind::Sftp);

/// Every factory of the family, for `loams-flow`'s `conn-objects` feature to
/// register.
pub fn factories() -> Vec<Box<dyn ConnectorFactory>> {
    [
        s3::S3,
        s3::CLOUDFLARE_R2,
        s3::MINIO_RUSTFS,
        gcs::GCS,
        azure_blob::AZURE_BLOB,
        ADLS_GEN2,
        SFTP,
    ]
    .into_iter()
    .map(|f| Box::new(f) as Box<dyn ConnectorFactory>)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_factory_per_manifest_id() {
        let ids: Vec<&str> = factories().iter().map(|f| f.id()).collect();
        assert_eq!(
            ids,
            [
                "s3",
                "cloudflare-r2",
                "minio-rustfs",
                "gcs",
                "azure-blob",
                "adls-gen2",
                "sftp"
            ]
        );
    }
}
