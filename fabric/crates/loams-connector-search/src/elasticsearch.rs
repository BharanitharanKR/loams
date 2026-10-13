//! The Elasticsearch connector factory (`runtime.ref:
//! loams_connector_search::elasticsearch`), which also serves every target
//! profile whose `module` is `search` (OpenSearch, OR-2). A profile's
//! `defaults` are merged under the instance config (OpenSearch's set `api:
//! opensearch`), and a profile that narrows a direction removes it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, Profile, ProfileKind, SinkFactory,
    SourceFactory, SplitEnumerator,
};
use serde_json::Value;

use crate::bulk::BulkSinkFactory;
use crate::config::{Api, EsConfig};
use crate::pit::{IndexEnumerator, PitSourceFactory};

/// The family's module name in profiles (`module: search`).
pub const MODULE: &str = "search";

/// See the module documentation.
#[derive(Debug, Clone, Copy)]
pub struct Elasticsearch {
    id: &'static str,
    /// The API a config without `api` speaks.
    default_api: Api,
}

impl Default for Elasticsearch {
    fn default() -> Self {
        Self::new()
    }
}

/// A profile id as a `&'static str`, interned once per id.
pub(crate) fn intern(id: &str) -> &'static str {
    static IDS: OnceLock<Mutex<BTreeMap<String, &'static str>>> = OnceLock::new();
    let mut ids = IDS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if let Some(s) = ids.get(id) {
        return s;
    }
    let s: &'static str = Box::leak(id.to_owned().into_boxed_str());
    ids.insert(id.to_owned(), s);
    s
}

pub(crate) fn narrows(p: &Profile, path: &str) -> bool {
    p.narrows.iter().any(|n| n == path)
}

fn check_profile(p: &Profile) -> Result<(), ConnectorError> {
    if p.kind != ProfileKind::Target || p.module != MODULE {
        return Err(ConnectorError::fatal(format!(
            "profile {} is not a target profile of the {MODULE} family",
            p.id
        )));
    }
    Ok(())
}

impl Elasticsearch {
    /// Elasticsearch itself (`elasticsearch`).
    pub const fn new() -> Self {
        Self {
            id: "elasticsearch",
            default_api: Api::Elasticsearch,
        }
    }

    /// OpenSearch (`opensearch`), for a runtime that has no profile at hand;
    /// [`Self::for_profile`] is the usual way.
    pub const fn opensearch() -> Self {
        Self {
            id: "opensearch",
            default_api: Api::Opensearch,
        }
    }

    /// The factory of a target profile on this family; its id is the
    /// profile's, which is the manifest's.
    pub fn for_profile(profile: &Profile) -> Result<Self, ConnectorError> {
        check_profile(profile)?;
        let default_api = match profile.defaults.get("api").and_then(Value::as_str) {
            Some("opensearch") => Api::Opensearch,
            _ => Api::Elasticsearch,
        };
        Ok(Self {
            id: intern(&profile.id),
            default_api,
        })
    }

    /// The instance config with the profile's defaults under it and the
    /// factory's API filled in.
    pub fn config(&self, ctx: &ConnectorContext) -> Result<EsConfig, ConnectorError> {
        let mut config = match &ctx.profile {
            Some(p) => {
                check_profile(p)?;
                p.config_with_defaults(&ctx.config)
            }
            None => ctx.config.clone(),
        };
        if let Value::Object(m) = &mut config
            && !m.contains_key("api")
            && self.default_api == Api::Opensearch
        {
            m.insert("api".to_owned(), Value::String("opensearch".to_owned()));
        }
        EsConfig::parse(&config)
    }
}

impl ConnectorFactory for Elasticsearch {
    fn id(&self) -> &'static str {
        self.id
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        if ctx.profile.as_ref().is_some_and(|p| narrows(p, "source")) {
            return Ok(None);
        }
        let cfg = Arc::new(self.config(ctx)?);
        Ok(Some((
            Box::new(IndexEnumerator {
                index: cfg.index.clone(),
            }),
            Box::new(PitSourceFactory {
                cfg,
                ctx: ctx.clone(),
                connector: self.id,
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        if ctx.profile.as_ref().is_some_and(|p| narrows(p, "sink")) {
            return Ok(None);
        }
        Ok(Some(Box::new(BulkSinkFactory {
            cfg: Arc::new(self.config(ctx)?),
            ctx: ctx.clone(),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interned_ids_are_shared() {
        let a = intern("opensearch-x");
        let b = intern("opensearch-x");
        assert!(std::ptr::eq(a, b));
    }
}
