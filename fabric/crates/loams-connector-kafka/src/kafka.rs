//! The [`ConnectorFactory`] of the Kafka-protocol family: `kafka` itself, and
//! every target profile whose `module` is `kafka` (Redpanda, Azure Event
//! Hubs). A profile's `defaults` are merged under the instance config, and
//! its `narrows` are enforced: a narrowed direction is not offered, and
//! `delivery: exactly_once` is refused when the profile narrows
//! `sink.transactional`.

use std::collections::BTreeSet;
use std::sync::{Mutex, OnceLock};

use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, Delivery, Profile, ProfileKind,
    SinkFactory, SourceFactory, SplitEnumerator,
};

use crate::config::{Connection, KafkaConfig, SinkDelivery};
use crate::sink::KafkaSinkFactory;
use crate::source::{KafkaEnumerator, KafkaSourceFactory};

/// The family's module name in profiles (`module: kafka`).
pub const MODULE: &str = "kafka";

/// The Kafka-protocol connector factory.
#[derive(Debug, Clone, Copy)]
pub struct Kafka {
    id: &'static str,
}

impl Default for Kafka {
    fn default() -> Self {
        Self::new()
    }
}

/// Profile ids as `&'static str`, interned once each (the factory's `id`).
fn intern(id: &str) -> &'static str {
    static IDS: OnceLock<Mutex<BTreeSet<&'static str>>> = OnceLock::new();
    let mut ids = IDS
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if let Some(found) = ids.get(id) {
        return found;
    }
    let leaked: &'static str = Box::leak(id.to_owned().into_boxed_str());
    ids.insert(leaked);
    leaked
}

impl Kafka {
    /// The `kafka` manifest's factory.
    pub fn new() -> Self {
        Self { id: "kafka" }
    }

    /// The factory of a target profile on this family (`redpanda`,
    /// `azure-event-hubs`); its id is the profile's, which is the manifest's.
    pub fn for_profile(profile: &Profile) -> Result<Self, ConnectorError> {
        check_profile(profile)?;
        Ok(Self {
            id: intern(&profile.id),
        })
    }

    /// The strongest sink delivery an instance may declare under `profile`:
    /// `exactly_once` through transactions, unless the profile narrows them
    /// (Event Hubs, R0.19), when the idempotent producer gives
    /// `effectively_once`.
    pub fn sink_delivery(profile: Option<&Profile>) -> Delivery {
        if profile.is_some_and(|p| narrows(p, "sink.transactional")) {
            Delivery::EffectivelyOnce
        } else {
            Delivery::ExactlyOnce
        }
    }

    fn connection(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<std::sync::Arc<Connection>, ConnectorError> {
        let config = match &ctx.profile {
            Some(p) => {
                check_profile(p)?;
                p.config_with_defaults(&ctx.config)
            }
            None => ctx.config.clone(),
        };
        Connection::new(self.id, ctx, &config)
    }
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

fn narrows(p: &Profile, path: &str) -> bool {
    p.narrows.iter().any(|n| n == path)
}

impl ConnectorFactory for Kafka {
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
        let conn = self.connection(ctx)?;
        Ok(Some((
            Box::new(KafkaEnumerator::new(conn.clone())),
            Box::new(KafkaSourceFactory::new(conn)),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        if let Some(p) = &ctx.profile {
            if narrows(p, "sink") {
                return Ok(None);
            }
            let cfg = KafkaConfig::parse(&p.config_with_defaults(&ctx.config))?;
            if cfg.delivery == SinkDelivery::ExactlyOnce && narrows(p, "sink.transactional") {
                return Err(ConnectorError::fatal(format!(
                    "{} narrows sink.transactional: its sink is effectively_once (the idempotent producer), so delivery: exactly_once is refused",
                    p.id
                )));
            }
        }
        let conn = self.connection(ctx)?;
        Ok(Some(Box::new(KafkaSinkFactory::new(conn)?)))
    }
}
