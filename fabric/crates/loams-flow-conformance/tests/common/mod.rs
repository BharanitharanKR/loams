//! Helpers shared by the kit's tests.
#![allow(dead_code)]

use std::time::Duration;

use async_trait::async_trait;
use loams_flow::manifest::repository_root;
use loams_flow::{AuthMethod, ConnectorSpec, Delivery, Evolution, Registry};
use loams_flow_conformance::{
    ConnectorHarness, FaultPoint, HarnessError, KillTarget, Observed, RunOptions, Seeded,
    ServiceKind,
};

pub fn registry() -> Registry {
    Registry::load(&repository_root().join("connectors/registry")).expect("the registry loads")
}

/// The manifest the reference connector is tested as: Postgres's shape (a
/// resumable batch source; a transactional, upserting sink), renamed, with
/// exactly-once sink delivery, no bulk path, no schema evolution and no
/// auth.
pub fn reference_spec() -> ConnectorSpec {
    let mut spec = registry()
        .get("postgresql")
        .expect("postgresql manifest")
        .clone();
    spec.id = "reference".to_owned();
    spec.name = "Reference connector".to_owned();
    spec.auth = vec![AuthMethod::None];
    spec.capabilities.bulk.arrow = false;
    spec.capabilities.schema.evolution = Evolution::None;
    spec.capabilities.delivery.sink = Delivery::ExactlyOnce;
    // Task 2 gave `postgresql` native CDC (R2.11); the reference connector
    // has none.
    if let Some(source) = spec.capabilities.source.as_mut() {
        source.cdc = false;
    }
    spec
}

/// Small and quick.
pub fn opts() -> RunOptions {
    RunOptions {
        records: 40,
        smoke_records: 10,
        bulk_rows: 1_000,
        smoke_bulk_rows: 100,
        settle: Duration::from_secs(30),
        fault: Duration::from_secs(10),
        throttle: Duration::from_millis(1),
    }
}

/// A harness that does nothing, for the checks that run before any suite.
#[derive(Debug)]
pub struct StubHarness(pub &'static [ServiceKind]);

#[async_trait]
impl ConnectorHarness for StubHarness {
    fn services(&self) -> &[ServiceKind] {
        self.0
    }
    async fn seed(&self, _n: usize) -> Result<Seeded, HarnessError> {
        Ok(Seeded::default())
    }
    async fn read_back(&self) -> Result<Vec<Observed>, HarnessError> {
        Ok(Vec::new())
    }
    async fn kill(&self, _what: KillTarget) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn inject(&self, _at: FaultPoint) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported("stub".into()))
    }
    async fn reset(&self) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn start(&self) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn settle(&self, _timeout: Duration) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn tripped(&self, _timeout: Duration) -> Result<bool, HarnessError> {
        Ok(false)
    }
}

/// A harness whose connector replays every record once after a crash (two
/// copies of each), testing `direction`: the bound `dup-check` applies comes
/// from the manifest's delivery for that direction.
#[derive(Debug)]
pub struct ReplayingHarness {
    pub services: &'static [ServiceKind],
    pub direction: Option<loams_flow::Direction>,
    pub seeded: std::sync::Mutex<usize>,
}

#[async_trait]
impl ConnectorHarness for ReplayingHarness {
    fn services(&self) -> &[ServiceKind] {
        self.services
    }
    fn direction(&self) -> Option<loams_flow::Direction> {
        self.direction
    }
    async fn seed(&self, n: usize) -> Result<Seeded, HarnessError> {
        let mut s = self.seeded.lock().unwrap();
        *s += n;
        Ok(Seeded {
            ids: (0..*s).map(|i| i.to_string()).collect(),
        })
    }
    async fn read_back(&self) -> Result<Vec<Observed>, HarnessError> {
        let n = *self.seeded.lock().unwrap();
        Ok((0..n)
            .map(|i| Observed {
                id: i.to_string(),
                copies: 2,
            })
            .collect())
    }
    async fn kill(&self, _what: KillTarget) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn inject(&self, _at: FaultPoint) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported("replaying".into()))
    }
    async fn reset(&self) -> Result<(), HarnessError> {
        *self.seeded.lock().unwrap() = 0;
        Ok(())
    }
    async fn start(&self) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn settle(&self, _timeout: Duration) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn tripped(&self, _timeout: Duration) -> Result<bool, HarnessError> {
        Ok(false)
    }
}
