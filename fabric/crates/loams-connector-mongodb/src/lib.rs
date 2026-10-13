//! `loams-connector-mongodb`: the MongoDB connector (CN2 Task 16, OR-6,
//! OR-9), on the connector SDK.
//!
//! | Direction | How | Delivery |
//! |---|---|---|
//! | batch source (`mode: batch`) | `_id`-range splits read at one cluster time ([`batch`]) | at least once across a restart (stable ids within a run) |
//! | CDC source (`mode: cdc`) | snapshot at T, then a change stream from just after T ([`cdc`]) | effectively once (ids from resume tokens) |
//! | sink (`collection` set) | ordered `update`/`delete`/`insert` commands ([`sink`]) | exactly once in a transaction with `_loams_checkpoints` on replica sets; effectively once (`upsert`) or at least once (`insert`) on a standalone server |
//!
//! It speaks the wire protocol itself ([`wire`], [`bson`], [`scram`]) over
//! connections `loams_connector_sdk::net` opens, so every address it dials
//! (a seed, or a primary a secondary names) passes the instance's egress
//! check after DNS, and TLS is the SDK's rustls with ring (plan ruling
//! R16.1). Secrets are resolved by name through the instance's
//! `SecretResolver` at connect time.

pub mod batch;
pub mod bson;
pub mod cdc;
pub mod client;
pub mod config;
#[cfg(feature = "conformance")]
pub mod conformance;
pub mod decimal;
pub mod extjson;
pub mod mongodb;
pub mod scan;
pub mod scram;
pub mod sink;
#[cfg(test)]
mod testkit;
pub mod types;
pub mod wire;

pub use config::{Mode, MongoConfig, Write};
pub use mongodb::MongoFactory;

/// Every factory of the family, for the runtime's registry (Task 8 links
/// them into `loams-flow` behind `conn-mongodb`).
pub fn factories() -> Vec<Box<dyn loams_connector_sdk::ConnectorFactory>> {
    vec![Box::new(MongoFactory)]
}

/// `N` bytes from the operating system's RNG (ring's `SystemRandom`).
pub fn random_bytes<const N: usize>() -> [u8; N] {
    use ring::rand::{SecureRandom, SystemRandom};
    let mut b = [0u8; N];
    SystemRandom::new()
        .fill(&mut b)
        .expect("the operating system's random number generator failed");
    b
}
