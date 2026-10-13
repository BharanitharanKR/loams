//! `loams-connector-brokers`: Loams' first-party broker connectors (CN2 plan
//! Task 18, OR-1, OR-6, OR-22), each a [`ConnectorFactory`] on
//! `loams-connector-sdk`:
//!
//! | Manifest | Module | Protocol | Client |
//! |---|---|---|---|
//! | `rabbitmq-amqp` | [`amqp091`] | AMQP 0-9-1 | `lapin` |
//! | `activemq-jms`, `azure-service-bus` | [`amqp10`] | AMQP 1.0 | `fe2o3-amqp` (+ `fe2o3-amqp-cbs`) |
//! | `nats` | [`nats`] | NATS, JetStream | `async-nats` |
//! | `mqtt` | [`mqtt`] | MQTT 5 | this crate (R18.3) |
//! | `pulsar` | [`pulsar`] | Pulsar binary protocol | `pulsar` |
//! | `websocket` | [`websocket`] | RFC 6455 | `tokio-tungstenite` |
//!
//! Every source acknowledges upstream only after the Fabric acknowledges
//! (`Source::committed`), so a crash redelivers rather than loses. Every
//! sink writes CloudEvents in binary mode, and every source passes an
//! upstream CloudEvent through with its `type`, `source` and `id`
//! ([`mapping`]). The plan's deviations are recorded as R18.x in
//! `docs/plans/2026-10-10-cn2-connectors.md`.

use std::sync::Arc;

use loams_connector_sdk::ConnectorFactory;

pub mod amqp091;
pub mod amqp10;
pub mod common;
pub mod mapping;
pub mod mqtt;
pub mod nats;
pub mod pulsar;
pub mod tls;
pub mod websocket;

pub use common::Declared;

/// Every factory this crate registers, one per manifest id.
pub fn factories() -> Vec<Arc<dyn ConnectorFactory>> {
    vec![
        Arc::new(amqp091::RabbitMq),
        Arc::new(amqp10::Amqp10::activemq()),
        Arc::new(amqp10::Amqp10::service_bus()),
        Arc::new(nats::Nats),
        Arc::new(mqtt::Mqtt),
        Arc::new(pulsar::Pulsar),
        Arc::new(websocket::WebSocket),
    ]
}

/// The manifest ids this crate serves, in `factories()` order.
pub const IDS: &[&str] = &[
    amqp091::ID,
    amqp10::ACTIVEMQ_ID,
    amqp10::SERVICE_BUS_ID,
    nats::ID,
    mqtt::ID,
    pulsar::ID,
    websocket::ID,
];

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn factories_match_ids() {
        let ids: Vec<&str> = factories().iter().map(|f| f.id()).collect();
        assert_eq!(ids, IDS);
    }
}
