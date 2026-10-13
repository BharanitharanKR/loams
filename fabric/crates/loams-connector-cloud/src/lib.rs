//! `loams-connector-cloud`: the cloud messaging family (CN2 plan Task 19,
//! with the Kinesis ★ of CN1 Task 13 per Task 2's table), on the connector
//! SDK (OR-5, OR-6).
//!
//! | Manifest | Module | Source | Sink |
//! |---|---|---|---|
//! | `aws-sqs` | [`sqs`] | long polling, visibility extended while in flight, deleted after the Fabric's ack | `SendMessageBatch`; FIFO deduplication id = `ce_id` |
//! | `aws-sns` | [`sns`] | an HTTPS subscription on the webhooks route: signatures verified, subscription confirmed | `PublishBatch` with `ce_` message attributes |
//! | `eventbridge` | [`eventbridge`] | a rule targeting an SQS queue, read as SQS | `PutEvents`, retrying only failed entries |
//! | `kinesis` ★ | [`kinesis`] | shards as splits, parents before children, sequence numbers as positions | `PutRecords`, retrying only failed records |
//! | `google-pub-sub` | [`pubsub`] | pull, acked after the Fabric's ack, ack deadline extended | `publish` with `ce-` attributes and ordering keys |
//! | `azure-event-grid` | [`event_grid`] | a webhook with Event Grid's validation handshakes, or a namespace topic's pull delivery | the CloudEvents schema, SAS key or Entra ID |
//!
//! Every service is spoken over its documented HTTP protocol through
//! `sdk::http::client`, so each connection is egress-checked after DNS (CN2
//! R19.1). Events are `message` for queues and topics, `record` for Kinesis
//! and `event` for EventBridge and Event Grid; input that already is a
//! CloudEvent (binary `ce_`/`ce-` attributes or the structured JSON form)
//! passes through with its `type`, `source` and `id` (design §33 §6).
//!
//! The queue clients ([`sqs::SqsQueue`], [`pubsub::PubSubSubscription`],
//! [`event_grid::NamespaceQueue`]) implement [`queue::Queue`], whose
//! `receive` and `ack` have the shape of the object-storage family's
//! `NotificationFeed` (CN2 R13.4); see R19.4 for how the two connect.

use std::sync::Arc;

use loams_connector_sdk::ConnectorFactory;

pub mod aws;
pub mod ce;
pub mod config;
pub mod crypto;
pub mod event_grid;
pub mod eventbridge;
pub mod gcp;
pub mod kinesis;
pub mod pubsub;
pub mod push;
pub mod queue;
pub mod sink;
pub mod sns;
pub mod sqs;

/// The manifest ids this crate serves, in [`factories`] order.
pub const CONNECTOR_IDS: &[&str] = &[
    sqs::ID,
    sns::ID,
    eventbridge::ID,
    kinesis::ID,
    pubsub::ID,
    event_grid::ID,
];

/// One factory per manifest, for `loams-flow`'s `conn-cloud` feature
/// (OR-6). The SNS and Event Grid factories also hand out the webhook
/// handlers of their push sources (`webhook(instance)`), which the webhooks
/// route (CN2 Task 8) calls.
pub fn factories() -> Vec<Arc<dyn ConnectorFactory>> {
    vec![
        Arc::new(sqs::SqsFactory::new()),
        Arc::new(sns::SnsFactory::new()),
        Arc::new(eventbridge::EventBridgeFactory::new()),
        Arc::new(kinesis::KinesisFactory::new()),
        Arc::new(pubsub::PubSubFactory::new()),
        Arc::new(event_grid::EventGridFactory::new()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factories_cover_the_manifest_ids() {
        let ids: Vec<&str> = factories().iter().map(|f| f.id()).collect();
        assert_eq!(ids, CONNECTOR_IDS);
    }
}
