//! CloudEvents over gRPC (design §02 §7.4, D270): the ingest the service
//! calls. The protobuf format and its conversion to and from [`CloudEvent`]
//! live in `loams_cloudevents::protobuf` (plan GT1 Task 3) and are
//! re-exported here.

use async_trait::async_trait;
use loams_cloudevents::CloudEvent;
use loams_query::ServiceError;

use crate::proto::EventResult;

/// The protobuf format of CloudEvents (`io.cloudevents.v1`).
pub use loams_cloudevents::protobuf::pb as cloudevents;
pub use loams_cloudevents::protobuf::{from_json_batch, from_proto, from_proto_batch, to_proto};

/// What `ProduceCloudEvents` appends through; the server implements it with
/// the ledger-backed ingest that HTTP uses.
#[async_trait]
pub trait EventProducer: Send + Sync + 'static {
    /// Appends `events` to `ns/stream`, once per `source` + `id`; one result
    /// per event, in order.
    async fn produce_events(
        &self,
        ns: &str,
        stream: &str,
        partition: Option<u32>,
        events: Vec<CloudEvent>,
    ) -> Result<Vec<EventResult>, ServiceError>;
}
