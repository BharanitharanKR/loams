//! `NotificationService` (G19; SD1 Task 14): the ndjson `WatchNotifications`.

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};
use loams_agentd_proto as rs;

use super::{Agentd, stream};
use crate::map::Wire;
use crate::proto::loams::agentd::v1 as pb;

impl pb::NotificationService for Agentd {
    async fn watch_notifications(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchNotificationsRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchNotificationsResponse>> {
        stream!(
            self,
            request,
            WatchNotifications,
            |req| rs::Empty {},
            |notification| pb::WatchNotificationsResponse {
                notification: buffa::MessageField::some(notification.to_pb()),
                ..Default::default()
            }
        )
    }
}
