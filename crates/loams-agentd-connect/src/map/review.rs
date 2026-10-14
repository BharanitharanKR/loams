//! review.proto (§53 §6.5 G8–G11, SD1 Task 11) and files.proto's content
//! search (G23).

use loams_agentd_proto as rs;

use super::{MapError, Wire};
use crate::proto::loams::agentd::v1 as pb;
use crate::proto::loams::agentd::v1::__buffa::oneof::watch_review_comments_response::Frame;

wire_enum!(rs::DiffSide => pb::DiffSide {
    Old => DIFF_SIDE_OLD,
    New => DIFF_SIDE_NEW,
});

wire_struct!(rs::ReviewComment => pb::ReviewComment {
    id => id,
    checkout_id => checkout_id,
    path => path,
    side => side,
    line => line,
    end_line => end_line,
    body => body,
    diff_checksum => diff_checksum,
    created_at_ms => created_at_ms,
    resolved => resolved,
    outdated => outdated,
});

wire_struct!(rs::WatchReviewCommentsParams => pb::WatchReviewCommentsRequest {
    checkout_id => checkout_id,
});

impl Wire for rs::ReviewCommentsFrame {
    type Pb = pb::WatchReviewCommentsResponse;
    fn to_pb(&self) -> Self::Pb {
        let frame = match self {
            rs::ReviewCommentsFrame::Reset { reset } => Frame::from(pb::ReviewCommentsReset {
                comments: reset.to_pb(),
                ..Default::default()
            }),
            rs::ReviewCommentsFrame::Delta { upsert, remove } => {
                Frame::from(pb::ReviewCommentsDelta {
                    upsert: upsert.to_pb(),
                    remove: remove.clone(),
                    ..Default::default()
                })
            }
        };
        pb::WatchReviewCommentsResponse {
            frame: Some(frame),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        match pb.frame {
            Some(Frame::Reset(reset)) => Ok(rs::ReviewCommentsFrame::Reset {
                reset: Wire::from_pb(reset.comments)?,
            }),
            Some(Frame::Delta(delta)) => {
                let delta = *delta;
                Ok(rs::ReviewCommentsFrame::Delta {
                    upsert: Wire::from_pb(delta.upsert)?,
                    remove: delta.remove,
                })
            }
            None => Err(MapError::empty_oneof("WatchReviewCommentsResponse.frame")),
        }
    }
}

wire_struct!(rs::DeleteReviewCommentParams => pb::DeleteReviewCommentRequest {
    checkout_id => checkout_id,
    id => id,
});

wire_struct!(rs::ResolveReviewCommentParams => pb::ResolveReviewCommentRequest {
    checkout_id => checkout_id,
    id => id,
    resolved => resolved,
});

wire_struct!(rs::SendReviewToThreadParams => pb::SendReviewToThreadRequest {
    chat_id => chat_id,
    comment_ids => comment_ids,
});

wire_enum!(rs::ReviewEvent => pb::ReviewEvent {
    Comment => REVIEW_EVENT_COMMENT,
    RequestChanges => REVIEW_EVENT_REQUEST_CHANGES,
});

wire_struct!(rs::PostReviewToChangeRequestParams => pb::PostReviewToChangeRequestRequest {
    checkout_id => checkout_id,
    comment_ids => comment_ids,
    event => event,
});

wire_struct!(rs::PostReviewToChangeRequestReply => pb::PostReviewToChangeRequestResponse {
    url => url,
});

wire_struct!(rs::SearchWorkspaceContentParams => pb::SearchContentRequest {
    space_id => space_id,
    chat_id => chat_id,
    query => query,
    regex => regex,
    case_sensitive => case_sensitive,
    globs => globs,
    max_results => max_results,
});

wire_struct!(rs::ContentSearchMatch => pb::SearchContentResponse {
    path => path,
    line => line,
    column => column,
    preview => preview,
});
