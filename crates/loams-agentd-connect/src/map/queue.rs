//! queue.proto: queued messages and their edit leases.

use loams_agentd_proto as rs;

use super::{MapError, Wire, from_opt_msg, opt_msg};
use crate::proto::loams::agentd::v1 as pb;
use crate::proto::loams::agentd::v1::__buffa::oneof::{
    begin_queue_edit_outcome::Outcome as BeginOutcome,
    finish_queue_edit_outcome::Outcome as FinishOutcome, queue_delivery_gate::Gate,
    renew_queue_edit_outcome::Outcome as RenewOutcome,
};

fn no_detail() -> pb::QueueEditNoDetail {
    pb::QueueEditNoDetail::default()
}

wire_struct!(rs::QueuedMessage => pb::QueuedMessage {
    id => id,
    text => text,
    attachments => attachments,
    hold_for_turn_end => hold_for_turn_end,
    issued_by => issued_by,
    issued_at => issued_at_ms,
    edited_at => edited_at_ms,
    delivery_gate => delivery_gate as opt_msg,
});

impl Wire for rs::QueueDeliveryGate {
    type Pb = pb::QueueDeliveryGate;
    fn to_pb(&self) -> Self::Pb {
        let gate = match self {
            Self::Editing {
                lease_id,
                owner_device_id,
                owner_instance_id,
                acquired_at_ms,
                expires_at_ms,
                base_text_hash,
            } => Gate::from(pb::QueueEditingGate {
                lease_id: lease_id.clone(),
                owner_device_id: owner_device_id.clone(),
                owner_instance_id: owner_instance_id.clone(),
                acquired_at_ms: *acquired_at_ms,
                expires_at_ms: *expires_at_ms,
                base_text_hash: base_text_hash.clone(),
                ..Default::default()
            }),
            Self::ReviewRequired {
                previous_lease_id,
                owner_device_id,
                since_ms,
                base_text_hash,
            } => Gate::from(pb::QueueReviewRequiredGate {
                previous_lease_id: previous_lease_id.clone(),
                owner_device_id: owner_device_id.clone(),
                since_ms: *since_ms,
                base_text_hash: base_text_hash.clone(),
                ..Default::default()
            }),
        };
        pb::QueueDeliveryGate {
            gate: Some(gate),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        match pb.gate {
            Some(Gate::Editing(gate)) => {
                let gate = *gate;
                Ok(Self::Editing {
                    lease_id: gate.lease_id,
                    owner_device_id: gate.owner_device_id,
                    owner_instance_id: gate.owner_instance_id,
                    acquired_at_ms: gate.acquired_at_ms,
                    expires_at_ms: gate.expires_at_ms,
                    base_text_hash: gate.base_text_hash,
                })
            }
            Some(Gate::ReviewRequired(gate)) => {
                let gate = *gate;
                Ok(Self::ReviewRequired {
                    previous_lease_id: gate.previous_lease_id,
                    owner_device_id: gate.owner_device_id,
                    since_ms: gate.since_ms,
                    base_text_hash: gate.base_text_hash,
                })
            }
            None => Err(MapError::empty_oneof("QueueDeliveryGate.gate")),
        }
    }
}

impl Wire for rs::BeginQueueEditOutcome {
    type Pb = pb::BeginQueueEditOutcome;
    fn to_pb(&self) -> Self::Pb {
        let outcome = match self {
            Self::Acquired {
                lease_id,
                text,
                attachments,
                base_text_hash,
                expires_at_ms,
            } => BeginOutcome::from(pb::QueueEditAcquired {
                lease_id: lease_id.clone(),
                text: text.clone(),
                attachments: attachments.clone(),
                base_text_hash: base_text_hash.clone(),
                expires_at_ms: *expires_at_ms,
                ..Default::default()
            }),
            Self::Locked {
                owner_device_id,
                expires_at_ms,
            } => BeginOutcome::from(pb::QueueEditLocked {
                owner_device_id: owner_device_id.clone(),
                expires_at_ms: *expires_at_ms,
                ..Default::default()
            }),
            Self::Missing => BeginOutcome::Missing(Box::new(no_detail())),
        };
        pb::BeginQueueEditOutcome {
            outcome: Some(outcome),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        match pb.outcome {
            Some(BeginOutcome::Acquired(acquired)) => {
                let acquired = *acquired;
                Ok(Self::Acquired {
                    lease_id: acquired.lease_id,
                    text: acquired.text,
                    attachments: acquired.attachments,
                    base_text_hash: acquired.base_text_hash,
                    expires_at_ms: acquired.expires_at_ms,
                })
            }
            Some(BeginOutcome::Locked(locked)) => Ok(Self::Locked {
                owner_device_id: locked.owner_device_id,
                expires_at_ms: locked.expires_at_ms,
            }),
            Some(BeginOutcome::Missing(_)) => Ok(Self::Missing),
            None => Err(MapError::empty_oneof("BeginQueueEditOutcome.outcome")),
        }
    }
}

impl Wire for rs::RenewQueueEditOutcome {
    type Pb = pb::RenewQueueEditOutcome;
    fn to_pb(&self) -> Self::Pb {
        let outcome = match self {
            Self::Renewed { expires_at_ms } => RenewOutcome::from(pb::QueueEditRenewed {
                expires_at_ms: *expires_at_ms,
                ..Default::default()
            }),
            Self::Lost => RenewOutcome::Lost(Box::new(no_detail())),
            Self::Missing => RenewOutcome::Missing(Box::new(no_detail())),
        };
        pb::RenewQueueEditOutcome {
            outcome: Some(outcome),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        match pb.outcome {
            Some(RenewOutcome::Renewed(renewed)) => Ok(Self::Renewed {
                expires_at_ms: renewed.expires_at_ms,
            }),
            Some(RenewOutcome::Lost(_)) => Ok(Self::Lost),
            Some(RenewOutcome::Missing(_)) => Ok(Self::Missing),
            None => Err(MapError::empty_oneof("RenewQueueEditOutcome.outcome")),
        }
    }
}

wire_enum!(rs::FinishQueuedMessageEditAction => pb::FinishQueuedMessageEditAction {
    Commit => FINISH_QUEUED_MESSAGE_EDIT_ACTION_COMMIT,
    Cancel => FINISH_QUEUED_MESSAGE_EDIT_ACTION_CANCEL,
    Discard => FINISH_QUEUED_MESSAGE_EDIT_ACTION_DISCARD,
    ReleaseUnchanged => FINISH_QUEUED_MESSAGE_EDIT_ACTION_RELEASE_UNCHANGED,
});

impl Wire for rs::FinishQueueEditOutcome {
    type Pb = pb::FinishQueueEditOutcome;
    fn to_pb(&self) -> Self::Pb {
        let unit = || Box::new(no_detail());
        let outcome = match self {
            Self::Committed => FinishOutcome::Committed(unit()),
            Self::Cancelled => FinishOutcome::Cancelled(unit()),
            Self::Discarded => FinishOutcome::Discarded(unit()),
            Self::Released => FinishOutcome::Released(unit()),
            Self::Conflict { current_text } => FinishOutcome::from(pb::QueueEditConflict {
                current_text: current_text.clone(),
                ..Default::default()
            }),
            Self::Lost => FinishOutcome::Lost(unit()),
            Self::Missing => FinishOutcome::Missing(unit()),
        };
        pb::FinishQueueEditOutcome {
            outcome: Some(outcome),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        match pb.outcome {
            Some(FinishOutcome::Committed(_)) => Ok(Self::Committed),
            Some(FinishOutcome::Cancelled(_)) => Ok(Self::Cancelled),
            Some(FinishOutcome::Discarded(_)) => Ok(Self::Discarded),
            Some(FinishOutcome::Released(_)) => Ok(Self::Released),
            Some(FinishOutcome::Conflict(conflict)) => Ok(Self::Conflict {
                current_text: conflict.current_text,
            }),
            Some(FinishOutcome::Lost(_)) => Ok(Self::Lost),
            Some(FinishOutcome::Missing(_)) => Ok(Self::Missing),
            None => Err(MapError::empty_oneof("FinishQueueEditOutcome.outcome")),
        }
    }
}

// Requests whose message is theirs alone.

wire_struct!(rs::QueueMessageParams => pb::QueueMessageRequest {
    chat_id => chat_id,
    text => text,
    attachments => attachments,
    hold_for_turn_end => hold_for_turn_end,
});

wire_struct!(rs::UpdateQueuedMessageParams => pb::UpdateQueuedMessageRequest {
    chat_id => chat_id,
    id => id,
    text => text,
});

wire_struct!(rs::BeginQueuedMessageEditParams => pb::BeginQueuedMessageEditRequest {
    chat_id => chat_id,
    id => id,
    editor_device_id => editor_device_id,
    editor_instance_id => editor_instance_id,
});

wire_struct!(rs::RenewQueuedMessageEditParams => pb::RenewQueuedMessageEditRequest {
    chat_id => chat_id,
    id => id,
    lease_id => lease_id,
});

wire_struct!(rs::MoveQueuedMessageParams => pb::MoveQueuedMessageRequest {
    chat_id => chat_id,
    id => id,
    to_index => to_index,
});

impl Wire for rs::FinishQueuedMessageEditParams {
    type Pb = pb::FinishQueuedMessageEditRequest;
    fn to_pb(&self) -> Self::Pb {
        let rs::FinishQueuedMessageEditParams {
            chat_id,
            id,
            lease_id,
            action,
            text,
            expected_text_hash,
            attachments,
        } = self;
        pb::FinishQueuedMessageEditRequest {
            chat_id: chat_id.clone(),
            id: id.clone(),
            lease_id: lease_id.clone(),
            action: action.to_pb(),
            text: text.clone(),
            expected_text_hash: expected_text_hash.clone(),
            attachments: opt_msg(&attachments.clone().map(QueueAttachments)),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        Ok(rs::FinishQueuedMessageEditParams {
            chat_id: pb.chat_id,
            id: pb.id,
            lease_id: pb.lease_id,
            action: Wire::from_pb(pb.action)?,
            text: pb.text,
            expected_text_hash: pb.expected_text_hash,
            attachments: from_opt_msg::<QueueAttachments, _>(pb.attachments)?
                .map(|attachments| attachments.0),
        })
    }
}

/// `Option<Vec<String>>` on the ndjson wire: a wrapper keeps presence.
struct QueueAttachments(Vec<String>);

impl Wire for QueueAttachments {
    type Pb = pb::QueueAttachments;
    fn to_pb(&self) -> Self::Pb {
        pb::QueueAttachments {
            paths: self.0.clone(),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        Ok(Self(pb.paths))
    }
}
