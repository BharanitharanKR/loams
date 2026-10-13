//! terminals.proto.

use loams_agentd_proto as rs;

use super::{MapError, Wire};
use crate::proto::loams::agentd::v1 as pb;
use crate::proto::loams::agentd::v1::__buffa::oneof::terminal_event::Event;

wire_struct!(rs::TerminalSession => pb::TerminalSession {
    id => id,
    cwd => cwd,
    shell => shell,
});

impl Wire for rs::TerminalEvent {
    type Pb = pb::TerminalEvent;
    fn to_pb(&self) -> Self::Pb {
        let event = match self {
            Self::Data { seq, data } => Event::from(pb::TerminalData {
                seq: *seq,
                data: data.clone(),
                ..Default::default()
            }),
            Self::Exit {
                seq,
                exit_code,
                signal,
            } => Event::from(pb::TerminalExit {
                seq: *seq,
                exit_code: *exit_code,
                signal: signal.clone(),
                ..Default::default()
            }),
        };
        pb::TerminalEvent {
            event: Some(event),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        match pb.event {
            Some(Event::Data(data)) => Ok(Self::Data {
                seq: data.seq,
                data: data.data,
            }),
            Some(Event::Exit(exit)) => Ok(Self::Exit {
                seq: exit.seq,
                exit_code: exit.exit_code,
                signal: exit.signal,
            }),
            None => Err(MapError::empty_oneof("TerminalEvent.event")),
        }
    }
}
