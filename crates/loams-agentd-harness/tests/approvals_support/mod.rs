//! The approval bridge of a supervised run, for the driver tests (plan SD1
//! Task 13): every ask lands on a channel with its answer's sender, so a test
//! decides when, and how, each tool call is answered.

#![allow(dead_code)]

use std::time::Duration;

use loams_agentd_harness::{ApprovalAsk, RequestApproval};
use loams_agentd_proto::approvals::ApprovalDecision;
use tokio::sync::{mpsc, oneshot};

/// One ask and the sender that answers it.
pub type Pending = (ApprovalAsk, oneshot::Sender<ApprovalDecision>);

/// A bridge whose asks arrive on the returned receiver.
pub fn bridge() -> (RequestApproval, mpsc::UnboundedReceiver<Pending>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let request: RequestApproval = Box::new(move |ask: ApprovalAsk| {
        let (answer_tx, answer_rx) = oneshot::channel();
        let _ = tx.send((ask, answer_tx));
        answer_rx
    });
    (request, rx)
}

/// The next ask, within ten seconds.
pub async fn next_ask(asks: &mut mpsc::UnboundedReceiver<Pending>) -> Pending {
    tokio::time::timeout(Duration::from_secs(10), asks.recv())
        .await
        .expect("the driver asked in time")
        .expect("the bridge is open")
}
