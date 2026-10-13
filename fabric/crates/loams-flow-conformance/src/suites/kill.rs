//! `kill-restart` (§33 §4 rule 3): the connector's task is killed mid-run,
//! at a fault point of the delivery path when the harness has one, and a new
//! task resumes from the checkpoint. Nothing is lost (unless the connector
//! declares `at_most_once`).

use std::time::Duration;

use super::{Ctx, Fail, Outcome, no_loss};
use crate::{FaultPoint, HarnessError, KillTarget, Observed, Seeded};

/// Where a kill lands, in order of preference: mid-batch in a sink, then
/// after the Fabric ack or before it in a source, then before the Fabric
/// commit in a sink.
const KILL_POINTS: &[FaultPoint] = &[
    FaultPoint::MidBatch,
    FaultPoint::AfterFabricAckBeforeUpstreamCommit,
    FaultPoint::BeforeFabricAck,
    FaultPoint::AfterCommitBeforeFabricCommit,
];

/// Seed, start, kill at the first fault point the harness supports (or
/// after a short run), restart, settle, and read back.
pub(crate) async fn crash_and_restart(
    ctx: &Ctx<'_>,
) -> Result<(Seeded, Vec<Observed>, Vec<String>), Fail> {
    let seeded = ctx.h.seed(ctx.opts.records).await?;
    let mut notes = Vec::new();
    let mut armed = None;
    for p in KILL_POINTS {
        match ctx.h.inject(*p).await {
            Ok(()) => {
                armed = Some(*p);
                break;
            }
            Err(HarnessError::Unsupported(m)) => {
                notes.push(format!("{p:?} does not apply: {m}"));
            }
            Err(e) => return Err(e.into()),
        }
    }
    ctx.h.start().await?;
    match armed {
        Some(p) => {
            if !ctx.h.tripped(ctx.opts.fault).await? {
                return Err(Fail::Failed(format!(
                    "the connector never reached {p:?} within {:?}",
                    ctx.opts.fault
                )));
            }
            notes.push(format!("killed at {p:?}"));
        }
        None => {
            tokio::time::sleep(Duration::from_millis(20)).await;
            notes.push("killed after 20 ms (no fault point applies)".to_owned());
        }
    }
    ctx.h.kill(KillTarget::Task).await?;
    ctx.h.start().await?;
    ctx.h.settle(ctx.opts.settle).await?;
    ctx.h.stop().await?;
    let observed = ctx.h.read_back().await?;
    Ok((seeded, observed, notes))
}

pub(crate) async fn run(ctx: &Ctx<'_>) -> Outcome {
    let (seeded, observed, mut notes) = crash_and_restart(ctx).await?;
    if ctx.delivery() == "at_most_once" {
        notes.push("at_most_once: loss is allowed and not checked".to_owned());
    } else {
        no_loss(&seeded, &observed)?;
        notes.push(format!("{} records, none lost", seeded.ids.len()));
    }
    Ok(notes)
}
