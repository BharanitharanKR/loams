//! `exactly-once` (Review Focus 3): a crash at every [`FaultPoint`] the
//! connector has, then a restart; each time the target must equal the input
//! exactly — every record once, nothing else.

use super::{Ctx, Fail, Outcome, dup_bound, no_loss};
use crate::{FaultPoint, HarnessError, KillTarget};

pub(crate) async fn run(ctx: &Ctx<'_>) -> Outcome {
    let mut notes = Vec::new();
    let mut ran = 0;
    for p in FaultPoint::ALL {
        ctx.h.reset().await?;
        let seeded = ctx.h.seed(ctx.opts.records).await?;
        match ctx.h.inject(*p).await {
            Ok(()) => {}
            Err(HarnessError::Unsupported(m)) => {
                notes.push(format!("{p:?} does not apply: {m}"));
                continue;
            }
            Err(e) => return Err(e.into()),
        }
        ctx.h.start().await?;
        if !ctx.h.tripped(ctx.opts.fault).await? {
            return Err(Fail::Failed(format!(
                "the connector never reached {p:?} within {:?}",
                ctx.opts.fault
            )));
        }
        ctx.h.kill(KillTarget::Task).await?;
        ctx.h.start().await?;
        ctx.h.settle(ctx.opts.settle).await?;
        ctx.h.stop().await?;
        let observed = ctx.h.read_back().await?;
        let at = |f: Fail| match f {
            Fail::Failed(m) => Fail::Failed(format!("after a crash at {p:?}: {m}")),
            other => other,
        };
        no_loss(&seeded, &observed).map_err(at)?;
        dup_bound(&observed, 1).map_err(at)?;
        ran += 1;
        notes.push(format!("{p:?}: {} records, exactly once", seeded.ids.len()));
    }
    if ran == 0 {
        return Err(Fail::Skipped("no fault point applies".to_owned()));
    }
    Ok(notes)
}
