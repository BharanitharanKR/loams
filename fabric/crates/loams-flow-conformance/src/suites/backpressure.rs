//! `backpressure` (Review Focus 5): the target and the Fabric are both
//! throttled; the connector delivers everything, and its in-flight memory
//! never passes its budget.

use super::{Ctx, Fail, Outcome, no_loss};
use crate::Throttle;

pub(crate) async fn run(ctx: &Ctx<'_>) -> Outcome {
    let seeded = ctx.h.seed(ctx.opts.records).await?;
    ctx.h.throttle(Throttle::Target(ctx.opts.throttle)).await?;
    ctx.h.throttle(Throttle::Fabric(ctx.opts.throttle)).await?;
    ctx.h.start().await?;
    let settled = ctx.h.settle(ctx.opts.settle).await;
    let memory = ctx.h.memory().await;
    ctx.h.throttle(Throttle::Off).await?;
    settled?;
    let memory = memory?;
    ctx.h.stop().await?;
    let observed = ctx.h.read_back().await?;
    no_loss(&seeded, &observed)?;
    if memory.peak_bytes > memory.budget_bytes {
        return Err(Fail::Failed(format!(
            "in-flight memory peaked at {} bytes, over the budget of {}",
            memory.peak_bytes, memory.budget_bytes
        )));
    }
    Ok(vec![format!(
        "{} records under throttle; peak {} of {} bytes",
        seeded.ids.len(),
        memory.peak_bytes,
        memory.budget_bytes
    )])
}
