//! `roundtrip` (§33 §4 rule 3): records go into the real system, through the
//! connector, and are read back; none is lost, and none is duplicated when
//! the declared delivery deduplicates.

use super::{Ctx, Outcome, dup_bound, no_loss};

pub(crate) async fn run(ctx: &Ctx<'_>) -> Outcome {
    let seeded = ctx.h.seed(ctx.opts.records).await?;
    ctx.h.start().await?;
    ctx.h.settle(ctx.opts.settle).await?;
    ctx.h.stop().await?;
    let observed = ctx.h.read_back().await?;
    no_loss(&seeded, &observed)?;
    if ctx.deduplicates() {
        dup_bound(&observed, 1)?;
    }
    Ok(vec![format!("{} records round-tripped", seeded.ids.len())])
}
