//! `contract`: exercises each declared capability. The basic flow runs in
//! small; each capability whose check lives in another suite is proven by
//! that suite — in this run when it was requested, else here, in small. A
//! capability with no registered check fails the contract.

use std::collections::BTreeSet;

use super::{Ctx, Fail, Outcome, no_loss, run_suite};
use crate::capabilities::{capability_checks, unchecked_capabilities};
use crate::{Suite, SuiteOutcome};

pub(crate) async fn run(ctx: &Ctx<'_>) -> Outcome {
    let unchecked = unchecked_capabilities(ctx.spec, ctx.profile);
    if !unchecked.is_empty() {
        return Err(Fail::Failed(format!(
            "no check is registered for: {}",
            unchecked.join(", ")
        )));
    }
    let mut notes = Vec::new();

    let seeded = ctx.h.seed(ctx.opts.smoke_records).await?;
    ctx.h.start().await?;
    ctx.h.settle(ctx.opts.settle).await?;
    ctx.h.stop().await?;
    no_loss(&seeded, &ctx.h.read_back().await?)?;
    notes.push(format!("basic flow: {} records", seeded.ids.len()));

    let checks = capability_checks(ctx.spec, ctx.profile);
    let others: BTreeSet<Suite> = checks
        .iter()
        .map(|c| c.suite)
        .filter(|s| !matches!(s, Suite::Contract | Suite::Roundtrip))
        .collect();
    let smoke = ctx.opts.smoke();
    let small = Ctx {
        spec: ctx.spec,
        profile: ctx.profile,
        h: ctx.h,
        opts: &smoke,
        requested: ctx.requested,
    };
    for suite in others {
        let caps: Vec<&str> = checks
            .iter()
            .filter(|c| c.suite == suite)
            .map(|c| c.capability.as_str())
            .collect();
        if ctx.requested.contains(&suite) {
            notes.push(format!(
                "{}: proven by {suite} in this run",
                caps.join(", ")
            ));
            continue;
        }
        let r = Box::pin(run_suite(&small, suite)).await;
        match r.outcome {
            SuiteOutcome::Passed => {
                notes.push(format!("{}: {suite} passed in small", caps.join(", ")));
            }
            SuiteOutcome::Failed(m) => {
                return Err(Fail::Failed(format!(
                    "{}: {suite} failed: {m}",
                    caps.join(", ")
                )));
            }
            SuiteOutcome::Skipped(m) => {
                return Err(Fail::Skipped(format!(
                    "{}: {suite} could not run: {m}",
                    caps.join(", ")
                )));
            }
        }
    }
    Ok(notes)
}
