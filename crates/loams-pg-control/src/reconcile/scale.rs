//! The autoscaler's applier (design §46 §7.4; PG2 Task 15): every
//! [`TICK`](crate::autoscale::TICK) it samples the running computes the
//! endpoint reconciler follows ([`Scaling`]), asks the policy, and applies
//! each decision under the project's lease:
//!
//! - **Up:** the runtime's resize, then `/configure` with the larger
//!   `neon.file_cache_size_limit`.
//! - **Down:** `/configure` with the smaller cache first, then the resize,
//!   so the cache never outgrows the memory; a resize that does not apply
//!   puts the cache back.
//!
//! An applied resize writes the compute's new size (and the version of
//! the spec it now runs) in its `C/` record, and the lifecycle observer
//! hears `Resized`. A refused resize is logged and asked again at the next
//! tick; a runtime that cannot resize (compose) is not asked again for
//! that compute. A compute that is no longer its endpoint's running one,
//! or whose project's lease another instance holds, is no longer followed
//! here (the reconciler that runs it follows it).

use futures::future::join_all;

use super::lease::Taken;
use super::{Act, Ctx, ReconcileError};
use crate::autoscale::{Decision, Observation, Scaling, Tracked};
use crate::ids::ComputeId;
use crate::model::{ComputeKey, ComputeRec, ComputeStatus, Cu, EndpointKey, EndpointRec};
use crate::neon::NeonWrite;
use crate::observer::LifecycleKind;
use crate::reconcile::endpoint::Computes;
use crate::runtime::ResizeOutcome;
use crate::store::{Batch, PgControlStore, Versioned};
use crate::upcall::spec::{self, SpecError};

/// What one autoscaler tick came to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScaleTick {
    /// Computes sampled.
    pub sampled: usize,
    /// Decisions the policy took.
    pub decided: usize,
    /// Resizes applied.
    pub applied: usize,
    /// Resizes the runtime refused (asked again at the next tick).
    pub refused: usize,
}

/// What applying one decision came to.
enum Outcome {
    Resized,
    Refused,
    /// Not applied, and not counted: the runtime cannot resize, the
    /// compute moved on, or another instance runs it.
    Skipped,
}

/// One tick (see the module docs).
pub(crate) async fn tick<S: PgControlStore, N: NeonWrite>(
    ctx: &Ctx<S, N>,
    computes: &Computes,
    scaling: &Scaling,
) -> ScaleTick {
    let tracked = scaling.tracked();
    let samples = join_all(
        tracked
            .iter()
            .map(|t| scaling.metrics.sample(&t.namespace, &t.compute)),
    )
    .await;
    let observations: Vec<Observation> = tracked
        .iter()
        .zip(samples)
        .map(|(t, s)| Observation {
            namespace: t.namespace.clone(),
            compute: t.compute.id.clone(),
            cu: t.compute.cu,
            min_cu: t.min_cu,
            max_cu: t.max_cu,
            sample: s.unwrap_or_else(|e| {
                tracing::debug!(compute = %t.compute.id, error = %e, "no sample");
                crate::autoscale::Sample::default()
            }),
        })
        .collect();
    let decisions = scaling.decide(ctx.now_ms(), &observations);
    let mut out = ScaleTick {
        sampled: observations.len(),
        decided: decisions.len(),
        ..ScaleTick::default()
    };
    for d in decisions {
        let Some(t) = tracked.iter().find(|t| t.compute.id == d.compute) else {
            continue;
        };
        match apply(ctx, computes, scaling, t, &d).await {
            Ok(Outcome::Resized) => out.applied += 1,
            Ok(Outcome::Refused) => out.refused += 1,
            Ok(Outcome::Skipped) => {}
            Err(e) => {
                tracing::warn!(compute = %d.compute, from = d.from.0, to = d.to.0, error = %e, "the resize did not finish; retrying");
            }
        }
    }
    out
}

async fn apply<S: PgControlStore, N: NeonWrite>(
    ctx: &Ctx<S, N>,
    computes: &Computes,
    scaling: &Scaling,
    t: &Tracked,
    d: &Decision,
) -> Result<Outcome, ReconcileError> {
    let fence = match ctx.leases.take(&ctx.store, &t.project_id).await? {
        Taken::Held(fence) => fence,
        Taken::Other(_) => {
            scaling.forget(&d.compute);
            return Ok(Outcome::Skipped);
        }
    };
    let act = Act {
        ctx,
        project_id: t.project_id.clone(),
        fence,
    };
    let Some(rec) = current(ctx, t).await? else {
        scaling.forget(&d.compute);
        return Ok(Outcome::Skipped);
    };
    if rec.record.cu != d.from {
        // Resized meanwhile: the next pass refreshes what is followed.
        return Ok(Outcome::Skipped);
    }
    let id: ComputeId = rec
        .record
        .id
        .parse()
        .map_err(|_| ReconcileError::Runtime(format!("compute id {}", rec.record.id)))?;
    let up = d.is_up();
    let mut version = None;
    if !up {
        version = Some(configure(&act, computes, &rec.record, d.to).await?);
    }
    act.renew().await?;
    let outcome = computes.runtime.resize(&t.namespace, &id, d.to).await;
    match outcome {
        Ok(ResizeOutcome::Applied) => {}
        Ok(ResizeOutcome::Refused(why)) => {
            tracing::warn!(compute = %d.compute, from = d.from.0, to = d.to.0, why, "the runtime refused the resize; retrying at the next tick");
            if !up {
                put_back(&act, computes, &rec.record, d, "refused").await;
            }
            return Ok(Outcome::Refused);
        }
        Ok(ResizeOutcome::Unsupported) => {
            tracing::info!(compute = %d.compute, "the runtime cannot resize this compute; it keeps its size");
            scaling.unsupported(&d.compute);
            if !up {
                put_back(&act, computes, &rec.record, d, "unsupported").await;
            }
            return Ok(Outcome::Skipped);
        }
        Err(e) => {
            if !up {
                put_back(&act, computes, &rec.record, d, "failed").await;
            }
            return Err(ReconcileError::Runtime(format!(
                "resizing the compute: {e}"
            )));
        }
    }
    if up {
        match configure(&act, computes, &rec.record, d.to).await {
            Ok(v) => version = Some(v),
            // The record's size moves anyway; the pass's spec check pushes
            // the cache's new size (its version differs).
            Err(e) => {
                tracing::info!(compute = %d.compute, error = %e, "the larger file cache was not pushed yet");
            }
        }
    }
    let mut resized = rec.record.clone();
    resized.cu = d.to;
    if let Some(v) = version {
        resized.spec_version = v;
    }
    let mut batch = Batch::new();
    batch.put(&resized, Some(rec.version))?;
    act.commit("compute.resize", batch).await?;
    scaling.applied(&d.compute, d.to);
    if let Some(binding) = &resized.binding {
        computes.lifecycle.emit(
            binding,
            &resized.id,
            LifecycleKind::Resized { cu: d.to },
            ctx.now_ms(),
        );
    }
    tracing::info!(compute = %d.compute, from = d.from.0, to = d.to.0, "compute resized (quarters of a CU)");
    Ok(Outcome::Resized)
}

/// After a scale-down's resize did not apply: the file cache back to the
/// compute's size.
async fn put_back<S: PgControlStore, N: NeonWrite>(
    act: &Act<'_, S, N>,
    computes: &Computes,
    compute: &ComputeRec,
    d: &Decision,
    why: &'static str,
) {
    if let Err(e) = configure(act, computes, compute, d.from).await {
        tracing::warn!(compute = %d.compute, error = %e, why, "the file cache was not put back; the next pass pushes the spec");
    }
}

/// The compute's record, if it is still its endpoint's running compute.
async fn current<S: PgControlStore, N>(
    ctx: &Ctx<S, N>,
    t: &Tracked,
) -> Result<Option<Versioned<ComputeRec>>, ReconcileError> {
    let Some(rec) = ctx
        .store
        .get::<ComputeRec>(&ComputeKey {
            id: t.compute.id.clone(),
        })
        .await?
    else {
        return Ok(None);
    };
    let endpoint = ctx
        .store
        .get::<EndpointRec>(&EndpointKey {
            project_id: t.project_id.clone(),
            id: t.endpoint_id.clone(),
        })
        .await?;
    let running = rec.record.status == ComputeStatus::Running
        && endpoint.is_some_and(|e| e.record.compute_id.as_deref() == Some(rec.record.id.as_str()));
    Ok(running.then_some(rec))
}

/// Pushes the compute's spec at size `cu` (its file cache); answers the
/// spec's inputs' version.
async fn configure<S: PgControlStore, N: NeonWrite>(
    act: &Act<'_, S, N>,
    computes: &Computes,
    compute: &ComputeRec,
    cu: Cu,
) -> Result<u64, ReconcileError> {
    let Some(binding) = &compute.binding else {
        return Err(ReconcileError::Runtime("an unbound compute".into()));
    };
    let inputs = match spec::read_inputs(
        &act.ctx.store,
        &computes.sources,
        binding,
        Some(&compute.id),
    )
    .await
    {
        Ok(i) => i,
        Err(SpecError::Store(e)) => return Err(e.into()),
        Err(e) => return Err(ReconcileError::Runtime(format!("no spec: {e:?}"))),
    };
    let (spec, version) = spec::build(&computes.sources, &compute.id, cu, &inputs)
        .map_err(|e| ReconcileError::Runtime(format!("no spec: {e:?}")))?;
    act.renew().await?;
    computes
        .ctl
        .configure(compute, &spec)
        .await
        .map_err(|e| ReconcileError::Runtime(format!("pushing the file cache's size: {e}")))?;
    Ok(version)
}
