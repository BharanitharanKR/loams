//! `schema-evolution` (Task 5): an additive change evolves; an incompatible
//! one pauses the instance with an error, never coercing data.

use super::{Ctx, Fail, Outcome};
use crate::{Evolved, SchemaStep};

pub(crate) async fn run(ctx: &Ctx<'_>) -> Outcome {
    if let Evolved::Paused { error } = ctx.h.evolve(SchemaStep::Additive).await? {
        return Err(Fail::Failed(format!(
            "an additive change paused the instance: {error}"
        )));
    }
    match ctx.h.evolve(SchemaStep::Incompatible).await? {
        Evolved::Paused { error } => Ok(vec![
            "additive change evolved".to_owned(),
            format!("incompatible change paused: {error}"),
        ]),
        Evolved::Evolved => Err(Fail::Failed(
            "an incompatible change was taken instead of pausing the instance".to_owned(),
        )),
    }
}
