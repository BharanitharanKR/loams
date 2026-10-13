//! `cdc` (§33 §4 rule 3): inserts, updates, deletes and DDL arrive as change
//! events, and a restart resumes from the slot or binlog without loss.

use super::{Ctx, Fail, Outcome};

pub(crate) async fn run(ctx: &Ctx<'_>) -> Outcome {
    let r = ctx.h.cdc().await?;
    let missing: Vec<&str> = [
        (r.inserts, "inserts"),
        (r.updates, "updates"),
        (r.deletes, "deletes"),
        (r.ddl, "DDL"),
        (r.resumed_without_loss, "resume without loss"),
    ]
    .into_iter()
    .filter(|(ok, _)| !ok)
    .map(|(_, what)| what)
    .collect();
    if missing.is_empty() {
        Ok(vec![
            "inserts, updates, deletes, DDL and resume observed".to_owned(),
        ])
    } else {
        Err(Fail::Failed(format!(
            "not observed: {}",
            missing.join(", ")
        )))
    }
}
