//! `auth`: each declared auth method that the service supports connects and
//! moves a record. A method the service lacks is noted, not failed; if no
//! method could run, the suite is skipped (never green).

use super::{Ctx, Fail, Outcome};
use crate::HarnessError;
use crate::capabilities::declared_capabilities;

pub(crate) async fn run(ctx: &Ctx<'_>) -> Outcome {
    let methods: Vec<String> = declared_capabilities(ctx.spec, ctx.profile)
        .into_iter()
        .filter_map(|c| c.strip_prefix("auth.").map(str::to_owned))
        .collect();
    let mut notes = Vec::new();
    let mut ran = 0;
    for m in &methods {
        match ctx.h.authenticate(m).await {
            Ok(()) => {
                ran += 1;
                notes.push(format!("{m}: ok"));
            }
            Err(HarnessError::Unsupported(why) | HarnessError::Unavailable(why)) => {
                notes.push(format!("{m}: not supported by the service ({why})"));
            }
            Err(HarnessError::Failed(why)) => {
                return Err(Fail::Failed(format!("auth {m}: {why}")));
            }
        }
    }
    if ran == 0 {
        return Err(Fail::Skipped(format!(
            "no declared auth method ({}) runs on this service",
            methods.join(", ")
        )));
    }
    Ok(notes)
}
