//! `dup-check` (§33 §4 rule 3): after a crash and restart, duplicates stay
//! within the declared delivery. `exactly_once` and `effectively_once` allow
//! none; `at_least_once` allows one replay per crash (two copies), never an
//! unbounded number.

use super::kill::crash_and_restart;
use super::{Ctx, Outcome, dup_bound, no_loss};

pub(crate) async fn run(ctx: &Ctx<'_>) -> Outcome {
    let (seeded, observed, mut notes) = crash_and_restart(ctx).await?;
    let delivery = ctx.delivery();
    let max = match delivery.as_str() {
        "exactly_once" | "effectively_once" | "at_most_once" => 1,
        _ => 2,
    };
    dup_bound(&observed, max)?;
    if delivery != "at_most_once" {
        no_loss(&seeded, &observed)?;
    }
    let dups = observed.iter().filter(|o| o.copies > 1).count();
    notes.push(format!(
        "{delivery}: {dups} of {} records duplicated (at most {max} copies allowed)",
        seeded.ids.len()
    ));
    Ok(notes)
}
