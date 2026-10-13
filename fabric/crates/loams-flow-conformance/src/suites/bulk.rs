//! `bulk` (§33 §4 rule 3, D356): the Arrow path at 1 M rows, with no row
//! decode.

use super::{Ctx, Fail, Outcome};

pub(crate) async fn run(ctx: &Ctx<'_>) -> Outcome {
    let rows = ctx.opts.bulk_rows;
    let r = ctx.h.bulk(rows).await?;
    if r.rows != rows {
        return Err(Fail::Failed(format!(
            "the bulk path delivered {} of {rows} rows",
            r.rows
        )));
    }
    if !r.arrow {
        return Err(Fail::Failed(
            "the bulk path decoded rows instead of moving Arrow batches".to_owned(),
        ));
    }
    Ok(vec![format!("{rows} rows as Arrow batches")])
}
