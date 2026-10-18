//! A `DisjunctionMaxQuery` that scores the same on every path.
//!
//! Tantivy 0.26 runs a disjunction whose clauses are all plain term scorers
//! as a `TermUnion`, and its `for_each_pruning` (what top-k search and the
//! engine's candidate pass call) is block-max WAND, which **sums** the term
//! scores and never consults the disjunction's combiner. So a
//! `multi_match` over one-token fields (`best_fields`, the default) ranked
//! its candidates by `most_fields` scores: a document matching in one field
//! could be pruned below documents whose best field scored less. `scorer()`
//! uses the combiner, so the exact rescoring was right and the candidate set
//! was not.
//!
//! [`ExactDisMax`] wraps the query and keeps only its `scorer` and `explain`,
//! so every other `Weight` path falls back to Tantivy's defaults, which drive
//! that scorer.

use tantivy::query::{DisjunctionMaxQuery, EnableScoring, Explanation, Query, Scorer, Weight};
use tantivy::{DocId, Score, SegmentReader, Term};

/// A `DisjunctionMaxQuery` whose top-k pruning uses its own combiner.
#[derive(Debug)]
pub struct ExactDisMax(DisjunctionMaxQuery);

impl ExactDisMax {
    pub fn with_tie_breaker(disjuncts: Vec<Box<dyn Query>>, tie_breaker: Score) -> Self {
        Self(DisjunctionMaxQuery::with_tie_breaker(
            disjuncts,
            tie_breaker,
        ))
    }
}

impl Clone for ExactDisMax {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Query for ExactDisMax {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(ExactWeight(self.0.weight(enable_scoring)?)))
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        self.0.query_terms(visitor);
    }
}

struct ExactWeight(Box<dyn Weight>);

impl Weight for ExactWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        self.0.scorer(reader, boost)
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        self.0.explain(reader, doc)
    }
}
