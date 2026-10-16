//! Retrieval on Loams Graph (GR1 Tasks 20–22; design §48 §10.3, §07 §5; D743).
//!
//! * [`expand`]: `QueryService/Search`'s `expand` stage, `loams-query`'s
//!   [`GraphExpander`](loams_query::expand::GraphExpander), built on parameterized GQL.
//! * [`filter`]: the search IR's filters, evaluated over element properties.

pub mod expand;
pub mod filter;
pub mod udtf;

pub use expand::LoamsGraphExpander;
