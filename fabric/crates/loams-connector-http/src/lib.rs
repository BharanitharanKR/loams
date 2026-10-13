//! `loams-connector-http`: the HTTP connector family (CN2 OR-6).
//!
//! * [`profile`] — the declarative HTTP-API profile engine (CN2 Task 7, OR-11): a
//!   [`loams_connector_sdk::ConnectorFactory`] over a `connectors/profiles/<id>.yaml`
//!   profile whose `profile_kind` is `http-api`. `list` and `async_job` resources
//!   become polling sources, `upsert` and `create` resources become sinks, and the
//!   `auth` block signs every request (`oauth2`, `bearer`, `basic`, `header`,
//!   `aws-sigv4`) with credentials resolved from the secrets the instance config
//!   names.
//! * [`webhook`] — the webhook signature schemes a profile names (Task 20).
//! * [`route`] — import routes on native profile sources and their
//!   projections (Task 20).
//!
//! The HTTP and webhooks ★ connectors move here in CN2 Task 8.

pub mod profile;
pub mod route;
pub mod webhook;
