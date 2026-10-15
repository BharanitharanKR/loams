//! The Iggy connectors runtime's fallback plugins (OR-4: `elasticsearch_*`,
//! `clickhouse_sink`, `s3_sink`, `iceberg_sink`, `postgres_*`,
//! `mongodb_sink`): one Deployment per namespace group, each instance's TOML
//! (CN1 Task 3's `IggyRuntime` rendering) in a ConfigMap of its own,
//! projected into `/etc/loams-iggy/connectors/<namespace>/`.

use serde_json::{Value, json};

use super::connect::group_workload;
use super::{Group, KubeRuntime, RenderError};

/// Where the connector configs are mounted.
pub const CONFIG_DIR: &str = "/etc/loams-iggy/connectors";
/// The Deployment's name prefix (`loams-iggy-connectors-<group>`).
pub const PREFIX: &str = "loams-iggy-connectors";

pub(crate) fn render(
    rt: &KubeRuntime,
    g: &Group<'_>,
    out: &mut Vec<Value>,
) -> Result<(), RenderError> {
    group_workload(
        rt,
        g,
        out,
        PREFIX,
        "iggy-connectors",
        CONFIG_DIR,
        "connectors",
        vec![json!({ "name": "LOAMS_IGGY_CONNECTORS_DIR", "value": CONFIG_DIR })],
    )
}
