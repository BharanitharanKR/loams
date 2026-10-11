//! `config.toml` (plan DD1 "Shared contracts"), written by Loams Desktop.
//!
//! ```toml
//! [paths]
//! engine_data = "<userData>/engine"
//! logs = "<logs>"
//! stacks_source = "<resources>/stacks"
//! stacks_run = "<userData>/stacks"
//! connectors = "<resources>/connectors.json"
//! runtime = "<LOCALAPPDATA>/Loams Desktop/agentd/runtime"   # Windows (T0-7)
//! [engine]
//! auto_start = true
//! [daemon]
//! mode = "child"            # child | service
//! notifications = true
//! [tool_outputs]            # plan DD1 ruling T2-13
//! file_cap_bytes = 4194304
//! budget_bytes = 1073741824
//! archived_retention_days = 30
//! ```
//!
//! Every key is optional and has the default shown. A value of the wrong
//! type or out of range makes the file invalid, and `run` exits 4 naming the
//! key. Unknown keys are ignored, so a newer Loams Desktop can add settings
//! that an older daemon does not know yet.

use std::path::{Path, PathBuf};
use std::time::Duration;

use loams_agentd_proto::daemon::DaemonMode;
use loams_agentd_sessions::tool_outputs::{
    TOOL_OUTPUT_ARCHIVED_RETENTION, TOOL_OUTPUT_BUDGET, TOOL_OUTPUT_FILE_CAP, ToolOutputLimits,
};
use serde::Deserialize;

/// The parsed `config.toml`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Config {
    pub paths: PathsConfig,
    pub engine: EngineConfig,
    pub daemon: DaemonConfig,
    pub tool_outputs: ToolOutputsConfig,
}

/// `[paths]`: locations Loams Desktop owns. The supervisor (DD1 Tasks 14
/// and 15) reads them; `None` until then means "not configured".
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct PathsConfig {
    pub engine_data: Option<PathBuf>,
    pub logs: Option<PathBuf>,
    pub stacks_source: Option<PathBuf>,
    pub stacks_run: Option<PathBuf>,
    pub connectors: Option<PathBuf>,
    pub runtime: Option<PathBuf>,
}

/// `[engine]`: the supervised `loams dev`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    pub auto_start: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self { auto_start: true }
    }
}

/// `[daemon]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct DaemonConfig {
    /// The mode when `run` is given neither `--child` nor `--service`.
    pub mode: DaemonMode,
    pub notifications: bool,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            mode: DaemonMode::Child,
            notifications: true,
        }
    }
}

/// `[tool_outputs]`: the bounds of the full tool-output store (T2-13).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct ToolOutputsConfig {
    pub file_cap_bytes: u64,
    pub budget_bytes: u64,
    pub archived_retention_days: u64,
}

impl Default for ToolOutputsConfig {
    fn default() -> Self {
        Self {
            file_cap_bytes: TOOL_OUTPUT_FILE_CAP,
            budget_bytes: TOOL_OUTPUT_BUDGET,
            archived_retention_days: TOOL_OUTPUT_ARCHIVED_RETENTION.as_secs() / DAY.as_secs(),
        }
    }
}

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

impl ToolOutputsConfig {
    pub fn limits(&self) -> ToolOutputLimits {
        ToolOutputLimits {
            file_cap: self.file_cap_bytes,
            budget: self.budget_bytes,
            archived_retention: DAY
                * u32::try_from(self.archived_retention_days).unwrap_or(u32::MAX),
        }
    }
}

/// Why a config could not be loaded. Each message names the file, and the
/// key when one is at fault.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid config {path}: {field}: {message}")]
    Invalid {
        path: PathBuf,
        /// The dotted key (`daemon.mode`), or the line of a syntax error.
        field: String,
        message: String,
    },
}

impl Config {
    /// Read and validate `path`.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&text).map_err(|(field, message)| ConfigError::Invalid {
            path: path.to_path_buf(),
            field,
            message,
        })
    }

    fn parse(text: &str) -> Result<Self, (String, String)> {
        let config: Self = serde_path_to_error::deserialize(toml::Deserializer::new(text))
            .map_err(|error| {
                let line = error
                    .inner()
                    .span()
                    .and_then(|span| text.get(..span.start))
                    .map(|before| 1 + before.matches('\n').count());
                let key = error.path().to_string();
                let field = match (key.as_str(), line) {
                    (".", Some(line)) => format!("line {line}"),
                    (".", None) => "the file".to_string(),
                    (_, Some(line)) => format!("`{key}` (line {line})"),
                    (_, None) => format!("`{key}`"),
                };
                (field, error.inner().message().to_string())
            })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), (String, String)> {
        let positive = [
            (
                "tool_outputs.file_cap_bytes",
                self.tool_outputs.file_cap_bytes,
            ),
            ("tool_outputs.budget_bytes", self.tool_outputs.budget_bytes),
            (
                "tool_outputs.archived_retention_days",
                self.tool_outputs.archived_retention_days,
            ),
        ];
        for (key, value) in positive {
            if value == 0 {
                return Err((format!("`{key}`"), "must be greater than 0".to_string()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invalid(text: &str) -> (String, String) {
        Config::parse(text).expect_err("invalid config")
    }

    #[test]
    fn an_empty_file_is_all_defaults() {
        let config = Config::parse("").expect("parse");
        assert_eq!(config, Config::default());
        assert!(config.engine.auto_start);
        assert_eq!(config.daemon.mode, DaemonMode::Child);
        assert!(config.daemon.notifications);
        assert_eq!(config.tool_outputs.limits(), ToolOutputLimits::default());
    }

    #[test]
    fn the_contract_example_parses() {
        let config = Config::parse(
            r#"
            [paths]
            engine_data = "/home/u/.config/Loams Desktop/engine"
            logs = "/home/u/.config/Loams Desktop/logs"
            stacks_source = "/opt/Loams Desktop/resources/stacks"
            stacks_run = "/home/u/.config/Loams Desktop/stacks"
            connectors = "/opt/Loams Desktop/resources/connectors.json"
            [engine]
            auto_start = false
            [daemon]
            mode = "service"
            notifications = false
            [tool_outputs]
            file_cap_bytes = 1024
            budget_bytes = 4096
            archived_retention_days = 7
            "#,
        )
        .expect("parse");
        assert_eq!(
            config.paths.engine_data.as_deref(),
            Some(Path::new("/home/u/.config/Loams Desktop/engine"))
        );
        assert!(!config.engine.auto_start);
        assert_eq!(config.daemon.mode, DaemonMode::Service);
        assert!(!config.daemon.notifications);
        assert_eq!(
            config.tool_outputs.limits(),
            ToolOutputLimits {
                file_cap: 1024,
                budget: 4096,
                archived_retention: Duration::from_secs(7 * 24 * 60 * 60),
            }
        );
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let config = Config::parse("future = 1\n[daemon]\nlater = true\n").expect("parse");
        assert_eq!(config, Config::default());
    }

    #[test]
    fn errors_name_the_key() {
        let (field, message) = invalid("[daemon]\nmode = \"sideways\"\n");
        assert_eq!(field, "`daemon.mode` (line 2)");
        assert!(message.contains("sideways"), "{message}");
        assert_eq!(
            invalid("[engine]\nauto_start = \"yes\"\n").0,
            "`engine.auto_start` (line 2)"
        );
        assert_eq!(
            invalid("[tool_outputs]\nfile_cap_bytes = 0\n").0,
            "`tool_outputs.file_cap_bytes`"
        );
        assert_eq!(invalid("[daemon\n").0, "line 1");
    }
}
