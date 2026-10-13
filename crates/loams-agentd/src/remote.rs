//! `serve --remote` and `[remote]`: the settings of the remote Connect
//! listener (design §53 §6.3, D909, D921; plan SD1 Task 7, Shared contracts
//! "Discovery, CLI and remote config").
//!
//! The listener is off unless `serve --remote` is given with every flag, or
//! `config.toml` has `[remote] enabled = true` with every key. There are no
//! defaults that open it up: a missing flag exits 4 and names it. Flags win
//! over `[remote]`.
//!
//! [`options`] turns the settings into the listener's options before the
//! daemon takes its lock, so a bad certificate or issuer also exits 4.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use loams_agentd_connect::RemoteOptions;
use loams_agentd_connect::cors::{self, CorsPolicy};
use loams_agentd_connect::oidc::{JwksSource, OidcOptions};

use crate::config::RemoteConfig;

/// Tests only: a local JWKS file the remote listener reads its signing keys
/// from instead of the issuer's discovery document (SD1 Shared contracts).
/// Honoured in debug builds only; a release daemon ignores it.
pub const TEST_ISSUER_ENV: &str = "LOAMS_AGENTD_TEST_ISSUER";

/// Where the remote listener listens and whom it admits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSettings {
    pub listen: SocketAddr,
    pub tls_cert: PathBuf,
    pub tls_key: PathBuf,
    pub issuer: String,
    pub audience: String,
    pub subject: String,
    /// Normalised console origins ([`cors::origin`]).
    pub allow_origins: Vec<String>,
}

/// `serve --remote …`, as given (the binary's clap arguments map to it).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteFlags {
    pub remote: bool,
    pub listen: Option<String>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub subject: Option<String>,
    pub allow_origin: Vec<String>,
}

/// Why the flags do not make a remote listener.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FlagError {
    /// The flag, for example `--tls-cert`.
    #[error("serve --remote needs {0}")]
    Missing(&'static str),
    #[error("{flag}: {message}")]
    Invalid { flag: &'static str, message: String },
}

impl RemoteFlags {
    /// The settings, when every required flag is given and valid.
    ///
    /// # Errors
    ///
    /// The first missing flag, in the order of the Shared contracts'
    /// command line, or an invalid value.
    pub fn settings(&self) -> Result<RemoteSettings, FlagError> {
        if !self.remote {
            return Err(FlagError::Missing("--remote"));
        }
        let listen = self
            .listen
            .as_deref()
            .ok_or(FlagError::Missing("--listen"))?;
        let tls_cert = self
            .tls_cert
            .clone()
            .ok_or(FlagError::Missing("--tls-cert"))?;
        let tls_key = self
            .tls_key
            .clone()
            .ok_or(FlagError::Missing("--tls-key"))?;
        let issuer = self.issuer.clone().ok_or(FlagError::Missing("--issuer"))?;
        let audience = self
            .audience
            .clone()
            .ok_or(FlagError::Missing("--audience"))?;
        let subject = self
            .subject
            .clone()
            .ok_or(FlagError::Missing("--subject"))?;
        build(
            listen,
            tls_cert,
            tls_key,
            issuer,
            audience,
            subject,
            &self.allow_origin,
        )
        .map_err(|(key, message)| FlagError::Invalid {
            flag: flag_of(key),
            message,
        })
    }
}

fn flag_of(key: &str) -> &'static str {
    match key {
        "listen" => "--listen",
        "tls_cert" => "--tls-cert",
        "tls_key" => "--tls-key",
        "issuer" => "--issuer",
        "audience" => "--audience",
        "subject" => "--subject",
        _ => "--allow-origin",
    }
}

fn build(
    listen: &str,
    tls_cert: PathBuf,
    tls_key: PathBuf,
    issuer: String,
    audience: String,
    subject: String,
    allow_origins: &[String],
) -> Result<RemoteSettings, (&'static str, String)> {
    let listen = listen
        .parse::<SocketAddr>()
        .map_err(|e| ("listen", format!("{listen:?} is not addr:port: {e}")))?;
    OidcOptions::new(&issuer, &audience, &subject)
        .validate()
        .map_err(|message| {
            let key = if message.contains("--subject") {
                "subject"
            } else if message.contains("--audience") {
                "audience"
            } else {
                "issuer"
            };
            (key, message)
        })?;
    let allow_origins = allow_origins
        .iter()
        .map(|origin| cors::origin(origin))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|message| ("allow_origins", message))?;
    Ok(RemoteSettings {
        listen,
        tls_cert,
        tls_key,
        issuer,
        audience,
        subject,
        allow_origins,
    })
}

impl RemoteSettings {
    /// `[remote]`'s settings: `None` when it is not enabled.
    ///
    /// # Errors
    ///
    /// The key at fault (`listen`, `tls_cert`, …) and why.
    pub fn from_config(config: &RemoteConfig) -> Result<Option<Self>, (&'static str, String)> {
        if !config.enabled {
            return Ok(None);
        }
        let required = "is required when remote.enabled = true".to_owned();
        let listen = config
            .listen
            .as_deref()
            .ok_or(("listen", required.clone()))?;
        let tls_cert = config
            .tls_cert
            .clone()
            .ok_or(("tls_cert", required.clone()))?;
        let tls_key = config
            .tls_key
            .clone()
            .ok_or(("tls_key", required.clone()))?;
        let issuer = config.issuer.clone().ok_or(("issuer", required.clone()))?;
        let audience = config
            .audience
            .clone()
            .ok_or(("audience", required.clone()))?;
        let subject = config.subject.clone().ok_or(("subject", required))?;
        build(
            listen,
            tls_cert,
            tls_key,
            issuer,
            audience,
            subject,
            &config.allow_origins,
        )
        .map(Some)
    }
}

/// The remote listener's settings for this run: the flags when given, else
/// `[remote]` when it is enabled, else none (the default).
///
/// # Errors
///
/// As [`RemoteFlags::settings`] and [`RemoteSettings::from_config`].
pub fn resolve(
    flags: Option<&RemoteFlags>,
    config: &RemoteConfig,
) -> Result<Option<RemoteSettings>, String> {
    match flags {
        Some(flags) => flags.settings().map(Some).map_err(|e| e.to_string()),
        None => RemoteSettings::from_config(config)
            .map_err(|(key, message)| format!("remote.{key}: {message}")),
    }
}

/// The listener's options: the TLS files read and checked, and the issuer's
/// keys from discovery (or, in a debug build, `LOAMS_AGENTD_TEST_ISSUER`).
///
/// # Errors
///
/// A certificate or key that cannot be used, naming its flag.
pub fn options(
    settings: &RemoteSettings,
    auth_failure_delay: Duration,
) -> Result<RemoteOptions, String> {
    let tls = loams_agentd_connect::remote::tls_config(&settings.tls_cert, &settings.tls_key)
        .map_err(|e| e.to_string())?;
    let mut oidc = OidcOptions::new(&settings.issuer, &settings.audience, &settings.subject);
    if let Some(path) = test_issuer() {
        tracing::warn!(
            path = %path.display(),
            "{TEST_ISSUER_ENV} is set: the remote listener trusts a local JWKS file (tests only)"
        );
        oidc.jwks = JwksSource::File(path);
    }
    Ok(RemoteOptions {
        listen: settings.listen,
        tls,
        oidc,
        cors: CorsPolicy::new(&settings.allow_origins)?,
        auth_failure_delay,
    })
}

/// `LOAMS_AGENTD_TEST_ISSUER`, in a debug build.
fn test_issuer() -> Option<PathBuf> {
    if !cfg!(debug_assertions) {
        return None;
    }
    std::env::var_os(TEST_ISSUER_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full() -> RemoteFlags {
        RemoteFlags {
            remote: true,
            listen: Some("0.0.0.0:8443".into()),
            tls_cert: Some("/c.pem".into()),
            tls_key: Some("/k.pem".into()),
            issuer: Some("https://auth.example.com/application/o/loams/".into()),
            audience: Some("loams-agentd".into()),
            subject: Some("s1".into()),
            allow_origin: vec!["https://console.example.com/".into()],
        }
    }

    #[test]
    fn every_flag_but_allow_origin_is_required() {
        let settings = full().settings().expect("settings");
        assert_eq!(settings.listen, "0.0.0.0:8443".parse().unwrap());
        assert_eq!(settings.allow_origins, ["https://console.example.com"]);
        let mut flags = full();
        flags.allow_origin.clear();
        assert!(flags.settings().is_ok());
        type Clear = fn(&mut RemoteFlags);
        let cases: [(&str, Clear); 7] = [
            ("--remote", |f| f.remote = false),
            ("--listen", |f| f.listen = None),
            ("--tls-cert", |f| f.tls_cert = None),
            ("--tls-key", |f| f.tls_key = None),
            ("--issuer", |f| f.issuer = None),
            ("--audience", |f| f.audience = None),
            ("--subject", |f| f.subject = None),
        ];
        for (flag, clear) in cases {
            let mut flags = full();
            clear(&mut flags);
            assert_eq!(flags.settings(), Err(FlagError::Missing(flag)));
        }
    }

    #[test]
    fn bad_values_name_their_flag() {
        let mut flags = full();
        flags.listen = Some("8443".into());
        assert!(matches!(
            flags.settings(),
            Err(FlagError::Invalid {
                flag: "--listen",
                ..
            })
        ));
        let mut flags = full();
        flags.issuer = Some("http://auth.example.com/".into());
        assert!(matches!(
            flags.settings(),
            Err(FlagError::Invalid {
                flag: "--issuer",
                ..
            })
        ));
        let mut flags = full();
        flags.allow_origin.push("*".into());
        assert!(matches!(
            flags.settings(),
            Err(FlagError::Invalid {
                flag: "--allow-origin",
                ..
            })
        ));
    }
}
