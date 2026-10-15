//! `CreateChangeRequest` (design §53 §6.5 G4, §8.2; plan SD1 Task 9): opens a
//! pull request for a pushed branch.
//!
//! - **Forgejo** through its REST API, `POST /api/v1/repos/{owner}/{repo}/pulls`,
//!   with the factory credential the daemon holds (§50 §12, D794, D795). The
//!   token goes only to the host it was given for: the credential's base URL
//!   must name the repository remote's host, over `https` (plain `http` only
//!   on loopback). A Forgejo pull request has no draft flag; a draft is
//!   titled `WIP: …`, Forgejo's own convention.
//! - **GitHub** through `gh pr create`, as the daemon reads pull requests
//!   today (`source_control.rs`).
//!
//! The owner and repository come from the head branch's remote (its
//! configured remote, else `origin`), parsed with
//! [`crate::source_control::parse_git_remote`] (T3 Code's `changeRequestUrl`
//! rules). Without a credential, or without `gh`, the answer is
//! `provider_not_configured`. No token is ever logged or put in an error.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use loams_agentd_proto::ChangeRequestState;
use loams_agentd_proto::git_write::{
    ChangeRequestProvider, CreateChangeRequestParams, CreateChangeRequestReply,
    PROVIDER_NOT_CONFIGURED,
};

use crate::git_write::{GitWriteError, bad, failed, git_ok, git_try, redact, valid_branch};
use crate::source_control::{GitRemote, parse_git_remote};

/// The keyring service of the daemon's secrets (§50 §12).
pub const KEYRING_SERVICE: &str = "dev.loams.agentd";
/// The Forgejo factory credential's token (`factory:<app>:<field>`).
pub const FORGEJO_TOKEN_ACCOUNT: &str = "factory:forgejo:token";
/// Its base URL, kept beside the token until DD1 Task 22's store holds the
/// non-secret part.
pub const FORGEJO_URL_ACCOUNT: &str = "factory:forgejo:url";

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const GH_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_TITLE: usize = 256;
const MAX_BODY: usize = 64 * 1024;

/// A Forgejo instance and the token for it.
#[derive(Clone, PartialEq, Eq)]
pub struct ForgejoCredential {
    /// For example `https://forgejo.example.com` (a sub-path is kept).
    pub base_url: String,
    pub token: String,
}

impl std::fmt::Debug for ForgejoCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForgejoCredential")
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// Where the daemon's forge credentials come from.
pub trait ForgeCredentials: Send + Sync + std::fmt::Debug {
    /// The Forgejo credential, if one is configured.
    ///
    /// # Errors
    ///
    /// The store could not be read (for example, no keyring).
    fn forgejo(&self) -> Result<Option<ForgejoCredential>, String>;
}

/// The OS keyring, service [`KEYRING_SERVICE`].
#[derive(Debug, Default, Clone, Copy)]
pub struct KeyringForgeCredentials;

impl KeyringForgeCredentials {
    fn read(account: &str) -> Result<Option<String>, String> {
        let entry =
            keyring::Entry::new(KEYRING_SERVICE, account).map_err(|error| error.to_string())?;
        match entry.get_password() {
            Ok(value) => Ok(Some(value)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }
}

impl ForgeCredentials for KeyringForgeCredentials {
    fn forgejo(&self) -> Result<Option<ForgejoCredential>, String> {
        let Some(token) = Self::read(FORGEJO_TOKEN_ACCOUNT)? else {
            return Ok(None);
        };
        let Some(base_url) = Self::read(FORGEJO_URL_ACCOUNT)? else {
            return Ok(None);
        };
        Ok(Some(ForgejoCredential { base_url, token }))
    }
}

/// A fixed credential (tests, and a host without a keyring).
#[derive(Debug, Clone)]
pub struct StaticForgeCredentials(Option<ForgejoCredential>);

impl StaticForgeCredentials {
    pub fn new(forgejo: Option<ForgejoCredential>) -> Self {
        Self(forgejo)
    }
}

impl ForgeCredentials for StaticForgeCredentials {
    fn forgejo(&self) -> Result<Option<ForgejoCredential>, String> {
        Ok(self.0.clone())
    }
}

pub(crate) fn not_configured(detail: impl std::fmt::Display) -> GitWriteError {
    GitWriteError::Failed(format!("{PROVIDER_NOT_CONFIGURED}: {detail}"))
}

/// `<base>/api/v1/repos/<owner>/<repo>/<rest…>`, each segment escaped.
pub(crate) fn forgejo_api_url(
    base: &reqwest::Url,
    remote: &GitRemote,
    rest: &[&str],
) -> Result<reqwest::Url, GitWriteError> {
    // `url` drops a `.` or `..` segment rather than escaping it, so a remote
    // named `…/../..` would send the token to another API path of the same
    // host (SD1 Task 30).
    for segment in [remote.owner.as_str(), remote.repository.as_str()] {
        if matches!(segment, "" | "." | "..") {
            return Err(failed(format!(
                "the remote names no repository: {}/{}",
                remote.owner, remote.repository
            )));
        }
    }
    let mut url = base.clone();
    url.path_segments_mut()
        .map_err(|_| not_configured("the Forgejo credential's URL is not valid"))?
        .pop_if_empty()
        .extend([
            "api",
            "v1",
            "repos",
            remote.owner.as_str(),
            remote.repository.as_str(),
        ])
        .extend(rest);
    Ok(url)
}

/// Opens change requests for the engine.
#[derive(Debug, Clone)]
pub struct ChangeRequestCreator {
    credentials: Arc<dyn ForgeCredentials>,
    gh_program: OsString,
    /// Directories searched for `gh` before the daemon's own `PATH` (tests).
    gh_path: Vec<PathBuf>,
    http: reqwest::Client,
}

impl ChangeRequestCreator {
    pub fn new(credentials: Arc<dyn ForgeCredentials>) -> Self {
        Self {
            credentials,
            gh_program: OsString::from("gh"),
            gh_path: Vec::new(),
            http: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                // The token never follows a redirect anywhere.
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// The daemon's own: the OS keyring and `gh` on the `PATH`.
    pub fn from_keyring() -> Self {
        Self::new(Arc::new(KeyringForgeCredentials))
    }

    /// Look for `gh` in `dirs` first.
    #[must_use]
    pub fn with_gh_path(mut self, dirs: Vec<PathBuf>) -> Self {
        self.gh_path = dirs;
        self
    }

    /// Run `program` instead of `gh`.
    #[must_use]
    pub fn with_gh_program(mut self, program: impl Into<OsString>) -> Self {
        self.gh_program = program.into();
        self
    }

    /// Open a change request from the checkout at `root`.
    ///
    /// # Errors
    ///
    /// `provider_not_configured`, bad params, or the provider's refusal.
    pub async fn create(
        &self,
        root: &Path,
        params: &CreateChangeRequestParams,
    ) -> Result<CreateChangeRequestReply, GitWriteError> {
        let title = params.title.trim();
        if title.is_empty() || title.chars().count() > MAX_TITLE {
            return Err(bad(
                "a change request needs a title of at most 256 characters",
            ));
        }
        if params.body.len() > MAX_BODY {
            return Err(bad("the body is longer than 64 KiB"));
        }
        if params.base == params.head {
            return Err(bad("base and head are the same branch"));
        }
        valid_branch(root, &params.base).await?;
        valid_branch(root, &params.head).await?;
        let mut body = params.body.clone();
        if let Some(issue) = &params.link_issue {
            let number = issue.trim().trim_start_matches('#');
            if number.is_empty() || !number.chars().all(|c| c.is_ascii_digit()) {
                return Err(bad(format!("not an issue number: {issue:?}")));
            }
            if !body.is_empty() {
                body.push_str("\n\n");
            }
            body.push_str(&format!("Closes #{number}"));
        }

        let remote = self.remote(root, &params.head).await?;
        match params.provider {
            ChangeRequestProvider::Forgejo => self.forgejo(&remote, params, title, &body).await,
            ChangeRequestProvider::Github => self.github(root, &remote, params, title, &body).await,
        }
    }

    /// The repository behind `head`'s remote (or `origin`).
    pub(crate) async fn remote(&self, root: &Path, head: &str) -> Result<GitRemote, GitWriteError> {
        let name = git_try(root, &["config", "--get", &format!("branch.{head}.remote")])
            .await
            .filter(|name| !name.is_empty() && name != ".")
            .unwrap_or_else(|| "origin".to_owned());
        let url = git_ok(root, &["remote", "get-url", "--", &name])
            .await
            .map_err(|_| bad(format!("no remote {name:?} to open a change request on")))?;
        parse_git_remote(&url).ok_or_else(|| {
            bad(format!(
                "the remote {name:?} ({}) names no owner and repository",
                redact(&url)
            ))
        })
    }

    /// The Forgejo credential for `remote`'s host: its base URL and token.
    /// The token is only ever for that host, over `https` (plain `http` only
    /// on loopback); the client follows no redirects (ruling T9-3).
    pub(crate) async fn forgejo_credential_for(
        &self,
        remote: &GitRemote,
    ) -> Result<(reqwest::Url, String), GitWriteError> {
        let credentials = Arc::clone(&self.credentials);
        let credential = tokio::task::spawn_blocking(move || credentials.forgejo())
            .await
            .map_err(|error| failed(format!("the credential store failed: {error}")))?
            .map_err(|error| not_configured(format!("the keyring could not be read: {error}")))?
            .ok_or_else(|| not_configured("no Forgejo credential is configured"))?;

        let mut url = reqwest::Url::parse(credential.base_url.trim())
            .map_err(|_| not_configured("the Forgejo credential's URL is not valid"))?;
        let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        if host != remote.host.to_ascii_lowercase() {
            return Err(not_configured(format!(
                "the Forgejo credential is for another host than {}",
                remote.host
            )));
        }
        let loopback = matches!(host.as_str(), "127.0.0.1" | "localhost" | "[::1]" | "::1");
        if !(url.scheme() == "https" || (url.scheme() == "http" && loopback)) {
            return Err(not_configured(
                "the Forgejo credential's URL must use https",
            ));
        }
        url.set_query(None);
        url.set_fragment(None);
        Ok((url, credential.token))
    }

    /// The HTTP client: a timeout, and no redirects (the token never follows
    /// one).
    pub(crate) fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// A `gh` command in `root` that never prompts, with stdout and stderr
    /// piped; its `PATH` puts [`Self::with_gh_path`]'s directories first,
    /// else it is the login shell's.
    pub(crate) fn gh_command(&self, root: &Path) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(&self.gh_program);
        if self.gh_path.is_empty() {
            loams_agentd_harness::compose_login_shell_path(&mut command);
        } else {
            let own = std::env::var_os("PATH").unwrap_or_default();
            let dirs = self
                .gh_path
                .iter()
                .cloned()
                .chain(std::env::split_paths(&own));
            if let Ok(joined) = std::env::join_paths(dirs) {
                command.env("PATH", joined);
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.as_std_mut().creation_flags(0x08000000);
        }
        command
            .current_dir(root)
            .env("GH_PROMPT_DISABLED", "1")
            .env("GH_NO_UPDATE_NOTIFIER", "1")
            .env("NO_COLOR", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command
    }

    async fn forgejo(
        &self,
        remote: &GitRemote,
        params: &CreateChangeRequestParams,
        title: &str,
        body: &str,
    ) -> Result<CreateChangeRequestReply, GitWriteError> {
        let (base, token) = self.forgejo_credential_for(remote).await?;
        let url = forgejo_api_url(&base, remote, &["pulls"])?;

        let title = if params.draft && !title.starts_with("WIP:") {
            format!("WIP: {title}")
        } else {
            title.to_owned()
        };
        let response = self
            .http
            .post(url)
            .header("authorization", format!("token {token}"))
            .header("accept", "application/json")
            .json(&serde_json::json!({
                "base": params.base,
                "head": params.head,
                "title": title,
                "body": body,
            }))
            .send()
            .await
            .map_err(|error| {
                failed(format!(
                    "forgejo: the request failed: {}",
                    redact(&error.without_url().to_string())
                ))
            })?;
        let status = response.status();
        let reply: serde_json::Value = response.json().await.unwrap_or_default();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(not_configured(format!(
                "Forgejo refused the credential (HTTP {})",
                status.as_u16()
            )));
        }
        if !status.is_success() {
            let message = reply
                .get("message")
                .and_then(serde_json::Value::as_str)
                .map(|message| redact(message.chars().take(512).collect::<String>().as_str()))
                .unwrap_or_default();
            return Err(failed(format!(
                "forgejo: HTTP {} {message}",
                status.as_u16()
            )));
        }
        let number = reply
            .get("number")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| failed("forgejo: the reply has no pull request number"))?;
        let url = reply
            .get("html_url")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let state = match reply.get("state").and_then(serde_json::Value::as_str) {
            Some("closed")
                if reply.get("merged").and_then(serde_json::Value::as_bool) == Some(true) =>
            {
                ChangeRequestState::Merged
            }
            Some("closed") => ChangeRequestState::Closed,
            _ => ChangeRequestState::Open,
        };
        Ok(CreateChangeRequestReply {
            provider: ChangeRequestProvider::Forgejo,
            number,
            url,
            state,
        })
    }

    async fn github(
        &self,
        root: &Path,
        remote: &GitRemote,
        params: &CreateChangeRequestParams,
        title: &str,
        body: &str,
    ) -> Result<CreateChangeRequestReply, GitWriteError> {
        let mut command = self.gh_command(root);
        // `--flag=value` keeps a value that starts with `-` a value.
        command
            .arg("pr")
            .arg("create")
            .arg(format!(
                "--repo={}/{}/{}",
                remote.host, remote.owner, remote.repository
            ))
            .arg(format!("--base={}", params.base))
            .arg(format!("--head={}", params.head))
            .arg(format!("--title={title}"))
            .arg(format!("--body={body}"));
        if params.draft {
            command.arg("--draft");
        }
        command.stdin(Stdio::null());
        let child = match command.spawn() {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(not_configured("gh is not installed"));
            }
            Err(error) => return Err(failed(format!("gh could not start: {error}"))),
        };
        let output = tokio::time::timeout(GH_TIMEOUT, child.wait_with_output())
            .await
            .map_err(|_| failed("gh timed out"))?
            .map_err(|error| failed(format!("gh failed: {error}")))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !output.status.success() {
            if stderr.contains("gh auth login") || stderr.contains("not logged in") {
                return Err(not_configured("gh is not signed in"));
            }
            let detail: String = stderr.trim().chars().take(1024).collect();
            return Err(failed(format!("gh: {}", redact(&detail))));
        }
        let url = stdout
            .lines()
            .map(str::trim)
            .rfind(|line| line.starts_with("https://") || line.starts_with("http://"))
            .ok_or_else(|| failed("gh printed no pull request URL"))?
            .to_owned();
        let number = url
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .and_then(|last| last.parse::<u64>().ok())
            .ok_or_else(|| failed("gh printed no pull request number"))?;
        Ok(CreateChangeRequestReply {
            provider: ChangeRequestProvider::Github,
            number,
            url,
            state: ChangeRequestState::Open,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SD1 Task 30: a remote's owner or repository of `.` or `..` would move
    /// the token's request to another API path; it is refused instead.
    #[test]
    fn forgejo_api_urls_stay_under_the_repository() {
        let base = reqwest::Url::parse("https://forgejo.example.test/sub/").unwrap();
        let remote = |owner: &str, repository: &str| GitRemote {
            host: "forgejo.example.test".into(),
            owner: owner.into(),
            repository: repository.into(),
        };
        let url = forgejo_api_url(&base, &remote("o", "r?x#y"), &["pulls"]).unwrap();
        assert_eq!(
            url.as_str(),
            "https://forgejo.example.test/sub/api/v1/repos/o/r%3Fx%23y/pulls"
        );
        for (owner, repository) in [("..", ".."), ("o", ".."), (".", "r"), ("", "r")] {
            assert!(
                forgejo_api_url(&base, &remote(owner, repository), &["pulls"]).is_err(),
                "{owner}/{repository}"
            );
        }
    }
}
