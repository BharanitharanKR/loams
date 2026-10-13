//! Log scrubbing for fallback runtimes (CN2 Task 25). Native connectors
//! never format a `Secret` (its `Debug` is `<redacted>`, it has no
//! `Display`, and the SDK scrubs positions, errors and DLQ records). A
//! fallback is a separate process whose logs Loams does not write, so the
//! supervisor reads them line by line through [`FallbackLogFilter`]:
//! 1. values of known secret option names, and a URL's password, are masked
//!    ([`mask_known_secret_options`]);
//! 2. a line that still holds a resolved secret's bytes (raw, base64 or
//!    percent-encoded; `Scrubber::contains`) is dropped and counted.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use loams_connector_sdk::Scrubber;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

const MASK: &str = "****";

/// Option names (lower case, without `.`, `_` and `-`) whose values are
/// masked when a key ends with one: `database.password`, `clientSecret`,
/// `sasl.jaas.config`, `AccountKey`, `access_token`, …
pub const SECRET_OPTION_SUFFIXES: &[&str] = &[
    "password",
    "passwd",
    "pwd",
    "passphrase",
    "secret",
    "token",
    "accesskey",
    "secretkey",
    "apikey",
    "privatekey",
    "accountkey",
    "sharedaccesskey",
    "jaasconfig",
    "credential",
    "credentials",
    "connectionstring",
    "authorization",
];

/// The logging options a fallback renderer sets so the fallback masks what
/// it can itself (Task 22 adds them to its renderers; R25.7): Camel's
/// `logMask` masks known sensitive endpoint options in its own logs.
pub const CAMEL_LOG_MASK_PROPERTIES: &[(&str, &str)] = &[("camel.main.logMask", "true")];

fn is_key_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')
}

fn is_secret_key(key: &str) -> bool {
    let norm: String = key
        .chars()
        .filter(|c| !matches!(c, '_' | '.' | '-'))
        .flat_map(char::to_lowercase)
        .collect();
    SECRET_OPTION_SUFFIXES.iter().any(|s| norm.ends_with(s))
}

/// Mask a URL's password: `scheme://user:pass@host` → `scheme://user:****@host`.
fn mask_userinfo(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(i) = rest.find("://") {
        let (head, tail) = rest.split_at(i + 3);
        out.push_str(head);
        let end = tail
            .find(|c: char| c == '/' || c == '?' || c == '#' || c.is_whitespace())
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        match authority.rfind('@') {
            Some(at) if authority[..at].contains(':') => {
                let colon = authority[..at].find(':').unwrap_or(at);
                out.push_str(&authority[..=colon]);
                out.push_str(MASK);
                out.push_str(&authority[at..]);
            }
            _ => out.push_str(authority),
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

/// `line` with the value of every `key=value`, `key: value` or
/// `"key": "value"` whose key names a secret ([`SECRET_OPTION_SUFFIXES`])
/// replaced by `****`, and every URL password masked. An `Authorization`
/// value's scheme (`Basic`, `Bearer`) is masked with its credentials.
pub fn mask_known_secret_options(line: &str) -> String {
    let line = mask_userinfo(line);
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::with_capacity(line.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let token_start = is_key_char(c) && (i == 0 || !is_key_char(chars[i - 1]));
        if !token_start {
            out.push(c);
            i += 1;
            continue;
        }
        let mut j = i;
        while j < chars.len() && is_key_char(chars[j]) {
            j += 1;
        }
        let key: String = chars[i..j].iter().collect();
        out.push_str(&key);
        if !is_secret_key(&key) {
            i = j;
            continue;
        }
        // Separator: an optional closing quote, spaces, `=` or `:`, spaces,
        // an optional opening quote.
        let mut k = j;
        if k < chars.len() && matches!(chars[k], '"' | '\'') {
            k += 1;
        }
        while k < chars.len() && chars[k] == ' ' {
            k += 1;
        }
        if k >= chars.len() || !matches!(chars[k], '=' | ':') {
            i = j;
            continue;
        }
        k += 1;
        while k < chars.len() && chars[k] == ' ' {
            k += 1;
        }
        let quote = (k < chars.len() && matches!(chars[k], '"' | '\'')).then(|| chars[k]);
        if quote.is_some() {
            k += 1;
        }
        out.extend(&chars[j..k]);
        let value_end = |from: usize| -> usize {
            let mut e = from;
            while e < chars.len() {
                let ch = chars[e];
                let stop = match quote {
                    Some(q) => ch == q,
                    None => {
                        ch.is_whitespace()
                            || matches!(ch, ',' | ';' | '&' | '"' | '\'' | '}' | ')' | ']')
                    }
                };
                if stop {
                    break;
                }
                e += 1;
            }
            e
        };
        let mut end = value_end(k);
        let value: String = chars[k..end].iter().collect();
        if quote.is_none()
            && matches!(
                value.to_ascii_lowercase().as_str(),
                "basic" | "bearer" | "digest"
            )
            && end < chars.len()
            && chars[end] == ' '
        {
            end = value_end(end + 1);
        }
        out.push_str(MASK);
        i = end;
    }
    out
}

/// The supervisor's filter for one fallback process's log stream.
pub struct FallbackLogFilter {
    scrubber: Arc<Scrubber>,
    dropped: AtomicU64,
}

impl fmt::Debug for FallbackLogFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FallbackLogFilter")
            .field("dropped", &self.dropped())
            .finish_non_exhaustive()
    }
}

impl FallbackLogFilter {
    /// A filter over the secrets `scrubber` has recorded (the instance's
    /// `ScrubbingResolver` records each one it resolves).
    pub fn new(scrubber: Arc<Scrubber>) -> Self {
        Self {
            scrubber,
            dropped: AtomicU64::new(0),
        }
    }

    /// The line to pass on, masked, or `None` when it held a resolved
    /// secret and was dropped.
    pub fn filter_line(&self, line: &str) -> Option<String> {
        let masked = mask_known_secret_options(line);
        if self.scrubber.contains(&masked) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("dropped a fallback log line that held a resolved secret");
            None
        } else {
            Some(masked)
        }
    }

    /// Lines dropped so far (Task 24 exports it as a metric).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Copy `input` (a fallback's stdout or stderr) to `output` line by
    /// line through [`filter_line`](Self::filter_line). Bytes that are not
    /// UTF-8 are replaced, so a secret cannot hide behind an encoding error.
    pub async fn pump<R, W>(&self, input: R, mut output: W) -> std::io::Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let mut reader = BufReader::new(input);
        let mut buf = Vec::new();
        loop {
            buf.clear();
            if reader.read_until(b'\n', &mut buf).await? == 0 {
                break;
            }
            let line = String::from_utf8_lossy(&buf);
            let line = line.trim_end_matches(['\n', '\r']);
            if let Some(kept) = self.filter_line(line) {
                output.write_all(kept.as_bytes()).await?;
                output.write_all(b"\n").await?;
            }
        }
        output.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks() {
        assert_eq!(
            mask_known_secret_options("Authorization: Bearer abc.def ok"),
            "Authorization: **** ok"
        );
        assert_eq!(
            mask_known_secret_options("AccountKey=abc==;EndpointSuffix=core"),
            "AccountKey=****;EndpointSuffix=core"
        );
        assert_eq!(
            mask_known_secret_options("the password expired"),
            "the password expired"
        );
        assert_eq!(
            mask_known_secret_options("https://u:p@h/x and http://h2/y"),
            "https://u:****@h/x and http://h2/y"
        );
    }
}
