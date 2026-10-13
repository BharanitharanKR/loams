//! Helpers the family's connectors share: secrets by name, endpoint checks,
//! SQL identifiers and polling.

use std::net::{IpAddr, SocketAddr};

use http::Uri;
use loams_connector_sdk::{ConnectorError, EgressCheck, Secret, SecretRef, SecretResolver};

/// Resolve a secret by its config **name** (never a value, Global
/// Constraints); `None` when the config names none.
pub async fn secret(
    secrets: &dyn SecretResolver,
    name: Option<&String>,
) -> Result<Option<Secret>, ConnectorError> {
    match name {
        Some(n) if !n.is_empty() => secrets.resolve(&SecretRef::new(n)).await.map(Some),
        _ => Ok(None),
    }
}

/// A secret's value as text (header values, passwords).
pub fn text(s: &Secret, what: &str) -> Result<String, ConnectorError> {
    s.expose_str()
        .map(|v| v.trim().to_owned())
        .ok_or_else(|| ConnectorError::auth(format!("{what} is not UTF-8")))
}

/// Parse a base URL and check its host against the egress policy before
/// any request (by name, and by address when it is an IP literal). The
/// SDK's client checks every resolved address again at connect.
pub fn base_url(url: &str, egress: &dyn EgressCheck) -> Result<Uri, ConnectorError> {
    let uri: Uri = url
        .trim_end_matches('/')
        .parse()
        .map_err(|e| ConnectorError::fatal(format!("{url:?} is not a URL: {e}")))?;
    let https = match uri.scheme_str() {
        Some("https") => true,
        Some("http") => false,
        _ => {
            return Err(ConnectorError::fatal(format!(
                "{url:?}: http or https only"
            )));
        }
    };
    let host = uri
        .host()
        .ok_or_else(|| ConnectorError::fatal(format!("{url:?} has no host")))?
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
    egress
        .check_host(host, port)
        .map_err(|e| ConnectorError::fatal(e.to_string()))?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        egress
            .check_addr(&SocketAddr::new(ip, port))
            .map_err(|e| ConnectorError::fatal(e.to_string()))?;
    }
    Ok(uri)
}

/// `base` joined with `path` (which starts with `/`) and an optional query.
pub fn join(base: &Uri, path: &str) -> Result<Uri, ConnectorError> {
    let b = base.to_string();
    format!("{}{path}", b.trim_end_matches('/'))
        .parse()
        .map_err(|e| ConnectorError::fatal(format!("URL: {e}")))
}

/// A table name for SQL text: one to three dot-separated identifiers of
/// letters, digits, `_` and `$`, not starting with a digit. Anything else is
/// refused rather than quoted, so a config can never inject SQL.
pub fn table_name(name: &str) -> Result<&str, ConnectorError> {
    let ok = !name.is_empty()
        && name.split('.').count() <= 3
        && name.split('.').all(|part| {
            !part.is_empty()
                && !part.starts_with(|c: char| c.is_ascii_digit())
                && part
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        });
    if ok {
        Ok(name)
    } else {
        Err(ConnectorError::fatal(format!(
            "{name:?} is not a plain table name (letters, digits, _ and $, up to three parts)"
        )))
    }
}

/// A column name as a double-quoted SQL identifier.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A string as a single-quoted SQL literal.
pub fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The first `n` characters of a server's error text, on one line.
pub fn snippet(body: &[u8], n: usize) -> String {
    let s = String::from_utf8_lossy(body);
    let one: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    one.chars().take(n).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::DenyLocal;

    #[test]
    fn table_names_and_quoting() {
        assert!(table_name("analytics.events").is_ok());
        assert!(table_name("cat.sch.t_1$").is_ok());
        assert!(table_name("t; drop table x").is_err());
        assert!(table_name("1abc").is_err());
        assert!(table_name("a.b.c.d").is_err());
        assert_eq!(quote_ident(r#"we"ird"#), r#""we""ird""#);
        assert_eq!(quote_literal("it's"), "'it''s'");
    }

    #[test]
    fn urls_are_egress_checked() {
        assert!(base_url("http://127.0.0.1:8123", &DenyLocal).is_err());
        assert!(base_url("ftp://x", &DenyLocal).is_err());
        let u = base_url("https://warehouse.example.com/", &DenyLocal).expect("ok");
        assert_eq!(
            join(&u, "/api/2.0/sql/statements")
                .expect("join")
                .to_string(),
            "https://warehouse.example.com/api/2.0/sql/statements"
        );
    }
}
