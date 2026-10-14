//! CORS for the remote listener (design §53 §6.3, D909; plan SD1 Task 7,
//! ruling T0-7: `connectrpc` 0.9.1 has none).
//!
//! - Only the configured console origins get CORS headers, matched exactly
//!   against the request's `Origin` (scheme, host and port, no path).
//! - `Access-Control-Allow-Headers` is the Connect headers plus
//!   `Authorization`, and nothing else.
//! - `Access-Control-Allow-Credentials` is never sent: the browser sends a
//!   bearer token, never a cookie, so there is no CSRF surface.
//! - Any other origin gets no CORS header at all; the listener refuses its
//!   requests (`origin_not_allowed`, 403).

use http::{HeaderMap, HeaderValue, header};

/// The request headers a Connect, gRPC-Web or gRPC client sends from a
/// browser (connect-es' `cors.allowedHeaders`), plus `Authorization`.
pub const ALLOW_HEADERS: &str = "authorization, content-type, connect-protocol-version, \
     connect-timeout-ms, connect-accept-encoding, connect-content-encoding, grpc-timeout, \
     x-grpc-web, x-user-agent";

/// The response headers a browser client may read (connect-es'
/// `cors.exposedHeaders`): the gRPC-Web status trailers sent as headers.
pub const EXPOSE_HEADERS: &str = "grpc-status, grpc-message, grpc-status-details-bin";

/// Connect unary calls are `POST`, or `GET` for side-effect-free ones.
pub const ALLOW_METHODS: &str = "POST, GET";

/// How long a browser may cache a preflight answer, in seconds.
pub const MAX_AGE_SECS: u32 = 7200;

/// The configured origins.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorsPolicy {
    origins: Vec<String>,
}

impl CorsPolicy {
    /// A policy for `origins`, each checked by [`origin`].
    ///
    /// # Errors
    ///
    /// The first origin that is not a plain `https://host[:port]` (or
    /// `http://` on a loopback host), with the reason.
    pub fn new<I, S>(origins: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let origins = origins
            .into_iter()
            .map(|value| origin(value.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { origins })
    }

    /// The configured origins, normalised.
    pub fn origins(&self) -> &[String] {
        &self.origins
    }

    /// Whether `value` (a request's `Origin`) is one of the configured ones.
    pub fn allows(&self, value: &HeaderValue) -> bool {
        value
            .to_str()
            .is_ok_and(|origin| self.origins.iter().any(|allowed| allowed == origin))
    }
}

/// `value` as an origin the allowlist can hold: its ASCII serialisation
/// (`https://console.example.com`, `http://localhost:5173`), accepted only
/// when the value is exactly that, apart from one trailing `/`.
///
/// # Errors
///
/// A wildcard, `null`, a path, query or credentials, a scheme other than
/// `https` (or `http` on a loopback host), or a value that is not a URL.
pub fn origin(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed == "*" || trimmed.eq_ignore_ascii_case("null") {
        return Err(format!(
            "{value:?} is not an origin; name each console origin"
        ));
    }
    let url =
        url::Url::parse(trimmed).map_err(|error| format!("{value:?} is not an origin: {error}"))?;
    let serialised = url.origin().ascii_serialization();
    if serialised != trimmed.strip_suffix('/').unwrap_or(trimmed) {
        return Err(format!(
            "{value:?} is not an origin (scheme://host[:port], no path); did you mean {serialised:?}?"
        ));
    }
    let loopback = match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(domain)) => domain == "localhost",
        None => false,
    };
    match url.scheme() {
        "https" => Ok(serialised),
        "http" if loopback => Ok(serialised),
        _ => Err(format!(
            "{value:?}: a console origin must be https:// (http:// only on loopback)"
        )),
    }
}

/// The headers every answer to an allowed `origin` carries.
pub fn response_headers(origin: &HeaderValue, headers: &mut HeaderMap) {
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static(EXPOSE_HEADERS),
    );
    headers.append(header::VARY, HeaderValue::from_static("origin"));
}

/// The headers of a preflight answer to an allowed `origin`.
pub fn preflight_headers(origin: &HeaderValue, headers: &mut HeaderMap) {
    response_headers(origin, headers);
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(ALLOW_METHODS),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(ALLOW_HEADERS),
    );
    headers.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from(MAX_AGE_SECS),
    );
}

/// Whether `headers` hold any CORS response header (the tests' check that a
/// refused origin learns nothing).
pub fn has_cors_headers(headers: &HeaderMap) -> bool {
    headers
        .keys()
        .any(|name| name.as_str().starts_with("access-control-"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_are_exact() {
        assert_eq!(
            origin("https://console.example.com").unwrap(),
            "https://console.example.com"
        );
        assert_eq!(
            origin("https://console.example.com/").unwrap(),
            "https://console.example.com"
        );
        assert_eq!(
            origin("https://console.example.com:8443").unwrap(),
            "https://console.example.com:8443"
        );
        assert_eq!(
            origin("http://localhost:5173").unwrap(),
            "http://localhost:5173"
        );
        for bad in [
            "*",
            "null",
            "https://console.example.com/app",
            "https://user@console.example.com",
            "http://console.example.com",
            "console.example.com",
            "https://console.example.com?x=1",
            "file:///etc",
        ] {
            assert!(origin(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_policy_matches_the_whole_origin() {
        let policy = CorsPolicy::new(["https://console.example.com"]).unwrap();
        let allowed = HeaderValue::from_static("https://console.example.com");
        assert!(policy.allows(&allowed));
        for other in [
            "https://console.example.com.evil.example",
            "https://evil.example",
            "http://console.example.com",
            "https://console.example.com:444",
            "null",
        ] {
            assert!(!policy.allows(&HeaderValue::from_static(other)), "{other}");
        }
    }

    #[test]
    fn preflight_never_allows_credentials() {
        let mut headers = HeaderMap::new();
        preflight_headers(
            &HeaderValue::from_static("https://console.example.com"),
            &mut headers,
        );
        assert!(!headers.contains_key(header::ACCESS_CONTROL_ALLOW_CREDENTIALS));
        assert_eq!(
            headers[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://console.example.com"
        );
        assert!(has_cors_headers(&headers));
    }
}
