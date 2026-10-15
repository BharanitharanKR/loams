//! Dev-server previews through the remote listener (design §53 §5.8, §6.3,
//! Q806; plan SD1 Task 26, ruling T7-12).
//!
//! The daemon's preview proxy listens on loopback only
//! (`http://<name>.<project>.localhost:7331`), which a browser elsewhere
//! cannot reach. The remote listener forwards `/preview/…` to it, behind the
//! same origin and token checks as the Connect API:
//!
//! - `/preview/<name>.<project>/<path>` with `Authorization: Bearer <token>`
//!   (a CLI or curl), the token checked exactly as for a method.
//! - `POST /preview/-/ticket` with the bearer token, from a configured console
//!   origin, body `{"preview": "<name>.<project>"}`: answers a **preview
//!   ticket**, a random 256-bit capability for that one preview that lasts no
//!   longer than the token (at most [`TICKET_MAX`]), as
//!   `{"base": "/preview/pt_<hex>/", "expiresAtMs": …}`.
//! - `/preview/pt_<hex>/<path>`: the ticket's preview. A frame cannot send an
//!   `Authorization` header, and the listener sets no cookies (§53 §6.3), so
//!   the console frames this URL; relative URLs keep the ticket.
//!
//! Every preview answer carries `Content-Security-Policy: frame-ancestors`
//! with the configured console origins only (`'none'` without any), and
//! `Referrer-Policy: same-origin`, so a ticket never leaves in a `Referer`.
//! The bearer token is never forwarded to the dev server. The origin check
//! also admits the listener's own origin on preview paths (not the ticket
//! endpoint): a preview page's own `fetch`, form posts and HMR WebSocket.
//! The dev server's pages run on the listener's origin, but the API answers
//! bearer tokens only and refuses that origin, so they reach no method.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::Request;
use axum::response::Response;
use connectrpc::ErrorCode;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use hyper_util::rt::TokioIo;

use crate::cors;
use crate::remote::{RemoteGuard, authenticate, refuse};

/// Where previews live on the remote listener.
pub const ROOT: &str = "/preview/";

/// The ticket endpoint.
pub const TICKET_PATH: &str = "/preview/-/ticket";

/// A ticket's prefix; preview names always hold a dot, tickets never do.
pub const TICKET_PREFIX: &str = "pt_";

/// The longest a ticket lasts, whatever the token's `exp`.
pub const TICKET_MAX: Duration = Duration::from_secs(8 * 60 * 60);

/// Live tickets kept at most; the soonest to expire goes first.
pub const MAX_TICKETS: usize = 64;

/// The longest ticket request body read.
const MAX_TICKET_BODY: usize = 1024;

/// The daemon's preview proxy and the tickets minted for it.
#[derive(Debug)]
pub struct Previews {
    /// The loopback preview proxy's port; `None`: previews are off.
    port: Option<u16>,
    /// `frame-ancestors` for every preview answer.
    frame_ancestors: HeaderValue,
    tickets: Mutex<HashMap<String, Ticket>>,
}

#[derive(Debug, Clone)]
struct Ticket {
    /// `<name>.<project>`.
    preview: String,
    expires: SystemTime,
}

impl Previews {
    /// Previews through the proxy on `port`, framable by `cors`' origins.
    pub fn new(port: Option<u16>, cors: &cors::CorsPolicy) -> Self {
        Self {
            port,
            frame_ancestors: frame_ancestors(cors.origins()),
            tickets: Mutex::new(HashMap::new()),
        }
    }

    fn mint(&self, preview: &str, token_exp: u64) -> Option<(String, SystemTime)> {
        let now = SystemTime::now();
        let cap = now + TICKET_MAX;
        let expires = UNIX_EPOCH
            .checked_add(Duration::from_secs(token_exp))
            .map_or(cap, |exp| exp.min(cap));
        if expires <= now {
            return None;
        }
        let mut bytes = [0u8; 32];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes).ok()?;
        let id: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let mut tickets = self.tickets.lock().ok()?;
        tickets.retain(|_, t| t.expires > now);
        while tickets.len() >= MAX_TICKETS {
            let soonest = tickets
                .iter()
                .min_by_key(|(_, t)| t.expires)
                .map(|(k, _)| k.clone())?;
            tickets.remove(&soonest);
        }
        tickets.insert(
            id.clone(),
            Ticket {
                preview: preview.to_owned(),
                expires,
            },
        );
        Some((id, expires))
    }

    fn redeem(&self, id: &str) -> Option<String> {
        let tickets = self.tickets.lock().ok()?;
        let ticket = tickets.get(id)?;
        (ticket.expires > SystemTime::now()).then(|| ticket.preview.clone())
    }
}

/// `'none'`, or the console origins separated by spaces.
pub fn frame_ancestors(origins: &[String]) -> HeaderValue {
    let list = if origins.is_empty() {
        "'none'".to_owned()
    } else {
        origins.join(" ")
    };
    HeaderValue::from_str(&format!("frame-ancestors {list}"))
        .unwrap_or_else(|_| HeaderValue::from_static("frame-ancestors 'none'"))
}

/// Whether `name` is `<name>.<project>`: two DNS labels as the preview
/// catalog makes them (lower-case letters, digits and inner hyphens).
pub fn valid_preview(name: &str) -> bool {
    let mut labels = name.split('.');
    let (Some(a), Some(b), None) = (labels.next(), labels.next(), labels.next()) else {
        return false;
    };
    [a, b].iter().all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
    })
}

fn valid_ticket(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}

/// What a `/preview/…` path asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// `POST /preview/-/ticket`.
    Ticket,
    /// `/preview/<name>.<project>/<rest>`, bearer only.
    Named { preview: String, rest: String },
    /// `/preview/pt_<hex>/<rest>`.
    Ticketed { ticket: String, rest: String },
    /// `/preview/<segment>` without its slash: redirect to add it.
    Slash,
    /// Anything else under `/preview/`.
    Unknown,
}

/// Reads `path` (no query).
pub fn route(path: &str) -> Route {
    if path == TICKET_PATH {
        return Route::Ticket;
    }
    let Some(tail) = path.strip_prefix(ROOT) else {
        return Route::Unknown;
    };
    let (segment, rest) = match tail.find('/') {
        Some(at) => (&tail[..at], &tail[at..]),
        None => (tail, ""),
    };
    let known = if let Some(ticket) = segment.strip_prefix(TICKET_PREFIX) {
        valid_ticket(ticket)
    } else {
        valid_preview(segment)
    };
    if !known {
        return Route::Unknown;
    }
    if rest.is_empty() {
        return Route::Slash;
    }
    match segment.strip_prefix(TICKET_PREFIX) {
        Some(ticket) => Route::Ticketed {
            ticket: ticket.to_owned(),
            rest: rest.to_owned(),
        },
        None => Route::Named {
            preview: segment.to_owned(),
            rest: rest.to_owned(),
        },
    }
}

/// The request's own origin, `https://<authority>`.
fn own_origin(request: &Request) -> Option<String> {
    let authority = request
        .uri()
        .authority()
        .map(|a| a.as_str().to_owned())
        .or_else(|| {
            request
                .headers()
                .get(header::HOST)
                .and_then(|h| h.to_str().ok())
                .map(str::to_owned)
        })?;
    Some(format!("https://{authority}"))
}

fn plain(status: StatusCode, message: &str) -> Response {
    let mut response = Response::new(Body::from(message.to_owned()));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// Serves a `/preview/…` request. `guard` holds the checks and the tickets.
pub async fn serve(guard: &RemoteGuard, request: Request) -> Response {
    let route = route(request.uri().path());
    let origin = request.headers().get(header::ORIGIN).cloned();
    let console = origin.as_ref().is_some_and(|o| guard.cors().allows(o));
    let same_origin = origin.as_ref().is_some_and(|o| {
        own_origin(&request).is_some_and(|own| o.to_str().is_ok_and(|o| o == own))
    });
    let ticket_route = route == Route::Ticket;
    // The ticket endpoint answers console origins (and non-browser clients)
    // only; a preview page may call its own preview but never mint.
    if origin.is_some() && !(console || (same_origin && !ticket_route)) {
        tracing::debug!("remote: refused a preview request from an origin not allowed (403)");
        return refuse(
            ErrorCode::PermissionDenied,
            Some("origin_not_allowed"),
            "this origin may not call this agentd (--allow-origin)",
        );
    }
    if request.method() == Method::OPTIONS {
        // Only the console's preflight for the ticket endpoint is answered.
        let asks_post = request
            .headers()
            .get(header::ACCESS_CONTROL_REQUEST_METHOD)
            .is_some_and(|m| m == "POST");
        return match (&origin, ticket_route && console && asks_post) {
            (Some(origin), true) => {
                let mut response = plain(StatusCode::NO_CONTENT, "");
                cors::preflight_headers(origin, response.headers_mut());
                response
            }
            _ => refuse(
                ErrorCode::PermissionDenied,
                Some("origin_not_allowed"),
                "only a CORS preflight from an allowed origin is answered",
            ),
        };
    }
    let mut response = match route {
        Route::Ticket => ticket(guard, request).await,
        Route::Unknown => plain(StatusCode::NOT_FOUND, "no such preview path"),
        Route::Slash => {
            let mut to = format!("{}/", request.uri().path());
            if let Some(query) = request.uri().query() {
                to.push('?');
                to.push_str(query);
            }
            let mut response = plain(StatusCode::PERMANENT_REDIRECT, "");
            if let Ok(value) = HeaderValue::from_str(&to) {
                response.headers_mut().insert(header::LOCATION, value);
            }
            response
        }
        Route::Named { preview, rest } => match authenticate(guard, request.headers()).await {
            Err(refusal) => refusal,
            Ok(_) => {
                let prefix = format!("{ROOT}{preview}");
                forward(guard, &preview, &rest, &prefix, request).await
            }
        },
        Route::Ticketed { ticket, rest } => match guard.previews().redeem(&ticket) {
            Some(preview) => {
                let prefix = format!("{ROOT}{TICKET_PREFIX}{ticket}");
                forward(guard, &preview, &rest, &prefix, request).await
            }
            None => {
                if guard.fail_delay().await {
                    tracing::warn!("remote: refused a preview with an unknown or expired ticket");
                    refuse(
                        ErrorCode::Unauthenticated,
                        Some("unauthenticated"),
                        "this preview link has expired; open the preview again",
                    )
                } else {
                    crate::remote::too_many_failures()
                }
            }
        },
    };
    let headers = response.headers_mut();
    headers.append(
        header::CONTENT_SECURITY_POLICY,
        guard.previews().frame_ancestors.clone(),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("same-origin"),
    );
    if ticket_route && let Some(origin) = &origin {
        cors::response_headers(origin, headers);
    }
    response
}

async fn ticket(guard: &RemoteGuard, request: Request) -> Response {
    if request.method() != Method::POST {
        return plain(StatusCode::METHOD_NOT_ALLOWED, "POST a ticket request");
    }
    let exp = match authenticate(guard, request.headers()).await {
        Ok(exp) => exp,
        Err(refusal) => return refusal,
    };
    let Ok(body) = axum::body::to_bytes(request.into_body(), MAX_TICKET_BODY).await else {
        return plain(StatusCode::PAYLOAD_TOO_LARGE, "ticket request too large");
    };
    let preview = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("preview")?.as_str().map(str::to_owned))
        .filter(|p| valid_preview(p));
    let Some(preview) = preview else {
        return refuse(
            ErrorCode::InvalidArgument,
            None,
            "expected {\"preview\": \"<name>.<project>\"}",
        );
    };
    if guard.previews().port.is_none() {
        return refuse(
            ErrorCode::Unavailable,
            None,
            "previews are off on this agentd",
        );
    }
    let Some((id, expires)) = guard.previews().mint(&preview, exp) else {
        return refuse(
            ErrorCode::Unauthenticated,
            Some("unauthenticated"),
            "the token has expired",
        );
    };
    let expires_ms = expires
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let body = serde_json::json!({
        "base": format!("{ROOT}{TICKET_PREFIX}{id}/"),
        "expiresAtMs": expires_ms,
    });
    let mut response = Response::new(Body::from(body.to_string()));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// Headers that end at a hop (RFC 9110 §7.6.1), plus our own credentials.
const HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

fn strip(headers: &mut HeaderMap, keep_upgrade: bool) {
    let nominated: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|v| v.trim().to_ascii_lowercase())
        .collect();
    let upgrade = headers.get(header::UPGRADE).cloned();
    for name in nominated
        .iter()
        .map(String::as_str)
        .chain(HOP.iter().copied())
    {
        headers.remove(name);
    }
    if keep_upgrade && let Some(upgrade) = upgrade {
        headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
        headers.insert(header::UPGRADE, upgrade);
    }
}

fn wants_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"))
        && headers
            .get_all(header::CONNECTION)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|v| v.trim().eq_ignore_ascii_case("upgrade"))
}

/// `Location` from the preview proxy (`http://<host>/…`) or the dev server
/// (`/…`), moved under `prefix`.
fn rewrite_location(headers: &mut HeaderMap, upstream: &str, prefix: &str) {
    let Some(value) = headers.get(header::LOCATION).and_then(|v| v.to_str().ok()) else {
        return;
    };
    let path = value
        .strip_prefix(&format!("http://{upstream}"))
        .or_else(|| (value.starts_with('/') && !value.starts_with("//")).then_some(value));
    let Some(path) = path.filter(|p| p.is_empty() || p.starts_with(['/', '?', '#'])) else {
        return;
    };
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    if let Ok(value) = HeaderValue::from_str(&format!("{prefix}{path}")) {
        headers.insert(header::LOCATION, value);
    }
}

async fn forward(
    guard: &RemoteGuard,
    preview: &str,
    rest: &str,
    prefix: &str,
    mut request: Request,
) -> Response {
    let Some(port) = guard.previews().port else {
        return plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "previews are off on this agentd",
        );
    };
    let upstream = format!("{preview}.localhost:{port}");
    let mut target = rest.to_owned();
    if let Some(query) = request.uri().query() {
        target.push('?');
        target.push_str(query);
    }
    let Ok(uri) = target.parse::<http::Uri>() else {
        return plain(StatusCode::BAD_REQUEST, "bad preview path");
    };
    let upgrade = wants_upgrade(request.headers());
    let browser_upgrade = upgrade.then(|| hyper::upgrade::on(&mut request));
    let (parts, body) = request.into_parts();
    let mut headers = parts.headers;
    strip(&mut headers, upgrade);
    // The bearer token is the person's; a dev server never sees it.
    headers.remove(header::AUTHORIZATION);
    headers.remove(HeaderName::from_static("forwarded"));
    let Ok(host) = HeaderValue::from_str(&upstream) else {
        return plain(StatusCode::BAD_REQUEST, "bad preview name");
    };
    headers.insert(header::HOST, host);
    if let Ok(value) = HeaderValue::from_str(prefix) {
        headers.insert(HeaderName::from_static("x-forwarded-prefix"), value);
    }
    let mut outgoing = http::Request::new(body);
    *outgoing.method_mut() = parts.method;
    *outgoing.uri_mut() = uri;
    *outgoing.headers_mut() = headers;

    let unreachable = || {
        plain(
            StatusCode::BAD_GATEWAY,
            "This preview is not running. Start the project's dev server and try again.",
        )
    };
    let Ok(stream) = tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).await
    else {
        return unreachable();
    };
    let Ok((mut sender, connection)) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await
    else {
        return unreachable();
    };
    tokio::spawn(async move {
        let _ = connection.with_upgrades().await;
    });
    let Ok(mut response) = sender.send_request(outgoing).await else {
        return unreachable();
    };
    if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        let Some(browser_upgrade) = browser_upgrade else {
            return plain(StatusCode::BAD_GATEWAY, "unsolicited upgrade");
        };
        let server_upgrade = hyper::upgrade::on(&mut response);
        tokio::spawn(async move {
            if let Ok((browser, server)) = tokio::try_join!(browser_upgrade, server_upgrade) {
                let _ = tokio::io::copy_bidirectional(
                    &mut TokioIo::new(browser),
                    &mut TokioIo::new(server),
                )
                .await;
            }
        });
        let (mut parts, _) = response.into_parts();
        strip(&mut parts.headers, true);
        return Response::from_parts(parts, Body::empty());
    }
    let (mut parts, body) = response.into_parts();
    strip(&mut parts.headers, false);
    rewrite_location(&mut parts.headers, &upstream, prefix);
    Response::from_parts(parts, Body::new(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_are_read_strictly() {
        let hex = "a".repeat(64);
        assert_eq!(route("/preview/-/ticket"), Route::Ticket);
        assert_eq!(
            route("/preview/dev.my-app/x/y"),
            Route::Named {
                preview: "dev.my-app".into(),
                rest: "/x/y".into()
            }
        );
        assert_eq!(
            route(&format!("/preview/pt_{hex}/")),
            Route::Ticketed {
                ticket: hex.clone(),
                rest: "/".into()
            }
        );
        assert_eq!(route("/preview/dev.my-app"), Route::Slash);
        for bad in [
            "/preview/",
            "/preview/dev/",
            "/preview/Dev.app/",
            "/preview/a.b.c/",
            "/preview/-a.b/",
            "/preview/pt_abc/",
            "/preview/..%2f/",
            &format!("/preview/pt_{}/", "A".repeat(64)),
        ] {
            assert_eq!(route(bad), Route::Unknown, "{bad}");
        }
    }

    #[test]
    fn locations_move_under_the_prefix() {
        let mut headers = HeaderMap::new();
        let upstream = "dev.app.localhost:7331";
        for (from, to) in [
            ("http://dev.app.localhost:7331/login", "/preview/p/login"),
            ("http://dev.app.localhost:7331", "/preview/p/"),
            ("/next?x=1", "/preview/p/next?x=1"),
            ("//evil.example/x", "//evil.example/x"),
            ("https://elsewhere.example/", "https://elsewhere.example/"),
        ] {
            headers.insert(header::LOCATION, HeaderValue::from_static(from));
            rewrite_location(&mut headers, upstream, "/preview/p");
            assert_eq!(headers[header::LOCATION], to, "{from}");
        }
    }

    #[test]
    fn frame_ancestors_name_the_console_only() {
        assert_eq!(frame_ancestors(&[]), "frame-ancestors 'none'");
        assert_eq!(
            frame_ancestors(&["https://a.example".into(), "https://b.example".into()]),
            "frame-ancestors https://a.example https://b.example"
        );
    }

    #[test]
    fn tickets_expire_with_the_token_and_are_bounded() {
        let previews = Previews::new(Some(1), &cors::CorsPolicy::default());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(previews.mint("a.b", now.saturating_sub(1)).is_none());
        let (id, expires) = previews.mint("a.b", now + 60).unwrap();
        assert!(expires <= SystemTime::now() + Duration::from_secs(61));
        assert_eq!(previews.redeem(&id).as_deref(), Some("a.b"));
        assert!(previews.redeem(&"0".repeat(64)).is_none());
        let (_, capped) = previews.mint("a.b", u64::MAX / 2).unwrap();
        assert!(capped <= SystemTime::now() + TICKET_MAX);
        for _ in 0..(MAX_TICKETS * 2) {
            previews.mint("a.b", now + 600).unwrap();
        }
        assert!(previews.tickets.lock().unwrap().len() <= MAX_TICKETS);
    }
}
