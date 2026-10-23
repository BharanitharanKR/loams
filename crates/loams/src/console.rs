//! The console, served by the engine itself at `/ui/` (design §19 §3).
//!
//! The console ships with each deployment (owner, 2026-10-23; design 78
//! retires the hosted console): the engine image and the Linux packages put
//! the built console (`web/apps/console/dist`, Vite `base: '/ui/'`) in
//! [`DEFAULT_DIR`], and the engine serves it on its own HTTP listeners, the
//! main port and the `tailscale serve` target, next to the API. So the
//! console is always same-origin with the engine it manages: it needs no
//! CORS, its build's `connect-src 'self'` holds, and it is reachable exactly
//! where the API is (loopback, or the deployment's tailnet), never publicly
//! (design §69).
//!
//! The files are public, like any web app's shell: they carry no data, and
//! everything the console then asks for goes through the authentication
//! layer like any other call. They are mounted outside that layer.
//!
//! Single-page routing: a path with no file behind it gets `index.html`; a
//! path that looks like a missing asset stays a 404.

use std::path::{Path, PathBuf};

use axum::Router;
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use tower_http::services::ServeDir;

/// Where the images and the Linux packages install the console.
pub const DEFAULT_DIR: &str = "/usr/share/loams/console";

/// The path the console is mounted at.
pub const MOUNT: &str = "/ui";

/// Headers on every console response. The build's own `<meta>` CSP limits
/// scripts and connections; a meta CSP cannot set `frame-ancestors`, so the
/// header does.
const SECURITY_HEADERS: [(&str, &str); 3] = [
    ("x-content-type-options", "nosniff"),
    ("referrer-policy", "no-referrer"),
    ("content-security-policy", "frame-ancestors 'none'"),
];

/// Where the console is served from: `explicit` (`--console-dir`), else
/// [`DEFAULT_DIR`] when it holds a build. `None` when there is no build
/// there, so a source build without the console still starts.
pub fn resolve(explicit: Option<&Path>) -> Option<PathBuf> {
    let dir = explicit.map_or_else(|| PathBuf::from(DEFAULT_DIR), Path::to_path_buf);
    dir.join("index.html").is_file().then_some(dir)
}

/// The console's routes: `/ui/…` from `dir`, and `/` redirecting to `/ui/`.
/// `None` when `dir` holds no `index.html`.
pub fn router(dir: &Path) -> Option<Router> {
    let index = dir.join("index.html");
    if !index.is_file() {
        return None;
    }
    let files = ServeDir::new(dir)
        .append_index_html_on_directories(true)
        .fallback(app_shell(index));
    let mount = Router::new()
        .route("/", get(|| async { Redirect::temporary("/ui/") }))
        .nest_service(MOUNT, files)
        .layer(axum::middleware::map_response(secure));
    Some(mount)
}

async fn secure(mut response: Response) -> Response {
    let headers = response.headers_mut();
    for (name, value) in SECURITY_HEADERS {
        headers.insert(name, HeaderValue::from_static(value));
    }
    response
}

/// `index.html` for a client-side route, a 404 for a missing asset.
fn app_shell(index: PathBuf) -> Router {
    Router::new().fallback(move |uri: Uri| {
        let index = index.clone();
        async move {
            let path = uri.path();
            if looks_like_asset(path) {
                return (
                    StatusCode::NOT_FOUND,
                    [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                    format!("not found: {path}\n"),
                )
                    .into_response();
            }
            match tokio::fs::read(&index).await {
                Ok(body) => (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                        (header::CACHE_CONTROL, "no-cache"),
                    ],
                    body,
                )
                    .into_response(),
                Err(err) => {
                    tracing::warn!(%err, path = %index.display(), "the console's index.html is unreadable");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                        "the console's index.html is unreadable\n",
                    )
                        .into_response()
                }
            }
        }
    })
}

/// Whether a path names a file the build ships, rather than a client-side route.
fn looks_like_asset(path: &str) -> bool {
    path.rsplit('/')
        .next()
        .is_some_and(|last| last.contains('.') && !last.contains(' '))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt as _;

    fn build() -> tempfile::TempDir {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("assets")).unwrap();
        std::fs::write(
            dir.path().join("index.html"),
            "<!doctype html><title>Loams</title>",
        )
        .unwrap();
        std::fs::write(dir.path().join("config.json"), "{}").unwrap();
        std::fs::write(dir.path().join("assets/app.js"), "console.log(1)").unwrap();
        dir
    }

    async fn get(router: &Router, path: &str) -> Response {
        router
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[test]
    fn no_build_no_mount() {
        let dir = tempfile::TempDir::new().unwrap();
        assert!(router(dir.path()).is_none());
        assert!(resolve(Some(dir.path())).is_none());
        assert!(resolve(Some(build().path())).is_some());
    }

    #[tokio::test]
    async fn serves_the_shell_assets_and_the_config() {
        let dir = build();
        let router = router(dir.path()).unwrap();

        let index = get(&router, "/ui/").await;
        assert_eq!(index.status(), StatusCode::OK);
        assert_eq!(index.headers()["x-content-type-options"], "nosniff");
        assert_eq!(
            index.headers()["content-security-policy"],
            "frame-ancestors 'none'"
        );
        let asset = get(&router, "/ui/assets/app.js").await;
        assert_eq!(asset.status(), StatusCode::OK);
        // The engine-served console's config is the build's own `{}`:
        // same origin, no engine URL.
        let config = get(&router, "/ui/config.json").await;
        assert_eq!(config.status(), StatusCode::OK);
        // A client-side route boots the app; a missing asset is a 404.
        let route = get(&router, "/ui/projects/x").await;
        assert_eq!(route.status(), StatusCode::OK);
        assert_eq!(
            route.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        let missing = get(&router, "/ui/assets/gone.js").await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        // The root lands on the console.
        let redirect = get(&router, "/").await;
        assert_eq!(redirect.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(redirect.headers()[header::LOCATION], "/ui/");
    }

    #[test]
    fn a_route_is_not_mistaken_for_a_missing_asset() {
        for route in ["/", "/settings", "/projects/loams"] {
            assert!(!looks_like_asset(route), "{route}");
        }
        for asset in ["/assets/app.js", "/favicon.svg", "/cordis.html"] {
            assert!(looks_like_asset(asset), "{asset}");
        }
    }
}
