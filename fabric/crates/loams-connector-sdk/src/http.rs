//! `sdk::http`: the HTTP client every HTTP-speaking family uses. Each request
//! opens its connection through [`crate::net::connect`] (egress checked after
//! DNS) and speaks HTTP/1.1 with hyper; `https` goes through rustls. Redirects
//! are **not** followed: a family that follows one makes a new request, which
//! is egress-checked again (Task 25's `redirect_rechecked`).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;

use crate::egress::EgressCheck;
use crate::error::{ConnectorError, ErrorClass};
use crate::net;

/// An HTTP client bound to an instance's egress check.
#[derive(Clone)]
pub struct HttpClient {
    egress: Arc<dyn EgressCheck>,
    tls: Arc<rustls::ClientConfig>,
    timeout: Duration,
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// The instance's HTTP client, with the default TLS roots and a 30 s
/// per-request timeout.
pub fn client(egress: Arc<dyn EgressCheck>) -> HttpClient {
    HttpClient {
        egress,
        tls: net::default_tls_config(),
        timeout: Duration::from_secs(30),
    }
}

impl HttpClient {
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_tls_config(mut self, tls: Arc<rustls::ClientConfig>) -> Self {
        self.tls = tls;
        self
    }

    /// Send one request and read the whole response. Transport failures are
    /// `Retryable`; the response's status is returned as is (use
    /// [`classify_status`] to turn it into an error class).
    pub async fn send(&self, req: Request<Bytes>) -> Result<Response<Bytes>, ConnectorError> {
        tokio::time::timeout(self.timeout, self.send_inner(req))
            .await
            .map_err(|_| ConnectorError::retryable("HTTP request timed out"))?
    }

    async fn send_inner(&self, req: Request<Bytes>) -> Result<Response<Bytes>, ConnectorError> {
        let uri = req.uri().clone();
        let (https, host, port) = target(&uri)?;
        let (mut parts, body) = req.into_parts();
        // hyper's HTTP/1 client sends the request target as given: use the
        // origin form and set Host.
        let path = uri
            .path_and_query()
            .map_or("/", |p| p.as_str())
            .parse::<Uri>()
            .map_err(|e| ConnectorError::fatal(format!("bad request path: {e}")))?;
        parts.uri = path;
        if !parts.headers.contains_key(http::header::HOST) {
            let host_header = if (https && port == 443) || (!https && port == 80) {
                host.clone()
            } else {
                format!("{host}:{port}")
            };
            parts.headers.insert(
                http::header::HOST,
                host_header
                    .parse()
                    .map_err(|e| ConnectorError::fatal(format!("bad host {host}: {e}")))?,
            );
        }
        let req = Request::from_parts(parts, Full::new(body));
        if https {
            let stream =
                net::connect_tls(&host, port, &*self.egress, Some(self.tls.clone())).await?;
            exchange(TokioIo::new(stream), req).await
        } else {
            let stream = net::connect(&host, port, &*self.egress).await?;
            exchange(TokioIo::new(stream), req).await
        }
    }
}

fn target(uri: &Uri) -> Result<(bool, String, u16), ConnectorError> {
    let https = match uri.scheme_str() {
        Some("https") => true,
        Some("http") => false,
        other => {
            return Err(ConnectorError::fatal(format!(
                "unsupported URL scheme {other:?}"
            )));
        }
    };
    let host = uri
        .host()
        .ok_or_else(|| ConnectorError::fatal("URL has no host"))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
    Ok((https, host, port))
}

async fn exchange<T>(
    io: TokioIo<T>,
    req: Request<Full<Bytes>>,
) -> Result<Response<Bytes>, ConnectorError>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| ConnectorError::retryable(format!("HTTP handshake: {e}")))?;
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| ConnectorError::retryable(format!("HTTP request: {e}")))?;
    let (parts, body) = resp.into_parts();
    let body = body
        .collect()
        .await
        .map_err(|e| ConnectorError::retryable(format!("HTTP body: {e}")))?
        .to_bytes();
    driver.abort();
    Ok(Response::from_parts(parts, body))
}

/// The error class of an HTTP status (CN1 Task 9's HTTP semantics): `None`
/// for success; 408, 425 and 5xx are `Retryable` and 429 is `Throttled`,
/// both honouring `Retry-After` (seconds); 401 and 403 are `Auth`; any other
/// 4xx is `Poison` (the request itself is bad: DLQ).
pub fn classify_status(status: StatusCode, retry_after: Option<&str>) -> Option<ErrorClass> {
    let after = retry_after
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    match status.as_u16() {
        100..=399 => None,
        429 => Some(ErrorClass::Throttled {
            after: after.unwrap_or(Duration::from_secs(1)),
        }),
        408 | 425 | 500..=599 => Some(ErrorClass::Retryable { after }),
        401 | 403 => Some(ErrorClass::Auth),
        _ => Some(ErrorClass::Poison),
    }
}

/// [`classify_status`] over a response, as an error carrying the status line.
pub fn error_for_response(resp: &Response<Bytes>) -> Option<ConnectorError> {
    let retry_after = resp
        .headers()
        .get(http::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok());
    classify_status(resp.status(), retry_after)
        .map(|class| ConnectorError::new(class, format!("HTTP {}", resp.status())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::egress::{AllowAll, DenyLocal};

    #[test]
    fn statuses_classify() {
        assert_eq!(classify_status(StatusCode::OK, None), None);
        assert_eq!(
            classify_status(StatusCode::TOO_MANY_REQUESTS, Some("7")),
            Some(ErrorClass::Throttled {
                after: Duration::from_secs(7)
            })
        );
        assert_eq!(
            classify_status(StatusCode::SERVICE_UNAVAILABLE, None),
            Some(ErrorClass::Retryable { after: None })
        );
        assert_eq!(
            classify_status(StatusCode::UNAUTHORIZED, None),
            Some(ErrorClass::Auth)
        );
        assert_eq!(
            classify_status(StatusCode::UNPROCESSABLE_ENTITY, None),
            Some(ErrorClass::Poison)
        );
    }

    #[tokio::test]
    async fn get_through_the_egress_check() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            if let Ok((mut s, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = s.read(&mut buf).await;
                let _ = s
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                    )
                    .await;
            }
        });
        let url = format!("http://127.0.0.1:{}/x?y=1", addr.port());
        let req = Request::get(&url).body(Bytes::new()).expect("request");
        let resp = client(Arc::new(AllowAll)).send(req).await.expect("send");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body().as_ref(), b"ok");

        let req = Request::get(&url).body(Bytes::new()).expect("request");
        let err = client(Arc::new(DenyLocal))
            .send(req)
            .await
            .expect_err("loopback refused");
        assert!(err.message.contains("egress refused"));
    }
}
