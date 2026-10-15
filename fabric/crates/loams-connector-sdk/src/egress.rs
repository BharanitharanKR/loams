//! The egress hook (CN2 Task 4; Task 25 fills it with `loams-flow`'s
//! `EgressPolicy`). Every family crate connects through [`crate::net::connect`]
//! or [`crate::http::client`], which check the host before DNS, resolve it
//! through [`EgressCheck::resolver`] (the SDK's resolver hook), and call
//! [`EgressCheck::check_addr`] on every address they are about to dial. A
//! name that resolves to a forbidden address is refused at connect time, on
//! every connection, so a DNS answer that changes between validation and use
//! (DNS rebinding) gains nothing.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;

/// Why a connection was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("egress refused: {0}")]
pub struct EgressDenied(pub String);

/// The SDK's resolver hook: how a host name becomes the addresses
/// [`crate::net::connect`] dials. The default is the system resolver; tests
/// and the runtime may substitute their own. An IP literal never reaches it.
#[async_trait]
pub trait Resolve: Send + Sync {
    async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>>;
}

/// The operating system's resolver (`getaddrinfo` through tokio).
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemResolver;

#[async_trait]
impl Resolve for SystemResolver {
    async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
        Ok(tokio::net::lookup_host((host, port)).await?.collect())
    }
}

pub trait EgressCheck: Send + Sync {
    /// Before DNS: the host name a config, a request or a redirect names.
    fn check_host(&self, host: &str, port: u16) -> Result<(), EgressDenied> {
        let _ = (host, port);
        Ok(())
    }
    /// After DNS, at connect: the address about to be dialled.
    fn check_addr(&self, addr: &SocketAddr) -> Result<(), EgressDenied>;
    /// The resolver [`crate::net::connect`] uses for this check's
    /// connections (the SDK's resolver hook).
    fn resolver(&self) -> Arc<dyn Resolve> {
        Arc::new(SystemResolver)
    }
}

/// Allows everything. For the runtime's own trusted connections (the
/// Kubernetes API, the Fabric) and for tests; never for an instance's
/// connections, which go through `loams-flow`'s `EgressPolicy` (Task 25).
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl EgressCheck for AllowAll {
    fn check_addr(&self, _addr: &SocketAddr) -> Result<(), EgressDenied> {
        Ok(())
    }
}

/// Refuses loopback, link-local (cloud metadata at 169.254.169.254) and
/// unspecified addresses: the minimal guard the SDK's own tests use.
/// `loams-flow`'s `EgressPolicy` is the full default-deny list (Task 25).
#[derive(Debug, Clone, Copy, Default)]
pub struct DenyLocal;

impl EgressCheck for DenyLocal {
    fn check_addr(&self, addr: &SocketAddr) -> Result<(), EgressDenied> {
        let ip = addr.ip();
        let local = match ip {
            IpAddr::V4(v4) => v4.is_loopback() || v4.is_link_local() || v4.is_unspecified(),
            IpAddr::V6(v6) => {
                v6.is_loopback()
                    || v6.is_unspecified()
                    || (v6.segments()[0] & 0xffc0) == 0xfe80
                    || v6.to_ipv4_mapped().is_some_and(|v4| {
                        v4.is_loopback() || v4.is_link_local() || v4.is_unspecified()
                    })
            }
        };
        if local {
            Err(EgressDenied(format!("{ip} is a local address")))
        } else {
            Ok(())
        }
    }
}
