//! The egress hook (CN2 Task 4; Task 25 fills it with `EgressPolicy`). Every
//! family crate connects through [`crate::net::connect`] or
//! [`crate::http::client`], which call [`EgressCheck::check_addr`] after DNS,
//! on every address they are about to dial.

use std::net::{IpAddr, SocketAddr};

/// Why a connection was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("egress refused: {0}")]
pub struct EgressDenied(pub String);

pub trait EgressCheck: Send + Sync {
    /// Before DNS: the host name a config names.
    fn check_host(&self, host: &str, port: u16) -> Result<(), EgressDenied> {
        let _ = (host, port);
        Ok(())
    }
    /// After DNS, at connect: the address about to be dialled.
    fn check_addr(&self, addr: &SocketAddr) -> Result<(), EgressDenied>;
}

/// Allows everything. The default until Task 25's guard; never use it for a
/// namespace an untrusted user configures.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl EgressCheck for AllowAll {
    fn check_addr(&self, _addr: &SocketAddr) -> Result<(), EgressDenied> {
        Ok(())
    }
}

/// Refuses loopback, link-local (cloud metadata at 169.254.169.254) and
/// unspecified addresses: the minimal guard the SDK's own tests use. Task 25's
/// `EgressPolicy` replaces it with the full default-deny list.
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
