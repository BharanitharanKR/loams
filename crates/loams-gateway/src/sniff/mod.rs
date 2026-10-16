//! Peek-only classifiers (design §57 §5): they read only what
//! classification needs and keep every byte, so a module can replay them
//! ([`crate::io::Prefixed`]) or forward them unchanged.

pub mod pg;
pub mod tls;

/// Why a sniffer gave up.
#[derive(Debug, thiserror::Error)]
pub enum SniffError {
    /// A socket error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The peer closed before the opening was complete.
    #[error("the peer closed before the opening was complete")]
    Eof,
    /// The opening exceeded its limit (D1020).
    #[error("the opening is larger than {0} bytes")]
    TooLarge(usize),
    /// The bytes are not a TLS ClientHello.
    #[error("not a TLS ClientHello: {0}")]
    NotClientHello(String),
}
