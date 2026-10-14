//! The House's TLS listener (HS1 Task 20, §49 §14, D111).
//!
//! Plaintext listeners bind loopback only; a listener any other host can reach
//! is TLS (8443). [`server_config`] reads a certificate chain and its private
//! key (PEM) into a rustls configuration with the ring provider, TLS 1.2 and
//! 1.3, and ALPN `http/1.1` (the front speaks HTTP/1.1, R3.8).
//!
//! A `--single-node` House with a TLS listener and no certificate configured
//! makes its own on first start ([`ensure_self_signed`]): a self-signed
//! certificate for the names it is reached by, written beside its key (mode
//! 0600). A client trusts it by that file (the engine's `--house-ca`, HS1 Task
//! 20's link); nothing else vouches for it.

use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::errors::{ChError, HouseError};

/// The names a self-signed House certificate is for when none are given:
/// loopback, and the service names the release compose stack gives the House.
pub const DEFAULT_NAMES: &[&str] = &["localhost", "127.0.0.1", "house", "loams-house"];

fn bad(message: String) -> HouseError {
    HouseError::from(ChError::bad_arguments(message))
}

/// A TLS server configuration from a PEM certificate chain and private key.
pub fn server_config(cert: &Path, key: &Path) -> Result<Arc<ServerConfig>, HouseError> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .and_then(|certs| certs.collect::<Result<_, _>>())
        .map_err(|err| bad(format!("the TLS certificate {}: {err}", cert.display())))?;
    if chain.is_empty() {
        return Err(bad(format!(
            "the TLS certificate {} holds no certificate",
            cert.display()
        )));
    }
    let private = PrivateKeyDer::from_pem_file(key)
        .map_err(|err| bad(format!("the TLS key {}: {err}", key.display())))?;
    let mut config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|err| bad(format!("TLS: {err}")))?
            .with_no_client_auth()
            .with_single_cert(chain, private)
            .map_err(|err| {
                bad(format!(
                    "the TLS key {} does not fit the certificate {}: {err}",
                    key.display(),
                    cert.display()
                ))
            })?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Makes a self-signed certificate for `names` at `cert` and its key at `key`
/// (mode 0600) unless both exist; answers whether it made them. One without the
/// other is refused rather than overwritten.
pub fn ensure_self_signed(cert: &Path, key: &Path, names: &[String]) -> Result<bool, HouseError> {
    match (cert.exists(), key.exists()) {
        (true, true) => return Ok(false),
        (false, false) => {}
        _ => {
            return Err(bad(format!(
                "TLS: {} and {} must both exist or neither (the House makes them together)",
                cert.display(),
                key.display()
            )));
        }
    }
    let made = rcgen::generate_simple_self_signed(names.to_vec())
        .map_err(|err| bad(format!("TLS: a self-signed certificate: {err}")))?;
    for path in [cert, key] {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)
                .map_err(|err| bad(format!("TLS: {}: {err}", dir.display())))?;
        }
    }
    let write = |path: &Path, text: &str, mode: u32| {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(path)
            .and_then(|mut file| file.write_all(text.as_bytes()))
            .map_err(|err| bad(format!("TLS: {}: {err}", path.display())))
    };
    write(key, &made.signing_key.serialize_pem(), 0o600)?;
    write(cert, &made.cert.pem(), 0o644)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    fn dir(name: &str) -> std::path::PathBuf {
        // Beside the test binary, in the target directory (never `/tmp`).
        let exe = std::env::current_exe().expect("the test binary");
        let dir = exe
            .parent()
            .expect("its directory")
            .join("house-tls")
            .join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn a_self_signed_certificate_is_made_once_and_loads() {
        let dir = dir("made");
        let (cert, key) = (dir.join("house.crt"), dir.join("private/house.key"));
        let names = vec!["localhost".to_string(), "house".to_string()];
        assert!(ensure_self_signed(&cert, &key, &names).expect("made"));
        assert!(!ensure_self_signed(&cert, &key, &names).expect("kept"));
        let mode = std::fs::metadata(&key).expect("key").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let config = server_config(&cert, &key).expect("loads");
        assert_eq!(config.alpn_protocols, vec![b"http/1.1".to_vec()]);
    }

    #[test]
    fn half_a_pair_and_bad_files_are_refused() {
        let dir = dir("bad");
        std::fs::create_dir_all(&dir).expect("dir");
        let (cert, key) = (dir.join("c.pem"), dir.join("k.pem"));
        std::fs::write(&cert, "not a certificate").expect("write");
        assert!(ensure_self_signed(&cert, &key, &["localhost".to_string()]).is_err());
        assert!(server_config(&cert, &key).is_err());
        // A key that is not the certificate's.
        let (c1, k1) = (dir.join("1.crt"), dir.join("1.key"));
        let (c2, k2) = (dir.join("2.crt"), dir.join("2.key"));
        ensure_self_signed(&c1, &k1, &["a".to_string()]).expect("one");
        ensure_self_signed(&c2, &k2, &["b".to_string()]).expect("two");
        assert!(server_config(&c1, &k2).is_err());
    }
}
