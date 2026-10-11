//! Compute TLS certificates from a local CA (design §46 §8.5, D710; PG2
//! Task 10).
//!
//! PgDog verifies each compute's certificate on the PgDog → compute hop
//! (Task 23). `pg-control` issues them from its own CA: an ECDSA P-256 key
//! and a self-signed certificate, kept in the [`SecretStore`] under
//! [`CA_REF`] and shared by every `pg-control` instance as the signing key
//! is (R9.6): [`load_or_create_ca`] reads it, and creates it only when it
//! is absent. A compute certificate is ECDSA P-256 too, names the compute's
//! addresses, lasts [`CERT_LIFETIME`] (30 days) and is due for renewal
//! after [`RENEW_AFTER`] (15 days), so a renewal that fails has half the
//! lifetime left to succeed.

use std::time::{Duration, SystemTime};

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, SerialNumber,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::secrets::{Secret, SecretError, SecretRef, SecretStore};

/// How long a compute certificate is valid.
pub const CERT_LIFETIME: Duration = Duration::from_secs(30 * 24 * 3600);

/// When a compute certificate is due for renewal, from its issue.
pub const RENEW_AFTER: Duration = Duration::from_secs(15 * 24 * 3600);

/// How long the CA's own certificate is valid.
pub const CA_LIFETIME: Duration = Duration::from_secs(10 * 365 * 24 * 3600);

/// The CA's reference in the secret store.
pub const CA_REF: &str = "pg-compute-ca";

/// How far a certificate's `notBefore` is set back, for clocks that lag.
const BACKDATE: Duration = Duration::from_secs(5 * 60);

/// The CA's subject.
const CA_NAME: &str = "Loams Postgres compute CA";

/// The stored CA's format.
const CA_FORMAT: u8 = 1;

/// Why a CA or certificate could not be made or read. Never holds a key.
#[derive(Debug, thiserror::Error)]
pub enum CertError {
    #[error("a compute certificate needs at least one name")]
    NoNames,
    #[error("making a certificate: {0}")]
    Generate(String),
    #[error("the stored compute CA is unreadable: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Store(#[from] SecretError),
}

impl From<rcgen::Error> for CertError {
    fn from(e: rcgen::Error) -> Self {
        CertError::Generate(e.to_string())
    }
}

/// The CA as the secret store keeps it.
#[derive(Serialize, Deserialize)]
struct StoredCa {
    format: u8,
    cert_pem: String,
    key_pem: String,
}

/// The computes' CA: its key and its certificate.
pub struct ComputeCa {
    key: KeyPair,
    cert_pem: String,
}

impl std::fmt::Debug for ComputeCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputeCa").finish_non_exhaustive()
    }
}

/// The CA's name, constraints and key usages. Signing needs these, not the
/// certificate: an issuer is rebuilt from them and the stored key.
fn ca_params() -> CertificateParams {
    let mut p = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::OrganizationName, "Loams");
    dn.push(DnType::CommonName, CA_NAME);
    p.distinguished_name = dn;
    p.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    p.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    p
}

fn at(t: SystemTime) -> OffsetDateTime {
    OffsetDateTime::from(t)
}

/// A random positive serial of 16 bytes.
fn serial() -> Result<SerialNumber, CertError> {
    let mut bytes = [0u8; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes)
        .map_err(|_| CertError::Generate("the OS random generator failed".into()))?;
    bytes[0] &= 0x7f;
    Ok(SerialNumber::from_slice(&bytes))
}

impl ComputeCa {
    /// A new CA, valid from `now` (less a few minutes) for [`CA_LIFETIME`].
    ///
    /// # Errors
    ///
    /// [`CertError::Generate`].
    pub fn generate(now: SystemTime) -> Result<Self, CertError> {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        let mut p = ca_params();
        p.not_before = at(now - BACKDATE);
        p.not_after = at(now + CA_LIFETIME);
        p.serial_number = Some(serial()?);
        let cert = p.self_signed(&key)?;
        Ok(ComputeCa {
            key,
            cert_pem: cert.pem(),
        })
    }

    /// The CA's certificate (PEM): what PgDog trusts for the compute hop.
    pub fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    /// A certificate for a compute, naming `names` (DNS names or IP
    /// addresses; the first is also the subject's common name).
    ///
    /// # Errors
    ///
    /// [`CertError::NoNames`]; [`CertError::Generate`] for a name that is
    /// neither.
    pub fn issue(&self, names: &[String], now: SystemTime) -> Result<ComputeCert, CertError> {
        let first = names.first().ok_or(CertError::NoNames)?;
        let mut p = CertificateParams::new(names.to_vec())?;
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, first.as_str());
        p.distinguished_name = dn;
        p.not_before = at(now - BACKDATE);
        p.not_after = at(now + CERT_LIFETIME);
        p.serial_number = Some(serial()?);
        p.is_ca = IsCa::ExplicitNoCa;
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        p.use_authority_key_identifier_extension = true;
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        let issuer = Issuer::new(ca_params(), &self.key);
        let cert = p.signed_by(&key, &issuer)?;
        Ok(ComputeCert {
            cert_pem: cert.pem(),
            key_pem: Secret::new(key.serialize_pem()),
            issued_at: now,
            not_after: now + CERT_LIFETIME,
        })
    }

    fn to_secret(&self) -> Result<Secret<Vec<u8>>, CertError> {
        serde_json::to_vec(&StoredCa {
            format: CA_FORMAT,
            cert_pem: self.cert_pem.clone(),
            key_pem: self.key.serialize_pem(),
        })
        .map(Secret::new)
        .map_err(|e| CertError::Generate(e.to_string()))
    }

    fn from_secret(s: &Secret<Vec<u8>>) -> Result<Self, CertError> {
        let stored: StoredCa =
            serde_json::from_slice(s.expose()).map_err(|e| CertError::Corrupt(e.to_string()))?;
        if stored.format != CA_FORMAT {
            return Err(CertError::Corrupt(format!(
                "unknown format {}",
                stored.format
            )));
        }
        let key = KeyPair::from_pem(&stored.key_pem)
            .map_err(|e| CertError::Corrupt(format!("the key: {e}")))?;
        if !stored.cert_pem.starts_with("-----BEGIN CERTIFICATE-----") {
            return Err(CertError::Corrupt("the certificate is not PEM".into()));
        }
        Ok(ComputeCa {
            key,
            cert_pem: stored.cert_pem,
        })
    }
}

/// A compute's certificate and key.
#[derive(Debug, Clone)]
pub struct ComputeCert {
    pub cert_pem: String,
    pub key_pem: Secret<String>,
    pub issued_at: SystemTime,
    pub not_after: SystemTime,
}

impl ComputeCert {
    /// When it is due for renewal: [`RENEW_AFTER`] after its issue.
    pub fn renew_at(&self) -> SystemTime {
        self.issued_at + RENEW_AFTER
    }

    pub fn needs_renewal(&self, now: SystemTime) -> bool {
        now >= self.renew_at()
    }
}

/// The CA under [`CA_REF`]; when there is none, a new one stored with
/// `create`, then whatever the store holds, so instances that start
/// together end on one CA (as `load_or_create_signing_key`).
///
/// # Errors
///
/// [`CertError::Store`] when the store does not answer;
/// [`CertError::Corrupt`] for a stored CA that does not parse (nothing
/// issues from a CA the store does not hold).
pub async fn load_or_create_ca(
    secrets: &dyn SecretStore,
    now: SystemTime,
) -> Result<ComputeCa, CertError> {
    let r = SecretRef::parse(CA_REF)?;
    match secrets.get(&r).await {
        Ok(stored) => return ComputeCa::from_secret(&stored),
        Err(SecretError::NotFound(_)) => {}
        Err(e) => return Err(e.into()),
    }
    let fresh = ComputeCa::generate(now)?;
    if secrets.create(&r, fresh.to_secret()?).await? {
        tracing::info!(secret_ref = %r, "stored a new compute CA");
    }
    ComputeCa::from_secret(&secrets.get(&r).await?)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use x509_parser::extensions::GeneralName;
    use x509_parser::pem::parse_x509_pem;

    use super::*;

    const NOW: u64 = 1_791_590_400; // 2026-10-10

    fn now() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(NOW)
    }

    fn der(pem: &str) -> Vec<u8> {
        parse_x509_pem(pem.as_bytes()).expect("PEM").1.contents
    }

    #[derive(Default)]
    struct Mem(Mutex<BTreeMap<String, Vec<u8>>>);

    #[async_trait]
    impl SecretStore for Mem {
        async fn put(&self, r: &SecretRef, s: Secret<Vec<u8>>) -> Result<(), SecretError> {
            self.0
                .lock()
                .unwrap()
                .insert(r.to_string(), s.expose().clone());
            Ok(())
        }
        async fn create(&self, r: &SecretRef, s: Secret<Vec<u8>>) -> Result<bool, SecretError> {
            let mut m = self.0.lock().unwrap();
            if m.contains_key(r.as_str()) {
                return Ok(false);
            }
            m.insert(r.to_string(), s.expose().clone());
            Ok(true)
        }
        async fn get(&self, r: &SecretRef) -> Result<Secret<Vec<u8>>, SecretError> {
            self.0
                .lock()
                .unwrap()
                .get(r.as_str())
                .cloned()
                .map(Secret::new)
                .ok_or_else(|| SecretError::NotFound(r.to_string()))
        }
        async fn delete(&self, r: &SecretRef) -> Result<(), SecretError> {
            self.0.lock().unwrap().remove(r.as_str());
            Ok(())
        }
        async fn list(&self) -> Result<Vec<SecretRef>, SecretError> {
            Ok(Vec::new())
        }
    }

    /// A compute certificate is ECDSA P-256, signed by the CA, names the
    /// compute, is a server certificate and not a CA, and lasts 30 days.
    #[test]
    fn issues_p256_server_certs_for_30_days() {
        let ca = ComputeCa::generate(now()).expect("CA");
        let names = vec![
            "ep-01jz0000000000000000000001".to_owned(),
            "127.0.0.1".to_owned(),
        ];
        let cert = ca.issue(&names, now()).expect("issued");
        let ca_der = der(ca.cert_pem());
        let (_, ca_x) = x509_parser::parse_x509_certificate(&ca_der).expect("CA parses");
        let leaf_der = der(&cert.cert_pem);
        let (_, leaf) = x509_parser::parse_x509_certificate(&leaf_der).expect("leaf parses");
        leaf.verify_signature(Some(ca_x.public_key()))
            .expect("signed by the CA");
        ca_x.verify_signature(None).expect("the CA is self-signed");
        assert_eq!(leaf.issuer(), ca_x.subject());
        assert!(ca_x.is_ca());
        assert!(!leaf.is_ca());
        // ECDSA with SHA-256 over P-256.
        assert_eq!(
            leaf.signature_algorithm.algorithm,
            x509_parser::oid_registry::OID_SIG_ECDSA_WITH_SHA256
        );
        assert_eq!(
            leaf.public_key().algorithm.algorithm,
            x509_parser::oid_registry::OID_KEY_TYPE_EC_PUBLIC_KEY
        );
        let validity = leaf.validity();
        let lifetime = validity.not_after.timestamp() - validity.not_before.timestamp();
        assert_eq!(
            lifetime,
            i64::try_from((CERT_LIFETIME + BACKDATE).as_secs()).unwrap()
        );
        assert_eq!(
            validity.not_after.timestamp(),
            i64::try_from(NOW + CERT_LIFETIME.as_secs()).unwrap()
        );
        let san = leaf
            .subject_alternative_name()
            .expect("parses")
            .expect("present");
        let dns: Vec<_> = san
            .value
            .general_names
            .iter()
            .map(|n| match n {
                GeneralName::DNSName(d) => (*d).to_owned(),
                GeneralName::IPAddress(ip) => format!("{ip:?}"),
                other => format!("{other:?}"),
            })
            .collect();
        assert_eq!(dns, ["ep-01jz0000000000000000000001", "[127, 0, 0, 1]"]);
        let eku = leaf.extended_key_usage().expect("parses").expect("present");
        assert!(eku.value.server_auth);
        // The key is P-256 PKCS#8, and its Debug shows nothing.
        assert!(
            cert.key_pem
                .expose()
                .starts_with("-----BEGIN PRIVATE KEY-----")
        );
        assert_eq!(format!("{:?}", cert.key_pem), "[redacted]");
        assert!(matches!(ca.issue(&[], now()), Err(CertError::NoNames)));
    }

    /// Renewal is due 15 days after issue, not before.
    #[test]
    fn renewal_is_due_at_15_days() {
        let ca = ComputeCa::generate(now()).expect("CA");
        let cert = ca.issue(&["ep-a".into()], now()).expect("issued");
        assert!(!cert.needs_renewal(now()));
        assert!(!cert.needs_renewal(now() + RENEW_AFTER - Duration::from_secs(1)));
        assert!(cert.needs_renewal(now() + RENEW_AFTER));
        assert_eq!(cert.not_after, now() + CERT_LIFETIME);
    }

    /// Two loads share one CA, and a CA read back still issues
    /// certificates its stored certificate verifies.
    #[tokio::test]
    async fn the_ca_is_stored_once_and_reloads() {
        let store = Mem::default();
        let a = load_or_create_ca(&store, now()).await.expect("created");
        let b = load_or_create_ca(&store, now()).await.expect("read");
        assert_eq!(a.cert_pem(), b.cert_pem());
        let cert = b.issue(&["ep-a".into()], now()).expect("issued");
        let ca_der = der(a.cert_pem());
        let (_, ca_x) = x509_parser::parse_x509_certificate(&ca_der).expect("CA");
        let leaf_der = der(&cert.cert_pem);
        let (_, leaf) = x509_parser::parse_x509_certificate(&leaf_der).expect("leaf");
        leaf.verify_signature(Some(ca_x.public_key()))
            .expect("the reloaded CA signs as the stored one");
        assert_eq!(leaf.issuer(), ca_x.subject());

        let bad = Mem::default();
        bad.put(
            &SecretRef::parse(CA_REF).unwrap(),
            Secret::new(b"junk".to_vec()),
        )
        .await
        .unwrap();
        assert!(matches!(
            load_or_create_ca(&bad, now()).await,
            Err(CertError::Corrupt(_))
        ));
        assert!(!format!("{a:?}").contains("PRIVATE"));
    }
}
