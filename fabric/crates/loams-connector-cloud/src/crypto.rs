//! The little public-key plumbing the family needs, over `ring`: PEM blocks,
//! the RSA public key inside an X.509 certificate (to verify SNS message
//! signatures), and RS256 signing with a PKCS #8 key (a Google service
//! account's JWT). The DER walk reads only the fields it needs; the
//! certificate's trust comes from where it was fetched (SNS's own HTTPS
//! host), not from a chain check.

use base64::Engine as _;

/// The DER bytes of the first PEM block labelled `label` (`CERTIFICATE`,
/// `PRIVATE KEY`).
pub fn pem_block(pem: &str, label: &str) -> Option<Vec<u8>> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let start = pem.find(&begin)? + begin.len();
    let stop = start + pem[start..].find(&end)?;
    let b64: String = pem[start..stop]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    base64::engine::general_purpose::STANDARD.decode(b64).ok()
}

/// One DER TLV: its tag, its contents, and what follows it.
fn tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let n = usize::from(first & 0x7f);
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |acc, b| (acc << 8) | usize::from(*b));
        (len, &rest[n..])
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

const SEQUENCE: u8 = 0x30;
const BIT_STRING: u8 = 0x03;
const OID: u8 = 0x06;
const RSA_ENCRYPTION: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01];

/// The DER `RSAPublicKey` of an X.509 certificate's subject public key, as
/// `ring::signature::UnparsedPublicKey` takes it for RSA.
pub fn rsa_public_key_from_certificate(cert_der: &[u8]) -> Result<Vec<u8>, String> {
    let err = |what: &str| format!("not an X.509 certificate with an RSA key: {what}");
    let (tag, cert, _) = tlv(cert_der).ok_or_else(|| err("certificate"))?;
    if tag != SEQUENCE {
        return Err(err("certificate"));
    }
    let (tag, tbs, _) = tlv(cert).ok_or_else(|| err("tbsCertificate"))?;
    if tag != SEQUENCE {
        return Err(err("tbsCertificate"));
    }
    let mut rest = tbs;
    // version [0] EXPLICIT (optional), serialNumber, signature, issuer,
    // validity, subject: skip them to reach subjectPublicKeyInfo.
    let (tag, _, after) = tlv(rest).ok_or_else(|| err("version"))?;
    if tag == 0xa0 {
        rest = after;
    }
    for field in ["serialNumber", "signature", "issuer", "validity", "subject"] {
        let (_, _, after) = tlv(rest).ok_or_else(|| err(field))?;
        rest = after;
    }
    let (tag, spki, _) = tlv(rest).ok_or_else(|| err("subjectPublicKeyInfo"))?;
    if tag != SEQUENCE {
        return Err(err("subjectPublicKeyInfo"));
    }
    let (tag, alg, after) = tlv(spki).ok_or_else(|| err("algorithm"))?;
    if tag != SEQUENCE {
        return Err(err("algorithm"));
    }
    let (tag, oid, _) = tlv(alg).ok_or_else(|| err("algorithm"))?;
    if tag != OID || oid != RSA_ENCRYPTION {
        return Err(err("the key is not rsaEncryption"));
    }
    let (tag, bits, _) = tlv(after).ok_or_else(|| err("subjectPublicKey"))?;
    if tag != BIT_STRING || bits.first() != Some(&0) {
        return Err(err("subjectPublicKey"));
    }
    Ok(bits[1..].to_vec())
}

/// RS256 (RSASSA-PKCS1-v1_5 with SHA-256) over `message` with a PKCS #8 RSA
/// private key.
pub fn rs256_sign(pkcs8_der: &[u8], message: &[u8]) -> Result<Vec<u8>, String> {
    let key = ring::signature::RsaKeyPair::from_pkcs8(pkcs8_der)
        .map_err(|e| format!("not a PKCS #8 RSA private key: {e}"))?;
    let mut sig = vec![0u8; key.public().modulus_len()];
    key.sign(
        &ring::signature::RSA_PKCS1_SHA256,
        &ring::rand::SystemRandom::new(),
        message,
        &mut sig,
    )
    .map_err(|_| "RSA signing failed".to_owned())?;
    Ok(sig)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tlv_lengths() {
        assert_eq!(
            tlv(&[0x30, 0x01, 0xaa, 0xbb]),
            Some((0x30, &[0xaa][..], &[0xbb][..]))
        );
        assert_eq!(
            tlv(&[0x04, 0x81, 0x02, 1, 2]),
            Some((0x04, &[1, 2][..], &[][..]))
        );
        assert_eq!(tlv(&[0x04, 0x05, 1]), None);
        assert!(rsa_public_key_from_certificate(&[0x30, 0x00]).is_err());
        assert!(pem_block("nothing here", "CERTIFICATE").is_none());
    }
}
