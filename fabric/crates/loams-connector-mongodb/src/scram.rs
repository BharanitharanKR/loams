//! SCRAM-SHA-256 (RFC 5802, RFC 7677), the client side, as MongoDB runs it
//! over `saslStart`/`saslContinue`. HMAC, PBKDF2 and SHA-256 are `ring`'s.
//!
//! MongoDB's SCRAM-SHA-256 hashes the SASLprep'd password itself (unlike its
//! SCRAM-SHA-1, which hashes an MD5 digest). SASLprep maps nothing in
//! printable ASCII, so this client takes printable-ASCII passwords as they
//! are and refuses others with a clear error rather than hash a password the
//! server would have normalised differently (plan ruling R16.4).

use std::num::NonZeroU32;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use ring::{digest, hmac, pbkdf2};

/// RFC 7677's minimum iteration count.
pub const MIN_ITERATIONS: u32 = 4096;

/// A SCRAM failure. Messages never contain the password or a proof.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScramError {
    /// Something only the configuration can fix.
    #[error("SCRAM-SHA-256: {0}")]
    Config(String),
    /// The server's messages are not what RFC 5802 says, or its signature
    /// does not verify (it does not know the password).
    #[error("SCRAM-SHA-256: {0}")]
    Protocol(String),
}

/// `=` and `,` escaped in a SASL name.
fn saslname(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, data)
        .as_ref()
        .try_into()
        .expect("SHA-256 is 32 bytes")
}

/// The password check SASLprep would leave unchanged.
pub fn check_password(password: &str) -> Result<(), ScramError> {
    if password.is_empty() {
        return Err(ScramError::Config("the password is empty".into()));
    }
    if !password.bytes().all(|b| (0x20..0x7f).contains(&b)) {
        return Err(ScramError::Config(
            "the password has characters outside printable ASCII, which need SASLprep; \
             this client does not implement SASLprep (use an ASCII password, or mtls)"
                .into(),
        ));
    }
    Ok(())
}

/// One conversation. `client_first` → server-first → `client_final` →
/// server-final, which [`Conversation::verify_server`] checks.
pub struct Conversation {
    bare: String,
    nonce: String,
    password: String,
    expected_server_signature: Option<[u8; 32]>,
}

/// `Debug` shows the user part only, never the password.
impl std::fmt::Debug for Conversation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conversation")
            .field("bare", &self.bare)
            .finish_non_exhaustive()
    }
}

impl Conversation {
    /// Start, with `nonce` (printable, no comma; [`random_nonce`] in use).
    pub fn new(user: &str, password: &str, nonce: &str) -> Result<Self, ScramError> {
        check_password(password)?;
        if user.is_empty() {
            return Err(ScramError::Config("the username is empty".into()));
        }
        Ok(Self {
            bare: format!("n={},r={nonce}", saslname(user)),
            nonce: nonce.to_owned(),
            password: password.to_owned(),
            expected_server_signature: None,
        })
    }

    /// `n,,n=<user>,r=<nonce>`: no channel binding.
    pub fn client_first(&self) -> String {
        format!("n,,{}", self.bare)
    }

    /// The client-final message for `server_first`.
    pub fn client_final(&mut self, server_first: &str) -> Result<String, ScramError> {
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for part in server_first.split(',') {
            match part.split_once('=') {
                Some(("r", v)) => nonce = Some(v),
                Some(("s", v)) => salt = Some(v),
                Some(("i", v)) => iterations = Some(v),
                Some(("m", _)) => {
                    return Err(ScramError::Protocol(
                        "the server requires an extension".into(),
                    ));
                }
                _ => {}
            }
        }
        let bad = |what: &str| ScramError::Protocol(format!("server-first message: {what}"));
        let nonce = nonce.ok_or_else(|| bad("no nonce"))?;
        if !nonce.starts_with(&self.nonce) || nonce.len() <= self.nonce.len() {
            return Err(bad("the nonce does not extend the client's"));
        }
        let salt = B64
            .decode(salt.ok_or_else(|| bad("no salt"))?)
            .map_err(|_| bad("the salt is not base64"))?;
        let iterations: u32 = iterations
            .ok_or_else(|| bad("no iteration count"))?
            .parse()
            .map_err(|_| bad("the iteration count is not a number"))?;
        if iterations < MIN_ITERATIONS {
            return Err(bad("fewer than 4096 iterations"));
        }
        let mut salted = [0u8; 32];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(iterations).expect("checked above"),
            &salt,
            self.password.as_bytes(),
            &mut salted,
        );
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = digest::digest(&digest::SHA256, &client_key);
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{},{server_first},{without_proof}", self.bare);
        let client_signature = hmac_sha256(stored_key.as_ref(), auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        let server_key = hmac_sha256(&salted, b"Server Key");
        self.expected_server_signature = Some(hmac_sha256(&server_key, auth_message.as_bytes()));
        Ok(format!("{without_proof},p={}", B64.encode(proof)))
    }

    /// Check the server-final message (`v=<signature>`): the server proves
    /// it knows the password too.
    pub fn verify_server(&self, server_final: &str) -> Result<(), ScramError> {
        if let Some(e) = server_final.strip_prefix("e=") {
            return Err(ScramError::Protocol(format!("the server refused: {e}")));
        }
        let v = server_final
            .split(',')
            .find_map(|p| p.strip_prefix("v="))
            .ok_or_else(|| ScramError::Protocol("server-final message has no signature".into()))?;
        let got = B64
            .decode(v)
            .map_err(|_| ScramError::Protocol("the server signature is not base64".into()))?;
        let want = self
            .expected_server_signature
            .ok_or_else(|| ScramError::Protocol("server-final before client-final".into()))?;
        // Constant time: every byte is compared whatever the first mismatch.
        let differ = got.len() != want.len()
            || got
                .iter()
                .zip(want.iter())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                != 0;
        if differ {
            return Err(ScramError::Protocol(
                "the server's signature does not verify".into(),
            ));
        }
        Ok(())
    }
}

/// 24 random bytes as base64: a client nonce.
pub fn random_nonce() -> String {
    B64.encode(crate::random_bytes::<24>())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677 §3's exchange, byte for byte.
    #[test]
    fn rfc_7677_example() {
        let mut c = Conversation::new("user", "pencil", "rOprNGfwEbeRWgbNEkqO").expect("conv");
        assert_eq!(c.client_first(), "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        assert_eq!(
            c.client_final(server_first).expect("final"),
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        c.verify_server("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .expect("server verifies");
        assert!(
            c.verify_server("v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
                .is_err()
        );
        assert!(c.verify_server("e=invalid-proof").is_err());
    }

    #[test]
    fn hostile_server_first_messages_are_refused() {
        let mut c = Conversation::new("u", "p", "abc").expect("conv");
        for bad in [
            "r=xyz123,s=AAAA,i=4096",
            "r=abc,s=AAAA,i=4096",
            "r=abcd,s=AAAA,i=1",
            "r=abcd,s=@@@,i=4096",
            "r=abcd,i=4096",
            "m=ext,r=abcd,s=AAAA,i=4096",
        ] {
            assert!(c.client_final(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn names_are_escaped_and_odd_passwords_refused() {
        let c = Conversation::new("a=b,c", "pw", "n").expect("conv");
        assert_eq!(c.client_first(), "n,,n=a=3Db=2Cc,r=n");
        assert!(Conversation::new("u", "pässword", "n").is_err());
        assert!(Conversation::new("u", "", "n").is_err());
        assert!(Conversation::new("", "pw", "n").is_err());
        assert_eq!(random_nonce().len(), 32);
        assert_ne!(random_nonce(), random_nonce());
    }

    #[test]
    fn debug_never_shows_the_password() {
        let c = Conversation::new("u", "s3cr3t-pw", "n").expect("conv");
        assert!(!format!("{c:?}").contains("s3cr3t"));
        assert!(!c.client_first().contains("s3cr3t"));
    }
}
