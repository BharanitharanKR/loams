//! The RPC token (design §50 §7): 32 random bytes, base64url, regenerated at
//! every start and written owner-only to `agentd.token`. Only Loams Desktop's
//! main process reads it. It is never logged, and its `Debug` is redacted.

use std::io;

use base64::Engine as _;

use crate::paths::Paths;

/// The main RPC token (role `owner`).
#[derive(Clone, PartialEq, Eq)]
pub struct Token(String);

impl Token {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Token(<redacted>)")
    }
}

/// Generate a new token and replace `agentd.token` with it, atomically.
pub fn rotate(paths: &Paths) -> io::Result<Token> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    crate::owner_only::write(&paths.token(), token.as_bytes())?;
    Ok(Token(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotate_writes_a_fresh_token_each_time() {
        let root = tempfile::tempdir().expect("temp dir");
        let paths = Paths::new(root.path()).expect("paths");
        let first = rotate(&paths).expect("rotate");
        assert_eq!(
            std::fs::read_to_string(paths.token()).expect("read"),
            first.as_str()
        );
        let second = rotate(&paths).expect("rotate");
        assert_ne!(first, second);
        assert_eq!(
            std::fs::read_to_string(paths.token()).expect("read"),
            second.as_str()
        );
    }

    #[test]
    fn debug_hides_the_token() {
        let token = Token("secret-value".into());
        assert_eq!(format!("{token:?}"), "Token(<redacted>)");
    }
}
