//! The RPC token (design §50 §7): 32 random bytes, base64url, regenerated at
//! every start and written owner-only to `agentd.token`. Only Loams Desktop's
//! main process reads it. It is never logged, and its `Debug` is redacted.

use std::io;

use crate::paths::Paths;

/// The main RPC token (role `owner`). `Debug` never shows it.
pub type Token = loams_agentd_rpc::Secret;

/// Generate a new token and replace `agentd.token` with it, atomically.
pub fn rotate(paths: &Paths) -> io::Result<Token> {
    let token = Token::generate()?;
    crate::owner_only::write(&paths.token(), token.expose().as_bytes())?;
    Ok(token)
}

/// The token a running daemon wrote, for the CLI's own calls (`status`, a
/// second `run`).
pub fn read(paths: &Paths) -> io::Result<Token> {
    Ok(Token::new(std::fs::read_to_string(paths.token())?.trim()))
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
            first.expose()
        );
        assert_eq!(first.expose().len(), 43);
        let second = rotate(&paths).expect("rotate");
        assert_ne!(first.expose(), second.expose());
        assert_eq!(read(&paths).expect("read").expose(), second.expose());
    }

    #[test]
    fn debug_hides_the_token() {
        let token = Token::new("secret-value");
        assert!(!format!("{token:?}").contains("secret-value"));
    }
}
