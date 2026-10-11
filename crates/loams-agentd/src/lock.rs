//! The single-instance lock (design §50 §5.4): `agentd.lock`, an exclusive OS
//! lock taken before the token, the listener or any store. It is the fork's
//! `InstanceLock`, on the daemon's own file.

pub use loams_agentd_sessions::{InstanceLock, LockError};

use crate::paths::Paths;

/// Take `agentd.lock`. Another daemon on this directory is
/// [`LockError::Held`], with the pid it stamped.
pub fn acquire(paths: &Paths) -> Result<InstanceLock, LockError> {
    InstanceLock::acquire_file(&paths.lock())
}

/// The pid of the daemon holding `agentd.lock`, if one does.
pub fn holder(paths: &Paths) -> Option<String> {
    if !paths.dir().is_dir() {
        return None;
    }
    InstanceLock::holder_file(&paths.lock())
}
