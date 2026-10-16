//! The Postgres module (design §57 §7): the front door of Loams Postgres
//! in front of each namespace's PgDog.

pub mod front;
pub mod upstream;

pub use front::{FrontDoorOptions, PgFrontDoor};

/// Which listener a connection came in on (§57 §7.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Entry {
    /// The shared port: the namespace comes from SNI.
    Shared,
    /// A dedicated port: the port is the namespace's.
    Dedicated(u16),
}
