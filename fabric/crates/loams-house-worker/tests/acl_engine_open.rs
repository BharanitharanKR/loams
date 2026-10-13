//! HS1 R-ACL.1: the same statements on an engine with chDB's own `GRANT ALL`
//! and no profile get past its access control; see `acl_engine/cases.rs`.

#[path = "acl_engine/cases.rs"]
mod cases;

#[test]
fn open_grants_let_the_control_connection_through() {
    cases::run(true);
}
