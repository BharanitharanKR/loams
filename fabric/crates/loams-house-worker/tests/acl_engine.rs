//! HS1 R-ACL.1: the control connection's grants on a sealed engine; see
//! `acl_engine/cases.rs`.

#[path = "acl_engine/cases.rs"]
mod cases;

#[test]
fn grants_confine_the_control_connection() {
    cases::run(false);
}
