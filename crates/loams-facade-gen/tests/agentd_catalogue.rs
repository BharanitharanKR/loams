//! `loams.agentd.v1` is not in the SDK catalogue (Loams Studio, SD1 Task 4;
//! design §53 §6.1, Q816's default).
//!
//! The package is `loams-agentd`'s surface for Loams Studio: served by the
//! per-user daemon, never by the Loams server, so no SDK may expose it. Its
//! services name a `ModuleOptions` (for `unstable`, as every package does) but
//! no method carries a `FacadeOptions`, so no module is generated for it; and
//! the generator leaves it out of `PROTO_PACKAGES`, the list an SDK reports as
//! the packages it speaks. Both are checked over the real `proto/` tree, with
//! a control that the package was in the request at all, so the test cannot
//! pass by reading nothing.

use loams_facade_gen::{model_from_request, reasons};

mod protoreq;

const AGENTD: &str = "loams.agentd.v1";

#[test]
fn agentd_v1_not_in_sdk_catalogue() {
    let request = protoreq::code_generator_request();
    // The control: the request the generator reads does carry the package.
    let needle = AGENTD.as_bytes();
    assert!(
        request.windows(needle.len()).any(|window| window == needle),
        "{AGENTD} is not in the descriptors read from proto/; this test would pass vacuously"
    );

    let reasons =
        reasons::read(&protoreq::root().join("docs/api/reasons.md")).expect("the reason registry");
    let model = model_from_request(request, reasons).expect("the descriptors");
    let modules: Vec<&str> = model
        .modules
        .iter()
        .filter(|module| module.package == AGENTD)
        .map(|module| module.name.as_str())
        .collect();
    assert!(
        modules.is_empty(),
        "{AGENTD} generated SDK modules {modules:?}; its methods must carry no FacadeOptions (Q816)"
    );
    assert!(
        !model
            .calls()
            .any(|call| call.service.starts_with("loams.agentd.")),
        "an SDK call is backed by a {AGENTD} method"
    );
    assert!(
        !model.packages.iter().any(|package| package == AGENTD),
        "{AGENTD} is in PROTO_PACKAGES: {:?}",
        model.packages
    );
    // The other packages are still listed: the filter is the one package.
    assert!(
        model
            .packages
            .iter()
            .any(|package| package == "loams.instance.v1")
    );
}
