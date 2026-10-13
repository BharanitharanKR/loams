//! CN2 plan Task 3: the licence gate for family crates, CI-only services and
//! user-supplied drivers (design §33 D359; Owner rulings OR-3 and OR-4).
//!
//! CN1's `licence_gate_refuses_flagged` stays in `tests/registry.rs`, unchanged. The
//! five tests here cover what CN2 adds: the `ci-service` and `user-supplied` kinds,
//! where each may be placed, the images Loams builds, and the crates compiled into a
//! `loams-connector-*` family crate. The fixtures under `tests/fixtures/licences/` are
//! shared with the `--self-test` modes of `scripts/connectors/image_licences.py` and
//! `scripts/connectors/crate_licences.py`, so the Rust gate and the CI scripts are
//! held to the same cases.

use std::path::{Path, PathBuf};

use loams_flow::validate::{
    ComponentKind, LicenceComponent, LicenceError, LicenceGate, Placement, is_ci_profile,
    is_family_crate,
};
use serde_json::Value;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/licences")
        .join(name)
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()))
}

fn fixture_gate() -> (LicenceGate, Vec<LicenceComponent>) {
    let text = read(&fixture("licences.toml"));
    let gate = LicenceGate::from_toml(&text, "fixture licences.toml")
        .unwrap_or_else(|error| panic!("the fixture gate must read: {error}"));
    let components = LicenceGate::read_components(&text, "fixture licences.toml")
        .unwrap_or_else(|error| panic!("the fixture components must read: {error}"));
    (gate, components)
}

fn component<'a>(components: &'a [LicenceComponent], key: &str) -> &'a LicenceComponent {
    components
        .iter()
        .find(|component| component.key == key)
        .unwrap_or_else(|| panic!("the fixture has components.{key}"))
}

/// A one-stanza gate input with the fixture's `[deny]` list, for the malformed cases.
fn with_stanza(stanza: &str) -> String {
    format!(
        "[deny]\nids = [\"AGPL-3.0-only\", \"BUSL-1.1\", \"SSPL-1.0\", \"unknown\"]\n\n{stanza}"
    )
}

const REDPANDA: &str = r#"
[components.redpanda]
name = "Redpanda"
spdx = "BUSL-1.1"
kind = "ci-service"
image = "docker.redpanda.com/redpandadata/redpanda"
digest = "sha256:0000000000000000000000000000000000000000000000000000000000000001"
reason = "Kafka-protocol conformance (OR-3)"
profiles = PROFILES
used_by = ["redpanda"]
source = "https://github.com/redpanda-data/redpanda"
"#;

/// A `ci-service` runs only in a `connectors-*` compose profile or a CI workflow
/// (OR-3): never in an image, never in the default compose profile, and never in the
/// bare `connectors` profile, which is the default connector stack.
#[test]
fn ci_service_allowed_only_in_ci_profiles() {
    let (_, components) = fixture_gate();
    let redpanda = component(&components, "redpanda");
    assert_eq!(redpanda.kind, ComponentKind::CiService);
    assert_eq!(redpanda.profiles, ["connectors-ci-only"]);

    // The profile names.
    for name in ["connectors-ci-only", "connectors-p2"] {
        assert!(is_ci_profile(name), "{name} is a CI profile");
    }
    for name in ["connectors", "", "default", "house", "ci-connectors"] {
        assert!(!is_ci_profile(name), "{name:?} is not a CI profile");
    }

    // Where it may run.
    let ci_only = vec!["connectors-ci-only".to_string()];
    let p2_and_ci = vec![
        "connectors-p2".to_string(),
        "connectors-ci-only".to_string(),
    ];
    for allowed in [
        Placement::Compose(&ci_only),
        Placement::Compose(&p2_and_ci),
        Placement::CiWorkflow(".github/workflows/fabric.yml"),
    ] {
        redpanda
            .check_placement(&allowed)
            .unwrap_or_else(|error| panic!("{allowed} must be allowed: {error}"));
    }
    // Where it may not. A service with any non-CI profile runs whenever that profile
    // does, so one CI profile beside `connectors` is not enough.
    let default: Vec<String> = Vec::new();
    let connectors = vec!["connectors".to_string()];
    let mixed = vec!["connectors".to_string(), "connectors-ci-only".to_string()];
    for refused in [
        Placement::Compose(&default),
        Placement::Compose(&connectors),
        Placement::Compose(&mixed),
        Placement::Image("loams-fabric"),
        Placement::Mount("/opt/redpanda"),
    ] {
        let error = redpanda
            .check_placement(&refused)
            .err()
            .unwrap_or_else(|| panic!("a ci-service in {refused} must be refused (OR-3)"));
        assert!(
            matches!(error, LicenceError::Misplaced { ref key, .. } if key == "redpanda"),
            "{error}"
        );
    }

    // The compose fixture: each `ok-*` service passes and each `bad-redpanda-*` one is
    // refused by name.
    let errors = LicenceGate::scan_compose(
        &components,
        "fixture compose.yaml",
        &read(&fixture("compose.yaml")),
    )
    .expect_err("the fixture compose has misplaced services");
    let rendered: Vec<String> = errors.iter().map(ToString::to_string).collect();
    for service in ["bad-redpanda-default", "bad-redpanda-connectors"] {
        assert!(
            rendered
                .iter()
                .any(|line| line.contains(service) && line.contains("redpanda")),
            "{service} must be refused: {rendered:#?}"
        );
    }
    assert!(
        rendered.iter().all(|line| !line.contains("ok-")),
        "no ok-* service may be refused: {rendered:#?}"
    );

    // The gate input itself: a ci-service that declares a non-CI profile, or none, or
    // that is not pinned by digest, is malformed.
    for (profiles, digest_ok) in [
        ("[\"connectors\"]", true),
        ("[]", true),
        ("[\"connectors-ci-only\"]", false),
    ] {
        let mut stanza = REDPANDA.replace("PROFILES", profiles);
        if !digest_ok {
            stanza = stanza.replace("sha256:0000", "sha256:zz00");
        }
        let error = LicenceGate::read_components(&with_stanza(&stanza), "fixture.toml")
            .err()
            .unwrap_or_else(|| {
                panic!("profiles {profiles}, digest ok {digest_ok} must be malformed")
            });
        assert!(
            matches!(error, LicenceError::Malformed { ref key, .. } if key == "redpanda"),
            "{error}"
        );
    }
    let stanza = REDPANDA.replace("PROFILES", "[\"connectors-ci-only\"]");
    LicenceGate::read_components(&with_stanza(&stanza), "fixture.toml")
        .unwrap_or_else(|error| panic!("the well-formed stanza must read: {error}"));
}

/// A `user-supplied` driver is only ever a documented mount (CN1 Ruling 8): never in
/// an image Loams builds, never in the default compose profile, and never at a path
/// other than the one its stanza documents.
#[test]
fn user_supplied_never_in_image() {
    let (_, components) = fixture_gate();
    let ojdbc = component(&components, "ojdbc");
    assert_eq!(ojdbc.kind, ComponentKind::UserSupplied);
    assert_eq!(
        ojdbc.mount.as_deref(),
        Some("/opt/loams-connect/drivers/oracle")
    );

    ojdbc
        .check_placement(&Placement::Mount("/opt/loams-connect/drivers/oracle"))
        .unwrap_or_else(|error| panic!("the documented mount must be allowed: {error}"));
    let default: Vec<String> = Vec::new();
    let ci_only = vec!["connectors-ci-only".to_string()];
    for refused in [
        Placement::Image("loams-connect"),
        Placement::Image("loams-fabric"),
        Placement::Compose(&default),
        Placement::Compose(&ci_only),
        Placement::CiWorkflow(".github/workflows/fabric.yml"),
        Placement::Mount("/usr/share/java"),
    ] {
        let error = ojdbc
            .check_placement(&refused)
            .err()
            .unwrap_or_else(|| panic!("a user-supplied driver in {refused} must be refused"));
        assert!(
            matches!(error, LicenceError::Misplaced { ref key, .. } if key == "ojdbc"),
            "{error}"
        );
    }

    // An image that copies the driver in is refused, naming the image and the stanza.
    let errors = LicenceGate::scan_image(
        &components,
        "loams-connect",
        &read(&fixture("Containerfile.leaks")),
    )
    .expect_err("the leaking Containerfile must be refused");
    let rendered: Vec<String> = errors.iter().map(ToString::to_string).collect();
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("components.ojdbc") && line.contains("image loams-connect")),
        "{rendered:#?}"
    );

    // In compose: mounted at the documented path in a CI profile is fine; in the
    // default profile, or at another path, it is refused.
    let errors = LicenceGate::scan_compose(
        &components,
        "fixture compose.yaml",
        &read(&fixture("compose.yaml")),
    )
    .expect_err("the fixture compose has misplaced services");
    let rendered: Vec<String> = errors.iter().map(ToString::to_string).collect();
    for service in ["bad-ojdbc-default", "bad-ojdbc-elsewhere"] {
        assert!(
            rendered
                .iter()
                .any(|line| line.contains(service) && line.contains("components.ojdbc")),
            "{service} must be refused: {rendered:#?}"
        );
    }

    // A user-supplied stanza with no mount, or a relative one, is malformed.
    for mount in ["", "drivers/oracle"] {
        let stanza = format!(
            "[components.ojdbc]\nname = \"Oracle JDBC\"\nspdx = \"LicenseRef-Oracle-FUTC\"\n\
             kind = \"user-supplied\"\ndriver = \"ojdbc*.jar\"\nmount = \"{mount}\"\n\
             used_by = [\"oracle\"]\nsource = \"https://example.invalid\"\n"
        );
        assert!(
            LicenceGate::read_components(&with_stanza(&stanza), "fixture.toml").is_err(),
            "mount {mount:?} must be malformed"
        );
    }
}

/// The images Loams builds (`loams-fabric`, `loams-connect`, any Debezium wrapper) and
/// the default compose profile carry no `ci-service` or `user-supplied` component.
#[test]
fn no_ci_only_or_user_supplied_in_images() {
    let root = repo_root();
    let real_text = read(&root.join("connectors/licences.toml"));
    let real = LicenceGate::read_components(&real_text, "connectors/licences.toml")
        .unwrap_or_else(|error| panic!("connectors/licences.toml must read: {error}"));
    // The real stanzas, and the fixture's proprietary ones besides, so the check is not
    // vacuous while connectors/licences.toml has no ci-service rows yet (CN2 Task 0).
    let (_, fixture_components) = fixture_gate();
    let mut components = real;
    components.extend(
        fixture_components
            .into_iter()
            .filter(|component| !component.kind.is_shipped()),
    );

    let images = built_images(&root);
    assert!(
        images.iter().any(|(name, _)| name == "loams-fabric"),
        "deploy/release/Containerfile.loams-fabric must be found: {images:?}"
    );
    for (name, path) in &images {
        LicenceGate::scan_image(&components, name, &read(path)).unwrap_or_else(|errors| {
            panic!(
                "{}: {}",
                path.display(),
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            )
        });
    }

    // The default compose profile Loams ships, and the connectors stack when it exists.
    for relative in ["deploy/release/compose.yaml", "deploy/fabric/compose.yaml"] {
        let path = root.join(relative);
        if !path.exists() {
            continue;
        }
        LicenceGate::scan_compose(&components, relative, &read(&path)).unwrap_or_else(|errors| {
            panic!(
                "{relative}: {}",
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            )
        });
    }

    // And the negative case: an image that bakes in a ci-service is refused.
    let errors = LicenceGate::scan_image(
        &components,
        "loams-connect",
        &read(&fixture("Containerfile.leaks")),
    )
    .expect_err("the leaking Containerfile must be refused");
    assert!(
        errors.iter().any(|error| error
            .to_string()
            .contains("components.oracle-database-free")),
        "{errors:?}"
    );
}

/// The Containerfiles of the images Loams builds and the gate covers: `loams-fabric`,
/// `loams-connect` and any Debezium wrapper, under `deploy/`.
fn built_images(root: &Path) -> Vec<(String, PathBuf)> {
    let mut found = Vec::new();
    let mut stack = vec![root.join("deploy")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let file = entry.file_name().to_string_lossy().to_lowercase();
            if !(file.starts_with("containerfile") || file.starts_with("dockerfile")) {
                continue;
            }
            let whole = path.to_string_lossy().to_lowercase();
            for image in ["loams-fabric", "loams-connect", "debezium"] {
                if whole.contains(image) {
                    found.push((image.to_string(), path.clone()));
                    break;
                }
            }
        }
    }
    found.sort();
    found
}

/// Every crate compiled into a `loams-connector-*` family crate passes D359: a denied
/// or missing licence is refused, so a family crate cannot pull in a denied crate. Dev
/// dependencies are not compiled into the crate and are not gated here; crates outside
/// the family are cargo-deny's (`fabric/deny.toml`).
#[test]
fn family_crates_pass_licence_gate() {
    let (gate, _) = fixture_gate();

    // Which crates are family crates. The SDK is one: it is compiled into every family
    // crate, so its dependencies are theirs.
    for name in [
        "loams-connector-sdk",
        "loams-connector-postgres",
        "loams-connector-fixture",
    ] {
        assert!(is_family_crate(name), "{name} is a family crate");
    }
    for name in [
        "loams-connect",
        "loams-flow",
        "loams-fabric",
        "connector-sdk",
    ] {
        assert!(!is_family_crate(name), "{name} is not a family crate");
    }

    // The fixture workspace passes: one family crate, five gated dependencies (the dev
    // dependency is skipped, and loams-flow's SSPL dependency is outside the family).
    let metadata: Value = serde_json::from_str(&read(&fixture("cargo-metadata.json")))
        .expect("the fixture metadata is JSON");
    let report = gate
        .check_family_crates(&metadata)
        .unwrap_or_else(|errors| panic!("the fixture must pass: {errors:?}"));
    assert_eq!(report.len(), 1, "{report:?}");
    assert_eq!(report[0].name, "loams-connector-fixture");
    assert_eq!(
        report[0].dependencies,
        [
            "build-helper",
            "deep-crate",
            "dual-agpl",
            "good-crate",
            "old-style"
        ]
    );

    // A workspace with no family crate yet (the repository before CN2 Task 4) passes
    // with nothing to report rather than failing.
    let mut empty = metadata.clone();
    empty["workspace_members"] = serde_json::json!(["path+file:///fabric/crates/loams-flow#0.1.0"]);
    assert_eq!(gate.check_family_crates(&empty).map(|r| r.len()), Ok(0));

    // A family crate that pulls in a denied crate, transitively, is refused by name.
    let mut denied = metadata.clone();
    set_licence(&mut denied, "deep-crate", Some("BUSL-1.1"));
    let errors = gate
        .check_family_crates(&denied)
        .expect_err("a BUSL dependency must be refused");
    assert_eq!(
        errors.iter().map(ToString::to_string).collect::<Vec<_>>(),
        [
            "loams-connector-fixture: dependency deep-crate 2.0.0 is licensed BUSL-1.1, \
          which the gate refuses (D359)"
        ]
    );

    // So is one with no licence at all.
    let mut missing = metadata.clone();
    set_licence(&mut missing, "good-crate", None);
    let errors = gate
        .check_family_crates(&missing)
        .expect_err("an unlicensed dependency must be refused");
    assert!(
        matches!(&errors[..], [LicenceError::UnlicensedCrate { krate, .. }] if krate == "good-crate"),
        "{errors:?}"
    );
}

/// Points a package of the fixture metadata at another licence, or at none.
fn set_licence(metadata: &mut Value, name: &str, licence: Option<&str>) {
    let packages = metadata["packages"]
        .as_array_mut()
        .expect("packages is an array");
    for package in packages {
        if package["name"] == name {
            package["license"] = licence.map_or(Value::Null, |id| Value::String(id.to_string()));
        }
    }
}

/// OR-3 lets a `ci-service` carry a denied id (the D60 precedent). Nothing else does:
/// a denied id on anything shipped, linked, loaded or mounted is still refused.
#[test]
fn denied_ids_still_refused_for_shipped() {
    let (gate, components) = fixture_gate();

    // The fixture's BSL ci-service passes the component check.
    gate.check_components(&read(&fixture("licences.toml")), "fixture licences.toml")
        .unwrap_or_else(|error| panic!("a BSL ci-service is allowed (OR-3): {error}"));
    assert_eq!(component(&components, "redpanda").spdx, "BUSL-1.1");

    // The same id on every other kind is refused.
    for kind in ["runtime", "library", "driver", "user-supplied"] {
        let extra = if kind == "user-supplied" {
            "driver = \"thing*.jar\"\nmount = \"/opt/thing\"\n"
        } else {
            ""
        };
        let text = with_stanza(&format!(
            "[components.thing]\nname = \"Thing\"\nspdx = \"BUSL-1.1\"\nkind = \"{kind}\"\n\
             {extra}used_by = []\nsource = \"https://example.invalid\"\n"
        ));
        let error = gate
            .check_components(&text, "fixture.toml")
            .err()
            .unwrap_or_else(|| panic!("BUSL-1.1 on kind = {kind} must be refused (D359)"));
        assert!(
            matches!(error, LicenceError::RefusedListedComponent { ref key, .. } if key == "thing"),
            "{kind}: {error}"
        );
    }

    // And on a crate compiled into a family crate, alone or as the only choice.
    for licence in [
        "BUSL-1.1",
        "AGPL-3.0-only",
        "SSPL-1.0 AND MIT",
        "AGPL-3.0",
        "unknown",
    ] {
        assert!(
            gate.check_crate_licence("loams-connector-x", "dep", "1.0.0", Some(licence))
                .is_err(),
            "{licence} must be refused for a family crate's dependency"
        );
    }
    // A choice with a permissive side, and the Boost licence (BSL-1.0, not BUSL), pass.
    for licence in [
        "MIT OR AGPL-3.0-only",
        "(MIT OR Apache-2.0) AND Unicode-3.0",
        "BSL-1.0",
        "MIT/Apache-2.0",
        "Apache-2.0 WITH LLVM-exception",
    ] {
        gate.check_crate_licence("loams-connector-x", "dep", "1.0.0", Some(licence))
            .unwrap_or_else(|error| panic!("{licence} must pass: {error}"));
    }

    // A ci-service id may never reach an image, whatever its licence.
    let redpanda = component(&components, "redpanda");
    assert!(
        redpanda
            .check_placement(&Placement::Image("loams-fabric"))
            .is_err()
    );
}
