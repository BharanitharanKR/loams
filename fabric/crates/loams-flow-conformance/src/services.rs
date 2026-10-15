//! The real systems behind the suites (OR-2): [`ServiceKind`], the
//! `conformance/connectors/systems.toml` catalog, the compose estate of
//! `deploy/fabric/compose.yaml`, and the check that a harness runs against
//! the manifest's real system and never against a Loams module standing in
//! for it.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use loams_connector_sdk::{Profile, ProfileKind};
use loams_flow::ConnectorSpec;
use serde::Deserialize;

/// A service a harness runs against (Shared contracts). Every variant names
/// the real system behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceKind {
    /// A real system's image (OR-2), a service of `deploy/fabric/compose.yaml`.
    Container {
        compose_service: &'static str,
        system: &'static str,
    },
    /// The vendor's (or a widely used) emulator, also a compose service.
    Emulator {
        compose_service: &'static str,
        system: &'static str,
        vendor: &'static str,
    },
    /// Exchanges recorded from the real system (`fixture::FixtureProxy`).
    Fixture {
        dir: &'static str,
        recorded_from: &'static str,
    },
    /// A real account, reached in the scheduled `connectors-nightly` job with
    /// these repository secrets.
    Nightly {
        env: &'static [&'static str],
        system: &'static str,
    },
}

impl ServiceKind {
    /// The real system this service is (a fixture: the one it was recorded
    /// from).
    pub fn system(&self) -> &'static str {
        match self {
            ServiceKind::Container { system, .. }
            | ServiceKind::Emulator { system, .. }
            | ServiceKind::Nightly { system, .. } => system,
            ServiceKind::Fixture { recorded_from, .. } => recorded_from,
        }
    }

    /// The compose service, for a container or an emulator.
    pub fn compose_service(&self) -> Option<&'static str> {
        match self {
            ServiceKind::Container {
                compose_service, ..
            }
            | ServiceKind::Emulator {
                compose_service, ..
            } => Some(compose_service),
            _ => None,
        }
    }

    /// The owned declaration `systems.toml` would hold for this service.
    pub fn to_decl(&self) -> ServiceDecl {
        match *self {
            ServiceKind::Container {
                compose_service,
                system,
            } => ServiceDecl::Container {
                compose: compose_service.to_owned(),
                system: system.to_owned(),
            },
            ServiceKind::Emulator {
                compose_service,
                system,
                vendor,
            } => ServiceDecl::Emulator {
                compose: compose_service.to_owned(),
                system: system.to_owned(),
                vendor: vendor.to_owned(),
            },
            ServiceKind::Fixture { dir, recorded_from } => ServiceDecl::Fixture {
                dir: dir.to_owned(),
                recorded_from: recorded_from.to_owned(),
            },
            ServiceKind::Nightly { env, system } => ServiceDecl::Nightly {
                env: env.iter().map(|s| (*s).to_owned()).collect(),
                system: system.to_owned(),
            },
        }
    }
}

impl fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_decl().fmt(f)
    }
}

/// A service as `systems.toml` declares it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum ServiceDecl {
    Container {
        compose: String,
        system: String,
    },
    Emulator {
        compose: String,
        system: String,
        vendor: String,
    },
    Fixture {
        dir: String,
        recorded_from: String,
    },
    Nightly {
        env: Vec<String>,
        system: String,
    },
}

impl ServiceDecl {
    pub fn system(&self) -> &str {
        match self {
            ServiceDecl::Container { system, .. }
            | ServiceDecl::Emulator { system, .. }
            | ServiceDecl::Nightly { system, .. } => system,
            ServiceDecl::Fixture { recorded_from, .. } => recorded_from,
        }
    }

    pub fn compose_service(&self) -> Option<&str> {
        match self {
            ServiceDecl::Container { compose, .. } | ServiceDecl::Emulator { compose, .. } => {
                Some(compose)
            }
            _ => None,
        }
    }
}

impl fmt::Display for ServiceDecl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceDecl::Container { compose, system } => {
                write!(f, "container {compose} ({system})")
            }
            ServiceDecl::Emulator {
                compose,
                system,
                vendor,
            } => write!(f, "emulator {compose} ({system}, {vendor})"),
            ServiceDecl::Fixture { dir, recorded_from } => {
                write!(f, "fixture {dir} (recorded from {recorded_from})")
            }
            ServiceDecl::Nightly { env, system } => {
                write!(f, "nightly {system} ({})", env.join(", "))
            }
        }
    }
}

/// Whether `service` names the system `expected`: the same name, or that
/// name followed by a qualifier ("Delta Lake (delta-spark 4.0)").
pub fn system_matches(service: &str, expected: &str) -> bool {
    service == expected
        || service
            .strip_prefix(expected)
            .is_some_and(|rest| rest.starts_with(' '))
}

/// The Loams modules that may never stand in for an external system (OR-2).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoamsModules {
    /// System names of Loams modules (House, the collections' APIs, …).
    pub systems: Vec<String>,
    /// Compose services with this prefix are Loams modules.
    pub compose_prefix: String,
}

/// One manifest's entry in `systems.toml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemEntry {
    /// The real system or systems (a row such as "MinIO / RustFS" names two).
    pub systems: Vec<String>,
    /// The family crate the connector lives in (`loams-connector-<family>`).
    pub family: String,
    /// The services allowed to stand for the system: its image, its vendor's
    /// emulator, a fixture recorded from it, or a nightly real account.
    pub services: Vec<ServiceDecl>,
    /// Additional targets tested after the real system (a Loams module that
    /// speaks the same protocol). Never counted as the real system.
    #[serde(default)]
    pub extra_targets: Vec<ServiceDecl>,
    /// The harness, as a Rust path in the family crate. Set by the family
    /// task that makes the manifest `preview`.
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub notes: Vec<String>,
}

/// `conformance/connectors/systems.toml`, the OR-2 truth.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemsCatalog {
    pub loams_modules: LoamsModules,
    pub systems: BTreeMap<String, SystemEntry>,
}

/// Failure to read the estate's files.
#[derive(Debug, thiserror::Error)]
pub enum EstateError {
    #[error("{path}: {message}")]
    Read { path: String, message: String },
}

fn read(path: &Path) -> Result<String, EstateError> {
    std::fs::read_to_string(path).map_err(|e| EstateError::Read {
        path: path.display().to_string(),
        message: e.to_string(),
    })
}

impl SystemsCatalog {
    pub const PATH: &'static str = "conformance/connectors/systems.toml";

    pub fn parse(text: &str) -> Result<Self, String> {
        toml::from_str(text).map_err(|e| e.to_string())
    }

    pub fn load(path: &Path) -> Result<Self, EstateError> {
        Self::parse(&read(path)?).map_err(|message| EstateError::Read {
            path: path.display().to_string(),
            message,
        })
    }

    pub fn get(&self, id: &str) -> Option<&SystemEntry> {
        self.systems.get(id)
    }

    /// Whether a service is a Loams module.
    pub fn is_loams_module(&self, decl: &ServiceDecl) -> bool {
        let prefix = &self.loams_modules.compose_prefix;
        self.loams_modules
            .systems
            .iter()
            .any(|m| system_matches(decl.system(), m))
            || (!prefix.is_empty()
                && decl
                    .compose_service()
                    .is_some_and(|c| c.starts_with(prefix.as_str())))
    }

    /// The catalog's own consistency (OR-2): every entry names at least one
    /// service; every service is one of the entry's systems; a Loams module
    /// appears among `services` only when the manifest's system *is* that
    /// module (Grafeo), and otherwise only under `extra_targets`.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (id, e) in &self.systems {
            if e.systems.is_empty() {
                out.push(format!("{id}: names no system"));
            }
            if e.services.is_empty() {
                out.push(format!("{id}: names no service for its real system"));
            }
            for s in &e.services {
                if !e.systems.iter().any(|sys| system_matches(s.system(), sys)) {
                    out.push(format!(
                        "{id}: service {s} is not one of its systems {:?}",
                        e.systems
                    ));
                }
                let is_itself = e.systems.iter().any(|sys| {
                    self.loams_modules
                        .systems
                        .iter()
                        .any(|m| system_matches(sys, m))
                });
                if self.is_loams_module(s) && !is_itself {
                    out.push(format!(
                        "{id}: {s} is a Loams module standing in for an external system; \
                         list it under extra_targets"
                    ));
                }
            }
        }
        out
    }
}

/// A compose profile of the estate and the variable that says it is up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stack {
    /// CN1's ★ profile (FL1 owns it).
    Connectors,
    /// Open images (CN2 Task 6).
    ConnectorsP2,
    /// `ci-service` images: proprietary, source-available or EULA (OR-3).
    ConnectorsCiOnly,
}

impl Stack {
    pub const ALL: &'static [Stack] = &[
        Stack::Connectors,
        Stack::ConnectorsP2,
        Stack::ConnectorsCiOnly,
    ];

    pub fn profile(self) -> &'static str {
        match self {
            Stack::Connectors => "connectors",
            Stack::ConnectorsP2 => "connectors-p2",
            Stack::ConnectorsCiOnly => "connectors-ci-only",
        }
    }

    /// Tests skip without services (Global Constraints): this variable is set
    /// by the CI job (or a developer) once the profile is up.
    pub fn env_var(self) -> &'static str {
        match self {
            Stack::Connectors => "LOAMS_CONNECTORS_STACK",
            Stack::ConnectorsP2 => "LOAMS_CONNECTORS_P2_STACK",
            Stack::ConnectorsCiOnly => "LOAMS_CONNECTORS_CI_ONLY_STACK",
        }
    }

    pub fn from_profile(p: &str) -> Option<Stack> {
        Stack::ALL.iter().copied().find(|s| s.profile() == p)
    }
}

/// One service of `deploy/fabric/compose.yaml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeService {
    pub image: String,
    pub profiles: Vec<String>,
    /// The `io.loams.ci-service` label (OR-3).
    pub ci_service: bool,
    /// The `io.loams.licence` label (SPDX).
    pub licence: Option<String>,
}

/// The services of `deploy/fabric/compose.yaml`.
#[derive(Debug, Clone, Default)]
pub struct ComposeEstate {
    pub services: BTreeMap<String, ComposeService>,
}

impl ComposeEstate {
    pub const PATH: &'static str = "deploy/fabric/compose.yaml";

    pub fn parse(text: &str) -> Result<Self, String> {
        let doc: serde_norway::Value = serde_norway::from_str(text).map_err(|e| e.to_string())?;
        let services = doc
            .get("services")
            .and_then(|s| s.as_mapping())
            .ok_or("no services mapping")?;
        let mut out = BTreeMap::new();
        for (name, svc) in services {
            let name = name.as_str().ok_or("a service name is not a string")?;
            let image = svc
                .get("image")
                .and_then(|i| i.as_str())
                .ok_or_else(|| format!("{name}: no image"))?
                .to_owned();
            let profiles = svc
                .get("profiles")
                .and_then(|p| p.as_sequence())
                .map(|seq| {
                    seq.iter()
                        .filter_map(|p| p.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            let label = |key: &str| {
                svc.get("labels")
                    .and_then(|l| l.get(key))
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            };
            out.insert(
                name.to_owned(),
                ComposeService {
                    image,
                    profiles,
                    ci_service: label("io.loams.ci-service").as_deref() == Some("true"),
                    licence: label("io.loams.licence"),
                },
            );
        }
        Ok(Self { services: out })
    }

    pub fn load(path: &Path) -> Result<Self, EstateError> {
        Self::parse(&read(path)?).map_err(|message| EstateError::Read {
            path: path.display().to_string(),
            message,
        })
    }

    /// The stacks a service runs in.
    pub fn stacks_of(&self, service: &str) -> Vec<Stack> {
        self.services
            .get(service)
            .map(|s| {
                s.profiles
                    .iter()
                    .filter_map(|p| Stack::from_profile(p))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The catalog and the compose estate, read from a repository checkout.
#[derive(Debug, Clone)]
pub struct Estate {
    pub root: PathBuf,
    pub catalog: SystemsCatalog,
    pub compose: ComposeEstate,
}

impl Estate {
    pub fn load(root: &Path) -> Result<Self, EstateError> {
        Ok(Self {
            root: root.to_owned(),
            catalog: SystemsCatalog::load(&root.join(SystemsCatalog::PATH))?,
            compose: ComposeEstate::load(&root.join(ComposeEstate::PATH))?,
        })
    }

    /// The checkout this crate was built in.
    pub fn load_default() -> Result<Self, EstateError> {
        Self::load(&loams_flow::manifest::repository_root())
    }
}

/// Check, before any suite runs, that `services` is the real system of the
/// manifest `id` (OR-2): each service is declared for it in `systems.toml`
/// (under `services`, or under `extra_targets`, which never count), names
/// one of its systems, and is no Loams module standing in for it; at least
/// one service is the real system. With a target profile (Task 7), the suite
/// runs against the **target's** system, named by the profile, and the
/// profile's service must be among the harness's (Review Focus 1).
///
/// Returns the real system's name.
pub fn check_services(
    id: &str,
    profile: Option<&Profile>,
    services: &[ServiceKind],
    catalog: &SystemsCatalog,
) -> Result<String, Vec<String>> {
    let Some(entry) = catalog.get(id) else {
        return Err(vec![format!(
            "{id}: no entry in {}; a suite must name its real system",
            SystemsCatalog::PATH
        )]);
    };
    let mut problems = Vec::new();
    let mut expected: Vec<String> = entry.systems.clone();
    if let Some(p) = profile.filter(|p| p.kind == ProfileKind::Target) {
        if !entry
            .systems
            .iter()
            .any(|s| system_matches(&p.system.name, s))
        {
            problems.push(format!(
                "{id}: profile {} names system {:?}, which is not this manifest's {:?}",
                p.id, p.system.name, entry.systems
            ));
        }
        expected = vec![p.system.name.clone()];
        if let Some(compose) = p.system.service.get("compose").and_then(|c| c.as_str())
            && !services
                .iter()
                .any(|s| s.compose_service() == Some(compose))
        {
            problems.push(format!(
                "{id}: the target profile {} runs against compose service {compose}, \
                 which the harness does not use",
                p.id
            ));
        }
    }
    if services.is_empty() {
        problems.push(format!("{id}: the harness names no service"));
    }
    let mut real = 0;
    for s in services {
        let decl = s.to_decl();
        let in_services = entry.services.contains(&decl);
        let in_extra = entry.extra_targets.contains(&decl);
        if !in_services && !in_extra {
            problems.push(format!(
                "{id}: {decl} is not declared for this manifest in {}",
                SystemsCatalog::PATH
            ));
            continue;
        }
        if in_extra {
            continue;
        }
        if catalog.is_loams_module(&decl)
            && !expected.iter().any(|e| {
                catalog
                    .loams_modules
                    .systems
                    .iter()
                    .any(|m| system_matches(e, m))
            })
        {
            problems.push(format!(
                "{id}: {decl} is a Loams module and may not stand in for {:?}",
                expected
            ));
            continue;
        }
        if expected.iter().any(|e| system_matches(decl.system(), e)) {
            real += 1;
        } else {
            problems.push(format!(
                "{id}: {decl} is not the system under test {:?}",
                expected
            ));
        }
    }
    if real == 0 && problems.is_empty() {
        problems.push(format!(
            "{id}: no service of the harness is the real system {:?}; extra targets never count",
            expected
        ));
    }
    if problems.is_empty() {
        Ok(expected.join(" / "))
    } else {
        Err(problems)
    }
}

/// What keeps a manifest from counting toward `preview` in this estate
/// (`every_p2_preview_manifest_has_a_harness`): a `preview` or `stable`
/// manifest needs a `systems.toml` entry naming its real system and
/// services, and a harness in its family crate, which must exist.
pub fn harness_problems(
    spec: &ConnectorSpec,
    catalog: &SystemsCatalog,
    root: &Path,
) -> Vec<String> {
    let id = &spec.id;
    let Some(e) = catalog.get(id) else {
        return vec![format!("{id}: no entry in {}", SystemsCatalog::PATH)];
    };
    let status = spec.status.slug();
    if !matches!(status, "preview" | "stable") {
        return Vec::new();
    }
    let mut out = Vec::new();
    if e.services.is_empty() {
        out.push(format!(
            "{id}: {status} with no service for its real system"
        ));
    }
    let krate = format!("loams_connector_{}", e.family.replace('-', "_"));
    match &e.harness {
        None => out.push(format!(
            "{id}: {status} but {} names no harness",
            SystemsCatalog::PATH
        )),
        Some(path) if !path.starts_with(&format!("{krate}::")) => out.push(format!(
            "{id}: harness {path} is not in its family crate {krate}"
        )),
        Some(_) => {}
    }
    let dir = root
        .join("fabric/crates")
        .join(format!("loams-connector-{}", e.family));
    if !dir.is_dir() {
        out.push(format!(
            "{id}: {status} but its family crate {} does not exist",
            dir.display()
        ));
    }
    out
}
