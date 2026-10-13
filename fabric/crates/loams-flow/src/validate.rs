//! What a manifest is checked against: `connectors/schema/connector.schema.json`,
//! CN1 plan Task 1's six semantic rules and CN2 plan Task 1's R-CN2-1 to R-CN2-8,
//! §33 §4 rule 1's refusal, and D359's licence gate.
//!
//! The order is the design's: **the JSON Schema first**, then the rules the schema
//! cannot express. Every problem is collected rather than reported one at a time, so
//! a manifest that is wrong in five ways says so in one run.
//!
//! * [`schema_violations`] is the schema alone, which the loader calls first so a
//!   malformed manifest is reported as malformed before it reaches into a `$ref`.
//! * [`validate_manifest`] is the schema plus the rules, and
//!   [`validate_manifest_with_config_schema`] adds CN1's secret rule, which needs
//!   the resolved instance-config schema that only the loader has.
//! * [`check_use`] is §33 §4 rule 1: a route that uses an undeclared capability is
//!   refused by connector, capability and manifest version.
//! * [`LicenceGate`] is D359: AGPL, BSL, SSPL, ELv2, `NOASSERTION` and unknown ids
//!   are refused for anything Loams ships or runs by default, and the refused set is
//!   read from `connectors/licences.toml` rather than written here a second time.
//!   CN2 plan Task 3 adds the `ci-service` and `user-supplied` kinds and where each
//!   may be placed ([`LicenceComponent::check_placement`], [`LicenceGate::scan_image`],
//!   [`LicenceGate::scan_compose`]), and the crates compiled into each
//!   `loams-connector-*` family crate ([`LicenceGate::check_family_crates`]);
//!   `scripts/connectors/{image_licences.py,crate_licences.py}` run the same rules in CI.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::manifest::{
    Choice, ConnectorSpec, Delivery, ManifestError, ManifestErrors, Ordering, Priority,
    RuntimeKind, Status, Suite,
};

/// §33 §4 rule 1's refusal, which names the connector, the capability and the
/// manifest version — the three things a user needs to know which manifest to look
/// at and which major it is.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "{connector} (manifest specVersion {spec_version}) does not declare the capability {capability}"
)]
pub struct CapabilityError {
    /// The connector's registry id.
    pub connector: String,
    /// The capability the route uses and the manifest does not declare, named as the
    /// manifest's own key (`cdc`, `upsert`, `delivery.sink`).
    pub capability: String,
    /// The manifest's `specVersion`, which §33 §4 rule 4 makes the thing a route pins.
    pub spec_version: String,
}

/// The manifest schema compiled once.
///
/// `jsonschema::validator_for` compiles the whole document, so a `Registry` holds one
/// of these and hands it to 200 manifests rather than compiling it 200 times. The
/// schema's path is a parameter and not a constant so a test can point at a fixture;
/// [`default_manifest_schema_path`] is the repository-relative default.
#[derive(Debug)]
pub struct ManifestSchema {
    validator: jsonschema::Validator,
}

impl ManifestSchema {
    /// The compiled schema at `path`.
    pub fn load(path: &Path) -> Result<Self, SchemaError> {
        let name = path.display().to_string();
        let text = std::fs::read_to_string(path).map_err(|error| SchemaError::Unreadable {
            path: name.clone(),
            message: error.to_string(),
        })?;
        let document: Value =
            serde_json::from_str(&text).map_err(|error| SchemaError::NotJson {
                path: name.clone(),
                message: error.to_string(),
            })?;
        let validator =
            jsonschema::validator_for(&document).map_err(|error| SchemaError::NotASchema {
                path: name,
                message: error.to_string(),
            })?;
        Ok(Self { validator })
    }

    /// The compiled schema at [`default_manifest_schema_path`].
    pub fn embedded() -> Result<Self, SchemaError> {
        Self::load(&default_manifest_schema_path())
    }

    /// The compiled validator, which [`validate_manifest`] takes.
    pub fn validator(&self) -> &jsonschema::Validator {
        &self.validator
    }
}

/// Why a manifest schema could not be compiled.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    /// The file is not there or not readable.
    #[error("{path}: cannot read the manifest schema: {message}")]
    Unreadable {
        /// The schema's path.
        path: String,
        /// The I/O error.
        message: String,
    },
    /// The file is not JSON.
    #[error("{path}: the manifest schema is not valid JSON: {message}")]
    NotJson {
        /// The schema's path.
        path: String,
        /// The parse error.
        message: String,
    },
    /// The JSON is not a usable JSON Schema.
    #[error("{path}: the manifest schema does not compile: {message}")]
    NotASchema {
        /// The schema's path.
        path: String,
        /// The compiler's error.
        message: String,
    },
}

/// The manifest schema's path in this repository. A default, not a constant: the
/// loader takes the path as a parameter so a test can point at a fixture.
pub fn default_manifest_schema_path() -> PathBuf {
    crate::manifest::repository_root().join("connectors/schema/connector.schema.json")
}

/// The licence gate's input in this repository (design §33 D359).
pub fn default_licences_path() -> PathBuf {
    crate::manifest::repository_root().join("connectors/licences.toml")
}

/// Every violation of `connector.schema.json`, not just the first.
///
/// The JSON Schema is the authority on the manifest's shape (CN1 plan Task 1), so a
/// caller gets all of its complaints in one pass: a hand-written manifest is fixed
/// once, not once per complaint.
pub fn schema_violations(value: &Value, schema: &jsonschema::Validator) -> Vec<ManifestError> {
    schema
        .iter_errors(value)
        .map(|error| {
            let instance = dotted(error.instance_path());
            let path = if instance.is_empty() {
                let schema_path = dotted(error.schema_path());
                if schema_path.is_empty() {
                    "manifest".to_string()
                } else {
                    schema_path
                }
            } else {
                instance
            };
            ManifestError::new(path, error.to_string())
        })
        .collect()
}

/// The manifest at `value`, checked against `connector.schema.json` and CN1 plan
/// Task 1's semantic rules.
///
/// CN1's sixth rule, that `secrets` and the config schema's `writeOnly` properties
/// are exactly equal, needs the resolved instance-config schema, which only
/// [`crate::manifest::load_manifest`] has; this entry point leaves it unchecked and
/// [`validate_manifest_with_config_schema`] is the one that runs it.
pub fn validate_manifest(
    value: &Value,
    schema: &jsonschema::Validator,
) -> Result<ConnectorSpec, Vec<ManifestError>> {
    validate_manifest_with_config_schema(value, schema, None)
}

/// [`validate_manifest`] with the instance-config schema already resolved, so CN1's
/// secret rule runs.
pub fn validate_manifest_with_config_schema(
    value: &Value,
    schema: &jsonschema::Validator,
    config_schema: Option<&Value>,
) -> Result<ConnectorSpec, Vec<ManifestError>> {
    let violations = schema_violations(value, schema);
    if !violations.is_empty() {
        return Err(violations);
    }
    let spec: ConnectorSpec = serde_json::from_value(value.clone())
        .map_err(|error| vec![ManifestError::new("manifest", error.to_string())])?;
    let rules = semantic_rules(&spec, schema, value, config_schema);
    if rules.is_empty() {
        Ok(spec)
    } else {
        Err(rules)
    }
}

/// CN1 plan Task 1's semantic rules and CN2 plan Task 1's R-CN2-1 to R-CN2-8, the
/// ones `connector.schema.json` cannot express. Every problem is returned, not just
/// the first. R-CN2-6 is here only once [`NATIVE_FIRST_ENFORCED`] is `true`; until
/// then [`semantic_warnings`] reports it.
pub fn semantic_rules(
    spec: &ConnectorSpec,
    schema: &jsonschema::Validator,
    value: &Value,
    config_schema: Option<&Value>,
) -> Vec<ManifestError> {
    let mut errors = Vec::new();
    rule_upsert_declares_a_key(spec, config_schema, &mut errors);
    rule_cdc_implies_streaming_and_position(spec, &mut errors);
    rule_exactly_once_needs_transactional_or_idempotent(spec, &mut errors);
    rule_starred_implies_p1(spec, &mut errors);
    rule_auth_enum_is_enforced(schema, value, &mut errors);
    rule_secrets_match_the_config_schema(spec, config_schema, &mut errors);
    // CN2 plan Task 1's rules, R-CN2-1 to R-CN2-8 in its "Shared contracts".
    rule_cn2_1_overrides_name_declared_directions(spec, &mut errors);
    rule_cn2_2_profile_needs_native_directions(spec, &mut errors);
    rule_cn2_3_reply_needs_an_emitted_type_and_reply_topic(spec, config_schema, &mut errors);
    rule_cn2_4_camel_components_are_in_the_catalog(spec, &mut errors);
    rule_cn2_5_preview_needs_the_contract_suite(spec, &mut errors);
    if NATIVE_FIRST_ENFORCED {
        rule_cn2_6_p1_p2_default_runtime_is_native(spec, &mut errors);
    }
    rule_cn2_7_delivery_claims_are_backed(spec, &mut errors);
    rule_cn2_8_fallback_is_declared_and_temporary(spec, &mut errors);
    errors
}

/// Whether R-CN2-6 (OR-1, native first for P1 and P2) is an error.
///
/// CN2 plan Task 1 makes it a warning until Task 2 has assigned the runtimes —
/// the registry as built has 63 Camel and 9 OpenAPI P2 rows — and an error after.
/// Task 2 flips this to `true` in the same change that assigns them; until then
/// [`semantic_warnings`] reports it and [`semantic_rules`] does not.
pub const NATIVE_FIRST_ENFORCED: bool = false;

/// The rules that warn rather than refuse: today only R-CN2-6, while
/// [`NATIVE_FIRST_ENFORCED`] is `false`. A [`crate::Registry`] load collects these
/// per manifest and keeps them in [`crate::Registry::warnings`].
pub fn semantic_warnings(spec: &ConnectorSpec) -> Vec<ManifestError> {
    let mut warnings = Vec::new();
    if !NATIVE_FIRST_ENFORCED {
        rule_cn2_6_p1_p2_default_runtime_is_native(spec, &mut warnings);
    }
    warnings
}

/// The directions a manifest declares under `capabilities`, in `Direction::ALL`'s
/// order.
fn declared_directions(spec: &ConnectorSpec) -> Vec<Direction> {
    let mut directions = Vec::with_capacity(2);
    if spec.capabilities.source.is_some() {
        directions.push(Direction::Source);
    }
    if spec.capabilities.sink.is_some() {
        directions.push(Direction::Sink);
    }
    directions
}

/// The manifest path of a direction's runtime: its override where there is one,
/// `runtime` otherwise.
fn runtime_path(spec: &ConnectorSpec, direction: Direction) -> &'static str {
    let overridden = match direction {
        Direction::Source => spec.runtime.source.is_some(),
        Direction::Sink => spec.runtime.sink.is_some(),
    };
    match (overridden, direction) {
        (true, Direction::Source) => "runtime.source",
        (true, Direction::Sink) => "runtime.sink",
        (false, _) => "runtime",
    }
}

/// **R-CN2-1 — a direction override names a direction the manifest declares.**
///
/// CN2 OR-10 gives each direction its own runtime; an override for a direction the
/// manifest does not serve would be a runtime for nothing, and a reader of the
/// manifest would take it as a claim.
fn rule_cn2_1_overrides_name_declared_directions(
    spec: &ConnectorSpec,
    errors: &mut Vec<ManifestError>,
) {
    let declared = declared_directions(spec);
    for (direction, path, present) in [
        (
            Direction::Source,
            "runtime.source",
            spec.runtime.source.is_some(),
        ),
        (Direction::Sink, "runtime.sink", spec.runtime.sink.is_some()),
    ] {
        if present && !declared.contains(&direction) {
            errors.push(ManifestError::new(
                path,
                format!(
                    "{}: the runtime overrides the {direction} direction, but capabilities \
                     declares no {direction} (R-CN2-1, CN2 OR-10)",
                    spec.id
                ),
            ));
        }
    }
}

/// **R-CN2-2 — `profile` is set only when the direction's kind is `native`.**
///
/// A target or HTTP-API profile is run by a native family module (CN2 OR-2,
/// OR-11), so every declared direction must run native by default for the profile
/// to mean anything. That the profile exists is checked by the loader, which knows
/// the `connectors/` root ([`crate::manifest::load_manifest_value`]); that it
/// validates and that its `module` matches its kind is CN2 Task 7's `profile.rs`.
fn rule_cn2_2_profile_needs_native_directions(
    spec: &ConnectorSpec,
    errors: &mut Vec<ManifestError>,
) {
    let Some(profile) = spec.runtime.profile.as_deref() else {
        return;
    };
    for direction in declared_directions(spec) {
        let Some((kind, _)) = spec.runtime.for_direction(direction, Choice::Default) else {
            continue;
        };
        if kind != RuntimeKind::Native {
            errors.push(ManifestError::new(
                "runtime.profile",
                format!(
                    "{}: names the profile {profile:?}, but its {direction} direction runs on \
                     {kind}; a profile is run by a native family module (R-CN2-2, CN2 OR-2, \
                     OR-11)",
                    spec.id
                ),
            ));
        }
    }
}

/// **R-CN2-3 — `sink.reply` implies a non-empty `envelope.emits` and a
/// `reply_topic` in the config schema.**
///
/// A replying sink (CN2 OR-19: AI and function connectors) emits one event per
/// input, so it emits a type, and the instance names where the replies go. The
/// config half needs the resolved instance-config schema, which only the loader
/// has, as CN1's rules 1 and 6 do.
fn rule_cn2_3_reply_needs_an_emitted_type_and_reply_topic(
    spec: &ConnectorSpec,
    config_schema: Option<&Value>,
    errors: &mut Vec<ManifestError>,
) {
    if !spec
        .capabilities
        .sink
        .as_ref()
        .is_some_and(|sink| sink.reply)
    {
        return;
    }
    if spec.envelope.emits.trim().is_empty() {
        errors.push(ManifestError::new(
            "capabilities.sink.reply",
            format!(
                "{}: the sink replies, so envelope.emits must name the reply's CloudEvents \
                 type (R-CN2-3, CN2 OR-19)",
                spec.id
            ),
        ));
    }
    if let Some(config) = config_schema
        && config["properties"]["reply_topic"].is_null()
    {
        errors.push(ManifestError::new(
            "capabilities.sink.reply",
            format!(
                "{}: the sink replies, so {} must declare a `reply_topic` property \
                 (R-CN2-3, CN2 OR-19)",
                spec.id, spec.config_ref.reference
            ),
        ));
    }
}

/// **R-CN2-4 — a `camel` direction or fallback names a component in the vendored
/// catalog snapshot with the matching consumer or producer.**
///
/// The snapshot (`connectors/vendor/camel-catalog-4.22.x.json.zst`) is CN2 Task 22's,
/// and is not in the tree yet. Until it is, the rule is a no-op behind the
/// `camel-catalog` feature, which is off by default; turning the feature on before
/// Task 22 refuses every Camel direction, so nobody mistakes the no-op for a pass.
// TODO(CN2 Task 22): read the vendored snapshot and check each Camel direction's
// component for a consumer (source) or producer (sink).
#[cfg(not(feature = "camel-catalog"))]
fn rule_cn2_4_camel_components_are_in_the_catalog(
    _spec: &ConnectorSpec,
    _errors: &mut Vec<ManifestError>,
) {
}

/// R-CN2-4 with the `camel-catalog` feature on, before CN2 Task 22 vendors the
/// snapshot it reads: every Camel direction is refused, by name.
#[cfg(feature = "camel-catalog")]
fn rule_cn2_4_camel_components_are_in_the_catalog(
    spec: &ConnectorSpec,
    errors: &mut Vec<ManifestError>,
) {
    for direction in declared_directions(spec) {
        for choice in [Choice::Default, Choice::Fallback] {
            if let Some((RuntimeKind::Camel, component)) =
                spec.runtime.for_direction(direction, choice)
            {
                errors.push(ManifestError::new(
                    runtime_path(spec, direction),
                    format!(
                        "{}: the Camel component {component:?} ({direction}) cannot be checked: \
                         the vendored catalog snapshot is CN2 Task 22's (R-CN2-4)",
                        spec.id
                    ),
                ));
            }
        }
    }
}

/// **R-CN2-5 — `status: preview` or `stable` implies `contract` in
/// `conformance`.**
///
/// CN2 OR-23's status ladder: `preview` means every declared capability is green,
/// which is the contract suite's job (D353 rule 3).
fn rule_cn2_5_preview_needs_the_contract_suite(
    spec: &ConnectorSpec,
    errors: &mut Vec<ManifestError>,
) {
    if matches!(spec.status, Status::Preview | Status::Stable)
        && !spec.conformance.contains(&Suite::Contract)
    {
        errors.push(ManifestError::new(
            "conformance",
            format!(
                "{}: status is {}, which needs the contract suite in conformance (R-CN2-5, \
                 CN2 OR-23, D353 rule 3)",
                spec.id, spec.status
            ),
        ));
    }
}

/// OR-4's reasons a P1 or P2 direction may run on a non-native runtime by default,
/// as the words a `fallback-only:` note names them by, matched case-insensitively:
/// Oracle (its client is proprietary), Salesforce CDC, JMS providers without AMQP
/// 1.0, the generic JDBC ★ (a Java API), and the Debezium Server CDC rows that stay
/// fallbacks of the native CDC.
pub const OR4_FALLBACK_REASONS: &[&str] = &["oracle", "salesforce", "jms", "jdbc", "debezium"];

/// The prefix of the note R-CN2-6 reads.
pub const FALLBACK_ONLY_PREFIX: &str = "fallback-only:";

/// Whether `spec.notes` has a `fallback-only:` note naming one of OR-4's reasons.
fn explains_fallback_only(spec: &ConnectorSpec) -> bool {
    spec.notes.iter().any(|note| {
        note.trim()
            .strip_prefix(FALLBACK_ONLY_PREFIX)
            .is_some_and(|reason| {
                let reason = reason.to_lowercase();
                OR4_FALLBACK_REASONS
                    .iter()
                    .any(|named| reason.contains(named))
            })
    })
}

/// **R-CN2-6 (OR-1) — a P1 or P2 manifest's declared directions run `native` by
/// default, unless `notes` has a `fallback-only:` entry naming one of OR-4's
/// reasons.**
///
/// A warning through [`semantic_warnings`] while [`NATIVE_FIRST_ENFORCED`] is
/// `false`, an error through [`semantic_rules`] after.
fn rule_cn2_6_p1_p2_default_runtime_is_native(
    spec: &ConnectorSpec,
    findings: &mut Vec<ManifestError>,
) {
    if !matches!(spec.priority, Priority::P1 | Priority::P2) || explains_fallback_only(spec) {
        return;
    }
    let mut reported = BTreeSet::new();
    for direction in declared_directions(spec) {
        let Some((kind, _)) = spec.runtime.for_direction(direction, Choice::Default) else {
            continue;
        };
        let path = runtime_path(spec, direction);
        if kind != RuntimeKind::Native && reported.insert(path) {
            findings.push(ManifestError::new(
                path,
                format!(
                    "{}: a {} manifest whose {direction} direction runs on {kind} by default; \
                     P1 and P2 run native unless notes has a \"{FALLBACK_ONLY_PREFIX} …\" entry \
                     naming one of OR-4's reasons (R-CN2-6, CN2 OR-1)",
                    spec.id, spec.priority
                ),
            ));
        }
    }
}

/// **R-CN2-7 (OR-7) — delivery claims are backed.**
///
/// `delivery.sink: exactly_once` requires `sink.transactional` and the
/// `exactly-once` suite: only a `TransactionalSink` stores the checkpoint atomically
/// with the data. `effectively_once` requires `sink.idempotent` or `sink.upsert` for
/// a sink, and `envelope.stable_ids` for a source, whose re-reads `ingest`'s
/// `(source, id)` dedup (D334) then collapses. CN1's rule 3 (idempotent or
/// transactional for `exactly_once`) is the weaker half of this and stays, so a
/// manifest that breaks both is told both.
fn rule_cn2_7_delivery_claims_are_backed(spec: &ConnectorSpec, errors: &mut Vec<ManifestError>) {
    let sink = spec.capabilities.sink.as_ref();
    match spec.capabilities.delivery.sink {
        Delivery::ExactlyOnce => {
            if !sink.is_some_and(|sink| sink.transactional) {
                errors.push(ManifestError::new(
                    "capabilities.delivery.sink",
                    format!(
                        "{}: delivery.sink is exactly_once, which needs sink.transactional: \
                         only a TransactionalSink stores the checkpoint with the data; an \
                         idempotent sink is effectively_once (R-CN2-7, CN2 OR-7)",
                        spec.id
                    ),
                ));
            }
            if !spec.conformance.contains(&Suite::ExactlyOnce) {
                errors.push(ManifestError::new(
                    "conformance",
                    format!(
                        "{}: delivery.sink is exactly_once, which needs the exactly-once suite \
                         in conformance (R-CN2-7, CN2 OR-7)",
                        spec.id
                    ),
                ));
            }
        }
        Delivery::EffectivelyOnce => {
            if !sink.is_some_and(|sink| sink.idempotent || sink.upsert) {
                errors.push(ManifestError::new(
                    "capabilities.delivery.sink",
                    format!(
                        "{}: delivery.sink is effectively_once, which needs sink.idempotent or \
                         sink.upsert: the target must dedupe by ce_id or a natural key \
                         (R-CN2-7, CN2 OR-7)",
                        spec.id
                    ),
                ));
            }
        }
        Delivery::AtMostOnce | Delivery::AtLeastOnce => {}
    }
    if spec.capabilities.delivery.source == Delivery::EffectivelyOnce && !spec.envelope.stable_ids {
        errors.push(ManifestError::new(
            "capabilities.delivery.source",
            format!(
                "{}: delivery.source is effectively_once, which needs envelope.stable_ids: \
                 ingest dedupes by (source, id) only when the ids survive a re-read \
                 (R-CN2-7, CN2 OR-7, §33 §6)",
                spec.id
            ),
        ));
    }
}

/// **R-CN2-8 (OR-4) — `runtime.fallback.kind` is `camel`, `debezium` or `iggy`, its
/// `directions` are declared, and `replaced_by` is non-empty.**
fn rule_cn2_8_fallback_is_declared_and_temporary(
    spec: &ConnectorSpec,
    errors: &mut Vec<ManifestError>,
) {
    let Some(fallback) = spec.runtime.fallback.as_ref() else {
        return;
    };
    if !matches!(
        fallback.kind,
        RuntimeKind::Camel | RuntimeKind::Debezium | RuntimeKind::Iggy
    ) {
        errors.push(ManifestError::new(
            "runtime.fallback.kind",
            format!(
                "{}: the fallback runs on {}, which is not one of OR-4's fallback runtimes \
                 (camel, debezium, iggy) (R-CN2-8)",
                spec.id, fallback.kind
            ),
        ));
    }
    let declared = declared_directions(spec);
    for direction in &fallback.directions {
        if !declared.contains(direction) {
            errors.push(ManifestError::new(
                "runtime.fallback.directions",
                format!(
                    "{}: the fallback serves the {direction} direction, but capabilities \
                     declares no {direction} (R-CN2-8)",
                    spec.id
                ),
            ));
        }
    }
    if fallback.replaced_by.trim().is_empty() {
        errors.push(ManifestError::new(
            "runtime.fallback.replaced_by",
            format!(
                "{}: a fallback is temporary, so replaced_by must name the task that made it \
                 one or replaces it, for example \"CN2 Task 23\" (R-CN2-8, CN2 OR-4)",
                spec.id
            ),
        ));
    }
}

/// **Rule 1 — a sink with `upsert: true` declares a key in its config schema.**
///
/// §33 §4's rule 1 refuses a route that upserts through a sink that does not declare
/// `upsert`, and `connector.schema.json`'s `sink.upsert` description says the sink
/// "upserts by a key it declares in its config schema". Declaring the capability and
/// giving no way to name the key would make the capability unusable, so the two must
/// come together. The property is looked for as `key`: the three ★ manifests that
/// declare `upsert` — `postgresql`, `mysql` and `elasticsearch` — all spell it
/// `key`, and each says in its description that it is required *because* the
/// manifest declares the capability.
fn rule_upsert_declares_a_key(
    spec: &ConnectorSpec,
    config_schema: Option<&Value>,
    errors: &mut Vec<ManifestError>,
) {
    let Some(_sink) = spec.capabilities.sink.as_ref().filter(|sink| sink.upsert) else {
        return;
    };
    let Some(config) = config_schema else {
        return;
    };
    if config["properties"]["key"].is_null() {
        errors.push(ManifestError::new(
            "capabilities.sink.upsert",
            format!(
                "{}: the sink declares upsert, so its config schema must declare a `key` \
                 property, and {} has none",
                spec.id, spec.config_ref.reference
            ),
        ));
    }
}

/// **Rule 2 — `cdc: true` implies `source.streaming` and a non-empty `source.position`.**
///
/// CN1 plan Task 1 states it; the reasoning is §33 §6 and Ruling 4. A change stream
/// is delivered continuously and its position is what makes a restart resume rather
/// than replay, so a connector that declares `cdc` without either has declared a
/// capability it cannot honour.
fn rule_cdc_implies_streaming_and_position(spec: &ConnectorSpec, errors: &mut Vec<ManifestError>) {
    let Some(source) = spec.capabilities.source.as_ref() else {
        return;
    };
    if !source.cdc {
        return;
    }
    if !source.streaming {
        errors.push(ManifestError::new(
            "capabilities.source.streaming",
            format!(
                "{}: source.cdc is true, which implies source.streaming (CN1 Task 1's \
                 semantic rules; §33 §7)",
                spec.id
            ),
        ));
    }
    if source.position.trim().is_empty() {
        errors.push(ManifestError::new(
            "capabilities.source.position",
            format!(
                "{}: source.cdc is true, so the source must name what it checkpoints \
                 (CN1 Task 1's semantic rules; §33 §6, Ruling 4)",
                spec.id
            ),
        ));
    }
}

/// **Rule 3 — `delivery.sink = exactly_once` requires `sink.transactional` or
/// `sink.idempotent`.**
///
/// CN1 plan Task 1 states it. §33 §2.2 says `exactly_once` is a claim only where the
/// external system deduplicates, so the manifest has to say which mechanism makes it
/// true: a transaction that spans the batch, or the sink's own dedup.
fn rule_exactly_once_needs_transactional_or_idempotent(
    spec: &ConnectorSpec,
    errors: &mut Vec<ManifestError>,
) {
    if spec.capabilities.delivery.sink != Delivery::ExactlyOnce {
        return;
    }
    let covered = spec
        .capabilities
        .sink
        .as_ref()
        .is_some_and(|sink| sink.transactional || sink.idempotent);
    if !covered {
        errors.push(ManifestError::new(
            "capabilities.delivery.sink",
            format!(
                "{}: delivery.sink is exactly_once, which needs sink.transactional or \
                 sink.idempotent (CN1 Task 1's semantic rules; §33 §2.2)",
                spec.id
            ),
        ));
    }
}

/// **Rule 4 — `starred` implies `priority = P1`.**
///
/// §33 §8 and D358 make P1 the ★ set of CN1, so a starred row that is not P1 says
/// the rollout phase and the star disagree. The Camel runtime row is P1 and *not*
/// starred, which is the other direction and legal.
fn rule_starred_implies_p1(spec: &ConnectorSpec, errors: &mut Vec<ManifestError>) {
    if spec.starred && spec.priority != Priority::P1 {
        errors.push(ManifestError::new(
            "priority",
            format!(
                "{}: starred is true, which implies priority P1 (CN1 Task 1's semantic \
                 rules; §33 §8, D358), but priority is {}",
                spec.id, spec.priority
            ),
        ));
    }
}

/// **Rule 5 — every auth method is one of Appendix A's legend's slugs.**
///
/// The JSON Schema's `auth.items.enum` is what enforces this, and
/// `AuthMethod`'s variants are pinned to that list by `enum_slugs_match_the_json_schema`.
/// The rule therefore asserts the enforcement rather than assuming it: the schema is
/// probed with a value no legend defines, and a schema that accepts it would let any
/// string into a manifest's `auth` unchecked.
fn rule_auth_enum_is_enforced(
    schema: &jsonschema::Validator,
    value: &Value,
    errors: &mut Vec<ManifestError>,
) {
    let mut probe = value.clone();
    probe["auth"] = Value::Array(vec![Value::String(AUTH_PROBE_SLUG.to_string())]);
    let enforced = schema
        .iter_errors(&probe)
        .any(|error| dotted(error.instance_path()).starts_with("auth"));
    if !enforced {
        errors.push(ManifestError::new(
            "auth",
            "the manifest schema does not enforce an enum on auth, so a manifest's auth \
             methods are unchecked (CN1 Task 1's semantic rules; Appendix A's legend)",
        ));
    }
}

/// A slug no legend defines, used only to probe the schema's `auth` enum.
const AUTH_PROBE_SLUG: &str = "loams-not-an-auth-method";

/// **Rule 6 — `secrets` and the config schema's `writeOnly` properties are exactly
/// equal.**
///
/// CN1 plan Task 1 asks for one direction: every name in `secrets` names a property
/// marked `"writeOnly": true`. CN1's execution ruling on that rule makes it an
/// equality in both directions, because a `writeOnly` property the manifest does not
/// name is a secret field no secret store entry resolves, and a `secrets` name with
/// no `writeOnly` property behind it names nothing.
///
/// The 21 ★ manifests write dotted JSON paths (`sasl.password`,
/// `s3.access_key_id`), so the walk follows nested `properties` objects and reports
/// the full path.
fn rule_secrets_match_the_config_schema(
    spec: &ConnectorSpec,
    config_schema: Option<&Value>,
    errors: &mut Vec<ManifestError>,
) {
    let Some(config) = config_schema else {
        return;
    };
    let write_only = write_only_paths(config);
    let declared: BTreeSet<String> = spec.secrets.iter().cloned().collect();
    for secret in &spec.secrets {
        if !write_only.contains(secret) {
            errors.push(ManifestError::new(
                format!("secrets.{secret}"),
                format!(
                    "{}: {} has no property at this path marked \"writeOnly\": true, so the \
                     name resolves to nothing (CN1 Task 1's semantic rules; D189)",
                    spec.id, spec.config_ref.reference
                ),
            ));
        }
    }
    for path in write_only.difference(&declared) {
        let path = path.as_str();
        errors.push(ManifestError::new(
            format!("secrets.{path}"),
            format!(
                "{}: {} marks {path:?} \"writeOnly\", so it is a secret field and belongs in \
                 the manifest's secrets list (CN1 Task 1's semantic rules; D189)",
                spec.id, spec.config_ref.reference
            ),
        ));
    }
}

/// Every `"writeOnly": true` property of a config schema, as dotted paths.
///
/// Nested objects are walked because a manifest's `secrets` are dotted paths; an
/// array's `items` are not, because no manifest names a secret inside a list (the
/// 21 ★ schemas put every secret under a named object or at the top level).
fn write_only_paths(schema: &Value) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    collect_write_only(schema, "", &mut found);
    found
}

fn collect_write_only(node: &Value, prefix: &str, found: &mut BTreeSet<String>) {
    let Some(properties) = node.get("properties").and_then(Value::as_object) else {
        return;
    };
    for (name, child) in properties {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        if child.get("writeOnly").and_then(Value::as_bool) == Some(true) {
            found.insert(path.clone());
        }
        collect_write_only(child, &path, found);
    }
}

/// A JSON Pointer as a dotted path, the way the manifest itself spells it
/// (`capabilities.source.position`).
fn dotted(pointer: &impl fmt::Display) -> String {
    pointer
        .to_string()
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join(".")
}

/// Every error, one per line, under no heading: what a `Display` impl that has to
/// render "all of them" needs.
pub fn render_errors(errors: &[ManifestError]) -> String {
    errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

/// Which way a route uses a connector, and the directions a manifest's runtime
/// overrides and fallback name (CN2 Task 1; `connector.schema.json`'s
/// `$defs/direction`, spelled `source` and `sink`).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// The connector is the route's `from`.
    #[default]
    Source,
    /// The connector is the route's `to`.
    Sink,
}

impl Direction {
    /// Both directions, in the order `$defs/direction` lists them.
    pub const ALL: &'static [Self] = &[Self::Source, Self::Sink];
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Source => "source",
            Self::Sink => "sink",
        })
    }
}

/// The source mode a route uses (§33 D353's modes: streaming, batch, CDC, webhook).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Continuous, low-latency delivery.
    Streaming,
    /// Bulk or scheduled transfer.
    Batch,
    /// Row-level change capture (§33 §7, D357).
    Cdc,
    /// Webhooks received as a source.
    Webhook,
}

impl Mode {
    /// The capability name §33 §4 rule 1's refusal names.
    pub const fn capability(self) -> &'static str {
        match self {
            Self::Streaming => "source.streaming",
            Self::Batch => "source.batch",
            Self::Cdc => "cdc",
            Self::Webhook => "webhook",
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.capability())
    }
}

/// What a route needs of its sink beyond the direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkRequirement {
    /// The route upserts, so the sink must declare `upsert`.
    Upsert,
    /// The route applies deletes.
    Delete,
    /// The route writes inside the target's transaction.
    Transactional,
}

impl SinkRequirement {
    /// The capability name §33 §4 rule 1's refusal names.
    pub const fn capability(self) -> &'static str {
        match self {
            Self::Upsert => "upsert",
            Self::Delete => "delete",
            Self::Transactional => "transactional",
        }
    }
}

impl fmt::Display for SinkRequirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.capability())
    }
}

/// What a route wants of one connector, which the manifest has to declare all of.
///
/// §33 §4 rule 1: "ValidateRoute refuses a route whose `from` connector lacks the
/// source mode it uses (for example `cdc` on a polling source), whose `to` connector
/// lacks `upsert` when the route upserts, or whose delivery asks for more than the
/// connector declares."
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Uses {
    /// Which way the connector is used.
    pub direction: Direction,
    /// The source mode, when the direction is `Source`.
    pub mode: Option<Mode>,
    /// What the sink must offer, when the direction is `Sink`.
    pub sink_requirements: Vec<SinkRequirement>,
    /// The delivery guarantee the route relies on, per direction.
    pub delivery: Option<Delivery>,
    /// The ordering the route relies on.
    pub ordering: Option<Ordering>,
    /// Whether the route carries Arrow batches, which D356 makes the only bulk path.
    pub arrow_bulk: bool,
}

/// The strength of a delivery guarantee: `at_least_once` is above `at_most_once`,
/// `effectively_once` (CN2 OR-7) above both, and `exactly_once` above all three.
fn delivery_strength(delivery: Delivery) -> u8 {
    match delivery {
        Delivery::AtMostOnce => 0,
        Delivery::AtLeastOnce => 1,
        Delivery::EffectivelyOnce => 2,
        Delivery::ExactlyOnce => 3,
    }
}

/// How much ordering a value promises, in the order §33 §4's `ordering` comment
/// lists them. A route that relies on per-partition order needs at least
/// `per_partition`, as that comment says.
fn ordering_strength(ordering: Ordering) -> u8 {
    match ordering {
        Ordering::None => 0,
        Ordering::PerKey => 1,
        Ordering::PerPartition => 2,
        Ordering::Total => 3,
    }
}

/// §33 §4 rule 1: refuse a route that uses a capability the manifest does not
/// declare, naming the connector, the capability and the manifest version.
///
/// The checks run in the order §33 §4 rule 1 lists them — the source mode, then the
/// sink's requirement, then delivery, then ordering, then the bulk path — so the
/// refusal names the first thing a reader of the rule would expect.
pub fn check_use(spec: &ConnectorSpec, uses: &Uses) -> Result<(), CapabilityError> {
    let refuse = |capability: &str| {
        Err(CapabilityError {
            connector: spec.id.clone(),
            capability: capability.to_string(),
            spec_version: spec.spec_version.to_string(),
        })
    };

    match uses.direction {
        Direction::Source => {
            let Some(source) = spec.capabilities.source.as_ref() else {
                return refuse("source");
            };
            let mode = match uses.mode {
                Some(Mode::Streaming) if !source.streaming => Some("source.streaming"),
                Some(Mode::Batch) if !source.batch => Some("source.batch"),
                Some(Mode::Cdc) if !source.cdc => Some("cdc"),
                Some(Mode::Webhook) if !source.webhook => Some("webhook"),
                _ => None,
            };
            if let Some(capability) = mode {
                return refuse(capability);
            }
            if uses.arrow_bulk && !spec.capabilities.bulk.arrow {
                return refuse("bulk.arrow");
            }
            if let Some(asked) = uses.delivery
                && delivery_strength(asked) > delivery_strength(spec.capabilities.delivery.source)
            {
                return refuse("delivery.source");
            }
        }
        Direction::Sink => {
            let Some(sink) = spec.capabilities.sink.as_ref() else {
                return refuse("sink");
            };
            for requirement in &uses.sink_requirements {
                let declared = match requirement {
                    SinkRequirement::Upsert => sink.upsert,
                    SinkRequirement::Delete => sink.delete,
                    SinkRequirement::Transactional => sink.transactional,
                };
                if !declared {
                    return refuse(requirement.capability());
                }
            }
            if uses.arrow_bulk && !spec.capabilities.bulk.arrow {
                return refuse("bulk.arrow");
            }
            if let Some(asked) = uses.delivery
                && delivery_strength(asked) > delivery_strength(spec.capabilities.delivery.sink)
            {
                return refuse("delivery.sink");
            }
        }
    }

    if let Some(asked) = uses.ordering
        && ordering_strength(asked) > ordering_strength(spec.capabilities.ordering)
    {
        return refuse("ordering");
    }
    Ok(())
}

/// Design §33 D359's licence gate, over the ids `connectors/licences.toml` refuses.
///
/// D359: each manifest names the licence of its runtime component and of every
/// library or driver it loads, and CI refuses AGPL, BSL, SSPL, ELv2 and unlicensed
/// dependencies for anything Loams ships or runs by default. The refused set is read
/// from `connectors/licences.toml`'s `[deny]` rather than written here again, so the
/// gate and `scripts/connectors/gen_registry.py --check` cannot disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicenceGate {
    denied: Vec<String>,
}

/// Why the gate could not read its input, or what it refuses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LicenceError {
    /// `connectors/licences.toml` could not be read.
    #[error("{path}: cannot read the licence gate input: {message}")]
    Unreadable {
        /// The file's path.
        path: String,
        /// The I/O error.
        message: String,
    },
    /// The file is not the shape the gate reads.
    #[error("{path}: not a readable licence list: {message}")]
    Syntax {
        /// The file's path.
        path: String,
        /// What the reader could not make of it.
        message: String,
    },
    /// The gate's input is the file `origin` names, which every message repeats so a
    /// log line stands on its own.
    /// A component's own SPDX id is refused (D359).
    #[error("{connector}: licence.component is {id}, which the gate refuses ({origin})")]
    RefusedComponent {
        /// The manifest's id.
        connector: String,
        /// The refused SPDX id.
        id: String,
        /// Where the refused list is written down.
        origin: String,
    },
    /// A loaded dependency's SPDX id is refused (D359).
    #[error("{connector}: licence.dependencies.{name} is {id}, which the gate refuses ({origin})")]
    RefusedDependency {
        /// The manifest's id.
        connector: String,
        /// The dependency's name, as the manifest spells it.
        name: String,
        /// The refused SPDX id.
        id: String,
        /// Where the refused list is written down.
        origin: String,
    },
    /// A `[components.*]` stanza of the gate input carries a refused id, which
    /// `scripts/connectors/gen_registry.py --check` also fails on.
    #[error("{path}: components.{key} carries refused licence {id}")]
    RefusedListedComponent {
        /// The file's path.
        path: String,
        /// The `[components.<key>]` stanza.
        key: String,
        /// The refused SPDX id.
        id: String,
    },
    /// A `[components.*]` stanza lacks a field its kind needs, or carries one in the
    /// wrong shape (CN2 Task 3: a `ci-service` names an image, a digest, a reason and
    /// its CI profiles; a `user-supplied` driver names its file and mount path).
    #[error("{path}: components.{key}: {message}")]
    Malformed {
        /// The file's path.
        path: String,
        /// The `[components.<key>]` stanza.
        key: String,
        /// What is wrong with it.
        message: String,
    },
    /// A `ci-service` or `user-supplied` component found where it may not be (OR-3;
    /// CN1 Ruling 8).
    #[error("{origin}: components.{key} ({kind}) is in {placement}: {rule}")]
    Misplaced {
        /// The file, image or service the component was found in.
        origin: String,
        /// The `[components.<key>]` stanza.
        key: String,
        /// The stanza's kind.
        kind: ComponentKind,
        /// Where it was found.
        placement: String,
        /// The rule it breaks.
        rule: &'static str,
    },
    /// A crate compiled into a family crate carries a refused licence (D359).
    #[error(
        "{family}: dependency {krate} {version} is licensed {licence}, which the gate refuses (D359)"
    )]
    RefusedCrate {
        /// The `loams-connector-*` crate it is compiled into.
        family: String,
        /// The dependency.
        krate: String,
        /// Its version.
        version: String,
        /// Its licence expression, as `cargo metadata` reports it.
        licence: String,
    },
    /// A crate compiled into a family crate declares no licence (D359 refuses an
    /// unlicensed dependency).
    #[error(
        "{family}: dependency {krate} {version} declares no licence, which the gate refuses (D359)"
    )]
    UnlicensedCrate {
        /// The `loams-connector-*` crate it is compiled into.
        family: String,
        /// The dependency.
        krate: String,
        /// Its version.
        version: String,
    },
    /// `cargo metadata`'s output is not the shape the gate reads.
    #[error("cargo metadata: {message}")]
    Metadata {
        /// What is missing.
        message: String,
    },
}

impl LicenceGate {
    /// The gate over the `[deny]` list in a `connectors/licences.toml`.
    pub fn from_toml(text: &str, origin: &str) -> Result<Self, LicenceError> {
        let flat = parse_licences_toml(text).map_err(|message| LicenceError::Syntax {
            path: origin.to_string(),
            message,
        })?;
        let Some(ids) = flat.get("deny.ids") else {
            return Err(LicenceError::Syntax {
                path: origin.to_string(),
                message: "no [deny] ids list".to_string(),
            });
        };
        let denied = split_toml_array(ids);
        if denied.is_empty() {
            return Err(LicenceError::Syntax {
                path: origin.to_string(),
                message: "[deny] ids is empty, so the gate would refuse nothing (D359)".to_string(),
            });
        }
        Ok(Self { denied })
    }

    /// The gate over `connectors/licences.toml`.
    pub fn load(path: &Path) -> Result<Self, LicenceError> {
        let origin = path.display().to_string();
        let text = std::fs::read_to_string(path).map_err(|error| LicenceError::Unreadable {
            path: origin.clone(),
            message: error.to_string(),
        })?;
        Self::from_toml(&text, &origin)
    }

    /// The gate over this repository's `connectors/licences.toml`.
    pub fn repository() -> Result<Self, LicenceError> {
        Self::load(&default_licences_path())
    }

    /// The refused ids, in the order `connectors/licences.toml` lists them.
    pub fn denied(&self) -> &[String] {
        &self.denied
    }

    /// Whether `id` is refused.
    pub fn refuses(&self, id: &str) -> bool {
        self.denied.iter().any(|denied| denied == id)
    }

    /// D359 applied to one manifest: its component and every loaded dependency.
    pub fn check_manifest(&self, spec: &ConnectorSpec, origin: &str) -> Result<(), LicenceError> {
        if self.refuses(&spec.licence.component) {
            return Err(LicenceError::RefusedComponent {
                connector: spec.id.clone(),
                id: spec.licence.component.clone(),
                origin: origin.to_string(),
            });
        }
        for (name, id) in &spec.licence.dependencies {
            if self.refuses(id) {
                return Err(LicenceError::RefusedDependency {
                    connector: spec.id.clone(),
                    name: name.clone(),
                    id: id.clone(),
                    origin: origin.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Every `[components.*]` stanza's SPDX id, by stanza key.
    pub fn components(text: &str, origin: &str) -> Result<BTreeMap<String, String>, LicenceError> {
        let flat = parse_licences_toml(text).map_err(|message| LicenceError::Syntax {
            path: origin.to_string(),
            message,
        })?;
        Ok(flat
            .iter()
            .filter_map(|(key, id)| {
                let stanza = key.strip_prefix("components.")?;
                let name = stanza.strip_suffix(".spdx")?;
                Some((name.to_string(), id.clone()))
            })
            .collect())
    }

    /// No `[components.*]` stanza of the gate input may itself carry a refused id:
    /// a component Loams names with an AGPL or BSL id is refused however no manifest
    /// uses it. `scripts/connectors/gen_registry.py --check` asserts the same.
    ///
    /// The one exception is a `ci-service` (OR-3, the D60 precedent): it runs only in
    /// CI, is never shipped, and so may carry a refused id such as Redpanda's BSL. Every
    /// stanza is also read with [`LicenceGate::read_components`], so a `ci-service` or
    /// `user-supplied` stanza missing a field its kind needs fails here too.
    pub fn check_components(&self, text: &str, origin: &str) -> Result<(), LicenceError> {
        for component in Self::read_components(text, origin)? {
            if component.kind != ComponentKind::CiService && self.refuses(&component.spdx) {
                return Err(LicenceError::RefusedListedComponent {
                    path: origin.to_string(),
                    key: component.key,
                    id: component.spdx,
                });
            }
        }
        Ok(())
    }

    /// Every `[components.*]` stanza, read and checked for the fields its kind needs.
    pub fn read_components(
        text: &str,
        origin: &str,
    ) -> Result<Vec<LicenceComponent>, LicenceError> {
        let flat = parse_licences_toml(text).map_err(|message| LicenceError::Syntax {
            path: origin.to_string(),
            message,
        })?;
        let keys: BTreeSet<&str> = flat
            .keys()
            .filter_map(|key| key.strip_prefix("components."))
            .filter_map(|rest| rest.rsplit_once('.').map(|(stanza, _)| stanza))
            .collect();
        keys.into_iter()
            .map(|key| LicenceComponent::read(&flat, key, origin))
            .collect()
    }

    /// D359 for one crate compiled into the family crate `family`: its licence
    /// expression must exist and must leave a choice outside the refused set.
    ///
    /// `OR` (and the old `/` spelling) is satisfied by either side, `AND` needs both,
    /// and `WITH <exception>` is judged by its licence. A leaf is refused when the
    /// `[deny]` list names it or when it is of a refused family (`AGPL-3.0` is
    /// `AGPL-3.0-only` spelled short), the same rule `gen_registry.py` applies.
    pub fn check_crate_licence(
        &self,
        family: &str,
        krate: &str,
        version: &str,
        licence: Option<&str>,
    ) -> Result<(), LicenceError> {
        let Some(licence) = licence.map(str::trim).filter(|licence| !licence.is_empty()) else {
            return Err(LicenceError::UnlicensedCrate {
                family: family.to_string(),
                krate: krate.to_string(),
                version: version.to_string(),
            });
        };
        let refused = || LicenceError::RefusedCrate {
            family: family.to_string(),
            krate: krate.to_string(),
            version: version.to_string(),
            licence: licence.to_string(),
        };
        let tokens = spdx_tokens(licence);
        let mut parser = SpdxParser {
            tokens: &tokens,
            at: 0,
            gate: self,
        };
        match parser.or_expression() {
            Some(true) if parser.at == tokens.len() => Ok(()),
            _ => Err(refused()),
        }
    }

    /// Whether a crate licence id is refused: by the `[deny]` list, or by family.
    fn refuses_crate_id(&self, id: &str) -> bool {
        if self.refuses(id) {
            return true;
        }
        let family = id.split('-').next().unwrap_or(id).to_ascii_uppercase();
        REFUSED_FAMILIES.contains(&family.as_str())
    }

    /// D359 over `cargo metadata --format-version 1`: every crate compiled into each
    /// workspace member named `loams-connector-*` (normal and build dependencies,
    /// transitively; dev dependencies are not compiled into the crate). A workspace
    /// with no family crate passes with an empty report.
    pub fn check_family_crates(
        &self,
        metadata: &Value,
    ) -> Result<Vec<FamilyCrateReport>, Vec<LicenceError>> {
        let malformed = |message: &str| {
            vec![LicenceError::Metadata {
                message: message.to_string(),
            }]
        };
        let Some(packages) = metadata["packages"].as_array() else {
            return Err(malformed("no packages array"));
        };
        let Some(members) = metadata["workspace_members"].as_array() else {
            return Err(malformed("no workspace_members array"));
        };
        let Some(nodes) = metadata["resolve"]["nodes"].as_array() else {
            return Err(malformed("no resolve.nodes array (run without --no-deps)"));
        };
        let by_id: BTreeMap<&str, &Value> = packages
            .iter()
            .filter_map(|package| Some((package["id"].as_str()?, package)))
            .collect();
        let edges: BTreeMap<&str, Vec<&str>> = nodes
            .iter()
            .filter_map(|node| {
                let deps = node["deps"]
                    .as_array()?
                    .iter()
                    .filter(|dep| compiled_in(dep))
                    .filter_map(|dep| dep["pkg"].as_str())
                    .collect();
                Some((node["id"].as_str()?, deps))
            })
            .collect();

        let mut reports = Vec::new();
        let mut errors = Vec::new();
        for member in members.iter().filter_map(Value::as_str) {
            let Some(package) = by_id.get(member) else {
                continue;
            };
            let family = package["name"].as_str().unwrap_or_default();
            if !is_family_crate(family) {
                continue;
            }
            let mut seen: BTreeSet<&str> = BTreeSet::new();
            let mut stack: Vec<&str> = edges.get(member).cloned().unwrap_or_default();
            while let Some(id) = stack.pop() {
                if !seen.insert(id) {
                    continue;
                }
                stack.extend(edges.get(id).into_iter().flatten().copied());
            }
            let mut dependencies = Vec::new();
            for id in &seen {
                let Some(dep) = by_id.get(id) else {
                    errors.push(LicenceError::Metadata {
                        message: format!("{family}: dependency {id} has no package entry"),
                    });
                    continue;
                };
                let name = dep["name"].as_str().unwrap_or(id);
                let version = dep["version"].as_str().unwrap_or_default();
                if let Err(error) =
                    self.check_crate_licence(family, name, version, dep["license"].as_str())
                {
                    errors.push(error);
                }
                dependencies.push(name.to_string());
            }
            dependencies.sort();
            dependencies.dedup();
            reports.push(FamilyCrateReport {
                name: family.to_string(),
                dependencies,
            });
        }
        if errors.is_empty() {
            Ok(reports)
        } else {
            Err(errors)
        }
    }

    /// The `ci-service` and `user-supplied` components that a Containerfile of the
    /// image `image` names: none may be in an image Loams builds (OR-3; CN1 Ruling 8).
    ///
    /// A `ci-service` is found by its image reference on any instruction line; a
    /// `user-supplied` driver by its file name (the pattern up to its first wildcard)
    /// or its mount path. Comment lines are skipped.
    pub fn scan_image(
        components: &[LicenceComponent],
        image: &str,
        containerfile: &str,
    ) -> Result<(), Vec<LicenceError>> {
        let placement = Placement::Image(image);
        let mut errors = Vec::new();
        for line in containerfile.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let lower = line.to_ascii_lowercase();
            for component in components {
                let named = match component.kind {
                    ComponentKind::CiService => component
                        .image
                        .as_deref()
                        .is_some_and(|reference| names_image(&lower, reference)),
                    ComponentKind::UserSupplied => component.names_driver(&lower),
                    _ => false,
                };
                if named && let Err(error) = component.check_placement(&placement) {
                    errors.push(error.with_origin(&format!("image {image}")));
                }
            }
        }
        dedup_errors(errors)
    }

    /// The services of a compose file, against where each `ci-service` and
    /// `user-supplied` component may run. A service with no `profiles` is in the
    /// default profile. `${NAME:-default}` in an image is read as its default.
    pub fn scan_compose(
        components: &[LicenceComponent],
        origin: &str,
        compose: &str,
    ) -> Result<(), Vec<LicenceError>> {
        let document: serde_json::Value = match serde_norway::from_str(compose) {
            Ok(document) => document,
            Err(error) => {
                return Err(vec![LicenceError::Syntax {
                    path: origin.to_string(),
                    message: error.to_string(),
                }]);
            }
        };
        let mut errors = Vec::new();
        let Some(services) = document["services"].as_object() else {
            return Ok(());
        };
        for (name, service) in services {
            let profiles: Vec<String> = service["profiles"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|profile| profile.as_str().map(str::to_string))
                .collect();
            let where_ = format!("{origin} service {name}");
            let image = service["image"]
                .as_str()
                .map(|image| expand_defaults(image).to_ascii_lowercase());
            let volumes: Vec<String> = service["volumes"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(volume_spec)
                .collect();
            for component in components {
                match component.kind {
                    ComponentKind::CiService => {
                        let named = image.as_deref().is_some_and(|image| {
                            component
                                .image
                                .as_deref()
                                .is_some_and(|reference| names_image(image, reference))
                        });
                        if named
                            && let Err(error) =
                                component.check_placement(&Placement::Compose(&profiles))
                        {
                            errors.push(error.with_origin(&where_));
                        }
                    }
                    ComponentKind::UserSupplied => {
                        for volume in &volumes {
                            let target = volume_target(volume);
                            let lower = volume.to_ascii_lowercase();
                            let mount = component.mount.as_deref().unwrap_or_default();
                            let at_mount = !mount.is_empty()
                                && (target == mount || target.starts_with(&format!("{mount}/")));
                            if !(at_mount || component.names_driver(&lower)) {
                                continue;
                            }
                            // A documented mount, but only outside the default profile.
                            let placement = if profiles.is_empty() {
                                Placement::Compose(&profiles)
                            } else {
                                Placement::Mount(target)
                            };
                            if let Err(error) = component.check_placement(&placement) {
                                errors.push(error.with_origin(&where_));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        dedup_errors(errors)
    }
}

/// The licence families D359 refuses whatever the exact spelling, as
/// `scripts/connectors/gen_registry.py`'s `REFUSED_LICENCE_FAMILIES` lists them.
/// `BSL-1.0` (Boost) is not `BUSL`, so it is not among them.
const REFUSED_FAMILIES: [&str; 6] = ["AGPL", "BUSL", "SSPL", "ELASTIC", "NOASSERTION", "UNKNOWN"];

/// Whether `name` is a connector family crate (CN2 Task 3): `loams-connector-*`,
/// which includes `loams-connector-sdk`, compiled into every family crate.
pub fn is_family_crate(name: &str) -> bool {
    name.strip_prefix("loams-connector-")
        .is_some_and(|rest| !rest.is_empty())
}

/// Whether a compose profile is one a `ci-service` may run in (OR-3): a name that
/// starts with `connectors-`, such as `connectors-ci-only` or `connectors-p2`. The
/// bare `connectors` profile is the default connector stack and is not one.
pub fn is_ci_profile(name: &str) -> bool {
    name.strip_prefix("connectors-")
        .is_some_and(|rest| !rest.is_empty())
}

/// One family crate the gate checked, with the crates compiled into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyCrateReport {
    /// The `loams-connector-*` crate.
    pub name: String,
    /// The names of the crates compiled into it, sorted.
    pub dependencies: Vec<String>,
}

/// The kind of a `[components.*]` stanza in `connectors/licences.toml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ComponentKind {
    /// A runtime Loams supervises.
    Runtime,
    /// A driver or shared library loaded from a mounted directory or an allowlist.
    Driver,
    /// A Rust crate compiled in.
    Library,
    /// A service reached over its API, which Loams neither ships nor links.
    Service,
    /// A proprietary or source-available image that runs only in CI (OR-3).
    CiService,
    /// A driver the operator supplies and mounts, never shipped (CN1 Ruling 8).
    UserSupplied,
}

impl ComponentKind {
    /// The kind as `connectors/licences.toml` spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Driver => "driver",
            Self::Library => "library",
            Self::Service => "service",
            Self::CiService => "ci-service",
            Self::UserSupplied => "user-supplied",
        }
    }

    /// The kind from its spelling.
    pub fn parse(text: &str) -> Option<Self> {
        [
            Self::Runtime,
            Self::Driver,
            Self::Library,
            Self::Service,
            Self::CiService,
            Self::UserSupplied,
        ]
        .into_iter()
        .find(|kind| kind.as_str() == text)
    }

    /// Whether Loams ships, links or loads it: the kinds D359's deny list gates
    /// everywhere.
    pub fn is_shipped(self) -> bool {
        matches!(self, Self::Runtime | Self::Driver | Self::Library)
    }
}

impl fmt::Display for ComponentKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One `[components.*]` stanza of `connectors/licences.toml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicenceComponent {
    /// The stanza key.
    pub key: String,
    /// The SPDX id, or a `LicenseRef-*` for a `ci-service` or `user-supplied` one.
    pub spdx: String,
    /// The kind.
    pub kind: ComponentKind,
    /// A `ci-service`'s image reference, without tag or digest.
    pub image: Option<String>,
    /// A `ci-service`'s pinned digest, `sha256:<64 hex>`.
    pub digest: Option<String>,
    /// Why a `ci-service` is needed.
    pub reason: Option<String>,
    /// The compose profiles a `ci-service` runs in, each a CI profile.
    pub profiles: Vec<String>,
    /// A `user-supplied` driver's file-name pattern, such as `ojdbc*.jar`.
    pub driver: Option<String>,
    /// A `user-supplied` driver's documented mount path, absolute.
    pub mount: Option<String>,
}

/// Where a `ci-service` or `user-supplied` component was found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement<'a> {
    /// In an image Loams builds, by name.
    Image(&'a str),
    /// In a compose service with these profiles; none is the default profile.
    Compose(&'a [String]),
    /// In a CI workflow file.
    CiWorkflow(&'a str),
    /// Mounted into a container at this path.
    Mount(&'a str),
}

impl fmt::Display for Placement<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Image(name) => write!(formatter, "image {name}"),
            Self::Compose([]) => formatter.write_str("the default compose profile"),
            Self::Compose(profiles) => {
                write!(formatter, "compose profiles [{}]", profiles.join(", "))
            }
            Self::CiWorkflow(path) => write!(formatter, "CI workflow {path}"),
            Self::Mount(path) => write!(formatter, "mount {path}"),
        }
    }
}

impl LicenceComponent {
    fn read(
        flat: &BTreeMap<String, String>,
        key: &str,
        origin: &str,
    ) -> Result<Self, LicenceError> {
        let field = |name: &str| {
            flat.get(&format!("components.{key}.{name}"))
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let malformed = |message: String| LicenceError::Malformed {
            path: origin.to_string(),
            key: key.to_string(),
            message,
        };
        let spdx = field("spdx").ok_or_else(|| malformed("no spdx".to_string()))?;
        let kind_text = field("kind").ok_or_else(|| malformed("no kind".to_string()))?;
        let kind = ComponentKind::parse(&kind_text).ok_or_else(|| {
            malformed(format!(
                "kind {kind_text:?} is not runtime, driver, library, service, ci-service or \
                 user-supplied"
            ))
        })?;
        let component = Self {
            key: key.to_string(),
            spdx,
            kind,
            image: field("image"),
            digest: field("digest"),
            reason: field("reason"),
            profiles: field("profiles")
                .map(|array| split_toml_array(&array))
                .unwrap_or_default(),
            driver: field("driver"),
            mount: field("mount"),
        };
        component.check_fields().map_err(malformed)?;
        Ok(component)
    }

    /// The fields CN2 Task 3 requires of the two new kinds.
    fn check_fields(&self) -> Result<(), String> {
        match self.kind {
            ComponentKind::CiService => {
                let image = self
                    .image
                    .as_deref()
                    .ok_or("a ci-service names its image")?;
                let last = image.rsplit('/').next().unwrap_or(image);
                if image.contains('@') || last.contains(':') {
                    return Err(format!(
                        "image {image:?} carries a tag or digest; the digest goes in `digest`"
                    ));
                }
                let digest = self.digest.as_deref().unwrap_or_default();
                let hex = digest.strip_prefix("sha256:").unwrap_or_default();
                if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(format!(
                        "digest {digest:?} is not sha256:<64 hex> (images are pinned by digest)"
                    ));
                }
                if self.reason.is_none() {
                    return Err("a ci-service gives the reason it is needed".to_string());
                }
                if !(is_spdx_id(&self.spdx) || is_eula_ref(&self.spdx)) {
                    return Err(format!(
                        "spdx {:?} is neither an SPDX id nor LicenseRef-<vendor>-EULA",
                        self.spdx
                    ));
                }
                if self.profiles.is_empty() {
                    return Err("a ci-service names the CI profiles it runs in".to_string());
                }
                if let Some(profile) = self.profiles.iter().find(|p| !is_ci_profile(p)) {
                    return Err(format!(
                        "profile {profile:?} is not a CI profile (connectors-*; OR-3)"
                    ));
                }
                if self.mount.is_some() || self.driver.is_some() {
                    return Err("a ci-service has no driver or mount".to_string());
                }
            }
            ComponentKind::UserSupplied => {
                if self.driver.is_none() {
                    return Err("a user-supplied component names its driver file".to_string());
                }
                let mount = self.mount.as_deref().unwrap_or_default();
                if !mount.starts_with('/') {
                    return Err(format!(
                        "mount {mount:?} is not an absolute path; a user-supplied driver is \
                         only ever a documented mount"
                    ));
                }
                if !(is_spdx_id(&self.spdx) || self.spdx.starts_with("LicenseRef-")) {
                    return Err(format!(
                        "spdx {:?} is neither an SPDX id nor a LicenseRef-*",
                        self.spdx
                    ));
                }
                if self.image.is_some() || !self.profiles.is_empty() {
                    return Err("a user-supplied driver has no image or profiles".to_string());
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Whether this component may be placed at `placement` (OR-3; CN1 Ruling 8).
    /// Kinds other than `ci-service` and `user-supplied` are placed anywhere: their
    /// licence, not their placement, is what the gate checks.
    pub fn check_placement(&self, placement: &Placement<'_>) -> Result<(), LicenceError> {
        let refuse = |rule: &'static str| {
            Err(LicenceError::Misplaced {
                origin: "connectors/licences.toml".to_string(),
                key: self.key.clone(),
                kind: self.kind,
                placement: placement.to_string(),
                rule,
            })
        };
        match (self.kind, placement) {
            (ComponentKind::CiService, Placement::CiWorkflow(_)) => Ok(()),
            (ComponentKind::CiService, Placement::Compose(profiles))
                if !profiles.is_empty() && profiles.iter().all(|p| is_ci_profile(p)) =>
            {
                Ok(())
            }
            (ComponentKind::CiService, _) => refuse(
                "a ci-service runs only in a connectors-* compose profile or a CI workflow, \
                 never in an image or the default profile (OR-3)",
            ),
            (ComponentKind::UserSupplied, Placement::Mount(path))
                if self.mount.as_deref().is_some_and(|mount| {
                    *path == mount || path.starts_with(&format!("{mount}/"))
                }) =>
            {
                Ok(())
            }
            (ComponentKind::UserSupplied, _) => refuse(
                "a user-supplied driver is only ever mounted at its documented path, never \
                 shipped in an image or the default profile (CN1 Ruling 8)",
            ),
            _ => Ok(()),
        }
    }

    /// Whether lowercased `text` names this `user-supplied` driver's file.
    fn names_driver(&self, text: &str) -> bool {
        let Some(driver) = self.driver.as_deref() else {
            return false;
        };
        let stem = driver
            .split(['*', '?', '['])
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let by_name = stem.len() >= 3 && text.contains(&stem);
        let by_mount = self
            .mount
            .as_deref()
            .is_some_and(|mount| text.contains(&mount.to_ascii_lowercase()));
        by_name || by_mount
    }
}

impl LicenceError {
    /// The same error, naming where it was found.
    fn with_origin(self, origin: &str) -> Self {
        match self {
            Self::Misplaced {
                key,
                kind,
                placement,
                rule,
                ..
            } => Self::Misplaced {
                origin: origin.to_string(),
                key,
                kind,
                placement,
                rule,
            },
            other => other,
        }
    }
}

fn dedup_errors(mut errors: Vec<LicenceError>) -> Result<(), Vec<LicenceError>> {
    let mut seen = BTreeSet::new();
    errors.retain(|error| seen.insert(error.to_string()));
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Whether a `cargo metadata` dependency edge is compiled into its parent: a normal
/// or build dependency, not dev-only.
fn compiled_in(dep: &Value) -> bool {
    match dep["dep_kinds"].as_array() {
        Some(kinds) if !kinds.is_empty() => kinds
            .iter()
            .any(|kind| kind["kind"].as_str() != Some("dev")),
        _ => true,
    }
}

/// Whether lowercased `text` names the image `reference` (no tag or digest):
/// `docker.io/` and `library/` prefixes are ignored, and the reference must end at a
/// tag, a digest, whitespace or the end.
fn names_image(text: &str, reference: &str) -> bool {
    let reference = normalise_image(&reference.to_ascii_lowercase());
    text.split_whitespace().any(|word| {
        let word = normalise_image(word);
        word.strip_prefix(reference.as_str())
            .is_some_and(|rest| rest.is_empty() || rest.starts_with([':', '@']))
    })
}

fn normalise_image(image: &str) -> String {
    let image = image.trim_start_matches("--from=");
    let image = image.strip_prefix("docker.io/").unwrap_or(image);
    image.strip_prefix("library/").unwrap_or(image).to_string()
}

/// `${NAME:-default}` and `${NAME-default}` replaced by their defaults, and a bare
/// `${NAME}` by nothing: what compose runs with no environment set.
fn expand_defaults(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            out.push_str(&rest[start..]);
            return out;
        };
        let inner = &after[..end];
        if let Some((_, default)) = inner.split_once(":-").or_else(|| inner.split_once('-')) {
            out.push_str(default);
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

/// A compose volume as one `source:target[:mode]` string, short or long syntax.
fn volume_spec(volume: &Value) -> Option<String> {
    if let Some(short) = volume.as_str() {
        return Some(expand_defaults(short));
    }
    let target = volume["target"].as_str()?;
    let source = volume["source"].as_str().unwrap_or_default();
    Some(format!("{}:{target}", expand_defaults(source)))
}

/// The container path of a `source:target[:mode]` volume.
fn volume_target(volume: &str) -> &str {
    let mut parts = volume.split(':');
    let first = parts.next().unwrap_or_default();
    parts.next().unwrap_or(first)
}

fn is_spdx_id(id: &str) -> bool {
    !id.is_empty()
        && !id.starts_with("LicenseRef-")
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'+' | b'-'))
}

/// `LicenseRef-<vendor>-EULA`, the id a vendor-licensed `ci-service` carries.
fn is_eula_ref(id: &str) -> bool {
    id.strip_prefix("LicenseRef-")
        .and_then(|rest| rest.strip_suffix("-EULA"))
        .is_some_and(|vendor| {
            !vendor.is_empty()
                && vendor
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
        })
}

/// An SPDX licence expression's tokens: `(`, `)`, and words.
fn spdx_tokens(expression: &str) -> Vec<String> {
    expression
        .replace('(', " ( ")
        .replace(')', " ) ")
        .replace('/', " OR ")
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

/// A recursive-descent reader of an SPDX expression that yields whether it leaves a
/// choice outside the refused set; `None` is an expression it cannot read, which the
/// gate refuses like a missing licence.
struct SpdxParser<'a> {
    tokens: &'a [String],
    at: usize,
    gate: &'a LicenceGate,
}

impl SpdxParser<'_> {
    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.at).map(String::as_str)
    }

    fn or_expression(&mut self) -> Option<bool> {
        let mut acceptable = self.and_expression()?;
        while self
            .peek()
            .is_some_and(|token| token.eq_ignore_ascii_case("OR"))
        {
            self.at += 1;
            acceptable |= self.and_expression()?;
        }
        Some(acceptable)
    }

    fn and_expression(&mut self) -> Option<bool> {
        let mut acceptable = self.term()?;
        while self
            .peek()
            .is_some_and(|token| token.eq_ignore_ascii_case("AND"))
        {
            self.at += 1;
            acceptable &= self.term()?;
        }
        Some(acceptable)
    }

    fn term(&mut self) -> Option<bool> {
        let token = self.peek()?.to_string();
        self.at += 1;
        if token == "(" {
            let inner = self.or_expression()?;
            if self.peek() != Some(")") {
                return None;
            }
            self.at += 1;
            return Some(inner);
        }
        if token == ")" || ["AND", "OR", "WITH"].contains(&token.to_ascii_uppercase().as_str()) {
            return None;
        }
        if self
            .peek()
            .is_some_and(|next| next.eq_ignore_ascii_case("WITH"))
        {
            self.at += 1;
            self.peek()?;
            self.at += 1;
        }
        let id = token.trim_end_matches('+');
        Some(!self.gate.refuses_crate_id(id))
    }
}

/// The parts of `connectors/licences.toml` the gate reads, as `section.key -> value`.
///
/// A small hand-rolled reader, not a TOML dependency: the gate needs `[deny] ids`
/// and each `[components.*]` stanza's `spdx`, and a crate for those two keys would
/// be a new dependency in a crate whose whole job is to hold the repository's
/// licence decisions (D359). Comments are stripped outside quotes, multi-line arrays
/// are joined, and a double- or single-quoted scalar is unquoted.
pub fn parse_licences_toml(text: &str) -> Result<BTreeMap<String, String>, String> {
    let mut flat = BTreeMap::new();
    let mut section = String::new();
    let mut pending: Option<(String, String)> = None;
    for (number, raw) in text.lines().enumerate() {
        let line = strip_toml_comment(raw).trim().to_string();
        if line.is_empty() {
            continue;
        }
        if let Some((_, value)) = pending.as_mut() {
            value.push(' ');
            value.push_str(&line);
            if value.contains(']') {
                let (key, value) = match pending.take() {
                    Some(pending) => pending,
                    None => continue,
                };
                flat.insert(format!("{section}.{key}"), value);
            }
            continue;
        }
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
        {
            section = name.trim().to_string();
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(format!("line {}: no `=` in {line:?}", number + 1));
        };
        let key = key.trim().to_string();
        let value = value.trim().to_string();
        if value.starts_with('[') {
            if value.contains(']') {
                flat.insert(format!("{section}.{key}"), value);
            } else {
                pending = Some((key, value));
            }
            continue;
        }
        flat.insert(format!("{section}.{key}"), unquote_toml(&value));
    }
    if pending.is_some() {
        return Err("a `[...]` array is never closed".to_string());
    }
    if flat.is_empty() {
        return Err("no table or key found".to_string());
    }
    Ok(flat)
}

/// A `#` comment ends a line unless it is inside a quoted string.
fn strip_toml_comment(line: &str) -> &str {
    let mut quote: Option<char> = None;
    for (index, character) in line.char_indices() {
        match (quote, character) {
            (None, '"') | (None, '\'') => quote = Some(character),
            (Some(open), character) if open == character => quote = None,
            (None, '#') => return &line[..index],
            _ => {}
        }
    }
    line
}

/// The members of a TOML array, unquoted and in order.
fn split_toml_array(array: &str) -> Vec<String> {
    let inner = array
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim();
    inner
        .split(',')
        .map(|item| unquote_toml(item.trim()))
        .filter(|item| !item.is_empty())
        .collect()
}

/// A TOML scalar with its quotes removed; anything else is returned as it stands.
fn unquote_toml(value: &str) -> String {
    for quote in ['"', '\''] {
        if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
            return value[1..value.len() - 1].to_string();
        }
    }
    value.to_string()
}

/// Every error of a failed load, ready to print: `Registry::load`'s `Display`.
pub fn render_manifest_errors(files: &[ManifestErrors]) -> String {
    files
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    //! The licence gate's reader is the only hand-rolled parser in the crate, so it
    //! is checked against the shape of `connectors/licences.toml` it must read.

    use super::*;

    const TOML: &str = r#"
# a comment, and one after a value
[deny]
ids = [
  "AGPL-3.0-only",
  "BUSL-1.1",
  "unknown",
]

[components.kafka]
name = "Apache Kafka"   # not a runtime Loams supervises by itself
spdx = "Apache-2.0"
kind = "runtime"
used_by = ["kafka", "kafka-connect"]
"#;

    #[test]
    fn the_licence_reader_reads_deny_and_the_component_ids() {
        let gate = LicenceGate::from_toml(TOML, "licences.toml")
            .unwrap_or_else(|error| panic!("the gate must read the file: {error}"));
        assert_eq!(gate.denied(), ["AGPL-3.0-only", "BUSL-1.1", "unknown"]);
        assert!(gate.refuses("AGPL-3.0-only"));
        assert!(!gate.refuses("Apache-2.0"));
        let components = LicenceGate::components(TOML, "licences.toml")
            .unwrap_or_else(|error| panic!("the components must read: {error}"));
        assert_eq!(
            components.get("kafka").map(String::as_str),
            Some("Apache-2.0"),
            "only spdx is read: {components:?}"
        );
    }

    #[test]
    fn a_gate_with_no_denied_ids_is_refused() {
        let error = LicenceGate::from_toml("[deny]\nids = []\n", "licences.toml").err();
        assert!(
            error.is_some(),
            "an empty [deny] would refuse nothing (D359)"
        );
    }
}
