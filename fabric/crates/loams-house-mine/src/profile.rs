//! Merging inventories into a ranking, and the profile written from it
//! (§75 §7.1).
//!
//! Features are ranked by the number of applications that use them, then by
//! how many statements do. A feature is in the profile if `min_apps` (3) or
//! more applications use it, or if a gate application (Langfuse, OpenPanel,
//! SigNoz: D1571) does. Variants of one application (Langfuse v3 and v4) count as one
//! application.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::features::KINDS;
use crate::inventory::{Counts, Inventory};

/// The ranking file's schema id.
pub const RANKING_SCHEMA: &str = "loams-house-mine/ranking/1";
/// The profile file's schema id.
pub const PROFILE_SCHEMA: &str = "loams-house-mine/profile/1";

/// One application in a ranking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSummary {
    /// Its name.
    pub app: String,
    /// The variants mined, if any.
    pub variants: Vec<String>,
    /// Its statements, summed over variants.
    pub statements: Counts,
}

/// One feature in a ranking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ranked {
    /// `kind:name`.
    pub name: String,
    /// The applications that use it.
    pub apps: Vec<String>,
    /// Statements that use it, over every application.
    pub frequency: u64,
    /// Whether it is in the profile.
    pub in_profile: bool,
    /// Why: `apps>=3`, `gate:langfuse`, or `below threshold`.
    pub reason: String,
}

/// The output of `merge`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ranking {
    /// [`RANKING_SCHEMA`].
    pub schema: String,
    /// The application threshold.
    pub min_apps: usize,
    /// The gate applications.
    pub gate: Vec<String>,
    /// The applications merged.
    pub apps: Vec<AppSummary>,
    /// Every feature, ranked.
    pub features: Vec<Ranked>,
}

/// Merges inventories.
pub fn merge(inventories: &[Inventory], min_apps: usize, gate: &[String]) -> Ranking {
    let mut apps: BTreeMap<String, AppSummary> = BTreeMap::new();
    let mut uses: BTreeMap<&str, (BTreeSet<&str>, u64)> = BTreeMap::new();
    for inv in inventories {
        let summary = apps.entry(inv.app.clone()).or_insert_with(|| AppSummary {
            app: inv.app.clone(),
            variants: Vec::new(),
            statements: Counts::default(),
        });
        if let Some(v) = &inv.variant {
            summary.variants.push(v.clone());
        }
        let s = &mut summary.statements;
        s.total += inv.statements.total;
        s.parsed += inv.statements.parsed;
        s.explained += inv.statements.explained;
        s.unparsed += inv.statements.unparsed;
        for (feature, u) in &inv.features {
            let entry = uses.entry(feature).or_default();
            entry.0.insert(&inv.app);
            entry.1 += u.count;
        }
    }
    let mut features: Vec<Ranked> = uses
        .into_iter()
        .map(|(name, (users, frequency))| {
            let gate_user = gate.iter().find(|g| users.contains(g.as_str()));
            let (in_profile, reason) = if users.len() >= min_apps {
                (true, format!("apps>={min_apps}"))
            } else if let Some(g) = gate_user {
                (true, format!("gate:{g}"))
            } else {
                (false, "below threshold".to_owned())
            };
            Ranked {
                name: name.to_owned(),
                apps: users.into_iter().map(str::to_owned).collect(),
                frequency,
                in_profile,
                reason,
            }
        })
        .collect();
    features.sort_by(|a, b| {
        b.apps
            .len()
            .cmp(&a.apps.len())
            .then(b.frequency.cmp(&a.frequency))
            .then(a.name.cmp(&b.name))
    });
    for summary in apps.values_mut() {
        summary.variants.sort();
    }
    Ranking {
        schema: RANKING_SCHEMA.to_owned(),
        min_apps,
        gate: gate.to_vec(),
        apps: apps.into_values().collect(),
        features,
    }
}

/// `profile.toml`: the features in the profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Profile {
    /// [`PROFILE_SCHEMA`].
    pub schema: String,
    /// The application threshold.
    pub min_apps: usize,
    /// The gate applications.
    pub gate: Vec<String>,
    /// The applications mined.
    pub apps: Vec<String>,
    /// The features in the profile, ranked.
    pub feature: Vec<ProfileFeature>,
}

/// One feature of `profile.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileFeature {
    /// `kind:name`.
    pub name: String,
    /// The applications that use it.
    pub apps: Vec<String>,
    /// Statements that use it.
    pub frequency: u64,
    /// Why it is in the profile.
    pub reason: String,
}

impl Profile {
    /// The profile of a ranking.
    pub fn from_ranking(ranking: &Ranking) -> Self {
        Self {
            schema: PROFILE_SCHEMA.to_owned(),
            min_apps: ranking.min_apps,
            gate: ranking.gate.clone(),
            apps: ranking.apps.iter().map(|a| a.app.clone()).collect(),
            feature: ranking
                .features
                .iter()
                .filter(|f| f.in_profile)
                .map(|f| ProfileFeature {
                    name: f.name.clone(),
                    apps: f.apps.clone(),
                    frequency: f.frequency,
                    reason: f.reason.clone(),
                })
                .collect(),
        }
    }

    /// Whether `feature` (`kind:name`) is in the profile.
    pub fn contains(&self, feature: &str) -> bool {
        self.feature.iter().any(|f| f.name == feature)
    }
}

/// The generated `docs/house/compat-profile.md`.
pub fn render_markdown(ranking: &Ranking) -> String {
    let mut md = String::new();
    let _ = writeln!(
        md,
        "<!-- Generated by `loams-house-mine report` from the mined inventories; do not edit. -->\n"
    );
    let _ = writeln!(md, "# The Loams House compatibility profile\n");
    let _ = writeln!(
        md,
        "The ClickHouse features real applications use, mined from their query logs and migrations (design §75 §7.1, plan HE1 Task 2). A feature is in the profile when {} or more applications use it, or when a gate application ({}) does.\n",
        ranking.min_apps,
        ranking.gate.join(", ")
    );
    let _ = writeln!(md, "## Applications\n");
    let _ = writeln!(
        md,
        "| Application | Variants | Statements | sqlparser | EXPLAIN AST | Unparsed |\n|---|---|---:|---:|---:|---:|"
    );
    for a in &ranking.apps {
        let s = a.statements;
        let _ = writeln!(
            md,
            "| {} | {} | {} | {} | {} | {} |",
            a.app,
            if a.variants.is_empty() {
                "".to_owned()
            } else {
                a.variants.join(", ")
            },
            s.total,
            s.parsed,
            s.explained,
            s.unparsed
        );
    }
    for kind in KINDS {
        let prefix = format!("{kind}:");
        let rows: Vec<&Ranked> = ranking
            .features
            .iter()
            .filter(|f| f.name.starts_with(&prefix))
            .collect();
        if rows.is_empty() {
            continue;
        }
        let _ = writeln!(md, "\n## `{kind}`\n");
        let _ = writeln!(
            md,
            "| Feature | Applications | Statements | In profile | Why |\n|---|---|---:|---|---|"
        );
        for f in rows {
            let _ = writeln!(
                md,
                "| `{}` | {} ({}) | {} | {} | {} |",
                f.name[prefix.len()..].replace('|', "\\|"),
                f.apps.len(),
                f.apps.join(", "),
                f.frequency,
                if f.in_profile { "yes" } else { "no" },
                f.reason
            );
        }
    }
    md
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{SCHEMA, Uses};

    fn inv(app: &str, variant: Option<&str>, features: &[(&str, u64)]) -> Inventory {
        Inventory {
            schema: SCHEMA.to_owned(),
            app: app.to_owned(),
            variant: variant.map(str::to_owned),
            statements: Counts::default(),
            skipped: Default::default(),
            features: features
                .iter()
                .map(|(f, n)| {
                    (
                        (*f).to_owned(),
                        Uses {
                            count: *n,
                            sources: vec!["q:1".to_owned()],
                        },
                    )
                })
                .collect(),
            unparsed: Uses::default(),
            samples: BTreeMap::new(),
        }
    }

    #[test]
    fn three_apps_or_a_gate_app_put_a_feature_in_the_profile() {
        let gate = vec!["langfuse".to_owned(), "openpanel".to_owned()];
        let ranking = merge(
            &[
                inv(
                    "langfuse",
                    Some("v3"),
                    &[("function:a", 1), ("function:g", 5)],
                ),
                inv(
                    "langfuse",
                    Some("v4"),
                    &[("function:a", 1), ("function:v4", 1)],
                ),
                inv("umami", None, &[("function:a", 2), ("function:b", 9)]),
                inv("signoz", None, &[("function:a", 3), ("function:b", 1)]),
                inv("plausible", None, &[("function:b", 1)]),
                inv("posthog", None, &[("function:c", 7)]),
            ],
            3,
            &gate,
        );
        let get = |n: &str| ranking.features.iter().find(|f| f.name == n).unwrap();
        assert!(get("function:a").in_profile);
        assert_eq!(get("function:a").apps, ["langfuse", "signoz", "umami"]);
        assert_eq!(get("function:a").frequency, 7);
        assert!(get("function:b").in_profile);
        assert_eq!(get("function:g").reason, "gate:langfuse");
        assert!(get("function:v4").in_profile);
        assert!(!get("function:c").in_profile);
        // Variants of one application are one application.
        assert_eq!(ranking.apps.len(), 5);
        // Ranked by applications, then frequency.
        let order: Vec<&str> = ranking.features.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            order,
            [
                "function:b",
                "function:a",
                "function:c",
                "function:g",
                "function:v4"
            ]
        );
        let profile = Profile::from_ranking(&ranking);
        assert!(profile.contains("function:g") && !profile.contains("function:c"));
        let md = render_markdown(&ranking);
        assert!(md.contains("| `a` | 3 (langfuse, signoz, umami) | 7 | yes | apps>=3 |"));
    }
}
