//! The door (design §54 §10.3 steps 4 and 12): the Tailscale sidecars are
//! the stack's only way in for clients (the engine, the House and the
//! console listen on loopback or on internal networks), so taking them away
//! closes the stack to clients while it is half upgraded.
//!
//! Closing removes the sidecars' containers (their state volumes stay)
//! rather than stopping them: each sidecar is in the network namespace of
//! the service it fronts (`network_mode: service:loams`), and Podman refuses
//! to recreate that service while a container, even a stopped one, depends
//! on its namespace. Every other service with `network_mode: service:…` is
//! treated the same way. Opening creates them again.

use std::path::Path;

use crate::compose;
use crate::errors::Result;
use crate::runtime::Runtime;

/// The sidecars that let clients in.
pub const SIDECARS: [&str; 2] = ["tailscale", "house-tailscale"];

/// What the door takes away and brings back: the sidecars `config` enables,
/// then the other long-running services in another service's network
/// namespace.
pub fn gates(config: &serde_json::Value) -> Vec<String> {
    let enabled = compose::services(config);
    let long = compose::long_running(config);
    let mut gates = compose::present(&enabled, &SIDECARS);
    for dependent in compose::network_dependents(config) {
        if long.contains(&dependent) && !gates.contains(&dependent) {
            gates.push(dependent);
        }
    }
    gates
}

/// Whether `service` is one of the door's `gates`.
pub fn is_gate(config: &serde_json::Value, service: &str) -> bool {
    SIDECARS.contains(&service) || gates(config).iter().any(|g| g == service)
}

/// Removes the gates' containers.
pub fn close(runtime: &dyn Runtime, dir: &Path, config: &serde_json::Value) -> Result<()> {
    compose::remove(runtime, dir, &gates(config))
}

/// Creates and starts them again.
pub fn open(runtime: &dyn Runtime, dir: &Path, config: &serde_json::Value) -> Result<()> {
    compose::up(runtime, dir, &gates(config))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn config() -> serde_json::Value {
        json!({"services": {
            "loams": {"restart": "unless-stopped"},
            "house": {"restart": "unless-stopped"},
            "loams-forward": {"restart": "unless-stopped", "network_mode": "service:loams"},
            "house-tailscale": {"restart": "unless-stopped", "network_mode": "service:house"},
            "tailscale": {"restart": "unless-stopped", "network_mode": "service:loams"},
            "probe": {"restart": "no", "network_mode": "service:loams"},
            "host-net": {"network_mode": "host"},
        }})
    }

    #[test]
    fn gates_are_sidecars_then_other_namespace_dependents() {
        assert_eq!(
            gates(&config()),
            ["tailscale", "house-tailscale", "loams-forward"]
        );
        assert!(is_gate(&config(), "loams-forward"));
        assert!(!is_gate(&config(), "loams"));
        assert!(
            !is_gate(&config(), "probe"),
            "a one-shot service is not a gate"
        );
        assert!(!is_gate(&config(), "host-net"));
    }

    #[test]
    fn gates_without_sidecars() {
        let config = json!({"services": {"loams": {}}});
        assert!(gates(&config).is_empty());
    }
}
