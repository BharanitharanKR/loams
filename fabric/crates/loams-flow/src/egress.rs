//! The egress guard (CN2 Task 25; Shared contracts "Egress guard"; Review
//! Focus 9, SSRF).
//!
//! [`EgressPolicy`] is applied twice:
//! - **at validation**, by [`EgressPolicy::check_config`], to every host an
//!   instance config names (CN1 Task 3's `ValidateInstance` calls it; R25.1);
//! - **at connect time, after DNS**, through the SDK's hook: the policy (or an
//!   [`EgressGuard`], which adds a resolver) is the instance's
//!   `ConnectorContext::egress`, and `sdk::net::connect` and `sdk::http`
//!   check the host before DNS and every resolved address before dialling,
//!   on every connection and on every followed redirect.
//!
//! Fallback pods cannot be hooked, so they get a Kubernetes NetworkPolicy
//! rendered from the same policy ([`render_network_policy`]).
//!
//! Precedence: an address in `allow_cidrs` is allowed; otherwise one in
//! `deny_cidrs` is refused; otherwise it is allowed. A host in `allow_hosts`
//! lifts only the *name* check (`deny_hosts`): its addresses are still
//! checked after DNS, so a name an admin allowlisted cannot be rebound to the
//! metadata endpoint (R25.2).

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

pub use ipnet::IpNet;
use loams_connector_sdk::{EgressCheck, EgressDenied, Resolve, SystemResolver};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::manifest::ConnectorSpec;

/// Addresses no instance reaches unless the namespace admin allowlists
/// them: unspecified, loopback, link-local (the AWS, GCP and Azure metadata
/// endpoint 169.254.169.254, and node-local link addresses), AWS's IPv6
/// metadata endpoint, Alibaba Cloud's metadata endpoint, Azure's WireServer,
/// multicast and broadcast.
pub const DEFAULT_DENY_CIDRS: &[&str] = &[
    "0.0.0.0/8",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "100.100.100.200/32",
    "168.63.129.16/32",
    "224.0.0.0/4",
    "255.255.255.255/32",
    "::/128",
    "::1/128",
    "fe80::/10",
    "fd00:ec2::254/128",
    "ff00::/8",
];

/// Names refused before DNS. A leading `*.` matches any subdomain (not the
/// name itself). In-cluster names are refused because the cluster's own
/// Services are (the plan's "cluster's pod and service CIDRs").
pub const DEFAULT_DENY_HOSTS: &[&str] = &[
    "localhost",
    "*.localhost",
    "metadata",
    "metadata.google.internal",
    "metadata.goog",
    "instance-data",
    "instance-data.ec2.internal",
    "kubernetes",
    "kubernetes.default",
    "*.svc",
    "*.cluster.local",
];

/// The cluster's own networks, from the fleet's configuration (Task 26
/// fills it from the cluster; R25.3).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNetworks {
    pub pod_cidrs: Vec<IpNet>,
    pub service_cidrs: Vec<IpNet>,
    /// The Loams control-plane Services' addresses.
    pub control_plane: Vec<IpNet>,
    /// The Loams control-plane Services' names.
    pub control_plane_hosts: Vec<String>,
}

/// The egress policy of a namespace (Shared contracts). `deny_hosts` is an
/// addition (R25.2): the name-level half of the default deny.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressPolicy {
    pub allow_cidrs: Vec<IpNet>,
    pub deny_cidrs: Vec<IpNet>,
    pub allow_hosts: Vec<String>,
    #[serde(default)]
    pub deny_hosts: Vec<String>,
}

/// Why an instance's egress was refused. Never carries a URL's userinfo,
/// path or query: only the host and the config path that named it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EgressError {
    #[error("egress to {target} refused: {reason}")]
    Denied { target: String, reason: String },
    #[error("config `{path}`: {reason}")]
    BadTarget { path: String, reason: String },
}

impl Default for EgressPolicy {
    fn default() -> Self {
        Self::default_deny()
    }
}

fn parse_nets(list: &[&str]) -> Vec<IpNet> {
    list.iter()
        .filter_map(|s| s.parse::<IpNet>().ok())
        .collect()
}

impl EgressPolicy {
    /// The always-denied set ([`DEFAULT_DENY_CIDRS`], [`DEFAULT_DENY_HOSTS`])
    /// with no cluster networks and no allowlist.
    pub fn default_deny() -> Self {
        Self {
            allow_cidrs: Vec::new(),
            deny_cidrs: parse_nets(DEFAULT_DENY_CIDRS),
            allow_hosts: Vec::new(),
            deny_hosts: DEFAULT_DENY_HOSTS.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    /// [`default_deny`](Self::default_deny) plus the cluster's pod and
    /// Service CIDRs and the Loams control-plane Services.
    pub fn for_cluster(cluster: &ClusterNetworks) -> Self {
        let mut p = Self::default_deny();
        p.deny_cidrs.extend(cluster.pod_cidrs.iter().copied());
        p.deny_cidrs.extend(cluster.service_cidrs.iter().copied());
        p.deny_cidrs.extend(cluster.control_plane.iter().copied());
        p.deny_hosts.extend(
            cluster
                .control_plane_hosts
                .iter()
                .map(|h| normalise_host(h)),
        );
        p
    }

    /// One IP address against the CIDR lists. An IPv4 address embedded in
    /// an IPv6 one (IPv4-mapped `::ffff:0:0/96`, IPv4-compatible `::/96`,
    /// NAT64 `64:ff9b::/96`) is checked as the IPv4 address too.
    pub fn check_ip(&self, ip: IpAddr) -> Result<(), EgressError> {
        let mut forms = vec![ip];
        if let IpAddr::V6(v6) = ip
            && let Some(v4) = embedded_v4(v6)
        {
            forms.push(IpAddr::V4(v4));
        }
        for f in forms {
            if self.allow_cidrs.iter().any(|n| n.contains(&f)) {
                continue;
            }
            if let Some(n) = self.deny_cidrs.iter().find(|n| n.contains(&f)) {
                return Err(EgressError::Denied {
                    target: ip.to_string(),
                    reason: format!("{f} is in the denied range {n}"),
                });
            }
        }
        Ok(())
    }

    /// After DNS, at connect: the address about to be dialled.
    pub fn check_addr(&self, addr: &SocketAddr) -> Result<(), EgressError> {
        self.check_ip(addr.ip())
    }

    /// Before DNS: a host name or IP literal (brackets allowed). A name that
    /// a resolver would read as a non-canonical IPv4 literal (`127.1`,
    /// `2130706433`, `0x7f.0.0.1`) is refused outright.
    pub fn check_host(&self, host: &str) -> Result<(), EgressError> {
        let h = normalise_host(host);
        if h.is_empty() {
            return Err(EgressError::Denied {
                target: host.to_owned(),
                reason: "empty host".into(),
            });
        }
        if let Ok(ip) = h.parse::<IpAddr>() {
            return self.check_ip(ip);
        }
        if looks_numeric(&h) {
            return Err(EgressError::Denied {
                target: h,
                reason: "a non-canonical IP literal".into(),
            });
        }
        if self.allow_hosts.iter().any(|p| host_matches(p, &h)) {
            return Ok(());
        }
        if let Some(p) = self.deny_hosts.iter().find(|p| host_matches(p, &h)) {
            return Err(EgressError::Denied {
                target: h,
                reason: format!("the name matches the denied pattern {p}"),
            });
        }
        Ok(())
    }

    /// At validation: every host the config names. The manifest's `secrets`
    /// paths hold secret *names* and are skipped. Hosts are found under the
    /// keys of [`is_host_key`] (comma-separated lists, `host:port`, URLs and
    /// multi-host URLs such as `mongodb://a:1,b:2/`) and in any string that
    /// holds a URL. A `file:` or `unix:` URL is refused. Every problem is
    /// reported; the first is returned.
    pub fn check_config(&self, spec: &ConnectorSpec, config: &Value) -> Result<(), EgressError> {
        for t in config_targets(spec, config) {
            match t.host {
                Ok(host) => self.check_host(&host).map_err(|e| match e {
                    EgressError::Denied { target, reason } => EgressError::Denied {
                        target: format!("{target} (config `{}` of {})", t.path, spec.id),
                        reason,
                    },
                    other => other,
                })?,
                Err(reason) => {
                    return Err(EgressError::BadTarget {
                        path: t.path,
                        reason,
                    });
                }
            }
        }
        Ok(())
    }
}

impl EgressCheck for EgressPolicy {
    fn check_host(&self, host: &str, _port: u16) -> Result<(), EgressDenied> {
        EgressPolicy::check_host(self, host).map_err(|e| EgressDenied(e.to_string()))
    }
    fn check_addr(&self, addr: &SocketAddr) -> Result<(), EgressDenied> {
        EgressPolicy::check_addr(self, addr).map_err(|e| EgressDenied(e.to_string()))
    }
}

/// An [`EgressPolicy`] with the resolver its connections use: the
/// instance's `ConnectorContext::egress`.
#[derive(Clone)]
pub struct EgressGuard {
    policy: EgressPolicy,
    resolver: Arc<dyn Resolve>,
}

impl std::fmt::Debug for EgressGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EgressGuard")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl EgressGuard {
    pub fn new(policy: EgressPolicy) -> Self {
        Self {
            policy,
            resolver: Arc::new(SystemResolver),
        }
    }

    pub fn with_resolver(mut self, resolver: Arc<dyn Resolve>) -> Self {
        self.resolver = resolver;
        self
    }

    pub fn policy(&self) -> &EgressPolicy {
        &self.policy
    }
}

impl EgressCheck for EgressGuard {
    fn check_host(&self, host: &str, port: u16) -> Result<(), EgressDenied> {
        EgressCheck::check_host(&self.policy, host, port)
    }
    fn check_addr(&self, addr: &SocketAddr) -> Result<(), EgressDenied> {
        EgressCheck::check_addr(&self.policy, addr)
    }
    fn resolver(&self) -> Arc<dyn Resolve> {
        self.resolver.clone()
    }
}

fn embedded_v4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    let s = v6.segments();
    let tail = Ipv4Addr::new(
        (s[6] >> 8) as u8,
        (s[6] & 0xff) as u8,
        (s[7] >> 8) as u8,
        (s[7] & 0xff) as u8,
    );
    let mapped = s[..5] == [0, 0, 0, 0, 0] && s[5] == 0xffff;
    let compatible = s[..6] == [0, 0, 0, 0, 0, 0] && !(s[6] == 0 && s[7] <= 1);
    let nat64 = s[..6] == [0x64, 0xff9b, 0, 0, 0, 0];
    (mapped || compatible || nat64).then_some(tail)
}

/// Lower case, no brackets, no trailing dot.
fn normalise_host(host: &str) -> String {
    host.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

/// Digits and dots only, or a `0x` hex component: what `inet_aton` would
/// take as an IPv4 address.
fn looks_numeric(h: &str) -> bool {
    !h.is_empty()
        && h.split('.').all(|part| {
            let hex = part
                .strip_prefix("0x")
                .is_some_and(|d| !d.is_empty() && d.chars().all(|c| c.is_ascii_hexdigit()));
            hex || (!part.is_empty() && part.chars().all(|c| c.is_ascii_digit()))
        })
}

fn host_matches(pattern: &str, host: &str) -> bool {
    let p = normalise_host(pattern);
    match p.strip_prefix("*.") {
        Some(suffix) => host.len() > suffix.len() && host.ends_with(&format!(".{suffix}")),
        None => p == host,
    }
}

/// Whether a config key names a network target: `host`, `hosts`,
/// `hostname`, `url`, `urls`, `uri`, `endpoint`, `endpoints`, `base_url`,
/// `dsn`, `brokers`, `bootstrap_servers`, `servers`, `seeds`,
/// `contact_points`, `nodes`, `address`, `addresses`, and any key ending in
/// `_host`, `_hosts`, `_url`, `_uri` or `_endpoint`.
pub fn is_host_key(key: &str) -> bool {
    const EXACT: &[&str] = &[
        "host",
        "hosts",
        "hostname",
        "url",
        "urls",
        "uri",
        "endpoint",
        "endpoints",
        "base_url",
        "dsn",
        "brokers",
        "bootstrap_servers",
        "servers",
        "seeds",
        "contact_points",
        "nodes",
        "address",
        "addresses",
    ];
    const SUFFIX: &[&str] = &["_host", "_hosts", "_url", "_uri", "_endpoint"];
    let k = key.to_ascii_lowercase();
    EXACT.contains(&k.as_str()) || SUFFIX.iter().any(|s| k.ends_with(s))
}

/// A network target a config names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigTarget {
    /// The dotted config path.
    pub path: String,
    /// The host, or why the value is not a usable target.
    pub host: Result<String, String>,
}

/// Every network target in `config` (see [`EgressPolicy::check_config`]).
pub fn config_targets(spec: &ConnectorSpec, config: &Value) -> Vec<ConfigTarget> {
    let mut out = Vec::new();
    walk(spec, config, "", false, &mut out);
    out
}

fn walk(spec: &ConnectorSpec, v: &Value, path: &str, host_key: bool, out: &mut Vec<ConfigTarget>) {
    if !path.is_empty() && spec.secrets.iter().any(|s| s == path) {
        return;
    }
    match v {
        Value::Object(map) => {
            for (k, x) in map {
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                walk(spec, x, &p, is_host_key(k), out);
            }
        }
        Value::Array(items) => {
            for x in items {
                walk(spec, x, path, host_key, out);
            }
        }
        Value::String(s) if host_key || s.contains("://") => {
            for host in targets_in(s, host_key) {
                out.push(ConfigTarget {
                    path: path.to_owned(),
                    host,
                });
            }
        }
        _ => {}
    }
}

/// The hosts in one string: a URL (possibly with several hosts), or, under
/// a host key, a comma- or space-separated list of `host`, `host:port` and
/// `[v6]:port`.
fn targets_in(s: &str, host_key: bool) -> Vec<Result<String, String>> {
    const LOCAL_SCHEMES: &[&str] = &["file", "unix", "jar", "netdoc"];
    let mut out = Vec::new();
    for word in s.split_whitespace() {
        if let Some((scheme, rest)) = word.split_once("://") {
            // `jdbc:postgresql://` names its network scheme last.
            let scheme = scheme
                .rsplit(':')
                .next()
                .unwrap_or(scheme)
                .to_ascii_lowercase();
            if LOCAL_SCHEMES.contains(&scheme.as_str()) {
                out.push(Err(format!(
                    "a {scheme}: URL reaches the runtime's own host"
                )));
                continue;
            }
            let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
            let hosts = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
            if hosts.is_empty() {
                out.push(Err(format!("a {scheme}: URL with no host")));
                continue;
            }
            // Multi-host URLs: `mongodb://u:p@h1:27017,h2:27017/db`.
            out.extend(
                hosts
                    .split(',')
                    .filter(|h| !h.is_empty())
                    .map(|h| Ok(strip_port(h))),
            );
        } else if word.split_once(':').is_some_and(|(scheme, _)| {
            LOCAL_SCHEMES.contains(&scheme.to_ascii_lowercase().as_str())
        }) {
            out.push(Err("a local URL reaches the runtime's own host".into()));
        } else if host_key {
            for piece in word.split(',').filter(|p| !p.is_empty()) {
                let piece = piece.split(['/', '?']).next().unwrap_or(piece);
                let piece = piece.rsplit_once('@').map_or(piece, |(_, h)| h);
                if !piece.is_empty() {
                    out.push(Ok(strip_port(piece)));
                }
            }
        }
    }
    out
}

fn strip_port(hostport: &str) -> String {
    if let Some(rest) = hostport.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest).to_owned();
    }
    if hostport.parse::<Ipv6Addr>().is_ok() {
        return hostport.to_owned();
    }
    match hostport.rsplit_once(':') {
        Some((h, port)) if port.chars().all(|c| c.is_ascii_digit()) => h.to_owned(),
        _ => hostport.to_owned(),
    }
}

/// The Kubernetes NetworkPolicy that confines a fallback pod's egress to
/// `policy` (fallback pods have no SDK hook). Rules, unioned by Kubernetes:
/// DNS to the cluster's `kube-dns` pods; every IPv4 and IPv6 address except
/// the policy's denied CIDRs; and each allowed CIDR explicitly, which
/// re-opens it where a deny would close it. Names (`allow_hosts`,
/// `deny_hosts`) cannot be expressed in a NetworkPolicy and are enforced by
/// validation only (R25.4). Deterministic: the same policy renders the same
/// object.
pub fn render_network_policy(
    policy: &EgressPolicy,
    k8s_namespace: &str,
    name: &str,
    pod_labels: &BTreeMap<String, String>,
) -> Value {
    let mut v4: Vec<String> = Vec::new();
    let mut v6: Vec<String> = Vec::new();
    for n in &policy.deny_cidrs {
        let n = n.trunc();
        if n.prefix_len() == 0 {
            continue;
        }
        let list = match n {
            IpNet::V4(_) => &mut v4,
            IpNet::V6(_) => &mut v6,
        };
        let s = n.to_string();
        if !list.contains(&s) {
            list.push(s);
        }
    }
    let mut open = Vec::new();
    for (cidr, except) in [("0.0.0.0/0", v4), ("::/0", v6)] {
        let block = if except.is_empty() {
            json!({ "ipBlock": { "cidr": cidr } })
        } else {
            json!({ "ipBlock": { "cidr": cidr, "except": except } })
        };
        open.push(block);
    }
    let mut egress = vec![
        json!({
            "to": [{
                "namespaceSelector": { "matchLabels": { "kubernetes.io/metadata.name": "kube-system" } },
                "podSelector": { "matchLabels": { "k8s-app": "kube-dns" } }
            }],
            "ports": [
                { "protocol": "UDP", "port": 53 },
                { "protocol": "TCP", "port": 53 }
            ]
        }),
        json!({ "to": open }),
    ];
    let mut allowed: Vec<String> = Vec::new();
    for n in &policy.allow_cidrs {
        let s = n.trunc().to_string();
        if !allowed.contains(&s) {
            allowed.push(s);
        }
    }
    if !allowed.is_empty() {
        let to: Vec<Value> = allowed
            .iter()
            .map(|c| json!({ "ipBlock": { "cidr": c } }))
            .collect();
        egress.push(json!({ "to": to }));
    }
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": name,
            "namespace": k8s_namespace,
            "labels": {
                "app.kubernetes.io/managed-by": "loams-flow",
                "app.kubernetes.io/component": "connector-egress"
            }
        },
        "spec": {
            "podSelector": { "matchLabels": pod_labels },
            "policyTypes": ["Egress"],
            "egress": egress
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_in_values() {
        let t = |s: &str| targets_in(s, true);
        assert_eq!(t("b1:9092,b2:9092"), [Ok("b1".into()), Ok("b2".into())]);
        assert_eq!(t("b1:9092 b2"), [Ok("b1".into()), Ok("b2".into())]);
        assert_eq!(
            targets_in("see https://a.example.com/x or mongodb://h1,h2:1/d", false),
            [Ok("a.example.com".into()), Ok("h1".into()), Ok("h2".into())]
        );
        assert_eq!(t("[::1]:5432"), [Ok("::1".into())]);
        assert_eq!(t("::1"), [Ok("::1".into())]);
        assert_eq!(
            t("mongodb://u:p@h1:27017,h2:27017/db"),
            [Ok("h1".into()), Ok("h2".into())]
        );
        assert_eq!(
            t("jdbc:postgresql://db.example.com:5432/x"),
            [Ok("db.example.com".into())]
        );
        assert!(t("file:///etc/passwd")[0].is_err());
        assert!(targets_in("just some text", false).is_empty());
    }

    #[test]
    fn numeric_hosts() {
        for h in ["127.1", "2130706433", "0x7f.0.0.1", "0x7f000001"] {
            assert!(looks_numeric(h), "{h}");
        }
        for h in ["db.example.com", "beef.cafe", "a1.example"] {
            assert!(!looks_numeric(h), "{h}");
        }
    }
}
