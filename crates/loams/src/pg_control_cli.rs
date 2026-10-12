//! `loams pg-control` and `loams pg-api-key` (PG2 Task 9): the flags of the
//! `pg-control` role, turned into a [`PgControlConfig`].

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use loams::MetaBackend;
use loams::pg_control::{
    ComputesConfig, PgAuth, PgControl, PgControlConfig, PgRuntime, PgStore, read_token,
};
use loams_pg_control::authz::{Actor, Grant, GrantObject, KeysFile, PrincipalKind, Relation};
use loams_pg_control::runtime::compose::CtlReach;
use loams_pg_control::runtime::images;
use url::Url;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum StoreArg {
    /// One redb file in --data-dir (single node).
    Local,
    /// TiKV, at --meta.
    Tikv,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum RuntimeArg {
    /// compose; secrets in an age-encrypted file in --data-dir.
    Compose,
    /// Kubernetes; secrets as Secrets in --kube-namespace.
    Kubernetes,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum CtlReachArg {
    /// pg-control runs on the host: each compute publishes compute_ctl on a
    /// loopback port.
    Host,
    /// pg-control runs on the compose network: http://<service>:3080.
    Network,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum AuthzArg {
    /// One principal with every relation; --listen must be loopback.
    Dev,
    /// API keys and their grants, from --pg-api-keys.
    ApiKey,
    /// OpenFGA (MT1; not built yet).
    Openfga,
}

#[derive(Debug, clap::Args)]
pub(crate) struct PgControlArgs {
    /// Where the records live.
    #[arg(long, value_enum, default_value = "local")]
    store: StoreArg,
    /// With --store tikv: tikv://<pd-host:port>[,<pd…>]/<keyspace>[?root=<hex>]
    /// (the metastore's keyspace).
    #[arg(long, value_parser = MetaBackend::parse)]
    meta: Option<MetaBackend>,
    /// Where computes run (and secrets live).
    #[arg(long, value_enum, default_value = "compose")]
    runtime: RuntimeArg,
    /// With --runtime compose: the storage stack's network computes join
    /// (loams-neon_default for deploy/loams-postgres-dev).
    #[arg(long)]
    compose_network: Option<String>,
    /// With --runtime compose: where the compose files live (default
    /// <data-dir>/compose).
    #[arg(long)]
    compose_dir: Option<PathBuf>,
    /// The upcall listener as computes reach it (default
    /// http://host.docker.internal:<port>, or host.containers.internal under
    /// Podman); --pg-upcall-listen must be an address they reach.
    #[arg(long)]
    compute_control_plane_uri: Option<String>,
    /// `<host>:<ip>` added to every compute's /etc/hosts (repeated).
    #[arg(long = "compose-extra-host")]
    compose_extra_host: Vec<String>,
    /// Where endpoints' ports are published.
    #[arg(long, default_value = "127.0.0.1")]
    compose_publish_ip: std::net::IpAddr,
    /// How pg-control reaches each compute's compute_ctl.
    #[arg(long, value_enum, default_value = "host")]
    compute_ctl_reach: CtlReachArg,
    /// `<major>=<repository>@sha256:<digest>`: the compute image of a
    /// Postgres major, over deploy/pins/loams-postgres-images.toml's
    /// (repeated).
    #[arg(long = "compute-image", value_parser = parse_image)]
    compute_image: Vec<(u32, String)>,
    /// With --runtime kubernetes: the namespace of the Secrets.
    #[arg(long, default_value = "loams-postgres")]
    kube_namespace: String,
    /// Holds the local store and the secret file.
    #[arg(long, default_value = ".loams/pg-control")]
    data_dir: PathBuf,
    /// The API listener (`loams.postgres.v1`).
    #[arg(long, default_value = "127.0.0.1:7680")]
    listen: SocketAddr,
    /// The upcall listener Neon's components call; never the API's.
    #[arg(long, default_value = "127.0.0.1:7690")]
    pg_upcall_listen: SocketAddr,
    /// Who may call.
    #[arg(long, value_enum, default_value = "dev")]
    pg_authz: AuthzArg,
    /// With --pg-authz api-key: the keys file (`loams pg-api-key` makes entries).
    #[arg(long)]
    pg_api_keys: Option<PathBuf>,
    /// Serve grpc.reflection.v1 on --listen.
    #[arg(long)]
    reflection: bool,
    /// A pageserver's management API.
    #[arg(long, default_value = "http://127.0.0.1:9898")]
    pageserver: Url,
    /// The storage controller; every storage call then goes through it.
    #[arg(long)]
    storage_controller: Option<Url>,
    /// loams-wal's HTTP API.
    #[arg(long, default_value = "http://127.0.0.1:7676")]
    wal: Url,
    /// `<node id>=<libpq URL>` of a pageserver computes read from (repeated).
    #[arg(long = "pageserver-pg", value_parser = parse_pageserver)]
    pageserver_pg: Vec<(u64, String)>,
    /// The pageserver node of a branch no notify-attach placed.
    #[arg(long)]
    default_pageserver: Option<u64>,
    /// `<pool>=<connstr>[,<connstr>…]`: a loams-wal pool computes write to
    /// (repeated).
    #[arg(long = "wal-pool", value_parser = parse_pool)]
    wal_pool: Vec<(String, Vec<String>)>,
    /// A file holding the storage controller's token; without it
    /// /notify-attach is not served.
    #[arg(long)]
    storage_controller_token_file: Option<PathBuf>,
}

fn parse_pageserver(s: &str) -> Result<(u64, String), String> {
    let (id, url) = s
        .split_once('=')
        .ok_or_else(|| format!("--pageserver-pg {s:?}: expected <node id>=<libpq URL>"))?;
    let id = id
        .parse()
        .map_err(|_| format!("--pageserver-pg {s:?}: the node id is a number"))?;
    if url.is_empty() {
        return Err(format!("--pageserver-pg {s:?}: the URL is empty"));
    }
    Ok((id, url.to_owned()))
}

fn parse_image(s: &str) -> Result<(u32, String), String> {
    images::parse_override(s).map_err(|e| format!("--compute-image: {e}"))
}

fn parse_pool(s: &str) -> Result<(String, Vec<String>), String> {
    let (name, list) = s
        .split_once('=')
        .ok_or_else(|| format!("--wal-pool {s:?}: expected <pool>=<connstr>[,<connstr>…]"))?;
    let conns: Vec<String> = list
        .split(',')
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(str::to_owned)
        .collect();
    if name.is_empty() || conns.is_empty() {
        return Err(format!(
            "--wal-pool {s:?}: a name and at least one connection string"
        ));
    }
    Ok((name.to_owned(), conns))
}

fn config(args: PgControlArgs) -> Result<PgControlConfig, String> {
    let mut config = PgControlConfig::new(args.data_dir);
    config.store = match (args.store, args.meta) {
        (StoreArg::Local, None) => PgStore::Local,
        (StoreArg::Local, Some(_)) => return Err("--meta goes with --store tikv".into()),
        (StoreArg::Tikv, None) => return Err("--store tikv needs --meta tikv://…".into()),
        #[cfg(feature = "tikv")]
        (StoreArg::Tikv, Some(MetaBackend::Tikv(meta))) => PgStore::Tikv(meta.tikv),
        #[allow(unreachable_patterns)]
        (StoreArg::Tikv, Some(_)) => return Err(loams::NO_TIKV_FEATURE.into()),
    };
    config.runtime = match args.runtime {
        RuntimeArg::Compose => PgRuntime::Compose,
        RuntimeArg::Kubernetes => PgRuntime::Kubernetes {
            namespace: args.kube_namespace,
        },
    };
    config.computes = Some(ComputesConfig {
        compose_bin: None,
        compose_dir: args.compose_dir,
        network: args.compose_network,
        control_plane_uri: args.compute_control_plane_uri,
        extra_hosts: args.compose_extra_host,
        publish_ip: args.compose_publish_ip,
        ctl_reach: match args.compute_ctl_reach {
            CtlReachArg::Host => CtlReach::Host,
            CtlReachArg::Network => CtlReach::Network,
        },
        image_overrides: args.compute_image.into_iter().collect(),
    });
    config.listen = args.listen;
    config.upcall_listen = args.pg_upcall_listen;
    config.auth = match (args.pg_authz, args.pg_api_keys) {
        (AuthzArg::Dev, None) => PgAuth::Dev,
        (AuthzArg::ApiKey, Some(path)) => PgAuth::ApiKeys(path),
        (AuthzArg::ApiKey, None) => return Err("--pg-authz api-key needs --pg-api-keys".into()),
        (AuthzArg::Openfga, None) => PgAuth::OpenFga,
        (_, Some(_)) => return Err("--pg-api-keys goes with --pg-authz api-key".into()),
    };
    config.reflection = args.reflection;
    config.pageserver = args.pageserver;
    config.storage_controller = args.storage_controller;
    config.wal = args.wal;
    config.pageservers = args.pageserver_pg.into_iter().collect::<BTreeMap<_, _>>();
    config.default_pageserver = args.default_pageserver;
    config.wal_pools = args.wal_pool.into_iter().collect::<BTreeMap<_, _>>();
    config.notify_token = args
        .storage_controller_token_file
        .as_deref()
        .map(read_token)
        .transpose()?;
    Ok(config)
}

/// `loams pg-control`: runs until a shutdown signal.
pub(crate) async fn run(args: PgControlArgs) -> ExitCode {
    let config = match config(args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("loams pg-control: {e}");
            return ExitCode::FAILURE;
        }
    };
    let control = match PgControl::start(config).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("loams pg-control: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "loams pg-control upcalls listening on http://{}",
        control.upcall_addr()
    );
    println!(
        "loams pg-control listening on http://{}",
        control.api_addr()
    );
    super::shutdown_signal().await;
    tracing::info!("shutting down");
    control.shutdown().await;
    ExitCode::SUCCESS
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum KindArg {
    User,
    Agent,
    ServiceAccount,
}

#[derive(Debug, clap::Args)]
pub(crate) struct PgApiKeyArgs {
    /// Whom the key authenticates.
    #[arg(long)]
    subject: String,
    #[arg(long, value_enum, default_value = "user")]
    kind: KindArg,
    /// `<kind>:<subject>` of an actor acting for the subject, such as
    /// `agent:release-bot` (repeated, nearest first).
    #[arg(long = "act", value_parser = parse_actor)]
    act: Vec<Actor>,
    /// `<relation>:namespace:<ns>` or `<relation>:project:<prj-…>`, the
    /// relation one of viewer, editor, admin, connect (repeated).
    #[arg(long = "grant", value_parser = parse_grant)]
    grant: Vec<Grant>,
}

fn kind(s: &str) -> Option<PrincipalKind> {
    match s {
        "user" => Some(PrincipalKind::User),
        "agent" => Some(PrincipalKind::Agent),
        "service_account" | "service-account" => Some(PrincipalKind::ServiceAccount),
        _ => None,
    }
}

fn parse_actor(s: &str) -> Result<Actor, String> {
    let (k, subject) = s
        .split_once(':')
        .ok_or_else(|| format!("--act {s:?}: expected <kind>:<subject>"))?;
    let kind =
        kind(k).ok_or_else(|| format!("--act {s:?}: kind is user, agent or service-account"))?;
    if subject.is_empty() {
        return Err(format!("--act {s:?}: the subject is empty"));
    }
    Ok(Actor {
        subject: subject.to_owned(),
        kind,
    })
}

fn parse_grant(s: &str) -> Result<Grant, String> {
    let mut parts = s.splitn(3, ':');
    let (Some(relation), Some(on), Some(name)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(format!(
            "--grant {s:?}: expected <relation>:namespace:<ns> or <relation>:project:<prj-…>"
        ));
    };
    let relation = match relation {
        "viewer" => Relation::Viewer,
        "editor" => Relation::Editor,
        "admin" => Relation::Admin,
        "connect" => Relation::Connect,
        _ => {
            return Err(format!(
                "--grant {s:?}: relation is viewer, editor, admin or connect"
            ));
        }
    };
    let on = match on {
        "namespace" => GrantObject::Namespace(name.to_owned()),
        "project" => GrantObject::Project(name.to_owned()),
        _ => return Err(format!("--grant {s:?}: on namespace or project")),
    };
    Ok(Grant { relation, on })
}

/// `loams pg-api-key`: one JSON object, `{"api_key": …, "entry": …}`.
pub(crate) fn mint(args: &PgApiKeyArgs) -> ExitCode {
    let kind = match args.kind {
        KindArg::User => PrincipalKind::User,
        KindArg::Agent => PrincipalKind::Agent,
        KindArg::ServiceAccount => PrincipalKind::ServiceAccount,
    };
    let minted = loams_pg_control::authz::mint_api_key(
        &args.subject,
        kind,
        args.act.clone(),
        args.grant.clone(),
    );
    let (key, entry) = match minted {
        Ok(m) => m,
        Err(e) => {
            eprintln!("loams pg-api-key: {e}");
            return ExitCode::FAILURE;
        }
    };
    // The entry must load: no admin for an agent, well-formed grants.
    let file = KeysFile {
        keys: vec![entry.clone()],
    };
    let checked = serde_json::to_string(&file)
        .map_err(|e| e.to_string())
        .and_then(|text| KeysFile::parse(&text).map_err(|e| e.to_string()));
    if let Err(e) = checked {
        eprintln!("loams pg-api-key: {e}");
        return ExitCode::FAILURE;
    }
    let out = serde_json::json!({ "api_key": key.expose(), "entry": entry });
    println!("{out}");
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_and_pools_parse() {
        let g = parse_grant("editor:namespace:acme").expect("a grant");
        assert_eq!(g.relation, Relation::Editor);
        assert_eq!(g.on, GrantObject::Namespace("acme".into()));
        assert!(parse_grant("owner:namespace:acme").is_err());
        assert!(parse_grant("editor:acme").is_err());
        let a = parse_actor("agent:bot").expect("an actor");
        assert_eq!(a.kind, PrincipalKind::Agent);
        assert_eq!(
            parse_pool("default=postgresql://wal-0:5454,postgresql://wal-1:5454").expect("a pool"),
            (
                "default".into(),
                vec![
                    "postgresql://wal-0:5454".into(),
                    "postgresql://wal-1:5454".into()
                ]
            )
        );
        assert!(parse_pool("default=").is_err());
        assert_eq!(
            parse_pageserver("1=postgresql://ps:6400").expect("a pageserver"),
            (1, "postgresql://ps:6400".into())
        );
        let digest = "sha256:13ab146d3e7bbabb25a8532f315ac443e7512351d1ede0bab586def5c70e26c3";
        let image = format!("ghcr.io/ostrium-labs/loams-postgres-compute-v17@{digest}");
        assert_eq!(
            parse_image(&format!("17={image}")).expect("an image"),
            (17, image)
        );
        assert!(parse_image("17=ghcr.io/x/compute:latest").is_err());
        assert!(
            parse_image(&format!(
                "17=ghcr.io/ostrium-labs/neon-archive/compute-node-v17@{digest}"
            ))
            .is_err()
        );
    }
}
