//! The House's catalogs behave alike (HS1 Task 10): `house_catalog_conformance!`
//! runs one suite against `MemCatalog`, `LocalCatalog` (SQLite) and, when
//! `LOAMS_HOUSE_LAKEKEEPER_URL` names one, `RestCatalog` over Lakekeeper.
//!
//! Lakekeeper needs: `LOAMS_HOUSE_LAKEKEEPER_URL` (`http://127.0.0.1:8181/catalog`),
//! `LOAMS_HOUSE_LAKEKEEPER_WAREHOUSE` (its name) and `LOAMS_HOUSE_LAKEKEEPER_ROOT`
//! (`s3://<bucket>/<prefix>`, inside the warehouse's storage profile). The
//! register test also needs the store behind that root: `LOAMS_HOUSE_S3_ENDPOINT`,
//! `_KEY`, `_SECRET` (the RustFS of Task 9's tests).

use std::collections::HashMap;
use std::sync::Arc;

use loams_house::catalog::{
    CatalogError, HouseCatalog, LakeRoot, LocalCatalog, MemCatalog, RestCatalog, RestOptions,
    register_all,
};
use loams_house_lake::spec::{NestedField, PrimitiveType, Schema, Type};
use loams_house_lake::{NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate};
use loams_store::Store;

/// A catalog under test, the root its tables go under, and a run-unique prefix
/// for its namespaces (so runs against one Lakekeeper do not meet).
struct Subject {
    catalog: Arc<dyn HouseCatalog>,
    root: LakeRoot,
    run: String,
}

fn run_id() -> String {
    format!("conf_{}", uuid::Uuid::new_v4().simple())
}

fn mem() -> Option<Subject> {
    Some(Subject {
        catalog: Arc::new(MemCatalog::in_memory()),
        root: LakeRoot::local(),
        run: run_id(),
    })
}

fn local() -> Option<Subject> {
    let dir = tempfile::tempdir().expect("tempdir").keep();
    let store = Store::from_url(
        &format!("file://{}", dir.join("bucket").display()),
        Vec::<(String, String)>::new(),
    )
    .expect("store");
    std::fs::create_dir_all(dir.join("bucket")).expect("bucket");
    let catalog =
        LocalCatalog::open(&dir.join("catalog.sqlite"), store, LakeRoot::local()).expect("opens");
    Some(Subject {
        catalog: Arc::new(catalog),
        root: LakeRoot::local(),
        run: run_id(),
    })
}

fn lakekeeper_env() -> Option<(RestOptions, LakeRoot)> {
    let uri = std::env::var("LOAMS_HOUSE_LAKEKEEPER_URL").ok()?;
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} is needed"));
    let root = LakeRoot::from_store_url(&var("LOAMS_HOUSE_LAKEKEEPER_ROOT")).expect("root");
    Some((
        RestOptions {
            uri,
            warehouse: var("LOAMS_HOUSE_LAKEKEEPER_WAREHOUSE"),
            token: None,
        },
        root,
    ))
}

async fn rest() -> Option<Subject> {
    let Some((options, root)) = lakekeeper_env() else {
        eprintln!("skipped the Lakekeeper half: LOAMS_HOUSE_LAKEKEEPER_URL is not set");
        return None;
    };
    Some(Subject {
        catalog: Arc::new(RestCatalog::connect(options).await.expect("connects")),
        root,
        run: run_id(),
    })
}

fn ns(levels: &[&str]) -> NamespaceIdent {
    NamespaceIdent::from_strs(levels).expect("namespace")
}

fn creation(subject: &Subject, name: &str) -> TableCreation {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "a", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "b", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema");
    TableCreation::builder()
        .name(name.to_string())
        .location(format!(
            "{}/conformance/{}/{name}-{}",
            subject.root,
            subject.run,
            uuid::Uuid::new_v4().simple()
        ))
        .schema(schema)
        .properties(HashMap::from([("loams.test".to_string(), "1".to_string())]))
        .build()
}

async fn namespaces_nest(subject: Subject) {
    let c = &subject.catalog;
    let root = ns(&[&subject.run]);
    let child = ns(&[&subject.run, "sales"]);
    c.create_database(&root, HashMap::new())
        .await
        .expect("root");
    c.create_database(
        &child,
        HashMap::from([("comment".to_string(), "c".to_string())]),
    )
    .await
    .expect("child");
    assert_eq!(
        c.list_databases(&root).await.expect("lists"),
        vec![child.clone()]
    );
    assert_eq!(
        c.database_properties(&child)
            .await
            .expect("props")
            .get("comment")
            .map(String::as_str),
        Some("c")
    );
    assert!(matches!(
        c.create_database(&child, HashMap::new()).await,
        Err(CatalogError::NamespaceExists(_))
    ));
    assert!(matches!(
        c.database_properties(&ns(&[&subject.run, "nope"])).await,
        Err(CatalogError::NoSuchNamespace(_))
    ));
    assert!(matches!(
        c.drop_database(&root).await,
        Err(CatalogError::NotEmpty(_))
    ));
    c.drop_database(&child).await.expect("drops child");
    assert!(c.list_databases(&root).await.expect("lists").is_empty());
    c.drop_database(&root).await.expect("drops root");
    assert!(matches!(
        c.drop_database(&root).await,
        Err(CatalogError::NoSuchNamespace(_))
    ));
}

async fn tables_live_and_move(subject: Subject) {
    let c = &subject.catalog;
    let a = ns(&[&subject.run]);
    let b = ns(&[&subject.run, "b"]);
    c.create_database(&a, HashMap::new()).await.expect("a");
    c.create_database(&b, HashMap::new()).await.expect("b");
    let made = c
        .create_table(&a, creation(&subject, "t"))
        .await
        .expect("creates");
    let t = TableIdent::new(a.clone(), "t".to_string());
    assert_eq!(made.ident, t);
    assert!(matches!(
        c.create_table(&a, creation(&subject, "t")).await,
        Err(CatalogError::TableExists(_))
    ));
    let loaded = c.load_table(&t).await.expect("loads");
    assert_eq!(loaded.metadata_location, made.metadata_location);
    assert_eq!(loaded.metadata.uuid(), made.metadata.uuid());
    let names: Vec<String> = loaded
        .metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(names, ["a", "b"]);
    assert_eq!(
        loaded
            .metadata
            .properties()
            .get("loams.test")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(c.list_tables(&a).await.expect("lists"), vec![t.clone()]);

    let moved = TableIdent::new(b.clone(), "u".to_string());
    c.rename_table(&t, &moved).await.expect("renames");
    assert!(matches!(
        c.load_table(&t).await,
        Err(CatalogError::NoSuchTable(_))
    ));
    assert_eq!(
        c.load_table(&moved).await.expect("loads").metadata.uuid(),
        made.metadata.uuid()
    );
    assert!(c.list_tables(&a).await.expect("lists").is_empty());
    assert!(matches!(
        c.rename_table(&t, &moved).await,
        Err(CatalogError::NoSuchTable(_))
    ));

    c.drop_table(&moved).await.expect("drops");
    assert!(matches!(
        c.drop_table(&moved).await,
        Err(CatalogError::NoSuchTable(_))
    ));
    assert!(matches!(
        c.load_table(&moved).await,
        Err(CatalogError::NoSuchTable(_))
    ));
    c.drop_database(&b).await.expect("drops b");
    c.drop_database(&a).await.expect("drops a");
}

async fn commits_are_compare_and_set(subject: Subject) {
    let c = &subject.catalog;
    let a = ns(&[&subject.run]);
    c.create_database(&a, HashMap::new()).await.expect("a");
    let base = c
        .create_table(&a, creation(&subject, "t"))
        .await
        .expect("creates");
    let t = base.ident.clone();

    let set = c
        .commit(
            &t,
            vec![TableRequirement::UuidMatch {
                uuid: base.metadata.uuid(),
            }],
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("k".to_string(), "v".to_string())]),
            }],
        )
        .await
        .expect("commits");
    assert_ne!(set.metadata_location, base.metadata_location);
    assert_eq!(
        set.metadata.properties().get("k").map(String::as_str),
        Some("v")
    );
    assert_eq!(
        c.load_table(&t).await.expect("loads").metadata_location,
        set.metadata_location
    );

    // Schema evolution from `set`; a second one from the same base conflicts.
    let mut fields = set.metadata.current_schema().as_struct().fields().to_vec();
    fields.push(
        NestedField::optional(
            set.metadata.last_column_id() + 1,
            "c",
            Type::Primitive(PrimitiveType::Int),
        )
        .into(),
    );
    let schema = Schema::builder()
        .with_fields(fields.clone())
        .build()
        .expect("schema");
    let evolved = c
        .update_schema(
            &set,
            schema.clone(),
            HashMap::from([("k".to_string(), "w".to_string())]),
        )
        .await
        .expect("evolves");
    let names: Vec<String> = evolved
        .metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(names, ["a", "b", "c"]);
    assert_eq!(
        evolved.metadata.properties().get("k").map(String::as_str),
        Some("w")
    );
    let mut more = fields;
    more.push(
        NestedField::optional(
            set.metadata.last_column_id() + 2,
            "d",
            Type::Primitive(PrimitiveType::Int),
        )
        .into(),
    );
    let stale = c
        .update_schema(
            &set,
            Schema::builder().with_fields(more).build().expect("schema"),
            HashMap::new(),
        )
        .await;
    assert!(
        matches!(stale, Err(CatalogError::Conflict { .. })),
        "{stale:?}"
    );
    assert_eq!(
        c.load_table(&t).await.expect("loads").metadata_location,
        evolved.metadata_location,
        "a refused commit changes nothing"
    );

    // A table forgotten and registered again from its metadata file.
    c.drop_table(&t).await.expect("forgets");
    let again = TableIdent::new(a.clone(), "again".to_string());
    let registered = c
        .register_table(&again, &evolved.metadata_location)
        .await
        .expect("registers");
    assert_eq!(registered.metadata.uuid(), base.metadata.uuid());
    assert_eq!(
        c.load_table(&again)
            .await
            .expect("loads")
            .metadata
            .current_schema()
            .as_struct()
            .fields()
            .len(),
        3
    );
    assert!(matches!(
        c.register_table(&again, &evolved.metadata_location).await,
        Err(CatalogError::TableExists(_))
    ));
    c.drop_table(&again).await.expect("drops");
    c.drop_database(&a).await.expect("drops a");
}

macro_rules! house_catalog_conformance {
    ($name:ident, $make:expr) => {
        mod $name {
            use super::*;

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn namespaces_nest_and_drop_only_when_empty() {
                if let Some(subject) = $make {
                    namespaces_nest(subject).await;
                }
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn tables_are_created_loaded_renamed_and_dropped() {
                if let Some(subject) = $make {
                    tables_live_and_move(subject).await;
                }
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn commits_are_compare_and_set_and_register_works() {
                if let Some(subject) = $make {
                    commits_are_compare_and_set(subject).await;
                }
            }
        }
    };
}

house_catalog_conformance!(mem_catalog, mem());
house_catalog_conformance!(local_catalog, local());
house_catalog_conformance!(rest_catalog, rest().await);

/// The SQLite file is the catalog: a second `LocalCatalog` on it sees the first's
/// tables, and its compare-and-set holds between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_catalog_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("bucket")).expect("bucket");
    let store = || {
        Store::from_url(
            &format!("file://{}", dir.path().join("bucket").display()),
            Vec::<(String, String)>::new(),
        )
        .expect("store")
    };
    let path = dir.path().join("catalog.sqlite");
    let first = LocalCatalog::open(&path, store(), LakeRoot::local()).expect("opens");
    let subject = Subject {
        catalog: Arc::new(MemCatalog::in_memory()),
        root: LakeRoot::local(),
        run: run_id(),
    };
    let a = ns(&["ns_1"]);
    first.create_database(&a, HashMap::new()).await.expect("a");
    let made = first
        .create_table(&a, creation(&subject, "t"))
        .await
        .expect("creates");
    let second = LocalCatalog::open(&path, store(), LakeRoot::local()).expect("reopens");
    let seen = second.load_table(&made.ident).await.expect("loads");
    assert_eq!(seen.metadata_location, made.metadata_location);
    let committed = second
        .commit(
            &made.ident,
            Vec::new(),
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("x".to_string(), "1".to_string())]),
            }],
        )
        .await
        .expect("commits");
    assert_eq!(
        first
            .load_table(&made.ident)
            .await
            .expect("loads")
            .metadata_location,
        committed.metadata_location,
        "both see one pointer"
    );
    assert!(committed.metadata.properties().contains_key("x"));
}

/// Moving a `--single-node` House to Lakekeeper: every table of the local catalog
/// is registered with its current metadata file (§49 §17). Without Lakekeeper the
/// target is a second catalog over the same store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_catalog_tables_register_into_lakekeeper() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s3 = std::env::var("LOAMS_HOUSE_S3_ENDPOINT").ok();
    let (store, root, target): (Store, LakeRoot, Arc<dyn HouseCatalog>) = match (
        lakekeeper_env(),
        s3,
    ) {
        (Some((options, root)), Some(endpoint)) => {
            let var =
                |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} is needed"));
            let store = Store::from_url(
                &root.to_string(),
                vec![
                    ("aws_endpoint".to_string(), endpoint),
                    ("aws_access_key_id".to_string(), var("LOAMS_HOUSE_S3_KEY")),
                    (
                        "aws_secret_access_key".to_string(),
                        var("LOAMS_HOUSE_S3_SECRET"),
                    ),
                    ("aws_region".to_string(), "us-east-1".to_string()),
                    ("aws_allow_http".to_string(), "true".to_string()),
                    (
                        "aws_virtual_hosted_style_request".to_string(),
                        "false".to_string(),
                    ),
                ],
            )
            .expect("store");
            let rest = RestCatalog::connect(options).await.expect("connects");
            (store, root, Arc::new(rest))
        }
        _ => {
            eprintln!(
                "the Lakekeeper half needs LOAMS_HOUSE_LAKEKEEPER_* and LOAMS_HOUSE_S3_*; registering into a second catalog"
            );
            let store = Store::in_memory();
            let target = MemCatalog::new(store.clone(), LakeRoot::local());
            (store, LakeRoot::local(), Arc::new(target))
        }
    };
    let local =
        LocalCatalog::open(&dir.path().join("catalog.sqlite"), store, root.clone()).expect("opens");
    // A namespace number no other run uses, so Lakekeeper runs do not meet.
    let namespace = u64::from(uuid::Uuid::new_v4().as_fields().0);
    let subject = Subject {
        catalog: Arc::new(MemCatalog::in_memory()),
        root: root.clone(),
        run: format!("ns/{namespace}/house"),
    };
    let top = ns(&[&format!("ns_{namespace}")]);
    let sales = ns(&[&format!("ns_{namespace}"), "sales"]);
    local
        .create_database(&top, HashMap::new())
        .await
        .expect("top");
    local
        .create_database(
            &sales,
            HashMap::from([("comment".to_string(), "s".to_string())]),
        )
        .await
        .expect("sales");
    let t1 = local
        .create_table(&top, creation(&subject, "t1"))
        .await
        .expect("t1");
    let t2 = local
        .create_table(&sales, creation(&subject, "t2"))
        .await
        .expect("t2");
    let t2 = local
        .commit(
            &t2.ident,
            Vec::new(),
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([("v".to_string(), "2".to_string())]),
            }],
        )
        .await
        .expect("commits");

    let registered = register_all(&local, target.as_ref(), namespace)
        .await
        .expect("registers");
    assert_eq!(registered, 2);
    for table in [&t1, &t2] {
        let there = target.load_table(&table.ident).await.expect("registered");
        assert_eq!(there.metadata.uuid(), table.metadata.uuid());
        assert_eq!(there.metadata_location, table.metadata_location);
    }
    assert_eq!(
        target
            .database_properties(&sales)
            .await
            .expect("props")
            .get("comment")
            .map(String::as_str),
        Some("s")
    );
    // Again: nothing more to register.
    assert_eq!(
        register_all(&local, target.as_ref(), namespace)
            .await
            .expect("again"),
        0
    );
    // Clean up what a shared Lakekeeper keeps.
    for table in [&t1, &t2] {
        let _ = target.drop_table(&table.ident).await;
    }
    let _ = target.drop_database(&sales).await;
    let _ = target.drop_database(&top).await;
}
