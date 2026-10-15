//! Connector metrics, alerts and the activity observer (CN2 plan Task 24, Q359).
//!
//! The activity counters are open observability: plain structs handed to a
//! `ConnectorActivityObserver`, with no wire format, buffer or persistence
//! (D548–D552). These tests hold that line, bound the metric labels, and prove
//! every alert in `deploy/observability/connectors/alerts.yaml` fires in a
//! promtool rule test.
// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::testing::reference::{
    CounterConfig, CounterEnumerator, CounterFactory, CounterHandle,
};
use loams_connector_sdk::{
    ConnectorObserver, ErrorClass, InstanceKey, LastError, Pipeline, PipelineConfig, PipelineState,
    PipelineStatus, Scrubber, Secret, SplitId,
};
use loams_flow::activity::{
    ActivityPump, ConnectorActivity, ConnectorActivityObserver, DEFAULT_ACTIVITY_WINDOW,
};
use loams_flow::manifest::repository_root;
use loams_flow::metrics::{
    ConnectorMetrics, FallbackMap, InstanceLabels, LabelError, MAX_SPLIT_LABELS, METRIC_FAMILIES,
    OVERFLOW_SPLIT, SplitLabels,
};
use loams_flow::{Direction, RuntimeKind};

// ------------------------------------------------------------------ helpers --

fn labels(instance: &str, direction: Direction) -> InstanceLabels {
    InstanceLabels::new(
        7,
        instance,
        "postgresql",
        RuntimeKind::Native,
        false,
        direction,
    )
    .unwrap()
}

fn key(instance: &str) -> InstanceKey {
    InstanceKey::new(7, instance)
}

/// One sample of the text exposition: its family, labels and value.
#[derive(Debug, Clone)]
struct Sample {
    name: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

fn samples(text: &str) -> Vec<Sample> {
    let mut out = Vec::new();
    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (series, value) = line.rsplit_once(' ').unwrap();
        let (name, labels) = match series.split_once('{') {
            Some((n, rest)) => (n, rest.strip_suffix('}').unwrap()),
            None => (series, ""),
        };
        let mut map = BTreeMap::new();
        if !labels.is_empty() {
            for pair in labels.split("\",") {
                let (k, v) = pair.split_once("=\"").unwrap();
                map.insert(k.to_owned(), v.trim_end_matches('"').to_owned());
            }
        }
        out.push(Sample {
            name: name.to_owned(),
            labels: map,
            value: value.parse().unwrap(),
        });
    }
    out
}

fn family<'a>(all: &'a [Sample], name: &'a str) -> impl Iterator<Item = &'a Sample> + 'a {
    all.iter().filter(move |s| s.name == name)
}

/// Records every activity record it is handed.
#[derive(Default)]
struct Collect(Mutex<Vec<ConnectorActivity>>);

impl ConnectorActivityObserver for Collect {
    fn on_activity(&self, a: &ConnectorActivity) {
        self.0.lock().unwrap().push(a.clone());
    }
}

impl Collect {
    fn take(&self) -> Vec<ConnectorActivity> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

fn observability_dir() -> PathBuf {
    repository_root().join("deploy/observability/connectors")
}

// -------------------------------------------------------------------- tests --

#[test]
fn metrics_labels_bounded() {
    let metrics = ConnectorMetrics::new();

    // A source with bounded splits (Kafka partitions, Kinesis shards) carries
    // a `split` label, capped at MAX_SPLIT_LABELS per instance; the rest fold
    // into one overflow series and nothing is lost.
    let orders = metrics.register(labels("orders", Direction::Source), SplitLabels::Bounded);
    for i in 0..3_000u64 {
        let split = SplitId::new(format!("orders/{i}"));
        orders.source_batch(&key("orders"), &split, 1, 10);
        orders.lag(&key("orders"), Some(&split), Duration::from_secs(i % 7));
        orders.position_risk(&key("orders"), Some(&split), 0.25);
    }
    let all = samples(&metrics.render());
    for name in [
        "loams_flow_connector_events_total",
        "loams_flow_connector_lag_seconds",
        "loams_flow_cdc_position_risk_ratio",
    ] {
        let splits: BTreeSet<&str> = family(&all, name)
            .filter(|s| s.labels["instance"] == "orders")
            .map(|s| s.labels["split"].as_str())
            .collect();
        assert!(
            splits.len() <= MAX_SPLIT_LABELS + 1,
            "{name}: {} split label values",
            splits.len()
        );
        assert!(
            splits.contains(OVERFLOW_SPLIT),
            "{name}: no overflow series"
        );
    }
    let events: f64 = family(&all, "loams_flow_connector_events_total")
        .filter(|s| s.labels["instance"] == "orders")
        .map(|s| s.value)
        .sum();
    assert_eq!(events, 3_000.0, "the overflow series keeps every event");
    let bytes_in: f64 = family(&all, "loams_flow_connector_bytes_total")
        .filter(|s| s.labels["instance"] == "orders" && s.labels["dir"] == "in")
        .map(|s| s.value)
        .sum();
    assert_eq!(bytes_in, 30_000.0);

    // An instance whose splits are unbounded (object prefixes, key ranges)
    // never carries a `split` label.
    let objects = metrics.register(labels("objects", Direction::Source), SplitLabels::Unbounded);
    for i in 0..100u64 {
        objects.source_batch(&key("objects"), &SplitId::new(format!("prefix-{i}")), 1, 1);
    }
    let all = samples(&metrics.render());
    assert!(
        all.iter()
            .filter(|s| s.labels.get("instance").map(String::as_str) == Some("objects"))
            .all(|s| !s.labels.contains_key("split"))
    );

    // Free user strings are refused as label values.
    for bad in [
        "my orders",
        "orders\"}",
        "x\ny",
        "",
        &"a".repeat(200),
        "ünïcode",
    ] {
        assert!(
            matches!(
                InstanceLabels::new(
                    7,
                    bad,
                    "postgresql",
                    RuntimeKind::Native,
                    false,
                    Direction::Sink
                ),
                Err(LabelError::Invalid { .. })
            ),
            "instance {bad:?} accepted"
        );
        assert!(
            InstanceLabels::new(
                7,
                "orders",
                bad,
                RuntimeKind::Native,
                false,
                Direction::Sink
            )
            .is_err(),
            "connector {bad:?} accepted"
        );
    }

    // A split id that is not a label-safe string, or that holds a resolved
    // secret, never reaches a label.
    let scrubber = Scrubber::new();
    scrubber.add(&Secret::new("hunter2-password"));
    let leaky = metrics.register_scrubbed(
        labels("leaky", Direction::Source),
        SplitLabels::Bounded,
        scrubber,
    );
    leaky.source_batch(&key("leaky"), &SplitId::new("db/hunter2-password"), 1, 1);
    leaky.source_batch(&key("leaky"), &SplitId::new("free text split"), 1, 1);
    let text = metrics.render();
    assert!(
        !text.contains("hunter2-password"),
        "a secret reached a label"
    );
    assert!(!text.contains("free text split"));

    // Error classes are the SDK's fixed set; every label key is one of the
    // declared ones.
    orders.retry(
        &key("orders"),
        &ErrorClass::Throttled {
            after: Duration::from_secs(1),
        },
        1,
        Duration::from_secs(1),
    );
    orders.error(&key("orders"), &ErrorClass::Auth, "credentials refused");
    orders.dead_letter(&key("orders"));
    let all = samples(&metrics.render());
    let classes: BTreeSet<&str> = family(&all, "loams_flow_connector_errors_total")
        .map(|s| s.labels["class"].as_str())
        .collect();
    let known: BTreeSet<&str> = [
        "retryable",
        "throttled",
        "poison",
        "auth",
        "schema_incompatible",
        "position_lost",
        "fatal",
    ]
    .into();
    assert!(classes.is_subset(&known), "{classes:?}");
    assert!(
        classes.contains("throttled") && classes.contains("auth") && classes.contains("poison")
    );
    let allowed_keys: BTreeSet<&str> = [
        "namespace",
        "instance",
        "connector",
        "runtime",
        "fallback",
        "direction",
        "split",
        "dir",
        "class",
    ]
    .into();
    for s in &all {
        assert!(
            METRIC_FAMILIES.contains(&s.name.as_str()),
            "undeclared family {}",
            s.name
        );
        for k in s.labels.keys() {
            assert!(allowed_keys.contains(k.as_str()), "{}: label {k}", s.name);
        }
        assert!(!s.labels.values().any(|v| v.contains("credentials")));
    }

    // A deregistered instance's series go away.
    metrics.deregister(&labels("objects", Direction::Source));
    assert!(!metrics.render().contains("instance=\"objects\""));
}

#[tokio::test(start_paused = true)]
async fn activity_observer_called_per_window() {
    assert_eq!(DEFAULT_ACTIVITY_WINDOW, Duration::from_secs(60));
    let metrics = ConnectorMetrics::new();
    let src = metrics.register(labels("orders", Direction::Source), SplitLabels::Unbounded);
    let sink = metrics.register(
        InstanceLabels::new(
            7,
            "search",
            "elasticsearch",
            RuntimeKind::Iggy,
            true,
            Direction::Sink,
        )
        .unwrap(),
        SplitLabels::Unbounded,
    );
    let seen = Arc::new(Collect::default());
    let pump = ActivityPump::new(metrics.clone(), seen.clone());
    let task = pump.spawn(DEFAULT_ACTIVITY_WINDOW);

    // Window 1: activity on both instances.
    src.source_batch(&key("orders"), &SplitId::new("0"), 10, 1_000);
    src.api_call(&key("orders"));
    src.api_call(&key("orders"));
    sink.sink_batch(&InstanceKey::new(7, "search"), 4, 400);
    sink.api_call(&InstanceKey::new(7, "search"));
    tokio::time::sleep(DEFAULT_ACTIVITY_WINDOW + Duration::from_millis(1)).await;
    let w1 = seen.take();
    assert_eq!(w1.len(), 2, "one record per instance per window: {w1:?}");
    let o = w1.iter().find(|a| a.instance == "orders").unwrap();
    assert_eq!(
        (
            o.namespace,
            o.connector.as_str(),
            o.events,
            o.bytes_in,
            o.bytes_out,
            o.api_calls
        ),
        (7, "postgresql", 10, 1_000, 0, 2)
    );
    assert_eq!(
        (o.runtime, o.fallback, o.direction),
        (RuntimeKind::Native, false, Direction::Source)
    );
    let s = w1.iter().find(|a| a.instance == "search").unwrap();
    assert_eq!(
        (s.events, s.bytes_in, s.bytes_out, s.api_calls),
        (4, 0, 400, 1)
    );
    assert_eq!(
        (s.runtime, s.fallback, s.direction),
        (RuntimeKind::Iggy, true, Direction::Sink)
    );
    assert!(o.window_end <= SystemTime::now());

    // Window 2: only the source moves; the record carries the window's delta,
    // and an idle instance still gets its (zero) record.
    src.source_batch(&key("orders"), &SplitId::new("0"), 5, 50);
    tokio::time::sleep(DEFAULT_ACTIVITY_WINDOW).await;
    let w2 = seen.take();
    assert_eq!(w2.len(), 2, "{w2:?}");
    let o = w2.iter().find(|a| a.instance == "orders").unwrap();
    assert_eq!((o.events, o.bytes_in, o.api_calls), (5, 50, 0));
    let s = w2.iter().find(|a| a.instance == "search").unwrap();
    assert_eq!((s.events, s.bytes_out, s.api_calls), (0, 0, 0));

    // A deregistered instance reports its last partial window once, then
    // nothing.
    src.source_batch(&key("orders"), &SplitId::new("0"), 3, 30);
    metrics.deregister(&labels("orders", Direction::Source));
    tokio::time::sleep(DEFAULT_ACTIVITY_WINDOW).await;
    let w3 = seen.take();
    let o = w3.iter().find(|a| a.instance == "orders").unwrap();
    assert_eq!(o.events, 3);
    tokio::time::sleep(DEFAULT_ACTIVITY_WINDOW).await;
    let w4 = seen.take();
    assert_eq!(w4.len(), 1, "{w4:?}");
    assert_eq!(w4[0].instance, "search");
    task.abort();

    // `tick` is the same step, callable directly by a supervisor that owns
    // its own timer.
    let pump = ActivityPump::new(metrics.clone(), seen.clone());
    sink.sink_batch(&InstanceKey::new(7, "search"), 1, 1);
    let end = SystemTime::now();
    pump.tick(end);
    let t = seen.take();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].window_end, end);
}

#[tokio::test(start_paused = true)]
async fn activity_observer_fed_by_the_sdk_pipeline() {
    // The SDK's ConnectorObserver hook is what feeds the counters for a
    // native connector: run the reference source through a real Pipeline.
    let metrics = ConnectorMetrics::new();
    let observer = metrics.register(labels("orders", Direction::Source), SplitLabels::Bounded);
    let handle = CounterHandle::new(&["a", "b"]);
    let fabric = MemoryFabric::new();
    let inst = key("orders");
    let cfg = PipelineConfig {
        max_batch_rows: 10,
        max_in_flight_batches: 1,
        checkpoint_interval: Duration::ZERO,
        linger: Duration::from_millis(10),
        ..PipelineConfig::default()
    };
    // The Fabric is held: sources fill the one in-flight batch and then wait
    // on the budget, which is the backpressure the metric counts.
    fabric.hold();
    let p = Pipeline::source(inst.clone(), cfg)
        .enumerator(Box::new(CounterEnumerator::new(handle.clone())))
        .sources(Arc::new(CounterFactory::new(
            handle.clone(),
            CounterConfig {
                events_per_split: Some(50),
                payload_bytes: 8,
                ignore_budget: false,
            },
            &inst,
        )))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .discover_every(Duration::from_secs(3600))
        .observer(observer.clone())
        .start()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    fabric.release();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while fabric.len() < 100 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the pipeline stalled"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let status = p.stop().await;
    assert_eq!(status.state, PipelineState::Stopped);

    let all = samples(&metrics.render());
    let events: f64 = family(&all, "loams_flow_connector_events_total")
        .map(|s| s.value)
        .sum();
    assert_eq!(events, 100.0);
    let api: f64 = family(&all, "loams_flow_connector_api_calls_total")
        .map(|s| s.value)
        .sum();
    assert!(api >= 10.0, "{api}");
    let waited: f64 = family(&all, "loams_flow_connector_budget_wait_seconds_total")
        .map(|s| s.value)
        .sum();
    assert!(
        waited >= 4.0,
        "sources paused by backpressure for {waited}s"
    );

    let seen = Arc::new(Collect::default());
    ActivityPump::new(metrics.clone(), seen.clone()).tick(SystemTime::now());
    let a = seen.take();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].events, 100);
    assert!(a[0].bytes_in > 0);
}

#[test]
fn instance_status_carries_lag_restarts_and_runtime_choice() {
    let metrics = ConnectorMetrics::new();
    let m = metrics.register(
        InstanceLabels::new(
            3,
            "cdc",
            "postgresql",
            RuntimeKind::Debezium,
            true,
            Direction::Source,
        )
        .unwrap(),
        SplitLabels::Unbounded,
    );
    m.restarted();
    m.restarted();
    m.lag(&InstanceKey::new(3, "cdc"), None, Duration::from_secs(42));
    let failed = PipelineStatus {
        state: PipelineState::Failed,
        last_error: Some(LastError {
            class: ErrorClass::PositionLost,
            message: "slot dropped".into(),
            hint: Some("re-snapshot".into()),
        }),
        splits: vec![],
    };
    let st = m.status(&failed);
    assert_eq!(st.restarts, 2);
    assert_eq!(st.lag_seconds, Some(42.0));
    assert_eq!(st.runtime_choice.kind, RuntimeKind::Debezium);
    assert!(st.runtime_choice.fallback);
    assert_eq!(
        st.last_error.as_ref().unwrap().class,
        ErrorClass::PositionLost
    );
    assert_eq!(st.state, PipelineState::Failed);
    // `status` also sets the instance's up gauge the "instance down" alert
    // reads.
    let all = samples(&metrics.render());
    let up = family(&all, "loams_flow_connector_up").next().unwrap();
    assert_eq!(up.value, 0.0);
    let restarts = family(&all, "loams_flow_connector_restarts_total")
        .next()
        .unwrap();
    assert_eq!(restarts.value, 2.0);
    let lag_alert = family(&all, "loams_flow_connector_lag_alert_seconds")
        .next()
        .unwrap();
    assert_eq!(lag_alert.value, 300.0, "lag_alert_seconds defaults to 300");
    m.set_lag_alert(Duration::from_secs(60));
    m.status(&PipelineStatus {
        state: PipelineState::Running,
        last_error: None,
        splits: vec![],
    });
    let all = samples(&metrics.render());
    assert_eq!(
        family(&all, "loams_flow_connector_up")
            .next()
            .unwrap()
            .value,
        1.0
    );
    assert_eq!(
        family(&all, "loams_flow_connector_lag_alert_seconds")
            .next()
            .unwrap()
            .value,
        60.0
    );
}

#[test]
fn fallback_scrape_feeds_counters_and_activity() {
    // A fallback runtime's own metrics are scraped and folded into the same
    // families, so the activity observer covers fallbacks too.
    let metrics = ConnectorMetrics::new();
    let camel = metrics.register(
        InstanceLabels::new(
            7,
            "jms-in",
            "jms",
            RuntimeKind::Camel,
            true,
            Direction::Source,
        )
        .unwrap(),
        SplitLabels::Unbounded,
    );
    let route = "loams-7-jms-in-source";
    let scrape = |n: u64, failed: u64| {
        format!(
            "# TYPE camel_exchanges_total counter\n\
             camel_exchanges_total{{camelContext=\"loams\",routeId=\"{route}\"}} {n}\n\
             camel_exchanges_total{{camelContext=\"loams\",routeId=\"other\"}} 999\n\
             camel_exchanges_failed_total{{routeId=\"{route}\"}} {failed}\n"
        )
    };
    let map = FallbackMap::camel();
    camel.apply_scrape(&scrape(10, 1), &map, &[("routeId", route)]);
    camel.apply_scrape(&scrape(25, 1), &map, &[("routeId", route)]);
    // A counter reset (the pod restarted) counts from zero again, not backwards.
    camel.apply_scrape(&scrape(4, 0), &map, &[("routeId", route)]);
    let all = samples(&metrics.render());
    let events = family(&all, "loams_flow_connector_events_total")
        .next()
        .unwrap();
    assert_eq!(events.value, 29.0);
    assert_eq!(events.labels["runtime"], "camel");
    assert_eq!(events.labels["fallback"], "true");
    let seen = Arc::new(Collect::default());
    ActivityPump::new(metrics.clone(), seen.clone()).tick(SystemTime::now());
    assert_eq!(seen.take()[0].events, 29);
    assert!(!FallbackMap::debezium().events.is_empty());
}

#[test]
fn activity_observer_has_no_wire_format() {
    // Autoderef probe: the inherent method exists only when the bound holds,
    // so `true` means the type implements the trait.
    struct Probe<T>(PhantomData<T>);
    trait Fallback {
        fn serialize_impl(&self) -> bool {
            false
        }
        fn deserialize_impl(&self) -> bool {
            false
        }
    }
    impl<T> Fallback for Probe<T> {}
    impl<T: serde::Serialize> Probe<T> {
        #[allow(dead_code)]
        fn serialize_impl(&self) -> bool {
            true
        }
    }
    struct DeProbe<T>(PhantomData<T>);
    impl<T> Fallback for DeProbe<T> {}
    impl<T: serde::de::DeserializeOwned> DeProbe<T> {
        #[allow(dead_code)]
        fn deserialize_impl(&self) -> bool {
            true
        }
    }
    assert!(
        !Probe::<ConnectorActivity>(PhantomData).serialize_impl(),
        "ConnectorActivity must not implement Serialize (D548)"
    );
    assert!(!DeProbe::<ConnectorActivity>(PhantomData).deserialize_impl());
    // The probe itself works.
    assert!(Probe::<String>(PhantomData).serialize_impl());

    // The observer module opens no socket or file, and links no serialiser:
    // the counters leave it only as a plain struct passed to a method call.
    let src =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/activity.rs"))
            .unwrap();
    let code: String = src
        .lines()
        .map(|l| l.split("//").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    for forbidden in [
        "std::net",
        "tokio::net",
        "std::fs",
        "tokio::fs",
        "File",
        "TcpStream",
        "UdpSocket",
        "UnixStream",
        "serde",
        "Serialize",
        "to_vec",
        "to_writer",
        "Vec<ConnectorActivity>",
        "VecDeque",
        "mpsc",
    ] {
        assert!(
            !code.contains(forbidden),
            "src/activity.rs uses {forbidden}"
        );
    }
}

#[test]
fn each_alert_fires_in_a_test() {
    let dir = observability_dir();
    let rules: serde_json::Value =
        serde_norway::from_str(&std::fs::read_to_string(dir.join("alerts.yaml")).unwrap()).unwrap();
    let tests: serde_json::Value =
        serde_norway::from_str(&std::fs::read_to_string(dir.join("alerts.test.yaml")).unwrap())
            .unwrap();

    let alerts: BTreeSet<String> = rules["groups"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|g| g["rules"].as_array().unwrap().iter())
        .filter_map(|r| r["alert"].as_str().map(str::to_owned))
        .collect();
    // The six alerts CN2 Task 24 names.
    for required in [
        "LoamsConnectorInstanceDown",
        "LoamsConnectorLagHigh",
        "LoamsConnectorCdcPositionAtRisk",
        "LoamsConnectorDlqGrowing",
        "LoamsConnectorRestartLoop",
        "LoamsConnectorBackpressureSustained",
    ] {
        assert!(alerts.contains(required), "alerts.yaml lacks {required}");
    }
    // Every alert has a promtool test case in which it fires (non-empty
    // exp_alerts), and every rule expression names only declared families.
    let mut fired = BTreeSet::new();
    for t in tests["tests"].as_array().unwrap() {
        for case in t["alert_rule_test"].as_array().unwrap() {
            let exp = case["exp_alerts"].as_array().map_or(0, Vec::len);
            if exp > 0 {
                fired.insert(case["alertname"].as_str().unwrap().to_owned());
            }
        }
    }
    assert_eq!(fired, alerts, "alerts without a firing test case");
    for g in rules["groups"].as_array().unwrap() {
        for r in g["rules"].as_array().unwrap() {
            let expr = r["expr"].as_str().unwrap();
            for word in expr.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
                if word.starts_with("loams_flow_") {
                    assert!(
                        METRIC_FAMILIES.contains(&word),
                        "{word} is not a declared family"
                    );
                }
            }
        }
    }

    // promtool runs the rule tests themselves where it is installed (the CI
    // job `connectors-alerts` installs a pinned one); without it the test
    // checks the structure above and says so.
    let promtool = std::env::var_os("LOAMS_PROMTOOL")
        .map(PathBuf::from)
        .or_else(|| {
            Command::new("promtool")
                .arg("--version")
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|_| PathBuf::from("promtool"))
        });
    let Some(promtool) = promtool else {
        eprintln!("promtool not found (set LOAMS_PROMTOOL): skipping the rule-test run");
        return;
    };
    let check = Command::new(&promtool)
        .args(["check", "rules"])
        .arg(dir.join("alerts.yaml"))
        .output()
        .unwrap();
    assert!(
        check.status.success(),
        "promtool check rules: {}",
        String::from_utf8_lossy(&check.stdout) + String::from_utf8_lossy(&check.stderr)
    );
    let run = Command::new(&promtool)
        .args(["test", "rules"])
        .arg(dir.join("alerts.test.yaml"))
        .output()
        .unwrap();
    assert!(
        run.status.success(),
        "promtool test rules: {}",
        String::from_utf8_lossy(&run.stdout) + String::from_utf8_lossy(&run.stderr)
    );
}

#[test]
fn dashboard_uses_declared_families() {
    let raw =
        std::fs::read_to_string(observability_dir().join("dashboards/connectors.json")).unwrap();
    let dash: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(dash["title"].as_str().unwrap().contains("Connectors"));
    let mut used = BTreeSet::new();
    for word in raw.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
        if word.starts_with("loams_flow_") {
            assert!(
                METRIC_FAMILIES.contains(&word),
                "{word} is not a declared family"
            );
            used.insert(word.to_owned());
        }
    }
    for f in METRIC_FAMILIES {
        assert!(used.contains(*f), "the dashboard has no panel for {f}");
    }
}
