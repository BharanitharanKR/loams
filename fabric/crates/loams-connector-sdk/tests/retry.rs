//! Retries by error class, the DLQ and rate limiting (CN2 Task 4).
#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::testing::reference::{CounterConfig, CounterHandle, MapHandle};
use loams_connector_sdk::*;
use tokio::time::Instant;

fn err(class: ErrorClass, m: &str) -> (ErrorClass, String) {
    (class, m.to_owned())
}

/// Ten events through a map sink that fails as told.
async fn sink_case(
    failures: Vec<(ErrorClass, String)>,
) -> (PipelineHandle, MemoryFabric, MapHandle, Arc<Recorder>) {
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 10);
    let map = MapHandle::new();
    map.fail_writes(failures);
    let rec = Arc::new(Recorder::default());
    let p = map_sink_pipeline(small_config(), &fabric, &map, false, 1, Some(rec.clone())).await;
    (p, fabric, map, rec)
}

#[tokio::test(start_paused = true)]
async fn retry_classes_behave() {
    // Retryable: backs off and succeeds.
    let retryable = ErrorClass::Retryable { after: None };
    let (p, fabric, map, rec) = sink_case(vec![
        err(retryable.clone(), "busy"),
        err(retryable.clone(), "busy"),
        err(retryable.clone(), "busy"),
    ])
    .await;
    eventually("retryable recovers", || fabric.committed(GROUP) == 10).await;
    assert_eq!(map.len(), 10);
    let retries = rec.retries.lock().unwrap().clone();
    assert_eq!(retries.len(), 3);
    for (i, (class, attempt, delay)) in retries.iter().enumerate() {
        assert_eq!(*class, retryable);
        assert_eq!(*attempt, i as u32 + 1);
        assert!(*delay <= small_config().retry.ceiling(*attempt));
    }
    assert_eq!(p.status().state, PipelineState::Running);
    p.stop().await;

    // Retryable, out of attempts: the instance fails.
    let attempts = small_config().retry.max_attempts as usize;
    let (p, fabric, _map, _) = sink_case(vec![err(retryable.clone(), "down"); attempts]).await;
    let st = p.wait_for(|s| s.state == PipelineState::Failed).await;
    assert_eq!(st.last_error.unwrap().class, retryable);
    assert_eq!(fabric.committed(GROUP), 0);

    // Throttled: waits `after`, then succeeds.
    let after = Duration::from_secs(2);
    let (p, fabric, map, _) = sink_case(vec![err(ErrorClass::Throttled { after }, "429")]).await;
    eventually("throttled recovers", || fabric.committed(GROUP) == 10).await;
    let calls = map.write_calls();
    assert!(calls[1] - calls[0] >= after, "{:?}", calls[1] - calls[0]);
    p.stop().await;

    // Poison: the event goes to the DLQ; the rest is written.
    let (p, fabric, map, rec) = sink_case(vec![]).await;
    map.poison("e/3");
    eventually("poison skipped", || fabric.committed(GROUP) == 10).await;
    assert_eq!(map.len(), 9);
    assert_eq!(fabric.dead_letters().len(), 1);
    assert_eq!(*rec.dead_letters.lock().unwrap(), 1);
    p.stop().await;

    // Auth: stops with last_error; nothing committed.
    let (p, fabric, map, _) = sink_case(vec![err(ErrorClass::Auth, "401")]).await;
    let st = p.wait_for(|s| s.state == PipelineState::Failed).await;
    assert_eq!(st.last_error.unwrap().class, ErrorClass::Auth);
    assert_eq!(fabric.committed(GROUP), 0);
    assert!(map.is_empty());

    // SchemaIncompatible: pauses until resumed.
    let (p, fabric, map, _) =
        sink_case(vec![err(ErrorClass::SchemaIncompatible, "column dropped")]).await;
    let st = p.wait_for(|s| s.state == PipelineState::Paused).await;
    assert_eq!(st.last_error.unwrap().class, ErrorClass::SchemaIncompatible);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(map.is_empty());
    assert_eq!(p.status().state, PipelineState::Paused);
    p.resume();
    eventually("resumed", || fabric.committed(GROUP) == 10).await;
    assert_eq!(p.status().state, PipelineState::Running);
    p.stop().await;

    // Fatal: stops with last_error.
    let (p, _fabric, _map, _) = sink_case(vec![err(ErrorClass::Fatal, "boom")]).await;
    let st = p.wait_for(|s| s.state == PipelineState::Failed).await;
    let e = st.last_error.unwrap();
    assert_eq!(e.class, ErrorClass::Fatal);
    assert!(e.message.contains("boom"));

    // PositionLost: stops with a re-snapshot hint.
    let handle = CounterHandle::new(&["a"]);
    handle.fail_open("a", [err(ErrorClass::PositionLost, "binlog purged")]);
    let p = counter_pipeline(
        small_config(),
        &handle,
        CounterConfig::default(),
        &MemoryFabric::new(),
        &MemoryCheckpointStore::new(),
    )
    .start()
    .await
    .unwrap();
    let st = p.wait_for(|s| s.state == PipelineState::Failed).await;
    let e = st.last_error.unwrap();
    assert_eq!(e.class, ErrorClass::PositionLost);
    assert!(e.hint.unwrap().contains("re-snapshot"));
}

#[tokio::test(start_paused = true)]
async fn poison_goes_to_dlq_with_error() {
    // Sink side: the one poison event, with the error, and nothing else.
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 10);
    let map = MapHandle::new();
    map.poison("e/3");
    let p = map_sink_pipeline(small_config(), &fabric, &map, false, 1, None).await;
    eventually("all committed", || fabric.committed(GROUP) == 10).await;
    p.stop().await;
    let dead = fabric.dead_letters();
    assert_eq!(dead.len(), 1);
    let d = &dead[0];
    assert_eq!(d.instance, instance());
    assert_eq!(d.class, ErrorClass::Poison);
    assert_eq!(d.event.as_ref().unwrap().id, "e/3");
    assert!(
        d.error.contains("rejected by the reference sink"),
        "{}",
        d.error
    );
    let ids = map.ids();
    assert_eq!(ids.len(), 9);
    assert!(!ids.contains(&"e/3".to_owned()));

    // Transactional sink: the same, inside the transaction.
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 10);
    let map = MapHandle::new();
    map.poison("e/5");
    let p = map_sink_pipeline(small_config(), &fabric, &map, true, 1, None).await;
    eventually("all committed", || fabric.committed(GROUP) == 10).await;
    p.stop().await;
    assert_eq!(fabric.dead_letters().len(), 1);
    assert_eq!(map.len(), 9);
    assert_eq!(map.max_applied(), 1);

    // Source side: a record the source cannot decode has no event; the DLQ
    // gets the error and the split, and the source carries on.
    let handle = CounterHandle::new(&["a"]);
    handle.fail_next("a", [err(ErrorClass::Poison, "undecodable record at 7")]);
    let fabric = MemoryFabric::new();
    let p = counter_pipeline(
        small_config(),
        &handle,
        CounterConfig {
            events_per_split: Some(30),
            ..CounterConfig::default()
        },
        &fabric,
        &MemoryCheckpointStore::new(),
    )
    .start()
    .await
    .unwrap();
    eventually("source finished", || fabric.len() == 30).await;
    let dead = fabric.dead_letters();
    assert_eq!(dead.len(), 1);
    assert!(dead[0].event.is_none());
    assert_eq!(dead[0].split, Some(SplitId::new("a")));
    assert!(dead[0].error.contains("undecodable record at 7"));
    assert_eq!(p.status().state, PipelineState::Running);
    p.stop().await;
}

#[tokio::test(start_paused = true)]
async fn rate_limit_honours_retry_after() {
    // The bucket: a Throttled blocks every caller for `after` and halves the
    // rate.
    let limiter = RateLimiter::unlimited();
    limiter.throttled(Duration::from_secs(3));
    let t0 = Instant::now();
    limiter.acquire(0).await;
    assert!(t0.elapsed() >= Duration::from_secs(3));
    assert!((limiter.factor() - 0.5).abs() < 1e-9);

    // The bucket is shared by an instance's splits: when split a is told to
    // wait, split b waits too.
    let handle = CounterHandle::new(&["a", "b"]);
    let fabric = MemoryFabric::new();
    let after = Duration::from_secs(3);
    let p = counter_pipeline(
        PipelineConfig {
            max_batch_rows: 1,
            ..small_config()
        },
        &handle,
        CounterConfig {
            events_per_split: Some(40),
            ..CounterConfig::default()
        },
        &fabric,
        &MemoryCheckpointStore::new(),
    )
    .start()
    .await
    .unwrap();
    eventually("a few events", || fabric.len() >= 10).await;
    handle.fail_next("a", [err(ErrorClass::Throttled { after }, "429")]);
    eventually("everything", || fabric.len() == 80).await;
    p.stop().await;
    let calls = handle.next_calls();
    let throttled_at = calls
        .iter()
        .find(|c| c.split.as_str() == "a" && c.rows == 0)
        .map(|c| c.at)
        .unwrap();
    let during: Vec<_> = calls
        .iter()
        .filter(|c| c.at > throttled_at && c.at < throttled_at + after)
        .collect();
    assert!(during.is_empty(), "calls during Retry-After: {during:?}");

    // A configured limit: 10 requests a second (burst 10).
    let handle = CounterHandle::new(&["a"]);
    let fabric = MemoryFabric::new();
    let t0 = Instant::now();
    let p = counter_pipeline(
        PipelineConfig {
            max_batch_rows: 1,
            ..small_config()
        },
        &handle,
        CounterConfig {
            events_per_split: Some(30),
            ..CounterConfig::default()
        },
        &fabric,
        &MemoryCheckpointStore::new(),
    )
    .rate_limit(RateLimit {
        requests: Some(Rate::per_second(10.0)),
        bytes: None,
    })
    .start()
    .await
    .unwrap();
    eventually("30 events", || fabric.len() == 30).await;
    assert!(
        t0.elapsed() >= Duration::from_millis(1_900),
        "{:?}",
        t0.elapsed()
    );
    p.stop().await;
}
