//! Fenced checkpoints and split reassignment (CN2 Task 4, Review Focus 4
//! and 12).
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::*;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::testing::reference::{CounterConfig, CounterHandle};
use loams_connector_sdk::*;

#[tokio::test(start_paused = true)]
async fn checkpoint_save_fenced_by_epoch() {
    // The store itself refuses an older epoch.
    let store = MemoryCheckpointStore::new();
    let i = instance();
    store.save(&Checkpoint::new(i.clone(), 2)).await.unwrap();
    assert_eq!(
        store.save(&Checkpoint::new(i.clone(), 1)).await,
        Err(CheckpointError::Fenced {
            stored: 2,
            attempted: 1
        })
    );
    store.save(&Checkpoint::new(i.clone(), 2)).await.unwrap();
    store.save(&Checkpoint::new(i.clone(), 3)).await.unwrap();

    // A pipeline whose epoch is older than the stored one does not start.
    let fabric = MemoryFabric::new();
    let handle = CounterHandle::new(&["a"]);
    let err = counter_pipeline(
        small_config(),
        &handle,
        CounterConfig::default(),
        &fabric,
        &store,
    )
    .epoch(1)
    .start()
    .await
    .unwrap_err();
    assert!(err.message.contains("fenced"), "{}", err.message);
    assert!(handle.opened().is_empty());

    // A running pipeline is fenced when its successor saves: it stops, and
    // it commits nothing upstream after the fence.
    let store = MemoryCheckpointStore::new();
    let fabric = MemoryFabric::new();
    fabric.set_append_delay(Some(Duration::from_millis(20)));
    let handle = CounterHandle::new(&["a"]);
    let p = counter_pipeline(
        small_config(),
        &handle,
        CounterConfig {
            events_per_split: None,
            ..CounterConfig::default()
        },
        &fabric,
        &store,
    )
    .epoch(4)
    .start()
    .await
    .unwrap();
    eventually("some commits", || handle.committed("a").len() >= 3).await;
    let mut successor = store.get(&i).unwrap();
    successor.epoch = 5;
    store.save(&successor).await.unwrap();
    let st = p.wait_for(|s| s.state == PipelineState::Fenced).await;
    assert!(st.last_error.unwrap().message.contains("fenced"));
    let commits = handle.committed("a").len();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        handle.committed("a").len(),
        commits,
        "a fenced pipeline committed upstream"
    );
    assert_eq!(store.get(&i).unwrap().epoch, 5, "the stale epoch overwrote");
}

#[tokio::test(start_paused = true)]
async fn split_reassignment_resumes_from_checkpoint() {
    let cfg = small_config();
    let store = MemoryCheckpointStore::new();
    let fabric = MemoryFabric::new();
    fabric.set_append_delay(Some(Duration::from_millis(5)));
    let handle = CounterHandle::new(&["a", "b"]);
    let counter = CounterConfig {
        events_per_split: Some(300),
        ..CounterConfig::default()
    };

    // The first holder (epoch 1) crashes part-way.
    let first = counter_pipeline(cfg.clone(), &handle, counter.clone(), &fabric, &store)
        .epoch(1)
        .start()
        .await
        .unwrap();
    eventually("a third appended", || fabric.len() >= 200).await;
    first.abort();
    let before = store.get(&instance()).unwrap();
    assert_eq!(before.epoch, 1);
    let appended_before = fabric.len();
    assert!(appended_before < 600);

    // The split is reassigned to a new holder (epoch 2), which resumes each
    // split from its checkpointed position.
    let second = counter_pipeline(cfg.clone(), &handle, counter, &fabric, &store)
        .epoch(2)
        .start()
        .await
        .unwrap();
    eventually("everything appended", || fabric.len() == 600).await;
    let reopened: Vec<_> = handle.opened().into_iter().skip(2).collect();
    for split in ["a", "b"] {
        let expected = before
            .splits
            .get(&SplitId::new(split))
            .and_then(Position::as_u64);
        assert!(
            reopened.contains(&(SplitId::new(split), expected)),
            "{split} reopened at {reopened:?}, checkpoint {expected:?}"
        );
    }
    // No loss, and the Fabric holds every id exactly once.
    let ids: BTreeSet<_> = fabric.ids().into_iter().collect();
    assert_eq!(ids.len(), 600);
    for split in ["a", "b"] {
        for n in 0..300 {
            assert!(ids.contains(&format!("{split}/{n}")), "lost {split}/{n}");
        }
    }
    // Re-reads are bounded by what was in flight at the crash.
    assert!(
        fabric.duplicates() <= (cfg.max_in_flight_batches * cfg.max_batch_rows) as u64,
        "{} duplicates",
        fabric.duplicates()
    );
    second.stop().await;
    let after = store.get(&instance()).unwrap();
    assert_eq!(after.epoch, 2);
    assert_eq!(after.splits[&SplitId::new("a")].as_u64(), Some(300));
    // The crashed holder's late save is refused.
    assert!(matches!(
        store.save(&before).await,
        Err(CheckpointError::Fenced {
            stored: 2,
            attempted: 1
        })
    ));
}
