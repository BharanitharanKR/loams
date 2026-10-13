//! `LogWriter::append_fenced` (GR1 Task 12, D744): an append made under a
//! lease commits only while the lease is at its epoch, and a broken fence
//! fails only the appends made under it.

use std::time::Duration;

use crate::common::{Meta, faulty_store, read_direct, records, value};
use loams_common::meta::{ApplyError, Fence, MetaError};
use loams_log::{LogConfig, LogError, LogWriter};

/// A writer that flushes only when asked, so several appends share a flush.
fn manual_flush() -> LogConfig {
    LogConfig {
        flush_interval: Duration::from_secs(3600),
        ..LogConfig::new(1)
    }
}

#[tokio::test]
async fn a_broken_fence_fails_only_its_appends_and_commits_nothing_of_them() {
    let meta = Meta::start().await;
    let (_, owned) = meta.stream("acme", "owned", 1).await;
    let (_, other) = meta.stream("acme", "other", 1).await;
    let (_, store) = faulty_store();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), manual_flush()).expect("start");
    let lease = "graph/gr_01J9ZQ3V7K8X2M4N6P8R0T2V4W/owner";
    let grant = meta
        .client
        .acquire_lease(lease, "node-1", Duration::from_secs(30))
        .await
        .expect("acquire");
    let fence = Fence {
        lease: lease.to_string(),
        epoch: grant.epoch,
    };

    // While the lease holds, a fenced append is acknowledged like any other.
    let held = {
        let (writer, fence) = (writer.clone(), fence.clone());
        tokio::spawn(async move {
            writer
                .append_fenced(owned, 0, records("held", 2), fence)
                .await
        })
    };
    writer.flush().await.expect("flush");
    let ack = held.await.expect("join").expect("a fenced append");
    assert_eq!((ack.base_offset, ack.last_offset), (0, 1));

    // The owner loses the lease; its next append shares a flush with an
    // unfenced one.
    meta.client
        .release_lease(lease, "node-1", grant.epoch)
        .await
        .expect("release");
    let stale = {
        let (writer, fence) = (writer.clone(), fence.clone());
        tokio::spawn(async move {
            writer
                .append_fenced(owned, 0, records("stale", 3), fence)
                .await
        })
    };
    let unfenced = {
        let writer = writer.clone();
        tokio::spawn(async move { writer.append(other, 0, records("free", 2)).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    writer.flush().await.expect("the flush that carried them");
    let refused = stale.await.expect("join").expect_err("refused");
    assert!(
        matches!(
            &refused,
            LogError::Meta(MetaError::Rejected(ApplyError::Fenced { lease: l })) if l == lease
        ),
        "{refused:?}"
    );
    let ack = unfenced
        .await
        .expect("join")
        .expect("the unfenced append goes out in the next object");
    assert_eq!((ack.base_offset, ack.last_offset), (0, 1));

    // Nothing of the refused append is in its stream.
    assert_eq!(meta.high_watermark(owned, 0).await, 2);
    let owned_values: Vec<String> = read_direct(&meta.client, &store, owned, 0)
        .await
        .iter()
        .map(|r| value(&r.record))
        .collect();
    assert!(
        owned_values.iter().all(|v| v.starts_with("held")),
        "{owned_values:?}"
    );
    assert_eq!(meta.high_watermark(other, 0).await, 2);
    writer.shutdown().await.expect("shutdown");
}

/// One flush may carry two epochs of one lease: a stale engine's late append and its
/// successor's. Only the stale one fails; the current one goes out in the next object (PR #443
/// review).
#[tokio::test]
async fn a_broken_fence_spares_the_current_epoch_of_its_lease() {
    let meta = Meta::start().await;
    let (_, owned) = meta.stream("acme", "owned", 1).await;
    let (_, store) = faulty_store();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), manual_flush()).expect("start");
    let lease = "graph/gr_01J9ZQ3V7K8X2M4N6P8R0T2V4W/owner";
    let ttl = Duration::from_secs(30);
    let first = meta
        .client
        .acquire_lease(lease, "node-1", ttl)
        .await
        .expect("acquire");
    meta.client
        .release_lease(lease, "node-1", first.epoch)
        .await
        .expect("release");
    let second = meta
        .client
        .acquire_lease(lease, "node-1", ttl)
        .await
        .expect("take again");
    assert!(second.epoch > first.epoch);
    let fence = |epoch| Fence {
        lease: lease.to_string(),
        epoch,
    };
    let stale = {
        let (writer, fence) = (writer.clone(), fence(first.epoch));
        tokio::spawn(async move {
            writer
                .append_fenced(owned, 0, records("stale", 2), fence)
                .await
        })
    };
    let current = {
        let (writer, fence) = (writer.clone(), fence(second.epoch));
        tokio::spawn(async move {
            writer
                .append_fenced(owned, 0, records("current", 2), fence)
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    writer.flush().await.expect("the flush that carried them");
    let refused = stale
        .await
        .expect("join")
        .expect_err("the stale epoch is refused");
    assert!(
        matches!(
            &refused,
            LogError::Meta(MetaError::Rejected(ApplyError::Fenced { .. }))
        ),
        "{refused:?}"
    );
    let ack = current
        .await
        .expect("join")
        .expect("the current epoch commits");
    assert_eq!((ack.base_offset, ack.last_offset), (0, 1));
    let values: Vec<String> = read_direct(&meta.client, &store, owned, 0)
        .await
        .iter()
        .map(|r| value(&r.record))
        .collect();
    assert!(
        values.iter().all(|v| v.starts_with("current")),
        "{values:?}"
    );
    writer.shutdown().await.expect("shutdown");
}
