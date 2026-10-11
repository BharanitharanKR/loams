use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use loams_store::{Store, StoreError};

use crate::backends;

const MIB: usize = 1024 * 1024;

/// `len` deterministic bytes as a stream of `chunk`-byte pieces.
fn body(len: usize, chunk: usize) -> (Bytes, BoxStream<'static, std::io::Result<Bytes>>) {
    let data: Bytes = (0..len)
        .map(|i| ((i * 31 + i / 4099) % 251) as u8)
        .collect::<Vec<u8>>()
        .into();
    let pieces: Vec<std::io::Result<Bytes>> = (0..len)
        .step_by(chunk)
        .map(|start| Ok(data.slice(start..(start + chunk).min(len))))
        .collect();
    (data, stream::iter(pieces).boxed())
}

#[tokio::test]
async fn multipart_round_trip_65_mib() {
    for backend in backends::all() {
        let (data, stream) = body(65 * MIB, MIB + 17);
        let version = backend
            .store
            .put_multipart("packs/big.lpk", stream, 8 * MIB)
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", backend.name));
        let (read, info) = backend.store.get("packs/big.lpk").await.unwrap();
        assert_eq!(info.size, 65 * 1024 * 1024, "{}", backend.name);
        assert!(read == data, "{}: bytes differ", backend.name);
        assert_eq!(info.version.e_tag, version.e_tag, "{}", backend.name);
    }
}

#[tokio::test]
async fn multipart_with_a_small_part_size_still_round_trips() {
    // Parts under the S3 minimum are raised to it, so any caller's size works.
    let store = Store::in_memory();
    let (data, stream) = body(3 * MIB + 5, 4096);
    store.put_multipart("small", stream, 1).await.unwrap();
    assert!(store.get("small").await.unwrap().0 == data);
}

#[tokio::test]
async fn multipart_of_an_empty_body_writes_an_empty_object() {
    let store = Store::in_memory();
    store
        .put_multipart("empty", stream::empty().boxed(), 8 * MIB)
        .await
        .unwrap();
    assert_eq!(store.head("empty").await.unwrap().size, 0);
}

#[tokio::test]
async fn multipart_abort_leaves_no_object() {
    for backend in backends::all() {
        let (data, _) = body(12 * MIB, MIB);
        let pieces: Vec<std::io::Result<Bytes>> = vec![
            Ok(data.slice(..6 * MIB)),
            Ok(data.slice(6 * MIB..)),
            Err(std::io::Error::other("the pusher went away")),
        ];
        let err = backend
            .store
            .put_multipart("packs/cut.lpk", stream::iter(pieces).boxed(), 5 * MIB)
            .await
            .unwrap_err();
        assert!(
            matches!(err, StoreError::Body(_)),
            "{}: got {err:?}",
            backend.name
        );
        assert!(
            matches!(
                backend.store.head("packs/cut.lpk").await.unwrap_err(),
                StoreError::NotFound { .. }
            ),
            "{}",
            backend.name
        );
        assert!(
            backend.store.list("").await.unwrap().is_empty(),
            "{}",
            backend.name
        );
        if let Some(dir) = &backend.dir {
            assert_eq!(backends::files_on_disk(dir.path()), Vec::<String>::new());
        }
    }
}

#[tokio::test]
async fn multipart_overwrites_an_existing_object() {
    let store = Store::in_memory();
    store.put("a", Bytes::from_static(b"old")).await.unwrap();
    let (data, stream) = body(MIB, 1000);
    store.put_multipart("a", stream, 5 * MIB).await.unwrap();
    assert!(store.get("a").await.unwrap().0 == data);
}
