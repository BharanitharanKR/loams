use std::time::Duration;

use bytes::Bytes;
use loams_store::StoreError;

use crate::backends;

#[tokio::test]
async fn copy_to_other_path_round_trips() {
    for backend in backends::all() {
        let store = &backend.store;
        store
            .put("a/src", Bytes::from_static(b"body"))
            .await
            .unwrap();
        store.copy("a/src", "b/dst").await.unwrap();
        assert_eq!(
            store.get("b/dst").await.unwrap().0,
            Bytes::from_static(b"body")
        );
        assert_eq!(
            store.get("a/src").await.unwrap().0,
            Bytes::from_static(b"body")
        );
    }
}

#[tokio::test]
async fn copy_of_missing_object_is_not_found() {
    for backend in backends::all() {
        let err = backend.store.copy("nope", "dst").await.unwrap_err();
        assert!(
            matches!(err, StoreError::NotFound { ref path } if path == "nope"),
            "{}: got {err:?}",
            backend.name
        );
    }
}

#[tokio::test]
async fn copy_to_self_updates_last_modified() {
    for backend in backends::all() {
        let store = &backend.store;
        store
            .put("cache/k", Bytes::from_static(b"entry"))
            .await
            .unwrap();
        let before = store.head("cache/k").await.unwrap().last_modified_ms;
        tokio::time::sleep(Duration::from_millis(30)).await;

        store.copy("cache/k", "cache/k").await.unwrap();

        let (data, info) = store.get("cache/k").await.unwrap();
        assert_eq!(data, Bytes::from_static(b"entry"), "{}", backend.name);
        assert!(
            info.last_modified_ms > before,
            "{}: {} is not after {before}",
            backend.name,
            info.last_modified_ms
        );
        if let Some(dir) = &backend.dir {
            // No staging link may be left beside the object.
            assert_eq!(backends::files_on_disk(dir.path()), vec!["bucket/cache/k"]);
        }
    }
}
