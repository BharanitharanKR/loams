use bytes::Bytes;

use crate::backends;

/// Paths inserted out of order, some nested, plus siblings outside the prefix.
const PATHS: [&str; 13] = [
    "ws/w07/head",
    "ws/w01/snap/00000000000000000002.lws",
    "ws/w03/head",
    "ws/w01/snap/00000000000000000000.lws",
    "ws/w10/head",
    "ws/w01/snap/00000000000000000001.lws",
    "ws/w02/head",
    "ws/w05/head",
    "ws/w04/head",
    "ws/w09/head",
    "ws/w08/head",
    "ws/w06/head",
    "ws/w01/head",
];

#[tokio::test]
async fn list_page_is_sorted_and_resumable() {
    for backend in backends::all() {
        let store = &backend.store;
        for path in PATHS {
            store.put(path, Bytes::from_static(b"x")).await.unwrap();
        }
        store
            .put("wsx/other", Bytes::from_static(b"x"))
            .await
            .unwrap();
        store
            .put("a/before", Bytes::from_static(b"x"))
            .await
            .unwrap();

        let mut expected: Vec<&str> = PATHS.to_vec();
        expected.sort_unstable();

        for limit in [1, 4, 5, 13, 100] {
            let mut seen = Vec::new();
            let mut after: Option<String> = None;
            loop {
                let page = store
                    .list_page("ws", after.as_deref(), limit)
                    .await
                    .unwrap();
                assert!(page.len() <= limit, "{}: page over {limit}", backend.name);
                let paths: Vec<String> = page.iter().map(|info| info.path.clone()).collect();
                let mut sorted = paths.clone();
                sorted.sort();
                assert_eq!(paths, sorted, "{}: page not sorted", backend.name);
                if page.is_empty() {
                    break;
                }
                assert!(page.iter().all(|info| info.size == 1), "{}", backend.name);
                after = paths.last().cloned();
                seen.extend(paths);
            }
            assert_eq!(seen, expected, "{} with limit {limit}", backend.name);
        }

        // `after` need not be a stored path, and a zero limit lists nothing.
        let page = store.list_page("ws", Some("ws/w05"), 3).await.unwrap();
        let paths: Vec<&str> = page.iter().map(|info| info.path.as_str()).collect();
        assert_eq!(
            paths,
            ["ws/w05/head", "ws/w06/head", "ws/w07/head"],
            "{}",
            backend.name
        );
        assert!(store.list_page("ws", None, 0).await.unwrap().is_empty());
        assert!(
            store
                .list_page("missing", None, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn list_page_with_an_unbounded_limit_lists_everything() {
    for backend in backends::all() {
        for path in PATHS {
            backend
                .store
                .put(path, Bytes::from_static(b"x"))
                .await
                .unwrap();
        }
        let page = backend
            .store
            .list_page("ws", None, usize::MAX)
            .await
            .unwrap();
        assert_eq!(page.len(), PATHS.len(), "{}", backend.name);
    }
}
