"""SDKP Part B for Python: the app scenario through the public facade.

`python_app_collections`, `python_app_search` and `python_app_graph` replay
`sdks/fixtures/recorded/app/` with both `Loams` and `AsyncLoams` against the
fixture server (`app_replay.py` says how), so each passes only when the facade
sends exactly the recorded requests and reads the recorded answers the way
they mean. They always use the fixture server, never `LOAMS_TEST_ENDPOINT`: a
live server cannot replay a recording (its tokens and ids are its own), and a
replay against one would create the `app` namespace a `--drift` re-record needs
to be fresh.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from app_replay import replay_app_fixture, replay_app_fixture_async  # type: ignore[import-not-found]
from conftest import _endpoint_from_node


@pytest.fixture(scope="module")
def fixture_server() -> Iterator[str]:
    started = _endpoint_from_node()
    if started is None:
        pytest.skip("no fixture server: install node")
    url, process = started
    try:
        yield url
    finally:
        process.terminate()
        process.wait(timeout=10)
        if process.stdout is not None:
            process.stdout.close()


@pytest.mark.parametrize("replay", [replay_app_fixture, replay_app_fixture_async], ids=["sync", "async"])
def test_python_app_collections(fixture_server: str, replay: object) -> None:
    """Namespaces, collections, documents, tokens, paging and the two refusals."""
    replay(fixture_server, "app_collections")  # type: ignore[operator]


@pytest.mark.parametrize("replay", [replay_app_fixture, replay_app_fixture_async], ids=["sync", "async"])
def test_python_app_search(fixture_server: str, replay: object) -> None:
    """Text, vector and hybrid search, the next page, and the refused weighted sum."""
    replay(fixture_server, "app_search")  # type: ignore[operator]


@pytest.mark.parametrize("replay", [replay_app_fixture, replay_app_fixture_async], ids=["sync", "async"])
def test_python_app_graph(fixture_server: str, replay: object) -> None:
    """A build without the graph feature refuses every graph call, typed."""
    replay(fixture_server, "app_graph")  # type: ignore[operator]


def test_a_replay_with_the_wrong_namespace_fails(fixture_server: str) -> None:
    """The tamper check: the fixture server compares request bodies, so a
    facade that sent another namespace must not pass."""
    with pytest.raises(AssertionError):
        replay_app_fixture(fixture_server, "app_search", namespace="appx")
