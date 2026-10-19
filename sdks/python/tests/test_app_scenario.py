"""SDKP Parts B and C for Python: the app scenario through the public facade.

`python_app_collections`, `python_app_search`, `python_app_graph` and
`python_app_sql` (Part C) replay
`sdks/fixtures/recorded/app/` with both `Loams` and `AsyncLoams` against the
fixture server (`app_replay.py` says how), so each passes only when the facade
sends exactly the recorded requests and reads the recorded answers the way
they mean. They always use the fixture server, never `LOAMS_TEST_ENDPOINT`: a
live server cannot replay a recording (its tokens and ids are its own), and a
replay against one would create the `app` namespace a `--drift` re-record needs
to be fresh.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any

import pytest

from loams import AsyncLoams, CallOptions, Loams

from app_replay import replay_app_fixture, replay_app_fixture_async
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


@pytest.mark.parametrize("replay", [replay_app_fixture, replay_app_fixture_async], ids=["sync", "async"])
def test_python_app_sql(fixture_server: str, replay: object) -> None:
    """Part C: a parameterised read, a capped read, Arrow IPC and the two refusals."""
    replay(fixture_server, "app_sql")  # type: ignore[operator]


_STEP0 = CallOptions(headers={"loams-fixture-name": "app_sql", "loams-fixture-step": "0"})
_PER_TAG = "SELECT tags, count(*) AS c FROM notes WHERE n > $1 GROUP BY tags ORDER BY tags"


def test_python_sql_records_reads_rows_as_dicts(fixture_server: str) -> None:
    """`sql.records` sends the `$1` value and answers one dict per row, on both clients."""
    with Loams(fixture_server, namespace="app", proto_json=True, max_retries=0) as loams:
        assert loams.sql.records(_PER_TAG, [1], options=_STEP0) == [
            {"tags": "policy", "c": 2},
            {"tags": "shipping", "c": 1},
        ]

    async def run() -> list[dict[str, Any]]:
        async with AsyncLoams(fixture_server, namespace="app", proto_json=True, max_retries=0) as client:
            return await client.sql.records(_PER_TAG, [1], options=_STEP0)

    assert asyncio.run(run()) == [{"tags": "policy", "c": 2}, {"tags": "shipping", "c": 1}]
