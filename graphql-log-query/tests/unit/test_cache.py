"""The result cache as pure logic: keys, codecs, TTL policy, and the two contracts that keep it
from taking the API down — spec §2 items 30-31.

No Redis and no database here. :class:`src.cache.ResultCache` is deliberately buildable from a
:class:`~src.config.Settings` and anything that answers ``get()`` / ``setex()``, which is what lets
this module drive the branches a healthy server cannot produce: a store that raises on every call, a
store that returns a blob from a build that no longer exists, a store that is not there at all. The
integration suite (``tests/integration/test_cache.py``) then proves the two claims that need a real
database to mean anything — a hit issues **zero** SQL, and a rehydrated entry resolves
``relatedLogs`` exactly as a database-loaded one does.

.. rubric:: What is asserted here that a "it did not raise" test would miss

Every never-raises test asserts **three** things, because "survived" has three halves and each can
regress alone: the caller got the *right value*, the ``errors`` counter moved (so the failure was
seen rather than skipped), and ``compute`` actually ran (so the answer came from the source rather
than from a silently empty cache read). A test that only checked "no exception escaped" would stay
green against a cache that swallowed the error and returned ``[]``.

And every key test is written as a **near-miss pair**. Asserting that two obviously different filter
sets get different keys is nearly free and proves nearly nothing; the failures that actually happen
are the pair that differs only in the limit, only in one boundary of the window, or only in the
query kind — because those are the fields a key builder forgets.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytest

from src.broker import encode_event
from src.cache import (
    CACHE_FORMAT_VERSION,
    KIND_LOG_STATS,
    KIND_LOGS,
    LOG_ENTRIES_CODEC,
    LOG_STATS_CODEC,
    ResultCache,
    cached_log_stats,
    cached_logs,
    create_result_cache,
    decode_log_entries,
    decode_log_stats,
    encode_log_entries,
    encode_log_stats,
    log_stats_key_payload,
    logs_key_payload,
    make_cache_key,
)
from src.config import Settings
from src.db.repository import LogQuery, LogStatsResult, ServiceLevelCount
from src.graphql.enums import LogLevel
from src.graphql.types import LogEntry
from tests.unit.test_broker import ANCHOR, fields_of, make_entry

#: A window used by the key tests. Two bounds, so "differs only by the start" and "differs only by
#: the end" are both expressible as one-field edits of a single base query.
WINDOW_START = datetime(2026, 7, 28, 9, 0, 0, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 7, 28, 10, 0, 0, tzinfo=timezone.utc)


def make_settings(
    *,
    cache_enabled: bool = True,
    cache_ttl_seconds: int = 30,
    agg_cache_ttl_seconds: int = 60,
    default_query_limit: int = 100,
    max_query_limit: int = 500,
) -> Settings:
    """Settings for a cache under test, built directly rather than read from the environment.

    ``_env_file=None`` so a stray ``.env`` cannot perturb the suite; every value a test's expected
    numbers depend on is passed explicitly so it is visible in the test rather than inherited.
    """
    return Settings(
        _env_file=None,
        seed_entries=0,
        seed_orders=0,
        log_level="WARNING",
        cache_enabled=cache_enabled,
        cache_ttl_seconds=cache_ttl_seconds,
        agg_cache_ttl_seconds=agg_cache_ttl_seconds,
        default_query_limit=default_query_limit,
        max_query_limit=max_query_limit,
    )


def base_query(**overrides: Any) -> LogQuery:
    """A fully-populated :class:`LogQuery`, so a near-miss test can edit exactly one field.

    Every field is non-``None``. A base with holes in it would let a key builder that ignored, say,
    ``search_text`` still pass the "differs only by search_text" test — because ``None`` vs ``None``
    is the same key whether the field is hashed or not.
    """
    fields: dict[str, Any] = {
        "service": "auth-svc",
        "level": "ERROR",
        "start_time": WINDOW_START,
        "end_time": WINDOW_END,
        "search_text": "token",
        "limit": 25,
    }
    fields.update(overrides)
    return LogQuery(**fields)


def stats_result(**overrides: Any) -> LogStatsResult:
    """An aggregate result with every field populated, for the codec tests."""
    fields: dict[str, Any] = {
        "total_logs": 1200,
        "error_count": 97,
        "earliest": WINDOW_START,
        "latest": WINDOW_END,
        "breakdown": (
            ServiceLevelCount(service="auth-svc", level="ERROR", entries=97),
            ServiceLevelCount(service="auth-svc", level="INFO", entries=800),
            ServiceLevelCount(service="cart-svc", level="DEBUG", entries=303),
        ),
    }
    fields.update(overrides)
    return LogStatsResult(**fields)


# =================================================================================================
# The store double
# =================================================================================================


class StubRedis:
    """The two methods :class:`~src.cache.ResultCache` uses, plus a switch for each failure.

    Duck-typed rather than a ``redis.asyncio.Redis`` subclass, exactly like the broker's fake: the
    cache's contract with its client is ``get()`` and ``setex()``, and writing the double to that
    contract is what keeps the contract small enough to be worth having.

    **Neither method contains an await point**, and that is load-bearing for the single-flight
    tests: awaiting a coroutine that never suspends does not yield to the event loop, so the
    interleaving those tests assert on is produced by the compute they gate, not by Redis timing.
    """

    def __init__(
        self,
        *,
        get_error: Optional[BaseException] = None,
        setex_error: Optional[BaseException] = None,
        as_bytes: bool = False,
    ) -> None:
        self.store: dict[str, str] = {}
        self.calls: list[tuple[Any, ...]] = []
        self.get_error = get_error
        self.setex_error = setex_error
        self.as_bytes = as_bytes
        self.closed = False

    async def get(self, key: str) -> Any:
        self.calls.append(("get", key))
        if self.get_error is not None:
            raise self.get_error
        value = self.store.get(key)
        if value is not None and self.as_bytes:
            return value.encode("utf-8")
        return value

    async def setex(self, key: str, ttl: int, value: str) -> bool:
        self.calls.append(("setex", key, ttl))
        if self.setex_error is not None:
            raise self.setex_error
        self.store[key] = value
        return True

    async def aclose(self) -> None:
        self.closed = True

    @property
    def gets(self) -> int:
        return sum(1 for call in self.calls if call[0] == "get")

    @property
    def setexes(self) -> list[tuple[Any, ...]]:
        return [call for call in self.calls if call[0] == "setex"]


class ExplodingRedis:
    """A client where **every** method raises. The never-raises contract's adversary.

    Separate from :class:`StubRedis`'s error switches because those still let a call succeed;
    this one exists to assert that no path through the cache depends on any Redis call working.
    """

    def __init__(self, error: Optional[BaseException] = None) -> None:
        self.error = error or ConnectionError("simulated: redis is unreachable")

    async def get(self, key: str) -> Any:
        raise self.error

    async def setex(self, key: str, ttl: int, value: str) -> bool:
        raise self.error

    async def aclose(self) -> None:
        raise self.error


def counting_compute(value: Any) -> tuple[Any, list[int]]:
    """An async callable returning ``value``, and the list its calls are appended to."""
    calls: list[int] = []

    async def compute() -> Any:
        calls.append(1)
        return value

    return compute, calls


# =================================================================================================
# Keys — determinism
# =================================================================================================


def test_dict_ordering_cannot_change_a_key() -> None:
    """``sort_keys=True`` is the entire determinism guarantee, so it gets a test of its own.

    Python dicts preserve insertion order. Without sorting, two call sites building the same logical
    filter set in different orders would hash differently — producing two keys, two misses, and a
    cache that silently never hits while every other test stayed green.
    """
    forwards = {"service": "auth-svc", "level": "ERROR", "limit": 25}
    backwards = {"limit": 25, "level": "ERROR", "service": "auth-svc"}

    assert make_cache_key(KIND_LOGS, forwards) == make_cache_key(KIND_LOGS, backwards)


def test_two_equal_queries_built_independently_agree() -> None:
    """The property the cache actually depends on, stated over ``LogQuery`` rather than dicts."""
    settings = make_settings()

    first = make_cache_key(KIND_LOGS, logs_key_payload(base_query(), settings))
    second = make_cache_key(KIND_LOGS, logs_key_payload(base_query(), settings))

    assert first == second


def test_a_key_is_the_namespace_the_version_the_kind_and_a_sha256() -> None:
    """The prefix is for humans reading ``KEYS``; the digest is the collision guarantee."""
    key = make_cache_key(KIND_LOGS, {"service": "auth-svc"}, namespace="proj:cache")

    namespace, version, kind, digest = key.rsplit(":", 3)
    assert namespace == "proj:cache"
    assert version == f"v{CACHE_FORMAT_VERSION}"
    assert kind == KIND_LOGS
    assert len(digest) == 64, "sha256 renders as 64 hex characters"
    assert int(digest, 16) >= 0, "…and all of them are hex"


def test_the_namespace_scopes_the_whole_key() -> None:
    """One Redis instance serves this cache, C9's documents, C6's channel — and sibling projects."""
    payload = {"service": "auth-svc"}

    assert make_cache_key(KIND_LOGS, payload, namespace="a") != make_cache_key(
        KIND_LOGS, payload, namespace="b"
    )


def test_bumping_the_format_version_strands_every_old_key() -> None:
    """The version is inside the digest as well as the prefix, so a bump cannot be half-applied."""
    payload = {"service": "auth-svc"}

    old = make_cache_key(KIND_LOGS, payload, version=1)
    new = make_cache_key(KIND_LOGS, payload, version=2)

    assert old != new
    assert old.rsplit(":", 1)[1] != new.rsplit(":", 1)[1], "the DIGEST moved, not just the prefix"


# =================================================================================================
# Keys — discrimination. Every case below is a ONE-FIELD edit of `base_query()`.
#
# THE FAILURE THESE PREVENT: a key that omits a field the query depends on serves one question's
# rows as another question's answer, for the length of the TTL, with no error anywhere.
# =================================================================================================


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("service", "cart-svc", id="service"),
        pytest.param("level", "INFO", id="level"),
        pytest.param("start_time", WINDOW_START + timedelta(seconds=1), id="start-boundary"),
        pytest.param("end_time", WINDOW_END - timedelta(seconds=1), id="end-boundary"),
        pytest.param("search_text", "tokens", id="search-text"),
        pytest.param("limit", 26, id="limit"),
    ],
)
def test_changing_one_filter_changes_the_key(field: str, value: Any) -> None:
    """Each of the six is a real near miss — a one-second window edit, a one-row limit edit."""
    settings = make_settings()

    original = make_cache_key(KIND_LOGS, logs_key_payload(base_query(), settings))
    edited = make_cache_key(KIND_LOGS, logs_key_payload(base_query(**{field: value}), settings))

    assert original != edited, f"two queries differing only in {field} shared a key"


def test_dropping_a_filter_changes_the_key() -> None:
    """"No service filter" and "service = auth-svc" are different questions, so different keys.

    The mirror of the parametrised test above, and the one that catches a payload builder using
    truthiness: ``if query.service:`` would drop the key entirely for ``None``, which is fine, and
    also for ``""`` — a *supplied* filter that matches nothing (see ``LogQuery``'s docstring).
    """
    settings = make_settings()

    with_filter = make_cache_key(KIND_LOGS, logs_key_payload(base_query(), settings))
    without = make_cache_key(KIND_LOGS, logs_key_payload(base_query(service=None), settings))
    empty = make_cache_key(KIND_LOGS, logs_key_payload(base_query(service=""), settings))

    assert len({with_filter, without, empty}) == 3


def test_logs_and_log_stats_never_share_a_key() -> None:
    """Handed byte-identical filters — which they routinely are, both being driven by one window."""
    payload = {"start_time": WINDOW_START.isoformat(), "end_time": WINDOW_END.isoformat()}

    assert make_cache_key(KIND_LOGS, payload) != make_cache_key(KIND_LOG_STATS, payload)


def test_the_kind_is_inside_the_digest_not_only_in_the_prefix() -> None:
    """A prefix is a naming convention; a naming convention is not a collision guarantee."""
    payload = {"start_time": WINDOW_START.isoformat()}

    logs_digest = make_cache_key(KIND_LOGS, payload).rsplit(":", 1)[1]
    stats_digest = make_cache_key(KIND_LOG_STATS, payload).rsplit(":", 1)[1]

    assert logs_digest != stats_digest


def test_the_stats_payload_has_no_limit_and_the_logs_payload_does() -> None:
    """``limit`` caps a list and means nothing to an aggregate, which ignores it entirely."""
    settings = make_settings()

    assert logs_key_payload(base_query(), settings)["limit"] == 25
    assert log_stats_key_payload(base_query())["limit"] is None


# =================================================================================================
# Keys — normalisation
# =================================================================================================


def test_the_same_instant_in_two_offsets_is_one_key() -> None:
    """A client in Kolkata and a client in UTC asking for the same moment ask the same question.

    The bound is normalised through the same :func:`~src.db.repository.as_utc` the WHERE clause is
    built with, so the key is derived from precisely the instant the query will run against.
    """
    settings = make_settings()
    india = timezone(timedelta(hours=5, minutes=30))
    same_moment = WINDOW_START.astimezone(india)

    assert same_moment.utcoffset() == timedelta(hours=5, minutes=30), "the fixture is a real offset"

    utc_key = make_cache_key(KIND_LOGS, logs_key_payload(base_query(), settings))
    india_key = make_cache_key(
        KIND_LOGS, logs_key_payload(base_query(start_time=same_moment), settings)
    )

    assert utc_key == india_key


def test_a_naive_bound_hashes_as_the_utc_bound_it_will_be_queried_as() -> None:
    """``as_utc`` assumes naive means UTC; the key has to make the same assumption or it lies."""
    settings = make_settings()
    naive = WINDOW_START.replace(tzinfo=None)

    aware_key = make_cache_key(KIND_LOGS, logs_key_payload(base_query(), settings))
    naive_key = make_cache_key(KIND_LOGS, logs_key_payload(base_query(start_time=naive), settings))

    assert aware_key == naive_key


def test_the_limit_in_the_key_is_the_clamped_one() -> None:
    """Two limits above ``MAX_QUERY_LIMIT`` return identical rows, so they get identical keys.

    Hashing the raw request instead would write two keys holding one identical result — correct,
    and a straight waste of both the cache and every write into it.
    """
    settings = make_settings(max_query_limit=500)

    ten_thousand = make_cache_key(KIND_LOGS, logs_key_payload(base_query(limit=10_000), settings))
    fifty_thousand = make_cache_key(KIND_LOGS, logs_key_payload(base_query(limit=50_000), settings))
    at_the_cap = make_cache_key(KIND_LOGS, logs_key_payload(base_query(limit=500), settings))

    assert ten_thousand == fifty_thousand == at_the_cap


def test_lowering_the_configured_cap_moves_the_keys() -> None:
    """The key follows the configuration: a smaller cap is a different answer, so a different key."""
    generous = make_settings(max_query_limit=500)
    # `default_query_limit` has to come down with the cap: Settings rejects a default above the
    # cap, because such a default would be clamped away on every request that omits `limit`.
    # Without it this line raises in the helper and the assertion below never runs.
    strict = make_settings(default_query_limit=50, max_query_limit=50)
    query = base_query(limit=400)

    assert make_cache_key(KIND_LOGS, logs_key_payload(query, generous)) != make_cache_key(
        KIND_LOGS, logs_key_payload(query, strict)
    )


def test_an_omitted_limit_hashes_as_the_default_it_resolves_to() -> None:
    """``limit: null`` and ``limit: 100`` are the same query under ``DEFAULT_QUERY_LIMIT=100``."""
    settings = make_settings(default_query_limit=100)

    omitted = make_cache_key(KIND_LOGS, logs_key_payload(base_query(limit=None), settings))
    explicit = make_cache_key(KIND_LOGS, logs_key_payload(base_query(limit=100), settings))

    assert omitted == explicit


# =================================================================================================
# TTL policy — spec §3 Feature Area D: "a TTL policy defined per aggregation"
# =================================================================================================


def test_each_kind_gets_its_own_configured_ttl() -> None:
    """The whole point of the policy table: an aggregate is not stored for as long as a row list."""
    cache = ResultCache(make_settings(cache_ttl_seconds=30, agg_cache_ttl_seconds=60))

    assert cache.ttl_for(KIND_LOGS) == 30
    assert cache.ttl_for(KIND_LOG_STATS) == 60


def test_the_ttls_follow_configuration_rather_than_a_literal() -> None:
    """An operator who moves either number must see it take effect on exactly that kind."""
    cache = ResultCache(make_settings(cache_ttl_seconds=5, agg_cache_ttl_seconds=900))

    assert cache.ttl_for(KIND_LOGS) == 5
    assert cache.ttl_for(KIND_LOG_STATS) == 900


def test_an_unknown_kind_gets_the_short_ttl() -> None:
    """A kind with no policy row is a caller the table has not been told about: go stale soonest."""
    cache = ResultCache(make_settings(cache_ttl_seconds=30, agg_cache_ttl_seconds=600))

    assert cache.ttl_for("somethingC11WillAdd") == 30


async def test_a_non_positive_ttl_stores_nothing_instead_of_failing_every_write() -> None:
    """``SETEX key 0`` is an error in Redis, so ``CACHE_TTL_SECONDS=0`` has to mean "do not store".

    An operator typing 0 means "read through, never cache". Passing it to Redis would instead mean
    "raise on every write", which the cache would survive and count as an error — a working service
    quietly logging a failure per request for a configuration that was perfectly reasonable.
    """
    client = StubRedis()
    cache = ResultCache(make_settings(), redis_client=client)
    compute, calls = counting_compute([make_entry()])

    await cache.get_or_compute("k", compute, ttl=0, codec=LOG_ENTRIES_CODEC)

    assert client.setexes == [], "nothing should have been written"
    assert calls == [1], "…and the value still came back from the source"
    assert cache.stats.errors == 0, "a documented configuration is not an error"


# =================================================================================================
# The cache-aside path
# =================================================================================================


async def test_a_second_call_is_answered_from_the_store_without_recomputing() -> None:
    """The headline behaviour, at unit altitude: one compute, two correct answers."""
    client = StubRedis()
    cache = ResultCache(make_settings(), redis_client=client)
    entries = [make_entry(entry_id=1), make_entry(entry_id=2, metadata={"k": "v"})]
    compute, calls = counting_compute(entries)

    first = await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)
    second = await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert calls == [1], "the source was consulted exactly once"
    assert [fields_of(entry) for entry in second] == [fields_of(entry) for entry in first]
    assert cache.stats.hits == 1
    assert cache.stats.misses == 1


async def test_a_hit_returns_rebuilt_objects_rather_than_the_ones_that_were_stored() -> None:
    """A hit must not depend on the process still holding the objects it cached.

    Identity is the observable difference between "decoded from the store" and "handed back out of
    a dictionary somewhere" — and the second would pass every value assertion while proving nothing
    about the serialisation the next process over depends on.
    """
    client = StubRedis()
    cache = ResultCache(make_settings(), redis_client=client)
    original = [make_entry(entry_id=7, metadata={"nested": {"a": [1, 2]}}, trace_id="abc")]
    compute, _ = counting_compute(original)

    await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)
    hit = await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert hit[0] is not original[0], "a hit decodes; it does not hand back the cached instance"
    assert fields_of(hit[0]) == fields_of(original[0])


async def test_the_stored_document_is_json_the_ttl_is_the_one_asked_for() -> None:
    """What lands in Redis is inspectable text under the requested expiry, not a pickle."""
    client = StubRedis()
    cache = ResultCache(make_settings(), redis_client=client)
    compute, _ = counting_compute([make_entry(entry_id=3, service="cart-svc")])

    await cache.get_or_compute("k", compute, ttl=42, codec=LOG_ENTRIES_CODEC)

    assert client.setexes == [("setex", "k", 42)]
    document = json.loads(client.store["k"])
    assert document["v"] == CACHE_FORMAT_VERSION
    assert document["entries"][0]["service"] == "cart-svc"


async def test_an_aggregate_is_cached_and_rebuilt_through_its_own_codec() -> None:
    """The other kind, end to end at unit altitude — including that a hit decodes rather than aliases."""
    client = StubRedis()
    cache = ResultCache(make_settings(), redis_client=client)
    expected = stats_result()
    compute, calls = counting_compute(expected)

    first = await cache.get_or_compute("s", compute, ttl=60, codec=LOG_STATS_CODEC)
    second = await cache.get_or_compute("s", compute, ttl=60, codec=LOG_STATS_CODEC)

    assert calls == [1]
    assert first is expected, "a miss returns exactly what the source produced"
    assert second == expected, "and a hit reproduces it by value"
    assert second is not expected, "…by decoding it, not by handing back the cached instance"
    assert client.setexes == [("setex", "s", 60)]


async def test_a_hit_decodes_bytes_as_happily_as_str() -> None:
    """redis-py hands back ``bytes`` without ``decode_responses``, which is how ours is built."""
    client = StubRedis(as_bytes=True)
    cache = ResultCache(make_settings(), redis_client=client)
    compute, calls = counting_compute([make_entry(entry_id=11)])

    await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)
    hit = await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert calls == [1]
    assert hit[0].id == "11"


async def test_hits_misses_and_bypasses_account_for_every_call() -> None:
    """``hits + misses + bypassed`` is the number of calls served. The invariant the counters keep.

    It is why a Redis failure counts as a *miss* as well as an error: the call was answered from the
    source, which is what a miss means, and folding the two would make the hit ratio a function of
    the outage rate.
    """
    cache = ResultCache(make_settings(), redis_client=StubRedis())
    compute, _ = counting_compute([make_entry()])

    for _ in range(5):
        await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    stats = cache.stats
    assert stats.hits + stats.misses + stats.bypassed == 5
    assert (stats.hits, stats.misses) == (4, 1)


# =================================================================================================
# The never-raises contract — property 3
# =================================================================================================


async def test_a_client_that_raises_on_every_call_still_produces_the_right_answer() -> None:
    """The contract in one test: correct value, error counted, source consulted."""
    cache = ResultCache(make_settings(), redis_client=ExplodingRedis())
    entries = [make_entry(entry_id=5, service="auth-svc")]
    compute, calls = counting_compute(entries)

    result = await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert [fields_of(entry) for entry in result] == [fields_of(entry) for entry in entries]
    assert calls == [1], "the answer came from the source, not from an empty cache read"
    assert cache.stats.errors >= 1, "the failure was seen rather than skipped"


async def test_every_call_survives_a_dead_store_not_merely_the_first() -> None:
    """A cache that degraded once and then wedged would pass a single-call test."""
    cache = ResultCache(make_settings(), redis_client=ExplodingRedis())
    compute, calls = counting_compute([make_entry()])

    for _ in range(4):
        assert await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert calls == [1, 1, 1, 1], "every call read through; none was served a stale or empty answer"
    assert cache.stats.misses == 4


async def test_a_failed_read_still_attempts_the_write() -> None:
    """So the cache repopulates on the FIRST request after Redis returns, not the one after that."""
    client = StubRedis(get_error=ConnectionError("down"))
    cache = ResultCache(make_settings(), redis_client=client)
    compute, _ = counting_compute([make_entry()])

    await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert client.setexes, "the write was attempted despite the read having failed"


async def test_a_write_failure_is_counted_and_invisible_to_the_caller() -> None:
    """A store that reads fine and refuses writes (OOM, a read-only replica) is a real state."""
    client = StubRedis(setex_error=ConnectionError("read only replica"))
    cache = ResultCache(make_settings(), redis_client=client)
    entries = [make_entry(entry_id=6)]
    compute, calls = counting_compute(entries)

    result = await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert [fields_of(entry) for entry in result] == [fields_of(entry) for entry in entries]
    assert calls == [1]
    assert cache.stats.errors == 1


@pytest.mark.parametrize(
    "blob",
    [
        pytest.param("not json at all", id="not-json"),
        pytest.param("[1, 2, 3]", id="json-but-not-an-object"),
        pytest.param('{"v": 99, "entries": []}', id="a-format-version-we-do-not-know"),
        pytest.param('{"v": 1}', id="no-entries-array"),
        pytest.param('{"v": 1, "entries": [{"id": "1"}]}', id="a-truncated-entry"),
        pytest.param(
            '{"v": 1, "entries": [{"id": "1", "timestamp": "2026-07-28T00:00:00+00:00",'
            ' "service": "s", "level": "TRACE", "message": "m"}]}',
            id="a-level-outside-the-enum",
        ),
    ],
)
async def test_an_undecodable_blob_is_a_miss_and_never_an_error_response(blob: str) -> None:
    """A blob written by an older build must cost a recompute, not a 500.

    This is the whole migration story for a cached value: bump the format version, and the entries
    the previous build wrote become unreadable, get discarded on sight, and expire on their own TTL
    without anybody flushing anything.
    """
    client = StubRedis()
    client.store["k"] = blob
    cache = ResultCache(make_settings(), redis_client=client)
    entries = [make_entry(entry_id=8)]
    compute, calls = counting_compute(entries)

    result = await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert [fields_of(entry) for entry in result] == [fields_of(entry) for entry in entries]
    assert calls == [1]
    assert cache.stats.errors == 1
    assert cache.stats.hits == 0, "an undecodable blob must never be counted as a hit"


async def test_an_unserialisable_value_costs_the_cache_and_not_the_request() -> None:
    """A codec that cannot render what it was handed is a bug — and still not a failed request."""
    client = StubRedis()
    cache = ResultCache(make_settings(), redis_client=client)

    # Not a list of entries, so the codec's own comprehension raises before `json.dumps` is
    # reached — which is the realistic shape of the bug (a caller passing the wrong codec), not a
    # value that merely defeats the JSON encoder.
    class NotAResultList:
        pass

    compute, calls = counting_compute(NotAResultList())
    result = await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert isinstance(result, NotAResultList)
    assert calls == [1]
    assert cache.stats.errors == 1
    assert client.setexes == [], "nothing unrenderable reached Redis"


async def test_an_error_from_compute_propagates_untouched() -> None:
    """The one thing the cache must NOT swallow.

    Turning a failed database query into an empty result set is worse than the failed query: the
    client receives a confident, wrong, cacheable answer.
    """
    cache = ResultCache(make_settings(), redis_client=StubRedis())

    async def compute() -> list[LogEntry]:
        raise RuntimeError("the database said no")

    with pytest.raises(RuntimeError, match="the database said no"):
        await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)


# =================================================================================================
# CACHE_ENABLED=false
# =================================================================================================


async def test_a_disabled_cache_makes_no_redis_calls_at_all() -> None:
    """Not a no-op wrapper that still round-trips: the store must be untouched."""
    client = StubRedis()
    cache = ResultCache(make_settings(cache_enabled=False), redis_client=client)
    compute, calls = counting_compute([make_entry()])

    for _ in range(3):
        await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert client.calls == [], "a disabled cache issued a Redis command"
    assert calls == [1, 1, 1], "every call went to the source"
    assert cache.enabled is False
    assert cache.stats.bypassed == 3
    assert (cache.stats.hits, cache.stats.misses) == (0, 0)


async def test_a_disabled_cache_does_not_even_build_a_client() -> None:
    """"Disabled" should be indistinguishable from "not built" — no pool, no socket, no CLIENT LIST."""
    cache = create_result_cache(make_settings(cache_enabled=False))

    assert cache.enabled is False
    assert cache.stats.enabled is False
    # Closing is still safe and still a no-op, which is what the lifespan's `finally` needs.
    await cache.aclose()


async def test_no_client_behaves_exactly_like_disabled() -> None:
    """A broken ``REDIS_URL`` is a different fault with identical, correct behaviour."""
    cache = ResultCache(make_settings(cache_enabled=True), redis_client=None)
    compute, calls = counting_compute([make_entry()])

    await cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert cache.enabled is False
    assert calls == [1]
    assert cache.stats.bypassed == 1


async def test_a_disabled_cache_does_not_coalesce_either() -> None:
    """With the cache off, "off" has to mean off.

    Coalescing is still cache-like behaviour — one caller receiving another's computed value — and
    an operator who set ``CACHE_ENABLED=false`` to make every request hit the database would find
    that most of them still did not.
    """
    cache = ResultCache(make_settings(cache_enabled=False), redis_client=StubRedis())
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[int] = []

    async def compute() -> list[LogEntry]:
        calls.append(1)
        started.set()
        await release.wait()
        return [make_entry()]

    tasks = [
        asyncio.create_task(cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC))
        for _ in range(5)
    ]
    await started.wait()
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(*tasks)

    assert calls == [1, 1, 1, 1, 1]
    assert cache.stats.coalesced == 0


# =================================================================================================
# Single-flight — property 4
# =================================================================================================


async def test_n_concurrent_misses_on_one_key_compute_once() -> None:
    """The stampede guard. Without it, a hot key expiring under load is N identical queries.

    The gate is deterministic rather than timing-based: the leader parks on an event, every other
    caller is given a turn to find the in-flight future, and only then is the leader released.
    """
    client = StubRedis()
    cache = ResultCache(make_settings(), redis_client=client)
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[int] = []

    async def compute() -> list[LogEntry]:
        calls.append(1)
        started.set()
        await release.wait()
        return [make_entry(entry_id=99, service="auth-svc")]

    tasks = [
        asyncio.create_task(cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC))
        for _ in range(25)
    ]
    await started.wait()
    await asyncio.sleep(0)
    assert cache.stats.inflight == 1, "one key, one computation in flight"
    release.set()
    results = await asyncio.gather(*tasks)

    assert calls == [1], "twenty-five callers, one computation"
    assert cache.stats.coalesced == 24
    assert all(result[0].id == "99" for result in results), "and all of them got the answer"
    assert len(client.setexes) == 1, "only the leader writes the value back"
    assert cache.stats.inflight == 0, "the in-flight map is empty once the work is done"


async def test_two_different_keys_do_not_coalesce_with_each_other() -> None:
    """Single-flight is per key. Collapsing across keys would be the collision bug, self-inflicted."""
    cache = ResultCache(make_settings(), redis_client=StubRedis())
    release = asyncio.Event()
    calls: list[str] = []

    def make_compute(name: str) -> Any:
        async def compute() -> list[LogEntry]:
            calls.append(name)
            await release.wait()
            return [make_entry(service=name)]

        return compute

    tasks = [
        asyncio.create_task(
            cache.get_or_compute(key, make_compute(key), ttl=30, codec=LOG_ENTRIES_CODEC)
        )
        for key in ("a", "b")
    ]
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(*tasks)

    assert sorted(calls) == ["a", "b"]
    assert [result[0].service for result in results] == ["a", "b"]
    assert cache.stats.coalesced == 0


async def test_a_raising_compute_does_not_wedge_the_key_forever() -> None:
    """The ``finally`` in :meth:`_compute_once`, and the reason it is a ``finally``.

    Without it the failed future stays in the map and every later caller re-raises the original
    error without ever retrying — a transient database blip turning into a permanently broken key
    that only a restart clears.
    """
    cache = ResultCache(make_settings(), redis_client=StubRedis())
    attempts: list[int] = []

    async def flaky() -> list[LogEntry]:
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("transient")
        return [make_entry(entry_id=4)]

    with pytest.raises(RuntimeError, match="transient"):
        await cache.get_or_compute("k", flaky, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert cache.stats.inflight == 0, "the failed computation was removed from the in-flight map"

    recovered = await cache.get_or_compute("k", flaky, ttl=30, codec=LOG_ENTRIES_CODEC)

    assert attempts == [1, 1], "the second call really did retry"
    assert recovered[0].id == "4"


async def test_a_coalesced_caller_shares_the_leaders_failure() -> None:
    """Documented behaviour, pinned: waiters do not each retry the query that just failed."""
    cache = ResultCache(make_settings(), redis_client=StubRedis())
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[int] = []

    async def compute() -> list[LogEntry]:
        calls.append(1)
        started.set()
        await release.wait()
        raise RuntimeError("the database said no")

    tasks = [
        asyncio.create_task(cache.get_or_compute("k", compute, ttl=30, codec=LOG_ENTRIES_CODEC))
        for _ in range(4)
    ]
    await started.wait()
    await asyncio.sleep(0)
    release.set()
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    assert calls == [1], "the failing query ran once, not four times"
    assert all(isinstance(outcome, RuntimeError) for outcome in outcomes)
    assert cache.stats.inflight == 0


# =================================================================================================
# Codecs — the round trip that spec §2 item 31 rests on
# =================================================================================================


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(make_entry(), id="plain"),
        pytest.param(make_entry(metadata=None), id="metadata-null"),
        pytest.param(make_entry(metadata={}), id="metadata-empty-object"),
        pytest.param(make_entry(metadata={"a": {"b": [1, 2, None]}}), id="metadata-nested"),
        pytest.param(make_entry(trace_id=None), id="trace-null"),
        pytest.param(make_entry(trace_id="abcdef0123456789"), id="trace-present"),
        pytest.param(make_entry(level=LogLevel.CRITICAL), id="level-critical"),
        pytest.param(make_entry(message='unicode: ✓ 日本語 " \' \\ {}'), id="message-escapes"),
        pytest.param(make_entry(entry_id=9_223_372_036_854_775_807), id="bigserial-max"),
        pytest.param(
            make_entry(timestamp=ANCHOR.replace(microsecond=123456)), id="microseconds"
        ),
    ],
)
def test_an_entry_survives_the_cache_round_trip_intact(entry: LogEntry) -> None:
    """A cache hit has to be indistinguishable from a database read, field for field."""
    restored = decode_log_entries(json.loads(json.dumps(encode_log_entries([entry]))))[0]

    assert restored.id == entry.id
    assert isinstance(restored.id, str), "GraphQL ID is a string on both sides"
    assert restored.timestamp == entry.timestamp
    assert restored.timestamp.tzinfo is not None, "a naive value is unequal to every aware one"
    assert restored.service == entry.service
    assert restored.level is entry.level, "the ENUM member, not its string value"
    assert restored.message == entry.message
    assert restored.metadata == entry.metadata
    assert restored.trace_id == entry.trace_id
    assert fields_of(restored) == fields_of(entry), "nothing else drifted either"


def test_metadata_absent_and_metadata_empty_stay_distinguishable() -> None:
    """They collapse into each other under a lazy codec (``metadata or {}``), and must not.

    C2 fixed a real bug about exactly this distinction on the storage side (``none_as_null``); a
    codec that undid it here would put the bug back one layer up, where no SQL probe can see it.
    """
    absent, empty = decode_log_entries(
        encode_log_entries([make_entry(metadata=None), make_entry(metadata={})])
    )

    assert absent.metadata is None
    assert empty.metadata == {}


def test_an_empty_result_round_trips_as_an_empty_result() -> None:
    """"No rows matched" is a perfectly good answer and a perfectly cacheable one."""
    assert decode_log_entries(encode_log_entries([])) == []


def test_the_cache_and_the_pubsub_bridge_encode_an_entry_identically() -> None:
    """One JSON representation of a published entry, not one per consumer.

    Two encoders for one type is how an entry that crossed the subscription bridge and the same
    entry read back out of the cache start disagreeing about whether ``metadata`` came back — and
    the disagreement survives a full suite, because each test exercises only one of the two paths.
    """
    entry = make_entry(entry_id=12, metadata={"k": "v"}, trace_id="t")

    from_broker = json.loads(encode_event(entry, origin="worker-1"))["entry"]
    from_cache = encode_log_entries([entry])["entries"][0]

    assert from_broker == from_cache


@pytest.mark.parametrize(
    "result",
    [
        pytest.param(stats_result(), id="populated"),
        pytest.param(
            stats_result(total_logs=0, error_count=0, earliest=None, latest=None, breakdown=()),
            id="a-quiet-window",
        ),
        pytest.param(stats_result(breakdown=()), id="no-breakdown"),
    ],
)
def test_an_aggregate_survives_the_cache_round_trip_intact(result: LogStatsResult) -> None:
    """Including the two nullable timestamps, which are ``None`` for a window with no rows."""
    restored = decode_log_stats(json.loads(json.dumps(encode_log_stats(result))))

    assert restored == result, "LogStatsResult is a frozen dataclass, so == is a value comparison"
    assert isinstance(restored.breakdown, tuple), "the frozen tuple, not a list"
    if result.earliest is not None:
        assert restored.earliest is not None and restored.earliest.tzinfo is not None


@pytest.mark.parametrize(
    "blob",
    [
        pytest.param("not an object", id="not-an-object"),
        pytest.param({"v": 99, "breakdown": []}, id="unknown-format-version"),
        pytest.param({"v": CACHE_FORMAT_VERSION}, id="no-breakdown-array"),
    ],
)
def test_a_malformed_aggregate_blob_raises_so_the_cache_can_treat_it_as_a_miss(blob: Any) -> None:
    """The codec's contract: it refuses rather than half-reading. The caller decides what that means."""
    with pytest.raises(ValueError):
        decode_log_stats(blob)


# =================================================================================================
# The resolver helpers
# =================================================================================================


async def test_a_context_without_a_cache_simply_does_not_cache() -> None:
    """``None`` means "no caching", not an error — every C3/C4 test builds exactly that context."""
    entries = [make_entry(entry_id=1)]
    compute, calls = counting_compute(entries)

    first = await cached_logs(None, base_query(), make_settings(), compute)
    second = await cached_logs(None, base_query(), make_settings(), compute)

    assert calls == [1, 1], "both calls went to the source"
    assert first == entries and second == entries


async def test_the_resolver_helpers_key_the_two_kinds_apart() -> None:
    """The end-to-end version of the kind test: one cache, two helpers, no cross-talk.

    If they shared a key, the second call would decode a list of entries with the aggregate codec
    (or the reverse) — which the codec would reject, so the visible symptom would be a permanent
    100% miss rate rather than wrong data. Both are worth failing on.
    """
    client = StubRedis()
    cache = ResultCache(make_settings(), redis_client=client)
    settings = make_settings()
    query = LogQuery(start_time=WINDOW_START, end_time=WINDOW_END)

    logs_compute, logs_calls = counting_compute([make_entry(entry_id=1)])
    stats_compute, stats_calls = counting_compute(stats_result())

    await cached_logs(cache, query, settings, logs_compute)
    await cached_log_stats(cache, query, stats_compute)
    await cached_logs(cache, query, settings, logs_compute)
    await cached_log_stats(cache, query, stats_compute)

    assert logs_calls == [1], "the second logs call was a hit"
    assert stats_calls == [1], "the second stats call was a hit"
    assert len(client.store) == 2, "two kinds, two keys"
    assert cache.stats.hits == 2 and cache.stats.errors == 0


async def test_the_aggregate_helper_uses_the_aggregate_ttl() -> None:
    """The per-aggregation TTL policy, observed where it actually takes effect."""
    client = StubRedis()
    cache = ResultCache(
        make_settings(cache_ttl_seconds=30, agg_cache_ttl_seconds=600), redis_client=client
    )
    settings = make_settings()
    query = LogQuery(start_time=WINDOW_START)

    logs_compute, _ = counting_compute([make_entry()])
    stats_compute, _ = counting_compute(stats_result())
    await cached_logs(cache, query, settings, logs_compute)
    await cached_log_stats(cache, query, stats_compute)

    ttls = [call[2] for call in client.setexes]
    assert ttls == [30, 600]
