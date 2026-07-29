"""``GET /metrics`` against the real stack — spec §2 item 37, §5 "monitoring ... working".

Item 37 names three things, and each is asserted here against **real traffic** rather than against a
hand-driven registry (which ``tests/unit/test_metrics.py`` already owns):

* **query execution time** — a histogram with samples that appeared because a query really ran;
* **per-field resolution time** — samples for the fields a document selected, and *none* for the
  fields it did not, which is the only assertion that can tell "the hook is attached" from "the hook
  reports everything it can see";
* **active subscription connections** — a gauge driven up and back down by a real
  ``graphql-transport-ws`` socket, not by calling ``inc()``.

.. rubric:: THE COUNTERS ARE GRADED AGAINST THE OBJECTS THEY MIRROR, NOT AGAINST LITERALS

``gql_cache_hits_total`` is compared with :attr:`src.cache.CacheStats.hits` on the very cache the
request went through. That is the claim worth testing: C9 deliberately does **not** keep a second
tally beside C6's and C7's counters, because a metric that disagrees with the thing it describes is
worse than no metric. A test asserting ``== 1`` would pass against a duplicated counter that had
started to drift; asserting ``metric == cache.stats.hits`` cannot.

.. rubric:: Cardinality is asserted as a property of the code, not of the traffic

:func:`test_a_client_cannot_mint_time_series_without_limit` sends more distinct operation names than
the cap allows and requires the series count to stop growing. Cardinality is the only way a metrics
layer takes a server down, and it is the one failure that does not show up until the process has
been running for a week.
"""

from __future__ import annotations

import threading
import time
from collections.abc import AsyncIterator
from typing import Any, Optional
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy import text as sql_text
from starlette.testclient import TestClient

from src.cache import ResultCache
from src.config import Settings
from src.db.models import LogRecord
from src.db.session import Database
from src.graphql.apq import compute_query_hash
from src.graphql.errors import ErrorCode
from src.main import create_app
from src.metrics import (
    ANONYMOUS_OPERATION_LABEL,
    MAX_OPERATION_LABELS,
    OTHER_OPERATION_LABEL,
    PROMETHEUS_CONTENT_TYPE,
    Metrics,
)
from tests.integration.corpus import run_sync

#: The shipped complexity budget, read off the declared default rather than the environment: the
#: compose ``test`` service raises ``MAX_QUERY_COMPLEXITY`` for the rest of the suite, and the
#: cost-rejection test here needs a budget the document below actually exceeds.
SHIPPED_COMPLEXITY = Settings.model_fields["max_query_complexity"].default

#: Two levels of correlation — 1,101,010 against a budget of 25,000. See
#: ``tests/integration/test_cost_gate.py``, which owns the arithmetic.
OVER_BUDGET = "{ logs { relatedLogs { relatedLogs { id } } } }"

#: The document the field-timing test sends. Deliberately narrow: what makes the assertion sharp is
#: the fields it does **not** select.
SELECTED = "query FieldTimings { logs(filters: {limit: 3}) { id service } }"

#: Fields on ``LogEntry`` this schema publishes that :data:`SELECTED` does not ask for. None of them
#: may appear in the field histogram — a hook that timed the whole type rather than the resolved
#: fields would show all of them, and would still pass a test that only checked ``id`` was present.
NOT_SELECTED = ("message", "timestamp", "metadata", "traceId", "relatedLogs")

#: The subprotocol the C13 Apollo client speaks and the one the E2E verifier will use.
GRAPHQL_TRANSPORT_WS = "graphql-transport-ws"

#: A failure deadline, not a delay: a healthy run satisfies every wait below in microseconds.
DEADLINE_SECONDS = 10.0

LOG_STREAM = "subscription Stream { logStream { id service level message } }"


# =================================================================================================
# Settings and applications
# =================================================================================================


def make_settings(**overrides: Any) -> Settings:
    """Settings for a metrics application, with the cost budget pinned to what we ship.

    ``DATABASE_URL`` and ``REDIS_URL`` come from the environment compose injects, so this is the
    real stack. The subscription channel is unique per app because Redis pub/sub is instance-wide
    and ignores the selected logical database — two applications alive in one test session would
    otherwise cross-talk and one test's entries would arrive on another's socket.
    """
    fields: dict[str, Any] = {
        "_env_file": None,
        "seed_entries": 0,
        "seed_orders": 0,
        "log_level": "WARNING",
        "cache_enabled": False,
        "metrics_enabled": True,
        "max_query_complexity": SHIPPED_COMPLEXITY,
        "default_query_limit": Settings.model_fields["default_query_limit"].default,
        "max_query_limit": Settings.model_fields["max_query_limit"].default,
        "subscription_channel": f"test:metrics:{uuid4().hex}",
    }
    fields.update(overrides)
    return Settings(**fields)


@pytest.fixture()
def make_metrics_app(database: Database) -> Any:  # noqa: ANN401 - a factory
    """Build an application whose metrics configuration the test chooses.

    Depends on ``database`` so ``log_entries`` exists and has been truncated before the app's own
    lifespan opens an engine over it.
    """

    def _make(**overrides: Any) -> FastAPI:
        return create_app(settings=make_settings(**overrides))

    return _make


@pytest.fixture()
def metrics_app(make_metrics_app: Any) -> FastAPI:  # noqa: ANN401
    """The default application: metrics on, cache off, shipped cost budget."""
    return make_metrics_app()


@pytest.fixture()
async def metrics_client(metrics_app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """An async HTTP client over ``metrics_app``, with the lifespan entered by hand.

    ``ASGITransport`` never sends lifespan events, and the registry is built *in* the lifespan — so
    without this ``app.state.metrics`` would not exist and ``/metrics`` would answer an empty scrape
    for a reason that has nothing to do with what is under test.
    """
    async with metrics_app.router.lifespan_context(metrics_app):
        transport = httpx.ASGITransport(app=metrics_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://graphql-metrics.test") as c:
            yield c


@pytest.fixture()
def clean_store(_schema: None, db_settings: Settings) -> None:
    """Truncate ``log_entries`` before the test, **synchronously**.

    A sync fixture because the subscription test is sync (the WebSocket session API is), and a sync
    test cannot consume the async ``database`` fixture the rest of the integration suite uses.
    :func:`~tests.integration.corpus.run_sync` uses a private loop that is created, used and closed
    here, so it never touches the loop pytest-asyncio manages or the one the ``TestClient`` portal
    will later start.
    """

    async def _truncate() -> None:
        database = Database.create(db_settings)
        try:
            async with database.engine.begin() as connection:
                await connection.execute(sql_text("TRUNCATE TABLE log_entries RESTART IDENTITY"))
        finally:
            await database.dispose()

    run_sync(_truncate())


# =================================================================================================
# Reading a scrape
# =================================================================================================


async def scrape(client: httpx.AsyncClient) -> str:
    """``GET /metrics``, with the transport-level contract asserted on the way past."""
    response = await client.get("/metrics")

    assert response.status_code == 200, response.text
    assert response.headers["content-type"] == PROMETHEUS_CONTENT_TYPE
    return response.text


def families(exposition: str) -> list[Any]:
    """Parse a scrape with ``prometheus_client``'s own parser.

    Its parser rather than a regular expression, deliberately: "the endpoint parses" is one of the
    things this file has to prove, and only a real parser can fail on a malformed exposition.
    """
    return list(text_string_to_metric_families(exposition))


def samples(exposition: str, name: str) -> list[Any]:
    """Every sample in the scrape whose name is exactly ``name``."""
    return [
        sample
        for family in families(exposition)
        for sample in family.samples
        if sample.name == name
    ]


def sample_value(exposition: str, name: str, **labels: str) -> Optional[float]:
    """The value of the one sample matching ``name`` and ``labels``, or ``None``."""
    for sample in samples(exposition, name):
        if all(sample.labels.get(key) == value for key, value in labels.items()):
            return sample.value
    return None


def metric_names(exposition: str) -> set[str]:
    """Every family name in the scrape."""
    return {family.name for family in families(exposition)}


async def run_query(client: httpx.AsyncClient, document: str, **variables: Any) -> dict[str, Any]:
    """POST one operation and return the parsed envelope."""
    body: dict[str, Any] = {"query": document}
    if variables:
        body["variables"] = variables
    response = await client.post("/graphql", json=body)
    assert response.status_code == 200, response.text
    return response.json()


# =================================================================================================
# The endpoint itself
# =================================================================================================


async def test_the_endpoint_answers_a_parseable_prometheus_exposition(
    metrics_client: httpx.AsyncClient,
) -> None:
    """200, the right content type, and a body ``prometheus_client`` itself can read back.

    The content type is asserted exactly rather than loosely: Prometheus negotiates on it, and a
    scrape served as ``application/json`` is a target that silently reports nothing.
    """
    exposition = await scrape(metrics_client)

    parsed = families(exposition)

    assert parsed, "the exposition parsed to no metric families at all"
    assert "gql_operation_duration_seconds" in metric_names(exposition)
    assert "gql_field_duration_seconds" in metric_names(exposition)


async def test_every_family_this_project_publishes_is_present_after_startup(
    metrics_client: httpx.AsyncClient,
) -> None:
    """The names are a contract: C12's E2E verifier greps for them and a dashboard depends on them.

    Asserted against a *live* application rather than a hand-built registry, because half of these
    families only exist when the lifespan has bound the broker, the cache and the persisted query
    store to the scrape-time collector — which is exactly the wiring that can be missing.
    """
    names = metric_names(await scrape(metrics_client))

    assert {
        "gql_operation_duration_seconds",
        "gql_field_duration_seconds",
        "gql_active_subscriptions",
        "gql_errors",
        "gql_broker_published",
        "gql_broker_dropped",
        "gql_cache_hits",
        "gql_cache_misses",
        "gql_cache_enabled",
        "gql_persisted_query_hits",
        "gql_persisted_query_misses",
        "gql_persisted_queries_enabled",
    } <= names, sorted(names)


# =================================================================================================
# Spec item 37, clause 1 — query execution time
# =================================================================================================


async def test_the_operation_histogram_fills_from_real_traffic(
    seeded: list[LogRecord], metrics_client: httpx.AsyncClient
) -> None:
    """A query that really ran leaves a sample, labelled by its own name and its outcome.

    Both labels are checked, and the ``_sum`` is required to be positive: a histogram wired to
    ``observe(0)`` would produce a count and no duration at all, which reads as "instant" on every
    dashboard ever built on it.
    """
    before = await scrape(metrics_client)
    assert (
        sample_value(
            before,
            "gql_operation_duration_seconds_count",
            operation="Timed",
            operation_type="query",
            outcome="success",
        )
        is None
    ), "the series existed before the operation ran"

    payload = await run_query(metrics_client, "query Timed { logs(filters: {limit: 2}) { id } }")
    assert len(payload["data"]["logs"]) == 2

    after = await scrape(metrics_client)

    assert (
        sample_value(
            after,
            "gql_operation_duration_seconds_count",
            operation="Timed",
            operation_type="query",
            outcome="success",
        )
        == 1.0
    )
    total = sample_value(
        after,
        "gql_operation_duration_seconds_sum",
        operation="Timed",
        operation_type="query",
        outcome="success",
    )
    assert total is not None and total > 0.0, "the operation was recorded as taking no time at all"


async def test_a_failed_operation_is_recorded_with_an_error_outcome(
    metrics_client: httpx.AsyncClient
) -> None:
    """``outcome`` is what separates "the endpoint is fast" from "the endpoint fails fast"."""
    await run_query(metrics_client, "query Broken { logs(filters: {level: NOT_A_LEVEL}) { id } }")

    exposition = await scrape(metrics_client)

    assert (
        sample_value(
            exposition,
            "gql_operation_duration_seconds_count",
            operation="Broken",
            outcome="error",
        )
        == 1.0
    )
    assert (
        sample_value(
            exposition,
            "gql_operation_duration_seconds_count",
            operation="Broken",
            outcome="success",
        )
        is None
    )


async def test_an_anonymous_operation_is_recorded_under_a_stable_label(
    seeded: list[LogRecord], metrics_client: httpx.AsyncClient
) -> None:
    """The spec's own acceptance command is anonymous, so it must not vanish from the histogram."""
    await run_query(metrics_client, "{ logs { id service level message } }")

    exposition = await scrape(metrics_client)

    assert (
        sample_value(
            exposition,
            "gql_operation_duration_seconds_count",
            operation=ANONYMOUS_OPERATION_LABEL,
            operation_type="query",
            outcome="success",
        )
        == 1.0
    )


# =================================================================================================
# Spec item 37, clause 2 — per-field resolution time
# =================================================================================================


async def test_only_the_fields_that_actually_resolved_have_samples(
    seeded: list[LogRecord], metrics_client: httpx.AsyncClient
) -> None:
    """**The assertion that distinguishes a working hook from an enumerating one.**

    ``id`` and ``service`` were selected, so they have samples; ``message``, ``timestamp``,
    ``metadata``, ``traceId`` and ``relatedLogs`` were not, so they must have **no series at all**.
    A hook that reported every field of every type touched would pass a test that only checked the
    positive half, and would also be the cardinality bomb the module docstring of
    :mod:`src.metrics` argues against.

    The count on ``LogEntry.id`` is pinned to the page size, not merely to "greater than zero": the
    hook fires once per *resolution*, so three rows must produce three samples. A hook attached at
    the type level rather than the field level would produce one.
    """
    await run_query(metrics_client, SELECTED)

    exposition = await scrape(metrics_client)

    assert (
        sample_value(
            exposition, "gql_field_duration_seconds_count", parent_type="Query", field="logs"
        )
        == 1.0
    )
    for field in ("id", "service"):
        assert (
            sample_value(
                exposition,
                "gql_field_duration_seconds_count",
                parent_type="LogEntry",
                field=field,
            )
            == 3.0
        ), f"LogEntry.{field} resolved once per row, so it should carry one sample per row"

    for field in NOT_SELECTED:
        assert (
            sample_value(
                exposition,
                "gql_field_duration_seconds_count",
                parent_type="LogEntry",
                field=field,
            )
            is None
        ), f"LogEntry.{field} was never selected, so it must have no time series"

    assert (
        sample_value(
            exposition, "gql_field_duration_seconds_count", parent_type="Query", field="logStats"
        )
        is None
    )


async def test_introspection_is_not_timed_field_by_field(
    metrics_client: httpx.AsyncClient,
) -> None:
    """GraphiQL sends a deep introspection query on every page load.

    Exempt for the same reason :mod:`src.graphql.cost` exempts it from pricing: bounded by the
    schema, unwidenable by a client, and touching no database. Timing it would let one page refresh
    dominate the field histogram and would add a series for every introspection field in the spec.
    """
    payload = await run_query(
        metrics_client, "{ __schema { types { name fields { name } } } }"
    )
    assert payload["data"]["__schema"]["types"]

    exposition = await scrape(metrics_client)
    field_samples = samples(exposition, "gql_field_duration_seconds_count")

    assert field_samples == [], f"introspection was timed: {field_samples!r}"


# =================================================================================================
# Spec item 37, clause 3 — active subscription connections
# =================================================================================================


def test_the_active_subscription_gauge_follows_a_real_socket_up_and_back_down(
    clean_store: None, db_settings: Settings
) -> None:
    """A real ``graphql-transport-ws`` upgrade, not a call to ``inc()``.

    The gauge is not a tally this module keeps: it is
    :attr:`src.broker.BrokerStats.active_subscribers` read at scrape time, which is the same number
    ``MAX_SUBSCRIPTIONS_PER_CONNECTION`` is enforced against. So this test is really asserting that
    the exposition mirrors the broker rather than shadowing it — the "one number per fact" rule from
    :mod:`src.metrics`.

    **The ``TestClient`` must be entered as a context manager**, and not only for the lifespan: an
    un-entered client starts a fresh portal (a second event loop, in a second thread) per call, so
    the WebSocket session and the HTTP scrape would run on different loops and the broker's queues
    would be woken across threads. Entered, everything shares one loop.

    The "back down" half is what makes this a test of deregistration rather than of registration —
    a subscription that never releases its slot is how ``MAX_SUBSCRIPTIONS_PER_CONNECTION`` starts
    rejecting a reconnecting client that holds nothing.
    """
    settings = make_settings(
        subscription_channel=f"test:metrics-sub:{uuid4().hex}", metrics_enabled=True
    )
    app = create_app(settings=settings)

    with TestClient(app) as client:
        broker = app.state.broker

        assert _gauge(client) == 0.0, "the gauge did not start at zero"

        with client.websocket_connect("/graphql", subprotocols=[GRAPHQL_TRANSPORT_WS]) as session:
            session.send_json({"type": "connection_init", "payload": {}})
            ack = _receive(session)
            assert ack["type"] == "connection_ack", ack

            session.send_json({"id": "sub-1", "type": "subscribe", "payload": {"query": LOG_STREAM}})
            _wait_for(lambda: broker.subscriber_count() == 1)

            assert _gauge(client) == 1.0, "an open subscription is not reported by the gauge"
            assert _gauge(client) == float(broker.stats.active_subscribers), (
                "the gauge disagrees with the broker counter it is supposed to mirror"
            )

        _wait_for(lambda: broker.subscriber_count() == 0)

        assert _gauge(client) == 0.0, "the gauge never came back down after the socket closed"

        # A subscription's operation hook wraps the whole stream, so its duration is the socket's
        # lifetime rather than an execution time. It is deliberately kept out of the operation
        # histogram — see MetricsExtension — and this is where that stays true.
        exposition = client.get("/metrics").text
        subscription_samples = [
            sample
            for sample in samples(exposition, "gql_operation_duration_seconds_count")
            if sample.labels.get("operation_type") == "subscription"
        ]
        assert subscription_samples == [], (
            "a subscription's socket lifetime was recorded as an operation duration"
        )


def _gauge(client: TestClient) -> Optional[float]:
    """The active-subscription gauge, read through the real HTTP endpoint."""
    response = client.get("/metrics")
    assert response.status_code == 200, response.text
    return sample_value(response.text, "gql_active_subscriptions")


def _receive(session: Any, *, timeout: float = DEADLINE_SECONDS) -> dict[str, Any]:  # noqa: ANN401
    """One protocol frame, or a failure after ``timeout``.

    ``WebSocketTestSession.receive_json`` blocks with no deadline, so a regression that never sent
    the expected frame would stop the suite rather than fail it. The read happens on a **daemon**
    thread so a receive that never returns cannot keep the interpreter alive at exit.
    """
    box: dict[str, Any] = {}

    def _pull() -> None:
        try:
            box["message"] = session.receive_json()
        except BaseException as exc:  # noqa: BLE001 - re-raised on the calling thread
            box["error"] = exc

    reader = threading.Thread(target=_pull, daemon=True, name="ws-receive")
    reader.start()
    reader.join(timeout)
    if reader.is_alive():
        raise AssertionError(f"no WebSocket frame arrived within {timeout}s")
    if "error" in box:
        raise box["error"]
    return box["message"]


def _wait_for(predicate: Any, *, timeout: float = DEADLINE_SECONDS) -> None:  # noqa: ANN401
    """Poll ``predicate`` until it holds, or fail.

    A subscription registers when its generator first runs, which is some number of event-loop turns
    after the ``subscribe`` frame is written — so waiting on the condition is the only way to make
    this deterministic. A sleep would be a guess about how long is long enough.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"condition was not reached within {timeout}s")


# =================================================================================================
# The mirrored counters
# =================================================================================================


async def test_the_cache_counters_agree_with_the_cache_after_a_known_sequence(
    seeded: list[LogRecord], make_metrics_app: Any  # noqa: ANN401
) -> None:
    """One miss, then one hit — and the exposition equals :attr:`ResultCache.stats` exactly.

    Graded against the cache's own counters rather than against literals. That is the property C9
    chose: the metrics layer **mirrors** C7's counters at scrape time instead of incrementing beside
    them, so the two cannot drift. A test that asserted ``== 1`` would pass against a duplicated
    counter that had started to diverge, which is the failure mode this design exists to make
    impossible.

    The filter carries a fresh UUID so the key is cold: the compose ``test`` service does not flush
    Redis between runs and the cache TTL is 30 seconds, so a fixed filter would be a hit on the
    first request of a second ``make test`` inside that window.
    """
    app = make_metrics_app(cache_enabled=True)
    document = "query Cached($text: String!) { logs(filters: {searchText: $text, limit: 5}) { id } }"

    async with app.router.lifespan_context(app):
        cache = app.state.cache
        assert isinstance(cache, ResultCache)
        assert cache.enabled is True, "this test is meaningless unless the cache is really on"

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://graphql-cachem.test") as c:
            marker = uuid4().hex
            first = await run_query(c, document, text=marker)
            second = await run_query(c, document, text=marker)

            assert first == second
            exposition = await scrape(c)

        stats = cache.stats
        assert (stats.misses, stats.hits) == (1, 1), (
            "the two identical queries did not produce exactly one miss and one hit"
        )
        assert sample_value(exposition, "gql_cache_hits_total") == float(stats.hits)
        assert sample_value(exposition, "gql_cache_misses_total") == float(stats.misses)
        assert sample_value(exposition, "gql_cache_errors_total") == float(stats.errors)
        assert sample_value(exposition, "gql_cache_enabled") == 1.0


async def test_the_persisted_query_counters_agree_with_the_store(
    seeded: list[LogRecord], metrics_client: httpx.AsyncClient, metrics_app: FastAPI
) -> None:
    """The same mirroring rule, applied to C9's own store.

    Driven through the real APQ handshake rather than by poking the store, so what is asserted is
    that a *request* moved both the store's counters and the exposition — and that they agree.
    """
    document = f"query Pq_{uuid4().hex} {{ logs(filters: {{limit: 1}}) {{ id }} }}"
    digest = compute_query_hash(document)
    extension = {"persistedQuery": {"version": 1, "sha256Hash": digest}}

    await metrics_client.post("/graphql", json={"extensions": extension})
    await metrics_client.post("/graphql", json={"query": document, "extensions": extension})
    await metrics_client.post("/graphql", json={"extensions": extension})

    exposition = await scrape(metrics_client)
    stats = metrics_app.state.apq.stats

    assert (stats.hits, stats.misses, stats.registered) == (1, 1, 1)
    assert sample_value(exposition, "gql_persisted_query_hits_total") == float(stats.hits)
    assert sample_value(exposition, "gql_persisted_query_misses_total") == float(stats.misses)
    assert sample_value(exposition, "gql_persisted_query_registered_total") == float(
        stats.registered
    )
    assert sample_value(exposition, "gql_persisted_queries_enabled") == 1.0


async def test_the_broker_counters_agree_with_the_broker_after_a_mutation(
    seeded: list[LogRecord], metrics_client: httpx.AsyncClient, metrics_app: FastAPI
) -> None:
    """``createLog`` publishes to the broker, and the exposition reports what the broker counted."""
    mutation = """
    mutation Created($data: CreateLogInput!) { createLog(logData: $data) { id service } }
    """
    payload = await run_query(
        metrics_client,
        mutation,
        data={"service": "metrics-svc", "level": "INFO", "message": "published for metrics"},
    )
    assert payload["data"]["createLog"]["service"] == "metrics-svc"

    exposition = await scrape(metrics_client)
    stats = metrics_app.state.broker.stats

    assert stats.published_total == 1
    assert sample_value(exposition, "gql_broker_published_total") == float(stats.published_total)
    assert sample_value(exposition, "gql_broker_dropped_total") == float(stats.dropped_total)
    assert sample_value(exposition, "gql_active_subscriptions") == 0.0


# =================================================================================================
# The error counter — where C8's rejections land
# =================================================================================================


async def test_a_cost_gate_rejection_increments_the_error_counter_under_its_own_code(
    metrics_client: httpx.AsyncClient,
) -> None:
    """C8 has no idea metrics exist, and it does not need to.

    The rejection is reported as a :class:`~src.graphql.errors.DomainError` carrying
    ``COST_LIMIT_EXCEEDED``, and the metrics extension — which sits **inside**
    ``MaskInternalErrors``, so it sees the code before masking could replace it — counts it by that
    code. The label set is therefore exactly :class:`~src.graphql.errors.ErrorCode`, which is closed.
    """
    response = await metrics_client.post("/graphql", json={"query": OVER_BUDGET})
    assert response.status_code == 200
    assert response.json()["errors"][0]["extensions"]["code"] == ErrorCode.COST_LIMIT_EXCEEDED.value

    exposition = await scrape(metrics_client)

    assert (
        sample_value(exposition, "gql_errors_total", code=ErrorCode.COST_LIMIT_EXCEEDED.value)
        == 1.0
    )
    assert (
        sample_value(exposition, "gql_errors_total", code=ErrorCode.INTERNAL_ERROR.value) is None
    ), "a client-side rejection was counted as a server fault"


async def test_a_persisted_query_miss_is_counted_under_its_own_code(
    metrics_client: httpx.AsyncClient,
) -> None:
    """The other refusal this commit introduces, and it must be distinguishable from the first."""
    digest = "a" * 64

    await metrics_client.post(
        "/graphql", json={"extensions": {"persistedQuery": {"version": 1, "sha256Hash": digest}}}
    )

    exposition = await scrape(metrics_client)

    assert (
        sample_value(
            exposition, "gql_errors_total", code=ErrorCode.PERSISTED_QUERY_NOT_FOUND.value
        )
        == 1.0
    )


# =================================================================================================
# Cardinality
# =================================================================================================


async def test_a_client_cannot_mint_time_series_without_limit(
    metrics_client: httpx.AsyncClient,
) -> None:
    """**The failure that does not show up until the process has been running for a week.**

    An operation name is free text, so a client sending a fresh one per request would create a time
    series per request — each of which lives for the life of the process. Past the cap every further
    name collapses into a single ``other`` series, so the family's size stops growing while the
    operations an application really sends keep their own labels.

    ``{ __typename }`` is used as the body so the loop costs no database work at all: what is being
    exercised is the label, not the resolver.
    """
    for index in range(MAX_OPERATION_LABELS + 20):
        await run_query(metrics_client, f"query Flood_{index} {{ __typename }}")

    exposition = await scrape(metrics_client)
    labels = {
        sample.labels["operation"]
        for sample in samples(exposition, "gql_operation_duration_seconds_count")
    }

    assert OTHER_OPERATION_LABEL in labels, "the cap never engaged"
    assert len(labels) <= MAX_OPERATION_LABELS + 2, (
        f"{len(labels)} distinct operation labels for {MAX_OPERATION_LABELS + 20} distinct names"
    )


async def test_no_label_carries_anything_a_client_controls_without_bound(
    seeded: list[LogRecord], metrics_client: httpx.AsyncClient
) -> None:
    """No query strings, no ids, no response paths — asserted over a real scrape.

    Three independent checks, because each catches a different mistake:

    * the **label names** are a fixed set, so a future metric cannot quietly add ``trace_id``;
    * no label **value** contains GraphQL syntax, which is what a document leaking into a label
      would look like;
    * no label value is longer than the truncation bound, which is what an id or a raw filter would
      typically exceed.
    """
    await run_query(
        metrics_client,
        "query Labelled($limit: Int!) { logs(filters: {limit: $limit}) { id message } }",
        limit=2,
    )
    await metrics_client.post("/graphql", json={"query": OVER_BUDGET})

    exposition = await scrape(metrics_client)
    label_names: set[str] = set()
    label_values: set[str] = set()
    for family in families(exposition):
        for sample in family.samples:
            label_names.update(sample.labels)
            label_values.update(str(value) for value in sample.labels.values())

    assert label_names <= {
        "operation",
        "operation_type",
        "outcome",
        "parent_type",
        "field",
        "code",
        # `le` is the histogram bucket boundary, which prometheus_client owns and which is bounded
        # by the bucket list declared in src/metrics.py.
        "le",
    }, sorted(label_names)

    for value in label_values:
        assert "{" not in value and "}" not in value, f"a GraphQL document leaked into a label: {value!r}"
        assert len(value) <= 64, f"an unbounded value became a label: {value!r}"


# =================================================================================================
# METRICS_ENABLED=false
# =================================================================================================


async def test_metrics_disabled_does_not_register_the_route_or_build_a_registry(
    seeded: list[LogRecord], make_metrics_app: Any  # noqa: ANN401
) -> None:
    """"Disabled" is indistinguishable from "not built", and the API is completely unaffected.

    A **404**, deliberately, rather than an empty 200: that is the shape ``GET /graphql`` takes when
    the playground is disabled, and it is honest. An empty 200 would be a scrape target that reports
    zero series forever and looks, on a dashboard, exactly like a service that has served no
    traffic.
    """
    app = make_metrics_app(metrics_enabled=False)

    async with app.router.lifespan_context(app):
        assert app.state.metrics is None, "a disabled registry must not be built at all"

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://graphql-nom.test") as c:
            scrape_response = await c.get("/metrics")
            payload = await run_query(c, "query Untouched { logs(filters: {limit: 2}) { id } }")
            health = await c.get("/health")

    assert scrape_response.status_code == 404
    assert len(payload["data"]["logs"]) == 2, "disabling metrics changed what the API returns"
    assert health.json() == {"status": "healthy"}, "/health stays dependency-free either way"


async def test_the_registry_is_per_application(
    make_metrics_app: Any,  # noqa: ANN401
) -> None:
    """Two applications in one process report independently and cannot see each other's traffic.

    Which is not a test convenience: it is why the registry is an explicit ``CollectorRegistry``
    built in the lifespan rather than ``prometheus_client``'s module-level default, where the second
    application constructed would raise ``Duplicated timeseries``.
    """
    first = make_metrics_app()
    second = make_metrics_app()

    async with first.router.lifespan_context(first):
        async with second.router.lifespan_context(second):
            assert isinstance(first.state.metrics, Metrics)
            assert isinstance(second.state.metrics, Metrics)
            assert first.state.metrics.registry is not second.state.metrics.registry

            transport = httpx.ASGITransport(app=first)
            async with httpx.AsyncClient(transport=transport, base_url="http://one.test") as c:
                await run_query(c, "query OnlyOnFirst { __typename }")
                first_exposition = await scrape(c)

            transport = httpx.ASGITransport(app=second)
            async with httpx.AsyncClient(transport=transport, base_url="http://two.test") as c:
                second_exposition = await scrape(c)

    assert (
        sample_value(
            first_exposition, "gql_operation_duration_seconds_count", operation="OnlyOnFirst"
        )
        == 1.0
    )
    assert (
        sample_value(
            second_exposition, "gql_operation_duration_seconds_count", operation="OnlyOnFirst"
        )
        is None
    ), "one application's traffic showed up in another's scrape"
