"""The metrics layer as pure logic — spec §2 item 37, §5 "monitoring and health checks working".

No app, no HTTP, no schema: a :class:`~src.metrics.Metrics` is buildable from nothing and its
sources are duck-typed on a single ``stats`` property, which is what lets this module assert the
things a live scrape cannot show cheaply.

Three claims are worth having here and are hard to make anywhere else:

* **The disabled hook costs nothing.** ``METRICS_ENABLED=false`` must not merely produce no samples
  — the per-field hook runs on every field of every request, so it must not even read the clock.
  That is asserted by removing :func:`time.perf_counter` from the module and requiring the disabled
  path to survive, which no timing measurement could establish as reliably.
* **The guarded import degrades to a no-op.** A broken ``prometheus_client`` must leave a running
  service, not an import-time crash — and ``src/main.py`` imports the GraphQL package, so an
  ``ImportError`` here is a container that will not boot rather than a missing dashboard.
* **The label sets are bounded.** Cardinality is the only way a metrics layer takes a server down,
  and the bound is a property of the code rather than of any particular traffic pattern.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest

from graphql import GraphQLError

from src.config import Settings
from src.graphql.errors import CostLimitExceededError, ErrorCode
from src.metrics import (
    ANONYMOUS_OPERATION_LABEL,
    MAX_OPERATION_LABELS,
    OTHER_OPERATION_LABEL,
    PROMETHEUS_CONTENT_TYPE,
    Metrics,
    MetricsExtension,
    create_metrics,
)


def make_settings(*, metrics_enabled: bool = True) -> Settings:
    """Settings for a metrics layer under test, built directly rather than from the environment."""
    return Settings(
        _env_file=None,
        seed_entries=0,
        seed_orders=0,
        log_level="WARNING",
        cache_enabled=False,
        metrics_enabled=metrics_enabled,
    )


# --- Duck-typed stand-ins for the three sources the collector mirrors ----------------------------
#
# Each of these mimics exactly the surface `_SourceCollector` reads: an object with a `stats`
# property returning something with the documented attribute names. Using stubs rather than a real
# broker/cache/store is what makes the assertions below about the MIRRORING — "the exposition equals
# the object" — rather than about whether a broker can count.


@dataclass(frozen=True)
class FakeBrokerStats:
    active_subscribers: int = 0
    published_total: int = 0
    delivered_total: int = 0
    dropped_total: int = 0
    remote_published_total: int = 0
    remote_received_total: int = 0
    remote_suppressed_total: int = 0
    remote_invalid_total: int = 0
    redis_errors_total: int = 0


@dataclass(frozen=True)
class FakeCacheStats:
    enabled: bool = True
    hits: int = 0
    misses: int = 0
    errors: int = 0
    coalesced: int = 0
    bypassed: int = 0
    inflight: int = 0


@dataclass(frozen=True)
class FakePersistedQueryStats:
    enabled: bool = True
    hits: int = 0
    misses: int = 0
    registered: int = 0
    mismatches: int = 0
    protocol_errors: int = 0
    oversized: int = 0
    errors: int = 0


class FakeSource:
    """Anything with a ``stats`` property. The only contract the collector depends on."""

    def __init__(self, stats: Any) -> None:  # noqa: ANN401
        self._stats = stats

    @property
    def stats(self) -> Any:  # noqa: ANN401
        return self._stats


class FakeInfo:
    """The two attributes :meth:`MetricsExtension.resolve` reads off a resolve info."""

    def __init__(self, parent_type: str, field_name: str) -> None:
        self.parent_type = type("ParentType", (), {"name": parent_type})()
        self.field_name = field_name


def exposition(metrics: Metrics) -> str:
    """The rendered scrape as text, with the content type asserted on the way past."""
    body, content_type = metrics.render()
    assert content_type == PROMETHEUS_CONTENT_TYPE
    return body.decode("utf-8")


def sample_value(metrics: Metrics, name: str, **labels: str) -> Optional[float]:
    """One sample's value out of the registry, or ``None`` when the series does not exist.

    ``get_sample_value`` is ``prometheus_client``'s own reader, so this asks the registry the same
    question a scrape does rather than parsing text with a regular expression.
    """
    return metrics.registry.get_sample_value(name, labels or None)


# =================================================================================================
# Construction
# =================================================================================================


def test_the_registry_is_this_objects_own_and_not_the_global_default() -> None:
    """Two applications in one process must not share a registry — or declare into one twice.

    The integration suite builds several apps per session. Module-level metrics against
    ``prometheus_client``'s default registry would raise ``Duplicated timeseries`` on the second
    construction, which is a test failure standing in for a real property: each app reports its own
    traffic and cannot see another's.
    """
    first = Metrics()
    second = Metrics()

    assert first.registry is not second.registry
    assert "gql_operation_duration_seconds" in exposition(first)


def test_every_published_metric_name_is_present_and_prefixed() -> None:
    """The names are a contract: C12's E2E verifier and any dashboard grep for exactly these.

    Asserted against a rendered scrape with all three sources bound, because a family produced by
    the scrape-time collector does not exist until something collects it — so a test that inspected
    the object would miss precisely the half of the surface that is computed rather than declared.
    """
    metrics = Metrics(
        broker=FakeSource(FakeBrokerStats()),
        cache=FakeSource(FakeCacheStats()),
        persisted_queries=FakeSource(FakePersistedQueryStats()),
    )
    metrics.observe_operation(
        name="Logs", operation_type="query", outcome="success", seconds=0.01
    )
    metrics.observe_field("LogEntry", "id", 0.0001)
    metrics.count_error("VALIDATION_ERROR")

    text = exposition(metrics)

    for name in (
        # Spec item 37, all three clauses.
        "gql_operation_duration_seconds",
        "gql_field_duration_seconds",
        "gql_active_subscriptions",
        # The taxonomy counter C8's cost rejections and C9's persisted-query misses land in.
        "gql_errors_total",
        # C6 broker counters.
        "gql_broker_published_total",
        "gql_broker_delivered_total",
        "gql_broker_dropped_total",
        "gql_broker_remote_published_total",
        "gql_broker_remote_received_total",
        "gql_broker_remote_suppressed_total",
        "gql_broker_remote_invalid_total",
        "gql_broker_redis_errors_total",
        # C7 cache counters.
        "gql_cache_hits_total",
        "gql_cache_misses_total",
        "gql_cache_errors_total",
        "gql_cache_coalesced_total",
        "gql_cache_bypassed_total",
        "gql_cache_inflight",
        "gql_cache_enabled",
        # C9 persisted query counters.
        "gql_persisted_query_hits_total",
        "gql_persisted_query_misses_total",
        "gql_persisted_query_registered_total",
        "gql_persisted_query_mismatches_total",
        "gql_persisted_query_protocol_errors_total",
        "gql_persisted_query_oversized_total",
        "gql_persisted_query_errors_total",
        "gql_persisted_queries_enabled",
    ):
        assert name in text, f"{name} is missing from the exposition"

    assert all(
        line.split("{")[0].split(" ")[0].startswith("gql_")
        for line in text.splitlines()
        if line and not line.startswith("#")
    ), "every sample must carry the project prefix"


def test_absent_sources_simply_omit_their_families() -> None:
    """A cache-less or broker-less application still scrapes; it just reports less.

    Which matters because the unit suite and any app assembled without a lifespan have neither, and
    a collector that assumed they existed would turn a scrape into an exception — and a collector
    that raises empties the *whole* exposition, not just its own families.
    """
    metrics = Metrics()

    text = exposition(metrics)

    assert "gql_operation_duration_seconds" in text
    assert "gql_active_subscriptions" not in text
    assert "gql_cache_hits_total" not in text
    assert "gql_persisted_query_hits_total" not in text


def test_the_collector_mirrors_its_sources_rather_than_keeping_its_own_tally() -> None:
    """**One number per fact.** The exposition is computed from the object, so it cannot drift.

    Asserted by mutating the source *after* the metrics object was built: a scrape-time collector
    reports the new values, while anything that had copied them at construction would keep
    reporting the old ones. That is the difference between mirroring the C6/C7/C9 counters and
    double-counting beside them.
    """
    broker = FakeSource(FakeBrokerStats(active_subscribers=3, published_total=11, dropped_total=2))
    cache = FakeSource(FakeCacheStats(hits=7, misses=4, errors=1, inflight=2, enabled=True))
    apq = FakeSource(FakePersistedQueryStats(hits=5, misses=6, registered=6, enabled=False))
    metrics = Metrics(broker=broker, cache=cache, persisted_queries=apq)

    assert sample_value(metrics, "gql_active_subscriptions") == 3.0
    assert sample_value(metrics, "gql_broker_published_total") == 11.0
    assert sample_value(metrics, "gql_broker_dropped_total") == 2.0
    assert sample_value(metrics, "gql_cache_hits_total") == 7.0
    assert sample_value(metrics, "gql_cache_misses_total") == 4.0
    assert sample_value(metrics, "gql_cache_errors_total") == 1.0
    assert sample_value(metrics, "gql_cache_inflight") == 2.0
    assert sample_value(metrics, "gql_cache_enabled") == 1.0
    assert sample_value(metrics, "gql_persisted_query_hits_total") == 5.0
    assert sample_value(metrics, "gql_persisted_query_misses_total") == 6.0
    assert sample_value(metrics, "gql_persisted_queries_enabled") == 0.0

    metrics.bind(broker=FakeSource(FakeBrokerStats(active_subscribers=0, published_total=12)))

    assert sample_value(metrics, "gql_active_subscriptions") == 0.0
    assert sample_value(metrics, "gql_broker_published_total") == 12.0


def test_a_source_that_raises_does_not_empty_the_whole_scrape() -> None:
    """One broken source must cost its own families and nothing else.

    A collector that propagated would take down the operation histogram, the field histogram and
    every other source's counters with it — turning a broken sub-component into a target that looks
    entirely dead.
    """

    class ExplodingSource:
        @property
        def stats(self) -> Any:  # noqa: ANN401
            raise RuntimeError("this source is broken")

    metrics = Metrics(
        broker=ExplodingSource(),
        cache=FakeSource(FakeCacheStats(hits=9)),
        persisted_queries=FakeSource(FakePersistedQueryStats(hits=4)),
    )
    metrics.observe_operation(name=None, operation_type="query", outcome="success", seconds=0.02)

    text = exposition(metrics)

    assert "gql_active_subscriptions" not in text
    assert sample_value(metrics, "gql_cache_hits_total") == 9.0
    assert sample_value(metrics, "gql_persisted_query_hits_total") == 4.0
    assert "gql_operation_duration_seconds" in text


# =================================================================================================
# Cardinality — the only way a metrics layer takes a server down
# =================================================================================================


def test_the_operation_label_is_capped_so_a_client_cannot_mint_series_forever() -> None:
    """``query <anything> {…}`` is free text, so the label it becomes has to be bounded.

    Past the cap every further name collapses to a single ``other`` series. Names already seen keep
    working, which is what makes the cap safe: the operations an application actually sends are
    registered in its first few seconds and keep their own series forever.
    """
    metrics = Metrics()

    admitted = [metrics.operation_label(f"Operation{index}") for index in range(MAX_OPERATION_LABELS)]
    overflow = [metrics.operation_label(f"Extra{index}") for index in range(50)]

    assert admitted == [f"Operation{index}" for index in range(MAX_OPERATION_LABELS)]
    assert set(overflow) == {OTHER_OPERATION_LABEL}
    # An already-admitted name is still reported by name after the cap is reached.
    assert metrics.operation_label("Operation0") == "Operation0"


def test_an_anonymous_operation_gets_its_own_stable_label() -> None:
    """``{ logs { id } }`` has no name, and "" would be an unreadable label on a dashboard."""
    metrics = Metrics()

    assert metrics.operation_label(None) == ANONYMOUS_OPERATION_LABEL
    assert metrics.operation_label("") == ANONYMOUS_OPERATION_LABEL
    # The anonymous label is not a name, so it must not consume one of the capped slots.
    assert metrics.operation_label("Named") == "Named"


def test_an_absurdly_long_operation_name_is_truncated() -> None:
    """A name is client-supplied text and would otherwise appear in full on every scrape line."""
    metrics = Metrics()

    label = metrics.operation_label("N" * 5_000)

    assert len(label) <= 64
    assert label.startswith("NNNN")


def test_no_metric_is_labelled_with_anything_a_client_can_make_unbounded() -> None:
    """The label NAMES are pinned, which is how "no query strings, no ids" is enforced.

    ``parent_type``/``field`` are schema coordinates — a client cannot invent a type or a field, so
    that family's series count is fixed at deploy time. ``operation`` is capped above. Nothing
    anywhere is labelled with a document, a variable value, a log id or a trace id.
    """
    metrics = Metrics()

    assert list(metrics.operation_duration._labelnames) == [
        "operation",
        "operation_type",
        "outcome",
    ]
    assert list(metrics.field_duration._labelnames) == ["parent_type", "field"]
    assert list(metrics.errors._labelnames) == ["code"]


# =================================================================================================
# The resolve hook — the one place where an extra allocation is measurable
# =================================================================================================


class FakeExecutionContext:
    """Enough of Strawberry's ``ExecutionContext`` for the extension's hooks."""

    def __init__(self, context: Any = None) -> None:  # noqa: ANN401
        self.context = context
        self.errors = None
        self.result = None


class FakeGraphQLContext:
    """A context object carrying (or not carrying) a metrics registry."""

    def __init__(self, metrics: Optional[Metrics]) -> None:
        self.metrics = metrics


def make_extension(metrics: Optional[Metrics]) -> MetricsExtension:
    """An extension wired the way Strawberry wires one: constructed bare, hooks driven by hand.

    Strawberry constructs every extension with **no arguments** and assigns ``execution_context``
    afterwards (see the factory note in :mod:`src.graphql.schema`), so this mirrors that exactly —
    a helper that passed constructor arguments would be testing an arrangement the server does not
    use.
    """
    extension = MetricsExtension()
    extension.execution_context = FakeExecutionContext(FakeGraphQLContext(metrics))
    return extension


def drive(extension: MetricsExtension) -> None:
    """Run the operation hook start to finish, as Strawberry's runner does."""
    hook = extension.on_operation()
    next(hook)
    with pytest.raises(StopIteration):
        next(hook)


def test_the_disabled_resolve_hook_does_not_even_read_the_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``METRICS_ENABLED=false`` must be free, not merely silent.

    This hook runs on **every field of every request** — 700 times for the flagship correlated
    query at the default page size — so "records nothing" is not the requirement; "does no work" is.
    Asserted by replacing :func:`time.perf_counter` in the module with something that fails the test
    if called, which is a far stronger statement than any timing measurement: a disabled path that
    took a timestamp and threw it away would pass a benchmark and fail here.
    """

    def forbidden() -> float:
        raise AssertionError("the disabled metrics hook read the clock")

    monkeypatch.setattr("src.metrics.perf_counter", forbidden)

    extension = make_extension(None)
    drive(extension)

    sentinel = object()
    result = extension.resolve(lambda root, info, **kwargs: sentinel, None, FakeInfo("Query", "logs"))

    assert result is sentinel, "the disabled hook must pass the resolver's value through untouched"


def test_the_enabled_resolve_hook_records_the_field_by_its_schema_coordinate() -> None:
    """Spec item 37, second clause — labelled ``LogEntry.relatedLogs``, never ``logs.7.relatedLogs``.

    The response path would embed list indices and mint a series per row; the schema coordinate is
    bounded by the schema. See the cardinality note in :mod:`src.metrics`.
    """
    metrics = Metrics()
    extension = make_extension(metrics)
    drive(extension)

    extension.resolve(lambda root, info, **kwargs: 42, None, FakeInfo("LogEntry", "relatedLogs"))

    count = sample_value(
        metrics, "gql_field_duration_seconds_count", parent_type="LogEntry", field="relatedLogs"
    )
    assert count == 1.0
    assert (
        sample_value(
            metrics, "gql_field_duration_seconds_count", parent_type="LogEntry", field="message"
        )
        is None
    ), "a field that never resolved must have no series at all"


async def test_an_async_resolver_is_timed_across_the_await_and_still_returns_its_value() -> None:
    """A field that awaits must be timed for the whole resolution, not for coroutine creation.

    Timing at the point ``_next`` returns would record the same tiny number for every async field in
    the schema — which is the one measurement the per-field histogram exists to avoid.
    """
    metrics = Metrics()
    extension = make_extension(metrics)
    drive(extension)

    async def resolver(root: Any, info: Any, **kwargs: Any) -> str:  # noqa: ANN401
        return "resolved"

    awaited = extension.resolve(resolver, None, FakeInfo("Query", "logs"))
    assert await awaited == "resolved"

    assert (
        sample_value(metrics, "gql_field_duration_seconds_count", parent_type="Query", field="logs")
        == 1.0
    )


async def test_a_resolver_that_raises_is_still_timed() -> None:
    """A field that fails slowly is exactly what an operator is looking for.

    Dropping those samples would make the histogram describe only the happy path, which is the
    distribution least likely to contain the problem.
    """
    metrics = Metrics()
    extension = make_extension(metrics)
    drive(extension)

    async def failing(root: Any, info: Any, **kwargs: Any) -> str:  # noqa: ANN401
        raise RuntimeError("resolver blew up")

    with pytest.raises(RuntimeError):
        await extension.resolve(failing, None, FakeInfo("Query", "logStats"))

    assert (
        sample_value(
            metrics, "gql_field_duration_seconds_count", parent_type="Query", field="logStats"
        )
        == 1.0
    )


@pytest.mark.parametrize(
    ("parent_type", "field_name"),
    [("Query", "__schema"), ("Query", "__typename"), ("__Type", "fields"), ("__Schema", "types")],
)
def test_introspection_is_not_timed(parent_type: str, field_name: str) -> None:
    """Exempt for the same reason :mod:`src.graphql.cost` exempts it from pricing.

    Introspection is bounded by the schema, cannot be widened by a client and touches no database.
    Timing it would let one GraphiQL page load dominate the field histogram on every refresh, and
    would triple the family's series count for no operational value.
    """
    metrics = Metrics()
    extension = make_extension(metrics)
    drive(extension)

    extension.resolve(lambda root, info, **kwargs: None, None, FakeInfo(parent_type, field_name))

    assert exposition(metrics).count("gql_field_duration_seconds_count") == 0


def test_the_operation_hook_records_a_duration_and_an_outcome() -> None:
    """The whole-operation timing, driven through the hook rather than through the helper."""
    metrics = Metrics()
    extension = make_extension(metrics)

    drive(extension)

    count = sample_value(
        metrics,
        "gql_operation_duration_seconds_count",
        operation=ANONYMOUS_OPERATION_LABEL,
        operation_type="unknown",
        outcome="success",
    )
    assert count == 1.0


def test_errors_are_counted_by_their_own_code_and_a_client_mistake_is_never_a_server_fault() -> None:
    """The label vocabulary is C4's closed :class:`~src.graphql.errors.ErrorCode`, and the folding
    of the two codeless cases is opposite on purpose.

    * A :class:`~src.graphql.errors.DomainError` carries its own code — this is where C8's cost
      rejections and C9's persisted-query misses land, without either module knowing metrics exist.
    * A bare ``GraphQLError`` that graphql-core manufactured while reading the client's document (a
      bad enum, a parse failure) has **no** code, and counting it as ``INTERNAL_ERROR`` would report
      the most common client mistake there is as a server fault. It is counted as a validation
      failure, which is what it is.
    * An error with a real exception underneath it is genuinely ours, and is counted as internal —
      note this runs BEFORE ``MaskInternalErrors``, so the classification has to be made from the
      cause chain rather than from a code that does not exist yet.
    """
    metrics = Metrics()
    extension = make_extension(metrics)
    extension.execution_context.errors = [
        CostLimitExceededError("far too expensive"),
        GraphQLError("Value 'EROR' does not exist in 'LogLevel' enum."),
        GraphQLError("boom", original_error=RuntimeError("a resolver blew up")),
    ]

    drive(extension)

    assert sample_value(
        metrics, "gql_errors_total", code=ErrorCode.COST_LIMIT_EXCEEDED.value
    ) == 1.0
    assert sample_value(metrics, "gql_errors_total", code=ErrorCode.VALIDATION_ERROR.value) == 1.0
    assert sample_value(metrics, "gql_errors_total", code=ErrorCode.INTERNAL_ERROR.value) == 1.0
    assert (
        sample_value(
            metrics,
            "gql_operation_duration_seconds_count",
            operation=ANONYMOUS_OPERATION_LABEL,
            operation_type="unknown",
            outcome="error",
        )
        == 1.0
    )


def test_one_error_reported_in_both_places_is_counted_once() -> None:
    """Strawberry puts validation errors on ``errors`` and execution errors on ``result.errors``.

    Both are read, because reading only one would miss **all** of C8's cost rejections and C9's
    persisted-query misses — but an error that appears in both must not be counted twice, or the
    error rate on a dashboard would be double the real one for exactly the failures that matter.
    """
    metrics = Metrics()
    extension = make_extension(metrics)
    shared = CostLimitExceededError("counted once")
    extension.execution_context.errors = [shared]
    extension.execution_context.result = type("Result", (), {"errors": [shared]})()

    drive(extension)

    assert sample_value(
        metrics, "gql_errors_total", code=ErrorCode.COST_LIMIT_EXCEEDED.value
    ) == 1.0


# =================================================================================================
# The guarded import
# =================================================================================================


def test_create_metrics_returns_none_when_the_feature_is_switched_off() -> None:
    """"Disabled" is indistinguishable from "not built" — no registry, no collectors, no route."""
    assert create_metrics(make_settings(metrics_enabled=False)) is None
    assert create_metrics(make_settings(metrics_enabled=True)) is not None


def test_a_broken_prometheus_client_degrades_to_a_no_op_instead_of_crashing_the_app(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """**Observability must never crash the service.**

    ``src/main.py`` imports the GraphQL package, which imports this module, so an unguarded import
    of a broken ``prometheus_client`` would not be a missing dashboard — it would be a container
    that fails to boot. The flag is forced off here to drive the branch a healthy install cannot
    reach, and the requirement is that :func:`create_metrics` answers ``None`` rather than raising.
    """
    monkeypatch.setattr("src.metrics.PROMETHEUS_AVAILABLE", False)

    assert create_metrics(make_settings(metrics_enabled=True)) is None

    # A direct construction still fails loudly — silently handing back an object whose methods all
    # do nothing would hide a broken install from whoever went looking for it.
    with pytest.raises(RuntimeError, match="prometheus_client"):
        Metrics()


def test_an_extension_with_no_metrics_object_runs_the_operation_unchanged() -> None:
    """Every unit test in this project builds a bare context; none of them should break.

    The same branch covers introspection through ``schema.execute()`` with no ``context_value`` and
    any application assembled without a lifespan.
    """
    extension = make_extension(None)

    drive(extension)  # must not raise

    sentinel = object()
    assert (
        extension.resolve(lambda root, info, **kwargs: sentinel, None, FakeInfo("Query", "logs"))
        is sentinel
    )
