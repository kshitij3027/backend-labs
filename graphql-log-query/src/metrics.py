"""Prometheus metrics — spec §2 item 37, §5 "monitoring and health checks working".

Item 37 names three things by hand: **query execution time**, **per-field resolution time**, and
**active subscription connection count**. All three are here, plus the counters C6 and C7 were
already keeping, exposed as ``GET /metrics`` in the standard text exposition.

.. rubric:: OBSERVABILITY MUST NEVER CRASH THE SERVICE, AND THAT IS ENFORCED IN THREE PLACES

1. **The import is guarded.** A broken or absent ``prometheus_client`` sets
   :data:`PROMETHEUS_AVAILABLE` to ``False`` and every entry point below degrades to a no-op. The
   alternative is an ``ImportError`` at module import, and ``src/main.py`` imports the GraphQL
   package, so that would not be a missing dashboard — it would be a container that will not boot.
2. **Every observation is wrapped.** A metric that cannot be recorded loses a data point; it must
   never turn a good response into a 500. Python 3.11's zero-cost exceptions make the ``try`` free
   on the path where nothing raises, which matters because one of these runs per resolved field.
3. **The scrape is wrapped.** A collector that raises breaks the *whole* exposition, so
   :meth:`Metrics.render` and :meth:`_SourceCollector.collect` both swallow and log.

.. rubric:: An explicit ``CollectorRegistry``, not the global default

The integration suite builds several applications in one process. Metrics declared against
``prometheus_client``'s module-level default registry would raise ``Duplicated timeseries in
CollectorRegistry`` the second time an app was constructed — which is a test-suite failure standing
in for a real property: the registry is **per application**, so two apps in one process report
independently and neither can see the other's numbers.

.. rubric:: CARDINALITY IS THE ONLY WAY A METRICS LAYER TAKES A SERVER DOWN

A Prometheus time series is created per distinct label combination and lives for the life of the
process. Two of the obvious labellings here are unbounded and are therefore not used:

* **The query document.** Never a label, anywhere. It is arbitrary client-supplied text.
* **The field path.** ``logs.0.relatedLogs.3.id`` embeds list indices, so one query at the default
  page size would mint a hundred series. The field histogram is labelled with the **parent type and
  the field name** instead — ``LogEntry.relatedLogs``, not ``logs.7.relatedLogs`` — which is bounded
  by the *schema* rather than by the request: a client cannot invent a type or a field, so the
  series count is fixed at deploy time and is a couple of hundred at most, whatever traffic does.

The one label that is genuinely client-controlled is the **operation name**: ``query Whatever {…}``
is free text, so a client sending a fresh name per request would mint a series per request. It is
kept — an operator wants per-operation latency, and it is the label item 37 implies — but it is
capped: the first :data:`MAX_OPERATION_LABELS` distinct names are reported by name and every later
one collapses to :data:`OTHER_OPERATION_LABEL`. Bounded by construction, and the collapse is visible
in the exposition rather than silent. ``tests/integration/test_metrics.py`` sends more than the cap
and asserts the series count stops growing.

.. rubric:: The counters are MIRRORED, never duplicated

:class:`src.broker.BrokerStats`, :class:`src.cache.CacheStats` and
:class:`src.graphql.apq.PersistedQueryStats` were all shaped for this commit. Rather than
incrementing a Prometheus counter *beside* each of them — which is two counters that can disagree,
and a metric that disagrees with the thing it mirrors is worse than no metric — a
:class:`_SourceCollector` reads the three snapshots **at scrape time** and emits them as metric
families. There is exactly one number for each fact, and the exposition cannot drift from the
object it describes because it is computed from it.

The active-subscription gauge is the same mechanism: ``BrokerStats.active_subscribers`` is already
the authoritative count (it is what ``MAX_SUBSCRIPTIONS_PER_CONNECTION`` is enforced against), so
the gauge is that number read live rather than a second tally kept in parallel with the broker's.

.. rubric:: What ``METRICS_ENABLED=false`` does

No :class:`Metrics` object is built, ``app.state.metrics`` is ``None``, the ``/metrics`` route is
**not registered at all** (so it 404s, exactly as ``GET /graphql`` does with the playground
disabled), and :meth:`MetricsExtension.resolve` short-circuits on its first attribute read — it does
not even look at the clock. "Disabled" is indistinguishable from "not built", which is the same rule
:func:`src.cache.create_result_cache` follows.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Iterator
from time import perf_counter
from typing import TYPE_CHECKING, Any, Optional

from strawberry.extensions import SchemaExtension
from strawberry.types.graphql import OperationType

from src.config import Settings

if TYPE_CHECKING:  # pragma: no cover - annotations only; `from __future__` makes them strings
    from graphql import GraphQLResolveInfo

    from src.broker import LogBroker
    from src.cache import ResultCache
    from src.graphql.apq import PersistedQueryStore

logger = logging.getLogger(__name__)

# =================================================================================================
# The guarded import
# =================================================================================================

try:  # pragma: no cover - the failure branch needs a broken install to reach naturally
    from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest
    from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

    PROMETHEUS_AVAILABLE = True
except Exception:  # noqa: BLE001 - a missing dashboard must never be a missing service
    PROMETHEUS_AVAILABLE = False

#: The content type Prometheus expects. Hard-coded rather than imported from ``prometheus_client``
#: so the ``/metrics`` route can answer with the right header even when the guarded import failed —
#: an empty body with a correct content type is a scrape that reports nothing, which is a far better
#: failure than a 500 that reports an outage that is not happening.
PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# =================================================================================================
# Names, labels and buckets
# =================================================================================================

#: Prefix on every metric this project publishes. Short because it is repeated on every line of
#: every scrape, and namespaced because a Prometheus server scrapes many jobs into one keyspace.
METRIC_PREFIX = "gql"

#: The label an anonymous operation (``{ logs { id } }``, with no ``query Name``) is reported under.
ANONYMOUS_OPERATION_LABEL = "anonymous"

#: Where every operation name past the cap goes. Visible in the exposition on purpose: an operator
#: seeing traffic on ``other`` knows the cap is biting and that per-operation latency has stopped
#: being complete, which a silently dropped label would not tell them.
OTHER_OPERATION_LABEL = "other"

#: How many distinct operation names get their own time series. See the cardinality note in the
#: module docstring. A module constant rather than a setting: it is a safety bound on the metrics
#: layer itself, and an operator who could raise it could raise it to the point of an OOM.
MAX_OPERATION_LABELS = 64

#: Operation names are truncated to this many characters before becoming a label. A name is free
#: text, and a megabyte of it would be a megabyte in every scrape.
MAX_OPERATION_NAME_LENGTH = 64

#: Buckets for whole operations, in seconds. ``0.1`` is present deliberately and not as a round
#: number: spec §5 gates a simple query at **under 100ms**, so the spec's own success criterion is
#: readable straight off the histogram as
#: ``gql_operation_duration_seconds_bucket{le="0.1"} / _count``.
OPERATION_DURATION_BUCKETS = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    float("inf"),
)

#: Buckets for a single field resolution, in seconds. An order of magnitude finer at the bottom than
#: the operation buckets, because a scalar field on an already-loaded object resolves in single-digit
#: microseconds and the stock buckets (which start at 5ms) would put every one of them in the first
#: bucket and report nothing at all. The interesting distinction here is "attribute read" versus
#: "this field went to the database", and it lives between 10µs and 10ms.
FIELD_DURATION_BUCKETS = (
    0.00001,
    0.000025,
    0.00005,
    0.0001,
    0.00025,
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.05,
    0.1,
    0.5,
    float("inf"),
)

#: ``(metric name, BrokerStats attribute, help text)``. Data rather than code so C12's order
#: subscriptions add rows instead of branches.
_BROKER_COUNTERS: tuple[tuple[str, str, str], ...] = (
    ("broker_published_total", "published_total", "Log entries handed to the broker by this process."),
    ("broker_delivered_total", "delivered_total", "Successful enqueues onto a subscriber's queue."),
    (
        "broker_dropped_total",
        "dropped_total",
        "Subscribers terminated because their bounded queue filled (slow consumers).",
    ),
    (
        "broker_remote_published_total",
        "remote_published_total",
        "Envelopes PUBLISHed to the Redis subscription channel.",
    ),
    (
        "broker_remote_received_total",
        "remote_received_total",
        "Messages read off the Redis subscription channel, including this process's own echo.",
    ),
    (
        "broker_remote_suppressed_total",
        "remote_suppressed_total",
        "Received messages discarded as this process's own echo.",
    ),
    (
        "broker_remote_invalid_total",
        "remote_invalid_total",
        "Events that failed to cross the bridge because of their content rather than the transport.",
    ),
    (
        "broker_redis_errors_total",
        "redis_errors_total",
        "Redis transport failures in the publish path or the reader loop.",
    ),
)

#: ``(metric name, CacheStats attribute, help text)``.
_CACHE_COUNTERS: tuple[tuple[str, str, str], ...] = (
    ("cache_hits_total", "hits", "Read-resolver calls answered from Redis."),
    ("cache_misses_total", "misses", "Read-resolver calls answered from PostgreSQL."),
    (
        "cache_errors_total",
        "errors",
        "Redis or codec failures the cache survived by reading through.",
    ),
    (
        "cache_coalesced_total",
        "coalesced",
        "Calls served by another caller's in-flight computation (stampede protection).",
    ),
    ("cache_bypassed_total", "bypassed", "Calls made while the cache was disabled."),
)

#: ``(metric name, PersistedQueryStats attribute, help text)``.
_APQ_COUNTERS: tuple[tuple[str, str, str], ...] = (
    ("persisted_query_hits_total", "hits", "Hash-only requests served from a registered document."),
    (
        "persisted_query_misses_total",
        "misses",
        "Hash-only requests with nothing registered (answered PersistedQueryNotFound).",
    ),
    (
        "persisted_query_registered_total",
        "registered",
        "Documents stored after their sha256 was verified against the document text.",
    ),
    (
        "persisted_query_mismatches_total",
        "mismatches",
        "Registrations refused because the supplied hash did not name the document.",
    ),
    (
        "persisted_query_protocol_errors_total",
        "protocol_errors",
        "persistedQuery payloads refused for their shape (bad version, malformed hash).",
    ),
    (
        "persisted_query_oversized_total",
        "oversized",
        "Verified documents too large to store.",
    ),
    (
        "persisted_query_errors_total",
        "errors",
        "Redis or decode failures the persisted query store survived.",
    ),
)


def _metric_name(suffix: str) -> str:
    """``gql_<suffix>``. One place, so a prefix change cannot land on half the metrics."""
    return f"{METRIC_PREFIX}_{suffix}"


# =================================================================================================
# The scrape-time bridge to the C6/C7/C9 counters
# =================================================================================================


class _SourceCollector:
    """Emits the broker, cache and persisted-query counters, read live at scrape time.

    A ``prometheus_client`` collector is any object with a ``collect()`` that yields metric
    families. Using one here is what makes "the metric equals the counter it mirrors" a structural
    property rather than a discipline: nothing increments these, so nothing can forget to.

    The three sources are read off the owning :class:`Metrics` **at collection time**, not captured
    in the constructor, so an application that replaces ``app.state.cache`` after startup (which the
    integration suite does, to get a per-test namespace) is still reported correctly after a
    :meth:`Metrics.bind` call.

    No ``describe()`` is implemented and the registry is built with ``auto_describe=False``, so
    registration never calls ``collect()``. That is deliberate: registration happens during the
    lifespan, before the broker has published anything, and a collector that ran then would be
    reading half-built objects for the sake of a duplicate-name check that a single fixed family
    list cannot fail.
    """

    def __init__(self, metrics: "Metrics") -> None:
        self._metrics = metrics

    def collect(self) -> Iterator[Any]:
        """Yield one family per counter. **Never raises** — a raising collector breaks the scrape."""
        try:
            yield from self._broker_families()
        except Exception:  # noqa: BLE001 - one bad source must not empty the whole exposition
            logger.debug("broker metrics could not be collected", exc_info=True)
        try:
            yield from self._cache_families()
        except Exception:  # noqa: BLE001
            logger.debug("cache metrics could not be collected", exc_info=True)
        try:
            yield from self._persisted_query_families()
        except Exception:  # noqa: BLE001
            logger.debug("persisted query metrics could not be collected", exc_info=True)

    def _broker_families(self) -> Iterator[Any]:
        broker = self._metrics.broker
        if broker is None:
            return
        stats = broker.stats
        yield GaugeMetricFamily(
            _metric_name("active_subscriptions"),
            "Live subscription operations held by this process (spec item 37).",
            value=float(stats.active_subscribers),
        )
        for suffix, attribute, help_text in _BROKER_COUNTERS:
            yield CounterMetricFamily(
                _metric_name(suffix), help_text, value=float(getattr(stats, attribute))
            )

    def _cache_families(self) -> Iterator[Any]:
        cache = self._metrics.cache
        if cache is None:
            return
        stats = cache.stats
        for suffix, attribute, help_text in _CACHE_COUNTERS:
            yield CounterMetricFamily(
                _metric_name(suffix), help_text, value=float(getattr(stats, attribute))
            )
        yield GaugeMetricFamily(
            _metric_name("cache_inflight"),
            "Cache keys currently being computed. Returns to zero when the process is idle.",
            value=float(stats.inflight),
        )
        yield GaugeMetricFamily(
            _metric_name("cache_enabled"),
            "1 when the result cache is configured on and holding a Redis client, else 0.",
            value=1.0 if stats.enabled else 0.0,
        )

    def _persisted_query_families(self) -> Iterator[Any]:
        store = self._metrics.persisted_queries
        if store is None:
            return
        stats = store.stats
        for suffix, attribute, help_text in _APQ_COUNTERS:
            yield CounterMetricFamily(
                _metric_name(suffix), help_text, value=float(getattr(stats, attribute))
            )
        yield GaugeMetricFamily(
            _metric_name("persisted_queries_enabled"),
            "1 when persisted queries are configured on and holding a Redis client, else 0.",
            value=1.0 if stats.enabled else 0.0,
        )


# =================================================================================================
# The metrics object
# =================================================================================================


class Metrics:
    """One registry and the instruments recorded into it. Built per application, in the lifespan.

    Reached by the ``/metrics`` route through ``app.state.metrics`` and by
    :class:`MetricsExtension` through ``info.context.metrics`` — the same arrangement the broker and
    the cache use, and for the same reason: a module-level singleton would be shared by every
    application in the test process and would report one app's traffic under another's scrape.
    """

    def __init__(
        self,
        *,
        registry: Optional[Any] = None,
        broker: Optional["LogBroker"] = None,
        cache: Optional["ResultCache"] = None,
        persisted_queries: Optional["PersistedQueryStore"] = None,
    ) -> None:
        """Build the registry and declare the instruments.

        Args:
            registry: An existing ``CollectorRegistry`` to declare into. Tests pass one to assert on
                the declaration itself; production lets this build its own.
            broker: The process broker, mirrored at scrape time. ``None`` is supported and simply
                omits those families.
            cache: The result cache, likewise.
            persisted_queries: The persisted query store, likewise.

        Raises:
            RuntimeError: If ``prometheus_client`` could not be imported. Callers go through
                :func:`create_metrics`, which checks :data:`PROMETHEUS_AVAILABLE` first and returns
                ``None`` instead — this raise exists so a direct construction fails loudly rather
                than producing an object whose methods all silently do nothing.
        """
        if not PROMETHEUS_AVAILABLE:  # pragma: no cover - needs a broken install to reach naturally
            raise RuntimeError(
                "prometheus_client is not importable, so no metrics registry can be built. Use "
                "src.metrics.create_metrics(), which degrades to None instead of raising."
            )

        self.broker = broker
        self.cache = cache
        self.persisted_queries = persisted_queries

        # `auto_describe=False` (the default) so registering the source collector does not call it.
        self.registry = registry if registry is not None else CollectorRegistry()

        #: Spec §2 item 37, first clause: query execution time. Covers parse + validate + execute,
        #: because that is what the client waited for.
        self.operation_duration = Histogram(
            _metric_name("operation_duration_seconds"),
            "End-to-end GraphQL operation time (parse, validate and execute), in seconds.",
            ["operation", "operation_type", "outcome"],
            buckets=OPERATION_DURATION_BUCKETS,
            registry=self.registry,
        )
        #: Item 37, second clause: per-field resolution time. Labelled by the SCHEMA coordinate, not
        #: by the response path — see the cardinality note in the module docstring.
        self.field_duration = Histogram(
            _metric_name("field_duration_seconds"),
            "Time to resolve one field, in seconds, by parent type and field name.",
            ["parent_type", "field"],
            buckets=FIELD_DURATION_BUCKETS,
            registry=self.registry,
        )
        #: Every error an operation produced, by ``extensions.code``. Bounded by
        #: :class:`src.graphql.errors.ErrorCode`, which is a closed vocabulary — so this is where
        #: C8's cost-gate rejections and C9's persisted-query misses are counted, without either
        #: module having to know that metrics exist.
        self.errors = Counter(
            _metric_name("errors_total"),
            "GraphQL errors returned to clients, by extensions.code.",
            ["code"],
            registry=self.registry,
        )

        #: Operation names already given a series. See :meth:`operation_label`.
        self._operation_labels: set[str] = set()

        self.registry.register(_SourceCollector(self))

    # -- wiring ---------------------------------------------------------------------------------

    def bind(
        self,
        *,
        broker: Optional["LogBroker"] = None,
        cache: Optional["ResultCache"] = None,
        persisted_queries: Optional["PersistedQueryStore"] = None,
    ) -> None:
        """Point the scrape-time collector at different sources. Only non-``None`` values apply.

        Needed because the integration suite replaces ``app.state.cache`` after startup to get a
        per-test key namespace; without this the exposition would keep reporting the cache the
        lifespan built and the test would be grading the wrong object.
        """
        if broker is not None:
            self.broker = broker
        if cache is not None:
            self.cache = cache
        if persisted_queries is not None:
            self.persisted_queries = persisted_queries

    # -- labels ---------------------------------------------------------------------------------

    def operation_label(self, name: Optional[str]) -> str:
        """The bounded label for an operation name. See the cardinality note in the module docstring.

        ``None`` or blank becomes :data:`ANONYMOUS_OPERATION_LABEL`; a name past
        :data:`MAX_OPERATION_NAME_LENGTH` is truncated; and once :data:`MAX_OPERATION_LABELS`
        distinct names have been seen, every further name becomes :data:`OTHER_OPERATION_LABEL`.

        The set only ever grows, which is correct: a series that exists cannot be unpublished, so
        "have I already minted this one" is a permanent question.
        """
        if not name:
            return ANONYMOUS_OPERATION_LABEL
        label = str(name)[:MAX_OPERATION_NAME_LENGTH]
        if label in self._operation_labels:
            return label
        if len(self._operation_labels) >= MAX_OPERATION_LABELS:
            return OTHER_OPERATION_LABEL
        self._operation_labels.add(label)
        return label

    # -- observations ---------------------------------------------------------------------------

    def observe_operation(
        self, *, name: Optional[str], operation_type: str, outcome: str, seconds: float
    ) -> None:
        """Record one completed operation. **Never raises.**"""
        try:
            self.operation_duration.labels(
                self.operation_label(name), operation_type, outcome
            ).observe(seconds)
        except Exception:  # noqa: BLE001 - a lost data point is not a failed request
            logger.debug("could not record an operation duration", exc_info=True)

    def observe_field(self, parent_type: str, field_name: str, seconds: float) -> None:
        """Record one field resolution. **Never raises.**

        The hot path of this whole module: it runs once per resolved field, which is 700 times for
        the flagship correlated query. Nothing is computed here that could be computed once per
        operation, and the labels are two strings that already exist on the resolve info.
        """
        try:
            self.field_duration.labels(parent_type, field_name).observe(seconds)
        except Exception:  # noqa: BLE001
            logger.debug("could not record a field duration", exc_info=True)

    def count_error(self, code: str) -> None:
        """Count one error by its ``extensions.code``. **Never raises.**"""
        try:
            self.errors.labels(code).inc()
        except Exception:  # noqa: BLE001
            logger.debug("could not count a GraphQL error", exc_info=True)

    # -- exposition -----------------------------------------------------------------------------

    def render(self) -> tuple[bytes, str]:
        """``(body, content_type)`` for ``GET /metrics``. **Never raises.**

        A failed scrape answers with an empty body and the correct content type rather than an
        error: Prometheus reads that as "this target has no series right now", which is true and
        harmless, whereas a 500 would page somebody about an outage that is not happening.
        """
        try:
            return generate_latest(self.registry), PROMETHEUS_CONTENT_TYPE
        except Exception:  # noqa: BLE001
            logger.warning("could not render the Prometheus exposition", exc_info=True)
            return b"", PROMETHEUS_CONTENT_TYPE


def create_metrics(
    settings: Settings,
    *,
    broker: Optional["LogBroker"] = None,
    cache: Optional["ResultCache"] = None,
    persisted_queries: Optional["PersistedQueryStore"] = None,
) -> Optional[Metrics]:
    """The process's :class:`Metrics`, or ``None``. **Never raises.**

    ``None`` on either of the two ways metrics can be absent — ``METRICS_ENABLED=false``, or a
    ``prometheus_client`` that will not import — and the caller treats both identically. That is the
    whole guarded-import contract: the service does not care *why* it has no metrics, only that it
    still runs.
    """
    if not settings.metrics_enabled:
        return None
    if not PROMETHEUS_AVAILABLE:  # pragma: no cover - needs a broken install to reach naturally
        logger.warning(
            "METRICS_ENABLED is set but prometheus_client could not be imported — GET /metrics "
            "will report nothing and the service is otherwise unaffected"
        )
        return None
    try:
        return Metrics(broker=broker, cache=cache, persisted_queries=persisted_queries)
    except Exception:  # noqa: BLE001 - observability must never stop the app from starting
        logger.warning("could not build the metrics registry; metrics are disabled", exc_info=True)
        return None


# =================================================================================================
# The extension
# =================================================================================================


def operation_type_name(execution_context: Any) -> str:  # noqa: ANN401 - Strawberry ExecutionContext
    """``"query"`` / ``"mutation"`` / ``"subscription"``, or ``"unknown"``.

    Guarded because ``ExecutionContext.operation_type`` reads the **parsed document** and raises
    ``RuntimeError("No GraphQL document available")`` when there is none — which is the ordinary
    state of an operation that was rejected during parsing, and exactly one of the cases this
    histogram wants to record.

    Read through ``.name.lower()`` rather than ``.value`` so the label is a property of the enum's
    members rather than of the strings Strawberry happens to give them.
    """
    try:
        return execution_context.operation_type.name.lower()
    except Exception:  # noqa: BLE001 - an unparsed document has no operation type; that is normal
        return "unknown"


def is_subscription(execution_context: Any) -> bool:  # noqa: ANN401 - Strawberry ExecutionContext
    """Is this operation a subscription? ``False`` when it cannot be determined."""
    try:
        return execution_context.operation_type is OperationType.SUBSCRIPTION
    except Exception:  # noqa: BLE001
        return False


class MetricsExtension(SchemaExtension):
    """Times operations and fields, and counts the errors an operation returned.

    Registered as a **class**, never an instance: Strawberry constructs one per execution, and this
    holds per-operation state (the start time, and the resolved :class:`Metrics`). See the ordering
    note in :mod:`src.graphql.schema` — it sits directly inside ``MaskInternalErrors``, which means
    its post-yield code runs **before** masking and therefore sees the errors with their real codes
    still attached. Counting them after masking would report every distinct failure in the system as
    ``INTERNAL_ERROR``.

    .. rubric:: Subscriptions are deliberately absent from the operation histogram

    Strawberry wraps a subscription's entire yield loop in one ``on_operation``, so "how long did
    this operation take" is *the lifetime of the socket* — minutes or hours. Recording that in the
    same histogram as query latency would put a two-hour sample in a distribution whose top bucket
    is ten seconds and make the p99 of the whole family meaningless the moment anybody aggregated
    across the ``operation_type`` label. Subscriptions are covered by the active-subscription gauge,
    which is what spec item 37 asks for, and their per-event work is covered by the field histogram
    (which does fire, once per field per delivered event).
    """

    #: Class-level defaults so an instance whose ``on_operation`` never ran still has a fast,
    #: correct ``resolve``. Strawberry constructs extensions with no arguments (see
    #: :mod:`src.graphql.schema`), so there is no ``__init__`` here to set them.
    _metrics: Optional[Metrics] = None
    _started: float = 0.0

    def on_operation(self) -> Iterator[None]:
        """Time the whole operation and count what it returned.

        A **synchronous** generator: nothing here awaits, and a sync hook is one less coroutine per
        request. (:class:`src.graphql.apq.PersistedQueries` and
        :class:`src.graphql.context.PerOperationResources` are async because they really do I/O.)
        """
        self._metrics = _metrics_from_context(self.execution_context)
        if self._metrics is None:
            yield
            return

        self._started = perf_counter()
        try:
            yield
        finally:
            elapsed = perf_counter() - self._started
            metrics = self._metrics
            errors = _collected_errors(self.execution_context)
            for code in _error_codes(errors):
                metrics.count_error(code)
            if not is_subscription(self.execution_context):
                metrics.observe_operation(
                    name=_operation_name(self.execution_context),
                    operation_type=operation_type_name(self.execution_context),
                    outcome="error" if errors else "success",
                    seconds=elapsed,
                )

    def resolve(
        self,
        _next: Callable[..., Any],
        root: Any,  # noqa: ANN401 - whatever the parent resolved to
        info: "GraphQLResolveInfo",
        *args: Any,
        **kwargs: Any,
    ) -> Any:  # noqa: ANN401 - whatever the field resolves to
        """Time one field resolution. Spec §2 item 37, second clause.

        **THIS IS THE ONE HOOK WHERE AN EXTRA ALLOCATION IS MEASURABLE.** It runs on every field of
        every request — 700 times for the flagship ``{ logs { … relatedLogs { … } } }`` at the
        default page size — so the disabled path below is a single attribute load and a branch, and
        the enabled path is a ``perf_counter`` pair plus one ``observe``. Nothing is formatted,
        nothing is looked up in a dict, and no coroutine is created for a field that resolved
        synchronously.

        That last point is why this method is **sync** rather than ``async def``. Most fields in
        this schema are scalars served by the default resolver, which returns a plain value; making
        this coroutine would wrap every one of those in a coroutine object that exists only to be
        awaited immediately. Instead the awaitable case — a real resolver — is handed to
        :meth:`_timed`, so the cost is paid only where there was already going to be an await.

        Introspection is skipped, exactly as :mod:`src.graphql.cost` exempts it from pricing: it is
        bounded by the schema, cannot be widened by a client, touches no database, and would
        otherwise dominate the histogram with GraphiQL's page-load query on every refresh.
        """
        metrics = self._metrics
        if metrics is None:
            return _next(root, info, *args, **kwargs)

        parent_type = info.parent_type.name
        field_name = info.field_name
        if field_name.startswith("__") or parent_type.startswith("__"):
            return _next(root, info, *args, **kwargs)

        started = perf_counter()
        try:
            result = _next(root, info, *args, **kwargs)
        except BaseException:
            # A synchronous resolver that raised. Timed anyway, for the same reason :meth:`_timed`
            # uses a ``finally``: a field that fails slowly is exactly what an operator is hunting
            # for, and a histogram that only ever saw successes describes the wrong population.
            # Free on the path that does not raise — Python 3.11 has zero-cost exceptions, which
            # matters because this block wraps every field of every request.
            metrics.observe_field(parent_type, field_name, perf_counter() - started)
            raise
        if hasattr(result.__class__, "__await__"):
            # An async resolver. The timing has to continue past this return, so the awaitable is
            # wrapped rather than observed here — observing now would record "time to create a
            # coroutine", which is the same tiny number for every field in the schema.
            return self._timed(result, metrics, parent_type, field_name, started)
        metrics.observe_field(parent_type, field_name, perf_counter() - started)
        return result

    @staticmethod
    async def _timed(
        awaitable: Any,  # noqa: ANN401 - whatever the resolver returned
        metrics: Metrics,
        parent_type: str,
        field_name: str,
        started: float,
    ) -> Any:  # noqa: ANN401
        """Await ``awaitable`` and record how long the whole resolution took.

        ``finally``, so a field that raised is still timed — a resolver that fails slowly is exactly
        the thing an operator is looking for, and dropping those samples would make the histogram
        describe only the happy path.
        """
        try:
            return await awaitable
        finally:
            metrics.observe_field(parent_type, field_name, perf_counter() - started)


def _metrics_from_context(execution_context: Any) -> Optional[Metrics]:  # noqa: ANN401
    """The :class:`Metrics` on this operation's context, or ``None``.

    ``None`` for an operation executed without a :class:`~src.graphql.context.Context` (introspection
    through ``schema.execute()`` with no ``context_value``, and every unit test that builds a bare
    context), and for an application built with ``METRICS_ENABLED=false``. Both mean the same thing
    to every caller: do nothing.
    """
    metrics = getattr(getattr(execution_context, "context", None), "metrics", None)
    return metrics if isinstance(metrics, Metrics) else None


def _operation_name(execution_context: Any) -> Optional[str]:  # noqa: ANN401
    """The operation's name, or ``None``. Guarded: it is derived from the parsed document."""
    try:
        return execution_context.operation_name
    except Exception:  # noqa: BLE001
        return None


def _collected_errors(execution_context: Any) -> list[Any]:  # noqa: ANN401
    """Every error this operation produced, from both places Strawberry puts them.

    Parse and validation failures land on ``execution_context.errors`` and return early, so they
    never reach ``execution_context.result``; execution failures land on the result. An extension
    that read only one of the two would count half the failures in the system — and specifically it
    would miss **all** of C8's cost rejections and C9's persisted-query misses, which are precisely
    the two this commit was asked to count.
    """
    collected: list[Any] = []
    seen: set[int] = set()
    for source in (
        getattr(execution_context, "errors", None),
        getattr(getattr(execution_context, "result", None), "errors", None),
    ):
        for error in source or ():
            if id(error) not in seen:
                seen.add(id(error))
                collected.append(error)
    return collected


def _error_codes(errors: Iterable[Any]) -> Iterator[str]:
    """The ``extensions.code`` of each error, folded onto the closed :class:`ErrorCode` vocabulary.

    Classified through C4's own :func:`~src.graphql.errors.is_expected_error` rather than by reading
    ``extensions`` alone, and the two fallbacks are opposites on purpose:

    * **Unexpected** — an exception escaped our code. It carries no code at all until
      ``MaskInternalErrors`` runs, and that happens *after* this extension's post-yield (masking is
      outermost, so it tears down last). Counted as ``INTERNAL_ERROR``, which is what the client is
      about to be told.
    * **Expected but codeless** — graphql-core manufactured it while reading the client's own
      document: a parse failure, a bad enum literal, a depth or token rejection. None of those pass
      through :class:`~src.graphql.errors.DomainError`, so none carries a code — and counting them as
      ``INTERNAL_ERROR`` would report the single most common client mistake there is as a server
      fault, which is exactly the misclassification C4 exists to prevent. They are validation
      failures, so they are counted as ``VALIDATION_ERROR``.

    The label set is therefore exactly :class:`~src.graphql.errors.ErrorCode`, which is closed and
    small — no unbounded values, and no empty string.
    """
    # Imported here rather than at module scope: `src.graphql.errors` is part of the GraphQL layer
    # and this module is deliberately importable without it (the /metrics route and the registry
    # have nothing to do with the schema).
    from src.graphql.errors import ErrorCode, error_code, is_expected_error  # noqa: PLC0415

    for error in errors:
        try:
            if is_expected_error(error):
                yield error_code(error) or ErrorCode.VALIDATION_ERROR.value
            else:
                yield ErrorCode.INTERNAL_ERROR.value
        except Exception:  # noqa: BLE001 - an error we cannot classify is still an error
            yield ErrorCode.INTERNAL_ERROR.value


__all__ = [
    "ANONYMOUS_OPERATION_LABEL",
    "FIELD_DURATION_BUCKETS",
    "MAX_OPERATION_LABELS",
    "METRIC_PREFIX",
    "OPERATION_DURATION_BUCKETS",
    "OTHER_OPERATION_LABEL",
    "PROMETHEUS_AVAILABLE",
    "PROMETHEUS_CONTENT_TYPE",
    "Metrics",
    "MetricsExtension",
    "create_metrics",
    "is_subscription",
    "operation_type_name",
]
