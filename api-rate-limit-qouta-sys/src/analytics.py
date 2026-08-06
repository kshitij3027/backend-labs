"""Usage analytics: one script call per served request in, one pipelined read out.

Every metered request is folded into three keys — a minute bucket, an hour bucket and a
per-minute top-consumer ZSET — by :data:`~src.lua.RLQ_RECORD_REQUEST`, fired **after** the
response body is on the wire. :meth:`AnalyticsCollector.snapshot` reads them back by computing
bucket names arithmetically and pipelining ``HGETALL`` over the result.

.. rubric:: Analytics is deliberately OUTSIDE the decision script. Three decisive reasons

This is the design decision this module exists to implement, and each of the three would be
sufficient on its own.

**1. The keys cannot share the ``{user}`` hash tag, so folding them in is a guaranteed
``CROSSSLOT``.** The decision script's four keys — ``rate_limit:{alice}:...``, ``quota:daily:{alice}:...``,
``quota:monthly:{alice}:...``, ``user:{alice}`` — all carry the same Redis Cluster hash tag, which
is precisely what makes one ``EVALSHA`` able to touch all four (see :mod:`src.keys`). Analytics
keys are *global and time-bucketed*: ``stats:min:29775511`` belongs to every principal at once.
There is no tag it could carry. Tag it with a user and every replica's writes for that minute
collapse onto one slot and the bucket stops being global; leave it untagged inside the decision
script and the script becomes ``CROSSSLOT`` the day this is sharded. There is no third option,
because the constraint is arithmetic rather than stylistic.

**2. It would triple the command count on the critical path.** The decision script issues about
seven commands. Recording adds **sixteen** — twelve ``HINCRBY``, one ``ZINCRBY`` and three
``EXPIRE`` — on a **single-threaded** server, inside the request's own latency budget, where this
project's entire allowance for the rate-limit check is 5 ms. Out here the same sixteen commands run
after the client already has its bytes. (That ratio is also why the write path needs its own pool
gate: see the connection rubric below.)

**3. An analytics error would break rate limiting.** ``redis.call`` aborts the *whole script* on
error and rolls nothing back. A ``WRONGTYPE`` on ``stats:min:*`` — one operator running
``SET stats:min:29775511 x`` while debugging, one future commit changing a bucket's type — would
therefore make the **decision** script raise, which :mod:`src.redis_client` correctly classifies
as a correctness failure and turns into a 500. Every request in the service would fail because a
dashboard counter was the wrong type. Keeping the write out here means the worst an analytics
failure can do is what :meth:`AnalyticsCollector.record` does with it: swallow it, count it, and
serve the request that was already served.

.. rubric:: The bucket index comes from the DECISION's clock, not from this replica's

``LimitDecision.server_now_ms`` is ``redis.call('TIME')`` — the one clock every replica shares.
Bucketing on ``time.time()`` here would mean two replicas whose system clocks differ by 30 seconds
write the same instant into two different minute buckets for a third of every minute, which shows
up on a chart as a permanent saw-tooth that no amount of staring at the traffic explains. Using the
decision's clock makes the bucket a property of *when Redis decided*, which is the same answer on
every replica by construction.

Local time is used only when there is no decision to read a clock from: a 401 (no principal, so
the script never ran), a 503 from the identity path, or a hand-built decision carrying a zeroed
``server_now_ms``. Those records land in whatever minute *this* replica believes it is, and the
error is bounded by the replica's skew. A degraded decision is the same case wearing a decision's
clothes — :meth:`src.limiter.Limiter._wall_clock_ms` already documents that its ``server_now_ms``
is the local clock, because during an outage there is no shared one to read.

**The read side reads the same clock, and it has to.** :meth:`AnalyticsCollector.snapshot` opens
with its own ``TIME`` call rather than ``time.time()``. Bucketing writes on Redis's clock and then
*naming the window* from the local one puts the two halves back out of step: a replica five minutes
fast computes ``newest_minute_index`` five minutes ahead of anything that was ever written and
reports ``totals.requests = 0`` for data a correct replica reports normally. That is worse than the
write-side skew it would reintroduce, because C12 puts two replicas behind round-robin nginx and
C11 polls every five seconds — so a dashboard would alternate between two different answers for the
same underlying data, which is the saw-tooth the shared clock exists to prevent, arriving through
the read path. One extra round trip on a five-second poll is free; agreement between replicas is
not.

.. rubric:: ``EXPIRE ... NX``, and why the ``NX`` is load-bearing

``NX`` sets a TTL only on a key that has none, so the countdown starts at bucket **creation**.
Without it every write re-arms the full TTL: a continuously hot minute bucket would live an hour
past its last write, and "minute buckets are retained for an hour" would silently become "an hour
after traffic stops" — unbounded retention under exactly the load that produces the most buckets.
Redis 7 is required for the flag, which this project already targets.

.. rubric:: Never ``SCAN``, never ``KEYS``

The read side never asks Redis which buckets exist; it *computes* their names from a time range
(:func:`src.keys.recent_minute_indices`). ``SCAN MATCH stats:min:*`` would walk the entire
keyspace — every ``rate_limit:*``, ``sw:*`` and ``quota:*`` key in the system — on a
single-threaded server, on the endpoint a dashboard polls every 5 seconds. A bucket that does not
exist comes back as an empty hash, which is the same answer a scan would have given and costs one
pipelined ``HGETALL`` instead of a keyspace walk. The fan-in is capped at
``ANALYTICS_MAX_BUCKETS`` so a caller cannot ask for a million minutes and turn a bounded read
into an unbounded one.

.. rubric:: Analytics may never take a connection the LIMITER needs

The record shares the limiter's ``BlockingConnectionPool``, and it is the greedier of the two: 16
commands per request (12 ``HINCRBY``, 3 ``EXPIRE``, 1 ``ZINCRBY``) against the decision script's
seven. Measured at ``REDIS_MAX_CONNECTIONS=6`` with 240 concurrent requests, before this gate
existed:

.. code-block:: text

    analytics OFF : 0 refusals (0.0%)
    analytics ON  : 63 refusals (26.2%) — and 260 analytics writes, 0 dropped

Every analytics write won a connection while 63 **enforcement decisions** were refused one and
turned into 503s. That is not the C8 security hole returning — exhaustion still classifies as
:class:`~src.redis_client.BackingStoreOverloaded` and refuses, so nothing is served unmetered — but
it inverts the priority this module is built on. Dropping a statistic is always better than
delaying a request, and a best-effort writer must not outbid the thing it is a statistic *about*.

So the write path holds a **non-blocking in-flight gate**: at most
``REDIS_MAX_CONNECTIONS // ANALYTICS_POOL_SHARE_DIVISOR`` records may be in flight at any instant,
and a record arriving when that many already are is **dropped rather than queued**. It never waits
for the pool, so it can never be ahead of a decision in the queue. After it, on the same benchmark:

.. code-block:: text

    pool=32 (shipped)   OFF: 0.0%          ON: 0.0% in 4 of 5 rounds, 1.7% in the fifth
    pool=6  (amplified) OFF: 0.0%          ON: 3.3-6.7%, with ~390 records SHED per round

The priority is now the right way round: under contention the records are the thing that gives way.
The residual at a six-connection pool is honest and explained rather than tuned away — one record
holds a connection for a 16-command script where a decision holds it for seven, so even a single
in-flight write is real connection-time on a pool that small. The shipped pool is 32.

A plain integer rather than an :class:`asyncio.Semaphore`, for two reasons. Waiting is exactly the
behaviour being removed, so a primitive whose entire purpose is to wait is the wrong shape — and a
``Semaphore`` binds to the first event loop that touches it, which would make a collector built on
one loop and driven on another raise instead of counting. The check and the increment have no
``await`` between them, and asyncio is cooperative, so the counter cannot be raced.

.. rubric:: Two richer gates were built, measured, and removed

Worth recording so nobody re-adds them on the same reasoning. Both looked obviously better and
neither did anything.

**A pool-headroom check** — refuse to write unless the pool has more free connections than the
gate allows in flight, so analytics provably never takes the *last* one. It needs redis-py's
private ``_in_use_connections``, since the public ``can_get_connection()`` only answers "is there
at least one?" and one is exactly the connection the limiter is about to want. **A saturation
back-off** — stand down entirely while :attr:`~src.redis_client.RedisGateway.is_overloaded`,
using only public API.

Measured at pool=6 / 240 concurrent, four rounds each, all four combinations: refusal rates of
3.3-6.7% (gate only), 5.4-9.2% (+headroom), 7.5-10.8% (+back-off) and 8.8-11.2% (both) — i.e. no
improvement, with run-to-run drift larger than any difference between them. The records counted
tell you why: 376-389 landed with the gate alone and 370-378 with all three, so the extra
conditions were shedding about 1% more writes and could not have moved the refusal rate. The
residual is connection-*time*, not connection-*count*, and neither check addresses that.

So the gate is one comparison against one integer, and a private-attribute dependency on redis-py
was not bought with a hunch.

:meth:`AnalyticsCollector.snapshot` is deliberately **not** gated. It runs once per dashboard poll
rather than once per request, it holds one connection for one pipeline, and shedding it would
replace a slow page with a blank one — the failure mode this module's read side exists to avoid.

.. rubric:: Analytics may NEVER break a request

:meth:`AnalyticsCollector.record` swallows **every** exception and counts it — including
:class:`~src.redis_client.BackingStoreUnavailable`,
:class:`~src.redis_client.BackingStoreOverloaded` and the correctness errors that
:mod:`src.redis_client` goes out of its way to let propagate everywhere else. That asymmetry is
deliberate rather than an oversight of the rule:

* The limiter re-raises a correctness error because a broken decision script must become a visible
  500 instead of a silent fail-open — the *enforcement* answer is wrong, and serving anyway would
  be serving unmetered. Nothing here decides whether a request is allowed. The worst outcome of a
  broken record script is a gap in a chart.
* It is called **after the response has been sent.** An exception at that point cannot turn into a
  500 even in principle: the status line and the body are already on the wire, so raising would
  produce a torn connection or an "Unexpected ASGI message" from the server, i.e. it would damage a
  request that had already succeeded. There is no failure here worth a client noticing.

``except Exception`` and not ``except BaseException``: :class:`asyncio.CancelledError` derives from
``BaseException`` and must keep propagating, or a cancelled request would be resurrected long
enough to finish a Redis write and a shutdown would hang on it.

:meth:`AnalyticsCollector.snapshot` does **not** swallow. It is a dashboard read with a caller who
can report the failure, and a snapshot that returned zeros during an outage would tell an operator
that traffic had stopped — which is the single most misleading thing an observability surface can
say. Malformed *data* inside an otherwise healthy reply is tolerated (see :func:`_count`), because
one bad hash field must not take the whole page down.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Final

from src.config import Settings
from src.keys import (
    MS_PER_HOUR,
    MS_PER_MINUTE,
    UNKNOWN_ENDPOINT_LABEL,
    hour_index,
    minute_index,
    recent_minute_indices,
    stats_hour_key,
    stats_minute_key,
    stats_top_key,
)
from src.lua import (
    RECORD_ARGV_ARITY,
    RECORD_FIELD_COST,
    RECORD_FIELD_ENDPOINT_PREFIX,
    RECORD_FIELD_OUTCOME_PREFIX,
    RECORD_FIELD_REQUESTS,
    RECORD_FIELD_STATUS_PREFIX,
    RECORD_FIELD_TIER_PREFIX,
    RECORD_KEYS_ARITY,
    RLQ_RECORD_REQUEST,
    RLQ_RECORD_REQUEST_NAME,
)
from src.models import (
    LimitDecision,
    StatsBucket,
    StatsSnapshot,
    StatsTotals,
    StatsWindow,
    TopConsumer,
)
from src.redis_client import RedisGateway

logger = logging.getLogger(__name__)

__all__ = [
    "ANALYTICS_POOL_SHARE_DIVISOR",
    "ANONYMOUS_USER_ID",
    "MS_PER_SECOND",
    "OUTCOMES",
    "OUTCOME_ALLOWED",
    "OUTCOME_DEGRADED",
    "OUTCOME_DENIED",
    "TOP_CONSUMERS_FANOUT",
    "TOP_CONSUMERS_LIMIT",
    "UNKNOWN_TIER",
    "AnalyticsCollector",
]

#: Milliseconds per second. Named so the wall-clock conversion below reads as a conversion rather
#: than as a bare multiplication next to a quantity that is already in milliseconds.
MS_PER_SECOND: Final = 1000

# ---------------------------------------------------------------------------------------------
# The outcome dimension
#
# Three values, and they PARTITION the traffic: every recorded request lands in exactly one, so
# `outcome:allowed + outcome:denied + outcome:degraded == requests` in every bucket. That is worth
# more than a richer taxonomy, because it is what lets a dashboard compute a rejection *rate*
# without knowing every value the field can take.
# ---------------------------------------------------------------------------------------------

#: The request was admitted by a decision Redis actually made.
OUTCOME_ALLOWED: Final = "allowed"

#: The request was refused. Covers the 429s **and** the 401s and the fail-closed / pool-exhausted
#: 503s — a request with no decision at all was still not served, and a rejection the graph cannot
#: see is a rejection nobody investigates. Which *kind* of refusal it was is not lost: the status
#: dimension separates ``status:401`` from ``status:429`` from ``status:503``.
OUTCOME_DENIED: Final = "denied"

#: The decision came from the local fallback bucket rather than from Redis — C8's fail-open path.
#:
#: It wins over ``allowed``/``denied`` when both apply, which means a *degraded 429* is counted
#: here rather than under ``denied``. That is the deliberate reading: during an outage the question
#: an operator is asking is "how much of this traffic was still being metered authoritatively?",
#: and folding degraded requests into the other two would answer it with a number that cannot
#: distinguish a healthy replica from one enforcing a guessed, replica-local limit. The refusal
#: itself is still visible as ``status:429``, so nothing is actually lost.
OUTCOME_DEGRADED: Final = "degraded"

#: Every outcome this service writes, in a fixed order. The read side seeds its counters from this
#: tuple so an outcome with no traffic reports ``0`` rather than being absent — a missing key and a
#: zero are the same picture to a human and a different one to a chart library.
OUTCOMES: Final[tuple[str, ...]] = (OUTCOME_ALLOWED, OUTCOME_DENIED, OUTCOME_DEGRADED)

# ---------------------------------------------------------------------------------------------
# Sentinels for a request that never had a principal
# ---------------------------------------------------------------------------------------------

#: Recorded as the user for a request with no principal — a 401, or the 503 the identity path
#: returns when the credential store is unreachable.
#:
#: **Recording those under a sentinel is much better than not recording them at all.** An
#: authentication-failure flood is exactly the traffic you want on the graph: it is the signature
#: of a key-guessing attack, of a client that shipped with the wrong credential, and of a rotation
#: that went wrong — and it is invisible in every other counter this service keeps, because an
#: unauthenticated request never reaches a bucket, a quota or a tier. Dropping it would mean the
#: dashboard's request count silently disagreed with the load balancer's for the one traffic
#: pattern nobody plans for.
#:
#: The cost is that ``anonymous`` can top the consumer ranking during such a flood. That is not a
#: defect; it is the finding.
ANONYMOUS_USER_ID: Final = "anonymous"

#: Recorded as the tier for a request whose tier was never read. A 401 never reaches ``user:{uid}``
#: — the decision script is where tier resolution happens — so any tier name here would be a guess.
#: ``unknown`` is a true statement; ``free`` would be a false one that quietly inflated the free
#: tier's share of every chart.
UNKNOWN_TIER: Final = "unknown"

# ---------------------------------------------------------------------------------------------
# Read-side shape
# ---------------------------------------------------------------------------------------------

#: How many principals the ranking **returns**. Ten is what a dashboard panel shows.
TOP_CONSUMERS_LIMIT: Final = 10

#: How many principals are read from **each** per-minute ZSET before the merge. Five times what is
#: returned, and the multiplier is the whole accuracy story — see :meth:`AnalyticsCollector._rank`,
#: which states the residual error with the number that motivated it.
TOP_CONSUMERS_FANOUT: Final = 50

#: The share of ``REDIS_MAX_CONNECTIONS`` the write path may hold at any instant: one eighth,
#: floored at one. See the "may never take a connection the LIMITER needs" rubric — this is the
#: number that turned a measured 26.2% refusal rate back into ~0%.
#:
#: A *share* rather than a fixed count, so the bound scales with the pool it is protecting: an
#: absolute 4 would be an eighth of the shipped pool and two thirds of a pool of six, which is the
#: configuration where the contention was measured in the first place.
ANALYTICS_POOL_SHARE_DIVISOR: Final = 8

#: Logical operation name for the pipelined read, as it appears in gateway logs and in a raised
#: :class:`~src.redis_client.BackingStoreUnavailable`. The write side's name is generated by
#: :meth:`~src.redis_client.RedisGateway.run_script` from the script name.
OP_SNAPSHOT: Final = "analytics:snapshot"

#: Logical operation name for the read window's ``TIME`` call. Its own name rather than folded into
#: :data:`OP_SNAPSHOT`, so a log line can distinguish "we could not ask what time it is" from "we
#: could not read the buckets" — two failures with the same remedy and different diagnoses.
OP_SNAPSHOT_CLOCK: Final = "analytics:snapshot:time"


def _text(value: object) -> str:
    """Decode one reply element to ``str``.

    The gateway builds its client with ``decode_responses=False`` (the decision script's reply is
    parsed positionally into integers, so a blanket per-value UTF-8 decode would cost the hot path
    and buy it nothing), so hash field names, hash values and ZSET members all arrive as ``bytes``.

    ``errors="replace"`` for the same reason :func:`src.models._as_text` uses it: a mangled byte in
    a tier name or an endpoint label must surface as a visibly wrong row on a dashboard, not as a
    ``UnicodeDecodeError`` thrown out of a stats read.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _count(value: object) -> int:
    """Decode one counter to ``int``, treating anything unreadable as ``0``.

    Tolerant, and deliberately unlike :func:`src.models._as_int`, which raises. The difference is
    what the two are decoding. A malformed element of the *decision* reply means the script and the
    decoder disagree, and building a verdict out of it would produce a confident wrong answer about
    whether a request is allowed. A malformed *bucket field* means somebody wrote a non-integer
    into one hash field of one minute — an operator poking at the keyspace, a future field this
    version has never heard of — and the correct response is to leave that field out of the
    arithmetic rather than to fail the whole dashboard read.

    The failure direction is the safe one: an unparseable counter under-reports, and a stats page
    that shows slightly less traffic than happened is a smaller problem than a stats page that
    shows an exception.
    """
    try:
        return int(_text(value))
    except (TypeError, ValueError):
        return 0


def _score(value: object) -> int:
    """Decode one ZSET score to ``int``.

    Redis scores are IEEE doubles and redis-py hands them back as ``float``. Every score this
    service writes is an accumulated integer cost, so rounding recovers the exact value for any
    magnitude below 2^53 — which a per-minute cost counter is never going near. ``round`` rather
    than ``int``: truncation would turn a score stored as ``4.999999999999999`` into ``4``, and the
    one thing a "top consumers" list must not do is under-report the top consumer.
    """
    try:
        return int(round(float(value)))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


class _Fold:
    """Accumulates the per-dimension totals while each bucket is decoded. One pass, no re-reads.

    A small mutable accumulator rather than four dictionary comprehensions over the raw replies:
    every hash field has to be inspected once anyway to build its :class:`~src.models.StatsBucket`,
    and a second pass to build the dimension maps would decode every field name a second time on
    the endpoint that is polled every 5 seconds.

    Unknown field names are **ignored rather than rejected**. A bucket written by a newer replica
    mid-rollout can legitimately carry a family this version has never heard of, and the useful
    behaviour is to keep folding the fields we do understand instead of failing the read for the
    whole window.
    """

    __slots__ = ("by_endpoint", "by_outcome", "by_status", "by_tier", "cost", "requests")

    def __init__(self) -> None:
        self.requests = 0
        self.cost = 0
        self.by_status: dict[str, int] = {}
        self.by_endpoint: dict[str, int] = {}
        self.by_tier: dict[str, int] = {}
        # Seeded with every known outcome at zero, so `by_outcome` is a complete partition even for
        # a window in which nothing was denied. A chart that has to cope with a missing key renders
        # a gap; one handed an explicit 0 renders a flat line, which is the true picture.
        self.by_outcome: dict[str, int] = dict.fromkeys(OUTCOMES, 0)

    def bucket(
        self,
        raw: Mapping[Any, Any] | None,
        *,
        index: int,
        width_ms: int,
        dimensions: bool,
    ) -> StatsBucket:
        """Decode one ``HGETALL`` reply into a bucket, optionally folding it into the dimensions.

        ``dimensions=False`` is what keeps the hour series from double-counting the minute series:
        both describe the same requests, so only one may contribute to the totals. See the rubric
        on :class:`~src.models.StatsSnapshot`.
        """
        requests = 0
        cost = 0
        outcomes = dict.fromkeys(OUTCOMES, 0)

        for raw_field, raw_value in (raw or {}).items():
            field = _text(raw_field)
            count = _count(raw_value)
            if field == RECORD_FIELD_REQUESTS:
                requests = count
            elif field == RECORD_FIELD_COST:
                cost = count
            elif field.startswith(RECORD_FIELD_OUTCOME_PREFIX):
                name = field[len(RECORD_FIELD_OUTCOME_PREFIX):]
                if name in outcomes:
                    outcomes[name] = count
                if dimensions:
                    _add(self.by_outcome, name, count)
            elif field.startswith(RECORD_FIELD_STATUS_PREFIX):
                if dimensions:
                    _add(self.by_status, field[len(RECORD_FIELD_STATUS_PREFIX):], count)
            elif field.startswith(RECORD_FIELD_ENDPOINT_PREFIX):
                if dimensions:
                    _add(self.by_endpoint, field[len(RECORD_FIELD_ENDPOINT_PREFIX):], count)
            elif field.startswith(RECORD_FIELD_TIER_PREFIX):
                if dimensions:
                    _add(self.by_tier, field[len(RECORD_FIELD_TIER_PREFIX):], count)

        if dimensions:
            self.requests += requests
            self.cost += cost

        return StatsBucket(
            index=index,
            start_ms=index * width_ms,
            width_ms=width_ms,
            requests=requests,
            cost=cost,
            allowed=outcomes[OUTCOME_ALLOWED],
            denied=outcomes[OUTCOME_DENIED],
            degraded=outcomes[OUTCOME_DEGRADED],
        )

    def totals(self) -> StatsTotals:
        """Lift the three known outcomes out of the map. See :class:`~src.models.StatsTotals`."""
        return StatsTotals(
            requests=self.requests,
            cost=self.cost,
            allowed=self.by_outcome.get(OUTCOME_ALLOWED, 0),
            denied=self.by_outcome.get(OUTCOME_DENIED, 0),
            degraded=self.by_outcome.get(OUTCOME_DEGRADED, 0),
        )


def _only(bounds: Sequence[int]) -> int | None:
    """The single element of a 0-or-1 length slice, or ``None``.

    :meth:`AnalyticsCollector._window` derives each bound from a one-element slice (``[-1:]`` /
    ``[:1]``) precisely so an empty series produces an empty list rather than an ``IndexError``.
    This turns that back into the ``int | None`` the model declares, in one place, so the four
    per-series bounds do not become four copies of the same conditional.
    """
    return bounds[0] if bounds else None


def _add(target: dict[str, int], name: str, count: int) -> None:
    """Accumulate ``count`` under ``name``, skipping the empty name a bare prefix would produce.

    A field literally called ``status:`` — which nothing writes, but which a hand-edited key could
    carry — would otherwise create a dimension row with a blank label that a dashboard renders as
    an unexplained empty bar.
    """
    if not name:
        return
    target[name] = target.get(name, 0) + count


class AnalyticsCollector:
    """Records one request per call, and reads whole windows back. See the module docstring.

    Constructed synchronously and performs **no I/O**, the same contract as
    :class:`~src.redis_client.RedisGateway`, :class:`~src.tiers.TierRegistry` and
    :class:`~src.limiter.Limiter`, so ``Runtime.build`` stays a plain function and the
    ``create_app(runtime=...)`` seam never opens a socket.

    ``clock`` is injectable and is :func:`time.time` — the **wall** clock, not
    :func:`time.monotonic`, and that is the one place in this project where that is the right
    choice. Every other clock here measures a duration (uptime, a breaker cooldown, a request's
    latency) and must survive an NTP step; this one has to name an absolute minute that another
    replica and a dashboard will agree on, and a monotonic clock has no epoch to name it in.
    """

    def __init__(
        self,
        gateway: RedisGateway,
        settings: Settings,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._gateway = gateway
        self._settings = settings
        self._clock = clock

        # Pre-formatted, because they are fixed for the life of the process and formatting them per
        # request would be two string conversions on every served request to reach two constants.
        # Floored at 0, which the script reads as "do not set a TTL at all" — an operator who zeroes
        # the retention gets buckets that never expire, which is visible in `INFO keyspace` rather
        # than being silently reinterpreted as some default.
        self._minute_ttl_sec = str(max(0, int(settings.analytics_minute_ttl_sec)))
        self._hour_ttl_sec = str(max(0, int(settings.analytics_hour_ttl_sec)))
        self._max_buckets = max(0, int(settings.analytics_max_buckets))

        # The non-blocking in-flight gate. See the pool rubric in the module docstring.
        self._max_inflight = max(
            1, int(settings.redis_max_connections) // ANALYTICS_POOL_SHARE_DIVISOR
        )
        self._inflight = 0

        #: Requests successfully folded into a bucket.
        self.records = 0
        #: Requests whose record was **lost** — the number that answers "what fraction of my
        #: traffic is actually on this graph?". ``records + dropped`` is every attempt.
        self.dropped = 0
        #: Of those drops, how many were an exception this collector swallowed. Strictly less than
        #: :attr:`dropped` whenever the gateway was never connected (the hermetic
        #: ``create_app(runtime=...)`` seam), which is a wiring state rather than a failure and
        #: must not be counted as one — an ``errors`` that ticks up once per request on a runtime
        #: nobody started is an alert that fires on every test run.
        self.errors = 0
        #: Of those drops, how many were **shed to protect the pool** — a record that arrived while
        #: :attr:`_max_inflight` writes were already in flight. Its own counter because it is the
        #: only drop reason that means the system is working as designed rather than failing: a
        #: rising ``shed`` next to a flat ``errors`` says "load, and the limiter is winning", which
        #: is a different operational story from "the store is broken".
        self.shed = 0
        #: ``repr`` of the most recent swallowed exception, or ``None``. One string, so an operator
        #: who sees a non-zero ``errors`` on ``/health`` or in C11's payload has somewhere to look
        #: that is not "grep the logs of whichever replica it was".
        self.last_error: str | None = None

        #: Completed :meth:`snapshot` calls.
        self.snapshots = 0
        #: Snapshots that returned a **partial** window because the cap truncated the fan-in.
        self.truncated_snapshots = 0
        #: Total buckets those truncations dropped, so "how much history is the dashboard not
        #: showing?" is answerable without re-deriving it from the settings.
        self.buckets_dropped = 0

        # Registering at construction is what the constructor is for, and it is also allowed to be
        # a no-op: `Runtime.build` is synchronous and I/O-free by contract, so in production the
        # gateway is not connected yet and `register` raises RuntimeError. Exactly the pattern
        # `src.limiter.Limiter` uses, for the same reason — `record` and `snapshot` register on
        # demand, and both paths reach the same handle because the script body, and therefore its
        # SHA, is a constant.
        try:
            self._ensure_registered()
        except RuntimeError:
            pass

    # ------------------------------------------------------------------ #
    # Script registration
    # ------------------------------------------------------------------ #
    def _ensure_registered(self) -> None:
        """Attach the record script's handle to the gateway if it is not already attached.

        Idempotent and cheap — the happy path is one dict lookup — and called on every
        :meth:`record` rather than once, because the handle can legitimately disappear underneath
        us: :meth:`~src.redis_client.RedisGateway.aclose` drops every registered handle precisely so
        a reconnect cannot dispatch onto a dead client.
        """
        try:
            self._gateway.script(RLQ_RECORD_REQUEST_NAME)
        except KeyError:
            self._gateway.register(RLQ_RECORD_REQUEST_NAME, RLQ_RECORD_REQUEST)

    # ------------------------------------------------------------------ #
    # Write side
    # ------------------------------------------------------------------ #
    @staticmethod
    def outcome_of(decision: LimitDecision | None) -> str:
        """Which of :data:`OUTCOMES` this decision belongs in. See the constants for the ordering.

        ``None`` means the request never got a decision — a 401, or the identity path's 503 — and
        is counted as :data:`OUTCOME_DENIED`, because it was refused. The status dimension keeps
        the distinction from a 429.
        """
        if decision is None:
            return OUTCOME_DENIED
        if decision.degraded:
            return OUTCOME_DEGRADED
        return OUTCOME_ALLOWED if decision.allowed else OUTCOME_DENIED

    def _bucket_ms(self, decision: LimitDecision | None, now_ms: int | None) -> int:
        """The instant this record is bucketed at. See the clock rubric in the module docstring.

        The decision's clock wins whenever there is one carrying a usable value, and it wins over
        the explicit ``now_ms`` argument too. That ordering is the whole property: an override able
        to beat the shared clock would be a seam that reintroduces exactly the per-replica bucketing
        it exists to prevent, and it would do so silently. ``now_ms`` is what a caller supplies
        *instead of* this replica's wall clock on the paths that have no decision to read.
        """
        if decision is not None and decision.server_now_ms > 0:
            return decision.server_now_ms
        if now_ms is not None:
            return now_ms
        return int(self._clock() * MS_PER_SECOND)

    async def record(
        self,
        decision: LimitDecision | None,
        *,
        status_code: int,
        user_id: str,
        endpoint: str,
        tier: str,
        cost: int,
        now_ms: int | None = None,
    ) -> bool:
        """Fold one served request into its minute bucket, hour bucket and top-consumer ZSET.

        **This coroutine cannot raise.** See the "may never break a request" rubric in the module
        docstring: it is called after the response body is already on the wire, so there is no
        status code left to change and nothing a failure here could usefully do except damage a
        request that already succeeded.

        Args:
            decision: The verdict this request was served under, or ``None`` for a request that
                never got one (a 401, or the identity path's 503). Supplies the shared clock and
                the outcome, and nothing else.
            status_code: What the client actually received. The one dimension only the response
                knows, which is why the call site is below ``await self.app(...)``.
            user_id: The principal, or :data:`ANONYMOUS_USER_ID`.
            endpoint: The **classified** label from :func:`src.keys.classify`, never a raw path —
                that bound is what keeps the number of ``endpoint:*`` hash fields finite.
            tier: The tier, or :data:`UNKNOWN_TIER`.
            cost: Weighted units this request ATTEMPTED to spend — the weight the limiter was
                asked for, which for a refused request is not the weight it charged. See
                :data:`src.lua.RECORD_FIELD_COST` for why demand rather than spend is the useful
                series, and for the reconciliation caveat that follows.
            now_ms: Test seam, and the fallback clock for the no-decision paths. It does **not**
                override a decision's clock — see :meth:`_bucket_ms`.

        Returns:
            ``True`` if the record landed. ``False`` if it was dropped, which the caller is free to
            ignore — every drop is already counted on :attr:`dropped` and :attr:`errors`.

        The dimensions are taken from the caller's arguments rather than from ``decision``'s own
        ``user_id`` / ``endpoint`` / ``cost`` / ``tier`` fields even when a decision is present, so
        that there is **one** call shape rather than one for the paths that have a decision and one
        for the paths that do not. The middleware passes the same values it passed to
        :meth:`src.limiter.Limiter.check`, so the two agree by construction.
        """
        # The gateway was never connected: `Runtime.build()` without `Runtime.start()`, which is
        # the documented hermetic test seam. Checked BEFORE the try, because it is not a failure —
        # counting it on `errors` would make an alert fire on a configuration nobody deployed, and
        # letting it take the exception path would log a warning per request for a process that was
        # never asked to talk to Redis. It IS a drop, though, and drops are what `dropped` counts.
        if not self._gateway.is_connected:
            self.dropped += 1
            return False

        # ------------------------------------------------------------------------------------ #
        # The non-blocking pool gate. See the "may never take a connection the LIMITER needs"
        # rubric — this branch is what keeps a best-effort statistic from outbidding an
        # enforcement decision for the last connection in a saturated pool.
        #
        # Checked here, before anything is built, so a shed record costs a comparison rather than
        # a key build and an exception. Note there is NO `await` between the test and the
        # increment: asyncio is cooperative and single-threaded, so no other coroutine can run in
        # between and the counter cannot be raced.
        # ------------------------------------------------------------------------------------ #
        # ONE condition, and two richer ones were measured and rejected — see the rubric.
        if self._inflight >= self._max_inflight:
            self.dropped += 1
            self.shed += 1
            return False
        self._inflight += 1

        try:
            at_ms = self._bucket_ms(decision, now_ms)
            minute = minute_index(at_ms)
            hour = hour_index(at_ms)

            keys = [
                stats_minute_key(minute),
                stats_hour_key(hour),
                stats_top_key(minute),
            ]
            args = [
                # Floored at 1 for the same reason `Limiter.check` refuses a cost below 1: a
                # zero-cost request is an unmetered request, and one that contributed nothing to
                # the cost series would be invisible in the ranking that decides who to call about
                # a load problem. Recorded whether or not the request was admitted — this series
                # measures demand, not spend. See `src.lua.RECORD_FIELD_COST`.
                str(max(1, int(cost))),
                self._minute_ttl_sec,
                self._hour_ttl_sec,
                user_id or ANONYMOUS_USER_ID,
                self.outcome_of(decision),
                tier or UNKNOWN_TIER,
                # `or UNKNOWN_ENDPOINT_LABEL` rather than an empty string: an empty suffix would
                # produce the bare `endpoint:` field `_add` has to filter out on the way back, and
                # "other" is the label the classifier already uses for a path it cannot name.
                endpoint or UNKNOWN_ENDPOINT_LABEL,
                str(int(status_code)),
            ]
            # Asserted rather than assumed, once per call and at negligible cost: these two are the
            # contract between this module and the script text in `src.lua`, and a mismatch would
            # not raise on the Lua side — it would silently read `nil` for a missing ARGV and write
            # a bucket field called `tier:false`.
            if len(keys) != RECORD_KEYS_ARITY or len(args) != RECORD_ARGV_ARITY:  # pragma: no cover
                raise ValueError(
                    f"rlq_record_request expects {RECORD_KEYS_ARITY} KEYS and "
                    f"{RECORD_ARGV_ARITY} ARGV, built {len(keys)} and {len(args)}"
                )

            self._ensure_registered()
            await self._gateway.run_script(RLQ_RECORD_REQUEST_NAME, keys=keys, args=args)
        except Exception as exc:  # noqa: BLE001 - deliberate; see the module docstring
            # EVERY exception class, including the ones `src.redis_client` is careful to let
            # propagate elsewhere: a `ResponseError` from a broken record script, a `WRONGTYPE`
            # from a bucket someone SET a string into, a `BackingStoreOverloaded` from a saturated
            # pool, a `ValueError` from an argument this method built wrong. None of them is worth
            # a served request. `CancelledError` is a BaseException and is deliberately not caught.
            self.dropped += 1
            self.errors += 1
            self.last_error = repr(exc)
            # WARNING and not ERROR: this is a dependency failure handled exactly as designed, it
            # arrives at request rate during an outage, and the gateway has already logged the
            # underlying cause once at its own site.
            logger.warning(
                "analytics record dropped for endpoint=%s status=%s: %r "
                "(the request itself was served; %d dropped so far)",
                endpoint,
                status_code,
                exc,
                self.dropped,
            )
            return False
        finally:
            # In a `finally`, so a cancelled record releases its slot too. Without it a
            # `CancelledError` — which this method deliberately does not catch — would leak a slot
            # per cancelled request until the gate was permanently shut and analytics silently
            # stopped, which is precisely the class of bug the gate exists to be simpler than.
            self._inflight -= 1

        self.records += 1
        return True

    # ------------------------------------------------------------------ #
    # Read side
    # ------------------------------------------------------------------ #
    async def _server_now_ms(self) -> int:
        """The **shared** clock, in epoch milliseconds — the one the write side buckets against.

        ``TIME`` returns two RESP strings (seconds, microseconds) even though both are numbers, and
        the milliseconds are floored, exactly as the decision script's own clock block does. Read
        through :meth:`~src.redis_client.RedisGateway.run` so it feeds the breaker and costs
        nothing while the breaker is open.

        There is deliberately **no fallback to the local clock** when this fails. A snapshot whose
        window was named by a skewed replica is precisely the bug this method exists to remove, and
        substituting the wrong clock on failure would reintroduce it exactly when the store is
        least healthy — while looking like a successful read. The failure propagates instead, which
        is :meth:`snapshot`'s stated policy for everything else that goes wrong on this path.
        """
        seconds, microseconds = await self._gateway.run(
            lambda: self._gateway.client.time(), op=OP_SNAPSHOT_CLOCK
        )
        return int(seconds) * MS_PER_SECOND + int(microseconds) // 1000

    def _empty(self, minutes_requested: int, hours_requested: int) -> StatsSnapshot:
        """A zeroed snapshot for a window nobody asked for any buckets of.

        Built without touching Redis at all — not even for the clock — so that "asking for nothing
        costs nothing" is true of the whole method rather than of its pipeline alone. The outcome
        map is still fully seeded, because a chart handed a missing key draws a gap and one handed
        an explicit zero draws the flat line that is the truth.

        ``window.server_now_ms`` is therefore ``None`` here, and that is the honest value: no
        ``TIME`` was issued, and substituting the local clock is the per-replica bucketing the
        module docstring rejects — it would be exactly as wrong on the read side as on the write.
        """
        return StatsSnapshot(
            totals=_Fold().totals(),
            by_outcome=dict.fromkeys(OUTCOMES, 0),
            window=self._window(
                [], [], minutes_requested=minutes_requested, hours_requested=hours_requested
            ),
        )

    async def snapshot(self, *, minutes: int, hours: int) -> StatsSnapshot:
        """Read ``minutes`` minute buckets and ``hours`` hour buckets, folded into one value.

        Bucket names are **computed**, never discovered: see the "never SCAN" rubric. A bucket that
        does not exist comes back as an empty hash and folds to zeros, which is the same answer a
        keyspace walk would have produced for a fraction of the cost.

        Unlike :meth:`record`, this **propagates**: a failed read raises
        :class:`~src.redis_client.BackingStoreUnavailable` out of the gateway so C11's endpoint can
        say the store is unreachable. Returning zeros instead would tell an operator that traffic
        had stopped, at the exact moment they are looking at this page to find out why it has not.

        The window's "now" is **Redis's** clock, not this replica's — see the clock rubric. That
        costs one extra round trip per call, which is free on an endpoint polled every five
        seconds and is the only thing that makes two replicas answer the same question the same
        way.

        Raises:
            BackingStoreUnavailable: the store did not answer (or the breaker is open).
            redis.exceptions.RedisError: a correctness failure, unchanged — see
                :mod:`src.redis_client`.
        """
        minutes_requested = max(0, int(minutes))
        hours_requested = max(0, int(hours))
        if minutes_requested <= 0 and hours_requested <= 0:
            # Asking for nothing costs nothing — not even the clock. Returned before the TIME call
            # rather than after it so "an empty range issues zero Redis commands" stays a property
            # of this method rather than of how few commands happen to be left after the guard in
            # `_read`.
            self.snapshots += 1
            return self._empty(minutes_requested, hours_requested)

        at_ms = await self._server_now_ms()

        # ------------------------------------------------------------------------------------ #
        # The cap bounds the TOTAL fan-in, and minute buckets are served first.
        #
        # A per-series cap — 120 minutes AND 120 hours — would let one read pipeline 240 HGETALLs
        # while the setting is called ANALYTICS_MAX_BUCKETS, i.e. the number would not bound the
        # thing it names. Minutes first because the per-minute series is the live chart the
        # dashboard draws and the hour series is the context line behind it; when the two compete
        # for the last slots, losing an hour of context is a smaller loss than losing the last
        # minutes of "what is happening right now".
        #
        # The starvation this admits is real and is reported rather than hidden: a caller asking
        # for 120 minutes and 24 hours gets 120 minutes and NO hours, `window.hours_covered == 0`,
        # `dropped == 24`, and a WARNING naming both numbers. C11 asks for 60 + 24, which fits.
        # ------------------------------------------------------------------------------------ #
        minute_span = min(minutes_requested, self._max_buckets)
        hour_span = min(hours_requested, max(0, self._max_buckets - minute_span))

        # `recent_minute_indices` for BOTH series. It is pure index arithmetic — "up to N
        # contiguous indices ending at `latest`, descending, clamped so the range never runs off
        # the start of the epoch" — and that clamp is the property being reused, not the unit. The
        # alternative is a second copy of the same three lines, which is a second place for the
        # epoch clamp to be got wrong.
        minute_indices = recent_minute_indices(minute_index(at_ms), minute_span)
        hour_indices = recent_minute_indices(hour_index(at_ms), hour_span)

        requested = minutes_requested + hours_requested
        covered = len(minute_indices) + len(hour_indices)
        dropped = max(0, requested - covered)

        self.snapshots += 1
        if dropped:
            self.truncated_snapshots += 1
            self.buckets_dropped += dropped
            # Logged, and logged loudly, because a silently truncated window reads as a complete
            # one: the payload has the same shape either way, and a chart drawn from 120 of the
            # 144 buckets that were asked for looks exactly like a chart drawn from all of them.
            logger.warning(
                "analytics snapshot truncated: asked for %d minute + %d hour buckets, covered "
                "%d + %d (ANALYTICS_MAX_BUCKETS=%d) — %d bucket(s) dropped, so this window is "
                "PARTIAL",
                minutes_requested,
                hours_requested,
                len(minute_indices),
                len(hour_indices),
                self._max_buckets,
                dropped,
            )

        minute_hashes, hour_hashes, top_slices = await self._read(minute_indices, hour_indices)

        fold = _Fold()
        # Reversed on the way out: the index lists are newest-first (that is what
        # `recent_minute_indices` is for), and a time series is drawn oldest-first. One reversal
        # here rather than one in every consumer. See the rubric on `StatsSnapshot`.
        per_minute = [
            fold.bucket(raw, index=index, width_ms=MS_PER_MINUTE, dimensions=True)
            for index, raw in zip(minute_indices, minute_hashes)
        ][::-1]
        # `dimensions=False`: the hour buckets describe the SAME requests at a coarser resolution,
        # so folding them into the totals would count every request twice — and unevenly, because
        # the hour window reaches further back than the minute window does.
        per_hour = [
            fold.bucket(raw, index=index, width_ms=MS_PER_HOUR, dimensions=False)
            for index, raw in zip(hour_indices, hour_hashes)
        ][::-1]

        return StatsSnapshot(
            totals=fold.totals(),
            per_minute=per_minute,
            per_hour=per_hour,
            by_status=fold.by_status,
            by_endpoint=fold.by_endpoint,
            by_tier=fold.by_tier,
            by_outcome=fold.by_outcome,
            top_consumers=self._rank(top_slices),
            window=self._window(
                minute_indices,
                hour_indices,
                minutes_requested=minutes_requested,
                hours_requested=hours_requested,
                # The SAME `TIME` the bucket names were computed from, not a second call and not
                # the local clock. A consumer clipping the newest still-filling bucket has to clip
                # it against the instant that *named* it, or the two disagree by the round trip.
                server_now_ms=at_ms,
            ),
            dropped=dropped,
            buckets_read=covered,
        )

    async def _read(
        self, minute_indices: Sequence[int], hour_indices: Sequence[int]
    ) -> tuple[list[Any], list[Any], list[Any]]:
        """Issue the whole window as ONE pipeline and split the replies back into three lists.

        ``HGETALL`` per minute bucket, ``HGETALL`` per hour bucket, and
        ``ZREVRANGE 0 <fanout-1> WITHSCORES`` per minute bucket — in that order, so the split below
        is arithmetic on known lengths rather than a search through the replies.

        ``transaction=False``: this is a batch of independent reads, and wrapping it in
        ``MULTI``/``EXEC`` would hold Redis's single thread for the whole batch to buy an atomicity
        nothing here needs — a counter that ticks between two ``HGETALL``s changes one bucket's
        number by one, which is the same uncertainty the 5-second poll interval already has.

        Routed through :meth:`~src.redis_client.RedisGateway.run` rather than touching the client
        directly, so the read feeds the circuit breaker and short-circuits for free while the
        breaker is open — a dashboard polling every 5 seconds through a 250 ms timeout is otherwise
        a second source of load on a store that has already stopped answering.
        """
        if not minute_indices and not hour_indices:
            # Nothing to ask for. Returning early rather than executing an empty pipeline keeps the
            # "an empty range costs zero Redis commands" property assertable rather than dependent
            # on how redis-py handles an empty command stack.
            return [], [], []

        minute_keys = [stats_minute_key(index) for index in minute_indices]
        hour_keys = [stats_hour_key(index) for index in hour_indices]
        top_keys = [stats_top_key(index) for index in minute_indices]

        async def _pipelined() -> list[Any]:
            async with self._gateway.client.pipeline(transaction=False) as pipe:
                for key in minute_keys:
                    pipe.hgetall(key)
                for key in hour_keys:
                    pipe.hgetall(key)
                for key in top_keys:
                    # FANOUT, not LIMIT. Reading only as many as are returned is what makes a
                    # steady consumer who never tops a single minute invisible — see `_rank`.
                    pipe.zrevrange(key, 0, TOP_CONSUMERS_FANOUT - 1, withscores=True)
                return list(await pipe.execute())

        replies = await self._gateway.run(_pipelined, op=OP_SNAPSHOT)

        minute_count = len(minute_keys)
        hour_count = len(hour_keys)
        return (
            replies[:minute_count],
            replies[minute_count : minute_count + hour_count],
            replies[minute_count + hour_count :],
        )

    @staticmethod
    def _rank(top_slices: Iterable[Any]) -> list[TopConsumer]:
        """Merge the per-minute top slices into one ranking, heaviest first.

        .. rubric:: What this can get wrong, stated accurately — it is a MISS, not a mis-ordering

        Each ``ZREVRANGE 0 <fanout-1>`` returns that minute's :data:`TOP_CONSUMERS_FANOUT` heaviest
        principals and the merge sums a principal's score across every covered minute. **A
        principal who never enters a single minute's top slice is invisible to this ranking, no
        matter how large their total is.**

        That is a genuine miss rather than a cosmetic re-ordering, and the error is unbounded in
        the wrong direction. An earlier version read only the ten it returns, and the C9
        verification constructed the failure exactly: over a 60-minute window with ten burst
        callers per minute and one steady consumer sitting eleventh in every single minute, the
        steady consumer's real total was **5940 against a reported number-one of 100 — 59.4x, and
        absent from the list entirely.** The error grows linearly with the covered window, so a
        60-minute dashboard panel is the worst configuration rather than an unusual one.

        :data:`TOP_CONSUMERS_FANOUT` is 5x what is returned for exactly that reason. It does not
        make the miss impossible — nothing bounded does — it makes it require **fifty** heavier
        callers in *every single minute* of the window instead of ten, which is the difference
        between a shape ordinary traffic produces and one somebody has to construct. The residual
        error is stated rather than hidden: a consumer ranked below 50th in every covered minute
        will not appear, and their true total can exceed the reported leader's without bound.

        The alternative is exactness at a price that removes the reason for the ZSET: the true
        answer needs every member of every covered minute — one per distinct principal per minute —
        which is the ``O(N)`` transfer over the wire that a HASH would already have given us, on
        the endpoint a dashboard polls every 5 seconds. 50 per bucket is ``O(covered x 50)``:
        bounded by the window rather than by how many principals the service has.

        Ties break on ``user_id`` ascending, so the ranking is deterministic and a test can assert
        an order rather than a set.
        """
        merged: dict[str, int] = {}
        for reply in top_slices:
            for member, score in reply or ():
                user_id = _text(member)
                merged[user_id] = merged.get(user_id, 0) + _score(score)

        ranked = sorted(merged.items(), key=lambda pair: (-pair[1], pair[0]))
        return [
            TopConsumer(user_id=user_id, cost=cost)
            for user_id, cost in ranked[:TOP_CONSUMERS_LIMIT]
        ]

    @staticmethod
    def _window(
        minute_indices: Sequence[int],
        hour_indices: Sequence[int],
        *,
        minutes_requested: int,
        hours_requested: int,
        server_now_ms: int | None = None,
    ) -> StatsWindow:
        """Describe what was actually covered. See :class:`~src.models.StatsWindow`.

        ``start_ms`` / ``end_ms`` span **both** series, so they answer "what period does this
        payload describe?" rather than "what period does one of its two charts describe". In
        practice the hour series sets the start and the minute series sets the end, but taking the
        min and the max rather than assuming that keeps the answer correct for a caller who asked
        for one series and not the other.

        .. rubric:: Each series also publishes its OWN bounds, and a consumer needs them

        Added after C11's verification measured the consequence of publishing only the union.
        ``totals`` and every ``by_*`` are folded from the minute buckets alone, so on the default
        60-minute + 24-hour read the spanning bounds describe 24 h while every number beside them
        describes 60 minutes — a KPI labelled from ``start_ms``/``end_ms`` is wrong by 24x, and
        ``end_ms`` (the close of the current *hour* bucket) was measured 52 minutes in the future.
        Neither is a defect in the spanning fields; they are correct about the question they
        answer. The defect was that they were the only ones on offer.

        ``server_now_ms`` is the read's own instant, carried through from ``snapshot``'s ``TIME``
        call so a consumer can clip the still-filling newest bucket without consulting its own
        clock — which, on a page polling two replicas through a load balancer, is the one clock
        that would disagree. It stays ``None`` on the empty-range path, where no ``TIME`` was
        issued and inventing one would be the local-clock substitution the module docstring refuses
        everywhere else.

        This is additive only: every field that existed before is computed from the same
        expressions, so the folding semantics C9 pinned are untouched.
        """
        minute_starts = [index * MS_PER_MINUTE for index in minute_indices[-1:]]
        minute_ends = [(index + 1) * MS_PER_MINUTE for index in minute_indices[:1]]
        hour_starts = [index * MS_PER_HOUR for index in hour_indices[-1:]]
        hour_ends = [(index + 1) * MS_PER_HOUR for index in hour_indices[:1]]

        starts = minute_starts + hour_starts
        ends = minute_ends + hour_ends

        return StatsWindow(
            minutes_requested=minutes_requested,
            minutes_covered=len(minute_indices),
            hours_requested=hours_requested,
            hours_covered=len(hour_indices),
            newest_minute_index=minute_indices[0] if minute_indices else None,
            oldest_minute_index=minute_indices[-1] if minute_indices else None,
            newest_hour_index=hour_indices[0] if hour_indices else None,
            oldest_hour_index=hour_indices[-1] if hour_indices else None,
            minutes_start_ms=_only(minute_starts),
            minutes_end_ms=_only(minute_ends),
            hours_start_ms=_only(hour_starts),
            hours_end_ms=_only(hour_ends),
            start_ms=min(starts) if starts else None,
            end_ms=max(ends) if ends else None,
            server_now_ms=server_now_ms,
        )

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    def stats(self) -> dict[str, Any]:
        """Counter snapshot for C11's stats payload (and for anything else that asks).

        Deliberately **not** published on ``GET /health``: see the "What is deliberately NOT
        reported here" rubric in :mod:`src.api.health`. That body is a pinned contract with an
        asserted key set, not a dashboard, and C11 is where every component's counters are
        published together — which is also the only place they can be compared.
        """
        return {
            "records": self.records,
            "dropped": self.dropped,
            "errors": self.errors,
            # Reported beside `errors` and never folded into it: a rising `shed` with a flat
            # `errors` says the pool gate is doing its job under load, which is the opposite
            # diagnosis from a store that is failing. One number could not say either.
            "shed": self.shed,
            "max_inflight": self._max_inflight,
            "last_error": self.last_error,
            "snapshots": self.snapshots,
            "truncated_snapshots": self.truncated_snapshots,
            "buckets_dropped": self.buckets_dropped,
            "minute_ttl_sec": int(self._minute_ttl_sec),
            "hour_ttl_sec": int(self._hour_ttl_sec),
            "max_buckets": self._max_buckets,
        }
