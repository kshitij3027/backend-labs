"""The observability surface — ``GET /dashboard/api/stats``, and the shell C15 will fill in.

:mod:`src.analytics` writes one record per served request and reads whole windows back. This
module is the *only* place those windows reach a client, and it is a thin wrapper over
:meth:`~src.analytics.AnalyticsCollector.snapshot` by design: the collector folds, this file
wraps the fold in the configuration and health context that makes it interpretable, and nothing
here recomputes a number the collector already produced.

.. rubric:: Unmetered, unauthenticated, and the hole is STATED rather than quietly shipped

``/dashboard`` is on :data:`src.middleware.EXEMPT_PATH_PREFIXES`, so nothing under it is rate
limited. That half is not negotiable and is the same argument the admin API's exemption rests on:
this is the page an operator opens *because* everything is 429ing, and a limiter that throttles
the view of itself makes the incident invisible at exactly the moment it matters. The container
healthcheck, C13's verifier and C15's 5-second poll all reach it, none of them carries a
credential, and none of them should be charged to anybody's quota.

The other half is a real hole, and it is worth naming precisely rather than burying:

    **This endpoint is unauthenticated and it publishes user ids.** :attr:`top_consumers` is a
    ranking of principals by attempted cost. Anyone who can reach the port learns who the heaviest
    callers of this service are, and — via ``totals`` and ``per_minute`` — what the whole service's
    request rate is. That is operator information being served to anonymous callers.

**In a real deployment ``top_consumers`` is the block that goes behind ``ADMIN_TOKEN``** — the
same in-process :func:`hmac.compare_digest` check :func:`src.api.admin.require_admin_token`
already implements, applied to one field of one payload, leaving the aggregate counters public so
the page still renders for anyone. It is left open here for exactly one reason, and it is a
product decision rather than a security one: this is a demo whose entire point is that ``make up``
followed by opening a browser shows the limiter working, and a credential prompt in front of that
would be the first thing anyone reading this repository had to work around. That is a considered
trade with a stated cost; anyone deploying this for real should treat the paragraph above as the
change list.

Note what is deliberately **not** offered as justification: that some other document says the
endpoint is public. It is not documented as unauthenticated anywhere outside this module — the
README's route table lists the feed without saying so, in the same table where ``/health`` *is*
marked unauthenticated — and citing a document that does not corroborate the claim would make this
disclosure weaker than no disclosure, because the next reader would stop here instead of checking.
The four places that do state it are all in code a developer reads: this docstring, the OpenAPI
``description`` on the route below, the ``top_consumers`` field description on
:class:`~src.models.DashboardStats`, and the mount comment in :func:`src.main.create_app`. C16 is
where the README is brought into line.

The aggregate half of the leak is smaller but not zero, and is accepted on the same terms:
``totals.requests`` is a public request-rate meter for the whole service, which is precisely the
side channel ``src/api/health.py`` refuses to open on the liveness probe. The difference is that
``/health`` is a *pinned contract* consumed by orchestrators, while this is the dashboard's own
feed — but "the other endpoint is stricter" is not a security property, so: same hole, same
``ADMIN_TOKEN`` remedy, stated in the same breath.

.. rubric:: ``rate_limit_enabled`` is MANDATORY, and it is the only field that can say this

Carried forward from C9's verification, and it is the reason this envelope exists at all rather
than the endpoint returning :class:`~src.models.StatsSnapshot` directly.

With ``RATE_LIMIT_ENABLED=false`` the middleware returns at step 3 of its own flow — before
identity, before the decision, and before the analytics record. Nothing is written. So every
number this endpoint serves is **byte-identical** to a healthy, fully-metered service that simply
has no callers: ``totals.requests == 0``, both series flat, every ``by_*`` empty. And ``GET
/health`` reports ``rate_limiter: "active"`` throughout, because that field tracks the C8 fallback
bucket rather than the switch, and no field anywhere else in this service names the switch at all.

Two states that could not be more different — "we are metering and nobody is calling" versus "we
are serving every request unauthenticated and unmetered, and cannot see any of it" — rendered
identically on every surface. :attr:`~src.models.DashboardStats.rate_limit_enabled` is the single
bit that separates them. It is load-bearing, and it is why this payload is flat: no client may be
able to fetch, cache or render the empty chart without the field that explains it.

.. rubric:: Never ask for hours without minutes

Also from C9's verification. ``totals`` and every ``by_*`` are folded from the **minute** buckets
only — the hour buckets describe the same requests at a coarser resolution, so folding both would
double-count, and unevenly, because the hour window reaches further back. That is
:class:`~src.models.StatsSnapshot`'s documented behaviour and it is correct.

The consequence is a payload shape that looks broken: a caller asking for hours alone gets a
populated hourly chart beside zeroed KPI tiles and empty breakdowns, and every reasonable reader
concludes the service is down. So this endpoint makes that shape **unreachable** rather than
documenting it: ``minutes`` is floored at :data:`MIN_MINUTES` before the collector ever sees it, so
``minutes=0`` (and ``minutes=-5``) is answered with one minute of real data rather than with an
honest-but-misleading emptiness. Clamped rather than 422'd, per the rubric below — the shape is
prevented, not punished.

.. rubric:: Both parameters are CLAMPED. This endpoint does not 422

The house pattern, argued in full at :func:`src.api.protected.clamp_limit`: the server already
knows the right answer, so refusing an over-large ``minutes`` teaches the caller nothing that
handing them the ceiling does not.

.. rubric:: The two ceilings are different things, and only one of them may be silent

This is the distinction an earlier version of this file got wrong, so it is spelled out.

``ANALYTICS_MAX_BUCKETS`` bounds **how much is read** — the collector's total fan-in, minutes
served first. It is *not* applied here. A window it truncates must stay visible as a truncation:
``window.minutes_covered`` below ``window.minutes_requested``, ``dropped.buckets`` counting the
difference, and a WARNING on the replica naming both numbers.

That only works if ``*_requested`` is **the caller's ask**. Clamping to the cap before the
collector sees it makes the two numbers agree by construction, and a partial answer then reads as
a complete one — which is precisely what :class:`~src.models.StatsWindow` exists to prevent. The
sharp case is not the absurd ask: it is a deployment with a low cap where C15's page sends *no
parameters at all* and is handed a one-minute chart reporting itself as fully covered.

:data:`MAX_REQUESTABLE_BUCKETS` is a different kind of bound and does nothing to the read. It caps
the *number a caller can put in the payload*, because ``dropped`` is arithmetic on the request and
an unauthenticated caller sending ``minutes=10**30`` would otherwise produce an integer the JSON
encoder cannot serialise — a 500 on the endpoint whose whole job is not to have one. It sits far
above any plausible cap, so in every real configuration the visible truncation is the collector's.

Both are reported the same way: the effective ask comes back in ``window.minutes_requested`` /
``hours_requested``, and what was actually covered beside it. Nothing about a partial window is
left to be inferred from a log line on a replica nobody is looking at.

.. rubric:: Three time ranges, and a chart that picks the wrong one is off by 24x

``window.start_ms`` / ``end_ms`` span **both** series. ``totals`` and every ``by_*`` come from the
minute series alone. On the default request those are a 24-hour range and a 60-minute measurement
sitting next to each other in the same object, so a tile captioned from the spanning pair states a
period 24 times longer than the number it captions — and ``end_ms``, being the close of the
*current hour bucket*, was measured 52 minutes in the future.

Neither field is wrong about its own question; the problem was that they were the only ranges on
offer. So :class:`~src.models.StatsWindow` now also carries ``minutes_start_ms`` /
``minutes_end_ms`` (the period ``totals`` actually describes, and the correct x-axis domain for
``per_minute``), ``hours_start_ms`` / ``hours_end_ms``, and ``server_now_ms`` — Redis's clock at
the instant of the read, which is the line to clip the still-filling newest bucket at. Every
``*_end_ms`` is a bucket *close* rather than "now", which is right for a bar chart's domain and
wrong to print; the model states which to use for which, and C15 has no excuse left.

.. rubric:: When Redis is down this endpoint answers 200 with what is knowable

The failure mode being avoided is specific: an observability endpoint that 500s during an incident
is lost at the exact moment it is the thing being opened. So a store failure is caught, and the
payload is served with

* every measurement zeroed **and** ``degraded.stats_unavailable: true`` beside it, plus
  ``degraded.store``, ``degraded.detail`` and ``dropped.buckets`` naming what was not read;
* every configuration field still true and still useful — ``tiers``, ``config_version``,
  ``rate_limit_enabled``, ``poll_ms``, ``served_by`` never needed the store.

This does **not** contradict :meth:`~src.analytics.AnalyticsCollector.snapshot`'s refusal to
swallow. That method must not return zeros, because a *caller* handed silent zeros would report
that traffic had stopped — the single most misleading thing an observability surface can say. The
flag is what makes the zeros safe to serve: they are labelled "not measured" rather than presented
as "nothing happened", which is a claim only this layer, with a response envelope to put the label
in, is able to make.

**A correctness error is deliberately NOT caught**, and becomes a 500. :mod:`src.redis_client`
classifies a ``WRONGTYPE`` on ``stats:min:*`` or a broken script as a bug in this service rather
than an outage, and reporting one as ``store: "unreachable"`` would send an operator to debug a
Redis that is answering every other client perfectly. Same rule, same reasoning, as
:func:`src.api.health._probe_redis`.

.. rubric:: What this endpoint does NOT claim about replicas

The analytics record carries six dimensions and none of them is "which replica served this". So
there is no per-replica breakdown to compute, ``replicas.observed`` is empty, and
``replicas.attributed`` is ``false``. Reporting ``API_REPLICAS`` as though it were a measurement
would state as fact the one thing this payload has no evidence for, on the page an operator opens
to find out whether a replica has stopped. C12 is what makes multiple replicas real; until then
``served_by`` names the one replica this payload can honestly speak for.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Final, NoReturn

from fastapi import APIRouter, HTTPException, Query, Request, status

from src.analytics import MS_PER_SECOND, OUTCOMES
from src.api.health import (
    POOL_OK,
    POOL_SATURATED,
    REDIS_OK,
    REDIS_SATURATED,
    REDIS_UNREACHABLE,
    SERVED_BY,
)
from src.models import (
    DashboardStats,
    DegradedSignals,
    DroppedSignals,
    PoolSignals,
    ReplicaInfo,
    StatsSnapshot,
    StatsTotals,
    StatsWindow,
)
from src.redis_client import BackingStoreUnavailable

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_HOURS",
    "DEFAULT_MINUTES",
    "MAX_REQUESTABLE_BUCKETS",
    "MIN_HOURS",
    "MIN_MINUTES",
    "SHELL_PENDING_DETAIL",
    "clamp_hours",
    "clamp_minutes",
    "router",
]

# ---------------------------------------------------------------------------------------------
# Window sizing
# ---------------------------------------------------------------------------------------------

#: Minute buckets a caller gets without asking. An hour of per-minute resolution is what a live
#: chart draws, and 60 + :data:`DEFAULT_HOURS` fits inside the shipped ``ANALYTICS_MAX_BUCKETS``
#: (120) — so the default request is never truncated, which is the only default worth having on an
#: endpoint whose truncation behaviour is otherwise correct but partial.
DEFAULT_MINUTES: Final = 60

#: Hour buckets a caller gets without asking. A day of context behind the live chart.
DEFAULT_HOURS: Final = 24

#: **The floor that makes the hours-only shape unreachable.** See the rubric in the module
#: docstring: ``totals`` and every ``by_*`` come from the minute buckets, so a window with zero
#: minutes renders every KPI tile empty beside a populated hourly chart and reads as an outage.
#: One minute is the smallest window that cannot lie about that.
MIN_MINUTES: Final = 1

#: Hours floor at zero, and that asymmetry is deliberate. ``hours=0`` is a legitimate ask — "just
#: the live chart, skip the context line" — and it costs nothing that the minute series does not
#: already provide. It is the *reverse* omission that produces the misleading payload.
MIN_HOURS: Final = 0

#: The largest window a caller may *ask* for, per series. **Not** a bound on how much is read —
#: ``ANALYTICS_MAX_BUCKETS`` is that, inside the collector, and it is deliberately not applied here
#: so that its truncation stays visible as ``covered < requested``. See the two-ceilings rubric.
#:
#: What this bounds is the arithmetic that ends up *in the payload*: ``dropped`` is
#: ``requested - covered``, so an anonymous caller sending ``minutes=10**30`` would otherwise
#: produce an integer orjson refuses to serialise (it encodes up to 64 bits) — a 500 on the one
#: endpoint that must not have one. 100 000 is ~70 days of minutes and three orders of magnitude
#: above the shipped cap of 120, so it can never be the ceiling that bites in a real
#: configuration; when it does bite it is reported exactly like any other truncation.
MAX_REQUESTABLE_BUCKETS: Final = 100_000

#: The ``detail`` on ``GET /dashboard/``. See :func:`dashboard_shell`.
SHELL_PENDING_DETAIL: Final = (
    "The dashboard page is not built yet — it arrives in C15 (src/static/index.html). Its data "
    "feed is live now: GET /dashboard/api/stats returns the whole payload the page will render, "
    "including totals, per-minute and per-hour series, per-status/endpoint/tier/outcome "
    "breakdowns, the tier table, and poll_ms."
)


def clamp_minutes(requested: int) -> int:
    """Resolve ``minutes`` into the ask this replica will make. **Never raises, never 422s.**

    Two adjustments, and they are not the same kind of thing.

    The **floor** is a substitution: ``minutes=0`` is answered with one minute of real data,
    because the alternative is the hours-only payload that reads as an outage. The substituted
    value *is* the ask from here on, and it is what ``window.minutes_requested`` reports — the
    house pattern where ``page.limit`` reports the effective page size.

    The **ceiling** is :data:`MAX_REQUESTABLE_BUCKETS`, which exists only to keep ``dropped``
    inside 64 bits. ``ANALYTICS_MAX_BUCKETS`` is deliberately **not** applied: clamping to it here
    would make ``requested`` and ``covered`` agree by construction and hide every truncation it
    causes. That is the whole point of the two-ceilings rubric in the module docstring.
    """
    return max(MIN_MINUTES, min(requested, MAX_REQUESTABLE_BUCKETS))


def clamp_hours(requested: int) -> int:
    """Resolve ``hours`` into the ask this replica will make. **Never raises, never 422s.**

    Floored at :data:`MIN_HOURS` (zero) rather than at one: unlike minutes, an empty hour series
    costs the payload nothing it needs. Same ceiling, for the same serialisation reason.
    """
    return max(MIN_HOURS, min(requested, MAX_REQUESTABLE_BUCKETS))


# ---------------------------------------------------------------------------------------------
# Reading the runtime
#
# `src/api/health.py` reaches everything through `getattr(..., default)`, because a liveness probe
# that raises gets the replica restarted. This module follows `src/api/admin.py` instead: ONE
# guarded read of `app.state.runtime`, then plain attribute access. `Runtime` is a frozen dataclass
# carrying all six collaborators, so a runtime that exists has every one of them, and a second
# layer of `getattr` fallbacks would be dead branches pretending to be safety — each one a value
# this payload could report having never been able to measure it.
# ---------------------------------------------------------------------------------------------


def _runtime(request: Request) -> Any:
    """The process Runtime, or a 503 naming the wiring failure.

    :func:`src.main.create_app` always attaches one, on both the lifespan path and the injected
    seam, so this cannot fire in a constructed app — exactly as :func:`src.api.admin._runtime`
    cannot. A 503 rather than a degraded payload, because with no runtime there is no settings
    object either, and a payload whose ``rate_limit_enabled`` was guessed would be worse than no
    payload at all.
    """
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:  # pragma: no cover - create_app always attaches one
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="this replica has no runtime wired, so no statistics can be assembled",
            headers={"Retry-After": "1"},
        )
    return runtime


def _empty_snapshot(*, minutes: int, hours: int) -> StatsSnapshot:
    """The zeroed measurement served when the store could not be read.

    Every counter is zero and ``dropped`` is **every bucket that was asked for**, which is the
    honest bookkeeping: nothing was covered, so everything was dropped. Read on its own this value
    says "no traffic"; read beside ``degraded.stats_unavailable`` — which is the only way it is
    ever served — it says "not measured". The envelope is what supplies that second half.

    ``by_outcome`` is still fully seeded from :data:`~src.analytics.OUTCOMES` so the map's keys do
    not change shape between a healthy read and a failed one: a chart that has to cope with a
    missing key on the unhappy path is a chart that breaks during the incident.

    Every bound on the window — including ``server_now_ms`` — is left ``None``. The clock is the
    *first* thing :meth:`~src.analytics.AnalyticsCollector.snapshot` asks Redis for, so on this
    path it is genuinely unknown, and filling it from this replica's wall clock would hand a
    consumer a shared-clock instant that no replica agrees on. ``None`` is the answer that cannot
    be silently plotted.
    """
    return StatsSnapshot(
        totals=StatsTotals(requests=0, cost=0, allowed=0, denied=0, degraded=0),
        by_outcome=dict.fromkeys(OUTCOMES, 0),
        window=StatsWindow(
            minutes_requested=minutes,
            minutes_covered=0,
            hours_requested=hours,
            hours_covered=0,
        ),
        dropped=minutes + hours,
        buckets_read=0,
    )


async def _read(collector: Any, *, minutes: int, hours: int) -> tuple[StatsSnapshot, str | None]:
    """Take one snapshot, returning ``(measurement, failure_detail)``.

    Only :class:`~src.redis_client.BackingStoreUnavailable` is caught — which covers its
    :class:`~src.redis_client.BackingStoreOverloaded` subclass, an open breaker, and a gateway that
    was never connected. A correctness error propagates and becomes a 500; see the module
    docstring for why dressing one up as an outage would be actively harmful.

    Logged at WARNING rather than ERROR: this is a dependency failure being handled exactly as
    designed, it arrives once per 5-second poll rather than per request, and the gateway has
    already logged the underlying cause once at its own site.
    """
    try:
        return await collector.snapshot(minutes=minutes, hours=hours), None
    except BackingStoreUnavailable as exc:
        logger.warning(
            "dashboard stats read failed (%s); serving the configuration half of the payload with "
            "degraded.stats_unavailable set — the zeros below are UNMEASURED, not empty",
            exc.op or "analytics",
        )
        return _empty_snapshot(minutes=minutes, hours=hours), repr(exc)


def _degraded(runtime: Any, gateway_stats: dict[str, Any], detail: str | None) -> DegradedSignals:
    """Assemble the four independent "something is not working" signals.

    ``store`` is derived from :attr:`~src.redis_client.RedisGateway.is_overloaded` rather than from
    the caught exception's class, and the two agree by construction: the gateway sets
    ``overloaded_since`` on exactly the failures it raises as
    :class:`~src.redis_client.BackingStoreOverloaded`, and clears it on the next success. Reading
    the gateway rather than the exception keeps this field identical to ``/health``'s ``redis``,
    which matters because an operator comparing the two surfaces mid-incident must not find them
    describing the same replica differently.

    The distinction it preserves is the one C4's verification called out by name: a saturated pool
    means **no packet was ever sent**, so the store's reachability is unknown rather than bad, and
    answering ``unreachable`` would blame a Redis that is answering every other client perfectly.
    """
    if detail is None:
        store = REDIS_OK
    elif runtime.redis.is_overloaded:
        store = REDIS_SATURATED
    else:
        store = REDIS_UNREACHABLE

    return DegradedSignals(
        # The LIMITER's own state, not this read's outcome: a store that just answered a stats
        # pipeline does not mean a request has been metered against it since. Same source, same
        # reasoning, as `/health`'s `rate_limiter` field.
        rate_limiter=runtime.limiter.degraded,
        store=store,
        stats_unavailable=detail is not None,
        since_sec=gateway_stats["degraded_for_sec"],
        breaker=gateway_stats["breaker_state"],
        detail=detail,
    )


def _pool(runtime: Any, gateway_stats: dict[str, Any]) -> PoolSignals:
    """This replica's connection capacity, from the gateway's own recent history.

    Driven by the gateway rather than by whether *this* call got a connection, so a stats read that
    happened to win one does not erase a replica that is shedding requests.
    """
    return PoolSignals(
        state=POOL_SATURATED if runtime.redis.is_overloaded else POOL_OK,
        max_connections=gateway_stats["pool_max_connections"],
        overloads=gateway_stats["overloads"],
        overloaded_for_sec=gateway_stats["overloaded_for_sec"],
    )


def _dropped(analytics_stats: dict[str, Any]) -> DroppedSignals:
    """The write path's lossiness. ``buckets`` is filled in from the snapshot by the envelope.

    Every value comes from :meth:`~src.analytics.AnalyticsCollector.stats`, which is a pure counter
    read — no I/O and no lock — so publishing it costs an attribute lookup per poll. Subscripted
    rather than ``.get()``-ed: these keys are that method's contract, and a typo silently
    publishing a zero is exactly the "a counter nobody can see" failure this block exists to end.
    """
    return DroppedSignals(
        # Placeholder. `DashboardStats.from_snapshot` overwrites it with the snapshot's own count,
        # because the number of buckets a read did not cover is knowable only from that read.
        buckets=0,
        records=analytics_stats["dropped"],
        records_written=analytics_stats["records"],
        errors=analytics_stats["errors"],
        shed=analytics_stats["shed"],
        last_error=analytics_stats["last_error"],
    )


# ---------------------------------------------------------------------------------------------
# The router
#
# Mounted at /dashboard, which `src.middleware.EXEMPT_PATH_PREFIXES` already exempts from metering
# on a path-SEGMENT boundary — so `/dashboard/api/stats` is exempt and `/dashboardx` is not. No
# middleware change was needed for this commit, and that is the point of having the exemption list
# be a literal a reviewer can read.
#
# NOT under /api/v1, despite serving JSON at a path containing `api`. The versioned prefix exists
# so a breaking change can arrive as a new namespace; this feed's consumer is the page shipped in
# the same image, versioned with it by construction. `/dashboard/api/stats` is the feed *of the
# dashboard*, and `src.api.protected.mounted_v1_routes` is scoped to the versioned prefix, so this
# route is invisible to the pricing cross-check without needing a second exclusion list.
# ---------------------------------------------------------------------------------------------

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get(
    "/api/stats",
    response_model=DashboardStats,
    summary="Everything the dashboard renders, in one payload",
    description=(
        "Aggregate usage over a rolling window, plus the configuration and health context needed "
        "to read it. Public, unauthenticated and **never rate limited** — it is the surface an "
        "operator opens *because* everything else is 429ing.\n\n"
        "**Documented hole:** `top_consumers` publishes user ids to anonymous callers. In a real "
        "deployment that block goes behind `ADMIN_TOKEN`; it is open here because this is a "
        "demo.\n\n"
        "**`cost` is weight ATTEMPTED, not consumed.** Refused requests record the price they "
        "tried to spend, so a cost chart over-reports during a throttling event and will not "
        "reconcile against `GET /api/v1/admin/users/{user_id}/usage`'s `daily.used`. The "
        "difference between the two *is* the throttled demand. Label the chart 'attempted'.\n\n"
        "`totals` and every `by_*` are folded from the **minute** buckets only, so `minutes` is "
        "floored at 1 and an hours-only window cannot be requested. Neither parameter is ever "
        "rejected: `window.*_requested` echoes what you asked for and `window.*_covered` says what "
        "was actually read, so a window truncated by `ANALYTICS_MAX_BUCKETS` is visible as "
        "`covered < requested` with `dropped.buckets` counting the difference.\n\n"
        "**Charting note:** `window.start_ms`/`end_ms` span BOTH series, so on the default request "
        "they describe 24 h while `totals` and every `by_*` describe the last 60 minutes. Plot the "
        "minute series and label its KPIs from `window.minutes_start_ms`/`minutes_end_ms`; "
        "`window.server_now_ms` is Redis's clock at read time and is the line to clip the "
        "still-filling newest bucket at.\n\n"
        "Answers **200 even when Redis is unreachable**, with the measurements zeroed and "
        "`degraded.stats_unavailable` set — an observability endpoint that 500s during an "
        "incident is lost exactly when it is needed."
    ),
)
async def stats(
    request: Request,
    minutes: int = Query(
        default=DEFAULT_MINUTES,
        description=(
            f"Minute buckets to fold. Never rejected: 0 or a negative is answered with "
            f"{MIN_MINUTES} minute, and anything above {MAX_REQUESTABLE_BUCKETS:,} is trimmed to "
            "it. The floor is deliberate — totals and every breakdown come from this series, so a "
            "zero-minute window would render every KPI empty beside a populated hourly chart and "
            "look broken. What you asked for is echoed in `window.minutes_requested`; how much of "
            "it `ANALYTICS_MAX_BUCKETS` allowed is `window.minutes_covered`, and the two differ "
            "exactly when this payload is partial."
        ),
    ),
    hours: int = Query(
        default=DEFAULT_HOURS,
        description=(
            f"Hour buckets to fold for the context line. Floored at {MIN_HOURS} — 0 is a "
            "legitimate ask (just the live chart) — and trimmed at the same upper bound as "
            "`minutes`. The collector caps the two series *together*, minutes first, so a large "
            "`minutes` can starve this one entirely: `hours_covered: 0` beside a non-zero "
            "`hours_requested`, counted in `dropped.buckets`, never hidden."
        ),
    ),
) -> DashboardStats:
    """Fold the recent buckets and wrap them in the envelope. One Redis round trip plus a clock.

    The whole method is: resolve the ask, snapshot, read two pure counter maps, assemble. There is
    no arithmetic here on purpose — every number below was produced by the component that owns it,
    so a discrepancy between this page and ``/health`` or the admin API can only be a difference of
    *when*, never of *how*. That includes the truncation bookkeeping: ``window`` and ``dropped``
    are the collector's own account of what it covered, not a subtraction performed here against a
    ceiling this function applied first.

    ``generated_at`` is this replica's wall clock, deliberately a different clock from
    ``window.server_now_ms`` and the bucket bounds (which come from Redis's ``TIME``, so that two
    replicas name the same window). The gap between them is this replica's skew, and it is worth
    being able to read off the payload rather than having to infer it from a chart with two humps
    in it.
    """
    runtime = _runtime(request)
    settings = runtime.settings

    # The ask, not the ceiling. `ANALYTICS_MAX_BUCKETS` is applied by the collector, where the
    # truncation it causes surfaces as `covered < requested` and `dropped.buckets` — see the
    # two-ceilings rubric. Pre-clamping here is what made a one-minute chart report itself as
    # complete on a deployment with a low cap.
    asked_minutes = clamp_minutes(minutes)
    asked_hours = clamp_hours(hours)

    snapshot, detail = await _read(runtime.analytics, minutes=asked_minutes, hours=asked_hours)

    # Synchronous, I/O-free, and the same snapshot the decision script is being handed — so the
    # per-tier bars on the page sit next to the limits that actually produced them. Reading
    # `config:tiers` afresh here would answer "what is stored?" while the useful question is "what
    # is this replica enforcing?", and C10's `GET /api/v1/admin/tiers` argues that at length.
    #
    # It also means this half of the payload keeps answering while Redis is down, which is the
    # whole reason the endpoint can serve a useful 200 during an outage.
    tier_snapshot = runtime.tiers.snapshot()

    # Two pure counter reads — no I/O, no lock. `/health` deliberately publishes neither (its body
    # is a pinned contract, not a dashboard); this is the surface its rubric points at, and the
    # only place these numbers sit next to the request rate that makes them mean anything.
    gateway_stats = runtime.redis.stats()
    analytics_stats = runtime.analytics.stats()

    return DashboardStats.from_snapshot(
        snapshot,
        tiers=tier_snapshot.tiers,
        config_version=tier_snapshot.version,
        # Read straight off Settings, never `getattr(..., True)`. A default of True on the field
        # whose entire job is to reveal that enforcement is off would report the safe-looking
        # answer in exactly the state it exists to expose.
        rate_limit_enabled=settings.rate_limit_enabled,
        poll_ms=settings.dashboard_poll_ms,
        replicas=ReplicaInfo(
            served_by=SERVED_BY,
            configured=settings.api_replicas,
            # Empty, always, and `attributed=False` says why: the recorded bucket fields carry no
            # replica dimension, so any list here would be invented. See the module docstring.
            observed=[],
            attributed=False,
        ),
        degraded=_degraded(runtime, gateway_stats, detail),
        pool=_pool(runtime, gateway_stats),
        dropped_records=_dropped(analytics_stats),
        generated_at=int(time.time() * MS_PER_SECOND),
        served_by=SERVED_BY,
    )


@router.get(
    "/",
    summary="The dashboard page (arrives in C15)",
    response_model=None,
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "There is no page yet. The detail names the commit that brings it and "
            "the feed that is already live."
        }
    },
    description=(
        "Reserved for C15's static shell (`src/static/index.html`). Until then this answers 404 "
        "with a readable detail rather than an empty 200: a placeholder page that renders nothing "
        "is indistinguishable from a broken one, and this path is already advertised in the "
        "README and by `make ui`."
    ),
)
async def dashboard_shell() -> NoReturn:
    """Answer 404 until C15 lands the HTML.

    .. rubric:: 404 with a detail, rather than a 200 placeholder

    Both were available and the choice is deliberate. A 200 returning "coming soon" is a page that
    *exists* as far as every consumer is concerned — a browser bookmarks it, a monitor goes green
    on it, and C15 then has to change a working URL's behaviour rather than fill in a hole. A 404
    is the true statement ("this resource does not exist yet"), it keeps ``make ui``'s note that
    the URL 404s until C15 accurate, and the ``detail`` still gives a human the two things they
    actually want: which commit brings the page, and where the data already is.

    The route exists at all — rather than simply not being declared — so that the answer is *this*
    sentence instead of FastAPI's bare ``{"detail": "Not Found"}``, and so the path appears in
    ``/docs`` with an explanation attached. C15 replaces the body; the path does not move.

    ``NoReturn`` with ``response_model=None``, the same pairing
    :func:`src.api.admin.admin_not_found` uses: FastAPI would otherwise try to build a response
    model out of the return annotation, and this handler only ever raises.
    """
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=SHELL_PENDING_DETAIL)
