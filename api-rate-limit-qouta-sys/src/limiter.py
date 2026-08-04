"""The one call site of the decision script: assemble, ``EVALSHA``, decode.

:class:`Limiter` is deliberately thin. Every *decision* is made inside
:data:`~src.lua.RLQ_CHECK_AND_CONSUME` on Redis's single thread, because that is the only place
where reading four counters and writing four counters is atomic with respect to every other
replica. What is left for Python is the part that must not be in Lua:

* **Key names.** Deterministic calendar arithmetic (which UTC day is it, when does this month end)
  that has to be unit-testable without a server, and that :mod:`src.keys` already owns.
* **The tier table.** Pre-rendered once per snapshot by
  :func:`~src.tiers.render_argv_tail`, so the hot path splices a tuple rather than formatting
  sixteen strings per request.
* **Decoding.** :meth:`~src.models.LimitDecision.from_lua`, which is the consumer half of the
  19-element contract the script produces.

.. rubric:: Why the key names are Python's job and the refill clock is Redis's

They look like the same decision and they are opposites.

A *key name* is a pure function of the calendar: ``quota:daily:{alice}:2026-08-10`` is correct or
incorrect regardless of who computes it, two replicas a few milliseconds apart compute the same
one, and the worst a skewed replica can do is straddle a midnight boundary by that skew — for one
request, on a counter that is about to roll over anyway. In exchange, computing it in Python makes
it testable against every edge of the calendar in microseconds with no server at all, which is the
whole reason :mod:`src.keys` is pure.

A *refill clock* is the opposite: it is differential, so an error does not cancel out, it
accumulates. A replica whose clock runs 40 seconds fast computes a 40-second refill on its very
first request and then keeps refilling its own bucket ahead of everybody else's, permanently. It is
also unfalsifiable from outside — the bucket simply appears to have more headroom than the tier
allows, with no error and no log line anywhere. So the clock is ``redis.call('TIME')``, read inside
the script, shared by every replica by construction.

.. rubric:: What this class does when Redis cannot answer — the C8 decision, made here

:meth:`Limiter.check` **never** raises :class:`~src.redis_client.BackingStoreUnavailable`. It
returns a :class:`~src.models.LimitDecision` in every case, and which one depends on *why* the
store did not answer. Three outcomes, three different events:

``FAIL_MODE=open`` and the store is unreachable
    The request is decided by :class:`~src.fallback.LocalBucketCache`, which reproduces **both**
    rate gates per process — the per-``(user, endpoint)`` burst bucket holding
    ``ceil(tier_burst / API_REPLICAS)`` *and* the account-wide sustained-rate gate holding
    ``ceil(tier_rpm / API_REPLICAS)`` — and admits the request only if both do. Reproducing the
    bucket alone gave each of the five route labels its own allowance, so the degraded ceiling
    became ``labels x share``: a measured 5x overspend on the free tier, i.e. this project's
    founding bug arriving on the endpoint axis. See :mod:`src.fallback` for the quantified bound.

    The decision carries ``degraded=True``, so :meth:`~src.models.LimitDecision.headers` emits
    ``X-RateLimit-Degraded: 1`` and omits every ``X-Quota-*``. This is the spec's graceful
    degradation, and the header is what keeps it from being a silent fail-open, which would be
    indistinguishable from having no rate limiter at all.

``FAIL_MODE=closed`` and the store is unreachable
    A denial with :attr:`~src.models.DenyReason.BACKING_STORE`, which the middleware renders as a
    **503**, not a 429. 429 means "you are over your limit"; this caller is not, and we cannot tell.

The pool is exhausted (:class:`~src.redis_client.BackingStoreOverloaded`)
    A denial with the same reason and therefore also a 503 — but with ``degraded=False``, because
    nothing was degraded. This is the case ``FAIL_MODE`` deliberately does **not** get a vote on:
    the store is healthy, this process simply ran out of connections to it, and serving through
    the fallback would let a traffic burst buy itself an unmetered window at exactly the moment
    the limiter matters most. See the third rubric in :mod:`src.redis_client`.

.. rubric:: What still propagates, and why that is the same decision rather than an exception to it

A Lua ``ResponseError`` propagates untouched, as does the ``ValueError`` from a malformed reply.
Both are bugs in *this service*: the store answered, and the answer was that we are wrong. Routing
either into the degraded path would mean a one-character typo in the decision script silently
disabling rate limiting for every request, with ``/health`` reporting it identically to an unplugged
Redis — the failure :mod:`src.redis_client`'s availability/correctness split exists to prevent,
re-introduced one layer up. They become a 500: visible, attributable, and the correct answer when
the service is the thing that is broken.

.. rubric:: The degraded decision is built from the DEFAULT tier, and it has to be

``user -> tier`` is read from ``user:{uid}`` *inside* the decision script (see
:mod:`src.identity`), so when Redis is unreachable this process does not know what tier the caller
is on and has no honest way to find out. It uses ``DEFAULT_TIER`` — the most restrictive tier —
sized down by ``API_REPLICAS``. Guessing upward would hand an unknown caller the best plan in the
system during precisely the window in which nothing can check; guessing downward is a throttle the
caller notices and that the ``X-RateLimit-Degraded`` header explains. What a tier *means* is still
correct: :meth:`~src.tiers.TierRegistry.snapshot` serves its last good table through an outage.
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from src.config import Settings, TierConfig
from src.fallback import LocalBucketCache, LocalDecision
from src.keys import (
    bucket_key,
    daily_quota_key,
    day_expire_at,
    month_expire_at,
    monthly_quota_key,
    sliding_window_prefix,
    user_key,
)
from src.lua import (
    NO_CLOCK_OVERRIDE,
    RLQ_CHECK_AND_CONSUME,
    RLQ_CHECK_AND_CONSUME_NAME,
    SW_DISABLED,
    SW_ENABLED,
    UNENFORCED_PERIOD,
)
from src.models import DenyReason, LimitDecision, QuotaPeriodState, ceil_seconds
from src.redis_client import (
    BackingStoreOverloaded,
    BackingStoreUnavailable,
    RedisGateway,
)
from src.tiers import TierRegistry

logger = logging.getLogger(__name__)

__all__ = ["FAIL_MODE_OPEN", "Limiter"]

#: Milliseconds per second, named so the two unit conversions below read as conversions rather than
#: as magic multiplications next to a quantity that is already in milliseconds.
MS_PER_SECOND = 1000

#: The ``FAIL_MODE`` value that selects the fallback bucket. Named rather than compared against a
#: literal in three places: ``Settings.fail_mode`` is a ``Literal["open", "closed"]``, so a typo
#: here would be a silent policy inversion that no validator catches.
FAIL_MODE_OPEN = "open"

#: The eight quota fields of a decision made **without** Redis, spliced into both hand-built
#: decisions below.
#:
#: Every one is zero or :attr:`~src.models.QuotaPeriodState.UNENFORCED`, and that is a statement
#: rather than a placeholder: a quota is a cumulative cross-replica counter, and this process
#: cannot know what the other replicas admitted, what it admitted before it restarted, or what was
#: spent before the outage began. There is no local approximation of that number which is not a
#: fabrication — see :mod:`src.fallback`.
#:
#: The three declarations that have to agree on this all do, and none of them is a coincidence:
#: :meth:`~src.models.LimitDecision.headers` omits every ``X-Quota-*`` while ``degraded`` is set;
#: ``limit = 0`` makes :attr:`~src.models.LimitDecision.daily_remaining` report
#: :data:`~src.models.UNLIMITED` rather than "0 left"; and ``UNENFORCED`` is the period state that
#: says a ceiling does not exist, as opposed to ``reset``, which would *claim* a rollover just
#: happened. A client reading the 429 body during degradation is told nothing is being counted,
#: which is true.
_NO_QUOTA: dict[str, Any] = {
    "daily_limit": 0,
    "daily_used": 0,
    "daily_reset_at": 0,
    "daily_state": QuotaPeriodState.UNENFORCED,
    "monthly_limit": 0,
    "monthly_used": 0,
    "monthly_reset_at": 0,
    "monthly_state": QuotaPeriodState.UNENFORCED,
}


class Limiter:
    """Runs one ``EVALSHA`` per metered request and returns a :class:`~src.models.LimitDecision`.

    Constructed synchronously and performs **no I/O**, the same contract as
    :class:`~src.redis_client.RedisGateway` and :class:`~src.tiers.TierRegistry`, so
    ``Runtime.build`` stays a plain function and the ``create_app(runtime=...)`` seam never opens a
    socket.

    Everything that does not change between requests is formatted once, here: the bucket TTL, the
    window width, the sliding-window flag and the default tier name are all fixed for the life of
    the process, and formatting them per request would be four string conversions on the hot path
    to produce four identical strings.
    """

    def __init__(
        self,
        gateway: RedisGateway,
        tiers: TierRegistry,
        settings: Settings,
        *,
        fallback: LocalBucketCache | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._gateway = gateway
        self._tiers = tiers
        self._settings = settings
        self._clock = clock

        # The degraded path's bucket. Constructed here rather than lazily on the first outage: an
        # outage is the worst possible moment to discover that a collaborator's constructor
        # raises, and this one performs no I/O and allocates one empty OrderedDict.
        self._fallback = LocalBucketCache(settings) if fallback is None else fallback

        # Resolved once. `fail_mode` is immutable for the life of the process (it is not runtime
        # configurable, deliberately — flipping a limiter between fail-open and fail-closed mid
        # incident is a policy change, not a tuning knob), so comparing the string per request would
        # be work on the hot path to reach a constant answer.
        self._fail_open = settings.fail_mode == FAIL_MODE_OPEN

        # `Retry-After` for a fail-closed refusal, in seconds. The breaker's cooldown is the honest
        # number: it is precisely how long this process will wait before it next finds out whether
        # the store is back, so telling a caller to return sooner is telling them to be refused
        # again. Floored at 1 — a `Retry-After: 0` is a retry storm.
        self._closed_retry_after_sec = max(1, int(settings.breaker_cooldown_sec))

        # Pre-formatted ARGV constants. Strings rather than ints because redis-py encodes every
        # argument anyway and a str skips one conversion; more importantly, what goes on the wire
        # is then exactly what a MONITOR trace shows and exactly what a test can assert.
        self._bucket_ttl_ms = str(settings.bucket_ttl_sec * MS_PER_SECOND)
        self._window_ms = str(settings.sliding_window_sec * MS_PER_SECOND)
        self._sw_enabled = SW_ENABLED if settings.sliding_window_enabled else SW_DISABLED
        self._default_tier = settings.default_tier

        #: Observability counter. C11 surfaces it; a limiter that has served a million requests and
        #: one that has served none look identical without it.
        self.checks = 0
        #: Decisions made by the local fallback bucket instead of by Redis. **The number that makes
        #: a fail-open non-silent**, alongside the ``X-RateLimit-Degraded`` header: a limiter that
        #: quietly stopped enforcing and one that is enforcing perfectly are the same graph without
        #: it.
        self.degraded_checks = 0
        #: Requests refused because ``FAIL_MODE=closed`` and the store was unreachable.
        self.fail_closed_denials = 0
        #: Requests refused because this process could not get a connection out of its own pool.
        #: Counted **separately** from :attr:`degraded_checks` because it is a different incident
        #: with a different remedy — see the third rubric in :mod:`src.redis_client`.
        self.overload_denials = 0
        #: Monotonic instant the current degraded run began, cleared by the next decision Redis
        #: actually made. Drives ``rate_limiter: "degraded"`` on ``/health``. Deliberately not a
        #: bare boolean: C11 wants to render "degraded for 34 s", and a duration cannot be
        #: reconstructed from a flag.
        self.degraded_since: float | None = None

        # Registering at construction is what the constructor is *for*, and it is also allowed to
        # be a no-op. `Runtime.build` is synchronous and I/O-free by contract, so in production the
        # gateway has not been connected yet and `register` raises RuntimeError. That is not an
        # error worth failing the build over: `check()` registers on demand, and both paths reach
        # the same handle because the script body — and therefore its SHA — is a constant.
        with contextlib.suppress(RuntimeError):
            self._ensure_registered()

    # ------------------------------------------------------------------ #
    # Script registration
    # ------------------------------------------------------------------ #
    def _ensure_registered(self) -> None:
        """Attach the decision script's handle to the gateway if it is not already attached.

        Idempotent and cheap: the happy path is one dict lookup on
        :meth:`~src.redis_client.RedisGateway.script`.

        It is called on **every** check rather than once, because the handle can legitimately
        disappear underneath us — :meth:`~src.redis_client.RedisGateway.aclose` drops every
        registered handle precisely so a reconnect cannot dispatch onto a dead client. Registering
        once in ``__init__`` and assuming it holds would make a reconnected gateway raise
        ``KeyError`` on every subsequent request.

        A gateway that was never connected raises ``RuntimeError`` from ``register``, unchanged and
        deliberately: that is a wiring bug (``Runtime.start()`` was never awaited), not a store
        outage, and dressing it up as :class:`~src.redis_client.BackingStoreUnavailable` would make
        C8 fail *open* on it — i.e. silently stop enforcing anything because someone forgot a line
        in the lifespan.
        """
        try:
            self._gateway.script(RLQ_CHECK_AND_CONSUME_NAME)
        except KeyError:
            self._gateway.register(RLQ_CHECK_AND_CONSUME_NAME, RLQ_CHECK_AND_CONSUME)

    # ------------------------------------------------------------------ #
    # The hot path
    # ------------------------------------------------------------------ #
    async def check(
        self,
        principal_user_id: str,
        endpoint_label: str,
        cost: int,
        *,
        now: datetime | None = None,
        now_ms_override: int | None = None,
    ) -> LimitDecision:
        """Evaluate all four gates for one request and consume the allowance if they all pass.

        Args:
            principal_user_id: The principal being metered. Goes through
                :func:`src.keys.hash_tag`, which refuses an empty id or one containing a brace —
                either would let a caller collide with somebody else's key slot.
            endpoint_label: The **classified** label from :func:`src.keys.classify`
                (``GET:/api/v1/logs/query``), never a raw request path. Using the raw path would
                make the set of bucket keys the set of possible URLs, which is unbounded and chosen
                by the caller.
            cost: Weighted units this request consumes, ``>= 1``.
            now: The instant used for **key derivation only** — which UTC day and month the quota
                counters belong to, and when they expire. Defaults to now, and must not be before
                the unix epoch (see :meth:`_period_boundaries`). It is explicitly *not* the refill
                clock; see the module docstring for why those are opposite decisions.
            now_ms_override: Test-only seam. See :meth:`_resolve_override`.

        Returns:
            A fully populated :class:`~src.models.LimitDecision` — every header, every body field
            and every analytics dimension, from one round trip.

        Never raises :class:`~src.redis_client.BackingStoreUnavailable`: a store that did not
        answer produces a degraded or refusing decision instead, per the module docstring. That is
        the C8 decision, made **once**, here, in the module that owns ``FAIL_MODE`` — the middleware
        adds no second handler.

        Raises:
            ValueError: ``cost < 1``, an unusable ``principal_user_id``, a pre-epoch ``now``, or a
                malformed reply. The last one is a bug in this service, not an outage — see the
                module docstring for why it is not laundered into a degradation.
            RuntimeError: ``now_ms_override`` was supplied with ``ALLOW_CLOCK_OVERRIDE`` off.
            redis.exceptions.ResponseError: a broken decision script. Propagates untouched, for the
                same reason as the ``ValueError``.
        """
        if cost < 1:
            # A zero- or negative-cost request is an unmetered request wearing a metered request's
            # clothes: it would pass every gate for free and, at cost 0, would leave the bucket
            # untouched no matter how many were sent. `src.config.parse_endpoint_costs` already
            # refuses a cost below 1 at startup; this is the same rule at the other end of the
            # pipe, where a middleware bug rather than a config typo would deliver one.
            raise ValueError(f"cost must be >= 1, got {cost}")

        override = self._resolve_override(now_ms_override)
        moment = now if now is not None else datetime.now(timezone.utc)
        daily_expire_at, monthly_expire_at = self._period_boundaries(moment)

        keys = [
            bucket_key(principal_user_id, endpoint_label),
            daily_quota_key(principal_user_id, moment),
            monthly_quota_key(principal_user_id, moment),
            user_key(principal_user_id),
        ]

        # `snapshot()` is a synchronous attribute read — no await, no Redis, no lock. See
        # `TierRegistry.snapshot`. Its `argv_tail` is spliced in as-is: the tuple was rendered once
        # when the snapshot was built and is shared, immutable, by every concurrent request, so
        # this is the only place the tier table costs anything at all and the cost is a memcpy.
        tail = self._tiers.snapshot().argv_tail

        # Bound rather than inlined into ARGV, because the degraded path needs the identical
        # string: the local account-wide gate is keyed on exactly the name the shared window is
        # built from, so a caller's local and shared account gates are the same gate by name.
        sw_prefix = sliding_window_prefix(principal_user_id)

        args: list[str] = [
            str(cost),                                            # 1  cost
            self._bucket_ttl_ms,                                  # 2  bucket_ttl_ms
            sw_prefix,                                            # 3  sw_prefix
            self._window_ms,                                      # 4  sw_window_ms
            self._sw_enabled,                                     # 5  sw_enabled
            str(daily_expire_at),                                 # 6  daily_expire_at
            str(monthly_expire_at),                               # 7  monthly_expire_at
            self._default_tier,                                   # 8  default_tier
            override,                                             # 9  now_ms_override
        ]
        args.extend(tail)                                         # 10 tier_count, 11+ the table

        self._ensure_registered()
        self.checks += 1

        # perf_counter, not time(): this is a duration, and a wall clock that steps mid-request
        # would report a negative or hour-long latency into the analytics C9 builds on it. Measured
        # across the failure paths too, because "how long did the degraded decision take?" is the
        # number that shows the circuit breaker is doing its job.
        started = time.perf_counter()
        try:
            raw = await self._gateway.run_script(RLQ_CHECK_AND_CONSUME_NAME, keys=keys, args=args)
        except BackingStoreOverloaded as exc:
            # FIRST, and the ordering is load-bearing rather than stylistic — `BackingStoreOverloaded`
            # IS a `BackingStoreUnavailable`, so the clause below would otherwise swallow it into
            # the fail-open path and hand a traffic burst the unmetered window C4's verification
            # measured. The same "spell the ordering out rather than leaving it to inheritance"
            # rule `RedisGateway.run` applies to `ReadOnlyError`.
            return self._overloaded_decision(
                principal_user_id,
                endpoint_label,
                cost,
                latency_ms=(time.perf_counter() - started) * MS_PER_SECOND,
                exc=exc,
            )
        except BackingStoreUnavailable as exc:
            # A CORRECTNESS failure (a broken script, a WRONGTYPE) is not caught here at all: the
            # gateway raises those as themselves, so they are not `BackingStoreUnavailable` and
            # this clause cannot see them. That is the whole point of C2's split, and it is why
            # there is no `except Exception` anywhere on this path.
            return self._degraded_decision(
                principal_user_id,
                endpoint_label,
                cost,
                bucket=keys[0],
                account=sw_prefix,
                latency_ms=(time.perf_counter() - started) * MS_PER_SECOND,
                exc=exc,
            )
        latency_ms = (time.perf_counter() - started) * MS_PER_SECOND

        # Redis answered, so this replica is enforcing for real again. Cleared here rather than on
        # the gateway's success path because `/health`'s `rate_limiter` field is about the
        # *limiter's* state: a successful `PING` from the health probe proves the store is back,
        # not that a request has been metered against it since.
        self.degraded_since = None

        return LimitDecision.from_lua(
            raw,
            user_id=principal_user_id,
            endpoint=endpoint_label,
            cost=cost,
            latency_ms=latency_ms,
        )

    # ------------------------------------------------------------------ #
    # Degradation
    # ------------------------------------------------------------------ #
    @property
    def degraded(self) -> bool:
        """Whether the fallback bucket is currently carrying this replica's traffic.

        Read by ``GET /health``, which reports it as ``rate_limiter: "degraded"`` **with a 200 and
        ``status: "healthy"``**. A liveness probe that goes red on a degraded-but-serving replica
        gets that replica restarted for working exactly as designed — and restarted on every
        replica at once, since they all share the one Redis that is down.
        """
        return self.degraded_since is not None

    def _fallback_tier(self) -> tuple[str, TierConfig]:
        """The tier the degraded path enforces: ``DEFAULT_TIER``, from the last good snapshot.

        See the module docstring for why it is the default tier and not the caller's. The snapshot
        is read through the same synchronous accessor the healthy path uses, and it serves its last
        good table straight through an outage — so *what a tier means* is still the operator's
        runtime value, even though *who is on which tier* is unknowable right now.

        ``snapshot().tiers`` always contains ``DEFAULT_TIER`` (``TierRegistry._parse_tiers`` starts
        from ``settings.tier_limits`` and only overlays Redis on top), and
        ``Settings._default_tier_must_exist`` guarantees the configured table has it — so the
        ``get`` default here is a belt on braces rather than a branch that can fire.
        """
        name = self._settings.default_tier
        table = self._tiers.snapshot().tiers
        return name, table.get(name, self._settings.tier_limits[name])

    def _mark_degraded(self) -> None:
        """Start (or continue) the current degraded run. Idempotent."""
        if self.degraded_since is None:
            self.degraded_since = self._clock()

    def _degraded_decision(
        self,
        user_id: str,
        endpoint_label: str,
        cost: int,
        *,
        bucket: str,
        account: str,
        latency_ms: float,
        exc: BackingStoreUnavailable,
    ) -> LimitDecision:
        """Decide this request without Redis: the local gates, or a 503, per ``FAIL_MODE``."""
        self._mark_degraded()
        tier_name, tier = self._fallback_tier()

        if not self._fail_open:
            # FAIL_MODE=closed. Nothing was evaluated, so nothing is reported: every rate and quota
            # number on this decision is zero, and the middleware emits `Retry-After` and
            # `X-RateLimit-Degraded` and no `X-RateLimit-Limit`/`Remaining` at all. A fabricated
            # allowance on a request that was never measured is the same lie the 401 path refuses
            # to tell, arriving through a 503.
            self.fail_closed_denials += 1
            logger.warning(
                "limiter refusing %s for %s: FAIL_MODE=closed and the backing store is "
                "unavailable (%s)",
                endpoint_label,
                user_id,
                exc,
            )
            return self._blank_decision(
                user_id,
                endpoint_label,
                cost,
                tier_name=tier_name,
                retry_after_sec=self._closed_retry_after_sec,
                degraded=True,
                latency_ms=latency_ms,
            )

        self.degraded_checks += 1

        # BOTH rate gates, evaluated as a set. The per-endpoint bucket alone is not a limit on the
        # *caller*: there are five route labels in the shipped table, so a bucket-only fallback
        # handed one principal five independent allowances and the degraded ceiling became
        # `labels x share` — a 5x overspend on the free tier at API_REPLICAS=2, which is the exact
        # multi-limiter failure this project exists to catch, arriving on the endpoint axis instead
        # of the replica axis. The account gate is keyed on the user alone, so it binds across
        # every endpoint and the multiplication cannot happen. See `src.fallback`.
        gates = [self._fallback.bucket_gate(bucket, tier)]
        if self._settings.sliding_window_enabled:
            # Gated on the same switch the shared window is, so the degraded path enforces neither
            # more nor less than the healthy one. With the account-wide gate switched off the
            # per-endpoint bucket is the only rate gate in BOTH modes, and the reporting below
            # falls back to it — so `X-RateLimit-Limit` still names the ceiling actually in force
            # rather than one that is merely configured.
            gates.append(self._fallback.account_gate(account, tier))

        local = self._fallback.consume(*gates, cost=cost)
        bucket_verdict = local.verdicts[0]
        # The gate whose numbers `X-RateLimit-Limit` / `X-RateLimit-Remaining` describe: the
        # account-wide one when it exists, because that is what binds a caller across endpoints.
        account_verdict = local.verdicts[1] if len(local.verdicts) > 1 else bucket_verdict

        return LimitDecision(
            allowed=local.allowed,
            # Both local gates are *rate* gates, so a refusal from either is a rate-limit refusal
            # and gets the spec's "Rate limit exceeded" body — not `BACKING_STORE`, which the
            # middleware reads as "we could not decide" and turns into a 503. We did decide; the
            # caller is genuinely over the (reduced) limit this replica is enforcing.
            #
            # WHICH reason names the gate that refused, mirroring the script: the bucket first,
            # the account-wide gate second, so a caller blocked by their overall rate is told
            # `sliding_window` rather than being pointed at an endpoint they could switch away
            # from. A tie resolves to the bucket, the same fixed order the script uses.
            reason=self._degraded_reason(local),
            tier=tier_name,
            user_id=user_id,
            endpoint=endpoint_label,
            cost=cost,
            bucket_limit=bucket_verdict.capacity,
            bucket_remaining=bucket_verdict.remaining,
            bucket_reset_sec=ceil_seconds(bucket_verdict.reset_ms),
            # `window_limit` is what `X-RateLimit-Limit` reports, and while degraded the honest
            # answer is the account-wide number actually being enforced — this replica's share of
            # the tier's rpm — rather than the tier's own figure, which nothing is currently able
            # to enforce. It was previously the per-endpoint bucket's capacity, which advertised an
            # account-wide ceiling that no gate checked: a header the code did not keep.
            window_limit=account_verdict.capacity,
            window_used=max(0, account_verdict.capacity - account_verdict.remaining),
            window_reset_sec=ceil_seconds(account_verdict.reset_ms),
            **_NO_QUOTA,
            retry_after_sec=ceil_seconds(local.retry_ms),
            degraded=True,
            server_now_ms=self._wall_clock_ms(),
            latency_ms=latency_ms,
        )

    @staticmethod
    def _degraded_reason(local: LocalDecision) -> DenyReason:
        """Name the local gate that refused, in the decision script's fixed order.

        The script evaluates bucket-then-window and keeps the reason belonging to the gate with the
        furthest retry, ties resolving to the earlier gate. The same rule here, expressed against
        the two local gates, so a degraded 429 and a healthy one label the same situation the same
        way — which is what lets a client's backoff logic, and C9's analytics, treat them as one
        series rather than two.
        """
        if local.allowed:
            return DenyReason.NONE
        refused = [
            (verdict.retry_ms, reason)
            for verdict, reason in zip(
                local.verdicts, (DenyReason.RATE_LIMIT, DenyReason.SLIDING_WINDOW)
            )
            if not verdict.allowed
        ]
        # `max` over (retry, reason) would order by the enum's string on a tie; the explicit key
        # keeps the tie-break positional, i.e. the earlier gate, exactly as the script does.
        # `allowed` is False only because some gate refused, so `refused` is never empty. `max`
        # returns the FIRST item holding the maximum, which is what keeps the tie-break positional
        # (the earlier gate) rather than alphabetical on the enum's value.
        return max(refused, key=lambda pair: pair[0])[1]

    def _overloaded_decision(
        self,
        user_id: str,
        endpoint_label: str,
        cost: int,
        *,
        latency_ms: float,
        exc: BackingStoreOverloaded,
    ) -> LimitDecision:
        """Refuse because this process ran out of connections. **Not** a degradation, and not
        ``FAIL_MODE``'s call.

        ``degraded`` stays ``False`` and :attr:`degraded_since` is untouched, which is the whole
        point of the separation: ``/health`` reports this on its own ``pool`` field while
        ``rate_limiter`` stays ``"active"``, because the limiter is active — the store is healthy
        and this replica is simply saturated. Marking it degraded would blame Redis for local
        backpressure, which is the misdiagnosis C4's verification called out by name.

        ``Retry-After: 1``. Pool contention clears in milliseconds; the honest advice is "come back
        immediately", and 1 is the smallest value RFC 9110 lets us say it with.
        """
        self.overload_denials += 1
        logger.warning(
            "limiter refusing %s for %s: the local connection pool is saturated (%s) — refusing "
            "rather than serving unmetered; the store itself is not implicated",
            endpoint_label,
            user_id,
            exc,
        )
        tier_name, _tier = self._fallback_tier()
        return self._blank_decision(
            user_id,
            endpoint_label,
            cost,
            tier_name=tier_name,
            retry_after_sec=1,
            degraded=False,
            latency_ms=latency_ms,
        )

    def _blank_decision(
        self,
        user_id: str,
        endpoint_label: str,
        cost: int,
        *,
        tier_name: str,
        retry_after_sec: int,
        degraded: bool,
        latency_ms: float,
    ) -> LimitDecision:
        """A refusal in which **no gate was evaluated**: every quantity is zero, and honestly so.

        Shared by the fail-closed and pool-exhausted paths because they differ in exactly two
        fields (``degraded`` and the retry interval) and in nothing else. Two hand-built copies
        would be two places for a number to be invented.

        :attr:`~src.models.DenyReason.BACKING_STORE` is what the middleware keys its **503** off —
        a 429 would tell the caller they are over a limit that was never measured.
        """
        return LimitDecision(
            allowed=False,
            reason=DenyReason.BACKING_STORE,
            tier=tier_name,
            user_id=user_id,
            endpoint=endpoint_label,
            cost=cost,
            bucket_limit=0,
            bucket_remaining=0,
            bucket_reset_sec=0,
            window_limit=0,
            window_used=0,
            window_reset_sec=0,
            **_NO_QUOTA,
            retry_after_sec=retry_after_sec,
            degraded=degraded,
            server_now_ms=self._wall_clock_ms(),
            latency_ms=latency_ms,
        )

    def stats(self) -> dict[str, Any]:
        """Counter snapshot for ``/health`` and C11's stats payload.

        ``degraded`` and ``degraded_for_sec`` are both published because they answer different
        questions: an operator wants the flag, and an incident review wants the duration.
        """
        return {
            "checks": self.checks,
            "degraded": self.degraded,
            "degraded_checks": self.degraded_checks,
            "degraded_for_sec": (
                None
                if self.degraded_since is None
                else max(0.0, self._clock() - self.degraded_since)
            ),
            "fail_closed_denials": self.fail_closed_denials,
            "overload_denials": self.overload_denials,
            "fail_mode": self._settings.fail_mode,
            "fallback": self._fallback.stats(),
        }

    @staticmethod
    def _wall_clock_ms() -> int:
        """``server_now_ms`` for a decision Redis never saw: **this replica's** wall clock.

        Every other decision in this project carries ``redis.call('TIME')``, which is the one clock
        every replica shares. There is no shared clock available on this path — that is what the
        outage *is* — so the field carries the local one, and C9's analytics will bucket degraded
        requests against a clock that may differ per replica. That is a real (small) consequence of
        degrading, noted here rather than discovered from a graph with two humps in it.
        """
        return int(time.time() * MS_PER_SECOND)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _resolve_override(self, now_ms_override: int | None) -> str:
        """Return ARGV[9]: the caller's override, or ``"0"`` meaning "use ``redis.call('TIME')``".

        .. rubric:: This seam must be inert by construction outside tests, not by convention

        A client-supplied clock is **the** vulnerability this design removed. Whoever controls
        ``now_ms`` controls the refill: pass a value a minute in the future and the bucket refills
        a full minute's worth of tokens on the spot, every time, for as long as you keep asking.
        The same value moves the sliding window's index, so the account-wide gate can be stepped
        into a window nobody else is counting in.

        So it is gated on :attr:`~src.config.Settings.allow_clock_override`, which defaults to
        ``False`` and is pinned to ``false`` in ``docker-compose.yml``. And when the setting is off
        the override is **refused, not ignored**: silently dropping it would let a test that
        believes it is driving a frozen clock pass against the real one, which is the failure mode
        where a green suite proves nothing. Loud is the only safe direction here — a raised
        exception is a bug report, an ignored parameter is a false negative.
        """
        if now_ms_override is None:
            return NO_CLOCK_OVERRIDE
        if not self._settings.allow_clock_override:
            raise RuntimeError(
                "now_ms_override was supplied but ALLOW_CLOCK_OVERRIDE is off. A caller-supplied "
                "clock lets a request refill its own token bucket, so the seam is inert unless a "
                "deployment opts in — and refusing is deliberate: ignoring the override would "
                "silently run the caller against the real clock instead."
            )
        return str(now_ms_override)

    def _period_boundaries(self, moment: datetime) -> tuple[int, int]:
        """Return ``(daily_expire_at, monthly_expire_at)`` — ARGV[6] and ARGV[7].

        ``QUOTA_DAILY_ENABLED=false`` / ``QUOTA_MONTHLY_ENABLED=false`` are encoded as
        :data:`~src.lua.UNENFORCED_PERIOD` (0) rather than as two more ARGV flags. A period with no
        boundary is not a period: the script neither reads nor increments the counter and reports
        ``limit = 0``, which :class:`~src.models.LimitDecision` already renders as
        :data:`~src.models.UNLIMITED` and already omits the ``X-Quota-*`` headers for. One
        sentinel, and it is legible in a ``MONITOR`` trace.

        .. rubric:: A sentinel a legitimate input can collide with has to be defended

        ``0`` is not only "switched off" — it is also a real instant. ``day_expire_at`` returns
        exactly 0 for any moment inside ``1969-12-31`` and ``month_expire_at`` returns 0 for any
        moment in ``1969-12``. Nothing in production produces such a value (the caller's default is
        :func:`datetime.now`), but :meth:`check` accepts a caller-supplied ``now``, and a
        pre-epoch one would reach the script as the sentinel and **silently switch quota
        enforcement off** for that request. A sentinel that a valid-looking input can impersonate
        is a bug waiting for one careless caller, so the collision is refused at the boundary with
        a message that names it.

        One guard covers both periods: the next-day instant is never later than the next-month
        instant, so any moment whose *monthly* boundary collides with the sentinel has a *daily*
        boundary that collides too. Checking the earlier of the two is therefore sufficient, and it
        runs before the enabled flags are consulted — it is a statement about the input, not about
        which gates happen to be on.
        """
        day_boundary = day_expire_at(moment)
        if day_boundary <= UNENFORCED_PERIOD:
            raise ValueError(
                f"now={moment!r} is at or before the unix epoch, so its period boundary is "
                f"{day_boundary} — which the decision script reads as the 'this period is not "
                "enforced' sentinel and would silently disable quota enforcement for this request"
            )

        daily = day_boundary if self._settings.quota_daily_enabled else UNENFORCED_PERIOD
        monthly = (
            month_expire_at(moment)
            if self._settings.quota_monthly_enabled
            else UNENFORCED_PERIOD
        )
        return daily, monthly
