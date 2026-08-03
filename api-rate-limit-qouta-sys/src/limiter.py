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

.. rubric:: This class does NOT handle a Redis outage

:class:`~src.redis_client.BackingStoreUnavailable` propagates out of :meth:`Limiter.check`
untouched, and that is C8's decision to make: fail open through a bounded local bucket, or fail
closed with a 503, plus the ``X-RateLimit-Degraded`` header that keeps the degradation from being
silent. Catching it here would make that choice in the wrong module, for every caller at once, and
would do it invisibly. **C8 should add exactly one handler, upstream of this call — not a second
one here.**

A Lua ``ResponseError`` propagates too, for the reason
:mod:`src.redis_client` spells out: a broken decision script is a bug in this service, and
classifying it as an outage would mean a one-character typo silently disabled rate limiting for
every request while ``/health`` reported the same thing it reports for an unplugged Redis.
"""

from __future__ import annotations

import contextlib
import time
from datetime import datetime, timezone

from src.config import Settings
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
from src.models import LimitDecision
from src.redis_client import RedisGateway
from src.tiers import TierRegistry

__all__ = ["Limiter"]

#: Milliseconds per second, named so the two unit conversions below read as conversions rather than
#: as magic multiplications next to a quantity that is already in milliseconds.
MS_PER_SECOND = 1000


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
    ) -> None:
        self._gateway = gateway
        self._tiers = tiers
        self._settings = settings

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

        Raises:
            ValueError: ``cost < 1``, an unusable ``principal_user_id``, a pre-epoch ``now``, or a
                malformed reply.
            RuntimeError: ``now_ms_override`` was supplied with ``ALLOW_CLOCK_OVERRIDE`` off.
            BackingStoreUnavailable: Redis did not answer. **Deliberately not caught here** — C8
                owns the fail-open/fail-closed decision.
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

        args: list[str] = [
            str(cost),                                            # 1  cost
            self._bucket_ttl_ms,                                  # 2  bucket_ttl_ms
            sliding_window_prefix(principal_user_id),             # 3  sw_prefix
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
        # would report a negative or hour-long latency into the analytics C9 builds on it.
        started = time.perf_counter()
        raw = await self._gateway.run_script(RLQ_CHECK_AND_CONSUME_NAME, keys=keys, args=args)
        latency_ms = (time.perf_counter() - started) * MS_PER_SECOND

        return LimitDecision.from_lua(
            raw,
            user_id=principal_user_id,
            endpoint=endpoint_label,
            cost=cost,
            latency_ms=latency_ms,
        )

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
