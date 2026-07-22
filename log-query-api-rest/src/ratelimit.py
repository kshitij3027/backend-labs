"""Per-principal token-bucket rate limiting — the algorithm, with no web framework attached.

.. rubric:: Why a token bucket

The README advertises two numbers per tier, not one: a **sustained rate** (10 / 100 / 1000
req/s) and a **burst** (20 / 200 / 2000). A token bucket is the smallest structure that
honours both at once — tokens accrue continuously at ``rate`` and the bucket holds at most
``burst`` of them, so the long-run average is bounded by ``rate`` while a client that has been
quiet may spend its accumulated credit all at once. That is exactly the contract a documented
"sustained + burst" API promises, and it is why a dashboard that paints twelve panels on load
is not punished for it.

The obvious alternative, a **fixed window** ("at most N in each wall-clock second"), is both
simpler and wrong at the seams: a client sending N requests at t=0.999 and N more at t=1.001
puts *2N* through in two milliseconds and every counter reads legal. It also produces a
sawtooth — everybody's window resets on the same second boundary, so traffic self-synchronises
into a spike. A **sliding-window log** fixes the boundary but stores a timestamp per request
per principal, which is unbounded memory driven by exactly the clients you least want to spend
memory on. A **leaky bucket** is the token bucket's dual and equally correct, but it shapes
output to a constant drip and has no natural "burst" number to advertise in a header — which
this API must do on every response.

.. rubric:: Framework-agnostic on purpose

Nothing here imports FastAPI, Starlette, or anything HTTP. This module is pure arithmetic over
an injectable clock, which is what makes it exhaustively unit-testable without a single
``sleep()`` — see ``tests/unit/test_ratelimit.py``. C8's wiring (the ``rate_limit`` dependency
in ``src/deps.py``, the header middleware in ``src/main.py``) is the only place that knows
about requests and responses, and it consumes :class:`Decision` objects produced here.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from src.auth import Tier
from src.config import TierLimit

logger = logging.getLogger(__name__)


# =============================================================================================
# Header names
#
# These three strings are the wire contract, and they also appear in ``src.main.EXPOSE_HEADERS``
# (the CORS allowlist, without which browser JS receives the headers and still cannot read
# them). They are duplicated rather than imported because the dependency arrow runs
# ``main -> deps -> ratelimit``: importing back the other way would close an import cycle, and
# this module must stay usable with no web framework installed at all. The seam that stops the
# two copies from drifting is a test —
# ``tests/unit/test_ratelimit.py::test_headers_match_expose_headers`` asserts every key
# :meth:`Decision.headers` emits is present in that allowlist.
# =============================================================================================

HEADER_LIMIT = "X-RateLimit-Limit"
HEADER_REMAINING = "X-RateLimit-Remaining"
HEADER_RESET = "X-RateLimit-Reset"

#: RFC 9110 §10.2.3's standard header. NOT emitted by :meth:`Decision.headers` — see the note
#: there — but named here so the 429 raiser in ``src/deps.py`` spells it exactly once.
HEADER_RETRY_AFTER = "Retry-After"

#: The triple attached to **every** response, in emit order.
RATE_LIMIT_HEADERS: tuple[str, str, str] = (HEADER_LIMIT, HEADER_REMAINING, HEADER_RESET)


# =============================================================================================
# Float tolerance
#
# Token accounting is floating point, so "the bucket holds exactly one token" is a statement
# that can be off by an ulp in either direction. Refilling for exactly ``1 / rate`` seconds can
# land on 0.9999999999999999 tokens, and a naive ``tokens >= 1.0`` would then deny a request the
# client is provably entitled to — a limiter that is wrong by one request per refill period, and
# a boundary test that fails on some machines and not others.
#
# One epsilon, applied in exactly two places (the take comparison and the `remaining` floor), so
# what the headers advertise and what :meth:`TokenBucket.take` will actually honour can never
# disagree. 1e-9 tokens is ~100 picoseconds of refill at the free tier's 10/s: far larger than
# accumulated float error, far smaller than any real token.
# =============================================================================================

_EPSILON = 1e-9


@dataclass(frozen=True, slots=True)
class Decision:
    """The outcome of one limiter consultation, plus everything the response must advertise.

    Frozen because a decision is a *record of what happened at one instant*: a handler that
    could rewrite ``allowed`` after the fact would be editing the audit trail, and the header
    middleware stashes this object on ``request.state`` where several layers can see it.

    ``limit`` is the **burst capacity**, not the sustained rate. That is the number a client can
    actually act on — "how many may I have in hand right now" — and it is what
    ``X-RateLimit-Remaining`` counts down from, so advertising the sustained rate there instead
    would make the pair incoherent.
    """

    allowed: bool
    #: Bucket capacity — what ``X-RateLimit-Limit`` advertises. Floored to an int because the
    #: header is a count; a fractional burst is a configuration curiosity, not a wire value.
    limit: int
    #: Whole tokens left *after* this request. Floored, never negative.
    remaining: int
    #: Seconds until the bucket is full again. ``0.0`` when it already is.
    reset_after: float
    #: Seconds until at least one token exists: ``ceil``, and **at least 1 when denied**. A
    #: ``Retry-After: 0`` is an invitation to retry immediately, which turns a rate-limited
    #: client into a hot loop against the exact endpoint that just said no.
    retry_after: int

    def headers(self) -> dict[str, str]:
        """The ``X-RateLimit-*`` triple as wire-ready strings.

        .. rubric:: Reset is delay-seconds, not a UNIX timestamp

        ``X-RateLimit-Reset`` carries *seconds from now*, deliberately. The ``X-RateLimit-*``
        family is famously inconsistent across APIs precisely because this field is a timestamp
        in some and a duration in others, and a timestamp forces the client to trust that its
        clock agrees with the server's — the exact skew ambiguity that has no answer over HTTP.
        A delay also matches the form ``Retry-After`` uses on the very same response (RFC 9110
        §10.2.3 allows either an HTTP-date or delay-seconds; we emit delay-seconds), so the two
        numbers on a ``429`` are read the same way rather than in two different units.

        The IETF is standardising this ground as a structured ``RateLimit`` field in
        ``draft-ietf-httpapi-ratelimit-headers``; when that lands, emitting it alongside is a
        purely additive change. The README specifies the ``X-`` triple, so the ``X-`` triple is
        what this returns.

        ``Retry-After`` is **not** in here even though :attr:`retry_after` is computed for every
        decision. It is a 429-only header: attaching it to a ``200`` tells a client to back off
        from a request that just succeeded. The dependency that raises the ``429`` sets it from
        :attr:`retry_after`; the middleware that decorates every response sets these three.

        Reset is rounded **up**, so a client that waits exactly as long as it was told is never
        early.
        """
        return {
            HEADER_LIMIT: str(self.limit),
            HEADER_REMAINING: str(self.remaining),
            HEADER_RESET: str(max(0, math.ceil(self.reset_after))),
        }


class TokenBucket:
    """One principal's bucket: capacity ``burst``, refilled continuously at ``rate`` per second.

    **Not internally synchronised, on purpose.** :class:`RateLimiter` owns every bucket it
    creates and serialises access under its own lock; a second lock per bucket would double the
    uncontended-acquire cost on the hot path to defend a class that is never shared. A bucket
    used standalone (as the unit tests do) is single-threaded by construction.

    ``__slots__`` because a limiter may hold thousands of these: five slots is ~100 bytes per
    principal against ~400 for a ``__dict__``-carrying instance, which is the difference between
    ``max_buckets=10_000`` costing ~1 MB and ~4 MB.
    """

    __slots__ = ("_burst", "_last_refill", "_rate", "_time_func", "_tokens")

    def __init__(
        self,
        rate: float,
        burst: float,
        *,
        time_func: Callable[[], float] = time.monotonic,
    ) -> None:
        """Build a bucket that starts **full**.

        Starting full rather than empty is the documented contract: a client that has never
        been seen has, by definition, not spent anything, so it gets its advertised burst
        immediately. Starting empty would mean a fresh client's first request is throttled by a
        bucket it never used — and would make process restarts visible to every caller.

        Args:
            rate: Sustained refill, tokens per second.
            burst: Capacity — the largest instantaneous burst, and what ``X-RateLimit-Limit``
                reports.
            time_func: The clock. Defaults to :func:`time.monotonic` and **must not** be
                :func:`time.time`: the wall clock can jump (an NTP correction, a container
                resuming from suspend, a manual set) and a jump forward would hand out free
                tokens proportional to the jump while a jump backward would freeze the bucket
                until real time caught up. ``monotonic`` cannot go backwards and has no
                relationship to civil time, which is precisely what an elapsed-time measurement
                needs. Injectable so the refill maths is testable without a single ``sleep()``.

        Raises:
            ValueError: If ``rate`` or ``burst`` is not positive. A zero rate is a bucket that
                never refills (one burst, then permanent 429s) and a zero burst rejects
                everything forever; both are configuration mistakes worth failing loudly on
                rather than serving.
        """
        if rate <= 0:
            raise ValueError(f"rate must be > 0, got {rate}")
        if burst <= 0:
            raise ValueError(f"burst must be > 0, got {burst}")

        self._rate = float(rate)
        self._burst = float(burst)
        self._time_func = time_func
        self._tokens = float(burst)
        self._last_refill = time_func()

    # -- introspection ---------------------------------------------------------------------

    @property
    def rate(self) -> float:
        """Sustained refill rate, tokens per second."""
        return self._rate

    @property
    def burst(self) -> float:
        """Capacity, in tokens."""
        return self._burst

    @property
    def tokens(self) -> float:
        """Raw (fractional, possibly stale) token count. Use :meth:`peek` for a fresh view."""
        return self._tokens

    @property
    def last_touched(self) -> float:
        """Clock reading at the last :meth:`take` / :meth:`peek` / :meth:`resize`.

        This doubles as the idle timestamp :meth:`RateLimiter.sweep` reads. It is the refill
        bookkeeping field rather than a second "last seen" variable on purpose: every operation
        that touches a bucket refills it first, so the two would always hold the same value and
        a separate field would only add a way for them to disagree.
        """
        return self._last_refill

    # -- core ------------------------------------------------------------------------------

    def _refill(self) -> None:
        """Credit the tokens earned since the last touch. Called by every public operation.

        Continuous refill: ``tokens += elapsed * rate``, capped at ``burst``. Not the
        "count whole elapsed intervals" variant, which quantises credit to ``1 / rate`` steps
        and makes a client's effective rate depend on how its request timing aligns with those
        steps. Capping at ``burst`` is what makes an idle client's credit stop accruing — an
        uncapped bucket would let a client silent for an hour spend 36,000 requests at once,
        which is a burst limit in name only.

        ``max(0.0, ...)`` on the elapsed time is belt-and-braces: :func:`time.monotonic` cannot
        run backwards, but an injected test clock or a future clock source could, and a negative
        elapsed would *remove* tokens the client had already earned.
        """
        now = self._time_func()
        elapsed = max(0.0, now - self._last_refill)
        self._last_refill = now
        if elapsed:
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate)

    def take(self, n: float = 1.0) -> Decision:
        """Try to spend ``n`` tokens. Returns the :class:`Decision`, consuming only on success.

        **A denied request must not consume anything.** Debiting on failure ("penalty
        accounting") means a client hammering the endpoint holds its own bucket at zero forever
        and can never recover no matter how long it waits — the limiter stops being a rate limit
        and becomes a ban. It also makes ``Retry-After`` a lie, because the deficit keeps
        growing under the client's feet.

        A request for more than ``burst`` can never be satisfied — the bucket will not hold that
        many tokens — so it is denied with ``retry_after`` describing the wait until the bucket
        is *full*, which is the most honest number available. Callers in this project always
        spend exactly one token.

        Raises:
            ValueError: If ``n`` is not positive. A zero- or negative-cost request is not a
                cheap request, it is a bug; :meth:`peek` is the way to observe without spending.
        """
        if n <= 0:
            raise ValueError(f"n must be > 0, got {n} (use peek() to observe without spending)")

        self._refill()
        allowed = self._tokens + _EPSILON >= n
        if allowed:
            # `max(0.0, ...)` keeps a float shortfall inside the epsilon window from parking a
            # tiny NEGATIVE balance in the bucket, which would silently steal the next refill.
            self._tokens = max(0.0, self._tokens - n)
        return self._decide(allowed=allowed, n=n)

    def peek(self, n: float = 1.0) -> Decision:
        """Report the bucket's state without spending anything.

        This still *refills* — it must, or it would report a token count that went stale the
        moment the last request finished — so it counts as a touch for idle-sweep purposes. What
        it never does is decrement.
        """
        self._refill()
        return self._decide(allowed=self._tokens + _EPSILON >= n, n=n)

    def resize(self, rate: float, burst: float) -> None:
        """Re-sized in place when a principal's tier changes. See :meth:`RateLimiter.acquire`.

        Order matters: refill **first**, at the old rate, so time already elapsed is credited
        under the contract that was in force while it passed; then adopt the new numbers; then
        clamp the balance to the new capacity. The clamp is the security-relevant half — without
        it, a principal downgraded from ``enterprise`` to ``free`` would keep spending an
        enterprise-sized balance until it drained.
        """
        if rate <= 0:
            raise ValueError(f"rate must be > 0, got {rate}")
        if burst <= 0:
            raise ValueError(f"burst must be > 0, got {burst}")

        self._refill()
        self._rate = float(rate)
        self._burst = float(burst)
        self._tokens = min(self._tokens, self._burst)

    def _decide(self, *, allowed: bool, n: float) -> Decision:
        """Snapshot the (already refilled, already debited) bucket as a :class:`Decision`."""
        tokens = self._tokens
        # Time until the bucket is full again. Zero when it is.
        reset_after = max(0.0, (self._burst - tokens) / self._rate)

        if allowed:
            retry_after = 0
        else:
            # Deficit for THIS request, but never more than a full bucket's worth: asking for
            # more than `burst` is unsatisfiable, and quoting the client a wait that will not
            # actually help is worse than quoting the time to full.
            deficit = min(max(0.0, n - tokens), self._burst - tokens)
            # `ceil` so the advertised wait is never short, and a floor of 1 so a sub-second
            # deficit does not produce `Retry-After: 0`.
            retry_after = max(1, math.ceil(deficit / self._rate))

        return Decision(
            allowed=allowed,
            limit=int(self._burst),
            # Same epsilon as `take`, for the same reason: `remaining` must describe what the
            # bucket will actually honour. Advertising 4 while a fifth token is spendable makes
            # a well-behaved client throttle itself early; advertising 5 while only 4 are
            # spendable makes it collect a 429 it was told it would not get.
            remaining=max(0, math.floor(tokens + _EPSILON)),
            reset_after=reset_after,
            retry_after=retry_after,
        )


# =============================================================================================
# The limiter
# =============================================================================================

#: Buckets are swept for idleness at most this often, expressed as a fraction of ``idle_ttl``.
#: A sweep is O(number of buckets); doing it per request would make every request pay for every
#: *other* principal the process has ever seen, which is the shape of an outage under exactly
#: the traffic a rate limiter exists for. At one fifth of the TTL a bucket lives at most
#: ``idle_ttl * 1.2`` past its last use, which is slack in the direction that costs memory
#: rather than correctness.
_SWEEP_INTERVAL_FRACTION = 0.2


class RateLimiter:
    """Per-principal token buckets sized by the principal's tier.

    Keyed by **subject alone** — one bucket per principal, never one per (principal, tier)
    pair. The tier is re-read on every :meth:`acquire` and the bucket is re-sized in place when
    it changes; see that method for what a mid-flight tier change does and why.

    .. rubric:: Locking

    A :class:`threading.Lock` guards the bucket map *and* every mutation of a bucket inside it.
    Under the deployment this project actually ships — one uvicorn worker, ``async def``
    handlers, one event loop — that lock is defence in depth rather than the correctness
    argument: coroutines interleave only at ``await`` points and there is no ``await`` between
    the read and the write here, so the whole operation is already atomic. It becomes
    load-bearing the moment any of that changes, and all three changes are one line away:
    ``uvicorn --workers N`` inside a single container, a ``def`` (not ``async def``) handler,
    which Starlette dispatches to a real threadpool, or a background sweeper thread. The lock
    is uncontended in the common case (~50 ns) and the alternative is a data race that shows up
    as buckets losing tokens under load, which is unreproducible by construction.
    """

    __slots__ = (
        "_buckets",
        "_enabled",
        "_fallback",
        "_idle_ttl",
        "_last_overflow_log",
        "_last_sweep",
        "_lock",
        "_max_buckets",
        "_sweep_interval",
        "_tier_limits",
        "_time_func",
        "_warned_tiers",
    )

    def __init__(
        self,
        tier_limits: Mapping[str, TierLimit],
        *,
        enabled: bool = True,
        time_func: Callable[[], float] = time.monotonic,
        idle_ttl: float = 300.0,
        max_buckets: int = 10_000,
    ) -> None:
        """Build a limiter over a tier table (normally ``Settings.tier_limits``).

        Args:
            tier_limits: ``tier name -> TierLimit(rate, burst)``. Copied, not aliased.
            enabled: The README's *operability switch*. ``False`` short-circuits the check;
                see :meth:`acquire` for what it does not change.
            time_func: Shared clock, handed to every bucket so the limiter and its buckets
                cannot disagree about what time it is. :func:`time.monotonic` by default — see
                :meth:`TokenBucket.__init__` for why never :func:`time.time`.
            idle_ttl: Seconds of inactivity after which a bucket is dropped by :meth:`sweep`.
            max_buckets: Hard ceiling on retained buckets; overflow evicts least-recently-used.

        Raises:
            ValueError: On an empty tier table (there would be no fallback to fail closed to),
                or a non-positive ``idle_ttl`` / ``max_buckets``.
        """
        if not tier_limits:
            raise ValueError("tier_limits must define at least one tier")
        if idle_ttl <= 0:
            raise ValueError(f"idle_ttl must be > 0, got {idle_ttl}")
        if max_buckets < 1:
            raise ValueError(f"max_buckets must be >= 1, got {max_buckets}")

        #: Snapshot, so a caller mutating the mapping it passed in cannot re-size live buckets.
        self._tier_limits: dict[str, TierLimit] = dict(tier_limits)
        self._enabled = enabled
        self._time_func = time_func
        self._idle_ttl = float(idle_ttl)
        self._max_buckets = max_buckets

        # The tier an unrecognised tier name falls back to. Resolved once at construction
        # because it cannot change afterwards, and because resolving it per request would put a
        # `min()` over the tier table on the hot path of the failure branch.
        self._fallback = _most_restrictive(self._tier_limits)

        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()
        self._last_sweep = time_func()
        self._sweep_interval = max(1.0, self._idle_ttl * _SWEEP_INTERVAL_FRACTION)
        #: Throttle for the overflow warning below. ``-inf`` so the first one is never delayed.
        self._last_overflow_log = float("-inf")
        #: Tier names already logged as unknown. An unconfigured tier would otherwise log once
        #: per request, i.e. a log line per request under exactly the load that made it happen.
        self._warned_tiers: set[str] = set()

        # A bucket may only be dropped once it is *provably indistinguishable from a fresh one*,
        # which is true after `burst / rate` seconds of idleness (the time to refill from empty
        # to full). At the README's tiers that is 2 s for all three (20/10, 200/100, 2000/1000)
        # against a 300 s default TTL, so sweeping is information-free by a factor of 150. A
        # misconfiguration that inverts this would make the sweep *grant* tokens, so it is
        # checked rather than assumed.
        slowest_refill = max(limit.burst / limit.rate for limit in self._tier_limits.values())
        if slowest_refill > self._idle_ttl:
            logger.warning(
                "idle_ttl=%.1fs is shorter than the %.1fs a bucket needs to refill; sweeping "
                "may hand a partially-drained principal a full bucket",
                self._idle_ttl,
                slowest_refill,
            )

    # -- tier resolution -------------------------------------------------------------------

    def _limit_for(self, tier: Tier | str) -> TierLimit:
        """Resolve a tier name to its sizing, **failing closed** on anything unrecognised.

        An unknown tier is not an error and not unlimited: it is the most restrictive tier in
        the table. Raising would turn a config typo into a 500 on every authenticated request —
        the limiter taking the whole API down is a worse outcome than the thing it prevents.
        Granting unlimited access is worse still, and is the default a plain ``dict[...]`` lookup
        with a ``None`` fallback quietly produces. For a *limiter*, the only safe direction to
        fail is closed, so an unrecognised principal is throttled like the cheapest paying one.

        ``str(tier)`` normalises both spellings the callers use: :class:`~src.auth.Tier` is a
        ``StrEnum``, so its members stringify to their wire value, and a raw claim string passes
        through untouched.
        """
        name = str(tier)
        limit = self._tier_limits.get(name)
        if limit is not None:
            return limit

        if name not in self._warned_tiers:
            self._warned_tiers.add(name)
            logger.warning(
                "unknown tier %r has no configured limit; falling back to the most restrictive "
                "tier (rate=%.1f/s, burst=%.0f)",
                name,
                self._fallback.rate,
                self._fallback.burst,
            )
        return self._fallback

    # -- the hot path ----------------------------------------------------------------------

    def acquire(self, subject: str, tier: Tier | str, n: float = 1.0) -> Decision:
        """Spend ``n`` tokens from ``subject``'s bucket. The one call the dependency makes.

        .. rubric:: What a tier change does

        Buckets are keyed by subject only, so a principal whose tier changes between two tokens
        keeps the *same* bucket and it is re-sized in place (:meth:`TokenBucket.resize`):

        * **Downgrade** (enterprise -> free) takes effect immediately — the balance is clamped
          to the new, smaller burst. A principal cannot keep spending a 2000-token balance after
          losing the tier that earned it.
        * **Upgrade** (free -> pro) raises the ceiling and the refill rate immediately, but does
          not gift the difference: a client that just drained its free bucket sees ``limit``
          jump to 200 with ``remaining`` still 0, and refills to the new ceiling at the new rate
          (two seconds at pro). It gets everything it now pays for, just not retroactively.

        The alternative — keying by ``(subject, tier)``, so a tier change starts a fresh full
        bucket — was rejected for the case that actually matters. Access tokens stay valid until
        they expire, so during the ``ACCESS_TOKEN_TTL_MIN`` window after a tier change a
        principal holds valid tokens for *both* tiers; with a compound key those are two
        independent buckets and the principal's real ceiling is their sum. One bucket per
        subject bounds a subject's spend by the highest tier in play instead of by the total,
        which is the property "a token bucket per principal" is supposed to mean.

        This is also the reason a downgrade cannot be dodged by alternating tokens: whichever
        one is presented, it addresses the same bucket, and the last tier seen sizes it.
        """
        limit = self._limit_for(tier)

        # The operability switch. Note what it does NOT do: it still returns a fully-formed
        # Decision carrying the tier's real ceiling, so the X-RateLimit-* headers stay present
        # and truthful and a client pacing itself against them does not have to cope with the
        # fields vanishing when an operator flips RATE_LIMIT_ENABLED. Turning the limiter off
        # changes the *enforcement*, not the response shape. It also stops allocating buckets,
        # so the switch is a real relief valve under memory pressure and not just a branch.
        if not self._enabled:
            return _unlimited(limit)

        with self._lock:
            bucket = self._bucket_for(subject, limit)
            decision = bucket.take(n)
            self._maybe_sweep_locked()
            return decision

    def peek(self, subject: str, tier: Tier | str) -> Decision:
        """Report ``subject``'s current allowance without spending a token.

        A subject with no bucket yet is reported from a **synthetic full** decision rather than
        by materialising one. An absent bucket and a full bucket describe the same state, so
        this is exact — and it keeps an observation call from being a way to grow the map,
        which matters because the middleware may peek on paths (401, 403) that never acquire.
        """
        limit = self._limit_for(tier)
        if not self._enabled:
            return _unlimited(limit)

        with self._lock:
            bucket = self._buckets.get(subject)
            if bucket is None:
                return _unlimited(limit)
            if bucket.rate != limit.rate or bucket.burst != limit.burst:
                bucket.resize(limit.rate, limit.burst)
            return bucket.peek()

    def _bucket_for(self, subject: str, limit: TierLimit) -> TokenBucket:
        """Fetch-or-create ``subject``'s bucket, re-sizing it if the tier changed. Lock held."""
        bucket = self._buckets.get(subject)
        if bucket is None:
            bucket = TokenBucket(limit.rate, limit.burst, time_func=self._time_func)
            self._buckets[subject] = bucket
        elif bucket.rate != limit.rate or bucket.burst != limit.burst:
            bucket.resize(limit.rate, limit.burst)
        return bucket

    # -- memory bound ----------------------------------------------------------------------

    def sweep(self) -> int:
        """Drop buckets untouched for ``idle_ttl``. Returns how many were dropped.

        Public so an operator or a test can force one; :meth:`acquire` also calls it
        opportunistically. See :meth:`_maybe_sweep_locked` for the cost argument and
        :meth:`__init__` for why dropping an idle bucket loses no information.
        """
        with self._lock:
            return self._sweep_locked(self._time_func())

    def _sweep_locked(self, now: float) -> int:
        """O(number of buckets) idle scan. Lock held."""
        cutoff = now - self._idle_ttl
        stale = [
            subject
            for subject, bucket in self._buckets.items()
            if bucket.last_touched <= cutoff
        ]
        for subject in stale:
            del self._buckets[subject]
        if stale:
            logger.debug("swept %d idle rate-limit buckets", len(stale))
        return len(stale)

    def _maybe_sweep_locked(self) -> None:
        """Opportunistic maintenance from the hot path. Lock held.

        Two triggers, and neither of them is "every request":

        * **Time.** At most once per :data:`_SWEEP_INTERVAL_FRACTION` of ``idle_ttl`` (60 s at
          the defaults). The scan is O(buckets); amortised over a minute of traffic that is
          nothing, whereas running it per request would make request cost scale with the number
          of *other* principals the process has ever seen.
        * **Pressure.** Whenever the map exceeds ``max_buckets``, regardless of the clock, so
          the ceiling is a real bound and not a bound-on-average.

        Without this a limiter keyed by principal is a slow memory leak: every distinct subject
        that ever authenticated keeps a bucket forever, and the leak is invisible in a test
        suite with four demo users and obvious only in production six weeks in.
        """
        now = self._time_func()
        over_cap = len(self._buckets) > self._max_buckets
        if not over_cap and now - self._last_sweep < self._sweep_interval:
            return

        self._last_sweep = now
        self._sweep_locked(now)

        if len(self._buckets) <= self._max_buckets:
            return

        # Still over the cap after dropping the idle ones: every bucket is active and the
        # process is facing more concurrent principals than it was sized for. Evict the
        # least-recently-touched down to the ceiling — O(n log n), only on the overflow path.
        #
        # This is the one lossy operation in the module and it is a deliberate trade: an evicted
        # principal's next request rebuilds a FULL bucket, so eviction can grant up to one extra
        # burst. Bounded memory beats perfect fidelity, because the alternative failure mode is
        # the process dying. It is safe *here* because subjects come from JWTs this service
        # signed — an attacker cannot mint 10,000 distinct subjects to flush a victim's bucket
        # without 10,000 sets of credentials. An API with self-serve signup would need a second,
        # coarser limiter in front of authentication rather than a bigger number here.
        victims = sorted(self._buckets, key=lambda s: self._buckets[s].last_touched)
        evicted = len(self._buckets) - self._max_buckets
        for subject in victims[:evicted]:
            del self._buckets[subject]

        # Throttled to once per sweep interval. Once the map is parked at its ceiling EVERY
        # request evicts one bucket, so an unthrottled warning here is a log line per request
        # under precisely the load that produced the condition — the operator learns nothing
        # from the 400th copy that the first did not tell them, and the log volume becomes its
        # own incident.
        if now - self._last_overflow_log >= self._sweep_interval:
            self._last_overflow_log = now
            logger.warning(
                "rate-limit bucket map hit its %d ceiling; evicted %d least-recently-used "
                "bucket(s) (active principals exceed the configured maximum)",
                self._max_buckets,
                evicted,
            )

    # -- introspection ---------------------------------------------------------------------

    def bucket_count(self) -> int:
        """How many buckets are currently retained. Used by tests and the debug surface."""
        with self._lock:
            return len(self._buckets)

    def reset(self) -> None:
        """Drop every bucket. Every principal starts full again.

        A test seam and an operator escape hatch, not something a request path calls: it hands
        the entire population a fresh burst.
        """
        with self._lock:
            self._buckets.clear()
            self._last_sweep = self._time_func()

    @property
    def enabled(self) -> bool:
        """Whether enforcement is on. The ceiling is still reported either way."""
        return self._enabled


# =============================================================================================
# Helpers
# =============================================================================================


def _most_restrictive(tier_limits: Mapping[str, TierLimit]) -> TierLimit:
    """The tightest tier in the table — the fail-closed fallback for an unknown tier.

    Ordered by sustained ``rate`` first (that is what bounds a client over any interval longer
    than a burst), then ``burst``, then the tier name so the choice is deterministic when two
    tiers are configured identically.
    """
    return min(
        tier_limits.items(),
        key=lambda item: (item[1].rate, item[1].burst, item[0]),
    )[1]


def _unlimited(limit: TierLimit) -> Decision:
    """A full-bucket decision for the paths that do not consult a bucket.

    Used by the disabled limiter and by :meth:`RateLimiter.peek` on a subject with no bucket.
    Reports the tier's real ceiling with the bucket full, which is exactly true in both cases.
    """
    capacity = int(limit.burst)
    return Decision(
        allowed=True,
        limit=capacity,
        remaining=capacity,
        reset_after=0.0,
        retry_after=0,
    )
