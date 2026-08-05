"""The live tier table: seeded into Redis, served from an in-process snapshot, never on the hot path.

Core requirement §2 is "config updates without a service restart". This module is the half of that
which is *hard*: a value that every replica must agree on, that an operator can change at runtime,
and that is read on **every single request** by a check with a 5 ms budget.

.. rubric:: Why a TTL'd snapshot rather than the two obvious alternatives

:meth:`TierRegistry.snapshot` is **synchronous**. It never awaits, never touches Redis, and returns
a pre-built object. Past its TTL it returns the **stale** value immediately and schedules a single
background refresh, so no request ever waits on tier configuration.

*Rejected: a per-request ``GET config:version``.* One extra round trip on the hot path is precisely
the cost the cache exists to remove, and it does not even buy freshness — it buys "we noticed the
version changed" one request earlier, in exchange for doubling the store traffic of the entire
service.

*Rejected: pub/sub invalidation.* Redis pub/sub is fire-and-forget: a replica that is reconnecting,
paused, or slow at the moment of publication misses the message **permanently** and serves the old
table until someone restarts it. Which means you need a TTL backstop anyway — and once you have the
TTL backstop, the pub/sub is an optimisation that converts a deterministic 5-second bound into a
usually-faster-but-occasionally-never one. C10's admin API therefore invalidates *its own* replica
synchronously (see :meth:`TierRegistry.invalidate`) and everyone else converges within
``TIER_CACHE_TTL_SEC``, which is an assertion a test can make rather than a race it must tolerate.

.. rubric:: What is cached here, and what deliberately is not

This registry caches **what a tier means** (its four numbers). It does *not* cache **who is on
which tier** — that is read from ``user:{id}`` inside the decision script on every request. So the
5-second staleness window applies only to a re-pricing, never to a reassignment: moving a principal
between tiers takes effect on the very next request on every replica. That split is what makes the
spec's "limits applied from the tier at request time" true while still keeping the hot path free of
a config round trip.

.. rubric:: Redis is authoritative; the config defaults are legitimate but not authoritative

``settings.tier_limits`` is what a brand-new deployment *starts* with. Once seeded, ``config:tiers``
is the truth, because that is where an operator's runtime change lives. But if Redis is unreachable
at startup and there has never been a good snapshot, this registry serves the configured defaults
rather than refusing to start: a process that crash-loops because it cannot read its limits enforces
*nothing*, which is strictly worse than enforcing the shipped numbers while the store recovers.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from src.config import Settings
from src.keys import CONFIG_TIERS_KEY, CONFIG_VERSION_KEY
from src.lua import (
    MERGE_TIER_REPLY_ARITY,
    MERGE_TIER_STATUS_ABSENT,
    MERGE_TIER_STATUS_MERGED,
    MERGE_TIER_UNCHANGED,
    RLQ_MERGE_TIER,
    RLQ_MERGE_TIER_NAME,
)
from src.models import TierConfig
from src.redis_client import BackingStoreUnavailable, RedisGateway

logger = logging.getLogger(__name__)

#: Separator inside a ``config:tiers`` HASH value: ``rpm|burst|daily|monthly``.
#:
#: A pipe rather than a colon, because the ``TIER_LIMITS`` environment form is already
#: colon-delimited (``free:60:60:1000:25000``) and the two would be trivially confusable in a
#: ``redis-cli HGETALL`` dump — the field name is the tier, so the value must not look like it
#: carries one too. Rather than JSON, because this is four integers: JSON would cost a parse on
#: both sides and make a hand-written ``HSET`` (the documented operator escape hatch) something you
#: have to get quoting right.
TIER_FIELD_SEPARATOR = "|"

#: How many numbers a well-formed ``config:tiers`` value carries.
TIER_FIELD_COUNT = 4

#: How many flat ARGV slots one tier occupies in the decision script's tail:
#: ``name, rpm, burst, daily, monthly``. Named because the Lua side does
#: ``ARGV[11 + (i - 1) * 5 + n]`` arithmetic against exactly this number.
ARGV_SLOTS_PER_TIER = 5

#: Floor on the post-failure refresh backoff, in seconds. The interval itself is
#: ``max(this, TIER_CACHE_TTL_SEC)`` — see :attr:`TierRegistry._refresh_backoff_sec`. The floor
#: exists so a deployment that sets ``TIER_CACHE_TTL_SEC=0`` (meaning "never cache") does not turn
#: a Redis outage into an unthrottled retry loop.
REFRESH_BACKOFF_FLOOR_SEC = 1.0

#: Characters that disqualify a tier name read from ``config:tiers``: any whitespace (space, tab,
#: newline), the C0 control range and ``DEL``.
#:
#: A tier name is a lookup key on the hot path and an ARGV element the decision script indexes by
#: name, and neither of those should have to defend against ``"free\n"``. The name is also what an
#: admin API response and a ``MONITOR`` trace print, where a name that differs from the intended
#: one by an invisible byte is a debugging session nobody wins.
#:
#: Such a row is **skipped and reported**, never normalised. Silently rewriting ``"pre mium"`` to
#: ``"premium"`` would guess at which tier an operator meant and could quietly re-price a
#: principal; skipping it leaves that tier on its configured default and puts the actual key in a
#: log line someone can act on.
_UNSAFE_TIER_NAME_RE = re.compile(r"[\s\x00-\x1f\x7f]")


# ---------------------------------------------------------------------------------------------
# Pure encode / decode
# ---------------------------------------------------------------------------------------------


def encode_tier(config: TierConfig) -> str:
    """Render one tier as its ``config:tiers`` value — ``60|60|1000|25000``."""
    return TIER_FIELD_SEPARATOR.join(
        str(number)
        for number in (
            config.rate_limit_per_min,
            config.burst,
            config.daily_quota,
            config.monthly_quota,
        )
    )


def decode_tier(name: str, raw: str) -> TierConfig:
    """Parse one ``config:tiers`` value, or raise :class:`ValueError` naming what was wrong.

    Every rule here is the same rule :func:`src.config.parse_tier_limits` applies to the
    environment form, and for the same reason: **a garbage row must never be able to make a tier
    unlimited.** The decision script reads a non-positive limit as "this gate is not enforcing
    anything", so a value of ``60|60|0|25000`` — a typo, a truncated write, a half-applied admin
    change — would silently hand that tier an infinite daily allowance. A limiter that fails open
    because someone fat-fingered a number is a limiter nobody can rely on.

    Raising rather than repairing is the point: the caller
    (:meth:`TierRegistry._parse_tiers`) logs at ERROR and falls back to the configured default for
    that tier, so one bad row costs that tier its runtime customisation and costs the registry
    nothing at all.
    """
    pieces = [piece.strip() for piece in raw.split(TIER_FIELD_SEPARATOR)]
    if len(pieces) != TIER_FIELD_COUNT:
        raise ValueError(
            f"tier {name!r} value {raw!r} has {len(pieces)} fields, expected "
            f"{TIER_FIELD_COUNT} ('rpm{TIER_FIELD_SEPARATOR}burst{TIER_FIELD_SEPARATOR}"
            f"daily{TIER_FIELD_SEPARATOR}monthly')"
        )
    try:
        numbers = [int(piece) for piece in pieces]
    except ValueError as exc:
        raise ValueError(
            f"tier {name!r} value {raw!r}: rpm, burst, daily and monthly must be integers"
        ) from exc
    if any(number <= 0 for number in numbers):
        raise ValueError(
            f"tier {name!r} value {raw!r}: every limit must be positive — a non-positive limit "
            "reads as 'unenforced' in the decision script"
        )
    rpm, burst, daily, monthly = numbers
    return TierConfig(
        name=name,
        rate_limit_per_min=rpm,
        burst=burst,
        daily_quota=daily,
        monthly_quota=monthly,
    )


def render_argv_tail(tiers: Mapping[str, TierConfig]) -> tuple[str, ...]:
    """Pre-render the decision script's flat tier ARGV tail.

    ``(tier_count, name1, rpm1, burst1, daily1, monthly1, name2, ...)`` — the shape C4's script
    reads as ``ARGV[10] = tier_count`` and ``ARGV[11..]`` = :data:`ARGV_SLOTS_PER_TIER` slots per
    tier.

    .. rubric:: Why it is rendered per *snapshot* and not per *request*

    Building this list is ~5 microseconds of string formatting. That is nothing — until you notice
    it would run on every metered request at the project's 1000 rps target, producing an identical
    tuple every time from data that changes at most once per TTL. Rendering it once when the
    snapshot is built removes the work entirely from the hottest path in the service, and the
    result is immutable so it can be shared by every concurrent request without copying.

    .. rubric:: Why flat ARGV and not ``cjson``

    Three reasons, in order of how much they would hurt:

    * **No dependency on the sandbox's ``cjson``.** It is present in stock Redis, but it is a
      property of the *server build*, and the decision script is the one piece of this system that
      must run identically on a laptop, in CI's ``redis:7-alpine`` and on whatever managed Redis a
      deployment points at. A flat list needs nothing but ``ARGV``.
    * **Zero parse time on Redis's single thread.** Every microsecond spent in ``cjson.decode``
      inside the script is a microsecond during which *no other client is served*. Indexing
      ``ARGV`` is free.
    * **A ``MONITOR`` trace is self-documenting.** Debugging a live limiter means reading the
      actual command, and ``... 3 enterprise 1000 1000 500000 12500000 free 60 ...`` is legible at
      a glance where an escaped JSON blob is not.

    Tiers are emitted in **sorted name order**. The Lua side reads a count-prefixed flat list, so
    the order carries no meaning — but determinism does: two replicas render byte-identical tails
    for identical config, which makes the ``EVALSHA`` argument list comparable across replicas in a
    trace and makes this function's output something a test can assert literally.
    """
    tail: list[str] = [str(len(tiers))]
    for name in sorted(tiers):
        config = tiers[name]
        tail.extend(
            (
                name,
                str(config.rate_limit_per_min),
                str(config.burst),
                str(config.daily_quota),
                str(config.monthly_quota),
            )
        )
    return tuple(tail)


def _as_text(value: object) -> str:
    """Decode a Redis reply element to ``str`` (the gateway uses ``decode_responses=False``)."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def is_safe_tier_name(name: str) -> bool:
    """Whether ``name`` is usable as a tier identifier. See :data:`_UNSAFE_TIER_NAME_RE`.

    Applied to names read from ``config:tiers`` only. Names from ``settings.tier_limits`` have
    already been through :func:`src.config.parse_tier_limits` at startup, where a malformed entry
    is a loud process-level failure rather than one row of a HASH — so filtering them here would
    add a second, quieter place for a tier to disappear.
    """
    return bool(name) and _UNSAFE_TIER_NAME_RE.search(name) is None


# ---------------------------------------------------------------------------------------------
# The snapshot
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Snapshot:
    """One immutable view of the tier table, plus everything derived from it.

    Frozen, and **replaced rather than mutated**. :meth:`TierRegistry.refresh` builds a complete new
    instance and then assigns it in a single statement, which is what makes
    :meth:`TierRegistry.snapshot` safe to call without a lock: a reader either sees the whole old
    table or the whole new one, never a half-applied update where ``argv_tail`` has been rebuilt
    but ``tiers`` has not. Mutating in place would make that window real, and the window is
    precisely the moment an operator's change is landing — i.e. the moment anyone is watching.

    ``fetched_monotonic`` is from :func:`time.monotonic`, not the wall clock, so an NTP step cannot
    make a snapshot look hours old (a refresh storm) or eternally fresh (a change that never
    propagates).

    ``tiers`` is a :class:`~types.MappingProxyType`, so the immutability the paragraph above argues
    for is **enforced rather than asserted**. ``frozen=True`` only stops the *field* being rebound;
    a plain ``dict`` behind it would still let any holder of a snapshot write
    ``snapshot().tiers["free"] = ...`` and mutate the table every concurrent request is reading —
    from a caller that never went near this module. The read-only view makes that a ``TypeError``
    at the line responsible instead of a limit that changed for reasons nobody can reconstruct.
    """

    tiers: Mapping[str, TierConfig]
    argv_tail: tuple[str, ...]
    version: int
    fetched_monotonic: float


@dataclass(frozen=True, slots=True)
class TierWrite:
    """The outcome of one :meth:`TierRegistry.merge_tier`, as reported by the script itself.

    ``previous`` is the row the merge was actually applied to, read inside Redis at the instant of
    the merge — **not** what this replica believed was stored. That distinction is the whole point
    of the server-side merge, and it is what makes C10's ``TierUpdated.previous`` an audit record
    rather than a guess: on a write that lands after another replica's, the guess is wrong exactly
    when someone would want to look it up.

    ``version`` is the ``config:version`` **this write produced**, not whatever the subsequent
    refresh happened to read. Under concurrent writes the two differ, and "the version my change
    created" is the number an audit trail wants; ``snapshot.version`` is the one describing what
    this replica now serves, and both are available for that reason.

    ``previous`` and ``config`` are ``None`` **exactly** when ``status`` is
    :data:`~src.lua.MERGE_TIER_STATUS_ABSENT` — the tier does not exist, the script returned before
    its ``HSET``, and nothing was written. Encoding "nothing happened" as a pair of ``None``s rather
    than as a separate flag is what lets the caller narrow both values with one check instead of
    asserting a relationship between a boolean and two optionals.
    """

    status: str
    previous: TierConfig | None
    config: TierConfig | None
    version: int
    snapshot: _Snapshot


def _build_snapshot(
    tiers: Mapping[str, TierConfig], *, version: int, fetched_monotonic: float
) -> _Snapshot:
    """Assemble a snapshot and its pre-rendered ARGV tail from a tier mapping.

    The mapping is copied and then wrapped: the copy detaches the snapshot from whatever the caller
    still holds a reference to, and the wrapper stops anyone downstream writing through it. Either
    one alone leaves a hole.
    """
    table = dict(tiers)
    return _Snapshot(
        tiers=MappingProxyType(table),
        argv_tail=render_argv_tail(table),
        version=version,
        fetched_monotonic=fetched_monotonic,
    )


# ---------------------------------------------------------------------------------------------
# The registry
# ---------------------------------------------------------------------------------------------


class TierRegistry:
    """Seeds the tier table into Redis and serves it to the hot path from an in-process snapshot.

    Constructed synchronously and performs **no I/O** in ``__init__`` — same contract as
    :class:`~src.redis_client.RedisGateway`, and for the same reason: ``Runtime.build`` must stay a
    plain function so the ``create_app(runtime=...)`` test seam never opens a socket.

    From construction until the first successful :meth:`refresh`, the registry serves
    ``settings.tier_limits``. That is a real, enforceable table — not a placeholder — so the
    service is metering correctly from its very first request even if Redis is not answering yet.
    """

    def __init__(
        self,
        settings: Settings,
        gateway: RedisGateway,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings
        self._gateway = gateway
        self._clock = clock

        # Single-flight machinery. `_refreshing` is set and read only from `snapshot()` and the
        # background task, both of which run on the event loop thread with no await point between
        # the check and the set — so the flag alone already collapses a hundred concurrent
        # `snapshot()` calls into one task. The lock additionally serialises a *directly* awaited
        # `refresh()` (C10's `POST /admin/config/reload`) against an in-flight background one, so
        # the two cannot interleave and race each other's snapshot assignment.
        self._lock = asyncio.Lock()
        self._refreshing = False
        self._refresh_task: asyncio.Task[None] | None = None
        self._closed = False

        # Post-failure backoff. Without it, a failed refresh leaves the snapshot stale forever, so
        # EVERY subsequent `snapshot()` schedules its own task and logs its own warning — a
        # thousand tasks and a thousand synchronous stdout writes per second at the project's
        # target load, during precisely the outage the fail-open path exists to ride out. The
        # breaker already keeps the actual Redis I/O at zero, so this is not a correctness bug; it
        # is the service getting slower and noisier exactly when it is already degraded.
        #
        # Keyed off the cache TTL because that is the rate at which a *healthy* registry refreshes:
        # there is no reason to retry a broken store more often than we would poll a working one.
        self._refresh_backoff_sec = max(
            REFRESH_BACKOFF_FLOOR_SEC, float(settings.tier_cache_ttl_sec)
        )
        self._refresh_backoff_until = 0.0

        # Monotonic counter bumped by `invalidate()`. `refresh()` samples it before issuing its
        # read and only clears the stale flag if it has not moved — see `refresh` for the race.
        self._invalidations = 0

        # Stale from birth: the seeded fallback below has never been reconciled with Redis, so the
        # first `snapshot()` must schedule a refresh rather than trusting it for a full TTL.
        self._stale = True
        self._snapshot = _build_snapshot(
            settings.tier_limits, version=0, fetched_monotonic=clock()
        )

        #: Observability counters, surfaced by C11. A registry that has silently been failing to
        #: refresh for an hour looks exactly like one that is up to date, unless it is counted.
        self.refreshes = 0
        self.refresh_failures = 0

    # ------------------------------------------------------------------ #
    # The hot path
    # ------------------------------------------------------------------ #
    def snapshot(self) -> _Snapshot:
        """Return the current tier snapshot. **Synchronous, no await, no Redis, no lock.**

        This is called on every metered request, so its entire cost is one attribute read, one
        ``time.monotonic()`` and a comparison.

        Past the TTL it returns the **stale snapshot immediately** and schedules a single
        background refresh. Blocking the request on the refresh would put Redis latency back on the
        hot path for a value that changes at most once per deploy — and would do it to *every*
        in-flight request at the same instant, since they all cross the TTL boundary together. A
        5-second-old tier limit is a non-event; a 250 ms stall on every request once every 5
        seconds is a latency graph with teeth in it.
        """
        current = self._snapshot
        if self._is_stale(current):
            self._schedule_refresh()
        return current

    def _is_stale(self, snapshot: _Snapshot) -> bool:
        """Whether ``snapshot`` has outlived ``TIER_CACHE_TTL_SEC`` or was explicitly invalidated.

        ``>=`` rather than ``>``: at exactly the TTL the snapshot has lived its full configured
        life. It also makes the boundary a value a test can hit exactly with an injected clock,
        instead of one that can only be approached.
        """
        if self._stale:
            return True
        return (self._clock() - snapshot.fetched_monotonic) >= self._settings.tier_cache_ttl_sec

    def invalidate(self) -> None:
        """Mark the snapshot stale so the next :meth:`snapshot` triggers a refresh. Synchronous.

        C10's admin API calls this on the replica that served a ``PUT``, so that replica does not
        spend up to ``TIER_CACHE_TTL_SEC`` serving the value it just overwrote — an operator
        watching the response of their own change and seeing the old number is how people conclude
        hot reload does not work. Every *other* replica converges on the TTL, which is a bound a
        test can assert.

        The counter is what makes this survive a race with a refresh that is already in flight. See
        :meth:`refresh`: a bare ``_stale = True`` here would be **erased** by an in-flight refresh
        that started before the write and finishes after it, leaving the replica serving the
        pre-edit value with nothing scheduled to correct it — the exact failure this method exists
        to prevent, reachable on any admin write that lands during a background refresh.
        """
        self._invalidations += 1
        self._stale = True

    def _schedule_refresh(self) -> None:
        """Start at most one background refresh. Never raises, never blocks.

        Three ways this deliberately does nothing:

        * a refresh is already in flight — that is the single-flight property; a hundred concurrent
          requests crossing the TTL boundary together must produce **one** Redis read, not a
          hundred;
        * the registry has been stopped — a task started during shutdown would either be cancelled
          a moment later or log a spurious outage against a gateway that is already closing;
        * the previous attempt failed and its backoff window has not elapsed. A failed refresh
          leaves the snapshot stale, so without this the *permanent* staleness of an outage would
          schedule a task and write a log line on every single request. Note this gates
          **scheduling** only: a directly awaited :meth:`refresh` (C10's
          ``POST /admin/config/reload``) is an explicit operator action and always attempts;
        * there is no running event loop. That happens when ``snapshot()`` is called from
          synchronous code (a unit test, a future CLI); returning the cached table is the correct
          answer there and is exactly what this method's caller already did.
        """
        if self._refreshing or self._closed:
            return
        if self._clock() < self._refresh_backoff_until:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._refreshing = True
        self._refresh_task = loop.create_task(self._background_refresh())

    async def _background_refresh(self) -> None:
        """Run one refresh off the request path and clear the in-flight flag whatever happens.

        The broad ``except`` is correct here and nowhere else in this module: this coroutine runs
        as a bare :class:`asyncio.Task` with nobody awaiting it, so an escaping exception is
        reported by the event loop's default handler at some arbitrary later garbage-collection
        time, detached from the operation that caused it. Worse, the ``finally`` would still clear
        the flag, so the registry would look healthy while silently never refreshing again. Logged
        at ERROR with a traceback, the failure is attributable; the previous snapshot keeps serving,
        which is the whole point of caching it.
        """
        try:
            await self.refresh()
        except Exception:  # noqa: BLE001 - see the docstring: a detached task must not vanish
            self.refresh_failures += 1
            logger.error(
                "background tier refresh failed; continuing to serve the last good snapshot "
                "(version=%d)",
                self._snapshot.version,
                exc_info=True,
            )
        finally:
            self._refreshing = False

    # ------------------------------------------------------------------ #
    # Redis
    # ------------------------------------------------------------------ #
    async def seed(self, *, reseed: bool = False) -> None:
        """Write the configured tier table into ``config:tiers`` with **``HSETNX``, never ``HSET``**.

        .. rubric:: This one-word difference is the whole credibility of hot reload

        Every replica runs this on startup. With ``HSET``, an operator who lowers premium's rpm at
        14:00 through the admin API gets it silently reverted the next time *any* replica restarts
        — a deploy, an OOM kill, a node drain, a scale-up. The change does not error, it does not
        log, and it does not revert immediately: it reverts at some unrelated later moment, which
        is the most expensive shape a bug can have. People then stop trusting runtime configuration
        and go back to redeploying for a number change, which is the exact workflow this feature
        exists to remove.

        ``HSETNX`` makes startup *seeding* and runtime *editing* compose: the first replica to boot
        against an empty store creates the table, every later boot is a no-op, and an operator's
        edit is never a thing anybody's restart can undo.

        Args:
            reseed: Use ``HSET`` and overwrite. **Nothing in the service calls this** — no startup
                path, no admin route — and that is the point: it is the deliberate escape hatch for
                the two situations where overwriting is what you actually want, namely an operator
                resetting a tier that has been edited into a state they no longer want (``python -c
                'await registry.seed(reseed=True)'`` beats reconstructing four numbers by hand) and
                a harness that needs a *known* table rather than whatever a previous run left in a
                shared store. It is a parameter rather than the default precisely so that choosing
                it is a decision someone made, and it has no caller so that nobody can make it by
                accident.

        ``config:version`` is ``INCR``-ed whenever a field was actually written, so a replica whose
        snapshot predates the change can see that it did. When nothing was written, ``SETNX`` still
        guarantees the key exists — a missing version key would otherwise read as "0" forever and
        C10's propagation check would have nothing to watch.
        """
        changed = False
        for name in sorted(self._settings.tier_limits):
            value = encode_tier(self._settings.tier_limits[name])
            if reseed:
                await self._gateway.run(
                    lambda field=name, payload=value: self._gateway.client.hset(
                        CONFIG_TIERS_KEY, field, payload
                    ),
                    op="tiers:seed",
                )
                changed = True
            else:
                created = await self._gateway.run(
                    lambda field=name, payload=value: self._gateway.client.hsetnx(
                        CONFIG_TIERS_KEY, field, payload
                    ),
                    op="tiers:seed",
                )
                changed = changed or bool(created)

        if changed:
            # INCR creates the key at 1 when it is absent and bumps it otherwise, so one command
            # covers both "first boot" and "an operator added a tier to TIER_LIMITS and restarted".
            await self._gateway.run(
                lambda: self._gateway.client.incr(CONFIG_VERSION_KEY), op="tiers:version"
            )
        else:
            await self._gateway.run(
                lambda: self._gateway.client.setnx(CONFIG_VERSION_KEY, 1), op="tiers:version"
            )

    def _ensure_merge_script(self) -> None:
        """Attach :data:`~src.lua.RLQ_MERGE_TIER`'s handle to the gateway if it is not attached.

        The same idempotent, called-per-use pattern as
        :meth:`src.analytics.AnalyticsCollector._ensure_registered`, and for the same reason: the
        handle can legitimately disappear underneath us, because
        :meth:`~src.redis_client.RedisGateway.aclose` drops every registered script precisely so a
        reconnect cannot dispatch onto a dead client.

        Deliberately **not** registered in ``__init__``: :meth:`register` reads
        :attr:`~src.redis_client.RedisGateway.client`, which raises until ``connect()`` has been
        awaited, and ``Runtime.build`` constructs this registry before the gateway is connected —
        by contract, so ``create_app(runtime=...)`` never opens a socket. Registering on first use
        keeps that contract without a separate wiring step for a later reconnect to invalidate.
        """
        try:
            self._gateway.script(RLQ_MERGE_TIER_NAME)
        except KeyError:
            self._gateway.register(RLQ_MERGE_TIER_NAME, RLQ_MERGE_TIER)

    async def merge_tier(
        self,
        name: str,
        *,
        rate_limit_per_min: int | None = None,
        burst: int | None = None,
        daily_quota: int | None = None,
        monthly_quota: int | None = None,
    ) -> TierWrite:
        """Apply a **partial** tier update, merging inside Redis, then re-read. **C10's write.**

        Only the fields that are not ``None`` are changed; the rest keep whatever is *committed* at
        the instant of the merge.

        .. rubric:: The merge base is the committed row, and it CANNOT be this replica's snapshot

        This is the whole reason the write is a Lua script (:data:`~src.lua.RLQ_MERGE_TIER`) rather
        than ``apply_to`` plus an ``HSET``. A partial update rewrites all four fields, so it needs a
        base for the three it is not changing — and :meth:`snapshot` is by design up to
        ``TIER_CACHE_TTL_SEC`` old on any replica that did not serve the previous write. Merging
        from it *deterministically reverts* whatever another replica committed inside that window:

        .. code-block:: text

           A: PUT premium {"daily_quota": 99999}      -> 300|300|99999|1250000
           B: PUT premium {"rate_limit_per_min": 77}  ->  77|300|50000|1250000   <- A's change gone

        With two replicas behind the C12 load balancer that is not a race, it is the default path
        for two operator ``PUT``s seconds apart. The same shape loses updates between concurrent
        ``PUT``s on one replica, and returns ``200`` for each of them. Merging server-side removes
        the window entirely, because Redis runs the script body without interleaving another client.

        Parameter names match :class:`~src.models.TierUpdate`'s fields but the model is deliberately
        **not** imported here: this registry has no business knowing the shape of an HTTP body, and
        four keyword arguments express the contract without the coupling. The Python-side statement
        of the same merge rule is :meth:`~src.models.TierUpdate.apply_to`, and the integration suite
        pins the two to agree rather than letting one drift into being a second implementation.

        .. rubric:: ``HSET``, and why that is not a contradiction of :meth:`seed`'s ``HSETNX``

        :meth:`seed` uses ``HSETNX`` because *startup* must never overwrite an operator's runtime
        change. This is that runtime change. The two are the same rule read from both ends: a
        replica booting has no opinion about limits and must defer to what is stored, while an
        operator issuing a ``PUT`` is stating the new opinion. Nothing about ``seed``'s semantics is
        weakened — it still cannot undo what this writes, on any later boot.

        .. rubric:: Invalidate, then refresh — in that order, and both of them

        :meth:`invalidate` is called *before* the read so its counter is already incremented when
        :meth:`refresh` samples it. That is what makes the two compose: ``refresh`` clears the stale
        flag only if the counter has not moved *during* its read, so an invalidation raised
        beforehand is satisfied by this very read rather than leaving the snapshot permanently
        stale. Calling ``invalidate`` after would mark the fresh snapshot stale for no reason and
        cost the next request a scheduled refresh.

        Awaiting :meth:`refresh` rather than relying on ``invalidate`` alone is what makes the
        write's visibility **synchronous**. With only ``invalidate``, the next ``snapshot()`` on
        this replica returns the *stale* value and schedules a background read — correct, and a
        millisecond later the new numbers are in force, but the request that immediately follows the
        ``PUT`` still sees the old ones. For an admin API that is a race an operator can lose, and
        for a test it is one that cannot be asserted without sleeping.

        Neither happens when the tier does not exist: :attr:`TierWrite.written` is ``False``, the
        store is untouched, and there is nothing to become stale.

        Returns:
            A :class:`TierWrite` carrying the outcome, the **true** committed prior row, the row now
            stored, the version this write produced, and this replica's refreshed snapshot.

        Raises:
            BackingStoreUnavailable: the write did not land. Deliberately propagated: C10 answers it
                with a 503, because an admin write that silently no-ops is worse than an error.
        """
        self._ensure_merge_script()

        # `settings.tier_limits` and NOT the snapshot: this is the process's shipped configuration,
        # a constant read from the environment at startup with no staleness window, and it is used
        # only when there is no committed row to merge onto. See `MERGE_TIER_ARGV_FALLBACK`.
        fallback = self._settings.tier_limits.get(name)
        args = [
            name,
            *(
                MERGE_TIER_UNCHANGED if value is None else str(value)
                for value in (rate_limit_per_min, burst, daily_quota, monthly_quota)
            ),
            *(
                (MERGE_TIER_UNCHANGED,) * TIER_FIELD_COUNT
                if fallback is None
                else (
                    str(fallback.rate_limit_per_min),
                    str(fallback.burst),
                    str(fallback.daily_quota),
                    str(fallback.monthly_quota),
                )
            ),
        ]

        reply = await self._gateway.run_script(
            RLQ_MERGE_TIER_NAME,
            keys=[CONFIG_TIERS_KEY, CONFIG_VERSION_KEY],
            args=args,
        )
        if len(reply) != MERGE_TIER_REPLY_ARITY:
            # A contract mismatch between the script and this decoder, not an outage. Raised rather
            # than papered over for the same reason `LimitDecision.from_lua` checks its arity: a
            # decision built out of whatever happened to be in the right slots is a plausible-looking
            # wrong answer, which is the failure mode this project exists to make impossible.
            raise ValueError(
                f"{RLQ_MERGE_TIER_NAME} returned {len(reply)} elements, expected "
                f"{MERGE_TIER_REPLY_ARITY}"
            )

        status = _as_text(reply[0])
        if status == MERGE_TIER_STATUS_ABSENT:
            # Nothing was written — the script returns before its HSET — so there is nothing to
            # invalidate and nothing to re-read. C10 turns this into a 404.
            return TierWrite(
                status=status,
                previous=None,
                config=None,
                version=self._snapshot.version,
                snapshot=self._snapshot,
            )

        previous = decode_tier(name, _as_text(reply[1]))
        current = decode_tier(name, _as_text(reply[2]))
        version = int(_as_text(reply[3]))

        if status != MERGE_TIER_STATUS_MERGED:
            logger.warning(
                "tier %r had no usable stored row (%s); the update was merged onto the configured "
                "default (%s) rather than onto a value nobody wrote",
                name,
                status,
                encode_tier(previous),
            )

        self.invalidate()
        snapshot = await self.refresh()
        return TierWrite(
            status=status,
            previous=previous,
            config=current,
            version=version,
            snapshot=snapshot,
        )

    async def refresh(self, *, strict: bool = False) -> _Snapshot:
        """Read ``config:tiers`` + ``config:version`` and swap in a new snapshot atomically.

        Returns the snapshot now in force — the new one on success, the **previous** one when Redis
        could not answer.

        Args:
            strict: Re-raise :class:`~src.redis_client.BackingStoreUnavailable` instead of
                swallowing it. **Default ``False``, and every internal caller keeps that default**
                — the background refresh, the startup refresh and :meth:`store_tier` all want an
                outage to leave the last good table serving rather than to become an exception.

                It exists for exactly one caller: C10's ``POST /admin/config/reload``. Swallowing
                there would answer a "reload now" with a ``200`` and a version number that had not
                moved, which is a reload button that lies — the one place where "keep serving the
                old table" is still correct behaviour but reporting success for it is not. The
                failure bookkeeping below (the counter, the backoff window, the log line) is
                identical either way; ``strict`` changes only who is told.

        .. rubric:: An outage keeps the last good table rather than clearing it

        :class:`~src.redis_client.BackingStoreUnavailable` is caught and logged at WARNING. There is
        no version of "we could not read the config" that is improved by forgetting the config we
        already had: the numbers in the current snapshot were correct five seconds ago and are
        almost certainly still correct, whereas an empty table would leave every principal with no
        ceiling to look up — and "no limit found" is indistinguishable from "unlimited" at the point
        where the decision is made. That stays true under ``strict``: the raised exception reports
        the failure, it does not discard the table.

        Correctness errors (a ``WRONGTYPE`` because something else was written to ``config:tiers``)
        are deliberately **not** caught here. They are bugs in this service, they do not get better
        by waiting, and the gateway's contract is that they propagate rather than being dressed up
        as an outage. :meth:`_background_refresh` and :meth:`start` each contain them at the one
        boundary where a bug must not become a crash.

        Two reads rather than one pipelined round trip: this runs at most once per TTL, off the
        request path, and each command goes through the breaker independently, so a partial failure
        degrades to "keep the old snapshot" instead of failing the batch as a unit. There is no
        latency here worth optimising.

        .. rubric:: An ``invalidate()`` that lands mid-read is not swallowed

        The invalidation counter is sampled **before** the read and re-checked after it. Clearing
        the stale flag unconditionally on success would drop any :meth:`invalidate` that arrived
        while the read was in flight: the reply we are about to install left Redis *before* the
        operator's write landed, so it is already out of date, and the flag it clears is the only
        thing that would have scheduled another read. The replica would then serve the pre-edit
        value for a full TTL with nothing pending — which is the precise scenario
        :meth:`invalidate` exists to prevent, reachable on any admin write that overlaps a
        background refresh. So the fresh snapshot is still installed (it is newer than what we had)
        and the registry stays stale, so the next :meth:`snapshot` goes round again.
        """
        async with self._lock:
            # Sampled before the read, so any invalidation from here on is known to describe a
            # write this read cannot have seen.
            invalidations_before = self._invalidations
            try:
                raw_tiers = await self._gateway.run(
                    lambda: self._gateway.client.hgetall(CONFIG_TIERS_KEY), op="tiers:read"
                )
                raw_version = await self._gateway.run(
                    lambda: self._gateway.client.get(CONFIG_VERSION_KEY), op="tiers:version"
                )
            except BackingStoreUnavailable as exc:
                # Deliberately NOT split on `BackingStoreOverloaded`, unlike `Limiter.check`.
                #
                # A momentary saturated pool therefore arms the full outage backoff, so a ~50 ms
                # burst can suppress tier-config propagation for a whole backoff window against a
                # store that never stopped answering. That is the known cost and it is accepted
                # here, where it is not accepted one module over, because this refresh is nothing
                # like the request path: it is off the hot path entirely (the snapshot is served
                # synchronously and stays *stale but correct* throughout), the only consequence is
                # that a runtime tier change reaches this replica a backoff later — bounded, and
                # already the documented convergence story for every replica that did not serve the
                # write — and treating it as a fast-retryable event would mean hammering a pool that
                # is signalling backpressure with the one call on this path that nobody is waiting
                # for. The distinction is load-bearing in `Limiter.check` because there it decides
                # whether a request is *metered*; here it decides only how soon we ask again.
                self.refresh_failures += 1
                self._refresh_backoff_until = self._clock() + self._refresh_backoff_sec
                # ONE line per backoff window, not one per request. The snapshot stays stale for
                # the whole outage, so an ungated log here is a per-request write to stdout on the
                # hot path of a service that is already degraded.
                logger.warning(
                    "tier refresh failed (%s); serving the last known table "
                    "(version=%d, tiers=%s) and suppressing further attempts for %.0fs",
                    exc,
                    self._snapshot.version,
                    ",".join(sorted(self._snapshot.tiers)),
                    self._refresh_backoff_sec,
                )
                if strict:
                    # Re-raised AFTER the bookkeeping above, never instead of it: an operator's
                    # reload that fails must still arm the backoff, or a dashboard polling reload
                    # during an outage would restore the per-request retry storm the backoff exists
                    # to prevent.
                    raise
                return self._snapshot

            tiers = self._parse_tiers(raw_tiers)
            version = self._parse_version(raw_version)

            # Built first, assigned second, never mutated. See `_Snapshot` for why the ordering of
            # these two statements is the reason `snapshot()` needs no lock.
            fresh = _build_snapshot(tiers, version=version, fetched_monotonic=self._clock())
            self._snapshot = fresh
            self._stale = self._invalidations != invalidations_before
            self._refresh_backoff_until = 0.0
            self.refreshes += 1
            logger.debug(
                "tier snapshot refreshed (version=%d, tiers=%s)", version, ",".join(sorted(tiers))
            )
            return fresh

    def _parse_tiers(self, raw: Mapping[object, object]) -> dict[str, TierConfig]:
        """Turn a raw ``config:tiers`` HASH into a tier table, tolerating individual bad rows.

        Starts from ``settings.tier_limits`` and overlays what Redis says. That base layer is not
        belt-and-braces: it is what keeps a *partially* populated hash — a half-finished manual
        edit, a tier added to the environment but not yet seeded — from producing a table that is
        missing a tier entirely. A principal on a missing tier has no ceiling to look up, and the
        one thing that must never happen is for that to read as "unlimited".

        A malformed row logs at ERROR and falls back to the configured default for that tier, so
        the blast radius of one bad value is exactly one tier's runtime customisation. A malformed
        row for a tier that has no configured default is **dropped**, loudly: inventing numbers for
        a tier nobody declared would be guessing at someone's billing.

        A row whose *field name* is unusable — empty, whitespace-only, or carrying whitespace or a
        control character — is dropped before its value is even looked at. Such a tier is inert
        (nothing can be assigned to a name a caller cannot type), but it would still be rendered
        into ``argv_tail`` and handed to the decision script, and Lua indexing a tier table by
        ``"free\\n"`` is not a defence anyone should have to write. Reported, never normalised: see
        :data:`_UNSAFE_TIER_NAME_RE`.
        """
        defaults = self._settings.tier_limits
        tiers: dict[str, TierConfig] = dict(defaults)
        for raw_name, raw_value in raw.items():
            name = _as_text(raw_name)
            if not is_safe_tier_name(name):
                logger.error(
                    "dropping %s row with an unusable tier name %r — a tier name may not be "
                    "empty and may not contain whitespace or control characters",
                    CONFIG_TIERS_KEY,
                    name,
                )
                continue
            try:
                tiers[name] = decode_tier(name, _as_text(raw_value))
            except ValueError as exc:
                fallback = defaults.get(name)
                if fallback is None:
                    logger.error(
                        "dropping unknown tier %r from the snapshot — %s (no configured default "
                        "to fall back to; a principal on this tier will use DEFAULT_TIER)",
                        name,
                        exc,
                    )
                    continue
                logger.error(
                    "malformed %s row for tier %r — %s; falling back to the configured default "
                    "(%s) rather than leaving the tier unenforced",
                    CONFIG_TIERS_KEY,
                    name,
                    exc,
                    encode_tier(fallback),
                )
                tiers[name] = fallback
        return tiers

    def _parse_version(self, raw: object) -> int:
        """Parse ``config:version``, keeping the previous number on anything unreadable.

        A missing key means nothing has been seeded yet; a non-numeric one means something wrote
        where it should not have. Neither is a reason to report a version *lower* than the one this
        replica already published on ``/health``, because C10 watches that number climb to prove a
        change propagated and a phantom rollback would read as a failed reload.
        """
        if raw is None:
            return self._snapshot.version
        try:
            return int(_as_text(raw))
        except ValueError:
            logger.error(
                "%s is not an integer (%r); keeping version %d",
                CONFIG_VERSION_KEY,
                raw,
                self._snapshot.version,
            )
            return self._snapshot.version

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        """Seed the table and take the first snapshot. Called by ``Runtime.start``.

        **Never raises.** The registry's fallback is ``settings.tier_limits``, which is a real
        enforceable table, so there is no failure here worth trading for a process that will not
        boot. A crash-looping replica enforces nothing at all, on every request, for as long as the
        loop lasts — strictly worse than enforcing the shipped numbers while Redis recovers. The
        failure is logged at ERROR and the degraded state is visible on ``/health`` as
        ``config_version: 0``.
        """
        self._closed = False
        try:
            await self.seed()
        except Exception:  # noqa: BLE001 - see the docstring: startup must not crash-loop
            logger.error(
                "tier registry seeding failed; anything already stored in %s is unchanged",
                CONFIG_TIERS_KEY,
                exc_info=True,
            )

        # A SEPARATE try, not a continuation of the one above. Seeding and reading fail for
        # different reasons and only one of them is fatal to correctness: a replica whose writes
        # are refused (a READONLY reply mid-failover, an ACL without write permission) can still
        # *read* a table that some other replica seeded, and that table is the authoritative one.
        # Letting a failed seed skip the refresh would boot such a replica on the configured
        # defaults while the real limits sat one GET away.
        try:
            await self.refresh()
        except Exception:  # noqa: BLE001 - same rule; `refresh` already absorbs plain outages
            logger.error(
                "initial tier refresh failed; serving the configured defaults (%s). Runtime tier "
                "changes will not be visible until a refresh succeeds.",
                ",".join(sorted(self._settings.tier_limits)),
                exc_info=True,
            )

    async def stop(self) -> None:
        """Cancel any in-flight background refresh. Called by ``Runtime.stop``, never raises.

        Ordered **before** the gateway is closed (see ``Runtime.stop``): a refresh in flight is
        holding a pooled connection, and closing the pool underneath it turns an orderly shutdown
        into a stack trace on the way out.
        """
        self._closed = True
        task, self._refresh_task = self._refresh_task, None
        self._refreshing = False
        if task is None or task.done():
            return
        task.cancel()
        # The task is ours and nobody else awaits it, so swallowing the cancellation here is the
        # complete story — there is no caller left to inform.
        with contextlib.suppress(asyncio.CancelledError):
            await task
