"""The ONE place this service talks to Redis, and the ONE place a Redis failure is classified.

Three things live here:

* :class:`CircuitBreaker` — pure, clock-injectable, no Redis import in its body. Turns a repeated
  250 ms timeout into a 0 ms refusal.
* :class:`BackingStoreUnavailable` — the single exception type every caller upstream catches. The
  limiter should not have to know that ``redis.exceptions.ConnectionError``,
  ``asyncio.TimeoutError`` and ``OSError`` all mean the same thing to it.
* :class:`RedisGateway` — one ``redis.asyncio.Redis`` for the process lifetime, the registered Lua
  script handles, the breaker, and :meth:`RedisGateway.run`, which is the only path to the server.

.. rubric:: Fail-soft is decided upstairs, not here

This module classifies failures; it does **not** swallow them. Every public method either returns a
result or raises :class:`BackingStoreUnavailable`. That is deliberate and it is the opposite of the
sibling caching project's ``L2Redis``, where a failed ``get`` is *correctly* just a cache miss.

A rate limiter has no such obvious default. "Redis is down" can mean serve the request through a
bounded local bucket (``FAIL_MODE=open``, the spec's graceful-degradation requirement) or refuse it
with a 503 (``FAIL_MODE=closed``, for deployments where the limit *is* the security control). Only
C8's limiter knows which, and only it can set ``X-RateLimit-Degraded`` so the fail-open is visible
rather than silent — a silent fail-open is indistinguishable from having no rate limiter at all.
A gateway that returned ``None`` on failure would make that decision by accident, in the wrong
module, for every caller at once.

.. rubric:: The classification rule: availability failures degrade, correctness failures raise

**Only a failure to *answer* is classified as :class:`BackingStoreUnavailable`.** A refused or hung
socket, a DNS failure, a server still ``LOADING`` — those are outages, they are nobody's bug, and
degrading through them is the whole point of ``FAIL_MODE``. Everything else that Redis can raise is
the opposite kind of event: the store is up, it answered, and *we* are wrong. A ``ResponseError``
from a Lua compile or runtime error in the C4 decision script, a ``NOAUTH``/``WRONGPASS`` from a
misconfigured password, a ``WRONGTYPE`` from a key-schema bug — none of those get better by waiting,
and none of them are survivable by "serving the request anyway".

Classifying them as an outage would be the worst possible outcome, because under ``FAIL_MODE=open``
it does not surface as an error at all: a one-character typo in the decision script would silently
disable rate limiting for **every request**, and ``/health`` would report it identically to a Redis
that had been unplugged. The bug would be invisible for exactly as long as nobody read the logs.

So they propagate as themselves, they do **not** feed the circuit breaker (a script bug fails on
every request, which would open the breaker and dress a permanent bug up as a passing degradation),
and they are logged at ``ERROR``. Upstream that becomes a 500 — visible, attributable, and the
correct answer when the service is the thing that is broken. This is the same reasoning
:meth:`RedisGateway.script` already applies to an unregistered script name, extended from script
*lookup* to script *execution*.

The rule is about *what the failure means*, not about which class redis-py happened to pick, and the
exception proves it: ``ReadOnlyError`` is a ``ResponseError`` and is nonetheless classified as an
outage, because ``READONLY You can't write against a read only replica`` is a failover in progress —
a store that transiently cannot serve us, which is the textbook case for degrading. The classes
therefore cut across redis-py's hierarchy in both directions, which is why the ``except`` ordering
in :meth:`RedisGateway.run` is spelled out rather than left to inheritance.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, NoReturn, TypeVar
from urllib.parse import urlsplit

import redis.asyncio
import redis.exceptions

from src.config import Settings

if TYPE_CHECKING:  # pragma: no cover - import exists only for the annotation below
    from redis.commands.core import AsyncScript

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Everything that means "the backing store did not answer", and **nothing else**. Classified into
#: ONE exception type by :meth:`RedisGateway.run` so no caller upstream has to enumerate them again
#: — and, more to the point, so no caller can enumerate them *incompletely*.
#:
#: ``redis.exceptions.ConnectionError`` covers a refused, reset or dropped connection;
#: ``redis.exceptions.TimeoutError`` covers a socket that stopped answering (a blackholed address
#: reaches ``socket_connect_timeout`` as exactly this); ``BusyLoadingError`` is a server that is up
#: but still loading its dataset, i.e. genuinely not yet available; ``asyncio.TimeoutError`` covers
#: a hang redis-py has not wrapped; ``OSError`` covers DNS failure and everything else the kernel
#: reports (the builtin ``ConnectionError`` and, on 3.11+, the builtin ``TimeoutError`` are both
#: ``OSError`` subclasses, so they are already in here).
#:
#: ``ReadOnlyError`` is the deliberate odd one out: it is a ``ResponseError`` subclass, and it is
#: still an availability failure. ``READONLY You can't write against a read only replica`` is what a
#: client sees for the window around a Redis failover — the store is transiently unable to accept
#: writes, which is a store we cannot use rather than a script we got wrong. Failing open through it
#: is precisely what the fail-open path is for; 500ing instead would take the API down during the
#: failover the limiter was supposed to ride out. Because it subclasses ``ResponseError``, membership
#: in this tuple is NOT enough on its own — see :meth:`RedisGateway.run`, which needs an explicit
#: earlier ``except`` for it, exactly as ``AuthenticationError`` needs the reverse.
#:
#: Deliberately NOT ``redis.exceptions.RedisError``: that is the root of the whole tree and swallows
#: the correctness failures below with it. See the module docstring.
FAILSOFT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    redis.exceptions.BusyLoadingError,
    redis.exceptions.ReadOnlyError,
    asyncio.TimeoutError,
    OSError,
)

#: The store answered and the answer was "your command is wrong". These are bugs in this service —
#: a broken Lua script (``ResponseError``), a misconfigured password (``NOAUTH`` / ``WRONGPASS``, or
#: an ACL denial), a key-schema mistake (``WRONGTYPE``, also a ``ResponseError``), a value this
#: client could not encode (``DataError``). :meth:`RedisGateway.run` re-raises them untouched.
#:
#: **Order is load-bearing where this tuple is used, in both directions.** redis-py makes
#: ``AuthenticationError`` and ``AuthorizationError`` subclasses of its *``ConnectionError``*, so an
#: ``except`` on :data:`FAILSOFT_EXCEPTIONS` would catch a wrong password as an outage unless this
#: tuple is matched first — while ``ReadOnlyError`` is a ``ResponseError`` that must be caught
#: *before* this tuple. Neither ordering alone is sufficient; :meth:`RedisGateway.run` spells out
#: all three clauses in the one order that satisfies both. ``NoPermissionError`` is listed for
#: explicitness even though it is already a ``ResponseError``.
CORRECTNESS_EXCEPTIONS: tuple[type[BaseException], ...] = (
    redis.exceptions.AuthenticationError,
    redis.exceptions.AuthorizationError,
    redis.exceptions.NoPermissionError,
    redis.exceptions.ResponseError,
    redis.exceptions.DataError,
)


class BackingStoreUnavailable(Exception):
    """Redis could not answer — the single classified failure type the limiter catches.

    Carries ``op`` (the logical operation name passed to :meth:`RedisGateway.run`) so a log line or
    a ``/health`` payload can say *what* failed without the caller re-deriving it from a traceback.
    """

    def __init__(self, message: str, *, op: str = "") -> None:
        super().__init__(message)
        self.op = op


class BreakerState(enum.StrEnum):
    """The three states of :class:`CircuitBreaker`."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Trip after N *consecutive* failures; refuse instantly; probe once after a cooldown.

    .. rubric:: Why a rate limiter needs this even though it already fails open

    Failing open without a breaker still pays for every failure. When Redis stops answering, each
    request blocks for the full ``REDIS_TIMEOUT_MS`` (250 ms) before the code can decide to fail
    open. At 1000 rps that is 250 concurrent coroutines parked on a dead socket at any instant —
    every one of them holding a pooled connection, an event-loop task and the client's request
    memory. p99 latency becomes 250 ms *even though the service is "gracefully degrading"*, the
    bounded 32-connection pool saturates so requests start queueing for a connection on top of the
    timeout, and the moment Redis comes back the entire backlog stampedes it at once and knocks it
    over again.

    The breaker converts a 250 ms failure into a 0 ms one. After five consecutive failures it stops
    dialling entirely: the fail-open path is taken immediately, latency stays flat, no connection is
    held, and a recovering Redis sees exactly **one** probe request every cooldown period instead of
    the full production load. Recovery becomes a thing that can happen rather than a thing the herd
    prevents.

    .. rubric:: Consecutive, not cumulative

    A counter that only ever increments trips eventually on *any* long-running healthy process —
    five transient blips over a week is not an outage, it is a week. Only an unbroken run of
    failures is evidence that the store is down, so :meth:`record_success` resets the count to zero.

    .. rubric:: Exactly one half-open probe

    When the cooldown elapses, the *first* caller to ask is let through and the breaker moves to
    ``HALF_OPEN``; everyone else is still refused until that probe reports back. Letting the whole
    backlog through at the cooldown boundary is the thundering herd the breaker exists to prevent,
    re-armed on a timer. A successful probe closes the breaker; a failed one re-opens it and
    restarts the **full** cooldown.

    ``clock`` is injectable (``time.monotonic`` by default) so the state machine is unit-testable
    without a single ``sleep`` — the tests advance a fake clock and assert transitions, which is
    both instant and deterministic. ``time.monotonic`` rather than ``time.time`` for the same reason
    ``Runtime.started_at`` uses it: an NTP step must not be able to end a cooldown early or extend
    it by hours.
    """

    def __init__(
        self,
        *,
        failure_threshold: int,
        cooldown_sec: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._failure_threshold = max(1, failure_threshold)
        self._cooldown_sec = max(0.0, float(cooldown_sec))
        self._clock = clock
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0

    @property
    def state(self) -> BreakerState:
        """The stored state — a pure read that never transitions.

        Note what this deliberately does *not* do: an ``OPEN`` breaker whose cooldown has already
        elapsed still reports ``OPEN`` here. The ``OPEN`` -> ``HALF_OPEN`` move happens inside
        :meth:`allow_request` because that is the moment a probe is actually being handed out, and
        making a *property* perform it would let ``/health`` — which reads this state on every
        10-second container probe — consume the single probe slot that a real request needed.
        Observation must not have side effects on a state machine whose whole contract is "exactly
        one".
        """
        return self._state

    @property
    def is_open(self) -> bool:
        """``True`` while the breaker is refusing everything (i.e. state is ``OPEN``).

        ``HALF_OPEN`` is *not* open: the breaker is mid-recovery with one probe outstanding, which
        is a different thing to report on ``/health`` than "we have given up dialling".
        """
        return self._state is BreakerState.OPEN

    @property
    def consecutive_failures(self) -> int:
        """Length of the current unbroken failure run. Reset to zero by any success."""
        return self._consecutive_failures

    def allow_request(self) -> bool:
        """Should the caller touch Redis right now? May move ``OPEN`` -> ``HALF_OPEN``."""
        if self._state is BreakerState.CLOSED:
            return True
        if self._state is BreakerState.HALF_OPEN:
            # The one probe is already out. Everyone else waits for its verdict rather than
            # joining it — a "half-open" that admits the whole backlog is just a slower outage.
            return False
        if self._clock() - self._opened_at < self._cooldown_sec:
            return False
        self._state = BreakerState.HALF_OPEN
        return True

    def record_success(self) -> None:
        """Close the breaker and clear the failure run. Any success is a full reset."""
        self._consecutive_failures = 0
        self._state = BreakerState.CLOSED

    def record_failure(self) -> None:
        """Count a failure, and open the breaker if this one is decisive."""
        self._consecutive_failures += 1
        if self._state is BreakerState.OPEN:
            # Unreachable through `RedisGateway.run` (allow_request() said no, so no call was
            # made), but a direct caller must not be able to extend the cooldown indefinitely by
            # reporting failures that never touched a socket. Count it; do not re-arm the timer.
            return
        if (
            self._state is BreakerState.HALF_OPEN
            or self._consecutive_failures >= self._failure_threshold
        ):
            self._open()

    def _open(self) -> None:
        """Enter ``OPEN`` and start the cooldown from now."""
        self._state = BreakerState.OPEN
        self._opened_at = self._clock()


def redact_redis_url(url: str) -> str:
    """Return ``url`` with any credentials removed — ``scheme://host:port/path`` only.

    Startup logs are the most-copied text in any incident: they land in issue reports, in chat, and
    in whatever aggregator ingests container stdout. A ``REDIS_URL`` may legitimately carry
    ``user:password@``, so the raw value is never logged; the host, port and database index are what
    an operator actually needs to see, and none of them are secret.

    Anything unparseable returns a fixed marker rather than the input — falling back to "log it raw"
    would leak precisely the credentials in the malformed URLs this branch exists to handle.
    """
    try:
        parts = urlsplit(url)
        host = parts.hostname or ""
        port = parts.port
    except ValueError:
        # urlsplit itself is lenient; `.port` is what raises on a non-numeric port.
        return "<unparseable redis url>"
    if not host:
        return "<unparseable redis url>"
    location = f"{host}:{port}" if port is not None else host
    return f"{parts.scheme}://{location}{parts.path}"


class RedisGateway:
    """One pooled async Redis client, the registered scripts, and the breaker in front of both.

    Constructed synchronously (no I/O) and connected separately, so ``Runtime.build`` stays a plain
    function and the test seam that injects a pre-built Runtime does not accidentally open a socket.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self._clock = clock
        self._client: redis.asyncio.Redis | None = None
        self._scripts: dict[str, AsyncScript] = {}

        self.breaker = CircuitBreaker(
            failure_threshold=settings.breaker_failures,
            cooldown_sec=settings.breaker_cooldown_sec,
            clock=clock,
        )

        # Observability counters, surfaced on /health and in the C11 stats payload. A degradation
        # nobody can see is a degradation nobody fixes.
        self.calls = 0
        self.errors = 0
        self.short_circuits = 0
        #: Monotonic timestamp of the first failure in the CURRENT failure run, or ``None`` while
        #: healthy. Monotonic (not wall-clock) so it is a duration source that an NTP step cannot
        #: corrupt; render it as ``clock() - degraded_since`` seconds, never as a date.
        self.degraded_since: float | None = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    @property
    def client(self) -> redis.asyncio.Redis:
        """The underlying client. Raises if :meth:`connect` has not been awaited."""
        if self._client is None:
            raise RuntimeError("RedisGateway.connect() must be awaited before use")
        return self._client

    @property
    def is_connected(self) -> bool:
        """Whether a client object exists (not whether the server is currently answering)."""
        return self._client is not None

    async def connect(self) -> None:
        """Build the ONE process-lifetime client. Idempotent; opens no socket by itself.

        Every keyword below is a decision:

        ``decode_responses=False``
            Values come back as ``bytes``. The decision script's reply is 19 positional elements
            that are parsed by index into integers, so a per-value UTF-8 decode on the hot path
            would buy nothing and cost a pass over every reply. It also keeps binary values safe if
            anything later stores one.

        ``retry_on_timeout=False``
            **This is a correctness setting, not a tuning knob, and it must not be "optimised" on.**
            The decision script is NOT idempotent: it spends a token, increments the daily counter
            and increments the monthly counter. If the command times out *after* Redis executed it —
            which is exactly what a socket timeout cannot distinguish from "never arrived" — an
            automatic retry runs the whole script a second time and charges the caller twice for one
            request. The caller then hits a 429 for traffic they never sent, and the daily quota
            they paid for silently became half of one. A retry is only safe for an idempotent
            command, and the single most important command in this service is not one.

        ``socket_timeout`` / ``socket_connect_timeout``
            Both ``REDIS_TIMEOUT_MS`` (250 ms). The entire rate-limit check has a 5 ms budget, so a
            Redis that has stopped answering has to be classified as failed in milliseconds. The
            kernel's default TCP timeout is ~60-120 s; inheriting it would mean a dead Redis hangs
            every request rather than degrading it, and a limiter that hangs is strictly worse for
            the caller than one that fails open.

        ``socket_keepalive=True`` / ``health_check_interval=30``
            Pooled connections idle between bursts. Without keepalive a middlebox (or a container
            network's conntrack table) can silently drop an idle connection, and the failure only
            surfaces as a timeout on the *next* real request — i.e. on a caller's latency rather
            than on a background check. The 30 s health check pings connections before handing them
            out, so a stale one is replaced instead of failing a request.
        """
        if self._client is not None:
            return
        timeout_sec = self.settings.redis_timeout_ms / 1000
        # ONE `Redis` object for the whole process, and it owns its pool.
        #
        # Never construct a second `Redis` around a pool this one already holds, and never share a
        # ConnectionPool between `Redis` instances (redis-py's own docs are explicit): closing
        # either instance disconnects the shared pool, and the other instance is left holding
        # connections that are already dead — a failure that appears as sporadic ConnectionErrors
        # long after the close that caused them, in whichever component did NOT do the closing.
        # If a second client is ever genuinely needed, share THIS object instead.
        self._client = redis.asyncio.from_url(
            self.settings.redis_url,
            decode_responses=False,
            max_connections=self.settings.redis_max_connections,
            socket_timeout=timeout_sec,
            socket_connect_timeout=timeout_sec,
            socket_keepalive=True,
            health_check_interval=30,
            retry_on_timeout=False,
        )
        logger.info(
            "redis gateway connected (url=%s, pool=%d, timeout=%dms)",
            redact_redis_url(self.settings.redis_url),
            self.settings.redis_max_connections,
            self.settings.redis_timeout_ms,
        )

    async def aclose(self) -> None:
        """Release the client and its pool. Idempotent, and never raises from teardown.

        ``aclose()`` rather than the deprecated ``close()``: in redis-py 5.x ``close()`` is an alias
        scheduled for removal and emits a ``DeprecationWarning``, and the pool release semantics are
        only documented for ``aclose``.

        The registered script handles are dropped too. Each one holds a reference to the client it
        was registered against, so keeping them across a close would leave :meth:`run_script`
        dispatching onto a disconnected client — a reconnect must re-register.
        """
        client, self._client = self._client, None
        self._scripts.clear()
        if client is None:
            return
        try:
            await client.aclose()
        except (redis.exceptions.RedisError, asyncio.TimeoutError, OSError) as exc:
            # Teardown is best-effort. Raising here would turn a shutdown into a crash and, under
            # compose, a crash loop — while the process is exiting anyway and the sockets are about
            # to be reclaimed by the kernel regardless.
            #
            # The whole `RedisError` tree, deliberately: this is the ONE place the
            # availability/correctness split does not apply. That split exists so a bug becomes
            # visible to a caller instead of degrading them, and on the way out there is no caller
            # left to tell and no decision left to make — only a log line.
            logger.warning("redis gateway close failed: %r", exc)

    # ------------------------------------------------------------------ #
    # Scripts
    # ------------------------------------------------------------------ #
    def register(self, name: str, script_body: str) -> AsyncScript:
        """Register a Lua script and keep its handle under ``name``.

        Uses redis-py's ``register_script``, which gives two things a hand-rolled
        ``SCRIPT LOAD`` + ``EVALSHA`` does not:

        * ``EVALSHA`` by default, so the script body is not re-sent on every request (the decision
          script is a few kilobytes, on the hot path, at 1000 rps);
        * **transparent ``NOSCRIPT`` reload** — on a ``NOSCRIPT`` error it re-sends the body with
          ``EVAL`` and carries on, transparently to the caller.

        That second property is the load-bearing one, because Redis's script cache is *volatile*.
        It is empty after a restart, it is emptied by ``SCRIPT FLUSH`` (which ``FLUSHALL`` in some
        configurations and plenty of ops runbooks perform), and — the case that actually bites in
        production — a replica promoted by a failover has never seen the script at all, because
        script loads are not replicated. Hand-rolled ``EVALSHA`` means every request 500s from the
        instant of the failover until someone redeploys. ``tests/integration/`` proves this by
        issuing ``SCRIPT FLUSH`` and re-running: the test only passes if this method was used.
        """
        script = self.client.register_script(script_body)
        self._scripts[name] = script
        return script

    def script(self, name: str) -> AsyncScript:
        """Return a previously :meth:`register`-ed handle, or raise :class:`KeyError`.

        ``KeyError`` and not :class:`BackingStoreUnavailable`: an unregistered script name is a
        wiring bug in this process, not a store outage, and classifying it as an outage would make
        the limiter fail *open* on a typo — i.e. silently stop enforcing anything.

        :data:`CORRECTNESS_EXCEPTIONS` extends the same rule from script *lookup* to script
        *execution*: a script that fails to compile or blows up at runtime raises a
        ``ResponseError``, and that is the identical bug arriving one step later.
        """
        try:
            return self._scripts[name]
        except KeyError:
            raise KeyError(
                f"lua script {name!r} was never registered "
                f"(known: {sorted(self._scripts) or 'none'})"
            ) from None

    # ------------------------------------------------------------------ #
    # The one path to Redis
    # ------------------------------------------------------------------ #
    async def run(self, coro_factory: Callable[[], Awaitable[T]], *, op: str) -> T:
        """Execute one Redis operation through the breaker. THE only way to reach the server.

        Args:
            coro_factory: a zero-argument callable returning the awaitable to run, e.g.
                ``lambda: gateway.client.ping()``. A *factory* rather than an already-created
                coroutine, because when the breaker is open the operation must never be constructed
                at all: an un-awaited coroutine emits a ``RuntimeWarning`` and, worse, the point of
                the open state is that nothing is built, dialled or queued on the way to the
                refusal.
            op: a short logical name for logs and for the raised exception.

        Raises:
            BackingStoreUnavailable: breaker open, gateway not connected, or the store failed to
                answer (:data:`FAILSOFT_EXCEPTIONS`, including a ``READONLY`` reply from a replica
                mid-failover).
            redis.exceptions.RedisError: unchanged, for a :data:`CORRECTNESS_EXCEPTIONS` failure —
                a broken script, a bad password, a wrong key type. Those are bugs in this service,
                not outages, and the module docstring explains why they must not be dressed up as
                one.

        Availability failures are classified and re-raised, never swallowed — see the module
        docstring. The breaker is fed on both outcomes here rather than at the call sites, so
        *every* Redis touch (including ``/health``'s ping) contributes evidence, and there is no
        path that can fail repeatedly without the breaker ever noticing.
        """
        if self._client is None:
            # Checked BEFORE `allow_request()`, and the order is the whole point. `allow_request()`
            # is not a query, it is a *withdrawal*: at the cooldown boundary it moves the breaker
            # OPEN -> HALF_OPEN and hands out the single probe. Consuming it and then raising here
            # — without a matching record_success()/record_failure() — would leave the breaker in
            # HALF_OPEN with a probe that never reports back, and HALF_OPEN refuses everyone else
            # forever. Unreachable today (`_client` only clears in `aclose()`), but a state machine
            # whose contract is "exactly one probe" must not have a path that spends one for free.
            #
            # No socket exists, so there is nothing for the breaker to learn either: this failure is
            # already free, and tripping the breaker on it would only mask the wiring bug behind a
            # cooldown. Counted as an error so it still shows up on /health.
            self.errors += 1
            raise BackingStoreUnavailable(
                f"{op}: RedisGateway.connect() was never awaited", op=op
            )
        if not self.breaker.allow_request():
            self.short_circuits += 1
            raise BackingStoreUnavailable(
                f"{op}: circuit breaker is open — refusing without touching Redis "
                f"(consecutive failures: {self.breaker.consecutive_failures})",
                op=op,
            )

        self.calls += 1
        try:
            result = await coro_factory()
        except redis.exceptions.ReadOnlyError as exc:
            # FIRST, and ahead of CORRECTNESS_EXCEPTIONS on purpose. `ReadOnlyError` is a
            # `ResponseError` subclass that is nonetheless an availability failure: a replica that
            # cannot accept writes is a store we cannot use, not a script we got wrong. It is what
            # every client sees for the window around a failover, and degrading through that window
            # is the entire job of the fail-open path — 500ing instead would take the API down
            # during exactly the event the limiter was built to survive.
            self._fail_soft(op, exc)
        except CORRECTNESS_EXCEPTIONS:
            # The store answered; we are the ones who are wrong. Re-raised as itself so it becomes
            # a 500 upstream instead of a fail-open, and the breaker is told NOTHING: a broken
            # script fails on every request, so feeding this to the breaker would open it and
            # relabel a permanent bug as a passing degradation. Counted as an error so /health
            # still shows that something is failing, but `degraded_since` is left alone — this is
            # not degradation, and the store is not degraded.
            #
            # ERROR with the operation name and a traceback, because upstream sees only a 500 and
            # this log line is the only place the actual cause is written down.
            self.errors += 1
            logger.error(
                "redis %s raised a correctness error — this is a bug in this service, not a store "
                "outage; NOT failing open and NOT counting it against the breaker",
                op,
                exc_info=True,
            )
            raise
        except FAILSOFT_EXCEPTIONS as exc:
            self._fail_soft(op, exc)

        self.breaker.record_success()
        self.degraded_since = None
        return result

    def _fail_soft(self, op: str, exc: BaseException) -> NoReturn:
        """Record one availability failure and raise the classified type. Never returns.

        Shared by both availability clauses in :meth:`run` so that "what an outage does to the
        counters and the breaker" has exactly one definition. ``ReadOnlyError`` has to be caught in
        a clause of its own for ordering reasons, and a second copy of this body would be a second
        place for the breaker bookkeeping to drift.
        """
        self.errors += 1
        self.breaker.record_failure()
        if self.degraded_since is None:
            self.degraded_since = self._clock()
        logger.warning("redis %s failed: %r (breaker=%s)", op, exc, self.breaker.state)
        raise BackingStoreUnavailable(f"{op} failed: {exc!r}", op=op) from exc

    async def run_script(self, name: str, keys: list[str], args: list[Any]) -> Any:
        """Run a registered script through :meth:`run`. Convenience over the two-step form."""
        script = self.script(name)
        return await self.run(lambda: script(keys=keys, args=args), op=f"script:{name}")

    async def ping(self) -> bool:
        """``PING`` the server, through :meth:`run` so the result feeds the breaker.

        Used by ``GET /health``. Routing it through :meth:`run` rather than calling the client
        directly matters in both directions: a failing health probe counts toward opening the
        breaker (so an outage is detected by the thing that polls every 10 seconds, not only by
        production traffic), and once the breaker is open the probe costs nothing — a ``/health``
        that blocks for 250 ms per call during an outage is how a Redis incident turns into a
        container restart loop.

        Raises:
            BackingStoreUnavailable: on any failure. The caller decides what to report.
        """
        return bool(await self.run(lambda: self.client.ping(), op="ping"))

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    def stats(self) -> dict[str, Any]:
        """Counter snapshot for ``/health`` and the C11 dashboard payload."""
        degraded_for = (
            None if self.degraded_since is None else max(0.0, self._clock() - self.degraded_since)
        )
        return {
            "connected": self.is_connected,
            "calls": self.calls,
            "errors": self.errors,
            "short_circuits": self.short_circuits,
            "breaker_state": str(self.breaker.state),
            "consecutive_failures": self.breaker.consecutive_failures,
            "degraded_for_sec": degraded_for,
        }
