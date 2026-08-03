"""Fixtures for the integration suite: a real Redis URL, a connected gateway, and a Lua driver.

The compose ``test`` service sets ``REDIS_URL=redis://redis:6379/0`` and waits on Redis's own
``redis-cli ping`` healthcheck, so by the time pytest starts the server is answering commands
(rather than merely listening — the distinction ``depends_on: condition: service_healthy`` exists
to make).

.. rubric:: Why the gateway fixture flushes BEFORE as well as after

Flushing only on teardown leaves the suite's correctness depending on every test that came before
it having exited cleanly. A test interrupted by ``ctrl-c``, an ``xdist`` worker crash, or simply a
failing assertion inside a ``with`` block leaves keys behind, and the very next test then reads a
counter it did not write. For a rate limiter that is the worst possible flakiness: a leftover
``rate_limit:{alice}`` hash makes a bucket test pass or fail depending on what ran before it, and
the failure looks like a limiter bug rather than a fixture bug. Flushing on the way in makes each
test's starting state a fact rather than an inference.

.. rubric:: Why C4's Lua tests drive the SCRIPT rather than the :class:`~src.limiter.Limiter`

:class:`ScriptDriver` below calls :data:`~src.lua.RLQ_CHECK_AND_CONSUME` directly, with every ARGV
under the test's control. That is deliberate. The properties under test are things like "a bucket
of capacity 5 admits exactly 5" and "a window of width W does not admit 2x limit across its
boundary", and stating them needs a *tiny* tier (capacity 5, not 60) and a *frozen* clock. Going
through the limiter would mean expressing both through :class:`~src.config.Settings` and a
:class:`~src.tiers.TierRegistry`, which is a lot of apparatus between the assertion and the thing
it is about — and it would make every test that wanted a different capacity build a different
registry.

The limiter's own wrapper contract (KEYS order, ARGV order, the clock-override gate) is asserted
in ``tests/unit/test_limiter.py`` against a stub, where it costs microseconds. The two meet in
``tests/integration/test_lua_contract.py``, which drives a **real** :class:`~src.limiter.Limiter`
against a **live** reply and round-trips it through
:meth:`~src.models.LimitDecision.from_lua` — the producer/consumer contract that neither half can
check alone.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest

from src.config import Settings
from src.keys import (
    bucket_key,
    daily_quota_key,
    day_expire_at,
    month_expire_at,
    monthly_quota_key,
    sliding_window_key,
    sliding_window_prefix,
    user_key,
    window_index,
)
from src.lua import (
    MICRO_TOKENS,
    RLQ_CHECK_AND_CONSUME,
    RLQ_CHECK_AND_CONSUME_NAME,
    SW_DISABLED,
    SW_ENABLED,
)
from src.models import LUA_REPLY_FIELDS, LimitDecision
from src.redis_client import RedisGateway

#: The compose ``test`` service injects this; the default is the same value so a developer running
#: pytest inside any container on the compose network gets the same target without extra setup.
DEFAULT_REDIS_URL = "redis://redis:6379/0"


@pytest.fixture(scope="session")
def redis_url() -> str:
    """The real Redis this suite runs against. Session-scoped — it is a constant, not a resource."""
    return os.environ.get("REDIS_URL", DEFAULT_REDIS_URL)


@pytest.fixture()
def redis_settings(redis_url: str) -> Settings:
    """Settings pointed at the real Redis, with everything else left at its shipped default.

    ``_env_file=None`` cuts the ``.env`` source out so nothing on a developer's disk influences the
    run. The three secrets are NOT passed: ``tests/conftest.py`` puts them in ``os.environ`` with
    ``setdefault`` before ``src`` is imported, so they resolve from there — one declaration, in the
    file the tests actually read, exactly as C1 designed it.
    """
    return Settings(_env_file=None, redis_url=redis_url)


@pytest.fixture()
async def gateway(redis_settings: Settings):
    """Yield a connected :class:`~src.redis_client.RedisGateway` against a flushed database.

    Function-scoped, deliberately. A session-scoped async fixture would be bound to whichever event
    loop created it, while ``pytest.ini`` sets ``asyncio_default_fixture_loop_scope = function`` —
    so a shared gateway would hand loop-bound connections to tests running on a different loop, and
    the failure surfaces as an intermittent "attached to a different loop" hundreds of lines from
    its cause. Connection setup here is lazy and local, so the per-test cost is a ``FLUSHDB``.
    """
    instance = RedisGateway(redis_settings)
    await instance.connect()
    try:
        await instance.client.flushdb()
        yield instance
    finally:
        try:
            await instance.client.flushdb()
        finally:
            await instance.aclose()


# ---------------------------------------------------------------------------------------------
# C4 — driving the decision script directly
# ---------------------------------------------------------------------------------------------

#: A classified endpoint label, exactly as :func:`src.keys.classify` would produce one. The bucket
#: is per ``(user, endpoint)``, so tests that want two independent buckets vary this.
DEFAULT_ENDPOINT = "GET:/api/v1/whoami"

#: One minute, matching ``SLIDING_WINDOW_SEC=60``. The tier's rpm is the ceiling *for one window*,
#: so a test that changes this changes what the limit means.
DEFAULT_WINDOW_MS = 60_000

#: ``BUCKET_TTL_SEC=3600`` in milliseconds — the FLOOR under a bucket's TTL, not its value.
DEFAULT_BUCKET_TTL_MS = 3_600_000


@dataclass(frozen=True, slots=True)
class TierRow:
    """One tier in the flat ARGV tail: ``name, rpm, burst, daily, monthly``.

    Constructed by tests rather than parsed from :class:`~src.config.Settings`, because the whole
    point of these tests is a capacity small enough to state exactly ("5 requests, then a 429"),
    and because the script must be provable against a ``0`` limit — the "unlimited" sentinel —
    which :func:`src.config.parse_tier_limits` and :func:`src.tiers.decode_tier` both refuse to
    produce. The *shape* is still pinned to the real one:
    ``tests/unit/test_limiter.py`` asserts this is 5 slots wide, the same as
    :data:`src.tiers.ARGV_SLOTS_PER_TIER`.
    """

    name: str
    rpm: int
    burst: int
    daily: int
    monthly: int

    def argv(self) -> tuple[str, ...]:
        return (
            self.name,
            str(self.rpm),
            str(self.burst),
            str(self.daily),
            str(self.monthly),
        )


#: A roomy tier, so a test that is about ONE gate is not accidentally about another. Individual
#: tests narrow whichever number they are actually asserting on.
ROOMY = TierRow(name="free", rpm=100_000, burst=100_000, daily=1_000_000, monthly=10_000_000)


def tier_tail(*rows: TierRow) -> tuple[str, ...]:
    """Render ``(count, name, rpm, burst, daily, monthly, ...)`` — the shape ARGV[10..] carries.

    A local re-implementation of :func:`src.tiers.render_argv_tail` on purpose: that function only
    accepts a mapping of validated :class:`~src.config.TierConfig` objects, and half of what these
    tests need to prove is how the script behaves on values the Python validators reject.
    """
    tail: list[str] = [str(len(rows))]
    for row in rows:
        tail.extend(row.argv())
    return tuple(tail)


def field(raw: list[Any], name: str) -> Any:
    """Read one element of a live reply **by name**, via :data:`~src.models.LUA_REPLY_FIELDS`.

    Never by literal index. The field order is a contract owned by ``src.models``, and a test that
    hard-coded ``raw[7]`` would keep passing while silently asserting about a different field the
    day an element is inserted.
    """
    return raw[LUA_REPLY_FIELDS.index(name)]


@dataclass(frozen=True, slots=True)
class Reply:
    """One decision: the raw 19-element list, and the same thing decoded.

    Both are kept because they answer different questions. ``raw`` is the wire contract (arity,
    positions, "no element is ever nil"); ``decision`` is what the middleware will actually see,
    and running every integration assertion through
    :meth:`~src.models.LimitDecision.from_lua` means the decoder is exercised against a live reply
    on every single one of these tests rather than only in the one that says so.
    """

    raw: list[Any]
    decision: LimitDecision

    @property
    def allowed(self) -> bool:
        return self.decision.allowed

    @property
    def reason(self) -> str:
        return self.decision.reason.value


class ScriptDriver:
    """Calls :data:`~src.lua.RLQ_CHECK_AND_CONSUME` with every KEY and ARGV under the test's control.

    Registered through :meth:`~src.redis_client.RedisGateway.register`, so these tests exercise the
    same ``EVALSHA`` + transparent-``NOSCRIPT``-reload path production uses.
    """

    def __init__(self, gateway: RedisGateway) -> None:
        self.gateway = gateway
        gateway.register(RLQ_CHECK_AND_CONSUME_NAME, RLQ_CHECK_AND_CONSUME)

    # -- key helpers, so a test never hand-builds a key string ------------------------------- #
    @staticmethod
    def bucket(user_id: str, endpoint: str = DEFAULT_ENDPOINT) -> str:
        return bucket_key(user_id, endpoint)

    @staticmethod
    def window(user_id: str, now_ms: int, window_ms: int = DEFAULT_WINDOW_MS) -> str:
        return sliding_window_key(user_id, window_index(now_ms, window_ms))

    @staticmethod
    def daily(user_id: str, now_ms: int) -> str:
        return daily_quota_key(user_id, moment_of(now_ms))

    @staticmethod
    def monthly(user_id: str, now_ms: int) -> str:
        return monthly_quota_key(user_id, moment_of(now_ms))

    @staticmethod
    def user(user_id: str) -> str:
        return user_key(user_id)

    async def call(
        self,
        user_id: str,
        *,
        now_ms: int,
        endpoint: str = DEFAULT_ENDPOINT,
        cost: int = 1,
        tiers: tuple[TierRow, ...] = (ROOMY,),
        default_tier: str = "free",
        window_ms: int = DEFAULT_WINDOW_MS,
        sw_enabled: bool = True,
        bucket_ttl_ms: int = DEFAULT_BUCKET_TTL_MS,
        daily_expire_at: int | None = None,
        monthly_expire_at: int | None = None,
    ) -> Reply:
        """Run one decision and return it both raw and decoded.

        ``now_ms`` is mandatory and has no default. Every property worth asserting here is about
        *time* — refill, decay, expiry — and a test that silently used the wall clock would be
        asserting about whatever the container's scheduler did between two lines. The quota keys
        and their ``EXPIREAT`` instants are derived from this same ``now_ms``, so the Python-side
        calendar and the script's clock cannot disagree about which day it is.
        """
        moment = moment_of(now_ms)
        keys = [
            bucket_key(user_id, endpoint),
            daily_quota_key(user_id, moment),
            monthly_quota_key(user_id, moment),
            user_key(user_id),
        ]
        args = [
            str(cost),
            str(bucket_ttl_ms),
            sliding_window_prefix(user_id),
            str(window_ms),
            SW_ENABLED if sw_enabled else SW_DISABLED,
            str(day_expire_at(moment) if daily_expire_at is None else daily_expire_at),
            str(month_expire_at(moment) if monthly_expire_at is None else monthly_expire_at),
            default_tier,
            str(now_ms),
            *tier_tail(*tiers),
        ]
        raw = await self.gateway.run_script(RLQ_CHECK_AND_CONSUME_NAME, keys=keys, args=args)
        decision = LimitDecision.from_lua(
            raw, user_id=user_id, endpoint=endpoint, cost=cost, latency_ms=0.0
        )
        return Reply(raw=list(raw), decision=decision)

    async def drain(self, user_id: str, *, now_ms: int, attempts: int, **kwargs: Any) -> int:
        """Fire ``attempts`` sequential calls at one frozen instant; return how many were allowed.

        Sequential rather than concurrent, so "how many got through" is a statement about the
        limits rather than about the event loop. The concurrent version — the one that proves
        atomicity — lives in ``test_lua_contract.py``.
        """
        allowed = 0
        for _ in range(attempts):
            reply = await self.call(user_id, now_ms=now_ms, **kwargs)
            if reply.allowed:
                allowed += 1
        return allowed


def moment_of(now_ms: int) -> datetime:
    """The aware UTC datetime for an epoch-milliseconds instant.

    Every quota key and expiry in these tests is derived from the same ``now_ms`` the script is
    given, so a frozen clock freezes the whole calendar with it.
    """
    return datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)


def tokens_of(micro: bytes | str | int | None) -> float:
    """Convert a stored ``t`` field (micro-tokens) back to whole tokens, for readable assertions."""
    if micro is None:
        raise AssertionError("bucket has no 't' field")
    if isinstance(micro, bytes):
        micro = micro.decode()
    return int(micro) / MICRO_TOKENS


@pytest.fixture()
def driver(gateway: RedisGateway) -> ScriptDriver:
    """The decision script, registered on the flushed gateway."""
    return ScriptDriver(gateway)


@pytest.fixture()
def now_ms() -> int:
    """A frozen "now", in epoch milliseconds, anchored to the container's **real** clock.

    Anchored rather than an arbitrary constant, and that is load-bearing: the quota counters are
    expired with ``EXPIREAT``, whose argument is an absolute instant compared against the
    *server's* clock. A fabricated ``now_ms`` from 2020 would make ``day_expire_at`` produce a
    timestamp in the past, and Redis would delete the counter the moment it was created — every
    quota test would then pass for the wrong reason, having asserted on a key that no longer
    exists.
    """
    return int(time.time() * 1000)
