"""Unit tests for :class:`~src.limiter.Limiter` — the **wrapper**, never the Lua.

Everything here runs against a stub gateway that records what it was handed and replays a canned
reply. Nothing in this file asserts a rate-limiting *behaviour*, and that boundary is deliberate:

* **fakeredis is the wrong oracle for this script.** It is a reimplementation whose ``TIME``, float
  coercion and Lua->RESP rules are approximations, and the entire job of the decision script is
  exactness. A test that "proved" the bucket admits exactly 60 requests against an approximation
  would be worse than no test, because it would be believed. Every behavioural assertion lives in
  ``tests/integration/test_lua_*.py`` against a real ``redis:7-alpine``.
* **The wrapper has its own contract and it is worth pinning here**, in microseconds: the KEYS go
  in a fixed order and must land in one hash tag; the ARGV head goes in a fixed order and the tier
  tail must be the snapshot's *own* tuple rather than a per-request rebuild; the clock-override
  seam must be inert; a store outage must propagate rather than being absorbed.

The single most important test in this file is
:func:`test_the_tier_tail_is_the_snapshots_own_objects`, because the property it protects is
invisible: a limiter that re-rendered the tier table on every request would be **correct** and
would simply be slower, which is exactly the kind of regression that ships.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import pytest

from src.config import Settings
from src.keys import (
    bucket_key,
    daily_quota_key,
    day_expire_at,
    hash_tag,
    month_expire_at,
    monthly_quota_key,
    sliding_window_prefix,
    user_key,
)
from src.limiter import Limiter
from src.lua import (
    ARGV_BUCKET_TTL_MS,
    ARGV_COST,
    ARGV_DAILY_EXPIRE_AT,
    ARGV_DEFAULT_TIER,
    ARGV_HEAD_ARITY,
    ARGV_MONTHLY_EXPIRE_AT,
    ARGV_NOW_MS_OVERRIDE,
    ARGV_SW_ENABLED,
    ARGV_SW_PREFIX,
    ARGV_SW_WINDOW_MS,
    ARGV_TIER_COUNT,
    ARGV_TIER_TABLE,
    KEYS_ARITY,
    KEY_BUCKET,
    KEY_QUOTA_DAILY,
    KEY_QUOTA_MONTHLY,
    KEY_USER,
    NO_CLOCK_OVERRIDE,
    RLQ_CHECK_AND_CONSUME,
    RLQ_CHECK_AND_CONSUME_NAME,
    SW_DISABLED,
    SW_ENABLED,
    TIER_ARGV_SLOTS,
    UNENFORCED_PERIOD,
)
from src.models import LUA_REPLY_ARITY, LUA_REPLY_FIELDS, DenyReason, QuotaPeriodState
from src.redis_client import BackingStoreUnavailable
from src.tiers import ARGV_SLOTS_PER_TIER, _build_snapshot

USER = "alice"
ENDPOINT = "GET:/api/v1/logs/query"

#: A fixed instant, so the daily/monthly keys and both ``EXPIREAT`` values are exact rather than
#: "whatever the clock said between two assertions".
MOMENT = datetime(2026, 8, 10, 13, 45, 30, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------------------------


def lua_reply(**overrides: Any) -> list[Any]:
    """A well-formed 19-element reply, in :data:`~src.models.LUA_REPLY_FIELDS` order.

    Built from a mapping and projected through ``LUA_REPLY_FIELDS`` rather than written as a
    literal list: a hand-ordered literal is a second, silent copy of the field order, and it would
    keep "passing" while asserting about the wrong slots the day an element moves.

    Strings arrive as ``bytes`` because the gateway runs with ``decode_responses=False``.
    """
    values: dict[str, Any] = {
        "allowed": 1,
        "reason": b"ok",
        "tier": b"free",
        "bucket_limit": 60,
        "bucket_remaining": 59,
        "bucket_reset_ms": 1_000,
        "window_limit": 60,
        "window_used": 1,
        "window_reset_ms": 45_000,
        "daily_limit": 1_000,
        "daily_used": 1,
        "daily_expire_at": 1_786_060_800,
        "daily_state": b"reset",
        "monthly_limit": 25_000,
        "monthly_used": 1,
        "monthly_expire_at": 1_788_307_200,
        "monthly_state": b"reset",
        "retry_ms": 0,
        "now_ms": 1_786_016_730_000,
    }
    values.update(overrides)
    return [values[name] for name in LUA_REPLY_FIELDS]


class StubGateway:
    """The three :class:`~src.redis_client.RedisGateway` methods the limiter touches.

    ``script``/``register`` mirror the real class's contract exactly — including the ``KeyError``
    for an unregistered name — because the limiter's re-registration path is driven by that
    ``KeyError`` and a stub that returned ``None`` instead would make the path untestable.
    """

    def __init__(self, reply: list[Any] | None = None) -> None:
        self.scripts: dict[str, str] = {}
        self.calls: list[tuple[str, list[str], list[str]]] = []
        self.reply: list[Any] = lua_reply() if reply is None else reply
        self.raises: BaseException | None = None
        self.register_raises: BaseException | None = None

    def register(self, name: str, body: str) -> str:
        if self.register_raises is not None:
            raise self.register_raises
        self.scripts[name] = body
        return body

    def script(self, name: str) -> str:
        try:
            return self.scripts[name]
        except KeyError:
            raise KeyError(f"lua script {name!r} was never registered") from None

    async def run_script(self, name: str, keys: list[str], args: list[str]) -> list[Any]:
        # `list(...)` copies the CONTAINER, never the elements, so the identity assertion in
        # `test_the_tier_tail_is_the_snapshots_own_objects` still sees the snapshot's own strings.
        self.calls.append((name, list(keys), list(args)))
        if self.raises is not None:
            raise self.raises
        return self.reply

    @property
    def last(self) -> tuple[str, list[str], list[str]]:
        assert self.calls, "the limiter never reached the gateway"
        return self.calls[-1]


class StubTiers:
    """Only ``snapshot()``, returning a **real** :class:`~src.tiers._Snapshot`.

    A real snapshot rather than a hand-rolled stand-in, so ``argv_tail`` is genuinely the output of
    :func:`~src.tiers.render_argv_tail` — the thing the limiter is supposed to splice. A stub that
    returned a list of strings it made up would let a limiter that rebuilt the tail pass.

    :class:`~src.tiers.TierRegistry` itself is not used because its ``snapshot()`` schedules a
    background refresh against the gateway when the cache is stale (which it is, from birth), and
    these tests have no Redis for that task to reach.
    """

    def __init__(self, settings: Settings) -> None:
        self._snapshot = _build_snapshot(
            settings.tier_limits, version=7, fetched_monotonic=0.0
        )
        self.reads = 0

    def snapshot(self):  # noqa: ANN201 - the private _Snapshot type is not exported
        self.reads += 1
        return self._snapshot


def build(settings: Settings, **overrides: Any) -> tuple[Limiter, StubGateway, StubTiers]:
    """A limiter over stubs, with any :class:`~src.config.Settings` field overridden."""
    if overrides:
        settings = settings.model_copy(update=overrides)
    gateway = StubGateway()
    tiers = StubTiers(settings)
    return Limiter(gateway, tiers, settings), gateway, tiers  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------
# The constants this module and the script must agree on
# ---------------------------------------------------------------------------------------------


def test_the_tier_slot_width_agrees_with_the_registry_that_renders_it():
    """**A producer/consumer pair that cannot be imported from one another.**

    ``src.tiers.render_argv_tail`` emits five slots per tier; the Lua script indexes with
    ``ARGV[11 + (i - 1) * 5]``. The 5 on the Lua side is baked into the script's arithmetic and
    cannot be read from ``src.tiers`` at runtime, and ``src.lua`` cannot import ``src.tiers``
    without dragging the ``redis`` import into a module whose whole contract is purity.

    So the two are declared separately and pinned here. Their disagreement would not raise: the
    script would simply read the wrong ARGV slots and silently enforce a different tier's numbers.
    """
    assert TIER_ARGV_SLOTS == ARGV_SLOTS_PER_TIER


def test_the_call_shape_constants_are_contiguous_and_one_based():
    """The KEYS/ARGV indices are 1-based because Lua's tables are, and they must not have gaps.

    The tier table starts immediately after the count, which starts immediately after the head —
    a gap anywhere would leave the script reading an ARGV slot nobody fills.
    """
    assert (KEY_BUCKET, KEY_QUOTA_DAILY, KEY_QUOTA_MONTHLY, KEY_USER) == (1, 2, 3, 4)
    assert KEYS_ARITY == 4
    assert ARGV_HEAD_ARITY == 9
    assert ARGV_TIER_COUNT == ARGV_HEAD_ARITY + 1
    assert ARGV_TIER_TABLE == ARGV_TIER_COUNT + 1


# ---------------------------------------------------------------------------------------------
# KEYS
# ---------------------------------------------------------------------------------------------


async def test_keys_are_built_in_the_documented_order(settings: Settings):
    limiter, gateway, _ = build(settings)

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    name, keys, _args = gateway.last
    assert name == RLQ_CHECK_AND_CONSUME_NAME
    assert len(keys) == KEYS_ARITY
    assert keys[KEY_BUCKET - 1] == bucket_key(USER, ENDPOINT)
    assert keys[KEY_QUOTA_DAILY - 1] == daily_quota_key(USER, MOMENT)
    assert keys[KEY_QUOTA_MONTHLY - 1] == monthly_quota_key(USER, MOMENT)
    assert keys[KEY_USER - 1] == user_key(USER)


async def test_all_four_keys_share_one_hash_tag(settings: Settings):
    """**The property that makes one EVALSHA over four keys legal.**

    Redis Cluster computes a key's slot from the bytes between the first ``{`` and the following
    ``}``. All four keys — and the ``sw:{alice}:*`` keys the script derives from ARGV[3] — carry
    the identical tag, so they are provably one slot and the script is not a ``CROSSSLOT`` waiting
    to happen the day this is sharded.

    Asserted as "exactly one distinct tag", not "each contains ``{alice}``": the second form is
    satisfied by a key that contains the tag *and something else braced*, which is precisely the
    shape that changes which slot it lands in.
    """
    limiter, gateway, _ = build(settings)

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    _name, keys, args = gateway.last
    tags = {re.search(r"\{[^}]*\}", key).group(0) for key in keys}  # type: ignore[union-attr]
    assert tags == {hash_tag(USER)}
    # ARGV[3] is a key PREFIX rather than a key, and it has to carry the same tag or the two
    # sliding-window counters the script builds from it land in a different slot.
    assert args[ARGV_SW_PREFIX - 1] == sliding_window_prefix(USER)
    assert hash_tag(USER) in args[ARGV_SW_PREFIX - 1]


async def test_the_quota_keys_follow_the_supplied_calendar_instant(settings: Settings):
    """``now`` selects which period's counters are touched — key derivation is Python's job."""
    limiter, gateway, _ = build(settings)
    new_year = datetime(2027, 1, 1, 0, 0, 1, tzinfo=timezone.utc)

    await limiter.check(USER, ENDPOINT, 1, now=new_year)

    _name, keys, args = gateway.last
    assert keys[KEY_QUOTA_DAILY - 1].endswith(":2027-01-01")
    assert keys[KEY_QUOTA_MONTHLY - 1].endswith(":2027-01")
    assert args[ARGV_DAILY_EXPIRE_AT - 1] == str(day_expire_at(new_year))
    assert args[ARGV_MONTHLY_EXPIRE_AT - 1] == str(month_expire_at(new_year))


async def test_an_omitted_instant_defaults_to_now(settings: Settings):
    """The default path is the production one, so it has to be exercised rather than assumed."""
    limiter, gateway, _ = build(settings)
    today = datetime.now(timezone.utc)

    await limiter.check(USER, ENDPOINT, 1)

    _name, keys, _args = gateway.last
    assert keys[KEY_QUOTA_DAILY - 1] == daily_quota_key(USER, today)


# ---------------------------------------------------------------------------------------------
# ARGV
# ---------------------------------------------------------------------------------------------


async def test_the_argv_head_is_assembled_in_the_documented_order(settings: Settings):
    limiter, gateway, _ = build(settings)

    await limiter.check(USER, ENDPOINT, 5, now=MOMENT)

    _name, _keys, args = gateway.last
    assert args[ARGV_COST - 1] == "5"
    assert args[ARGV_BUCKET_TTL_MS - 1] == str(settings.bucket_ttl_sec * 1000)
    assert args[ARGV_SW_PREFIX - 1] == sliding_window_prefix(USER)
    assert args[ARGV_SW_WINDOW_MS - 1] == str(settings.sliding_window_sec * 1000)
    assert args[ARGV_SW_ENABLED - 1] == SW_ENABLED
    assert args[ARGV_DAILY_EXPIRE_AT - 1] == str(day_expire_at(MOMENT))
    assert args[ARGV_MONTHLY_EXPIRE_AT - 1] == str(month_expire_at(MOMENT))
    assert args[ARGV_DEFAULT_TIER - 1] == settings.default_tier
    assert args[ARGV_NOW_MS_OVERRIDE - 1] == NO_CLOCK_OVERRIDE
    # Every element on the wire is a string: redis-py encodes them anyway, and what a MONITOR trace
    # shows is then exactly what this test asserted.
    assert all(isinstance(value, str) for value in args)


async def test_the_tier_tail_is_the_snapshots_own_objects(settings: Settings):
    """**The test that proves the tier table is not rebuilt per request.**

    ``render_argv_tail`` is ~5 microseconds of string formatting. That is nothing — until it runs
    on every metered request at 1000 rps to produce a tuple that is identical every time. A limiter
    that rebuilt it would be *correct*, would pass every behavioural test in this project, and
    would simply be slower forever.

    Equality cannot catch that; object identity can. ``str(60)`` allocates a fresh object on each
    call, so if these elements are the same objects the snapshot holds, they were spliced rather
    than re-rendered.
    """
    limiter, gateway, tiers = build(settings)
    tail = tiers.snapshot().argv_tail

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    _name, _keys, args = gateway.last
    sent = args[ARGV_TIER_COUNT - 1 :]
    assert len(args) == ARGV_HEAD_ARITY + len(tail)
    assert tuple(sent) == tail
    assert all(a is b for a, b in zip(sent, tail, strict=True)), (
        "the tier tail was rebuilt per request instead of spliced from the snapshot"
    )
    # And its documented shape: a count, then five slots per tier, sorted by name.
    assert sent[0] == str(len(settings.tier_limits))
    assert len(sent) == 1 + len(settings.tier_limits) * TIER_ARGV_SLOTS
    assert args[ARGV_TIER_TABLE - 1] == sorted(settings.tier_limits)[0]


async def test_the_sliding_window_flag_follows_the_setting(settings: Settings):
    limiter, gateway, _ = build(settings, sliding_window_enabled=False)

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert gateway.last[2][ARGV_SW_ENABLED - 1] == SW_DISABLED


async def test_a_disabled_quota_period_is_sent_as_the_unenforced_sentinel(settings: Settings):
    """``QUOTA_*_ENABLED=false`` is encoded as an ``EXPIREAT`` of 0 — a period with no boundary.

    The alternative would be a tenth ARGV flag per period, which grows the fixed head for a switch
    almost nobody flips. The script reads a non-positive expiry as "do not read this counter, do
    not increment it, report limit 0", and ``LimitDecision`` already renders a limit of 0 as
    ``UNLIMITED`` and already suppresses the ``X-Quota-*`` headers for it.
    """
    limiter, gateway, _ = build(
        settings, quota_daily_enabled=False, quota_monthly_enabled=False
    )

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    args = gateway.last[2]
    assert args[ARGV_DAILY_EXPIRE_AT - 1] == str(UNENFORCED_PERIOD)
    assert args[ARGV_MONTHLY_EXPIRE_AT - 1] == str(UNENFORCED_PERIOD)


# ---------------------------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("cost", [0, -1, -100])
async def test_a_cost_below_one_is_refused(settings: Settings, cost: int):
    """A zero-cost request is an unmetered request wearing a metered one's clothes."""
    limiter, gateway, _ = build(settings)

    with pytest.raises(ValueError, match="cost must be >= 1"):
        await limiter.check(USER, ENDPOINT, cost, now=MOMENT)

    assert gateway.calls == [], "a refused request must not reach Redis"


async def test_an_unusable_user_id_is_refused_before_any_key_is_built(settings: Settings):
    """A brace in the id can forge or collide with another principal's key slot — see `src.keys`."""
    limiter, gateway, _ = build(settings)

    with pytest.raises(ValueError, match="brace"):
        await limiter.check("alice}:x:{bob", ENDPOINT, 1, now=MOMENT)

    assert gateway.calls == []


@pytest.mark.parametrize(
    ("moment", "collides_with"),
    [
        # day_expire_at(...) == 0 for any instant inside 1969-12-31.
        (datetime(1969, 12, 31, 12, 0, tzinfo=timezone.utc), "daily"),
        # month_expire_at(...) == 0 for any instant in 1969-12; its daily boundary is negative.
        (datetime(1969, 12, 3, 9, 30, tzinfo=timezone.utc), "monthly"),
        (datetime(1900, 1, 1, tzinfo=timezone.utc), "both"),
    ],
    ids=["day-boundary-is-zero", "month-boundary-is-zero", "long-before-the-epoch"],
)
async def test_a_pre_epoch_instant_is_refused_rather_than_disabling_quota_enforcement(
    settings: Settings, moment: datetime, collides_with: str
):
    """**A sentinel a legitimate-looking input can impersonate has to be defended at the boundary.**

    ``0`` is how "this quota period is switched off" is encoded in ARGV[6]/ARGV[7] — and it is also
    a real instant. ``day_expire_at`` returns exactly 0 for any moment inside 1969-12-31 and
    ``month_expire_at`` returns 0 for any moment in 1969-12, so a caller-supplied ``now`` from
    before the epoch would reach the script wearing the sentinel's clothes and **silently switch
    quota enforcement off** for that request.

    Nothing in production produces one (the default is ``datetime.now``), which is exactly why it
    would never be noticed: the request would simply be allowed, with correct-looking headers.
    """
    limiter, gateway, _ = build(settings)

    with pytest.raises(ValueError, match="sentinel"):
        await limiter.check(USER, ENDPOINT, 1, now=moment)

    assert collides_with  # the id names which boundary this case is about
    assert gateway.calls == []


async def test_the_epoch_itself_is_accepted(settings: Settings):
    """The guard is ``<= 0``, so the first instant that yields a positive boundary must pass.

    Rejecting one second too much would be its own bug, and a boundary check that has never been
    exercised from the legal side is a boundary check nobody has actually located.
    """
    limiter, gateway, _ = build(settings)

    await limiter.check(USER, ENDPOINT, 1, now=datetime(1970, 1, 1, tzinfo=timezone.utc))

    args = gateway.last[2]
    assert args[ARGV_DAILY_EXPIRE_AT - 1] == "86400"  # 1970-01-02T00:00:00Z
    assert args[ARGV_MONTHLY_EXPIRE_AT - 1] == "2678400"  # 1970-02-01T00:00:00Z


async def test_a_pre_epoch_instant_is_refused_even_when_both_quota_gates_are_off(
    settings: Settings,
):
    """The guard is a statement about the INPUT, not about which gates happen to be enabled.

    It therefore runs before the enabled flags are consulted. Gating it behind
    ``quota_daily_enabled`` would make a bad argument silently acceptable in exactly the
    configuration where the resulting ARGV is indistinguishable from the intended one.
    """
    limiter, gateway, _ = build(
        settings, quota_daily_enabled=False, quota_monthly_enabled=False
    )

    with pytest.raises(ValueError, match="sentinel"):
        await limiter.check(USER, ENDPOINT, 1, now=datetime(1969, 6, 1, tzinfo=timezone.utc))

    assert gateway.calls == []


# ---------------------------------------------------------------------------------------------
# The clock-override seam
# ---------------------------------------------------------------------------------------------


async def test_an_override_is_REFUSED_when_the_setting_is_off(settings: Settings):
    """**Refused, not ignored** — and the difference is the whole point of the seam.

    A caller-supplied clock lets a request refill its own bucket, which is the vulnerability this
    design removed by reading ``redis.call('TIME')`` inside the script. So the seam is inert unless
    a deployment opts in.

    Ignoring the argument would be the dangerous kind of safe: a test that believed it was driving
    a frozen clock would silently run against the real one and pass for reasons unrelated to what
    it asserted. An exception is a bug report; a dropped parameter is a false negative.
    """
    assert settings.allow_clock_override is False, "the shipped default must be off"
    limiter, gateway, _ = build(settings)

    with pytest.raises(RuntimeError, match="ALLOW_CLOCK_OVERRIDE"):
        await limiter.check(USER, ENDPOINT, 1, now=MOMENT, now_ms_override=1_700_000_000_000)

    assert gateway.calls == []


async def test_an_override_reaches_argv_9_when_the_setting_is_on(settings: Settings):
    limiter, gateway, _ = build(settings, allow_clock_override=True)

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT, now_ms_override=1_700_000_000_000)

    assert gateway.last[2][ARGV_NOW_MS_OVERRIDE - 1] == "1700000000000"


async def test_production_sends_the_inert_override(settings: Settings):
    """With the seam enabled but unused, ARGV[9] is still ``"0"`` — i.e. "use the server's TIME"."""
    limiter, gateway, _ = build(settings, allow_clock_override=True)

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert gateway.last[2][ARGV_NOW_MS_OVERRIDE - 1] == NO_CLOCK_OVERRIDE == "0"


# ---------------------------------------------------------------------------------------------
# Failure handling — what this class deliberately does NOT do
# ---------------------------------------------------------------------------------------------


async def test_a_store_outage_is_absorbed_here_and_nowhere_else(settings: Settings):
    """**Changed at C8, and the change is the commit.** This used to assert propagation.

    Until C7 this test read ``pytest.raises(BackingStoreUnavailable)`` and existed so that no
    commit before C8 would quietly choose a fail-open policy in the wrong module. C8 is the module
    that owns ``FAIL_MODE``, so the handler lands here — and the assertion inverts: ``check()`` now
    **never** raises that type, and the middleware adds no second handler.

    What is asserted is deliberately the *shape* of the answer (a degraded decision came back
    instead of an exception). The policy itself — the fallback bucket's capacity, the fail-closed
    denial, the ``X-Quota-*`` suppression — is `tests/unit/test_degradation.py`'s subject, where it
    is stated once rather than half-stated in two files.
    """
    limiter, gateway, _ = build(settings)
    gateway.raises = BackingStoreUnavailable("script:rlq failed", op="script:rlq")

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert verdict.degraded is True
    assert limiter.degraded is True


async def test_a_malformed_reply_surfaces_as_a_ValueError(settings: Settings):
    """A short reply means the script and the decoder disagree about the contract.

    Building a decision out of whatever landed in the right slots would produce a confident, wrong
    answer — a caller allowed because a quota counter happened to sit in the ``allowed`` position.
    """
    limiter, gateway, _ = build(settings)
    gateway.reply = lua_reply()[:-1]

    with pytest.raises(ValueError, match=f"exactly {LUA_REPLY_ARITY} elements"):
        await limiter.check(USER, ENDPOINT, 1, now=MOMENT)


async def test_an_unknown_enum_in_the_reply_is_a_ValueError(settings: Settings):
    limiter, gateway, _ = build(settings)
    gateway.reply = lua_reply(reason=b"because")

    with pytest.raises(ValueError, match="unknown DenyReason"):
        await limiter.check(USER, ENDPOINT, 1, now=MOMENT)


# ---------------------------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------------------------


def test_a_connected_gateway_is_registered_at_construction(settings: Settings):
    _limiter, gateway, _ = build(settings)

    assert gateway.scripts[RLQ_CHECK_AND_CONSUME_NAME] == RLQ_CHECK_AND_CONSUME


def test_an_unconnected_gateway_does_not_fail_construction(settings: Settings):
    """``Runtime.build`` is synchronous and I/O-free, so this is the production path.

    The real gateway raises ``RuntimeError`` from ``register`` until ``connect()`` has been
    awaited. Failing here would make the whole app un-constructible before startup and would break
    the ``create_app(runtime=...)`` seam every hermetic test in this suite depends on.
    """
    gateway = StubGateway()
    gateway.register_raises = RuntimeError("RedisGateway.connect() must be awaited before use")

    limiter = Limiter(gateway, StubTiers(settings), settings)  # type: ignore[arg-type]

    assert gateway.scripts == {}
    assert limiter.checks == 0


async def test_a_handle_dropped_by_a_reconnect_is_re_registered_on_the_next_check(
    settings: Settings,
):
    """``RedisGateway.aclose`` drops every handle on purpose, so re-registration must be on demand.

    Registering once in ``__init__`` and assuming it holds would make a reconnected gateway raise
    ``KeyError`` on every subsequent request — a limiter that stops working after a Redis blip and
    only recovers on a process restart.
    """
    limiter, gateway, _ = build(settings)
    gateway.scripts.clear()

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert gateway.scripts[RLQ_CHECK_AND_CONSUME_NAME] == RLQ_CHECK_AND_CONSUME
    assert len(gateway.calls) == 1


# ---------------------------------------------------------------------------------------------
# The decoded decision
# ---------------------------------------------------------------------------------------------


async def test_the_decision_carries_the_inputs_the_script_was_never_told_to_echo(
    settings: Settings,
):
    """``user_id``, ``endpoint`` and ``cost`` are inputs, so they are re-attached here.

    Echoing them back through Redis would be bytes on the wire, per request, to tell us something
    we already knew.
    """
    limiter, _gateway, _ = build(settings)

    decision = await limiter.check(USER, ENDPOINT, 5, now=MOMENT)

    assert decision.user_id == USER
    assert decision.endpoint == ENDPOINT
    assert decision.cost == 5
    assert decision.allowed is True
    assert decision.reason is DenyReason.NONE
    assert decision.tier == "free"
    assert decision.daily_state is QuotaPeriodState.RESET
    assert decision.degraded is False


async def test_latency_is_measured_and_attached(settings: Settings):
    """The one observation on the decision, and the only field that is not a decision."""
    limiter, _gateway, _ = build(settings)

    decision = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert decision.latency_ms >= 0.0
    # A stubbed round trip cannot plausibly take a second; a wall-clock source that stepped
    # mid-request could report anything at all, which is why this is `perf_counter`.
    assert decision.latency_ms < 1000.0


async def test_a_denial_never_advertises_a_zero_retry(settings: Settings):
    """``Retry-After: 0`` is a retry storm the limiter manufactured itself."""
    limiter, gateway, _ = build(settings)
    gateway.reply = lua_reply(allowed=0, reason=b"rate_limit", retry_ms=0)

    decision = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert decision.allowed is False
    assert decision.retry_after_sec >= 1


async def test_the_check_counter_moves(settings: Settings):
    """A limiter that has served a million requests and one that has served none look identical."""
    limiter, _gateway, _ = build(settings)

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)
    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert limiter.checks == 2


async def test_the_snapshot_is_read_exactly_once_per_check(settings: Settings):
    """Two reads would mean two potentially different tier tables inside one decision."""
    limiter, _gateway, tiers = build(settings)
    before = tiers.reads

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert tiers.reads == before + 1
