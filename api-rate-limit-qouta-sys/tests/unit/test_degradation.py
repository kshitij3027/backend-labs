"""What the enforcement layer does when Redis cannot answer — the C8 policy, asserted.

``tests/unit/test_fallback.py`` proves the local bucket's arithmetic. This file proves the
*decisions* built on top of it: which failure degrades, which refuses, what a caller is told, and —
the assertion that protects the whole design — which failures are **not** allowed to degrade at
all.

.. rubric:: The most important test in this file is the one about ``ResponseError``

:func:`test_a_response_error_still_propagates_and_is_never_laundered_into_a_degradation` is the
guard on C2's availability/correctness split. A ``ResponseError`` means the store answered and the
answer was "your command is wrong" — a broken Lua script, a key-schema bug, a bad password. If that
were classified as an outage then under the shipped ``FAIL_MODE=open`` a **one-character typo in
the decision script would silently disable rate limiting for every request**, and ``/health`` would
report it identically to an unplugged Redis. The bug would be invisible for exactly as long as
nobody read the logs. It has to become a 500.

.. rubric:: Stubs, not fakeredis

The subject is the limiter's exception handling and the middleware's rendering of the result, so
the failures are *injected* rather than provoked. A stub gateway can raise a ``ReadOnlyError`` on
demand; a real Redis cannot be made to fail over on cue, and ``fakeredis`` is a reimplementation
whose error taxonomy is an approximation — which is the wrong oracle for a test whose whole subject
is which exception class means what. The end-to-end behaviour against a genuinely unreachable
server is ``tests/integration/test_degradation.py``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
import redis.exceptions

from src.config import Settings
from src.fallback import LocalBucketCache
from src.keys import (
    ROUTE_TABLE,
    UNKNOWN_ENDPOINT_LABEL,
    bucket_key,
    sliding_window_prefix,
)
from src.limiter import Limiter
from src.models import (
    DEGRADED_HEADER,
    LUA_REPLY_FIELDS,
    QUOTA_LIMIT_HEADER,
    QUOTA_REMAINING_HEADER,
    QUOTA_RESET_HEADER,
    RATELIMIT_LIMIT_HEADER,
    RATELIMIT_REMAINING_HEADER,
    DenyReason,
    QuotaPeriodState,
)
from src.redis_client import (
    BackingStoreUnavailable,
    BreakerState,
    CircuitBreaker,
    RedisGateway,
)
from src.tiers import _build_snapshot

USER = "alice"
ENDPOINT = "GET:/api/v1/whoami"
MOMENT = datetime(2026, 8, 10, 13, 45, 30, tzinfo=timezone.utc)

#: **Every label the shipped route table can produce**, taken from :data:`~src.keys.ROUTE_TABLE`
#: rather than written out, so a route added in a later commit joins these assertions by itself.
#:
#: Driving all of them is the whole point of the multi-endpoint tests below. A degraded path that
#: reproduces only the per-``(user, endpoint)`` bucket gives each of these its own allowance, so
#: the caller's real ceiling becomes ``len(LABELS) x share`` — a 5x overspend that is completely
#: invisible to a test using one label, which is how it survived a commit.
LABELS: tuple[str, ...] = tuple(route.label for route in ROUTE_TABLE) + (
    UNKNOWN_ENDPOINT_LABEL,
)

#: The three quota headers, which the degraded path must omit **entirely**.
QUOTA_HEADERS = (QUOTA_LIMIT_HEADER, QUOTA_REMAINING_HEADER, QUOTA_RESET_HEADER)


# =============================================================================================
# Doubles
# =============================================================================================


def lua_reply(**overrides: Any) -> list[Any]:
    """A well-formed 19-element reply, projected through :data:`~src.models.LUA_REPLY_FIELDS`.

    Ordered by the field tuple rather than written as a literal list, so it cannot silently start
    asserting about the wrong slots the day an element moves.
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


class FlakyGateway:
    """A gateway whose script call fails, succeeds, or alternates — under the test's control.

    ``script_calls`` is what proves the breaker actually removed a round trip: from outside, a
    short-circuited refusal and a refused connection produce the same exception.
    """

    def __init__(self, *, raises: BaseException | None = None) -> None:
        self.scripts: dict[str, str] = {}
        self.raises = raises
        self.reply: list[Any] = lua_reply()
        self.script_calls = 0

    def register(self, name: str, body: str) -> str:
        self.scripts[name] = body
        return body

    def script(self, name: str) -> str:
        try:
            return self.scripts[name]
        except KeyError:
            raise KeyError(f"lua script {name!r} was never registered") from None

    async def run_script(self, name: str, keys: list[str], args: list[str]) -> list[Any]:
        self.script_calls += 1
        if self.raises is not None:
            raise self.raises
        return self.reply


class StubTiers:
    """Only ``snapshot()``, returning a **real** :class:`~src.tiers._Snapshot`.

    A real snapshot so the degraded path reads the same object shape the healthy path does — the
    fallback tier is looked up in ``snapshot().tiers``, and a hand-rolled stand-in would let a
    lookup against the wrong structure pass.
    """

    def __init__(self, settings: Settings) -> None:
        self._snapshot = _build_snapshot(settings.tier_limits, version=7, fetched_monotonic=0.0)

    def snapshot(self):  # noqa: ANN201 - the private _Snapshot type is not exported
        return self._snapshot


class FakeClock:
    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def build(
    settings: Settings, *, raises: BaseException | None = None, **overrides: Any
) -> tuple[Limiter, FlakyGateway]:
    """A real :class:`~src.limiter.Limiter` over a controllable gateway."""
    if overrides:
        settings = settings.model_copy(update=overrides)
    gateway = FlakyGateway(raises=raises)
    limiter = Limiter(gateway, StubTiers(settings), settings)  # type: ignore[arg-type]
    return limiter, gateway


def outage() -> BackingStoreUnavailable:
    return BackingStoreUnavailable("script:rlq_check_and_consume failed", op="script:rlq")


@pytest.fixture()
async def gateway(settings: Settings):
    """A **connected** :class:`~src.redis_client.RedisGateway` that never opens a socket.

    ``connect()`` is deliberately lazy — redis-py builds the pool and dials nothing until the first
    command — so a real gateway is available to these unit tests without a server. The two tests
    that use it inject their failure as the operation itself, so no command is ever issued.
    """
    instance = RedisGateway(settings)
    await instance.connect()
    try:
        yield instance
    finally:
        await instance.aclose()


# =============================================================================================
# 1. FAIL_MODE=open — the fallback carries the request
# =============================================================================================


async def test_a_redis_failure_produces_a_degraded_decision(settings: Settings):
    """The spec's graceful degradation: the request is served, and it says so.

    ``degraded=True`` is what makes this non-silent. A fail-open with no marker is
    indistinguishable from having no rate limiter at all — the same graph, the same latency, the
    same 200s, and no way for an operator or a caller to tell that nothing is being enforced.
    """
    limiter, _gateway = build(settings, raises=outage())

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert verdict.allowed is True
    assert verdict.degraded is True
    assert verdict.reason is DenyReason.NONE
    assert verdict.headers()[DEGRADED_HEADER] == "1"
    assert limiter.degraded is True
    assert limiter.degraded_checks == 1


@pytest.mark.parametrize(("replicas", "expected"), [(1, 60), (2, 30), (4, 15)])
async def test_the_degraded_total_across_all_endpoints_is_the_replicas_share(
    settings: Settings, replicas: int, expected: int
):
    """**The test that would have caught the 5x, and the reason this file now drives five labels.**

    Every other degradation assertion here uses ONE endpoint, which is exactly why a fallback that
    reproduced only the per-``(user, endpoint)`` bucket looked correct for a whole commit: on a
    single label, ``bucket == the limit``. Spread the same principal's traffic across the shipped
    route table and the omission is immediate — five labels, five independent 30-token buckets, 150
    units per replica, 300 across two, against a tier that says 60.

    The account-wide gate is keyed on the user alone, so it does not know what an endpoint is and
    cannot be multiplied by them. The number below is ``ceil(rpm / API_REPLICAS)`` regardless of
    how the traffic is spread — which is what `SLIDING_WINDOW_SEC` in `src.config` promises the
    account-wide gate means: "free tier limited after ~60 req/min" is a fact about the caller, not
    about each route they call.

    Driven round-robin rather than label-by-label so the account gate has to bind *interleaved*
    traffic; a sequential drive would let the first label exhaust it and hide whether the others
    were being checked at all.
    """
    limiter, _gateway = build(settings, raises=outage(), api_replicas=replicas)

    allowed = 0
    for index in range(400):
        verdict = await limiter.check(USER, LABELS[index % len(LABELS)], 1, now=MOMENT)
        allowed += verdict.allowed

    assert allowed == expected
    # Stated as the arithmetic rather than only as a literal, so the intent survives a tier change.
    assert expected == -(-settings.tier_limits["free"].rate_limit_per_min // replicas)


async def test_spreading_traffic_across_endpoints_buys_no_extra_allowance(settings: Settings):
    """The same total whether a caller uses one endpoint or all five. That IS the account gate.

    Asserted as an equality between two runs rather than against a constant, because the property
    is a *relation*: whatever the degraded ceiling is, moving traffic sideways across the route
    table must not raise it. A regression that reintroduced per-endpoint allowances would make the
    second number a multiple of the first, and no single-label test could see it.
    """
    one_label, _ = build(settings, raises=outage(), api_replicas=2)
    all_labels, _ = build(settings, raises=outage(), api_replicas=2)

    concentrated = 0
    for _ in range(400):
        concentrated += (await one_label.check(USER, LABELS[0], 1, now=MOMENT)).allowed

    spread = 0
    for index in range(400):
        spread += (await all_labels.check(USER, LABELS[index % len(LABELS)], 1, now=MOMENT)).allowed

    assert concentrated == spread == 30


async def test_the_advertised_limit_is_the_account_wide_number_actually_enforced(
    settings: Settings,
):
    """``X-RateLimit-Limit`` must name the ceiling that binds, not a per-endpoint slice of it.

    The header previously carried the per-endpoint bucket's capacity while nothing enforced an
    account-wide ceiling at all — a claim the code did not keep, and precisely the fabricated
    header the quota rubric refuses to emit. It now comes from the account gate, so a client pacing
    off it is pacing off the number that will actually refuse them.

    ``Remaining`` is checked from a *second* endpoint on purpose: it is
    ``min(bucket_remaining, window_limit - window_used)``, so a fresh label with a full bucket is
    the case where reporting the bucket alone would advertise 30 immediately before a refusal.
    """
    limiter, _gateway = build(settings, raises=outage(), api_replicas=2)

    for _ in range(25):
        await limiter.check(USER, LABELS[0], 1, now=MOMENT)

    # A label this caller has never touched: its own bucket is untouched and full at 30.
    verdict = await limiter.check(USER, LABELS[2], 1, now=MOMENT)
    headers = verdict.headers()

    assert headers[RATELIMIT_LIMIT_HEADER] == "30"
    assert verdict.bucket_remaining == 29        # the fresh endpoint bucket, barely used
    assert headers[RATELIMIT_REMAINING_HEADER] == "4"  # ...but only 4 of the account's 30 are left
    assert verdict.window_limit == 30
    assert verdict.window_used == 26


async def test_the_account_gate_names_itself_when_it_is_the_one_refusing(settings: Settings):
    """A caller blocked by their overall rate is told ``sliding_window``, not ``rate_limit``.

    The distinction is the client's, not ours: ``rate_limit`` points at an endpoint they could
    switch away from, and ``sliding_window`` says the account is the thing that is saturated. The
    script names the gate that refused and so does the degraded path, so a degraded 429 and a
    healthy one label the same situation the same way.
    """
    limiter, _gateway = build(settings, raises=outage(), api_replicas=2)

    for _ in range(30):
        await limiter.check(USER, LABELS[0], 1, now=MOMENT)

    # Fresh label, full bucket — so the only gate that can refuse is the account-wide one.
    verdict = await limiter.check(USER, LABELS[1], 1, now=MOMENT)

    assert verdict.allowed is False
    assert verdict.reason is DenyReason.SLIDING_WINDOW
    # ...while a caller who exhausted a single endpoint's bucket first is told the bucket refused,
    # which is the tie the script resolves to the earlier gate.
    drained, _ = build(settings, raises=outage(), api_replicas=2)
    for _ in range(30):
        await drained.check(USER, LABELS[0], 1, now=MOMENT)
    assert (await drained.check(USER, LABELS[0], 1, now=MOMENT)).reason is DenyReason.RATE_LIMIT


async def test_switching_the_account_gate_off_leaves_the_bucket_as_the_advertised_limit(
    settings: Settings,
):
    """``SLIDING_WINDOW_ENABLED=false`` removes the gate from BOTH modes, so degraded matches healthy.

    With the account-wide gate switched off there is no account-wide ceiling to enforce or to
    report, in either mode — so the per-endpoint bucket is the binding limit and is what
    ``X-RateLimit-Limit`` names. Reporting a number nothing checks would be the same fabrication
    the fix removed, arriving through the operability switch.
    """
    limiter, _gateway = build(
        settings, raises=outage(), api_replicas=2, sliding_window_enabled=False
    )

    allowed = 0
    for index in range(400):
        allowed += (await limiter.check(USER, LABELS[index % len(LABELS)], 1, now=MOMENT)).allowed

    # Five labels x 30, because the operator asked for exactly that by turning the gate off.
    assert allowed == 150
    verdict = await limiter.check(USER, LABELS[0], 1, now=MOMENT)
    assert verdict.headers()[RATELIMIT_LIMIT_HEADER] == "30"
    assert verdict.window_limit == verdict.bucket_limit == 30


async def test_a_degraded_denial_spends_nothing_from_the_endpoint_bucket(settings: Settings):
    """A retry loop refused by the account gate must not drain the buckets it is not refused by.

    The script's "a denial writes nothing" rule, at the level where it is now observable: without
    it, a client hammering ``/whoami`` while account-blocked would empty that endpoint's bucket
    too, and would still be refused there after the account gate had recovered — a refusal that
    outlived its own cause.
    """
    limiter, _gateway = build(settings, raises=outage(), api_replicas=2)

    for _ in range(30):
        await limiter.check(USER, LABELS[0], 1, now=MOMENT)

    for _ in range(20):
        refused = await limiter.check(USER, LABELS[1], 1, now=MOMENT)
        assert refused.allowed is False
        # Untouched at its full share throughout: nothing was taken for a request never served.
        assert refused.bucket_remaining == 30


async def test_the_fallback_bucket_holds_the_replicas_share_and_not_the_tiers(
    settings: Settings,
):
    """**The N-times overspend, asserted through the limiter rather than the cache.**

    The free tier is 60/60. With ``API_REPLICAS=2`` this replica must admit 30 and then refuse:
    two replicas each admitting 60 is 120, which is precisely the double-spend C12's distributed
    test exists to catch. Reproducing it in the degraded mode would make degradation worse than the
    outage it degrades from.
    """
    limiter, _gateway = build(settings, raises=outage(), api_replicas=2)

    allowed = 0
    for _ in range(80):
        verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)
        allowed += verdict.allowed

    assert allowed == 30
    assert settings.tier_limits["free"].burst == 60


async def test_a_single_replica_deployment_keeps_the_whole_tier_capacity(settings: Settings):
    """No division to do, so none is done — 60 stays 60 rather than being halved by a default."""
    limiter, _gateway = build(settings, raises=outage(), api_replicas=1)

    allowed = 0
    for _ in range(80):
        allowed += (await limiter.check(USER, ENDPOINT, 1, now=MOMENT)).allowed

    assert allowed == 60


async def test_a_drained_fallback_bucket_refuses_with_a_429_shaped_decision(
    settings: Settings,
):
    """A refusal from the local bucket is a **rate-limit** refusal, not a "we could not decide".

    The distinction is what the middleware keys its status code off: `RATE_LIMIT` is a 429 with the
    spec's body, while `BACKING_STORE` is a 503. We *did* decide here — the caller is genuinely
    over the (reduced) limit this replica is enforcing — so telling them 503 would be wrong in the
    other direction.
    """
    limiter, _gateway = build(settings, raises=outage(), api_replicas=2)
    for _ in range(30):
        await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert verdict.allowed is False
    assert verdict.reason is DenyReason.RATE_LIMIT
    assert verdict.degraded is True
    assert verdict.retry_after_sec >= 1
    assert verdict.error_title == "Rate limit exceeded"


async def test_the_degraded_decision_advertises_the_limit_it_is_actually_enforcing(
    settings: Settings,
):
    """`X-RateLimit-Limit` must be the local ceiling, not the tier's unreachable one.

    Reporting 60 next to a `Remaining` drawn from a 30-token bucket would advertise headroom the
    caller can never reach and would make a well-behaved client pace itself off fiction — the same
    failure `effective_remaining` exists to prevent, arriving through the degraded path.
    """
    limiter, _gateway = build(settings, raises=outage(), api_replicas=2)

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)
    headers = verdict.headers()

    assert headers[RATELIMIT_LIMIT_HEADER] == "30"
    assert headers[RATELIMIT_REMAINING_HEADER] == "29"


async def test_every_quota_header_is_omitted_while_degraded(settings: Settings):
    """**No counter was consulted, so no number is published.**

    There is no local approximation of a cumulative cross-replica counter that is not a
    fabrication: this process does not know what the other replicas admitted, what it admitted
    before it restarted, or what was spent before the outage began. A client can detect a *missing*
    header and fall back to its own accounting; it cannot detect a *wrong* one, and will build a
    usage display or a spend alarm on top of it.
    """
    limiter, _gateway = build(settings, raises=outage())

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert not [name for name in verdict.headers() if name in QUOTA_HEADERS]
    # ...and the body agrees rather than contradicting the headers: both periods report a ceiling
    # that does not exist, not one that is exhausted.
    assert verdict.daily_state is QuotaPeriodState.UNENFORCED
    assert verdict.monthly_state is QuotaPeriodState.UNENFORCED
    assert verdict.daily_remaining == -1
    assert verdict.monthly_remaining == -1


async def test_both_fallback_gates_are_keyed_the_way_the_shared_ones_are(settings: Settings):
    """One caller, the same two key names — whichever store is holding them.

    Sharing the key strings means a degraded reading is comparable to a healthy one in a log line.
    The pair is what this asserts: ``rate_limit:{alice}:GET:...`` for the per-endpoint burst gate
    and ``sw:{alice}`` for the account-wide one, exactly the names the decision script builds from.
    They are also in *different namespaces*, which is what lets a single ``OrderedDict`` hold both
    key spaces under one cap without them ever colliding.

    A second endpoint adds a second bucket and **no** second account gate — that asymmetry is the
    fix for the endpoint multiplication, expressed as a data structure.
    """
    fallback = LocalBucketCache(settings)
    gateway = FlakyGateway(raises=outage())
    limiter = Limiter(
        gateway, StubTiers(settings), settings, fallback=fallback  # type: ignore[arg-type]
    )

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert bucket_key(USER, ENDPOINT) in fallback
    assert sliding_window_prefix(USER) in fallback
    assert len(fallback) == 2

    await limiter.check(USER, LABELS[0], 1, now=MOMENT)

    # Three entries, not four: one more bucket, and the SAME account gate.
    assert len(fallback) == 3
    assert bucket_key(USER, LABELS[0]) in fallback


async def test_the_degraded_decision_uses_the_default_tier(settings: Settings):
    """`user -> tier` lives in Redis, so during an outage the caller's tier is unknowable.

    `DEFAULT_TIER` is the most restrictive tier, and guessing upward would hand an unknown caller
    the best plan in the system during exactly the window in which nothing can check. What a tier
    *means* is still the operator's runtime value: the snapshot serves its last good table through
    the outage.
    """
    limiter, _gateway = build(settings, raises=outage())

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert verdict.tier == settings.default_tier == "free"


# =============================================================================================
# 2. FAIL_MODE=closed — refuse, and say why
# =============================================================================================


async def test_fail_mode_closed_denies_with_the_backing_store_reason(settings: Settings):
    """For deployments where the limit IS the security control, an unmeterable request is refused.

    `BACKING_STORE` is the reason the decision script cannot produce; the middleware renders it as
    a **503**, never a 429, because 429 is a claim about the caller's behaviour and this caller has
    done nothing measurable.
    """
    limiter, _gateway = build(settings, raises=outage(), fail_mode="closed")

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert verdict.allowed is False
    assert verdict.reason is DenyReason.BACKING_STORE
    assert verdict.degraded is True
    assert limiter.fail_closed_denials == 1
    assert limiter.degraded_checks == 0


async def test_fail_mode_closed_publishes_no_allowance_at_all(settings: Settings):
    """Nothing was evaluated, so nothing is reported — the same rule as the 401 path.

    Every rate and quota quantity is zero and the middleware emits no `X-RateLimit-Limit` or
    `Remaining` on the 503 it builds from this. A fabricated allowance on a request that was never
    measured is a lie a client cannot detect.
    """
    limiter, _gateway = build(settings, raises=outage(), fail_mode="closed")

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert (verdict.bucket_limit, verdict.bucket_remaining) == (0, 0)
    assert (verdict.window_limit, verdict.window_used) == (0, 0)
    assert not [name for name in verdict.headers() if name in QUOTA_HEADERS]


async def test_fail_mode_closed_advertises_the_breaker_cooldown_as_the_retry(
    settings: Settings,
):
    """The honest number: it is exactly how long before this process next learns anything.

    Telling a caller to return sooner is telling them to be refused again, which converts a
    degradation into a retry storm against a service that is already struggling.
    """
    limiter, _gateway = build(
        settings, raises=outage(), fail_mode="closed", breaker_cooldown_sec=9
    )

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert verdict.retry_after_sec == 9
    assert verdict.headers()["Retry-After"] == "9"


async def test_fail_mode_closed_never_touches_the_fallback_bucket(settings: Settings):
    """Fail-closed means no local allowance is spent, so no local state is created either."""
    fallback = LocalBucketCache(settings)
    gateway = FlakyGateway(raises=outage())
    limiter = Limiter(
        gateway,
        StubTiers(settings.model_copy(update={"fail_mode": "closed"})),
        settings.model_copy(update={"fail_mode": "closed"}),
        fallback=fallback,  # type: ignore[arg-type]
    )

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert len(fallback) == 0
    assert fallback.allows == 0


# =============================================================================================
# 3. The breaker — a degradation that costs nothing
# =============================================================================================


async def test_an_open_breaker_skips_redis_entirely(gateway: RedisGateway, settings: Settings):
    """**The call counter is the assertion**, and nothing else can make it.

    From outside, "the socket was refused" and "the breaker refused without dialling" produce the
    same degraded decision. The difference is 250 ms per request against a dead store, times every
    in-flight request, each one holding a pooled connection — which is how a Redis incident becomes
    a latency incident on a service that is supposedly degrading gracefully.
    """
    breaker = gateway.breaker
    for _ in range(settings.breaker_failures):
        breaker.record_failure()
    assert breaker.state is BreakerState.OPEN

    calls = 0

    async def counted() -> None:
        nonlocal calls
        calls += 1

    for _ in range(20):
        with pytest.raises(BackingStoreUnavailable, match="circuit breaker is open"):
            await gateway.run(counted, op="probe")

    assert calls == 0
    assert gateway.short_circuits == 20


async def test_a_half_open_probe_restores_real_enforcement(settings: Settings):
    """Recovery has to be a thing that can happen, and exactly one request may test it.

    Letting the whole backlog through at the cooldown boundary is the thundering herd the breaker
    exists to prevent, re-armed on a timer — so the second caller is still refused until the probe
    reports back, and a successful probe closes the breaker for everyone.
    """
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=2, cooldown_sec=5, clock=clock)
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state is BreakerState.OPEN

    assert breaker.allow_request() is False
    clock.advance(5)

    assert breaker.allow_request() is True          # the single probe
    assert breaker.state is BreakerState.HALF_OPEN
    assert breaker.allow_request() is False         # everybody else still waits

    breaker.record_success()
    assert breaker.state is BreakerState.CLOSED
    assert breaker.allow_request() is True


async def test_recovery_clears_the_degraded_flag(settings: Settings):
    """One real decision from Redis, and the replica stops reporting itself degraded.

    Cleared by the *limiter* seeing a reply rather than by the gateway's ping succeeding: `/health`
    reports whether enforcement is authoritative, and a store that is answering does not mean a
    request has been metered against it since.
    """
    limiter, gateway = build(settings, raises=outage())
    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)
    assert limiter.degraded is True

    gateway.raises = None
    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert verdict.degraded is False
    assert limiter.degraded is False
    # The counters are lifetime totals and do NOT reset — a degradation that erased its own record
    # on recovery would be one nobody could review afterwards.
    assert limiter.degraded_checks == 1


async def test_a_readonly_reply_degrades_because_a_failover_is_an_outage(gateway: RedisGateway):
    """`ReadOnlyError` is a `ResponseError` and is still an availability failure.

    ``READONLY You can't write against a read only replica`` is what every client sees for the
    window around a Redis failover — a store that transiently cannot serve us. Degrading through
    that window is the entire job of the fail-open path; 500ing instead would take the API down
    during exactly the event the limiter was built to survive. Asserted through the **real gateway**
    rather than by injecting the classified type, because the classification is the subject.
    """

    async def readonly() -> None:
        raise redis.exceptions.ReadOnlyError("READONLY You can't write against a read only replica")

    with pytest.raises(BackingStoreUnavailable):
        await gateway.run(readonly, op="script:rlq")

    assert gateway.degraded_since is not None
    assert gateway.breaker.consecutive_failures == 1


# =============================================================================================
# 4. THE GUARD ON C2'S SPLIT — correctness failures must not become degradations
# =============================================================================================


async def test_a_response_error_still_propagates_and_is_never_laundered_into_a_degradation(
    settings: Settings,
):
    """**The test that protects C2's availability/correctness split. Read the module docstring.**

    A `ResponseError` means the store answered and the answer was "your command is wrong" — the
    signature of a broken Lua script. Classified as an outage, it would take the fail-open path:
    under the shipped `FAIL_MODE=open` a one-character typo in the decision script would silently
    disable rate limiting for **every request**, on **every replica**, and `/health` would report
    it identically to an unplugged Redis. It would be invisible until somebody read the logs.

    So it propagates as itself and becomes a 500 — visible, attributable, and the correct answer
    when the service is the thing that is broken.
    """
    limiter, _gateway = build(
        settings, raises=redis.exceptions.ResponseError("Error compiling script")
    )

    with pytest.raises(redis.exceptions.ResponseError):
        await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    # Not one bit of degradation bookkeeping happened: this is not a degradation.
    assert limiter.degraded is False
    assert limiter.degraded_checks == 0
    assert limiter.fail_closed_denials == 0


@pytest.mark.parametrize(
    "error",
    [
        redis.exceptions.ResponseError("Error compiling script"),
        redis.exceptions.ResponseError("WRONGTYPE Operation against a key holding the wrong kind"),
        redis.exceptions.AuthenticationError("WRONGPASS invalid username-password pair"),
        redis.exceptions.NoPermissionError("NOPERM this user has no permissions"),
        redis.exceptions.DataError("Invalid input of type dict"),
    ],
    ids=["script-bug", "wrongtype", "wrongpass", "noperm", "dataerror"],
)
async def test_no_correctness_failure_reaches_the_fallback_in_either_fail_mode(
    settings: Settings, error
):
    """The whole family, in both modes. A bug must not become a policy in either configuration.

    `FAIL_MODE=closed` is included deliberately: refusing with a 503 *looks* like a safe answer for
    a script bug, and it is still wrong. It would report a permanent bug in this service as a
    backing-store outage, sending an operator to look at Redis while the actual fault sat in a
    script that has never once compiled.
    """
    for mode in ("open", "closed"):
        limiter, _gateway = build(settings, raises=error, fail_mode=mode)

        with pytest.raises(type(error)):
            await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

        assert limiter.degraded is False


async def test_a_malformed_reply_is_a_bug_and_not_an_outage(settings: Settings):
    """An arity mismatch means the script and the decoder disagree — a bug, on every request.

    Same reasoning as the `ResponseError` above: routing it through `FAIL_MODE` would hide a
    permanent contract break behind a passing degradation. Building a decision out of whatever
    landed in the right slots would be worse still — a caller allowed because a quota counter
    happened to sit in the `allowed` position.
    """
    limiter, gateway = build(settings)
    gateway.reply = lua_reply()[:-1]

    with pytest.raises(ValueError, match="exactly 19 elements"):
        await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert limiter.degraded is False


async def test_a_wiring_bug_raises_rather_than_degrading(settings: Settings):
    """A gateway that was never connected is a missing `await runtime.start()`, not an outage.

    Dressing it up as `BackingStoreUnavailable` would make the limiter fail *open* on it — i.e.
    silently stop enforcing anything because somebody forgot a line in the lifespan. The
    `RuntimeError` from `register` is left to propagate for exactly that reason.
    """
    limiter = Limiter(RedisGateway(settings), StubTiers(settings), settings)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="connect\\(\\) must be awaited"):
        await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert limiter.degraded is False


# =============================================================================================
# 5. Bookkeeping
# =============================================================================================


async def test_degraded_decisions_carry_this_replicas_clock(settings: Settings):
    """`server_now_ms` is normally `redis.call('TIME')` — the one clock every replica shares.

    There is no shared clock on this path; that is what the outage *is*. The field carries the
    local one instead, which means C9 will bucket degraded requests against a clock that may differ
    per replica. A real (small) consequence of degrading, pinned here rather than discovered from a
    graph with two humps in it.
    """
    limiter, _gateway = build(settings, raises=outage())

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert verdict.server_now_ms > 1_700_000_000_000
    assert verdict.latency_ms >= 0.0


async def test_stats_report_the_degradation_for_health_and_the_dashboard(settings: Settings):
    """A degradation nobody can see is a degradation nobody fixes."""
    limiter, gateway = build(settings, raises=outage())
    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    stats = limiter.stats()

    assert stats["degraded"] is True
    assert stats["degraded_checks"] == 1
    assert stats["degraded_for_sec"] >= 0.0
    assert stats["fail_mode"] == "open"
    # Two gates for one request — the per-endpoint bucket and the account-wide gate — held in one
    # map under one cap. See `test_both_fallback_gates_are_keyed_the_way_the_shared_ones_are`.
    assert stats["fallback"]["size"] == 2

    gateway.raises = None
    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)
    assert limiter.stats()["degraded"] is False
    assert limiter.stats()["degraded_for_sec"] is None


async def test_concurrent_degraded_checks_share_one_bucket(settings: Settings):
    """The fallback is per-process state under an event loop, so concurrency must not double-spend.

    `consume` mutates between two awaits in the caller, so the loop cannot interleave two of them —
    but that is an argument, and this is the assertion. 80 concurrent degraded checks against a
    30-token replica share must admit 30, not 80.
    """
    limiter, _gateway = build(settings, raises=outage(), api_replicas=2)

    verdicts = await asyncio.gather(
        *(limiter.check(USER, ENDPOINT, 1, now=MOMENT) for _ in range(80))
    )

    assert sum(verdict.allowed for verdict in verdicts) == 30
