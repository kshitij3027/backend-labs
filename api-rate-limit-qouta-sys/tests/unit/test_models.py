"""Unit tests for the domain vocabulary — headers, the 429 body, and the Lua reply decoder.

Three things in ``src/models.py`` are contracts that cross a boundary no Python in this repo can
police on its own, so each gets a direct assertion here:

1. **The header set.** C6 emits whatever ``headers()`` returns; nothing downstream re-checks it, so
   a header dropped from the dict is a header that silently stops existing.
2. **The two literal 429 strings.** The C13 verifier compares them character-for-character across a
   container boundary. A reworded string would pass every other test in this suite.
3. **The 19-element reply order.** ``from_lua`` is the only consumer of C4's Lua reply. Decoding it
   with an off-by-one would produce a *confident, wrong* decision — a caller allowed because a
   quota number landed in the ``allowed`` slot — which is exactly the failure mode this project
   exists to make impossible to have quietly.

The reply is built by hand here rather than obtained from a real script. That is deliberate and it
is the right oracle: ``from_lua`` decodes a *shape*, and pinning the shape independently of the
producer is what makes the two testable against each other at all. C4's integration tests assert
that the script actually emits this shape against real Redis.
"""

from __future__ import annotations

import dataclasses

import pytest
from pydantic import ValidationError

from src.config import TierConfig
from src.main import EXPOSE_HEADERS
from src.models import (
    DEGRADED_HEADER,
    ERROR_QUOTA,
    ERROR_RATE_LIMIT,
    LUA_REPLY_ARITY,
    LUA_REPLY_FIELDS,
    QUOTA_LIMIT_HEADER,
    QUOTA_REMAINING_HEADER,
    QUOTA_RESET_HEADER,
    RATELIMIT_LIMIT_HEADER,
    RATELIMIT_REMAINING_HEADER,
    RATELIMIT_RESET_HEADER,
    RETRY_AFTER_HEADER,
    UNLIMITED,
    CredentialKind,
    DenyReason,
    LimitDecision,
    Principal,
    QuotaPeriodState,
    QuotaUsage,
    Tier,
    TierUpdate,
    UserTierUpdate,
    UserUsage,
    ceil_seconds,
)

#: Every rate/quota header a metered response can carry, excluding the two conditional ones
#: (``Retry-After``, ``X-RateLimit-Degraded``).
SIX_HEADERS = {
    RATELIMIT_LIMIT_HEADER,
    RATELIMIT_REMAINING_HEADER,
    RATELIMIT_RESET_HEADER,
    QUOTA_LIMIT_HEADER,
    QUOTA_REMAINING_HEADER,
    QUOTA_RESET_HEADER,
}

#: The three quota headers, which the degraded path must omit **entirely**.
QUOTA_HEADERS = {QUOTA_LIMIT_HEADER, QUOTA_REMAINING_HEADER, QUOTA_RESET_HEADER}


def decision(**overrides) -> LimitDecision:
    """Build a plausible allowed decision, with any field overridden.

    A factory rather than a fixture: most tests here vary two or three fields of a 24-field record,
    and a keyword override reads as "the same decision, except out of daily quota" — which is the
    sentence the test is actually about.
    """
    base = {
        "allowed": True,
        "reason": DenyReason.NONE,
        "tier": "free",
        "user_id": "alice",
        "endpoint": "GET:/api/v1/logs/query",
        "cost": 5,
        "bucket_limit": 60,
        "bucket_remaining": 40,
        "bucket_reset_sec": 3,
        "window_limit": 60,
        "window_used": 20,
        "window_reset_sec": 37,
        "daily_limit": 1000,
        "daily_used": 120,
        "daily_reset_at": 1_786_752_000,
        "daily_state": QuotaPeriodState.ACTIVE,
        "monthly_limit": 25_000,
        "monthly_used": 4_200,
        "monthly_reset_at": 1_788_220_800,
        "monthly_state": QuotaPeriodState.ACTIVE,
        "retry_after_sec": 0,
        "degraded": False,
        "server_now_ms": 1_786_700_000_000,
        "latency_ms": 0.42,
    }
    base.update(overrides)
    return LimitDecision(**base)  # type: ignore[arg-type]


def lua_reply(**overrides) -> list[object]:
    """Build a well-formed 19-element reply, in :data:`LUA_REPLY_FIELDS` order.

    Keyed by field name and then flattened, so a test that changes ``retry_ms`` says so rather than
    counting to index 17 — and so a reordering of the contract breaks this helper loudly instead of
    silently shifting every test's meaning.
    """
    values: dict[str, object] = {
        "allowed": 1,
        "reason": b"ok",
        "tier": b"free",
        "bucket_limit": 60,
        "bucket_remaining": 40,
        "bucket_reset_ms": 2_100,
        "window_limit": 60,
        "window_used": 20,
        "window_reset_ms": 36_400,
        "daily_limit": 1000,
        "daily_used": 120,
        "daily_expire_at": 1_786_752_000,
        "daily_state": b"active",
        "monthly_limit": 25_000,
        "monthly_used": 4_200,
        "monthly_expire_at": 1_788_220_800,
        "monthly_state": b"active",
        "retry_ms": 0,
        "now_ms": 1_786_700_000_000,
    }
    values.update(overrides)
    return [values[name] for name in LUA_REPLY_FIELDS]


# --------------------------------------------------------------------------------------------
# Enums
# --------------------------------------------------------------------------------------------


def test_the_enums_carry_their_wire_strings():
    """StrEnum members ARE their wire values, so nothing has to coerce them on the way out."""
    assert Tier.FREE == "free"
    assert Tier.PREMIUM == "premium"
    assert Tier.ENTERPRISE == "enterprise"
    assert DenyReason.NONE == "ok"
    assert DenyReason.RATE_LIMIT == "rate_limit"
    assert DenyReason.SLIDING_WINDOW == "sliding_window"
    assert DenyReason.QUOTA_DAILY == "quota_daily"
    assert DenyReason.QUOTA_MONTHLY == "quota_monthly"
    assert DenyReason.BACKING_STORE == "backing_store"
    assert QuotaPeriodState.RESET == "reset"
    assert QuotaPeriodState.ACTIVE == "active"
    assert QuotaPeriodState.EXHAUSTED == "exhausted"
    assert CredentialKind.API_KEY == "api_key"
    assert CredentialKind.JWT == "jwt"


def test_tier_config_is_re_exported_and_not_redefined():
    """`src.models` is the single import site; `src.config` is where it is parsed.

    Identity, not equality: two structurally identical classes would satisfy an ``==`` on their
    fields and still be two contracts that can drift apart.
    """
    from src import config, models

    assert models.TierConfig is config.TierConfig


# --------------------------------------------------------------------------------------------
# Principal
# --------------------------------------------------------------------------------------------


def test_principal_is_frozen_and_defaults_its_audit_label():
    principal = Principal(user_id="alice", credential=CredentialKind.API_KEY)

    assert principal.key_id is None
    with pytest.raises(dataclasses.FrozenInstanceError):
        principal.user_id = "mallory"  # type: ignore[misc]


# --------------------------------------------------------------------------------------------
# headers()
# --------------------------------------------------------------------------------------------


def test_an_allowed_decision_emits_all_six_headers_and_no_retry_after():
    """Limits are advertised on the 200 path too — a client cannot pace off a header it only sees
    once it has already been refused."""
    headers = decision().headers()

    assert SIX_HEADERS <= set(headers)
    assert RETRY_AFTER_HEADER not in headers
    assert DEGRADED_HEADER not in headers

    assert headers[RATELIMIT_LIMIT_HEADER] == "60"  # the TIER's per-minute number
    assert headers[RATELIMIT_REMAINING_HEADER] == "40"
    assert headers[RATELIMIT_RESET_HEADER] == "37"  # delay-seconds
    assert headers[QUOTA_LIMIT_HEADER] == "1000"
    assert headers[QUOTA_REMAINING_HEADER] == "880"
    assert headers[QUOTA_RESET_HEADER] == "1786752000"  # absolute unix seconds
    assert all(isinstance(value, str) for value in headers.values())


def test_a_denied_decision_carries_a_retry_after_of_at_least_one():
    """`Retry-After: 0` is a retry storm — the caller retries immediately and is refused again."""
    headers = decision(
        allowed=False,
        reason=DenyReason.RATE_LIMIT,
        bucket_remaining=0,
        window_used=60,
        retry_after_sec=3,
    ).headers()

    assert headers[RETRY_AFTER_HEADER] == "3"
    assert int(headers[RETRY_AFTER_HEADER]) >= 1
    # Still advertises the full picture on the rejection path.
    assert SIX_HEADERS <= set(headers)


def test_a_denied_decision_never_emits_retry_after_zero_even_when_built_by_hand():
    """The >= 1 floor is enforced at emit time as well as at decode time.

    C8 builds a degraded decision by hand rather than through ``from_lua``, so the invariant has to
    hold at the boundary it actually crosses rather than only where the Lua reply is parsed.
    """
    headers = decision(
        allowed=False, reason=DenyReason.SLIDING_WINDOW, retry_after_sec=0
    ).headers()

    assert headers[RETRY_AFTER_HEADER] == "1"


def test_a_degraded_decision_flags_itself_and_omits_every_quota_header():
    """**No counter was consulted, so a quota number here would be fabricated.**

    A missing header is a state a client can detect and handle; a wrong one is not, and it will
    happily build a usage display or a spend alarm on top of it.
    """
    headers = decision(degraded=True).headers()

    assert headers[DEGRADED_HEADER] == "1"
    assert QUOTA_HEADERS.isdisjoint(headers)
    # The rate headers are still emitted: the fallback bucket DID make a decision.
    assert headers[RATELIMIT_LIMIT_HEADER] == "60"
    assert RATELIMIT_REMAINING_HEADER in headers
    assert RATELIMIT_RESET_HEADER in headers


def test_an_unenforced_daily_quota_omits_the_quota_headers_rather_than_reporting_zero():
    """Same rule as degraded, reached through configuration instead of an outage.

    With ``QUOTA_DAILY_ENABLED=false`` the script reports a non-positive daily limit. Emitting
    ``X-Quota-Remaining: 0`` there would tell a caller they are exhausted when their quota is
    simply not being enforced — the one wrong answer worse than no answer.
    """
    headers = decision(daily_limit=0, daily_used=0).headers()

    assert QUOTA_HEADERS.isdisjoint(headers)
    assert RATELIMIT_LIMIT_HEADER in headers


@pytest.mark.parametrize(
    ("bucket_remaining", "window_limit", "window_used"),
    [
        (-5, 60, 20),  # a negative bucket must never reach the wire
        (40, 60, 90),  # the window is over-spent (a limit lowered at runtime under a live counter)
        (0, 60, 60),   # both exhausted
    ],
    ids=["negative-bucket", "overspent-window", "both-empty"],
)
def test_remaining_is_never_negative_and_is_an_integer_string(
    bucket_remaining, window_limit, window_used
):
    """A negative `Remaining` is not a number any HTTP client knows what to do with."""
    headers = decision(
        bucket_remaining=bucket_remaining, window_limit=window_limit, window_used=window_used
    ).headers()

    raw = headers[RATELIMIT_REMAINING_HEADER]
    assert raw == str(int(raw))
    assert int(raw) >= 0


def test_reset_headers_are_never_negative():
    assert decision(window_reset_sec=-9).headers()[RATELIMIT_RESET_HEADER] == "0"


def test_every_emitted_header_is_exposed_to_browser_javascript():
    """A header the CORS allowlist omits is one browser JS cannot read, however correctly sent.

    The dashboard's entire job is displaying exactly these numbers, and the failure is invisible
    from Python: the header is on the wire, and only ``response.headers.get(...)`` in the browser
    returns null.
    """
    emitted = set(decision(allowed=False, reason=DenyReason.RATE_LIMIT, degraded=True).headers())
    emitted |= set(decision().headers())

    assert emitted <= set(EXPOSE_HEADERS)


# --------------------------------------------------------------------------------------------
# Derived quantities
# --------------------------------------------------------------------------------------------


def test_effective_remaining_reports_the_window_when_the_window_is_lower():
    """**The reason this property exists.**

    A caller who spent their minute on `/logs/query` arrives at `/whoami` with a full bucket for
    that endpoint and zero account-wide headroom. Reporting the bucket would advertise 60 and 429
    the very next request.
    """
    verdict = decision(bucket_remaining=60, window_limit=60, window_used=60)

    assert verdict.effective_remaining == 0
    assert verdict.headers()[RATELIMIT_REMAINING_HEADER] == "0"


def test_effective_remaining_reports_the_bucket_when_the_bucket_is_lower():
    """And the other way round: a drained endpoint bucket binds even with a quiet account."""
    verdict = decision(bucket_remaining=3, window_limit=60, window_used=0)

    assert verdict.effective_remaining == 3


def test_effective_remaining_ignores_an_unenforced_window():
    """A non-positive window limit is 'this gate is off', not 'you have no headroom'."""
    assert decision(bucket_remaining=17, window_limit=0, window_used=0).effective_remaining == 17


def test_daily_and_monthly_remaining_subtract_and_floor_at_zero():
    verdict = decision(daily_limit=1000, daily_used=1200, monthly_limit=25_000, monthly_used=10)

    assert verdict.daily_remaining == 0
    assert verdict.monthly_remaining == 24_990


@pytest.mark.parametrize("limit", [0, -1], ids=["zero", "negative"])
def test_an_unlimited_period_reports_minus_one_not_zero(limit):
    """`-1` and `0` are opposite facts and must not share an encoding.

    A client pacing off a `0` would stop calling an endpoint it has infinite allowance on.
    """
    verdict = decision(daily_limit=limit, daily_used=99, monthly_limit=limit, monthly_used=99)

    assert verdict.daily_remaining == UNLIMITED == -1
    assert verdict.monthly_remaining == UNLIMITED


# --------------------------------------------------------------------------------------------
# error_body()
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reason", [DenyReason.RATE_LIMIT, DenyReason.SLIDING_WINDOW], ids=["bucket", "window"]
)
def test_both_rate_reasons_produce_the_spec_string_verbatim(reason):
    """The C13 verifier asserts this character-for-character across a container boundary."""
    body = decision(allowed=False, reason=reason, retry_after_sec=2).error_body()

    assert body["error"] == "Rate limit exceeded"
    assert body["error"] == ERROR_RATE_LIMIT
    assert body["reason"] == reason.value


@pytest.mark.parametrize(
    "reason", [DenyReason.QUOTA_DAILY, DenyReason.QUOTA_MONTHLY], ids=["daily", "monthly"]
)
def test_both_quota_reasons_produce_the_other_spec_string_verbatim(reason):
    """Status is 429 for both families; `reason` is what carries the distinction."""
    body = decision(allowed=False, reason=reason, retry_after_sec=60).error_body()

    assert body["error"] == "Quota exceeded"
    assert body["error"] == ERROR_QUOTA
    assert body["reason"] == reason.value


def test_the_error_body_carries_both_quota_periods_in_full():
    """The daily headers say nothing about a monthly exhaustion, so the body has to."""
    body = decision(
        allowed=False,
        reason=DenyReason.QUOTA_MONTHLY,
        retry_after_sec=4_000,
        monthly_used=25_000,
        monthly_state=QuotaPeriodState.EXHAUSTED,
    ).error_body()

    assert body["tier"] == "free"
    assert body["limit"] == 60
    assert body["remaining"] >= 0
    assert body["retry_after"] == 4_000
    assert body["detail"]
    assert body["quota"]["daily"] == {
        "limit": 1000,
        "remaining": 880,
        "reset_at": 1_786_752_000,
        "state": "active",
    }
    assert body["quota"]["monthly"] == {
        "limit": 25_000,
        "remaining": 0,
        "reset_at": 1_788_220_800,
        "state": "exhausted",
    }
    # Nothing correlated this request, so no null key is published.
    assert "request_id" not in body


def test_the_error_body_carries_a_request_id_when_one_is_supplied():
    body = decision(allowed=False, reason=DenyReason.RATE_LIMIT, retry_after_sec=1).error_body(
        "req-123"
    )

    assert body["request_id"] == "req-123"


def test_the_error_body_never_advertises_a_zero_retry_after():
    body = decision(allowed=False, reason=DenyReason.RATE_LIMIT, retry_after_sec=0).error_body()

    assert body["retry_after"] == 1


def test_an_unexpected_reason_still_produces_one_of_the_two_spec_strings():
    """`backing_store` is C8's fail-closed refusal and should never reach a 429 body.

    If it ever does, it must still be one of the two literals the verifier accepts — a body with a
    third string would fail a check across a container boundary that no unit test would catch
    first.
    """
    body = decision(allowed=False, reason=DenyReason.BACKING_STORE, retry_after_sec=1).error_body()

    assert body["error"] in {ERROR_RATE_LIMIT, ERROR_QUOTA}
    assert body["reason"] == "backing_store"


# --------------------------------------------------------------------------------------------
# ceil_seconds
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("milliseconds", "expected"),
    [
        (0, 0),
        (-5, 0),
        (1, 1),        # THE case: floor would say 0, which is a retry storm
        (999, 1),
        (1_000, 1),
        (1_001, 2),
        (36_400, 37),
        (86_400_000, 86_400),
    ],
)
def test_millisecond_to_second_conversion_always_rounds_up(milliseconds, expected):
    assert ceil_seconds(milliseconds) == expected


# --------------------------------------------------------------------------------------------
# from_lua
# --------------------------------------------------------------------------------------------


def test_from_lua_round_trips_a_well_formed_reply():
    verdict = LimitDecision.from_lua(
        lua_reply(),
        user_id="alice",
        endpoint="GET:/api/v1/logs/query",
        cost=5,
        latency_ms=1.25,
    )

    assert verdict.allowed is True
    assert verdict.reason is DenyReason.NONE
    assert verdict.tier == "free"
    assert verdict.user_id == "alice"
    assert verdict.endpoint == "GET:/api/v1/logs/query"
    assert verdict.cost == 5
    assert verdict.bucket_limit == 60
    assert verdict.bucket_remaining == 40
    assert verdict.bucket_reset_sec == 3  # 2100 ms, ceiled
    assert verdict.window_limit == 60
    assert verdict.window_used == 20
    assert verdict.window_reset_sec == 37  # 36400 ms, ceiled
    assert verdict.daily_limit == 1000
    assert verdict.daily_used == 120
    assert verdict.daily_reset_at == 1_786_752_000
    assert verdict.daily_state is QuotaPeriodState.ACTIVE
    assert verdict.monthly_limit == 25_000
    assert verdict.monthly_used == 4_200
    assert verdict.monthly_reset_at == 1_788_220_800
    assert verdict.monthly_state is QuotaPeriodState.ACTIVE
    assert verdict.retry_after_sec == 0
    assert verdict.degraded is False  # a reply exists, so Redis answered
    assert verdict.server_now_ms == 1_786_700_000_000
    assert verdict.latency_ms == 1.25


def test_from_lua_decodes_bytes_elements():
    """`decode_responses=False`, so every string element arrives as bytes off the wire."""
    verdict = LimitDecision.from_lua(
        lua_reply(
            allowed=b"0",
            reason=b"quota_daily",
            tier=b"premium",
            daily_state=b"exhausted",
            monthly_state=b"active",
            bucket_limit=b"300",
            retry_ms=b"7000",
        ),
        user_id="bob",
        endpoint="GET:/api/v1/whoami",
        cost=1,
        latency_ms=0.9,
    )

    assert verdict.allowed is False
    assert verdict.reason is DenyReason.QUOTA_DAILY
    assert verdict.tier == "premium"
    assert verdict.bucket_limit == 300
    assert verdict.daily_state is QuotaPeriodState.EXHAUSTED
    assert verdict.retry_after_sec == 7


def test_from_lua_decodes_already_decoded_string_elements():
    """The decoder must not depend on the client's `decode_responses` setting.

    The production gateway uses `decode_responses=False`, but the double the unit suite reaches
    for and any future diagnostic client may hand back `str`. A decoder that only understood one
    of the two would fail in whichever context it was not written against, at the moment the
    decision was being made.
    """
    verdict = LimitDecision.from_lua(
        lua_reply(reason="ok", tier="enterprise", daily_state="reset", monthly_state="active"),
        user_id="carol",
        endpoint="GET:/api/v1/whoami",
        cost=1,
        latency_ms=0.3,
    )

    assert verdict.tier == "enterprise"
    assert verdict.reason is DenyReason.NONE
    assert verdict.daily_state is QuotaPeriodState.RESET


def test_an_allowed_decision_advertises_no_retry_at_all():
    """`error_body()` is a 429 shape, so an allowed decision reaching it must still not invent a
    retry interval — the >= 1 floor applies to denials only."""
    body = decision(allowed=True, retry_after_sec=0).error_body()

    assert body["retry_after"] == 0


def test_from_lua_ceils_a_single_millisecond_to_one_second():
    """1 ms must decode to 1 s, not 0. A floored `Retry-After: 0` is a hot retry loop."""
    verdict = LimitDecision.from_lua(
        lua_reply(allowed=0, reason=b"rate_limit", retry_ms=1, bucket_reset_ms=1),
        user_id="alice",
        endpoint="GET:/api/v1/whoami",
        cost=1,
        latency_ms=0.1,
    )

    assert verdict.retry_after_sec == 1
    assert verdict.bucket_reset_sec == 1


def test_a_denied_decision_never_decodes_to_a_zero_retry_after():
    """Even when the script says 0 ms, a denial must advertise at least one second."""
    verdict = LimitDecision.from_lua(
        lua_reply(allowed=0, reason=b"sliding_window", retry_ms=0),
        user_id="alice",
        endpoint="GET:/api/v1/whoami",
        cost=1,
        latency_ms=0.1,
    )

    assert verdict.allowed is False
    assert verdict.retry_after_sec == 1
    assert verdict.headers()[RETRY_AFTER_HEADER] == "1"


@pytest.mark.parametrize("length", [0, 18, 20], ids=["empty", "one-short", "one-long"])
def test_a_reply_of_the_wrong_arity_raises_naming_the_expected_count(length):
    """A short or long reply means the script and this decoder disagree about the contract.

    Building a decision out of whatever landed in the right slots would produce a confident wrong
    answer; C4/C8 classify the raised ValueError as a failure instead.
    """
    with pytest.raises(ValueError, match=r"exactly 19 elements"):
        LimitDecision.from_lua(
            [0] * length, user_id="alice", endpoint="x", cost=1, latency_ms=0.0
        )


def test_a_non_sequence_reply_raises_naming_the_expected_count():
    with pytest.raises(ValueError, match=r"19 elements"):
        LimitDecision.from_lua(
            None,  # type: ignore[arg-type]
            user_id="alice",
            endpoint="x",
            cost=1,
            latency_ms=0.0,
        )


def test_a_non_integer_field_raises_naming_the_field():
    with pytest.raises(ValueError, match=r"'bucket_remaining' is not an integer"):
        LimitDecision.from_lua(
            lua_reply(bucket_remaining=b"forty"),
            user_id="alice",
            endpoint="x",
            cost=1,
            latency_ms=0.0,
        )


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"reason": b"nope"}, r"'reason' carries an unknown DenyReason"),
        ({"daily_state": b"nope"}, r"'daily_state' carries an unknown QuotaPeriodState"),
        ({"monthly_state": b"nope"}, r"'monthly_state' carries an unknown QuotaPeriodState"),
    ],
    ids=["reason", "daily-state", "monthly-state"],
)
def test_an_unknown_enum_value_raises_rather_than_defaulting(overrides, match):
    """Mapping an unknown reason onto a plausible default would let a script bug ship as a 429."""
    with pytest.raises(ValueError, match=match):
        LimitDecision.from_lua(
            lua_reply(**overrides), user_id="alice", endpoint="x", cost=1, latency_ms=0.0
        )


def test_the_reply_contract_is_nineteen_fields_in_the_documented_order():
    """C4's `src/lua.py` imports these rather than restating the order — pinned here."""
    assert LUA_REPLY_ARITY == 19
    assert LUA_REPLY_FIELDS == (
        "allowed",
        "reason",
        "tier",
        "bucket_limit",
        "bucket_remaining",
        "bucket_reset_ms",
        "window_limit",
        "window_used",
        "window_reset_ms",
        "daily_limit",
        "daily_used",
        "daily_expire_at",
        "daily_state",
        "monthly_limit",
        "monthly_used",
        "monthly_expire_at",
        "monthly_state",
        "retry_ms",
        "now_ms",
    )


def test_a_decision_is_frozen():
    """A decision is a record of something that already happened; no formatter edits history."""
    verdict = decision()

    with pytest.raises(dataclasses.FrozenInstanceError):
        verdict.allowed = False  # type: ignore[misc]


# --------------------------------------------------------------------------------------------
# Admin wire shapes
# --------------------------------------------------------------------------------------------


def test_a_tier_update_is_partial_and_merges_onto_the_current_config():
    base = TierConfig(
        name="premium",
        rate_limit_per_min=300,
        burst=300,
        daily_quota=50_000,
        monthly_quota=1_250_000,
    )

    merged = TierUpdate(rate_limit_per_min=10).apply_to(base)

    assert merged.rate_limit_per_min == 10
    assert merged.burst == 300
    assert merged.daily_quota == 50_000
    assert merged.monthly_quota == 1_250_000
    assert merged.name == "premium"
    # Frozen: the table currently being enforced is untouched until the caller stores the result.
    assert base.rate_limit_per_min == 300


@pytest.mark.parametrize(
    "payload",
    [
        {"daily_quota": 0},
        {"burst": -1},
        {"rate_limit_per_min": 0},
        {"monthly_quota": -5},
    ],
    ids=["daily-zero", "burst-negative", "rpm-zero", "monthly-negative"],
)
def test_a_tier_update_refuses_a_non_positive_limit(payload):
    """**A non-positive limit reads as 'unenforced' in the decision script.**

    So `{"daily_quota": 0}` through the admin API would silently grant that tier an unlimited daily
    allowance — through the endpoint whose purpose is tightening limits.
    """
    with pytest.raises(ValidationError):
        TierUpdate(**payload)


@pytest.mark.parametrize("payload", [{}, {"burst": None}], ids=["empty", "explicit-null"])
def test_a_tier_update_that_changes_nothing_is_refused(payload):
    """An empty update would bump config:version, invalidate every replica, and change nothing."""
    with pytest.raises(ValidationError):
        TierUpdate(**payload)


def test_a_tier_update_refuses_an_unknown_field():
    """`{"rate_limit": 10}` is a plausible misspelling; a silent 200 is the worst answer to it."""
    with pytest.raises(ValidationError):
        TierUpdate(rate_limit=10)  # type: ignore[call-arg]


def test_a_user_tier_update_trims_and_refuses_blank():
    """A trailing space would match no tier and silently demote the principal to DEFAULT_TIER."""
    assert UserTierUpdate(tier="  premium  ").tier == "premium"

    with pytest.raises(ValidationError):
        UserTierUpdate(tier="   ")


def test_a_user_tier_update_accepts_a_tier_the_enum_does_not_know():
    """Tiers are runtime data — C10 can create one, and this body must not reject it."""
    assert UserTierUpdate(tier="platinum").tier == "platinum"


def test_the_usage_shapes_serialise_their_enum_as_a_plain_string():
    usage = UserUsage(
        user_id="alice",
        tier="free",
        daily=QuotaUsage(
            limit=1000, used=120, remaining=880, reset_at=1_786_752_000, state="active"
        ),
        monthly=QuotaUsage(
            limit=25_000, used=25_000, remaining=0, reset_at=1_788_220_800, state="exhausted"
        ),
    )

    dumped = usage.model_dump(mode="json")

    assert dumped["daily"]["state"] == "active"
    assert dumped["monthly"]["state"] == "exhausted"
    assert dumped["monthly"]["remaining"] == 0
