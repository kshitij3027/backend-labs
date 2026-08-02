"""The whole vocabulary of the enforcement layer: who is calling, what was decided, how it is told.

Every other module in this project speaks in the types declared here. ``src.keys`` says what a
Redis key is *called*; this module says what the answer *means*.

.. rubric:: :class:`LimitDecision` is the contract that keeps the response path free of I/O

The middleware needs six response headers, a 429 body, and (C9) an analytics record. Each of those
wants a number, and every number they want is a field on :class:`LimitDecision`. That is not
convenience — it is the design:

    **If a field is not on this dataclass, the header that needs it does not get emitted.**

The alternative is a second Redis round trip on the response path ("what was their quota again?"),
which would double the store traffic of the whole service, put a second point of failure *after*
the request was already admitted, and — worst — report numbers read at a different instant than
the ones the decision was made from. A caller would then be told "remaining: 12" by a read that
raced the very request being answered. One script call in, one immutable answer out.

Frozen, and with ``slots=True``. Frozen because a decision is a *record of something that already
happened*: the tokens are spent, the quota counter is incremented, and no downstream formatter may
edit history on its way to a header. Slots because one of these is allocated per metered request
at the project's 1000 rps target, and slots drop the per-instance ``__dict__``.

.. rubric:: The deliberate unit asymmetry in the headers

``X-RateLimit-Reset`` is **delay-seconds**; ``X-Quota-Reset`` is an **absolute unix timestamp**.
This is a decision, documented here and in the README, because it otherwise reads as a bug.

A rate-limit reset is a *duration* — "your bucket has headroom again in 3 seconds" — and a client
must be able to act on it without a clock synchronised to ours. Handing them an absolute
millisecond-accurate instant would make every client's retry correctness depend on their NTP.
A quota rollover is the opposite kind of fact: it is a *wall-clock event* (00:00 UTC), it is hours
away, and a client that wants to display "resets at midnight" needs the instant, not a countdown
that was already stale when it was serialised. ``Retry-After`` is delay-seconds because RFC 9110
says so.

.. rubric:: ``TierConfig`` lives in ``src.config`` and is re-exported here

Tier sizing is *parsed* from the environment, so its model belongs beside the parser. But every
downstream module (``src.tiers``, C4's limiter, C10's admin API) thinks of it as a domain type, so
it is re-exported here and ``src.models`` is the single import site. One class, one definition; the
import is the alias, not a second copy that can drift.

``HealthResponse`` deliberately does **not** live here. It is the wire shape of exactly one route
and it stays in ``src/api/health.py`` next to that route.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Re-exported, NOT redefined. See the module docstring: `TierConfig` is parsed in `src.config`
# (which is the only module that reads the environment) and consumed as a domain type everywhere
# else. A second declaration here would be a contract that holds only by coincidence.
from src.config import TierConfig

__all__ = [
    "CredentialKind",
    "DEGRADED_HEADER",
    "DEGRADED_HEADER_VALUE",
    "DENY_DETAIL",
    "DenyReason",
    "ERROR_QUOTA",
    "ERROR_RATE_LIMIT",
    "LUA_REPLY_ARITY",
    "LUA_REPLY_FIELDS",
    "LimitDecision",
    "Principal",
    "QUOTA_LIMIT_HEADER",
    "QUOTA_REASONS",
    "QUOTA_REMAINING_HEADER",
    "QUOTA_RESET_HEADER",
    "QuotaPeriodState",
    "QuotaUsage",
    "RATELIMIT_LIMIT_HEADER",
    "RATELIMIT_REMAINING_HEADER",
    "RATELIMIT_RESET_HEADER",
    "RETRY_AFTER_HEADER",
    "Tier",
    "TierConfig",
    "TierUpdate",
    "UNLIMITED",
    "UserTierUpdate",
    "UserUsage",
    "ceil_seconds",
]


# ---------------------------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------------------------


class Tier(StrEnum):
    """The three shipped tiers, spelled exactly as they appear in `user:{id}` and `config:tiers`.

    ``StrEnum`` so a member *is* its wire string: ``Tier.FREE == "free"`` is true, a value read
    straight out of a Redis HASH can be compared without coercion, and serialisation emits the
    plain name.

    **This enum is a convenience, not the authority.** Tiers are runtime-configurable (C10 can add
    one with a `PUT`), so nothing in the decision path may require a tier name to be a member here
    — ``LimitDecision.tier`` is a plain ``str`` for exactly that reason. Treating this enum as the
    closed set of tiers would mean an operator adding a tier at runtime gets a 500 instead of a
    limit, which is the same "no limit found" failure mode ``DEFAULT_TIER`` validation exists to
    prevent.
    """

    FREE = "free"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"


class DenyReason(StrEnum):
    """Which of the four gates refused — or ``ok`` when none did.

    The value is what appears in the 429 body's ``reason`` field and in C9's analytics, so these
    strings are a public contract.

    Four gates, four distinct reasons, and the distinction is load-bearing rather than cosmetic:
    ``rate_limit`` and ``sliding_window`` both mean "slow down, come back shortly", while
    ``quota_daily`` and ``quota_monthly`` mean "you have spent your allowance and no amount of
    waiting for a few seconds will help". Collapsing them into one reason would make a client's
    backoff strategy the same for a 3-second problem and an 8-hour one.

    ``backing_store`` is C8's fail-*closed* refusal: Redis is unreachable and ``FAIL_MODE=closed``,
    so nothing was evaluated at all. It is never produced by the Lua script.
    """

    NONE = "ok"
    RATE_LIMIT = "rate_limit"
    SLIDING_WINDOW = "sliding_window"
    QUOTA_DAILY = "quota_daily"
    QUOTA_MONTHLY = "quota_monthly"
    BACKING_STORE = "backing_store"


class QuotaPeriodState(StrEnum):
    """Where a quota period stands — reported per period on every response.

    ``reset``
        The counter for this period did not exist before this request; the period has just rolled
        over. Reported so a client can tell "you have 1000 left because it is a new day" from
        "you have 1000 left because you have not called us yet this month".
    ``active``
        The counter exists and is below its limit.
    ``exhausted``
        The counter has reached or passed its limit. A request denied for
        :attr:`DenyReason.QUOTA_DAILY` always carries ``exhausted`` for the daily period.
    """

    RESET = "reset"
    ACTIVE = "active"
    EXHAUSTED = "exhausted"


class CredentialKind(StrEnum):
    """How a :class:`Principal` proved who they are. C5 produces both forms.

    Recorded on the principal because the two credential kinds have genuinely different trust
    properties and different failure modes — an expired JWT and a revoked API key are different
    operational events — and because a log line saying "denied" is far less useful than one saying
    "denied, authenticated by api_key".
    """

    API_KEY = "api_key"
    JWT = "jwt"


# ---------------------------------------------------------------------------------------------
# Response header names and the two literal error strings
#
# Defined once, here, next to the model that emits them. Every one of these names is also listed
# in ``src.main.EXPOSE_HEADERS`` (the CORS allowlist) — a header the browser is not told to expose
# is a header browser JavaScript cannot read no matter that the server sent it, and the dashboard's
# entire job is displaying exactly these numbers. ``tests/unit/test_models.py`` pins the two lists
# together so a rename here cannot silently desync the allowlist.
# ---------------------------------------------------------------------------------------------

#: The tier's per-minute number. NOT the token bucket's capacity — see :meth:`LimitDecision.headers`.
RATELIMIT_LIMIT_HEADER = "X-RateLimit-Limit"

#: The *binding* remaining allowance across both rate gates. See ``effective_remaining``.
RATELIMIT_REMAINING_HEADER = "X-RateLimit-Remaining"

#: **Delay-seconds**, not a timestamp. See the module docstring for the asymmetry with the quota.
RATELIMIT_RESET_HEADER = "X-RateLimit-Reset"

QUOTA_LIMIT_HEADER = "X-Quota-Limit"
QUOTA_REMAINING_HEADER = "X-Quota-Remaining"

#: **Absolute unix seconds**, not a delay. See the module docstring.
QUOTA_RESET_HEADER = "X-Quota-Reset"

#: RFC 9110 delay-seconds. Emitted on denial only, and never below 1 — see :meth:`_retry_after`.
RETRY_AFTER_HEADER = "Retry-After"

#: Emitted only while the C8 fail-open fallback is carrying the request. A silent fail-open is
#: indistinguishable from having no rate limiter at all, so the degradation is put on the wire.
DEGRADED_HEADER = "X-RateLimit-Degraded"
DEGRADED_HEADER_VALUE = "1"

#: The spec's literal 429 strings. Named constants because the C13 E2E verifier asserts them
#: character-for-character across a container boundary — a "harmless" rewording here fails a check
#: that no Python in this repo would catch first.
ERROR_RATE_LIMIT = "Rate limit exceeded"
ERROR_QUOTA = "Quota exceeded"

#: The reasons that mean "you are out of allowance for the period" rather than "you are going too
#: fast". This frozenset is the single definition of that split; both the error string and any
#: future client-facing backoff advice derive from it.
QUOTA_REASONS: frozenset[DenyReason] = frozenset(
    {DenyReason.QUOTA_DAILY, DenyReason.QUOTA_MONTHLY}
)

#: Human-readable explanation per reason, for the 429 body's ``detail``. A machine-readable
#: ``reason`` plus a sentence a human can act on, rather than one string trying to be both.
DENY_DETAIL: Mapping[DenyReason, str] = MappingProxyType(
    {
        DenyReason.NONE: "Request allowed.",
        DenyReason.RATE_LIMIT: (
            "Request rate for this endpoint exceeded its burst capacity. The token bucket "
            "refills continuously — retry after the interval below."
        ),
        DenyReason.SLIDING_WINDOW: (
            "Account-wide request rate exceeded the tier's sustained per-minute limit across "
            "all endpoints."
        ),
        DenyReason.QUOTA_DAILY: (
            "Daily request quota for this tier is exhausted. It resets at 00:00 UTC."
        ),
        DenyReason.QUOTA_MONTHLY: (
            "Monthly request quota for this tier is exhausted. It resets at 00:00 UTC on the "
            "first of next month."
        ),
        DenyReason.BACKING_STORE: (
            "The rate limiter's backing store is unavailable and this deployment is configured "
            "to fail closed."
        ),
    }
)

#: Sentinel returned by :attr:`LimitDecision.daily_remaining` / ``monthly_remaining`` for a period
#: with no enforced ceiling (``limit <= 0``).
#:
#: ``-1`` rather than ``None`` because every other quantity on this dataclass is an ``int`` and the
#: whole return path is integer-typed on purpose (Lua 5.1 numbers are doubles whose RESP encoding
#: truncates decimals, so every quantity in the decision path is designed as an integer at the
#: source). Rather than ``0``, because "unlimited" and "you have nothing left" are opposite facts
#: and must not share an encoding — a client pacing off a ``0`` would stop calling an endpoint it
#: has infinite allowance on.
UNLIMITED = -1

#: The decision script's reply, **in order**. C4's ``src/lua.py`` must emit exactly this sequence;
#: importing this tuple from there rather than restating the order is what keeps the producer and
#: the consumer from drifting. Every element is an integer or a string — Lua->RESP truncates
#: numbers *and* stops at the first nil, so a reply designed with a nullable slot silently
#: truncates into a shorter list that this decoder then rejects.
LUA_REPLY_FIELDS: tuple[str, ...] = (
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

#: 19. Asserted on every decode: a reply of any other length means the script and this decoder
#: disagree, and C4/C8 classify that as a failure rather than serving a decision built from
#: whatever happened to be in the right slots.
LUA_REPLY_ARITY = len(LUA_REPLY_FIELDS)


# ---------------------------------------------------------------------------------------------
# Decoding helpers — pure, and shared by every field of the Lua reply
# ---------------------------------------------------------------------------------------------


def ceil_seconds(milliseconds: int) -> int:
    """Convert a millisecond duration to whole seconds, rounding **up**. Never down.

    ``ceil_seconds(1) == 1``, not ``0``.

    Flooring here is the single most consequential rounding mistake available in this project. A
    caller told ``Retry-After: 0`` retries immediately, is refused again, is told ``0`` again, and
    the "backoff" the header exists to produce becomes a hot loop against the service that is
    already refusing them — a retry storm manufactured by the limiter itself. The same applies to
    ``X-RateLimit-Reset``: a well-behaved client that sleeps for the advertised interval and finds
    the bucket still empty will conclude the header is unreliable and stop reading it.

    Rounding up costs a caller at most 999 ms of extra patience. Rounding down costs the service
    an unbounded number of doomed requests. The asymmetry is not close.

    Integer ceiling division rather than ``math.ceil(ms / 1000)``: the float form is exact for the
    magnitudes involved today but silently is not for large ones, and there is no reason to
    introduce a float into a path whose every other quantity is deliberately an integer.

    A non-positive duration is 0 — "no wait at all" — rather than a negative delay a client would
    have to interpret.
    """
    if milliseconds <= 0:
        return 0
    return -(-milliseconds // 1000)


def _as_text(value: object) -> str:
    """Decode one reply element to ``str``.

    The gateway builds its client with ``decode_responses=False`` (the decision reply is parsed
    positionally into integers, so a per-value UTF-8 decode of every reply would buy nothing on the
    hot path), so string elements arrive as ``bytes``. ``errors="replace"`` because a mangled byte
    in a tier name must surface as a visibly wrong value in an error message, not as a
    ``UnicodeDecodeError`` thrown from inside the limiter.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _as_int(value: object, *, field: str) -> int:
    """Coerce one reply element to ``int``, or raise a :class:`ValueError` naming the field.

    ``isinstance(value, int)`` also catches ``bool``, which is why the branch converts rather than
    returning as-is: a ``True`` left in an integer field would compare equal to 1 everywhere and
    still render as ``"True"`` the moment it reached a header.
    """
    if isinstance(value, int):
        return int(value)
    try:
        return int(_as_text(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"lua reply field {field!r} is not an integer: {value!r}"
        ) from exc


def _as_enum(enum_cls: type[StrEnum], value: object, *, field: str) -> Any:
    """Coerce one reply element to a member of ``enum_cls``, or raise naming the valid set.

    An unrecognised value is a **contract mismatch** between the Lua script and this decoder, not
    caller input, so it raises rather than degrading to some default. Silently mapping an unknown
    reason onto :attr:`DenyReason.RATE_LIMIT` would let a script bug ship as a plausible-looking
    429 — and a plausible-looking wrong answer from a rate limiter is exactly the failure this
    project is built to make impossible to have quietly.
    """
    text = _as_text(value)
    try:
        return enum_cls(text)
    except ValueError as exc:
        valid = [member.value for member in enum_cls]
        raise ValueError(
            f"lua reply field {field!r} carries an unknown {enum_cls.__name__} {text!r}; "
            f"expected one of {valid}"
        ) from exc


# ---------------------------------------------------------------------------------------------
# Principal
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Principal:
    """Who is calling, once C5's :class:`~src.identity.IdentityResolver` has decided.

    Frozen and slotted for the same two reasons as :class:`LimitDecision`: one is resolved (or
    served from the LRU cache) per metered request, and nothing downstream may edit *who* a request
    belongs to on its way to the bucket key.

    .. rubric:: ``key_id`` is a label, and MUST NOT be the key or its digest

    ``key_id`` exists so a log line, an admin lookup or an audit trail can say *which* of a user's
    credentials was used — "ci-runner", "laptop", "key-3". It is written to logs and may appear in
    an admin response.

    That makes it the one field on this class where a mistake is a credential leak rather than a
    bug. **The raw API key must never be put here**: it would be written into log aggregators, into
    incident tickets and into whatever ingests container stdout, from where it can be replayed
    verbatim. **Nor may the HMAC digest**: it is not the secret, but it *is* the exact lookup key
    for ``apikey:v1:<digest>``, so anyone holding it can read the principal record out of Redis and
    correlate a user's whole history across log lines that were supposed to be anonymous. The
    ``label`` field of the stored API-key record is what belongs here — an operator-chosen name
    that identifies a credential without being one.
    """

    user_id: str
    credential: CredentialKind
    key_id: str | None = None


# ---------------------------------------------------------------------------------------------
# LimitDecision
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LimitDecision:
    """Everything the response path needs, decided once, at the moment of admission.

    See the module docstring for why this carries so many fields: each one is the source of a
    header, a body field or an analytics dimension, and a field that is absent here is a number
    that would otherwise require a second Redis round trip on the response path.

    Field notes worth stating rather than inferring:

    ``tier``
        A plain ``str``, not :class:`Tier`. Tiers are runtime-configurable, so a tier name that is
        not a member of the enum is a valid operational state, not an error.
    ``endpoint``
        The **classified label** (``GET:/api/v1/logs/query``), which is the component that went
        into the bucket key — never the raw request path. See :func:`src.keys.classify`.
    ``*_reset_sec``
        Delay-seconds. ``*_reset_at`` fields are absolute unix seconds. The two spellings are
        deliberate; see the module docstring.
    ``retry_after_sec``
        0 when allowed, and **never** 0 when denied — :meth:`from_lua` enforces the floor of 1 at
        decode time and :meth:`_retry_after` enforces it again at emit time, because a
        ``Retry-After: 0`` is a retry storm.
    ``degraded``
        True when the decision came from C8's local fallback bucket rather than from Redis. Drives
        both ``X-RateLimit-Degraded`` and the suppression of every ``X-Quota-*`` header.
    ``server_now_ms``
        Redis's clock (``redis.call('TIME')``), not this replica's. It is the clock the bucket was
        refilled against, and C9 derives its analytics bucket index from it so that replicas with
        skewed system clocks still write into the *same* minute bucket.
    ``latency_ms``
        How long the check itself took, measured by the caller. The one float on the class, and the
        only field that is an observation rather than a decision.
    """

    allowed: bool
    reason: DenyReason
    tier: str
    user_id: str
    endpoint: str
    cost: int

    bucket_limit: int
    bucket_remaining: int
    bucket_reset_sec: int

    window_limit: int
    window_used: int
    window_reset_sec: int

    daily_limit: int
    daily_used: int
    daily_reset_at: int
    daily_state: QuotaPeriodState

    monthly_limit: int
    monthly_used: int
    monthly_reset_at: int
    monthly_state: QuotaPeriodState

    retry_after_sec: int
    degraded: bool
    server_now_ms: int
    latency_ms: float

    # ------------------------------------------------------------------ #
    # Derived quantities
    # ------------------------------------------------------------------ #
    @property
    def daily_remaining(self) -> int:
        """Requests left in the current UTC day, or :data:`UNLIMITED` (``-1``) when unenforced.

        ``max(0, limit - used)`` so an over-shoot (possible when the limit is *lowered* at runtime
        below a counter that has already passed it — C10 can do exactly that) reports "none left"
        rather than a negative number that would render as a nonsense header.

        ``limit <= 0`` means the daily gate is not enforcing anything — either the tier genuinely
        has no daily ceiling or ``QUOTA_DAILY_ENABLED=false`` — and is reported as
        :data:`UNLIMITED`. See that constant for why the sentinel is ``-1`` and not ``0``.
        """
        if self.daily_limit <= 0:
            return UNLIMITED
        return max(0, self.daily_limit - self.daily_used)

    @property
    def monthly_remaining(self) -> int:
        """Requests left in the current UTC month, or :data:`UNLIMITED`. Same rules as daily."""
        if self.monthly_limit <= 0:
            return UNLIMITED
        return max(0, self.monthly_limit - self.monthly_used)

    @property
    def effective_remaining(self) -> int:
        """The **binding** remaining allowance across both rate gates — the smaller of the two.

        ``min(bucket_remaining, window_limit - window_used)``.

        .. rubric:: Why not just report the bucket

        Because the two gates are independent and either can be the one that refuses. The token
        bucket is per ``(user, endpoint)``; the sliding window is account-wide across every
        endpoint. A caller who has just spent their whole minute on ``/logs/query`` arrives at
        ``/whoami`` with a **full** bucket for that endpoint and **zero** account-wide headroom.
        Reporting the bucket alone would advertise ``40`` and the very next request would be a 429
        — for a caller who was pacing themselves off the number this API told them to pace off.

        A rate-limit header that can say "you have 40 left" immediately before refusing is worse
        than no header at all: it converts a well-behaved client into a badly-behaved one, and it
        does so specifically to the clients that bothered to implement backoff.

        A non-positive ``window_limit`` means the account-wide gate is not enforcing anything, so
        it contributes no constraint and the bucket is the answer. Feeding a ``0`` limit through
        the formula would instead report ``0`` remaining on every response while the bucket sat
        full — the mirror image of the bug above, and just as misleading.
        """
        bucket = max(0, self.bucket_remaining)
        if self.window_limit <= 0:
            return bucket
        return min(bucket, max(0, self.window_limit - self.window_used))

    @property
    def error_title(self) -> str:
        """The 429 body's ``error`` string: the spec's two literals, chosen by reason family.

        Anything that is not a quota reason gets :data:`ERROR_RATE_LIMIT`. That covers the two rate
        gates and, defensively, a reason that should never reach a 429 at all — a 429 whose body
        said something other than one of the two spec strings would fail the E2E verifier's
        character-for-character check, which is a worse outcome than naming the more likely family.
        """
        return ERROR_QUOTA if self.reason in QUOTA_REASONS else ERROR_RATE_LIMIT

    def _retry_after(self) -> int:
        """``Retry-After`` in seconds: 0 while allowed, never below 1 once denied.

        The floor is applied here as well as in :meth:`from_lua` on purpose. ``from_lua`` protects
        the decode path; this protects every *other* construction site — C8's fallback decision is
        built by hand, and a hand-built denial with a zeroed ``retry_after_sec`` would emit the
        retry storm this rule exists to prevent. One invariant, enforced at the boundary it
        actually crosses.
        """
        if self.allowed:
            return max(0, self.retry_after_sec)
        return max(1, self.retry_after_sec)

    # ------------------------------------------------------------------ #
    # Wire shapes
    # ------------------------------------------------------------------ #
    def headers(self) -> dict[str, str]:
        """The response headers for this decision — emitted on the 200 path **and** the 429 path.

        Advertising limits only on rejection is the common mistake and it is self-defeating: a
        client cannot pace itself off information it only receives once it has already been
        refused. Every metered response carries the full picture.

        What each name means, and why:

        ``X-RateLimit-Limit`` = ``window_limit``
            The tier's **per-minute** number, not the token bucket's capacity. "My plan is 60
            requests per minute" is what a user believes their limit is, and it is the number on
            the pricing page; the bucket capacity is an implementation detail of how burst is
            absorbed. Reporting the bucket here would make ``Limit`` disagree with the tier for any
            future configuration where burst != rpm.
        ``X-RateLimit-Remaining`` = :attr:`effective_remaining`
            The binding number across both gates. See that property.
        ``X-RateLimit-Reset`` = ``window_reset_sec``
            The **account-wide sliding window's** reset, in **delay-seconds** — deliberately *not*
            ``bucket_reset_sec``. ``X-RateLimit-Limit`` is the tier's per-minute number, which is
            the window's ceiling, so the matching ``Reset`` has to be the window's too: a
            ``Limit``/``Reset`` pair describing two different gates is worse than either one alone,
            because the obvious client behaviour — "I am at my limit, so sleep until Reset" —
            would then sleep for the wrong gate's recovery and wake into another 429. The bucket's
            own recovery is on the decision as ``bucket_reset_sec`` for anyone who needs it. See
            the module docstring for the unit asymmetry with ``X-Quota-Reset``.
        ``X-Quota-Limit`` / ``X-Quota-Remaining`` / ``X-Quota-Reset``
            The **daily** period. Daily is the one that binds in practice (monthly is daily x 25),
            it is the one that rolls over within a client's session, and three more headers for the
            monthly period would be noise on every single response. The monthly numbers are in the
            429 body and in ``GET /admin/users/{id}/usage``, where someone is actually looking.
        ``Retry-After``
            Denied responses only, always >= 1. See :meth:`_retry_after`.
        ``X-RateLimit-Degraded``
            Only while C8's fallback is carrying the request.

        .. rubric:: While degraded, every ``X-Quota-*`` header is omitted entirely

        No quota counter was consulted — that is what "degraded" means — so any number emitted
        here would be fabricated. A **missing** header is a state a client can detect and handle
        ("the server did not tell me"); a *wrong* header is one it cannot, and it will happily
        build a usage display or a spend alarm on top of it. The same reasoning suppresses the
        quota headers when the daily gate is not enforcing a ceiling at all
        (``daily_limit <= 0``): reporting ``X-Quota-Remaining: 0`` for a caller whose quota is
        simply not being enforced would tell them they are exhausted when they are not.

        Every value is a string, because that is what an ASGI header is; ``Remaining`` is an
        integer string that is never negative.
        """
        out: dict[str, str] = {
            RATELIMIT_LIMIT_HEADER: str(max(0, self.window_limit)),
            RATELIMIT_REMAINING_HEADER: str(self.effective_remaining),
            RATELIMIT_RESET_HEADER: str(max(0, self.window_reset_sec)),
        }

        if self.degraded:
            out[DEGRADED_HEADER] = DEGRADED_HEADER_VALUE
        elif self.daily_limit > 0:
            out[QUOTA_LIMIT_HEADER] = str(self.daily_limit)
            out[QUOTA_REMAINING_HEADER] = str(max(0, self.daily_remaining))
            out[QUOTA_RESET_HEADER] = str(max(0, self.daily_reset_at))

        if not self.allowed:
            out[RETRY_AFTER_HEADER] = str(self._retry_after())

        return out

    def error_body(self, request_id: str | None = None) -> dict[str, Any]:
        """The 429 response body.

        ``error`` is one of the spec's two literal strings and nothing else — see
        :attr:`error_title`. The **status is 429 for both families**; ``reason`` is what
        distinguishes "slow down" from "you are out of allowance". Using 429 for a rate problem and
        something else (402, 403) for a quota problem would be defensible in a green-field API and
        is wrong here: the spec names 429, and every HTTP client library already has retry
        behaviour attached to it.

        ``limit`` and ``remaining`` mirror ``X-RateLimit-Limit`` / ``X-RateLimit-Remaining`` so a
        client that reads the body and a client that reads the headers cannot come to different
        conclusions.

        The ``quota`` sub-object carries **both** periods in full — the daily headers say nothing
        about a monthly exhaustion, and a caller refused for ``quota_monthly`` needs to see the
        number that refused them. ``reset_at`` is absolute unix seconds in both, matching
        ``X-Quota-Reset``.

        ``request_id`` is included only when the caller supplies one. A key that is always present
        and always ``null`` is noise in every response body that the middleware could not correlate
        anyway; when C6 has a request id, it passes it.
        """
        return {
            "error": self.error_title,
            "reason": self.reason.value,
            "detail": DENY_DETAIL[self.reason],
            "tier": self.tier,
            "limit": max(0, self.window_limit),
            "remaining": self.effective_remaining,
            "retry_after": self._retry_after(),
            "quota": {
                "daily": {
                    "limit": self.daily_limit,
                    "remaining": self.daily_remaining,
                    "reset_at": self.daily_reset_at,
                    "state": self.daily_state.value,
                },
                "monthly": {
                    "limit": self.monthly_limit,
                    "remaining": self.monthly_remaining,
                    "reset_at": self.monthly_reset_at,
                    "state": self.monthly_state.value,
                },
            },
            **({"request_id": request_id} if request_id is not None else {}),
        }

    # ------------------------------------------------------------------ #
    # Construction from the decision script's reply
    # ------------------------------------------------------------------ #
    @classmethod
    def from_lua(
        cls,
        raw: list[Any],
        *,
        user_id: str,
        endpoint: str,
        cost: int,
        latency_ms: float,
    ) -> LimitDecision:
        """Decode the decision script's 19-element positional reply into a decision.

        The reply's order is :data:`LUA_REPLY_FIELDS` and its length is :data:`LUA_REPLY_ARITY`.
        Elements arrive as ``bytes`` (``decode_responses=False``) or as ``int``; both are handled.

        ``user_id``, ``endpoint`` and ``cost`` are keyword-only and supplied by the caller because
        they are *inputs* to the script rather than outputs of it — echoing them back through Redis
        would be three more bytes on the wire per request to tell us something we already knew.

        .. rubric:: A malformed reply raises rather than degrading

        A reply of the wrong length means this decoder and the script disagree about the contract.
        Building a decision out of whatever happened to land in the right slots would produce a
        confident, wrong answer — a caller allowed because a quota field landed in the ``allowed``
        position. C4 and C8 classify the raised :class:`ValueError` as a failure and take the
        configured fail-open / fail-closed path, which is a *known* state with a header on it.

        Raises:
            ValueError: on a reply that is not a sequence, is not exactly
                :data:`LUA_REPLY_ARITY` elements long, or carries a non-integer where an integer
                belongs / an unknown enum value.
        """
        try:
            fields = list(raw)
        except TypeError as exc:
            raise ValueError(
                f"lua reply must be a sequence of {LUA_REPLY_ARITY} elements "
                f"{LUA_REPLY_FIELDS}, got {type(raw).__name__}"
            ) from exc

        if len(fields) != LUA_REPLY_ARITY:
            raise ValueError(
                f"lua reply must carry exactly {LUA_REPLY_ARITY} elements "
                f"{LUA_REPLY_FIELDS}, got {len(fields)}"
            )

        (
            raw_allowed,
            raw_reason,
            raw_tier,
            raw_bucket_limit,
            raw_bucket_remaining,
            raw_bucket_reset_ms,
            raw_window_limit,
            raw_window_used,
            raw_window_reset_ms,
            raw_daily_limit,
            raw_daily_used,
            raw_daily_expire_at,
            raw_daily_state,
            raw_monthly_limit,
            raw_monthly_used,
            raw_monthly_expire_at,
            raw_monthly_state,
            raw_retry_ms,
            raw_now_ms,
        ) = fields

        allowed = bool(_as_int(raw_allowed, field="allowed"))

        # ceil, never floor — see `ceil_seconds`. And the >= 1 floor on a denial is applied here,
        # at the decode boundary, so a `retry_after_sec` of 0 on a denied decision cannot exist in
        # this process at all rather than being caught later by whoever remembers to clamp.
        retry_after_sec = ceil_seconds(_as_int(raw_retry_ms, field="retry_ms"))
        if not allowed:
            retry_after_sec = max(1, retry_after_sec)

        return cls(
            allowed=allowed,
            reason=_as_enum(DenyReason, raw_reason, field="reason"),
            tier=_as_text(raw_tier),
            user_id=user_id,
            endpoint=endpoint,
            cost=cost,
            bucket_limit=_as_int(raw_bucket_limit, field="bucket_limit"),
            bucket_remaining=_as_int(raw_bucket_remaining, field="bucket_remaining"),
            bucket_reset_sec=ceil_seconds(_as_int(raw_bucket_reset_ms, field="bucket_reset_ms")),
            window_limit=_as_int(raw_window_limit, field="window_limit"),
            window_used=_as_int(raw_window_used, field="window_used"),
            window_reset_sec=ceil_seconds(_as_int(raw_window_reset_ms, field="window_reset_ms")),
            daily_limit=_as_int(raw_daily_limit, field="daily_limit"),
            daily_used=_as_int(raw_daily_used, field="daily_used"),
            daily_reset_at=_as_int(raw_daily_expire_at, field="daily_expire_at"),
            daily_state=_as_enum(QuotaPeriodState, raw_daily_state, field="daily_state"),
            monthly_limit=_as_int(raw_monthly_limit, field="monthly_limit"),
            monthly_used=_as_int(raw_monthly_used, field="monthly_used"),
            monthly_reset_at=_as_int(raw_monthly_expire_at, field="monthly_expire_at"),
            monthly_state=_as_enum(QuotaPeriodState, raw_monthly_state, field="monthly_state"),
            retry_after_sec=retry_after_sec,
            # A reply exists, so Redis answered: this decision is authoritative by construction.
            # Only C8's fallback path builds a degraded decision, and it does not come through here.
            degraded=False,
            server_now_ms=_as_int(raw_now_ms, field="now_ms"),
            latency_ms=latency_ms,
        )


# ---------------------------------------------------------------------------------------------
# Admin / stats wire shapes (C10, C11)
#
# Only what those commits need and what is cheap to declare now. `StatsSnapshot` is deliberately
# absent: its shape is driven by what C9's analytics collector actually records, and declaring it
# early would mean guessing a schema and then either living with the guess or breaking it.
# ---------------------------------------------------------------------------------------------


class TierUpdate(BaseModel):
    """Body of ``PUT /api/v1/admin/tiers/{tier}`` — a partial change to one tier's sizing.

    Every field is optional so an operator can raise a single number mid-incident without
    restating the other three (and without a race in which they restate a value another operator
    changed thirty seconds ago). At least one must be present: an empty body would bump
    ``config:version``, invalidate every replica's snapshot and change nothing, which is an
    expensive way to do nothing.

    ``gt=0`` on all four, and it is a **safety** constraint rather than input hygiene. A tier
    written with a ``0`` or negative limit is a tier whose gate stops binding — the decision script
    reads a non-positive limit as "not enforced", so ``{"daily_quota": 0}`` would quietly grant
    that tier an unlimited daily allowance through an endpoint whose whole purpose is tightening
    limits. A 422 naming the field is the correct answer; silently unlimited is not.

    ``extra="forbid"`` so ``{"rate_limit": 10}`` (a plausible misspelling of
    ``rate_limit_per_min``) is a 422 rather than a 200 that changed nothing — the worst possible
    outcome for an operator who believes they have just lowered a limit.
    """

    model_config = ConfigDict(extra="forbid")

    rate_limit_per_min: int | None = Field(
        default=None, gt=0, description="New sustained requests-per-minute ceiling for this tier."
    )
    burst: int | None = Field(
        default=None, gt=0, description="New token-bucket capacity per (user, endpoint)."
    )
    daily_quota: int | None = Field(
        default=None, gt=0, description="New cumulative requests allowed per UTC day."
    )
    monthly_quota: int | None = Field(
        default=None, gt=0, description="New cumulative requests allowed per UTC month."
    )

    @model_validator(mode="after")
    def _at_least_one_change(self) -> TierUpdate:
        """Refuse a body that asks for nothing. See the class docstring.

        Checks the **values** rather than ``model_fields_set``, so an explicit
        ``{"burst": null}`` is rejected too: it is a body that names a field and then changes
        nothing, which is exactly the "I thought I had lowered a limit" failure this guard exists
        to catch.
        """
        supplied = (self.rate_limit_per_min, self.burst, self.daily_quota, self.monthly_quota)
        if all(value is None for value in supplied):
            raise ValueError(
                "supply at least one of rate_limit_per_min, burst, daily_quota, monthly_quota "
                "— an empty update would bump config:version and change nothing"
            )
        return self

    def apply_to(self, base: TierConfig) -> TierConfig:
        """Return ``base`` with this update's supplied fields replaced.

        The merge rule lives here rather than in the admin handler so that "a PUT is a partial
        update" has exactly one implementation. ``base`` is frozen, so this is a new object; the
        tier the rest of the process is currently enforcing is untouched until the caller stores
        the result.
        """
        return TierConfig(
            name=base.name,
            rate_limit_per_min=(
                base.rate_limit_per_min
                if self.rate_limit_per_min is None
                else self.rate_limit_per_min
            ),
            burst=base.burst if self.burst is None else self.burst,
            daily_quota=base.daily_quota if self.daily_quota is None else self.daily_quota,
            monthly_quota=(
                base.monthly_quota if self.monthly_quota is None else self.monthly_quota
            ),
        )


class UserTierUpdate(BaseModel):
    """Body of ``PUT /api/v1/admin/users/{user_id}/tier`` — move a principal to another tier.

    ``tier`` is a ``str`` and not the :class:`Tier` enum, deliberately. Tiers are runtime data:
    C10 can create one, and an enum here would make the API reject a tier that the very same admin
    surface had just defined. The handler validates the name against the **live** tier table
    instead, which is the only check that can be correct at the moment it runs.

    This is the half of hot reload that is instant. ``user -> tier`` is read *inside* the decision
    script, so this change takes effect on the very next request on **every** replica; the 5-second
    snapshot TTL bounds only how long it takes a change to what a tier *means* to propagate.
    """

    model_config = ConfigDict(extra="forbid")

    tier: str = Field(
        min_length=1,
        max_length=64,
        description="Name of the tier to assign. Must exist in the live `config:tiers` table.",
    )

    @field_validator("tier")
    @classmethod
    def _strip(cls, value: str) -> str:
        """Trim surrounding whitespace, and refuse a value that was only whitespace.

        The name becomes a HASH field name and is compared against the live table; ``"free "``
        would match nothing and the principal would fall through to ``DEFAULT_TIER`` — a silent
        demotion produced by a trailing space in a curl command. Normalise where the value enters,
        so the value validated and the value stored are the same one.
        """
        stripped = value.strip()
        if not stripped:
            raise ValueError("tier must not be blank")
        return stripped


class QuotaUsage(BaseModel):
    """One quota period's state — the shape used for both ``daily`` and ``monthly``.

    ``remaining`` carries the :data:`UNLIMITED` (``-1``) sentinel for an unenforced period, exactly
    as :attr:`LimitDecision.daily_remaining` does; ``reset_at`` is absolute unix seconds, matching
    ``X-Quota-Reset``.
    """

    limit: int = Field(description="Ceiling for the period; <= 0 means the period is unenforced.")
    used: int = Field(description="Requests consumed in the period so far.")
    remaining: int = Field(description="limit - used, floored at 0; -1 when unenforced.")
    reset_at: int = Field(description="Unix seconds at which this period's counter expires.")
    state: QuotaPeriodState = Field(description="reset | active | exhausted.")


class UserUsage(BaseModel):
    """Body of ``GET /api/v1/admin/users/{user_id}/usage`` — both quota periods for one principal.

    This is what the C13 verifier reads to prove, **from outside the process**, that a rejected
    request burned neither a token nor quota: it fires a known number of requests, counts the
    allowed ones, and asserts ``daily.used == allowed`` rather than ``== total``. An in-process
    assertion could only prove the code believes its own bookkeeping.
    """

    user_id: str = Field(description="The principal these counters belong to.")
    tier: str = Field(description="Tier currently recorded for the principal in `user:{id}`.")
    daily: QuotaUsage = Field(description="Current UTC day.")
    monthly: QuotaUsage = Field(description="Current UTC month.")
