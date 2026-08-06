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
    "DashboardStats",
    "DegradedSignals",
    "DenyReason",
    "DroppedSignals",
    "ERROR_QUOTA",
    "ERROR_RATE_LIMIT",
    "LUA_REPLY_ARITY",
    "LUA_REPLY_FIELDS",
    "LimitDecision",
    "PoolSignals",
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
    "ReplicaInfo",
    "StatsBucket",
    "StatsSnapshot",
    "StatsTotals",
    "StatsWindow",
    "Tier",
    "TierConfig",
    "TierUpdate",
    "TopConsumer",
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
    ``unenforced``
        There is **no ceiling on this period at all** — either the tier declares one of the
        :data:`UNLIMITED` limits (``limit <= 0``) or the period is switched off entirely
        (``QUOTA_DAILY_ENABLED=false``, which reaches the decision script as an ``EXPIREAT`` of 0
        and stops the counter being read or written).

        This member exists because the alternative was reporting ``reset``, and ``reset`` is a
        *claim*: it says a period boundary has just rolled over. For a period that has no boundary
        that is simply false, and it is false in the direction that invites a client to build a
        "your quota just refreshed" display on top of a quota nobody is counting. The condition is
        exactly the one under which :attr:`LimitDecision.daily_remaining` reports
        :data:`UNLIMITED` and :meth:`LimitDecision.headers` omits the ``X-Quota-*`` headers, so all
        three now agree rather than two of them agreeing and the third saying something else.
    """

    RESET = "reset"
    ACTIVE = "active"
    EXHAUSTED = "exhausted"
    UNENFORCED = "unenforced"


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

    def _rate_limit_reset(self) -> int:
        """``X-RateLimit-Reset`` in delay-seconds: the window's recovery, or the bucket's.

        .. rubric:: Why there is a fallback at all

        The account-wide window is an **operability switch** (``SLIDING_WINDOW_ENABLED``), which
        means it gets flipped during an incident — precisely when a client-side retry storm is
        least affordable. With the gate off, the decision script reports the tier's per-minute
        number as ``window_limit`` (that is still what the caller's plan says) but has no window to
        report a recovery for, so ``window_reset_sec`` is 0.

        Emitting ``X-RateLimit-Reset: 0`` there is the same bug ``Retry-After: 0`` would be,
        arriving through a different header: a caller looking at ``Limit: 60, Remaining: 0,
        Reset: 0`` retries immediately, is refused by the *bucket* — whose real recovery was five
        seconds away — and loops. A limiter that manufactures its own retry storm is worse than one
        that advertises nothing.

        So when the window has no recovery to report, the bucket's own recovery is reported
        instead. It is the gate that is actually refusing the request at that point, so it is the
        honest number, and it is never zero while the bucket is short of capacity.

        .. rubric:: Why the guard lives here and not in the script

        The script's job is to report **raw per-gate facts**: gate 2 was not consulted, therefore
        gate 2 has no reset. Which of those facts becomes which header is presentation, and
        presentation belongs to the layer that emits headers. Folding the fallback into Lua would
        make ``window_reset_ms`` mean "the window's reset, except sometimes the bucket's", which is
        the kind of field nobody can reason about six months later.

        :attr:`effective_remaining` already carries the mirror-image guard for
        ``window_limit <= 0``; this is the same idea applied to the other half of the pair.

        A non-positive value from *both* gates floors at 0 — that path is reachable only from a
        hand-built decision (C8's fallback), and a negative delay is not a number any HTTP client
        knows what to do with.
        """
        if self.window_reset_sec > 0:
            return self.window_reset_sec
        return max(0, self.bucket_reset_sec)

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
        ``X-RateLimit-Reset`` = ``window_reset_sec``, falling back to ``bucket_reset_sec``
            The **account-wide sliding window's** reset, in **delay-seconds** — deliberately *not*
            ``bucket_reset_sec`` while that window is running. ``X-RateLimit-Limit`` is the tier's
            per-minute number, which is the window's ceiling, so the matching ``Reset`` has to be
            the window's too: a ``Limit``/``Reset`` pair describing two different gates is worse
            than either one alone, because the obvious client behaviour — "I am at my limit, so
            sleep until Reset" — would then sleep for the wrong gate's recovery and wake into
            another 429. The bucket's own recovery is on the decision as ``bucket_reset_sec`` for
            anyone who needs it.

            When the window gate is switched off there is no window recovery to report, and the
            bucket's is emitted instead rather than a zero — see :meth:`_rate_limit_reset`. See the
            module docstring for the unit asymmetry with ``X-Quota-Reset``.
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
            RATELIMIT_RESET_HEADER: str(self._rate_limit_reset()),
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
        position.

        **C8 lets the raised :class:`ValueError` propagate** rather than routing it through
        ``FAIL_MODE``, and that is the same call :mod:`src.redis_client` makes for a
        ``ResponseError``: an arity or enum mismatch is a *bug in this service*, it fails on every
        single request rather than transiently, and dressing it up as a degradation would mean a
        one-element edit to the script silently disabled rate limiting everywhere while
        ``/health`` reported it identically to an unplugged Redis. It becomes a 500 — visible and
        attributable — which is the correct answer when the service is the broken thing.
        ``tests/unit/test_limiter.py`` pins both directions.

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
# Admin / stats wire shapes (C9, C10, C11)
#
# `StatsSnapshot` and its parts were deliberately absent until C9, because their shape is driven
# by what the analytics collector actually records and declaring them earlier would have meant
# guessing a schema and then either living with the guess or breaking it. C9 records, so C9
# declares.
#
# What is declared here is the *measurement*, and nothing else. C11's `GET /dashboard/api/stats`
# wraps this in a response envelope that also carries `tiers`, `config_version`, `poll_ms`,
# `replicas` and the degradation counters — none of which come from the analytics buckets, and all
# of which would make this model a description of one HTTP route rather than of a measurement that
# the admin API and the E2E verifier read too.
# ---------------------------------------------------------------------------------------------


class StatsTotals(BaseModel):
    """The whole window, folded: how much traffic, how much load, and how it ended.

    ``requests`` counts requests; ``cost`` counts **weighted** units. Both, because they are
    genuinely different numbers once endpoints are priced — 100 calls to ``/whoami`` and 20 to
    ``/logs/query`` are the same 100 units of load and a 5x different request count, and a
    dashboard showing only one of them cannot tell a busy caller from an expensive one.

    .. rubric:: ``cost`` is weight ATTEMPTED, not weight charged. Label a chart accordingly

    A refused request contributes its full weight here and **nothing** to the quota counter,
    because a denial writes nothing (C4's founding property). The series is therefore a measure of
    *demand*: a caller hammering the 5-token endpoint is generating five times the load of one
    hammering ``/whoami`` whether or not the limiter admits them, and recording refusals at cost 1
    would make the most expensive endpoint look like the cheapest exactly as it started being
    throttled.

    The reconciliation caveat that follows from it, stated because someone will otherwise spend an
    afternoon on it: this number does **not** match ``GET /admin/users/{id}/usage``'s
    ``daily.used``, and it is not supposed to. The difference between the two *is* the throttled
    demand, and during a throttling event it can be several times the charged figure. A C11 panel
    titled "cost consumed" would be wrong; "cost attempted" is the honest label.

    ``allowed`` / ``denied`` / ``degraded`` are lifted out of
    :attr:`StatsSnapshot.by_outcome` rather than left only in that map. The map is the raw
    dimension and stays authoritative (it survives an outcome name this model has never heard of);
    these three are the ones every consumer wants by name, and making a chart guess at dictionary
    keys is how a rename becomes a silently empty graph. The three partition the traffic, so
    ``allowed + denied + degraded == requests`` for any window this service wrote in full.
    """

    requests: int = Field(description="Requests folded into the window.")
    cost: int = Field(
        description=(
            "Weighted cost units ATTEMPTED across the window — refused requests included. Not "
            "reconcilable against `daily.used`; see the rubric above."
        )
    )
    allowed: int = Field(description="Requests admitted by an authoritative (Redis) decision.")
    denied: int = Field(
        description="Requests refused — 429, 401 and the fail-closed/overloaded 503s."
    )
    degraded: int = Field(
        description="Requests decided by the local fallback bucket rather than by Redis."
    )


class StatsBucket(BaseModel):
    """One time bucket, folded — the element type of ``per_minute`` and ``per_hour``.

    Deliberately **light**: five counters and two timestamps, with no per-dimension maps. A window
    of 120 buckets each carrying four dictionaries would be the largest thing this service
    serialises, on the endpoint polled every 5 seconds, to draw a line chart that needs one number
    per point. The dimensional breakdown is folded once for the whole window instead
    (:attr:`StatsSnapshot.by_status` and friends).

    ``start_ms`` and ``width_ms`` are carried rather than left for the client to recompute from
    ``index``. The multiplier differs per series (60 000 vs 3 600 000), so a client that derived it
    would need to know which series it was holding — and a chart that multiplies by the wrong
    constant plots the right numbers in the wrong century.
    """

    index: int = Field(description="Bucket index: epoch_ms // width_ms.")
    start_ms: int = Field(description="Unix milliseconds at which this bucket opens.")
    width_ms: int = Field(description="Bucket width: 60_000 for a minute, 3_600_000 for an hour.")
    requests: int = Field(description="Requests folded into this bucket.")
    cost: int = Field(
        description="Weighted cost units attempted in this bucket, refused requests included."
    )
    allowed: int = Field(description="Of those requests, how many were admitted.")
    denied: int = Field(description="Of those requests, how many were refused.")
    degraded: int = Field(description="Of those requests, how many were decided without Redis.")


class TopConsumer(BaseModel):
    """One entry of the top-consumers ranking, ordered by **cost** rather than request count.

    Ranking by cost is the point of the ZSET: a caller making 20 requests to a 5-token endpoint is
    a heavier consumer than one making 60 to a 1-token endpoint, and a ranking by request count
    would put the cheap caller on top and send an operator after the wrong client.
    """

    user_id: str = Field(description="The principal, or the anonymous sentinel for a 401 flood.")
    cost: int = Field(description="Weighted cost units attributed to this principal.")


class StatsWindow(BaseModel):
    """What the snapshot **actually covered** — never what was asked for.

    The distinction is the entire reason this model exists. A read that was truncated by
    ``ANALYTICS_MAX_BUCKETS`` and one that covered everything produce the same-shaped payload, and
    without the two ``*_requested`` fields sitting next to the two ``*_covered`` ones, a partial
    answer reads as a complete one: a dashboard would render 30 minutes of history under a heading
    that says 120 and nobody would ever know the difference. Reporting both makes the truncation a
    fact on the wire rather than something a reader has to infer from a log line on a replica they
    are not looking at.

    The index and millisecond bounds are ``None`` — not ``0``, and not ``-1`` — when a series
    covers no buckets at all. ``0`` is a real minute index (the unix epoch) and a real instant, so
    reusing it would mean "we covered nothing" and "we covered 1970" were the same payload. This is
    the one place the project's integer-sentinel convention (:data:`UNLIMITED`) does not apply:
    that sentinel exists because a *quantity* has to survive being rendered into a header, and
    these fields never touch one.

    .. rubric:: Which bound a chart should actually use — there are three answers and they differ

    ``start_ms`` / ``end_ms`` span **both** series, and that is a genuine hazard for a consumer
    that assumes otherwise. :attr:`StatsSnapshot.totals` and every ``by_*`` are folded from the
    *minute* buckets alone, so on the default 60-minute + 24-hour request these two fields describe
    a 24-hour period while every KPI beside them describes the last hour. A tile labelled
    "requests in {start_ms}-{end_ms}" is then wrong by 24x, and it is wrong in the direction that
    looks plausible.

    So each series also publishes its own pair:

    * ``minutes_start_ms`` / ``minutes_end_ms`` — the period ``totals`` and every ``by_*``
      **actually** describe, and the correct x-axis domain for :attr:`StatsSnapshot.per_minute`.
    * ``hours_start_ms`` / ``hours_end_ms`` — the same for :attr:`StatsSnapshot.per_hour`.
    * ``start_ms`` / ``end_ms`` — the union, i.e. "what period does this whole payload touch".
      Useful as a caption, wrong as a KPI label.

    Every ``*_end_ms`` is the instant the newest covered bucket **closes**, not "now". The newest
    bucket is still filling, so an end bound is always in the future relative to the read — by up
    to 60 s for the minute series and up to an hour for the hour series (measured at 52 minutes on
    the default request, which is what makes an hour-spanned axis render an hour of empty future).
    That is the right domain for a bar chart, because the in-progress bar is a full-width bar with
    a partial value; it is the wrong number to *print*.

    ``server_now_ms`` is the line to clip at, and it is the read's own instant from **Redis's**
    clock — the same ``TIME`` the write side buckets against, so two replicas answering the same
    poll name the same instant. Null only when no clock was read at all: an empty range, or a store
    that could not be reached.
    """

    minutes_requested: int = Field(
        description="Minute buckets the caller asked for — their ask, never a server-side "
        "ceiling. Greater than `minutes_covered` exactly when this payload is partial."
    )
    minutes_covered: int = Field(description="Minute buckets actually read.")
    hours_requested: int = Field(
        description="Hour buckets the caller asked for, on the same terms as `minutes_requested`."
    )
    hours_covered: int = Field(description="Hour buckets actually read.")
    newest_minute_index: int | None = Field(
        default=None, description="Index of the most recent minute bucket read; null if none."
    )
    oldest_minute_index: int | None = Field(
        default=None, description="Index of the oldest minute bucket read; null if none."
    )
    newest_hour_index: int | None = Field(
        default=None, description="Index of the most recent hour bucket read; null if none."
    )
    oldest_hour_index: int | None = Field(
        default=None, description="Index of the oldest hour bucket read; null if none."
    )
    minutes_start_ms: int | None = Field(
        default=None,
        description="Unix ms at which the oldest covered MINUTE bucket opens. **This is the "
        "period `totals` and every `by_*` describe** — label KPI tiles from this pair, not from "
        "`start_ms`/`end_ms`, which span both series.",
    )
    minutes_end_ms: int | None = Field(
        default=None,
        description="Unix ms at which the newest covered minute bucket CLOSES — up to 60 s ahead "
        "of `server_now_ms`, because that bucket is still filling. Correct as an x-axis domain "
        "for `per_minute`; clip at `server_now_ms` before printing it as a time.",
    )
    hours_start_ms: int | None = Field(
        default=None, description="Unix ms at which the oldest covered HOUR bucket opens."
    )
    hours_end_ms: int | None = Field(
        default=None,
        description="Unix ms at which the newest covered hour bucket closes — up to an hour ahead "
        "of `server_now_ms`, for the same reason.",
    )
    start_ms: int | None = Field(
        default=None,
        description="Unix ms at which the earliest covered bucket opens, across BOTH series. The "
        "payload's outer extent, not the period any KPI describes.",
    )
    end_ms: int | None = Field(
        default=None,
        description="Unix ms at which the latest covered bucket closes, across BOTH series. On "
        "the default request this is the end of the current HOUR, so it can sit an hour in the "
        "future — see the rubric before plotting against it.",
    )
    server_now_ms: int | None = Field(
        default=None,
        description="Redis's clock at the instant of the read — the shared clock the write side "
        "buckets against, so every replica answering the same poll reports the same value. The "
        "line to clip a still-filling newest bucket at. Null when no clock was read: an empty "
        "range, or an unreachable store.",
    )


class StatsSnapshot(BaseModel):
    """Everything :meth:`src.analytics.AnalyticsCollector.snapshot` read, in one JSON-ready value.

    C11's ``GET /dashboard/api/stats`` is a thin wrapper over this: it adds the response envelope
    (``tiers``, ``config_version``, ``poll_ms``, ``replicas``) and serialises the rest untouched.
    That is the shape this model is sized for — enough that the endpoint has nothing left to
    compute, and no more, so the same value is equally usable from the admin API and the E2E
    verifier without either of them parsing a page's worth of chart state.

    .. rubric:: ``totals`` and every ``by_*`` dimension are folded from the MINUTE buckets only

    Not from both series. The hour buckets describe the same requests at a coarser resolution, so
    folding them in as well would count every request twice — and inconsistently, because the hour
    window reaches further back than the minute window, so the double-counting would apply to some
    of the traffic and not the rest. The per-hour series is the long-tail context line and nothing
    else; :attr:`per_hour` is where it lives.

    The consequence worth stating: a caller who asks for ``hours`` and **no** ``minutes`` gets a
    populated ``per_hour`` and zeroed totals. That is honest rather than convenient — the totals
    describe the per-minute window, and inventing them from a different series when the first one
    is empty would make the field mean two things depending on the arguments.

    .. rubric:: Both series run OLDEST FIRST

    :func:`src.keys.recent_minute_indices` returns newest-first, because element zero of *that*
    list is "the minute happening now". A **time series** wants the opposite: a chart draws left to
    right, so ``per_minute[0]`` is the oldest point and ``per_minute[-1]`` is now. The reversal
    happens once, here, rather than in every consumer.
    """

    totals: StatsTotals = Field(description="The per-minute window, folded.")
    per_minute: list[StatsBucket] = Field(
        default_factory=list, description="Minute series, oldest first."
    )
    per_hour: list[StatsBucket] = Field(
        default_factory=list, description="Hour series, oldest first."
    )
    by_status: dict[str, int] = Field(
        default_factory=dict,
        description="HTTP status code (as a string) -> request count, over the minute window.",
    )
    by_endpoint: dict[str, int] = Field(
        default_factory=dict,
        description="Classified endpoint label -> request count, over the minute window.",
    )
    by_tier: dict[str, int] = Field(
        default_factory=dict,
        description="Tier name -> request count, over the minute window.",
    )
    by_outcome: dict[str, int] = Field(
        default_factory=dict,
        description="allowed | denied | degraded -> request count, over the minute window.",
    )
    top_consumers: list[TopConsumer] = Field(
        default_factory=list, description="Heaviest principals by cost, highest first."
    )
    window: StatsWindow = Field(description="What range this snapshot actually covered.")
    dropped: int = Field(
        default=0,
        description=(
            "Buckets asked for and not read, because ANALYTICS_MAX_BUCKETS capped the fan-in "
            "(or the range ran off the start of the epoch). Non-zero means this payload is "
            "partial."
        ),
    )
    buckets_read: int = Field(
        default=0, description="Time buckets actually pipelined and folded (minutes + hours)."
    )


# ---------------------------------------------------------------------------------------------
# The C11 envelope
#
# `StatsSnapshot` above is the MEASUREMENT — what the analytics buckets said. Everything below is
# the ENVELOPE `GET /dashboard/api/stats` wraps it in: the configuration the measurement has to be
# read against (`tiers`, `config_version`, `rate_limit_enabled`, `poll_ms`) and the health of the
# machinery that produced it (`degraded`, `pool`, `dropped`, `replicas`).
#
# Declared here rather than beside the route — unlike `HealthResponse` and C10's `TierTable`, which
# live next to their handlers — because C13's verifier and C15's page both parse this shape, and
# `StatsSnapshot`'s own rubric already promises "C11 wraps this in a response envelope". The two
# halves are one contract and are read together.
#
# The envelope is FLAT rather than `{"stats": {...}, "meta": {...}}`. A dashboard tile showing "0
# requests" is only interpretable next to `rate_limit_enabled`, and a nesting that let a client
# fetch, cache or render one half without the other would be a nesting that lets it draw the empty
# chart without the field that explains it. See `DashboardStats.rate_limit_enabled`.
# ---------------------------------------------------------------------------------------------


class ReplicaInfo(BaseModel):
    """Which replicas this payload can honestly name — and, more importantly, which it cannot.

    The analytics record carries six dimensions (requests, cost, outcome, status, endpoint, tier)
    and **no replica dimension**. So a per-replica breakdown cannot be computed from the buckets,
    and this model reports that rather than inventing one: :attr:`observed` is empty, and
    :attr:`attributed` says why in a field a UI can branch on.

    Fabricating the number was the tempting alternative and would have been worse than useless.
    ``configured`` (``API_REPLICAS``) is what an operator *declared*, and rendering it as "2
    replicas serving" would state as measurement the one thing this payload has no evidence for —
    on the surface an operator opens precisely to find out whether a replica has stopped.
    """

    model_config = ConfigDict(frozen=True)

    served_by: str = Field(
        description="Hostname of the replica that built this payload. Same value as the "
        "envelope's top-level `served_by`; repeated here so this block is self-contained."
    )
    configured: int = Field(
        description="API_REPLICAS — how many replicas are *declared* to share the store. It sizes "
        "the C8 degraded fallback bucket; it is NOT evidence that that many are serving."
    )
    observed: list[str] = Field(
        default_factory=list,
        description="Replicas the analytics data itself names. Always empty today — see "
        "`attributed`.",
    )
    attributed: bool = Field(
        default=False,
        description="False: the recorded bucket fields carry no replica dimension, so `observed` "
        "cannot be populated from them. With C12's load balancer in front, polling this endpoint "
        "repeatedly and collecting `served_by` is what reveals the other replicas.",
    )


class DegradedSignals(BaseModel):
    """Whether anything between the traffic and this payload is currently not working.

    Four different failures, four fields, for the reason ``GET /health`` keeps ``rate_limiter``,
    ``redis`` and ``pool`` apart: they have different remedies, they occur independently, and one
    flag could only ever report whichever happened to be checked first.

    :attr:`stats_unavailable` is the field with no analogue on ``/health``, and it is the one that
    makes the rest of the payload readable. Every counter in a snapshot that could not be read is
    ``0`` — and a zero that means "we could not ask" renders identically to a zero that means
    "nothing happened", which is the single most misleading thing an observability surface can say
    during an incident. :meth:`src.analytics.AnalyticsCollector.snapshot` refuses to return those
    zeros at all (it raises); this flag is how the endpoint is able to serve them anyway without
    lying about what they are.
    """

    model_config = ConfigDict(frozen=True)

    rate_limiter: bool = Field(
        description="True while the C8 local fallback bucket is carrying this replica's traffic — "
        "the same condition `/health` reports as `rate_limiter: \"degraded\"`. Enforcement is "
        "replica-local and approximate while this is true."
    )
    store: str = Field(
        description="'ok', 'unreachable', or 'saturated' when this replica had no pooled "
        "connection to ask with — so the store's health is genuinely unknown rather than bad."
    )
    stats_unavailable: bool = Field(
        description="True when the analytics read failed. Every measurement in this payload is "
        "then a zero that was NOT measured: totals, both series and every `by_*` are unknown, not "
        "empty. The configuration fields (`tiers`, `config_version`, `rate_limit_enabled`, "
        "`poll_ms`) are still true — they never needed the store."
    )
    since_sec: float | None = Field(
        default=None,
        description="Seconds this replica has been failing to reach the store, or null while "
        "healthy. A duration from a monotonic clock, never a date.",
    )
    breaker: str = Field(
        description="Circuit-breaker state: 'closed', 'open' or 'half_open'. 'open' means this "
        "replica is refusing store calls without dialling — including this endpoint's read."
    )
    detail: str | None = Field(
        default=None,
        description="Why the analytics read failed, if it did. Null otherwise.",
    )


class PoolSignals(BaseModel):
    """This replica's own connection capacity — a different incident from a store outage.

    Separate from :class:`DegradedSignals` for the reason ``/health`` gives ``pool`` its own field:
    a saturated pool means the store is fine and *this process* is the bottleneck, which is fixed
    by adding connections or shedding load rather than by waiting for Redis. Reporting it as an
    outage sends an operator to debug the wrong machine.
    """

    model_config = ConfigDict(frozen=True)

    state: str = Field(description="'ok' or 'saturated'. Cleared by the next successful call.")
    max_connections: int = Field(description="REDIS_MAX_CONNECTIONS — the bound being hit.")
    overloads: int = Field(
        description="Calls refused because this process could not get a connection out of its own "
        "pool. Non-zero next to a null `degraded.since_sec` means the store is healthy and this "
        "replica ran out of connections to it."
    )
    overloaded_for_sec: float | None = Field(
        default=None, description="Seconds in the current saturation run, or null."
    )


class DroppedSignals(BaseModel):
    """The two ways this payload can be incomplete, side by side.

    They are unrelated failures that produce the same symptom — a chart that under-reports — so
    they are counted separately and published together:

    * :attr:`buckets` is about **this read**: buckets asked for and not pipelined, because
      ``ANALYTICS_MAX_BUCKETS`` capped the fan-in. Recoverable by asking for a smaller window.
    * :attr:`records` is about **the write path**, over this process's lifetime: requests that
      were served and never made it into a bucket. Not recoverable at all — that traffic is gone
      from every chart on this page.

    The second is why this block exists. :meth:`src.analytics.AnalyticsCollector.record` swallows
    every exception by design (it runs after the response is already on the wire, so there is
    nothing left for a failure to usefully do), which means **a collector that has recorded nothing
    for an hour looks exactly like one that is working**. These counters are the only place that
    difference is visible, and they are only meaningful next to the request rate on the same
    payload — which is the argument ``src/api/health.py`` makes for keeping them off the liveness
    probe and publishing them here instead.
    """

    model_config = ConfigDict(frozen=True)

    buckets: int = Field(
        description="Time buckets this read asked for and did not cover. Non-zero means the "
        "window below is PARTIAL; compare `window.minutes_requested` with `minutes_covered`."
    )
    records: int = Field(
        description="Requests whose analytics record was lost, since process start. "
        "`records + records_written` is every attempt, so the ratio answers 'what fraction of my "
        "traffic is actually on this graph?'."
    )
    records_written: int = Field(
        description="Requests successfully folded into a bucket, since process start."
    )
    errors: int = Field(
        description="Of the drops, how many were an exception the collector swallowed."
    )
    shed: int = Field(
        description="Of the drops, how many were shed to protect the connection pool — a record "
        "arriving while the in-flight gate was full. Rising `shed` beside a flat `errors` means "
        "load and a limiter that is winning the contention, which is the system working as "
        "designed rather than failing."
    )
    last_error: str | None = Field(
        default=None,
        description="repr of the most recent swallowed record exception, or null. Somewhere to "
        "look that is not 'grep the logs of whichever replica it was'.",
    )


class DashboardStats(BaseModel):
    """Body of ``GET /dashboard/api/stats`` — the measurement plus everything needed to read it.

    :class:`StatsSnapshot`'s nine fields are flattened in verbatim (they are *not* nested under a
    ``stats`` key — see the rubric above), and the envelope adds the configuration and health
    context that turns them from numbers into a diagnosis.

    .. rubric:: ``cost`` is weight ATTEMPTED. Any chart built from it is "attempted", never
       "consumed"

    Stated here as well as on :class:`StatsTotals` because this is the model a UI author reads. A
    refused request contributes its full weight to ``totals.cost`` and **nothing** to the quota
    counter, so this series measures demand rather than spend: during a throttling event it
    over-reports relative to what was charged, and it will not reconcile against
    ``GET /api/v1/admin/users/{id}/usage``'s ``daily.used``. That gap is not an error to be
    explained away — **the difference between the two IS the throttled demand**, which is the most
    interesting number on the page. A tile labelled "cost consumed" would be wrong.
    """

    model_config = ConfigDict(frozen=True)

    # -- the measurement, flattened from StatsSnapshot ------------------------------------- #
    totals: StatsTotals = Field(
        description="The per-minute window, folded. `cost` is weight ATTEMPTED — label any chart "
        "built from it 'attempted', never 'consumed'; it does not reconcile against `daily.used`."
    )
    per_minute: list[StatsBucket] = Field(
        default_factory=list, description="Minute series, oldest first — the live chart."
    )
    per_hour: list[StatsBucket] = Field(
        default_factory=list,
        description="Hour series, oldest first — the long-tail context line. It describes the "
        "SAME requests at a coarser resolution and is therefore folded into nothing above.",
    )
    by_status: dict[str, int] = Field(
        default_factory=dict, description="HTTP status (as a string) -> requests, minute window."
    )
    by_endpoint: dict[str, int] = Field(
        default_factory=dict, description="Classified endpoint label -> requests, minute window."
    )
    by_tier: dict[str, int] = Field(
        default_factory=dict, description="Tier -> requests, minute window."
    )
    by_outcome: dict[str, int] = Field(
        default_factory=dict,
        description="allowed | denied | degraded -> requests, minute window. The three partition "
        "the traffic, so a rejection *rate* can be computed without knowing every value.",
    )
    top_consumers: list[TopConsumer] = Field(
        default_factory=list,
        description="Heaviest principals by attempted cost, highest first. **This block is the "
        "documented hole**: it exposes user ids on an unauthenticated endpoint, and in a real "
        "deployment it is what goes behind ADMIN_TOKEN. Approximate by construction — see "
        "`AnalyticsCollector._rank`.",
    )
    window: StatsWindow = Field(
        description="What this read actually covered, against what the caller asked for. The two "
        "differ whenever ANALYTICS_MAX_BUCKETS truncated the fan-in, and `dropped.buckets` counts "
        "the difference. **Plot and label the KPIs above from `minutes_start_ms`/`minutes_end_ms`, "
        "not from `start_ms`/`end_ms`** — the latter span both series, so on the default request "
        "they describe 24 h while every number above describes 60 minutes. See the model."
    )

    # -- configuration the measurement has to be read against ------------------------------ #
    tiers: dict[str, TierConfig] = Field(
        description="The tier table THIS replica is enforcing right now (its in-process snapshot, "
        "not a fresh read of `config:tiers`) — so a chart's per-tier bars sit next to the limits "
        "that produced them."
    )
    config_version: int = Field(
        description="`config:version` behind that snapshot. 0 means no snapshot has been read "
        "from Redis yet and the configured defaults are in force."
    )
    rate_limit_enabled: bool = Field(
        description="RATE_LIMIT_ENABLED. **Mandatory, and load-bearing rather than decorative.** "
        "With the switch off the middleware returns before it records anything, so every number "
        "on this page is byte-identical to a service receiving no traffic — while `/health` still "
        "reports `rate_limiter: \"active\"` and names no switch anywhere. This field is the ONLY "
        "thing on any surface this service exposes that separates 'metering is off' from 'nobody "
        "is calling us', and it is false during the one configuration in which the service is "
        "also serving every request unauthenticated."
    )
    poll_ms: int = Field(
        description="DASHBOARD_POLL_MS — how often the page should re-request this payload. "
        "Served by the API so the interval has ONE source of truth rather than one in Python and "
        "one in JavaScript."
    )

    # -- health of the machinery that produced the measurement ----------------------------- #
    replicas: ReplicaInfo = Field(description="Which replicas this payload can name. See the model.")
    degraded: DegradedSignals = Field(description="What is currently not working. See the model.")
    pool: PoolSignals = Field(description="This replica's connection capacity. See the model.")
    dropped: DroppedSignals = Field(
        description="The two ways this payload can be incomplete. See the model."
    )
    buckets_read: int = Field(
        description="Time buckets actually pipelined and folded (minutes + hours). Zero with "
        "`degraded.stats_unavailable` true means nothing was read at all."
    )

    # -- provenance ------------------------------------------------------------------------ #
    generated_at: int = Field(
        description="Unix **milliseconds** at which this replica built the payload, from its own "
        "wall clock. Deliberately a different clock from `window.start_ms`/`end_ms`, which come "
        "from Redis's `TIME` so that every replica names the same window: the gap between the two "
        "is this replica's skew, which is worth being able to see rather than hide."
    )
    served_by: str = Field(
        description="Hostname of the replica that answered. Under C12's load balancer, polling "
        "this endpoint and watching this value change is how you prove the fan-out is real."
    )

    @classmethod
    def from_snapshot(
        cls,
        snapshot: StatsSnapshot,
        *,
        tiers: Mapping[str, TierConfig],
        config_version: int,
        rate_limit_enabled: bool,
        poll_ms: int,
        replicas: ReplicaInfo,
        degraded: DegradedSignals,
        pool: PoolSignals,
        dropped_records: DroppedSignals,
        generated_at: int,
        served_by: str,
    ) -> DashboardStats:
        """Flatten a measurement into the envelope. The one place the mapping is written down.

        Every envelope-only field is keyword-only and required, so a future field cannot be
        forgotten into a default that renders as a plausible zero — the failure mode this whole
        payload is built to make impossible.

        ``dropped_records`` is spelled differently from the ``dropped`` field it lands in because
        the two ``dropped`` counts here mean different things (see :class:`DroppedSignals`), and a
        parameter that silently accepted the snapshot's bucket count would produce a payload that
        under-reports lost traffic as zero. The bucket half is taken from the snapshot below, which
        is the only place it exists.
        """
        return cls(
            totals=snapshot.totals,
            per_minute=snapshot.per_minute,
            per_hour=snapshot.per_hour,
            by_status=snapshot.by_status,
            by_endpoint=snapshot.by_endpoint,
            by_tier=snapshot.by_tier,
            by_outcome=snapshot.by_outcome,
            top_consumers=snapshot.top_consumers,
            window=snapshot.window,
            tiers=dict(tiers),
            config_version=config_version,
            rate_limit_enabled=rate_limit_enabled,
            poll_ms=poll_ms,
            replicas=replicas,
            degraded=degraded,
            pool=pool,
            dropped=dropped_records.model_copy(update={"buckets": snapshot.dropped}),
            buckets_read=snapshot.buckets_read,
            generated_at=generated_at,
            served_by=served_by,
        )


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

    ``strict=True`` for the same reason, one type down. Pydantic's lax mode accepts
    ``{"rate_limit_per_min": "10"}`` and applies it as ``10``, which is harmless in effect and
    inconsistent in principle: a body whose *shape* is wrong should be refused rather than guessed
    at, and this model already refuses a misspelled field name on exactly that argument. It also
    keeps the four numbers integers at the boundary rather than after a coercion — every quantity
    in the decision path is deliberately an integer at the source, because Lua 5.1 numbers are
    doubles whose RESP encoding truncates. ``true`` (an ``int`` subclass in Python) is refused too.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

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

        ``base`` is frozen, so this is a new object; the tier the rest of the process is currently
        enforcing is untouched.

        .. rubric:: This is the RULE, and it is deliberately not the WRITE

        The admin ``PUT`` does not call this. Its merge runs inside
        :data:`src.lua.RLQ_MERGE_TIER`, because a partial update rewrites all four fields and
        therefore needs a base for the three it is not changing — and the only base that cannot
        silently revert another replica's committed change is the row in Redis at the instant of the
        merge, never a :class:`~src.tiers.TierRegistry` snapshot (which is by design up to
        ``TIER_CACHE_TTL_SEC`` stale). :meth:`src.tiers.TierRegistry.merge_tier` documents the
        measured data loss that argument comes from.

        What lives here is the *statement* of the rule in Python — "supplied fields replace, absent
        fields keep" — for callers that already hold a base and simply want it applied: tests, and
        anything that needs to predict what a ``PUT`` will produce without issuing one.
        ``tests/integration/test_admin_api.py`` asserts this function and the script agree across a
        matrix of field combinations, so the two cannot drift into being two different rules.
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
    state: QuotaPeriodState = Field(
        description=(
            "unenforced | exhausted | reset | active, checked in that order — the same ladder "
            "the decision script walks. `unenforced` covers both an unlimited tier and a period "
            "switched off in configuration; see `QuotaPeriodState` and `src.api.admin."
            "period_state` for why `reset` must not stand in for it."
        )
    )


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
