"""The buckets that carry a request when Redis cannot: bounded, per-process, and honest about it.

This module exists for exactly one situation — the shared store is unreachable and
``FAIL_MODE=open`` — and it is deliberately the *smallest* thing that can serve that situation
without becoming the bug this project exists to prevent.

.. rubric:: BOTH rate gates are reproduced locally, not just the bucket

The healthy path has **two** rate gates, and they constrain different things:

* the token bucket per ``(user, endpoint)`` — the *burst* ceiling, one bucket per endpoint;
* the account-wide sliding window per ``user`` — the *sustained rate* across every endpoint. It is
  what makes "free tier is limited after ~60 req/min" true of the caller rather than true of each
  route they happen to call (``SLIDING_WINDOW_SEC`` in :mod:`src.config` says exactly that).

An earlier version of this file reproduced only the first, and the arithmetic of that omission is
the reason this rubric leads the module. There are five route labels in the shipped table
(``logs/query``, ``logs/ingest``, ``logs/{id}``, ``whoami``, ``other``), so a caller on the free
tier with ``API_REPLICAS=2`` got a 30-unit bucket **per label per replica**: 5 x 30 x 2 = 300
weighted units per minute against an intended 60. Dividing by ``API_REPLICAS`` bought back the
replica factor and the dropped window gave away a larger *endpoint* factor — a net **5x**, i.e.
precisely the multi-limiter overspend this project exists to catch, reappearing on a different
axis, during an outage, which is when the store is least able to absorb it.

So :meth:`LocalBucketCache.consume` takes **every** gate at once and admits the request only if
**all** of them admit it, mirroring the decision script's "all gates evaluated, most restrictive
wins" shape. The account-wide gate is keyed on the user alone (``sw:{alice}``, the same name the
shared window uses minus its window index), so one caller has exactly one of them however many
endpoints they touch.

.. rubric:: A token bucket standing in for a weighted sliding window

The account-wide gate is a token bucket here and a Cloudflare-weighted counter in Lua, and that is
an approximation rather than a transcription. It is the right one: a bucket of capacity
``ceil(rpm/N)`` refilling at ``ceil(rpm/N)`` per minute enforces the *same sustained rate* as a
window of limit ``ceil(rpm/N)`` over 60 s, in the same integer micro-token arithmetic the other
gate already uses, with one stored entry instead of two-plus-an-index. Reproducing the weighting
would need the previous window's counter, which this process does not have and cannot reconstruct
— the counter it would decay from was spent on replicas it cannot see. The shapes differ only in
how a fixed allowance is spread within a minute, and that difference is quantified below rather
than waved at.

.. rubric:: The sizing rule, and the bound it actually holds

Every local ceiling is one replica's slice: :func:`replica_share` is ``ceil(total / API_REPLICAS)``
and **everything** goes through it — the bucket's capacity, the bucket's refill rate, and the
account gate's ceiling (which is the tier's ``rpm``, so its capacity and its refill rate are the
same number). Dividing only some of them leaves the rest at N times the tier's figure, which is the
same overspend arriving on a slower axis.

The honest consequence, stated numerically here and in the README rather than discovered:

* **Sustained rate.** N replicas each admit at most ``ceil(rpm/N)`` weighted units per minute
  across all endpoints, so the cluster admits at most ``N * ceil(rpm/N)`` — that is ``rpm`` plus at
  most ``N - 1`` units, the whole of the excess being ``ceil`` rounding. For the shipped free tier
  (60 rpm) at ``API_REPLICAS=2`` the excess is **zero**: 2 x 30 = 60. At an awkward ratio (60 rpm,
  7 replicas) it is 7 x 9 = 63, i.e. 3 over.
* **The first minute, and only the first.** A bucket that has never been seen starts **full** (see
  :meth:`LocalBucketCache.consume` for why the alternatives are worse), so the opening minute of an
  outage admits the initial fill *plus* a minute of refill: up to ``2 * ceil(rpm/N)`` per replica,
  ``~2x`` the tier cluster-wide. It is one-off per replica per outage, it is bounded, and it decays
  to the steady-state figure above. The per-endpoint bucket has always had this property; the
  account gate now has it too, which is why it is stated once for both.
* **Eviction.** A bucket dropped by the LRU comes back full, which hands its owner one extra fill.
  Bounded by the same ``ceil(rpm/N)``, and :attr:`LocalBucketCache.evictions` is the number that
  says it happened.

That is a bound of ``rpm + (N-1)`` sustained and ``2 x`` for one minute. It is **not** the
``labels x rpm`` this file used to hold, and the difference is the point: a caller cannot buy extra
allowance by spreading traffic across endpoints, because the gate that binds is the one that does
not know what an endpoint is.

.. rubric:: There is NO quota enforcement here, and there cannot be

A rate limit is a *rate*: it is recoverable, it is local in time, and an approximation that is off
by a few tokens for the duration of an outage is a real limit. A quota is a **cumulative
cross-replica counter** — "you have used 743 of your 1000 requests today" — and there is no local
approximation of that number which is not a fabrication. This process does not know what the other
replicas admitted, does not know what this replica admitted before it restarted, and cannot know
what was spent before the outage began.

Reporting a made-up number would be worse than reporting none, and for a specific reason: a client
can *detect* a missing header and fall back to its own accounting, and cannot detect a wrong one —
it will build a usage display, a spend alarm or a scheduling decision on top of it. That is why
:meth:`~src.models.LimitDecision.headers` omits **every** ``X-Quota-*`` header while ``degraded``
is set, and why the degraded decision reports both quota periods as
:attr:`~src.models.QuotaPeriodState.UNENFORCED` rather than as ``reset`` or ``active``. Nothing is
being counted, so nothing is claimed.

The same rule is why the account gate exists at all rather than being left out with a shrug: the
numbers this replica *can* honestly produce for ``X-RateLimit-Limit`` and ``X-RateLimit-Remaining``
are the ones it is genuinely enforcing. With the account gate missing, ``X-RateLimit-Limit: 30``
described an account-wide ceiling that nothing checked — a fabricated header of exactly the kind
the paragraph above refuses to emit for quotas.

.. rubric:: Bounded memory, because a degraded flood must not also be an OOM

Both key spaces live in **one** ``OrderedDict``, with ``move_to_end`` on every touch and
``popitem(last=False)`` at :data:`FALLBACK_MAX_BUCKETS`. One map rather than two so the cap covers
the total rather than being a cap each: two 10 000-entry maps is a 20 000-entry bound wearing a
10 000-entry label. They cannot collide, because the limiter passes
``rate_limit:{user}:METHOD:/path`` for one and ``sw:{user}`` for the other — the same names the
shared store uses, which is what makes a degraded log line comparable to a healthy one.

Without the cap the key space is chosen by the caller, so an outage plus a flood of distinct
principals would grow the heap until the pod is killed. Trading an OOM for an evicted bucket is not
a close call: an evicted bucket costs its owner one extra fill (bounded, recoverable, and counted),
while an OOM kill takes the whole replica out during an incident it was supposed to be riding out.

``move_to_end`` on a *hit* is what makes it an LRU rather than a FIFO, and the distinction is
load-bearing here in a way it is not for a cache: the entries evicted must be the ones nobody is
using, because evicting a *busy* caller's bucket is what hands them a fresh full one. A FIFO would
evict on age and therefore evict the busiest callers on a fixed cycle. It also has a property worth
naming now that one caller owns several entries: a user's account gate is touched on **every**
request they make, while each of their endpoint buckets is touched only by traffic to that
endpoint — so the account gate is the last of that user's entries the LRU will drop, which is the
right order, because it is the one whose loss hands back the most allowance.

.. rubric:: The arithmetic is the Lua script's, transcribed

:meth:`LocalBucketCache.consume` is the same lazy refill as
:data:`~src.lua.RLQ_CHECK_AND_CONSUME`'s gate 1, in the same micro-token integers, deliberately —
so a caller who crosses from the shared bucket to this one and back does not see the limiter change
its arithmetic underneath them. It keeps the script's two structural properties as well: every gate
is evaluated before any of them is mutated, and **a denial spends nothing** from any gate. Three
differences, all intentional and all noted at their site:

* the clock is :func:`time.monotonic` rather than ``redis.call('TIME')``, because there is no
  shared clock available — that is what "degraded" means here;
* ``capacity <= 0`` denies rather than meaning "unenforced". In the script a non-positive limit is
  the documented "this gate is not enforcing anything" convention; on *this* path the same reading
  would be a silent unmetered request during an outage, which is the exact failure the degraded
  header exists to make visible;
* the account gate is a bucket rather than a weighted window, as argued above.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final

from src.config import Settings, TierConfig
from src.keys import MS_PER_MINUTE
from src.lua import MICRO_TOKENS

__all__ = [
    "FALLBACK_MAX_BUCKETS",
    "GateSpec",
    "LocalBucketCache",
    "LocalDecision",
    "LocalVerdict",
    "MS_PER_SECOND",
    "replica_share",
]

#: Hard cap on locally held buckets, counting **both** key spaces (see the module docstring).
#: 10 000 entries is far more than this demo will ever see and is still a bounded amount of process
#: memory (two integers and a string key per entry). The same number and the same reasoning as
#: :data:`~src.identity.IDENTITY_CACHE_MAX_ENTRIES`: the cap is what stops a flood of distinct
#: principals relocating an attack from Redis into the pod's heap.
#:
#: A module constant plus a constructor keyword rather than a ``Settings`` field, matching the
#: identity cache's precedent exactly. It is a memory bound, not a limit anybody tunes per
#: deployment, and every knob that genuinely sizes this path (``API_REPLICAS``, ``FAIL_MODE``)
#: already lives in :class:`~src.config.Settings`.
FALLBACK_MAX_BUCKETS: Final = 10_000

#: Milliseconds per second. Named so the clock conversion below reads as a conversion.
MS_PER_SECOND: Final = 1000


def replica_share(total: int, replicas: int) -> int:
    """``ceil(total / replicas)`` — one replica's slice of a cluster-wide allowance.

    THE function that keeps the degraded path from being an N-times overspend, applied to *every*
    ceiling this module enforces. See the module docstring for the full argument; the short version
    is that N replicas each holding a full allowance is N rate limits, not one, and reproducing
    that in the fallback would make the degraded mode a *worse* answer than the failure it is
    degrading from.

    Rounds **up**, so a tier of 1 with 4 replicas gives each replica 1 rather than 0. Rounding down
    would silently produce a zero-capacity gate, which on this path denies everything (see the
    module docstring) — i.e. a small tier would be refused outright for the duration of an outage
    because of integer division. Up-rounding costs at most ``replicas - 1`` extra units across the
    whole cluster, which is the entirety of the excess the module docstring quantifies;
    down-rounding costs a tier its entire service.

    ``replicas`` is floored at 1: ``API_REPLICAS`` is a plain integer field with no positivity
    constraint, and a ``0`` there would otherwise be a ``ZeroDivisionError`` raised from inside the
    degraded path — a crash during an outage, produced by a config typo, on the code whose only job
    is surviving outages.

    A non-positive ``total`` returns 0 and therefore a gate that admits nothing. That is the
    fail-closed reading, and it is the right one here: a tier configured with no capacity must not
    become an unlimited one the moment Redis goes away.
    """
    if total <= 0:
        return 0
    return -(-total // max(1, replicas))


@dataclass(frozen=True, slots=True)
class GateSpec:
    """One local gate to evaluate: which bucket, how many tokens it holds, how fast it refills.

    A value type rather than three positional arguments because :meth:`LocalBucketCache.consume`
    takes a *variable number* of these and evaluates them as a set — and the one thing that must
    never happen is a caller spending from one gate without the other having been consulted. Making
    the gate a thing you construct and hand over, rather than a call you make, is what stops that
    being expressible.

    The cache deliberately does not know which of these is "the bucket" and which is "the window".
    They are both token buckets over different key spaces; the *meaning* lives in the limiter, and
    the sizing lives in :meth:`LocalBucketCache.bucket_gate` / :meth:`LocalBucketCache.account_gate`.
    """

    #: The bucket identity. The limiter passes the **same** string it would have sent to Redis, so
    #: a caller's local gate and their shared gate are the same gate by name.
    key: str
    #: Tokens this replica is enforcing on this gate — always a :func:`replica_share`, never the
    #: tier's own figure.
    capacity: int
    #: Whole tokens per minute this gate refills at. ``0`` means it never refills.
    rpm: int


@dataclass(frozen=True, slots=True)
class LocalVerdict:
    """One gate's answer. Frozen and slotted, like every other value type in this project.

    ``remaining`` and ``capacity`` are whole tokens (the micro-token scaling is an internal detail
    of the arithmetic, exactly as it is in the Lua script), and both millisecond durations are
    integers so the caller's ``ceil_seconds`` conversion has no float in front of it.
    """

    #: Whether **this gate** would have admitted the request. The request is admitted only if every
    #: gate says so — see :attr:`LocalDecision.allowed`.
    allowed: bool
    #: Tokens this replica is enforcing on this gate.
    capacity: int
    #: Whole tokens left after this decision. Never negative.
    remaining: int
    #: Milliseconds until this gate is full again from its post-decision level.
    reset_ms: int
    #: Milliseconds until **this gate** would admit the request. 0 when it already would.
    retry_ms: int


@dataclass(frozen=True, slots=True)
class LocalDecision:
    """The combined answer across every gate. The most restrictive one wins.

    ``verdicts`` is in the order the gates were passed, so the limiter can name each one without
    the cache having to know what any of them means.
    """

    #: True only if **every** gate admitted the request.
    allowed: bool
    #: One :class:`LocalVerdict` per :class:`GateSpec`, in the order supplied.
    verdicts: tuple[LocalVerdict, ...]
    #: The **maximum** retry across the gates that refused, floored at 1; 0 when admitted. The
    #: maximum and not the minimum, for the reason the decision script spells out: telling a caller
    #: who is blocked by two gates to come back when the *nearer* one clears is telling them to be
    #: refused again.
    retry_ms: int


@dataclass(slots=True)
class _Bucket:
    """One gate's state: micro-tokens, and the monotonic millisecond they were true at.

    Mutable and slotted rather than frozen: this is replaced on every touch of a hot key at the
    project's target rate, and the frozen-record argument that applies to
    :class:`~src.models.LimitDecision` (a decision is history and must not be edited on its way to
    a header) does not apply to a live counter, which is state by definition.
    """

    tokens_micro: int
    updated_ms: int


class LocalBucketCache:
    """An LRU-bounded, per-process set of token buckets used **only** while Redis is unreachable.

    Constructed synchronously, performs no I/O, and holds no lock — the same contract as every
    other collaborator on the request path. No lock because every mutation below happens between
    two ``await`` points in the caller (:meth:`src.limiter.Limiter.check`'s exception handler), so
    the event loop cannot interleave two ``consume`` calls; a lock would serialise nothing that is
    not already serial and would add an acquire to the degraded hot path. That property is load
    bearing now that one call touches two gates: the pair is atomic because nothing can run between
    them, not because anything is guarding them.

    Args:
        settings: read for ``API_REPLICAS`` only, and read *per call* rather than cached, so a
            future admin route that changes the replica count does not leave this object sizing
            gates from a number nobody is running any more.
        clock: seconds-resolution monotonic clock, injectable so the refill tests need no
            ``sleep``. :func:`time.monotonic` and not :func:`time.time`: an NTP step must not be
            able to refill every bucket in the process at once, which is precisely the skew
            vulnerability the shared clock was moved into Redis to remove.
        max_entries: hard cap across **both** key spaces. Floored at 1, so a mis-set cap cannot
            produce a "cache" that evicts the entry it just inserted on every call.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], float] = time.monotonic,
        max_entries: int = FALLBACK_MAX_BUCKETS,
    ) -> None:
        self._settings = settings
        self._clock = clock
        self._max_entries = max(1, max_entries)
        self._buckets: OrderedDict[str, _Bucket] = OrderedDict()

        #: Requests this cache admitted. With :attr:`denies`, the pair is what makes a degradation
        #: legible on the dashboard — "we served 4 000 requests locally and refused 900" is an
        #: incident report; a silent fail-open is indistinguishable from having no limiter.
        #: Counted per **request**, not per gate, so the pair still sums to the number of degraded
        #: decisions now that one decision consults two gates.
        self.allows = 0
        #: Requests this cache refused, for any gate's reason.
        self.denies = 0
        #: Gates dropped because the cap was reached. A non-zero value here during an outage means
        #: the degraded working set is larger than the cap, i.e. some callers are getting a freshly
        #: full gate; it is the number that says so rather than a silent effect.
        self.evictions = 0
        #: Gates created because the key had never been seen (or had been evicted).
        self.creations = 0

    # ------------------------------------------------------------------ #
    # Sizing
    # ------------------------------------------------------------------ #
    def capacity_for(self, tier_capacity: int) -> int:
        """This replica's share of a tier's burst capacity. See :func:`replica_share`."""
        return replica_share(tier_capacity, self._settings.api_replicas)

    def rate_for(self, tier_rpm: int) -> int:
        """This replica's share of a tier's per-minute allowance. See :func:`replica_share`.

        Divided for the same reason the capacity is: a bucket of ``capacity/N`` that refilled at
        the *full* tier rate would admit N times the tier's sustained throughput as soon as the
        first burst drained — the overspend arriving a minute late rather than immediately.
        """
        return replica_share(tier_rpm, self._settings.api_replicas)

    def bucket_gate(self, key: str, tier: TierConfig) -> GateSpec:
        """The per-``(user, endpoint)`` **burst** gate — the local mirror of the script's gate 1.

        Capacity is the tier's burst and the refill is the tier's rpm, both divided. This is the
        gate that stops one endpoint being hammered; on its own it is *not* a limit on the caller,
        which is what :meth:`account_gate` is for and why the two are always passed together.
        """
        return GateSpec(
            key=key,
            capacity=self.capacity_for(tier.burst),
            rpm=self.rate_for(tier.rate_limit_per_min),
        )

    def account_gate(self, key: str, tier: TierConfig) -> GateSpec:
        """The account-wide **sustained rate** gate — the local mirror of the script's gate 2.

        Its ceiling and its refill rate are the *same number*, and that is the whole idea rather
        than a shortcut: the shared gate is a window of limit ``rpm`` over one minute, so a bucket
        that holds ``ceil(rpm/N)`` and refills ``ceil(rpm/N)`` per minute enforces the identical
        sustained rate. See the module docstring for the difference in burst *shape* and for the
        number it costs.

        Keyed on the user alone by the caller (``sw:{alice}``), so a caller has exactly one of
        these no matter how many endpoints they touch — which is the entire fix for the endpoint
        multiplication the module docstring opens with.
        """
        share = self.rate_for(tier.rate_limit_per_min)
        return GateSpec(key=key, capacity=share, rpm=share)

    # ------------------------------------------------------------------ #
    # The degraded hot path
    # ------------------------------------------------------------------ #
    def consume(self, *gates: GateSpec, cost: int) -> LocalDecision:
        """Refill every gate to now, then spend ``cost`` from **all** of them, or from none.

        Args:
            gates: the gates to evaluate, in the order their verdicts should be returned. The
                limiter passes the per-endpoint bucket first and the account-wide gate second,
                matching the decision script's gate order — which is also the order ties in
                :attr:`LocalDecision.retry_ms` resolve in.
            cost: the request's weighted cost, ``>= 1``. Charged to **every** gate, exactly as the
                script charges the bucket and the window the same weighted units.

        .. rubric:: All gates are evaluated before any is mutated, and a denial spends nothing

        Both properties are transcribed from the decision script's mutation block and both matter
        here. Spending the bucket's token and *then* discovering the account gate refuses would
        charge a caller for a request that was never served — and a client in a retry loop would
        drain its own bucket with requests it is being refused, so the refusal would outlive its
        own cause. Refilled-but-unspent state is still written back, which is not a divergence:
        the stored figure is "tokens as of now", identical to what recomputing from the older stamp
        would produce, and here it is a dict assignment rather than the round trip the script is
        avoiding.

        A key that has never been seen — including one the LRU evicted — starts **full**. It is the
        same choice the Lua script makes for a missing bucket key and the only defensible one:
        starting empty would refuse a first-time caller for a whole refill period, and any other
        starting level is a number nobody can explain. The cost is quantified in the module
        docstring (a fresh gate is worth one extra fill) and is why the cap is generous.

        Returns:
            A :class:`LocalDecision` whose ``allowed`` is the AND across gates and whose
            ``retry_ms`` is the MAX across the gates that refused.
        """
        now_ms = int(self._clock() * MS_PER_SECOND)
        cost_micro = cost * MICRO_TOKENS

        # Phase 1 — read and refill every gate. Nothing is spent and nothing is stored yet.
        refilled: list[tuple[GateSpec, int, int]] = []
        allowed = True
        for gate in gates:
            tokens_micro, full_refill_ms = self._refill(gate, now_ms)
            refilled.append((gate, tokens_micro, full_refill_ms))
            if tokens_micro < cost_micro:
                allowed = False

        # Counted per request, so `allows + denies` is the number of degraded decisions rather
        # than the number of gate evaluations, which is a number nobody wants on a dashboard.
        if allowed:
            self.allows += 1
        else:
            self.denies += 1

        # Phase 2 — commit. Every gate is written back; `cost` comes off all of them or none.
        verdicts: list[LocalVerdict] = []
        retry_ms = 0
        for gate, tokens_micro, full_refill_ms in refilled:
            gate_allowed = tokens_micro >= cost_micro
            gate_retry = 0
            if not gate_allowed:
                # Floored at 1 **per gate**, not only on the combined answer: a `Retry-After: 0` is
                # a retry storm the limiter manufactured for itself, and the limiter reads
                # individual verdicts (for `X-RateLimit-Reset` and the deny reason), so a single
                # gate must not be able to report one either. Same floor `LimitDecision` applies,
                # applied at the source.
                gate_retry = max(
                    1,
                    self._retry_ms(
                        gate, tokens_micro, cost_micro=cost_micro, full_refill_ms=full_refill_ms
                    ),
                )
                # Strict `>`, so ties resolve to the earlier gate — the same fixed, deterministic
                # ordering rule the decision script applies when two gates both refuse.
                if gate_retry > retry_ms:
                    retry_ms = gate_retry
            if allowed:
                tokens_micro -= cost_micro

            self._store(gate.key, tokens_micro, now_ms)
            verdicts.append(
                LocalVerdict(
                    allowed=gate_allowed,
                    capacity=gate.capacity,
                    # Floored at whole tokens, matching the script's `math.floor(tokens / MICRO)`,
                    # so a gate holding 4.9 tokens advertises 4 rather than promising a fifth it
                    # does not have.
                    remaining=tokens_micro // MICRO_TOKENS,
                    reset_ms=self._reset_ms(gate, tokens_micro),
                    retry_ms=gate_retry,
                )
            )

        # ONE eviction pass, after every gate of this request has been stored — so a two-gate
        # request cannot evict its own first gate on the way to writing its second. `while`, not
        # `if`, so a cap lowered at construction converges instead of sitting one entry over
        # forever. `popitem(last=False)` takes the OLDEST end.
        while len(self._buckets) > self._max_entries:
            self._buckets.popitem(last=False)
            self.evictions += 1

        return LocalDecision(
            allowed=allowed,
            verdicts=tuple(verdicts),
            # The MAX across the gates that refused, and therefore already >= 1 because each of
            # those floored its own. 0 when admitted, because no gate refused.
            retry_ms=retry_ms,
        )

    # ------------------------------------------------------------------ #
    # Arithmetic — the decision script's gate 1, transcribed
    # ------------------------------------------------------------------ #
    def _refill(self, gate: GateSpec, now_ms: int) -> tuple[int, int]:
        """Return ``(tokens_micro_as_of_now, full_refill_ms)`` for ``gate``. Stores nothing.

        Touches the LRU ordering (a read *is* a use) but never the token level, so phase 1 of
        :meth:`consume` can evaluate every gate without committing any of them.
        """
        capacity_micro = gate.capacity * MICRO_TOKENS
        # Milliseconds to refill an empty gate to capacity. Used twice, exactly as in the script:
        # to bound `elapsed` (which keeps the refill product exactly representable) and to cap a
        # retry that could never actually be satisfied.
        full_refill_ms = (
            -(-gate.capacity * MS_PER_MINUTE // gate.rpm) if gate.rpm > 0 else 0
        )

        entry = self._buckets.get(gate.key)
        if entry is None:
            self.creations += 1
            return capacity_micro, full_refill_ms

        # `move_to_end` on every touch is what makes this an LRU rather than a FIFO. Without it,
        # the busiest caller in the process is evicted on a fixed cycle — and an evicted gate comes
        # back FULL, so a FIFO would hand extra allowance to exactly the callers using the most.
        self._buckets.move_to_end(gate.key)
        tokens_micro = entry.tokens_micro

        elapsed_ms = now_ms - entry.updated_ms
        if elapsed_ms < 0:
            # Unreachable with `time.monotonic`, which is why there is no "the stamp is absurdly
            # far in the future, reset the gate" branch here as there is in the script: nothing but
            # this cache ever writes the stamp, and a monotonic clock cannot step. It is a guard
            # against an injected clock, and clamping to zero is the answer that cannot
            # over-credit.
            elapsed_ms = 0
        elif elapsed_ms > full_refill_ms:
            # Refill is clamped at capacity anyway, so anything past a full period is the same
            # answer — and clamping keeps `elapsed * rpm * MICRO` far below 2**53, which is where
            # the script's identical arithmetic would stop being exact.
            elapsed_ms = full_refill_ms

        if gate.rpm > 0 and elapsed_ms > 0:
            # Integer-first ordering, the same as the script's: folding the per-millisecond rate
            # first and multiplying afterwards loses up to a micro-token per call, which shows up
            # only as a gate that is mysteriously short after a few thousand requests.
            tokens_micro += elapsed_ms * gate.rpm * MICRO_TOKENS // MS_PER_MINUTE
        # Clamped on the way out, which also absorbs a capacity that SHRANK since the gate was
        # stored (an operator raising API_REPLICAS at runtime) rather than leaving a caller holding
        # tokens against a ceiling that no longer exists.
        return min(tokens_micro, capacity_micro), full_refill_ms

    @staticmethod
    def _retry_ms(
        gate: GateSpec, tokens_micro: int, *, cost_micro: int, full_refill_ms: int
    ) -> int:
        """Milliseconds until ``gate`` would admit ``cost``. Only called when it currently will not."""
        if gate.rpm <= 0:
            # A gate that never refills has no honest interval to offer, so the caller floors it
            # at 1 rather than promising a wait that will not help.
            return 0
        retry_ms = -(-(cost_micro - tokens_micro) * MS_PER_MINUTE // (gate.rpm * MICRO_TOKENS))
        # Reachable when cost > capacity, i.e. a request this gate can never admit. There is no
        # honest interval for that, so report the time to a full gate: bounded, and it does not
        # promise a wait that would work.
        return min(retry_ms, full_refill_ms)

    @staticmethod
    def _reset_ms(gate: GateSpec, tokens_micro: int) -> int:
        """Milliseconds until ``gate`` is full again from its post-decision level."""
        capacity_micro = gate.capacity * MICRO_TOKENS
        if gate.rpm <= 0 or tokens_micro >= capacity_micro:
            return 0
        return -(-(capacity_micro - tokens_micro) * MS_PER_MINUTE // (gate.rpm * MICRO_TOKENS))

    def _store(self, key: str, tokens_micro: int, now_ms: int) -> None:
        """Write one gate's post-decision state and mark it the most recently used."""
        self._buckets[key] = _Bucket(tokens_micro=tokens_micro, updated_ms=now_ms)
        # Assigning to an existing key leaves its position alone in an OrderedDict, so a refreshed
        # entry would keep the recency of its predecessor and be evicted early.
        self._buckets.move_to_end(key)

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    def __len__(self) -> int:
        """How many gates are held right now, across both key spaces. Never above ``max_entries``."""
        return len(self._buckets)

    def __contains__(self, key: object) -> bool:
        """Whether ``key`` currently has a gate. Used by the eviction tests, and by nothing else."""
        return key in self._buckets

    def clear(self) -> None:
        """Drop every gate, keeping the counters.

        The counters are lifetime totals for ``/health`` and C11, so resetting them here would
        produce a degradation metric that silently restarts whenever anyone cleared the cache —
        i.e. one that only ever looks fine. Same rule as
        :meth:`~src.identity.IdentityResolver.clear`.
        """
        self._buckets.clear()

    def stats(self) -> dict[str, Any]:
        """Counter snapshot for ``/health`` and C11's stats payload."""
        return {
            "size": len(self._buckets),
            "max_entries": self._max_entries,
            "replicas": max(1, self._settings.api_replicas),
            "allows": self.allows,
            "denies": self.denies,
            "evictions": self.evictions,
            "creations": self.creations,
        }
