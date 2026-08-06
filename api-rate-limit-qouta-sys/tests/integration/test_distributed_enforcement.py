"""**The test that justifies the project**: two app instances, one limiter.

Everything else in this suite proves that *one* process enforces a limit correctly. That was never
the interesting claim. The claim this project is built around is the one in the first paragraph of
``docker-compose.yml``: a token bucket held in Python memory is not a rate limit once there are two
replicas behind a load balancer — it is TWO rate limits, and the caller gets double the allowance
they paid for. This file is where that stops being a paragraph.

.. rubric:: The shape, and why each half of it is load bearing

The :func:`~tests.integration.test_admin_api.two_replicas` fixture (C10's, reused rather than
re-invented) builds **two independently constructed apps**: two :class:`~src.main.Runtime`
objects, two :class:`~src.redis_client.RedisGateway` instances, **two connection pools**, two
:class:`~src.tiers.TierRegistry` snapshots — over **one** real ``redis:7-alpine``. Drop any one of
those and the test quietly stops being about anything:

* Sharing a gateway would prove two ``FastAPI`` objects can talk to one client, which nobody
  doubted.
* Sharing a pool would prove two clients can share a socket.
* Sharing a Redis is the *subject*: it is the only thing the two processes have in common, so a
  combined allowance equal to one tier's capacity can only come from the store.

Driven through ``httpx.ASGITransport`` — no socket, no server, no proxy. That is deliberate and it
is the honest limitation of this file: it proves the *state* is shared, not that nginx fans out.
The fan-out half is proven where it lives, against the running stack —
``curl localhost:8020/health`` twenty times must return two distinct ``served_by`` hostnames — and
C13's verifier makes it an assertion (``len({X-Served-By}) >= 2``) rather than an eyeball. The two
halves are separable and this one runs on every ``make test``, in ~2 seconds, with no containers
beyond the store.

.. rubric:: Every count here is asserted with ``==``, never ``<=``

``allowed <= capacity`` is the assertion a broken limiter passes most easily: one that denies
**everything** satisfies it perfectly, and so does one that has silently stopped being reachable
(every request 429s for the wrong reason). The number that matters is exactly ``capacity`` — one
tier's worth of allowance, spent across two processes — and the failure message on every one of
them states the counterfactual it is refuting: *2 instances x capacity 60 = 120 would mean
per-process buckets; got N*. A failure here should read as the bug report, because the bug it
catches is the founding bug of the project and it is completely silent in production: no error, no
log line, just a customer using twice what they bought.

.. rubric:: Why the burst is repeated, and why "exactly capacity" is safe to assert

Repeated (:data:`BURST_ROUNDS` rounds, a fresh ``uuid4`` principal each time) because a race is
probabilistic: one green run of a concurrency property is close to no evidence, and the cost here
is milliseconds.

The exactness is not luck, and it does not depend on the burst being fast. Two gates bound a fresh
free-tier principal at 60, and the account-wide sliding window is the one that makes the number
*stable*: within a single window the weighted counter reads ``ceil(prev * (W - into)/W + curr)``
with ``prev = 0``, so it admits precisely ``rpm`` requests however long the burst takes, and across
a window boundary ``prev`` is still counted at nearly full weight (that is what the weighting is
FOR). The token bucket refills at 1 token/s for this tier, so it cannot contribute a 61st admission
inside a burst that takes ~100 ms either. The measured elapsed time is carried into the failure
message anyway, so the one environment that could break the reasoning — a container so slow that a
whole second of refill lands mid-burst — reports itself instead of looking like a limiter bug.

.. rubric:: What is deliberately NOT re-tested here

``/health``'s exemption is asserted in ``tests/integration/test_headers.py`` (200 with no
``X-RateLimit-*`` while the principal is fully limited) and its root_path variant in
``tests/integration/test_protected_api.py``. That property is the reason ``nginx/nginx.conf``
proxies at ``/`` with no prefix, and it is checked against the live LB in this commit's
verification — but re-asserting it in-process, where there is no proxy, would only restate what
those two files already own.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from uuid import uuid4

import httpx
import pytest

from src.models import (
    QUOTA_REMAINING_HEADER,
    RATELIMIT_LIMIT_HEADER,
    RATELIMIT_REMAINING_HEADER,
)

# The fixture and its three helpers are IMPORTED rather than copied, and that is the point of the
# import: `two_replicas` is C10's construction of "two apps, two gateways, two pools, one Redis",
# and a second local copy of it would be a second definition of what "two replicas" means — free
# to drift from the one the admin suite asserts against, in the direction where both copies still
# pass. `bearer` mints a JWT (no `apikey:v1:*` record to seed, and no Redis lookup on the identity
# path, so a burst here measures the limiter rather than the credential cache); `auth` is the admin
# token header.
from tests.integration.test_admin_api import (  # noqa: F401 — `two_replicas` is a fixture
    ADMIN,
    WHOAMI,
    auth,
    bearer,
    two_replicas,
)

#: The shipped free tier: ``free:60:60:1000:25000``. Written out rather than read off
#: :class:`~src.config.Settings` so this file states what the numbers *are* instead of asserting
#: that they equal themselves — the same rule ``tests/integration/test_admin_api.py`` applies.
FREE_RPM = 60
FREE_BURST = 60
FREE_DAILY = 1000
PREMIUM_RPM = 300

#: What one free-tier principal may spend in one window, across however many processes are
#: serving them. ``burst`` and ``rpm`` are equal for this tier, so both rate gates bind at the same
#: number and which one refuses is not a fact worth pinning.
CAPACITY = FREE_BURST

#: Requests fired **past** capacity in a burst. Enough that a per-process bucket would be obvious
#: (it would admit 2 x capacity and this would not even be close), small enough that the whole
#: burst is one short loop.
OVERSHOOT = 10

#: How many times the headline burst is repeated, each against a fresh principal. See the
#: docstring: one green run of a concurrency property proves close to nothing.
BURST_ROUNDS = 5

#: The two instances, named for failure messages. Index-aligned with the ``clients`` tuple.
INSTANCE_NAMES = ("instance-a", "instance-b")

#: ``ENDPOINT_COSTS`` prices ``logs_query`` at 5. Spelled with **no trailing slash**, deliberately:
#: ``/api/v1/logs/query/`` is metered here, 307'd by the router, and metered again on the
#: follow-up — 10 tokens for one logical call. That is the documented safe asymmetry
#: (over-charging is a bug report, under-charging is a bypass), but a test asserting an exact
#: admission count would read it as a double-spend and send someone hunting a bug that is not
#: there.
LOGS_QUERY = "/api/v1/logs/query"
LOGS_QUERY_COST = 5


@dataclass(frozen=True, slots=True)
class Burst:
    """The outcome of one burst fired across N instances."""

    #: Requests that came back 200.
    allowed: int
    #: Requests that came back 429.
    refused: int
    #: Admissions per instance, index-aligned with the ``clients`` tuple that produced them.
    by_instance: tuple[int, ...]
    #: Wall time for the whole burst. Carried only into failure messages — see the module
    #: docstring's note on why the exactness does not depend on it.
    elapsed_sec: float

    @property
    def total(self) -> int:
        return self.allowed + self.refused


async def burst(
    clients: tuple[httpx.AsyncClient, ...],
    headers: dict[str, str],
    *,
    attempts: int,
    path: str = WHOAMI,
) -> Burst:
    """Fire ``attempts`` requests, round-robining across ``clients``, and count the outcomes.

    Sequential rather than concurrent, and that is not a shortcut. "How many got through" must be
    a statement about the *limits*; firing concurrently would make it partly a statement about the
    event loop's scheduling, and the atomicity that makes concurrency safe is already proven where
    it belongs (``tests/integration/test_lua_contract.py`` drives the script concurrently against a
    real server). Here the subject is which STORE the two processes counted against, which is
    visible one request at a time.

    Round-robining in Python rather than through nginx for the same reason the whole file is
    in-process: the property under test is shared state, and a real proxy would add a moving part
    that can only make the assertion weaker.

    Anything that is not a 200 or a 429 fails immediately rather than being bucketed as "refused":
    a 401 (a credential that stopped working) or a 503 (a store that went away mid-burst) would
    otherwise be counted as evidence of a limit being enforced.
    """
    allowed = refused = 0
    per_instance = [0] * len(clients)
    started = time.perf_counter()
    for index in range(attempts):
        which = index % len(clients)
        response = await clients[which].get(path, headers=headers)
        if response.status_code == 200:
            allowed += 1
            per_instance[which] += 1
        else:
            assert response.status_code == 429, (
                f"{INSTANCE_NAMES[which]} answered {response.status_code} on request "
                f"{index + 1}/{attempts}, which is neither an admission nor a refusal: "
                f"{response.text}"
            )
            refused += 1
    return Burst(
        allowed=allowed,
        refused=refused,
        by_instance=tuple(per_instance),
        elapsed_sec=time.perf_counter() - started,
    )


def per_process_would_be(
    result: Burst, *, instances: int, ceiling: int, named: str, per_process: str
) -> str:
    """The counterfactual, rendered as the failure message. See the module docstring.

    Names the number a *broken* system produces, next to the number this run actually produced, so
    the assertion output is the bug report rather than the start of one. ``per_process`` is spelled
    out by the caller rather than derived from ``named``: the two nouns differ (a *capacity* being
    doubled means per-process **buckets**; a *daily quota* being doubled means per-process **quota
    counters**), and naming the wrong mechanism in the message would point the reader at the wrong
    file.
    """
    return (
        f"{instances} instances x {named} {ceiling} = {instances * ceiling} would mean "
        f"per-process {per_process}; got {result.allowed} "
        f"(refused {result.refused}, per instance {result.by_instance}, "
        f"{result.elapsed_sec * 1000:.0f} ms)"
    )


@pytest.fixture()
def instances(two_replicas) -> tuple[httpx.AsyncClient, ...]:
    """Just the two clients, as a tuple — the shape :func:`burst` round-robins over."""
    client_a, client_b, _runtime = two_replicas
    return (client_a, client_b)


def fresh(prefix: str) -> str:
    """A principal nobody has ever metered.

    ``uuid4`` rather than a fixed name, even though the fixture flushes the store: a burst against
    a partially drained bucket proves nothing, and "the fixture flushed, so the bucket is empty" is
    an inference about another file. This is a fact.
    """
    return f"{prefix}-{uuid4()}"


# =============================================================================================
# The headline: one bucket, two processes
# =============================================================================================


async def test_two_app_instances_share_one_bucket(two_replicas, instances):
    """``capacity + OVERSHOOT`` requests alternating across two apps admit **exactly** capacity.

    The assertion this whole project exists to make. Two processes, two connection pools, one
    Redis: the combined allowance is one tier's worth, not two.

    Note the ORDER of the three assertions, which is chosen so a failure is diagnosable from the
    message alone:

    1. ``total`` first — if the two counts do not add up, some request got a third status code and
       neither of the numbers below means what it says.
    2. ``allowed > 0`` second. It is arithmetically implied by the equality below and is asserted
       anyway, because it separates the two failures that look identical in a bare count: "nothing
       got through at all" (a broken credential, an unreachable store) from "too much got through"
       (the actual bug). One is a fixture problem and one is a rate limiter that does not work.
    3. The equality, with the counterfactual in its message.

    And finally that BOTH instances served traffic — without it, an alternation that silently sent
    everything to one client would pass this test while being a single-process test.
    """
    _client_a, _client_b, runtime = two_replicas

    for round_number in range(1, BURST_ROUNDS + 1):
        user_id = fresh("burst")
        headers = bearer(runtime.settings, user_id)
        attempts = CAPACITY + OVERSHOOT

        result = await burst(instances, headers, attempts=attempts)
        where = f"round {round_number}/{BURST_ROUNDS}: "

        assert result.total == attempts, (
            f"{where}{result.allowed} + {result.refused} != {attempts} — some request was "
            "neither admitted nor refused"
        )
        assert result.allowed > 0, (
            f"{where}every one of {attempts} requests was refused. That is not a shared bucket, "
            "it is a limiter that is admitting nobody — and it would pass an 'allowed <= "
            "capacity' assertion perfectly."
        )
        assert result.allowed == CAPACITY, where + per_process_would_be(
            result,
            instances=len(instances),
            ceiling=CAPACITY,
            named="capacity",
            per_process="buckets",
        )
        assert all(count > 0 for count in result.by_instance), (
            f"{where}one instance served nothing ({result.by_instance}) — this run exercised a "
            "single process and proves nothing about two"
        )


async def test_one_instance_alone_admits_the_same_capacity(two_replicas, instances):
    """The control. Everything at ONE instance admits exactly what the pair admitted.

    Without this the headline number is unanchored: ``60`` could be a property of alternating
    between two clients rather than the tier's ceiling. Firing the identical burst at a single
    client and getting the identical count is what makes "the two of them shared one allowance"
    the only remaining reading — and it is the in-process twin of C13's ``DIRECT_URL`` control,
    which exists for exactly this reason ("if the control says 60 and the LB says 120, the
    shared-state claim is false").
    """
    _client_a, _client_b, runtime = two_replicas
    headers = bearer(runtime.settings, fresh("control"))

    result = await burst((instances[0],), headers, attempts=CAPACITY + OVERSHOOT)

    assert result.allowed == CAPACITY, (
        f"one instance admitted {result.allowed} where the tier allows {CAPACITY} — the "
        "two-instance number below is only meaningful next to this one"
    )
    assert result.refused == OVERSHOOT


async def test_alternating_instances_never_double_spend_a_token(two_replicas, instances):
    """Request *k* reports ``capacity - k`` remaining, whichever instance served it.

    A stronger statement than the burst count and a different one: the burst proves the *total* is
    right, this proves every individual step is. With per-process buckets the sequence would read
    59, 59, 58, 58, 57, 57 — each replica counting its own half — and the total would still be
    wrong only at the very end. Here it must read 59, 58, 57, 56 ... with no repeats, which is what
    "one counter" looks like from the outside.

    ``X-RateLimit-Remaining`` carries
    :attr:`~src.models.LimitDecision.effective_remaining` — ``min(bucket, window)`` — and both
    gates are sized at 60 for this tier, so the expected value is unambiguous.
    """
    _client_a, _client_b, runtime = two_replicas
    headers = bearer(runtime.settings, fresh("walk"))
    probes = 12

    seen: list[int] = []
    for index in range(probes):
        client = instances[index % len(instances)]
        response = await client.get(WHOAMI, headers=headers)

        assert response.status_code == 200, response.text
        remaining = int(response.headers[RATELIMIT_REMAINING_HEADER])
        seen.append(remaining)
        assert remaining == CAPACITY - (index + 1), (
            f"request {index + 1} (served by {INSTANCE_NAMES[index % len(instances)]}) reported "
            f"{remaining} remaining; one shared bucket owes {CAPACITY - (index + 1)}. The "
            f"sequence so far is {seen} — a repeated value is one replica re-spending a token "
            "the other already spent."
        )


# =============================================================================================
# The quota counters — the same property, on the number that represents money
# =============================================================================================


async def test_the_daily_quota_counter_is_shared_across_instances(two_replicas, instances):
    """One daily counter, incremented by both instances, readable from either.

    A per-process bucket costs a customer twice their burst for a few seconds. A per-process
    *quota* counter costs them N times their paid daily allowance for a whole day, and unlike the
    bucket it does not self-correct — which is why ``redis`` runs with ``appendonly yes`` and why
    this assertion is made from ``GET /admin/users/{id}/usage`` on **both** replicas rather than
    from a header on one.
    """
    _client_a, _client_b, runtime = two_replicas
    user_id = fresh("quota")
    headers = bearer(runtime.settings, user_id)
    fired = 12

    for index in range(fired):
        response = await instances[index % len(instances)].get(WHOAMI, headers=headers)
        assert response.status_code == 200, response.text
        assert int(response.headers[QUOTA_REMAINING_HEADER]) == FREE_DAILY - (index + 1)

    for name, client in zip(INSTANCE_NAMES, instances):
        body = (await client.get(f"{ADMIN}/users/{user_id}/usage", headers=auth())).json()
        assert body["daily"]["used"] == fired, (
            f"{name} reports daily.used={body['daily']['used']} after {fired} requests split "
            f"across two instances; per-process counters would each report {fired // 2}"
        )
        assert body["daily"]["remaining"] == FREE_DAILY - fired
        # The monthly period is the same counter one level up, and it is where the same bug would
        # be most expensive: a month of double-counting is not something a reset fixes.
        assert body["monthly"]["used"] == fired


async def test_the_daily_quota_CEILING_binds_across_both_instances(two_replicas, instances):
    """A daily quota of N admits N requests **in total**, not N per replica.

    The counter test above proves the two instances add to the same number. This proves the number
    is what *refuses* them, which is a different failure: a shared counter that each replica
    compares against its own copy of the ceiling still lets 2N through.

    The quota is lowered at runtime rather than exhausted at its shipped 1000, which keeps the test
    to fourteen requests — and lowering it exercises the propagation path this file tests below,
    so the cheap setup is also coverage.
    """
    client_a, client_b, runtime = two_replicas
    quota = 6

    lowered = await client_a.put(
        f"{ADMIN}/tiers/free", json={"daily_quota": quota}, headers=auth()
    )
    assert lowered.status_code == 200, lowered.text
    # B served neither the write nor a request since; end its snapshot's TTL explicitly rather
    # than sleeping through `TIER_CACHE_TTL_SEC`. See the propagation tests below for the
    # unforced version of the same convergence.
    assert (await client_b.post(f"{ADMIN}/config/reload", headers=auth())).status_code == 200

    headers = bearer(runtime.settings, fresh("exhaust"))
    result = await burst(instances, headers, attempts=quota + 2)

    assert result.allowed == quota, per_process_would_be(
        result,
        instances=len(instances),
        ceiling=quota,
        named="daily quota",
        per_process="quota counters",
    )
    assert result.refused == 2

    denied = await client_b.get(WHOAMI, headers=headers)
    assert denied.status_code == 429
    # The reason names the gate that actually refused, so a quota exhaustion cannot be mistaken
    # for a burst the caller could retry out of.
    assert denied.json()["reason"] == "quota_daily"


# =============================================================================================
# Agreement — the two instances describe the same state
# =============================================================================================


async def test_both_instances_agree_on_remaining_within_one_token(two_replicas, instances):
    """Probe each instance once; the two ``Remaining`` values differ by exactly the probe between
    them.

    "Within one token" is the property, and on its own it is a *weak* assertion — worth saying
    plainly, because it is the trap: with per-process buckets both replicas would answer ``54``
    after five requests each, differ by **zero**, and sail through a ``<= 1`` tolerance. The
    tolerance only means something next to the absolute anchor, so both are asserted: after
    ``spent`` shared requests, A's probe (which spends one itself) owes ``capacity - spent - 1``
    and B's owes one less again.
    """
    client_a, client_b, runtime = two_replicas
    headers = bearer(runtime.settings, fresh("agree"))
    spent = 10

    warmup = await burst(instances, headers, attempts=spent)
    assert warmup.allowed == spent, warmup

    from_a = await client_a.get(WHOAMI, headers=headers)
    from_b = await client_b.get(WHOAMI, headers=headers)
    assert (from_a.status_code, from_b.status_code) == (200, 200)

    remaining_a = int(from_a.headers[RATELIMIT_REMAINING_HEADER])
    remaining_b = int(from_b.headers[RATELIMIT_REMAINING_HEADER])

    # The anchor: each probe is itself a request against the shared bucket.
    assert remaining_a == CAPACITY - spent - 1
    assert remaining_b == CAPACITY - spent - 2
    # The stated property, which the anchor above is what gives meaning to.
    assert abs(remaining_a - remaining_b) <= 1


# =============================================================================================
# Configuration propagation — instant for who, TTL-bounded for what
# =============================================================================================


async def test_a_user_tier_change_on_a_is_enforced_by_b_on_the_very_next_request(
    two_replicas,
):
    """``PUT /users/{id}/tier`` on A binds on B immediately: no reload, no TTL, no sleep.

    This is the half of hot reload that is instant, and it is instant by *omission*: ``user ->
    tier`` is read from ``user:{id}`` inside the decision script on every request and is cached
    nowhere — not in the tier registry (which caches what a tier *means*, never who is on one), not
    in the identity resolver, and not in a JWT claim. So there is no cache for B to be behind on.

    Asserted through ``X-RateLimit-Limit`` (the tier's rpm) rather than by reading the write back,
    because reading it back would only prove a write happened. The number a caller is *enforced*
    against is the claim.
    """
    client_a, client_b, runtime = two_replicas
    user_id = fresh("promote")
    headers = bearer(runtime.settings, user_id)

    before = await client_b.get(WHOAMI, headers=headers)
    assert before.status_code == 200
    assert before.json()["tier"] == "free"
    assert int(before.headers[RATELIMIT_LIMIT_HEADER]) == FREE_RPM

    assigned = await client_a.put(
        f"{ADMIN}/users/{user_id}/tier", json={"tier": "premium"}, headers=auth()
    )
    assert assigned.status_code == 200, assigned.text

    # No sleep, no reload, no restart — the very next request on the OTHER instance.
    after = await client_b.get(WHOAMI, headers=headers)
    assert after.status_code == 200
    assert after.json()["tier"] == "premium"
    assert int(after.headers[RATELIMIT_LIMIT_HEADER]) == PREMIUM_RPM, (
        "instance B is still pricing this principal at the free tier after instance A moved them "
        "to premium — user -> tier is read inside the Lua script and must not be cached anywhere"
    )


async def test_a_tier_definition_change_on_a_is_enforced_by_b_after_it_converges(
    two_replicas, instances
):
    """``PUT /tiers/{tier}`` on A binds on B once B's snapshot catches up — and then B *enforces*
    it.

    The asymmetry with the test above is the design, not a wrinkle: what a tier MEANS is cached per
    replica for ``TIER_CACHE_TTL_SEC`` (5 s) so that reading the tier table costs no round trip on
    the hot path, while WHO is on a tier is read inside the script every time. One is a bounded
    staleness an operator can be told about; the other could not be cached without making a
    downgrade wait for a TTL.

    Convergence is forced with ``POST /config/reload`` rather than awaited with ``sleep(5)``: the
    TTL bound is asserted where it can be asserted exactly (``tests/integration/test_tiers_redis.py``
    drives the registry against an injected clock), and a five-second sleep in the integration
    suite would buy nothing but wall time.

    The last block is the part that matters: after converging, B is asked to *enforce* the new
    ceiling alongside A, and the pair admit exactly the lowered number. A propagation test that
    stopped at "B reports the new value" would pass on a replica that reported one table and
    metered against another.
    """
    client_a, client_b, runtime = two_replicas
    lowered = 5

    written = await client_a.put(
        f"{ADMIN}/tiers/free", json={"rate_limit_per_min": lowered}, headers=auth()
    )
    assert written.status_code == 200, written.text
    assert written.json()["config"]["rate_limit_per_min"] == lowered
    version = written.json()["config_version"]

    # A served the write and re-read the table as part of it, so A is already enforcing the change.
    probe_user = fresh("definition")
    probe_headers = bearer(runtime.settings, probe_user)
    on_a = await client_a.get(WHOAMI, headers=probe_headers)
    assert int(on_a.headers[RATELIMIT_LIMIT_HEADER]) == lowered

    reloaded = await client_b.post(f"{ADMIN}/config/reload", headers=auth())
    assert reloaded.status_code == 200, reloaded.text
    assert reloaded.json()["config_version"] == version
    assert reloaded.json()["tiers"]["free"]["rate_limit_per_min"] == lowered

    on_b = await client_b.get(WHOAMI, headers=probe_headers)
    assert int(on_b.headers[RATELIMIT_LIMIT_HEADER]) == lowered, (
        "instance B reports a converged config_version and is still advertising the old ceiling"
    )

    # Reported is not enforced. A fresh principal, the lowered ceiling, both instances.
    result = await burst(
        instances, bearer(runtime.settings, fresh("lowered")), attempts=lowered + 3
    )
    assert result.allowed == lowered, per_process_would_be(
        result,
        instances=len(instances),
        ceiling=lowered,
        named="the lowered rpm",
        per_process="tier tables",
    )


# =============================================================================================
# The weighted cost, distributed
# =============================================================================================


async def test_a_weighted_endpoint_shares_one_bucket_at_its_real_cost(two_replicas, instances):
    """``/logs/query`` costs 5, so one free tier's capacity is 12 calls — across both instances.

    Two independent things could go wrong here and only their product is observable in a single
    number, which is why the number is asserted exactly. The cost could be per-process (24 calls
    admitted), or the classification could be per-process-correct but the *charge* could land on
    the wrong bucket — the failure ``src.keys.classify`` documents, and the one a forwarded path
    prefix would cause at the proxy (a prefixed ``/gw/api/v1/logs/query`` is served by the
    expensive handler and classified as ``other``, charged 1 token to an unrelated key). Both show
    up as "more than 12 got through".

    This is the in-process half of the live check the LB verification makes with
    ``curl localhost:8020/api/v1/logs/query``: the weighted price must survive the hop.
    """
    _client_a, _client_b, runtime = two_replicas
    headers = bearer(runtime.settings, fresh("weighted"))
    affordable = CAPACITY // LOGS_QUERY_COST

    result = await burst(instances, headers, attempts=affordable + 3, path=LOGS_QUERY)

    assert result.allowed == affordable, per_process_would_be(
        result,
        instances=len(instances),
        ceiling=affordable,
        named=f"{LOGS_QUERY} calls at {LOGS_QUERY_COST} tokens each",
        per_process="buckets",
    )
    assert all(count > 0 for count in result.by_instance)
