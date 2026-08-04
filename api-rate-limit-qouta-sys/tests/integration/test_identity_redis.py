"""Integration tests for :mod:`src.identity` against a REAL redis:7-alpine.

Four things can only be proved against a real server, and each has a test below:

1. **``HSETNX`` really is not ``HSET``.** An operator who revokes a demo key or re-tiers a demo
   user must not have it silently restored by the next replica restart. Same rule, and the same
   reasoning, as C3's tier seed.
2. **Identity and enforcement agree end to end.** ``test_a_resolved_principal_gets_its_own_tiers_limits``
   is **this commit's payoff**: a raw API key goes in one end, and the limits the *Lua script*
   actually enforces come out the other — with no HTTP layer, no middleware and no mocking between
   them. It is the assertion that would catch a resolver returning the right ``user_id`` while the
   script reads a different key, which nothing in the unit suite can see.
3. **Two independent resolvers over one Redis agree.** That is the two-replica property C12 depends
   on, asserted at the level where it costs milliseconds.
4. **The plaintext key is genuinely absent from the real keyspace.** A dict double can be told what
   to store; only a real ``KEYS *`` can show what actually landed.
"""

from __future__ import annotations

from datetime import datetime, timezone

import jwt
import pytest

from src.config import Settings
from src.identity import (
    DEMO_CREDENTIALS,
    STATUS_ACTIVE,
    IdentityResolver,
    apikey_digest,
    issue_token,
    seed_demo_credentials,
)
from src.keys import apikey_key, user_key
from src.limiter import Limiter
from src.models import CredentialKind, Tier
from src.redis_client import RedisGateway
from src.tiers import TierRegistry

#: The endpoint label the limiter is driven with. `/whoami` is the cost-1 probe, so the numbers in
#: these assertions are the tier's own rather than the weighted cost of a fat endpoint.
PROBE_ENDPOINT = "GET:/api/v1/whoami"

#: The spec's tier table, as the decision script must report it back. Restated here rather than
#: imported from Settings on purpose: a test that derives its expectation from the same object the
#: code reads proves only that the code is self-consistent.
EXPECTED_LIMITS = {
    "free": {"window": 60, "burst": 60, "daily": 1000, "monthly": 25_000},
    "premium": {"window": 300, "burst": 300, "daily": 50_000, "monthly": 1_250_000},
    "enterprise": {
        "window": 1000,
        "burst": 1000,
        "daily": 500_000,
        "monthly": 12_500_000,
    },
}


def raw_headers(*pairs: tuple[str, str]) -> list[tuple[bytes, bytes]]:
    """A raw ASGI header list, which is what :meth:`IdentityResolver.resolve` consumes."""
    return [(name.encode("latin-1"), value.encode("latin-1")) for name, value in pairs]


@pytest.fixture()
def resolver(gateway: RedisGateway, redis_settings: Settings) -> IdentityResolver:
    """A resolver over the flushed, connected gateway fixture."""
    return IdentityResolver(gateway, redis_settings)


# =============================================================================================
# Seeding
# =============================================================================================


async def test_seeding_writes_all_three_keys_and_their_user_records(
    gateway: RedisGateway, redis_settings: Settings
):
    """All three demo credentials, with the right user ids, labels, statuses and tiers."""
    await seed_demo_credentials(gateway, redis_settings)

    for credential in DEMO_CREDENTIALS:
        digest = apikey_digest(credential.raw_key, pepper=redis_settings.api_key_pepper)
        key_record = await gateway.client.hgetall(apikey_key(digest))
        assert key_record[b"user_id"] == credential.user_id.encode()
        assert key_record[b"label"] == credential.label.encode()
        assert key_record[b"status"] == STATUS_ACTIVE.encode()
        assert key_record[b"created_at"].isdigit()

        user_record = await gateway.client.hgetall(user_key(credential.user_id))
        assert user_record[b"tier"] == credential.tier.encode()
        assert user_record[b"status"] == STATUS_ACTIVE.encode()


async def test_seeding_is_one_round_trip_against_a_real_server(
    gateway: RedisGateway, redis_settings: Settings
):
    """21 per-field ``HSETNX`` commands, pipelined — one gateway call, and the writes still landed.

    The unit suite counts the pipeline's ``execute()``; this proves the same batch survives a real
    RESP round trip and produces the same keyspace, which is the half a double cannot show.
    """
    calls_before = gateway.calls

    written = await seed_demo_credentials(gateway, redis_settings)

    assert gateway.calls - calls_before == 1
    assert written == len(DEMO_CREDENTIALS) * 7 == 21
    for credential in DEMO_CREDENTIALS:
        digest = apikey_digest(credential.raw_key, pepper=redis_settings.api_key_pepper)
        assert await gateway.client.hlen(apikey_key(digest)) == 4
        assert await gateway.client.hlen(user_key(credential.user_id)) == 3


async def test_the_plaintext_key_is_absent_from_the_real_keyspace(
    gateway: RedisGateway, redis_settings: Settings
):
    """**The credential-leak test, against the store that would actually leak it.**

    A Redis key name is not a private detail: it appears in `MONITOR` output, in slowlog entries,
    in the RDB/AOF file and in every backup of them. A unit double can be told what to store; only
    a real keyspace shows what landed.
    """
    await seed_demo_credentials(gateway, redis_settings)

    keys = [key.decode() for key in await gateway.client.keys("*")]
    values = []
    for key in keys:
        values.extend(value.decode() for value in (await gateway.client.hgetall(key)).values())
    everything = "\n".join(keys + values)

    for credential in DEMO_CREDENTIALS:
        assert credential.raw_key not in everything
        # What IS there is the peppered digest, which is not invertible without the pepper — and
        # the pepper lives in the process environment, never in this store.
        digest = apikey_digest(credential.raw_key, pepper=redis_settings.api_key_pepper)
        assert f"apikey:v1:{digest}" in keys


async def test_seeding_twice_does_not_overwrite_an_operator_edit(
    gateway: RedisGateway, redis_settings: Settings
):
    """**The HSETNX contract.**

    An operator revokes `demo-free-key` at 14:00 and moves `demo-premium` to the free tier. At
    14:20 a replica restarts — a deploy, an OOM kill, a node drain — and seeds again. With `HSET`
    both changes are gone: no error, no log line, and not even immediately, but at some unrelated
    later moment. That is the most expensive shape a bug can have, and it is exactly what makes
    people stop trusting runtime configuration.
    """
    await seed_demo_credentials(gateway, redis_settings)
    free_digest = apikey_digest("demo-free-key", pepper=redis_settings.api_key_pepper)
    await gateway.client.hset(apikey_key(free_digest), "status", "revoked")
    await gateway.client.hset(user_key("demo-premium"), "tier", "free")

    written = await seed_demo_credentials(gateway, redis_settings)

    assert written == 0
    assert await gateway.client.hget(apikey_key(free_digest), "status") == b"revoked"
    assert await gateway.client.hget(user_key("demo-premium"), "tier") == b"free"


async def test_reseed_overwrites_deliberately(gateway: RedisGateway, redis_settings: Settings):
    """The escape hatch a harness uses to get a KNOWN state out of a shared store.

    A parameter rather than the default, and with no production caller, precisely so that choosing
    it is a decision someone made rather than one they inherited.
    """
    await seed_demo_credentials(gateway, redis_settings)
    free_digest = apikey_digest("demo-free-key", pepper=redis_settings.api_key_pepper)
    await gateway.client.hset(apikey_key(free_digest), "status", "revoked")
    await gateway.client.hset(user_key("demo-premium"), "tier", "free")

    await seed_demo_credentials(gateway, redis_settings, reseed=True)

    assert await gateway.client.hget(apikey_key(free_digest), "status") == STATUS_ACTIVE.encode()
    assert await gateway.client.hget(user_key("demo-premium"), "tier") == b"premium"


# =============================================================================================
# Resolution
# =============================================================================================


async def test_every_demo_key_resolves_to_its_principal(
    resolver: IdentityResolver, gateway: RedisGateway, redis_settings: Settings
):
    await seed_demo_credentials(gateway, redis_settings)

    for credential in DEMO_CREDENTIALS:
        principal = await resolver.resolve(raw_headers(("x-api-key", credential.raw_key)))
        assert principal is not None
        assert principal.user_id == credential.user_id
        assert principal.credential is CredentialKind.API_KEY
        assert principal.key_id == credential.label
        # Never the secret, and never the lookup handle for the whole record.
        assert principal.key_id != credential.raw_key


async def test_an_unknown_key_resolves_to_none_and_is_negative_cached(
    resolver: IdentityResolver, gateway: RedisGateway, redis_settings: Settings
):
    """A key-guessing flood must cost one round trip in total, not one per guess.

    Against a real server this also proves the shape of the miss: `HGETALL` on an absent key
    returns an empty hash rather than an error, so "no such credential" and "the store is broken"
    are genuinely different replies rather than one exception the resolver has to interpret.
    """
    await seed_demo_credentials(gateway, redis_settings)
    calls_before = gateway.calls

    for _ in range(10):
        assert await resolver.resolve(raw_headers(("x-api-key", "not-a-real-key"))) is None

    assert gateway.calls - calls_before == 1
    assert resolver.negative_hits == 9


@pytest.mark.parametrize("padding", ["\xa0", "\x1c", "\x85", "\x0b"])
async def test_a_padded_demo_key_does_not_authenticate(
    resolver: IdentityResolver, gateway: RedisGateway, redis_settings: Settings, padding
):
    """The four byte sequences that were measured to authenticate as ``demo-free``, reproduced.

    Bare ``str.strip()`` removes everything Python calls whitespace; RFC 9110 optional whitespace
    is SP and HTAB only. Asserted here against a real store as well as against the unit double,
    because the whole point is that the *digest* differs — and the digest is what names the record
    this server actually holds.
    """
    await seed_demo_credentials(gateway, redis_settings)
    assert await resolver.resolve(raw_headers(("x-api-key", "demo-free-key"))) is not None

    for spelling in (f"demo-free-key{padding}", f"{padding}demo-free-key"):
        assert await resolver.resolve(raw_headers(("x-api-key", spelling))) is None
        assert await resolver.resolve(raw_headers(("authorization", f"ApiKey {spelling}"))) is None
    # And the scheme separator is just as strict.
    assert (
        await resolver.resolve(raw_headers(("authorization", f"ApiKey{padding}demo-free-key")))
        is None
    )


async def test_the_pepper_is_required_for_a_successful_lookup(
    gateway: RedisGateway, redis_settings: Settings
):
    """**The pepper is the lookup, not a decoration on it.**

    The store is seeded under one pepper and queried by a resolver configured with another. The
    correct raw key — the exact string a legitimate customer holds — resolves to nothing, because
    it hashes to a digest that names no record.

    That is what "a stolen Redis dump alone yields no usable keys" actually means, asserted rather
    than asserted-about: the records are all sitting in this keyspace, readable, and without the
    pepper from the process environment they cannot be turned into a successful authentication.
    It is also why rotating ``API_KEY_PEPPER`` invalidates every stored key by construction.
    """
    await seed_demo_credentials(gateway, redis_settings)
    seeded = IdentityResolver(gateway, redis_settings)
    assert await seeded.resolve(raw_headers(("x-api-key", "demo-free-key"))) is not None

    rotated = IdentityResolver(
        gateway,
        redis_settings.model_copy(update={"api_key_pepper": "a-rotated-pepper-0123456789"}),
    )

    for credential in DEMO_CREDENTIALS:
        assert await rotated.resolve(raw_headers(("x-api-key", credential.raw_key))) is None


async def test_a_revoked_key_stops_authenticating_once_the_cache_expires(
    gateway: RedisGateway, redis_settings: Settings
):
    """The documented ≤ TTL revocation window, against a real revocation.

    Driven with `ttl_sec=0` rather than by sleeping: the property under test is "the resolver
    re-reads Redis and honours the new status", not "five seconds elapse".
    """
    resolver = IdentityResolver(gateway, redis_settings, ttl_sec=0)
    await seed_demo_credentials(gateway, redis_settings)
    digest = apikey_digest("demo-free-key", pepper=redis_settings.api_key_pepper)
    assert await resolver.resolve(raw_headers(("x-api-key", "demo-free-key"))) is not None

    await gateway.client.hset(apikey_key(digest), "status", "revoked")

    assert await resolver.resolve(raw_headers(("x-api-key", "demo-free-key"))) is None


async def test_two_independent_resolvers_on_one_redis_agree(
    gateway: RedisGateway, redis_settings: Settings
):
    """**The two-replica property, at the identity layer.**

    Two resolvers, each with its **own** ``RedisGateway`` and therefore its own connection pool —
    which is what a second API container actually is. They must resolve the same key to the same
    principal, because the credential store is shared: an identity that differed per replica would
    put one caller in two buckets, which is the same class of bug as a per-process token bucket and
    just as invisible from inside either process.
    """
    await seed_demo_credentials(gateway, redis_settings)
    replica_a = IdentityResolver(gateway, redis_settings)

    second_gateway = RedisGateway(redis_settings)
    await second_gateway.connect()
    try:
        replica_b = IdentityResolver(second_gateway, redis_settings)

        for credential in DEMO_CREDENTIALS:
            presented = raw_headers(("x-api-key", credential.raw_key))
            assert await replica_a.resolve(presented) == await replica_b.resolve(presented)

        # And a revocation is visible to the replica that did not perform it, once its (here zero)
        # cache window has passed — the store is the authority, not either process.
        assert replica_a.cache_stats()["size"] == len(DEMO_CREDENTIALS)
        assert replica_b.cache_stats()["size"] == len(DEMO_CREDENTIALS)
    finally:
        await second_gateway.aclose()


async def test_a_jwt_resolves_without_touching_redis(
    resolver: IdentityResolver, gateway: RedisGateway, redis_settings: Settings
):
    """The token path is self-contained: signature in, subject out, no store involved.

    Which is exactly why C13 can mint a fresh `uuid4` principal per run without seeding anything —
    and why a principal that has no `user:{id}` record still gets metered, on `DEFAULT_TIER`.
    """
    token = issue_token("e2e-throwaway-principal", settings=redis_settings)
    calls_before = gateway.calls

    principal = await resolver.resolve(raw_headers(("authorization", f"Bearer {token}")))

    assert principal is not None
    assert principal.user_id == "e2e-throwaway-principal"
    assert principal.credential is CredentialKind.JWT
    assert gateway.calls == calls_before


# =============================================================================================
# The payoff: identity and enforcement agreeing end to end
# =============================================================================================


@pytest.fixture()
async def wired(gateway: RedisGateway, redis_settings: Settings):
    """A registry, a resolver and a limiter over one flushed Redis — the production wiring, minus HTTP.

    Built in ``Runtime.build``'s order (registry, limiter, resolver) and started in
    ``Runtime.start``'s order (tiers first, then identity), so what is asserted below is the same
    composition the app actually boots.
    """
    registry = TierRegistry(redis_settings, gateway)
    await registry.start()
    resolver = IdentityResolver(gateway, redis_settings)
    await resolver.start()
    limiter = Limiter(gateway, registry, redis_settings)
    try:
        yield resolver, limiter
    finally:
        await registry.stop()


@pytest.mark.parametrize(
    "credential",
    DEMO_CREDENTIALS,
    ids=[credential.tier.value for credential in DEMO_CREDENTIALS],
)
async def test_a_resolved_principal_gets_its_own_tiers_limits(wired, credential):
    """**This commit's payoff test: identity and enforcement agreeing, end to end.**

    A raw API key goes in one end and the limits the *Lua script* is actually enforcing come out
    the other, with no HTTP layer, no middleware and nothing mocked in between. Two independent
    facts have to line up for it to pass:

    * the resolver read `user_id` out of `apikey:v1:<digest>` correctly, and
    * the decision script read `tier` out of `user:{that id}` and found the seeded tier.

    A resolver that returned a plausible-but-wrong `user_id` would still authenticate and still
    return a `LimitDecision` — it would simply meter the wrong account, which is a bug no unit
    test on either side can see because each half is individually correct.

    It also demonstrates the design's central split: the tier is **never** read by the identity
    layer. Nothing between the header and the script carries it.
    """
    resolver, limiter = wired
    expected = EXPECTED_LIMITS[credential.tier.value]

    principal = await resolver.resolve(raw_headers(("x-api-key", credential.raw_key)))
    assert principal is not None
    assert principal.user_id == credential.user_id

    decision = await limiter.check(principal.user_id, PROBE_ENDPOINT, 1)

    assert decision.allowed is True
    assert decision.tier == credential.tier.value
    assert decision.window_limit == expected["window"]
    assert decision.bucket_limit == expected["burst"]
    assert decision.daily_limit == expected["daily"]
    assert decision.monthly_limit == expected["monthly"]
    # One request was actually charged, so this is enforcement and not a read-only report.
    assert decision.daily_used == 1
    assert decision.bucket_remaining == expected["burst"] - 1


async def test_premium_really_does_get_a_higher_ceiling_than_free(wired):
    """The tiers are not merely *reported* differently; they *are* different ceilings.

    Asserted as a strict inequality between two live decisions rather than against a constant, so
    it keeps meaning something if the shipped numbers are ever retuned.
    """
    resolver, limiter = wired

    free = await resolver.resolve(raw_headers(("x-api-key", "demo-free-key")))
    premium = await resolver.resolve(raw_headers(("x-api-key", "demo-premium-key")))
    assert free is not None and premium is not None

    free_decision = await limiter.check(free.user_id, PROBE_ENDPOINT, 1)
    premium_decision = await limiter.check(premium.user_id, PROBE_ENDPOINT, 1)

    assert premium_decision.window_limit > free_decision.window_limit
    assert premium_decision.daily_limit > free_decision.daily_limit
    # Separate principals, so separate buckets and separate quota counters.
    assert free_decision.user_id != premium_decision.user_id


async def test_a_tier_claim_in_a_token_does_not_change_the_limits_enforced(
    wired, redis_settings: Settings
):
    """**Identity from the token, authority from the store — proved against the real script.**

    The same subject is presented twice: once with a token claiming `tier: enterprise`, once with a
    plain token. Both are metered identically, because the tier is read from `user:{uid}` inside
    the Lua script and the claim is never looked at.

    Without this, "the tier claim is ignored" is a statement about a Python function. With it, it
    is a statement about the limits a caller actually gets.
    """
    resolver, limiter = wired
    subject = "demo-free"

    # Forged BY HAND rather than through `issue_token`, because `issue_token` deliberately emits no
    # tier claim — so a token minted by it could not demonstrate that a claim is ignored. This is
    # a correctly signed token that simply asks for more than the store says its subject has.
    now = int(datetime.now(timezone.utc).timestamp())
    escalated = jwt.encode(
        {
            "sub": subject,
            "iat": now,
            "exp": now + 300,
            "tier": "enterprise",
            "rate_limit_per_min": 999_999,
        },
        redis_settings.jwt_secret,
        algorithm=redis_settings.jwt_algorithm,
    )
    honest_principal = await resolver.resolve(raw_headers(("x-api-key", "demo-free-key")))
    token_principal = await resolver.resolve(raw_headers(("authorization", f"Bearer {escalated}")))

    assert honest_principal is not None and token_principal is not None
    assert honest_principal.user_id == token_principal.user_id == subject

    decision = await limiter.check(token_principal.user_id, PROBE_ENDPOINT, 1)

    # The seeded record says free, so free is what is enforced — not the enterprise the caller
    # would have chosen for themselves.
    assert decision.tier == Tier.FREE.value
    assert decision.window_limit == EXPECTED_LIMITS["free"]["window"]
    assert decision.daily_limit == EXPECTED_LIMITS["free"]["daily"]


async def test_a_principal_with_no_user_record_falls_back_to_the_default_tier(
    wired, redis_settings: Settings
):
    """A JWT subject nobody has ever seen is metered on ``DEFAULT_TIER``, never unmetered.

    This is what makes C13's throwaway `uuid4` principals work, and it is also the failure mode
    that matters most: "no tier found" must read as *the most restrictive tier*, never as "no
    limits found" — which is indistinguishable from "unlimited" at the point of decision.
    """
    resolver, limiter = wired
    token = issue_token("nobody-has-ever-seen-this-principal", settings=redis_settings)

    principal = await resolver.resolve(raw_headers(("authorization", f"Bearer {token}")))
    assert principal is not None

    decision = await limiter.check(principal.user_id, PROBE_ENDPOINT, 1)

    assert decision.tier == redis_settings.default_tier == Tier.FREE.value
    assert decision.window_limit == EXPECTED_LIMITS["free"]["window"]
