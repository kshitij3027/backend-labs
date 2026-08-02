"""Integration tests for :class:`~src.tiers.TierRegistry` against a REAL redis:7-alpine.

Four things can only be proved against a real server, and each has a test below:

1. **``HSETNX`` really is not ``HSET``.** ``test_seeding_twice_does_not_overwrite_an_operator_edit``
   is the single most important test in this file. It is the difference between "runtime tier
   changes are a feature" and "runtime tier changes silently revert on the next replica restart,
   at some unrelated later moment, with no error and no log line".
2. **The stored shape is what an operator can hand-edit.** The value is a pipe-delimited string in
   a HASH, not a pickled blob, and ``redis-cli HSET config:tiers free 10|60|1000|25000`` is a
   supported way to change a limit. Only a real server can prove the value we write is the value
   that comes back.
3. **A refresh picks up an out-of-band write within one call.** That is the mechanism behind
   C10's whole "no restart required" claim, minus the HTTP.
4. **Two independent registries on one Redis converge.** This is the two-replica property C12
   depends on, asserted at the level where it is cheap to assert.
"""

from __future__ import annotations

import time

import pytest

from src.config import Settings, TierConfig
from src.keys import CONFIG_TIERS_KEY, CONFIG_VERSION_KEY
from src.redis_client import RedisGateway
from src.tiers import TierRegistry, encode_tier, render_argv_tail

#: The spec's three tiers with the spec's numbers, in the exact stored form.
EXPECTED_VALUES = {
    b"free": b"60|60|1000|25000",
    b"premium": b"300|300|50000|1250000",
    b"enterprise": b"1000|1000|500000|12500000",
}

#: What an operator's runtime change to `free` looks like on the wire — rpm dropped to 10.
OPERATOR_EDIT = b"10|60|1000|25000"


@pytest.fixture()
def registry(redis_settings: Settings, gateway: RedisGateway) -> TierRegistry:
    """A registry over the flushed, connected gateway fixture."""
    return TierRegistry(redis_settings, gateway)


async def test_seed_writes_all_three_tiers_and_the_version(
    registry: TierRegistry, gateway: RedisGateway
):
    """The spec's table, byte for byte, plus the version key C10 watches."""
    await registry.seed()

    stored = await gateway.client.hgetall(CONFIG_TIERS_KEY)
    assert stored == EXPECTED_VALUES
    assert await gateway.client.get(CONFIG_VERSION_KEY) == b"1"


async def test_seeding_twice_does_not_overwrite_an_operator_edit(
    registry: TierRegistry, gateway: RedisGateway
):
    """**The HSETNX contract, and the most important assertion in this file.**

    An operator lowers `free`'s rpm at 14:00 through the admin API. At 14:20 a replica restarts —
    a deploy, an OOM kill, a node drain, a scale-up — and runs `seed()` again. With `HSET` their
    change is gone: no error, no log line, and not even immediately, but at some unrelated later
    moment. That is the most expensive shape a bug can have, and it is exactly what makes people
    stop trusting hot reload and go back to redeploying for a number change.

    With `HSETNX` the seed is a no-op over anything that already exists, so startup seeding and
    runtime editing compose instead of fighting.
    """
    await registry.seed()
    await gateway.client.hset(CONFIG_TIERS_KEY, "free", OPERATOR_EDIT)

    await registry.seed()

    assert await gateway.client.hget(CONFIG_TIERS_KEY, "free") == OPERATOR_EDIT
    # And the untouched tiers are still the seeded values.
    assert await gateway.client.hget(CONFIG_TIERS_KEY, "premium") == EXPECTED_VALUES[b"premium"]
    # Nothing was written, so the version did not move — a bump on every restart would invalidate
    # every replica's snapshot for no reason.
    assert await gateway.client.get(CONFIG_VERSION_KEY) == b"1"


async def test_a_second_replica_seeding_the_same_store_changes_nothing(
    redis_settings: Settings, gateway: RedisGateway
):
    """Every replica runs `seed()` on boot; only the first one may actually write."""
    first = TierRegistry(redis_settings, gateway)
    second = TierRegistry(redis_settings, gateway)
    await first.seed()
    await gateway.client.hset(CONFIG_TIERS_KEY, "free", OPERATOR_EDIT)

    await second.seed()

    assert await gateway.client.hget(CONFIG_TIERS_KEY, "free") == OPERATOR_EDIT


async def test_reseed_overwrites_and_bumps_the_version(
    registry: TierRegistry, gateway: RedisGateway
):
    """The deliberate escape hatch: the compose `test` service wants a known table, not history."""
    await registry.seed()
    await gateway.client.hset(CONFIG_TIERS_KEY, "free", OPERATOR_EDIT)

    await registry.seed(reseed=True)

    assert await gateway.client.hget(CONFIG_TIERS_KEY, "free") == EXPECTED_VALUES[b"free"]
    assert await gateway.client.get(CONFIG_VERSION_KEY) == b"2"


async def test_seeding_a_tier_added_to_the_configuration_bumps_the_version(
    redis_settings: Settings, gateway: RedisGateway
):
    """An operator adds a tier to TIER_LIMITS and restarts: HSETNX creates it, the version moves.

    Without the bump, replicas that already hold a snapshot would have no signal that the table
    grew, and `config_version` on `/health` — which C10 uses to watch propagation — would be flat
    across a change that really happened.
    """
    await TierRegistry(redis_settings, gateway).seed()
    assert await gateway.client.get(CONFIG_VERSION_KEY) == b"1"

    extended = redis_settings.model_copy(
        update={
            "tier_limits": {
                **redis_settings.tier_limits,
                "platinum": TierConfig(
                    name="platinum",
                    rate_limit_per_min=5000,
                    burst=5000,
                    daily_quota=1_000_000,
                    monthly_quota=25_000_000,
                ),
            }
        }
    )
    await TierRegistry(extended, gateway).seed()

    assert await gateway.client.hget(CONFIG_TIERS_KEY, "platinum") == b"5000|5000|1000000|25000000"
    assert await gateway.client.get(CONFIG_VERSION_KEY) == b"2"


async def test_refresh_picks_up_an_out_of_band_edit_within_one_call(
    registry: TierRegistry, gateway: RedisGateway
):
    """**The mechanism behind 'config updates without a service restart', minus the HTTP.**

    A plain `redis-cli HSET` is a supported way to change a limit, which is only true because the
    stored value is a legible pipe-delimited string rather than an encoded blob.
    """
    await registry.start()
    assert registry.snapshot().tiers["free"].rate_limit_per_min == 60
    before_version = registry.snapshot().version

    await gateway.client.hset(CONFIG_TIERS_KEY, "free", OPERATOR_EDIT)
    await gateway.client.incr(CONFIG_VERSION_KEY)
    await registry.refresh()

    snapshot = registry.snapshot()
    assert snapshot.tiers["free"].rate_limit_per_min == 10
    assert snapshot.tiers["free"].burst == 60  # the untouched fields survive the edit
    assert snapshot.version == before_version + 1
    # The pre-rendered ARGV tail is rebuilt with it, or the hot path would keep sending the old
    # number to the decision script while /health cheerfully reported a new version.
    assert snapshot.argv_tail == render_argv_tail(snapshot.tiers)
    assert snapshot.argv_tail[snapshot.argv_tail.index("free") + 1] == "10"

    await registry.stop()


async def test_a_malformed_row_written_by_hand_cannot_make_a_tier_unlimited(
    registry: TierRegistry, gateway: RedisGateway, redis_settings: Settings
):
    """The fat-finger case, end to end: `0` in a real HASH, read back by a real refresh.

    The decision script reads a non-positive limit as "this gate is not enforcing anything", so a
    typo here would hand `free` an unmetered daily allowance. The configured default is used
    instead.
    """
    await registry.seed()
    await gateway.client.hset(CONFIG_TIERS_KEY, "free", "60|60|0|25000")

    snapshot = await registry.refresh()

    assert snapshot.tiers["free"].daily_quota == 1000
    assert snapshot.tiers["free"] == redis_settings.tier_limits["free"]


async def test_a_snapshot_survives_redis_going_away(
    redis_settings: Settings, gateway: RedisGateway
):
    """A store that stops answering must not cost the replica the table it already has.

    Reaching that state honestly means closing the connection out from under the registry, which is
    what a Redis restart looks like from this side — so this test owns a **second** gateway and
    closes that one. The shared ``gateway`` fixture is still requested, purely for its
    flush-on-entry / flush-on-exit contract: closing it here would leave its own teardown reaching
    through a client it no longer has.
    """
    own = RedisGateway(redis_settings)
    await own.connect()
    registry = TierRegistry(redis_settings, own)
    await registry.start()
    good = registry.snapshot()
    assert good.tiers["free"].rate_limit_per_min == 60

    await own.aclose()
    after = await registry.refresh()

    assert after is good
    assert after.tiers["free"].rate_limit_per_min == 60
    assert registry.refresh_failures == 1


async def test_two_registries_on_one_redis_converge_on_the_same_table(
    redis_settings: Settings, gateway: RedisGateway
):
    """**The two-replica property C12 depends on**, asserted where it is cheap to assert.

    Two independently constructed registries — the same relationship two API replicas have — must
    agree on the table *and* on the pre-rendered ARGV tail after a refresh, because that tail is
    literally what each of them sends to the decision script. Two replicas that disagreed about a
    tier's numbers would be two rate limits, which is the bug this whole project exists to not
    have.
    """
    replica_a = TierRegistry(redis_settings, gateway)
    replica_b = TierRegistry(redis_settings, gateway)
    await replica_a.start()

    # An admin change lands on replica A's Redis (which is everyone's Redis).
    await gateway.client.hset(CONFIG_TIERS_KEY, "premium", "42|99|4242|99999")
    await gateway.client.incr(CONFIG_VERSION_KEY)

    await replica_a.refresh()
    await replica_b.refresh()

    snapshot_a = replica_a.snapshot()
    snapshot_b = replica_b.snapshot()
    assert snapshot_a.tiers == snapshot_b.tiers
    assert snapshot_a.argv_tail == snapshot_b.argv_tail
    assert snapshot_a.version == snapshot_b.version == 2
    assert snapshot_b.tiers["premium"].rate_limit_per_min == 42

    await replica_a.stop()
    await replica_b.stop()


async def test_the_stored_value_is_exactly_what_encode_tier_produces(
    registry: TierRegistry, gateway: RedisGateway, redis_settings: Settings
):
    """The encoder and the store agree, so a hand-written HSET and a seeded one are the same thing."""
    await registry.seed()

    for name, config in redis_settings.tier_limits.items():
        stored = await gateway.client.hget(CONFIG_TIERS_KEY, name)
        assert stored is not None
        assert stored.decode() == encode_tier(config)


async def test_the_registry_never_blocks_the_hot_path_on_redis(
    redis_settings: Settings, gateway: RedisGateway
):
    """`snapshot()` against a real, live Redis is still a pure attribute read.

    Timed rather than merely asserted structurally: this is the property the 5 ms budget depends
    on, and a future `await` slipped into `snapshot()` would show up here as a jump from
    nanoseconds to a network round trip long before it showed up as a latency regression in
    production.
    """
    registry = TierRegistry(redis_settings, gateway)
    await registry.start()

    started = time.perf_counter()
    for _ in range(1000):
        registry.snapshot()
    elapsed = time.perf_counter() - started

    await registry.stop()

    # 1000 reads. Even a single localhost round trip would blow this budget by an order of
    # magnitude; the generous ceiling is so a loaded CI box cannot flake it.
    assert elapsed < 0.05, f"1000 snapshot() calls took {elapsed:.4f}s — that is not a cache"
