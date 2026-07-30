"""Unit tests for :mod:`src.config`.

Four things are pinned here, in rough order of how expensive they are to get wrong:

1. **Every declared default matches the documented table.** ``README.md``, ``.env.example`` and
   ``docker-compose.yml``'s ``${VAR:-default}`` all restate these numbers, and there is no
   mechanism that keeps four copies in agreement except a test that fails when they diverge.
2. **Every field is documented.** A field added without a row in the table fails
   :func:`test_every_settings_field_is_documented`, which is what stops the table from silently
   becoming a subset of reality.
3. **Every validator actually rejects its bad input**, asserted on the *message* rather than just
   on the raise. A validator that fires with an unhelpful message is only half a feature: the
   whole reason these exist is that the values they refuse are ones a reasonable operator would
   type on purpose, so the message has to explain why they are wrong.
4. **Overrides and caching behave as documented** — env vars win over defaults, and
   ``get_settings`` is memoised per process.

Defaults are read off ``Settings.model_fields`` rather than from a constructed instance. That is
not a stylistic choice: the compose ``test`` service sets ``SEED_ENTRIES``, ``DATABASE_URL`` and
others in the container's environment, so ``Settings().seed_entries`` is 0 in Docker and 2000 on a
bare host. Asserting on the *declared* default compares the same thing in both places.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.config import Settings, get_settings

#: The configuration table exactly as ``README.md`` and ``.env.example`` publish it. Keep the
#: three in sync — :func:`test_every_settings_field_is_documented` fails when a field is added
#: here-or-there but not everywhere.
EXPECTED_DEFAULTS: dict[str, object] = {
    # Server / logging
    "host": "0.0.0.0",
    "port": 8000,
    "log_level": "INFO",
    # Database
    "database_url": "postgresql+asyncpg://gqllogs:gqllogs@postgres:5432/gqllogs",
    "db_pool_size": 10,
    "db_max_overflow": 5,
    "db_init_retries": 10,
    "db_init_retry_delay_seconds": 2.0,
    # Redis
    "redis_url": "redis://redis:6379/0",
    # Seed corpus
    "seed_entries": 2000,
    "seed_orders": 200,
    "random_seed": 20260725,
    # Query limits
    "default_query_limit": 100,
    "max_query_limit": 500,
    # Caching
    "cache_enabled": True,
    "cache_ttl_seconds": 30,
    #: Shared by `logStats` and `paymentOutcomeBreakdown` — the two ADDITIVE aggregates, whose
    #: counts a single write moves by one out of thousands.
    "agg_cache_ttl_seconds": 60,
    #: C11's per-aggregation TTL policy (spec §3 Feature Area D). The two numbers below bracket the
    #: shared one above, and the ordering IS the policy: `orderStatusDistribution` is
    #: REDISTRIBUTIVE (one event moves an order between buckets, so a stale answer is wrong in two
    #: places at once) and `orderFunnel` is MONOTONIC (a status once reached is never un-reached, so
    #: a stale read can only undercount). `src.cache.TTL_POLICY` maps kind -> field.
    "order_status_agg_ttl_seconds": 20,
    "funnel_agg_ttl_seconds": 300,
    # Cost gating
    "max_query_depth": 10,
    #: Calibrated so ONE level of `relatedLogs` at DEFAULT_QUERY_LIMIT is admitted (11,110) and
    #: two levels are not (1,101,010). tests/unit/test_cost.py pins both sides of that boundary
    #: against this very number; see the calibration note on the field in `src/config.py`.
    "max_query_complexity": 25000,
    "max_query_tokens": 2000,
    "max_query_aliases": 30,
    # Persisted queries
    "persisted_queries_enabled": True,
    "persisted_query_ttl_seconds": 3600,
    # DataLoader
    #: 0 = dispatch on the next event-loop tick. C5 changed this from 5: Strawberry's DataLoader
    #: has no batch-window knob (it dispatches with `loop.call_soon`), so 5 was a documented
    #: default the implementation could not honour. A positive value now opens a real window via
    #: `src.graphql.loaders.WindowedDataLoader`; 0 is the default because a selection set's fields
    #: already resolve in one tick, so a window would add latency and widen nothing.
    "dataloader_batch_window_ms": 0,
    # Subscriptions
    "subscription_queue_maxsize": 500,
    "max_subscriptions_per_connection": 10,
    "subscription_channel": "graphql-log-query:events",
    # GraphQL IDE / metrics / CORS
    "graphql_playground_enabled": True,
    "metrics_enabled": True,
    "cors_origins": "*",
}


@pytest.mark.parametrize(("field", "expected"), sorted(EXPECTED_DEFAULTS.items()))
def test_declared_default_matches_documented_table(field: str, expected: object) -> None:
    """Each field's declared default is the value the README and .env.example publish."""
    assert field in Settings.model_fields, f"{field} is documented but not declared on Settings"
    actual = Settings.model_fields[field].default
    assert actual == expected, (
        f"{field}: declared default {actual!r} disagrees with the documented default "
        f"{expected!r} — README.md, .env.example and docker-compose.yml all restate this value"
    )
    assert type(actual) is type(expected), (
        f"{field}: declared default {actual!r} has type {type(actual).__name__}, documented as "
        f"{type(expected).__name__}"
    )


def test_every_settings_field_is_documented() -> None:
    """No undocumented settings. A new field must arrive with a row in the table."""
    declared = set(Settings.model_fields)
    documented = set(EXPECTED_DEFAULTS)
    assert declared == documented, (
        f"undocumented fields: {sorted(declared - documented)}; "
        f"documented but missing from Settings: {sorted(documented - declared)}"
    )


def test_settings_construct_with_no_arguments() -> None:
    """The whole surface has a usable default — no key is required to start the process.

    Deliberately the opposite of the sibling REST project, which refuses to start without a
    ``JWT_SECRET``. There is no secret in this configuration, so a clean clone must be runnable
    with no ``.env`` at all; that is what makes ``env_file: {required: false}`` in compose honest.
    """
    settings = Settings(_env_file=None)
    assert settings.default_query_limit <= settings.max_query_limit


# --- Validators -------------------------------------------------------------------------------


def test_subscription_queue_maxsize_zero_is_rejected() -> None:
    """0 is refused, and the message says why: asyncio.Queue reads it as UNBOUNDED."""
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None, subscription_queue_maxsize=0)

    message = str(excinfo.value)
    assert "SUBSCRIPTION_QUEUE_MAXSIZE" in message
    assert "UNBOUNDED" in message, (
        "the message must explain that 0 removes the bound rather than tightening it — that "
        "confusion is the entire reason this validator exists"
    )
    assert ("subscription_queue_maxsize",) in [
        error["loc"] for error in excinfo.value.errors()
    ]


@pytest.mark.parametrize("value", [1, 500, 10_000])
def test_subscription_queue_maxsize_accepts_positive_values(value: int) -> None:
    """1 is the smallest genuinely-bounded queue and must be allowed."""
    assert Settings(_env_file=None, subscription_queue_maxsize=value).subscription_queue_maxsize == value


@pytest.mark.parametrize("value", [0, -1])
def test_max_query_depth_below_one_is_rejected(value: int) -> None:
    """A depth budget under 1 would reject every operation, introspection included."""
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None, max_query_depth=value)

    message = str(excinfo.value)
    assert "MAX_QUERY_DEPTH" in message
    assert "introspection" in message


@pytest.mark.parametrize("field", ["cache_ttl_seconds", "agg_cache_ttl_seconds"])
def test_negative_cache_ttl_is_rejected(field: str) -> None:
    """A negative TTL is not a valid Redis expiry — refused for both cache TTLs."""
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None, **{field: -1})

    assert "must be >= 0" in str(excinfo.value)
    assert (field,) in [error["loc"] for error in excinfo.value.errors()]


def test_zero_cache_ttl_is_accepted() -> None:
    """0 is a legitimate value (no expiry set) and must not be swept up by the >= 0 check."""
    assert Settings(_env_file=None, cache_ttl_seconds=0).cache_ttl_seconds == 0


def test_default_limit_above_max_limit_is_rejected() -> None:
    """The cross-field check fires, and the message reports both numbers."""
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None, default_query_limit=200, max_query_limit=100)

    message = str(excinfo.value)
    assert "DEFAULT_QUERY_LIMIT (200)" in message
    assert "MAX_QUERY_LIMIT (100)" in message
    assert "clamped" in message


def test_default_limit_equal_to_max_limit_is_accepted() -> None:
    """Equality is the boundary and must be allowed — only *exceeding* the ceiling is wrong."""
    settings = Settings(_env_file=None, default_query_limit=250, max_query_limit=250)
    assert settings.default_query_limit == settings.max_query_limit == 250


# --- Sources, overrides, caching ---------------------------------------------------------------


def test_environment_variable_overrides_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """An env var named for the upper-cased field wins over the declared default."""
    monkeypatch.setenv("MAX_QUERY_COMPLEXITY", "77")
    monkeypatch.setenv("SUBSCRIPTION_CHANNEL", "custom:channel")

    settings = Settings(_env_file=None)

    assert settings.max_query_complexity == 77
    assert settings.subscription_channel == "custom:channel"
    assert Settings.model_fields["max_query_complexity"].default == 25000, (
        "overriding through the environment must not mutate the declared default"
    )


def test_boolean_environment_values_are_coerced(monkeypatch: pytest.MonkeyPatch) -> None:
    """``CACHE_ENABLED=false`` disables the cache — not the truthiness of the string "false"."""
    monkeypatch.setenv("CACHE_ENABLED", "false")
    monkeypatch.setenv("GRAPHQL_PLAYGROUND_ENABLED", "0")

    settings = Settings(_env_file=None)

    assert settings.cache_enabled is False
    assert settings.graphql_playground_enabled is False


def test_explicit_argument_beats_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """A value passed to the constructor outranks the environment — the test fixtures rely on it."""
    monkeypatch.setenv("SEED_ENTRIES", "5000")
    assert Settings(_env_file=None, seed_entries=0).seed_entries == 0


def test_get_settings_is_cached() -> None:
    """One parse per process: repeated calls return the same object until the cache is cleared."""
    first = get_settings()
    assert get_settings() is first, "get_settings must be memoised, not re-parsed per call"

    get_settings.cache_clear()
    assert get_settings() is not first, "cache_clear must force a fresh parse"


def test_get_settings_sees_environment_after_cache_clear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The documented test escape hatch works: clear the cache, then the new env is visible."""
    get_settings()  # populate the cache with the ambient environment
    monkeypatch.setenv("MAX_QUERY_DEPTH", "3")

    get_settings.cache_clear()

    assert get_settings().max_query_depth == 3


# --- Derived views -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("*", ("*",)),
        ("http://localhost:5173", ("http://localhost:5173",)),
        ("http://a, http://b", ("http://a", "http://b")),
        ("http://a,http://b,", ("http://a", "http://b")),  # trailing comma dropped
        ("   ", ("*",)),  # empty degrades to wildcard, never to an empty allowlist
    ],
)
def test_cors_origin_list_splits_the_raw_value(raw: str, expected: tuple[str, ...]) -> None:
    """The derived view splits and trims; an empty value degrades to ``*`` rather than to nothing.

    An empty allowlist would block every browser client while looking, from the browser's side,
    exactly like a CORS misconfiguration — the failure mode is silent and expensive to diagnose.
    """
    assert Settings(_env_file=None, cors_origins=raw).cors_origin_list == expected


def test_cors_origins_round_trips_unparsed() -> None:
    """The raw setting keeps the operator's spelling; only the derived view is normalised."""
    settings = Settings(_env_file=None, cors_origins="http://a, http://b")
    assert settings.cors_origins == "http://a, http://b"
