"""Unit tests for src.config — the settings surface, its parsers and its validators.

``test_declared_defaults`` asserts against the **declared** field defaults
(``Settings.model_fields``) rather than instantiating ``Settings()``. That is deliberate: the
compose ``test`` service sets ``REDIS_URL`` and ``tests/conftest.py`` sets the three secrets, so
``Settings()`` inside the tester container would report the container's environment, not the
code's defaults — the test would pass or fail for entirely the wrong reason. Reading the model
fields tests exactly what an operator gets when they set nothing.

Every other test constructs ``Settings(...)`` through :func:`build`, which supplies explicit
kwargs (highest priority in pydantic-settings' source order) and ``_env_file=None``, so no ambient
state can influence the result.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from src.config import (
    DEFAULT_COST_CATEGORY,
    DEFAULT_ENDPOINT_COSTS_SPEC,
    DEFAULT_TIER_LIMITS_SPEC,
    MIN_SECRET_LEN,
    PLACEHOLDER_SECRETS,
    POSITIVE_DURATION_FIELDS,
    SECRET_FIELDS,
    Settings,
    TierConfig,
    get_settings,
    parse_endpoint_costs,
    parse_tier_limits,
    secret_error,
)

#: A realistic secret: `openssl rand -hex 32` output shape, comfortably over MIN_SECRET_LEN.
VALID_SECRET = "9f2c1a7b4e6d8f0a3c5e7b9d1f3a5c7e9b1d3f5a7c9e1b3d5f7a9c1e3b5d7f9a"


def declared_default(name: str) -> Any:
    """Return a field's declared default, resolving ``default_factory`` when one is used."""
    model_field = Settings.model_fields[name]
    if model_field.default_factory is not None:
        return model_field.default_factory()  # type: ignore[call-arg]
    return model_field.default


def build(**kwargs: Any) -> Settings:
    """Construct Settings hermetically: explicit kwargs, no .env file, all secrets valid."""
    for secret_field in SECRET_FIELDS:
        kwargs.setdefault(secret_field, VALID_SECRET)
    return Settings(_env_file=None, **kwargs)


# --------------------------------------------------------------------------------------------
# Declared defaults
# --------------------------------------------------------------------------------------------


def test_declared_defaults():
    """Every one of the 29 fields, as an operator gets them with nothing set."""
    # --- server / logging ---
    assert declared_default("log_level") == "INFO"

    # --- shared state ---
    assert declared_default("redis_url") == "redis://redis:6379/0"
    assert declared_default("redis_max_connections") == 32
    assert declared_default("redis_timeout_ms") == 250

    # --- secrets: required, so the default is deliberately unusable ---
    assert declared_default("jwt_secret") == ""
    assert declared_default("api_key_pepper") == ""
    assert declared_default("admin_token") == ""
    assert declared_default("jwt_algorithm") == "HS256"
    assert declared_default("access_token_ttl_min") == 30

    # --- limits and quotas ---
    assert declared_default("rate_limit_enabled") is True
    assert declared_default("default_tier") == "free"
    assert declared_default("bucket_ttl_sec") == 3600
    assert declared_default("sliding_window_sec") == 60
    assert declared_default("sliding_window_enabled") is True
    assert declared_default("quota_daily_enabled") is True
    assert declared_default("quota_monthly_enabled") is True
    assert declared_default("tier_cache_ttl_sec") == 5

    # --- analytics ---
    assert declared_default("analytics_minute_ttl_sec") == 3600
    assert declared_default("analytics_hour_ttl_sec") == 604800
    assert declared_default("analytics_max_buckets") == 120

    # --- degradation ---
    assert declared_default("fail_mode") == "open"
    assert declared_default("api_replicas") == 2
    assert declared_default("breaker_failures") == 5
    assert declared_default("breaker_cooldown_sec") == 5

    # --- seams / UI / CORS ---
    assert declared_default("allow_clock_override") is False
    assert declared_default("dashboard_poll_ms") == 5000
    assert declared_default("cors_origins") == ["*"]

    # --- the two parsed maps, which are the compact strings the compose file passes ---
    assert declared_default("tier_limits") == parse_tier_limits(DEFAULT_TIER_LIMITS_SPEC)
    assert declared_default("endpoint_costs") == parse_endpoint_costs(
        DEFAULT_ENDPOINT_COSTS_SPEC
    )

    # No field was added without a test noticing.
    assert len(Settings.model_fields) == 29


# --------------------------------------------------------------------------------------------
# TIER_LIMITS
# --------------------------------------------------------------------------------------------


def test_tier_limits_parses_the_spec_numbers():
    """The three tiers, with the numbers the spec names, from the compact operator string."""
    settings = build(tier_limits=DEFAULT_TIER_LIMITS_SPEC)

    assert set(settings.tier_limits) == {"free", "premium", "enterprise"}

    free = settings.tier_limits["free"]
    assert (free.name, free.rate_limit_per_min, free.burst) == ("free", 60, 60)
    assert (free.daily_quota, free.monthly_quota) == (1000, 25000)

    premium = settings.tier_limits["premium"]
    assert (premium.rate_limit_per_min, premium.burst) == (300, 300)
    assert (premium.daily_quota, premium.monthly_quota) == (50000, 1250000)

    enterprise = settings.tier_limits["enterprise"]
    assert (enterprise.rate_limit_per_min, enterprise.burst) == (1000, 1000)
    assert (enterprise.daily_quota, enterprise.monthly_quota) == (500000, 12500000)

    # Monthly is daily x 25 for every tier — the documented deviation from an unspecified spec
    # value, chosen because x30 never binds and is therefore untestable.
    for tier in settings.tier_limits.values():
        assert tier.monthly_quota == tier.daily_quota * 25


def test_tier_limits_tolerates_blank_fragments():
    """A trailing comma or stray whitespace from hand-edited .env is not a configuration error."""
    parsed = parse_tier_limits(" free:60:60:1000:25000 , , premium:300:300:50000:1250000 ,")

    assert set(parsed) == {"free", "premium"}


def test_tier_limits_accepts_a_mapping_and_the_result_is_frozen():
    """A mapping supplied from code skips the string parser; a parsed limit is immutable config."""
    settings = build(
        tier_limits={
            "solo": {
                "name": "solo",
                "rate_limit_per_min": 5,
                "burst": 7,
                "daily_quota": 11,
                "monthly_quota": 13,
            }
        },
        default_tier="solo",
    )

    assert settings.tier_limits["solo"] == TierConfig(
        name="solo", rate_limit_per_min=5, burst=7, daily_quota=11, monthly_quota=13
    )
    with pytest.raises(ValidationError):
        settings.tier_limits["solo"].burst = 99


@pytest.mark.parametrize(
    "spec",
    [
        "free:60:60:1000",  # too few fields
        "free:60:60:1000:25000:1",  # too many fields
        ":60:60:1000:25000",  # empty tier name
        "free:abc:60:1000:25000",  # non-integer rpm
        "free:60:60:1000:xyz",  # non-integer monthly
        "free:60.5:60:1000:25000",  # a float is not an integer; Lua would truncate it silently
        "free:0:60:1000:25000",  # a zero rate is an unusable limit
        "free:60:60:-1:25000",  # negative daily quota
        "",  # no tiers at all
        "   ,  ",  # only blanks
    ],
)
def test_tier_limits_rejects_malformed_spec(spec):
    """A typo must be a loud startup failure — a missing tier reads as an unlimited principal."""
    with pytest.raises(ValueError):
        parse_tier_limits(spec)

    with pytest.raises(ValidationError):
        build(tier_limits=spec)


def test_tier_limits_rejects_a_duplicate_tier():
    """Last-wins would silently apply an edit meant to ADD a tier as an edit to an existing one.

    Dict assignment makes ``free:...,free:...`` quietly resolve to whichever entry came last, so
    the service would start enforcing limits the operator never wrote and cannot see anywhere.
    Every other failure in this parser is loud; a repeated name is no different.
    """
    duplicated = "free:60:60:1000:25000,free:1000:1000:500000:12500000"

    with pytest.raises(ValueError) as excinfo:
        parse_tier_limits(duplicated)

    # The message has to name WHICH tier was repeated — that is the whole fix an operator makes.
    assert "duplicate" in str(excinfo.value)
    assert "'free'" in str(excinfo.value)

    with pytest.raises(ValidationError):
        build(tier_limits=duplicated)


def test_tier_limits_error_names_the_offending_fragment():
    """The message has to be actionable: which entry, and what shape was expected."""
    with pytest.raises(ValueError) as excinfo:
        parse_tier_limits("free:60:60:1000:25000,premium:oops")

    assert "'premium:oops'" in str(excinfo.value)
    assert "tier:rpm:burst:daily:monthly" in str(excinfo.value)


# --------------------------------------------------------------------------------------------
# ENDPOINT_COSTS
# --------------------------------------------------------------------------------------------


def test_endpoint_costs_parses_compact_spec():
    """The weighted-cost table: a fan-out query is not the same unit of work as a whoami."""
    settings = build(endpoint_costs=DEFAULT_ENDPOINT_COSTS_SPEC)

    assert settings.endpoint_costs == {"logs_query": 5, "logs_ingest": 2, "default": 1}


def test_endpoint_costs_accepts_a_mapping_and_blank_fragments():
    assert build(endpoint_costs={"default": 3}).endpoint_costs == {"default": 3}
    assert parse_endpoint_costs("default:1, logs_query:5 ,") == {
        "default": 1,
        "logs_query": 5,
    }


@pytest.mark.parametrize(
    "spec",
    [
        "default",  # no cost at all
        "default:1:2",  # too many fields
        ":1",  # empty category name
        "default:abc",  # non-integer cost
        "default:0",  # a zero-weight metered endpoint is an unlimited endpoint
        "default:-1",  # negative cost would refund tokens
        "logs_query:5",  # no 'default' category
        "",  # nothing at all, hence no 'default' either
    ],
)
def test_endpoint_costs_rejects_malformed_spec(spec):
    with pytest.raises(ValueError):
        parse_endpoint_costs(spec)

    with pytest.raises(ValidationError):
        build(endpoint_costs=spec)


def test_endpoint_costs_requires_a_default_category():
    """Without it the failure would be a KeyError in the middleware, at request time."""
    with pytest.raises(ValueError) as excinfo:
        parse_endpoint_costs("logs_query:5")

    assert DEFAULT_COST_CATEGORY in str(excinfo.value)
    assert "classifier does not recognise" in str(excinfo.value)


# --------------------------------------------------------------------------------------------
# CORS_ORIGINS
# --------------------------------------------------------------------------------------------


def test_cors_origins_parses_comma_separated():
    """CORS_ORIGINS arrives from the environment as a raw string, not JSON (NoDecode)."""
    settings = build(cors_origins="http://localhost:5173, https://app.example ,")

    assert settings.cors_origins == ["http://localhost:5173", "https://app.example"]


def test_cors_origins_accepts_a_list():
    """A list supplied from code passes through the before-validator untouched."""
    assert build(cors_origins=["http://a", "http://b"]).cors_origins == [
        "http://a",
        "http://b",
    ]
    assert build(cors_origins="*").cors_origins == ["*"]


# --------------------------------------------------------------------------------------------
# Positive durations: SLIDING_WINDOW_SEC, BUCKET_TTL_SEC
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("field_name", sorted(POSITIVE_DURATION_FIELDS))
@pytest.mark.parametrize("value", [0, -1, -30])
def test_a_non_positive_duration_is_a_startup_failure(field_name, value):
    """**A config typo must not be able to switch an enforcement mechanism off silently.**

    Both of these are passed straight through to the decision script as ARGV, where a non-positive
    value already means "this thing is not in force". Measured before this validator existed:
    ``SLIDING_WINDOW_SEC=0`` with ``SLIDING_WINDOW_ENABLED=true`` wrote **zero** ``sw:*`` keys, the
    account-wide gate never fired, and there was no error and no log line — one character removing
    one of the two mechanisms this project is built out of.

    The house rule in ``src.config`` is that a bad value is a startup message, not a runtime
    surprise, and a runtime surprise you cannot even observe is the worst version of it.
    """
    with pytest.raises(ValidationError) as excinfo:
        build(**{field_name: value})

    message = str(excinfo.value)
    assert field_name.upper() in message
    # The message has to say what the value would have DONE, not merely that it was out of range —
    # otherwise the obvious fix is "put any number there" rather than "use the on/off switch".
    assert str(value) in message


@pytest.mark.parametrize("field_name", sorted(POSITIVE_DURATION_FIELDS))
def test_one_second_is_the_boundary_and_is_accepted(field_name):
    """The bound is ``>= 1``, not ``> 1``: a one-second window is legal, if aggressive."""
    assert getattr(build(**{field_name: 1}), field_name) == 1


def test_the_shipped_defaults_pass_their_own_validator():
    """``validate_default=True`` means the declared defaults go through the same code path.

    A bound that the shipped configuration itself would fail is a bound that gets deleted rather
    than fixed, so it is worth asserting that the default and the rule agree.
    """
    settings = build()

    assert settings.sliding_window_sec == 60
    assert settings.bucket_ttl_sec == 3600


def test_the_tier_cache_ttl_is_deliberately_allowed_to_be_zero():
    """Not every duration is bounded, and the exception is a decision rather than an oversight.

    ``TIER_CACHE_TTL_SEC=0`` means "never cache the tier table" — expensive, but a legitimate
    operational choice — and ``src.tiers`` already floors its post-failure retry backoff so that
    choice cannot turn a Redis outage into a hot loop.
    """
    assert "tier_cache_ttl_sec" not in POSITIVE_DURATION_FIELDS
    assert build(tier_cache_ttl_sec=0).tier_cache_ttl_sec == 0


# --------------------------------------------------------------------------------------------
# Secrets: JWT_SECRET, API_KEY_PEPPER, ADMIN_TOKEN
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("field_name", sorted(SECRET_FIELDS))
@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_secret_rejected_when_empty(field_name, value):
    """`validate_default=True` is what makes a *missing* env var fail here, not just a blank one."""
    with pytest.raises(ValidationError) as excinfo:
        build(**{field_name: value})

    assert field_name.upper() in str(excinfo.value)


@pytest.mark.parametrize("field_name", sorted(SECRET_FIELDS))
@pytest.mark.parametrize("value", ["abc123", "x" * (MIN_SECRET_LEN - 1)])
def test_secret_rejected_when_too_short(field_name, value):
    with pytest.raises(ValidationError) as excinfo:
        build(**{field_name: value})

    assert str(MIN_SECRET_LEN) in str(excinfo.value)


@pytest.mark.parametrize("field_name", sorted(SECRET_FIELDS))
@pytest.mark.parametrize(
    "value", ["change-me", "CHANGE-ME", "changeme", "PLACEHOLDER", "secret", "TODO", "  todo  "]
)
def test_secret_rejected_when_placeholder(field_name, value):
    """Copying a template verbatim must fail fast, not quietly ship a demo secret.

    Note that most of these are also under MIN_SECRET_LEN — the point of the case is that a
    *long* placeholder would be caught too, which the next test pins down directly.
    """
    with pytest.raises(ValidationError):
        build(**{field_name: value})


def test_placeholder_matching_is_exact_not_substring():
    """The compose dev values contain the word 'secret' and must still be accepted.

    Substring matching would reject `dev-only-insecure-signing-key-...`, which is what
    `docker-compose.yml` defaults to — the whole stack would refuse to start on a clean clone,
    and the fix people would reach for is loosening the check rather than tightening the config.
    """
    assert "secret" in PLACEHOLDER_SECRETS

    dev_value = "dev-only-insecure-signing-key-0123456789abcdef"
    assert build(jwt_secret=dev_value).jwt_secret == dev_value
    assert build(api_key_pepper=dev_value).api_key_pepper == dev_value
    assert build(admin_token=dev_value).admin_token == dev_value


@pytest.mark.parametrize("field_name", sorted(SECRET_FIELDS))
def test_valid_secret_accepted(field_name):
    assert getattr(build(**{field_name: VALID_SECRET}), field_name) == VALID_SECRET

    # The floor is inclusive: a secret exactly MIN_SECRET_LEN long is fine.
    at_floor = "y" * MIN_SECRET_LEN
    assert getattr(build(**{field_name: at_floor}), field_name) == at_floor


@pytest.mark.parametrize("field_name", sorted(SECRET_FIELDS))
def test_secret_is_stored_stripped(field_name):
    """The string that passes validation must be the string that gets used.

    A `.env` line written as ``JWT_SECRET= my-signing-key-abc `` clears the length and placeholder
    checks on its trimmed form; returning the untrimmed original would then hand the surrounding
    whitespace to `hmac` as part of the key. Nothing raises — the tokens simply fail verification
    against any client that used the obvious value, and the pepper produces API-key digests that
    cannot be reproduced after the space is noticed and removed.
    """
    assert getattr(build(**{field_name: f"  {VALID_SECRET}\t\n"}), field_name) == VALID_SECRET

    # Including the case where the trimmed form is what cleared the floor in the first place.
    at_floor = "y" * MIN_SECRET_LEN
    assert getattr(build(**{field_name: f" {at_floor} "}), field_name) == at_floor


@pytest.mark.parametrize("field_name", sorted(SECRET_FIELDS))
def test_secret_error_message_is_actionable(field_name):
    """It has to name the env var and how to produce a good value, or it gets worked around."""
    message = secret_error(field_name)

    assert field_name.upper() in message
    assert "openssl rand -hex 32" in message
    assert str(MIN_SECRET_LEN) in message

    with pytest.raises(ValidationError) as excinfo:
        build(**{field_name: "change-me"})
    assert "openssl rand -hex 32" in str(excinfo.value)


def test_secret_error_falls_back_for_an_unknown_field():
    """Defensive: a fourth secret added to the validator but not to SECRET_FIELDS still reports."""
    assert "MYSTERY_KEY is required" in secret_error("mystery_key")


# --------------------------------------------------------------------------------------------
# FAIL_MODE, DEFAULT_TIER, and the settings cache
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["open", "closed"])
def test_fail_mode_accepts_the_two_documented_values(mode):
    assert build(fail_mode=mode).fail_mode == mode


@pytest.mark.parametrize("mode", ["OPEN", "half-open", "fail-open", "", "true"])
def test_fail_mode_rejects_anything_else(mode):
    """Degradation behaviour is a decision, not a free-text field — an unknown value is a typo."""
    with pytest.raises(ValidationError):
        build(fail_mode=mode)


def test_default_tier_must_exist_in_tier_limits():
    """A DEFAULT_TIER naming no tier means 'no limits found', which reads as 'unlimited'."""
    with pytest.raises(ValidationError) as excinfo:
        build(default_tier="platinum")

    assert "DEFAULT_TIER" in str(excinfo.value)
    assert "platinum" in str(excinfo.value)
    # The message lists what IS defined, so the fix does not require reading the source.
    assert "enterprise" in str(excinfo.value)


def test_default_tier_default_is_the_most_restrictive_tier():
    """An unknown caller must inherit the smallest plan, never the best one."""
    settings = build()
    tiers = settings.tier_limits

    assert settings.default_tier == "free"
    assert tiers[settings.default_tier].daily_quota == min(
        tier.daily_quota for tier in tiers.values()
    )


def test_get_settings_is_cached():
    """One parse per process: every collaborator shares a single Settings instance.

    Two independently parsed Settings would be two answers to 'what is the free tier's burst?',
    which is precisely the sort of divergence an enforcement layer cannot afford.
    """
    get_settings.cache_clear()

    first = get_settings()
    second = get_settings()

    assert first is second

    get_settings.cache_clear()
    assert get_settings() is not first
