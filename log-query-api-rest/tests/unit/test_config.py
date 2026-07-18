"""Unit tests for src.config — the settings surface and its three validators.

``test_defaults_match_readme_table`` asserts against the **declared** field defaults
(``Settings.model_fields``) rather than instantiating ``Settings()``. That is deliberate: the
compose ``test`` service deliberately overrides SEED_ENTRIES=0 and STORE_CAPACITY=1000 for
determinism, so ``Settings()`` inside the tester container would report the container's
environment, not the code's defaults — the test would pass or fail for entirely the wrong
reason. Reading the model fields tests exactly what the README's table promises: what an
operator gets when they set nothing.

Every other test constructs ``Settings(...)`` with explicit kwargs (which outrank both the
environment and ``.env`` in pydantic-settings' source order) and ``_env_file=None``, so no
ambient state can influence the result.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from src.config import (
    DEFAULT_TIER_LIMITS_SPEC,
    MIN_SECRET_LEN,
    PLACEHOLDER_SECRETS,
    Settings,
    TierLimit,
    get_settings,
    parse_tier_limits,
)

#: A realistic key: `openssl rand -hex 32` output shape, comfortably over MIN_SECRET_LEN.
VALID_SECRET = "9f2c1a7b4e6d8f0a3c5e7b9d1f3a5c7e9b1d3f5a7c9e1b3d5f7a9c1e3b5d7f9a"


def declared_default(name: str) -> Any:
    """Return a field's declared default, resolving ``default_factory`` when one is used."""
    model_field = Settings.model_fields[name]
    if model_field.default_factory is not None:
        return model_field.default_factory()  # type: ignore[call-arg]
    return model_field.default


def build(**kwargs: Any) -> Settings:
    """Construct Settings hermetically: explicit kwargs, no .env file."""
    kwargs.setdefault("jwt_secret", VALID_SECRET)
    return Settings(_env_file=None, **kwargs)


def test_defaults_match_readme_table():
    """All sixteen README config rows, plus the two documented non-README knobs."""
    # --- the sixteen rows of the README's "Planned Configuration" table ---
    assert declared_default("log_level") == "INFO"
    assert declared_default("jwt_secret") == ""  # required: no usable default
    assert declared_default("jwt_algorithm") == "HS256"
    assert declared_default("access_token_ttl_min") == 30
    assert declared_default("store_capacity") == 100000
    assert declared_default("seed_entries") == 10000
    assert declared_default("default_page_size") == 50
    assert declared_default("max_page_size") == 500
    assert declared_default("sse_heartbeat_sec") == 15
    assert declared_default("sse_queue_size") == 1000
    assert declared_default("max_streams_per_principal") == 3
    assert declared_default("rate_limit_enabled") is True
    assert declared_default("stats_bucket_sec") == 60
    assert declared_default("cors_origins") == ("*",)
    assert declared_default("tier_limits") == {
        "free": TierLimit(rate=10, burst=20),
        "pro": TierLimit(rate=100, burst=200),
        "enterprise": TierLimit(rate=1000, burst=2000),
    }

    # --- API_PORT: documented deviation from the table's 8000 (siblings hold :8000) ---
    assert declared_default("api_port") == 8010

    # --- bcrypt_rounds: not a README row; production cost, lowered to 4 in tests ---
    assert declared_default("bcrypt_rounds") == 12


@pytest.mark.parametrize(
    "placeholder",
    [
        "change-me",
        "CHANGE-ME",
        "Change_Me",
        "changeme",
        "please-change-me",
        "your-secret-here",
        "PLACEHOLDER",
        "secret",
        "changeit",
        "TODO",
        "  change-me  ",  # stripped before comparison
    ],
)
def test_placeholder_jwt_secret_rejected(placeholder):
    """Copying .env.example verbatim must fail fast, not quietly ship a demo signing key."""
    with pytest.raises(ValidationError) as excinfo:
        build(jwt_secret=placeholder)

    assert "JWT_SECRET is required and must not be a placeholder" in str(excinfo.value)
    assert "openssl rand -hex 32" in str(excinfo.value)


def test_placeholder_set_is_matched_exactly_not_by_substring():
    """The compose dev key contains the word 'secret' and must still be accepted.

    Substring matching would reject `dev-only-insecure-key-not-a-secret-...`, which is the
    default `make up` runs with — the whole stack would refuse to start on a clean clone.
    """
    assert "secret" in PLACEHOLDER_SECRETS
    dev_key = "dev-only-insecure-key-not-a-secret-0123456789abcdef"

    assert build(jwt_secret=dev_key).jwt_secret == dev_key


@pytest.mark.parametrize("secret", ["abc123", "x" * (MIN_SECRET_LEN - 1)])
def test_short_jwt_secret_rejected(secret):
    with pytest.raises(ValidationError) as excinfo:
        build(jwt_secret=secret)

    assert "JWT_SECRET is required" in str(excinfo.value)


@pytest.mark.parametrize("secret", ["", "   ", "\t\n"])
def test_empty_jwt_secret_rejected(secret):
    with pytest.raises(ValidationError):
        build(jwt_secret=secret)


def test_valid_jwt_secret_accepted():
    settings = build(jwt_secret=VALID_SECRET)

    assert settings.jwt_secret == VALID_SECRET
    # A key exactly at the floor is fine; the floor is inclusive.
    assert build(jwt_secret="y" * MIN_SECRET_LEN).jwt_secret == "y" * MIN_SECRET_LEN


def test_cors_origins_parses_comma_separated():
    """CORS_ORIGINS arrives from the environment as a raw string, not JSON (NoDecode)."""
    settings = build(cors_origins="http://localhost:5173, https://app.example ,")

    assert settings.cors_origins == ("http://localhost:5173", "https://app.example")


def test_cors_origins_accepts_list():
    """A list/tuple supplied from code passes through the before-validator untouched."""
    settings = build(cors_origins=["http://a", "http://b"])

    assert settings.cors_origins == ("http://a", "http://b")
    assert build(cors_origins="*").cors_origins == ("*",)


def test_tier_limits_parses_compact_spec():
    """The README's tier table, in the compact form an operator actually types."""
    settings = build(tier_limits=DEFAULT_TIER_LIMITS_SPEC)

    assert settings.tier_limits["free"].rate == 10
    assert settings.tier_limits["free"].burst == 20
    assert settings.tier_limits["pro"].rate == 100
    assert settings.tier_limits["pro"].burst == 200
    assert settings.tier_limits["enterprise"].rate == 1000
    assert settings.tier_limits["enterprise"].burst == 2000

    # A mapping supplied from code works too, and a parsed limit is frozen config.
    from_mapping = build(tier_limits={"free": {"rate": 5, "burst": 9}})
    assert from_mapping.tier_limits["free"] == TierLimit(rate=5, burst=9)
    with pytest.raises(ValidationError):
        from_mapping.tier_limits["free"].rate = 99


@pytest.mark.parametrize(
    "spec",
    [
        "free:10",  # missing burst
        "free:10:20:30",  # too many fields
        "free:abc:20",  # non-numeric rate
        "free:10:xyz",  # non-numeric burst
        ":10:20",  # empty tier name
        "free:0:20",  # a zero rate is an unusable bucket, not a limit
        "free:10:-5",  # negative burst
        "",  # no tiers at all
        "   ,  ",  # only blanks
    ],
)
def test_tier_limits_rejects_malformed_spec(spec):
    """A typo must be a loud startup failure — a missing tier means an unlimited principal."""
    with pytest.raises(ValueError):
        parse_tier_limits(spec)

    with pytest.raises(ValidationError):
        build(tier_limits=spec)


def test_get_settings_is_cached():
    """One parse per process: call sites share a single Settings instance."""
    get_settings.cache_clear()

    first = get_settings()
    second = get_settings()

    assert first is second

    get_settings.cache_clear()
    assert get_settings() is not first
