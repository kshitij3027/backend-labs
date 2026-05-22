"""Unit tests for ``src.settings``.

These tests intentionally avoid leaking the project's real ``.env``
file into the construction of :class:`Settings`. Two defences are
applied at every relevant test boundary:

* ``monkeypatch.chdir(tmp_path)`` — pydantic-settings resolves a
  relative ``env_file=".env"`` against the current working directory,
  so chdir-ing to an empty temp directory ensures no stray ``.env`` is
  ever read.
* Per-instance ``Settings(_env_file=None, ...)`` — explicitly disables
  ``.env`` loading for that one construction (pydantic-settings 2.x
  supports overriding ``env_file`` via the leading-underscore kwarg).

Either guard would be sufficient on its own; we use both belt-and-
suspenders because the cost is zero and the failure mode of "the real
.env bled into a unit test" is silent and confusing.
"""

import base64
import os

import pytest
from pydantic import ValidationError

from src.settings import Settings, get_settings


def _valid_key_b64() -> str:
    """Return a fresh base64-encoded 32-byte Ed25519 seed."""
    return base64.b64encode(os.urandom(32)).decode()


def _clear_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip every ``Settings``-bound env var from the process env.

    Pytest does not isolate ``os.environ`` between tests by default;
    this helper makes each test start from a clean slate so an earlier
    test (or the shell that launched pytest) cannot leak values into
    the field defaults under test.
    """
    for var in (
        "SIGNING_KEY_B64",
        "PORT",
        "LOG_LEVEL",
        "DATABASE_URL",
        "CHAIN_GENESIS_NOTE",
        "REPORT_DEFAULT_RANGE_DAYS",
        "DASHBOARD_REFRESH_MS",
        "ANONYMOUS_USER_ID",
        "USER_HEADER_NAME",
        "SESSION_HEADER_NAME",
    ):
        monkeypatch.delenv(var, raising=False)


def test_settings_loads_from_explicit_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Env vars set via monkeypatch flow through into ``Settings`` fields."""
    monkeypatch.chdir(tmp_path)
    _clear_settings_env(monkeypatch)

    key = _valid_key_b64()
    monkeypatch.setenv("SIGNING_KEY_B64", key)
    monkeypatch.setenv("PORT", "9090")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    s = Settings(_env_file=None)  # type: ignore[call-arg]
    assert s.signing_key_b64 == key
    assert s.port == 9090
    assert s.log_level == "DEBUG"


def test_missing_signing_key_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """``SIGNING_KEY_B64`` is required — absence is a ``ValidationError``."""
    monkeypatch.chdir(tmp_path)
    _clear_settings_env(monkeypatch)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_signing_key_validates_bad_base64(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Non-base64 input is rejected (validator's ``ValueError`` is wrapped)."""
    monkeypatch.chdir(tmp_path)
    _clear_settings_env(monkeypatch)

    with pytest.raises(ValidationError):
        Settings(signing_key_b64="not!base64!!", _env_file=None)  # type: ignore[call-arg]


def test_signing_key_validates_wrong_length(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """A correctly-encoded but wrong-length key is rejected."""
    monkeypatch.chdir(tmp_path)
    _clear_settings_env(monkeypatch)

    sixteen_byte_key = base64.b64encode(os.urandom(16)).decode()
    with pytest.raises(ValidationError):
        Settings(signing_key_b64=sixteen_byte_key, _env_file=None)  # type: ignore[call-arg]


def test_signing_key_accepts_valid_32_bytes(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """A base64-encoded 32-byte key passes validation."""
    monkeypatch.chdir(tmp_path)
    _clear_settings_env(monkeypatch)

    key = _valid_key_b64()
    s = Settings(signing_key_b64=key, _env_file=None)  # type: ignore[call-arg]
    assert s.signing_key_b64 == key


def test_get_settings_is_cached(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """``get_settings()`` returns the memoised instance until cleared."""
    monkeypatch.chdir(tmp_path)
    _clear_settings_env(monkeypatch)
    monkeypatch.setenv("SIGNING_KEY_B64", _valid_key_b64())

    # Cache may be warm from prior tests / imports — clear before the
    # first call so the identity assertion is meaningful.
    get_settings.cache_clear()

    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2  # lru_cache memoisation

    get_settings.cache_clear()
    s3 = get_settings()
    assert s3 is not s1  # cache cleared → fresh instance

    # Leave a clean cache behind so we don't pin a Settings built
    # against a per-test temp env into module state.
    get_settings.cache_clear()


def test_default_values(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """All non-required fields fall back to the documented defaults."""
    monkeypatch.chdir(tmp_path)
    _clear_settings_env(monkeypatch)

    key = _valid_key_b64()
    s = Settings(signing_key_b64=key, _env_file=None)  # type: ignore[call-arg]

    assert s.port == 8000
    assert s.log_level == "INFO"
    assert s.database_url == "sqlite+aiosqlite:////app/data/audit.db"
    assert s.chain_genesis_note == "immutable-audit-trail"
    assert s.anonymous_user_id == "anonymous"
    assert s.user_header_name == "X-User-ID"
    assert s.session_header_name == "X-Session-ID"
    assert s.dashboard_refresh_ms == 10000
    assert s.report_default_range_days == 30
