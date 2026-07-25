"""Application configuration for the Log Query API (REST).

Configuration precedence (lowest to highest)::

    field defaults  ->  .env file (optional)  ->  environment variables

Defaults live on the :class:`Settings` model (pydantic-settings v2 ``BaseSettings``). This is
the standard pydantic-settings source order, so no source customization is needed. Environment
variable names are the upper-cased field names (``case_sensitive=False``), e.g. ``store_capacity``
<- ``STORE_CAPACITY``.

C1 carries the **entire** settings surface up front — all sixteen rows of the README's config
table, plus ``bcrypt_rounds`` (seventeen fields; see below) — even though most keys are not read
until a later commit. That is deliberate: ``docker-compose.yml`` passes every key through as ``${VAR:-default}``,
and declaring them all now means the compose file never has to change again as features land.

Two fields deserve a note:

* ``api_port`` — host-side only and purely informational. The container CMD hard-codes
  ``--port 8000``; compose maps ``${API_PORT:-8010}:8000``. Default is **8010** because sibling
  projects in this repo routinely hold ``:8000``. The README's config table documents 8010 to
  match.
* ``bcrypt_rounds`` — the one setting with no README table row, and the only field here that is
  a test-speed knob rather than an operational one. The bcrypt work factor for the demo user
  hashes (C6). Production cost 12
  is ~250 ms per hash, which would add minutes to the suite, so tests construct Settings with
  ``bcrypt_rounds=4`` (~2 ms). It is a knob purely so the tests can be fast without the
  production default being weak.

Use :func:`get_settings` (LRU-cached) at call sites so the config is parsed once per process;
tests that need a fresh global clear the cache via ``get_settings.cache_clear()``.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

#: Signing keys that are obviously demo values. Rejected by :meth:`Settings._check_jwt_secret`
#: so a placeholder can never quietly become a production secret — which is exactly what
#: ``.env.example`` ships (``JWT_SECRET=change-me``), so copying that file verbatim fails fast.
#: Matching is EXACT (stripped + lower-cased), never substring: the compose dev key
#: ``dev-only-insecure-key-not-a-secret-0123456789abcdef`` contains the word "secret" and must
#: still be accepted.
PLACEHOLDER_SECRETS: frozenset[str] = frozenset(
    {
        "change-me",
        "changeme",
        "change_me",
        "please-change-me",
        "your-secret-here",
        "placeholder",
        "secret",
        "changeit",
        "todo",
    }
)

#: Minimum accepted ``JWT_SECRET`` length. 16 characters is well below the 32 bytes
#: ``openssl rand -hex 32`` produces; it is a floor against typos and toy keys, not a
#: substitute for real entropy.
MIN_SECRET_LEN = 16

#: The single message every ``JWT_SECRET`` rejection carries. One message for all three failure
#: modes (empty / too short / placeholder) on purpose: telling an attacker *which* rule a key
#: tripped is not useful, and telling an operator how to fix it is.
JWT_SECRET_ERROR = (
    "JWT_SECRET is required and must not be a placeholder "
    "— generate one with 'openssl rand -hex 32'"
)

#: The README's tier table in the compact wire form operators actually type. Parsed by
#: :func:`parse_tier_limits`, which is also what backs the field default — so the default and
#: the documented string are the same thing rather than two values that can drift apart.
DEFAULT_TIER_LIMITS_SPEC = "free:10:20,pro:100:200,enterprise:1000:2000"


class TierLimit(BaseModel):
    """One tier's token-bucket sizing: sustained refill rate and burst capacity.

    Frozen because a limit is configuration, not state — the C8 :class:`RateLimiter` reads it
    on every request and must never be able to mutate the config it was sized from.
    """

    model_config = ConfigDict(frozen=True)

    rate: float = Field(description="Sustained refill rate, requests per second.")
    burst: float = Field(description="Bucket capacity — the maximum instantaneous burst.")


def parse_tier_limits(spec: str) -> dict[str, TierLimit]:
    """Parse the compact ``tier:rate:burst,...`` form into a name -> :class:`TierLimit` map.

    ``free:10:20,pro:100:200`` -> ``{"free": TierLimit(rate=10, burst=20), "pro": ...}``.
    Raises :class:`ValueError` with the offending fragment on anything malformed, so an
    operator typo surfaces as a loud startup failure rather than a silently missing tier
    (a missing tier would mean an unlimited principal, which is the worst possible default).
    """
    tiers: dict[str, TierLimit] = {}
    for fragment in (part.strip() for part in spec.split(",")):
        if not fragment:
            continue
        pieces = fragment.split(":")
        if len(pieces) != 3:
            raise ValueError(
                f"malformed TIER_LIMITS entry {fragment!r}: expected 'tier:rate:burst'"
            )
        name, raw_rate, raw_burst = (piece.strip() for piece in pieces)
        if not name:
            raise ValueError(f"malformed TIER_LIMITS entry {fragment!r}: empty tier name")
        try:
            rate, burst = float(raw_rate), float(raw_burst)
        except ValueError as exc:
            raise ValueError(
                f"malformed TIER_LIMITS entry {fragment!r}: rate and burst must be numeric"
            ) from exc
        if rate <= 0 or burst <= 0:
            raise ValueError(
                f"malformed TIER_LIMITS entry {fragment!r}: rate and burst must be positive"
            )
        tiers[name] = TierLimit(rate=rate, burst=burst)
    if not tiers:
        raise ValueError("TIER_LIMITS must define at least one 'tier:rate:burst' entry")
    return tiers


class Settings(BaseSettings):
    """Flat application settings sourced from defaults, optional .env, then environment."""

    # ``validate_default=True`` is load-bearing, not boilerplate: pydantic does NOT validate a
    # field that fell back to its default, so without it the empty ``jwt_secret`` default would
    # sail straight past its validator and the process would happily start with no signing key.
    # With it, constructing Settings without a JWT_SECRET raises — which is precisely the
    # README's "no usable default; the service refuses to start" contract.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        validate_default=True,
    )

    # --- Server / logging ---
    log_level: str = Field(
        default="INFO",
        description="Root log level for the process (DEBUG | INFO | WARNING | ERROR).",
    )
    api_port: int = Field(
        default=8010,
        description=(
            "HOST-side port only, and purely informational — the container CMD hard-codes "
            "--port 8000 and compose maps ${API_PORT:-8010}:8000. Defaults to 8010 because "
            "sibling projects in this repo hold :8000."
        ),
    )

    # --- Auth (C6) ---
    jwt_secret: str = Field(
        default="",
        description=(
            "HS256 signing key. REQUIRED — the empty default is rejected by the validator "
            "below, so the service fails loudly at startup rather than signing with a demo key."
        ),
    )
    jwt_algorithm: str = Field(default="HS256", description="Token signing algorithm.")
    access_token_ttl_min: int = Field(
        default=30, description="Access-token lifetime in minutes."
    )

    # --- Log store (C4) ---
    store_capacity: int = Field(
        default=100000,
        description="Max entries retained in the in-memory ring; oldest are evicted past this.",
    )
    seed_entries: int = Field(
        default=10000,
        description="Entries generated into the store at startup (0 leaves it empty).",
    )

    # --- Pagination (C5) ---
    default_page_size: int = Field(
        default=50, description="`limit` applied when the client omits it."
    )
    max_page_size: int = Field(
        default=500,
        description="Ceiling that `limit` is CLAMPED to — an over-large limit is never a 422.",
    )

    # --- SSE streaming (C10) ---
    sse_heartbeat_sec: int = Field(
        default=15, description="Comment-frame keepalive interval, seconds."
    )
    sse_queue_size: int = Field(
        default=1000,
        description=(
            "Per-subscriber buffer. Overflow DROPS the slow consumer rather than growing "
            "server memory — a stalled reader must never be able to OOM the process."
        ),
    )
    max_streams_per_principal: int = Field(
        default=3, description="Concurrent SSE connections allowed per principal."
    )

    # --- Rate limiting (C8) ---
    rate_limit_enabled: bool = Field(
        default=True, description="Operability switch for the per-principal token buckets."
    )
    #: ``NoDecode`` + the ``mode="before"`` validator let operators set TIER_LIMITS as the
    #: compact ``free:10:20,pro:100:200`` string rather than JSON, matching the README.
    tier_limits: Annotated[dict[str, TierLimit], NoDecode] = Field(
        default_factory=lambda: parse_tier_limits(DEFAULT_TIER_LIMITS_SPEC),
        description="Per-tier sustained rate + burst, as 'tier:rate:burst,...' or a mapping.",
    )

    # --- Stats (C11) ---
    stats_bucket_sec: int = Field(
        default=60, description="Time-bucket width for the /stats histogram, seconds."
    )

    # --- CORS ---
    #: A tuple (not a list) so the value is immutable and hashable — configuration that the
    #: middleware reads should not be editable through the settings object.
    cors_origins: Annotated[tuple[str, ...], NoDecode] = Field(
        default=("*",),
        description=(
            "Comma-separated allowed origins, or '*' for any. With '*' anywhere in the list, "
            "credentials are disabled (the CORS spec forbids pairing wildcard + credentials)."
        ),
    )

    # --- Password hashing (C6; NOT a README row — see the module docstring) ---
    bcrypt_rounds: int = Field(
        default=12,
        description=(
            "bcrypt work factor for the demo user hashes. Production cost 12 is ~250 ms per "
            "hash; tests construct Settings with 4 (~2 ms) purely so the suite stays fast."
        ),
    )

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split_cors_origins(cls, value: object) -> object:
        """Accept ``CORS_ORIGINS`` as a comma-separated string, not only a JSON / Python list.

        With ``NoDecode`` the environment value reaches this validator as a raw string
        (pydantic-settings skips its usual JSON decode for the field), so we split on commas
        and trim each origin — ``CORS_ORIGINS=http://a, http://b`` and ``CORS_ORIGINS=*`` both
        just work, and blank fragments from a trailing comma are dropped. A real list/tuple
        (the Python default, or one supplied from code) passes straight through untouched.
        """
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @field_validator("tier_limits", mode="before")
    @classmethod
    def _parse_tier_limits(cls, value: object) -> object:
        """Accept ``TIER_LIMITS`` as the compact ``tier:rate:burst,...`` string or a mapping.

        Same ``NoDecode`` rationale as the CORS validator: the raw environment string reaches
        us undecoded, and :func:`parse_tier_limits` turns it into the mapping the field
        declares. A mapping (the default, or one supplied from code) is passed through for
        pydantic to validate item-by-item. Anything else raises a clear :class:`ValueError`.
        """
        if isinstance(value, str):
            return parse_tier_limits(value)
        return value

    @field_validator("jwt_secret")
    @classmethod
    def _check_jwt_secret(cls, value: str) -> str:
        """Reject an empty, too-short or placeholder signing key.

        The README's contract is that ``JWT_SECRET`` has *no usable default* and the service
        refuses to start on a placeholder. Because ``validate_default=True`` is set on the
        model, this fires even when the field fell back to its empty default — so a process
        started with no JWT_SECRET at all dies at import/startup with this message rather than
        serving forgeable tokens.

        Placeholder matching is EXACT on the stripped, lower-cased value. Substring matching
        would reject the perfectly good compose dev key, which contains the word "secret".
        """
        normalised = value.strip()
        if not normalised:
            raise ValueError(JWT_SECRET_ERROR)
        if normalised.lower() in PLACEHOLDER_SECRETS:
            raise ValueError(JWT_SECRET_ERROR)
        if len(normalised) < MIN_SECRET_LEN:
            raise ValueError(JWT_SECRET_ERROR)
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance.

    Cached so the ``.env`` file and environment are parsed exactly once per process. Tests that
    need to observe a changed environment call ``get_settings.cache_clear()`` first.
    """
    return Settings()
