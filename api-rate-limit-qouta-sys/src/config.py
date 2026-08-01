"""Application configuration for the API Rate Limiter & Quota Manager.

Configuration precedence (lowest to highest)::

    field defaults  ->  .env file (optional)  ->  environment variables

Defaults live on the :class:`Settings` model (pydantic-settings v2 ``BaseSettings``). This is the
standard pydantic-settings source order, so no source customization is needed. Environment
variable names are the upper-cased field names (``case_sensitive=False``), e.g. ``bucket_ttl_sec``
<- ``BUCKET_TTL_SEC``.

C1 carries the **entire** settings surface up front — all twenty-nine fields — even though most
are not read until C4-C15. That is deliberate: ``docker-compose.yml`` passes every key through as
``${VAR:-default}``, so declaring them all now means the compose file never has to change again
as features land, and the config table in the README is a description of one file rather than a
running total.

.. rubric:: This is the only module that reads the environment

Everything downstream takes a :class:`Settings` instance (or a value off one). That matters more
here than in a typical service: the limiter's behaviour *is* its configuration, and a second place
that read ``os.environ`` directly would be a second, invisible source of truth for how many
requests a caller is allowed. Use :func:`get_settings` (LRU-cached) at call sites so the config is
parsed once per process; tests that need a fresh global clear the cache via
``get_settings.cache_clear()``.

.. rubric:: Runtime config vs. startup config

``TIER_LIMITS`` here is the **seed**, not the live truth. C3's ``TierRegistry`` seeds Redis
``config:tiers`` from it with ``HSETNX`` (never ``HSET``) and thereafter reads the live values, so
an operator's runtime change through the admin API survives a replica restart. Read this field as
"what a brand-new deployment starts with", not "what is being enforced right now".
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

#: Secret values that are obviously demo/scaffold text. Rejected by
#: :meth:`Settings._check_secret` so a placeholder can never quietly become a production secret —
#: which is exactly what a copied ``.env.example`` would ship.
#:
#: Matching is EXACT (stripped + lower-cased), never substring: the compose dev values contain the
#: word "secret" (``dev-only-insecure-signing-key-...``) and must still be accepted, or a clean
#: clone could not `make up` at all.
#:
#: The empty string is a member for completeness, though :meth:`Settings._check_secret` catches an
#: empty value on its own line first — the two paths raise the identical message, so which one
#: fires is not observable.
PLACEHOLDER_SECRETS: frozenset[str] = frozenset(
    {
        "change-me",
        "changeme",
        "secret",
        "placeholder",
        "todo",
        "",
    }
)

#: Minimum accepted length for every secret below. 16 characters is well under the 32 bytes
#: ``openssl rand -hex 32`` produces; it is a floor against typos and toy keys, not a substitute
#: for real entropy.
MIN_SECRET_LEN = 16

#: The three fields :meth:`Settings._check_secret` guards, and what each one protects. Kept as a
#: mapping rather than three separate validators so a fourth secret cannot be added later without
#: inheriting the same floor by default.
SECRET_FIELDS: dict[str, str] = {
    "jwt_secret": "HS256 signing key for Bearer tokens",
    "api_key_pepper": "server-side pepper for the HMAC-SHA256 API-key digests",
    "admin_token": "bearer token for the runtime tier/quota admin API",
}


def secret_error(field_name: str) -> str:
    """Build the rejection message for one secret field.

    One message for all three failure modes (empty / too short / placeholder) on purpose: telling
    an attacker *which* rule a value tripped is not useful, and telling an operator how to fix it
    is. The env var name and the ``openssl`` incantation are both in the message because a config
    error a human has to go read source code to act on is a config error that gets worked around.
    """
    purpose = SECRET_FIELDS.get(field_name, "secret")
    return (
        f"{field_name.upper()} is required, must be at least {MIN_SECRET_LEN} characters and "
        f"must not be a placeholder ({purpose}) "
        "— generate one with 'openssl rand -hex 32'"
    )


#: The spec's tier table in the compact wire form operators actually type:
#: ``tier:rpm:burst:daily:monthly``. Parsed by :func:`parse_tier_limits`, which also backs the
#: field default — so the default and the documented string are the same thing rather than two
#: values that can drift apart.
#:
#: ``burst == rpm`` for every tier is not a copy-paste slip. The token bucket is per
#: ``(user, endpoint)``; the account-wide sustained rate is enforced by the separate sliding
#: window. Sizing the bucket at exactly one minute of tokens makes the two gates agree on what
#: "60 requests per minute" means rather than multiplying it by the endpoint count.
#:
#: Monthly is daily x 25, not x 30. The spec leaves monthly unspecified; x30 never binds (a caller
#: at the daily cap every single day exactly reaches it) and is therefore untestable, while x25
#: binds around day 25 of sustained maximum usage, which is a limit that can actually be observed.
DEFAULT_TIER_LIMITS_SPEC = (
    "free:60:60:1000:25000,"
    "premium:300:300:50000:1250000,"
    "enterprise:1000:1000:500000:12500000"
)

#: Weighted per-request cost by endpoint category (the in-scope bonus). A read that fans out
#: across the log store is not the same unit of work as a whoami, and charging both one token
#: prices the expensive call as though it were free.
DEFAULT_ENDPOINT_COSTS_SPEC = "logs_query:5,logs_ingest:2,default:1"

#: The category every unclassified endpoint falls back to. Required to be present in
#: ``ENDPOINT_COSTS`` — see :func:`parse_endpoint_costs`.
DEFAULT_COST_CATEGORY = "default"


class TierConfig(BaseModel):
    """One tier's complete enforcement sizing: burst, sustained rate, and both quota periods.

    Frozen because a limit is configuration, not state — the limiter reads it on every request and
    must never be able to mutate the config it was sized from. (C3 may relocate this to
    ``src/models.py`` alongside the other domain types and re-export it from here; the shape is
    the contract, not the module it lives in.)

    All four numbers are integers. That is not incidental either: they are rendered straight into
    the Lua script's ARGV, and Lua 5.1 numbers are doubles whose RESP encoding truncates decimals,
    so every quantity in the decision path is designed as an integer at the source.
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(description="Tier name, as used in `user:{id}` and `config:tiers`.")
    rate_limit_per_min: int = Field(
        description="Sustained requests per minute — the sliding window's account-wide ceiling."
    )
    burst: int = Field(
        description="Token-bucket capacity per (user, endpoint) — the instantaneous burst."
    )
    daily_quota: int = Field(description="Cumulative requests allowed per UTC day.")
    monthly_quota: int = Field(description="Cumulative requests allowed per UTC month.")


def parse_tier_limits(spec: str) -> dict[str, TierConfig]:
    """Parse the compact ``tier:rpm:burst:daily:monthly,...`` form into a name -> config map.

    ``free:60:60:1000:25000`` -> ``{"free": TierConfig(name="free", rate_limit_per_min=60, ...)}``.

    Raises :class:`ValueError` with the offending fragment on anything malformed, so an operator
    typo surfaces as a loud startup failure rather than a silently missing tier. A missing tier is
    the worst possible failure mode here: the principal on it has no ceiling to look up, and "no
    limit found" reads as "unlimited" in every implementation that does not go out of its way to
    make it read as "refuse".

    A repeated tier name is malformed too. Dict assignment would otherwise make it last-wins, so
    ``free:60:...,free:1000:...`` would start the service on limits the operator did not mean and
    cannot see — an edit intended to add a tier, applied silently as an edit to an existing one.
    Every other failure in this parser is loud; this one is no different.
    """
    tiers: dict[str, TierConfig] = {}
    for fragment in (part.strip() for part in spec.split(",")):
        if not fragment:
            continue
        pieces = fragment.split(":")
        if len(pieces) != 5:
            raise ValueError(
                f"malformed TIER_LIMITS entry {fragment!r}: "
                "expected 'tier:rpm:burst:daily:monthly'"
            )
        name, *raw_numbers = (piece.strip() for piece in pieces)
        if not name:
            raise ValueError(f"malformed TIER_LIMITS entry {fragment!r}: empty tier name")
        if name in tiers:
            raise ValueError(
                f"malformed TIER_LIMITS entry {fragment!r}: duplicate tier {name!r} — "
                "each tier may be defined only once"
            )
        try:
            numbers = [int(raw) for raw in raw_numbers]
        except ValueError as exc:
            raise ValueError(
                f"malformed TIER_LIMITS entry {fragment!r}: "
                "rpm, burst, daily and monthly must be integers"
            ) from exc
        if any(number <= 0 for number in numbers):
            raise ValueError(
                f"malformed TIER_LIMITS entry {fragment!r}: "
                "rpm, burst, daily and monthly must be positive"
            )
        rpm, burst, daily, monthly = numbers
        tiers[name] = TierConfig(
            name=name,
            rate_limit_per_min=rpm,
            burst=burst,
            daily_quota=daily,
            monthly_quota=monthly,
        )
    if not tiers:
        raise ValueError(
            "TIER_LIMITS must define at least one 'tier:rpm:burst:daily:monthly' entry"
        )
    return tiers


def parse_endpoint_costs(spec: str) -> dict[str, int]:
    """Parse the compact ``category:cost,...`` form into a category -> weight map.

    The ``default`` category is **required**, because it is what an unclassified request is priced
    at. C2's classifier deliberately collapses every unknown path to one label (otherwise
    ``/logs/1``, ``/logs/2``, ... is an unbounded Redis key generator), and the cost lookup for
    that label has to resolve to a number. Without this check the failure would be a ``KeyError``
    raised inside the middleware on the hot path — at request time, in production, for a route
    nobody thought about — rather than a startup message naming the missing key.

    A cost of zero is rejected for the same reason a missing tier is: a zero-weight metered
    endpoint is an unlimited endpoint. Exemption is expressed by the middleware's exempt-path list
    (``/health``, ``/dashboard/*``, ``/admin/*``), which is an explicit decision, not by quietly
    pricing a route at nothing.
    """
    costs: dict[str, int] = {}
    for fragment in (part.strip() for part in spec.split(",")):
        if not fragment:
            continue
        pieces = fragment.split(":")
        if len(pieces) != 2:
            raise ValueError(
                f"malformed ENDPOINT_COSTS entry {fragment!r}: expected 'category:cost'"
            )
        category, raw_cost = (piece.strip() for piece in pieces)
        if not category:
            raise ValueError(
                f"malformed ENDPOINT_COSTS entry {fragment!r}: empty category name"
            )
        try:
            cost = int(raw_cost)
        except ValueError as exc:
            raise ValueError(
                f"malformed ENDPOINT_COSTS entry {fragment!r}: cost must be an integer"
            ) from exc
        if cost < 1:
            raise ValueError(
                f"malformed ENDPOINT_COSTS entry {fragment!r}: cost must be >= 1"
            )
        costs[category] = cost
    if DEFAULT_COST_CATEGORY not in costs:
        raise ValueError(
            f"ENDPOINT_COSTS must define a {DEFAULT_COST_CATEGORY!r} category — it is the weight "
            "charged for any endpoint the classifier does not recognise"
        )
    return costs


class Settings(BaseSettings):
    """Flat application settings sourced from defaults, optional .env, then environment."""

    # ``validate_default=True`` is load-bearing, not boilerplate: pydantic does NOT validate a
    # field that fell back to its default, so without it the empty ``jwt_secret`` /
    # ``api_key_pepper`` / ``admin_token`` defaults would sail straight past their validator and
    # the process would happily start with no signing key, an unpeppered key store and an open
    # admin API. With it, constructing Settings without them raises — the "refuses to start"
    # contract, enforced by the model rather than by a startup check someone can forget to call.
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

    # --- Shared state: Redis (C2) ---
    redis_url: str = Field(
        default="redis://redis:6379/0",
        description=(
            "Connection URL for the ONE store every replica shares. The service name (not "
            "localhost) is the default because the API only ever reaches Redis over the compose "
            "network."
        ),
    )
    redis_max_connections: int = Field(
        default=32,
        description=(
            "Pool ceiling per process. Bounded rather than unlimited: Redis is single-threaded, "
            "so past a point extra connections add queueing, not throughput."
        ),
    )
    redis_timeout_ms: int = Field(
        default=250,
        description=(
            "Socket AND connect timeout. The whole rate-limit check has a 5 ms budget, so a "
            "Redis that has stopped answering must be classified as failed in milliseconds — a "
            "limiter that hangs is worse for the caller than one that fails open."
        ),
    )

    # --- Auth: JWT (C5) ---
    jwt_secret: str = Field(
        default="",
        description=(
            "HS256 signing key. REQUIRED — the empty default is rejected by the validator below, "
            "so the service fails loudly at startup rather than accepting forged tokens."
        ),
    )
    jwt_algorithm: str = Field(default="HS256", description="Token signing algorithm.")
    access_token_ttl_min: int = Field(default=30, description="Access-token lifetime, minutes.")

    # --- Auth: API keys (C5) ---
    api_key_pepper: str = Field(
        default="",
        description=(
            "Server-side pepper for the `apikey:v1:<hmac_sha256_hex>` digests. REQUIRED. Lives in "
            "the process environment and NEVER in Redis, so a stolen dump yields digests an "
            "attacker cannot invert into usable keys. Rotating it invalidates every stored key."
        ),
    )

    # --- Admin API (C10) ---
    admin_token: str = Field(
        default="",
        description=(
            "Bearer token for the runtime tier/quota admin API, compared with "
            "hmac.compare_digest. REQUIRED: the admin surface can raise any caller's limits."
        ),
    )

    # --- Limits and quotas (C3, C4) ---
    rate_limit_enabled: bool = Field(
        default=True,
        description=(
            "Operability switch for the whole enforcement path. Also the baseline the C14 "
            "overhead phase measures against, so it isolates the limiter's cost from the "
            "handler's."
        ),
    )
    #: ``NoDecode`` + the ``mode="before"`` validator let operators set TIER_LIMITS as the compact
    #: ``free:60:60:1000:25000,...`` string rather than JSON, matching the README and compose.
    tier_limits: Annotated[dict[str, TierConfig], NoDecode] = Field(
        default_factory=lambda: parse_tier_limits(DEFAULT_TIER_LIMITS_SPEC),
        description=(
            "Seed tier table, as 'tier:rpm:burst:daily:monthly,...' or a mapping. Seeded into "
            "Redis with HSETNX; the live values are read from there."
        ),
    )
    default_tier: str = Field(
        default="free",
        description=(
            "Tier assigned to a principal with no `tier` field in `user:{id}`. The most "
            "restrictive tier, deliberately — an unknown user must not inherit the best plan."
        ),
    )
    endpoint_costs: Annotated[dict[str, int], NoDecode] = Field(
        default_factory=lambda: parse_endpoint_costs(DEFAULT_ENDPOINT_COSTS_SPEC),
        description=(
            "Weighted per-request cost by endpoint category, as 'category:cost,...' or a "
            "mapping. Must include a 'default' entry."
        ),
    )
    bucket_ttl_sec: int = Field(
        default=3600,
        description=(
            "Floor for a token bucket's TTL. The script actually sets "
            "max(this, time-to-refill), because expiring a partially drained bucket early would "
            "silently gift the caller a full one."
        ),
    )
    sliding_window_sec: int = Field(
        default=60,
        description=(
            "Width of the account-wide weighted sliding window — the gate that makes 'free tier "
            "limited after ~60 req/min' true across all endpoints rather than per endpoint."
        ),
    )
    sliding_window_enabled: bool = Field(
        default=True, description="Operability switch for the account-wide sustained-rate gate."
    )
    quota_daily_enabled: bool = Field(default=True, description="Enforce the daily quota gate.")
    quota_monthly_enabled: bool = Field(
        default=True, description="Enforce the monthly quota gate."
    )
    tier_cache_ttl_sec: int = Field(
        default=5,
        description=(
            "Lifetime of a replica's in-process tier snapshot. Bounds how long a runtime tier "
            "change takes to reach every replica — 5 s, which is a deterministic assertion rather "
            "than a race against pub/sub delivery. Note this bounds what a tier MEANS, never who "
            "is on which tier: user->tier is read inside the script and takes effect instantly."
        ),
    )

    # --- Analytics (C9, C11) ---
    analytics_minute_ttl_sec: int = Field(
        default=3600,
        description="Minute-bucket retention. Set with `EXPIRE ... NX` so the TTL is anchored to "
        "bucket creation — without NX a continuously hot bucket lives an hour past its last "
        "write, and '1 h retention' quietly becomes '1 h after traffic stops'.",
    )
    analytics_hour_ttl_sec: int = Field(
        default=604800, description="Hour-bucket retention (7 days), same EXPIRE NX rule."
    )
    analytics_max_buckets: int = Field(
        default=120,
        description=(
            "Ceiling on how many time buckets one stats read will pipeline. The read side "
            "computes key names arithmetically and never SCANs, so this is a response-size bound "
            "rather than a scan bound."
        ),
    )

    # --- Degradation (C8) ---
    fail_mode: Literal["open", "closed"] = Field(
        default="open",
        description=(
            "Behaviour when Redis is unreachable. 'open' serves the request through a bounded "
            "local fallback bucket (the spec's graceful-degradation requirement); 'closed' "
            "returns 503, for deployments where the limit IS the security control."
        ),
    )
    api_replicas: int = Field(
        default=2,
        description=(
            "How many API replicas share the store. Sizes the degraded fallback bucket at "
            "ceil(tier_capacity / this): N replicas each holding a FULL local bucket is exactly "
            "the N-times overspend this project exists to prevent, so this must track compose."
        ),
    )
    breaker_failures: int = Field(
        default=5, description="Consecutive Redis failures that trip the circuit breaker open."
    )
    breaker_cooldown_sec: int = Field(
        default=5,
        description="How long the breaker stays open before it allows a single half-open probe.",
    )

    # --- Test-only seam (C4) ---
    allow_clock_override: bool = Field(
        default=False,
        description=(
            "Gates the Lua script's `now_ms_override` ARGV; the limiter refuses to send it "
            "otherwise. A client-supplied clock is the skew vulnerability the design removed (a "
            "replica with a fast clock would permanently refill its own bucket), so the seam that "
            "lets tests freeze time must be inert by construction everywhere else."
        ),
    )

    # --- Dashboard (C15) ---
    dashboard_poll_ms: int = Field(
        default=5000,
        description=(
            "Poll interval the dashboard uses, served to the page by the stats endpoint so the "
            "interval has ONE source of truth rather than one in Python and one in JavaScript."
        ),
    )

    # --- CORS ---
    cors_origins: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["*"],
        description=(
            "Comma-separated allowed origins, or '*' for any. With '*' anywhere in the list, "
            "credentials are disabled (the CORS spec forbids pairing wildcard + credentials)."
        ),
    )

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split_cors_origins(cls, value: object) -> object:
        """Accept ``CORS_ORIGINS`` as a comma-separated string, not only a JSON / Python list.

        With ``NoDecode`` the environment value reaches this validator as a raw string
        (pydantic-settings skips its usual JSON decode for the field), so we split on commas and
        trim each origin — ``CORS_ORIGINS=http://a, http://b`` and ``CORS_ORIGINS=*`` both just
        work, and blank fragments from a trailing comma are dropped. A real list (the Python
        default, or one supplied from code) passes straight through untouched.
        """
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @field_validator("tier_limits", mode="before")
    @classmethod
    def _parse_tier_limits(cls, value: object) -> object:
        """Accept ``TIER_LIMITS`` as the compact string or as a mapping.

        Same ``NoDecode`` rationale as the CORS validator: the raw environment string reaches us
        undecoded, and :func:`parse_tier_limits` turns it into the mapping the field declares. A
        mapping (the default, or one supplied from code) is passed through for pydantic to
        validate item by item.
        """
        if isinstance(value, str):
            return parse_tier_limits(value)
        return value

    @field_validator("endpoint_costs", mode="before")
    @classmethod
    def _parse_endpoint_costs(cls, value: object) -> object:
        """Accept ``ENDPOINT_COSTS`` as the compact ``category:cost,...`` string or a mapping."""
        if isinstance(value, str):
            return parse_endpoint_costs(value)
        return value

    @field_validator(*SECRET_FIELDS)
    @classmethod
    def _check_secret(cls, value: str, info: ValidationInfo) -> str:
        """Reject an empty, too-short or placeholder value for any of the three secrets.

        One validator over all three rather than three near-identical ones, so a fourth secret
        added later inherits the same floor by being listed in :data:`SECRET_FIELDS` instead of by
        someone remembering to copy this method.

        Because ``validate_default=True`` is set on the model, this fires even when the field fell
        back to its empty default — so a process started with no ``API_KEY_PEPPER`` at all dies at
        import with an actionable message rather than hashing every API key with an empty pepper
        and producing a key store that is, in effect, unpeppered.

        Placeholder matching is EXACT on the stripped, lower-cased value. Substring matching would
        reject the compose dev values, which contain the word "secret", and a clean clone could
        then not start at all.

        The stripped value is what gets STORED, not merely what gets checked. Validating one string
        and returning another is the asymmetry that turns a stray space in a ``.env`` line into a
        silent failure with no error anywhere: ``JWT_SECRET=" my-signing-key-abc "`` would pass on
        its 18-character trimmed form and then be used verbatim — whitespace included — as the
        HMAC key, so every token signed here fails verification against a client that used the
        obvious trimmed value, and the pepper hashes every API key under a digest nobody can
        reproduce. Normalise once, here, so the value checked and the value used are the same one.
        """
        field_name = info.field_name or "secret"
        normalised = value.strip()
        if not normalised:
            raise ValueError(secret_error(field_name))
        if normalised.lower() in PLACEHOLDER_SECRETS:
            raise ValueError(secret_error(field_name))
        if len(normalised) < MIN_SECRET_LEN:
            raise ValueError(secret_error(field_name))
        return normalised

    @model_validator(mode="after")
    def _default_tier_must_exist(self) -> Settings:
        """Refuse a ``DEFAULT_TIER`` that names no tier in ``TIER_LIMITS``.

        This is the one cross-field rule worth enforcing, because its failure mode is silent and
        expensive: ``default_tier`` is what a principal with no tier recorded in ``user:{id}``
        falls back to, so if it names nothing the lookup produces no limits — and "no limits
        found" is indistinguishable from "unlimited" at the point where the decision is made. A
        typo in one env var would quietly exempt every unrecognised caller from the entire
        enforcement layer.
        """
        if self.default_tier not in self.tier_limits:
            raise ValueError(
                f"DEFAULT_TIER {self.default_tier!r} is not defined in TIER_LIMITS "
                f"(known tiers: {', '.join(sorted(self.tier_limits))})"
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance.

    Cached so the ``.env`` file and environment are parsed exactly once per process — and, more
    importantly, so every collaborator that asks for configuration gets the *same* object. Two
    independently parsed Settings would be two answers to "what is the free tier's burst?", which
    is the sort of divergence a rate limiter cannot afford.

    Tests that need to observe a changed environment call ``get_settings.cache_clear()`` first.
    """
    return Settings()
