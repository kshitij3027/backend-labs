"""Application configuration for the GraphQL Log Query Platform.

Configuration precedence (lowest to highest)::

    field defaults  ->  .env file (optional)  ->  environment variables

Defaults live on the :class:`Settings` model (pydantic-settings v2 ``BaseSettings``). That is the
standard pydantic-settings source order, so no source customization is needed. Environment
variable names are the **upper-cased field names** (``case_sensitive=False``), e.g.
``max_query_depth`` <- ``MAX_QUERY_DEPTH``.

C1 carries the **entire** settings surface up front — every parameter in the spec's §7 table plus
the keys later commits need — even though most are not read until a later commit. That is
deliberate: ``docker-compose.yml`` passes every key through as ``${VAR:-default}``, and declaring
them all now means the compose file never has to change again as features land. A field with no
reader yet is annotated with the commit that starts reading it.

.. rubric:: Two names that are NOT settings

``API_PORT`` is a **compose-only** variable: it is the host side of ``${API_PORT:-8020}:8000`` and
the container CMD hard-codes ``--port 8000``. It is not declared here, and ``extra="ignore"`` means
an operator who puts it in ``.env`` gets no error — the value is simply irrelevant to this process.
``HOST``/``PORT`` *are* declared, because the spec's §7 table names them, but they are equally
informational inside the container for exactly the same reason.

The E2E and load gates (``MAX_P95_MS``, ``LOAD_MIN_RPS``, …) are likewise not settings: they are
read by ``scripts/verify_e2e.py`` and ``scripts/load_test.py``, which are separate processes in a
separate container. They live in ``.env.example`` for discoverability, not here.

Use :func:`get_settings` (LRU-cached) at call sites so the config is parsed once per process; tests
that need a fresh global clear the cache via ``get_settings.cache_clear()``.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Flat application settings sourced from defaults, an optional ``.env``, then environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Server / logging -----------------------------------------------------------------
    #: Informational inside the container: the image CMD hard-codes `--host 0.0.0.0 --port 8000`
    #: and compose maps `${API_PORT:-8020}:8000`. Declared because the spec's §7 table names both.
    host: str = "0.0.0.0"
    port: int = 8000
    #: Root log level for the process (DEBUG | INFO | WARNING | ERROR). An unrecognised value
    #: degrades to INFO at configuration time rather than killing the process over a typo.
    log_level: str = "INFO"

    # --- Database (C2) --------------------------------------------------------------------
    #: SQLAlchemy 2.x async URL. `postgresql+asyncpg` (not `postgresql+psycopg2`) is load-bearing:
    #: every resolver in this project is a coroutine, and a blocking DBAPI driver would park the
    #: event loop for the duration of each query — turning the concurrency gate in the load
    #: harness into a measurement of the driver rather than of the service.
    database_url: str = "postgresql+asyncpg://gqllogs:gqllogs@postgres:5432/gqllogs"
    db_pool_size: int = 10
    db_max_overflow: int = 5
    #: Boot-time schema creation retries. Compose already gates `api` on `pg_isready`, but "the
    #: server accepts connections" and "the database is ready for DDL" are not the same instant,
    #: so `init_db` retries rather than trusting the healthcheck alone.
    db_init_retries: int = 10
    db_init_retry_delay_seconds: float = 2.0

    # --- Redis (C6 pub/sub, C7 cache, C9 persisted queries) --------------------------------
    redis_url: str = "redis://redis:6379/0"

    # --- Seed corpus (C2, C10) ------------------------------------------------------------
    #: Rows generated at startup when the store is empty. The compose `test` service pins both to
    #: 0 so the suite starts from an empty database and builds its own corpus.
    seed_entries: int = 2000
    seed_orders: int = 200
    #: Fixed RNG seed. The corpus is deterministic so the E2E verifier can grade responses against
    #: a locally regenerated oracle instead of asserting "some rows came back".
    random_seed: int = 20260725

    # --- Query limits (C3) ----------------------------------------------------------------
    #: `limit` applied when the client omits it. Also the multiplier the C8 cost gate assumes for
    #: an unbounded list field, so an omitted limit cannot score zero complexity.
    default_query_limit: int = 100
    #: Hard ceiling `limit` is clamped to on every query path, including nested list fields.
    max_query_limit: int = 500

    # --- Caching (C7 results, C11 aggregates) ---------------------------------------------
    cache_enabled: bool = True
    #: Result-cache TTL in seconds (the spec's §7 value). Short on purpose: a log query is a
    #: point-in-time answer, and a long TTL would make "live" data visibly stale in the dashboard.
    cache_ttl_seconds: int = 30
    #: Aggregations get their own, longer TTL: they are far more expensive to compute (SQL GROUP
    #: BY over the whole window) and far less sensitive to a single new row. Shared by
    #: `Query.logStats` and `Query.paymentOutcomeBreakdown` — see `src.cache.TTL_POLICY`.
    agg_cache_ttl_seconds: int = 60
    #: `Query.orderStatusDistribution` — the SHORTEST TTL in the system, and deliberately shorter
    #: than the generic aggregate one. It answers "where does every order stand right now", so a
    #: single new order event MOVES an order from one bucket to another: the number is not merely
    #: incremented by a write, it is redistributed by one. That is the dashboard panel an operator
    #: watches during an incident, so it gets the tightest staleness bound any cached read has.
    order_status_agg_ttl_seconds: int = 20
    #: `Query.orderFunnel` — the LONGEST, for the opposite reason. The funnel counts orders that
    #: have EVER reached each status, so it is monotonic: a status once reached is never
    #: un-reached, and a stale answer can only ever undercount by the orders that moved during the
    #: window. It is also the most expensive of the three (a COUNT DISTINCT per group over the
    #: whole window), so it is the one where a long TTL buys the most.
    funnel_agg_ttl_seconds: int = 300

    # --- Cost gating (C8) -----------------------------------------------------------------
    #: Maximum operation nesting depth. GraphQL's cyclic type graph means a client can otherwise
    #: write `logs { relatedLogs { relatedLogs { … } } }` forever; depth is the cheapest bound.
    max_query_depth: int = 10
    #: Complexity budget, computed from the AST BEFORE execution (field weights x list multipliers).
    #:
    #: 25,000 is calibrated on ONE requirement: the flagship correlated query — `logs` at
    #: DEFAULT_QUERY_LIMIT with ONE level of `relatedLogs` — must be ADMITTED. That is spec §2
    #: item 17 at item 29's default page size, the whole reason the C5 DataLoader exists, and the
    #: query C13's dashboard and C11's traversals send. `{ logs { id relatedLogs { id } } }` prices
    #: at 11,110 and `{ logs { id message relatedLogs { id message } } }` at 21,210, so the earlier
    #: default of 1000 rejected this API's own headline capability — a broken default, not a strict
    #: one.
    #:
    #: What it still refuses is what NESTING MULTIPLIES out of reach: a second level of correlation
    #: (`logs { relatedLogs { relatedLogs { id } } }` = 1,101,010, forty-four times the budget) and
    #: a wide page with correlation attached (`logs(limit: 500) { relatedLogs { id } }` = 55,010).
    #: The boundary a reader should expect: at the default page size every extra field selected
    #: under `relatedLogs` costs 100 x 100 = 10,000, so the budget affords two of them; the first
    #: realistic shape it trips is the full seven-field projection on BOTH levels past 34 parents
    #: (10 + N x 717 > 25,000 at N = 35). The calibration table lives in `.env.example` and the
    #: weights it is computed from in `src/graphql/cost.py`.
    max_query_complexity: int = 25_000
    #: Token and alias ceilings, enforced by Strawberry's own MaxTokensLimiter / MaxAliasesLimiter.
    #: They close the two amplification attacks depth and complexity do not: a document that is
    #: enormous but shallow, and one field requested 10,000 times under 10,000 aliases.
    max_query_tokens: int = 2000
    max_query_aliases: int = 30

    # --- Persisted queries (C9) -----------------------------------------------------------
    persisted_queries_enabled: bool = True
    #: How long a registered query document is retained in Redis.
    persisted_query_ttl_seconds: int = 3600

    # --- DataLoader (C5, C11) -------------------------------------------------------------
    #: How long a DataLoader holds a batch open before dispatching it, in milliseconds.
    #:
    #: **0 (the default) means "dispatch on the next event-loop tick"** — every key loaded in the
    #: current tick joins one batch. That is Strawberry's native behaviour and the only point on
    #: this scale its `DataLoader` implements (it dispatches with `loop.call_soon`; there is no
    #: window knob in its constructor). It is also the right default for this schema: Strawberry
    #: resolves a selection set's fields concurrently, so all N `relatedLogs` calls are issued in
    #: one tick and are already maximally batched — a window would add latency and widen nothing.
    #:
    #: A positive value opens a real window, implemented in `src.graphql.loaders`
    #: (`WindowedDataLoader`) because Strawberry has none. It buys wider batches only when loads
    #: straddle awaits, and costs up to its own length in latency on the fields that use it. C5
    #: changed the default from 5 to 0 rather than leave a documented knob whose value the pinned
    #: Strawberry could not honour; see that module's docstring for the whole argument.
    dataloader_batch_window_ms: int = 0

    # --- Subscriptions (C6, C12) ----------------------------------------------------------
    #: Per-subscriber bounded queue. Overflow DROPS the slow consumer rather than growing server
    #: memory or blocking the publisher — see the validator below for why 0 is refused.
    subscription_queue_maxsize: int = 500
    #: Cap on concurrent subscription operations multiplexed over ONE WebSocket connection. The
    #: graphql-transport-ws protocol allows unlimited `subscribe` messages on a single socket, so
    #: without this a single connection can allocate unbounded queues.
    max_subscriptions_per_connection: int = 10
    #: Redis pub/sub channel the broker bridges events over, so subscriptions survive
    #: `uvicorn --workers N`. One channel; each worker suppresses its own echo by publisher id.
    subscription_channel: str = "graphql-log-query:events"

    # --- GraphQL IDE / metrics / CORS -----------------------------------------------------
    #: Serve GraphiQL on `GET /graphql`. POST is unaffected either way.
    graphql_playground_enabled: bool = True
    #: Expose `GET /metrics` (Prometheus text exposition) — C9.
    metrics_enabled: bool = True
    #: Comma-separated allowed origins, or `*` for any. With `*`, credentials are disabled (the
    #: CORS spec forbids pairing the wildcard origin with credentialed requests). Note the SPA is
    #: served from this same process at the same origin, so the dashboard path has no CORS in it
    #: at all — this exists for third-party clients and for a Vite dev server on another port.
    cors_origins: str = "*"

    # -- Validators ---------------------------------------------------------------------------
    #
    # Five checks, each guarding a configuration that is *accepted* by the type system and
    # *catastrophic* at run time. A `bool`/`int` annotation cannot express "0 means the opposite
    # of what you intended", which is exactly the first case below.

    @field_validator("subscription_queue_maxsize")
    @classmethod
    def _check_subscription_queue_maxsize(cls, value: int) -> int:
        """Refuse ``0``, because :class:`asyncio.Queue` reads it as *unbounded*.

        This is the one setting where the dangerous value looks like the safe one. Somebody
        tightening back-pressure reaches for the smallest number they can type; ``maxsize=0`` in
        :class:`asyncio.Queue` means **no limit at all**, so the "tightest" setting silently
        removes the only bound standing between a stalled WebSocket reader and the server's
        memory. Refusing it at startup is the difference between a config error and an OOM.
        """
        if value < 1:
            raise ValueError(
                "SUBSCRIPTION_QUEUE_MAXSIZE must be >= 1: asyncio.Queue treats maxsize=0 as "
                "UNBOUNDED, so 0 removes the back-pressure bound instead of tightening it and "
                f"lets one stalled subscriber grow server memory without limit (got {value})"
            )
        return value

    @field_validator("max_query_depth")
    @classmethod
    def _check_max_query_depth(cls, value: int) -> int:
        """Refuse a depth budget below 1, which rejects every possible operation."""
        if value < 1:
            raise ValueError(
                "MAX_QUERY_DEPTH must be >= 1: a budget below 1 rejects every operation the "
                f"server can be sent, including introspection and GraphiQL itself (got {value})"
            )
        return value

    @field_validator("dataloader_batch_window_ms")
    @classmethod
    def _check_dataloader_batch_window(cls, value: int) -> int:
        """Refuse a negative window, which would read as "dispatch before the keys arrive".

        Zero is legal and is the default — it means next-tick dispatch. A negative value has no
        meaning at all, and the loader would clamp it to zero anyway, so accepting it would leave
        an operator believing they had configured something.
        """
        if value < 0:
            raise ValueError(
                "DATALOADER_BATCH_WINDOW_MS must be >= 0: 0 means 'dispatch on the next "
                f"event-loop tick' and a positive value is a real hold-open window (got {value})"
            )
        return value

    @field_validator(
        "cache_ttl_seconds",
        "agg_cache_ttl_seconds",
        "order_status_agg_ttl_seconds",
        "funnel_agg_ttl_seconds",
    )
    @classmethod
    def _check_cache_ttl(cls, value: int) -> int:
        """Refuse a negative TTL — Redis has no expiry that means "in the past"."""
        if value < 0:
            raise ValueError(
                "cache TTL seconds must be >= 0: a negative value is not a valid Redis expiry "
                f"and would make every SET fail rather than every read miss (got {value})"
            )
        return value

    @model_validator(mode="after")
    def _check_limit_ordering(self) -> Settings:
        """Refuse a default limit above the ceiling it will immediately be clamped to.

        Cross-field, so it cannot be a ``field_validator``. The failure it prevents is quiet
        rather than loud: the server would keep serving, every request that omitted ``limit``
        would silently receive ``MAX_QUERY_LIMIT`` rows, and the operator's ``DEFAULT_QUERY_LIMIT``
        would have no observable effect at all.
        """
        if self.default_query_limit > self.max_query_limit:
            raise ValueError(
                f"DEFAULT_QUERY_LIMIT ({self.default_query_limit}) must not exceed "
                f"MAX_QUERY_LIMIT ({self.max_query_limit}): the default would be clamped on every "
                "request that omits `limit`, so the configured default would never take effect"
            )
        return self

    # -- Derived views --------------------------------------------------------------------

    @property
    def cors_origin_list(self) -> tuple[str, ...]:
        """``cors_origins`` split into the list form the CORS middleware wants.

        Kept as a derived property rather than a parsed field so the setting round-trips exactly
        as the operator wrote it (and as ``.env.example`` documents it). Blank fragments from a
        trailing comma are dropped; an entirely empty value degrades to ``("*",)`` rather than to
        an empty allowlist, because an empty allowlist silently blocks every browser client and
        looks identical to a CORS bug.
        """
        origins = tuple(part.strip() for part in self.cors_origins.split(",") if part.strip())
        return origins or ("*",)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance.

    Cached so the ``.env`` file and the environment are parsed exactly once per process. Tests
    that need to observe a changed environment call ``get_settings.cache_clear()`` first.
    """
    return Settings()
