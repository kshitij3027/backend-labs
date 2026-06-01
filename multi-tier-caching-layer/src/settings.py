from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the multi-tier caching layer.

    Every field is overridable via an environment variable of the same name
    (case-insensitive) or a `.env` file. Defaults track the spec's
    "Configurable Parameters" table (project_requirements.md §7) plus the
    control knobs for the L2 codec, pattern engine, warmer, and dashboard.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
    )

    # --- L1 in-process cache (spec §7) ---
    l1_max_size: int = 1000  # max entries before LRU eviction
    l1_memory_mb: int = 100  # soft memory allocation for L1
    l1_ttl: int = 300  # default L1 TTL in seconds

    # --- L2 distributed cache / Redis (spec §7) ---
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_url: str = ""  # if empty, derived from host/port via effective_redis_url
    l2_max_mb: int = 2048  # L2 cache size budget
    l2_ttl_seconds: int = 600  # default L2 TTL in seconds
    l2_timeout: float = 2.0  # per-call Redis timeout in seconds
    l2_compress: bool = True  # compress time-series blobs with zstd

    # --- Overall cache budget ---
    cache_mem_cap_mb: int = 200  # total cache memory cap across tiers

    # --- L3 / Postgres backend ---
    database_url: str = "postgresql://cache:cache@postgres:5432/cache"
    backend_delay_ms: int = 150  # artificial slow-backend latency for the demo
    time_bucket_seconds: int = 300  # timestamp bucketing for cache keys

    # --- Proactive warmer ---
    warmer_interval_seconds: float = 5.0
    warmer_top_n: int = 20

    # --- Heuristic pattern engine ---
    pattern_history_size: int = 5000
    pattern_freq_weight: float = 1.0
    pattern_recency_weight: float = 1.0
    pattern_cost_weight: float = 1.0
    pattern_recency_half_life_seconds: float = 3600.0

    # --- Degradation alerting ---
    degradation_hit_rate_threshold: float = 0.5

    # --- Synthetic data seeding ---
    seed_rows: int = 200000  # synthetic raw_logs rows
    seed_random_seed: int = 1337

    # --- Dashboard / WebSocket ---
    ws_push_interval_seconds: float = 2.0
    dashboard_points: int = 60

    # --- API / server ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    dash_port: int = 3000
    log_level: str = "INFO"

    @property
    def effective_redis_url(self) -> str:
        """Return the explicit REDIS_URL if set, else derive from host/port."""
        if self.redis_url:
            return self.redis_url
        return f"redis://{self.redis_host}:{self.redis_port}/0"


@lru_cache
def get_settings() -> Settings:
    """Return a process-wide cached Settings instance."""
    return Settings()
