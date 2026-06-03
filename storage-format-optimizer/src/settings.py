from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the adaptive storage-format optimizer.

    Every field is overridable via an environment variable of the same name
    (case-insensitive) or a ``.env`` file. Defaults track the design in
    ``plan.md`` (§"Config defaults") and ``project_requirements.md`` §7.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
    )

    # --- storage paths ---
    data_dir: str = "./data"  # root dir for per-tenant partition data
    log_dir: str = "./logs"  # root dir for application logs

    # --- partitioning ---
    partition_bucket_seconds: int = 3600  # time-bucket width per partition
    hybrid_seal_age_seconds: int = 1800  # age after which HYBRID recent rows seal

    # --- query classification thresholds ---
    analytical_max_columns: int = 3  # <= this many columns -> analytical
    full_record_min_columns: int = 10  # >= this many columns -> full_record

    # --- format selector ---
    select_write_ratio_row: float = 0.3  # write fraction above which ROW wins
    select_point_lookup_row: float = 0.5  # point-lookup fraction favouring ROW
    select_scan_ratio_columnar: float = 0.6  # scan fraction favouring COLUMNAR
    select_few_columns_fraction: float = 0.4  # column-touch fraction for COLUMNAR
    select_min_confidence: float = 0.6  # below this confidence -> keep current
    select_min_rows: int = 256  # below this row count -> keep current

    # --- tiers ---
    tier_hot_max_age_seconds: int = 3600  # max age to still qualify as hot
    tier_cold_min_age_seconds: int = 86400  # min age to qualify as cold
    tier_hot_min_reads_per_min: float = 1.0  # read rate to stay hot

    # --- migration engine ---
    migration_interval_seconds: float = 5.0  # background loop tick interval
    migration_max_per_tick: int = 4  # max partitions migrated per tick
    migration_cooldown_seconds: float = 60.0  # per-partition re-migration cooldown

    # --- compression (Feature B) ---
    row_codec: str = "lz4"  # codec for ROW JSONL frames
    columnar_default_codec: str = "SNAPPY"  # default Parquet codec
    columnar_cold_codec: str = "ZSTD"  # Parquet codec for cold partitions
    compression_learn_enabled: bool = True  # enable learned-codec selection
    compression_learn_sample_rows: int = 2000  # rows sampled when learning
    compression_learn_size_weight: float = 1.0  # weight on compressed size
    compression_learn_latency_weight: float = 0.2  # weight on (de)compress latency

    # --- indexing (Feature C) ---
    index_min_filter_hits: int = 5  # filter hits before building an index
    index_min_selectivity: float = 0.2  # min selectivity to justify an index
    index_drop_benefit_window: int = 200  # window of queries for benefit calc
    index_drop_min_benefit: float = 0.01  # min benefit before an index is dropped

    # --- metrics / dashboard ---
    metrics_history_points: int = 60  # retained time-series points
    ws_push_interval_seconds: float = 2.0  # WebSocket broadcast interval

    # --- API / server ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Return a process-wide cached Settings instance."""
    return Settings()
