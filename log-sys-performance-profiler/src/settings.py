from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
    )

    profiler_port: int = 8000
    api_host: str = "0.0.0.0"
    dashboard_refresh_sec: int = 2
    detection_window_sec: int = 10
    overhead_target_pct: float = 2.0
    metrics_buffer_size: int = 10_000
    metrics_batch_size: int = 100
    load_test_log_count: int = 1000
    load_test_concurrency: int = 4
    bottleneck_z_threshold: float = 2.0
    instrumented_stages: str = "parse,validate,transform,write"
    log_level: str = "INFO"

    queue_maxsize: int = 256
    resource_sampler_interval_sec: float = 0.5
    detector_eval_interval_sec: float = 2.0
    tracemalloc_enabled: bool = True
    theoretical_max_lps: int = 50_000

    @property
    def instrumented_stages_list(self) -> list[str]:
        return [s.strip() for s in self.instrumented_stages.split(",") if s.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
