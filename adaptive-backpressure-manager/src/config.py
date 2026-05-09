from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    port: int = Field(default=8000, alias="PORT")
    max_queue_size: int = Field(default=10000, alias="MAX_QUEUE_SIZE")
    critical_queue_max: int = Field(default=1000, alias="CRITICAL_QUEUE_MAX")
    high_queue_max: int = Field(default=2000, alias="HIGH_QUEUE_MAX")
    normal_queue_max: int = Field(default=5000, alias="NORMAL_QUEUE_MAX")
    low_queue_max: int = Field(default=2000, alias="LOW_QUEUE_MAX")
    worker_count: int = Field(default=4, alias="WORKER_COUNT")
    sampling_interval: float = Field(default=1.0, alias="SAMPLING_INTERVAL")
    pressure_history_size: int = Field(default=100, alias="PRESSURE_HISTORY_SIZE")
    ewma_alpha: float = Field(default=0.3, alias="EWMA_ALPHA")
    aimd_beta: float = Field(default=0.7, alias="AIMD_BETA")
    ai_period_ticks: int = Field(default=3, alias="AI_PERIOD_TICKS")
    min_dwell_seconds: float = Field(default=3.0, alias="MIN_DWELL_SECONDS")
    up_normal_to_pressure: float = Field(default=0.7, alias="UP_NORMAL_TO_PRESSURE")
    up_pressure_to_overload: float = Field(default=0.85, alias="UP_PRESSURE_TO_OVERLOAD")
    up_overload_to_emergency: float = Field(default=0.95, alias="UP_OVERLOAD_TO_EMERGENCY")
    down_overload_to_pressure: float = Field(default=0.75, alias="DOWN_OVERLOAD_TO_PRESSURE")
    down_pressure_to_normal: float = Field(default=0.55, alias="DOWN_PRESSURE_TO_NORMAL")
    down_recovery_to_normal: float = Field(default=0.45, alias="DOWN_RECOVERY_TO_NORMAL")
    anti_starvation_age_seconds: float = Field(default=30.0, alias="ANTI_STARVATION_AGE_SECONDS")
    retry_after_jitter: float = Field(default=0.3, alias="RETRY_AFTER_JITTER")
    lag_norm_divisor: float = Field(default=10.0, alias="LAG_NORM_DIVISOR")
    recovery_slowdown_factor: float = Field(default=3.0, alias="RECOVERY_SLOWDOWN_FACTOR")
    processing_latency_seconds: float = Field(default=0.05, alias="PROCESSING_LATENCY_SECONDS")
    loadtest_default_rps: int = Field(default=200, alias="LOADTEST_DEFAULT_RPS")
    loadtest_default_duration: int = Field(default=60, alias="LOADTEST_DEFAULT_DURATION")
    loadtest_default_spike_multiplier: float = Field(
        default=10.0, alias="LOADTEST_DEFAULT_SPIKE_MULTIPLIER"
    )


def get_settings() -> Settings:
    return Settings()
