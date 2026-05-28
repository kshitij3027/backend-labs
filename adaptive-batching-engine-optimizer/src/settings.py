from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the adaptive batching engine.

    Every field is overridable via an environment variable of the same name
    (case-insensitive) or a `.env` file. Defaults track the spec's
    "Configurable Parameters" table plus a handful of control-loop internals.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Batch size bounds & seed (spec §7) ---
    min_batch_size: int = 50
    max_batch_size: int = 5000
    initial_batch_size: int = 100

    # --- Optimizer dynamics (spec §7) ---
    smoothing_alpha: float = 0.2  # exponential smoothing factor for batch updates
    optimization_interval: float = 5.0  # seconds between control-loop ticks
    batch_increase_factor: float = 1.1  # multiplier when probing larger batches
    batch_decrease_factor: float = 0.9  # multiplier when probing smaller batches

    # --- Safety constraint thresholds (spec §7) ---
    cpu_constraint_threshold: float = 90.0  # percent
    memory_constraint_threshold: float = 90.0  # percent
    latency_constraint_threshold: float = 1000.0  # milliseconds

    # --- Load simulation defaults ---
    default_messages_per_second: float = 100.0
    default_burst_probability: float = 0.2

    # --- Multi-objective optimization weights ---
    weight_throughput: float = 0.7
    weight_latency: float = 0.3

    # --- Control-loop internals (tunable) ---
    learning_samples: int = 5  # samples collected in LEARNING before OPTIMIZING
    stable_gradient_threshold: float = 0.01  # |norm. gradient| below this => STABLE
    recovery_cpu_threshold: float = 70.0  # percent; below this counts as healthy
    recovery_memory_threshold: float = 70.0  # percent; below this counts as healthy
    recovery_latency_threshold: float = 300.0  # ms; below this counts as healthy
    recovery_cycles: int = 3  # consecutive healthy cycles before leaving EMERGENCY
    metrics_history_size: int = 200  # rolling metrics buffer length
    dashboard_points: int = 20  # data points charted in the dashboard

    # --- API / server ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Return a process-wide cached Settings instance."""
    return Settings()
