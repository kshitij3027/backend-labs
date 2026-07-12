"""Application configuration for the RCA Analysis Engine.

Configuration precedence (lowest to highest):

    field defaults  ->  .env file (optional)  ->  environment variables

Defaults live on the :class:`Settings` model (pydantic-settings v2 ``BaseSettings``).
This is the standard pydantic-settings source order (environment beats dotenv beats
defaults), so no source customization is needed. Environment variable names are the
upper-cased field names (pydantic-settings default), e.g. ``temporal_window`` <-
``TEMPORAL_WINDOW``.

Every Req §7 tunable is a field here (the scoring formulas in the analysis package
read them off :func:`get_settings`), so operators can retune causal-edge strength,
confidence weights, the temporal window and history/streaming behaviour purely via
environment / ``.env`` without touching code.

Use :func:`get_settings` (LRU-cached) at call sites so the config is parsed once per
process; tests that monkeypatch the environment clear the cache via
``get_settings.cache_clear()``.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Flat application settings sourced from defaults, optional .env, then environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Causal-edge scoring (Req §7) ---
    #: Max seconds between two events for a causal edge to be admissible at all.
    temporal_window: int = 300
    #: Base edge strength before any bonuses/penalties.
    base_causal_strength: float = 0.5
    #: Added when the service dependency map declares upstream -> downstream.
    service_dependency_bonus: float = 0.3
    #: Added for an ERROR -> ERROR edge (error propagation).
    error_propagation_bonus: float = 0.2
    #: Seconds beyond which the temporal-gap penalty applies to an edge.
    temporal_gap_threshold: int = 60
    #: Subtracted when the inter-event gap exceeds temporal_gap_threshold.
    temporal_gap_penalty: float = 0.1
    #: Edge-strength clamp range (strengths are clamped to [min, max]).
    causal_strength_min: float = 0.1
    causal_strength_max: float = 1.0

    # --- Confidence scoring (Req §7) ---
    #: Severity component of the confidence score, by level.
    score_critical: float = 0.6
    score_error: float = 0.4
    score_warning: float = 0.2
    #: Weight on temporal position (earlier events score higher).
    temporal_score_weight: float = 0.3
    #: Weight on normalized out-degree centrality in the causal graph.
    centrality_score_weight: float = 0.2

    # --- Multi-hypothesis tracking + anomaly amplification (C7) ---
    #: Max concurrent root-cause hypotheses retained (top-k by personalized PageRank).
    max_hypotheses: int = 5
    #: Confidence at/above which a hypothesis is marked CONFIRMED.
    hypothesis_confirm_threshold: float = 0.6
    #: Confidence below which a hypothesis is PRUNED (dropped from the report).
    hypothesis_prune_threshold: float = 0.1
    #: Personalized-PageRank damping factor (restart probability = 1 - alpha).
    pagerank_alpha: float = 0.85
    #: Max power-iteration steps before giving up on convergence.
    pagerank_max_iter: int = 100
    #: L1 convergence tolerance for the power iteration.
    pagerank_tol: float = 1e-6

    # --- Server / API ---
    #: uvicorn bind port (compose maps ${BACKEND_PORT} -> this).
    server_port: int = 8000
    #: CORS allowed origins (comma-separated, or "*" for any).
    cors_origins: str = "*"
    log_level: str = "INFO"

    # --- Clock-skew correction (C8, feature area E) ---
    #: Tolerance band in seconds: two events whose |Δt| is below this are treated as
    #: "concurrent" (near-simultaneous), so a sub-ε timestamp difference never forces an
    #: order — only a dependency / happens-before constraint may reorder within the band.
    clock_skew_epsilon: float = 2.0

    # --- Incident history / live stream ---
    #: Max incidents retained in the bounded in-memory history.
    max_incident_history: int = 1000
    #: Master switch for the background live-stream loop. Defaults OFF so tests/CI
    #: never spin up the loop; the real-time streaming path is turned on in C8.
    live_stream_enabled: bool = False
    #: Seconds between synthetic incidents in the background live-stream loop (C8). The
    #: loop is self-correcting: it sleeps ``max(0.1, interval - elapsed)`` each tick.
    live_stream_interval: float = 5.0
    #: Base RNG seed for the live-stream loop's generated incidents (C8); each tick uses
    #: ``live_stream_seed + counter`` so successive incidents vary yet stay reproducible.
    live_stream_seed: int = 0
    #: Max events retained in the IncrementalAnalyzer's rolling window (C8). The window
    #: is bounded by BOTH this count and ``temporal_window`` (time), whichever is tighter.
    incremental_max_events: int = 500

    # --- Service dependency map ---
    #: Path to the externalized upstream -> downstream service map (Req §7). Relative
    #: to the process working directory (/app in the container).
    service_dependency_map_path: str = "src/config/service_dependency_map.json"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance."""
    return Settings()
