"""System-metrics domain models.

These models are the on-the-wire shape pushed over the WebSocket channel
and stored in the rolling history maintained by ``SystemMonitor`` (1000
entries, ``project_requirements.md`` §2 / §7). They are intentionally
behavior-free at this commit.

Example payload pushed to WS clients (4 Hz throttled)::

    {
      "timestamp": "2026-05-12T18:30:05+00:00",
      "cpu_pct": 23.7,
      "mem_pct": 41.2,
      "disk_pct": 58.0,
      "network_latency_ms": 12.4,
      "service_health": [
        {"name": "log-producer", "is_healthy": true, "last_check_at": "...", "latency_ms": 4.1},
        {"name": "log-consumer", "is_healthy": true, "last_check_at": "...", "latency_ms": 5.0}
      ],
      "container_stats": {
        "log-consumer": {"cpu_pct": 12.5, "mem_pct": 4.1},
        "log-producer": {"cpu_pct": 3.2, "mem_pct": 2.8}
      }
    }
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field


class ServiceHealth(BaseModel):
    """One row of "is service X healthy right now?".

    Populated by ``SystemMonitor`` on each 5s tick by GET-ing ``/health`` on
    every allowlisted target. ``latency_ms`` is ``None`` when the probe
    failed before a TCP connect (so callers can distinguish "slow" from
    "unreachable").
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    name: str = Field(min_length=1, description="Container/service name.")
    is_healthy: bool = Field(description="True iff /health returned 2xx within timeout.")
    last_check_at: datetime = Field(description="UTC instant the probe completed.")
    latency_ms: float | None = Field(
        default=None,
        ge=0.0,
        description="Probe round-trip in ms; None if the probe never connected.",
    )


class SystemMetrics(BaseModel):
    """A single 5s snapshot of host + container + service-health state.

    This is the unit pushed onto ``SystemMonitor.history`` (deque, maxlen
    1000) and broadcast to WS subscribers.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC instant the snapshot was collected.",
    )
    cpu_pct: float = Field(
        ge=0.0,
        le=100.0,
        description="Host-level CPU utilisation (0..100).",
    )
    mem_pct: float = Field(
        ge=0.0,
        le=100.0,
        description="Host-level memory utilisation (0..100).",
    )
    disk_pct: float = Field(
        ge=0.0,
        le=100.0,
        description="Host-level disk utilisation (0..100).",
    )
    network_latency_ms: float | None = Field(
        default=None,
        ge=0.0,
        description="Mean RTT to allowlisted targets; None if no target reachable.",
    )
    service_health: list[ServiceHealth] = Field(
        default_factory=list,
        description="Per-target health probe results for this tick.",
    )
    container_stats: dict[str, dict[str, float]] = Field(
        default_factory=dict,
        description=(
            "Per-container Docker stats; outer key is container name, inner "
            "dict carries at minimum 'cpu_pct' and 'mem_pct'."
        ),
    )
