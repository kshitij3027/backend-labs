"""End-to-end integration tests driven via the engine's Python API.

These tests assume the docker compose stack is already up:
    chaos-framework, redis, log-producer, log-consumer.
The pytest process runs INSIDE chaos-framework via `docker exec`.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

# Allow running with PYTHONPATH=/app inside the framework container.
sys.path.insert(0, "/app")

from src.config.settings import get_settings  # noqa: E402
from src.docker_client.client import DockerClient  # noqa: E402
from src.engine.experiment_engine import (  # noqa: E402
    ExperimentEngine,
    default_probes_for_latency,
)
from src.injection.injector import FailureInjector  # noqa: E402
from src.models.experiments import ExperimentDefinition, RunStatus  # noqa: E402
from src.models.scenarios import FailureType  # noqa: E402


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_latency_lifecycle(latency_target: str) -> None:
    settings = get_settings()
    dc = DockerClient(
        allowlist=settings.target_allowlist,
        socket_path=settings.docker_socket_path,
    )
    injector = FailureInjector(
        docker_client=dc,
        allowlist=settings.target_allowlist,
        max_concurrent=settings.max_concurrent_scenarios,
        cpu_emergency_threshold_pct=settings.cpu_emergency_threshold_pct,
        mem_emergency_threshold_pct=settings.mem_emergency_threshold_pct,
    )
    event_queue: asyncio.Queue = asyncio.Queue()

    engine = ExperimentEngine(
        injector=injector,
        monitor=None,  # not required for this test
        probes_factory=default_probes_for_latency,
        event_queue=event_queue,
        observe_grace_s=3.0,
    )

    definition = ExperimentDefinition(
        name="latency-200ms-3s",
        type=FailureType.LATENCY_INJECTION,
        target=latency_target,
        parameters={"latency_ms": 200, "jitter_ms": 0},
        duration=3,
        severity=2,
    )

    outcome = await engine.run(definition)

    # Drain events for visibility on failure.
    events = []
    while not event_queue.empty():
        events.append(event_queue.get_nowait())

    assert outcome.error is None, (
        f"unexpected error={outcome.error!r}; events={[e['event'] for e in events]}"
    )
    assert outcome.run.status in (RunStatus.COMPLETED, RunStatus.FAILED), outcome.run.status
    assert outcome.report is not None
    assert outcome.report.scenario_id == outcome.run.scenario_id
    assert outcome.report.summary.total_tests == 3  # health + latency-baseline + data-loss

    # Confirm phase events fired in order:
    event_names = [e["event"] for e in events]
    expected_order = [
        "run_started",
        "baseline_captured",
        "injecting",
        "observing",
        "rolling_back",
        "validating",
        "run_completed",
    ]
    assert event_names == expected_order, event_names

    # The run took at least the observe duration.
    assert (outcome.run.ended_at - outcome.run.started_at).total_seconds() >= 3.0
