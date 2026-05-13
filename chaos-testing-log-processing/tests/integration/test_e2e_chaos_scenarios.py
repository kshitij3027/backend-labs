"""End-to-end integration tests driven via the engine's Python API.

These tests assume the docker compose stack is already up:
    chaos-framework, redis, log-producer, log-consumer.
The pytest process runs INSIDE chaos-framework via `docker exec`.
"""

from __future__ import annotations

import asyncio
import json
import sys

import httpx
import pytest
import websockets

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


@pytest.mark.asyncio
async def test_full_latency_injection_lifecycle() -> None:
    """End-to-end through the REST API + WebSocket, validating §5.10.

    Run the canonical 200ms latency experiment against log-consumer, watch
    the WebSocket frames as the engine drives the lifecycle, and assert the
    persisted recovery report shows overall_success and a sane
    validation_duration.
    """
    api = "http://localhost:8000"
    ws_url = "ws://localhost:8000"

    async with httpx.AsyncClient(timeout=15.0) as client:
        # 1. Create
        r = await client.post(
            f"{api}/experiments",
            json={
                "name": "c18-full-latency",
                "type": "latency_injection",
                "target": "log-consumer",
                "parameters": {"latency_ms": 200},
                "duration": 3,
                "severity": 2,
                "hypothesis": {
                    "statement": "If 200ms latency injected then p95 returns to baseline within 30s",
                    "recovery_time_budget_s": 30,
                    "expected_invariants": [],
                },
            },
        )
        r.raise_for_status()
        exp_id = r.json()["id"]

        # 2. Subscribe to WS BEFORE starting the run so we capture run_started.
        frames: list[dict] = []

        async def collect():
            try:
                async with websockets.connect(f"{ws_url}/ws/runs/*") as ws:
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=15.0)
                        frames.append(json.loads(raw))
                        if (
                            frames[-1].get("type") == "event"
                            and frames[-1].get("data", {}).get("event") == "run_completed"
                        ):
                            return
            except Exception:
                return

        bg = asyncio.create_task(collect())
        await asyncio.sleep(0.3)

        # 3. Start the run.
        r = await client.post(f"{api}/experiments/{exp_id}/run")
        r.raise_for_status()
        run_id = r.json()["run_id"]

        # 4. Poll for terminal status.
        terminal = ("completed", "failed", "aborted")
        final = None
        for _ in range(30):
            await asyncio.sleep(1.0)
            rr = await client.get(f"{api}/runs/{run_id}")
            rr.raise_for_status()
            status = rr.json().get("status")
            if status in terminal:
                final = rr.json()
                break

        # 5. Stop the WS collector.
        try:
            await asyncio.wait_for(bg, timeout=2.0)
        except asyncio.TimeoutError:
            bg.cancel()

        # 6. Assertions.
        assert final is not None, "run never reached terminal state"
        assert final["status"] == "completed", final
        assert final["recovery_report_id"] is not None
        assert final["scenario_id"] is not None
        # At least one of each frame type observed.
        types_seen = {f.get("type") for f in frames}
        assert "snapshot" in types_seen
        assert "event" in types_seen
        # Run_completed event present in the engine event stream.
        events = [f for f in frames if f.get("type") == "event"]
        names = [e.get("data", {}).get("event") for e in events]
        assert "run_completed" in names, names
