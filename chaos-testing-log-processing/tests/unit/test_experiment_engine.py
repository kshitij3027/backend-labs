"""Unit tests for C11 — :class:`ExperimentEngine` lifecycle orchestration.

Covers the eight behavioural axes called out in the C11 test brief:

1. Happy path (no monitor, no probes).
2. Event-queue phase ordering.
3. With-probes success/failure paths (validator stubbed via monkeypatch).
4. Inject-failure path (engine catches, emits ``run_failed``, no re-raise).
5. Rollback-failure path (engine catches, emits ``run_failed``, no re-raise).
6. Monitor wiring (baseline + post snapshots populated).
7. Observe phase hard-bounded by ``duration + observe_grace_s``.
8. ``default_probes_for_latency`` factory: shape + baseline propagation.

The :class:`FailureInjector` is mocked with ``AsyncMock`` so we don't drag
in docker/network state. The :class:`SystemMonitor` is mocked with
``MagicMock`` (``snapshot()`` is sync). The :class:`RecoveryValidator`
is stubbed via ``monkeypatch.setattr`` on the engine module so probe
construction is free.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.engine import experiment_engine as engine_module
from src.engine.experiment_engine import (
    ExperimentEngine,
    RunOutcome,
    default_probes_for_latency,
)
from src.injection.injector import SafetyCheckError
from src.models.experiments import ExperimentDefinition, RunStatus
from src.models.metrics import SystemMetrics
from src.models.scenarios import FailureScenario, FailureType
from src.models.validation import (
    RecoveryReport,
    RecoverySummary,
    RecoveryTestStatus,
)
from src.models.validation import TestResult as _TestResult
from src.validation.tests import (
    DataLossTest,
    HealthProbeTest,
    LatencyBaselineTest,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def make_definition(
    *,
    name: str = "test-experiment",
    failure_type: FailureType = FailureType.LATENCY_INJECTION,
    target: str = "log-consumer",
    parameters: dict[str, Any] | None = None,
    duration: int = 1,
    severity: int = 2,
) -> ExperimentDefinition:
    """Build a fresh ExperimentDefinition with safe defaults for tests."""
    return ExperimentDefinition(
        name=name,
        type=failure_type,
        target=target,
        parameters=parameters or {"latency_ms": 200, "jitter_ms": 0},
        duration=duration,
        severity=severity,
    )


def make_metrics(
    *,
    cpu_pct: float = 5.0,
    mem_pct: float = 10.0,
    disk_pct: float = 20.0,
    network_latency_ms: float | None = None,
) -> SystemMetrics:
    """Build a minimal SystemMetrics for monitor mocks."""
    return SystemMetrics(
        timestamp=datetime.now(timezone.utc),
        cpu_pct=cpu_pct,
        mem_pct=mem_pct,
        disk_pct=disk_pct,
        network_latency_ms=network_latency_ms,
    )


def make_report(*, scenario_id: str, overall_success: bool) -> RecoveryReport:
    """Build a minimal RecoveryReport with the requested verdict."""
    results = [
        _TestResult(
            name="FakeProbe",
            status=(
                RecoveryTestStatus.COMPLETED
                if overall_success
                else RecoveryTestStatus.FAILED
            ),
            duration=0.01,
            details={},
            error_message=None if overall_success else "synthetic failure",
        )
    ]
    summary = RecoverySummary(
        total_tests=1,
        passed_tests=1 if overall_success else 0,
        failed_tests=0 if overall_success else 1,
        timeout_tests=0,
    )
    return RecoveryReport(
        scenario_id=scenario_id,
        overall_success=overall_success,
        validation_duration=0.05,
        test_results=results,
        summary=summary,
    )


def make_injector(
    *,
    inject_side_effect: Any = None,
    rollback_side_effect: Any = None,
) -> AsyncMock:
    """Build an AsyncMock FailureInjector with ``inject`` and ``rollback``."""
    injector = AsyncMock()
    # ``inject`` returns an ``ActiveScenario``-ish object; the engine doesn't
    # introspect it, so a plain AsyncMock return value (None) is fine. We
    # only pin side_effect when the test wants ``inject`` to raise.
    if inject_side_effect is not None:
        injector.inject.side_effect = inject_side_effect
    if rollback_side_effect is not None:
        injector.rollback.side_effect = rollback_side_effect
    return injector


async def drain(queue: asyncio.Queue) -> list[dict[str, Any]]:
    """Drain everything currently in the queue (non-blocking)."""
    out: list[dict[str, Any]] = []
    while not queue.empty():
        out.append(queue.get_nowait())
    return out


# --------------------------------------------------------------------------- #
# 1. Happy path — no monitor, no probes.
# --------------------------------------------------------------------------- #


async def test_happy_path_no_monitor_no_probes() -> None:
    injector = make_injector()
    definition = make_definition(duration=1)

    engine = ExperimentEngine(injector=injector)
    outcome = await engine.run(definition)

    assert isinstance(outcome, RunOutcome)
    assert outcome.run.status == RunStatus.COMPLETED
    assert outcome.report is None
    assert outcome.error is None
    assert outcome.run.started_at is not None
    assert outcome.run.ended_at is not None
    assert outcome.run.ended_at >= outcome.run.started_at

    # inject called once with a FailureScenario shaped from the definition.
    assert injector.inject.await_count == 1
    inject_args = injector.inject.await_args
    scenario_arg = inject_args.args[0]
    assert isinstance(scenario_arg, FailureScenario)
    assert scenario_arg.type == definition.type
    assert scenario_arg.target == definition.target
    assert scenario_arg.parameters == definition.parameters
    assert scenario_arg.duration == definition.duration
    assert scenario_arg.severity == definition.severity

    # rollback called once with the scenario's id.
    assert injector.rollback.await_count == 1
    rollback_args = injector.rollback.await_args
    assert rollback_args.args[0] == scenario_arg.id


# --------------------------------------------------------------------------- #
# 2. Event queue ordering.
# --------------------------------------------------------------------------- #


async def test_event_queue_receives_ordered_phases() -> None:
    injector = make_injector()
    definition = make_definition(duration=1)
    queue: asyncio.Queue = asyncio.Queue()

    engine = ExperimentEngine(injector=injector, event_queue=queue)
    await engine.run(definition)

    events = await drain(queue)
    event_names = [e["event"] for e in events]

    expected = [
        "run_started",
        "baseline_captured",
        "injecting",
        "observing",
        "rolling_back",
        "validating",
        "run_completed",
    ]
    assert event_names == expected, f"unexpected event order: {event_names}"


# --------------------------------------------------------------------------- #
# 3a. With probes — validator stubbed to succeed.
# --------------------------------------------------------------------------- #


async def test_with_probes_success(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_scenario_id: list[str] = []

    async def fake_run(self: Any, scenario_id: str) -> RecoveryReport:
        captured_scenario_id.append(scenario_id)
        return make_report(scenario_id=scenario_id, overall_success=True)

    monkeypatch.setattr(
        engine_module.RecoveryValidator, "run", fake_run, raising=True
    )

    sentinel_probe = MagicMock(name="probe")

    def probes_factory(definition, baseline_metrics):
        return [sentinel_probe]

    injector = make_injector()
    definition = make_definition(duration=1)

    engine = ExperimentEngine(injector=injector, probes_factory=probes_factory)
    outcome = await engine.run(definition)

    assert outcome.run.status == RunStatus.COMPLETED
    assert outcome.report is not None
    assert outcome.report.overall_success is True
    assert outcome.run.recovery_report_id == outcome.report.report_id
    assert outcome.run.scenario_id is not None
    # Validator was driven with the scenario_id minted by the engine.
    assert captured_scenario_id == [outcome.run.scenario_id]
    assert outcome.error is None


# --------------------------------------------------------------------------- #
# 3b. With probes — validator stubbed to fail.
# --------------------------------------------------------------------------- #


async def test_with_probes_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_run(self: Any, scenario_id: str) -> RecoveryReport:
        return make_report(scenario_id=scenario_id, overall_success=False)

    monkeypatch.setattr(
        engine_module.RecoveryValidator, "run", fake_run, raising=True
    )

    sentinel_probe = MagicMock(name="probe")

    def probes_factory(definition, baseline_metrics):
        return [sentinel_probe]

    injector = make_injector()
    definition = make_definition(duration=1)

    engine = ExperimentEngine(injector=injector, probes_factory=probes_factory)
    outcome = await engine.run(definition)

    assert outcome.run.status == RunStatus.FAILED
    assert outcome.report is not None
    assert outcome.report.overall_success is False
    assert outcome.run.recovery_report_id == outcome.report.report_id
    # No exception path: error stays None even though the verdict is FAILED.
    assert outcome.error is None


# --------------------------------------------------------------------------- #
# 4. Inject raises -> engine catches, emits run_failed, does not re-raise.
# --------------------------------------------------------------------------- #


async def test_inject_failure_safety_check() -> None:
    boom = SafetyCheckError("target not in allowlist: log-consumer")
    injector = make_injector(inject_side_effect=boom)
    definition = make_definition(duration=1)
    queue: asyncio.Queue = asyncio.Queue()

    engine = ExperimentEngine(injector=injector, event_queue=queue)
    outcome = await engine.run(definition)

    assert outcome.run.status == RunStatus.FAILED
    assert outcome.report is None
    assert outcome.error is not None
    assert "SafetyCheckError" in outcome.error
    assert "target not in allowlist" in outcome.error

    # rollback may or may not have been called (inject failed before recording
    # would matter from the injector's POV — but the engine still tries it on
    # exception path; the AsyncMock just accepts the call). We do NOT pin
    # this; what matters is the verdict and event ordering.
    events = await drain(queue)
    event_names = [e["event"] for e in events]

    # The exception was raised inside inject(), AFTER the engine emitted the
    # "injecting" event (see ExperimentEngine.run: emit happens before await).
    # So we expect run_started, baseline_captured, injecting, run_failed.
    # We assert the FIRST is run_started and the LAST is run_failed.
    assert event_names[0] == "run_started"
    assert event_names[-1] == "run_failed"
    # No completed event.
    assert "run_completed" not in event_names


# --------------------------------------------------------------------------- #
# 5. Rollback raises after a successful inject.
# --------------------------------------------------------------------------- #


async def test_rollback_failure_after_observe() -> None:
    boom = RuntimeError("simulated rollback failure")
    injector = make_injector(rollback_side_effect=boom)
    definition = make_definition(duration=1)
    queue: asyncio.Queue = asyncio.Queue()

    engine = ExperimentEngine(injector=injector, event_queue=queue)
    outcome = await engine.run(definition)

    assert outcome.run.status == RunStatus.FAILED
    assert outcome.error is not None
    assert "RuntimeError" in outcome.error
    assert "simulated rollback failure" in outcome.error

    # inject succeeded once; rollback was called from main path (raised);
    # engine's except clause calls rollback a SECOND time best-effort which
    # also raises (and is suppressed). So rollback was awaited twice.
    assert injector.inject.await_count == 1
    assert injector.rollback.await_count >= 1  # at least the failing one

    events = await drain(queue)
    event_names = [e["event"] for e in events]

    # Phase progression up to rolling_back, then failure.
    assert event_names[0] == "run_started"
    assert "baseline_captured" in event_names
    assert "injecting" in event_names
    assert "observing" in event_names
    assert "rolling_back" in event_names
    assert event_names[-1] == "run_failed"
    assert "run_completed" not in event_names


# --------------------------------------------------------------------------- #
# 6. Monitor wiring — baseline + post metrics propagated.
# --------------------------------------------------------------------------- #


async def test_monitor_wired_captures_baseline_and_post() -> None:
    baseline = make_metrics(cpu_pct=5.0, mem_pct=10.0, disk_pct=20.0)
    post = make_metrics(cpu_pct=7.0, mem_pct=12.0, disk_pct=20.0)

    monitor = MagicMock()
    # Each .snapshot() call returns the next pinned value.
    monitor.snapshot.side_effect = [baseline, post]

    injector = make_injector()
    definition = make_definition(duration=1)

    engine = ExperimentEngine(injector=injector, monitor=monitor)
    outcome = await engine.run(definition)

    assert outcome.run.status == RunStatus.COMPLETED
    assert outcome.run.baseline_metrics == baseline
    assert outcome.run.post_metrics == post
    # Monitor was hit exactly twice: pre-inject and post-rollback.
    assert monitor.snapshot.call_count == 2


# --------------------------------------------------------------------------- #
# 7. Observe phase is hard-bounded by duration + grace.
# --------------------------------------------------------------------------- #


async def test_observe_phase_hard_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When observe sleeps too long, ``asyncio.wait_for`` cuts it off.

    We force ``asyncio.sleep`` (only when the engine awaits it for the
    observe-phase pause) to sleep WAY longer than the duration+grace
    budget, and assert the run still completes within
    ``duration + grace + slack`` seconds.
    """
    real_sleep = asyncio.sleep

    async def slow_sleep(seconds: float, *args: Any, **kwargs: Any) -> None:
        # Only blow up the observe-phase pause (the only call site that
        # asks for a multi-second sleep). Sub-second sleeps still flow
        # to the real implementation so internal yields work.
        if seconds >= 0.5:
            await real_sleep(10.0)
        else:
            await real_sleep(seconds)

    monkeypatch.setattr(engine_module.asyncio, "sleep", slow_sleep)

    injector = make_injector()
    definition = make_definition(duration=1)
    engine = ExperimentEngine(
        injector=injector, observe_grace_s=0.3
    )

    t0 = time.monotonic()
    outcome = await engine.run(definition)
    elapsed = time.monotonic() - t0

    # 1s duration + 0.3s grace = 1.3s observe budget; the full run must be
    # well under (1 + 0.3 + 0.5) = 1.8s.
    assert elapsed < 1 + 0.3 + 0.5, f"engine took {elapsed:.2f}s; should be <1.8s"
    # The run still completes (TimeoutError is swallowed in the observe block).
    assert outcome.run.status == RunStatus.COMPLETED


# --------------------------------------------------------------------------- #
# 8. default_probes_for_latency — factory shape + baseline propagation.
# --------------------------------------------------------------------------- #


def test_default_probes_for_latency_with_baseline() -> None:
    """When the monitor has a recent latency, the probe inherits it."""
    definition = make_definition()
    baseline = make_metrics(network_latency_ms=20.0)

    probes = default_probes_for_latency(definition, baseline)

    assert len(probes) == 3
    assert isinstance(probes[0], HealthProbeTest)
    assert isinstance(probes[1], LatencyBaselineTest)
    assert isinstance(probes[2], DataLossTest)
    # The latency probe inherited the baseline.
    assert probes[1]._baseline_ms == 20.0


@pytest.mark.asyncio
async def test_engine_uses_caller_provided_run_so_run_id_is_preserved():
    """RunManager pre-allocates an ExperimentRun and the engine must use it,
    not silently allocate a fresh one with a different run_id. Otherwise WS
    event routing by run_id breaks (see C20)."""
    from unittest.mock import AsyncMock
    from src.engine.experiment_engine import ExperimentEngine
    from src.models.experiments import ExperimentDefinition, ExperimentRun, RunStatus
    from src.models.scenarios import FailureType

    injector = AsyncMock()
    engine = ExperimentEngine(injector=injector)
    definition = ExperimentDefinition(
        name="caller-supplied",
        type=FailureType.LATENCY_INJECTION,
        target="log-consumer",
        parameters={"latency_ms": 100},
        duration=1,
        severity=2,
    )
    pre_run = ExperimentRun(experiment_id=definition.id, status=RunStatus.PENDING)
    pre_run_id = pre_run.run_id

    outcome = await engine.run(definition, run=pre_run)

    # The engine returned the SAME run object the caller passed in.
    assert outcome.run is pre_run
    assert outcome.run.run_id == pre_run_id
    # And the engine actually progressed it through the lifecycle:
    assert outcome.run.status == RunStatus.COMPLETED


def test_default_probes_for_latency_without_baseline() -> None:
    """When no baseline data is available, the factory uses a 50ms default."""
    definition = make_definition()

    # Case A: no metrics at all.
    probes_none = default_probes_for_latency(definition, None)
    assert len(probes_none) == 3
    assert probes_none[1]._baseline_ms == 50.0

    # Case B: metrics present but network_latency_ms is None.
    baseline_no_net = make_metrics(network_latency_ms=None)
    probes_no_net = default_probes_for_latency(definition, baseline_no_net)
    assert len(probes_no_net) == 3
    assert probes_no_net[1]._baseline_ms == 50.0
    # Order is still the same three classes.
    assert isinstance(probes_no_net[0], HealthProbeTest)
    assert isinstance(probes_no_net[1], LatencyBaselineTest)
    assert isinstance(probes_no_net[2], DataLossTest)
