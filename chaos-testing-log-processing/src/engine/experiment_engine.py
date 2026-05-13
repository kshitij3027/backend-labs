"""Lifecycle orchestration for chaos experiments.

The engine consumes an :class:`ExperimentDefinition` and produces a
:class:`RunOutcome` (``ExperimentRun`` + ``RecoveryReport``). Phase
transitions are emitted as events on a caller-supplied :class:`asyncio.Queue`
so the WS broadcaster (C14) and persistence layer (C12) can subscribe
without coupling to this module.

Lifecycle (each phase emits its own event):
    1. baseline   — snapshot SystemMonitor pre-fault.
    2. inject     — FailureInjector.inject(scenario).
    3. observe    — sleep for ``definition.duration`` (hard-bounded by ``wait_for``).
    4. rollback   — FailureInjector.rollback(scenario.id); idempotent.
    5. validate   — RecoveryValidator over the supplied probes.
    6. verdict    — RunStatus.COMPLETED iff report.overall_success else FAILED.

Any exception triggers best-effort rollback and fans the failure out
through the event queue before returning a RunOutcome with status FAILED
and the error message. The engine does NOT re-raise.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from ..injection.injector import FailureInjector
from ..models.experiments import ExperimentDefinition, ExperimentRun, RunStatus
from ..models.metrics import SystemMetrics
from ..models.scenarios import FailureScenario
from ..models.validation import RecoveryReport
from ..monitoring.system_monitor import SystemMonitor
from ..validation.tests import RecoveryProbe
from ..validation.validator import RecoveryValidator

logger = logging.getLogger(__name__)


@dataclass
class RunOutcome:
    """Aggregate result of one experiment run."""

    run: ExperimentRun
    report: Optional[RecoveryReport] = None
    error: Optional[str] = None


ProbesFactory = Callable[
    [ExperimentDefinition, Optional[SystemMetrics]], Sequence[RecoveryProbe]
]


class ExperimentEngine:
    """Orchestrates a single experiment run end-to-end."""

    def __init__(
        self,
        injector: FailureInjector,
        monitor: Optional[SystemMonitor] = None,
        probes_factory: Optional[ProbesFactory] = None,
        event_queue: Optional[asyncio.Queue] = None,
        observe_grace_s: float = 5.0,
    ) -> None:
        self._injector = injector
        self._monitor = monitor
        self._probes_factory = probes_factory
        self._event_queue = event_queue
        self._observe_grace_s = observe_grace_s

    async def _emit(self, event_type: str, run: ExperimentRun, **extra: Any) -> None:
        if self._event_queue is None:
            return
        payload = {
            "event": event_type,
            "run_id": run.run_id,
            "experiment_id": run.experiment_id,
            "status": run.status.value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **extra,
        }
        try:
            self._event_queue.put_nowait(payload)
        except asyncio.QueueFull:
            logger.warning("event queue full; dropping event=%s", event_type)

    async def run(self, definition: ExperimentDefinition) -> RunOutcome:
        run = ExperimentRun(
            experiment_id=definition.id,
            status=RunStatus.PENDING,
        )
        scenario: Optional[FailureScenario] = None

        run.started_at = datetime.now(timezone.utc)
        run.status = RunStatus.RUNNING
        await self._emit(
            "run_started", run, type=definition.type.value, target=definition.target
        )

        try:
            if self._monitor is not None:
                run.baseline_metrics = self._monitor.snapshot()
            await self._emit(
                "baseline_captured",
                run,
                baseline_cpu_pct=getattr(run.baseline_metrics, "cpu_pct", None),
            )

            scenario = FailureScenario(
                type=definition.type,
                target=definition.target,
                parameters=dict(definition.parameters),
                duration=definition.duration,
                severity=definition.severity,
            )
            run.scenario_id = scenario.id
            run.status = RunStatus.INJECTING
            await self._emit(
                "injecting", run, scenario_id=scenario.id, parameters=scenario.parameters
            )
            await self._injector.inject(scenario)

            run.status = RunStatus.OBSERVING
            await self._emit("observing", run, duration_s=definition.duration)
            try:
                await asyncio.wait_for(
                    asyncio.sleep(definition.duration),
                    timeout=definition.duration + self._observe_grace_s,
                )
            except asyncio.TimeoutError:
                logger.warning("observe phase exceeded duration+grace; continuing to rollback")

            run.status = RunStatus.ROLLING_BACK
            await self._emit("rolling_back", run, scenario_id=scenario.id)
            await self._injector.rollback(scenario.id)

            run.status = RunStatus.VALIDATING
            probes: Sequence[RecoveryProbe] = (
                self._probes_factory(definition, run.baseline_metrics)
                if self._probes_factory is not None
                else []
            )
            await self._emit("validating", run, probe_count=len(probes))

            report: Optional[RecoveryReport] = None
            if probes:
                validator = RecoveryValidator(probes)
                report = await validator.run(scenario.id)
                run.recovery_report_id = report.report_id

            if self._monitor is not None:
                run.post_metrics = self._monitor.snapshot()

            if report is None or report.overall_success:
                run.status = RunStatus.COMPLETED
            else:
                run.status = RunStatus.FAILED

            run.ended_at = datetime.now(timezone.utc)
            await self._emit(
                "run_completed",
                run,
                verdict=run.status.value,
                overall_success=getattr(report, "overall_success", None),
                validation_duration=getattr(report, "validation_duration", None),
            )
            return RunOutcome(run=run, report=report)

        except Exception as exc:  # noqa: BLE001
            err = repr(exc)
            run.error_message = err
            run.status = RunStatus.FAILED
            run.ended_at = datetime.now(timezone.utc)
            await self._emit("run_failed", run, error=err)
            if scenario is not None:
                with suppress(Exception):
                    await self._injector.rollback(scenario.id)
            return RunOutcome(run=run, report=None, error=err)


def default_probes_for_latency(
    definition: ExperimentDefinition,
    baseline_metrics: Optional[SystemMetrics],
) -> list[RecoveryProbe]:
    """Convenience probes factory for LATENCY_INJECTION experiments."""
    from ..validation.tests import (
        DataLossTest,
        HealthProbeTest,
        LatencyBaselineTest,
    )

    baseline_ms = 50.0
    if baseline_metrics is not None and baseline_metrics.network_latency_ms:
        baseline_ms = max(baseline_metrics.network_latency_ms, 5.0)

    target_health_pairs = [
        ("log-producer", "http://log-producer:8000/health"),
        ("log-consumer", "http://log-consumer:8000/health"),
    ]
    return [
        HealthProbeTest(targets=target_health_pairs, timeout_s=20.0),
        LatencyBaselineTest(
            url="http://log-consumer:8000/health",
            baseline_ms=baseline_ms,
            tolerance_pct=200.0,
            sample_count=10,
            sample_interval_s=0.1,
            timeout_s=15.0,
        ),
        DataLossTest(
            producer_url="http://log-producer:8000/sent_count",
            consumer_url="http://log-consumer:8000/counter",
            acceptable_loss=50,
            drain_grace_s=2.0,
            max_wait_s=8.0,
            timeout_s=20.0,
        ),
    ]
