"""RecoveryValidator: runs a suite of recovery probes after each scenario.

Probes execute serially in declaration order so failures are easy to read
in the resulting report. Each probe is wrapped in ``asyncio.wait_for``
using its own ``timeout_s``. The verdict (``overall_success``) is True
iff every probe whose ``required_for_success`` is True reached
``COMPLETED``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence

from ..models.validation import (
    RecoveryReport,
    RecoverySummary,
    RecoveryTestStatus,
    TestResult,
)
from .tests import RecoveryProbe

logger = logging.getLogger(__name__)


class RecoveryValidator:
    """Drives a sequence of :class:`RecoveryProbe` instances."""

    def __init__(self, probes: Sequence[RecoveryProbe]) -> None:
        self._probes = list(probes)

    async def run(self, scenario_id: str) -> RecoveryReport:
        started = time.monotonic()
        results: list[TestResult] = []

        for probe in self._probes:
            probe_started = time.monotonic()
            try:
                details = await asyncio.wait_for(probe.execute(), timeout=probe.timeout_s)
                duration = time.monotonic() - probe_started
                results.append(
                    TestResult(
                        name=probe.name,
                        status=RecoveryTestStatus.COMPLETED,
                        duration=duration,
                        details=details,
                    )
                )
                logger.info("probe %s passed in %.2fs", probe.name, duration)
            except asyncio.TimeoutError:
                duration = time.monotonic() - probe_started
                results.append(
                    TestResult(
                        name=probe.name,
                        status=RecoveryTestStatus.TIMEOUT,
                        duration=duration,
                        details={},
                        error_message=f"exceeded timeout_s={probe.timeout_s}",
                    )
                )
                logger.warning("probe %s timed out after %.2fs", probe.name, probe.timeout_s)
            except Exception as exc:  # noqa: BLE001
                duration = time.monotonic() - probe_started
                results.append(
                    TestResult(
                        name=probe.name,
                        status=RecoveryTestStatus.FAILED,
                        duration=duration,
                        details={},
                        error_message=repr(exc),
                    )
                )
                logger.warning("probe %s failed in %.2fs: %r", probe.name, duration, exc)

        validation_duration = time.monotonic() - started

        # Overall success: every probe with required_for_success=True must be COMPLETED.
        required_flags = [p.required_for_success for p in self._probes]
        overall_success = all(
            (not required) or (r.status == RecoveryTestStatus.COMPLETED)
            for r, required in zip(results, required_flags)
        )

        summary = RecoverySummary(
            total_tests=len(results),
            passed_tests=sum(1 for r in results if r.status == RecoveryTestStatus.COMPLETED),
            failed_tests=sum(1 for r in results if r.status == RecoveryTestStatus.FAILED),
            timeout_tests=sum(1 for r in results if r.status == RecoveryTestStatus.TIMEOUT),
        )

        return RecoveryReport(
            scenario_id=scenario_id,
            overall_success=overall_success,
            validation_duration=validation_duration,
            test_results=results,
            summary=summary,
        )
