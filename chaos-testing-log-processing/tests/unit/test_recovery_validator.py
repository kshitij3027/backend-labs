"""Unit tests for C10 — :class:`RecoveryValidator` + built-in recovery probes.

Covers:

- The success-criteria test ``test_validation_flow`` (``project_requirements.md``
  §5), which exercises the validator end-to-end through three variants
  (mixed, all-pass, optional-failure) using small fake probes.
- The :class:`RecoveryProbe` abstract base — abstractness enforcement and
  constructor override semantics.
- The three built-in probes (:class:`HealthProbeTest`,
  :class:`LatencyBaselineTest`, :class:`DataLossTest`) driven by a single
  ``httpx.AsyncClient(transport=httpx.MockTransport(...))`` so no real
  network I/O happens.

Test conventions:

- ``pytest-asyncio`` is in ``asyncio_mode = auto`` (see ``pytest.ini``), so
  ``async def test_...`` functions are picked up without explicit markers.
- Probes are kept fast: tiny backoffs, tiny sample counts, sub-second
  timeouts. The full file runs in well under a second.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
import pytest

from src.models.validation import (
    RecoveryReport,
    RecoverySummary,
    RecoveryTestStatus,
    TestResult,
)
from src.validation import (
    DataLossTest,
    HealthProbeTest,
    LatencyBaselineTest,
    RecoveryProbe,
    RecoveryValidator,
)


# --------------------------------------------------------------------------- #
# Helpers: tiny fake probes used by the validator tests.
# --------------------------------------------------------------------------- #


class _PassingProbe(RecoveryProbe):
    """Returns a pre-canned ``details`` dict immediately."""

    def __init__(self, payload: dict[str, Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._payload = payload

    async def execute(self) -> dict[str, Any]:
        return dict(self._payload)


class _FailingProbe(RecoveryProbe):
    """Raises a ``RuntimeError`` with the given message."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._message = message

    async def execute(self) -> dict[str, Any]:
        raise RuntimeError(self._message)


class _TimingOutProbe(RecoveryProbe):
    """Sleeps long enough to blow the configured ``timeout_s``."""

    def __init__(self, sleep_s: float = 5.0, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sleep_s = sleep_s

    async def execute(self) -> dict[str, Any]:
        await asyncio.sleep(self._sleep_s)
        return {"slept": self._sleep_s}


# --------------------------------------------------------------------------- #
# A. Success-criteria test (must be named exactly ``test_validation_flow``).
# --------------------------------------------------------------------------- #


async def test_validation_flow() -> None:
    """C10 success-criteria: validator drives probes and yields a report.

    Three variants exercised in a single function so the validator's
    behaviour across pass/fail/timeout/optional mixes is covered.
    """
    # ----------------------------------------------------------------- #
    # Variant A: ALL four probes present. Required failure + timeout
    # => overall_success=False. Details / error_message / order checked.
    # ----------------------------------------------------------------- #
    passing = _PassingProbe({"x": 1}, name="passing-required")
    failing = _FailingProbe("boom", name="failing-required")
    timing_out = _TimingOutProbe(sleep_s=5.0, name="timing-out", timeout_s=0.1)
    optional_pass = _PassingProbe(
        {"y": 2}, name="passing-optional", required_for_success=False
    )

    validator = RecoveryValidator([passing, failing, timing_out, optional_pass])
    report = await validator.run("scn-1")

    # Validator never re-raises probe exceptions/timeouts — they're absorbed.
    assert isinstance(report, RecoveryReport)
    assert report.scenario_id == "scn-1"
    assert report.overall_success is False

    # Same order as construction; one TestResult per probe.
    assert len(report.test_results) == 4
    names = [r.name for r in report.test_results]
    assert names == [
        "passing-required",
        "failing-required",
        "timing-out",
        "passing-optional",
    ]
    statuses = [r.status for r in report.test_results]
    assert statuses == [
        RecoveryTestStatus.COMPLETED,
        RecoveryTestStatus.FAILED,
        RecoveryTestStatus.TIMEOUT,
        RecoveryTestStatus.COMPLETED,
    ]

    # Summary counters match the statuses.
    assert isinstance(report.summary, RecoverySummary)
    assert report.summary.total_tests == 4
    assert report.summary.passed_tests == 2
    assert report.summary.failed_tests == 1
    assert report.summary.timeout_tests == 1

    # Per-result details / error messages.
    assert report.test_results[0].details == {"x": 1}
    assert report.test_results[0].error_message is None
    assert "boom" in (report.test_results[1].error_message or "")
    assert report.test_results[1].details == {}
    assert "timeout" in (report.test_results[2].error_message or "").lower()
    assert report.test_results[2].details == {}
    assert report.test_results[3].details == {"y": 2}
    assert report.test_results[3].error_message is None

    # Wall-clock duration is dominated by the timing-out probe (0.1s timeout).
    assert report.validation_duration > 0.1

    # Each TestResult is a proper Pydantic model with the expected shape.
    for r in report.test_results:
        assert isinstance(r, TestResult)
        assert r.duration >= 0.0

    # ----------------------------------------------------------------- #
    # Variant B: only passing probes. overall_success=True.
    # ----------------------------------------------------------------- #
    all_pass_validator = RecoveryValidator(
        [
            _PassingProbe({"a": 1}, name="pass-a"),
            _PassingProbe({"b": 2}, name="pass-b"),
        ]
    )
    report_b = await all_pass_validator.run("scn-2")
    assert report_b.overall_success is True
    assert report_b.summary.total_tests == 2
    assert report_b.summary.passed_tests == 2
    assert report_b.summary.failed_tests == 0
    assert report_b.summary.timeout_tests == 0

    # ----------------------------------------------------------------- #
    # Variant C: a probe fails but is marked optional. overall_success=True.
    # ----------------------------------------------------------------- #
    optional_failure_validator = RecoveryValidator(
        [
            _FailingProbe(
                "harmless",
                name="optional-failure",
                required_for_success=False,
            ),
        ]
    )
    report_c = await optional_failure_validator.run("scn-3")
    assert report_c.overall_success is True
    assert report_c.summary.total_tests == 1
    assert report_c.summary.passed_tests == 0
    assert report_c.summary.failed_tests == 1
    assert report_c.summary.timeout_tests == 0


# --------------------------------------------------------------------------- #
# B. RecoveryProbe ABC.
# --------------------------------------------------------------------------- #


class TestRecoveryProbeABC:
    """The probe base class must be abstract and accept attr overrides."""

    def test_cannot_instantiate_base_abc(self) -> None:
        with pytest.raises(TypeError):
            RecoveryProbe()  # type: ignore[abstract]

    def test_subclass_without_execute_is_still_abstract(self) -> None:
        class _NotConcrete(RecoveryProbe):
            pass

        with pytest.raises(TypeError):
            _NotConcrete()  # type: ignore[abstract]

    def test_constructor_overrides_apply(self) -> None:
        """Each of the four constructor knobs updates the instance attrs."""

        class _OK(RecoveryProbe):
            async def execute(self) -> dict[str, Any]:
                return {}

        probe = _OK(
            name="custom-name",
            required_for_success=False,
            timeout_s=1.25,
            description="custom description",
        )
        assert probe.name == "custom-name"
        assert probe.required_for_success is False
        assert probe.timeout_s == pytest.approx(1.25)
        assert probe.description == "custom description"

        # Defaults preserved when nothing is overridden.
        default = _OK()
        assert default.name == "probe"
        assert default.required_for_success is True
        assert default.timeout_s == pytest.approx(30.0)
        assert default.description == ""


# --------------------------------------------------------------------------- #
# C. HealthProbeTest.
# --------------------------------------------------------------------------- #


class TestHealthProbeTest:
    """GET each target /health URL with exponential-backoff retries."""

    async def test_happy_path_single_attempt(self) -> None:
        seen_hosts: list[str] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            seen_hosts.append(request.url.host)
            return httpx.Response(200, json={"status": "ok"})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_handler)
        ) as client:
            probe = HealthProbeTest(
                targets=[
                    ("log-producer", "http://log-producer:8000/health"),
                    ("log-consumer", "http://log-consumer:8000/health"),
                ],
                max_attempts=1,
                initial_backoff_s=0.001,
                max_backoff_s=0.01,
                http_client=client,
            )
            details = await probe.execute()

        assert set(seen_hosts) == {"log-producer", "log-consumer"}
        assert details["targets"]["log-consumer"]["code"] == 200
        assert details["targets"]["log-consumer"]["attempts"] == 1
        assert details["targets"]["log-producer"]["code"] == 200
        assert details["targets"]["log-producer"]["attempts"] == 1

    async def test_retry_then_success(self) -> None:
        """First call returns 503, the second 200 -> ``attempts == 2``."""
        calls = {"n": 0}

        def _handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(503, json={"status": "warming"})
            return httpx.Response(200, json={"status": "ok"})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_handler)
        ) as client:
            probe = HealthProbeTest(
                targets=[("log-consumer", "http://log-consumer:8000/health")],
                max_attempts=2,
                initial_backoff_s=0.001,
                max_backoff_s=0.01,
                http_client=client,
            )
            details = await probe.execute()

        assert calls["n"] == 2
        target_info = details["targets"]["log-consumer"]
        assert target_info["code"] == 200
        assert target_info["attempts"] == 2

    async def test_all_attempts_fail_raises(self) -> None:
        """Transport raises ``ConnectError`` -> the probe surfaces the target."""

        def _handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError(
                "boom", request=request
            )  # MockTransport propagates this.

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_handler)
        ) as client:
            probe = HealthProbeTest(
                targets=[("log-consumer", "http://log-consumer:8000/health")],
                max_attempts=2,
                initial_backoff_s=0.001,
                max_backoff_s=0.01,
                http_client=client,
            )
            with pytest.raises(RuntimeError) as excinfo:
                await probe.execute()

        assert "log-consumer" in str(excinfo.value)


# --------------------------------------------------------------------------- #
# D. LatencyBaselineTest.
# --------------------------------------------------------------------------- #


class TestLatencyBaselineTest:
    """p95 latency must stay within ``baseline_ms * (1 + tolerance_pct/100)``."""

    async def test_passes_when_responses_are_fast(self) -> None:
        def _handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": True})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_handler)
        ) as client:
            probe = LatencyBaselineTest(
                url="http://log-consumer:8000/metrics",
                baseline_ms=10.0,
                tolerance_pct=500.0,  # ceiling = 60ms -> easily met.
                sample_count=3,
                sample_interval_s=0.001,
                http_client=client,
            )
            details = await probe.execute()

        assert details["sample_count"] == 3
        # ceiling = baseline_ms * (1 + 500/100) = 10 * 6 = 60.0
        assert details["ceiling_ms"] == pytest.approx(60.0)
        assert details["p95_ms"] <= details["ceiling_ms"]
        assert "mean_ms" in details
        assert details["baseline_ms"] == pytest.approx(10.0)
        assert details["tolerance_pct"] == pytest.approx(500.0)

    async def test_fails_when_responses_too_slow(self) -> None:
        """Sleep ~50ms per request; baseline 1ms + 10% -> ceiling 1.1ms."""

        async def _handler(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0.05)  # 50 ms
            return httpx.Response(200, json={"ok": True})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_handler)
        ) as client:
            probe = LatencyBaselineTest(
                url="http://log-consumer:8000/metrics",
                baseline_ms=1.0,
                tolerance_pct=10.0,  # ceiling = 1.1ms
                sample_count=3,
                sample_interval_s=0.001,
                http_client=client,
            )
            with pytest.raises(RuntimeError) as excinfo:
                await probe.execute()

        msg = str(excinfo.value)
        assert "p95" in msg
        assert "ceiling" in msg


# --------------------------------------------------------------------------- #
# E. DataLossTest.
# --------------------------------------------------------------------------- #


class TestDataLossTest:
    """Compare producer ``sent_count`` to consumer ``counter`` post-drain."""

    async def test_passes_when_drained(self) -> None:
        def _handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "log-producer":
                return httpx.Response(200, json={"sent_count": 100})
            return httpx.Response(200, json={"counter": 100})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_handler)
        ) as client:
            probe = DataLossTest(
                producer_url="http://log-producer:8000/sent_count",
                consumer_url="http://log-consumer:8000/counter",
                acceptable_loss=0,
                drain_grace_s=0.0,
                max_wait_s=1.0,
                http_client=client,
            )
            details = await probe.execute()

        assert details["produced"] == 100
        assert details["processed"] == 100
        assert details["delta"] == 0
        assert details["acceptable_loss"] == 0

    async def test_fails_when_consumer_lags(self) -> None:
        """Producer at 100, consumer stuck at 50 -> delta=50 -> raise."""

        def _handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "log-producer":
                return httpx.Response(200, json={"sent_count": 100})
            return httpx.Response(200, json={"counter": 50})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_handler)
        ) as client:
            probe = DataLossTest(
                producer_url="http://log-producer:8000/sent_count",
                consumer_url="http://log-consumer:8000/counter",
                acceptable_loss=0,
                drain_grace_s=0.0,
                max_wait_s=0.5,
                http_client=client,
            )
            with pytest.raises(RuntimeError) as excinfo:
                await probe.execute()

        msg = str(excinfo.value)
        assert "data loss" in msg
        assert "delta=50" in msg
