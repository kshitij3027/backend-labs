"""Unit tests for C2 Pydantic v2 domain models.

Covers ``src/models/scenarios.py``, ``src/models/metrics.py``,
``src/models/experiments.py``, and ``src/models/validation.py`` — the four
behavior-free domain modules delivered by commit C2 of plan.md.

Test goals:

* Enum coverage (members + string values match the JSON-on-the-wire contract
  from ``project_requirements.md`` §8).
* Field constraints fire (bounds, min_length, ge/le, extra="forbid").
* Defaults are sane (auto-assigned id, status=PENDING, container_stats={}).
* JSON round-trip preserves nested types (``service_health``, ``container_stats``,
  ``baseline_metrics``).
* ``validate_assignment=True`` blocks bad mutations post-construction.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.models.experiments import (
    ExperimentDefinition,
    ExperimentRun,
    FailureType as ExperimentsFailureType,
    Hypothesis,
    RunStatus,
)
from src.models.metrics import ServiceHealth, SystemMetrics
from src.models.scenarios import (
    FailureScenario,
    FailureType,
    ScenarioStatus,
)
from src.models.validation import (
    RecoveryReport,
    RecoverySummary,
)
from src.models.validation import RecoveryTest as RecoveryTestModel
from src.models.validation import RecoveryTestStatus
from src.models.validation import TestResult as TestResultModel

# Pydantic models whose class names start with "Test" trigger pytest's class
# collection heuristic. Tag them so pytest skips collection (they're imports,
# not test cases). __test__ = False is the documented opt-out.
TestResultModel.__test__ = False  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# scenarios.py
# ---------------------------------------------------------------------------


class TestFailureTypeEnum:
    """``FailureType`` covers the 5 fault families with exact string values."""

    def test_all_members_present(self) -> None:
        members = {m.name for m in FailureType}
        assert members == {
            "NETWORK_PARTITION",
            "RESOURCE_EXHAUSTION",
            "COMPONENT_FAILURE",
            "LATENCY_INJECTION",
            "PACKET_LOSS",
        }

    def test_string_values_exact(self) -> None:
        assert FailureType.NETWORK_PARTITION.value == "network_partition"
        assert FailureType.RESOURCE_EXHAUSTION.value == "resource_exhaustion"
        assert FailureType.COMPONENT_FAILURE.value == "component_failure"
        assert FailureType.LATENCY_INJECTION.value == "latency_injection"
        assert FailureType.PACKET_LOSS.value == "packet_loss"

    def test_is_str_enum(self) -> None:
        # Subclassing str means values serialize cleanly into JSON.
        assert isinstance(FailureType.LATENCY_INJECTION, str)
        assert FailureType.LATENCY_INJECTION == "latency_injection"


class TestScenarioStatusEnum:
    """``ScenarioStatus`` lifecycle values match the spec."""

    def test_all_members_present(self) -> None:
        members = {m.name for m in ScenarioStatus}
        assert members == {
            "PENDING",
            "ACTIVE",
            "COMPLETED",
            "FAILED",
            "ABORTED",
        }

    def test_string_values_exact(self) -> None:
        assert ScenarioStatus.PENDING.value == "pending"
        assert ScenarioStatus.ACTIVE.value == "active"
        assert ScenarioStatus.COMPLETED.value == "completed"
        assert ScenarioStatus.FAILED.value == "failed"
        assert ScenarioStatus.ABORTED.value == "aborted"


class TestFailureScenarioHappyPath:
    """A minimal ``FailureScenario`` constructs with sane defaults."""

    def _minimal_kwargs(self) -> dict:
        return {
            "type": FailureType.LATENCY_INJECTION,
            "target": "log-consumer",
            "duration": 300,
            "severity": 2,
        }

    def test_minimal_construction_succeeds(self) -> None:
        scenario = FailureScenario(**self._minimal_kwargs())
        assert scenario.type == FailureType.LATENCY_INJECTION
        assert scenario.target == "log-consumer"
        assert scenario.duration == 300
        assert scenario.severity == 2

    def test_id_is_32_char_hex(self) -> None:
        scenario = FailureScenario(**self._minimal_kwargs())
        assert isinstance(scenario.id, str)
        assert len(scenario.id) == 32
        # All chars must be lowercase hex (matches uuid4().hex output).
        int(scenario.id, 16)  # raises ValueError if not hex
        assert scenario.id == scenario.id.lower()

    def test_id_is_unique_per_instance(self) -> None:
        a = FailureScenario(**self._minimal_kwargs())
        b = FailureScenario(**self._minimal_kwargs())
        assert a.id != b.id

    def test_status_defaults_to_pending(self) -> None:
        scenario = FailureScenario(**self._minimal_kwargs())
        assert scenario.status == ScenarioStatus.PENDING

    def test_created_at_is_timezone_aware_utc(self) -> None:
        scenario = FailureScenario(**self._minimal_kwargs())
        assert isinstance(scenario.created_at, datetime)
        assert scenario.created_at.tzinfo is not None
        # UTC offset is exactly zero.
        assert scenario.created_at.utcoffset().total_seconds() == 0

    def test_parameters_defaults_to_empty_dict(self) -> None:
        scenario = FailureScenario(**self._minimal_kwargs())
        assert scenario.parameters == {}

    def test_parameters_accept_arbitrary_kwargs(self) -> None:
        scenario = FailureScenario(
            **self._minimal_kwargs(),
            parameters={"latency_ms": 200, "jitter_ms": 50},
        )
        assert scenario.parameters == {"latency_ms": 200, "jitter_ms": 50}


class TestFailureScenarioValidation:
    """Field constraints fire as documented in the source docstrings."""

    def _minimal_kwargs(self) -> dict:
        return {
            "type": FailureType.LATENCY_INJECTION,
            "target": "log-consumer",
            "duration": 300,
            "severity": 2,
        }

    def test_duration_zero_raises(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["duration"] = 0
        with pytest.raises(ValidationError):
            FailureScenario(**kwargs)

    def test_duration_above_3600_raises(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["duration"] = 3601
        with pytest.raises(ValidationError):
            FailureScenario(**kwargs)

    def test_duration_at_boundaries_passes(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["duration"] = 1
        FailureScenario(**kwargs)
        kwargs["duration"] = 3600
        FailureScenario(**kwargs)

    def test_severity_zero_raises(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["severity"] = 0
        with pytest.raises(ValidationError):
            FailureScenario(**kwargs)

    def test_severity_six_raises(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["severity"] = 6
        with pytest.raises(ValidationError):
            FailureScenario(**kwargs)

    def test_severity_at_boundaries_passes(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["severity"] = 1
        FailureScenario(**kwargs)
        kwargs["severity"] = 5
        FailureScenario(**kwargs)

    def test_empty_target_raises(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["target"] = ""
        with pytest.raises(ValidationError):
            FailureScenario(**kwargs)

    def test_extra_field_forbidden(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["mystery_extra_field"] = "nope"
        with pytest.raises(ValidationError):
            FailureScenario(**kwargs)

    def test_validate_assignment_blocks_bad_severity(self) -> None:
        """Post-construction mutation goes through validation."""
        scenario = FailureScenario(**self._minimal_kwargs())
        with pytest.raises(ValidationError):
            scenario.severity = 99  # out of [1, 5]

    def test_validate_assignment_blocks_bad_duration(self) -> None:
        scenario = FailureScenario(**self._minimal_kwargs())
        with pytest.raises(ValidationError):
            scenario.duration = 0

    def test_validate_assignment_allows_good_status_change(self) -> None:
        scenario = FailureScenario(**self._minimal_kwargs())
        scenario.status = ScenarioStatus.ACTIVE  # should not raise
        assert scenario.status == ScenarioStatus.ACTIVE


class TestFailureScenarioJsonRoundTrip:
    """``model_dump_json`` -> ``model_validate_json`` returns an equal model."""

    def test_round_trip_preserves_fields(self) -> None:
        original = FailureScenario(
            type=FailureType.LATENCY_INJECTION,
            target="log-consumer",
            parameters={"latency_ms": 200},
            duration=300,
            severity=2,
        )
        dumped = original.model_dump_json()
        restored = FailureScenario.model_validate_json(dumped)
        assert restored == original

    def test_round_trip_includes_sample_fields_from_spec(self) -> None:
        """JSON example from project_requirements.md §8 sample experiment definition.

        Expected shape: ``type``, ``target``, ``parameters``, ``duration``, ``severity``
        all present in the serialized form.
        """
        scenario = FailureScenario(
            type=FailureType.LATENCY_INJECTION,
            target="log-collector-service",
            parameters={"latency_ms": 200},
            duration=300,
            severity=2,
        )
        as_dict = json.loads(scenario.model_dump_json())
        for required_field in ("type", "target", "parameters", "duration", "severity"):
            assert required_field in as_dict, f"missing field {required_field}"
        assert as_dict["type"] == "latency_injection"
        assert as_dict["target"] == "log-collector-service"
        assert as_dict["parameters"] == {"latency_ms": 200}
        assert as_dict["duration"] == 300
        assert as_dict["severity"] == 2

    def test_enum_serializes_as_lowercase_string(self) -> None:
        scenario = FailureScenario(
            type=FailureType.PACKET_LOSS,
            target="log-consumer",
            duration=60,
            severity=1,
        )
        as_dict = json.loads(scenario.model_dump_json())
        assert as_dict["type"] == "packet_loss"
        assert as_dict["status"] == "pending"


# ---------------------------------------------------------------------------
# metrics.py
# ---------------------------------------------------------------------------


class TestServiceHealth:
    """``ServiceHealth`` requires name + is_healthy + last_check_at."""

    def test_happy_path(self) -> None:
        now = datetime.now(timezone.utc)
        health = ServiceHealth(name="log-producer", is_healthy=True, last_check_at=now)
        assert health.name == "log-producer"
        assert health.is_healthy is True
        assert health.last_check_at == now
        assert health.latency_ms is None  # default

    def test_missing_name_raises(self) -> None:
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError):
            ServiceHealth(is_healthy=True, last_check_at=now)  # type: ignore[call-arg]

    def test_empty_name_raises(self) -> None:
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError):
            ServiceHealth(name="", is_healthy=True, last_check_at=now)

    def test_missing_is_healthy_raises(self) -> None:
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError):
            ServiceHealth(name="log-producer", last_check_at=now)  # type: ignore[call-arg]

    def test_missing_last_check_at_raises(self) -> None:
        with pytest.raises(ValidationError):
            ServiceHealth(name="log-producer", is_healthy=True)  # type: ignore[call-arg]

    def test_latency_negative_raises(self) -> None:
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError):
            ServiceHealth(
                name="log-producer",
                is_healthy=True,
                last_check_at=now,
                latency_ms=-1.0,
            )


class TestSystemMetricsHappyPath:
    """``SystemMetrics`` has sane defaults for collections."""

    def _minimal_kwargs(self) -> dict:
        return {"cpu_pct": 23.7, "mem_pct": 41.2, "disk_pct": 58.0}

    def test_minimal_construction(self) -> None:
        m = SystemMetrics(**self._minimal_kwargs())
        assert m.cpu_pct == 23.7
        assert m.mem_pct == 41.2
        assert m.disk_pct == 58.0
        assert m.network_latency_ms is None
        assert m.service_health == []
        assert m.container_stats == {}

    def test_timestamp_defaults_to_utc(self) -> None:
        m = SystemMetrics(**self._minimal_kwargs())
        assert m.timestamp.tzinfo is not None
        assert m.timestamp.utcoffset().total_seconds() == 0


class TestSystemMetricsBounds:
    """0..100 bounds on cpu/mem/disk percentages."""

    def test_cpu_negative_raises(self) -> None:
        with pytest.raises(ValidationError):
            SystemMetrics(cpu_pct=-1.0, mem_pct=50.0, disk_pct=50.0)

    def test_cpu_above_100_raises(self) -> None:
        with pytest.raises(ValidationError):
            SystemMetrics(cpu_pct=101.0, mem_pct=50.0, disk_pct=50.0)

    def test_mem_negative_raises(self) -> None:
        with pytest.raises(ValidationError):
            SystemMetrics(cpu_pct=50.0, mem_pct=-1.0, disk_pct=50.0)

    def test_mem_above_100_raises(self) -> None:
        with pytest.raises(ValidationError):
            SystemMetrics(cpu_pct=50.0, mem_pct=101.0, disk_pct=50.0)

    def test_disk_negative_raises(self) -> None:
        with pytest.raises(ValidationError):
            SystemMetrics(cpu_pct=50.0, mem_pct=50.0, disk_pct=-1.0)

    def test_disk_above_100_raises(self) -> None:
        with pytest.raises(ValidationError):
            SystemMetrics(cpu_pct=50.0, mem_pct=50.0, disk_pct=101.0)

    def test_boundaries_zero_and_hundred_accepted(self) -> None:
        SystemMetrics(cpu_pct=0.0, mem_pct=0.0, disk_pct=0.0)
        SystemMetrics(cpu_pct=100.0, mem_pct=100.0, disk_pct=100.0)


class TestSystemMetricsJsonRoundTrip:
    """Nested ``service_health`` list + ``container_stats`` dict preserved."""

    def test_full_round_trip(self) -> None:
        now = datetime.now(timezone.utc)
        original = SystemMetrics(
            cpu_pct=23.7,
            mem_pct=41.2,
            disk_pct=58.0,
            network_latency_ms=12.4,
            service_health=[
                ServiceHealth(
                    name="log-producer",
                    is_healthy=True,
                    last_check_at=now,
                    latency_ms=4.1,
                ),
                ServiceHealth(
                    name="log-consumer",
                    is_healthy=True,
                    last_check_at=now,
                    latency_ms=5.0,
                ),
            ],
            container_stats={
                "log-consumer": {"cpu_pct": 12.5, "mem_pct": 4.1},
                "log-producer": {"cpu_pct": 3.2, "mem_pct": 2.8},
            },
        )
        dumped = original.model_dump_json()
        restored = SystemMetrics.model_validate_json(dumped)
        assert restored == original
        assert len(restored.service_health) == 2
        assert restored.container_stats["log-consumer"]["cpu_pct"] == 12.5


# ---------------------------------------------------------------------------
# experiments.py
# ---------------------------------------------------------------------------


class TestHypothesis:
    """``Hypothesis`` defaults + validation."""

    def test_happy_path(self) -> None:
        h = Hypothesis(statement="If 200ms latency is injected then p95 returns to baseline within 30s")
        assert h.statement.startswith("If")
        assert h.recovery_time_budget_s == 30  # default
        assert h.expected_invariants == []

    def test_empty_statement_raises(self) -> None:
        with pytest.raises(ValidationError):
            Hypothesis(statement="")

    def test_recovery_time_budget_below_one_raises(self) -> None:
        with pytest.raises(ValidationError):
            Hypothesis(statement="x", recovery_time_budget_s=0)

    def test_custom_recovery_time_budget(self) -> None:
        h = Hypothesis(statement="x", recovery_time_budget_s=60)
        assert h.recovery_time_budget_s == 60

    def test_invariants_list(self) -> None:
        h = Hypothesis(
            statement="x",
            expected_invariants=["no data loss", "p95 < 200ms"],
        )
        assert h.expected_invariants == ["no data loss", "p95 < 200ms"]


class TestExperimentDefinitionHappyPath:
    """``ExperimentDefinition`` defaults match spec."""

    def _minimal_kwargs(self) -> dict:
        return {
            "name": "latency-burn-in",
            "type": FailureType.LATENCY_INJECTION,
            "target": "log-consumer",
        }

    def test_minimal_construction(self) -> None:
        d = ExperimentDefinition(**self._minimal_kwargs())
        assert d.name == "latency-burn-in"
        assert d.type == FailureType.LATENCY_INJECTION

    def test_duration_defaults_to_300(self) -> None:
        d = ExperimentDefinition(**self._minimal_kwargs())
        assert d.duration == 300

    def test_severity_defaults_to_two(self) -> None:
        d = ExperimentDefinition(**self._minimal_kwargs())
        assert d.severity == 2

    def test_parameters_defaults_to_empty_dict(self) -> None:
        d = ExperimentDefinition(**self._minimal_kwargs())
        assert d.parameters == {}

    def test_hypothesis_defaults_to_none(self) -> None:
        d = ExperimentDefinition(**self._minimal_kwargs())
        assert d.hypothesis is None

    def test_description_defaults_to_empty(self) -> None:
        d = ExperimentDefinition(**self._minimal_kwargs())
        assert d.description == ""

    def test_id_is_32_char_hex(self) -> None:
        d = ExperimentDefinition(**self._minimal_kwargs())
        assert len(d.id) == 32
        int(d.id, 16)

    def test_type_accepts_failure_type_enum(self) -> None:
        d = ExperimentDefinition(
            name="x",
            type=FailureType.NETWORK_PARTITION,
            target="log-consumer",
        )
        assert d.type == FailureType.NETWORK_PARTITION

    def test_type_accepts_string_form(self) -> None:
        d = ExperimentDefinition(
            name="x",
            type="latency_injection",  # type: ignore[arg-type]
            target="log-consumer",
        )
        assert d.type == FailureType.LATENCY_INJECTION

    def test_type_rejects_unknown_string(self) -> None:
        with pytest.raises(ValidationError):
            ExperimentDefinition(
                name="x",
                type="not_a_real_type",  # type: ignore[arg-type]
                target="log-consumer",
            )

    def test_failure_type_reexport(self) -> None:
        """experiments.FailureType is the same symbol as scenarios.FailureType."""
        assert ExperimentsFailureType is FailureType


class TestRunStatusEnum:
    """``RunStatus`` covers the engine lifecycle."""

    def test_required_members_present(self) -> None:
        required = {
            "PENDING",
            "RUNNING",
            "INJECTING",
            "OBSERVING",
            "ROLLING_BACK",
            "VALIDATING",
            "COMPLETED",
            "FAILED",
            "ABORTED",
        }
        actual = {m.name for m in RunStatus}
        assert required.issubset(actual)

    def test_string_values_are_snake_case(self) -> None:
        assert RunStatus.PENDING.value == "pending"
        assert RunStatus.ROLLING_BACK.value == "rolling_back"
        assert RunStatus.COMPLETED.value == "completed"


class TestExperimentRunHappyPath:
    """``ExperimentRun`` defaults — PENDING + None for optionals."""

    def test_minimal_construction(self) -> None:
        run = ExperimentRun(experiment_id="abc123")
        assert run.experiment_id == "abc123"
        assert run.status == RunStatus.PENDING
        assert run.started_at is None
        assert run.ended_at is None
        assert run.baseline_metrics is None
        assert run.post_metrics is None
        assert run.scenario_id is None
        assert run.recovery_report_id is None
        assert run.error_message is None

    def test_run_id_is_32_char_hex(self) -> None:
        run = ExperimentRun(experiment_id="abc123")
        assert len(run.run_id) == 32
        int(run.run_id, 16)

    def test_missing_experiment_id_raises(self) -> None:
        with pytest.raises(ValidationError):
            ExperimentRun()  # type: ignore[call-arg]

    def test_empty_experiment_id_raises(self) -> None:
        with pytest.raises(ValidationError):
            ExperimentRun(experiment_id="")


class TestExperimentRunJsonRoundTrip:
    """Fully populated run including nested ``baseline_metrics`` round-trips."""

    def test_full_round_trip(self) -> None:
        now = datetime.now(timezone.utc)
        baseline = SystemMetrics(
            cpu_pct=10.0,
            mem_pct=20.0,
            disk_pct=30.0,
            service_health=[
                ServiceHealth(name="log-consumer", is_healthy=True, last_check_at=now),
            ],
            container_stats={"log-consumer": {"cpu_pct": 5.0, "mem_pct": 2.0}},
        )
        post = SystemMetrics(cpu_pct=12.0, mem_pct=22.0, disk_pct=30.0)
        original = ExperimentRun(
            experiment_id="exp-abc",
            status=RunStatus.COMPLETED,
            started_at=now,
            ended_at=now,
            baseline_metrics=baseline,
            post_metrics=post,
            scenario_id="scen-xyz",
            recovery_report_id="rep-123",
            error_message=None,
        )
        dumped = original.model_dump_json()
        restored = ExperimentRun.model_validate_json(dumped)
        assert restored == original
        assert restored.baseline_metrics is not None
        assert restored.baseline_metrics.cpu_pct == 10.0
        assert len(restored.baseline_metrics.service_health) == 1
        assert restored.baseline_metrics.container_stats["log-consumer"]["cpu_pct"] == 5.0


# ---------------------------------------------------------------------------
# validation.py
# ---------------------------------------------------------------------------


class TestRecoveryTestStatusEnum:
    """5 lifecycle members for a recovery test."""

    def test_all_members_present(self) -> None:
        members = {m.name for m in RecoveryTestStatus}
        assert members == {"PENDING", "RUNNING", "COMPLETED", "FAILED", "TIMEOUT"}

    def test_values_snake_case(self) -> None:
        assert RecoveryTestStatus.PENDING.value == "pending"
        assert RecoveryTestStatus.RUNNING.value == "running"
        assert RecoveryTestStatus.COMPLETED.value == "completed"
        assert RecoveryTestStatus.FAILED.value == "failed"
        assert RecoveryTestStatus.TIMEOUT.value == "timeout"


class TestRecoveryTest:
    """``RecoveryTest`` defaults for required_for_success + timeout."""

    def test_happy_path(self) -> None:
        t = RecoveryTestModel(name="HealthProbeTest")
        assert t.name == "HealthProbeTest"
        assert t.required_for_success is True
        assert t.timeout_s == 30.0
        assert t.description == ""

    def test_empty_name_raises(self) -> None:
        with pytest.raises(ValidationError):
            RecoveryTestModel(name="")

    def test_timeout_zero_raises(self) -> None:
        with pytest.raises(ValidationError):
            RecoveryTestModel(name="x", timeout_s=0.0)

    def test_negative_timeout_raises(self) -> None:
        with pytest.raises(ValidationError):
            RecoveryTestModel(name="x", timeout_s=-1.0)


class TestTestResult:
    """``TestResult`` defaults for details + error_message."""

    def test_happy_path(self) -> None:
        r = TestResultModel(name="HealthProbeTest", status=RecoveryTestStatus.COMPLETED, duration=1.2)
        assert r.name == "HealthProbeTest"
        assert r.status == RecoveryTestStatus.COMPLETED
        assert r.duration == 1.2
        assert r.details == {}
        assert r.error_message is None

    def test_failed_with_error_message(self) -> None:
        r = TestResultModel(
            name="LatencyBaselineTest",
            status=RecoveryTestStatus.FAILED,
            duration=2.5,
            error_message="p95 exceeded baseline by 300%",
            details={"observed_p95_ms": 800, "baseline_p95_ms": 200},
        )
        assert r.error_message == "p95 exceeded baseline by 300%"
        assert r.details["observed_p95_ms"] == 800

    def test_negative_duration_raises(self) -> None:
        with pytest.raises(ValidationError):
            TestResultModel(
                name="x",
                status=RecoveryTestStatus.COMPLETED,
                duration=-0.1,
            )

    def test_empty_name_raises(self) -> None:
        with pytest.raises(ValidationError):
            TestResultModel(name="", status=RecoveryTestStatus.COMPLETED, duration=1.0)


class TestRecoverySummary:
    """RecoverySummary is just storage — no derived field validation."""

    def test_happy_path(self) -> None:
        s = RecoverySummary(
            total_tests=4,
            passed_tests=3,
            failed_tests=1,
            timeout_tests=0,
        )
        assert s.total_tests == 4
        assert s.passed_tests == 3
        assert s.failed_tests == 1
        assert s.timeout_tests == 0

    def test_negative_count_raises(self) -> None:
        with pytest.raises(ValidationError):
            RecoverySummary(
                total_tests=-1,
                passed_tests=0,
                failed_tests=0,
                timeout_tests=0,
            )

    def test_zero_counts_accepted(self) -> None:
        # No derived field validation — math doesn't have to add up.
        s = RecoverySummary(
            total_tests=0,
            passed_tests=0,
            failed_tests=0,
            timeout_tests=0,
        )
        assert s.total_tests == 0


class TestRecoveryReportFromSpecPayload:
    """Spec sample payload from ``project_requirements.md`` §8 parses cleanly."""

    def test_spec_payload_validates(self) -> None:
        payload = {
            "scenario_id": "abc",
            "overall_success": True,
            "validation_duration": 12.4,
            "test_results": [
                {
                    "name": "x",
                    "status": "completed",
                    "duration": 1.2,
                    "details": {},
                    "error_message": None,
                }
            ],
            "summary": {
                "total_tests": 1,
                "passed_tests": 1,
                "failed_tests": 0,
                "timeout_tests": 0,
            },
        }
        report = RecoveryReport.model_validate(payload)
        assert report.scenario_id == "abc"
        assert report.overall_success is True
        assert report.validation_duration == 12.4
        assert len(report.test_results) == 1
        assert report.test_results[0].name == "x"
        assert report.test_results[0].status == RecoveryTestStatus.COMPLETED
        assert report.test_results[0].duration == 1.2
        assert report.test_results[0].details == {}
        assert report.test_results[0].error_message is None
        assert isinstance(report.summary, RecoverySummary)
        assert report.summary.total_tests == 1
        assert report.summary.passed_tests == 1
        # ``report_id`` is auto-generated when not provided.
        assert len(report.report_id) == 32
        int(report.report_id, 16)
        # ``created_at`` is auto-generated and tz-aware UTC.
        assert report.created_at.tzinfo is not None
        assert report.created_at.utcoffset().total_seconds() == 0


class TestRecoveryReportValidation:
    """Required fields, ``extra='forbid'`` enforcement."""

    def _minimal_kwargs(self) -> dict:
        return {
            "scenario_id": "abc",
            "overall_success": True,
            "validation_duration": 12.4,
            "test_results": [],
            "summary": RecoverySummary(
                total_tests=0,
                passed_tests=0,
                failed_tests=0,
                timeout_tests=0,
            ),
        }

    def test_minimal_construction(self) -> None:
        r = RecoveryReport(**self._minimal_kwargs())
        assert r.scenario_id == "abc"
        assert len(r.report_id) == 32

    def test_extra_field_forbidden(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["unknown_key"] = "nope"
        with pytest.raises(ValidationError):
            RecoveryReport(**kwargs)

    def test_negative_validation_duration_raises(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["validation_duration"] = -0.1
        with pytest.raises(ValidationError):
            RecoveryReport(**kwargs)

    def test_empty_scenario_id_raises(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["scenario_id"] = ""
        with pytest.raises(ValidationError):
            RecoveryReport(**kwargs)

    def test_round_trip_preserves_nested_test_results(self) -> None:
        kwargs = self._minimal_kwargs()
        kwargs["test_results"] = [
            TestResultModel(
                name="HealthProbeTest",
                status=RecoveryTestStatus.COMPLETED,
                duration=1.2,
                details={"endpoint": "/health", "code": 200},
            ),
            TestResultModel(
                name="LatencyBaselineTest",
                status=RecoveryTestStatus.TIMEOUT,
                duration=30.0,
                error_message="probe timed out",
            ),
        ]
        kwargs["summary"] = RecoverySummary(
            total_tests=2,
            passed_tests=1,
            failed_tests=0,
            timeout_tests=1,
        )
        original = RecoveryReport(**kwargs)
        restored = RecoveryReport.model_validate_json(original.model_dump_json())
        assert restored == original
        assert restored.test_results[0].details == {"endpoint": "/health", "code": 200}
        assert restored.test_results[1].status == RecoveryTestStatus.TIMEOUT
