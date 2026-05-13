"""Unit tests for C12 --- async SQLAlchemy persistence layer.

Exercises the three repos exposed by ``src.persistence``:

* :class:`ExperimentDefinitionRepo` --- CRUD + filtered list_all.
* :class:`ExperimentRunRepo` --- CRUD + update_status + upsert + list_by_experiment.
* :class:`RecoveryReportRepo` --- CRUD + list_by_scenario.

In addition to the per-repo behaviour, two FK / cascade scenarios are
verified end-to-end: definition deletion cascades into runs, and report
deletion nulls the back-reference on a run that pointed at it.

Each test gets a fresh in-memory SQLite database. The fixture also enables
``PRAGMA foreign_keys = ON`` so SQLAlchemy's CASCADE / SET NULL declarations
are enforced (aiosqlite + SQLite would otherwise treat them as inert).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine

from src.models.experiments import (
    ExperimentDefinition,
    ExperimentRun,
    Hypothesis,
    RunStatus,
)
from src.models.metrics import ServiceHealth, SystemMetrics
from src.models.scenarios import FailureType
from src.models.validation import (
    RecoveryReport,
    RecoverySummary,
    RecoveryTestStatus,
)
# Imported under an alias so pytest doesn't try to collect the Pydantic
# ``TestResult`` model as a test class (it has its own ``__init__``).
from src.models.validation import TestResult as _TestResult
from src.persistence import (
    ExperimentDefinitionRepo,
    ExperimentRunRepo,
    RecoveryReportRepo,
    create_all_tables,
    make_engine,
    make_sessionmaker,
)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def _install_sqlite_fk_pragma(engine: AsyncEngine) -> None:
    """Force ``PRAGMA foreign_keys = ON`` on every new aiosqlite connection.

    SQLite defaults to off; without this, CASCADE / SET NULL declarations
    on the schema are silently ignored. The SQLAlchemy recipe is to hook
    ``connect`` and execute the pragma synchronously. ``engine.sync_engine``
    is the underlying sync engine the async one wraps, which is where the
    event API lives.
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragma(dbapi_conn, _connection_record):  # noqa: ANN001 - SA signature
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.close()


@pytest_asyncio.fixture
async def sessionmaker():
    """Function-scoped fixture: fresh in-memory DB per test, with FK pragma on."""
    engine = make_engine("sqlite+aiosqlite:///:memory:")
    _install_sqlite_fk_pragma(engine)
    await create_all_tables(engine)

    # Sanity check the pragma actually applied; if SQLite reports 0 we know
    # the FK-dependent tests will mis-pass / mis-fail.
    async with engine.connect() as conn:
        result = await conn.execute(text("PRAGMA foreign_keys"))
        fk_setting = result.scalar()
    assert fk_setting == 1, "FK pragma not honoured by SQLite"

    sm = make_sessionmaker(engine)
    try:
        yield sm
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------- #
# Builders
# --------------------------------------------------------------------------- #


def make_definition(
    *,
    id_: str | None = None,
    name: str = "latency-test",
    failure_type: FailureType = FailureType.LATENCY_INJECTION,
    target: str = "log-consumer",
    parameters: dict | None = None,
    duration: int = 60,
    severity: int = 2,
    hypothesis: Hypothesis | None = None,
    created_at: datetime | None = None,
) -> ExperimentDefinition:
    """Build an ExperimentDefinition with reproducible defaults."""
    kwargs: dict = {
        "name": name,
        "type": failure_type,
        "target": target,
        "parameters": parameters if parameters is not None else {"latency_ms": 200},
        "duration": duration,
        "severity": severity,
        "hypothesis": hypothesis,
    }
    if id_ is not None:
        kwargs["id"] = id_
    if created_at is not None:
        kwargs["created_at"] = created_at
    return ExperimentDefinition(**kwargs)


def make_metrics(
    *,
    cpu_pct: float = 12.5,
    mem_pct: float = 33.4,
    disk_pct: float = 55.6,
    network_latency_ms: float | None = 4.2,
    timestamp: datetime | None = None,
) -> SystemMetrics:
    """Build a SystemMetrics snapshot populated in every field."""
    return SystemMetrics(
        timestamp=timestamp or datetime.now(timezone.utc),
        cpu_pct=cpu_pct,
        mem_pct=mem_pct,
        disk_pct=disk_pct,
        network_latency_ms=network_latency_ms,
        service_health=[
            ServiceHealth(
                name="log-consumer",
                is_healthy=True,
                last_check_at=datetime.now(timezone.utc),
                latency_ms=5.0,
            )
        ],
        container_stats={"log-consumer": {"cpu_pct": 7.5, "mem_pct": 3.1}},
    )


def make_run(
    *,
    run_id: str | None = None,
    experiment_id: str,
    status: RunStatus = RunStatus.PENDING,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    baseline_metrics: SystemMetrics | None = None,
    post_metrics: SystemMetrics | None = None,
    scenario_id: str | None = None,
    recovery_report_id: str | None = None,
    error_message: str | None = None,
) -> ExperimentRun:
    """Build an ExperimentRun with controllable fields."""
    kwargs: dict = {
        "experiment_id": experiment_id,
        "status": status,
        "started_at": started_at,
        "ended_at": ended_at,
        "baseline_metrics": baseline_metrics,
        "post_metrics": post_metrics,
        "scenario_id": scenario_id,
        "recovery_report_id": recovery_report_id,
        "error_message": error_message,
    }
    if run_id is not None:
        kwargs["run_id"] = run_id
    return ExperimentRun(**kwargs)


def make_report(
    *,
    report_id: str | None = None,
    scenario_id: str = "scen-abc",
    overall_success: bool = True,
    created_at: datetime | None = None,
) -> RecoveryReport:
    """Build a RecoveryReport with one populated TestResult + summary."""
    test_results = [
        _TestResult(
            name="HealthProbeTest",
            status=RecoveryTestStatus.COMPLETED,
            duration=1.2,
            details={"endpoint": "/health", "code": 200},
            error_message=None,
        ),
        _TestResult(
            name="LatencyBaselineTest",
            status=RecoveryTestStatus.FAILED,
            duration=2.1,
            details={"observed_ms": 142.0, "baseline_ms": 110.0},
            error_message="latency above baseline",
        ),
    ]
    summary = RecoverySummary(
        total_tests=2, passed_tests=1, failed_tests=1, timeout_tests=0
    )
    kwargs: dict = {
        "scenario_id": scenario_id,
        "overall_success": overall_success,
        "validation_duration": 3.3,
        "test_results": test_results,
        "summary": summary,
    }
    if report_id is not None:
        kwargs["report_id"] = report_id
    if created_at is not None:
        kwargs["created_at"] = created_at
    return RecoveryReport(**kwargs)


# --------------------------------------------------------------------------- #
# ExperimentDefinitionRepo
# --------------------------------------------------------------------------- #


class TestExperimentDefinitionRepo:
    """Coverage for the definition repo's CRUD + filter surface."""

    @pytest.mark.asyncio
    async def test_create_then_get_roundtrip(self, sessionmaker):
        hypothesis = Hypothesis(
            statement="If 200ms latency on log-consumer then RTT < 30s",
            recovery_time_budget_s=30,
            expected_invariants=["no data loss"],
        )
        defn = make_definition(
            name="round-trip",
            parameters={"latency_ms": 200, "jitter_ms": 25},
            hypothesis=hypothesis,
        )

        async with sessionmaker() as session:
            repo = ExperimentDefinitionRepo(session)
            await repo.create(defn)
            fetched = await repo.get(defn.id)

        assert fetched is not None
        assert fetched.id == defn.id
        assert fetched.name == "round-trip"
        # Type round-trips as string form of the enum.
        assert fetched.type == FailureType.LATENCY_INJECTION
        assert fetched.type.value == "latency_injection"
        assert fetched.parameters == {"latency_ms": 200, "jitter_ms": 25}
        assert fetched.hypothesis is not None
        assert fetched.hypothesis.statement == hypothesis.statement
        # SQLite strips tzinfo on read; compare the naive instant instead.
        # Production backends (Postgres etc.) preserve tzinfo, so this assertion
        # is intentionally backend-permissive: equal-when-naive-compared.
        assert fetched.created_at.replace(tzinfo=None) == defn.created_at.replace(
            tzinfo=None
        )

    @pytest.mark.asyncio
    async def test_create_then_get_preserves_none_hypothesis(self, sessionmaker):
        defn = make_definition(name="no-hyp", hypothesis=None)
        async with sessionmaker() as session:
            repo = ExperimentDefinitionRepo(session)
            await repo.create(defn)
            fetched = await repo.get(defn.id)
        assert fetched is not None
        assert fetched.hypothesis is None

    @pytest.mark.asyncio
    async def test_get_unknown_id_returns_none(self, sessionmaker):
        async with sessionmaker() as session:
            repo = ExperimentDefinitionRepo(session)
            assert await repo.get("does-not-exist") is None

    @pytest.mark.asyncio
    async def test_list_all_returns_newest_first(self, sessionmaker):
        base = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        defn_old = make_definition(name="d-old", created_at=base)
        defn_mid = make_definition(name="d-mid", created_at=base + timedelta(seconds=10))
        defn_new = make_definition(name="d-new", created_at=base + timedelta(seconds=20))

        async with sessionmaker() as session:
            repo = ExperimentDefinitionRepo(session)
            # Insert deliberately in an unsorted order.
            await repo.create(defn_mid)
            await repo.create(defn_old)
            await repo.create(defn_new)
            rows = await repo.list_all()

        names = [d.name for d in rows]
        assert names == ["d-new", "d-mid", "d-old"]

    @pytest.mark.asyncio
    async def test_list_all_filters_by_target(self, sessionmaker):
        d1 = make_definition(name="lc", target="log-consumer")
        d2 = make_definition(name="lp", target="log-producer")
        d3 = make_definition(name="lc2", target="log-consumer")
        async with sessionmaker() as session:
            repo = ExperimentDefinitionRepo(session)
            for d in (d1, d2, d3):
                await repo.create(d)
            consumer_rows = await repo.list_all(target="log-consumer")
            producer_rows = await repo.list_all(target="log-producer")

        assert {r.name for r in consumer_rows} == {"lc", "lc2"}
        assert {r.name for r in producer_rows} == {"lp"}

    @pytest.mark.asyncio
    async def test_list_all_filters_by_type_using_enum_value(self, sessionmaker):
        d_latency = make_definition(
            name="lat", failure_type=FailureType.LATENCY_INJECTION
        )
        d_packet = make_definition(
            name="pkt", failure_type=FailureType.PACKET_LOSS
        )
        d_partition = make_definition(
            name="part", failure_type=FailureType.NETWORK_PARTITION
        )
        async with sessionmaker() as session:
            repo = ExperimentDefinitionRepo(session)
            for d in (d_latency, d_packet, d_partition):
                await repo.create(d)
            latency_rows = await repo.list_all(
                type_=FailureType.LATENCY_INJECTION.value
            )
            packet_rows = await repo.list_all(type_="packet_loss")

        assert {r.name for r in latency_rows} == {"lat"}
        assert {r.name for r in packet_rows} == {"pkt"}

    @pytest.mark.asyncio
    async def test_list_all_respects_limit(self, sessionmaker):
        base = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        async with sessionmaker() as session:
            repo = ExperimentDefinitionRepo(session)
            for i in range(5):
                await repo.create(
                    make_definition(
                        name=f"d-{i}",
                        created_at=base + timedelta(seconds=i),
                    )
                )
            rows = await repo.list_all(limit=2)

        assert len(rows) == 2
        # Newest-first -> the two highest indices.
        assert [r.name for r in rows] == ["d-4", "d-3"]

    @pytest.mark.asyncio
    async def test_delete_returns_true_then_get_returns_none(self, sessionmaker):
        defn = make_definition(name="to-delete")
        async with sessionmaker() as session:
            repo = ExperimentDefinitionRepo(session)
            await repo.create(defn)
            assert await repo.delete(defn.id) is True
            assert await repo.get(defn.id) is None

    @pytest.mark.asyncio
    async def test_delete_unknown_id_returns_false(self, sessionmaker):
        async with sessionmaker() as session:
            repo = ExperimentDefinitionRepo(session)
            assert await repo.delete("nope") is False


# --------------------------------------------------------------------------- #
# ExperimentRunRepo
# --------------------------------------------------------------------------- #


class TestExperimentRunRepo:
    """Coverage for the run repo's CRUD + status + upsert + listing surface."""

    @pytest.mark.asyncio
    async def test_create_then_get_status_roundtrips_as_string(self, sessionmaker):
        defn = make_definition(name="run-rt")
        run = make_run(experiment_id=defn.id, status=RunStatus.RUNNING)

        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)
            run_repo = ExperimentRunRepo(session)
            await run_repo.create(run)
            fetched = await run_repo.get(run.run_id)

        assert fetched is not None
        assert fetched.run_id == run.run_id
        # Pydantic re-coerces the stored string back into the enum.
        assert fetched.status == RunStatus.RUNNING
        assert fetched.status.value == "running"

    @pytest.mark.asyncio
    async def test_create_with_metrics_and_error_message_roundtrip(self, sessionmaker):
        defn = make_definition(name="full-payload")
        baseline = make_metrics(cpu_pct=10.0, mem_pct=20.0)
        post = make_metrics(cpu_pct=15.0, mem_pct=25.0)
        run = make_run(
            experiment_id=defn.id,
            status=RunStatus.COMPLETED,
            started_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
            ended_at=datetime(2026, 5, 1, 0, 5, tzinfo=timezone.utc),
            baseline_metrics=baseline,
            post_metrics=post,
            scenario_id="scen-xyz",
            error_message=None,
        )

        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)
            run_repo = ExperimentRunRepo(session)
            await run_repo.create(run)
            fetched = await run_repo.get(run.run_id)

        assert fetched is not None
        assert fetched.baseline_metrics is not None
        assert fetched.post_metrics is not None
        # Compare via model_dump(mode="json") since timestamps/floats survive
        # through JSON round-trip without loss.
        assert (
            fetched.baseline_metrics.model_dump(mode="json")
            == baseline.model_dump(mode="json")
        )
        assert (
            fetched.post_metrics.model_dump(mode="json")
            == post.model_dump(mode="json")
        )
        assert fetched.scenario_id == "scen-xyz"

    @pytest.mark.asyncio
    async def test_update_status_marks_failed_with_error_message(self, sessionmaker):
        defn = make_definition(name="upd")
        run = make_run(experiment_id=defn.id, status=RunStatus.RUNNING)

        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)
            run_repo = ExperimentRunRepo(session)
            await run_repo.create(run)
            ok = await run_repo.update_status(run.run_id, RunStatus.FAILED, "boom")
            assert ok is True
            fetched = await run_repo.get(run.run_id)

        assert fetched is not None
        assert fetched.status == RunStatus.FAILED
        assert fetched.status.value == "failed"
        assert fetched.error_message == "boom"

    @pytest.mark.asyncio
    async def test_update_status_unknown_run_returns_false(self, sessionmaker):
        async with sessionmaker() as session:
            run_repo = ExperimentRunRepo(session)
            ok = await run_repo.update_status("no-such-run", RunStatus.FAILED, "nope")
            assert ok is False

    @pytest.mark.asyncio
    async def test_upsert_inserts_then_updates_in_place(self, sessionmaker):
        defn = make_definition(name="ups")
        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)
            run_repo = ExperimentRunRepo(session)

            first = make_run(
                run_id="stable-id",
                experiment_id=defn.id,
                status=RunStatus.RUNNING,
                error_message=None,
            )
            await run_repo.upsert(first)

            second = make_run(
                run_id="stable-id",
                experiment_id=defn.id,
                status=RunStatus.COMPLETED,
                ended_at=datetime(2026, 5, 1, 1, 0, tzinfo=timezone.utc),
                error_message="finalised",
            )
            await run_repo.upsert(second)

            rows = await run_repo.list_by_experiment(defn.id)

        assert len(rows) == 1
        only = rows[0]
        assert only.run_id == "stable-id"
        assert only.status == RunStatus.COMPLETED
        assert only.error_message == "finalised"
        # SQLite strips tzinfo; compare naive forms (Postgres would keep it).
        assert only.ended_at is not None
        assert only.ended_at.replace(tzinfo=None) == datetime(2026, 5, 1, 1, 0)

    @pytest.mark.asyncio
    async def test_list_by_experiment_newest_first_and_capped(self, sessionmaker):
        defn = make_definition(name="list-by-exp")
        other_defn = make_definition(name="other-exp")
        base = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)
            await ExperimentDefinitionRepo(session).create(other_defn)
            run_repo = ExperimentRunRepo(session)

            for i in range(4):
                await run_repo.create(
                    make_run(
                        run_id=f"r-{i}",
                        experiment_id=defn.id,
                        status=RunStatus.COMPLETED,
                        started_at=base + timedelta(seconds=i),
                    )
                )
            # A run on the other experiment should not appear.
            await run_repo.create(
                make_run(
                    run_id="other-1",
                    experiment_id=other_defn.id,
                    status=RunStatus.COMPLETED,
                    started_at=base + timedelta(seconds=100),
                )
            )

            rows = await run_repo.list_by_experiment(defn.id, limit=3)

        assert [r.run_id for r in rows] == ["r-3", "r-2", "r-1"]
        assert all(r.experiment_id == defn.id for r in rows)


# --------------------------------------------------------------------------- #
# RecoveryReportRepo
# --------------------------------------------------------------------------- #


class TestRecoveryReportRepo:
    """Coverage for the report repo's CRUD + scenario listing surface."""

    @pytest.mark.asyncio
    async def test_create_then_get_preserves_test_results_and_summary(
        self, sessionmaker
    ):
        report = make_report(scenario_id="scen-abc")

        async with sessionmaker() as session:
            repo = RecoveryReportRepo(session)
            await repo.create(report)
            fetched = await repo.get(report.report_id)

        assert fetched is not None
        assert fetched.report_id == report.report_id
        assert fetched.scenario_id == "scen-abc"
        assert fetched.overall_success is True
        assert fetched.summary.total_tests == 2

        # JSON-form comparison guards against silent shape drift through the
        # JSON column (status values are enum-string on both sides).
        assert (
            [tr.model_dump(mode="json") for tr in fetched.test_results]
            == [tr.model_dump(mode="json") for tr in report.test_results]
        )
        # And the detail dicts survive intact on individual TestResults.
        assert fetched.test_results[0].status == RecoveryTestStatus.COMPLETED
        assert fetched.test_results[0].details == {
            "endpoint": "/health",
            "code": 200,
        }
        assert fetched.test_results[1].status == RecoveryTestStatus.FAILED
        assert fetched.test_results[1].error_message == "latency above baseline"

    @pytest.mark.asyncio
    async def test_list_by_scenario_newest_first(self, sessionmaker):
        base = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        r_old = make_report(scenario_id="scen-1", created_at=base)
        r_mid = make_report(
            scenario_id="scen-1", created_at=base + timedelta(seconds=10)
        )
        r_new = make_report(
            scenario_id="scen-1", created_at=base + timedelta(seconds=20)
        )
        r_other = make_report(scenario_id="scen-2", created_at=base)

        async with sessionmaker() as session:
            repo = RecoveryReportRepo(session)
            # Insert out-of-order, including an unrelated scenario.
            await repo.create(r_mid)
            await repo.create(r_other)
            await repo.create(r_old)
            await repo.create(r_new)

            rows = await repo.list_by_scenario("scen-1")

        assert [r.report_id for r in rows] == [
            r_new.report_id,
            r_mid.report_id,
            r_old.report_id,
        ]


# --------------------------------------------------------------------------- #
# FK / cascade behaviour
# --------------------------------------------------------------------------- #


class TestForeignKeyBehaviour:
    """Verify the on-delete behaviour declared on the schema."""

    @pytest.mark.asyncio
    async def test_delete_definition_cascades_to_runs(self, sessionmaker):
        defn = make_definition(name="cascade")
        run = make_run(
            run_id="run-cas",
            experiment_id=defn.id,
            status=RunStatus.RUNNING,
        )

        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)
            await ExperimentRunRepo(session).create(run)

        # Sanity: the run is there before we delete the parent.
        async with sessionmaker() as session:
            assert (
                await ExperimentRunRepo(session).get(run.run_id) is not None
            )

        # Delete the parent definition.
        async with sessionmaker() as session:
            ok = await ExperimentDefinitionRepo(session).delete(defn.id)
            assert ok is True

        # The run should be gone too (CASCADE).
        async with sessionmaker() as session:
            assert await ExperimentRunRepo(session).get(run.run_id) is None

    @pytest.mark.asyncio
    async def test_delete_report_sets_recovery_report_id_null_on_run(
        self, sessionmaker
    ):
        defn = make_definition(name="set-null")
        report = make_report(scenario_id="scen-cas", report_id="rep-cas")
        run = make_run(
            run_id="run-cas2",
            experiment_id=defn.id,
            status=RunStatus.COMPLETED,
            recovery_report_id=report.report_id,
        )

        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)
            await RecoveryReportRepo(session).create(report)
            await ExperimentRunRepo(session).create(run)

        # Confirm the back-reference is wired up.
        async with sessionmaker() as session:
            fetched = await ExperimentRunRepo(session).get(run.run_id)
            assert fetched is not None
            assert fetched.recovery_report_id == report.report_id

        # Now delete the report. The run survives with the FK null'd.
        async with sessionmaker() as session:
            row = await session.get(
                __import__(
                    "src.persistence.schema",
                    fromlist=["RecoveryReportRow"],
                ).RecoveryReportRow,
                report.report_id,
            )
            assert row is not None
            await session.delete(row)
            await session.commit()

        async with sessionmaker() as session:
            fetched = await ExperimentRunRepo(session).get(run.run_id)

        assert fetched is not None, "run should survive report deletion"
        assert fetched.recovery_report_id is None
