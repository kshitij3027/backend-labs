"""APScheduler wiring — build the scheduler, register the 3 lifecycle jobs.

This is the C12 glue layer that converts the lifecycle functions
(``scan_once`` / ``apply_once`` / ``sweep_once`` from C09/C10/C11) into
APScheduler jobs and records one ``JobRun`` row per tick so the
dashboard can show "last scan / apply / sweep" recency + outcome.

Design notes:

  * ``AsyncIOScheduler`` runs jobs inside the FastAPI event loop — no
    extra threads, no extra processes. That's the right shape for our
    single-worker uvicorn setup (the plan pins ``--workers 1`` precisely
    so APScheduler in-process is safe).
  * ``SQLAlchemyJobStore`` persists job state in SQLite under
    ``ap_jobs``. The store API is **synchronous** — it uses plain
    SQLAlchemy under the hood — so the caller must hand us a SYNC URL
    (``sqlite:///...``) NOT the project's async URL
    (``sqlite+aiosqlite:///...``). The conversion lives in ``main.py``;
    we expose ``database_sync_url`` here to make the contract obvious.
  * ``SQLAlchemyJobStore`` serializes job callables via ``pickle`` so the
    callables MUST be importable module-level functions (closures over
    ``CatalogRepo`` / ``async_sessionmaker`` won't pickle — the engine
    holds C-level file descriptors). We work around this with a
    process-global ``_REGISTRY`` populated at ``register_jobs`` time:
    APScheduler stores only the module-level function reference (which
    pickles fine); the function looks up the live state from the
    registry at call time.
  * ``coalesce=True`` + ``max_instances=1`` + ``misfire_grace_time=30``
    are the canonical safety knobs: if the scheduler falls behind, drop
    accumulated misfires (we'll catch up on the next tick anyway), never
    run two copies of the same job at once, and forgive ticks that
    arrive up to 30 s late.
  * Each tick wraps the work in ``_run_job_and_record`` so a JobRun row
    is written whether the job succeeds or raises. On exception we
    re-raise so APScheduler's own miss accounting kicks in too —
    swallowing exceptions silently would hide a broken job from operators.
"""
from __future__ import annotations

import dataclasses
import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.audit.chain import AuditAppender
from src.audit.verifier import ChainVerifier
from src.lifecycle.applier import apply_once
from src.lifecycle.scanner import scan_once
from src.lifecycle.sweeper import sweep_once
from src.persistence.models import JobRun
from src.policy.schema import PolicySet
from src.storage.catalog import CatalogRepo

logger = logging.getLogger(__name__)


# Process-global registry holding the live state each scheduled job needs
# at call time. Populated by ``register_jobs``; read by the module-level
# ``_scan_tick`` / ``_apply_tick`` / ``_sweep_tick`` functions APScheduler
# stores in its (pickleable) job rows.
#
# Why a global: ``SQLAlchemyJobStore`` pickles the job's callable. We can
# only register module-level functions (which pickle as
# ``"module:func_name"`` references). Those functions are stateless on
# their own — they fetch ``catalog_repo`` / ``policy_set`` etc. from this
# registry at call time. Under the single-process uvicorn-worker setup
# the plan mandates, there's exactly one live registry per Python
# interpreter — no cross-process coordination needed.
#
# Tests that spin up the app per-test simply overwrite the registry keys
# in their own ``register_jobs`` invocation; this is intentional.
_REGISTRY: dict[str, Any] = {}


def build_scheduler(database_sync_url: str) -> AsyncIOScheduler:
    """Construct the AsyncIOScheduler with a SQLAlchemy jobstore.

    ``database_sync_url`` must be a SYNC SQLAlchemy URL (e.g.
    ``sqlite:///data/retention.db``), NOT the project's async form
    (``sqlite+aiosqlite:///data/retention.db``). APScheduler's
    ``SQLAlchemyJobStore`` uses plain (blocking) SQLAlchemy under the
    hood — handing it the async URL would raise at first use because
    the aiosqlite dialect cannot be driven synchronously.

    Defaults applied to every job:

      * ``coalesce=True`` — if the scheduler is behind by N ticks, run
        the job exactly once on catch-up (don't fire N separate copies).
      * ``max_instances=1`` — never overlap two runs of the same job;
        the current run must finish before the next tick fires.
      * ``misfire_grace_time=30`` — forgive ticks that arrive up to 30 s
        late (typical pause-the-VM / suspend-resume scenarios).
      * ``replace_existing=True`` — re-running ``register_jobs`` after a
        crash recovery overwrites the stored job rather than raising
        ``ConflictingIdError``.
    """
    jobstore = SQLAlchemyJobStore(url=database_sync_url, tablename="ap_jobs")
    scheduler = AsyncIOScheduler(
        jobstores={"default": jobstore},
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 30,
            "replace_existing": True,
        },
    )
    return scheduler


def _utcnow_naive() -> datetime:
    """Return UTC ``datetime`` with no tzinfo — matches the schema convention.

    The ORM columns are declared as plain ``DateTime`` (naive). The app
    layer is responsible for ensuring every datetime that hits SQLite is
    in UTC — stripping ``tzinfo`` here gives SQLite a stable text
    representation across rows and lets ``<=`` / ``>=`` filters compare
    apples to apples.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _summary_to_json(result: Any) -> str:
    """Serialize a job's return value into the ``JobRun.summary_json`` string.

    The lifecycle jobs return dataclass instances (``ScanReport``,
    ``ApplyReport``, ``SweepReport``) — converting via ``asdict`` gives
    us a stable JSON dict per job kind. Non-dataclass returns (None,
    primitives, plain dicts) are best-effort serialized via ``str`` /
    ``json.dumps`` so a future job that returns something exotic still
    yields a string the dashboard can display.
    """
    if result is None:
        return json.dumps({})
    if dataclasses.is_dataclass(result):
        try:
            return json.dumps(dataclasses.asdict(result), default=str)
        except (TypeError, ValueError):
            return json.dumps({"repr": repr(result)})
    if isinstance(result, dict):
        try:
            return json.dumps(result, default=str)
        except (TypeError, ValueError):
            return json.dumps({"repr": repr(result)})
    return json.dumps({"value": str(result)})


async def _run_job_and_record(
    session_factory: async_sessionmaker[AsyncSession],
    job_name: str,
    coro_fn: Callable[[], Awaitable[Any]],
) -> Any:
    """Execute ``coro_fn`` and persist a ``JobRun`` row for the invocation.

    Lifecycle:

      1. Insert a ``JobRun`` row with ``status='running'`` and the
         current UTC start time. We commit before the work so a crash
         mid-run still leaves a forensic "started but never finished"
         row in the table.
      2. Run ``coro_fn()`` and capture its return value.
      3. Update the row with ``status='ok'``, the finish time, and a
         JSON summary derived from the return value.
      4. On exception: update the row with ``status='error'`` and a
         JSON ``{"error": ...}`` payload, log at exception level (full
         traceback in the container log), then re-raise so APScheduler's
         own missed-job / failed-job machinery sees the failure.

    Returns the coroutine's value on success so callers can chain results
    if they want — the scheduler-registered jobs ignore the return,
    but tests that call this helper directly find it useful.
    """
    started = _utcnow_naive()

    # Insert the "running" sentinel up front.
    async with session_factory() as session:
        row = JobRun(
            job_name=job_name,
            started_at=started,
            finished_at=None,
            status="running",
            summary_json=None,
        )
        session.add(row)
        await session.commit()
        await session.refresh(row)
        row_id = row.id

    try:
        result = await coro_fn()
    except Exception as exc:
        finished = _utcnow_naive()
        logger.exception("job %s failed", job_name)
        try:
            async with session_factory() as session:
                err_row = await session.get(JobRun, row_id)
                if err_row is not None:
                    err_row.finished_at = finished
                    err_row.status = "error"
                    err_row.summary_json = json.dumps({"error": str(exc)})
                    await session.commit()
        except Exception:
            # If the failure-row write itself fails, log and let the
            # original exception propagate — APScheduler still records
            # the miss, and the operator gets two log lines instead of one.
            logger.exception(
                "job %s: failed to write error row id=%s", job_name, row_id
            )
        raise

    finished = _utcnow_naive()
    async with session_factory() as session:
        ok_row = await session.get(JobRun, row_id)
        if ok_row is not None:
            ok_row.finished_at = finished
            ok_row.status = "ok"
            ok_row.summary_json = _summary_to_json(result)
            await session.commit()
    return result


# ---------------------------------------------------------------------------
# Module-level tick functions
# ---------------------------------------------------------------------------
#
# These three functions are what APScheduler stores in its (pickled) job
# rows. They have no captured state — they look up everything they need
# from the module-global ``_REGISTRY`` at call time.
#
# Splitting registry lookup from the actual work keeps the pickling
# constraint loosely coupled to the lifecycle code: the lifecycle
# functions (``scan_once`` etc.) stay pure-async with explicit args;
# only this thin wrapping layer cares about APScheduler's serialization
# rules.


async def _scan_tick() -> Any:
    """APScheduler entry point for the ``scan_job``.

    Reads ``catalog_repo`` / ``policy_set`` / ``session_factory`` from
    the registry. The actual work is in ``scan_once``; this wrapper just
    plumbs the registry lookup and routes the call through
    ``_run_job_and_record`` so a JobRun row is written.
    """
    catalog_repo = _REGISTRY["catalog_repo"]
    policy_set = _REGISTRY["policy_set"]
    session_factory = _REGISTRY["session_factory"]

    async def _do() -> Any:
        return await scan_once(catalog_repo, policy_set, _utcnow_naive())

    return await _run_job_and_record(session_factory, "scan_job", _do)


async def _apply_tick() -> Any:
    """APScheduler entry point for the ``apply_job``.

    Passes the registry-held :class:`AuditAppender` (may be ``None`` in
    tests that don't wire one) through to :func:`apply_once` so each
    successful transition emits one audit-chain entry.
    """
    catalog_repo = _REGISTRY["catalog_repo"]
    storage_root = _REGISTRY["storage_root"]
    delete_delay_hours = _REGISTRY["delete_delay_hours"]
    session_factory = _REGISTRY["session_factory"]
    audit_appender = _REGISTRY.get("audit_appender")

    async def _do() -> Any:
        return await apply_once(
            catalog_repo,
            storage_root,
            _utcnow_naive(),
            delete_delay_hours=delete_delay_hours,
            audit_appender=audit_appender,
        )

    return await _run_job_and_record(session_factory, "apply_job", _do)


async def _sweep_tick() -> Any:
    """APScheduler entry point for the ``sweep_job``.

    Passes the registry-held :class:`AuditAppender` (may be ``None``)
    through to :func:`sweep_once` so each hard delete emits one
    audit-chain entry.
    """
    catalog_repo = _REGISTRY["catalog_repo"]
    session_factory = _REGISTRY["session_factory"]
    audit_appender = _REGISTRY.get("audit_appender")

    async def _do() -> Any:
        return await sweep_once(
            catalog_repo,
            _utcnow_naive(),
            audit_appender=audit_appender,
        )

    return await _run_job_and_record(session_factory, "sweep_job", _do)


async def _verify_chain_tick() -> Any:
    """APScheduler entry point for the nightly ``verify_chain_job``.

    Builds a :class:`ChainVerifier` from the registry-held
    ``session_factory`` and runs a full chain walk. The
    :class:`VerifyResult` is returned so ``_run_job_and_record``
    serialises it into the ``JobRun.summary_json`` field — that's how
    the dashboard / future alerting will see the most recent integrity
    state.

    The verifier is constructed per-tick (cheap; just holds the session
    factory) rather than registry-cached, so a future hot-reload of the
    session factory is naturally picked up.
    """
    session_factory = _REGISTRY["session_factory"]
    verifier = ChainVerifier(session_factory)

    async def _do() -> Any:
        return await verifier.verify_full()

    return await _run_job_and_record(session_factory, "verify_chain_job", _do)


def register_jobs(
    scheduler: AsyncIOScheduler,
    *,
    catalog_repo: CatalogRepo,
    policy_set: PolicySet,
    storage_root: Any,
    session_factory: async_sessionmaker[AsyncSession],
    scan_interval_sec: int,
    apply_interval_sec: int,
    sweep_interval_sec: int,
    delete_delay_hours: int,
    audit_appender: AuditAppender | None = None,
) -> None:
    """Register the 4 scheduler jobs on ``scheduler``.

    Three lifecycle jobs run on an interval trigger; the fourth
    (``verify_chain_job``) runs nightly via a cron trigger.

    Each job is a module-level coroutine (``_scan_tick`` / ``_apply_tick``
    / ``_sweep_tick`` / ``_verify_chain_tick``) — APScheduler can pickle
    the bare function reference, and the function looks up live state
    from the module ``_REGISTRY`` at call time. See the module docstring
    for why the closure / lambda approach doesn't work with
    ``SQLAlchemyJobStore``.

    Intervals come from settings (defaults: 60 s each) — production-grade
    intervals would be measured in minutes, but for the demo / E2E tests
    we want fast feedback so the defaults stay low.

    The optional ``audit_appender`` is stored in the registry so
    ``_apply_tick`` / ``_sweep_tick`` can emit chain entries per
    successful action. When omitted (existing tests, early-boot
    scenarios) the lifecycle jobs simply skip the emit step.

    The nightly ``verify_chain_job`` runs at 03:00 UTC daily — picked
    to land outside normal ingest activity. It writes a ``JobRun`` row
    whose ``summary_json`` carries the :class:`VerifyResult` so the
    dashboard / future alerting can surface a broken chain promptly.
    """
    # Populate the registry FIRST so a tick that fires immediately after
    # ``scheduler.start()`` (e.g. ``next_run_time=now``) finds its state.
    _REGISTRY["catalog_repo"] = catalog_repo
    _REGISTRY["policy_set"] = policy_set
    _REGISTRY["storage_root"] = storage_root
    _REGISTRY["session_factory"] = session_factory
    _REGISTRY["delete_delay_hours"] = delete_delay_hours
    _REGISTRY["audit_appender"] = audit_appender

    scheduler.add_job(
        _scan_tick,
        IntervalTrigger(seconds=scan_interval_sec),
        id="scan_job",
        replace_existing=True,
    )
    scheduler.add_job(
        _apply_tick,
        IntervalTrigger(seconds=apply_interval_sec),
        id="apply_job",
        replace_existing=True,
    )
    scheduler.add_job(
        _sweep_tick,
        IntervalTrigger(seconds=sweep_interval_sec),
        id="sweep_job",
        replace_existing=True,
    )
    scheduler.add_job(
        _verify_chain_tick,
        CronTrigger(hour=3, minute=0),
        id="verify_chain_job",
        replace_existing=True,
    )
    logger.info(
        "registered scheduler jobs: scan=%ss apply=%ss sweep=%ss verify=cron(03:00 UTC)",
        scan_interval_sec,
        apply_interval_sec,
        sweep_interval_sec,
    )
