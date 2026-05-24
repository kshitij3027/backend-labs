"""FastAPI app entry point for the automated-log-retention service.

C12 wires the full pipeline into the lifespan:

  1. DB engine + session factory + ``init_db``.
  2. Filesystem tier directories (idempotent ``mkdir -p``).
  3. Policy YAML load + compliance validation (fails the boot if a
     tagged policy violates its framework's minimum retention).
  4. Catalog repo over the session factory.
  5. APScheduler (``AsyncIOScheduler`` + ``SQLAlchemyJobStore``) with
     the three lifecycle jobs (scan / apply / sweep) registered.
  6. Ingest buffer (per-source open segment dict) + asyncio.Lock.
  7. All of the above attached to ``app.state.*`` so HTTP routes can
     reach them via ``request.app.state`` without module-level globals.

The router with ``/api/health``, ``/v1/logs/ingest``, ``/v1/files``,
``/v1/evaluate`` lives in ``src/api/routes.py`` and is mounted below.
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.api.routes import router as api_router
from src.audit.chain import AuditAppender
from src.audit.verifier import ChainVerifier
from src.lifecycle import applier as _applier  # noqa: F401  (kept for tests/imports)
from src.lifecycle import scanner as _scanner  # noqa: F401
from src.lifecycle import sweeper as _sweeper  # noqa: F401
from src.persistence.db import init_db, make_engine, make_session_factory
from src.policy.loader import load_policy_set
from src.scheduler.runner import build_scheduler, register_jobs
from src.settings import get_settings
from src.storage.catalog import CatalogRepo
from src.storage.tiers import ensure_tier_dirs


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Wire the full retention pipeline on startup; tear down on shutdown.

    Ordering matters:

      * DB engine + tables must exist before the policy loader runs
        (the loader doesn't touch the DB, but the catalog repo we build
        immediately after does).
      * Tier dirs must exist before the scheduler starts — the very
        first ``apply_once`` tick would otherwise crash on a missing
        directory.
      * The scheduler is started LAST so it cannot fire a tick against
        half-wired state.

    On shutdown we (a) flush any open ingest segments so we don't drop
    pending log lines, (b) ``shutdown(wait=True)`` the scheduler so
    in-flight jobs finish cleanly, and (c) ``engine.dispose()`` to
    release SQLite connections.
    """
    settings = get_settings()
    logging.basicConfig(level=settings.log_level.upper())

    # 1. Persistence
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    await init_db(engine)

    # 2. Filesystem tiers
    ensure_tier_dirs(settings.storage_root)

    # 3. Policy + compliance validation
    policy_set = load_policy_set(settings.policy_config_path)
    logger.info(
        "loaded %d retention policies from %s",
        len(policy_set.policies),
        settings.policy_config_path,
    )

    # 4. Catalog
    catalog_repo = CatalogRepo(session_factory)

    # 4b. Audit chain — appender drives writes (per applied transition /
    #     per sweep), verifier drives reads (nightly cron + future
    #     dashboard partial). Both share the same session_factory; the
    #     genesis row was already inserted by init_db so append() can
    #     run immediately.
    audit_appender = AuditAppender(session_factory)
    chain_verifier = ChainVerifier(session_factory)

    # 5. Scheduler — needs a SYNC URL for its SQLAlchemy jobstore. The
    #    project's ``database_url`` defaults to ``sqlite+aiosqlite:///...``
    #    so we strip the ``+aiosqlite`` driver prefix to get the plain
    #    ``sqlite:///...`` URL APScheduler's SQLAlchemyJobStore expects.
    sync_url = settings.database_url.replace("+aiosqlite", "")
    scheduler = build_scheduler(sync_url)
    register_jobs(
        scheduler,
        catalog_repo=catalog_repo,
        policy_set=policy_set,
        storage_root=settings.storage_root,
        session_factory=session_factory,
        scan_interval_sec=settings.scan_interval_sec,
        apply_interval_sec=settings.apply_interval_sec,
        sweep_interval_sec=settings.sweep_interval_sec,
        delete_delay_hours=settings.delete_delay_hours,
        audit_appender=audit_appender,
    )
    scheduler.start()

    # 6. Attach state for routes
    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.catalog_repo = catalog_repo
    app.state.policy_set = policy_set
    app.state.scheduler = scheduler
    app.state.audit_appender = audit_appender
    app.state.chain_verifier = chain_verifier
    app.state.ingest_buffer = {}
    app.state.ingest_lock = asyncio.Lock()
    app.state.startup_time = int(time.time())

    logger.info(
        "automated-log-retention: startup complete; scheduler running with %d jobs",
        len(scheduler.get_jobs()),
    )

    try:
        yield
    finally:
        # Flush any open segments so we don't drop pending log lines on
        # a clean shutdown. We do NOT register them with the catalog
        # here (we just close the handles) — the next boot will start
        # a new segment per source, which is intentional: the
        # half-written segment from the last process stays on disk but
        # is invisible to the scanner. A future enhancement could
        # discover orphan segments at startup and register them.
        for entry in app.state.ingest_buffer.values():
            try:
                entry["fh"].flush()
            except Exception:
                pass
            try:
                entry["fh"].close()
            except Exception:
                pass

        try:
            scheduler.shutdown(wait=True)
        except Exception:
            logger.exception("scheduler shutdown raised")

        await engine.dispose()
        logger.info("automated-log-retention: shutdown complete")


app = FastAPI(
    title="automated-log-retention",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(api_router)
