"""HTTP route definitions for the automated-log-retention service.

Routes wired in C12:

  * ``GET  /api/health``        — liveness probe.
  * ``POST /v1/logs/ingest``    — append a batch of records to the open
                                   hot-tier segment; roll segments at 5 MiB.
  * ``GET  /v1/files``          — paginated list of catalog rows
                                   (optional ``tier`` filter).
  * ``POST /v1/evaluate``       — synchronously run one scan+apply+sweep
                                   cycle (forces a buffer flush first so
                                   any just-ingested records are visible
                                   to the scanner).

State lives on ``request.app.state`` (no module-level globals beyond the
``APIRouter`` itself):

  * ``catalog_repo`` — async ``CatalogRepo`` for DB reads/writes.
  * ``policy_set``   — frozen ``PolicySet`` from policy YAML.
  * ``settings``     — typed Settings instance (storage_root, delete_delay_hours, ...).
  * ``ingest_buffer``— dict keyed by source name with the open segment's
                       file handle / path / size / timestamps.
  * ``ingest_lock``  — asyncio.Lock guarding buffer mutation. The route
                       grabs the lock for the duration of one POST so two
                       concurrent ingests cannot corrupt the buffer.

The ingest path performs synchronous filesystem I/O inside the route
because the segments are append-only JSONL and the target write rate
(>=1000 rps) is well within Python's blocking write throughput. A future
upgrade could move the actual ``write`` calls into a thread via
``asyncio.to_thread`` if benchmarks show the event loop is starved.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.api.models import (
    EvaluateResponse,
    FileSummary,
    FilesListResponse,
    HealthResponse,
    IngestRequest,
    IngestResponse,
)
from src.compliance.reports import render_report
from src.lifecycle.applier import apply_once
from src.lifecycle.scanner import scan_once
from src.lifecycle.sweeper import sweep_once
from src.storage.tiers import tier_dir

logger = logging.getLogger(__name__)

router = APIRouter()


# Module-level Jinja2Templates factory pointing at the project's
# ``templates/`` directory (Dockerfile COPYs that into ``/app/templates``
# so the same relative path resolves in container and host test runs).
# C16 introduces ``GET /``; C17/C18 add the partials that the dashboard
# polls via HTMX.
_TEMPLATES = Jinja2Templates(directory="templates")


# Roll the open hot-tier segment when it crosses this many bytes. The
# 5 MiB threshold matches the plan's spec; in practice that's ~5K-25K
# small records per segment, comfortably small for the scanner's batch
# and big enough to amortise catalog inserts.
_SEGMENT_ROLLOVER_BYTES = 5 * 1024 * 1024


def _utcnow_naive() -> datetime:
    """Naive UTC ``datetime`` — the project convention for DB columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# --- Health ---------------------------------------------------------------


@router.get("/api/health", response_model=HealthResponse, tags=["health"])
async def health() -> HealthResponse:
    """Liveness probe consumed by the Docker HEALTHCHECK.

    Stable ``{"status":"healthy","timestamp":<int>}`` shape preserved
    from C01 so existing tests / probes continue to pass.
    """
    return HealthResponse(status="healthy", timestamp=int(time.time()))


# --- Dashboard ------------------------------------------------------------


@router.get("/", response_class=HTMLResponse, tags=["dashboard"])
async def dashboard(request: Request) -> HTMLResponse:
    """Render the HTMX-polled dashboard shell.

    Returns the full ``dashboard.html`` page; the four placeholder cards
    inside (``#stats-card``, ``#tiers-card``, ``#policies-card``,
    ``#audit-card``) self-load their content via ``hx-get`` against
    ``/partials/...`` on initial render and re-poll every
    ``dashboard_refresh_ms`` milliseconds (5 s default).

    The partial routes themselves land in C17 (stats + tiers) and C18
    (policies + audit) — until then the cards will surface a 404 from
    the HTMX swap, but the page itself loads cleanly.
    """
    settings = request.app.state.settings
    return _TEMPLATES.TemplateResponse(
        "dashboard.html",
        {"request": request, "refresh_ms": settings.dashboard_refresh_ms},
    )


# --- Dashboard partials ---------------------------------------------------


@router.get("/partials/tiers", response_class=HTMLResponse, tags=["partials"])
async def partial_tiers(request: Request) -> HTMLResponse:
    """Render the ``#tiers-card`` HTMX partial.

    Returns a small HTML fragment listing every tier (hot/warm/cold/
    archive/pending) with its file count and total bytes. Driven by
    ``count_by_tier`` + ``total_bytes_by_tier`` which both pre-seed all
    five tier keys to 0, so the markup is stable on an empty DB (the
    template iterates the count dict; the bytes dict is keyed by the
    same tier names).
    """
    catalog_repo = request.app.state.catalog_repo
    tier_counts = await catalog_repo.count_by_tier()
    tier_bytes = await catalog_repo.total_bytes_by_tier()
    return _TEMPLATES.TemplateResponse(
        "_tiers_card.html",
        {"request": request, "tier_counts": tier_counts, "tier_bytes": tier_bytes},
    )


@router.get("/partials/stats", response_class=HTMLResponse, tags=["partials"])
async def partial_stats(request: Request) -> HTMLResponse:
    """Render the ``#stats-card`` HTMX partial.

    Surfaces six values:

      * ``total_files`` / ``total_bytes`` — sum across every tier (the
        per-tier breakdown lives in the sibling ``#tiers-card``).
      * ``pending_transitions`` — applier backpressure indicator.
      * ``last_scan_at`` / ``last_apply_at`` / ``last_sweep_at`` —
        wall-clock recency of each scheduler job, pulled from the
        ``job_runs`` table. ``None`` when the job hasn't fired yet;
        the template renders that as an em-dash.
    """
    catalog_repo = request.app.state.catalog_repo
    # Re-use the per-tier aggregates for the totals so the stats card and
    # tiers card stay numerically consistent (one DB round per metric).
    tier_counts = await catalog_repo.count_by_tier()
    tier_bytes = await catalog_repo.total_bytes_by_tier()
    total_files = sum(tier_counts.values())
    total_bytes = sum(tier_bytes.values())
    pending_transitions = await catalog_repo.count_pending_transitions()
    last_scan = await catalog_repo.last_job_run("scan_job")
    last_apply = await catalog_repo.last_job_run("apply_job")
    last_sweep = await catalog_repo.last_job_run("sweep_job")
    return _TEMPLATES.TemplateResponse(
        "_stats_card.html",
        {
            "request": request,
            "total_files": total_files,
            "total_bytes": total_bytes,
            "pending_transitions": pending_transitions,
            "last_scan_at": last_scan.finished_at if last_scan else None,
            "last_apply_at": last_apply.finished_at if last_apply else None,
            "last_sweep_at": last_sweep.finished_at if last_sweep else None,
        },
    )


# --- Ingest ---------------------------------------------------------------


async def _flush_buffer_entry(app: FastAPI, source: str) -> None:
    """Close the open segment for ``source`` and register it in the catalog.

    Called from two places:

      * the rollover branch of ``ingest`` when the open segment crosses
        ``_SEGMENT_ROLLOVER_BYTES`` — so the just-finished segment
        becomes visible to the scanner before the next batch starts a
        fresh one.
      * ``_flush_open_segments`` before ``POST /v1/evaluate`` — so any
        records ingested in the same test run are catalog-visible.

    The buffer entry is removed on success. Idempotent for missing
    sources (silent no-op) — keeps the evaluate helper simple.
    """
    buf = app.state.ingest_buffer
    entry = buf.get(source)
    if entry is None:
        return
    fh = entry["fh"]
    try:
        fh.flush()
    except Exception:
        # Best-effort flush; close still proceeds.
        logger.exception("ingest: flush failed for source=%s", source)
    try:
        fh.close()
    except Exception:
        logger.exception("ingest: close failed for source=%s", source)

    seg_path: Path = entry["path"]
    size_bytes: int = entry["size_bytes"]
    first_ts: datetime = entry["first_ts"]
    last_ts: datetime = entry["last_ts"]

    # Register the closed segment with the catalog so the scanner can
    # plan its lifecycle. ``next_eval_at`` defaults to "tomorrow" so the
    # scanner doesn't try to plan a transition the instant the file
    # exists — most policies have an after_days=0 promote phase that
    # would otherwise generate noise; a one-day delay lines up with the
    # typical compliance windows.
    now = _utcnow_naive()
    next_eval_at = now + timedelta(days=1)
    try:
        await app.state.catalog_repo.add_file(
            source=source,
            segment_path=str(seg_path),
            tier="hot",
            size_bytes=size_bytes,
            oldest_record_ts=first_ts,
            newest_record_ts=last_ts,
            next_eval_at=next_eval_at,
        )
    except Exception:
        # If the catalog insert fails (unique-violation on segment_path,
        # DB locked, etc.), the bytes are still on disk — log loudly so
        # an operator can manually reconcile. Don't crash the route.
        logger.exception(
            "ingest: catalog insert failed for segment_path=%s", seg_path
        )

    # Whether or not the insert succeeded, drop the entry so the next
    # ingest opens a fresh segment.
    buf.pop(source, None)


async def _flush_open_segments(app: FastAPI) -> None:
    """Flush + register every open segment in the ingest buffer.

    Used by ``POST /v1/evaluate`` to ensure any records ingested in this
    test/run are visible to the scanner. The lock guards the iteration
    so concurrent ingests don't reshape the dict mid-flush.
    """
    async with app.state.ingest_lock:
        # Snapshot the keys first — we mutate the dict inside the loop.
        for source in list(app.state.ingest_buffer.keys()):
            await _flush_buffer_entry(app, source)


def _open_new_segment(
    app: FastAPI, source: str, now: datetime
) -> dict:
    """Open a fresh ``hot/segment-<unix_ts>-<source>.jsonl`` and return the
    buffer entry the route will mutate on subsequent writes.

    Synchronous (no ``await``) — pure filesystem + dict construction. The
    caller is responsible for holding ``ingest_lock`` while invoking this
    so the buffer mutation stays consistent.
    """
    settings = app.state.settings
    hot_dir = tier_dir(settings.storage_root, "hot")
    hot_dir.mkdir(parents=True, exist_ok=True)

    # Filename: ``segment-<unix_ts>-<source>.jsonl``. Including the source
    # in the name disambiguates segments from different ingest streams
    # that roll within the same second.
    unix_ts = int(now.replace(tzinfo=timezone.utc).timestamp())
    # Conservatively scrub the source so we can't accidentally write
    # outside the hot dir via a malicious source string.
    safe_source = "".join(
        c if c.isalnum() or c in ("-", "_") else "_" for c in source
    )
    seg_path = hot_dir / f"segment-{unix_ts}-{safe_source}.jsonl"

    # Open in append+text mode; buffering=1 → line-buffered so each
    # written record is flushed to disk at newline (cheap durability).
    fh = open(seg_path, "a", encoding="utf-8", buffering=1)

    return {
        "fh": fh,
        "path": seg_path,
        "first_ts": None,
        "last_ts": None,
        "size_bytes": 0,
    }


@router.post("/v1/logs/ingest", response_model=IngestResponse, tags=["ingest"])
async def ingest(body: IngestRequest, request: Request) -> IngestResponse:
    """Append a batch of records to the open hot-tier segment.

    Steps:

      1. Group records by ``source`` (each source has its own rolling
         segment so a single ingest call may touch multiple segments).
      2. For each (source, group):
         * If no open segment, open one in ``tiers/hot/``.
         * If the existing segment crossed the 5 MiB rollover threshold,
           close it, register it in the catalog, and open a fresh one.
         * Write each record as one ``json.dumps(...) + "\\n"`` line;
           bump ``last_ts`` and ``size_bytes`` accordingly; initialize
           ``first_ts`` on first write.
      3. Return ``accepted`` and the segment path of the last touched
         buffer entry (for client-side debugging).

    The whole buffer mutation runs under ``app.state.ingest_lock`` so
    two concurrent ingests can't race on the open file handle.
    """
    app = request.app
    accepted = 0
    last_segment_path: str = ""

    # Pre-bucket by source so we touch each entry once per call.
    groups: dict[str, list] = {}
    for rec in body.records:
        groups.setdefault(rec.source, []).append(rec)

    async with app.state.ingest_lock:
        now = _utcnow_naive()
        for source, recs in groups.items():
            entry = app.state.ingest_buffer.get(source)
            # Rollover OR no entry yet — open a fresh segment. The
            # rollover branch closes the prior one and registers it
            # with the catalog so the scanner sees the bytes.
            if entry is None or entry["size_bytes"] >= _SEGMENT_ROLLOVER_BYTES:
                if entry is not None:
                    await _flush_buffer_entry(app, source)
                entry = _open_new_segment(app, source, now)
                app.state.ingest_buffer[source] = entry

            fh = entry["fh"]
            for rec in recs:
                # ``mode="json"`` so the datetime serialises to an ISO
                # string (default would emit a tuple under pydantic v2).
                line = json.dumps(rec.model_dump(mode="json")) + "\n"
                fh.write(line)
                entry["size_bytes"] += len(line.encode("utf-8"))
                rec_ts = rec.ts
                # Pydantic preserves tzinfo on parse; strip to naive UTC
                # so the catalog row matches the rest of the schema.
                if rec_ts.tzinfo is not None:
                    rec_ts = rec_ts.astimezone(timezone.utc).replace(tzinfo=None)
                if entry["first_ts"] is None:
                    entry["first_ts"] = rec_ts
                entry["last_ts"] = rec_ts
                accepted += 1
            # Force the data through the kernel buffer so a subsequent
            # ``stat`` from another tier (or a test) sees the bytes.
            try:
                fh.flush()
            except Exception:
                logger.exception(
                    "ingest: flush after write failed for source=%s", source
                )
            last_segment_path = str(entry["path"])

    return IngestResponse(accepted=accepted, segment_path=last_segment_path)


# --- Files listing --------------------------------------------------------


@router.get("/v1/files", response_model=FilesListResponse, tags=["files"])
async def list_files(
    request: Request,
    tier: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> FilesListResponse:
    """Return a page of catalog rows.

    Filters:

      * ``tier`` — exact match against ``files.tier`` (hot / warm / cold
        / archive / pending). Omit to return all tiers.
      * ``limit`` — page size, default 100, max 1000.
      * ``offset`` — offset, default 0.

    ``total`` is the unfiltered-by-pagination total under the same tier
    filter — clients use it for paging UI.
    """
    catalog_repo = request.app.state.catalog_repo
    rows = await catalog_repo.list_files(tier=tier, limit=limit, offset=offset)
    total = await catalog_repo.count_files(tier=tier)
    files = [
        FileSummary(
            id=r.id,
            source=r.source,
            segment_path=r.segment_path,
            tier=r.tier,
            size_bytes=r.size_bytes,
            oldest_record_ts=r.oldest_record_ts,
            newest_record_ts=r.newest_record_ts,
            next_eval_at=r.next_eval_at,
            created_at=r.created_at,
        )
        for r in rows
    ]
    return FilesListResponse(files=files, total=total)


# --- Evaluate -------------------------------------------------------------


@router.post("/v1/evaluate", response_model=EvaluateResponse, tags=["lifecycle"])
async def evaluate(request: Request) -> EvaluateResponse:
    """Synchronously run one scan+apply+sweep cycle.

    Used by tests and operators who don't want to wait for the next
    scheduler tick. Steps:

      1. Force-flush any open ingest segments so the bytes ingested in
         this test/run are visible to the scanner (without this step a
         test that ingests + evaluates in the same request would see
         zero scans because the segment is still open in the buffer).
      2. Run ``scan_once`` — plan transitions.
      3. Run ``apply_once`` — execute pending transitions.
      4. Run ``sweep_once`` — drain the pending-delete queue (a fresh
         apply produces rows with ``delete_after = now + 24 h``, so this
         won't typically sweep anything unless the test backdates ``now``;
         we still run it for completeness so the report is uniform).
      5. Wrap counts in an ``EvaluateResponse`` with wall-clock timing.
    """
    app = request.app

    await _flush_open_segments(app)

    settings = app.state.settings
    catalog_repo = app.state.catalog_repo
    policy_set = app.state.policy_set

    start = time.perf_counter()
    now = _utcnow_naive()

    scan_report = await scan_once(catalog_repo, policy_set, now)
    apply_report = await apply_once(
        catalog_repo,
        settings.storage_root,
        now,
        delete_delay_hours=settings.delete_delay_hours,
    )
    sweep_report = await sweep_once(catalog_repo, now)
    elapsed = time.perf_counter() - start

    return EvaluateResponse(
        scanned=scan_report.scanned,
        transitions_planned=scan_report.transitions_planned,
        applied=apply_report.applied,
        failed=apply_report.failed,
        swept=sweep_report.swept,
        eval_seconds=elapsed,
    )


# --- Compliance reports ---------------------------------------------------


@router.get("/v1/reports/{framework}", tags=["reports"])
async def get_report(framework: str, request: Request) -> dict:
    """Render a per-framework compliance report.

    Path arg ``framework`` is the slug (``gdpr``, ``sox``, ``hipaa`` in
    C14; ``pci_dss`` and ``soc2`` land in C15). Optional query params:

      * ``from`` — ISO timestamp; defaults to ``to - 30 days``.
      * ``to``   — ISO timestamp; defaults to ``utcnow``.

    Returns a JSON body matching :class:`ReportBundle`. Unknown framework
    slugs return HTTP 400 listing the supported set (the unsupported
    case is the most common operator misconfiguration — the list saves
    a round trip to the docs).
    """
    qp = request.query_params
    time_to_str = qp.get("to")
    time_from_str = qp.get("from")
    now = datetime.utcnow()
    time_to = datetime.fromisoformat(time_to_str) if time_to_str else now
    time_from = (
        datetime.fromisoformat(time_from_str)
        if time_from_str
        else (time_to - timedelta(days=30))
    )

    session_factory = request.app.state.session_factory
    policy_set = request.app.state.policy_set
    try:
        bundle = await render_report(
            framework, session_factory, policy_set, time_from, time_to
        )
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"unknown framework: {framework}. Supported: gdpr, sox, hipaa, pci_dss, soc2",
        )
    return bundle.model_dump(mode="json")
