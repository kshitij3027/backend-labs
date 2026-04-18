"""Ingest + synthetic-generate HTTP endpoints.

Accepts a single ``LogEntry`` or a list (``Union[LogEntry, list[LogEntry]]``)
on ``POST /api/logs`` to match the spec's "single or batch on one
endpoint" shape, and exposes ``POST /api/logs/generate?count=N&seed=S``
for seeding synthetic data.

Timings are measured with ``time.perf_counter()`` so the response
includes a ``query_time_ms`` that reflects only the server-side
ingest cost (excluding network).
"""

from __future__ import annotations

import logging
import time
from typing import List, Union

from fastapi import APIRouter, Body, HTTPException, Query, Request, status

from src.models import GenerateResponse, IngestResponse, LogEntry
from src.search.generator import generate_batch
from src.storage import sqlite_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["logs"])


@router.post(
    "/logs",
    response_model=IngestResponse,
    status_code=status.HTTP_201_CREATED,
)
async def ingest_logs(
    request: Request,
    payload: Union[LogEntry, List[LogEntry]] = Body(...),
) -> IngestResponse:
    """Insert a single log entry or a list of entries.

    Returns 201 with the inserted ids. Ids for batches over 100 are
    dropped from the response to keep it small — the ``inserted_count``
    is still authoritative.
    """
    entries: List[LogEntry]
    if isinstance(payload, list):
        entries = payload
    else:
        entries = [payload]

    if not entries:
        raise HTTPException(status_code=400, detail="empty payload")

    db = request.app.state.db
    ids = await sqlite_store.insert_logs(db, entries)

    logger.info("ingest count=%d", len(ids))
    return IngestResponse(
        inserted_count=len(ids),
        ids=ids if len(ids) <= 100 else [],
    )


@router.post(
    "/logs/generate",
    response_model=GenerateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def generate_logs(
    request: Request,
    count: int = Query(500, ge=1, le=100_000),
    seed: int | None = Query(None),
) -> GenerateResponse:
    """Generate ``count`` synthetic logs and bulk-insert them.

    Runs ``ANALYZE`` after insert so query-planner stats stay current.
    ``seed`` is an optional knob for reproducibility in tests.
    """
    db = request.app.state.db

    t0 = time.perf_counter()
    batch = list(generate_batch(count, seed=seed))
    await sqlite_store.insert_logs(db, batch)
    await sqlite_store.analyze(db)
    t1 = time.perf_counter()

    elapsed_ms = (t1 - t0) * 1000.0
    logger.info("generated count=%d elapsed_ms=%.2f", count, elapsed_ms)
    return GenerateResponse(
        generated_count=count,
        query_time_ms=round(elapsed_ms, 3),
    )
