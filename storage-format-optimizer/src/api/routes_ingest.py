"""The ``POST /api/ingest`` endpoint — land a batch of log entries.

A batch is landed for one tenant by the
:class:`~src.ingest_engine.IngestEngine`, which flattens, partitions, writes
each partition through its current format's backend, and records the write into
the manifest / pattern tracker / metrics. The engine returns a summary dict that
maps straight onto :class:`~src.api.schemas.IngestResponse`. Malformed bodies are
rejected as ``422`` by Pydantic's validation of
:class:`~src.models.IngestRequest` before the handler runs.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from src.api.dependencies import get_ingest_engine
from src.api.schemas import IngestResponse
from src.ingest_engine import IngestEngine
from src.models import IngestRequest

router = APIRouter(prefix="/api", tags=["ingest"])


@router.post("/ingest", response_model=IngestResponse)
async def ingest(
    req: IngestRequest,
    engine: Annotated[IngestEngine, Depends(get_ingest_engine)],
) -> IngestResponse:
    """Ingest ``req.entries`` for ``req.tenant`` and report what was landed.

    Delegates to :meth:`~src.ingest_engine.IngestEngine.ingest`, whose summary
    dict (``ingested`` / ``partitions_touched`` / ``tenant``) is unpacked into the
    response model. ``IngestRequest`` requires at least one entry, so an empty
    batch is rejected as ``422`` before reaching this handler.
    """
    result = await engine.ingest(req.tenant, req.entries)
    return IngestResponse(**result)
