from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from src.shared.models import PartitionExecuteRequest, PartitionExecuteResponse


router = APIRouter()


_DEFAULT_LIMIT = 1000


@router.get("/health")
async def health(request: Request) -> dict[str, str]:
    """Cheap liveness endpoint used by the Docker healthcheck and by the
    coordinator's periodic poll.
    """

    metadata = getattr(request.app.state, "metadata", None)
    partition_id = metadata.id if metadata is not None else "unknown"
    return {"status": "ok", "partition_id": partition_id}


@router.get("/metadata")
async def metadata(request: Request) -> dict[str, Any]:
    """Return this partition's advertised metadata.

    Returned shape matches :class:`PartitionMetadata`.
    """

    meta = getattr(request.app.state, "metadata", None)
    if meta is None:
        raise HTTPException(status_code=503, detail="partition not initialised")
    return meta.model_dump(mode="json")


@router.post("/execute", response_model=PartitionExecuteResponse)
async def execute(
    request: Request, payload: PartitionExecuteRequest
) -> PartitionExecuteResponse:
    """Run a pushed-down filter (and optional partial aggregation) against
    the local log store.

    Response shape matches :class:`PartitionExecuteResponse`: either
    ``rows`` (truncated to ``limit``) or ``partial_aggregate``.
    """

    executor = getattr(request.app.state, "executor", None)
    storage = getattr(request.app.state, "storage", None)
    if executor is None or storage is None:
        raise HTTPException(status_code=503, detail="partition not initialised")

    try:
        matched = executor.filter(payload.filter_ast_json)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=f"malformed filter AST: {exc}"
        ) from exc
    except KeyError as exc:
        raise HTTPException(
            status_code=400, detail=f"malformed filter AST: missing field {exc}"
        ) from exc

    records_scanned = len(storage.rows())

    if payload.aggregation is not None:
        try:
            partial = executor.partial_aggregate(matched, payload.aggregation)
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=f"invalid aggregation: {exc}"
            ) from exc
        return PartitionExecuteResponse(
            rows=[],
            partial_aggregate=partial,
            records_scanned=records_scanned,
        )

    limit = payload.limit if payload.limit is not None else _DEFAULT_LIMIT
    truncated = matched[:limit] if limit >= 0 else matched

    if payload.select_fields:
        projected = [
            {field: row.get(field) for field in payload.select_fields}
            for row in truncated
        ]
        out_rows = projected
    else:
        out_rows = truncated

    return PartitionExecuteResponse(
        rows=out_rows,
        partial_aggregate=None,
        records_scanned=records_scanned,
    )
