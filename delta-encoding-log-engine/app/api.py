"""REST API surface for the delta-encoding log engine.

This router is the thin HTTP layer over the in-memory :class:`~app.store.SegmentStore`
and the live :class:`~app.metrics.MetricsRegistry`. It owns no state of its own: every
handler reaches the shared object graph through ``request.app.state`` (``store`` /
``metrics`` / ``settings``), which :mod:`app.main` builds once in its ``lifespan``.

**Event-loop discipline (load-bearing — see *plan.md → Architecture*).** The handlers
split deliberately by cost:

* **CPU-bound handlers are plain ``def``** — ``generate``, ``compress``, ``reconstruct``,
  ``logs_page``, ``logs_index``. Starlette dispatches a sync handler to its AnyIO
  threadpool, so the codec's encode/decode work never blocks the event loop (which stays
  free to serve ``/health`` and, in a later commit, the dashboard websocket). The store's
  own :class:`threading.Lock` makes this concurrency safe.
* **Trivial handlers are ``async def``** — ``health``, ``stats``, ``reset``. They do a
  handful of dict reads / counter resets and would only waste a threadpool hop.

**Error-counter discipline (the ``system.errors == 0`` reliability gate).** Only an
*unexpected* failure (a 500) bumps :meth:`~app.metrics.MetricsRegistry.incr_error`.
Client errors — FastAPI's 422 validation, and the 400 / 404 raised here — are normal,
expected outcomes of bad input and must **not** touch the error counter, or the gate
would trip under ordinary client usage.

Paths are written in full on each route (``/health`` at the root, the rest under
``/api/...``) and the router is mounted with **no prefix** by :mod:`app.main`, so the
final paths are exactly: ``/health``, ``/api/generate``, ``/api/compress``,
``/api/reconstruct``, ``/api/logs``, ``/api/logs/{index}``, ``/api/stats``, ``/api/reset``.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from app.codec import entries_equal
from app.generator import generate_logs
from app.models import (
    CompressRequest,
    GenerateRequest,
    GenerateResponse,
    ReconstructRequest,
)

router = APIRouter()


# --------------------------------------------------------------------------- #
# Liveness.
# --------------------------------------------------------------------------- #
@router.get("/health")
async def health() -> dict:
    """Liveness probe used by Docker's HEALTHCHECK and the E2E wait loop."""
    return {"status": "healthy"}


# --------------------------------------------------------------------------- #
# Generate — synthetic structured logs (CPU-bound → sync def → threadpool).
# --------------------------------------------------------------------------- #
@router.post("/api/generate", response_model=GenerateResponse)
def generate(req: GenerateRequest, request: Request) -> GenerateResponse:
    """Generate a synthetic batch, store it as the pending raw batch, and return it.

    ``churn`` / ``schema_width`` fall back to the configured generator defaults when
    omitted (``None``). The batch is stored via :meth:`~app.store.SegmentStore.set_raw`
    so a subsequent ``POST /api/compress`` with ``use_generated=True`` picks it up.
    """
    settings = request.app.state.settings
    store = request.app.state.store
    metrics = request.app.state.metrics

    churn = req.churn if req.churn is not None else settings.generator_field_churn
    schema_width = (
        req.schema_width
        if req.schema_width is not None
        else settings.generator_schema_width
    )

    logs = generate_logs(
        req.count,
        seed=req.seed,
        churn=churn,
        schema_width=schema_width,
    )
    with metrics.time_block("generate", entries=len(logs)):
        store.set_raw(logs)

    return GenerateResponse(logs=logs, count=len(logs))


# --------------------------------------------------------------------------- #
# Compress — delta-encode a batch (CPU-bound → sync def → threadpool).
# --------------------------------------------------------------------------- #
@router.post("/api/compress")
def compress(req: CompressRequest, request: Request) -> dict:
    """Delta-encode the chosen batch and return its byte-accounting :class:`CompressionStats`.

    The batch is the stored raw batch when ``use_generated`` is ``True`` (400 if none has
    been generated yet), otherwise ``req.logs`` (400 if absent). ``keyframe_interval`` /
    ``baseline`` overrides, when provided, are applied to the store for this call only and
    then restored. Unexpected failures bump the error counter and surface as a 500.
    """
    store = request.app.state.store
    metrics = request.app.state.metrics

    # Resolve the batch to compress.
    if req.use_generated:
        batch = store.get_raw()
        if not batch:
            raise HTTPException(
                status_code=400,
                detail="no generated batch to compress: call /api/generate first",
            )
    else:
        if not req.logs:
            raise HTTPException(
                status_code=400,
                detail="use_generated is false but no logs were provided",
            )
        batch = req.logs

    # Per-call overrides: temporarily swap the store's encode config, restoring it
    # in a finally so a later compression sees the configured defaults again.
    override_kf = req.keyframe_interval is not None
    override_base = req.baseline is not None
    saved_kf = store._keyframe_interval
    saved_base = store._baseline

    try:
        if override_kf:
            store._keyframe_interval = req.keyframe_interval
        if override_base:
            store._baseline = req.baseline

        with metrics.time_block("compress", entries=len(batch)):
            stats = store.compress(batch)
        return stats.to_dict()
    except HTTPException:
        # Client error already classified — re-raise without touching the counter.
        raise
    except Exception as exc:  # noqa: BLE001 — unexpected: count it and 500.
        metrics.incr_error()
        raise HTTPException(status_code=500, detail=f"compression failed: {exc}") from exc
    finally:
        if override_kf:
            store._keyframe_interval = saved_kf
        if override_base:
            store._baseline = saved_base


# --------------------------------------------------------------------------- #
# Reconstruct — rebuild originals (CPU-bound → sync def → threadpool).
# --------------------------------------------------------------------------- #
@router.post("/api/reconstruct")
def reconstruct(req: ReconstructRequest, request: Request) -> dict:
    """Reconstruct a single entry, a range, or the whole batch; optionally verify fidelity.

    Precedence: ``index`` (single, 404 on out-of-range) → ``start``/``end`` (half-open
    range) → all. When ``verify`` is set, the reconstructed entries are compared
    element-wise against the stored raw batch (the matching slice) via
    :func:`~app.codec.entries_equal`, and ``fidelity_ok`` reports whether every pair is
    canonically equal (``null`` when ``verify`` is false).
    """
    store = request.app.state.store
    metrics = request.app.state.metrics

    if req.index is not None:
        # Single-entry random access from the nearest keyframe.
        try:
            with metrics.time_block("reconstruct", entries=1):
                entry = store.reconstruct_index(req.index)
        except IndexError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        logs = [entry]
        raw_slice = store.get_raw()[req.index : req.index + 1] if req.verify else None
    elif req.start is not None or req.end is not None:
        # Half-open range; the store clamps bounds and never raises here.
        start = req.start if req.start is not None else 0
        end = req.end if req.end is not None else store.count
        with metrics.time_block("reconstruct", entries=max(0, end - start)):
            logs = store.reconstruct_range(start, end)
        raw_slice = store.get_raw()[start:end] if req.verify else None
    else:
        # Whole batch.
        all_raw = store.get_raw() if req.verify else None
        with metrics.time_block("reconstruct", entries=store.count):
            logs = store.reconstruct_all()
        raw_slice = all_raw

    fidelity_ok = None
    if req.verify:
        raw_slice = raw_slice or []
        fidelity_ok = len(logs) == len(raw_slice) and all(
            entries_equal(a, b) for a, b in zip(logs, raw_slice)
        )

    return {"logs": logs, "count": len(logs), "fidelity_ok": fidelity_ok}


# --------------------------------------------------------------------------- #
# Logs paging + single random access (CPU-bound → sync def → threadpool).
# --------------------------------------------------------------------------- #
@router.get("/api/logs")
def logs_page(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=1000),
) -> dict:
    """Reconstruct a page of ``limit`` entries starting at ``offset`` (with the total)."""
    store = request.app.state.store
    metrics = request.app.state.metrics

    with metrics.time_block("reconstruct", entries=limit):
        logs = store.page(offset, limit)
    return {"logs": logs, "offset": offset, "limit": limit, "total": store.count}


@router.get("/api/logs/{index}")
def logs_index(index: int, request: Request) -> dict:
    """Random-access reconstruct entry ``index`` from its nearest keyframe.

    Timed **per entry** (``entries=1``) because this single-entry path is exactly what
    the ``<100ms`` reconstruction-latency p99 gate measures. 404 on out-of-range.
    """
    store = request.app.state.store
    metrics = request.app.state.metrics

    try:
        with metrics.time_block("reconstruct", entries=1):
            entry = store.reconstruct_index(index)
        nearest = store.nearest_keyframe_index(index)
    except IndexError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {"index": index, "entry": entry, "nearest_keyframe_index": nearest}


# --------------------------------------------------------------------------- #
# Stats + reset (trivial → async def → event loop).
# --------------------------------------------------------------------------- #
@router.get("/api/stats")
async def stats(request: Request) -> dict:
    """Return the three-section view: storage byte accounting, performance, system health."""
    store = request.app.state.store
    metrics = request.app.state.metrics

    storage = store.stats()
    # Requirements naming alias: surface the delta reduction as storage_savings_percent.
    storage["storage_savings_percent"] = storage.get("delta_reduction", 0.0)

    return {
        "storage": storage,
        "performance": metrics.snapshot(),
        "system": {
            "status": "healthy",
            "errors": metrics.errors,
            "uptime_seconds": metrics.uptime_seconds(),
        },
    }


@router.post("/api/reset")
async def reset(request: Request) -> dict:
    """Clear the store (raw + encoding + stats) and the metrics registry back to empty."""
    request.app.state.store.reset()
    request.app.state.metrics.reset()
    return {"status": "reset"}
