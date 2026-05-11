from fastapi import APIRouter, Request, Response, status
from fastapi.responses import JSONResponse, PlainTextResponse

from src.admission import AdmissionVerdict
from src.api.models import (
    BackpressureBlock,
    CircuitBreakerBlock,
    IngestRequest,
    IngestResponse,
    ProcessorBlock,
    SystemStatus,
)
from src.api.prometheus import Metrics
from src.logging_setup import get_logger

router = APIRouter()
_metrics = Metrics()
_log = get_logger("routes")


def _state(request: Request):
    return request.app.state.components


@router.post("/ingest", response_model=IngestResponse)
async def ingest(req: IngestRequest, request: Request):
    import time
    c = _state(request)
    verdict = c.admission.decide(req.priority, c.manager.level)
    if verdict == AdmissionVerdict.ACCEPT:
        try:
            c.queues.put_nowait(req.priority, req.message, time.monotonic())
        except Exception:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"accepted": False, "verdict": "queue_full", "priority": req.priority.value},
            )
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"accepted": True, "verdict": "accept", "priority": req.priority.value},
        )
    if verdict == AdmissionVerdict.THROTTLE_429:
        retry_after = c.aimd.retry_after(1.0)
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"accepted": False, "verdict": "throttle_429", "priority": req.priority.value},
            headers={"Retry-After": f"{retry_after:.2f}"},
        )
    if verdict == AdmissionVerdict.DROP_SILENT:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"accepted": False, "verdict": "reject_503", "priority": req.priority.value},
    )


@router.get("/system/status", response_model=SystemStatus)
async def system_status(request: Request) -> SystemStatus:
    c = _state(request)
    return SystemStatus(
        backpressure=BackpressureBlock(
            pressure_level=c.manager.level.value,
            throttle_rate=c.aimd.throttle_rate,
            queue_size=c.queues.total_qsize(),
            pressure_score=c.fuser.last_score,
        ),
        processor=ProcessorBlock(
            processed_count=c.workers.processed_count,
            dropped_count=c.admission.counters["dropped"] + c.admission.counters["rejected"],
            error_count=c.workers.error_count,
        ),
        circuit_breaker=CircuitBreakerBlock(
            state=c.breaker.state.value,
            failure_count=c.breaker.failure_count,
        ),
    )


@router.get("/system/health")
async def v1_health() -> dict:
    return {"status": "ok"}


@router.get("/metrics/json")
async def metrics_json(request: Request) -> dict:
    c = _state(request)
    _metrics.pressure_score.set(c.fuser.last_score)
    _metrics.throttle_rate.set(c.aimd.throttle_rate)
    _metrics.aimd_limit.set(c.aimd.limit)
    from src.state import Priority
    for p in Priority:
        _metrics.queue_size.labels(priority=p.value).set(c.queues.qsize(p))
    return _metrics.json_snapshot(c)


@router.get("/prometheus")
async def metrics_prometheus(request: Request) -> Response:
    c = _state(request)
    _metrics.pressure_score.set(c.fuser.last_score)
    _metrics.throttle_rate.set(c.aimd.throttle_rate)
    _metrics.aimd_limit.set(c.aimd.limit)
    from src.state import Priority
    for p in Priority:
        _metrics.queue_size.labels(priority=p.value).set(c.queues.qsize(p))
    return PlainTextResponse(_metrics.text(), media_type="text/plain; version=0.0.4")
