from fastapi import APIRouter, Request, Response, status
from fastapi.responses import JSONResponse, PlainTextResponse

from src.admission import AdmissionVerdict
from src.api.models import (
    AdminConfigResponse,
    AdminConfigUpdate,
    BackpressureBlock,
    CircuitBreakerBlock,
    IngestRequest,
    IngestResponse,
    LoadTestStartRequest,
    LoadTestStatusResponse,
    ProcessorBlock,
    SystemStatus,
)
from src.api.prometheus import Metrics
from src.logging_setup import TAG_CONFIG_UPDATE, get_logger

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


@router.post("/loadtest/start", response_model=LoadTestStatusResponse)
async def loadtest_start(req: LoadTestStartRequest, request: Request) -> LoadTestStatusResponse:
    c = _state(request)
    status = await c.load_tester.start(
        profile=req.profile,
        rps=req.rps,
        duration_seconds=req.duration_seconds,
        spike_multiplier=req.spike_multiplier,
    )
    return LoadTestStatusResponse(**status.__dict__)


@router.post("/loadtest/stop", response_model=LoadTestStatusResponse)
async def loadtest_stop(request: Request) -> LoadTestStatusResponse:
    c = _state(request)
    status = await c.load_tester.stop()
    return LoadTestStatusResponse(**status.__dict__)


@router.get("/loadtest/status", response_model=LoadTestStatusResponse)
async def loadtest_status(request: Request) -> LoadTestStatusResponse:
    c = _state(request)
    status = c.load_tester.status()
    return LoadTestStatusResponse(**status.__dict__)


@router.post("/admin/config", response_model=AdminConfigResponse)
async def admin_config(req: AdminConfigUpdate, request: Request) -> AdminConfigResponse:
    c = _state(request)
    updated = []
    payload = req.model_dump(exclude_none=True)
    for key, value in payload.items():
        try:
            setattr(c.settings, key, value)
        except Exception:
            c.settings.__dict__[key] = value
        updated.append(key)
    if "ewma_alpha" in payload:
        c.fuser.alpha = c.settings.ewma_alpha
    _log.info(
        "admin_config_update",
        tag=TAG_CONFIG_UPDATE,
        updated_fields=updated,
    )
    current = {
        "ewma_alpha": c.settings.ewma_alpha,
        "up_normal_to_pressure": c.settings.up_normal_to_pressure,
        "up_pressure_to_overload": c.settings.up_pressure_to_overload,
        "up_overload_to_emergency": c.settings.up_overload_to_emergency,
        "down_overload_to_pressure": c.settings.down_overload_to_pressure,
        "down_pressure_to_normal": c.settings.down_pressure_to_normal,
        "down_recovery_to_normal": c.settings.down_recovery_to_normal,
        "min_dwell_seconds": c.settings.min_dwell_seconds,
        "processing_latency_seconds": c.settings.processing_latency_seconds,
        "sampling_interval": c.settings.sampling_interval,
        "aimd_beta": c.settings.aimd_beta,
        "max_queue_size": c.settings.max_queue_size,
    }
    return AdminConfigResponse(updated_fields=updated, current=current)
