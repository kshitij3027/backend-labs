"""Real-time web dashboard for the Bloom Filter Log Membership service (C12).

This is a **separate FastAPI application** served by its own uvicorn process
on :8002 (compose runs it from the same image as the API with a different
command). It deliberately imports NOTHING from :mod:`src.api`,
:mod:`src.manager`, :mod:`src.bloom`, or :mod:`src.pipeline` — the dashboard
reaches the membership API **over HTTP only** (``settings.api_base_url``).
Process separation is the point: a browser refresh storm, a slow WebSocket
client, or a Chart.js asset download can never steal event-loop time from
the hot ``/logs/add`` / ``/logs/query`` path, and the dashboard process holds
no filter state to diverge.

What it serves
--------------
* ``GET /``           — the single-page dashboard (plain HTML read from
  ``dashboard/templates/index.html`` at request time; no Jinja, no deps).
* ``GET /static/*``   — the page's assets, including the **vendored**
  Chart.js 4.4.1 UMD bundle (no CDN at runtime).
* ``GET /health``     — liveness probe for Docker's healthcheck.
* ``WS  /ws``         — the live feed: an immediate tick on connect (the
  page paints instantly), then one broadcast tick every
  ``dashboard_refresh_ms`` from the poll loop.
* ``POST /proxy/add | /proxy/query | /proxy/session-query`` — thin relays to
  the API's ``/logs/add``, ``/logs/query``, and ``/sessions/query``. The
  browser only ever talks to :8002, so no CORS configuration is needed
  anywhere; an unreachable API surfaces as a 502 with a useful detail.

Tick shape (the WebSocket contract ``dashboard.js`` consumes)::

    {
      "type": "tick",
      "ts": <epoch float>,
      "refresh_ms": <int>,
      "api":      <GET /stats JSON>          | null,
      "pipeline": <GET /pipeline/stats JSON> | null,
      "sessions": <GET /sessions/stats JSON> | null,
      "error":    null | "<why every payload above is null>"
    }

The three payload keys are all-or-nothing: one fetch failure nulls the lot
and fills ``error`` (a half-populated tick would make the client guess which
sections are trustworthy). The loop keeps ticking through API outages, so
the page always shows a live "API unreachable" state instead of freezing.
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.settings import Settings, get_settings

logger = logging.getLogger("bloom_filter_dashboard")

#: Project root resolved from this file (``src/dashboard.py`` → parent of
#: ``src/``), so the page + assets are found both in Docker (``/app``) and
#: when tests run from the repo checkout — never dependent on the CWD.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_TEMPLATES_DIR = _PROJECT_ROOT / "dashboard" / "templates"
_STATIC_DIR = _PROJECT_ROOT / "dashboard" / "static"

#: Timeout for every dashboard → API HTTP call. Short on purpose: the API
#: answers stats in microseconds when healthy, so anything slower is an
#: outage and the tick should degrade to its error shape quickly instead of
#: stalling the poll loop (or a proxy request) for a default 5+ seconds.
_HTTP_TIMEOUT_SECONDS = 3.0


# --------------------------------------------------------------------- #
# request models (local duplicates — see comment)                        #
# --------------------------------------------------------------------- #

#: Duplicated from ``src.api.LogType`` ON PURPOSE, not imported: importing
#: ``src.api`` would drag the manager / pipeline / sqlite dependency graph
#: into this process, and the whole design premise of the dashboard is that
#: it shares no code path (and no state) with the membership service —
#: process isolation over DRY for three string literals. Kept in sync by the
#: API's own 422 behaviour: even if these drifted, the upstream service
#: still enforces its own Literal.
LogType = Literal["error_logs", "access_logs", "security_logs"]


class ProxyLogRequest(BaseModel):
    """Body of ``/proxy/add`` and ``/proxy/query`` — mirrors the API's shape.

    Validated dashboard-side so a typo'd ``log_type`` 422s locally with
    FastAPI's permitted-values message instead of round-tripping to the API.
    """

    log_type: LogType
    log_key: str = Field(min_length=1)


class ProxySessionRequest(BaseModel):
    """Body of ``/proxy/session-query`` — mirrors the API's ``SessionRequest``."""

    session_id: str = Field(min_length=1)


# --------------------------------------------------------------------- #
# API access (module-level symbols — the tests' monkeypatch seams)       #
# --------------------------------------------------------------------- #


async def fetch_all_stats(settings: Settings) -> dict:
    """Fetch the three stats documents from the membership API in one client.

    Returns ``{"api": <GET /stats>, "pipeline": <GET /pipeline/stats>,
    "sessions": <GET /sessions/stats>, "error": None}`` on success. On ANY
    exception — connect refused, timeout, non-2xx, bad JSON — it returns the
    same four keys with every payload ``None`` and ``error`` set to the
    stringified cause, so callers always receive the full tick-payload shape
    and never need their own partial-failure logic.

    **Test contract:** this is a MODULE-LEVEL symbol precisely so the
    integration tests can ``monkeypatch.setattr(src.dashboard,
    "fetch_all_stats", fake)`` before starting the app — every caller (the
    poll loop and the per-connect immediate tick) resolves the name through
    module globals at call time, so the patch takes effect everywhere.
    """
    base = settings.api_base_url.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SECONDS) as client:
            api_resp = await client.get(f"{base}/stats")
            pipeline_resp = await client.get(f"{base}/pipeline/stats")
            sessions_resp = await client.get(f"{base}/sessions/stats")
            api_resp.raise_for_status()
            pipeline_resp.raise_for_status()
            sessions_resp.raise_for_status()
            return {
                "api": api_resp.json(),
                "pipeline": pipeline_resp.json(),
                "sessions": sessions_resp.json(),
                "error": None,
            }
    except Exception as exc:  # noqa: BLE001 — degrade to the error tick shape
        return {"api": None, "pipeline": None, "sessions": None, "error": str(exc)}


async def proxy_post(url: str, payload: dict) -> tuple[int, dict]:
    """POST ``payload`` as JSON to ``url``; return ``(status_code, body)``.

    The single forwarding primitive behind all three ``/proxy/*`` routes,
    kept as a MODULE-LEVEL symbol so tests can monkeypatch it to (a) assert
    exactly which API URL + payload a proxy route forwards and (b) simulate
    connection failures by raising ``httpx.ConnectError``. Unlike
    :func:`fetch_all_stats` it deliberately does NOT swallow exceptions —
    the routes translate transport failures into a 502 so the browser gets
    an actionable error instead of a fake success.
    """
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SECONDS) as client:
        resp = await client.post(url, json=payload)
        return resp.status_code, resp.json()


# --------------------------------------------------------------------- #
# tick building + the safety wrapper around the fetch seam               #
# --------------------------------------------------------------------- #


def build_tick(payload: dict, refresh_ms: int) -> dict:
    """Wrap one fetch payload into the wire tick: type + timestamp + cadence.

    ``refresh_ms`` rides inside every tick so the client can display (and
    trust) the live cadence without a separate config endpoint — change the
    env var, restart the dashboard, and every connected page relabels itself
    on the next tick.
    """
    return {"type": "tick", "ts": time.time(), "refresh_ms": refresh_ms, **payload}


async def _safe_fetch(settings: Settings) -> dict:
    """Call :func:`fetch_all_stats` and force the error shape on any raise.

    :func:`fetch_all_stats` already converts its own failures into the
    error-shaped dict, but the symbol is a documented monkeypatch seam — a
    test (or a future edit) may swap in a version that raises. The broadcast
    loop must NEVER die to one bad fetch (a dashboard that silently stops
    ticking is worse than one reporting an outage), so this wrapper is the
    belt to the function's suspenders: any exception becomes the same
    ``{"api": None, ..., "error": str(exc)}`` payload and the tick still
    goes out. The lookup of ``fetch_all_stats`` goes through module globals
    at call time, which is exactly what makes the monkeypatch visible here.
    """
    try:
        return await fetch_all_stats(settings)
    except Exception as exc:  # noqa: BLE001 — keep the tick loop immortal
        return {"api": None, "pipeline": None, "sessions": None, "error": str(exc)}


# --------------------------------------------------------------------- #
# WebSocket fan-out                                                     #
# --------------------------------------------------------------------- #


class ConnectionManager:
    """Track every connected dashboard client and fan ticks out to all.

    Mirrors the pattern used across backend-labs: connections live in a
    plain ``set``; broadcasts iterate a ``list(...)`` snapshot so pruning a
    dead socket mid-loop never mutates the structure being walked; every
    send is individually guarded so one dead client is discarded instead of
    aborting the fan-out (or killing the poll loop). Single event loop,
    single process — no locking needed around the set.
    """

    def __init__(self) -> None:
        self.active: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        """Accept the handshake and register the connection.

        The immediate first tick is sent by the ``/ws`` route right after
        this returns (it needs a fresh fetch, which is route business, not
        connection bookkeeping).
        """
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        """Deregister a connection (no-op if a failed send already pruned it)."""
        self.active.discard(ws)

    async def broadcast(self, message: dict) -> None:
        """Send ``message`` as JSON to every client, pruning any that fail."""
        for ws in list(self.active):
            try:
                await ws.send_json(message)
            except Exception:  # noqa: BLE001 — prune dead clients, never abort
                self.active.discard(ws)

    async def send_personal(self, ws: WebSocket, message: dict) -> None:
        """Send ``message`` to one client; prune it on failure instead of raising."""
        try:
            await ws.send_json(message)
        except Exception:  # noqa: BLE001 — prune on failure, never raise
            self.active.discard(ws)


#: Process-wide manager. Module-level (not app.state) because the broadcast
#: loop, the /ws route, and tests all want one obvious handle to the same
#: connection set — and this process only ever hosts one dashboard app.
manager = ConnectionManager()


async def _poll_and_broadcast_loop(settings: Settings) -> None:
    """Fetch stats and broadcast one tick every ``dashboard_refresh_ms``.

    Broadcast-then-sleep ordering: the first loop tick goes out immediately
    at startup, so even a client that connected between app start and the
    first interval gets fresh data without waiting a full period. Failures
    are impossible by construction (``_safe_fetch`` never raises and
    ``broadcast`` swallows per-socket errors), so the only exit is
    cancellation at shutdown — ``CancelledError`` is deliberately not
    caught.
    """
    interval_seconds = max(0.01, settings.dashboard_refresh_ms / 1000.0)
    while True:
        tick = build_tick(await _safe_fetch(settings), settings.dashboard_refresh_ms)
        await manager.broadcast(tick)
        await asyncio.sleep(interval_seconds)


# --------------------------------------------------------------------- #
# lifespan                                                              #
# --------------------------------------------------------------------- #


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Resolve settings, run the poll-and-broadcast task, cancel it on exit.

    Nothing here touches disk or builds filters — the dashboard's entire
    state is the settings object, the connection set, and one background
    task. Tests that monkeypatch :func:`fetch_all_stats` must do so BEFORE
    entering ``TestClient`` (i.e. before this lifespan starts), because the
    loop performs its first fetch immediately.
    """
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    app.state.settings = settings

    poll_task = asyncio.create_task(
        _poll_and_broadcast_loop(settings), name="dashboard-poll-loop"
    )
    logger.info(
        "dashboard starting on %s:%s (api=%s, refresh every %sms)",
        settings.dashboard_host,
        settings.dashboard_port,
        settings.api_base_url,
        settings.dashboard_refresh_ms,
    )
    try:
        yield
    finally:
        poll_task.cancel()
        await asyncio.gather(poll_task, return_exceptions=True)
        logger.info("dashboard shutdown")


app = FastAPI(title="Bloom Filter Log Membership Dashboard", lifespan=lifespan)

# The page's assets: dashboard.css, dashboard.js, and the vendored Chart.js
# 4.4.1 UMD bundle — the dashboard renders with zero outbound network calls.
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


# --------------------------------------------------------------------- #
# routes                                                                #
# --------------------------------------------------------------------- #


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    """Serve the single-page dashboard.

    Read from disk at request time (not cached at import) so iterating on
    the markup needs only a browser refresh; it's one small file read per
    page load on a low-traffic UI process — simplicity beats caching here,
    and skipping Jinja keeps the dashboard dependency-free.
    """
    html = (_TEMPLATES_DIR / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe used by compose's dashboard healthcheck."""
    return {"status": "healthy"}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    """Live tick stream: one immediate tick on connect, then broadcasts.

    The immediate tick is fetched FRESH (not replayed from the last
    broadcast) so a newly opened page paints current numbers instantly
    instead of waiting up to a full refresh interval. After that the
    connection is passive — the client never needs to send anything — so the
    receive loop exists purely to park the coroutine until the browser
    departs and ``WebSocketDisconnect`` unregisters it.
    """
    settings: Settings = ws.app.state.settings
    await manager.connect(ws)
    first_tick = build_tick(
        await _safe_fetch(settings), settings.dashboard_refresh_ms
    )
    await manager.send_personal(ws, first_tick)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


# --------------------------------------------------------------------- #
# proxy routes (browser → :8002 → API; no CORS anywhere)                #
# --------------------------------------------------------------------- #


async def _relay(api_path: str, payload: dict) -> JSONResponse:
    """Forward ``payload`` to the API at ``api_path``; relay status + body.

    All three proxy routes funnel through here so the forwarding behaviour
    (URL joining, 502 translation, status passthrough) lives in one place.
    Transport-level failures — connect refused, DNS, timeout — become a 502
    Bad Gateway whose detail names the cause; an HTTP error response from
    the API (e.g. a 422 it chose to return) is NOT a failure and is relayed
    verbatim, status code and all.
    """
    settings: Settings = app.state.settings
    url = settings.api_base_url.rstrip("/") + api_path
    try:
        status_code, body = await proxy_post(url, payload)
    except (httpx.HTTPError, OSError) as exc:
        raise HTTPException(
            status_code=502, detail=f"membership API unreachable: {exc}"
        ) from exc
    return JSONResponse(status_code=status_code, content=body)


@app.post("/proxy/add")
async def proxy_add(body: ProxyLogRequest) -> JSONResponse:
    """Relay the dashboard's add form to ``POST /logs/add`` on the API."""
    return await _relay("/logs/add", body.model_dump())


@app.post("/proxy/query")
async def proxy_query(body: ProxyLogRequest) -> JSONResponse:
    """Relay the dashboard's query form to ``POST /logs/query`` on the API."""
    return await _relay("/logs/query", body.model_dump())


@app.post("/proxy/session-query")
async def proxy_session_query(body: ProxySessionRequest) -> JSONResponse:
    """Relay the session box to ``POST /sessions/query`` on the API."""
    return await _relay("/sessions/query", body.model_dump())


if __name__ == "__main__":
    # Convenience entrypoint for `python -m src.dashboard`; Docker runs
    # uvicorn directly (same image as the API, different command).
    settings = get_settings()
    uvicorn.run(
        "src.dashboard:app",
        host=settings.dashboard_host,
        port=settings.dashboard_port,
        log_level=settings.log_level.lower(),
    )
