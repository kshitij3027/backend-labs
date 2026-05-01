"""FastAPI dashboard server for the failover cluster.

Runs in its own container, completely independent of the cluster nodes.
Polls every peer's ``/role`` and ``/metrics`` once per second, broadcasts
the aggregated snapshot to all connected WebSocket clients, and serves
the static dashboard at ``/``.

Why it's a separate container
-----------------------------
The dashboard outlives the primary. If it were served from a node, the
moment that node died you'd lose visibility into the failover you were
trying to observe. By running it in its own container that polls all
three nodes, the dashboard tolerates any single-node failure — exactly
the failure mode the cluster is designed to recover from.

Why the failover button proxies through the server
--------------------------------------------------
The browser doesn't know which node currently holds ``leader:lock``.
Asking the dashboard server to look that up via Redis (using the same
``RedisClient`` the cluster uses) keeps the button work-correct even
mid-failover. If we hard-coded a target in the JS, the user would have
to refresh after every promotion to keep the button accurate.

Transport: WebSocket primary, HTTP fallback
-------------------------------------------
The browser connects to ``/ws`` for live push. We also expose
``GET /api/snapshot`` so the JS can fall back to 2s polling if the
WebSocket can't be established (some corporate proxies strip the
upgrade). The poll loop builds the same snapshot dict either way.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from src.redis_client import RedisClient

logger = logging.getLogger(__name__)


class DashboardConfig:
    """Env-driven configuration for the dashboard process.

    Reads the same Redis coordinates the cluster uses (so we can look
    up the current lock holder for the failover-proxy endpoint) plus a
    ``PEER_NODES`` CSV identifying every node to poll.
    """

    def __init__(self) -> None:
        self.redis_host: str = os.environ.get("REDIS_HOST", "redis")
        self.redis_port: int = int(os.environ.get("REDIS_PORT", "6379"))
        self.peer_nodes_raw: str = os.environ.get("PEER_NODES", "").strip()
        self.poll_interval: float = float(
            os.environ.get("DASHBOARD_POLL_INTERVAL", "1.0")
        )
        self.web_dir: str = os.environ.get("DASHBOARD_WEB_DIR", "/app/web")
        self.throughput_window_sec: float = float(
            os.environ.get("DASHBOARD_THROUGHPUT_WINDOW", "60.0")
        )

    def peers(self) -> list[tuple[str, int]]:
        """Parse ``PEER_NODES`` into a list of ``(host, port)``.

        Mirrors :py:meth:`NodeConfig.peer_list` semantics — bad entries
        are dropped silently rather than crashing the dashboard.
        """
        out: list[tuple[str, int]] = []
        if not self.peer_nodes_raw:
            return out
        for raw in self.peer_nodes_raw.split(","):
            entry = raw.strip()
            if not entry:
                continue
            host, _, port_str = entry.partition(":")
            host = host.strip()
            port_str = port_str.strip()
            if not host or not port_str:
                continue
            try:
                out.append((host, int(port_str)))
            except ValueError:
                continue
        return out


def create_app(config: Optional[DashboardConfig] = None) -> FastAPI:
    """Build the dashboard FastAPI app.

    Construction is idempotent and dependency-free — callers may pass a
    pre-built :class:`DashboardConfig` (used by tests to point at
    fakes), or let it read its env-driven defaults.

    The poll loop is started inside the ``lifespan`` context so
    ``TestClient(app) as client`` (which drives the lifespan event)
    spins it up automatically.
    """
    cfg = config if config is not None else DashboardConfig()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Connect Redis. We swallow connection failures during startup so
        # the dashboard still serves the static UI even if Redis is
        # transiently unreachable — the UI will simply show "no primary"
        # until Redis comes back.
        try:
            await app.state.redis.connect()
        except Exception:
            logger.exception("dashboard Redis.connect failed at startup")

        app.state.poll_task = asyncio.create_task(
            _poll_loop(app), name="dashboard-poll"
        )
        try:
            yield
        finally:
            task: Optional[asyncio.Task[None]] = app.state.poll_task
            if task is not None:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
                app.state.poll_task = None
            try:
                await app.state.client.aclose()
            except Exception:
                logger.exception("dashboard httpx.aclose failed")
            try:
                await app.state.redis.close()
            except Exception:
                logger.exception("dashboard redis.close failed")

    app = FastAPI(title="failover-dashboard", lifespan=lifespan)

    # Stash configuration + shared state on app.state. Tests may swap
    # ``app.state.client`` and ``app.state.redis`` after construction.
    app.state.cfg = cfg
    app.state.connections = set()
    app.state.last_snapshot = None
    app.state.poll_task = None
    app.state.client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=1.0, read=1.5, write=1.0, pool=2.0),
    )
    app.state.redis = RedisClient(cfg.redis_host, cfg.redis_port, "dashboard")
    app.state.throughput_history = []

    # =====================================================================
    # GET / — static dashboard HTML
    # =====================================================================
    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        path = os.path.join(cfg.web_dir, "index.html")
        with open(path, "rt", encoding="utf-8") as f:
            return HTMLResponse(f.read())

    @app.get("/app.js")
    async def app_js() -> FileResponse:
        path = os.path.join(cfg.web_dir, "app.js")
        return FileResponse(path, media_type="application/javascript")

    # =====================================================================
    # GET /api/snapshot — HTTP polling fallback for the JS client.
    # =====================================================================
    @app.get("/api/snapshot")
    async def api_snapshot(request: Request) -> JSONResponse:
        snap = request.app.state.last_snapshot
        if snap is None:
            # The poll loop hasn't produced a snapshot yet; build one
            # synchronously so the first poll request still returns
            # something useful.
            snap = await _collect_snapshot(request.app)
            request.app.state.last_snapshot = snap
        return JSONResponse(snap, status_code=status.HTTP_200_OK)

    # =====================================================================
    # GET /ws — primary live-update transport.
    # =====================================================================
    @app.websocket("/ws")
    async def ws(websocket: WebSocket) -> None:
        await websocket.accept()
        websocket.app.state.connections.add(websocket)
        try:
            # Push the latest snapshot immediately on connect so the UI
            # doesn't have to wait a full poll cycle to draw something.
            snap = websocket.app.state.last_snapshot
            if snap is not None:
                await websocket.send_json(snap)
            while True:
                # We don't expect client→server frames; we just need to
                # block long enough to detect disconnects. wait_for with
                # a timeout lets the loop yield periodically.
                try:
                    await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                except asyncio.TimeoutError:
                    continue
                except WebSocketDisconnect:
                    break
        except WebSocketDisconnect:
            pass
        finally:
            websocket.app.state.connections.discard(websocket)

    # =====================================================================
    # POST /proxy/admin/trigger-failover — server-side proxy to current primary.
    # =====================================================================
    @app.post("/proxy/admin/trigger-failover")
    async def proxy_failover(request: Request) -> JSONResponse:
        cfg_local: DashboardConfig = request.app.state.cfg
        rc: RedisClient = request.app.state.redis
        client: httpx.AsyncClient = request.app.state.client

        try:
            holder = await rc.read_lock_holder()
        except Exception:
            logger.exception("read_lock_holder raised inside /proxy/admin/trigger-failover")
            holder = None

        if not holder:
            return JSONResponse(
                {"status": "error", "reason": "no_primary"},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        # Map holder -> peer (host, port). The convention in
        # docker-compose is that node_id matches the service hostname,
        # so we look up the peer entry whose host equals holder.
        target: Optional[tuple[str, int]] = None
        for host, port in cfg_local.peers():
            if host == holder:
                target = (host, port)
                break

        if target is None:
            return JSONResponse(
                {
                    "status": "error",
                    "reason": "primary_not_in_peers",
                    "holder": holder,
                },
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        url = f"http://{target[0]}:{target[1]}/admin/trigger-failover"
        try:
            r = await client.post(url, json={}, timeout=5.0)
        except httpx.HTTPError as e:
            return JSONResponse(
                {
                    "status": "error",
                    "reason": "proxy_failed",
                    "detail": str(e),
                    "holder": holder,
                },
                status_code=status.HTTP_502_BAD_GATEWAY,
            )

        return JSONResponse(
            {"status": "forwarded", "code": r.status_code, "holder": holder},
            status_code=r.status_code,
        )

    return app


# =========================================================================
# Polling + broadcast
# =========================================================================


async def _poll_loop(app: FastAPI) -> None:
    """Poll every peer at ``cfg.poll_interval`` and broadcast to WS clients.

    Crashes are swallowed: we never want a flake on one peer to take down
    the whole dashboard. ``asyncio.CancelledError`` is re-raised so the
    lifespan teardown can cancel us cleanly.
    """
    cfg: DashboardConfig = app.state.cfg
    while True:
        try:
            snapshot = await _collect_snapshot(app)
            app.state.last_snapshot = snapshot
            await _broadcast(app, snapshot)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("dashboard poll iteration failed")
        try:
            await asyncio.sleep(cfg.poll_interval)
        except asyncio.CancelledError:
            raise


async def _collect_snapshot(app: FastAPI) -> dict:
    """Build a single snapshot dict by polling every peer in parallel."""
    cfg: DashboardConfig = app.state.cfg
    client: httpx.AsyncClient = app.state.client
    peers = cfg.peers()
    now = time.time()

    async def fetch_one(host: str, port: int) -> dict:
        # node_id defaults to the host (docker-compose service name); /role
        # overwrites it with the canonical value if the call succeeds.
        result: dict = {
            "node_id": host,
            "host": host,
            "port": port,
            "state": "UNREACHABLE",
            "role": None,
            "lock_holder": None,
            "known_winner": None,
            "term": 0,
            "log_count": 0,
        }
        url_role = f"http://{host}:{port}/role"
        try:
            r = await client.get(url_role, timeout=1.5)
            if r.status_code == 200:
                try:
                    payload = r.json()
                    if isinstance(payload, dict):
                        result.update(payload)
                except Exception:
                    logger.exception("dashboard: bad /role JSON from %s:%s", host, port)
        except Exception:
            # leave state == UNREACHABLE
            pass

        url_metrics = f"http://{host}:{port}/metrics"
        try:
            r = await client.get(url_metrics, timeout=1.5)
            if r.status_code == 200:
                # Cheap text parse — pull the first ``logs_ingested_total{...} N``
                # line we see. Robust enough for a learning project; real
                # dashboards would scrape via the prometheus client.
                for line in r.text.splitlines():
                    if line.startswith("logs_ingested_total{"):
                        try:
                            result["log_count"] = int(line.rsplit(" ", 1)[1])
                        except (ValueError, IndexError):
                            pass
                        break
        except Exception:
            pass
        return result

    if not peers:
        nodes: list[dict] = []
    else:
        nodes = list(
            await asyncio.gather(
                *(fetch_one(h, p) for h, p in peers),
                return_exceptions=False,
            )
        )

    # Throughput: derived from the rolling history of total log_count.
    # Only the primary ingests, so summing across nodes gives a clean
    # cluster-wide count without double-counting.
    total_logs = sum(int(n.get("log_count", 0)) for n in nodes)
    history: list[tuple[float, int]] = app.state.throughput_history
    history.append((now, total_logs))
    cutoff = now - cfg.throughput_window_sec
    while history and history[0][0] < cutoff:
        history.pop(0)
    if len(history) >= 2:
        dt = history[-1][0] - history[0][0]
        dn = history[-1][1] - history[0][1]
        throughput = (dn / dt) if dt > 0 else 0.0
        # Failover replays the snapshot, which can make total_logs
        # decrease briefly; clamp to 0 so the chart doesn't render a
        # negative spike.
        if throughput < 0:
            throughput = 0.0
    else:
        throughput = 0.0

    return {
        "nodes": nodes,
        "throughput_lps": round(throughput, 2),
        "timestamp": now,
    }


async def _broadcast(app: FastAPI, snapshot: dict) -> None:
    """Send ``snapshot`` to every connected WebSocket; drop the ones that fail."""
    dead: list[WebSocket] = []
    for ws in list(app.state.connections):
        try:
            await ws.send_json(snapshot)
        except Exception:
            dead.append(ws)
    for ws in dead:
        app.state.connections.discard(ws)


__all__ = ["DashboardConfig", "create_app"]
