"""aiohttp web dashboard with REST API and WebSocket for live stats."""

import asyncio
import json
import os
import time

from aiohttp import web

from src.config import Settings
from src.dlq_handler import DLQHandler
from src.models import FailureType
from src.redis_client import RedisClient
from src.stats import StatsTracker


class Dashboard:
    """aiohttp-based web dashboard with REST API and WebSocket."""

    def __init__(self, redis_client: RedisClient, settings: Settings):
        self.redis = redis_client
        self.settings = settings
        self.stats = StatsTracker(redis_client, settings)
        self.dlq_handler = DLQHandler(redis_client, settings)
        self.app = web.Application()
        self._ws_clients: set[web.WebSocketResponse] = set()
        self._setup_routes()

    def _setup_routes(self):
        self.app.router.add_get("/", self.index)
        self.app.router.add_get("/health", self.health)
        self.app.router.add_get("/api/stats", self.api_stats)
        self.app.router.add_get("/api/dlq", self.api_dlq)
        self.app.router.add_get("/api/dlq/analysis", self.api_dlq_analysis)
        self.app.router.add_post("/api/dlq/reprocess", self.api_dlq_reprocess)
        self.app.router.add_post(
            "/api/dlq/reprocess/{failure_type}", self.api_dlq_reprocess_by_type
        )
        self.app.router.add_post("/api/dlq/purge", self.api_dlq_purge)
        self.app.router.add_get("/api/trends", self.api_trends)
        self.app.router.add_get("/api/alerts", self.api_alerts)
        self.app.router.add_get("/ws", self.websocket_handler)

    async def index(self, request: web.Request) -> web.Response:
        """Serve the dashboard HTML from templates/index.html."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "templates", "index.html"
        )
        with open(template_path) as f:
            html = f.read()
        return web.Response(text=html, content_type="text/html")

    async def health(self, request: web.Request) -> web.Response:
        """Return a simple health-check response."""
        return web.json_response({"status": "ok", "timestamp": time.time()})

    async def api_stats(self, request: web.Request) -> web.Response:
        """Return current processing statistics plus DLQ and queue sizes."""
        stats = await self.stats.get_stats()
        dlq_count = await self.dlq_handler.get_dlq_count()
        queue_length = await self.redis.get_queue_length(self.settings.main_queue)
        stats["dlq_size"] = dlq_count
        stats["queue_length"] = queue_length
        return web.json_response(stats)

    async def api_dlq(self, request: web.Request) -> web.Response:
        """Return all DLQ messages as a JSON array."""
        messages = await self.dlq_handler.get_dlq_messages()
        return web.json_response([json.loads(m.to_json()) for m in messages])

    async def api_dlq_analysis(self, request: web.Request) -> web.Response:
        """Return DLQ analysis breakdown."""
        analysis = await self.dlq_handler.analyze_dlq()
        return web.json_response(analysis)

    async def api_dlq_reprocess(self, request: web.Request) -> web.Response:
        """Move all DLQ messages back to the main queue for reprocessing."""
        count = await self.dlq_handler.reprocess_all()
        return web.json_response({"reprocessed": count})

    async def api_dlq_reprocess_by_type(self, request: web.Request) -> web.Response:
        """Move DLQ messages of a specific failure type back for reprocessing."""
        ft_str = request.match_info["failure_type"].upper()
        try:
            ft = FailureType(ft_str)
        except ValueError:
            return web.json_response(
                {"error": f"Invalid failure type: {ft_str}"}, status=400
            )
        count = await self.dlq_handler.reprocess_by_type(ft)
        return web.json_response({"reprocessed": count, "failure_type": ft_str})

    async def api_dlq_purge(self, request: web.Request) -> web.Response:
        """Delete all messages from the DLQ."""
        count = await self.dlq_handler.purge()
        return web.json_response({"purged": count})

    async def api_trends(self, request: web.Request) -> web.Response:
        """Return failure trends over a rolling window."""
        window = float(request.query.get("window", "300"))
        trends = await self.stats.get_failure_trends(window_seconds=window)
        return web.json_response(trends)

    async def api_alerts(self, request: web.Request) -> web.Response:
        """Return currently active alerts."""
        alerts = await self.stats.check_alerts()
        return web.json_response(alerts)

    async def websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        """Accept a WebSocket connection and add it to the broadcast set."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws_clients.add(ws)
        try:
            async for _msg in ws:
                pass  # We only broadcast, don't process incoming messages
        finally:
            self._ws_clients.discard(ws)
        return ws

    async def broadcast_stats(self, stop_event: asyncio.Event) -> None:
        """Periodically broadcast stats to all connected WebSocket clients."""
        while not stop_event.is_set():
            if self._ws_clients:
                try:
                    stats = await self.stats.get_stats()
                    dlq_count = await self.dlq_handler.get_dlq_count()
                    queue_length = await self.redis.get_queue_length(
                        self.settings.main_queue
                    )
                    alerts = await self.stats.check_alerts()
                    payload = json.dumps(
                        {
                            "type": "stats_update",
                            "data": {
                                **stats,
                                "dlq_size": dlq_count,
                                "queue_length": queue_length,
                                "alerts": alerts,
                                "timestamp": time.time(),
                            },
                        }
                    )
                    dead: set[web.WebSocketResponse] = set()
                    for ws in self._ws_clients:
                        try:
                            await ws.send_str(payload)
                        except Exception:
                            dead.add(ws)
                    self._ws_clients -= dead
                except Exception:
                    pass
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self.settings.ws_broadcast_interval
                )
            except asyncio.TimeoutError:
                pass

    async def start(self, stop_event: asyncio.Event) -> None:
        """Start the dashboard server and broadcast loop."""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.settings.dashboard_port)
        await site.start()
        try:
            await self.broadcast_stats(stop_event)
        finally:
            await runner.cleanup()
