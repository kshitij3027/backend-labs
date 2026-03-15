"""FastAPI dashboard with real-time SSE streaming for Kafka log monitoring."""

import asyncio
import json
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from src.config import Settings
from src.consumer import DashboardConsumer
from src.error_aggregator import ErrorAggregator
from src.metrics import MetricsTracker

logger = structlog.get_logger()

# Resolve the templates directory relative to the project root.
# Inside Docker this is /app/templates; locally it sits next to the src/ package.
_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"


def _create_lifespan(settings: Settings):
    """Return a lifespan context manager that wires up consumers and metrics."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # --- startup ---
        dashboard_consumer = DashboardConsumer(settings)
        error_aggregator = ErrorAggregator(settings)
        metrics_tracker = MetricsTracker(
            bootstrap_servers=settings.active_bootstrap_servers,
        )

        dashboard_consumer.start()
        error_aggregator.start()

        app.state.dashboard_consumer = dashboard_consumer
        app.state.error_aggregator = error_aggregator
        app.state.metrics_tracker = metrics_tracker

        # Background task: periodically sample dashboard_consumer.stats to
        # feed the MetricsTracker (avoids modifying consumer.py).
        stop_event = asyncio.Event()
        app.state.stop_event = stop_event

        async def _sample_throughput():
            """Sample the consumer's total count every second and record deltas."""
            prev_total = dashboard_consumer.stats["total"]
            while not stop_event.is_set():
                await asyncio.sleep(1.0)
                current_total = dashboard_consumer.stats["total"]
                delta = current_total - prev_total
                for _ in range(delta):
                    metrics_tracker.record_consumed("all")
                prev_total = current_total

        sampling_task = asyncio.create_task(_sample_throughput())

        async def _update_lag():
            """Periodically update consumer lag from the dashboard consumer."""
            while not stop_event.is_set():
                await asyncio.sleep(5.0)
                # The consumer's internal _consumer is a confluent_kafka Consumer
                if hasattr(dashboard_consumer, "_consumer") and dashboard_consumer._consumer:
                    try:
                        metrics_tracker.update_consumer_lag(dashboard_consumer._consumer)
                    except Exception:
                        pass

        lag_task = asyncio.create_task(_update_lag())

        logger.info("dashboard_startup_complete")
        yield

        # --- shutdown ---
        stop_event.set()
        sampling_task.cancel()
        lag_task.cancel()
        try:
            await sampling_task
        except asyncio.CancelledError:
            pass
        try:
            await lag_task
        except asyncio.CancelledError:
            pass

        metrics_tracker.stop()
        dashboard_consumer.stop()
        error_aggregator.stop()
        logger.info("dashboard_shutdown_complete")

    return lifespan


def create_app(settings: Settings) -> FastAPI:
    """Application factory -- returns a fully configured FastAPI instance."""

    app = FastAPI(
        title="Kafka Log Streaming Dashboard",
        lifespan=_create_lifespan(settings),
    )

    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.get("/")
    async def index(request: Request):
        """Serve the single-page dashboard UI."""
        return templates.TemplateResponse(request, "index.html")

    @app.get("/api/logs")
    async def api_logs():
        """Return recent log messages from the dashboard consumer."""
        consumer: DashboardConsumer = app.state.dashboard_consumer
        return JSONResponse(content=consumer.recent_messages)

    @app.get("/api/stats")
    async def api_stats():
        """Return aggregated statistics and throughput."""
        consumer: DashboardConsumer = app.state.dashboard_consumer
        tracker: MetricsTracker = app.state.metrics_tracker
        stats = consumer.stats
        throughput = tracker.throughput
        return JSONResponse(content={
            "total": stats["total"],
            "by_service": stats["by_service"],
            "by_level": stats["by_level"],
            "messages_per_second": throughput["messages_per_second"],
        })

    @app.get("/api/errors")
    async def api_errors():
        """Return error aggregation data."""
        aggregator: ErrorAggregator = app.state.error_aggregator
        return JSONResponse(content={
            "recent_errors": aggregator.recent_errors,
            "error_counts": aggregator.error_counts,
            "error_rate": aggregator.error_rate,
        })

    @app.get("/api/metrics")
    async def api_metrics():
        """Detailed metrics: throughput history, consumer lag, latency stats."""
        tracker: MetricsTracker = app.state.metrics_tracker
        return JSONResponse(content={
            "throughput": tracker.throughput,
            "throughput_history": tracker.throughput_history,
            "consumer_lag": tracker.consumer_lag,
            "latency": tracker.latency_stats,
        })

    @app.get("/api/ordering")
    async def api_ordering():
        """Verify message ordering within partitions.

        Check that sequence_numbers within each (topic, key)
        combination are monotonically increasing in the recent messages.
        """
        consumer: DashboardConsumer = app.state.dashboard_consumer
        messages = consumer.recent_messages

        # Group by (topic, key) -- messages with same key go to same partition
        sequences: dict[tuple, list] = defaultdict(list)
        for msg in messages:
            key = (msg.get("topic", ""), msg.get("key", ""))
            seq = msg.get("data", {}).get("sequence_number", 0)
            sequences[key].append(seq)

        # Check ordering for each group
        violations = []
        ordered_count = 0
        total_groups = 0

        for key, seqs in sequences.items():
            total_groups += 1
            is_ordered = all(seqs[i] <= seqs[i + 1] for i in range(len(seqs) - 1))
            if is_ordered:
                ordered_count += 1
            else:
                violations.append({
                    "topic": key[0],
                    "key": key[1][:8] + "..." if key[1] and len(key[1]) > 8 else key[1],
                    "sequences_sample": seqs[:10],
                })

        return JSONResponse(content={
            "ordered": len(violations) == 0,
            "total_groups": total_groups,
            "ordered_groups": ordered_count,
            "violations": violations[:10],
            "message_count": len(messages),
        })

    @app.get("/api/stream")
    async def api_stream():
        """Server-Sent Events endpoint for real-time log streaming."""

        async def _event_generator():
            consumer: DashboardConsumer = app.state.dashboard_consumer
            # Start from 0 so newly connected clients get the current buffer.
            last_index = 0
            last_heartbeat = time.time()

            while True:
                messages = consumer.recent_messages
                current_len = len(messages)

                if current_len > last_index:
                    # New messages available
                    new_msgs = messages[last_index:current_len]
                    for msg in new_msgs:
                        # Serialize with default=str to handle enum/datetime types
                        payload = json.dumps(msg, default=str)
                        yield f"data: {payload}\n\n"
                    last_index = current_len
                    last_heartbeat = time.time()
                elif current_len < last_index:
                    # Buffer wrapped around (deque maxlen exceeded)
                    last_index = current_len
                else:
                    # No new messages -- send heartbeat every 5 seconds
                    if time.time() - last_heartbeat >= 5.0:
                        yield ": heartbeat\n\n"
                        last_heartbeat = time.time()

                await asyncio.sleep(0.5)

        return StreamingResponse(
            _event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/health")
    async def health():
        """Health check endpoint."""
        consumer: DashboardConsumer = app.state.dashboard_consumer
        aggregator: ErrorAggregator = app.state.error_aggregator
        return JSONResponse(content={
            "status": "ok",
            "consumers": {
                "dashboard": consumer.is_running,
                "error_aggregator": aggregator.is_running,
            },
        })

    return app
