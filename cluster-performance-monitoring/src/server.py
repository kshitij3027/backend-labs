"""FastAPI application for the cluster performance monitoring system."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from src.config import Config
from src.models import NodeInfo
from src.storage import MetricStore
from src.simulator import NodeSimulator
from src.collector import MetricCollector
from src.aggregator import MetricAggregator
from src.analyzer import PerformanceAnalyzer
from src.reporter import ReportGenerator

# Module-level globals (set during lifespan)
config: Config = None  # type: ignore[assignment]
store: MetricStore = None  # type: ignore[assignment]
simulators: list[NodeSimulator] = []
collectors: list[MetricCollector] = []
aggregator: MetricAggregator = None  # type: ignore[assignment]
analyzer: PerformanceAnalyzer = None  # type: ignore[assignment]
reporter: ReportGenerator = None  # type: ignore[assignment]

NODE_DEFINITIONS = [
    NodeInfo(node_id="node-1", role="primary", host="localhost", port=8001),
    NodeInfo(node_id="node-2", role="replica", host="localhost", port=8002),
    NodeInfo(node_id="node-3", role="replica", host="localhost", port=8003),
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown."""
    global config, store, simulators, collectors, aggregator, analyzer, reporter

    # Startup
    config = Config.load()
    store = MetricStore(
        max_points_per_series=int(config.retention_seconds / config.collection_interval)
    )

    for node_def in NODE_DEFINITIONS[: config.num_nodes]:
        sim = NodeSimulator(node_def)
        simulators.append(sim)
        collector = MetricCollector(sim, store, interval=config.collection_interval)
        collectors.append(collector)
        await collector.start()

    aggregator = MetricAggregator(store, window_seconds=config.aggregation_window)
    analyzer = PerformanceAnalyzer(aggregator, config)
    reporter = ReportGenerator(analyzer)

    yield

    # Shutdown
    for collector in collectors:
        await collector.stop()


app = FastAPI(title="Cluster Performance Monitor", lifespan=lifespan)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "nodes": len(collectors)}


@app.get("/api/metrics")
async def get_metrics():
    """Return cluster-wide metric totals."""
    totals = aggregator.get_cluster_totals()
    return totals


@app.get("/api/nodes")
async def get_nodes():
    """Return list of monitored nodes."""
    return [
        {
            "node_id": sim.node_info.node_id,
            "role": sim.node_info.role,
            "host": sim.node_info.host,
            "port": sim.node_info.port,
        }
        for sim in simulators
    ]


@app.get("/api/nodes/{node_id}/metrics")
async def get_node_metrics(node_id: str):
    """Return aggregated metrics for a specific node."""
    all_stats = aggregator.get_all_node_stats()
    node_stats = [s for s in all_stats if s.node_id == node_id]
    if not node_stats:
        return {"node_id": node_id, "metrics": {}}
    return {
        "node_id": node_id,
        "metrics": {
            s.metric_name: {
                "min": s.min,
                "max": s.max,
                "avg": s.avg,
                "p95": s.p95,
                "p99": s.p99,
                "count": s.count,
            }
            for s in node_stats
        },
    }


@app.get("/api/alerts")
async def get_alerts():
    """Return current alerts."""
    alerts = analyzer.get_alerts()
    return {"alerts": [a.model_dump() for a in alerts], "count": len(alerts)}


@app.get("/api/report")
async def get_report():
    """Return the latest performance report."""
    report = reporter.get_latest()
    if report is None:
        return {"message": "No reports generated yet"}
    return report.model_dump()


@app.post("/api/report/generate")
async def generate_report():
    """Generate a new performance report."""
    report = reporter.generate()
    return report.model_dump()
