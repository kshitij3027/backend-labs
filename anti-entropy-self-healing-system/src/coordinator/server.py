import time
import structlog
from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
from src.config import Config
from src.coordinator.client import NodeClient
from src.coordinator.scanner import AntiEntropyScanner
from src.coordinator.repair import RepairWorker
from src.coordinator.read_repair import ReadRepairHandler
from src.coordinator.strategies import Strategy
from src.metrics import ConsistencyMetrics

logger = structlog.get_logger()

# Initialize components
clients = [NodeClient(url) for url in Config.NODE_URLS]
scanner = AntiEntropyScanner(clients)
repair_worker = RepairWorker()
read_repair_handler = ReadRepairHandler(clients)
metrics = ConsistencyMetrics()

app = Flask(__name__)


def run_scan_cycle():
    """Run a full scan + repair cycle. Called by scheduler and manual trigger."""
    start = time.time()
    repair_tasks = scanner.run_scan()

    # Count pairwise comparisons (n choose 2)
    n = len(clients)
    metrics.record_comparison(n * (n - 1) // 2)

    inconsistencies = len(repair_tasks)
    if inconsistencies > 0:
        metrics.record_inconsistency(inconsistencies)

    repair_worker.add_tasks(repair_tasks)
    completed, failed = repair_worker.execute_repairs()
    duration = time.time() - start

    metrics.record_repair(completed, failed, duration)
    metrics.record_scan(inconsistencies, completed, failed, duration)

    logger.info(
        "scan_cycle.complete",
        inconsistencies=inconsistencies,
        repairs_completed=completed,
        repairs_failed=failed,
        duration=round(duration, 3),
    )
    return {
        "inconsistencies": inconsistencies,
        "repairs_completed": completed,
        "repairs_failed": failed,
        "duration": round(duration, 4),
    }


# APScheduler
scheduler = BackgroundScheduler()
scheduler.add_job(run_scan_cycle, "interval", seconds=Config.SCAN_INTERVAL, id="anti_entropy_scan")

# --- Routes ---


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "role": "coordinator"})


@app.route("/api/status", methods=["GET"])
def api_status():
    """System health + node connectivity."""
    node_statuses = []
    for client in clients:
        h = client.health()
        node_statuses.append({
            "node_id": client.node_id,
            "url": client.node_url,
            "healthy": h is not None,
        })
    return jsonify({
        "coordinator": "healthy",
        "nodes": node_statuses,
        "scan_interval": Config.SCAN_INTERVAL,
    })


@app.route("/api/data/<key>", methods=["GET"])
def api_read(key: str):
    """Read with read repair."""
    result = read_repair_handler.read_with_repair(key)
    if result is None:
        return jsonify({"error": "not found", "key": key}), 404
    return jsonify(result)


@app.route("/api/data/<key>", methods=["PUT"])
def api_write(key: str):
    """Write to ALL nodes."""
    body = request.get_json(force=True)
    value = body.get("value", "")
    timestamp = time.time()
    version = 1  # Will be overridden by node's auto-increment if not specified

    results = []
    for client in clients:
        success = client.put_data(key, value, version, timestamp)
        results.append({"node_id": client.node_id, "success": success})

    return jsonify({"key": key, "value": value, "nodes": results})


@app.route("/api/scan/trigger", methods=["POST"])
def api_trigger_scan():
    """Trigger an immediate scan."""
    result = run_scan_cycle()
    return jsonify(result)


@app.route("/api/metrics", methods=["GET"])
def api_metrics():
    """Return consistency metrics as JSON."""
    return jsonify(metrics.to_dict())


@app.route("/api/replicas", methods=["GET"])
def api_replicas():
    """Node statuses - queries each node's /health."""
    replicas = []
    for client in clients:
        h = client.health()
        info = {
            "node_id": client.node_id,
            "url": client.node_url,
            "healthy": h is not None,
        }
        if h is not None:
            info["status"] = h.get("status")
        replicas.append(info)
    return jsonify({"replicas": replicas})


@app.route("/api/inject", methods=["POST"])
def api_inject():
    """Inject inconsistency - write to ONE node only."""
    body = request.get_json(force=True)
    node_id = body.get("node_id")
    key = body.get("key")
    value = body.get("value", "injected")

    target_client = None
    for client in clients:
        if client.node_id == node_id:
            target_client = client
            break

    if target_client is None:
        return jsonify({"error": f"node {node_id} not found"}), 404

    success = target_client.put_data(key, value, version=999, timestamp=time.time())
    return jsonify({"injected": success, "node_id": node_id, "key": key, "value": value})


if __name__ == "__main__":
    scheduler.start()
    try:
        app.run(host=Config.COORDINATOR_HOST, port=Config.COORDINATOR_PORT)
    finally:
        scheduler.shutdown()
