import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.coordinator import QuorumCoordinator, NodeConnection
from app.models import ConsistencyLevel, QuorumConfig
from app.metrics import QuorumMetrics


coordinator: QuorumCoordinator | None = None
config: QuorumConfig | None = None
metrics: QuorumMetrics | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global coordinator, config, metrics

    node_urls = os.environ.get(
        "NODE_URLS",
        "http://node-1:8001,http://node-2:8001,http://node-3:8001,http://node-4:8001,http://node-5:8001",
    )
    connections = []
    for i, url in enumerate(node_urls.split(","), 1):
        url = url.strip()
        node_id = f"node-{i}"
        connections.append(NodeConnection(node_id=node_id, base_url=url))

    config = QuorumConfig()
    metrics = QuorumMetrics()
    coordinator = QuorumCoordinator(connections, config, metrics)

    yield

    await coordinator.close()


app = FastAPI(title="Quorum Consistency System", lifespan=lifespan)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "coordinator"}


@app.get("/api/cluster/status")
async def cluster_status():
    return await coordinator.get_cluster_status()


@app.post("/api/cluster/config")
async def update_config(body: dict):
    level_str = body.get("level", "balanced")
    try:
        level = ConsistencyLevel(level_str)
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"error": f"Invalid level: {level_str}. Use strong, balanced, or eventual"},
        )
    config.update_for_consistency_level(level)
    coordinator.config = config
    return config.to_dict()


@app.post("/api/logs")
async def write_log(body: dict):
    key = body.get("key")
    value = body.get("value")
    if not key or value is None:
        return JSONResponse(status_code=400, content={"error": "key and value required"})
    return await coordinator.write(key, value)


@app.get("/api/logs/{key}")
async def read_log(key: str):
    return await coordinator.read(key)


@app.get("/api/logs")
async def list_logs():
    keys = await coordinator.list_keys()
    return {"keys": keys}


@app.post("/api/nodes/{node_id}/fail")
async def fail_node(node_id: str):
    return await coordinator.fail_node(node_id)


@app.post("/api/nodes/{node_id}/recover")
async def recover_node(node_id: str):
    return await coordinator.recover_node(node_id)


@app.get("/api/nodes/{node_id}/data")
async def node_data(node_id: str):
    return await coordinator.get_node_data(node_id)


@app.get("/metrics")
async def get_metrics():
    return metrics.to_dict()


# Aliases
@app.post("/write")
async def write_alias(body: dict):
    return await write_log(body)


@app.post("/read")
async def read_alias(body: dict):
    key = body.get("key")
    if not key:
        return JSONResponse(status_code=400, content={"error": "key required"})
    return await coordinator.read(key)


@app.post("/consistency")
async def consistency_alias(body: dict):
    return await update_config(body)
