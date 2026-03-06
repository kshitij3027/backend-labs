import os
from fastapi import FastAPI, HTTPException
from app.models import VectorClock, LogEntry


def create_node_app(node_id: str) -> FastAPI:
    app = FastAPI(title=f"Quorum Node {node_id}")

    state = {
        "node_id": node_id,
        "is_healthy": True,
        "data": {},  # key -> entry dict
        "vector_clock": VectorClock(),
    }

    @app.post("/store")
    async def store_entry(entry: dict):
        if not state["is_healthy"]:
            raise HTTPException(status_code=503, detail="Node is unhealthy")

        key = entry["key"]
        # Increment own vector clock
        state["vector_clock"].increment(state["node_id"])
        # Merge incoming vector clock
        incoming_vc = VectorClock.from_dict(entry.get("vector_clock", {}))
        state["vector_clock"].update(incoming_vc)

        # Store with updated clock
        stored = {
            "key": entry["key"],
            "value": entry["value"],
            "timestamp": entry.get("timestamp", 0),
            "vector_clock": state["vector_clock"].to_dict(),
            "node_id": state["node_id"],
        }
        state["data"][key] = stored

        return {"success": True, "vector_clock": state["vector_clock"].to_dict()}

    @app.get("/store/{key}")
    async def get_entry(key: str):
        if not state["is_healthy"]:
            raise HTTPException(status_code=503, detail="Node is unhealthy")
        if key not in state["data"]:
            raise HTTPException(status_code=404, detail="Key not found")
        return state["data"][key]

    @app.get("/store")
    async def list_keys():
        return {"keys": list(state["data"].keys())}

    @app.get("/health")
    async def health():
        return {
            "node_id": state["node_id"],
            "is_healthy": state["is_healthy"],
            "keys_count": len(state["data"]),
        }

    @app.post("/admin/fail")
    async def admin_fail():
        state["is_healthy"] = False
        return {"node_id": state["node_id"], "is_healthy": False}

    @app.post("/admin/recover")
    async def admin_recover():
        state["is_healthy"] = True
        return {"node_id": state["node_id"], "is_healthy": True}

    @app.get("/admin/data")
    async def admin_data():
        return dict(state["data"])

    return app


node_id = os.environ.get("NODE_ID", "node-unknown")
app = create_node_app(node_id)
