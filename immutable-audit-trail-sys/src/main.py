import time
from fastapi import FastAPI

app = FastAPI(title="Immutable Audit Trail System", version="0.1.0")


@app.get("/api/health")
async def health() -> dict:
    return {"status": "healthy", "timestamp": int(time.time())}
