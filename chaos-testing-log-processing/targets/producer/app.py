"""Log producer for the chaos testing framework target stack.

Continuously LPUSHes monotonic-id JSON messages into the Redis list
`logs` so the consumer has something to drain. Exposes a small HTTP
surface so the framework can probe it and the recovery validator can
compare counters against the consumer."""

from __future__ import annotations

import asyncio
import json
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis.asyncio as redis
from fastapi import FastAPI

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
LOGS_KEY = os.getenv("LOGS_KEY", "logs")
PRODUCE_HZ = float(os.getenv("PRODUCE_HZ", "10"))

state = {
    "sent_count": 0,
    "last_ids": deque(maxlen=100),
    "next_id": 1,
    "last_error": None,
    "running": False,
}


async def produce_loop(r: redis.Redis) -> None:
    state["running"] = True
    period = 1.0 / max(PRODUCE_HZ, 0.1)
    try:
        while True:
            msg = {
                "id": state["next_id"],
                "ts": datetime.now(timezone.utc).isoformat(),
                "payload": f"log-{state['next_id']}",
            }
            try:
                await r.lpush(LOGS_KEY, json.dumps(msg))
                state["sent_count"] += 1
                state["last_ids"].append(state["next_id"])
                state["next_id"] += 1
                state["last_error"] = None
            except Exception as exc:
                state["last_error"] = repr(exc)
            await asyncio.sleep(period)
    except asyncio.CancelledError:
        state["running"] = False
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    task = asyncio.create_task(produce_loop(r))
    app.state.redis = r
    app.state.task = task
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await r.aclose()


app = FastAPI(title="log-producer", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health():
    try:
        ok = await app.state.redis.ping()
        return {
            "status": "ok" if ok else "degraded",
            "redis": "ok" if ok else "ping_false",
            "running": state["running"],
        }
    except Exception as exc:
        return {"status": "degraded", "redis": repr(exc), "running": state["running"]}


@app.get("/sent_count")
async def sent_count():
    return {"sent_count": state["sent_count"], "last_error": state["last_error"]}


@app.get("/last_ids")
async def last_ids():
    return {"last_ids": list(state["last_ids"])}
