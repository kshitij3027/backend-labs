"""Log consumer for the chaos testing framework target stack.

Continuously BLPOPs JSON messages off `logs` and tracks counters so the
recovery validator can compare against the producer (data-loss check)."""

from __future__ import annotations

import asyncio
import json
import os
from collections import deque
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
LOGS_KEY = os.getenv("LOGS_KEY", "logs")
BLPOP_TIMEOUT_S = int(os.getenv("BLPOP_TIMEOUT_S", "1"))  # seconds; 0 = block forever

state = {
    "counter": 0,
    "last_ids": deque(maxlen=100),
    "last_error": None,
    "running": False,
}


async def consume_loop(r: redis.Redis) -> None:
    state["running"] = True
    try:
        while True:
            try:
                # BLPOP returns (key, value) or None on timeout.
                popped = await r.blpop(LOGS_KEY, timeout=BLPOP_TIMEOUT_S)
            except Exception as exc:
                state["last_error"] = repr(exc)
                await asyncio.sleep(0.25)  # backoff on connection error
                continue
            if popped is None:
                continue
            _, raw = popped
            try:
                msg = json.loads(raw)
                state["counter"] += 1
                if "id" in msg:
                    state["last_ids"].append(msg["id"])
                state["last_error"] = None
            except Exception as exc:
                state["last_error"] = repr(exc)
    except asyncio.CancelledError:
        state["running"] = False
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    task = asyncio.create_task(consume_loop(r))
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


app = FastAPI(title="log-consumer", version="0.1.0", lifespan=lifespan)


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


@app.get("/counter")
async def counter():
    return {"counter": state["counter"], "last_error": state["last_error"]}


@app.get("/ids")
async def ids():
    return {"last_ids": list(state["last_ids"])}
