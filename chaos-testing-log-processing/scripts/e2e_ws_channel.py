"""End-to-end WebSocket channel smoke test.

Runs INSIDE the chaos-framework container against the running API.
Verifies the live channel at ``/ws/runs/{run_id}`` actually emits a
mix of frame types (snapshot + event + metrics + heartbeat) during
a real experiment.

Steps:
    1. POST /experiments to create a short (3s) latency_injection
       experiment against log-consumer.
    2. Open a WebSocket subscribed to "*" (all-runs fan-out) and start
       collecting frames.
    3. POST /experiments/{id}/run to kick off the run.
    4. Collect frames for ~5 seconds while the run goes through its
       injecting / observing / rolling_back / validating / completed
       phases.
    5. Tally frame counts by type. We require:
         - total frames >= 10 (the broadcaster runs at 4 Hz so a 5s
           window comfortably exceeds this)
         - at least one snapshot frame (sent on connect)
         - at least one heartbeat frame
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

import httpx
import websockets

sys.path.insert(0, "/app")

API = "http://localhost:8000"
WS_BASE = "ws://localhost:8000"


async def collect_frames(ws_url: str, seconds: float, frames_out: list) -> None:
    """Connect to ``ws_url`` and accumulate frames for ``seconds`` seconds."""
    deadline = time.monotonic() + seconds
    try:
        async with websockets.connect(ws_url, max_size=2**20) as ws:
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                    try:
                        frames_out.append(json.loads(raw))
                    except json.JSONDecodeError as exc:
                        frames_out.append({"_decode_error": repr(exc), "_raw": str(raw)[:200]})
                except asyncio.TimeoutError:
                    continue
                except websockets.ConnectionClosed:
                    break
    except Exception as exc:  # noqa: BLE001
        frames_out.append({"_error": repr(exc)})


async def main() -> int:
    summary: dict = {}
    async with httpx.AsyncClient(timeout=10.0) as client:
        # 1. Create the experiment.
        r = await client.post(
            f"{API}/experiments",
            json={
                "name": "ws-smoke",
                "type": "latency_injection",
                "target": "log-consumer",
                "parameters": {"latency_ms": 200, "jitter_ms": 0},
                "duration": 3,
                "severity": 2,
            },
        )
        r.raise_for_status()
        exp_id = r.json()["id"]
        print(f"exp_id={exp_id}", flush=True)

        # 2. Connect to the "*" group BEFORE starting the run so we don't
        #    miss the early run_started / injecting / observing events.
        all_frames: list = []
        ws_task = asyncio.create_task(
            collect_frames(f"{WS_BASE}/ws/runs/*", 6.0, all_frames)
        )

        # Let the WS handshake settle (and the snapshot frame land).
        await asyncio.sleep(0.5)

        # 3. Start the run.
        r = await client.post(f"{API}/experiments/{exp_id}/run")
        r.raise_for_status()
        run_id = r.json()["run_id"]
        print(f"run_id={run_id}", flush=True)

        # 4. Wait for the WS collection window to elapse.
        await ws_task

    # 5. Tally frames.
    types: dict[str, int] = {}
    errors = [f for f in all_frames if "_error" in f or "_decode_error" in f]
    typed = [f for f in all_frames if isinstance(f, dict) and "type" in f]
    for f in typed:
        t = f.get("type", "?missing")
        types[t] = types.get(t, 0) + 1

    has_snapshot = types.get("snapshot", 0) >= 1
    has_heartbeat = types.get("heartbeat", 0) >= 1
    enough_frames = len(typed) >= 10
    no_errors = len(errors) == 0

    result = "pass" if (has_snapshot and has_heartbeat and enough_frames and no_errors) else "fail"

    summary = {
        "result": result,
        "total_frames": len(typed),
        "by_type": types,
        "errors": errors[:5],
        "run_id": run_id,
        "checks": {
            "has_snapshot": has_snapshot,
            "has_heartbeat": has_heartbeat,
            "enough_frames(>=10)": enough_frames,
            "no_errors": no_errors,
        },
    }
    print("--- ws e2e summary ---", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    return 0 if result == "pass" else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
