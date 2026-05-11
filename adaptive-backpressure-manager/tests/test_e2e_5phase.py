import asyncio
import os
import time

import httpx
import pytest


_BASE = os.environ.get("ABPM_BASE_URL", "http://localhost:8000")


async def _wait_healthy(client: httpx.AsyncClient, timeout_s: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            r = await client.get("/system/health")
            if r.status_code == 200:
                return True
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return False


async def _tune_and_spike(client: httpx.AsyncClient) -> dict:
    """Drive the same flow as verify_e2e.py but at finer poll resolution."""
    r = await client.post(
        "/api/v1/admin/config",
        json={
            "processing_latency_seconds": 0.1,
            "sampling_interval": 0.5,
            "min_dwell_seconds": 1.0,
            "max_queue_size": 200,
            "ewma_alpha": 0.5,
        },
    )
    r.raise_for_status()

    await client.post(
        "/api/v1/loadtest/start",
        json={"profile": "spike", "rps": 500, "duration_seconds": 12, "spike_multiplier": 1.0},
    )

    timeline = []
    t0 = time.monotonic()
    for _ in range(140):
        try:
            s = (await client.get("/api/v1/system/status")).json()
            m = (await client.get("/api/v1/metrics/json")).json()
            timeline.append({
                "t": time.monotonic() - t0,
                "level": s["backpressure"]["pressure_level"],
                "score": s["backpressure"]["pressure_score"],
                "queue_size": s["backpressure"]["queue_size"],
                "dropped_critical": m.get("admission_counters", {}).get("dropped_per_priority", {}).get("critical", 0),
                "rejected_critical": m.get("admission_counters", {}).get("rejected_per_priority", {}).get("critical", 0),
            })
        except Exception:
            pass
        if time.monotonic() - t0 > 13.5:
            try:
                await client.post("/api/v1/loadtest/stop")
            except Exception:
                pass
        if time.monotonic() - t0 > 35.0:
            break
        await asyncio.sleep(0.25)

    return {"timeline": timeline}


def _state_dwells(timeline: list) -> dict:
    """Compute the dwell duration per state across the timeline."""
    dwells: list[tuple[str, float]] = []
    if not timeline:
        return {"per_transition": dwells, "min_dwell_s": float("inf")}
    cur = timeline[0]["level"]
    start = timeline[0]["t"]
    for snap in timeline[1:]:
        if snap["level"] != cur:
            dwells.append((cur, snap["t"] - start))
            cur = snap["level"]
            start = snap["t"]
    dwells.append((cur, timeline[-1]["t"] - start))
    min_dwell = min((d for _, d in dwells), default=float("inf"))
    return {"per_transition": dwells, "min_dwell_s": min_dwell}


def _transition_order(timeline: list) -> list:
    seq = []
    last = None
    for snap in timeline:
        if snap["level"] != last:
            seq.append(snap["level"])
            last = snap["level"]
    return seq


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_full_invariants():
    async with httpx.AsyncClient(base_url=_BASE, timeout=10.0) as client:
        healthy = await _wait_healthy(client)
        assert healthy, "app never became healthy"
        result = await _tune_and_spike(client)
    timeline = result["timeline"]
    assert len(timeline) >= 30, f"timeline too short: {len(timeline)} snapshots"

    # Invariant 1: CRITICAL never dropped or rejected.
    max_dropped_critical = max(s["dropped_critical"] for s in timeline)
    max_rejected_critical = max(s["rejected_critical"] for s in timeline)
    assert max_dropped_critical == 0, f"CRITICAL was dropped {max_dropped_critical} times"
    assert max_rejected_critical == 0, f"CRITICAL was rejected {max_rejected_critical} times"

    # Invariant 2: transition sequence touches at least normal -> pressure -> overload -> recovery -> normal (in some order touching all 4).
    seq = _transition_order(timeline)
    assert "overload" in seq, f"never saw overload, sequence={seq}"
    assert "recovery" in seq, f"never saw recovery, sequence={seq}"
    assert seq[-1] == "normal", f"did not end at normal, sequence={seq}"

    # Invariant 3: no state had a dwell < 1.0s (oscillation guard). Allow the final 'normal' window to be short if the polling tail is short.
    info = _state_dwells(timeline)
    transitions = info["per_transition"]
    # All transitions except possibly the very last (where polling ends prematurely) must have dwell >= 0.9s
    # (0.9 instead of 1.0 to tolerate sampling granularity at 0.25s resolution).
    for state, dwell in transitions[:-1]:
        assert dwell >= 0.9, f"state {state} dwell={dwell:.2f}s violates oscillation guard"

    # Invariant 4: no second overload appears after returning to normal post-recovery.
    found_recovery_then_normal_at = None
    for idx, snap in enumerate(timeline):
        if snap["level"] == "normal" and any(s["level"] == "recovery" for s in timeline[:idx]):
            found_recovery_then_normal_at = snap["t"]
            break
    if found_recovery_then_normal_at is not None:
        re_overload = [s for s in timeline if s["t"] > found_recovery_then_normal_at and s["level"] == "overload"]
        assert not re_overload, f"second overload observed at {re_overload[0]['t']:.1f}s after first normal"
