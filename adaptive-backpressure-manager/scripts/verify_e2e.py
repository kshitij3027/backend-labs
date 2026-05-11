"""E2E verifier: drives the adaptive backpressure pipeline through PRESSURE -> OVERLOAD -> RECOVERY."""

import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import httpx


@dataclass
class _Snapshot:
    t: float
    pressure_level: str
    pressure_score: float
    queue_size: int
    throttle_rate: float


@dataclass
class _Report:
    healthy: bool = False
    transitions: list[str] = field(default_factory=list)
    snapshots: list[_Snapshot] = field(default_factory=list)
    overload_observed: bool = False
    pressure_observed: bool = False
    recovered: bool = False
    dropped_critical: int = 0
    accepted_total: int = 0
    spike_rps: int = 0
    elapsed_s: float = 0.0


async def _wait_healthy(client: httpx.AsyncClient, timeout_s: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            r = await client.get("/system/health")
            if r.status_code == 200 and r.json().get("status") == "ok":
                return True
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return False


async def _tune_for_fast_e2e(client: httpx.AsyncClient) -> None:
    payload = {
        "processing_latency_seconds": 0.1,
        "sampling_interval": 0.5,
        "min_dwell_seconds": 1.0,
        "max_queue_size": 200,
        "ewma_alpha": 0.5,
    }
    r = await client.post("/api/v1/admin/config", json=payload)
    r.raise_for_status()


async def _drive_spike(client: httpx.AsyncClient, rps: int, duration_s: int) -> None:
    r = await client.post(
        "/api/v1/loadtest/start",
        json={"profile": "spike", "rps": rps, "duration_seconds": duration_s, "spike_multiplier": 1.0},
    )
    r.raise_for_status()


async def _stop_loadtest(client: httpx.AsyncClient) -> None:
    try:
        await client.post("/api/v1/loadtest/stop")
    except Exception:
        pass


async def _poll_status_window(
    client: httpx.AsyncClient,
    duration_s: float,
    report: _Report,
) -> None:
    t0 = time.monotonic()
    deadline = t0 + duration_s
    last_level = None
    while time.monotonic() < deadline:
        try:
            r = await client.get("/api/v1/system/status")
            if r.status_code == 200:
                body = r.json()
                snap = _Snapshot(
                    t=time.monotonic() - t0,
                    pressure_level=body["backpressure"]["pressure_level"],
                    pressure_score=body["backpressure"]["pressure_score"],
                    queue_size=body["backpressure"]["queue_size"],
                    throttle_rate=body["backpressure"]["throttle_rate"],
                )
                report.snapshots.append(snap)
                if snap.pressure_level != last_level:
                    report.transitions.append(f"{snap.t:.1f}s {last_level}->{snap.pressure_level}")
                    last_level = snap.pressure_level
                if snap.pressure_level == "pressure":
                    report.pressure_observed = True
                if snap.pressure_level == "overload":
                    report.overload_observed = True
                if snap.pressure_level in ("normal", "recovery") and report.overload_observed:
                    report.recovered = True
        except Exception:
            pass
        await asyncio.sleep(0.5)


async def _capture_metrics_snapshot(client: httpx.AsyncClient, report: _Report) -> None:
    try:
        r = await client.get("/api/v1/metrics/json")
        if r.status_code == 200:
            body = r.json()
            report.dropped_critical = body.get("processed_per_priority", {}).get("critical", 0) - body.get("processed_per_priority", {}).get("critical", 0)  # placeholder
            # Better: examine admission counters. Drops are not split by priority in counters today; this is intentionally approximate.
            report.accepted_total = body.get("admission_counters", {}).get("accepted", 0)
    except Exception:
        pass


async def run(base: str) -> _Report:
    report = _Report()
    t_start = time.monotonic()
    async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
        report.healthy = await _wait_healthy(client)
        if not report.healthy:
            return report

        await _tune_for_fast_e2e(client)

        report.spike_rps = 500
        await _drive_spike(client, rps=report.spike_rps, duration_s=12)

        await _poll_status_window(client, duration_s=14.0, report=report)

        await _stop_loadtest(client)

        await _poll_status_window(client, duration_s=15.0, report=report)

        await _capture_metrics_snapshot(client, report)
    report.elapsed_s = time.monotonic() - t_start
    return report


def _print(report: _Report) -> None:
    print(json.dumps({
        "healthy": report.healthy,
        "transitions": report.transitions,
        "snapshots_count": len(report.snapshots),
        "pressure_observed": report.pressure_observed,
        "overload_observed": report.overload_observed,
        "recovered": report.recovered,
        "elapsed_s": round(report.elapsed_s, 2),
        "spike_rps": report.spike_rps,
        "accepted_total": report.accepted_total,
        "last_snapshot": report.snapshots[-1].__dict__ if report.snapshots else None,
    }, indent=2))


def _exit_code(report: _Report) -> int:
    if not report.healthy:
        print("FAIL: health endpoint never responded", file=sys.stderr)
        return 2
    if not report.overload_observed:
        print("FAIL: pressure_level never reached 'overload' during spike", file=sys.stderr)
        return 3
    if not report.recovered:
        print("FAIL: pressure_level did not return to normal/recovery after spike", file=sys.stderr)
        return 4
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://localhost:8000")
    args = parser.parse_args()
    report = asyncio.run(run(args.base))
    _print(report)
    return _exit_code(report)


if __name__ == "__main__":
    sys.exit(main())
