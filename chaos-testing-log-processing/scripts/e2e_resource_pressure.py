"""End-to-end smoke test for RESOURCE_EXHAUSTION (CPU pressure)."""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time

sys.path.insert(0, "/app")

from src.config.settings import get_settings  # noqa: E402
from src.docker_client.client import DockerClient  # noqa: E402
from src.injection.injector import FailureInjector  # noqa: E402
from src.models.scenarios import FailureScenario, FailureType  # noqa: E402

TARGET = "log-consumer"
INJECT_DURATION = 10
LOAD_PCT = 80
CORES = 1


def _container_cpu_pct(client: DockerClient, name: str) -> float:
    """Compute container CPU% from a non-streaming stats sample."""
    container = client.get_target(name)
    stats = container.stats(stream=False)
    cpu = stats.get("cpu_stats", {})
    pre = stats.get("precpu_stats", {})
    cpu_delta = cpu.get("cpu_usage", {}).get("total_usage", 0) - pre.get("cpu_usage", {}).get("total_usage", 0)
    system_delta = cpu.get("system_cpu_usage", 0) - pre.get("system_cpu_usage", 0)
    online = cpu.get("online_cpus") or len(cpu.get("cpu_usage", {}).get("percpu_usage", []) or []) or 1
    if system_delta <= 0 or cpu_delta < 0:
        return 0.0
    return (cpu_delta / system_delta) * online * 100.0


async def sample_peak(client: DockerClient, name: str, duration_s: float, interval_s: float = 0.5) -> float:
    """Sample CPU% repeatedly over a window and return the peak."""
    end = time.monotonic() + duration_s
    samples: list[float] = []
    while time.monotonic() < end:
        try:
            samples.append(_container_cpu_pct(client, name))
        except Exception:
            pass
        await asyncio.sleep(interval_s)
    return max(samples) if samples else 0.0


async def main() -> int:
    settings = get_settings()
    dc = DockerClient(allowlist=settings.target_allowlist, socket_path=settings.docker_socket_path)
    injector = FailureInjector(
        docker_client=dc,
        allowlist=settings.target_allowlist,
        max_concurrent=settings.max_concurrent_scenarios,
        cpu_emergency_threshold_pct=settings.cpu_emergency_threshold_pct,
        mem_emergency_threshold_pct=settings.mem_emergency_threshold_pct,
    )

    failures: list[str] = []
    results: dict = {}

    # Baseline CPU% over 3s
    baseline = await sample_peak(dc, TARGET, duration_s=3.0)
    results["baseline_peak_pct"] = round(baseline, 1)
    print(f"baseline_peak_pct={baseline:.1f}", flush=True)

    scenario = FailureScenario(
        type=FailureType.RESOURCE_EXHAUSTION,
        target=TARGET,
        parameters={"pressure": "cpu", "cores": CORES, "load_pct": LOAD_PCT, "duration_s": INJECT_DURATION},
        duration=INJECT_DURATION,
        severity=3,
    )
    await injector.inject(scenario)
    print(f"injected scenario_id={scenario.id} ({LOAD_PCT}% on {CORES} core(s) for {INJECT_DURATION}s)", flush=True)

    # Let stress-ng spin up, then sample peak during the fault.
    await asyncio.sleep(2.0)
    during_peak = await sample_peak(dc, TARGET, duration_s=5.0)
    results["during_peak_pct"] = round(during_peak, 1)
    print(f"during_peak_pct={during_peak:.1f}", flush=True)

    if during_peak < 70.0:
        failures.append(f"during peak ({during_peak:.1f}%) did not exceed 70% threshold")

    # Rollback (kills stress-ng early if still running; no-op otherwise).
    await injector.rollback(scenario.id)
    print(f"rolled back scenario_id={scenario.id}", flush=True)

    # Let the container settle, then sample after.
    await asyncio.sleep(3.0)
    after_peak = await sample_peak(dc, TARGET, duration_s=3.0)
    results["after_peak_pct"] = round(after_peak, 1)
    print(f"after_peak_pct={after_peak:.1f}", flush=True)

    if after_peak > 30.0:
        failures.append(f"after peak ({after_peak:.1f}%) did not recover below 30%")

    summary = {
        "result": "pass" if not failures else "fail",
        "load_pct_injected": LOAD_PCT,
        "duration_s": INJECT_DURATION,
        **results,
        "failures": failures,
    }
    print("--- e2e summary ---", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
