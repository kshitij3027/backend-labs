"""End-to-end smoke test for the LATENCY_INJECTION failure type.

Runs INSIDE the chaos-framework container, so it can import the framework's
own modules and talk to log-consumer over chaos-net. Exits 0 on success.

Asserts the canonical success criterion from project_requirements.md §5:
    "A latency_injection experiment of 200ms produces measurable latency
     on the target and the system returns to baseline after the
     experiment ends."
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time

import httpx

# Allow running with PYTHONPATH=/app inside the framework container.
sys.path.insert(0, "/app")

from src.config.settings import get_settings, Settings  # noqa: E402
from src.docker_client.client import DockerClient  # noqa: E402
from src.injection.injector import FailureInjector  # noqa: E402
from src.models.scenarios import FailureScenario, FailureType  # noqa: E402


TARGET = "log-consumer"
TARGET_URL = f"http://{TARGET}:8000/health"
LATENCY_MS = 200
SAMPLES = 5


async def measure_rtt(client: httpx.AsyncClient, n: int = SAMPLES) -> float:
    samples_ms: list[float] = []
    for _ in range(n):
        t0 = time.perf_counter()
        try:
            r = await client.get(TARGET_URL, timeout=5.0)
            r.raise_for_status()
        except Exception:
            # Treat as 5s on error so we don't silently mask
            samples_ms.append(5000.0)
            continue
        samples_ms.append((time.perf_counter() - t0) * 1000.0)
    return statistics.median(samples_ms)


async def main() -> int:
    settings: Settings = get_settings()
    dc = DockerClient(
        allowlist=settings.target_allowlist,
        socket_path=settings.docker_socket_path,
    )
    injector = FailureInjector(
        docker_client=dc,
        allowlist=settings.target_allowlist,
        max_concurrent=settings.max_concurrent_scenarios,
        cpu_emergency_threshold_pct=settings.cpu_emergency_threshold_pct,
        mem_emergency_threshold_pct=settings.mem_emergency_threshold_pct,
    )

    results: dict[str, float] = {}
    failures: list[str] = []

    async with httpx.AsyncClient() as client:
        # Warmup
        for _ in range(3):
            try:
                await client.get(TARGET_URL, timeout=2.0)
            except Exception:
                pass

        # 1. Baseline
        baseline = await measure_rtt(client)
        results["baseline_ms"] = baseline
        print(f"baseline_ms={baseline:.1f}", flush=True)

        # 2. Inject 200ms latency
        scenario = FailureScenario(
            type=FailureType.LATENCY_INJECTION,
            target=TARGET,
            parameters={"latency_ms": LATENCY_MS, "jitter_ms": 0},
            duration=10,
            severity=2,
        )
        await injector.inject(scenario)
        print(f"injected scenario_id={scenario.id}", flush=True)

        # 3. Let netem settle, then measure during-fault
        await asyncio.sleep(1.0)
        during = await measure_rtt(client)
        results["during_ms"] = during
        print(f"during_ms={during:.1f}", flush=True)

        if during < baseline + (LATENCY_MS * 0.75):
            failures.append(
                f"during RTT ({during:.1f}ms) did not exceed baseline+150ms "
                f"(baseline={baseline:.1f}ms, expected >= "
                f"{baseline + LATENCY_MS * 0.75:.1f}ms)"
            )

        # 4. Rollback
        await injector.rollback(scenario.id)
        print(f"rolled back scenario_id={scenario.id}", flush=True)

        # 5. After-recovery RTT
        await asyncio.sleep(1.0)
        after = await measure_rtt(client)
        results["after_ms"] = after
        print(f"after_ms={after:.1f}", flush=True)

        if after > baseline + 50:
            failures.append(
                f"after RTT ({after:.1f}ms) did not recover to baseline+50ms "
                f"(baseline={baseline:.1f}ms, expected <= "
                f"{baseline + 50:.1f}ms)"
            )

    summary = {
        "result": "pass" if not failures else "fail",
        "latency_ms_injected": LATENCY_MS,
        "samples": SAMPLES,
        **results,
        "failures": failures,
    }
    print("--- e2e summary ---", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
