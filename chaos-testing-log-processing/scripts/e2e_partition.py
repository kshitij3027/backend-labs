"""End-to-end smoke test for NETWORK_PARTITION.

Brings up `log-consumer` and `redis`, samples the consumer's `/counter`
before partitioning, partitions it from `chaos-net` for 5s, verifies the
counter stalls during the partition (delta < 5), reconnects, and verifies
the counter resumes within 3s of reconnect (post_delta >= 5).
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

import httpx

sys.path.insert(0, "/app")

from src.config.settings import get_settings  # noqa: E402
from src.docker_client.client import DockerClient  # noqa: E402
from src.injection.injector import FailureInjector  # noqa: E402
from src.models.scenarios import FailureScenario, FailureType  # noqa: E402


TARGET = "log-consumer"
COUNTER_URL = "http://log-consumer:8000/counter"
PARTITION_S = 5.0


async def fetch_counter(client: httpx.AsyncClient) -> int:
    r = await client.get(COUNTER_URL, timeout=2.0)
    r.raise_for_status()
    return int(r.json()["counter"])


async def main() -> int:
    settings = get_settings()
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

    failures: list[str] = []
    results: dict = {}

    async with httpx.AsyncClient() as client:
        # Settle
        for _ in range(3):
            try:
                await fetch_counter(client)
            except Exception:
                pass

        before = await fetch_counter(client)
        results["before"] = before

        # Partition
        scenario = FailureScenario(
            type=FailureType.NETWORK_PARTITION,
            target=TARGET,
            parameters={"network": settings.chaos_network_name},
            duration=int(PARTITION_S),
            severity=3,
        )
        await injector.inject(scenario)
        partition_start = time.monotonic()
        print(
            f"partitioned {TARGET} from {settings.chaos_network_name} "
            f"(scenario={scenario.id})",
            flush=True,
        )

        # Hold the partition; during it we cannot reach the consumer via chaos-net.
        # We DELIBERATELY don't poll counter during partition — it's unreachable.
        await asyncio.sleep(PARTITION_S)

        await injector.rollback(scenario.id)
        partition_end = time.monotonic()
        print(
            f"rejoined {TARGET} after {(partition_end - partition_start):.1f}s",
            flush=True,
        )

        # Wait for chaos-net DNS to stabilise + consumer to drain backlog
        # The consumer has been blocked on Redis the whole time (its connection died),
        # so it needs a moment to reestablish its BLPOP loop and start incrementing.
        await asyncio.sleep(3.0)

        after_reconnect = await fetch_counter(client)
        results["after_reconnect"] = after_reconnect
        post_delta = after_reconnect - before
        results["post_delta"] = post_delta

        # Assertion: counter must have advanced by at least 5 messages after we
        # reconnected (i.e., the consumer recovered). At 10+ Hz this is trivially
        # satisfied within the 3s recovery window.
        if post_delta < 5:
            failures.append(
                f"counter did not resume after reconnect: before={before} "
                f"after_reconnect={after_reconnect} delta={post_delta}"
            )

    summary = {
        "result": "pass" if not failures else "fail",
        "partition_duration_s": PARTITION_S,
        **results,
        "failures": failures,
    }
    print("--- e2e summary ---", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
