"""CLI demo of the circuit breaker engine.

Runnable as:
    python -m src.demo --logs 20 --simulate-duration 3
"""
from __future__ import annotations
import argparse
import asyncio
import random

from src.breaker import CircuitBreaker
from src.config import critical_service_config, standard_service_config
from src.failure_injection import FailureInjector
from src.registry import CircuitBreakerRegistry
from src.services.database import DatabaseService
from src.services.external_api import ExternalAPIService
from src.services.log_processor import LogProcessorService
from src.services.queue import MessageQueueService
from src.state import CircuitState


def _build_world(seed: int | None = None):
    if seed is not None:
        random.seed(seed)
    registry = CircuitBreakerRegistry()
    primary_br = registry.register(critical_service_config("database_primary"))
    backup_br = registry.register(standard_service_config("database_backup"))
    queue_br = registry.register(standard_service_config("queue_main"))
    api_br = registry.register(standard_service_config("external_api"))

    primary_db = DatabaseService("database_primary", primary_br, FailureInjector())
    backup_db = DatabaseService("database_backup", backup_br, FailureInjector())
    queue_svc = MessageQueueService("queue_main", queue_br, FailureInjector())
    api_svc = ExternalAPIService("external_api", api_br, FailureInjector())
    processor = LogProcessorService(primary_db, backup_db, queue_svc, api_svc)
    return registry, processor, primary_db, backup_db, queue_svc, api_svc


def _print_breaker_table(registry: CircuitBreakerRegistry) -> None:
    print(f"  {'breaker':<22} {'state':<10} {'success_rate':>14} {'total':>8}")
    for name in registry.names():
        br = registry.get(name)
        d = br.to_dict()
        print(f"  {name:<22} {d['state']:<10} {d['success_rate']:>14.1%} {d['total_calls']:>8}")


async def run_demo(args: argparse.Namespace) -> int:
    print("=" * 60)
    print("🚀 Circuit Breaker System Demo")
    print("=" * 60)
    registry, processor, primary_db, backup_db, queue_svc, api_svc = _build_world(seed=args.seed)

    # Phase 1: happy path
    print(f"\n1. Processing {args.logs} synthetic logs (clean run)...")
    agg = await processor.process_batch(args.logs)
    print(
        f"   processed={agg['processed']}, successful={agg['successful']}, "
        f"fallbacks={agg['fallback_responses']}, duration_ms={agg['duration_ms']:.1f}"
    )
    print("\n2. Circuit Breaker Statistics:")
    _print_breaker_table(registry)

    # Phase 2: simulate failures
    print(f"\n3. Simulating {args.simulate_duration}s of database_primary failures...")
    primary_db.injector.set_failure_rate(0.9)
    end = asyncio.get_event_loop().time() + args.simulate_duration
    while asyncio.get_event_loop().time() < end:
        await processor.process_log({"message": "during failure", "level": "ERROR", "service": "demo-app"})
        await asyncio.sleep(0.05)
    primary_db.injector.set_failure_rate(0.0)
    print("\n4. Updated Statistics (after failure storm):")
    _print_breaker_table(registry)
    stats = processor.get_processing_stats()
    print(
        f"   processing: total={stats['total_processed']}, successful={stats['successful_processed']}, "
        f"fallbacks={stats['fallback_responses']}"
    )

    # Phase 3: recovery
    if args.wait_recovery:
        wait_seconds = max(p.breaker.config.recovery_timeout for p in (primary_db,))
        print(f"\n5. Waiting {wait_seconds:.1f}s for recovery_timeout, then probing...")
        await asyncio.sleep(wait_seconds + 0.5)
        for _ in range(10):
            await processor.process_log({"message": "post-recovery", "level": "INFO", "service": "demo-app"})
        print("\n6. Final Statistics:")
        _print_breaker_table(registry)

    print("\n✅ Demo completed!\n")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Circuit Breaker Engine — CLI demo")
    parser.add_argument("--logs", type=int, default=20, help="number of synthetic logs to process before failure storm")
    parser.add_argument("--simulate-duration", type=int, default=3, help="seconds of injected DB failures")
    parser.add_argument("--seed", type=int, default=None, help="optional RNG seed for determinism")
    parser.add_argument("--wait-recovery", action="store_true", default=True, help="wait for recovery_timeout and re-probe (default on)")
    parser.add_argument("--no-wait-recovery", dest="wait_recovery", action="store_false", help="skip recovery wait")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(run_demo(args))


if __name__ == "__main__":
    raise SystemExit(main())
