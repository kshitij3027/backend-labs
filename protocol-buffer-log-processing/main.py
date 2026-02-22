"""CLI entry point — orchestrates the full Protocol Buffers log processing pipeline."""

from __future__ import annotations

import os
import sys

from src.benchmark import run_all_benchmarks
from src.config import Config
from src.high_load import print_high_load_report, run_high_load_simulation
from src.log_generator import generate_log_batch
from src.schema_evolution import run_schema_evolution_demo
from src.report import generate_report
from src.serializer import serialize_json, serialize_protobuf
from src.validator import validate_log_entry


def _is_high_load_mode() -> bool:
    """Check if high-load mode was requested via CLI arg or env var."""
    if "--high-load" in sys.argv:
        return True
    return os.environ.get("HIGH_LOAD", "").lower() in ("true", "1", "yes")


def _banner() -> None:
    """Print a startup banner."""
    width = 72
    print("=" * width)
    print("  PROTOCOL BUFFERS LOG PROCESSING PIPELINE")
    print("=" * width)
    print()


def _human_bytes(n: int) -> str:
    """Format byte count in human-readable form."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f} MB"
    if n >= 1_000:
        return f"{n / 1_000:.2f} KB"
    return f"{n} B"


def main() -> None:
    """Run the full pipeline: generate, validate, serialize, benchmark, report."""

    # --- Config ---
    config = Config.from_env()

    # --- Schema evolution mode ---
    if "--schema-evolution" in sys.argv:
        _banner()
        print("Mode: SCHEMA EVOLUTION DEMO\n")
        run_schema_evolution_demo()
        return

    # --- High-load mode ---
    if _is_high_load_mode():
        _banner()
        print("Mode: HIGH-LOAD CONCURRENT SIMULATION\n")
        results = run_high_load_simulation(config)
        print_high_load_report(results)
        return
    _banner()

    # --- Generate ---
    print(f"[1/5] Generating {config.NUM_LOGS:,} log entries ...")
    entries = generate_log_batch(config.NUM_LOGS)
    print(f"       Generated {len(entries):,} entries.\n")

    # --- Validate ---
    print("[2/5] Validating all entries ...")
    for i, entry in enumerate(entries):
        try:
            validate_log_entry(entry)
        except Exception as exc:
            print(f"       Validation failed on entry {i}: {exc}", file=sys.stderr)
            sys.exit(1)
    print(f"       All {len(entries):,} entries valid.\n")

    # --- Serialize ---
    print("[3/5] Serializing to JSON and Protobuf ...")
    json_bytes = serialize_json(entries)
    proto_bytes = serialize_protobuf(entries)

    # Create output directories
    os.makedirs(config.JSON_LOG_DIR, exist_ok=True)
    os.makedirs(config.PROTOBUF_LOG_DIR, exist_ok=True)

    json_path = os.path.join(config.JSON_LOG_DIR, "batch.json")
    proto_path = os.path.join(config.PROTOBUF_LOG_DIR, "batch.pb")

    with open(json_path, "wb") as f:
        f.write(json_bytes)
    with open(proto_path, "wb") as f:
        f.write(proto_bytes)

    json_size = len(json_bytes)
    proto_size = len(proto_bytes)
    ratio = json_size / proto_size if proto_size > 0 else float("inf")

    print(f"       JSON file  : {json_path}  ({_human_bytes(json_size)})")
    print(f"       Proto file : {proto_path}  ({_human_bytes(proto_size)})")
    print(f"       Size ratio : {ratio:.2f}x (JSON / Protobuf)\n")

    # --- Benchmark ---
    print(
        f"[4/5] Running benchmarks ({config.BENCHMARK_ITERATIONS} iterations) ..."
    )
    results = run_all_benchmarks(entries, config.BENCHMARK_ITERATIONS)
    print("       Benchmarks complete.\n")

    # --- Report ---
    print("[5/5] Generating report ...\n")
    report = generate_report(results, config=config, num_entries=len(entries))
    print(report)

    # --- Done ---
    print("Pipeline finished successfully.")


if __name__ == "__main__":
    main()
