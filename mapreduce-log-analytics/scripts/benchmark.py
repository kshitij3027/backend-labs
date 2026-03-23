"""Performance benchmarks for MapReduce Log Analytics."""

import os
import sys
import time

sys.path.insert(0, "/app")

from src.generator import generate_json_logs, generate_apache_logs
from src.engine import MapReduceEngine

DATA_DIR = os.environ.get("DATA_DIR", "/tmp/benchmark")


def benchmark_10k():
    """Benchmark: Process 10K logs in under 60 seconds."""
    print("=" * 50)
    print("Benchmark: 10K log entries")
    print("=" * 50)

    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "bench-10k.jsonl")
    generate_json_logs(path, num_lines=10_000, seed=42)

    engine = MapReduceEngine(num_workers=4, chunk_size=67_108_864)

    all_pass = True
    for analysis in ["word_count", "pattern_frequency", "service_distribution", "security"]:
        start = time.time()
        results = engine.run([path], analysis, analysis)
        elapsed = time.time() - start
        status = "PASS" if elapsed < 60 else "FAIL"
        if status == "FAIL":
            all_pass = False
        print(f"  {analysis}: {elapsed:.2f}s ({len(results)} keys) [{status}]")

    return all_pass


def benchmark_concurrent():
    """Benchmark: 3 concurrent jobs."""
    print("\n" + "=" * 50)
    print("Benchmark: 3 concurrent jobs")
    print("=" * 50)

    import concurrent.futures

    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "bench-concurrent.jsonl")
    generate_json_logs(path, num_lines=5_000, seed=42)

    def run_job(analysis):
        engine = MapReduceEngine(num_workers=2, chunk_size=67_108_864)
        start = time.time()
        results = engine.run([path], analysis, analysis)
        return analysis, time.time() - start, len(results)

    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(run_job, "word_count"),
            executor.submit(run_job, "pattern_frequency"),
            executor.submit(run_job, "service_distribution"),
        ]
        for f in concurrent.futures.as_completed(futures):
            name, elapsed, keys = f.result()
            print(f"  {name}: {elapsed:.2f}s ({keys} keys)")

    total = time.time() - start
    status = "PASS" if total < 120 else "FAIL"
    print(f"  Total wall time: {total:.2f}s [{status}]")

    return total < 120


def main():
    passed = 0
    total = 0

    total += 1
    if benchmark_10k():
        passed += 1

    total += 1
    if benchmark_concurrent():
        passed += 1

    print(f"\n{'=' * 50}")
    print(f"Benchmarks: {passed}/{total} passed")
    print(f"{'=' * 50}")

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
