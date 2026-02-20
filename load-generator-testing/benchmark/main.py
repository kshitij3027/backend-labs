import argparse
import asyncio
import os

from benchmark.runner import BenchmarkRunner


async def main():
    parser = argparse.ArgumentParser(description="Benchmark suite for TCP log server")
    parser.add_argument(
        "--host", default=os.environ.get("SERVER_HOST", "perf-server")
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("SERVER_PORT", "9000")),
    )
    parser.add_argument("--no-tls", action="store_true")
    args = parser.parse_args()

    runner = BenchmarkRunner(
        server_host=args.host,
        server_port=args.port,
        enable_tls=not args.no_tls if not args.no_tls else False,
    )

    report_path = await runner.run_all()
    print(f"\nBenchmark complete. Report: {report_path}")


if __name__ == "__main__":
    asyncio.run(main())
