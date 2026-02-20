import asyncio
import time

from generator.config import GeneratorConfig
from generator.load_generator import LoadGenerator
from benchmark.reporter import ResourceMonitor, BenchmarkReporter

TEST_CONFIGS = [
    {"name": "warmup",      "total": 100,   "duration": 5,  "concurrency": 2},
    {"name": "baseline",    "total": 1000,  "duration": 10, "concurrency": 5},
    {"name": "medium_load", "total": 5000,  "duration": 15, "concurrency": 20},
    {"name": "high_load",   "total": 10000, "duration": 20, "concurrency": 50},
    {"name": "stress_test", "total": 50000, "duration": 30, "concurrency": 100},
]


class BenchmarkRunner:
    def __init__(
        self,
        server_host: str = "perf-server",
        server_port: int = 9000,
        enable_tls: bool = False,
    ):
        self.server_host = server_host
        self.server_port = server_port
        self.enable_tls = enable_tls
        self.reporter = BenchmarkReporter(output_dir="/app")

    async def run_all(self) -> str:
        print("=" * 60)
        print("BENCHMARK SUITE")
        print("=" * 60)

        for i, test_cfg in enumerate(TEST_CONFIGS):
            print(f"\n--- Test {i+1}/{len(TEST_CONFIGS)}: {test_cfg['name']} ---")
            print(
                f"  Total: {test_cfg['total']}, Duration: {test_cfg['duration']}s, "
                f"Concurrency: {test_cfg['concurrency']}"
            )

            config = GeneratorConfig(
                SERVER_HOST=self.server_host,
                SERVER_PORT=self.server_port,
                TOTAL_LOGS=test_cfg["total"],
                DURATION_SECS=test_cfg["duration"],
                CONCURRENCY=test_cfg["concurrency"],
                ENABLE_TLS=self.enable_tls,
            )

            monitor = ResourceMonitor()
            generator = LoadGenerator(config)

            # Sample resources during the test
            async def monitor_resources():
                while True:
                    monitor.sample()
                    await asyncio.sleep(1.0)

            monitor_task = asyncio.create_task(monitor_resources())

            try:
                results = await generator.run()
            finally:
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    pass

            self.reporter.add_test(
                name=test_cfg["name"],
                config=test_cfg,
                results=results,
                resources=monitor.summary(),
            )

            # Cooldown between tests
            if i < len(TEST_CONFIGS) - 1:
                print("  Cooling down for 5 seconds...")
                await asyncio.sleep(5)

        report_path = self.reporter.generate_report()
        return report_path
