import asyncio
import gzip
import json
import random
import ssl
import time
from typing import Optional

from generator.config import GeneratorConfig
from generator.connection_pool import ConnectionPool
from generator.metrics import Metrics

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
SAMPLE_MESSAGES = [
    "User login successful",
    "Database query completed",
    "Cache miss for key",
    "Request timeout after 30s",
    "Disk usage at 85%",
    "Memory allocation failed",
    "Connection pool exhausted",
    "SSL handshake completed",
    "Rate limit exceeded for client",
    "Backup job completed successfully",
    "Service health check passed",
    "Configuration reload triggered",
    "File descriptor limit approaching",
    "Garbage collection took 150ms",
    "Network partition detected",
]


class LoadGenerator:
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.metrics = Metrics()
        self._stop_event = asyncio.Event()
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._pool: Optional[ConnectionPool] = None

    def _generate_message(self) -> dict:
        return {
            "level": random.choice(LOG_LEVELS),
            "message": random.choice(SAMPLE_MESSAGES),
        }

    def _encode_message(self, msg: dict) -> bytes:
        data = json.dumps(msg).encode() + b"\n"
        if self.config.COMPRESS:
            data = gzip.compress(data)
        return data

    async def run(self) -> dict:
        """Run the load test and return metrics summary."""
        ssl_ctx = None
        if self.config.ENABLE_TLS:
            ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_ctx.load_verify_locations(self.config.CA_CERT_PATH)

        self._pool = ConnectionPool(
            self.config.SERVER_HOST,
            self.config.SERVER_PORT,
            self.config.CONCURRENCY,
            ssl_context=ssl_ctx,
        )
        self._semaphore = asyncio.Semaphore(self.config.CONCURRENCY)

        self.metrics.start()

        # Start progress reporter
        progress_task = asyncio.create_task(self._progress_reporter())

        # Create worker tasks
        tasks = []
        for i in range(self.config.TOTAL_LOGS):
            if self._stop_event.is_set():
                break
            # Check duration limit
            elapsed = time.monotonic() - self.metrics.start_time
            if elapsed >= self.config.DURATION_SECS:
                break
            tasks.append(asyncio.create_task(self._send_one()))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self.metrics.stop()
        self._stop_event.set()

        try:
            await asyncio.wait_for(progress_task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            progress_task.cancel()

        await self._pool.close_all()

        summary = self.metrics.summary()
        self._print_summary(summary)
        return summary

    async def _send_one(self):
        async with self._semaphore:
            msg = self._generate_message()
            data = self._encode_message(msg)

            start = time.monotonic()
            success = False
            try:
                reader, writer = await asyncio.wait_for(
                    self._pool.acquire(), timeout=5.0
                )
                writer.write(data)
                await writer.drain()

                response_data = await asyncio.wait_for(
                    reader.readline(), timeout=5.0
                )
                if response_data:
                    response = json.loads(response_data.decode())
                    success = response.get("status") == "ok"

                await self._pool.release(reader, writer)
            except Exception:
                success = False

            latency_ms = (time.monotonic() - start) * 1000
            self.metrics.record(latency_ms, success, len(data))

    async def _progress_reporter(self):
        """Print progress every 1 second."""
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(1.0)
                elapsed = time.monotonic() - self.metrics.start_time
                rps = self.metrics.total_sent / max(elapsed, 0.001)
                error_rate = self.metrics.total_errors / max(
                    self.metrics.total_sent, 1
                )
                print(
                    f"[PROGRESS] {elapsed:.1f}s | Sent: {self.metrics.total_sent} | "
                    f"RPS: {rps:.0f} | Errors: {self.metrics.total_errors} "
                    f"({error_rate:.1%})"
                )
        except asyncio.CancelledError:
            pass

    def _print_summary(self, summary: dict):
        print("\n" + "=" * 60)
        print("LOAD TEST RESULTS")
        print("=" * 60)
        print(f"  Total Sent:     {summary['total_sent']}")
        print(f"  Total Success:  {summary['total_success']}")
        print(f"  Total Errors:   {summary['total_errors']}")
        print(f"  Error Rate:     {summary['error_rate']:.2%}")
        print(f"  Duration:       {summary['duration_secs']:.3f}s")
        print(f"  Actual RPS:     {summary['actual_rps']:.0f}")
        print(f"  Latency Avg:    {summary['latency_avg_ms']:.3f}ms")
        print(f"  Latency P50:    {summary['latency_p50_ms']:.3f}ms")
        print(f"  Latency P95:    {summary['latency_p95_ms']:.3f}ms")
        print(f"  Latency P99:    {summary['latency_p99_ms']:.3f}ms")
        print("=" * 60)
