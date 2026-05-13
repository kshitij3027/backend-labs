"""Built-in recovery probes.

Each :class:`RecoveryProbe` subclass is the *executable* counterpart to a
``RecoveryTest`` config (see :mod:`src.models.validation`). The
``RecoveryValidator`` (next file) drives a list of probes, wraps each in
an ``asyncio.wait_for``, and records the outcome as a ``TestResult``.

A probe's :meth:`execute` returns a free-form ``details`` dict on success
and raises on failure (or times out). Exceptions are intentionally
allowed to bubble — the validator translates them into ``FAILED``/
``TIMEOUT`` statuses.
"""

from __future__ import annotations

import abc
import asyncio
import statistics
import time
from collections.abc import Sequence
from typing import Any

import httpx


class RecoveryProbe(abc.ABC):
    """Common interface for recovery tests."""

    name: str = "probe"
    required_for_success: bool = True
    timeout_s: float = 30.0
    description: str = ""

    def __init__(
        self,
        name: str | None = None,
        required_for_success: bool | None = None,
        timeout_s: float | None = None,
        description: str | None = None,
    ) -> None:
        if name is not None:
            self.name = name
        if required_for_success is not None:
            self.required_for_success = required_for_success
        if timeout_s is not None:
            self.timeout_s = timeout_s
        if description is not None:
            self.description = description

    @abc.abstractmethod
    async def execute(self) -> dict[str, Any]:
        ...


class HealthProbeTest(RecoveryProbe):
    """GET ``{base_url}{path}`` on each target with exponential backoff retries."""

    name = "HealthProbeTest"
    description = "All targets respond 2xx on /health within grace window."

    def __init__(
        self,
        targets: Sequence[tuple[str, str]],  # [(name, url)], e.g. ("log-consumer", "http://log-consumer:8000/health")
        *,
        max_attempts: int = 6,
        initial_backoff_s: float = 0.25,
        max_backoff_s: float = 4.0,
        http_client: httpx.AsyncClient | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._targets = list(targets)
        self._max_attempts = max_attempts
        self._initial = initial_backoff_s
        self._max = max_backoff_s
        self._http_client = http_client
        self._owns_client = http_client is None

    async def execute(self) -> dict[str, Any]:
        client = self._http_client or httpx.AsyncClient(timeout=httpx.Timeout(3.0))
        try:
            results: dict[str, dict[str, Any]] = {}
            failures: list[str] = []
            for name, url in self._targets:
                backoff = self._initial
                last_exc: Exception | None = None
                code: int | None = None
                attempts = 0
                for attempts in range(1, self._max_attempts + 1):
                    try:
                        r = await client.get(url)
                        code = r.status_code
                        if 200 <= code < 300:
                            results[name] = {"code": code, "attempts": attempts}
                            break
                    except Exception as exc:  # noqa: BLE001
                        last_exc = exc
                    await asyncio.sleep(min(backoff, self._max))
                    backoff *= 2.0
                else:
                    results[name] = {
                        "code": code,
                        "attempts": attempts,
                        "error": repr(last_exc) if last_exc else None,
                    }
                    failures.append(name)
            if failures:
                raise RuntimeError(f"unhealthy targets after retries: {failures} (details={results})")
            return {"targets": results}
        finally:
            if self._owns_client:
                await client.aclose()


class LatencyBaselineTest(RecoveryProbe):
    """Sample latency over a window and assert p95 <= ``baseline_ms * (1 + tolerance_pct/100)``."""

    name = "LatencyBaselineTest"
    description = "p95 of post-fault latency must be within X% of the pre-fault baseline."

    def __init__(
        self,
        url: str,
        *,
        baseline_ms: float,
        tolerance_pct: float = 50.0,
        sample_count: int = 15,
        sample_interval_s: float = 0.2,
        http_client: httpx.AsyncClient | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._url = url
        self._baseline_ms = baseline_ms
        self._tolerance_pct = tolerance_pct
        self._n = sample_count
        self._interval = sample_interval_s
        self._http_client = http_client
        self._owns_client = http_client is None

    async def execute(self) -> dict[str, Any]:
        client = self._http_client or httpx.AsyncClient(timeout=httpx.Timeout(3.0))
        try:
            samples_ms: list[float] = []
            for _ in range(self._n):
                t0 = time.perf_counter()
                try:
                    r = await client.get(self._url)
                    r.raise_for_status()
                    samples_ms.append((time.perf_counter() - t0) * 1000.0)
                except Exception:  # noqa: BLE001
                    samples_ms.append(3000.0)  # treat as 3s on error so it shows up in p95
                await asyncio.sleep(self._interval)
            samples_ms.sort()
            p95 = samples_ms[int(0.95 * (len(samples_ms) - 1))]
            mean = statistics.mean(samples_ms)
            ceiling = self._baseline_ms * (1.0 + self._tolerance_pct / 100.0)
            details = {
                "baseline_ms": self._baseline_ms,
                "tolerance_pct": self._tolerance_pct,
                "ceiling_ms": ceiling,
                "p95_ms": p95,
                "mean_ms": mean,
                "sample_count": self._n,
            }
            if p95 > ceiling:
                raise RuntimeError(
                    f"p95 ({p95:.1f}ms) exceeded ceiling {ceiling:.1f}ms "
                    f"(baseline={self._baseline_ms:.1f}ms +{self._tolerance_pct:.0f}%)"
                )
            return details
        finally:
            if self._owns_client:
                await client.aclose()


class DataLossTest(RecoveryProbe):
    """Compare ``sent_count`` vs ``counter`` after a drain window."""

    name = "DataLossTest"
    description = "Consumer must drain producer's queue within an acceptable_loss window."

    def __init__(
        self,
        producer_url: str,
        consumer_url: str,
        *,
        acceptable_loss: int = 0,
        drain_grace_s: float = 3.0,
        max_wait_s: float = 10.0,
        http_client: httpx.AsyncClient | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._producer = producer_url  # e.g., http://log-producer:8000/sent_count
        self._consumer = consumer_url  # e.g., http://log-consumer:8000/counter
        self._acceptable_loss = acceptable_loss
        self._grace = drain_grace_s
        self._max_wait = max_wait_s
        self._http_client = http_client
        self._owns_client = http_client is None

    async def execute(self) -> dict[str, Any]:
        client = self._http_client or httpx.AsyncClient(timeout=httpx.Timeout(3.0))
        try:
            await asyncio.sleep(self._grace)
            deadline = time.monotonic() + self._max_wait
            produced = -1
            processed = -1
            while time.monotonic() < deadline:
                r1 = await client.get(self._producer)
                r2 = await client.get(self._consumer)
                r1.raise_for_status()
                r2.raise_for_status()
                produced = int(r1.json()["sent_count"])
                processed = int(r2.json()["counter"])
                if produced - processed <= self._acceptable_loss:
                    break
                await asyncio.sleep(0.5)
            delta = produced - processed
            details = {
                "produced": produced,
                "processed": processed,
                "delta": delta,
                "acceptable_loss": self._acceptable_loss,
            }
            if delta > self._acceptable_loss:
                raise RuntimeError(
                    f"data loss: produced={produced} processed={processed} "
                    f"delta={delta} > acceptable_loss={self._acceptable_loss}"
                )
            return details
        finally:
            if self._owns_client:
                await client.aclose()
