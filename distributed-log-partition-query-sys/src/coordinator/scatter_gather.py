import time
from dataclasses import dataclass, field
import asyncio

import httpx

from src.models import Query, LogEntry, PartitionInfo


@dataclass
class ScatterResult:
    partition_id: str
    success: bool
    entries: list[LogEntry] = field(default_factory=list)
    error: str | None = None
    response_time_ms: float = 0.0


class ScatterGather:
    def __init__(self, client: httpx.AsyncClient, timeout: float = 5.0):
        self.client = client
        self.timeout = timeout

    async def scatter(
        self, partitions: list[PartitionInfo], query: Query
    ) -> list[ScatterResult]:
        """Fan out query to all partitions in parallel, collect results."""
        tasks = [self._query_partition(p, query) for p in partitions]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        scatter_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                scatter_results.append(
                    ScatterResult(
                        partition_id=partitions[i].partition_id,
                        success=False,
                        error=str(result),
                    )
                )
            else:
                scatter_results.append(result)

        return scatter_results

    async def _query_partition(
        self, partition: PartitionInfo, query: Query
    ) -> ScatterResult:
        """Query a single partition."""
        start_time = time.time()
        try:
            response = await self.client.post(
                f"{partition.url}/query",
                json=query.model_dump(mode="json"),
                timeout=self.timeout,
            )
            elapsed_ms = (time.time() - start_time) * 1000

            if response.status_code != 200:
                return ScatterResult(
                    partition_id=partition.partition_id,
                    success=False,
                    error=f"HTTP {response.status_code}",
                    response_time_ms=round(elapsed_ms, 2),
                )

            data = response.json()
            entries = [LogEntry(**entry) for entry in data.get("results", [])]

            return ScatterResult(
                partition_id=partition.partition_id,
                success=True,
                entries=entries,
                response_time_ms=round(elapsed_ms, 2),
            )
        except httpx.TimeoutException:
            elapsed_ms = (time.time() - start_time) * 1000
            return ScatterResult(
                partition_id=partition.partition_id,
                success=False,
                error="Timeout",
                response_time_ms=round(elapsed_ms, 2),
            )
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            return ScatterResult(
                partition_id=partition.partition_id,
                success=False,
                error=str(e),
                response_time_ms=round(elapsed_ms, 2),
            )
