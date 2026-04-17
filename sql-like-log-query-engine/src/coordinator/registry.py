"""In-memory registry of partition nodes.

The coordinator asks the registry for the list of live partitions; the
registry keeps that list warm with a periodic background health poll.

Semantics:
  - Each configured partition URL produces exactly one ``PartitionMetadata``
    entry — the registry never grows or shrinks at runtime.
  - If a partition is unreachable on startup, it still appears in the list
    but with ``healthy=False`` and a placeholder ``time_range`` (so the
    planner can reason about it without crashing). As soon as the partition
    becomes reachable and ``/metadata`` succeeds, the stored entry is
    replaced with the live advertised metadata.
  - If metadata fetch fails later, we keep the last-known metadata and just
    flip ``healthy=False`` so the planner can still plan around it.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Iterable

import httpx

from src.shared.models import PartitionMetadata, TimeRange

from .node_client import check_health, fetch_metadata


_PLACEHOLDER_START = datetime(1970, 1, 1, 0, 0, 0)
_PLACEHOLDER_END = _PLACEHOLDER_START + timedelta(days=365 * 200)


class PartitionRegistry:
    """Track the current health + metadata of each partition node."""

    def __init__(self, urls: dict[str, str]) -> None:
        self._urls = dict(urls)
        self._partitions: dict[str, PartitionMetadata] = {
            pid: _placeholder(pid, url) for pid, url in urls.items()
        }
        self._lock = asyncio.Lock()

    # --- public read surface -------------------------------------------

    def partitions(self) -> list[PartitionMetadata]:
        """Return a snapshot of every known partition (healthy + unhealthy)."""

        # Dict insertion order preserves config order for stable output.
        return list(self._partitions.values())

    def healthy_partitions(self) -> list[PartitionMetadata]:
        return [p for p in self._partitions.values() if p.healthy]

    def urls(self) -> dict[str, str]:
        return dict(self._urls)

    # --- refresh --------------------------------------------------------

    async def refresh(self, client: httpx.AsyncClient) -> None:
        """Probe every partition in parallel and update the local cache.

        We first fan out health checks, then fetch metadata for the healthy
        nodes. Any exception per partition is swallowed locally — one broken
        partition must never bring the coordinator down.
        """

        ids = list(self._urls.keys())
        health_tasks = [
            check_health(client, self._urls[pid]) for pid in ids
        ]
        health_results = await asyncio.gather(*health_tasks, return_exceptions=True)

        # Fetch metadata only for healthy partitions.
        healthy_ids: list[str] = []
        for pid, result in zip(ids, health_results):
            ok = isinstance(result, bool) and result
            if ok:
                healthy_ids.append(pid)

        meta_tasks = [
            fetch_metadata(client, self._urls[pid]) for pid in healthy_ids
        ]
        meta_results = await asyncio.gather(*meta_tasks, return_exceptions=True)

        new_meta: dict[str, PartitionMetadata] = {}
        for pid, result in zip(healthy_ids, meta_results):
            if isinstance(result, PartitionMetadata):
                new_meta[pid] = result.model_copy(update={"healthy": True})
            # else: treat as unhealthy below

        async with self._lock:
            for pid in ids:
                url = self._urls[pid]
                if pid in new_meta:
                    # Overwrite with freshly-advertised metadata.
                    updated = new_meta[pid].model_copy(
                        update={"url": url, "healthy": True}
                    )
                    self._partitions[pid] = updated
                else:
                    # Flip the existing entry's healthy flag to False, but
                    # keep time_range/indexed_fields so downstream planning
                    # can still reason about the partition if we choose to.
                    existing = self._partitions.get(pid) or _placeholder(pid, url)
                    self._partitions[pid] = existing.model_copy(
                        update={"healthy": False, "url": url}
                    )

    # --- background task ----------------------------------------------

    async def poll_forever(
        self, client: httpx.AsyncClient, interval: float = 5.0
    ) -> None:
        """Periodic refresh loop. Exits cleanly on ``asyncio.CancelledError``."""

        try:
            while True:
                try:
                    await self.refresh(client)
                except Exception:
                    # Never let one bad refresh kill the loop.
                    pass
                await asyncio.sleep(interval)
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            raise


# --- helpers --------------------------------------------------------------


def _placeholder(partition_id: str, url: str) -> PartitionMetadata:
    """Synthesise a conservative placeholder entry for an unreachable node."""

    return PartitionMetadata(
        id=partition_id,
        url=url,
        time_range=TimeRange(start=_PLACEHOLDER_START, end=_PLACEHOLDER_END),
        indexed_fields=[],
        healthy=False,
    )


def _ids(partitions: Iterable[PartitionMetadata]) -> list[str]:
    return [p.id for p in partitions]
