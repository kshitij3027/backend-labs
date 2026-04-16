"""Static node registry + concurrent health polling."""

from __future__ import annotations

import asyncio

import httpx


class ClusterRegistry:
    """Static map of ``node_id -> base_url`` with health checks."""

    def __init__(self, node_urls: dict[str, str]) -> None:
        self._node_urls: dict[str, str] = dict(node_urls)

    def nodes(self) -> list[str]:
        """Sorted list of known node ids."""
        return sorted(self._node_urls.keys())

    def url_for(self, node_id: str) -> str | None:
        return self._node_urls.get(node_id)

    async def check_health(self, client: httpx.AsyncClient) -> dict[str, bool]:
        """Concurrently hit ``GET /health`` on every node.

        A node is considered healthy iff the response is 200 and the JSON
        body's ``status`` field equals ``"healthy"``.
        """
        node_ids = self.nodes()
        if not node_ids:
            return {}

        async def _probe(node_id: str) -> bool:
            url = self._node_urls[node_id].rstrip("/") + "/health"
            try:
                resp = await client.get(url)
            except Exception:
                return False
            if resp.status_code != 200:
                return False
            try:
                body = resp.json()
            except Exception:
                return False
            return bool(body.get("status") == "healthy")

        results = await asyncio.gather(
            *(_probe(n) for n in node_ids), return_exceptions=True
        )
        out: dict[str, bool] = {}
        for nid, res in zip(node_ids, results):
            out[nid] = bool(res) if not isinstance(res, BaseException) else False
        return out
