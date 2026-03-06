import asyncio
import time
from dataclasses import dataclass

import httpx

from app.models import ConsistencyLevel, QuorumConfig, VectorClock, LogEntry
from app.metrics import QuorumMetrics


@dataclass
class NodeConnection:
    node_id: str
    base_url: str  # e.g. "http://node-1:8001"


class QuorumCoordinator:
    def __init__(
        self,
        node_connections: list[NodeConnection],
        config: QuorumConfig,
        metrics: QuorumMetrics,
    ):
        self.nodes = node_connections
        self.config = config
        self.metrics = metrics
        self.hint_buffer: dict[str, list[dict]] = {}
        self.client = httpx.AsyncClient(timeout=config.timeout_ms / 1000)

    async def close(self):
        await self.client.aclose()

    async def write(self, key: str, value: str) -> dict:
        entry = LogEntry(
            key=key,
            value=value,
            timestamp=time.time(),
            vector_clock=VectorClock(),
            node_id="coordinator",
        )
        entry_dict = entry.to_dict()

        async def send_to_node(node: NodeConnection):
            try:
                resp = await self.client.post(
                    f"{node.base_url}/store", json=entry_dict
                )
                if resp.status_code == 200:
                    return node, True, resp.json()
                return node, False, None
            except Exception:
                return node, False, None

        tasks = [send_to_node(n) for n in self.nodes]
        results = await asyncio.gather(*tasks)

        successes = 0
        for node, ok, resp_data in results:
            if ok:
                successes += 1
            else:
                # Hinted handoff: buffer for failed nodes
                if node.node_id not in self.hint_buffer:
                    self.hint_buffer[node.node_id] = []
                self.hint_buffer[node.node_id].append(entry_dict)

        w = self.config.write_quorum
        success = successes >= w
        self.metrics.record_write(success)

        return {
            "success": success,
            "key": key,
            "value": value,
            "nodes_acked": successes,
            "nodes_required": w,
            "total_nodes": len(self.nodes),
        }

    async def read(self, key: str) -> dict:
        async def read_from_node(node: NodeConnection):
            try:
                resp = await self.client.get(f"{node.base_url}/store/{key}")
                if resp.status_code == 200:
                    return node, True, resp.json()
                return node, False, None
            except Exception:
                return node, False, None

        tasks = [read_from_node(n) for n in self.nodes]
        results = await asyncio.gather(*tasks)

        entries = []
        responding_nodes = []
        for node, ok, data in results:
            if ok and data:
                entries.append((node, data))
                responding_nodes.append(node)

        r = self.config.read_quorum
        if len(entries) < r:
            self.metrics.record_read(False)
            return {
                "success": False,
                "key": key,
                "error": f"Only {len(entries)} nodes responded, need {r}",
                "nodes_responded": len(entries),
                "nodes_required": r,
                "total_nodes": len(self.nodes),
            }

        # Resolve conflicts
        entry_dicts = [e[1] for e in entries]
        winner = self._resolve_conflicts(entry_dicts)

        # Trigger read repair for stale nodes
        stale_urls = []
        winner_vc = VectorClock.from_dict(winner.get("vector_clock", {}))
        for node, data in entries:
            data_vc = VectorClock.from_dict(data.get("vector_clock", {}))
            if data_vc.compare(winner_vc) == "before":
                stale_urls.append(node.base_url)

        if stale_urls:
            asyncio.create_task(self._read_repair(key, winner, stale_urls))

        self.metrics.record_read(True)
        return {
            "success": True,
            "key": winner["key"],
            "value": winner["value"],
            "vector_clock": winner.get("vector_clock", {}),
            "nodes_responded": len(entries),
            "nodes_required": r,
            "total_nodes": len(self.nodes),
        }

    def _resolve_conflicts(self, entries: list[dict]) -> dict:
        if not entries:
            return {}
        if len(entries) == 1:
            return entries[0]

        winner = entries[0]
        winner_vc = VectorClock.from_dict(winner.get("vector_clock", {}))

        for entry in entries[1:]:
            entry_vc = VectorClock.from_dict(entry.get("vector_clock", {}))
            cmp = winner_vc.compare(entry_vc)

            if cmp == "before":
                # entry is newer
                winner = entry
                winner_vc = entry_vc
            elif cmp == "concurrent":
                # Tie-break: higher timestamp, then higher node_id
                if entry.get("timestamp", 0) > winner.get("timestamp", 0):
                    winner = entry
                    winner_vc = entry_vc
                elif entry.get("timestamp", 0) == winner.get("timestamp", 0):
                    if entry.get("node_id", "") > winner.get("node_id", ""):
                        winner = entry
                        winner_vc = entry_vc
            # "after" or "equal" -> keep current winner

        return winner

    async def _read_repair(self, key: str, winning_entry: dict, stale_node_urls: list[str]):
        for url in stale_node_urls:
            try:
                await self.client.post(f"{url}/store", json=winning_entry)
            except Exception:
                pass

    async def _replay_hints(self, node_id: str, base_url: str):
        hints = self.hint_buffer.pop(node_id, [])
        for entry_dict in hints:
            try:
                await self.client.post(f"{base_url}/store", json=entry_dict)
            except Exception:
                pass

    async def fail_node(self, node_id: str) -> dict:
        for node in self.nodes:
            if node.node_id == node_id:
                try:
                    resp = await self.client.post(f"{node.base_url}/admin/fail")
                    return resp.json()
                except Exception as e:
                    return {"error": str(e)}
        return {"error": f"Node {node_id} not found"}

    async def recover_node(self, node_id: str) -> dict:
        for node in self.nodes:
            if node.node_id == node_id:
                try:
                    resp = await self.client.post(f"{node.base_url}/admin/recover")
                    result = resp.json()
                    await self._replay_hints(node_id, node.base_url)
                    return result
                except Exception as e:
                    return {"error": str(e)}
        return {"error": f"Node {node_id} not found"}

    async def get_node_health(self, node_id: str) -> dict:
        for node in self.nodes:
            if node.node_id == node_id:
                try:
                    resp = await self.client.get(f"{node.base_url}/health")
                    return resp.json()
                except Exception as e:
                    return {"error": str(e)}
        return {"error": f"Node {node_id} not found"}

    async def get_cluster_status(self) -> dict:
        async def get_health(node: NodeConnection):
            try:
                resp = await self.client.get(f"{node.base_url}/health")
                if resp.status_code == 200:
                    return resp.json()
                return {"node_id": node.node_id, "is_healthy": False, "keys_count": 0, "error": "unreachable"}
            except Exception:
                return {"node_id": node.node_id, "is_healthy": False, "keys_count": 0, "error": "unreachable"}

        tasks = [get_health(n) for n in self.nodes]
        statuses = await asyncio.gather(*tasks)

        return {
            "config": self.config.to_dict(),
            "nodes": list(statuses),
            "metrics": self.metrics.to_dict(),
        }

    async def list_keys(self) -> list[str]:
        all_keys = set()

        async def get_keys(node: NodeConnection):
            try:
                resp = await self.client.get(f"{node.base_url}/store")
                if resp.status_code == 200:
                    return resp.json().get("keys", [])
                return []
            except Exception:
                return []

        tasks = [get_keys(n) for n in self.nodes]
        results = await asyncio.gather(*tasks)
        for keys in results:
            all_keys.update(keys)

        return sorted(all_keys)

    async def get_node_data(self, node_id: str) -> dict:
        for node in self.nodes:
            if node.node_id == node_id:
                try:
                    resp = await self.client.get(f"{node.base_url}/admin/data")
                    return resp.json()
                except Exception as e:
                    return {"error": str(e)}
        return {"error": f"Node {node_id} not found"}
