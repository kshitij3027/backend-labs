import httpx
import structlog

logger = structlog.get_logger()


class NodeClient:
    def __init__(self, node_url: str, timeout: float = 5.0):
        self.node_url = node_url.rstrip("/")
        self.timeout = timeout
        # Extract node name from URL for logging (e.g., "node-a" from "http://node-a:8001")
        self.node_id = node_url.split("//")[1].split(":")[0] if "//" in node_url else node_url

    def health(self) -> dict | None:
        """Returns health dict or None if unreachable."""
        try:
            resp = httpx.get(f"{self.node_url}/health", timeout=self.timeout)
            return resp.json()
        except Exception:
            return None

    def get_data(self, key: str) -> dict | None:
        """Get a single key's data. Returns dict or None if 404/error."""
        try:
            resp = httpx.get(f"{self.node_url}/data/{key}", timeout=self.timeout)
            if resp.status_code == 404:
                return None
            return resp.json()
        except Exception:
            return None

    def put_data(self, key: str, value: str, version: int, timestamp: float) -> bool:
        """Write a key-value pair. Returns True on success."""
        try:
            resp = httpx.put(
                f"{self.node_url}/data/{key}",
                json={"value": value, "version": version, "timestamp": timestamp},
                timeout=self.timeout,
            )
            return resp.status_code == 200
        except Exception:
            return False

    def get_merkle_root(self) -> str | None:
        """Returns root hash string or None if unreachable."""
        try:
            resp = httpx.get(f"{self.node_url}/merkle/root", timeout=self.timeout)
            return resp.json().get("root_hash")
        except Exception:
            return None

    def get_merkle_leaves(self) -> dict[str, str] | None:
        """Returns leaf hashes dict or None if unreachable."""
        try:
            resp = httpx.get(f"{self.node_url}/merkle/leaves", timeout=self.timeout)
            return resp.json().get("leaves")
        except Exception:
            return None
