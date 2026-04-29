"""Per-node runtime configuration loaded from environment variables.

Values come from environment (and optionally a ``.env`` file). The only
required value is ``NODE_ID`` — every other field has a sensible default
that lines up with the timing budget in plan.md.
"""

from __future__ import annotations

import hashlib

from pydantic_settings import BaseSettings, SettingsConfigDict


class NodeConfig(BaseSettings):
    """Configuration for a single failover node."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
    )

    # Required — must be set in the environment.
    node_id: str

    # Behavioural switches.
    is_primary: bool = False

    # Networking.
    port: int = 8001
    redis_host: str = "redis"
    redis_port: int = 6379

    # Failover timing.
    heartbeat_interval: float = 2.0
    heartbeat_timeout: float = 6.0
    election_timeout: float = 10.0
    state_sync_interval: float = 5.0
    lock_ttl: int = 6

    # Comma-separated list of peer host:port pairs.
    peer_nodes: str = ""

    def peer_list(self) -> list[tuple[str, int]]:
        """Parse the ``PEER_NODES`` CSV into a list of ``(host, port)`` tuples.

        Empty entries (and trailing commas) are ignored.
        """
        out: list[tuple[str, int]] = []
        if not self.peer_nodes:
            return out
        for raw in self.peer_nodes.split(","):
            entry = raw.strip()
            if not entry:
                continue
            host, _, port_str = entry.partition(":")
            host = host.strip()
            port_str = port_str.strip()
            if not host or not port_str:
                continue
            try:
                out.append((host, int(port_str)))
            except ValueError:
                # Silently skip malformed entries — we'd rather start with a
                # smaller peer set than crash the whole node on a typo.
                continue
        return out

    def priority(self) -> int:
        """Return a deterministic election priority in ``[0, 999]``.

        Python's built-in ``hash()`` is randomised per process (PYTHONHASHSEED),
        which would break consensus across nodes. We use an MD5 digest of the
        node_id instead, which is stable across processes and Python builds.
        """
        digest = hashlib.md5(self.node_id.encode("utf-8")).digest()
        return int.from_bytes(digest[:4], "big") % 1000
