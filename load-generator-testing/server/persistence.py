"""Async batched file writer for log persistence."""

import os
from datetime import datetime, timezone

import aiofiles


class LogPersistence:
    """Persists log messages to file using async I/O."""

    def __init__(self, log_dir: str) -> None:
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self.log_path = os.path.join(log_dir, "server.log")

    async def write_batch(self, messages: list[dict]) -> None:
        """Write a batch of log messages to the log file.

        Each message is written as: {ISO timestamp} [{LEVEL}] {message}
        """
        async with aiofiles.open(self.log_path, mode="a") as f:
            for msg in messages:
                timestamp = datetime.now(timezone.utc).isoformat()
                level = msg.get("level", "UNKNOWN")
                message = msg.get("message", "")
                line = f"{timestamp} [{level}] {message}\n"
                await f.write(line)

    async def close(self) -> None:
        """No persistent file handle to close in the simple approach."""
        pass
