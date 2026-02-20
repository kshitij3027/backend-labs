"""Per-connection message handler for NDJSON protocol."""

import asyncio
import json
import time

from server.config import ServerConfig
from server.persistence import LogPersistence

# Log level hierarchy (higher index = higher severity)
LOG_LEVELS = {
    "DEBUG": 0,
    "INFO": 1,
    "WARNING": 2,
    "ERROR": 3,
    "CRITICAL": 4,
}


class ConnectionHandler:
    """Handles a single TCP client connection using NDJSON protocol."""

    def __init__(self, config: ServerConfig, persistence: LogPersistence) -> None:
        self.config = config
        self.persistence = persistence
        self.batch: list[dict] = []
        self.last_flush = time.monotonic()

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle an incoming client connection.

        Reads NDJSON lines, validates them, batches for persistence,
        and sends responses.
        """
        peer = writer.get_extra_info("peername")
        peer_str = f"{peer[0]}:{peer[1]}" if peer else "unknown"
        print(f"[CONN] New connection from {peer_str}")

        try:
            while True:
                line = await reader.readline()
                if not line:
                    # EOF — client disconnected
                    break

                line_str = line.decode("utf-8", errors="replace").strip()
                if not line_str:
                    continue

                # Parse JSON
                try:
                    msg = json.loads(line_str)
                except json.JSONDecodeError:
                    response = {"status": "error", "message": "invalid JSON"}
                    writer.write(json.dumps(response).encode() + b"\n")
                    await writer.drain()
                    continue

                # Validate required fields
                if "level" not in msg or "message" not in msg:
                    response = {
                        "status": "error",
                        "message": "missing required fields",
                    }
                    writer.write(json.dumps(response).encode() + b"\n")
                    await writer.drain()
                    continue

                # Check log level filter — still respond ok, just don't persist
                if self._check_log_level(msg["level"]):
                    self.batch.append(msg)

                # Flush if batch is full or time threshold exceeded
                now = time.monotonic()
                if (
                    len(self.batch) >= self.config.BATCH_SIZE
                    or (now - self.last_flush) > (self.config.BATCH_FLUSH_MS / 1000)
                ):
                    await self._flush_batch()

                # Send success response
                response = {"status": "ok", "message": "received"}
                writer.write(json.dumps(response).encode() + b"\n")
                await writer.drain()

        except ConnectionResetError:
            print(f"[CONN] Connection reset by {peer_str}")
        except Exception as e:
            print(f"[CONN] Error handling {peer_str}: {e}")
        finally:
            # Flush any remaining messages in the batch
            await self._flush_batch()
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            print(f"[CONN] Connection closed: {peer_str}")

    async def _flush_batch(self) -> None:
        """Flush the current batch to persistence."""
        if self.batch and self.config.ENABLE_PERSISTENCE:
            await self.persistence.write_batch(self.batch)
            self.batch = []
            self.last_flush = time.monotonic()

    def _check_log_level(self, level: str) -> bool:
        """Check if the message level meets the minimum log level threshold.

        Returns True if the message should be persisted.
        """
        msg_level = LOG_LEVELS.get(level.upper(), -1)
        min_level = LOG_LEVELS.get(self.config.MIN_LOG_LEVEL.upper(), 0)
        return msg_level >= min_level
