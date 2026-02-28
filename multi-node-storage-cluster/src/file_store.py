"""File-based storage engine for a single cluster node.

Provides thread-safe read, write, list, and replication operations
backed by JSON files on the local filesystem.
"""

import hashlib
import json
import os
import threading
import time
import uuid


class FileStore:
    """Manages file-based storage for a single node.

    Each write creates a uniquely-named JSON file containing the user
    data plus internal metadata (version, checksum, timestamps).
    All public methods are thread-safe.
    """

    def __init__(self, storage_dir: str, node_id: str, version_manager=None):
        self.storage_dir = storage_dir
        self.node_id = node_id
        self.version_manager = version_manager
        os.makedirs(storage_dir, exist_ok=True)
        # Thread-safe counters
        self._lock = threading.Lock()
        self._stats = {"writes": 0, "reads": 0, "replications_received": 0}

    def write(self, data: dict) -> dict:
        """Write log data to a new file.

        Args:
            data: Arbitrary JSON-serialisable payload to persist.

        Returns:
            Dict with keys ``file_path``, ``checksum``, and ``version``.
        """
        timestamp = int(time.time() * 1000)
        file_id = uuid.uuid4().hex[:8]
        file_path = f"log_{timestamp}_{file_id}.json"

        checksum = hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()

        version = self.version_manager.next_version(file_path) if self.version_manager else 1

        record = {
            "data": data,
            "metadata": {
                "version": version,
                "checksum": checksum,
                "created_at": time.time(),
                "node_id": self.node_id,
                "file_path": file_path,
            },
        }

        full_path = os.path.join(self.storage_dir, file_path)
        with open(full_path, "w") as f:
            json.dump(record, f, indent=2)

        with self._lock:
            self._stats["writes"] += 1

        return {"file_path": file_path, "checksum": checksum, "version": version}

    def read(self, file_path: str) -> dict | None:
        """Read a stored file by its path.

        Args:
            file_path: Filename (relative to *storage_dir*) to read.

        Returns:
            The full record dict, or ``None`` if the file does not exist.
        """
        full_path = os.path.join(self.storage_dir, file_path)
        if not os.path.exists(full_path):
            return None

        with open(full_path, "r") as f:
            record = json.load(f)

        with self._lock:
            self._stats["reads"] += 1

        return record

    def list_files(self) -> list[str]:
        """List all stored JSON file paths, sorted alphabetically."""
        if not os.path.exists(self.storage_dir):
            return []
        return sorted(
            f for f in os.listdir(self.storage_dir) if f.endswith(".json")
        )

    def write_replica(self, file_path: str, data: dict, metadata: dict) -> dict:
        """Write a replica received from another node.

        Args:
            file_path: Target filename for the replica.
            data: The payload section of the record.
            metadata: The metadata section of the record.

        Returns:
            Dict with ``file_path`` and ``status``.
        """
        record = {"data": data, "metadata": metadata}
        full_path = os.path.join(self.storage_dir, file_path)
        with open(full_path, "w") as f:
            json.dump(record, f, indent=2)

        if self.version_manager:
            self.version_manager.set_version(file_path, metadata.get("version", 1))

        with self._lock:
            self._stats["replications_received"] += 1

        return {"file_path": file_path, "status": "replicated"}

    def rotate(self, max_files: int = 1000, max_age_hours: int = 24) -> dict:
        """Remove old files to keep storage under limits.

        Args:
            max_files: Maximum number of files to retain.
            max_age_hours: Remove files older than this many hours.

        Returns:
            Dict with rotation stats.
        """
        files = self.list_files()
        removed_count = 0
        now = time.time()
        max_age_seconds = max_age_hours * 3600

        # Remove files older than max_age
        for f in files:
            full_path = os.path.join(self.storage_dir, f)
            try:
                mtime = os.path.getmtime(full_path)
                if now - mtime > max_age_seconds:
                    os.remove(full_path)
                    removed_count += 1
            except OSError:
                pass

        # If still over max_files, remove oldest first
        remaining = self.list_files()
        if len(remaining) > max_files:
            # Sort by modification time, oldest first
            by_mtime = sorted(
                remaining,
                key=lambda f: os.path.getmtime(os.path.join(self.storage_dir, f)),
            )
            to_remove = by_mtime[: len(remaining) - max_files]
            for f in to_remove:
                try:
                    os.remove(os.path.join(self.storage_dir, f))
                    removed_count += 1
                except OSError:
                    pass

        return {
            "removed": removed_count,
            "remaining": len(self.list_files()),
        }

    def get_storage_stats(self) -> dict:
        """Return storage statistics."""
        files = self.list_files()
        total_size = 0
        for f in files:
            full_path = os.path.join(self.storage_dir, f)
            try:
                total_size += os.path.getsize(full_path)
            except OSError:
                pass

        return {
            "files_count": len(files),
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "storage_dir": self.storage_dir,
        }

    def get_stats(self) -> dict:
        """Return a snapshot of the current operation counters."""
        with self._lock:
            return dict(self._stats)
