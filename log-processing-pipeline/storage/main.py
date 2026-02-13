"""Storage entry point — ingests parsed JSON into NDJSON with indexing."""

import os
import signal
import time

from shared.config_loader import load_yaml
from storage.src.config import StorageConfig
from storage.src.state_tracker import StateTracker
from storage.src.indexer import Indexer
from storage.src.rotator import Rotator
from storage.src.storage import StorageEngine


def main() -> None:
    cfg = StorageConfig.from_dict(load_yaml()["storage"])
    tracker = StateTracker(cfg.state_file)
    indexer = Indexer(os.path.join(cfg.storage_dir, "index"))
    rotator = Rotator(
        active_dir=os.path.join(cfg.storage_dir, "active"),
        archive_dir=os.path.join(cfg.storage_dir, "archive"),
        size_threshold_bytes=int(cfg.rotation_size_mb * 1024 * 1024),
        age_threshold_seconds=cfg.rotation_hours * 3600,
    )
    engine = StorageEngine(cfg.input_dir, cfg.storage_dir, tracker, indexer, rotator)

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"Storage: watching {cfg.input_dir}", flush=True)

    while running:
        n = engine.poll_once()
        if n > 0:
            print(f"Storage: stored {n} entries", flush=True)
        time.sleep(cfg.poll_interval)

    print("Storage: shutdown complete", flush=True)


if __name__ == "__main__":
    main()
