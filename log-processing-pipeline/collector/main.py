"""Collector entry point — polls the source log file for new lines."""

import signal
import time

from shared.config_loader import load_yaml
from collector.src.config import CollectorConfig
from collector.src.offset_tracker import OffsetTracker
from collector.src.collector import Collector


def main() -> None:
    cfg = CollectorConfig.from_dict(load_yaml()["collector"])
    tracker = OffsetTracker(cfg.state_file)
    collector = Collector(cfg.source_file, cfg.output_dir, cfg.batch_size, tracker)

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"Collector: watching {cfg.source_file}, batch size={cfg.batch_size}", flush=True)

    while running:
        n = collector.poll_once()
        if n > 0:
            print(f"Collector: collected {n} lines", flush=True)
        time.sleep(cfg.poll_interval)

    print("Collector: shutdown complete", flush=True)


if __name__ == "__main__":
    main()
