"""Collector entry point — polls the source log file for new lines."""

import signal
import time

from shared.config_loader import load_yaml
from shared.metrics import Metrics
from collector.src.config import CollectorConfig
from collector.src.offset_tracker import OffsetTracker
from collector.src.collector import Collector
from collector.src.filter import RawLineFilter


def main() -> None:
    cfg = CollectorConfig.from_dict(load_yaml()["collector"])
    tracker = OffsetTracker(cfg.state_file)
    line_filter = RawLineFilter(list(cfg.filters)) if cfg.filters else None
    collector = Collector(cfg.source_file, cfg.output_dir, cfg.batch_size, tracker, line_filter)
    metrics = Metrics("/data/collected/.collector_metrics.json")

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"Collector: watching {cfg.source_file}, batch size={cfg.batch_size}", flush=True)

    metrics_interval = 10
    last_metrics_save = time.time()

    while running:
        metrics.increment("polls_performed")
        n = collector.poll_once()
        if n > 0:
            metrics.increment("lines_collected", n)
            metrics.increment("batches_written")
            print(f"Collector: collected {n} lines", flush=True)
        else:
            metrics.increment("empty_polls")

        now = time.time()
        if now - last_metrics_save >= metrics_interval:
            metrics.save()
            last_metrics_save = now

        time.sleep(cfg.poll_interval)

    metrics.save()
    print("Collector: shutdown complete", flush=True)


if __name__ == "__main__":
    main()
