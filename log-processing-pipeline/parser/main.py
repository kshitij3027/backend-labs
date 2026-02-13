"""Parser entry point — polls for batch files and produces parsed JSON."""

import signal
import time

from shared.config_loader import load_yaml
from shared.metrics import Metrics
from parser.src.config import ParserConfig
from parser.src.state_tracker import StateTracker
from parser.src.parser import Parser


def main() -> None:
    cfg = ParserConfig.from_dict(load_yaml()["parser"])
    tracker = StateTracker(cfg.state_file)
    parser = Parser(cfg.input_dir, cfg.output_dir, tracker)
    metrics = Metrics("/data/parsed/.parser_metrics.json")

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"Parser: watching {cfg.input_dir}", flush=True)

    metrics_interval = 10
    last_metrics_save = time.time()

    while running:
        n = parser.poll_once()
        if n > 0:
            metrics.increment("entries_parsed", n)
            metrics.increment("files_parsed")
            print(f"Parser: parsed {n} entries", flush=True)

        now = time.time()
        if now - last_metrics_save >= metrics_interval:
            metrics.save()
            last_metrics_save = now

        time.sleep(cfg.poll_interval)

    metrics.save()
    print("Parser: shutdown complete", flush=True)


if __name__ == "__main__":
    main()
