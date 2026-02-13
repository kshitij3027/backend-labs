"""Parser entry point — polls for batch files and produces parsed JSON."""

import signal
import time

from shared.config_loader import load_yaml
from parser.src.config import ParserConfig
from parser.src.state_tracker import StateTracker
from parser.src.parser import Parser


def main() -> None:
    cfg = ParserConfig.from_dict(load_yaml()["parser"])
    tracker = StateTracker(cfg.state_file)
    parser = Parser(cfg.input_dir, cfg.output_dir, tracker)

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"Parser: watching {cfg.input_dir}", flush=True)

    while running:
        n = parser.poll_once()
        if n > 0:
            print(f"Parser: parsed {n} entries", flush=True)
        time.sleep(cfg.poll_interval)

    print("Parser: shutdown complete", flush=True)


if __name__ == "__main__":
    main()
