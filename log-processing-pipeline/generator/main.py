"""Generator entry point — produces log lines at a configurable rate."""

import signal
import sys
import time

from shared.config_loader import load_yaml
from generator.src.config import GeneratorConfig
from generator.src.apache_formatter import generate_apache_line
from generator.src.writer import LogFileWriter


def main() -> None:
    cfg = GeneratorConfig.from_dict(load_yaml()["generator"])
    writer = LogFileWriter(cfg.log_file)

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    interval = 1.0 / cfg.rate
    print(f"Generator: writing to {cfg.log_file} at {cfg.rate} lines/sec", flush=True)

    while running:
        line = generate_apache_line()
        writer.write(line)
        time.sleep(interval)

    writer.close()
    print("Generator: shutdown complete", flush=True)


if __name__ == "__main__":
    main()
