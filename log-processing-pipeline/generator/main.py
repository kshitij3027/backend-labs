"""Generator entry point — produces log lines at a configurable rate."""

import random
import signal
import time

from shared.config_loader import load_yaml
from generator.src.config import GeneratorConfig
from generator.src.apache_formatter import generate_apache_line
from generator.src.nginx_formatter import generate_nginx_line
from generator.src.syslog_formatter import generate_syslog_line
from generator.src.json_formatter import generate_json_line
from generator.src.writer import LogFileWriter

_FORMAT_MAP = {
    "apache": generate_apache_line,
    "nginx": generate_nginx_line,
    "syslog": generate_syslog_line,
    "json": generate_json_line,
}

_MULTI_FORMATS = ["apache", "nginx", "syslog", "json"]
_MULTI_WEIGHTS = [40, 25, 20, 15]


def _get_line_generator(fmt: str):
    if fmt == "multi":
        def _multi():
            chosen = random.choices(_MULTI_FORMATS, weights=_MULTI_WEIGHTS, k=1)[0]
            return _FORMAT_MAP[chosen]()
        return _multi
    return _FORMAT_MAP[fmt]


def main() -> None:
    cfg = GeneratorConfig.from_dict(load_yaml()["generator"])
    writer = LogFileWriter(cfg.log_file)
    gen_line = _get_line_generator(cfg.format)

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    interval = 1.0 / cfg.rate
    print(f"Generator: writing to {cfg.log_file} at {cfg.rate} lines/sec (format={cfg.format})",
          flush=True)

    while running:
        line = gen_line()
        writer.write(line)
        time.sleep(interval)

    writer.close()
    print("Generator: shutdown complete", flush=True)


if __name__ == "__main__":
    main()
