"""CLI entry point for the UDP log client."""

import argparse
import logging
import sys
import time

from src.client import UDPLogClient

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(description="UDP Log Client")
    parser.add_argument("--server", default="localhost", help="Server host")
    parser.add_argument("--port", type=int, default=5514, help="Server port")
    parser.add_argument("--count", type=int, default=20, help="Number of logs to send")
    parser.add_argument("--interval", type=float, default=0.1, help="Seconds between logs")
    parser.add_argument("--app", default="udp-client", help="Application name")
    args = parser.parse_args()

    client = UDPLogClient(args.server, args.port, args.app)
    try:
        client.generate_sample_logs(args.count, args.interval)
        time.sleep(0.5)
        acks = client.get_acks()
        logger.info("Received %d ACKs for ERROR logs: sequences %s", len(acks), list(acks.keys()))
    finally:
        client.close()


if __name__ == "__main__":
    main()
