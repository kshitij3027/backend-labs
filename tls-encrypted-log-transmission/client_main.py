"""TLS Log Client — entry point."""

import logging
import sys

from src.config import load_client_config
from src.client import TLSLogClient
from src.simulation import run_simulation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main():
    config = load_client_config()
    client = TLSLogClient(config)

    try:
        print(f"[CLIENT] Connecting to {config.host}:{config.port}...")
        client.connect_with_retry()
        run_simulation(client)
        client.print_stats()
    except Exception as e:
        print(f"[CLIENT] Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
