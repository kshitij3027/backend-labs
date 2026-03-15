"""Entry point for the priority queue log processor."""

import logging

from src.app import create_app
from src.config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
)


def main():
    settings = load_config()
    app = create_app(settings)
    print(f"   Dashboard: http://0.0.0.0:{settings.dashboard_port}")
    print(f"   Health: http://0.0.0.0:{settings.dashboard_port}/health")
    app.run(host="0.0.0.0", port=settings.dashboard_port, threaded=True)


if __name__ == "__main__":
    main()
