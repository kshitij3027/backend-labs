"""CLI entry for the coordinator (``python -m src.coordinator``)."""

from __future__ import annotations

import uvicorn

from src.shared.config import CoordinatorSettings

from .app import create_coordinator_app


def main() -> None:
    settings = CoordinatorSettings()
    app = create_coordinator_app(settings)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=settings.coordinator_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
