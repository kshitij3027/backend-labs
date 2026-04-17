from __future__ import annotations

import uvicorn

from src.shared.config import PartitionSettings

from .app import create_partition_app


def main() -> None:
    """Entry-point used by ``python -m src.partition`` and by the Dockerfile.

    Loads :class:`PartitionSettings` from the environment, constructs the
    FastAPI app, and runs uvicorn bound to the configured port.
    """

    settings = PartitionSettings()
    app = create_partition_app(settings)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=settings.partition_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
