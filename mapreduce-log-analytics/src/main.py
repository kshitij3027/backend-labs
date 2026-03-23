"""Entry point for MapReduce Log Analytics server."""

import logging
import os

import uvicorn


def setup_logging():
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


if __name__ == "__main__":
    setup_logging()
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run("src.api:app", host="0.0.0.0", port=port, log_level="info")
