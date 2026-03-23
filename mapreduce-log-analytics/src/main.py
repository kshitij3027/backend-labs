import logging
import os


def setup_logging():
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


if __name__ == "__main__":
    setup_logging()
    logging.getLogger(__name__).info("MapReduce Log Analytics starting...")
