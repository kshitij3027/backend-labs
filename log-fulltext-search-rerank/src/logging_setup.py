"""Structured JSON logging for the full-text log search service.

Every module uses stdlib ``logging`` normally; :func:`configure_logging`
is called once at app-build time to install a JSON formatter on the
root logger. Each record becomes a single JSON object on stdout so
container log collectors can parse it without regex.

Kept intentionally lean: stdlib ``logging`` plus ``json.dumps`` — no
new dependencies.
"""

import json
import logging
import sys


class JsonFormatter(logging.Formatter):
    """Render a :class:`logging.LogRecord` as one JSON line.

    Always emits ``ts``, ``level``, ``logger``, ``message``. If the
    record carries ``exc_info`` (e.g. from ``logger.exception``) the
    formatted traceback is included so container log readers can see
    the stack without a second round trip.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"))


def configure_logging(level: str = "INFO") -> None:
    """Configure root logging once per process.

    ``level`` is matched case-insensitively; ``WARN`` is normalised to
    ``WARNING`` so either dialect works. Existing handlers on the root
    logger are replaced so repeated calls don't double-emit each
    record (app-factory tests rebuild the app between cases).
    """
    normalized = "WARNING" if level.upper() == "WARN" else level.upper()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(normalized)
