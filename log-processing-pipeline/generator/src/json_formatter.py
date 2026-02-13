"""Produces structured JSON log lines."""

import json
import random
import uuid
from datetime import datetime, timezone

from generator.src.pools import SERVICE_POOL, MESSAGE_POOL

_LEVEL_WEIGHTS = {"INFO": 60, "WARNING": 20, "ERROR": 10, "DEBUG": 10}


def generate_json_line() -> str:
    """Return a single JSON log line."""
    level = random.choices(
        list(_LEVEL_WEIGHTS.keys()),
        weights=list(_LEVEL_WEIGHTS.values()),
        k=1,
    )[0]

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "service": random.choice(SERVICE_POOL),
        "message": random.choice(MESSAGE_POOL),
        "request_id": uuid.uuid4().hex[:12],
        "duration_ms": random.randint(1, 2000),
    }
    return json.dumps(entry, separators=(",", ":"))
