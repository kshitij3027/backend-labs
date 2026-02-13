"""Produces Apache Combined Log Format lines.

Format must match the regex in the sibling log-parsing-service/src/parsers.py
so that the parser can reliably parse every generated line.
"""

import random
from datetime import datetime, timezone

from generator.src.pools import IP_POOL, PATH_POOL, METHOD_POOL, METHOD_WEIGHTS, STATUS_POOL


def generate_apache_line() -> str:
    """Return a single Apache Combined Log Format line."""
    host = random.choice(IP_POOL)
    now = datetime.now(timezone.utc)
    time_str = now.strftime("%d/%b/%Y:%H:%M:%S %z")
    method = random.choices(METHOD_POOL, weights=METHOD_WEIGHTS, k=1)[0]
    path = random.choice(PATH_POOL)
    protocol = "HTTP/1.1"
    status = random.choice(STATUS_POOL)
    size = random.randint(128, 65536) if status != 204 else 0

    return f'{host} - - [{time_str}] "{method} {path} {protocol}" {status} {size}'
