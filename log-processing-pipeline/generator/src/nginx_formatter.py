"""Produces Nginx Combined Log Format lines (Apache + referer + user_agent)."""

import random
from datetime import datetime, timezone

from generator.src.pools import (
    IP_POOL, PATH_POOL, METHOD_POOL, METHOD_WEIGHTS, STATUS_POOL,
    USER_AGENT_POOL, REFERER_POOL,
)


def generate_nginx_line() -> str:
    """Return a single Nginx Combined Log Format line."""
    host = random.choice(IP_POOL)
    now = datetime.now(timezone.utc)
    time_str = now.strftime("%d/%b/%Y:%H:%M:%S %z")
    method = random.choices(METHOD_POOL, weights=METHOD_WEIGHTS, k=1)[0]
    path = random.choice(PATH_POOL)
    protocol = "HTTP/1.1"
    status = random.choice(STATUS_POOL)
    size = random.randint(128, 65536) if status != 204 else 0
    referer = random.choice(REFERER_POOL)
    user_agent = random.choice(USER_AGENT_POOL)

    return (
        f'{host} - - [{time_str}] "{method} {path} {protocol}" '
        f'{status} {size} "{referer}" "{user_agent}"'
    )
