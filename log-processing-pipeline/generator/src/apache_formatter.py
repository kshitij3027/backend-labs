"""Produces Apache Combined Log Format lines.

Format must match the regex in the sibling log-parsing-service/src/parsers.py
so that the parser can reliably parse every generated line.
"""

import random
from datetime import datetime, timezone

IP_POOL = [
    "192.168.1.1", "10.0.0.42", "172.16.0.5", "203.0.113.7",
    "198.51.100.23", "192.0.2.88", "10.10.10.10", "172.31.255.1",
]

PATH_POOL = [
    "/", "/index.html", "/api/users", "/api/orders", "/api/products",
    "/api/health", "/login", "/logout", "/dashboard", "/static/app.js",
    "/images/logo.png", "/api/search?q=test", "/docs", "/api/v2/items",
]

METHOD_POOL = ["GET", "POST", "PUT", "DELETE", "PATCH"]
METHOD_WEIGHTS = [60, 15, 10, 10, 5]

# Weighted status distribution: 60% 2xx, 10% 3xx, 20% 4xx, 10% 5xx
STATUS_POOL = [200, 200, 200, 201, 204, 301, 302, 400, 401, 403, 404, 404, 500, 502, 503]


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
