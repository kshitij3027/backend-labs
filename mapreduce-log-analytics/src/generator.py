"""Log data generator producing JSONL and Apache format logs."""

import json
import random
from datetime import datetime, timedelta, timezone


SERVICES = [
    "auth-service",
    "api-gateway",
    "payment-service",
    "user-service",
    "notification-service",
]

LEVELS = ["INFO", "WARN", "ERROR", "FATAL"]
LEVEL_WEIGHTS = [60, 20, 15, 5]

IP_POOL = [
    "192.168.1.10",
    "192.168.1.22",
    "192.168.1.33",
    "192.168.1.44",
    "192.168.1.55",
    "192.168.1.100",
    "192.168.1.150",
    "192.168.1.200",
    "10.0.0.1",
    "10.0.0.5",
    "10.0.0.12",
    "10.0.0.25",
    "10.0.0.50",
    "10.0.0.100",
    "172.16.0.3",
    "172.16.0.7",
    "172.16.0.15",
    "172.16.0.22",
    "203.0.113.5",
    "198.51.100.14",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/17.2",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 Mobile",
    "curl/8.4.0",
    "python-requests/2.31.0",
    "PostmanRuntime/7.36.0",
]

URLS = [
    "/api/users",
    "/api/orders",
    "/api/products",
    "/health",
    "/login",
    "/api/search",
    "/api/payments",
    "/api/admin",
    "/api/reports",
]

STATUS_CODES = [200, 201, 301, 400, 404, 500, 503]
STATUS_WEIGHTS = [60, 10, 5, 8, 7, 7, 3]

METHODS = ["GET", "POST", "PUT", "DELETE"]
METHOD_WEIGHTS = [70, 20, 5, 5]

INFO_MESSAGES = [
    "Request processed successfully",
    "User authenticated",
    "Cache hit for resource",
    "Health check passed",
    "Connection established",
    "Session created for user",
    "Data fetched from database",
    "Response sent to client",
    "Background job completed",
    "Configuration reloaded",
]

WARN_MESSAGES = [
    "Slow query detected (>500ms)",
    "Rate limit approaching for client",
    "Deprecated API version used",
    "Memory usage above 80%",
    "Connection pool running low",
    "Retry attempt 2 of 3",
    "Disk usage above 75%",
    "Certificate expiring in 30 days",
]

ERROR_MESSAGES = [
    "Database connection timeout",
    "Authentication failed for user",
    "Invalid request payload",
    "Service unavailable: upstream timeout",
    "File not found: resource missing",
    "Permission denied: insufficient privileges",
    "Rate limit exceeded",
    "Internal server error: null pointer",
    "Failed to parse request body",
    "Connection refused by downstream service",
]

FATAL_MESSAGES = [
    "Out of memory: killing process",
    "Unrecoverable database corruption detected",
    "Critical security breach detected",
    "System crash: segmentation fault",
    "Disk full: cannot write data",
]

# Security-relevant messages for the security analyzer
SECURITY_MESSAGES = [
    "SQL injection attempt detected in query parameter",
    "Brute force login attempt from {ip}",
    "Unauthorized access attempt to /api/admin",
    "XSS payload detected in form input",
    "Failed login attempt #{n} for user admin",
    "Suspicious user-agent detected: sqlmap/1.7",
    "CSRF token validation failed",
    "Access denied: invalid API key",
    "Potential directory traversal attack: ../../etc/passwd",
    "Multiple failed authentication attempts from {ip}",
]


def _pick_message(rng: random.Random, level: str) -> str:
    """Pick a message appropriate for the log level."""
    if level == "INFO":
        return rng.choice(INFO_MESSAGES)
    elif level == "WARN":
        return rng.choice(WARN_MESSAGES)
    elif level == "ERROR":
        return rng.choice(ERROR_MESSAGES)
    else:  # FATAL
        return rng.choice(FATAL_MESSAGES)


def generate_json_logs(
    output_path: str, num_lines: int = 10_000, seed: int = 42
) -> dict:
    """Generate JSONL-format log data.

    Returns a stats dict with counts by level, service, and total lines.
    """
    rng = random.Random(seed)
    base_time = datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    seconds_in_day = 86400

    level_counts: dict[str, int] = {}
    service_counts: dict[str, int] = {}
    status_counts: dict[int, int] = {}

    with open(output_path, "w") as f:
        for i in range(num_lines):
            timestamp = base_time + timedelta(
                seconds=rng.randint(0, seconds_in_day)
            )
            level = rng.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
            service = rng.choice(SERVICES)
            ip = rng.choice(IP_POOL)
            url = rng.choice(URLS)
            status_code = rng.choices(
                STATUS_CODES, weights=STATUS_WEIGHTS, k=1
            )[0]
            user_agent = rng.choice(USER_AGENTS)
            user_id = f"user-{rng.randint(1, 500)}"

            # Inject security-related messages periodically
            if i % 50 == 0 and level in ("ERROR", "WARN", "FATAL"):
                msg_template = rng.choice(SECURITY_MESSAGES)
                message = msg_template.replace("{ip}", ip).replace(
                    "{n}", str(rng.randint(3, 20))
                )
            else:
                message = _pick_message(rng, level)

            record = {
                "timestamp": timestamp.isoformat(),
                "level": level,
                "service": service,
                "message": message,
                "ip": ip,
                "url": url,
                "status_code": status_code,
                "user_agent": user_agent,
                "user_id": user_id,
            }

            f.write(json.dumps(record) + "\n")

            level_counts[level] = level_counts.get(level, 0) + 1
            service_counts[service] = service_counts.get(service, 0) + 1
            status_counts[status_code] = status_counts.get(status_code, 0) + 1

    return {
        "total_lines": num_lines,
        "level_counts": level_counts,
        "service_counts": service_counts,
        "status_counts": status_counts,
        "output_path": output_path,
    }


def generate_apache_logs(
    output_path: str, num_lines: int = 5_000, seed: int = 42
) -> dict:
    """Generate Apache combined log format data.

    Returns a stats dict with counts by method, status, and total lines.
    """
    rng = random.Random(seed)
    base_time = datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    seconds_in_day = 86400

    method_counts: dict[str, int] = {}
    status_counts: dict[int, int] = {}

    referrers = [
        "-",
        "https://example.com",
        "https://google.com/search?q=test",
        "https://internal.app/dashboard",
    ]

    with open(output_path, "w") as f:
        for _ in range(num_lines):
            timestamp = base_time + timedelta(
                seconds=rng.randint(0, seconds_in_day)
            )
            ip = rng.choice(IP_POOL)
            method = rng.choices(METHODS, weights=METHOD_WEIGHTS, k=1)[0]
            url = rng.choice(URLS)
            status = rng.choices(
                STATUS_CODES, weights=STATUS_WEIGHTS, k=1
            )[0]
            size = rng.randint(128, 65536)
            referrer = rng.choice(referrers)
            user_agent = rng.choice(USER_AGENTS)

            # Apache combined log format
            ts_str = timestamp.strftime("%d/%b/%Y:%H:%M:%S +0000")
            line = (
                f'{ip} - - [{ts_str}] '
                f'"{method} {url} HTTP/1.1" {status} {size} '
                f'"{referrer}" "{user_agent}"'
            )
            f.write(line + "\n")

            method_counts[method] = method_counts.get(method, 0) + 1
            status_counts[status] = status_counts.get(status, 0) + 1

    return {
        "total_lines": num_lines,
        "method_counts": method_counts,
        "status_counts": status_counts,
        "output_path": output_path,
    }
