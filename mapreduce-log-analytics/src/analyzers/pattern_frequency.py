"""Pattern frequency analyzer: detects error patterns, IPs, and HTTP status codes."""

import re

from src.analyzers.registry import register_map, register_reduce

IP_REGEX = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')
ERROR_KEYWORDS = [
    "error", "exception", "timeout", "failed", "refused",
    "denied", "crash", "fault", "unavailable", "expired",
]


@register_map("pattern_frequency")
def pattern_frequency_map(record: dict) -> list[tuple[str, int]]:
    """Detect patterns in log records: error keywords, IPs, HTTP status codes."""
    results = []
    message = record.get("message", "").lower()

    # Error/exception pattern detection
    for keyword in ERROR_KEYWORDS:
        if keyword in message:
            results.append((f"error_pattern:{keyword}", 1))

    # IP address detection from the ip field
    ip = record.get("ip", "")
    if ip and IP_REGEX.match(ip):
        results.append((f"ip_address:{ip}", 1))

    # Also search for IPs in the message
    for ip_match in IP_REGEX.findall(message):
        results.append((f"ip_address:{ip_match}", 1))

    # HTTP status code from record
    status_code = record.get("status_code")
    if status_code is not None:
        results.append((f"http_status:{status_code}", 1))

    return results


@register_reduce("pattern_frequency")
def pattern_frequency_reduce(key: str, values: list) -> int:
    """Sum counts per pattern."""
    return sum(values)
