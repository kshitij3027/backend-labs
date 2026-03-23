"""Security log analyzer: IP extraction, 404 detection, hour-of-day, user-agent tracking."""

from datetime import datetime
from src.analyzers.registry import register_map, register_reduce, register_postprocess


@register_map("security")
def security_map(record: dict) -> list[tuple[str, int]]:
    """Emit entries for 4 security categories."""
    results = []

    # 1. IP address extraction
    ip = record.get("ip", "")
    if ip:
        results.append((f"ip:{ip}", 1))

    # 2. 404 error detection
    status_code = record.get("status_code")
    if status_code == 404:
        url = record.get("url", "unknown")
        results.append((f"404_error:{url}", 1))

    # 3. Hour-of-day extraction from timestamp
    timestamp = record.get("timestamp", "")
    if timestamp:
        try:
            # Handle ISO format timestamps
            dt = datetime.fromisoformat(timestamp)
            results.append((f"hour:{dt.hour:02d}", 1))
        except (ValueError, TypeError):
            pass

    # 4. User-agent tracking
    user_agent = record.get("user_agent", "")
    if user_agent:
        results.append((f"user_agent:{user_agent}", 1))

    return results


@register_reduce("security")
def security_reduce(key: str, values: list) -> int:
    """Sum counts per key."""
    return sum(values)


@register_postprocess("security")
def security_postprocess(results: dict) -> dict:
    """Group results by category prefix and return top-10 per category."""
    categories = {
        "top_ips": [],
        "top_404_paths": [],
        "peak_hours": [],
        "top_user_agents": [],
    }

    prefix_map = {
        "ip:": "top_ips",
        "404_error:": "top_404_paths",
        "hour:": "peak_hours",
        "user_agent:": "top_user_agents",
    }

    for key, value in results.items():
        for prefix, category in prefix_map.items():
            if key.startswith(prefix):
                label = key[len(prefix):]
                categories[category].append({"key": label, "count": value})
                break

    # Sort each category by count descending, take top 10
    for cat_name in categories:
        categories[cat_name] = sorted(
            categories[cat_name], key=lambda x: x["count"], reverse=True
        )[:10]

    return categories
