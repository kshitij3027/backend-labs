"""Service distribution analyzer: counts entries per service and per log level."""

from src.analyzers.registry import register_map, register_reduce


@register_map("service_distribution")
def service_distribution_map(record: dict) -> list[tuple[str, int]]:
    """Emit service name and log level counts."""
    results = []

    service = record.get("service", "unknown")
    results.append((f"service:{service}", 1))

    level = record.get("level", "unknown")
    results.append((f"level:{level}", 1))

    return results


@register_reduce("service_distribution")
def service_distribution_reduce(key: str, values: list) -> int:
    """Sum counts per service/level."""
    return sum(values)
