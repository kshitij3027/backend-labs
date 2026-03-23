"""URL path map function."""

from src.mapfunctions.registry import register_map


@register_map("url_path")
def url_path_map(log_line: dict):
    """Emit (url_path, 1) for each log with a URL path."""
    url = log_line.get("url")
    if url:
        yield (url, 1)
