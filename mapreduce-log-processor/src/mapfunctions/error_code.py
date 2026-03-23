"""Error code map function."""

from src.mapfunctions.registry import register_map


@register_map("error_code")
def error_code_map(log_line: dict):
    """Emit (error_code, 1) for each log with an error code."""
    code = log_line.get("error_code")
    if code:
        yield (str(code), 1)
