"""Validators for parsed log entries."""
from typing import List, Tuple
from src.models import ParsedLog


def validate_parsed_log(log: ParsedLog) -> Tuple[bool, List[str]]:
    """
    Validate a parsed log entry.

    Returns (is_valid, list_of_errors).
    A log is valid if it has at minimum a message and source_format.
    """
    errors = []

    if not log.message or not log.message.strip():
        errors.append("Missing or empty message field")

    if not log.source_format or not log.source_format.strip():
        errors.append("Missing or empty source_format field")

    if log.confidence < 0.0 or log.confidence > 1.0:
        errors.append(f"Confidence {log.confidence} out of range [0.0, 1.0]")

    if log.priority is not None and (log.priority < 0 or log.priority > 191):
        errors.append(f"Priority {log.priority} out of valid range [0, 191]")

    return (len(errors) == 0, errors)
