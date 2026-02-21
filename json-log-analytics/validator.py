import json
from collections import defaultdict

import jsonschema


class LogValidator:
    """Validates log entries against a JSON schema."""

    def __init__(self, schema_path):
        with open(schema_path, "r") as f:
            schema = json.load(f)

        self._validator = jsonschema.Draft202012Validator(schema)
        self._stats = {
            "total": 0,
            "valid": 0,
            "invalid": 0,
            "error_types": defaultdict(int),
        }

    def validate(self, log_entry):
        """Validate a log entry against the schema.

        Returns:
            tuple: (is_valid: bool, errors: list[str])
        """
        self._stats["total"] += 1
        errors = list(self._validator.iter_errors(log_entry))

        if not errors:
            self._stats["valid"] += 1
            return True, []

        self._stats["invalid"] += 1
        error_messages = []
        for error in errors:
            self._stats["error_types"][error.validator] += 1
            error_messages.append(error.message)

        return False, error_messages

    def get_stats(self):
        """Return a copy of the stats dict."""
        stats = dict(self._stats)
        stats["error_types"] = dict(stats["error_types"])
        return stats

    def reset_stats(self):
        """Reset all stat counters."""
        self._stats = {
            "total": 0,
            "valid": 0,
            "invalid": 0,
            "error_types": defaultdict(int),
        }
