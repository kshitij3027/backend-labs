"""Schema registry for loading and managing Avro schema versions."""

import glob
import json
import os
import re
from copy import deepcopy

from fastavro.schema import parse_schema


class SchemaRegistry:
    """Loads and manages versioned Avro schemas from disk."""

    def __init__(self, schema_dir: str = None):
        """Initialize the registry by loading all schema versions.

        Args:
            schema_dir: Path to the directory containing .avsc files.
                        Defaults to the ``schemas/`` directory at the
                        project root (one level above ``src/``).
        """
        if schema_dir is None:
            src_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(src_dir)
            schema_dir = os.path.join(project_root, "schemas")

        self._raw_schemas: dict[str, dict] = {}
        self._parsed_schemas: dict[str, dict] = {}

        self._load_schemas(schema_dir)

    def _load_schemas(self, schema_dir: str) -> None:
        """Discover and load all ``log_event_v*.avsc`` files."""
        pattern = os.path.join(schema_dir, "log_event_v*.avsc")
        for filepath in sorted(glob.glob(pattern)):
            filename = os.path.basename(filepath)
            match = re.search(r"log_event_(v\d+)\.avsc", filename)
            if not match:
                continue
            version = match.group(1)

            with open(filepath, "r") as f:
                raw_schema = json.load(f)

            self._raw_schemas[version] = raw_schema

            # parse_schema mutates in place, so work on a deep copy
            schema_copy = deepcopy(raw_schema)
            self._parsed_schemas[version] = parse_schema(schema_copy)

    def get_schema(self, version: str) -> dict:
        """Return the raw (unmodified) schema dict for *version*.

        Args:
            version: Schema version string, e.g. ``"v1"``.

        Raises:
            KeyError: If the version is not loaded.
        """
        return self._raw_schemas[version]

    def get_parsed_schema(self, version: str) -> dict:
        """Return the fastavro-parsed schema for *version*.

        Args:
            version: Schema version string, e.g. ``"v1"``.

        Raises:
            KeyError: If the version is not loaded.
        """
        return self._parsed_schemas[version]

    def list_versions(self) -> list[str]:
        """Return a sorted list of loaded version strings."""
        return sorted(self._raw_schemas.keys())

    def get_field_names(self, version: str) -> list[str]:
        """Return the field names defined in the given schema version.

        Args:
            version: Schema version string, e.g. ``"v1"``.

        Raises:
            KeyError: If the version is not loaded.
        """
        return [field["name"] for field in self._raw_schemas[version]["fields"]]
