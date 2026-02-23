"""Schema validation utilities for Avro .avsc files."""

import glob
import json
import os
import re

from fastavro.schema import parse_schema

from src.compatibility import CompatibilityChecker
from src.deserializer import AvroDeserializer
from src.schema_registry import SchemaRegistry
from src.serializer import AvroSerializer


class SchemaValidator:
    """Validates individual Avro schemas and cross-version compatibility."""

    def __init__(self, schema_dir: str = None):
        """Initialize the validator.

        Args:
            schema_dir: Path to the directory containing .avsc files.
                        Defaults to the ``schemas/`` directory at the
                        project root (one level above ``src/``).
        """
        if schema_dir is None:
            src_dir = os.path.dirname(os.path.abspath(__file__))
            # Go up from src/validators/ -> src/ -> project root
            project_root = os.path.dirname(os.path.dirname(src_dir))
            schema_dir = os.path.join(project_root, "schemas")

        self._schema_dir = schema_dir

    def validate_schema(self, filepath: str) -> tuple[bool, str]:
        """Validate a single .avsc schema file.

        Loads the JSON from *filepath* and attempts to parse it with
        ``fastavro.schema.parse_schema()``.

        Args:
            filepath: Absolute or relative path to an ``.avsc`` file.

        Returns:
            A tuple of ``(True, "Valid")`` on success, or
            ``(False, error_message)`` on failure.
        """
        try:
            with open(filepath, "r") as f:
                raw_schema = json.load(f)
            parse_schema(raw_schema)
            return (True, "Valid")
        except Exception as e:
            return (False, str(e))

    def validate_all(self) -> dict:
        """Validate every ``.avsc`` file in the schema directory.

        Returns:
            A dict mapping version strings (e.g. ``"v1"``) to
            ``(bool, str)`` tuples from :meth:`validate_schema`.
        """
        results: dict[str, tuple[bool, str]] = {}
        pattern = os.path.join(self._schema_dir, "log_event_v*.avsc")
        for filepath in sorted(glob.glob(pattern)):
            filename = os.path.basename(filepath)
            match = re.search(r"log_event_(v\d+)\.avsc", filename)
            if not match:
                continue
            version = match.group(1)
            results[version] = self.validate_schema(filepath)
        return results

    def validate_cross_version_compatibility(self) -> dict:
        """Build a full cross-version compatibility report.

        Creates a :class:`SchemaRegistry`, :class:`AvroSerializer`,
        :class:`AvroDeserializer`, and :class:`CompatibilityChecker`,
        then produces an NxN compatibility matrix.

        Returns:
            A dict with keys ``total_pairs``, ``compatible_pairs``,
            ``matrix``, and ``all_compatible``.
        """
        registry = SchemaRegistry(self._schema_dir)
        serializer = AvroSerializer(registry)
        deserializer = AvroDeserializer(registry)
        checker = CompatibilityChecker(registry, serializer, deserializer)

        matrix = checker.build_compatibility_matrix()

        total_pairs = 0
        compatible_pairs = 0
        for writer in matrix:
            for reader in matrix[writer]:
                total_pairs += 1
                if matrix[writer][reader]:
                    compatible_pairs += 1

        return {
            "total_pairs": total_pairs,
            "compatible_pairs": compatible_pairs,
            "matrix": matrix,
            "all_compatible": total_pairs == compatible_pairs,
        }
