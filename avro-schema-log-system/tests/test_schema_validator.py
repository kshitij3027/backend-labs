"""Tests for the SchemaValidator utility."""

import os

from src.validators.schema_validator import SchemaValidator


def test_validate_all_schemas_valid():
    """Every .avsc file in schemas/ should parse successfully."""
    validator = SchemaValidator()
    results = validator.validate_all()

    assert len(results) > 0, "No schemas found to validate"
    for version, (is_valid, message) in results.items():
        assert is_valid, f"Schema {version} failed validation: {message}"
        assert message == "Valid"


def test_cross_version_compatibility():
    """All writer/reader version pairs should be compatible."""
    validator = SchemaValidator()
    report = validator.validate_cross_version_compatibility()

    assert report["all_compatible"] is True
    assert report["compatible_pairs"] == 9
    assert report["total_pairs"] == 9
    assert len(report["matrix"]) == 3


def test_validate_single_schema():
    """Validating an individual .avsc file should succeed."""
    validator = SchemaValidator()
    # Resolve the path to schemas/log_event_v1.avsc
    src_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(src_dir)
    filepath = os.path.join(project_root, "schemas", "log_event_v1.avsc")

    is_valid, message = validator.validate_schema(filepath)
    assert is_valid is True
    assert message == "Valid"
