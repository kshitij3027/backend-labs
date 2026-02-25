"""Tests for schema validators."""
from src.validators import JsonSchemaValidator, AvroSchemaValidator, ValidatorManager


class TestJsonSchemaValidator:
    def test_compile_and_validate_valid(self):
        v = JsonSchemaValidator()
        schema = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        v.compile(1, schema)
        valid, errors = v.validate(1, {"name": "Alice"})
        assert valid is True
        assert errors == []

    def test_validate_invalid_data(self):
        v = JsonSchemaValidator()
        schema = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        v.compile(1, schema)
        valid, errors = v.validate(1, {"name": 123})
        assert valid is False
        assert len(errors) > 0

    def test_validate_missing_required(self):
        v = JsonSchemaValidator()
        schema = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        v.compile(1, schema)
        valid, errors = v.validate(1, {})
        assert valid is False

    def test_validate_uncached_schema(self):
        v = JsonSchemaValidator()
        valid, errors = v.validate(999, {"foo": "bar"})
        assert valid is False
        assert "No compiled validator" in errors[0]


class TestAvroSchemaValidator:
    def test_compile_and_validate_valid(self):
        v = AvroSchemaValidator()
        schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "name", "type": "string"}]
        }
        v.compile(1, schema)
        valid, errors = v.validate(1, {"name": "Alice"})
        assert valid is True

    def test_validate_invalid_data(self):
        v = AvroSchemaValidator()
        schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "name", "type": "string"}]
        }
        v.compile(1, schema)
        valid, errors = v.validate(1, {"name": 123})
        assert valid is False

    def test_validate_uncached_schema(self):
        v = AvroSchemaValidator()
        valid, errors = v.validate(999, {})
        assert valid is False


class TestValidatorManager:
    def test_compile_and_validate_json(self):
        m = ValidatorManager()
        schema = {"type": "object", "properties": {"x": {"type": "integer"}}, "required": ["x"]}
        m.compile(1, schema, "json")
        valid, _ = m.validate(1, {"x": 42}, "json")
        assert valid is True

    def test_compile_and_validate_avro(self):
        m = ValidatorManager()
        schema = {"type": "record", "name": "Test", "fields": [{"name": "x", "type": "int"}]}
        m.compile(1, schema, "avro")
        valid, _ = m.validate(1, {"x": 42}, "avro")
        assert valid is True

    def test_unsupported_type(self):
        m = ValidatorManager()
        valid, errors = m.validate(1, {}, "xml")
        assert valid is False
