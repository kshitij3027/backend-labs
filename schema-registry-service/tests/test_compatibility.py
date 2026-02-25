"""Tests for CompatibilityChecker."""
from src.compatibility import CompatibilityChecker


class TestJsonCompatibility:
    def setup_method(self):
        self.checker = CompatibilityChecker()

    def test_add_optional_property_safe(self):
        old = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        new = {"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}, "required": ["name"]}
        compatible, issues = self.checker.check_backward(new, old, "json")
        assert compatible is True
        assert issues == []

    def test_new_required_field_breaking(self):
        old = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        new = {"type": "object", "properties": {"name": {"type": "string"}, "email": {"type": "string"}}, "required": ["name", "email"]}
        compatible, issues = self.checker.check_backward(new, old, "json")
        assert compatible is False
        assert any("email" in i for i in issues)

    def test_remove_property_breaking(self):
        old = {"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}}
        new = {"type": "object", "properties": {"name": {"type": "string"}}}
        compatible, issues = self.checker.check_backward(new, old, "json")
        assert compatible is False
        assert any("age" in i for i in issues)

    def test_change_type_breaking(self):
        old = {"type": "object", "properties": {"age": {"type": "integer"}}}
        new = {"type": "object", "properties": {"age": {"type": "string"}}}
        compatible, issues = self.checker.check_backward(new, old, "json")
        assert compatible is False
        assert any("type" in i.lower() for i in issues)

    def test_required_to_optional_safe(self):
        old = {"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}, "required": ["name", "age"]}
        new = {"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}, "required": ["name"]}
        compatible, issues = self.checker.check_backward(new, old, "json")
        assert compatible is True


class TestAvroCompatibility:
    def setup_method(self):
        self.checker = CompatibilityChecker()

    def test_new_field_with_default_safe(self):
        old = {"type": "record", "name": "T", "fields": [{"name": "a", "type": "string"}]}
        new = {"type": "record", "name": "T", "fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "string", "default": ""}]}
        compatible, issues = self.checker.check_backward(new, old, "avro")
        assert compatible is True

    def test_new_field_without_default_breaking(self):
        old = {"type": "record", "name": "T", "fields": [{"name": "a", "type": "string"}]}
        new = {"type": "record", "name": "T", "fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]}
        compatible, issues = self.checker.check_backward(new, old, "avro")
        assert compatible is False
        assert any("b" in i for i in issues)

    def test_remove_field_without_default_breaking(self):
        old = {"type": "record", "name": "T", "fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]}
        new = {"type": "record", "name": "T", "fields": [{"name": "a", "type": "string"}]}
        compatible, issues = self.checker.check_backward(new, old, "avro")
        assert compatible is False

    def test_remove_field_with_default_safe(self):
        old = {"type": "record", "name": "T", "fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "string", "default": "x"}]}
        new = {"type": "record", "name": "T", "fields": [{"name": "a", "type": "string"}]}
        compatible, issues = self.checker.check_backward(new, old, "avro")
        assert compatible is True
