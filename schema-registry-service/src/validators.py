"""Schema validators with compiled caching."""
import jsonschema
import fastavro.schema
import fastavro.validation


class JsonSchemaValidator:
    """Validates data against JSON Schema (Draft 7) with compiled caching."""

    def __init__(self):
        self._cache = {}  # schema_id -> compiled validator

    def compile(self, schema_id, schema):
        """Compile and cache a JSON Schema validator."""
        validator_cls = jsonschema.Draft7Validator
        validator_cls.check_schema(schema)  # Validate the schema itself
        self._cache[schema_id] = validator_cls(schema)

    def validate(self, schema_id, data):
        """Validate data against a cached schema. Returns (valid, errors)."""
        validator = self._cache.get(schema_id)
        if not validator:
            return False, [f"No compiled validator for schema_id {schema_id}"]
        errors = list(validator.iter_errors(data))
        if errors:
            return False, [e.message for e in errors]
        return True, []


class AvroSchemaValidator:
    """Validates data against Avro schemas with compiled caching."""

    def __init__(self):
        self._cache = {}  # schema_id -> parsed avro schema

    def compile(self, schema_id, schema):
        """Parse and cache an Avro schema."""
        parsed = fastavro.schema.parse_schema(schema)
        self._cache[schema_id] = parsed

    def validate(self, schema_id, data):
        """Validate data against a cached Avro schema. Returns (valid, errors)."""
        parsed = self._cache.get(schema_id)
        if not parsed:
            return False, [f"No compiled validator for schema_id {schema_id}"]
        try:
            fastavro.validation.validate(data, parsed, raise_errors=True)
            return True, []
        except Exception as e:
            return False, [str(e)]


class ValidatorManager:
    """Manages both JSON and Avro validators."""

    def __init__(self):
        self.json_validator = JsonSchemaValidator()
        self.avro_validator = AvroSchemaValidator()

    def compile(self, schema_id, schema, schema_type):
        """Compile a schema based on its type."""
        if schema_type == "json":
            self.json_validator.compile(schema_id, schema)
        elif schema_type == "avro":
            self.avro_validator.compile(schema_id, schema)

    def validate(self, schema_id, data, schema_type):
        """Validate data against a schema based on its type."""
        if schema_type == "json":
            return self.json_validator.validate(schema_id, data)
        elif schema_type == "avro":
            return self.avro_validator.validate(schema_id, data)
        return False, [f"Unsupported schema_type: {schema_type}"]
