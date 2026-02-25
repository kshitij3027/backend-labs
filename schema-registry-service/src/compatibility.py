"""Backward compatibility checker for JSON Schema and Avro schemas."""


class CompatibilityChecker:
    """Check backward compatibility between schema versions."""

    def check_backward(self, new_schema, old_schema, schema_type):
        """Check if new_schema is backward compatible with old_schema.

        Returns (compatible, issues) where issues is a list of breaking change descriptions.
        """
        if schema_type == "json":
            return self._check_json_backward(new_schema, old_schema)
        elif schema_type == "avro":
            return self._check_avro_backward(new_schema, old_schema)
        return False, [f"Unsupported schema_type: {schema_type}"]

    def _check_json_backward(self, new_schema, old_schema):
        """Check backward compatibility for JSON Schema.

        Rules:
        - Adding optional properties -> SAFE
        - New required field without default -> BREAKING
        - Removing existing property -> BREAKING
        - Changing property type -> BREAKING
        - Required -> optional -> SAFE
        """
        issues = []
        old_props = old_schema.get("properties", {})
        new_props = new_schema.get("properties", {})
        old_required = set(old_schema.get("required", []))
        new_required = set(new_schema.get("required", []))

        # Check for removed properties
        for prop_name in old_props:
            if prop_name not in new_props:
                issues.append(f"Removed property '{prop_name}'")

        # Check for type changes in existing properties
        for prop_name in old_props:
            if prop_name in new_props:
                old_type = old_props[prop_name].get("type")
                new_type = new_props[prop_name].get("type")
                if old_type and new_type and old_type != new_type:
                    issues.append(f"Changed type of '{prop_name}' from '{old_type}' to '{new_type}'")

        # Check for new required fields (that didn't exist before)
        for prop_name in new_required:
            if prop_name not in old_props and prop_name not in old_required:
                # Completely new required field
                if new_props.get(prop_name, {}).get("default") is None:
                    issues.append(f"New required property '{prop_name}' without default")

        compatible = len(issues) == 0
        return compatible, issues

    def _check_avro_backward(self, new_schema, old_schema):
        """Check backward compatibility for Avro schemas.

        Rules:
        - New field with default -> SAFE
        - New field without default -> BREAKING
        - Removed field without default in old schema -> BREAKING
        """
        issues = []
        old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
        new_fields = {f["name"]: f for f in new_schema.get("fields", [])}

        # Check for removed fields
        for fname, field in old_fields.items():
            if fname not in new_fields:
                if "default" not in field:
                    issues.append(f"Removed field '{fname}' that has no default in old schema")

        # Check for new fields without default
        for fname, field in new_fields.items():
            if fname not in old_fields:
                if "default" not in field:
                    issues.append(f"New field '{fname}' without default value")

        compatible = len(issues) == 0
        return compatible, issues
