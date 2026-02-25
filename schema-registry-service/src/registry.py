"""Schema Registry: registration, versioning, deduplication, retrieval."""
import hashlib
import json
from datetime import datetime, timezone


class SchemaRegistry:
    def __init__(self, storage):
        self._storage = storage

    def _hash_schema(self, schema):
        """SHA-256 hash of canonical JSON for deduplication."""
        canonical = json.dumps(schema, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def register(self, subject, schema, schema_type="json"):
        """Register a schema under a subject. Returns (schema_record, created)."""
        state = self._storage.get_state()
        schema_hash = self._hash_schema(schema)

        # Check for duplicate: same subject + same hash
        subject_info = state["subjects"].get(subject, {"versions": [], "schema_ids": [], "compatibility": "BACKWARD"})
        for sid in subject_info.get("schema_ids", []):
            existing = state["schemas"].get(str(sid))
            if existing and existing["hash"] == schema_hash:
                return existing, False  # Deduplicated

        # New schema version
        schema_id = state["next_id"]
        version = len(subject_info.get("versions", [])) + 1

        record = {
            "id": schema_id,
            "subject": subject,
            "version": version,
            "schema": schema,
            "schema_type": schema_type,
            "hash": schema_hash,
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }

        state["schemas"][str(schema_id)] = record
        state["next_id"] = schema_id + 1

        if subject not in state["subjects"]:
            state["subjects"][subject] = {"versions": [], "schema_ids": [], "compatibility": "BACKWARD"}
        state["subjects"][subject]["versions"].append(version)
        state["subjects"][subject]["schema_ids"].append(schema_id)

        self._storage.set_state(state)
        return record, True

    def get_latest(self, subject):
        """Get the latest schema for a subject. Raises KeyError if not found."""
        state = self._storage.get_state()
        if subject not in state["subjects"]:
            raise KeyError(f"Subject '{subject}' not found")
        schema_ids = state["subjects"][subject]["schema_ids"]
        latest_id = schema_ids[-1]
        return state["schemas"][str(latest_id)]

    def get_version(self, subject, version):
        """Get a specific version of a schema. Raises KeyError if not found."""
        state = self._storage.get_state()
        if subject not in state["subjects"]:
            raise KeyError(f"Subject '{subject}' not found")
        subject_info = state["subjects"][subject]
        versions = subject_info["versions"]
        if version not in versions:
            raise KeyError(f"Version {version} not found for subject '{subject}'")
        idx = versions.index(version)
        schema_id = subject_info["schema_ids"][idx]
        return state["schemas"][str(schema_id)]

    def list_subjects(self):
        """List all subject names."""
        state = self._storage.get_state()
        return sorted(state["subjects"].keys())

    def list_versions(self, subject):
        """List all version numbers for a subject. Raises KeyError if not found."""
        state = self._storage.get_state()
        if subject not in state["subjects"]:
            raise KeyError(f"Subject '{subject}' not found")
        return state["subjects"][subject]["versions"]

    def get_by_id(self, schema_id):
        """Get a schema by its global ID. Raises KeyError if not found."""
        state = self._storage.get_state()
        record = state["schemas"].get(str(schema_id))
        if not record:
            raise KeyError(f"Schema ID {schema_id} not found")
        return record
