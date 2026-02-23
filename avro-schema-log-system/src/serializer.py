"""Avro serialization utilities for log events."""

from io import BytesIO

import fastavro

from src.schema_registry import SchemaRegistry


class AvroSerializer:
    """Serializes log event dicts into Avro binary format."""

    def __init__(self, registry: SchemaRegistry):
        """Initialize with a schema registry.

        Args:
            registry: A loaded :class:`SchemaRegistry` instance.
        """
        self._registry = registry

    def serialize(self, event_dict: dict, version: str) -> bytes:
        """Serialize a single event dict using schemaless encoding.

        Args:
            event_dict: The log event as a plain dict.
            version: Schema version to encode against (e.g. ``"v1"``).

        Returns:
            The Avro-encoded bytes (no embedded schema).
        """
        schema = self._registry.get_parsed_schema(version)
        buf = BytesIO()
        fastavro.schemaless_writer(buf, schema, event_dict)
        return buf.getvalue()

    def serialize_to_container(self, events: list[dict], version: str) -> bytes:
        """Serialize multiple events into an Avro Object Container File.

        The container format embeds the schema and supports multiple records,
        making it suitable for file-based storage and transport.

        Args:
            events: List of log event dicts.
            version: Schema version to use (e.g. ``"v1"``).

        Returns:
            Bytes representing a complete Avro container file.
        """
        schema = self._registry.get_parsed_schema(version)
        buf = BytesIO()
        fastavro.writer(buf, schema, events)
        return buf.getvalue()
