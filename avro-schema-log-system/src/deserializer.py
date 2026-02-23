"""Avro deserialization utilities for log events."""

from io import BytesIO
from typing import Optional

import fastavro

from src.schema_registry import SchemaRegistry


class AvroDeserializer:
    """Deserializes Avro binary data back into Python dicts."""

    def __init__(self, registry: SchemaRegistry):
        """Initialize with a schema registry.

        Args:
            registry: A loaded :class:`SchemaRegistry` instance.
        """
        self._registry = registry

    def deserialize(
        self,
        data: bytes,
        writer_version: str,
        reader_version: Optional[str] = None,
    ) -> dict:
        """Deserialize schemaless Avro bytes into a dict.

        When *reader_version* differs from *writer_version*, Avro schema
        resolution is applied (fields added with defaults are filled in;
        removed fields are dropped).

        Args:
            data: The raw Avro-encoded bytes (no embedded schema).
            writer_version: Schema version the data was written with.
            reader_version: Schema version to project the data onto.
                            Defaults to *writer_version* when ``None``.

        Returns:
            A dict representing the deserialized log event.
        """
        writer_schema = self._registry.get_parsed_schema(writer_version)

        if reader_version is None:
            reader_version = writer_version
        reader_schema = self._registry.get_parsed_schema(reader_version)

        buf = BytesIO(data)
        return fastavro.schemaless_reader(buf, writer_schema, reader_schema)

    def deserialize_container(
        self,
        data: bytes,
        reader_version: Optional[str] = None,
    ) -> list[dict]:
        """Deserialize an Avro Object Container File into a list of dicts.

        Args:
            data: Bytes of a complete Avro container file.
            reader_version: Optional schema version for schema resolution.
                            If ``None``, reads using the embedded writer schema.

        Returns:
            A list of dicts, one per record in the container.
        """
        buf = BytesIO(data)

        reader_schema = None
        if reader_version is not None:
            reader_schema = self._registry.get_parsed_schema(reader_version)

        reader = fastavro.reader(buf, reader_schema=reader_schema)
        return list(reader)
