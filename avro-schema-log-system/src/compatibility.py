"""Schema compatibility checking via trial serialization / deserialization."""

from fastavro._read_common import SchemaResolutionError

from src.deserializer import AvroDeserializer
from src.log_event import LogEvent
from src.schema_registry import SchemaRegistry
from src.serializer import AvroSerializer


class CompatibilityChecker:
    """Tests cross-version compatibility by round-tripping sample data."""

    def __init__(
        self,
        registry: SchemaRegistry,
        serializer: AvroSerializer,
        deserializer: AvroDeserializer,
    ):
        self._registry = registry
        self._serializer = serializer
        self._deserializer = deserializer

    def check_compatibility(self, writer_version: str, reader_version: str) -> bool:
        """Check whether data written with one schema can be read with another.

        Generates a sample event for *writer_version*, serializes it, then
        attempts to deserialize using *reader_version*.

        Args:
            writer_version: Schema version used to write data.
            reader_version: Schema version used to read data.

        Returns:
            ``True`` if the round-trip succeeds, ``False`` otherwise.
        """
        try:
            sample = LogEvent.generate_sample(writer_version)
            event_dict = sample.to_dict(writer_version)
            data = self._serializer.serialize(event_dict, writer_version)
            self._deserializer.deserialize(data, writer_version, reader_version)
            return True
        except (SchemaResolutionError, Exception):
            return False

    def build_compatibility_matrix(self) -> dict:
        """Build a full NxN compatibility matrix across all loaded versions.

        Returns:
            A nested dict where ``matrix[writer][reader]`` is ``True`` when
            data written with *writer* can be read by *reader*.
        """
        versions = self._registry.list_versions()
        matrix: dict[str, dict[str, bool]] = {}
        for writer in versions:
            matrix[writer] = {}
            for reader in versions:
                matrix[writer][reader] = self.check_compatibility(writer, reader)
        return matrix
