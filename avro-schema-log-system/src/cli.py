"""Click CLI for the Avro Schema Evolution Log System."""

import json
import sys
import time

import click

from src.compatibility import CompatibilityChecker
from src.deserializer import AvroDeserializer
from src.log_event import LogEvent
from src.schema_registry import SchemaRegistry
from src.serializer import AvroSerializer


def _build_components():
    """Instantiate the shared registry, serializer, deserializer, and checker."""
    registry = SchemaRegistry()
    serializer = AvroSerializer(registry)
    deserializer = AvroDeserializer(registry)
    checker = CompatibilityChecker(registry, serializer, deserializer)
    return registry, serializer, deserializer, checker


@click.group()
def cli():
    """Avro Schema Evolution Log System CLI."""
    pass


# ── Schema commands ──────────────────────────────────────────────────────────

@cli.group()
def schemas():
    """Schema management commands."""
    pass


@schemas.command("list")
def schemas_list():
    """List all schema versions with field counts."""
    registry, _, _, _ = _build_components()
    versions = registry.list_versions()

    click.echo(f"{'Version':<10} {'Fields':<8} {'Field Names'}")
    click.echo("-" * 60)
    for v in versions:
        fields = registry.get_field_names(v)
        click.echo(f"{v:<10} {len(fields):<8} {', '.join(fields)}")


@schemas.command("show")
@click.argument("version")
def schemas_show(version):
    """Show schema details for a specific version."""
    registry, _, _, _ = _build_components()
    try:
        schema = registry.get_schema(version)
    except KeyError:
        click.echo(f"Error: Unknown schema version '{version}'", err=True)
        sys.exit(1)

    click.echo(f"Schema Version: {version}")
    click.echo(f"Name: {schema.get('name', 'N/A')}")
    click.echo(f"Namespace: {schema.get('namespace', 'N/A')}")
    click.echo(f"Type: {schema.get('type', 'N/A')}")
    click.echo(f"Fields ({len(schema['fields'])}):")
    for field in schema["fields"]:
        field_type = field["type"]
        default = field.get("default", "<none>")
        click.echo(f"  - {field['name']}: type={json.dumps(field_type)}, default={json.dumps(default)}")


# ── Compatibility commands ───────────────────────────────────────────────────

@cli.group()
def compatibility():
    """Compatibility checking commands."""
    pass


@compatibility.command("check")
@click.option("--writer", required=True, help="Writer schema version (e.g. v1)")
@click.option("--reader", required=True, help="Reader schema version (e.g. v2)")
def compat_check(writer, reader):
    """Check compatibility between a writer and reader schema version."""
    registry, serializer, deserializer, checker = _build_components()

    # Validate versions
    versions = registry.list_versions()
    for v, label in [(writer, "writer"), (reader, "reader")]:
        if v not in versions:
            click.echo(f"Error: Unknown {label} version '{v}'. Available: {versions}", err=True)
            sys.exit(1)

    compatible = checker.check_compatibility(writer, reader)
    status = "COMPATIBLE" if compatible else "INCOMPATIBLE"
    symbol = "+" if compatible else "x"
    click.echo(f"[{symbol}] {writer} -> {reader}: {status}")


@compatibility.command("matrix")
def compat_matrix():
    """Show the full NxN compatibility matrix."""
    _, _, _, checker = _build_components()
    registry = checker._registry
    versions = registry.list_versions()
    matrix = checker.build_compatibility_matrix()

    # Header row
    header = f"{'W\\R':<8}" + "".join(f"{v:<8}" for v in versions)
    click.echo(header)
    click.echo("-" * len(header))

    for writer in versions:
        row = f"{writer:<8}"
        for reader in versions:
            mark = "yes" if matrix[writer][reader] else "NO"
            row += f"{mark:<8}"
        click.echo(row)


# ── Benchmark command ────────────────────────────────────────────────────────

@cli.command()
@click.option("--iterations", default=10000, help="Number of serialize/deserialize cycles")
@click.option("--schema-version", default=None, help="Schema version to benchmark (default: all)")
def benchmark(iterations, schema_version):
    """Run serialization/deserialization benchmark."""
    registry, serializer, deserializer, _ = _build_components()

    if schema_version:
        versions = [schema_version]
    else:
        versions = registry.list_versions()

    for v in versions:
        # Validate
        if v not in registry.list_versions():
            click.echo(f"Error: Unknown schema version '{v}'", err=True)
            sys.exit(1)

        sample = LogEvent.generate_sample(v)
        event_dict = sample.to_dict(v)

        # Benchmark serialization
        start = time.perf_counter()
        for _ in range(iterations):
            data = serializer.serialize(event_dict, v)
        ser_elapsed = time.perf_counter() - start

        # Benchmark deserialization
        start = time.perf_counter()
        for _ in range(iterations):
            deserializer.deserialize(data, v)
        deser_elapsed = time.perf_counter() - start

        click.echo(f"Schema {v} ({iterations} iterations):")
        click.echo(f"  Serialize:   {ser_elapsed:.3f}s ({iterations / ser_elapsed:.0f} ops/sec)")
        click.echo(f"  Deserialize: {deser_elapsed:.3f}s ({iterations / deser_elapsed:.0f} ops/sec)")
        click.echo(f"  Payload size: {len(data)} bytes")
        click.echo()


# ── Serialize command ────────────────────────────────────────────────────────

@cli.command()
@click.option("--schema-version", required=True, help="Schema version (e.g. v1)")
@click.option("--output", required=True, type=click.Path(), help="Output .avro file path")
def serialize(schema_version, output):
    """Serialize a sample log event to an Avro container file."""
    registry, serializer, _, _ = _build_components()

    if schema_version not in registry.list_versions():
        click.echo(f"Error: Unknown schema version '{schema_version}'", err=True)
        sys.exit(1)

    sample = LogEvent.generate_sample(schema_version)
    event_dict = sample.to_dict(schema_version)
    data = serializer.serialize_to_container([event_dict], schema_version)

    with open(output, "wb") as f:
        f.write(data)

    click.echo(f"Serialized 1 event (schema {schema_version}) to {output}")
    click.echo(f"  Size: {len(data)} bytes")
    click.echo(f"  Event: {json.dumps(event_dict, indent=2)}")


# ── Deserialize command ──────────────────────────────────────────────────────

@cli.command()
@click.option("--schema-version", required=True, help="Reader schema version (e.g. v1)")
@click.option("--input", "input_file", required=True, type=click.Path(exists=True), help="Input .avro file path")
def deserialize(schema_version, input_file):
    """Deserialize an Avro container file and print the records."""
    registry, _, deserializer, _ = _build_components()

    if schema_version not in registry.list_versions():
        click.echo(f"Error: Unknown schema version '{schema_version}'", err=True)
        sys.exit(1)

    with open(input_file, "rb") as f:
        data = f.read()

    records = deserializer.deserialize_container(data, reader_version=schema_version)

    click.echo(f"Deserialized {len(records)} record(s) from {input_file} (reader schema: {schema_version}):")
    for i, record in enumerate(records):
        click.echo(f"\n  Record {i + 1}:")
        click.echo(f"  {json.dumps(record, indent=4, default=str)}")


if __name__ == "__main__":
    cli()
