"""CLI interface for the Universal Log Translator."""
import json
import sys

import click

from src.normalizer import LogNormalizer
from src.performance import PerformanceAwareNormalizer

# Import handlers to trigger auto-registration
import src.handlers  # noqa: F401


@click.group()
def cli():
    """Universal Log Translator - normalize logs from any format."""
    pass


@cli.command()
@click.argument("input_file")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["json", "text", "protobuf", "avro"], case_sensitive=False),
    default=None,
    help="Force a specific input format (skips auto-detection).",
)
@click.option(
    "--output",
    "output_fmt",
    type=click.Choice(["json", "text"], case_sensitive=False),
    default="json",
    help="Output format (default: json).",
)
@click.option("--adaptive", is_flag=True, help="Enable adaptive performance tracking")
def translate(input_file: str, fmt: str | None, output_fmt: str, adaptive: bool) -> None:
    """Translate a log file into a normalized format.

    INPUT_FILE is a path to a log file, or '-' to read from stdin.
    """
    try:
        if input_file == "-":
            raw_data = sys.stdin.buffer.read()
        else:
            with open(input_file, "rb") as f:
                raw_data = f.read()
    except FileNotFoundError:
        raise click.ClickException(f"File not found: {input_file}")
    except OSError as e:
        raise click.ClickException(f"Error reading file: {e}")

    normalizer = PerformanceAwareNormalizer() if adaptive else LogNormalizer()

    try:
        entry = normalizer.normalize(raw_data, source_format=fmt)
    except Exception as e:
        raise click.ClickException(f"Failed to parse log: {e}")

    if output_fmt == "json":
        click.echo(json.dumps(entry.to_dict(), indent=2))
    else:
        click.echo(
            f"[{entry.timestamp.isoformat()}] [{entry.level.value}] "
            f"{entry.message} (source={entry.source_format})"
        )


@cli.command()
@click.option("--count", default=10000, help="Number of logs to process")
@click.option("--adaptive", is_flag=True, help="Enable adaptive performance tracking")
def benchmark(count, adaptive):
    """Run a benchmark with mixed-format logs."""
    import time
    import random
    import json as json_module
    import io
    import fastavro
    from src.generated import log_entry_pb2

    click.echo(f"Benchmarking with {count} mixed-format logs...")
    click.echo("")

    if adaptive:
        normalizer = PerformanceAwareNormalizer()
    else:
        normalizer = LogNormalizer()

    # Generate sample data for each format
    json_data = json_module.dumps({
        "timestamp": "2024-01-15T10:30:00",
        "level": "INFO",
        "message": "Benchmark test message",
        "source": "benchmark",
        "hostname": "bench-host",
        "service": "bench-service",
    }).encode()

    text_data = b"<165>1 2024-01-15T10:30:00.000Z bench-host bench-service 1234 - - Benchmark test message"

    pb_entry = log_entry_pb2.LogEntry()
    pb_entry.timestamp = "2024-01-15T10:30:00"
    pb_entry.level = log_entry_pb2.LOG_LEVEL_INFO
    pb_entry.message = "Benchmark test message"
    pb_entry.source = "benchmark"
    pb_entry.hostname = "bench-host"
    pb_entry.service = "bench-service"
    pb_data = pb_entry.SerializeToString()

    avro_schema = {
        "type": "record",
        "name": "LogEntry",
        "namespace": "com.logtranslator",
        "fields": [
            {"name": "timestamp", "type": "string"},
            {"name": "level", "type": "string"},
            {"name": "message", "type": "string"},
            {"name": "source", "type": ["null", "string"], "default": None},
            {"name": "hostname", "type": ["null", "string"], "default": None},
            {"name": "service", "type": ["null", "string"], "default": None},
            {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}},
        ],
    }
    parsed_schema = fastavro.parse_schema(avro_schema)
    buf = io.BytesIO()
    fastavro.writer(buf, parsed_schema, [{
        "timestamp": "2024-01-15T10:30:00",
        "level": "INFO",
        "message": "Benchmark test message",
        "source": "benchmark",
        "hostname": "bench-host",
        "service": "bench-service",
        "metadata": {},
    }])
    avro_data = buf.getvalue()

    samples = [json_data, text_data, pb_data, avro_data]
    format_names = ["json", "text", "protobuf", "avro"]
    format_counts = {name: 0 for name in format_names}

    successes = 0
    errors = 0
    error_details = []

    start_time = time.perf_counter()

    for i in range(count):
        idx = random.randint(0, 3)
        data = samples[idx]
        format_counts[format_names[idx]] += 1
        try:
            normalizer.normalize(data)
            successes += 1
        except Exception as e:
            errors += 1
            if len(error_details) < 5:
                error_details.append(f"{format_names[idx]}: {e}")

    elapsed = time.perf_counter() - start_time
    throughput = count / elapsed

    click.echo("=== Benchmark Results ===")
    click.echo(f"Total logs:     {count}")
    click.echo(f"Successes:      {successes}")
    click.echo(f"Errors:         {errors}")
    click.echo(f"Success rate:   {successes/count:.1%}")
    click.echo(f"Elapsed:        {elapsed:.3f}s")
    click.echo(f"Throughput:     {throughput:.0f} logs/sec")
    click.echo("")
    click.echo("Format distribution:")
    for name, cnt in sorted(format_counts.items()):
        click.echo(f"  {name}: {cnt}")

    if adaptive and isinstance(normalizer, PerformanceAwareNormalizer):
        click.echo("")
        click.echo(normalizer.stats_report)

    if error_details:
        click.echo("")
        click.echo("Sample errors:")
        for detail in error_details:
            click.echo(f"  {detail}")


@cli.command()
@click.argument("input_file")
def detect(input_file: str) -> None:
    """Detect the log format of a file.

    INPUT_FILE is a path to a log file.
    """
    try:
        with open(input_file, "rb") as f:
            raw_data = f.read()
    except FileNotFoundError:
        raise click.ClickException(f"File not found: {input_file}")
    except OSError as e:
        raise click.ClickException(f"Error reading file: {e}")

    from src.detector import FormatDetector

    detector = FormatDetector()

    try:
        handler = detector.detect(raw_data)
        click.echo(f"Detected format: {handler.format_name}")
    except Exception as e:
        raise click.ClickException(f"Could not detect format: {e}")
