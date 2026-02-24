"""CLI interface for the Universal Log Translator."""
import json
import sys

import click

from src.normalizer import LogNormalizer

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
def translate(input_file: str, fmt: str | None, output_fmt: str) -> None:
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

    normalizer = LogNormalizer()

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
