"""CLI for the log format compatibility layer."""
import os
import sys
import json
import click
from src.detection import FormatDetectionEngine
from src.pipeline import process_file
from src.formatters import get_formatter


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """Log Format Compatibility Layer - Auto-detect, parse, and translate log formats."""
    pass


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--output-dir", "-o", default="output", help="Output directory for results")
@click.option("--format", "-f", "output_format", default="json",
              type=click.Choice(["json", "structured", "plain"]),
              help="Output format")
def process(input_file, output_dir, output_format):
    """Process a log file and translate to unified format.

    INPUT_FILE: Path to the log file to process.
    """
    # Ensure output dir exists
    os.makedirs(output_dir, exist_ok=True)

    basename = os.path.splitext(os.path.basename(input_file))[0]
    ext = ".json" if output_format == "json" else ".txt"
    output_path = os.path.join(output_dir, f"{basename}_output{ext}")

    click.echo(f"Processing: {input_file}")
    click.echo(f"Output format: {output_format}")
    click.echo(f"Output file: {output_path}")
    click.echo("")

    metrics = None
    count = 0

    with open(output_path, "w") as out:
        for result, data in process_file(input_file, output_format=output_format):
            if result == "__metrics__":
                metrics = data
                continue
            out.write(result + "\n")
            count += 1

    click.echo(f"Processed {count} log entries")

    if metrics:
        click.echo(f"\n--- Metrics ---")
        click.echo(f"Total lines:    {metrics['total_lines']}")
        click.echo(f"Successful:     {metrics['successful']}")
        click.echo(f"Failed:         {metrics['failed']}")
        click.echo(f"Skipped:        {metrics['skipped']}")
        click.echo(f"Success rate:   {metrics['success_rate_percent']:.1f}%")
        click.echo(f"Throughput:     {metrics['throughput_per_second']:.0f} lines/sec")
        click.echo(f"\nFormat distribution:")
        for fmt, cnt in metrics['format_distribution'].items():
            click.echo(f"  {fmt}: {cnt}")

        # Also write metrics to a separate file
        metrics_path = os.path.join(output_dir, f"{basename}_metrics.json")
        with open(metrics_path, "w") as mf:
            json.dump(metrics, mf, indent=2)
        click.echo(f"\nMetrics saved to: {metrics_path}")


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
def detect(input_file):
    """Detect log formats in a file without full parsing.

    INPUT_FILE: Path to the log file to analyze.
    """
    engine = FormatDetectionEngine()

    with open(input_file, "r") as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]

    result = engine.detect_batch(lines)

    click.echo(f"File: {input_file}")
    click.echo(f"Total lines: {result['total_lines']}")
    click.echo(f"Detected: {result['detected_lines']}")
    click.echo(f"Detection rate: {result['detection_rate']:.1%}")
    click.echo(f"\nFormat breakdown:")

    for fmt, info in result['formats'].items():
        click.echo(f"  {fmt}:")
        click.echo(f"    Count: {info['count']}")
        click.echo(f"    Avg confidence: {info['avg_confidence']:.3f}")


@cli.command()
@click.option("--host", "-H", default="0.0.0.0", help="Host to bind to")
@click.option("--port", "-p", default=8080, type=int, help="Port to listen on")
@click.option("--debug/--no-debug", default=False, help="Enable debug mode")
def serve(host, port, debug):
    """Start the web UI server."""
    from src.web.app import create_app
    app = create_app()
    click.echo(f"Starting web UI on {host}:{port}")
    app.run(host=host, port=port, debug=debug)


@cli.command()
@click.option("--type", "-t", "sample_type", default="mixed",
              type=click.Choice(["syslog", "journald", "json", "mixed"]),
              help="Type of sample data to show")
@click.option("--parse/--no-parse", default=True, help="Also show parsed output")
def sample(sample_type, parse):
    """Show sample log data and optionally parse it.

    Useful for testing and demonstration.
    """
    sample_dir = "logs/samples"
    file_map = {
        "syslog": "syslog_sample.txt",
        "journald": "journald_sample.txt",
        "json": "json_sample.txt",
        "mixed": "mixed_sample.txt",
    }

    filepath = os.path.join(sample_dir, file_map[sample_type])

    if not os.path.exists(filepath):
        click.echo(f"Sample file not found: {filepath}", err=True)
        sys.exit(1)

    click.echo(f"=== Sample {sample_type} logs ===\n")

    engine = FormatDetectionEngine()

    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            click.echo(f"RAW: {line}")

            if parse:
                parsed = engine.parse_line(line)
                if parsed:
                    click.echo(f"  Format:     {parsed.source_format}")
                    click.echo(f"  Confidence: {parsed.confidence:.2f}")
                    click.echo(f"  Message:    {parsed.message}")
                    if parsed.level:
                        click.echo(f"  Level:      {parsed.level.name}")
                    if parsed.hostname:
                        click.echo(f"  Hostname:   {parsed.hostname}")
                else:
                    click.echo("  [Unrecognized format]")
            click.echo("")
