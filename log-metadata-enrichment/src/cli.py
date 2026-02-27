"""Click CLI for the log metadata enrichment pipeline."""

from __future__ import annotations

import json
import sys

import click

from src.config import AppConfig
from src.enricher import LogEnricher
from src.formatter import format_enriched_log, format_enriched_log_dict
from src.models import EnrichmentRequest


@click.group()
@click.version_option(version="1.0.0", prog_name="log-enrichment")
def cli():
    """Log Metadata Enrichment Pipeline CLI."""
    pass


@cli.command()
@click.argument("log_message")
@click.option("--source", default="cli", help="Log source identifier")
@click.option("--pretty/--compact", default=True, help="Pretty or compact JSON output")
def enrich(log_message, source, pretty):
    """Enrich a single log message with metadata."""
    config = AppConfig()
    enricher = LogEnricher(config)
    request = EnrichmentRequest(log_message=log_message, source=source)
    enriched = enricher.enrich(request)

    if pretty:
        click.echo(format_enriched_log(enriched))
    else:
        click.echo(enriched.model_dump_json(exclude_none=True))


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--source", default="batch", help="Log source identifier")
@click.option("--output", "-o", type=click.Path(), help="Output file (default: stdout)")
def batch(input_file, source, output):
    """Enrich log messages from a file in batch mode."""
    config = AppConfig()
    enricher = LogEnricher(config)
    results = []

    with open(input_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            request = EnrichmentRequest(log_message=line, source=source)
            enriched = enricher.enrich(request)
            results.append(format_enriched_log_dict(enriched))

    json_output = json.dumps(results, indent=2)

    if output:
        with open(output, "w") as f:
            f.write(json_output)
    else:
        click.echo(json_output)

    stats = enricher.get_stats()
    click.echo(
        f"Processed: {stats.processed_count}, "
        f"Errors: {stats.error_count}, "
        f"Rate: {stats.success_rate:.1%}, "
        f"Throughput: {stats.average_throughput:.1f} logs/sec",
        err=True,
    )


@cli.command()
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8080, type=int, help="Port to listen on")
@click.option("--debug", is_flag=True, help="Enable debug mode")
def serve(host, port, debug):
    """Start the web server."""
    from src.web.app import create_app

    config = AppConfig(host=host, port=port, debug=debug)
    app = create_app(config)
    click.echo(f"Starting server on {host}:{port}...")
    app.run(host=host, port=port, debug=debug)
