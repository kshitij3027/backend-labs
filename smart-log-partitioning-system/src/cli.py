"""CLI commands for the Smart Log Partitioning System."""

import click

from src.app import create_app
from src.config import load_config


@click.group()
def cli():
    """Smart Log Partitioning System CLI."""
    pass


@cli.command()
@click.option("--host", default=None, help="Host to bind to")
@click.option("--port", default=None, type=int, help="Port to bind to")
@click.option("--debug", is_flag=True, default=False, help="Enable debug mode")
def serve(host, port, debug):
    """Start the Flask web server."""
    config = load_config()
    if host:
        config.host = host
    if port:
        config.port = port

    app = create_app(config)
    app.run(host=config.host, port=config.port, debug=debug)


@cli.command()
@click.option("--count", default=1000, help="Number of logs to generate")
@click.option("--nodes", default=3, type=int, help="Number of partition nodes")
@click.option("--strategy", default="source", help="Partition strategy")
def demo(count, nodes, strategy):
    """Run the benchmark demo."""
    # Import here to avoid circular imports
    from scripts.demo import run_demo

    run_demo(count=count, nodes=nodes, strategy=strategy)
