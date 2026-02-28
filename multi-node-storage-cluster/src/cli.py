"""CLI entry point for the multi-node storage cluster."""

import click


@click.group()
def cli():
    """Multi-node storage cluster management CLI."""
    pass


@cli.command()
@click.option("--node-id", envvar="NODE_ID", default=None)
@click.option("--port", envvar="PORT", type=int, default=5001)
def serve(node_id, port):
    """Start a storage node."""
    from src.config import load_config
    from src.storage_node import create_app

    config = load_config()
    app = create_app(config)
    click.echo(f"Starting storage node {config.node_id} on port {config.port}...")
    app.run(host=config.host, port=config.port, threaded=True)


@cli.command()
def dashboard():
    """Start the monitoring dashboard (to be implemented in Commit 7)."""
    click.echo("Starting dashboard...")
