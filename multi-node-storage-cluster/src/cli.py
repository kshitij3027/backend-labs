"""CLI entry point for the multi-node storage cluster."""

import click


@click.group()
def cli():
    """Multi-node storage cluster management CLI."""
    pass


@cli.command()
def serve():
    """Start a storage node (to be implemented in Commit 2)."""
    click.echo("Starting storage node...")


@cli.command()
def dashboard():
    """Start the monitoring dashboard (to be implemented in Commit 7)."""
    click.echo("Starting dashboard...")
