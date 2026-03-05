import click


@click.group()
def cli():
    """Distributed Log Query System."""
    pass


@cli.command()
@click.option("--host", envvar="HOST", default="0.0.0.0")
@click.option("--port", envvar="PORT", type=int, default=8080)
def coordinator(host, port):
    """Start the Query Coordinator server."""
    click.echo(f"Coordinator starting on {host}:{port}...")


@cli.command()
@click.option("--host", envvar="HOST", default="0.0.0.0")
@click.option("--port", envvar="PORT", type=int, default=8081)
@click.option("--partition-id", envvar="PARTITION_ID", default="partition_1")
def partition(host, port, partition_id):
    """Start a Partition Server."""
    click.echo(f"Partition {partition_id} starting on {host}:{port}...")
