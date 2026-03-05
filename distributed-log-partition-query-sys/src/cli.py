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
    import uvicorn
    from src.config import load_partition_config
    from src.partition.app import create_partition_app

    config = load_partition_config()
    config.host = host
    config.port = port
    config.partition_id = partition_id

    app = create_partition_app(config)
    click.echo(f"Starting partition {partition_id} on {host}:{port}...")
    uvicorn.run(app, host=host, port=port)
