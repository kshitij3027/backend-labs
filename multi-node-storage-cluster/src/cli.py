"""CLI entry point for the multi-node storage cluster."""

import click


@click.group()
def cli():
    """Multi-node storage cluster management CLI."""
    pass


@cli.command()
def serve():
    """Start a storage node."""
    from src.cluster_manager import ClusterManager
    from src.config import load_config
    from src.consistent_hash import HashRing
    from src.replication import ReplicationManager
    from src.storage_node import create_app

    config = load_config()

    node_ids = [n["id"] for n in config.cluster_nodes]
    hash_ring = HashRing(node_ids) if node_ids else HashRing([config.node_id])
    replication_manager = ReplicationManager(config, hash_ring)
    cluster_manager = ClusterManager(config)

    app = create_app(config, replication_manager=replication_manager, cluster_manager=cluster_manager)
    click.echo(f"Starting storage node {config.node_id} on port {config.port}...")
    app.run(host=config.host, port=config.port, threaded=True)


@cli.command()
def dashboard():
    """Start the monitoring dashboard (to be implemented in Commit 7)."""
    click.echo("Starting dashboard...")
